using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Worf;

/// <summary>
/// Looks a host name up as libpq does, with the system's getaddrinfo for stream sockets of any family, so that a probe
/// connects to the addresses libpq would, in its order: .NET's own lookup adds every address of the machine's interfaces
/// for the machine's own name, which getaddrinfo doesn't give. Outside Windows, getaddrinfo is the one the server's own
/// symbols resolve to, as libpq's calls are, so that a library the server preloads that answers lookups answers these
/// too. The structures' layouts are the platforms' own:
/// addrinfo's address comes before its canonical name on Linux and after it on macOS and Windows, and macOS's sockaddr
/// starts with its length.
/// </summary>
internal static unsafe class HostLookup
{
    private const int SockStream = 1;
    private const int AddressFamilyInet = 2;

    // Each name's latest lookup, and whether a probe has taken its outcome since it ended.
    private static readonly Dictionary<string, (Task<IPAddress[]> Lookup, StrongBox<bool> Taken)> s_lookups = [];

    /// <summary>
    /// Starts looking a host name up away from the calling thread, or joins its lookup under way, or takes the outcome
    /// of one that ended after the probe that started it gave up, which no probe has taken since: a lookup that takes
    /// longer than a call has is started once, and the next call connects with what it found, as psql would once the
    /// lookup ends, rather than start it again on each call, and leave each running.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <returns>The lookup.</returns>
    internal static Task<IPAddress[]> Start(string name)
    {
        lock (s_lookups)
        {
            if (s_lookups.TryGetValue(name, out (Task<IPAddress[]> Lookup, StrongBox<bool> Taken) kept) && (!kept.Lookup.IsCompleted || !kept.Taken.Value))
            {
                return kept.Lookup;
            }

            Task<IPAddress[]> lookup = Task.Run(() => Find(name));
            s_lookups[name] = (lookup, new StrongBox<bool>(false));
            return lookup;
        }
    }

    /// <summary>
    /// Notes that a probe took a lookup's outcome, so that the name's next probe looks it up again.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="lookup">The lookup.</param>
    internal static void Took(string name, Task<IPAddress[]> lookup)
    {
        lock (s_lookups)
        {
            if (s_lookups.TryGetValue(name, out (Task<IPAddress[]> Lookup, StrongBox<bool> Taken) kept) && kept.Lookup == lookup)
            {
                kept.Taken.Value = true;
            }
        }
    }

    /// <summary>
    /// Looks a host name up.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <returns>Its addresses, in the order getaddrinfo gives them.</returns>
    /// <exception cref="IOException">The lookup failed, with getaddrinfo's own message.</exception>
    internal static IPAddress[] Find(string name)
    {
        byte* hints = stackalloc byte[64];
        new Span<byte>(hints, 64).Clear();
        *(int*)(hints + 8) = SockStream;
        void* found = null;
        int error;
        if (OperatingSystem.IsWindows())
        {
            fixed (char* text = name)
            {
                error = ((delegate* unmanaged<char*, char*, byte*, void**, int>)WinSock("GetAddrInfoW"))(text, null, hints, &found);
            }
        }
        else
        {
            byte[] node = [.. Encoding.UTF8.GetBytes(name), 0];
            fixed (byte* text = node)
            {
                error = ((delegate* unmanaged<byte*, byte*, byte*, void**, int>)Symbol("getaddrinfo"))(text, null, hints, &found);
            }
        }

        if (error != 0)
        {
            throw new IOException(Message(error));
        }

        try
        {
            List<IPAddress> addresses = [];
            for (byte* entry = (byte*)found; entry is not null; entry = *(byte**)(entry + 40))
            {
                byte* address = *(byte**)(entry + (OperatingSystem.IsLinux() ? 24 : 32));
                if (address is null)
                {
                    continue;
                }

                int family = OperatingSystem.IsMacOS() ? address[1] : *(ushort*)address;
                if (family == AddressFamilyInet)
                {
                    addresses.Add(new IPAddress(new ReadOnlySpan<byte>(address + 4, 4)));
                }
                else if (family == InetSix())
                {
                    addresses.Add(new IPAddress(new ReadOnlySpan<byte>(address + 8, 16), *(uint*)(address + 24)));
                }
            }

            return [.. addresses];
        }
        finally
        {
            if (OperatingSystem.IsWindows())
            {
                ((delegate* unmanaged<void*, void>)WinSock("FreeAddrInfoW"))(found);
            }
            else
            {
                ((delegate* unmanaged<void*, void>)Symbol("freeaddrinfo"))(found);
            }
        }
    }

    // AF_INET6, which each platform numbers its own way.
    private static int InetSix() => OperatingSystem.IsWindows() ? 23 : OperatingSystem.IsMacOS() ? 30 : 10;

    // getaddrinfo's message for an error, as libpq gives it; Windows's lookups set an error that Win32 words.
    private static string Message(int error)
    {
        if (OperatingSystem.IsWindows())
        {
            return new SocketException(error).Message;
        }

        sbyte* text = ((delegate* unmanaged<int, sbyte*>)Symbol("gai_strerror"))(error);
        return text is null ? $"error {error}" : new string(text);
    }

    // A function the server's own symbols resolve to.
    private static nint Symbol(string name) => NativeLibrary.GetExport(NativeLibrary.GetMainProgramHandle(), name);

    // A function of Windows's sockets library, which the server has loaded.
    private static nint WinSock(string name) => NativeLibrary.GetExport(NativeLibrary.Load("ws2_32.dll"), name);
}
