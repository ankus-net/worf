using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Worf.CreateSubscriber;

/// <summary>
/// Keeps this process's standard handles from the processes it starts for a while, on Windows, where a process
/// started with handles of its own still inherits every handle of its parent's that may be inherited, and passes them on
/// to the processes it starts: as <c>pg_ctl</c> does to the <c>cmd.exe</c> that stays the server's parent, which would
/// keep this process's output open as long as the server runs.
/// </summary>
[SupportedOSPlatform("windows")]
internal sealed partial class InheritedHandles : IDisposable
{
    // STD_INPUT_HANDLE, STD_OUTPUT_HANDLE and STD_ERROR_HANDLE, and HANDLE_FLAG_INHERIT.
    private static readonly int[] s_standard = [-10, -11, -12];
    private const uint Inherit = 1;

    private readonly List<nint> _withheld = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="InheritedHandles"/> class, which withholds this process's standard
    /// handles that may be inherited until it's disposed.
    /// </summary>
    internal InheritedHandles()
    {
        foreach (nint handle in s_standard.Select(GetStdHandle))
        {
            if (handle is 0 or -1 || !GetHandleInformation(handle, out uint flags) || (flags & Inherit) == 0)
            {
                continue;
            }

            if (SetHandleInformation(handle, Inherit, 0))
            {
                _withheld.Add(handle);
            }
        }
    }

    /// <summary>
    /// Lets the processes started after this inherit the handles again.
    /// </summary>
    public void Dispose()
    {
        foreach (nint handle in _withheld)
        {
            _ = SetHandleInformation(handle, Inherit, Inherit);
        }

        _withheld.Clear();
    }

    [LibraryImport("kernel32.dll", SetLastError = true)]
    private static partial nint GetStdHandle(int nStdHandle);

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool GetHandleInformation(nint hObject, out uint lpdwFlags);

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool SetHandleInformation(nint hObject, uint dwMask, uint dwFlags);
}
