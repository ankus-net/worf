#if ANKUS_PG15 || ANKUS_PG16
using System.Runtime.InteropServices;

namespace Worf;

/// <summary>
/// libpqwalreceiver's WalReceiverConn, as it has kept it since PostgreSQL 10, for an ordinary connection that
/// <see cref="WorfConnection"/> opens before PostgreSQL 17: the libpq connection, whether it's logical, and the buffer of
/// the copy message being read.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct OrdinaryWalReceiverConn
{
    /// <summary>
    /// The libpq connection.
    /// </summary>
    public void* StreamConn;

    /// <summary>
    /// Whether the connection is logical.
    /// </summary>
    public bool Logical;

    /// <summary>
    /// The buffer of the copy message being read, which libpq allocates.
    /// </summary>
    public sbyte* RecvBuf;
}
#endif
