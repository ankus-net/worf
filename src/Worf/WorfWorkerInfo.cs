using System.Runtime.InteropServices;

namespace Worf;

/// <summary>
/// Type-specific worker info.
/// </summary>
[StructLayout(LayoutKind.Explicit)]
internal struct WorfWorkerInfo
{
    /// <summary>
    /// The apply worker's info.
    /// </summary>
    [FieldOffset(0)]
    public WorfApplyWorker Apply;

    /// <summary>
    /// The sync worker's info.
    /// </summary>
    [FieldOffset(0)]
    public WorfSyncWorker Sync;

    /// <summary>
    /// The exchange worker's info.
    /// </summary>
    [FieldOffset(0)]
    public WorfExchangeWorker Exchange;
}
