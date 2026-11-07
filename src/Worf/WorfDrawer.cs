namespace Worf;

/// <summary>
/// The number of statements that hold a managed sequence's switch gate, which may each draw a value before they check
/// the sequence again, the number that wait for it to switch, the number of switches that wait for its gate, and how
/// often a worker has tried to switch it.
/// </summary>
internal struct WorfDrawer
{
    /// <summary>
    /// The sequence's database, or zero for an entry never used. An entry whose counts are all zero is free for another
    /// sequence.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// The sequence.
    /// </summary>
    public uint Seqoid;

    /// <summary>
    /// The number of statements that hold the gate.
    /// </summary>
    public int Statements;

    /// <summary>
    /// The number of statements that wait for the switch from the active block that <see cref="WaitingFor"/> starts.
    /// </summary>
    public int Waiting;

    /// <summary>
    /// The start of the active block the waiting statements wait to leave, so that those that still wait for a switch
    /// that has come, until they next look, don't count for the block after it.
    /// </summary>
    public long WaitingFor;

    /// <summary>
    /// The number of switches that wait for, or hold, the gate.
    /// </summary>
    public int Switching;

    /// <summary>
    /// The number of times the range worker has checked the sequence, or a switch worker tried to switch it, which a
    /// statement waiting for the switch counts to tell a worker that hasn't come to the sequence yet from one that can't
    /// switch it.
    /// </summary>
    public long Attempts;
}
