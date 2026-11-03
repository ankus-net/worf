namespace Worf;

/// <summary>
/// The number of statements that hold a managed sequence's switch gate, which may each draw a value before they check
/// the sequence again, the number that wait for it to switch, and the number of switches that wait for its gate.
/// </summary>
internal struct WorfDrawer
{
    /// <summary>
    /// The sequence's database, or zero for an unused entry.
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
    /// The number of statements that wait for the switch.
    /// </summary>
    public int Waiting;

    /// <summary>
    /// The number of switches that wait for, or hold, the gate.
    /// </summary>
    public int Switching;
}
