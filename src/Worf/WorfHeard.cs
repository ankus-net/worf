namespace Worf;

/// <summary>
/// When this server last heard from another node of a database that manages sequence ranges.
/// </summary>
internal struct WorfHeard
{
    /// <summary>
    /// The database, or zero for an unused entry.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// The node.
    /// </summary>
    public uint NodeId;

    /// <summary>
    /// The neighbor whose relayed report claimed the entry, while the node hasn't exchanged with this server.
    /// </summary>
    public uint Via;

    /// <summary>
    /// When the node last exchanged with this server, or zero.
    /// </summary>
    public long DirectAt;

    /// <summary>
    /// The newest time that a report the node made carries, received directly or relayed, or zero.
    /// </summary>
    public long ReportedAt;

    /// <summary>
    /// The system identifier the node's WAL positions below belong to.
    /// </summary>
    public ulong Sysid;

    /// <summary>
    /// The timeline the node's WAL positions below belong to.
    /// </summary>
    public long Timeline;

    /// <summary>
    /// The WAL position the node's last call to this server carried, or zero.
    /// </summary>
    public ulong CallLsn;

    /// <summary>
    /// The start, as its messages give it, of the node's server when its last call to this server carried
    /// <see cref="CallLsn"/>, or zero.
    /// </summary>
    public long CallStart;

    /// <summary>
    /// The WAL position the node's last answer to this server carried, or zero.
    /// </summary>
    public ulong ReplyLsn;

    /// <summary>
    /// The start of the node's server when its last answer to this server carried <see cref="ReplyLsn"/>, or zero.
    /// </summary>
    public long ReplyStart;

    /// <summary>
    /// When the node, as a neighbor, last had this server reset a sequence's blocks, or zero.
    /// </summary>
    public long ResetAt;

    /// <summary>
    /// How long, in microseconds, after <see cref="ResetAt"/> the node may next have this server reset, as
    /// <see cref="SequenceRangeHeard.ResetWindow"/> works it out.
    /// </summary>
    public long ResetEvery;

    /// <summary>
    /// When the node, as this server's grantor, last had it reset a sequence's blocks with a fence's lift that named
    /// blocks it granted, or zero.
    /// </summary>
    public long LiftResetAt;

    /// <summary>
    /// How long, in microseconds, after <see cref="LiftResetAt"/> the node may next have this server reset so.
    /// </summary>
    public long LiftResetEvery;

    /// <summary>
    /// When a statement last waited for this server to hear from the node, its grantor, since it started, or zero.
    /// </summary>
    public long WantedAt;

    /// <summary>
    /// When the node, as a provider, last asked this server to call it, as an owner that granted itself a block does.
    /// </summary>
    public long CallForAt;

    /// <summary>
    /// When the node's last message that this server took in whole, leaving no sequence out, committed.
    /// </summary>
    public long CompleteAt;

    /// <summary>
    /// Where, among the sequences of the node's calls in the order of their names, the last call this server cut short
    /// stopped, which the next call goes on from, or zero.
    /// </summary>
    public int ResumeAt;

    /// <summary>
    /// When the minute began in which this server's processes logged the notices the node sent them, or zero.
    /// </summary>
    public long NoticeMinute;

    /// <summary>
    /// How many notices the node sent this server's processes in that minute.
    /// </summary>
    public int Notices;
}
