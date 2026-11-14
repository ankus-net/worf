namespace Worf;

/// <summary>
/// What a neighbor's report of one sequence depends on here, as a check of it reads it: the sequence, the versions of
/// the rows the check reads, and which of its reports' times have run out, as text that differs once any of them does.
/// </summary>
/// <param name="Seqoid">The sequence the report's name names here.</param>
/// <param name="Fingerprint">The text.</param>
/// <param name="Recorded">The WAL position the tables recorded of the neighbor, as of a call or an answer.</param>
/// <param name="Watermark">This node's watermark of the sequence.</param>
/// <param name="Ascending">Whether the sequence ascends.</param>
/// <param name="Nodes">The nodes the tables have a report of for the sequence.</param>
/// <param name="Steady">Whether none of the times runs out for two seconds, nor depends on when it's read, and no prepared
/// transaction holds the range row.</param>
internal sealed record WorfEntryFingerprint(uint Seqoid, string Fingerprint, string? Recorded, long? Watermark, bool Ascending, uint[] Nodes, bool Steady);
