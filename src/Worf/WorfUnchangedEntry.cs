namespace Worf;

/// <summary>
/// A neighbor's report of one sequence that a check found would change nothing here, with what the check depended on, so
/// that the same report, met again while all of that is as it was, needs no check.
/// </summary>
/// <param name="Entry">The report, as the message gave it.</param>
/// <param name="Key">What of the rest of the message, and of this server, the check depended on.</param>
/// <param name="Seqoid">The sequence.</param>
/// <param name="Fingerprint">What the check read, as <see cref="WorfEntryFingerprint"/> gives it.</param>
/// <param name="Recorded">The WAL position the tables recorded of the neighbor.</param>
/// <param name="Relayed">The nodes whose reports the neighbor relayed.</param>
internal sealed record WorfUnchangedEntry(string Entry, string Key, uint Seqoid, string Fingerprint, string? Recorded, uint[] Relayed);
