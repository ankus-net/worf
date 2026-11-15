namespace Worf;

/// <summary>
/// Handles a message of a copy read ahead of its turn, if it is one to handle at once.
/// </summary>
/// <param name="message">The message.</param>
/// <returns>Whether it handled the message, which is then not given in its turn.</returns>
internal delegate bool ReadAheadHandler(ReadOnlySpan<byte> message);
