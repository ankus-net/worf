namespace Worf.CreateSubscriber;

/// <summary>
/// How much the command reports.
/// </summary>
internal enum VerbosityLevel
{
    /// <summary>
    /// Progress messages.
    /// </summary>
    Normal,

    /// <summary>
    /// Progress messages and details, with <c>-v</c>.
    /// </summary>
    Verbose,

    /// <summary>
    /// Everything, including the commands run, with <c>-v -v</c>.
    /// </summary>
    Debug,
}
