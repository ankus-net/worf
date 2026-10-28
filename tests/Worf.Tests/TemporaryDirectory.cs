namespace Worf.Tests;

/// <summary>
/// A directory under the temporary directory that is deleted, with whatever a test left in it, once the test is done.
/// </summary>
/// <param name="name">The directory's name.</param>
internal sealed class TemporaryDirectory(string name) : IDisposable
{
    /// <summary>
    /// Gets the directory's path.
    /// </summary>
    public string FullName { get; } = Directory.CreateDirectory(Path.Join(Path.GetTempPath(), name)).FullName;

    /// <summary>
    /// Deletes the directory. Links in it, such as the junctions a Windows test makes to an installation's lib and
    /// share, are removed without deleting what they point to. A file a process still holds is left for the system's
    /// own cleanup.
    /// </summary>
    public void Dispose()
    {
        try
        {
            Directory.Delete(FullName, recursive: true);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
        }
    }
}
