namespace Worf.Tests;

/// <summary>
/// A directory under the temporary directory, or another, that is deleted, with whatever a test left in it, once the
/// test is done.
/// </summary>
/// <param name="name">The directory's name.</param>
/// <param name="parent">The directory to make it in, or null for the temporary directory.</param>
internal sealed class TemporaryDirectory(string name, string? parent = null) : IDisposable
{
    /// <summary>
    /// Gets the directory's path.
    /// </summary>
    public string FullName { get; } = Directory.CreateDirectory(Path.Join(string.IsNullOrEmpty(parent) ? Path.GetTempPath() : parent, name)).FullName;

    /// <summary>
    /// Deletes the directory. Links in it, such as the junctions a Windows test makes to an installation's lib and
    /// share, are removed without deleting what they point to.
    /// </summary>
    public void Dispose()
    {
        try
        {
            Directory.Delete(FullName, recursive: true);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            // A file a process still holds is left for the system's own cleanup, rather than replacing the test's
            // own failure.
        }
    }
}
