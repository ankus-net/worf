namespace Worf.Tests;

/// <summary>
/// Locates files in the repository the tests run from.
/// </summary>
internal static class RepositoryPaths
{
    /// <summary>
    /// Gets the repository's root directory, which holds <c>Worf.slnx</c>.
    /// </summary>
    public static string Root { get; } = FindRoot();

    /// <summary>
    /// Gets the extension's project file.
    /// </summary>
    public static string ExtensionProject => Path.Join(Root, "src", "Worf", "Worf.csproj");

    private static string FindRoot()
    {
        for (DirectoryInfo? directory = new(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Join(directory.FullName, "Worf.slnx")))
            {
                return directory.FullName;
            }
        }

        throw new InvalidOperationException("Worf.slnx was not found above the test assembly.");
    }
}
