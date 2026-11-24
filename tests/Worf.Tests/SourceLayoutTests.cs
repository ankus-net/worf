namespace Worf.Tests;

/// <summary>
/// Checks the layout of the repository's C# sources.
/// </summary>
[TestClass]
public sealed class SourceLayoutTests
{
    /// <summary>
    /// No line after an opening brace is blank, and no two blank lines follow each other.
    /// </summary>
    [TestMethod]
    public void SourcesHaveNoStrayBlankLines()
    {
        List<string> findings = [];
        foreach (string file in SourceFiles())
        {
            string[] lines = File.ReadAllLines(file);
            for (int index = 1; index < lines.Length; index++)
            {
                if (lines[index].Trim().Length != 0)
                {
                    continue;
                }

                string previous = lines[index - 1].Trim();
                if (previous == "{")
                {
                    findings.Add($"{Path.GetRelativePath(RepositoryPaths.Root, file)}:{index + 1}: blank line after an opening brace");
                }
                else if (previous.Length == 0)
                {
                    findings.Add($"{Path.GetRelativePath(RepositoryPaths.Root, file)}:{index + 1}: second blank line");
                }
            }
        }

        Assert.IsEmpty(findings, string.Join(Environment.NewLine, findings));
    }

    private static IEnumerable<string> SourceFiles()
    {
        foreach (string directory in new[] { "src", "tests", "eng" })
        {
            foreach (string file in Directory.EnumerateFiles(Path.Join(RepositoryPaths.Root, directory), "*.cs", SearchOption.AllDirectories))
            {
                string relative = Path.GetRelativePath(RepositoryPaths.Root, file);
                string[] parts = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                if (!parts.Contains("bin") && !parts.Contains("obj"))
                {
                    yield return file;
                }
            }
        }
    }
}
