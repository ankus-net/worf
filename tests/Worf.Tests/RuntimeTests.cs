namespace Worf.Tests;

/// <summary>
/// Checks the .NET runtime that worf's published library embeds.
/// </summary>
[TestClass]
public sealed class RuntimeTests
{
    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// The library embeds a runtime that reads its lock settings and configuration without its UTF-8 encoder, as Ankus
    /// 0.2.30's runtime first does, so that a process whose first contended lock comes while the runtime starts doesn't
    /// abort without a message: the publication's library, or the debug symbols publishing keeps beside it, carries the
    /// method that runtime decodes a configuration setting with.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task LibraryEmbedsTheRuntimeThatStartsWithoutItsEncoder()
    {
        string publication = await TestNode.PublicationAsync(TestContext.CancellationToken);
        byte[] method = "AppContext__DecodeKnob"u8.ToArray();
        string[] files = [.. Directory.EnumerateFiles(publication, "*", SearchOption.AllDirectories)];
        string[] carrying = [.. files.Where(file => File.ReadAllBytes(file).AsSpan().IndexOf(method) >= 0)];
        Assert.IsNotEmpty(carrying, $"the runtime's AppContext.DecodeKnob in any of {string.Join(", ", files.Select(file => Path.GetRelativePath(publication, file)))}");
    }
}
