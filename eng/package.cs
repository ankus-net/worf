// Packages worf for one PostgreSQL major version on this machine: the extension from ankus package and
// worf_create_subscriber, laid out as bin, lib and share/extension, in a tar.gz on Linux and macOS or a zip and an
// MSI on Windows. Usage: dotnet run eng/package.cs -- --pg <major> --version <version> [--output <directory>]

using System.Diagnostics;
using System.IO.Compression;
using System.Runtime.InteropServices;

string? major = null;
string? version = null;
string? output = null;
for (int index = 0; index < args.Length; index++)
{
    switch (args[index])
    {
        case "--pg":
            major = args[++index];
            break;
        case "--version":
            version = args[++index];
            break;
        case "--output":
            output = args[++index];
            break;
        default:
            Console.Error.WriteLine($"Unknown option: {args[index]}");
            return 2;
    }
}

if (major is null || version is null)
{
    Console.Error.WriteLine("usage: dotnet run eng/package.cs -- --pg <major> --version <version> [--output <directory>]");
    return 2;
}

string root = Path.GetFullPath(Path.Join(AppContext.GetData("EntryPointFileDirectoryPath") as string ?? ".", ".."));
output = Path.GetFullPath(output ?? Path.Join(root, "artifacts", "packages"));
string rid = RuntimeInformation.RuntimeIdentifier;
string platform = rid switch
{
    "linux-x64" => "linux-x64",
    "osx-arm64" => "macos-arm64",
    "osx-x64" => "macos-x64",
    "win-x64" => "windows-x64",
    _ => throw new PlatformNotSupportedException(rid),
};
string name = $"worf-{version}-pg{major}-{platform}";
string tree = Path.Join(output, name);
string work = Path.Join(root, "artifacts", "package-work", name);

foreach (string directory in (string[])[tree, work])
{
    if (Directory.Exists(directory))
    {
        Directory.Delete(directory, recursive: true);
    }
}

Directory.CreateDirectory(Path.Join(tree, "bin"));
Directory.CreateDirectory(Path.Join(tree, "lib"));
Directory.CreateDirectory(Path.Join(tree, "share", "extension"));

// The extension, in the installation's layout, moved to the package's.
string extension = Path.Join(work, "extension");
Run("dotnet", ["ankus", "package", "--pg", major, "--project", Path.Join(root, "src", "Worf", "Worf.csproj"), "--output", extension]);
foreach (string file in Directory.EnumerateFiles(extension, "*", SearchOption.AllDirectories))
{
    string fileName = Path.GetFileName(file);
    string destination = fileName.EndsWith(".control", StringComparison.Ordinal) || fileName.EndsWith(".sql", StringComparison.Ordinal)
        ? Path.Join(tree, "share", "extension", fileName)
        : Path.Join(tree, "lib", fileName);
    File.Copy(file, destination);
}

// worf_create_subscriber.
string command = OperatingSystem.IsWindows() ? "worf_create_subscriber.exe" : "worf_create_subscriber";
string published = Path.Join(work, "create-subscriber");
Run("dotnet", ["publish", Path.Join(root, "src", "Worf.CreateSubscriber", "Worf.CreateSubscriber.csproj"), "-c", "Release", "-r", rid, "-o", published]);
File.Copy(Path.Join(published, command), Path.Join(tree, "bin", command));
Run(Path.Join(tree, "bin", command), ["--help"]);

if (OperatingSystem.IsWindows())
{
    string zip = Path.Join(output, name + ".zip");
    File.Delete(zip);
    ZipFile.CreateFromDirectory(tree, zip, CompressionLevel.Optimal, includeBaseDirectory: true);
    Console.WriteLine($"Created {zip}");

    // The MSI's version is numeric, without a prerelease suffix.
    string msi = Path.Join(output, name + ".msi");
    Run("wix", ["build", "-o", msi, Path.Join(root, "packaging", "windows", "worf.wxs"), "-ext", "WixToolset.UI.wixext",
        "-d", $"VERSION={version}", "-d", $"MSI_VERSION={version.Split('-')[0]}", "-d", $"PG_VERSION={major}", "-d", $"PackageDir={tree}"],
        workingDirectory: root);
    Console.WriteLine($"Created {msi}");
}
else
{
    string tarball = Path.Join(output, name + ".tar.gz");
    File.Delete(tarball);
    Run("tar", ["-czf", tarball, "-C", output, name]);
    Console.WriteLine($"Created {tarball}");
}

return 0;

static void Run(string fileName, IEnumerable<string> arguments, string? workingDirectory = null)
{
    ProcessStartInfo start = new(fileName) { UseShellExecute = false };
    if (workingDirectory is not null)
    {
        start.WorkingDirectory = workingDirectory;
    }

    foreach (string argument in arguments)
    {
        start.ArgumentList.Add(argument);
    }

    Console.WriteLine("> " + fileName + " " + string.Join(' ', start.ArgumentList.Select(static argument => '"' + argument + '"')));
    using Process process = Process.Start(start)!;
    process.WaitForExit();
    if (process.ExitCode != 0)
    {
        throw new InvalidOperationException($"{fileName} exited with code {process.ExitCode}.");
    }
}
