// Packages worf for one PostgreSQL major version on this machine: the extension from ankus package and
// worf_create_subscriber, laid out as bin, lib and share/extension beside the licenses, in a tar.gz on Linux and
// macOS or a zip and an MSI on Windows. First the package is installed into a copy of the PostgreSQL installation it
// was built for, whose server must load the extension and report the package's version.
// Usage: dotnet run eng/package.cs -- --pg <major> --version <version> [--output <directory>]

using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
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

string[] previous = [tree, work];
foreach (string directory in previous.Where(Directory.Exists))
{
    Directory.Delete(directory, recursive: true);
}

Directory.CreateDirectory(Path.Join(tree, "bin"));
Directory.CreateDirectory(Path.Join(tree, "lib"));
Directory.CreateDirectory(Path.Join(tree, "share", "extension"));
string[] notices = ["LICENSE", "THIRD-PARTY-NOTICES.txt"];
foreach (string notice in notices)
{
    File.Copy(Path.Join(root, notice), Path.Join(tree, notice));
}

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

// A smoke test of the package in a copy of the installation, which the copy's programs find their files beside.
string installation = Capture("dotnet", ["ankus", "info", "path", major]).Trim();
string pgConfig = Capture("dotnet", ["ankus", "info", "pg-config", major]).Trim();
string postgresql = Path.Join(work, "postgresql");
string[] parts = ["bin", "lib", "share"];
foreach (string part in parts)
{
    CopyDirectory(Path.Join(installation, part), Path.Join(postgresql, part));
}

string copiedPgConfig = Path.Join(postgresql, Path.GetRelativePath(installation, pgConfig));
string libraries = Capture(copiedPgConfig, ["--pkglibdir"]).Trim();
if (libraries == Capture(pgConfig, ["--pkglibdir"]).Trim())
{
    throw new InvalidOperationException($"The PostgreSQL installation in {installation} can't be copied to test the package.");
}

// worf files an earlier installation left in the copy would stand in for any the package lacks.
string extensions = Path.Join(Capture(copiedPgConfig, ["--sharedir"]).Trim(), "extension");
foreach (string stale in Directory.GetFiles(libraries, "worf*").Concat(Directory.GetFiles(extensions, "worf*")))
{
    File.Delete(stale);
}

CopyDirectory(Path.Join(tree, "lib"), libraries);
CopyDirectory(Path.Join(tree, "share", "extension"), extensions);

string bin = Path.GetDirectoryName(copiedPgConfig)!;
string suffix = OperatingSystem.IsWindows() ? ".exe" : "";
string data = Path.Join(work, "data");
string log = Path.Join(work, "postgresql.log");
Run(Path.Join(bin, "initdb" + suffix), ["-D", data, "-U", "postgres", "-A", "trust", "--no-sync"]);
string port = FreePort().ToString(CultureInfo.InvariantCulture);
File.AppendAllLines(Path.Join(data, "postgresql.conf"),
[
    $"port = {port}",
    "listen_addresses = 'localhost'",
    "unix_socket_directories = ''",
    "shared_preload_libraries = 'worf'",
]);
try
{
    Run(Path.Join(bin, "pg_ctl" + suffix), ["start", "-D", data, "-l", log, "-w"]);
    string expected = version.Split('-')[0];
    string[] reported = Capture(Path.Join(bin, "psql" + suffix),
    [
        "-X", "-A", "-t", "-q", "-w", "-v", "ON_ERROR_STOP=1", "-h", "localhost", "-p", port, "-U", "postgres", "-d", "postgres",
        "-c", "CREATE EXTENSION worf", "-c", "SELECT extversion, worf.worf_version() FROM pg_extension WHERE extname = 'worf'",
    ]).Trim().Split('|');
    if (reported.Any(reportedVersion => reportedVersion != expected))
    {
        throw new InvalidOperationException(
            $"The package is version {version}, but its extension is version {reported[0]} and worf_version() returns {reported[^1]}.");
    }

    Console.WriteLine($"The packaged extension loads into PostgreSQL {major} and reports version {expected}.");
}
catch
{
    if (File.Exists(log))
    {
        Console.WriteLine($"Server log {log}:");
        Console.WriteLine(File.ReadAllText(log));
    }

    throw;
}
finally
{
    if (File.Exists(Path.Join(data, "postmaster.pid")))
    {
        Run(Path.Join(bin, "pg_ctl" + suffix), ["stop", "-D", data, "-m", "fast", "-w"], check: false);
    }
}

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

static void Run(string fileName, IEnumerable<string> arguments, string? workingDirectory = null, bool check = true)
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
    if (check && process.ExitCode != 0)
    {
        throw new InvalidOperationException($"{fileName} exited with code {process.ExitCode}.");
    }
}

static string Capture(string fileName, IEnumerable<string> arguments)
{
    ProcessStartInfo start = new(fileName) { UseShellExecute = false, RedirectStandardOutput = true };
    foreach (string argument in arguments)
    {
        start.ArgumentList.Add(argument);
    }

    using Process process = Process.Start(start)!;
    string output = process.StandardOutput.ReadToEnd();
    process.WaitForExit();
    if (process.ExitCode != 0)
    {
        throw new InvalidOperationException($"{fileName} exited with code {process.ExitCode}.");
    }

    return output;
}

static void CopyDirectory(string source, string destination)
{
    Directory.CreateDirectory(destination);
    foreach (string file in Directory.EnumerateFiles(source, "*", SearchOption.AllDirectories))
    {
        string target = Path.Join(destination, Path.GetRelativePath(source, file));
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
        File.Copy(file, target, overwrite: true);
    }
}

static int FreePort()
{
    using TcpListener listener = new(IPAddress.Loopback, 0);
    listener.Start();
    return ((IPEndPoint)listener.LocalEndpoint).Port;
}
