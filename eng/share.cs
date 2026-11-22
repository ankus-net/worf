// Prints the MSTest filter for the share of the multi-server tests one CI job runs. A platform's jobs, one for each
// PostgreSQL major version, split the tests between them, so that each job takes minutes rather than running all of them:
// the tests go into one share for each version, balanced by eng/test-times.txt's durations, with tests that run alone
// counted for the workers they leave idle, and each job takes the share that the version, the platform and the workflow
// run's number pick, so that every test runs on each platform in every run and on every version over consecutive runs.
// Tests in the EveryVersion category, which check behavior that differs by version, run in every job.
// Usage: dotnet run eng/share.cs -- --pg <major> --platform <rid> --run <number>

using System.Globalization;
using System.Text.RegularExpressions;

int? major = null;
string? platform = null;
int? run = null;
for (int index = 0; index < args.Length; index++)
{
    switch (args[index])
    {
        case "--pg":
            major = int.Parse(args[++index], CultureInfo.InvariantCulture);
            break;
        case "--platform":
            platform = args[++index];
            break;
        case "--run":
            run = int.Parse(args[++index], CultureInfo.InvariantCulture);
            break;
        default:
            Console.Error.WriteLine($"Unknown option: {args[index]}");
            return 1;
    }
}

if (major is not int pg || platform is null || run is not int number)
{
    Console.Error.WriteLine("usage: dotnet run eng/share.cs -- --pg <major> --platform <rid> --run <number>");
    return 1;
}

const int Shares = 5;
const int Workers = 10;
string root = Path.GetFullPath(Path.Join(AppContext.GetData("EntryPointFileDirectoryPath") as string ?? ".", ".."));

// Each test method, with whether it runs alone and whether it runs on every version.
List<(string Name, bool Alone, bool Everywhere)> tests = [];
Regex signature = Signature();
foreach (string file in Directory.EnumerateFiles(Path.Join(root, "tests", "Worf.Tests"), "*.cs").Order(StringComparer.Ordinal))
{
    string source = File.ReadAllText(file);
    Match type = TypeName().Match(source);
    if (!type.Success)
    {
        continue;
    }

    // A test method's attributes come between its documentation comment and its signature.
    foreach (Match method in signature.Matches(source))
    {
        int documented = source.LastIndexOf("///", method.Index, StringComparison.Ordinal);
        string attributes = documented < 0 ? "" : source[documented..method.Index];
        if (attributes.Contains("[TestMethod]", StringComparison.Ordinal))
        {
            tests.Add(($"Worf.Tests.{type.Groups[1].Value}.{method.Groups[1].Value}", attributes.Contains("[DoNotParallelize]", StringComparison.Ordinal),
                attributes.Contains("TestCategory(\"EveryVersion\")", StringComparison.Ordinal)));
        }
    }
}

// Durations in seconds by class and method, as a macOS job last took them; a test without one counts as the median.
Dictionary<string, double> times = [];
foreach (string line in File.ReadAllLines(Path.Join(root, "eng", "test-times.txt")))
{
    string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
    if (parts.Length == 2)
    {
        times[$"Worf.Tests.{parts[0]}"] = double.Parse(parts[1], CultureInfo.InvariantCulture);
    }
}

double median = times.Count == 0 ? 1 : times.Values.Order().ElementAt(times.Count / 2);
double Weight((string Name, bool Alone, bool Everywhere) test) => times.GetValueOrDefault(test.Name, median) * (test.Alone ? Workers : 1);

// The longest first, each into the share with the least so far.
double[] load = new double[Shares];
List<string>[] shares = [.. Enumerable.Range(0, Shares).Select(_ => new List<string>())];
foreach ((string Name, bool Alone, bool Everywhere) test in tests.Where(test => !test.Everywhere).OrderByDescending(Weight).ThenBy(test => test.Name, StringComparer.Ordinal))
{
    int least = Array.IndexOf(load, load.Min());
    load[least] += Weight(test);
    shares[least].Add(test.Name);
}

int offset = platform switch
{
    "linux-x64" => 0,
    "osx-arm64" => 2,
    _ => 4,
};
int share = (pg + number + offset) % Shares;
Console.WriteLine(string.Join('|', shares[share].Order(StringComparer.Ordinal).Select(name => $"FullyQualifiedName={name}").Prepend("TestCategory=EveryVersion")));
return 0;

internal static partial class Program
{
    // A public test method's signature, with its name.
    [GeneratedRegex(@"public (?:async )?(?:Task|void) (\w+)\(", RegexOptions.CultureInvariant)]
    private static partial Regex Signature();

    // A test class's declaration, with its name.
    [GeneratedRegex(@"\bclass (\w+)", RegexOptions.CultureInvariant)]
    private static partial Regex TypeName();
}
