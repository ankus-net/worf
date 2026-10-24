// Runs the regression suite repeatedly to catch flaky tests, as pglogical's scripts/stress-tests.sh does. Each run uses
// eng/regress.cs, so it starts from a fresh cluster. Failed runs keep their diffs, results and server log under
// artifacts/stress/run-N.
// Usage: dotnet run eng/stress.cs -- --pg <major> [-n|--runs <count>] [-s|--stop-on-fail] [--huge-tx]

using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

string? major = null;
int runs = 10;
bool stopOnFailure = false;
bool hugeTx = false;
for (int index = 0; index < args.Length; index++)
{
    switch (args[index])
    {
        case "--pg":
            major = args[++index];
            break;
        case "-n":
        case "--runs":
            runs = int.Parse(args[++index], CultureInfo.InvariantCulture);
            break;
        case "-s":
        case "--stop-on-fail":
            stopOnFailure = true;
            break;
        case "--huge-tx":
            hugeTx = true;
            break;
        case "-h":
        case "--help":
            Console.WriteLine("usage: dotnet run eng/stress.cs -- --pg <major> [-n|--runs <count>] [-s|--stop-on-fail] [--huge-tx]");
            return 0;
        default:
            Console.Error.WriteLine($"Unknown option: {args[index]}");
            return 1;
    }
}

if (major is null)
{
    Console.Error.WriteLine("usage: dotnet run eng/stress.cs -- --pg <major> [-n|--runs <count>] [-s|--stop-on-fail] [--huge-tx]");
    return 2;
}

string root = Path.GetFullPath(Path.Join(AppContext.GetData("EntryPointFileDirectoryPath") as string ?? ".", ".."));
string suite = Path.Join(root, "src", "Worf", "pg_regress");
string resultsDir = Path.Join(root, "artifacts", "stress");
Directory.CreateDirectory(resultsDir);

Regex passedLine = PassedLine();
Regex failedLine = FailedLine();

SortedDictionary<string, List<int>> failedTests = new(StringComparer.Ordinal);
int passedRuns = 0;
int failedRuns = 0;
Stopwatch total = Stopwatch.StartNew();

Console.WriteLine("========================================");
Console.WriteLine("worf Stress Test Runner");
Console.WriteLine("========================================");
Console.WriteLine($"Runs planned: {runs}");
Console.WriteLine($"Stop on failure: {stopOnFailure}");
Console.WriteLine($"Results dir: {resultsDir}");
Console.WriteLine($"Started at: {DateTime.Now}");
Console.WriteLine("========================================");
Console.WriteLine();

for (int run = 1; run <= runs; run++)
{
    Stopwatch runTime = Stopwatch.StartNew();
    Console.WriteLine($"Run {run} of {runs} starting at {DateTime.Now}...");

    // Clean previous results
    File.Delete(Path.Join(suite, "regression.diffs"));
    File.Delete(Path.Join(suite, "regression.out"));

    List<string> regressArguments = ["run", Path.Join(root, "eng", "regress.cs"), "--", "--pg", major];
    if (hugeTx)
    {
        regressArguments.Add("--huge-tx");
    }

    (int exitCode, string output) = Run("dotnet", regressArguments);

    // Parse results
    List<string> passed = [];
    List<string> failed = [];
    foreach (string line in output.Split('\n'))
    {
        Match match = failedLine.Match(line);
        if (match.Success)
        {
            string name = match.Groups[1].Success ? match.Groups[1].Value : match.Groups[2].Value;
            failed.Add(name);

            // Track which runs each test failed in
            if (!failedTests.TryGetValue(name, out List<int>? failedRunsOfTest))
            {
                failedTests[name] = failedRunsOfTest = [];
            }

            failedRunsOfTest.Add(run);
            continue;
        }

        match = passedLine.Match(line);
        if (match.Success)
        {
            passed.Add(match.Groups[1].Success ? match.Groups[1].Value : match.Groups[2].Value);
        }
    }

    // A run that fails without failing a test, such as a build or startup failure, still fails.
    if (failed.Count > 0 || exitCode != 0)
    {
        // Save artifacts for failed runs
        string runDir = Path.Join(resultsDir, $"run-{run}");
        Directory.CreateDirectory(runDir);
        CopyIfExists(Path.Join(suite, "regression.diffs"), Path.Join(runDir, "regression.diffs"));
        CopyIfExists(Path.Join(suite, "regression.out"), Path.Join(runDir, "regression.out"));
        CopyDirectoryIfExists(Path.Join(suite, "results"), Path.Join(runDir, "results"));
        CopyDirectoryIfExists(Path.Join(root, "artifacts", "regress-home", "clusters"), Path.Join(runDir, "log"));
        File.WriteAllText(Path.Join(runDir, "output.txt"), output);

        failedRuns++;
        Console.WriteLine($"  Run {run} FAILED ({failed.Count} failures: {string.Join(' ', failed)})");
        Console.WriteLine($"  Artifacts saved to: {runDir}");

        if (stopOnFailure)
        {
            Console.WriteLine();
            Console.WriteLine("Stopping due to --stop-on-fail flag.");
            break;
        }
    }
    else
    {
        passedRuns++;
        Console.WriteLine($"  Run {run} PASSED ({passed.Count} tests in {(int)runTime.Elapsed.TotalSeconds}s)");
    }
}

// Summary
int totalRuns = passedRuns + failedRuns;
Console.WriteLine();
Console.WriteLine("========================================");
Console.WriteLine("SUMMARY");
Console.WriteLine("========================================");
Console.WriteLine($"Total runs: {totalRuns}");
Console.WriteLine($"Total time: {(int)total.Elapsed.TotalMinutes} minutes");
Console.WriteLine();
Console.WriteLine($"Passed runs: {passedRuns}");
Console.WriteLine($"Failed runs: {failedRuns}");
Console.WriteLine();

if (failedTests.Count > 0)
{
    Console.WriteLine("Flaky tests detected:");
    foreach ((string test, List<int> testRuns) in failedTests)
    {
        double failRate = testRuns.Count * 100.0 / totalRuns;
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"  {test} - failed in runs: {string.Join(", ", testRuns)} ({failRate:0.0}% failure rate)"));
    }
}
else
{
    Console.WriteLine($"No flaky tests detected across {totalRuns} runs!");
}

Console.WriteLine();
Console.WriteLine($"Results saved to: {resultsDir}");

// Return exit code based on whether any tests failed
return failedRuns > 0 ? 1 : 0;

static (int ExitCode, string Output) Run(string fileName, IEnumerable<string> arguments)
{
    ProcessStartInfo start = new(fileName)
    {
        UseShellExecute = false,
        RedirectStandardOutput = true,
        RedirectStandardError = true,
    };
    foreach (string argument in arguments)
    {
        start.ArgumentList.Add(argument);
    }

    StringBuilder output = new();
    using Process process = Process.Start(start)!;
    process.OutputDataReceived += (_, e) =>
    {
        if (e.Data is not null)
        {
            lock (output)
            {
                output.AppendLine(e.Data);
            }
        }
    };
    process.ErrorDataReceived += (_, e) =>
    {
        if (e.Data is not null)
        {
            lock (output)
            {
                output.AppendLine(e.Data);
            }
        }
    };
    process.BeginOutputReadLine();
    process.BeginErrorReadLine();
    process.WaitForExit();
    return (process.ExitCode, output.ToString());
}

static void CopyIfExists(string from, string to)
{
    if (File.Exists(from))
    {
        File.Copy(from, to, overwrite: true);
    }
}

static void CopyDirectoryIfExists(string from, string to)
{
    if (!Directory.Exists(from))
    {
        return;
    }

    Directory.CreateDirectory(to);
    foreach (string file in Directory.GetFiles(from))
    {
        File.Copy(file, Path.Join(to, Path.GetFileName(file)), overwrite: true);
    }
}

/// <summary>
/// The stress test runner.
/// </summary>
internal static partial class Program
{
    // pg_regress prints "ok 1 - name" and "not ok 1 - name" from PostgreSQL 16, and "test name ... ok" or
    // "... FAILED" before.
    [GeneratedRegex(@"^\s*ok\s+\d+\s+[-+]\s+(\S+)|^test\s+(\S+)\s+\.\.\.\s+ok\b", RegexOptions.CultureInvariant)]
    private static partial Regex PassedLine();

    [GeneratedRegex(@"^\s*not\s+ok\s+\d+\s+[-+]\s+(\S+)|^test\s+(\S+)\s+\.\.\.\s+FAILED\b", RegexOptions.CultureInvariant)]
    private static partial Regex FailedLine();
}
