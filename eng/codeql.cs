// Fails when CodeQL found anything: code scanning lists findings without failing a check, so a new one would go unseen.
// Lists each result of the SARIF files under a directory that no `// codeql[rule] reason` comment suppresses.
// Usage: dotnet run eng/codeql.cs -- <sarif-directory>

using System.Text.Json;

if (args.Length != 1 || !Directory.Exists(args[0]))
{
    Console.Error.WriteLine("usage: dotnet run eng/codeql.cs -- <sarif-directory>");
    return 2;
}

int findings = 0;
foreach (string file in Directory.EnumerateFiles(args[0], "*.sarif", SearchOption.AllDirectories))
{
    using JsonDocument sarif = JsonDocument.Parse(File.ReadAllText(file));
    foreach (JsonElement run in sarif.RootElement.GetProperty("runs").EnumerateArray())
    {
        if (!run.TryGetProperty("results", out JsonElement results))
        {
            continue;
        }

        foreach (JsonElement result in results.EnumerateArray())
        {
            if (result.TryGetProperty("suppressions", out JsonElement suppressions) && suppressions.GetArrayLength() > 0)
            {
                continue;
            }

            string rule = result.TryGetProperty("ruleId", out JsonElement id) ? id.GetString() ?? "" : "";
            string message = result.GetProperty("message").GetProperty("text").GetString() ?? "";
            string where = "";
            if (result.TryGetProperty("locations", out JsonElement locations) && locations.GetArrayLength() > 0)
            {
                JsonElement physical = locations[0].GetProperty("physicalLocation");
                string uri = physical.GetProperty("artifactLocation").GetProperty("uri").GetString() ?? "";
                int line = physical.TryGetProperty("region", out JsonElement region) && region.TryGetProperty("startLine", out JsonElement start)
                    ? start.GetInt32()
                    : 0;
                where = $"{uri}:{line}";
            }

            Console.WriteLine($"{where}: {rule}: {message}");
            findings++;
        }
    }
}

Console.WriteLine(findings == 0 ? "No CodeQL findings." : $"{findings} CodeQL finding(s).");
return findings == 0 ? 0 : 1;
