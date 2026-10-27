// Checks the SARIF files CodeQL writes under a directory.
// `drop-generated` removes the results under obj/, the output of the Ankus and regular expression source generators the
// traced C# build compiles, which worf does not own, and rewrites each file for the upload.
// Otherwise it fails when CodeQL found anything: code scanning lists findings without failing a check, so a new one
// would go unseen. It lists each result that no `// codeql[rule] reason` comment suppresses.
// Usage: dotnet run eng/codeql.cs -- [drop-generated] <sarif-directory>

using System.Text.Json.Nodes;

bool drop = args.Length == 2 && args[0] == "drop-generated";
if (args.Length != (drop ? 2 : 1) || !Directory.Exists(args[^1]))
{
    Console.Error.WriteLine("usage: dotnet run eng/codeql.cs -- [drop-generated] <sarif-directory>");
    return 2;
}

int findings = 0;
foreach (string file in Directory.EnumerateFiles(args[^1], "*.sarif", SearchOption.AllDirectories))
{
    JsonNode sarif = JsonNode.Parse(File.ReadAllText(file))!;
    foreach (JsonNode? run in sarif["runs"]!.AsArray())
    {
        if (run?["results"] is not JsonArray results)
        {
            continue;
        }

        if (drop)
        {
            int dropped = results.RemoveAll(result => Uri(result).Split('/').Contains("obj"));
            Console.WriteLine($"{file}: dropped {dropped} result(s) in generated code.");
            continue;
        }

        foreach (JsonNode? result in results)
        {
            if (result?["suppressions"] is JsonArray { Count: > 0 })
            {
                continue;
            }

            string rule = result?["ruleId"]?.GetValue<string>() ?? "";
            string message = result?["message"]?["text"]?.GetValue<string>() ?? "";
            int line = Location(result)?["region"]?["startLine"]?.GetValue<int>() ?? 0;
            Console.WriteLine($"{Uri(result)}:{line}: {rule}: {message}");
            findings++;
        }
    }

    if (drop)
    {
        File.WriteAllText(file, sarif.ToJsonString());
    }
}

if (drop)
{
    return 0;
}

Console.WriteLine(findings == 0 ? "No CodeQL findings." : $"{findings} CodeQL finding(s).");
return findings == 0 ? 0 : 1;

static JsonNode? Location(JsonNode? result)
    => result?["locations"] is JsonArray { Count: > 0 } locations ? locations[0]?["physicalLocation"] : null;

static string Uri(JsonNode? result) => Location(result)?["artifactLocation"]?["uri"]?.GetValue<string>() ?? "";
