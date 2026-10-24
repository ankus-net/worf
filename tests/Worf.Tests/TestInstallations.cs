using System.Globalization;
using Ankus.PgConfig;

namespace Worf.Tests;

/// <summary>
/// Finds the PostgreSQL installation the test run selects, as the Ankus test fixture does.
/// </summary>
internal static class TestInstallations
{
    /// <summary>
    /// Gets the installation that <c>ankus test</c> or the build's settings select.
    /// </summary>
    /// <param name="cancellationToken">Cancels the lookup.</param>
    /// <returns>The installation.</returns>
    public static async Task<PostgresInstallation> CurrentAsync(CancellationToken cancellationToken)
    {
        if (Environment.GetEnvironmentVariable("ANKUS_TEST_PG_CONFIG") is { Length: > 0 } commandPath)
        {
            return await PostgresInstallation.CreateAsync(commandPath, cancellationToken);
        }

        if (HostValue("Ankus.Testing.PgConfigPath") is { } hostPath)
        {
            return await PostgresInstallation.CreateAsync(hostPath, cancellationToken);
        }

        string? major = Environment.GetEnvironmentVariable("ANKUS_TEST_POSTGRES_MAJOR") is { Length: > 0 } commandMajor
            ? commandMajor
            : HostValue("Ankus.Testing.PostgresMajor");
        return major is null
            ? await PostgresInstallation.DiscoverAsync(cancellationToken)
            : await PostgresInstallation.DiscoverAsync(int.Parse(major, CultureInfo.InvariantCulture),
                HostValue("Ankus.Testing.HomeDirectory"), cancellationToken);
    }

    private static string? HostValue(string name)
        => Convert.ToString(AppContext.GetData(name), CultureInfo.InvariantCulture) is { Length: > 0 } value ? value : null;
}
