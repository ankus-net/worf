# Engineering apps

These are .NET file-based apps. Run each with `dotnet run eng/<app>.cs -- <arguments>`.

- `regress.cs` runs the ported pglogical regression suite with `ankus regress` on a fresh development cluster, using the settings from pglogical's `regress-postgresql.conf`. Pass `--pg <major>`, and optionally `--repeat <count>`, `--huge-tx` to add the three `huge_tx` tests, and a test name filter.
- `stress.cs` runs the suite repeatedly to catch flaky tests, as pglogical's `scripts/stress-tests.sh` does. Pass `--pg <major>`, and optionally `--runs <count>` (default 10), `--stop-on-fail` and `--huge-tx`. Failed runs keep their diffs, results and server log under `artifacts/stress/run-N`.
- `package.cs` packages worf for one PostgreSQL major version on the current platform: the extension from `ankus package` and `worf_create_subscriber`, laid out as `bin/`, `lib/` and `share/extension/`, in a tar.gz on Linux and macOS, or a zip and an MSI (with WiX 5) on Windows. Pass `--pg <major>` and `--version <version>`, and optionally `--output <directory>` (default `artifacts/packages`).
