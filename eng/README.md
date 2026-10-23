# Engineering apps

These are .NET file-based apps. Run each with `dotnet run eng/<app>.cs -- <arguments>`.

- `regress.cs` runs the ported pglogical regression suite with `ankus regress` on a fresh development cluster, using the settings from pglogical's `regress-postgresql.conf`. Pass `--pg <major>`, and optionally `--repeat <count>` and a test name filter.
