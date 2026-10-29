# Worf plan

Worf is a port of [pglogical](https://github.com/willibrandon/pglogical) to C# and
.NET Native AOT, built with [Ankus](https://github.com/ankus-net/ankus). It
replicates tables between PostgreSQL databases with pglogical's publish and
subscribe model and installs as the `worf` extension.

The source is the fork at tag `v2.5.3`. That fork adds native Windows builds and
MSI installers, conflict history, and fixes for PostgreSQL 15–18 to pglogical 2.4.

## Scope

Worf 0.1.0 does everything pglogical 2.5.3 does, and runs multi-master, on
PostgreSQL 15 through 19, on Linux x64, macOS ARM64 and x64, and Windows x64.

- **Provider:** the `worf` logical decoding output plugin, with:
  - pglogical's native and JSON protocols and their startup parameters;
  - text, binary send/recv and internal datum formats;
  - replication sets, row filters and column lists;
  - origin forwarding and sequence state.
- **Subscriber:**
  - the supervisor, per-database manager, apply and table sync workers;
  - heap and SPI apply with batch inserts;
  - all five conflict resolutions, conflict logging and conflict history;
  - apply delay, synchronous commit, multiple upstreams and subscription interfaces.
- **Initial sync:**
  - structure through `pg_dump` and `pg_restore`, and data through COPY, both from the slot's exported snapshot;
  - resynchronization of a single table;
  - sequence synchronization.
- **DDL:**
  - `replicate_ddl_command` and the queue table;
  - TRUNCATE replication;
  - the drop and dependency handling in `pglogical_executor.c` and `pglogical_dependency.c`.
- **SQL interface:** every table, view, index, function and setting in `pglogical--2.5.0.sql` and `pglogical.c`, renamed as described below.
- **`worf_create_subscriber`:** the port of `pglogical_create_subscriber`, which turns a physical standby into a subscriber.
- **The fork's own changes:**
  - conflict history;
  - generated columns;
  - the PostgreSQL 18 `fill_missing_defaults` fix;
  - the table sync catchup timeout fix;
  - the manager restart fix for databases without the extension;
  - its Windows behavior: latch waits, timer granularity in apply delay, `pg_dump` and `pg_restore` quoting and interruption, and temporary directory selection.
- **Packages:** tar.gz for Linux and macOS, and zip and MSI for Windows, each including `worf_create_subscriber`.

These parts are not ported, because they exist only for PostgreSQL releases
before 15 or for upgrading older pglogical installations:

- `compat94` through `compat14`;
- `pglogical_dump`, a copy of PostgreSQL 9.4's `pg_dump` that pglogical runs only on 9.4. On later versions it runs the server's own `pg_dump`, as worf does;
- the `pglogical_origin` extension;
- the `pglogical_output` forwarding module;
- the 1.x through 2.4 install and upgrade scripts.

The CMake, Make, Perl and shell tooling is replaced by the .NET equivalents described below.

## Naming

Worf keeps pglogical's names, with `worf` in place of `pglogical`.

- The extension, schema, library and output plugin are named `worf`.
- Settings are `worf.*`, such as `worf.conflict_resolution`.
- Functions keep their names. `pglogical.create_subscription` becomes `worf.create_subscription`, and `pglogical.pglogical_node_info` becomes `worf.worf_node_info`.
- Slot and origin names start with `worf_` where pglogical's start with `pgl_`.
- Worker names, application names and messages say `worf` where pglogical's say `pglogical`.

Nothing else about the interface changes.

## Building on Ankus

| pglogical uses | Ankus provides |
| --- | --- |
| C functions | [`[PgFunction]`](https://ankus.net/function-declarations/) |
| Catalog tables, views and SQL functions | [Custom SQL](https://ankus.net/custom-sql/) |
| GUCs | [Configuration settings](https://ankus.net/configuration/) |
| `_PG_init`, `ProcessUtility_hook`, `object_access_hook`, cache callbacks | [Initialization](https://ankus.net/initialization/) and [native callbacks](https://ankus.net/raw-values/) |
| Shared memory and LWLocks | [Shared memory](https://ankus.net/shared-memory/) |
| Supervisor, manager, apply and sync workers | [Background workers](https://ankus.net/background-workers/) |
| Output plugin | [Logical decoding](https://ankus.net/logical-decoding/) |
| libpq inside the backend | [Replication connections](https://ankus.net/replication-connections/) |
| Heap apply, conflict lookup, replication origins | [Applying changes](https://ankus.net/applying-changes/) |
| SPI apply and catalog queries | [SPI](https://ankus.net/spi/) |
| `pg_dump` and `pg_restore` | [`PgChildProcess`](https://ankus.net/background-workers/#running-programs) |
| TRUNCATE trigger | [Triggers](https://ankus.net/triggers/) |
| Any other PostgreSQL function | `NativeMethods` [raw bindings](https://ankus.net/raw-values/) |
| `make check` | [`ankus regress`](https://ankus.net/reference/cli/#run-sql-regression-suites) |
| Multi-server Perl tests | [`Ankus.Testing`](https://ankus.net/getting-started/testing/) |
| Packaging | [`ankus package`](https://ankus.net/reference/cli/#package-for-distribution) |

### Ankus issues

When worf needs something that Ankus does not support, or Ankus behaves wrongly,
open an issue at <https://github.com/ankus-net/ankus/issues>. Include a short
reproduction, the PostgreSQL version and the platform. Do not change the Ankus
repository or copy its code into worf.

If Ankus's raw `NativeMethods` bindings can cover the gap, use them, add a
comment linking the issue, and remove that code once Ankus ships the fix.
Otherwise, move on to other work until it ships.

## Layout

```text
Worf.slnx
src/Worf/                     the extension
src/Worf/pg_regress/          the ported regression suite
src/Worf.CreateSubscriber/    worf_create_subscriber
tests/Worf.Tests/             multi-server and create_subscriber tests
eng/                          file-based C# apps for stress runs and packaging, with a README
packaging/windows/worf.wxs    MSI definition
docs/                         user guides
.github/workflows/            ci.yml and release.yml
```

Start from `ankus new Worf --background-worker` and delete the sample functions
and tests it creates.

Each pglogical C file becomes one C# file in `src/Worf`, with its header folded in:

| pglogical | worf |
| --- | --- |
| `pglogical.c` | `Worf.cs` (initialization, settings, supervisor, shared helpers) |
| `pglogical_worker.c` | `Worker.cs` |
| `pglogical_manager.c` | `Manager.cs` |
| `pglogical_apply.c` | `Apply.cs` |
| `pglogical_apply_heap.c` | `ApplyHeap.cs` |
| `pglogical_apply_spi.c` | `ApplySpi.cs` |
| `pglogical_conflict.c` | `Conflict.cs` |
| `pglogical_conflict_history.c` | `ConflictHistory.cs` |
| `pglogical_sync.c` | `Sync.cs` |
| `pglogical_sequences.c` | `Sequences.cs` |
| `pglogical_queue.c` | `Queue.cs` |
| `pglogical_executor.c` | `Executor.cs` |
| `pglogical_dependency.c` | `Dependency.cs` |
| `pglogical_node.c` | `Node.cs` |
| `pglogical_relcache.c` | `RelCache.cs` |
| `pglogical_repset.c` | `ReplicationSet.cs` |
| `pglogical_rpc.c` | `Rpc.cs` |
| `pglogical_functions.c` | `Functions.cs` |
| `pglogical_monitoring.c` | `Monitoring.cs` |
| `pglogical_fe.c` | `Fe.cs`, shared with `Worf.CreateSubscriber` |
| `pglogical_output_plugin.c` | `OutputPlugin.cs` |
| `pglogical_output_config.c` | `OutputConfig.cs` |
| `pglogical_output_proto.c` | `OutputProto.cs` |
| `pglogical_proto_native.c` | `ProtoNative.cs` |
| `pglogical_proto_json.c` | `ProtoJson.cs` |
| `pglogical--2.5.0.sql` | `worf--0.1.0.sql` content, declared through Ankus |
| `pglogical_create_subscriber.c` | `src/Worf.CreateSubscriber/Program.cs` |

How to port the code:

- Port pglogical directly. Keep its functions, control flow, error messages and SQLSTATEs.
- Use PascalCase names; `handle_insert` becomes `HandleInsert`.
- Where the code in the `compat` folders differs by PostgreSQL version, use a version check at that spot, and include PostgreSQL 19.

## Order of work

This order only decides what is built first. Worf 0.1.0 needs every step.

A step is done when its tests pass in CI on every PostgreSQL version and
platform. Check the step off here and link the CI run that proved it.

- [x] **1. Catalog and nodes.** Settings, the `worf` schema and tables, node and interface functions, and version and info functions. Tests: `preseed`, `infofuncs`. Proved by [CI run 40045197753](https://github.com/ankus-net/worf/actions/runs/40045197753).
- [x] **2. One table end to end.** Tests: `init_fail`, `init`, `preseed_check`, `basic`. Proved by [CI run 40108211458](https://github.com/ankus-net/worf/actions/runs/40108211458). This step covers:
  - shared memory, and the supervisor, manager and apply workers;
  - the output plugin and native protocol;
  - `create_subscription` with structure and data sync;
  - heap apply of inserts, updates and deletes;
  - origin tracking.
- [x] **3. Apply and conflicts.** Tests: `extended`, `conflict_secondary_unique`, `conflict_history`, `toasted`, `primary_key`, `foreign_key`, `copy`, `triggers`, `matview`. Proved by [CI run 40108211458](https://github.com/ankus-net/worf/actions/runs/40108211458). This step covers:
  - every conflict resolution and conflict history;
  - SPI apply and batch inserts;
  - TOAST, triggers, foreign keys, primary key changes, COPY and materialized views.
- [x] **4. Replication sets, DDL and sequences.** Tests: `replication_set`, `add_table`, `interfaces`, `functions`, `sequence`, `row_filter`, `row_filter_sampling`, `att_list`, `column_filter`, plus a test that reads the JSON protocol. Proved by [CI run 40108211458](https://github.com/ankus-net/worf/actions/runs/40108211458). This step covers:
  - replication set management, row filters and column lists;
  - adding and removing tables, with resynchronization;
  - the queue, `replicate_ddl_command` and TRUNCATE;
  - executor and dependency handling;
  - sequences, the wait functions, interfaces and the JSON protocol.
- [x] **5. Topologies.** Origin forwarding, bidirectional replication, multiple upstreams, cascades, apply delay, parallel subscriptions and drop. Proved by [CI run 40409905502](https://github.com/ankus-net/worf/actions/runs/40409905502), with the `huge_tx` tests run on demand. Tests:
  - `bidirectional`, `apply_delay`, `multiple_upstreams`, `node_origin_cascade`, `parallel`, `drop`;
  - the three `huge_tx` tests;
  - the Perl tests `011`, `020` and `040`.
- [x] **6. `worf_create_subscriber`.** Tests: Perl test `010` and the command's options. Proved by [CI run 40409905502](https://github.com/ankus-net/worf/actions/runs/40409905502).
- [ ] **7. Release.** Packages, the MSI, the release workflow and docs.

### Progress notes

- 2026-10-26: Dropped PostgreSQL 13 and 14. 13 is out of support and 14 reaches end of life on November 12, 2026. No alternate expected output was specific to them: psql titles `\dt` "List of relations" through 17, so the `huge_tx` `_1` files stay. The `huge_tx` tests can't finish within CI's limit, so they left CI and run on demand with `eng/regress.cs --huge-tx`. CI passed on all three platforms for 15–19 in [run 40375031087](https://github.com/ankus-net/worf/actions/runs/40375031087): Linux jobs took 7–9 minutes, macOS 9–11 and Windows 12–14, most of it [publishing the extension again for every test cluster](https://github.com/ankus-net/ankus/issues/122).
- 2026-10-26: Ankus 0.2.11 publishes once per test run, and the create-subscriber test checkpoints the provider before its base backup, so warm CI jobs take about 5–9 minutes. [CI run 40409905502](https://github.com/ankus-net/worf/actions/runs/40409905502) passed on all three platforms for 15–19. `eng/regress.cs --huge-tx` passed all 33 tests on release builds of 15–19 on Linux, taking 51–71 minutes a version.

## Multi-master

Worf goes beyond pglogical to run multi-master on stock PostgreSQL, with no
patched server. Worf 0.1.0 needs every step, before the release. The porting
rules don't apply to these steps, and each adds multi-server tests.

- [x] **1. Convergence.** Equal commit timestamps resolve by the id of the node that wrote each change, wherever it was relayed from. A warning when `apply_remote` or `keep_local` resolves conflicts between nodes that replicate both ways. Apply waits out a configured maximum clock skew. Proved by [CI run 40826673559](https://github.com/ankus-net/worf/actions/runs/40826673559).
- [ ] **2. Apply errors.** A failing statement or transaction is skipped, or its subscription disabled, and recorded in an exception log, so errors don't stop replication.
- [ ] **3. DDL.** Apply tolerates columns one node has dropped or not yet added. DDL is captured and queued automatically, and new tables join replication sets. DDL that can't apply in any order locks every node first.
- [ ] **4. Sequences.** Automatic sequence ranges, for schemas that can't change, and Snowflake IDs, for those that can.
- [ ] **5. Membership.** Joining and leaving a group of nodes, a sync barrier across them, and a lag view.
- [ ] **6. Delta apply.** Columns that merge as increments rather than overwrite.
- [ ] **7. Constraints.** Apply checks every unique index and foreign key and records violations as conflicts. Those checks find a violation after it has happened; only eager commit, which commits a transaction on every node or none, prevents it.

## Tests

The fork's tests are the acceptance tests. Port all of them, and never weaken a test to make it pass.

### Regression suite

- Copy `sql/` and `expected/` into `src/Worf/pg_regress` and apply the renaming.
- Run them with `ankus regress`. Pass the settings from `regress-postgresql.conf` and its `pg_hba.conf` with `--postgresql-conf`.
- Keep the order of the Makefile's `REGRESS` list. `ankus regress` runs files in name order, so prefix each file name with its position.
- Record each output with `ankus regress --add` and compare it with the renamed pglogical file. The only differences allowed are:
  - the renaming and the column widths it shifts;
  - version strings;
  - the multi-master steps' warnings.
- Keep alternate outputs such as `functions_1.out`.
- pglogical's default run leaves out the three `huge_tx` tests. They take longer than CI allows, so worf runs them on demand with `eng/regress.cs --huge-tx`.

### Multi-server tests

- Port the Perl tests `t/010`, `011`, `020` and `040` to MSTest in `tests/Worf.Tests`. Each node gets its own cluster from `Ankus.Testing`.
- pglogical disables `t/030` because it changes the system clock. Cover what it checks, apply delay across a daylight saving change, by testing the apply delay calculation with commit times on either side of a transition.

### Additional tests

Add only these tests beyond pglogical's:

- **Restart and crash recovery:** stop the provider and the subscriber with an immediate shutdown while changes are streaming. After restart, no rows are lost or applied twice.
- **Windows installation paths:** `pg_dump` and `pg_restore` work when PostgreSQL is installed under `C:\Program Files`.
- **`worf_create_subscriber` options.**
- **Multi-master:** each multi-master step's tests.

### Test rules

- Tests run the published Native AOT extension in a real PostgreSQL server.
- Do not add test-only SQL functions or `[PgTest]` checks to the extension.
- `eng/stress.cs` runs the suite repeatedly to find flaky tests, as `scripts/stress-tests.sh` does.

## CI and release

**`ci.yml`:**

- Runs on every push and pull request.
- Builds and runs the full regression suite and the MSTest tests on PostgreSQL 15–19 on Linux x64, macOS ARM64 and Windows x64.
- A nightly run on Linux adds a stress run.
- No job runs longer than 30 minutes.

**`release.yml`:**

- Runs on a `v*` tag; tags with a hyphen are prereleases.
- Runs CI, then packages each PostgreSQL version and platform with `ankus package`, including macOS x64, and adds `worf_create_subscriber`.
- Produces tar.gz, zip and MSI files and SHA-256 checksums, and publishes a GitHub release.
- Builds the MSI with WiX 5, as the fork's `packaging/windows` does.

## Docs

- **`README.md`:** pglogical's README with the renaming applied, covering setup, every function and setting, conflicts and limitations.
- **`docs/conflict-history.md`** and **`docs/bidirectional-replication.md`.**
- **`docs/windows.md`:** installing from the MSI or zip, and how worf behaves on Windows.

## Rules

- Outside the multi-master steps, add no interfaces, layers or types that pglogical does not have. Add no projects beyond the three above.
- Do not build or vendor the C pglogical. Its expected outputs are the reference.
- Keep test-run logs, generated reports, local paths and machine names out of the repository.
- Follow Ankus's C# conventions in its `AGENTS.md` and `.editorconfig`.
