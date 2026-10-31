---
title: Configuration options
description: worf's settings.
---

Some aspects of worf can be configured using configuration options that
can be either set in `postgresql.conf` or via `ALTER SYSTEM SET`.

- `worf.conflict_resolution`
  Sets the resolution method for any detected conflicts between local data
  and incoming changes.

  Possible values:
  - `error` - the replication will stop on error if conflict is detected and
    manual action is needed for resolving
  - `apply_remote` - always apply the change that's conflicting with local
    data
  - `keep_local` - keep the local version of the data and ignore the
     conflicting change that is coming from the remote node
  - `last_update_wins` - the version of data with newest commit timestamp
     will be kept (this can be either local or remote version)
  - `first_update_wins` - the version of the data with oldest timestamp will
     be kept (this can be either local or remote version)

  When the timestamps are equal, `last_update_wins` and `first_update_wins`
  keep the change written by the node with the higher node id, including
  changes another node forwarded, so every node keeps the same version.
  Nodes that replicate both ways need the same one of these two settings to
  keep the same rows, and `create_subscription` warns when they don't. It
  compares the settings the apply workers run with, from the configuration
  files and `ALTER SYSTEM`, `ALTER DATABASE` or `ALTER ROLE`, not a session's
  `SET`.

  The default value is `apply_remote`.

  The `keep_local`, `last_update_wins` and `first_update_wins` settings
  require `track_commit_timestamp` PostgreSQL setting to be enabled.

- `worf.conflict_log_level`
  Sets the log level for reporting detected conflicts when the
  `worf.conflict_resolution` is set to anything else than `error`.

  Main use for this setting is to suppress logging of conflicts.

  Possible values are same as for `log_min_messages` PostgreSQL setting.

  The default is `LOG`.

- `worf.max_clock_skew`
  How far ahead of the local clock a remote transaction's commit time can be
  for apply to wait until the local clock passes it, so that local changes
  made afterwards commit later than it, as `last_update_wins` and
  `first_update_wins` need. Only the apply worker of a subscription whose
  provider also subscribes to this node waits, and only under those two
  settings. A remote transaction further ahead applies at once, with a warning
  at most once a minute. `-1` never waits. The default is `1s`.

- `worf.exception_behaviour`
  What apply does with a remote transaction that fails, as on a constraint
  only the subscriber has:
  - `error` - stop, and apply the transaction again when the worker
    restarts, until the error is fixed
  - `discard` - apply the transaction again change by change, skipping the
    changes that fail, or the whole transaction when it fails at commit, as
    on a deferred constraint
  - `transdiscard` - skip the whole transaction
  - `sub_disable` - disable the subscription, which applies the transaction
    again once it's enabled

  Errors that don't come from the data, such as deadlocks, serialization
  failures, lock and statement timeouts, cancellations, shutdowns and running
  out of resources, are retried as under `error`. Every skipped change,
  including each change of a transaction skipped as a whole, and disabled
  subscriptions are recorded in `worf.exception_log`, with the error, the
  change's rows or queued DDL, and the transaction it belongs to. The
  transaction that a subscription applies again, and how, is kept in
  `worf.exception_state` across restarts. Table sync workers catching up
  after a copy handle failures the same way. The default is `error`.

- `worf.enable_ddl_replication`
  Replicates DDL as it runs, through the `ddl_sql` replication set as
  `worf.replicate_ddl_command` does, with the settings that change what it
  means: `search_path`, `DateStyle`, `IntervalStyle`, `TimeZone`,
  `standard_conforming_strings`, `default_tablespace`,
  `default_table_access_method` and `default_toast_compression`. Statements
  about databases, tablespaces, roles, server settings and PostgreSQL's own
  publications and subscriptions stay on their node, as do statements that use
  the session's temporary objects or prepared statements, and what they create.
  A `DROP` of temporary and other objects drops the others everywhere. worf
  itself can only be dropped in a statement of its own. Other nodes run
  concurrent statements, such as `CREATE INDEX CONCURRENTLY`, in their apply
  transaction. The default is `off`.

- `worf.include_ddl_repset`
  Adds the tables DDL creates to the `default` replication set, or to
  `default_insert_only` when they have no replica identity, and moves a table
  and its partitions between the two as they gain or lose one. A table that
  becomes unlogged leaves its replication sets, since its changes can't
  replicate, and a dropped table leaves them too. The default is `off`.

- `worf.allow_ddl_from_functions`
  Replicates DDL that functions and procedures run too. The default is
  `off`.

- `worf.global_ddl_lock`
  With `worf.enable_ddl_replication`, locks every node for DDL that can't
  apply in any order: changing a column's type, renaming a table, column or
  schema, moving a table to another schema, adding a constraint, a unique
  index or a `NOT NULL` column without a default, dropping a table or a schema
  with `CASCADE`, adding a domain constraint, renaming an enum value, or
  changing a composite type. It covers a partitioned table's partitions and
  every table with a column of a changed type. In a transaction block, any
  DDL takes the lock for its tables, so later DDL there needn't wait. Before
  the DDL runs, each node this node subscribes to that receives its DDL
  through `ddl_sql` holds back writes to those tables it replicates, which
  fail with SQLSTATE `55P03`, and DDL on them, until the DDL has applied
  there. Every node applies the others' changes up to that point first. A
  node keeps holding back across a restart, and lets go once the DDL applies
  or the transaction that ran it aborts. Only the table's, schema's or type's
  owner takes the lock. `CREATE UNIQUE INDEX CONCURRENTLY`, which commits
  partway, can't take it. The user in each provider connection string needs
  `EXECUTE` on `worf.fence_writes` and `worf.keep_fence`, which `PUBLIC`
  doesn't have. The default is `on`.

- `worf.sequence_range_check_interval`
  How often the range worker checks each managed sequence, switching it to
  its next block when the active one runs low. The default is `1s`.

- `worf.sequence_range_exchange_interval`
  How often each node exchanges sequence range reports with its neighbors
  when no request, grant or reset is waiting. While one waits, a node asks
  every check interval, then less often, up to this interval, while nothing
  changes. The user in each provider
  connection string needs `EXECUTE` on `worf.sequence_range_exchange`, which
  `PUBLIC` doesn't have. The default is `1min`.

- `worf.sequence_range_timeout`
  How long a sequence range exchange waits to connect to the other node, and
  for each call to it, before the range worker cancels it. The default is
  `10s`.

- `worf.ddl_lock_timeout`
  How long DDL waits for the other nodes to hold back their writes and catch
  up before it fails. The default is `1min`.

- `worf.max_fenced_tables`
  How many tables other nodes' DDL can hold back on this server at once.
  Takes effect at server start. The default is `4096`.

- `worf.batch_inserts`
  Tells worf to use batch insert mechanism if possible. Batch mechanism
  uses PostgreSQL internal batch insert mode which is also used by `COPY`
  command.

  The batch inserts will improve replication performance of transactions that
  did many inserts into one table. worf will switch to batch mode when
  transaction did more than 5 INSERTs.

  It's only possible to switch to batch mode when there are no
  `INSTEAD OF INSERT` and `BEFORE INSERT` triggers on the table and when
  there are no defaults with volatile expressions for columns of the table.
  Also the batch mode will only work when `worf.conflict_resolution` is
  set to `error`.

  The default is `true`.

- `worf.use_spi`
  Tells worf to use SPI interface to form actual SQL
  (`INSERT`, `UPDATE`, `DELETE`) statements to apply incoming changes instead
  of using internal low level interface.

  This is mainly useful for debugging purposes.

  The default is `false`.

  This can be set to `true` only when `worf.conflict_resolution` is set to `error`.
In this state, conflicts are not detected.

- `worf.temp_directory`
  Defines system path where to put temporary files needed for schema
  synchronization. This path need to exist and be writable by user running
  Postgres.

  Default is empty, which tells worf to use default temporary directory
  based on environment and operating system settings.

- `worf.synchronous_commit`
  Sets the `synchronous_commit` of the apply and sync workers' transactions:
  `local` when enabled, `off` when not. Committing asynchronously is safe,
  because worf confirms changes to the provider only once they are flushed
  locally.

  The default is `false`. It can only be set at server start.

- `worf.extra_connection_options`
  Connection options to add to all peer node connections; see
  [Subscription management](../subscriptions/).

  The default is empty.

- `worf.conflict_history_enabled`
  Records each resolved conflict in the `worf.conflict_history` table; see
  [docs/conflict-history.md](../conflict-history/).

  The default is `false`.

- `worf.conflict_history_store_tuples`
  Stores the local and remote rows of each recorded conflict as `jsonb`.

  The default is `true`.

- `worf.conflict_history_max_tuple_size`
  The maximum bytes of each column value stored with a recorded conflict, from
  64 to 65536. Longer values are truncated.

  The default is `1024`.
