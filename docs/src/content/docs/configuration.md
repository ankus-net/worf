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
  keep the same rows, and `create_subscription` warns when they don't.

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
  made afterwards commit later and win under `last_update_wins`. A remote
  transaction further ahead applies at once, with a warning at most once a
  minute. `-1` never waits. The default is `1s`.

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
  out of resources, are retried as under `error`. Skipped changes and
  disabled subscriptions are recorded in `worf.exception_log`, with the error,
  the change's rows or queued DDL, and the transaction it belongs to. The
  default is `error`.

- `worf.enable_ddl_replication`
  Replicates DDL as it runs, through the `ddl_sql` replication set as
  `worf.replicate_ddl_command` does, with the search path it ran with.
  Statements about databases, tablespaces, roles, server settings and
  PostgreSQL's own publications and subscriptions stay on their node, as do
  temporary objects. Other nodes run concurrent statements, such as
  `CREATE INDEX CONCURRENTLY`, in their apply transaction. The default is
  `off`.

- `worf.include_ddl_repset`
  Adds the tables DDL creates to the `default` replication set, or to
  `default_insert_only` when they have no primary key, and moves a table to
  `default` once it gets one. A dropped table leaves its replication sets
  with it. The default is `off`.

- `worf.allow_ddl_from_functions`
  Replicates DDL that functions and procedures run too. The default is
  `off`.

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
