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
  compares the settings the apply workers run with, from the server's command
  line, the configuration files, `ALTER SYSTEM`, `ALTER DATABASE`, and
  `ALTER ROLE` for the bootstrap superuser the workers run as, not a session's
  `SET` or the settings of the role that runs it. A provider connection's user
  that can't read `pg_file_settings` reports the value its session started
  with, and one that can't run `pg_read_file` misses the command line's.

  The default value is `apply_remote`.

  The `keep_local`, `last_update_wins` and `first_update_wins` settings
  require `track_commit_timestamp` PostgreSQL setting to be enabled. A row whose
  tuple header says it's frozen, as vacuum or `COPY FREEZE` leaves it, has no
  commit timestamp or origin here, as in pglogical, though PostgreSQL may still
  keep both: an update to it doesn't conflict, and an insert that finds it
  resolves against a row with no commit timestamp.

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
  `first_update_wins` need. Only apply workers on a node that other nodes
  subscribe to wait, as their local changes can meet the remote ones there
  or further on, and only under those two settings. A remote transaction further ahead applies at once, with a warning
  at most once a minute. `-1` never waits. The default is `1s`.

- `worf.exception_behaviour`
  What apply does with a remote transaction that fails, as on a constraint
  only the subscriber has:
  - `error` - stop, and apply the transaction again when the worker
    restarts, until the error is fixed
  - `discard` - apply the transaction again change by change, skipping the
    changes that fail, and those whose rows conflict at commit in a
    deferrable unique or exclusion constraint with a row that stays, which
    it applies again without, so rows that swap values still apply; or the
    whole transaction when it fails at commit otherwise, as on a deferred
    constraint trigger
  - `transdiscard` - skip the whole transaction
  - `sub_disable` - disable the subscription, which applies the transaction
    again once it's enabled

  Errors that don't come from the data, such as deadlocks, serialization
  failures, lock and statement timeouts, cancellations, shutdowns, running out
  of resources, money that a provider formats otherwise than this node, every
  value of which fails, and a call of `nextval` that waited too long for a
  sequence block's grantor this server hasn't heard from since it started, are
  retried as under `error`. Every skipped change,
  including each change of a transaction skipped as a whole, and disabled
  subscriptions are recorded in `worf.exception_log`, with the error, the
  change's rows or queued DDL, and the transaction it belongs to. A value the
  subscriber can't read is recorded as its bytes in hex. The transaction that a
  subscription applies again, and how, is kept in `worf.exception_state` across
  restarts. Table sync workers catching up after a copy handle failures the same
  way, streaming from their slot again. When a walsender of their earlier
  connection, with the same user and application name, still holds the slot
  after a second, they stop it; they wait for anything else with a warning, and
  fail after a minute. Their cleanup, which drops the slot, waits five seconds
  instead, or none when it can't reach the provider, then leaves the slot, with
  a warning, to the subscription's apply worker, which drops it, as it does any
  slot its table syncs left when it starts, and tries again every five seconds
  while one stays, and once a minute for the slot of a sync that ended without
  saying so, as one killed; dropping the subscription drops them too. One that
  stops after its copy has committed catches up again from where it got to when
  it restarts, rather than copying again. When its slot is gone or invalidated, or
  its origin has no progress, it copies the table again and merges the copy in,
  in one transaction: the provider's rows replace those with the same replica
  identity or primary key, or join them, and the table's other rows stay, since
  whether the subscription wrote one can't be told in general. So a row the
  provider deleted while the sync couldn't catch up stays here; resynchronizing
  the table empties it and copies it again. Writers go on meanwhile, and readers
  see the rows as they were until it commits. A table with neither key fails
  instead, and retries, until it is resynchronized, and so does a merge that a
  unique index other than the key, deferrable or not, or an exclusion constraint
  refuses, as when a row the provider has since deleted here holds a value one
  of the provider's rows takes, or the provider moved a value from one row to
  another, with a hint to resynchronize. Under `discard` and `transdiscard`,
  the rows the merge writes that a `NOT NULL` column or `CHECK` constraint
  refuses, and those that an immediate unique index or exclusion constraint
  refuses, as a row the table keeps or an earlier row of the copy holds their
  values, whatever the index's expressions and predicate, are skipped at
  once, unless a `BEFORE ROW` trigger could change them first, and the rest
  merged again, and if it still fails, the rows it writes in halves, and a
  few rows each alone, down to the rows that fail alone, as on a trigger's
  error, each part taking a transaction ID; skipped
  rows are recorded as a `MERGE` with their columns' text and their numbers
  in the copy, with a warning, so that the provider's other rows, and its
  changes after the copy, still arrive; under `sub_disable`, it disables the subscription. In a table's initial copy, a row that fails, whether its value
  doesn't convert to its column's type, the column's input refuses it or a
  constraint does, is skipped under `discard` and `transdiscard` and disables
  the subscription under `sub_disable`, and is recorded as a `COPY` with its
  text, its number in the copy as the action counter and the copy's position.
  Under `discard`, `transdiscard` and `sub_disable`, the table copies at once,
  in one subtransaction, as under `error`. Once a row fails, under `sub_disable`
  the row is read again to record, and otherwise the table copies again from
  the start in batches of up to a thousand rows and 8 MB, each in its own
  subtransaction, which take a transaction ID each and go on past each row that
  fails: a row whose value doesn't convert is left out before the batch is
  copied, as is a row that one of the table's immediate unique indexes or
  exclusion constraints refuses, as a row the table holds or an earlier row
  of the batch that's copied holds its values, whatever the index's
  expressions and predicate, and a row that its column's input, a `NOT NULL`
  column or a `CHECK` constraint refuses is left out as the batch is copied;
  neither check runs where a `BEFORE ROW` trigger or a generated column could
  change the row first. A row that `COPY` refuses otherwise, as one whose key
  a concurrent transaction writes, one that a trigger refuses, or one that a
  deferrable constraint that is initially immediate refuses as the batch's
  `COPY` ends, has the batch's rows before it copied again. Foreign keys
  aren't checked, as for apply. The rows that fail are recorded in runs of up
  to a thousand rows and 8 MB, in order. A deferrable unique or exclusion
  constraint that is initially deferred is checked once the table has copied:
  a conflict fails the copy under `error`, disables the subscription under
  `sub_disable`, and otherwise a row is deleted and recorded when it
  conflicts with an earlier row that stays, as rows checked in the copy's
  order would be. Such rows have no number in the copy, which the table
  doesn't keep, and are recorded with 0 as the action counter. Under `error` the copy stops,
  naming the row, column and value, and starts again after a delay of 5 seconds
  that doubles each time, up to 5 minutes, from the start of a table's last copy
  or the failure of a subscription's first;
  `worf.alter_subscription_resynchronize_table` starts it again at once. The
  default is `error`.

- `worf.enable_ddl_replication`
  Replicates DDL as it runs, through the `ddl_sql` replication set as
  `worf.replicate_ddl_command` does, with the settings that change what it
  means: `search_path`, `DateStyle`, `IntervalStyle`, `TimeZone`,
  `standard_conforming_strings`, `default_tablespace`,
  `default_table_access_method` and `default_toast_compression`. Statements
  about databases, tablespaces, roles, server settings and PostgreSQL's own
  publications and subscriptions stay on their node, including renaming them,
  changing their owners, commenting on them and granting on them, as do
  statements that use the session's temporary objects or prepared statements
  and create or change only temporary objects. Those that would create or
  change other objects with them fail, since the other nodes don't have them.
  A statement uses a temporary object when it opens a temporary table, view
  or sequence; creates, changes, drops, comments on, labels or grants on a
  temporary object; names any object with the temporary schema, as `pg_temp`
  or by its own name; names a temporary type without a schema in a cast
  written with `::`, `CAST` or `TREAT`; runs or inlines a temporary function;
  or makes what it creates or changes depend on a temporary object, as a
  column of a temporary type or a default that calls a temporary function
  does. A `DROP` of temporary and other tables drops the others everywhere;
  temporary objects of other kinds, and worf itself, can only be dropped in a
  statement of their own. Other nodes run concurrent statements, such as
  `CREATE INDEX CONCURRENTLY`, in their apply transaction. The default is
  `off`.

- `worf.subscription_schema_restore`
  Marks the session that restores a subscription's structure, which DDL
  replication, the global DDL lock and sequence range checks leave alone. worf
  sets it on the connection to the subscriber's own node. Only a superuser, or
  a role granted `SET` on it with
  `GRANT SET ON PARAMETER worf.subscription_schema_restore`, can set it, so the
  user in the subscriber node's connection string needs one or the other to
  synchronize structure. The default is `off`.

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
  index or a `NOT NULL` column without a default, adding a foreign key, which
  locks the table it references too, attaching a partition, dropping a table
  or a schema with `CASCADE`, adding a domain constraint, renaming an enum
  value, or changing a composite type. It covers a partitioned table's
  partitions, and every table with a column of a changed type, or of a domain,
  array, range, composite type or table row type built on it. Detaching a
  partition applies in any order. In a transaction block, any DDL that locks a
  table against writes, such as `DROP INDEX` or `CREATE TRIGGER`, takes the
  lock for it first, so later DDL there needn't wait. Before the DDL runs,
  each node this node subscribes to that receives its DDL through `ddl_sql`
  holds back writes to those tables it replicates, including writes through a
  partitioned table, which fail with SQLSTATE `55P03`, and DDL on them, until
  the DDL has applied there. Every node applies the others' changes up to that
  point first. A savepoint rolled back lets go of its tables at the
  transaction's next statement. A node keeps holding back across a restart,
  and while it can't reach the node that ran the DDL, until the DDL applies,
  until it has applied past the transaction that ran it, as when
  `worf.exception_behaviour` skips that transaction, or until that node says
  the transaction aborted. Dropping its last subscription to that node, or
  `worf.release_fence`, lets go too. Only a user who may run the DDL takes the
  lock: the owner of the table, schema or type, a user with the `TRIGGER`
  privilege for `CREATE TRIGGER`, and for a `DROP`, the owner of the object's
  schema too. `CREATE UNIQUE INDEX CONCURRENTLY` holds the lock across the
  transactions it commits, until the last commits or the statement fails, and
  from its first commit, a node keeps holding back if its connection is lost,
  until it learns that the statement has ended. An index that such a build, or
  a concurrent `REINDEX`, leaves when it fails is on its node alone, as are
  statements on it, however it's built again since, and
  `DROP INDEX CONCURRENTLY` drops it there. DDL that would make other objects
  use it, as a constraint's, the replica identity, the clustering index or a
  partition of another index, fails. DDL that copies its table's indexes with
  `LIKE` leaves its copy out. DDL that attaches its table as a partition,
  indexes the partitioned table above it, adds a foreign key that references
  its table, or changes the type or generation expression of a column of a
  table whose foreign keys reference its table, or the type of an attribute of
  a composite type such a table uses, passes over it where it would use it: a
  partitioned index that PostgreSQL may take it for, as one that matches it in
  uniqueness, access method, columns, included columns, and whether each has a
  predicate, or a foreign key's valid unique index by the columns it
  references. Such DDL then does what it does on the other
  nodes, and fails if an event trigger or a function it runs writes to or
  rewrites a table whose index it attaches, since the index wouldn't get the
  rows. DDL that gives a relation its name renames it, with a warning, and once
  a statement has run, the indexes and sequences it created that PostgreSQL
  named beside it get the names the other nodes give them, renaming it if it
  has one of those names. A statement first takes the locks it takes on the
  tables, as it would without worf. Only an index it uses, or whose name it
  takes, is locked until the transaction ends, as PostgreSQL locks an index it
  uses or a name it takes, so DDL waits on no other such index.
  `worf.replicate_ddl_command` does the same on every node.
  DDL from another node that can't build it again, as a unique one that finds
  duplicate keys when a table is rewritten, drops it, with a warning that
  gives its definition. Drop it: until then a unique one still rejects
  duplicates, so apply on its node stops at a row the other nodes wrote that
  it rejects. A transaction holds each
  node back through one connection, whatever the `idle_session_timeout` of its
  role. The user in each provider connection string needs `USAGE` on schema
  `worf` and `EXECUTE` on `worf.fence_writes`, `worf.unfence_writes` and
  `worf.keep_fence`, which `PUBLIC` doesn't have. They let that user hold back
  writes to any table on the node, across restarts, and let go only of what
  the same session held back, so grant them only to the role other nodes
  connect as. The default is `on`.

- `worf.sequence_range_check_interval`
  How often the range worker looks at each managed sequence, switching it to
  its next block when the active one runs low. A sequence that hasn't moved,
  whose blocks no committed change has touched, and that no statement waits
  on, is checked in full only every ten intervals, and at least a second
  apart; changes to other sequences' blocks don't count, for up to 16,384
  sequences across the server's databases, past which a sequence counts
  every change. A session that
  passes half a block, or reaches its last twentieth, wakes it at once. The
  default is `1s`.

- `worf.sequence_range_exchange_interval`
  How often each node exchanges sequence range reports with its neighbors
  when no request, grant or reset is waiting. While one waits, a node asks
  every check interval, then less often, up to this interval, while nothing
  changes, and while it waits on a neighbor's fence, or on a pool holder,
  up to ten check intervals. A failed exchange is retried after a delay that
  doubles up to this, or up to half `worf.sequence_range_timeout` while
  something waits on that neighbor; one that ran out of time, up to a minute
  or this, whichever is longer. The user in each provider
  connection string needs `EXECUTE` on `worf.sequence_range_exchange`, which
  `PUBLIC` doesn't have. The default is `1min`.

- `worf.sequence_range_restore`
  Lets the session write rows whose values no managed sequence may still
  issue as far as this node knows, as a restore of other nodes' rows, or of
  rows from before range management, needs. See
  [Sequence ranges](../sequence-ranges/). Only superusers and roles granted
  `SET` on it can set it. The default is `off`.

- `worf.sequence_range_timeout`
  How long a sequence range exchange has to connect to the other node, and for
  each call to it, which the exchange worker makes without waiting on either.
  Inserts near the end of a block wait for the switch, which a switch worker
  makes beside the transactions drawing from the block, without waiting for
  them to end, though it waits, as for a lock, for their statements to reach
  their next rows: up to twice this for each block in all while its next block
  is still to come, or once a worker has tried and couldn't switch it, and
  otherwise until it comes, as for a lock. A node whose last call ran out of
  time is probed, by a connection that has this long, until one answers. The
  default is `10s`.

- `worf.ddl_lock_timeout`
  How long DDL waits in all for the other nodes to hold back their writes and
  for this node to catch up with them before it fails. The default is `1min`.

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

## PostgreSQL settings

- `wal_receiver_timeout`
  As PostgreSQL's apply worker does, apply stops once its provider has sent
  nothing for this long, asking it for a reply halfway through, and each call
  it makes to its provider as it starts gives up after this long. The manager
  gives up on a worker that hasn't started after this long, with a warning,
  and starts it again later. `0` turns these off.

- `log_lock_waits`
  worf's workers log a wait for a lock that outlasts `deadlock_timeout`, with
  the lock and the processes that hold it, as PostgreSQL does by default from
  version 18, unless the server's configuration sets it. Apply waits for a lock
  that a session holds for as long as the session holds it, as PostgreSQL's
  apply does; the manager gives up after 5 seconds, with a warning, and waits
  for it again on its next look.
