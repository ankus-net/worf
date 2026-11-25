---
title: Configuration options
description: worf's settings.
---

Set these in `postgresql.conf` or with `ALTER SYSTEM SET`.

## Conflicts

- `worf.conflict_resolution`
  What to do when an incoming change conflicts with local data:
  - `error` - stop replicating until the conflict is resolved by hand
  - `apply_remote` - apply the incoming change
  - `keep_local` - keep the local row and skip the incoming change
  - `last_update_wins` - keep the version that committed last
  - `first_update_wins` - keep the version that committed first

  The default is `apply_remote`.

  When both versions have the same commit time, `last_update_wins` and
  `first_update_wins` keep the one from the node with the higher node id, so
  every node keeps the same row. Nodes that replicate both ways must use the same
  setting, and `create_subscription` warns when they don't. It compares the
  settings the apply workers run with: the server's command line, the
  configuration files, `ALTER SYSTEM`, `ALTER DATABASE`, and `ALTER ROLE` for the
  bootstrap superuser. A session's `SET` doesn't count. If the provider
  connection's user can't read `pg_file_settings` or run `pg_read_file`, the
  check may miss what the configuration files or the command line set.

  `keep_local`, `last_update_wins` and `first_update_wins` need
  `track_commit_timestamp` to be on. worf treats a frozen row, as left by
  `VACUUM FREEZE` or `COPY FREEZE`, as having no commit time: an update to it
  isn't a conflict, and an insert that finds it is resolved as against a row
  with no commit time.

- `worf.conflict_log_level`
  The log level for conflicts that are resolved automatically, that is, when
  `worf.conflict_resolution` isn't `error`. Use it mainly to stop logging
  conflicts. It takes the same values as `log_min_messages`. The default is
  `LOG`.

- `worf.max_clock_skew`
  With `last_update_wins` or `first_update_wins`, an incoming transaction may
  have a commit time ahead of this node's clock. If it's ahead by no more than
  this, apply waits until the local clock passes it, so local changes made
  afterwards get later commit times. Only nodes that other nodes subscribe to
  wait. A transaction further ahead applies at once, with a warning at most
  once a minute. `-1` never waits. The default is `1s`.

## Errors

- `worf.exception_behaviour`
  What apply does when an incoming transaction fails, for example on a
  constraint that only the subscriber has:
  - `error` - stop, and retry the transaction each time the worker restarts,
    until the problem is fixed
  - `discard` - apply the transaction again one change at a time, skipping the
    changes that fail
  - `transdiscard` - skip the whole transaction
  - `sub_disable` - disable the subscription; the transaction is applied again
    when the subscription is enabled

  The default is `error`.

  Under `discard`, a change whose row conflicts at commit with a deferrable
  unique or exclusion constraint is skipped too, and the rest is applied again,
  so rows that swap values still apply. If the transaction fails at commit for
  another reason, such as a deferred constraint trigger, the whole transaction
  is skipped.

  Errors that don't come from the data are always retried, as under `error`:
  deadlocks, serialization failures, lock and statement timeouts,
  cancellations, shutdowns, running out of resources, lost connections, I/O
  errors, and `money` values that the provider formats differently from this
  node.

  Every skipped change, including each change of a skipped transaction, and
  every disabled subscription is recorded in `worf.exception_log`, with the
  error, the change's rows or DDL, and its transaction. Values the subscriber
  can't read are recorded as hex. `worf.exception_state` keeps track, across
  restarts, of the transaction being applied again and how.

### Table synchronization

Table sync workers copy a table and then catch it up with the changes made
during the copy. While catching up, they handle failures the same way apply
does.

- **The slot.** If the sync's own earlier connection still holds its slot after
  a second, the sync ends that connection. If something else holds it, the sync
  waits, with a warning, and fails after a minute. When it's done, the sync
  drops its slot. If it can't within five seconds, or can't reach the provider,
  it leaves the slot to the subscription's apply worker, with a warning. The
  apply worker drops leftover sync slots when it starts, then retries every
  five seconds while any remain, or once a minute for a sync that was killed.
  Dropping the subscription drops them too.
- **Restarts.** A sync that stops after its copy committed picks up where it
  left off. If its slot is gone or invalid, or it has no recorded progress, it
  copies the table again and merges the copy in, in one transaction. Rows from
  the provider replace local rows with the same replica identity or primary
  key, or are added. Other local rows stay, since worf can't tell which of them
  the subscription wrote. So a row the provider deleted in the meantime stays;
  resynchronizing the table, which empties it and copies it again, removes it.
  Other sessions keep writing during the merge, and readers see the old rows
  until it commits.
- **Failed merges.** The merge fails, and retries until you resynchronize the
  table, when the table has neither a replica identity nor a primary key, or
  when another unique index or an exclusion constraint rejects it. That happens,
  for example, when a row the provider deleted still holds a value that one of
  its other rows now has. The error's hint says to resynchronize. Under
  `discard` and `transdiscard`, the merge skips rows that a `NOT NULL` column, a
  `CHECK` constraint, or an immediate unique index or exclusion constraint
  rejects, unless a `BEFORE ROW` trigger could change them first, and merges
  the rest. If it still fails, it retries the rows in halves, then one at a
  time, until only the rows that fail on their own are left; each part uses a
  transaction ID. Skipped rows are recorded as a `MERGE`, with their column
  values as text and their row numbers in the copy, and a warning, so the
  provider's other rows and later changes still arrive. Under `sub_disable`, a
  failed merge disables the subscription.

### The initial copy

When a row of a table's initial copy fails, because a value doesn't convert to
its column's type or a constraint rejects it:

- under `error`, the copy stops with an error naming the row, column and value.
  It starts again after 5 seconds, doubling the wait each time up to 5
  minutes. `worf.alter_subscription_resynchronize_table` starts it again at
  once.
- under `sub_disable`, the subscription is disabled and the row is recorded.
- under `discard` and `transdiscard`, the row is skipped and recorded, and the
  rest of the table is copied.

Each failed row is recorded in `worf.exception_log` as a `COPY`, with its text,
its row number in the copy as the action counter, and the copy's position.

The copy first runs in one piece. After a row fails, under `discard` and
`transdiscard`, the table is copied again from the start in batches of up to
1,000 rows and 8 MB, each in its own subtransaction, which uses a transaction
ID. A row is left out of its batch when a value doesn't convert, or when a
column's input, a `NOT NULL` column, a `CHECK` constraint, or an immediate
unique index or exclusion constraint rejects it. These checks are skipped when
a `BEFORE ROW` trigger or a generated column could change the row. If `COPY`
rejects a row for another reason, such as a concurrent write to the same key, a
trigger, or a deferrable constraint checked at the end of the batch, the
batch's rows before it are copied again without it. Foreign keys aren't
checked, just as apply doesn't check them. Failed rows are recorded in groups
of up to 1,000 rows and 8 MB, in order.

A deferrable unique or exclusion constraint that is initially deferred is
checked after the whole table is copied. A conflict fails the copy under
`error` and disables the subscription under `sub_disable`. Otherwise worf
deletes each row that conflicts with an earlier row and records it, with 0 as
the action counter, since the table doesn't keep row numbers.

## DDL

- `worf.enable_ddl_replication`
  Replicates DDL as it runs, through the `ddl_sql` replication set, as
  `worf.replicate_ddl_command` does. It sends along the settings that can
  change what a statement means: `search_path`, `DateStyle`, `IntervalStyle`,
  `TimeZone`, `standard_conforming_strings`, `default_tablespace`,
  `default_table_access_method` and `default_toast_compression`. The default is
  `off`.

  These statements stay on their node:
  - statements on databases, tablespaces, roles, server settings, and
    PostgreSQL's own publications and subscriptions, including renaming them,
    changing their owner, commenting on them and granting on them
  - statements that only create or change temporary objects

  A statement that uses a temporary object or a prepared statement to create or
  change anything else fails, since the other nodes don't have it. A statement
  uses a temporary object when it:
  - reads a temporary table, view or sequence
  - creates, changes, drops, comments on, labels or grants on one
  - names the temporary schema, as `pg_temp` or by its real name
  - casts to a temporary type named without a schema
  - runs a temporary function
  - creates something that depends on a temporary object, such as a column of
    a temporary type

  A `DROP` that names both temporary and permanent tables drops the permanent
  ones on every node. Other temporary objects, and worf itself, must be dropped
  in a statement of their own. On the other nodes, statements such as
  `CREATE INDEX CONCURRENTLY` run inside the apply transaction, so not
  concurrently.

- `worf.subscription_schema_restore`
  worf sets this on the session that copies a subscription's table definitions
  to the subscriber, so that DDL replication, the global DDL lock and the
  sequence range checks leave that session alone. Only a superuser, or a role
  granted it with `GRANT SET ON PARAMETER worf.subscription_schema_restore`, can
  set it. So the user in the subscriber node's own connection string needs one
  or the other to synchronize structure. The default is `off`.

- `worf.include_ddl_repset`
  Adds tables that DDL creates to the `default` replication set, or to
  `default_insert_only` if they have no replica identity. A table and its
  partitions move between the two as it gains or loses a replica identity. A
  table that becomes unlogged, or is dropped, is removed from its replication
  sets. The default is `off`.

- `worf.allow_ddl_from_functions`
  Also replicates DDL that functions and procedures run. The default is `off`.

- `worf.global_ddl_lock`
  With `worf.enable_ddl_replication`, DDL that would break if nodes applied it
  in a different order locks every node first. The default is `on`. That DDL
  is:
  - changing a column's type
  - renaming a table, column or schema, or moving a table to another schema
  - adding a constraint, a unique index, a foreign key (which also locks the
    table it references), or a `NOT NULL` column without a default
  - attaching a partition
  - dropping a table or schema with `CASCADE`
  - adding a domain constraint, renaming an enum value, or changing a
    composite type

  The lock covers a partitioned table's partitions, and every table with a
  column whose type is, or is built on, a type the DDL changes. Detaching a
  partition doesn't need the lock. In a transaction block, any DDL that blocks
  writes to a table, such as `DROP INDEX` or `CREATE TRIGGER`, takes the lock
  too, so later DDL in the transaction doesn't have to wait.

  Before the DDL runs, each node this node subscribes to, and that receives its
  DDL through `ddl_sql`, blocks writes and DDL on the tables involved until the
  DDL has reached it. Writes there fail with SQLSTATE `55P03` meanwhile,
  including writes through a partitioned table. Every node first applies the
  others' changes up to that point. A savepoint that rolls back releases its
  tables at the transaction's next statement.

  A node keeps blocking writes, across restarts and while it can't reach the
  node that ran the DDL, until the DDL applies, until it has applied past the
  DDL's transaction (as when `worf.exception_behaviour` skips it), or until it
  learns the transaction aborted. Dropping its last subscription to that node,
  or `worf.release_fence`, also releases it.

  Only a user allowed to run the DDL takes the lock: the owner of the table,
  schema or type, a user with the `TRIGGER` privilege for `CREATE TRIGGER`,
  and, for a `DROP`, the owner of the object's schema too.

  `CREATE UNIQUE INDEX CONCURRENTLY` holds the lock across the transactions it
  commits, until the last one commits or the statement fails. After its first
  commit, a node that loses its connection keeps blocking until it learns that
  the statement ended.

  A failed `CREATE INDEX CONCURRENTLY` or `REINDEX CONCURRENTLY` leaves an
  invalid index on its own node only. Statements on that index stay on its
  node, `DROP INDEX CONCURRENTLY` drops it there, and DDL that would make
  another object use it, such as a constraint or the replica identity, fails.
  Other DDL that PostgreSQL could match to it, such as attaching a partition or
  adding a foreign key, skips it, so the DDL does the same as on the other
  nodes. Replicated DDL that can't rebuild such an index, such as a unique one
  that finds duplicates when the table is rewritten, drops it with a warning
  that gives its definition. Drop these indexes yourself: a unique one still
  rejects duplicates, so apply on its node stops at a row from another node
  that it rejects.

  If DDL creates a relation with the name of such an index, worf renames the
  index, with a warning. After each statement, worf gives the indexes and
  sequences that PostgreSQL named automatically the names the other nodes give
  them.

  A transaction holds one connection to each node, whatever the
  `idle_session_timeout` of its role. The user in each provider connection
  string needs `USAGE` on schema `worf` and `EXECUTE` on `worf.fence_writes`,
  `worf.unfence_writes` and `worf.keep_fence`, which `PUBLIC` doesn't have.
  These let the user block writes to any table on the node, across restarts,
  so grant them only to the role other nodes connect as.

- `worf.ddl_lock_timeout`
  How long DDL waits in total for the other nodes to block writes, and for this
  node to catch up with them, before it fails. The default is `1min`.

- `worf.max_fenced_tables`
  How many tables other nodes' DDL can block on this server at once. Takes
  effect at server start. The default is `4096`.

## Sequence ranges

See [Sequence ranges](../sequence-ranges/).

- `worf.sequence_range_check_interval`
  How often the range worker checks each managed sequence and switches it to
  its next block when the active one runs low. A sequence that hasn't changed
  is checked fully only every tenth time, and at most once a second. A session
  that uses up half a block, or reaches its last twentieth, wakes the worker at
  once. The default is `1s`.

- `worf.sequence_range_exchange_interval`
  How often each node exchanges sequence range information with its neighbors
  when nothing is waiting. While a request waits, nodes exchange more often,
  starting at every check interval. A failed exchange is retried after a delay
  that doubles each time, up to this interval, or up to half of
  `worf.sequence_range_timeout` while something waits on that neighbor. A call
  that runs out of time, or a failed probe, is retried after a delay that
  starts at no more than 10 seconds and doubles each time, up to a minute or
  this interval, whichever is longer, whether or not anything waits. The
  user in each provider connection string needs `EXECUTE` on
  `worf.sequence_range_exchange`, which `PUBLIC` doesn't have. The default is
  `1min`.

- `worf.sequence_range_timeout`
  How long an exchange may take to connect to another node, and for each call.
  Inserts near the end of a block wait for the switch to the next block: up to
  twice this long, plus `worf.sequence_range_offline_wait`, if the next block
  hasn't arrived or a switch failed, and otherwise until the switch is done.
  The default is `10s`.

- `worf.sequence_range_offline_wait`
  How long a running node waits for the other nodes before it goes on without
  them. A node takes its next block from its reserve once its request has gone
  unanswered this long. The allocator waits this long once for another node to
  hear of a block it gave itself, and then uses its own blocks without waiting
  until another node answers. With `-1`, a node that can't reach the others
  keeps drawing from the blocks it holds, and then waits for them. The default
  is `10s`.

- `worf.sequence_range_restart_wait`
  How long a node that restarted waits for the other nodes to answer before it
  draws from its blocks anyway, with a warning. The wait lets the other nodes
  catch a node restored from a backup before it hands out numbers it already
  used, so a value other than `-1` risks duplicate keys after a restore. With
  `-1`, a restarted node waits until they answer, and each statement that draws
  gives up after twice `worf.sequence_range_timeout`. The default is `-1`.

- `worf.sequence_range_restore`
  Lets a session write rows with values that this node's sequences can no
  longer hand out, as when restoring rows written by other nodes or before
  sequence ranges were enabled. Only superusers and roles granted `SET` on it
  can set it. The default is `off`.

- `worf.max_sequence_range_counters`
  How many managed sequences, across all databases, get their own change
  counter, so the range worker fully checks only the ones that changed. Beyond
  that, sequences share 256 counters. Takes effect at server start. The default
  is `16384`.

## Apply

- `worf.batch_inserts`
  Applies runs of inserts into the same table in batches, as `COPY` does, which
  makes transactions with many inserts replicate faster. worf switches to
  batches after more than 5 inserts in a row into one table. It can't when the
  table has `INSTEAD OF INSERT` or `BEFORE INSERT` triggers or columns with
  volatile defaults, or when `worf.conflict_resolution` isn't `error`. The
  default is `true`.

- `worf.use_spi`
  Applies changes by running SQL `INSERT`, `UPDATE` and `DELETE` statements
  instead of PostgreSQL's internal interface. This is mainly for debugging. It
  can only be `true` when `worf.conflict_resolution` is `error`, and then
  conflicts aren't detected. The default is `false`.

- `worf.synchronous_commit`
  Sets `synchronous_commit` for the apply and sync workers: `local` when on,
  `off` when off. Committing asynchronously is safe, because worf confirms
  changes to the provider only after they're flushed locally. It can only be
  set at server start. The default is `false`.

- `worf.temp_directory`
  Where to put the temporary files used to synchronize structure. The
  directory must exist and be writable by the user that runs PostgreSQL. When
  empty, the system's temporary directory is used. The default is empty.

- `worf.extra_connection_options`
  Connection options added to every connection to another node; see
  [Subscription management](../subscriptions/). The default is empty.

## Conflict history

See [Conflict history](../conflict-history/).

- `worf.conflict_history_enabled`
  Records each resolved conflict in the `worf.conflict_history` table. The
  default is `false`.

- `worf.conflict_history_store_tuples`
  Stores the local and remote rows of each recorded conflict as `jsonb`. The
  default is `true`.

- `worf.conflict_history_max_tuple_size`
  The most bytes of each column value stored with a recorded conflict, from 64
  to 65536. Longer values are cut short. The default is `1024`.

## PostgreSQL settings

- `wal_receiver_timeout`
  Like PostgreSQL's own apply worker, apply stops when its provider has sent
  nothing for this long, and asks it for a reply halfway through. Each call
  apply makes to its provider while starting also gives up after this long.
  The manager gives up on a worker that hasn't started after this long, with a
  warning, and starts it again later. `0` turns these off.

- `log_lock_waits`
  Unless the server's configuration sets it, worf's workers log lock waits
  longer than `deadlock_timeout`, with the lock and the processes holding it,
  as PostgreSQL does by default from version 18. Apply waits for a lock as long
  as a session holds it, as PostgreSQL's apply does. The manager gives up after
  5 seconds, with a warning, and tries again on its next pass.
