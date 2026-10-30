---
title: Limitations and restrictions
description: What worf does not do, and what it requires.
---

## Superuser is required

Currently worf replication and administration requires superuser
privileges. It may be later extended to more granular privileges.

## `UNLOGGED` and `TEMPORARY` not replicated

`UNLOGGED` and `TEMPORARY` tables will not and cannot be replicated, much like
with physical streaming replication.

## One database at a time

To replicate multiple databases you must set up individual provider/subscriber
relationships for each. There is no way to configure replication for all databases
in a PostgreSQL install at once.

## PRIMARY KEY or REPLICA IDENTITY required

`UPDATE`s and `DELETE`s cannot be replicated for tables that lack a `PRIMARY
KEY` or other valid replica identity such as using an index, which must be unique,
not partial, not deferrable, and include only columns marked NOT NULL.
Replication has no way to find the tuple that should be updated/deleted since
there is no unique identifier.
`REPLICA IDENTITY FULL` is not supported yet.


## Only one unique index/constraint/PK

If more than one upstream is configured or the downstream accepts local writes
then only one `UNIQUE` index should be present on downstream replicated tables.
Conflict resolution can only use one index at a time so conflicting rows may
`ERROR` if a row satisfies the `PRIMARY KEY` but violates a `UNIQUE` constraint
on the downstream side. This will stop replication until the downstream table
is modified to remove the violation.

It's fine to have extra unique constraints on an upstream if the downstream only
gets writes from that upstream and nowhere else. The rule is that the downstream
constraints must *not be more restrictive* than those on the upstream(s).

Partial secondary unique indexes are permitted, but will be ignored for
conflict resolution purposes.

## Unique constraints must not be deferrable

On the downstream end worf does not support index-based constraints
defined as `DEFERRABLE`. It will emit the error

    ERROR: worf doesn't support index rechecks needed for deferrable indexes
    DETAIL: relation "public"."test_relation" has deferrable indexes: "index1", "index2"

if such an index is present when it attempts to apply changes to a table.

## DDL

With `worf.enable_ddl_replication` on, DDL replicates as it runs, and
`worf.include_ddl_repset` adds the tables it creates to replication sets.
Otherwise, managing DDL so that the provider and subscriber database(s) remain
compatible is the responsibility of the user.

worf provides the `worf.replicate_ddl_command` function to allow DDL
to be run on the provider and subscriber at a consistent point.

A replicated `ALTER TABLE ... ADD COLUMN` with a volatile default, such as
`now()` or `gen_random_uuid()`, fills existing rows with each node's own
values.

With `worf.global_ddl_lock`, DDL that takes the lock fails at once with
SQLSTATE `55P03` when an earlier statement in its transaction locked or wrote
to rows that another node's changes, which this node applies first, need, as
`LOCK TABLE`, `TRUNCATE` or an `UPDATE` of a row another node changed can. Run
such DDL first in its transaction, or in a transaction of its own. A node
notices within about a minute that a node holding back its writes for DDL died
before the DDL committed.

`worf.replicate_ddl_command` doesn't take the global DDL lock: the DDL it
queues applies on each node wherever it falls among that node's own writes.

DDL that uses the session's temporary objects, as
`worf.enable_ddl_replication` describes, runs on its node alone, without a
notice. A temporary type named without a schema before a string constant, as
in `tmp_type '1'`, in an expression the statement doesn't keep, such as in
`ALTER TABLE ... USING`, isn't noticed, and the statement replicates; qualify
the type with `pg_temp`, or write the cast with `::`.

## Schema changes without the global DDL lock

Without `worf.global_ddl_lock`, there's no support for freezing transactions
on the provider and waiting until all pending queued transactions are replayed
from slots.

This means that care must be taken when applying table structure changes. If
there are committed transactions that aren't yet replicated and the table
structure of the provider and subscriber are changed at the same time in a way
that makes the subscriber table incompatible with the queued transactions,
replication will stop.

Administrators should either ensure that writes to the provider are stopped
before making schema changes, or use the `worf.replicate_ddl_command`
function to queue schema changes so they're replayed at a consistent point
on the subscriber. Where every node writes, that isn't enough, as another node
may write with the old structure after the change committed: replicate DDL with
`worf.enable_ddl_replication` and `worf.global_ddl_lock`, or stop writes on all
nodes and let all slots catch up before making schema changes.

## FOREIGN KEYS

Foreign keys constraints are not enforced for the replication process - what
succeeds on provider side gets applied to subscriber even if the `FOREIGN KEY`
would be violated.

## TRUNCATE

Using `TRUNCATE ... CASCADE` will only apply the `CASCADE` option on the
provider side.

(Properly handling this would probably require the addition of `ON TRUNCATE CASCADE`
support for foreign keys in PostgreSQL).

`TRUNCATE ... RESTART IDENTITY` is not supported. The identity restart step is
not replicated to the replica.

## Sequences

The state of sequences added to replication sets is replicated periodically
and not in real-time. Dynamic buffer is used for the value being replicated so
that the subscribers actually receive future state of the sequence. This
minimizes the chance of subscriber's notion of sequence's `last_value` falling
behind but does not completely eliminate the possibility.

It might be desirable to call `synchronize_sequence` to ensure all subscribers
have up to date information about given sequence after "big events" in the
database such as data loading or during the online upgrade.

It's generally recommended to use `bigserial` and `bigint` types for sequences
on multi-node systems as smaller sequences might reach end of the sequence
space fast.

Nodes that insert into the same table can give each node its own blocks of a
sequence's values with [sequence ranges](../sequence-ranges/).

Users who want to have independent sequences on provider and subscriber can
also avoid adding sequences to replication sets and create sequences with step
interval equal to or greater than the number of nodes. And then setting a
different offset on each node. Use the `INCREMENT BY` option for
`CREATE SEQUENCE` or `ALTER SEQUENCE`, and use `setval(...)` to set the start
point.

## Triggers

Apply process and the initial COPY process both run with
`session_replication_role` set to `replica` which means that `ENABLE REPLICA`
and `ENABLE ALWAYS` triggers will be fired.

## PostgreSQL Version differences

worf can replicate across PostgreSQL major versions. Despite that, long
term cross-version replication is not considered a design target, though it may
often work. Issues where changes are valid on the provider but not on the
subscriber are more likely to arise when replicating across versions.

It is safer to replicate from an old version to a newer version since PostgreSQL
maintains solid backward compatibility but only limited forward compatibility.
Initial schema synchronization is only supported when replicating between same
version of PostgreSQL or from lower version to higher version.

Replicating between different minor versions makes no difference at all.

## Database encoding differences

worf does not support replication between databases with different
encoding. We recommend using `UTF-8` encoding in all replicated databases.

## Large objects

PostgreSQL's logical decoding facility does not support decoding changes
to large objects, so worf cannot replicate large objects.
