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

## Deferrable constraints

PostgreSQL normally skips the recheck of deferrable unique and exclusion
constraints for rows that apply writes. worf makes it run, so these constraints
are checked for replicated rows just as for local ones:

- A constraint that is initially deferred is checked at commit, and a
  violation fails the whole incoming transaction.
- A constraint that is initially immediate is checked as each change ends. If a
  local transaction is writing a conflicting row, apply waits for it.

`worf.exception_behaviour` handles these failures like any other. A table's
initial copy is checked against these constraints once it has copied.

## DDL

With `worf.enable_ddl_replication` on, DDL replicates as it runs, and
`worf.include_ddl_repset` adds the tables it creates to replication sets.
Otherwise, it's up to you to keep the provider and subscriber databases
compatible. `worf.replicate_ddl_command` runs DDL on the provider and
subscribers at a consistent point.

A replicated `ALTER TABLE ... ADD COLUMN` with a volatile default, such as
`now()` or `gen_random_uuid()`, fills existing rows with different values on
each node.

With `worf.global_ddl_lock`, DDL that needs the lock fails at once with SQLSTATE
`55P03` if an earlier statement in the same transaction holds a lock that the
other nodes' changes need. For example, the earlier statement ran `LOCK TABLE`
or `TRUNCATE`, or updated a row that another node also changed. Run such DDL
first in its transaction, or in a transaction of its own. If a node that is
blocking writes for DDL dies before the DDL commits, the other nodes notice
within about a minute.

`worf.replicate_ddl_command` doesn't take the global DDL lock, so the DDL it
queues applies on each node wherever it falls among that node's own writes.

DDL that uses temporary objects runs on its own node only, without a notice,
or fails if it also creates or changes permanent objects; see
`worf.enable_ddl_replication`. worf misses one case: a temporary type named
without a schema before a string constant, as in `tmp_type '1'`, inside an
expression the statement doesn't keep, such as `ALTER TABLE ... USING`. Such a
statement replicates. Write the type as `pg_temp.tmp_type`, or write the cast
with `::`.

## Schema changes without the global DDL lock

Without `worf.global_ddl_lock`, worf doesn't stop writes on the other nodes
while DDL replicates. If transactions that aren't replicated yet no longer fit
the changed table, replication stops.

Either stop writes to the provider before changing a table, or use
`worf.replicate_ddl_command` so the change applies at a consistent point on the
subscribers. When every node writes, that isn't enough, since another node may
write with the old structure after the change commits. Then use
`worf.enable_ddl_replication` with `worf.global_ddl_lock`, or stop writes on
every node and let all slots catch up before changing tables.

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

A sequence's state goes to the subscriber's sequence with the same schema and
name. If the subscriber has no such sequence, for example because it was
renamed on only one node, apply stops with an error until it exists. To go on
without it, remove the sequence from its replication sets on the provider with
`worf.replication_set_remove_sequence`. Apply then skips it, with a warning.

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

Apply and the initial copy both run with `session_replication_role` set to
`replica`, so `ENABLE REPLICA` and `ENABLE ALWAYS` triggers fire.

They run with the `search_path` set for the database and role, so a trigger
that names a function or table without a schema finds the same one a local
session would. Row filters run with the `search_path` of the provider's
database and role. worf's own queries search only `pg_catalog`, then temporary
objects.

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
