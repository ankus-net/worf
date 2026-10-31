---
title: Sequence ranges
description: Give each node blocks of a sequence's values, so rows inserted on different nodes never share a key.
---

Nodes that all insert into the same table need keys that never collide. Sequence ranges give each node blocks of a sequence's values that no other node uses, with no change to the tables or the sequence's type. `nextval` and column defaults keep working as before.

## Enabling

On the node that will own the sequence's values, name every other node that issues values from it:

```sql
SELECT worf.enable_sequence_range('inventory_id_seq', participants := '{node_beta}');
```

The other nodes learn of the sequence over their subscriptions, so each node needs a subscription to or from another node in the topology. The user in each provider connection string needs `EXECUTE` on `worf.sequence_range_exchange`, which `PUBLIC` doesn't have. A provider tells the nodes that call it apart by these users alone, so give each subscriber node its own: a user that isn't a superuser speaks for one node only. Once every participant has reported, the owner starts granting blocks above every value any node has issued or stored. Until a node gets its first block, its `nextval` fails with SQLSTATE `2200H`. A node that joins later, with values that already reach into the granted range, is refused: its state shows `refused` and its `nextval` keeps failing.

```sql
SELECT * FROM worf.sequence_ranges;
```

shows each sequence's state, its active and next blocks, and the values left. `worf.sequence_range_grants` lists the blocks this node granted. A role that isn't a superuser needs `USAGE` on schema `worf` to call the functions below, and `SELECT` to read these views.

| Function | Runs on | Does |
| --- | --- | --- |
| `worf.enable_sequence_range(sequence regclass, participants text[] DEFAULT NULL, range_size bigint DEFAULT NULL, max_range_size bigint DEFAULT NULL, pool_start bigint DEFAULT NULL, force boolean DEFAULT false)` | Owner | Puts the sequence under range management. With `participants` `NULL`, it waits for `activate_sequence_range`. `pool_start` raises where the values start |
| `worf.activate_sequence_range(sequence regclass)` | Owner | Starts granting with the nodes that have reported so far, or lifts a fence |
| `worf.alter_sequence_range(sequence regclass, range_size bigint DEFAULT NULL, max_range_size bigint DEFAULT NULL)` | Owner | Changes the block sizes, each at least 2 |
| `worf.disable_sequence_range(sequence regclass)` | Owner | Stops granting. Each node keeps using the blocks it holds |
| `worf.reset_sequence_range(sequence regclass DEFAULT NULL)` | Any node | Abandons this node's blocks and requests new ones. An owner or pool holder grants nothing until the nodes it granted to report what they hold |
| `worf.reserve_sequence_range(sequence regclass, count bigint, timeout interval DEFAULT '1 minute')` | Any node | Returns once the active block has at least `count` values left |
| `worf.reserve_sequence_values(sequence regclass, lo bigint, hi bigint, timeout interval DEFAULT '1 minute')` | Any node | Gives this node exactly `[lo, hi]`, for rows that already carry those keys |

A node's first block has `range_size` values: 100 for `smallint`, 10,000 for `integer` and 1,000,000 for `bigint`. A node whose block lasted under an hour asks for twice as many next time, up to `max_range_size`, and one whose block lasted over a day asks for half.

## How it works

The owner holds the root, the values not yet granted. Each node draws from its active block and keeps a next block ready. Each database that manages a sequence runs a range worker and an exchange worker, which take two of `max_worker_processes`; a database that only subscribes runs the exchange worker briefly every `worf.sequence_range_exchange_interval` to ask its providers whether they manage any. The range worker switches to the next block when the active one runs low, by setting the sequence's `MINVALUE` and `MAXVALUE` to the block, and asks for another. It also leaves the block when it finds the sequence behind where it was at its last check, or behind a value a row holds in the block, in a column that uses the sequence and leads an index, as after a superuser's `setval`. The exchange worker makes the calls to other nodes, so a node that never answers holds up no switch, and the range worker cancels a call after `worf.sequence_range_timeout`. Idle, neither worker writes anything, so a node uses no transaction IDs, except once an hour on a node that holds a Snowflake node number. Nodes reach the owner over their subscriptions: a node asks its neighbor nearest the owner that still exchanges with it, and a node that serves others holds pool blocks to grant from, asking for more before they run out. The owner switches to a block it granted itself only once a neighbor has heard of it, or at once when no neighbor is in touch, so an owner that only answers calls may wait up to an exchange interval for its next block. Each node keeps the reports of up to 1,024 other nodes for a sequence. No node can check the reports a neighbor relays for others, so a subscriber's role can make up reports of other nodes, up to that limit; at worst they hold the sequence up, as by keeping its root from starting or a fence from lifting, or use its root up. The owner logs a warning once the root has granted nine tenths of its values.

Two triggers on each table that uses the sequence check every row a local session writes, after any `BEFORE` trigger has changed it: a new row's value, and a value an update changes. A value passes when it is in one of this node's blocks and the sequence has passed it. Any other value fails the statement with SQLSTATE `23514`. Apply and sync workers write other nodes' rows unchecked, and the DDL they apply checks rows as a restore does, below. A partitioned table's triggers check every partition, in the columns the table numbers from the sequence. A partition whose own default numbers another column from it, or that of a partitioned table between, has triggers of its own for that column, so attaching it holds up no writers to the rest of the tree. An `UPDATE`, or a `MERGE` without an `INSERT` action, that moves a row to another partition is checked as an update, so the row keeps its value, except in a partition's own column, which checks the moved row as a new one, as a `MERGE` with an `INSERT` action does. The column must be `smallint`, `integer`, `bigint` or `numeric`, in a table or a partitioned table, and its default, or its domain's when it has none of its own, `nextval` of the sequence, alone or cast to the column's type; worf refuses any other default that uses it, such as `nextval('s') + 1000000`, whose values would always fail. worf finds the columns that use a sequence by the dependencies PostgreSQL records, so a default that reaches it through a function, or through a name looked up as it runs, as `nextval('s'::text)` does, leaves its table unguarded.

DDL that brings rows under the triggers, by setting a column's default to the sequence, attaching a partition to a guarded table, `ALTER SEQUENCE ... OWNED BY` or a column's type change, fails with SQLSTATE `23514` if a row holds a value the triggers would refuse, other than one from before range management started or a `numeric` value that isn't a whole `bigint`. An index on the column, or the table's constraints, as a partition's bound, spare the check from reading every row.

To restore rows that other nodes wrote, or that predate range management, as `pg_restore` of a table does, set `worf.sequence_range_restore`, which needs a superuser or `GRANT SET ON PARAMETER`. The triggers and the DDL then refuse only the values a sequence may still issue as far as this node knows: ahead of this node's sequence in its blocks, not yet granted to any node, or past every grant. A node can't tell the values another node has issued from those it has yet to issue, so a restored row may hold a key that node issues again.

A single transaction that needs more values than the active block has left fails at the block's end. Before a large load, reserve enough values, before the transaction calls `nextval` on the sequence:

```sql
SELECT worf.reserve_sequence_range('inventory_id_seq', 500000);
```

To load rows that already carry keys, or bring them under the triggers, reserve exactly those keys on the node that loads them. The owner refuses keys that may already be in use. Both reservations need the privileges of the sequence's owner.

A transaction that has called `nextval`, or written a value the guard checked against the active block, and stays open holds off the switch to the next block until it ends.

## Moving the owner

Run `disable_sequence_range` on the old owner, then `enable_sequence_range` on the new one, naming every participant. The new owner's values start above every block the old owner granted, and nodes keep using their blocks until new ones arrive. If the old owner is gone for good, pass `force := true`.

## Restores and failover

A node whose node id, system identifier or timeline changes, as after a promotion, `pg_upgrade` or point-in-time recovery, abandons its blocks and asks for new ones. An owner or pool holder also fences its root and pool blocks: it grants nothing until the nodes it granted to, and its neighbors, report the blocks they hold. It takes back into its ledger any grant it lost. The owner then also gives up its free values up to the highest value any node has seen granted.

A restore from a backup or a snapshot keeps the timeline. The node's neighbors notice its WAL position moving backwards, behind where it was at an exchange or behind its changes they applied, or its granting values they already hold, and tell it to reset, which does the same as a change of identity. Until it has, they take no grant from it and answer none of its requests. An owner or pool holder also resets on its own once a neighbor holds a block it granted and has no record of, and an owner once a neighbor has heard of grants beyond the last it knows of. They notice only at their next exchange, and not at all once the node has written past where they saw its WAL position or applied its changes to, so a restored node may issue values it issued before. Keep writes away from a restored node until `reset_sequence_range` has run on it.

A node that `worf_create_subscriber` makes from a base backup gives up its source's blocks and stops its managed sequences before it takes a node id of its own. The rows it copied reach into the root, so it is refused until the sequence moves to a new epoch: run `disable_sequence_range` and then `enable_sequence_range` on the owner, naming the new node among the participants.

An owner or pool holder that loses grants without its neighbors noticing, as when one is restored before any exchange or loses a grant in a crash, may grant those values again. A synchronous standby for each of them prevents a crash from losing a grant.

## Snowflake IDs

Where the schema can change, a `bigint` column can take Snowflake IDs instead, which need no blocks:

```sql
-- On one node, naming the others
SELECT worf.enable_snowflake(participants := '{node_beta}');

CREATE TABLE events (id bigint PRIMARY KEY DEFAULT worf.snowflake_id(), payload jsonb);
```

An ID holds the milliseconds since 2025 in its top 41 bits, the node's number in the next 10 and a counter in the last 12, so up to 1,024 nodes at a time each issue up to 4,096 IDs a millisecond, and the IDs sort by time. Node numbers come from the same allocator as sequence ranges, so no number is held by two nodes, and `worf.snowflake_node()` shows this node's. A node that changes identity takes a new number; `reset_sequence_range()` without a sequence keeps it. Moving the owner of the numbers never runs out of them: once the owner has none left to grant, a number comes back once its holder has reported, a day after the grant, without it, or has reported nothing for a day, and every other node has reported without it, while the numbers of a pool block a node holds to grant on stay its as long as it reports, and it grants none of them once it hasn't heard from the owner for 12 hours; its new holder's IDs start after every time its earlier holders reserved. A node that hasn't heard from the owner of the numbers, directly or through its neighbors, for 12 hours gives up its number, since the owner may give it to another node, and takes a new one once it hears from the owner again. It records hearing from the owner at least once an hour, so that it keeps its number across a restart. A node that sees another report holding its number gives it up too. Each node's IDs only grow, even when its clock goes back across a restart, since the range worker records how far ahead the clock may run.

## Restrictions

Managed sequences can't cycle, be temporary or unlogged, or be in a replication set; `replication_set_add_all_sequences` leaves them out. Except when worf runs it, worf rejects:

- `ALTER SEQUENCE` on them, other than `OWNED BY` and `OWNER TO`, including `RENAME`, `SET SCHEMA` and `SET UNLOGGED`;
- renaming their schema, and moving or unlogging a table that owns one;
- `GRANT UPDATE` or `GRANT ALL` on them, directly or through `ALL SEQUENCES IN SCHEMA`;
- changing the default or identity of a column that uses one, other than giving a column the sequence owns a default of `nextval` of it, and the default of a domain that uses one;
- dropping, renaming or disabling the guard triggers and making them depend on an extension, and, wherever worf is installed, creating other triggers with their names or function or renaming a trigger to or from them;
- `TRUNCATE ... RESTART IDENTITY` that would restart one, including through `CASCADE` or inheritance without `ONLY`.

A `CREATE TRIGGER` of the guard, as a restored dump holds, is skipped, since worf creates the triggers on the tables that need them, and replaces any triggers with their names that predate it. A column that uses a managed sequence can change its type or be dropped, and worf puts the triggers back for the columns that still use one. A type change with `USING`, or from `numeric`, checks the column's rows again.

Superusers and members of `pg_write_all_data` can still call `setval`. The range worker leaves the block only when it finds the sequence behind where it was at its last check or behind a row, as above, so a `setval` back to a value past both makes the sequence issue again the values it issued beyond it that no row in an indexed column holds.
