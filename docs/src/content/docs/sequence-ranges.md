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

The other nodes learn of the sequence over their subscriptions, so each node needs a subscription to or from another node in the topology. Once every participant has reported, the owner starts granting blocks above every value any node has issued or stored. Until a node gets its first block, its `nextval` fails with SQLSTATE `2200H`.

```sql
SELECT * FROM worf.sequence_ranges;
```

shows each sequence's state, its active and next blocks, and the values left. `worf.sequence_range_grants` lists the blocks this node granted.

| Function | Runs on | Does |
| --- | --- | --- |
| `worf.enable_sequence_range(sequence regclass, participants text[] DEFAULT NULL, range_size bigint DEFAULT NULL, max_range_size bigint DEFAULT NULL, pool_start bigint DEFAULT NULL, force boolean DEFAULT false)` | Owner | Puts the sequence under range management. With `participants` `NULL`, it waits for `activate_sequence_range`. `pool_start` raises where the values start |
| `worf.activate_sequence_range(sequence regclass)` | Owner | Starts granting with the nodes that have reported so far, or lifts a fence |
| `worf.alter_sequence_range(sequence regclass, range_size bigint DEFAULT NULL, max_range_size bigint DEFAULT NULL)` | Owner | Changes the block sizes |
| `worf.disable_sequence_range(sequence regclass)` | Owner | Stops granting. Each node keeps using the blocks it holds |
| `worf.reset_sequence_range(sequence regclass DEFAULT NULL)` | Any node | Abandons this node's blocks and requests new ones |
| `worf.reserve_sequence_range(sequence regclass, count bigint, timeout interval DEFAULT '1 minute')` | Any node | Returns once the active block has at least `count` values left |
| `worf.reserve_sequence_values(sequence regclass, lo bigint, hi bigint, timeout interval DEFAULT '1 minute')` | Any node | Gives this node exactly `[lo, hi]`, for rows that already carry those keys |

A node's first block has `range_size` values: 100 for `smallint`, 10,000 for `integer` and 1,000,000 for `bigint`. A node whose block lasted under an hour asks for twice as many next time, up to `max_range_size`, and one whose block lasted over a day asks for half.

## How it works

The owner holds the root, the values not yet granted. Each node draws from its active block and keeps a next block ready. The range worker switches to the next block when the active one runs low, by setting the sequence's `MINVALUE` and `MAXVALUE` to the block, and asks for another. Nodes reach the owner over their subscriptions: a node asks its neighbor nearest the owner, and a node that serves others holds pool blocks to grant from.

A trigger on each table that uses the sequence checks values written by hand. A value passes when it is in one of this node's blocks and the sequence has passed it, or when a row with it already exists. Any other value fails with SQLSTATE `23514`. Apply and sync workers write other nodes' rows unchecked.

A single transaction that needs more values than the active block has left fails at the block's end. Before a large load, reserve enough values:

```sql
SELECT worf.reserve_sequence_range('inventory_id_seq', 500000);
```

To load rows that already carry keys, reserve exactly those keys on the node that loads them. The owner refuses keys that may already be in use.

## Moving the owner

Run `disable_sequence_range` on the old owner, then `enable_sequence_range` on the new one, naming every participant. The new owner's values start above every block the old owner granted, and nodes keep using their blocks until new ones arrive. If the old owner is gone for good, pass `force := true`.

## Restores and failover

A node whose node id, system identifier or timeline changes, as after a promotion, `pg_upgrade` or point-in-time recovery, abandons its blocks and asks for new ones. Its neighbors also notice when its WAL position moves backwards, as after a restore from a snapshot, and tell it to reset. They notice only at their next exchange, so keep writes away from a restored node until `reset_sequence_range` has run on it.

An owner or pool holder whose timeline changes may have lost grants. It grants nothing until the nodes it granted to report the blocks they hold. A synchronous standby for each of them prevents the loss.

## Restrictions

Managed sequences can't cycle, be temporary or unlogged, or be in a replication set. worf rejects `ALTER SEQUENCE` on them, `GRANT UPDATE` on them, changing the default or identity of a column that uses one, dropping or disabling the trigger, and `TRUNCATE ... RESTART IDENTITY`. Superusers and members of `pg_write_all_data` can still call `setval`.
