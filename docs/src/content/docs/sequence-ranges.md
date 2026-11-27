---
title: Sequence ranges
description: Give each node its own blocks of a sequence's numbers, so rows inserted on different nodes never get the same key.
---

When several nodes insert into the same table, their keys must not collide. With sequence ranges, each node takes
its numbers from blocks that no other node uses. Tables, column types and `nextval` stay as they are.

## How it works

A node takes its numbers from **blocks**: ranges of numbers, such as 1 to 10,000, that only that node uses.

One node, the **allocator**, keeps track of which numbers are still free and hands out the blocks. You don't need to
remember which node it is: `worf.sequence_ranges` shows it, and a function that has to run there tells you which node
to run it on.

Every node draws from its **active block** and keeps a **next block** ready. When the active block runs low, worf
switches to the next block and asks for another. Every node except the allocator also keeps a **reserve**: a larger
block of its own that it takes new blocks from when it can't reach the other nodes, so it keeps taking inserts while
it's cut off.

Nodes talk to each other over the connections their subscriptions use. A node that isn't connected to the allocator
asks its neighbor closest to the allocator, which hands out blocks from a pool of numbers it gets from the allocator.

Each database with managed sequences runs two background workers: a range worker, which switches blocks and asks
for new ones, and an exchange worker, which talks to the other nodes. A short-lived switch worker may also run for a
sequence at a block's end. Leave room for them in `max_worker_processes`.

## Setting up

On any one node, name every other node that inserts with the sequence. That node becomes the allocator.

```sql
SELECT worf.enable_sequence_range('inventory_id_seq', participants := '{node_beta}');
```

- Each node needs a subscription to or from another node, since that is how they reach each other.
- The user in each provider connection string needs `EXECUTE` on `worf.sequence_range_exchange`, which `PUBLIC`
  doesn't have. Give each subscriber node its own user: a provider tells nodes apart by user, and a user that isn't a
  superuser speaks for one node only. To let a user speak for a different node, delete its row in
  `worf.sequence_range_role`.
- Grant that `EXECUTE` only to the users of nodes you trust with the sequence's numbers. See [Trust](#trust).

The allocator starts handing out blocks once every other node has reported in. Blocks start above every number any node
has already used. Until a node has its first block, its `nextval` fails with SQLSTATE `2200H` ("reached maximum
value", or "reached minimum value" for a descending sequence), because worf parks the sequence at the edge of its type
until then. A node that joins later with rows whose
keys are already in the handed-out range is refused, and its state shows `refused`.

To call the functions below, a role that isn't a superuser needs `USAGE` on schema `worf` and the privileges of the
role that owns the sequence, as for `ALTER SEQUENCE`. To read the views it needs `SELECT` on them.

## Checking on it

```sql
SELECT * FROM worf.sequence_ranges;
```

shows each sequence's allocator, its state, its active and next blocks, how many numbers are left, and which nodes
it waits for, if any. `worf.sequence_range_grants` lists the blocks this node handed out.

## Functions

| Function | Run on | What it does |
| --- | --- | --- |
| `worf.enable_sequence_range` | Any one node, which becomes the allocator | Puts the sequence under range management. Without `participants`, it waits for `activate_sequence_range`. `pool_start` sets where the handed-out numbers start. |
| `worf.activate_sequence_range` | The allocator | Starts handing out blocks without waiting for the nodes that haven't reported yet. With `node_name`, stops waiting for that one node, as for a node that is gone for good. |
| `worf.alter_sequence_range` | The allocator | Changes the block sizes, each at least 2. The other nodes pick them up at their next exchange. |
| `worf.disable_sequence_range` | The allocator | Stops handing out blocks. Nodes keep using the blocks they have. |
| `worf.reset_sequence_range` | Any node | Gives up this node's blocks and asks for new ones; without a sequence, for every managed sequence. |
| `worf.reserve_sequence_range` | Any node | Makes sure the active block has `count` numbers left, plus 5% for other sessions, before a bulk load. The block is handed out whole, up to a sixteenth of the values the allocator has left. |
| `worf.reserve_sequence_values` | Any node | Gives this node exactly the numbers from `lo` to `hi`, for rows that already carry those keys. |

```sql
worf.enable_sequence_range(sequence regclass, participants text[] DEFAULT NULL, range_size bigint DEFAULT NULL,
    max_range_size bigint DEFAULT NULL, pool_start bigint DEFAULT NULL, force boolean DEFAULT false)
worf.activate_sequence_range(sequence regclass, node_name name DEFAULT NULL)
worf.alter_sequence_range(sequence regclass, range_size bigint DEFAULT NULL, max_range_size bigint DEFAULT NULL)
worf.disable_sequence_range(sequence regclass)
worf.reset_sequence_range(sequence regclass DEFAULT NULL)
worf.reserve_sequence_range(sequence regclass, count bigint, timeout interval DEFAULT '1 minute')
worf.reserve_sequence_values(sequence regclass, lo bigint, hi bigint, timeout interval DEFAULT '1 minute')
```

## Block sizes

A node's first block holds `range_size` numbers: by default 100 for `smallint`, 10,000 for `integer` and 1,000,000
for `bigint`. If a block lasted less than an hour, the node asks for twice as many next time, up to
`max_range_size`. If it lasted more than a day, it asks for half as many. A node also asks for at least two minutes'
worth at its peak insert rate.

A node's reserve holds sixteen of its largest blocks, or 1/256 of the numbers the sequence's type can hold,
whichever is smaller. A block taken from the reserve has the largest size, or what the reserve has left if that's
less. A node asks for a new reserve once less than half of its current one is left. The allocator takes its own
blocks straight from the free numbers, so it needs no reserve.

## Working without the other nodes

A node keeps taking inserts while it can't reach the other nodes. It waits for them for up to
`worf.sequence_range_offline_wait`, 10 seconds by default, and then goes on without them:

- A node takes its next block from its reserve when the node it asks hasn't answered. That includes blocks for
  `reserve_sequence_range`, so bulk loads work offline too, as long as they fit in what is left of the reserve.
- The allocator waits once for another node to hear of a block it gave itself, and then uses its own blocks without
  waiting until another node answers.

When the nodes reach each other again, they exchange what they hold and carry on. If a node's reserve has nothing
left while it's cut off, `worf.sequence_ranges` says so, and statements at the end of its last block fail at once
with SQLSTATE `2200H` instead of waiting.

A node that restarted is different: it might have been restored from a backup, so it draws nothing, and takes nothing
from its reserve, until the other nodes it exchanges with have answered it. See
[Restores and failover](#restores-and-failover).

## When a block runs low

The range worker switches to the next block when the active one has about a minute's worth left at the peak rate,
or a twentieth of the block if that's more. Whatever is left in the old block goes unused.

Statements never run past a block while the next one can still come:

- From a block's last twentieth, statements wait for the switch before they draw more. A statement that draws several
  numbers a row, through several `nextval` calls or through triggers, waits that much earlier. When worf can't tell
  from a trigger's code how many numbers it draws (a loop, `EXECUTE`, or a call to one of your own functions), it
  allows half a block per row until it has seen such a row.
- Inserts, `COPY FROM`, `INSERT … ON CONFLICT` and `MERGE` check between rows. `SELECT nextval(...)` checks before it
  draws and as its rows go on.
- Statements that run at the same time leave each other enough numbers.
- A statement waits for a block for up to twice `worf.sequence_range_timeout` plus `worf.sequence_range_offline_wait`,
  so a node that can't reach the others has time to take one from its reserve. `statement_timeout` also ends the
  wait.

If a transaction uses up the active block and no other transaction is drawing from the sequence, it switches to the
next block itself, and other sessions' inserts wait for it to end. Such a transaction can't be prepared.

A transaction that drew from a block and stays open delays the range worker's switch until it ends. In the block's
last twentieth, a switch worker switches around it instead.

While they draw, inserts and `COPY FROM` hold a lock on the sequence (an advisory lock that worf calls its switch
gate) and let it go between rows when a switch is waiting. A statement that holds it while it waits for something
else, such as `COPY FROM STDIN` waiting for its client, holds the switch up until it continues. If such waits form a
cycle, PostgreSQL ends one of the statements with SQLSTATE `40P01`.

Other draws, such as those of `SELECT nextval(...)` or of a PL/pgSQL assignment in a loop, take the gate only when
they have to check the block, as a statement's first draw does, or the first by a PL/pgSQL expression in a
transaction. The draws between hold nothing, and a switch waits only for the ones under way. They cost about what
draws from a plain sequence do, and the statements around them cost nothing extra. A cursor between fetches, a client
that reads slowly and a transaction left idle hold nothing, so they don't delay a switch.

worf can't always reach a statement's calls one by one, for example where another extension uses PostgreSQL's hook
for function calls. Such statements check before each row instead and hold the gate while they work out a row. Where the next row draws
straight away, they keep the gate and let it go when a switch waits for it. They look for a waiting switch at each
row, or every tenth of a millisecond when rows take less.

worf doesn't see C code that calls `nextval` directly rather than through SQL. Such code can run a block out, so don't
use it with managed sequences.

## Bulk loads

Before a large load, reserve the numbers it needs, before the load's transaction first calls `nextval`:

```sql
BEGIN;
SELECT worf.reserve_sequence_range('inventory_id_seq', 500000);
-- the load
COMMIT;
```

Other sessions keep drawing from the same block. If they leave the load short, it carries on in the next block. One
load may take at most a sixteenth of the values the allocator has left; a larger reservation fails at once with
SQLSTATE `22023`.

To load rows that already have keys, reserve exactly those keys with `worf.reserve_sequence_values` on the node that
loads them. The allocator refuses keys that may already be in use.

## The guard triggers

worf adds two triggers to each table with a column that takes its default from a managed sequence. They check each
row a local session writes: a new row's value, and a value an update changes. A value is allowed only if it's in one
of this node's blocks and the sequence has already gone past it. Anything else fails the statement with SQLSTATE
`23514`. Rows written with `session_replication_role` set to `replica`, as worf's apply and sync workers write the
rows they bring from other nodes, aren't checked. Setting it takes a superuser or `GRANT SET ON PARAMETER`. The
triggers can't be disabled.

The column must be `smallint`, `integer`, `bigint` or `numeric`, and its default, or its domain's, must be `nextval`
of the sequence, alone or cast to the column's type. worf refuses other defaults that use the sequence, such as
`nextval('s') + 1000000`. A default that calls a function which calls `nextval` leaves the table unchecked.

A change that brings existing rows under the triggers, such as setting a default, attaching a partition,
`ALTER SEQUENCE ... OWNED BY` or changing a column's type, fails with `23514` if a row holds a value the triggers
would refuse. An index or `CHECK` constraint on the column lets worf check without reading every row.

To restore rows that other nodes wrote, or rows from before range management, as `pg_restore` does, set
`worf.sequence_range_restore`. That takes a superuser or `GRANT SET ON PARAMETER`. The checks then refuse only values
this node's sequence may still hand out.

## Moving the allocator

Run `worf.disable_sequence_range` on the allocator, then `worf.enable_sequence_range` on the node that should take
over, naming every other node. If the old allocator is gone for good, pass `force := true`. The new allocator's numbers
start above every block the old one handed out, and nodes keep using their blocks until new ones arrive.

Dropping a managed sequence, or its table, ends range management for it on that node. Creating it again on another
node joins the sequence's current range again. On the allocator, run `worf.enable_sequence_range` to start a new range
above the old numbers.

## Restores and failover

A node restored from a backup may hand out again numbers it had already used after the backup was taken. worf
guards against this where it can:

- A node whose identity changes, as after a promotion, `pg_upgrade` or point-in-time recovery, gives up its blocks
  and asks for new ones. An allocator whose identity changes also stops handing out blocks until the other nodes report
  what they hold, or until `worf.activate_sequence_range` lets it go on. `worf.sequence_ranges` names the nodes it
  waits for.
- After a restore that keeps the same identity, the other nodes see that the node went back in time, from the WAL
  position it reports and from its last 32 starts, which it keeps in the file `worf_starts` in its data directory.
  They tell it to reset, and take nothing from it until it has. If `worf_starts` is missing, the node's neighbors treat
  every earlier position as gone back.
- After a restart, a node draws nothing from the blocks it held until the other nodes it exchanges with have answered
  it, so they can tell it whether it went back. Each statement that draws waits up to twice
  `worf.sequence_range_timeout`, and then fails with SQLSTATE `55000` naming the node it waits for. Blocks it gets
  after the restart, as `worf.reset_sequence_range()` brings, it draws from at once. `worf.sequence_range_restart_wait` lets it draw after a set time
  instead, at the risk of duplicate keys after a restore.

- A node tells its other neighbors about each new neighbor, such as a node that starts subscribing to it, and draws
  only once one of them has heard of it. A node restored from a backup taken before then learns of the new neighbor
  from them, and waits for it too.

worf can't catch everything, for example a node whose only neighbor is the one it took on after the backup. So after
restoring a node from a backup, run `worf.reset_sequence_range()` on it before it takes writes. A
synchronous standby for the allocator keeps a crash from losing the record of a block it handed out.

A node that `worf_create_subscriber` makes from a base backup gives up its source's blocks. Its copied rows overlap
the handed-out range, so it is refused until the allocator runs `worf.disable_sequence_range` and then
`worf.enable_sequence_range`, naming the new node.

## Trust

A subscriber's user can send made-up messages to its provider, and no node can check the reports a neighbor passes on
for other nodes. At worst, a user with `EXECUTE` on `worf.sequence_range_exchange` can:

- hold a sequence up, for example by keeping the allocator from starting or a node from finishing its wait after a
  reset;
- make a node give up its blocks: once in ten minutes for each neighbor, and, for the node's grantor, once in ten
  seconds at first, both less often if it keeps asking;
- make a later range start above a made-up value;
- take numbers, since a made-up request can't be told from a real one. Every ten minutes, that's up to two pools of
  sixteen times `max_range_size` for the node and for each node behind it, and loads of up to a sixteenth of the
  values the allocator has left.

Grant `EXECUTE` on `worf.sequence_range_exchange` only to the users of nodes you trust with the sequence's numbers.

## Snowflake IDs

If you can change the table, a `bigint` column can use Snowflake IDs instead. They need no blocks:

```sql
-- On one node, naming the others
SELECT worf.enable_snowflake(participants := '{node_beta}');

CREATE TABLE events (id bigint PRIMARY KEY DEFAULT worf.snowflake_id(), payload jsonb);
```

An ID holds the milliseconds since 2025 in its top 41 bits, the node's number in the next 10 and a counter in the
last 12. So up to 1,024 nodes can each issue up to 4,096 IDs a millisecond, and IDs sort by time.
`worf.snowflake_node()` shows this node's number. A node gets its number from the allocator the same way it gets
blocks, so no two nodes hold the same number at once. A node that hasn't heard from the allocator for 12 hours gives up
its number, because the allocator may give it to another node, and takes a new one when it hears from the allocator
again. A node's IDs
only grow, even if its clock goes back across a restart.

## Restrictions

Managed sequences can't cycle, be temporary or unlogged, or be in a replication set.
`replication_set_add_all_sequences` leaves them out. worf rejects:

- `ALTER SEQUENCE` on them, apart from `OWNED BY` and `OWNER TO`;
- renaming their schema, and moving or unlogging a table that owns one;
- `GRANT UPDATE` or `GRANT ALL` on them;
- changing the default or identity of a column that uses one, apart from giving a column a default of `nextval` of the
  sequence it belongs to;
- dropping, renaming or disabling the guard triggers;
- `TRUNCATE ... RESTART IDENTITY` that would restart one;
- `PREPARE TRANSACTION` for a transaction that switched one itself or ran one of the functions above, since a prepared
  transaction could hold the sequence across restarts.

worf skips a `CREATE TRIGGER` of a guard trigger, as a restored dump holds, since it creates the triggers itself. A
column that uses a managed sequence can change its type, as long as the new type holds the node's blocks, or be
dropped.

Superusers and members of `pg_write_all_data` can still call `setval`. When a sequence goes back, worf notices and
leaves the block. But if `setval` moves it back past both the last position worf checked and the rows in an indexed
column, the sequence hands out those numbers again.

Dropping worf leaves each managed sequence with its block's bounds, so that it hands out no number another node may
hold. Once that block is used up, `nextval` fails. Widen the sequence's bounds with `ALTER SEQUENCE` once no other node
uses its numbers. While the drop waits for its tables, an insert that uses a managed sequence fails with SQLSTATE
`55006`, and one waiting for a block gives up, so new inserts don't hold the drop up. The drop does wait for a
transaction that already holds one of worf's tables, such as one that read them or reserved a block. That
transaction's inserts go on, and the drop runs once it ends.
