---
title: Node management
description: Create, drop and alter nodes and their interfaces.
---

Nodes can be added and removed dynamically using the SQL interfaces.

- `worf.create_node(node_name name, dsn text)`
  Creates a node.

  Parameters:
  - `node_name` - name of the new node, only one node is allowed per database
  - `dsn` - connection string to the node, for nodes that are supposed to be
    providers, this should be reachable from outside

- `worf.drop_node(node_name name, ifexists bool)`
  Drops the worf node.

  Parameters:
  - `node_name` - name of an existing node
  - `ifexists` - if true, error is not thrown when subscription does not exist,
    default is false

A node keeps worf while it provides subscriptions: `DROP EXTENSION worf` is
refused while worf's replication slots in the database stream its changes to
subscribers, whose walsenders read worf's tables to decode them, the changes
made before the drop too. Drop those subscriptions on their subscribers first,
which drops their slots, or drop the slots with `pg_drop_replication_slot`,
which gives up the changes their subscribers haven't received.

Each sequence whose values worf's sequence ranges managed keeps, after worf is
dropped, the bounds of the block the node drew from, with a warning naming
them, so that it issues no value another node may hold: once the block is used
up, its `nextval` fails. Widen its bounds with `ALTER SEQUENCE` once no other
node draws from the sequence's values. The drop takes the tables it needs, worf's
and those whose triggers it drops, all at once, waiting for a transaction that
holds one with none of them held.

- `worf.alter_node_add_interface(node_name name, interface_name name, dsn text)`
  Adds additional interface to a node.

  When node is created, the interface for it is also created with the `dsn`
  specified in the `create_node` and with the same name as the node. This
  interface allows adding alternative interfaces with different connection
  strings to an existing node.

  Parameters:
  - `node_name` - name of an existing node
  - `interface_name` - name of a new interface to be added
  - `dsn` - connection string to the node used for the new interface

- `worf.alter_node_drop_interface(node_name name, interface_name name)`
  Remove existing interface from a node.

  Parameters:
  - `node_name` - name of and existing node
  - `interface_name` - name of an existing interface
