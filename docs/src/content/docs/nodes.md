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

`DROP EXTENSION worf` is refused while other nodes subscribe to this one, since
their replication reads worf's tables. Drop those subscriptions on the
subscribers first, which drops their replication slots here. Or drop the slots
with `pg_drop_replication_slot`, which throws away the changes those
subscribers haven't received yet.

After worf is dropped, each sequence that used sequence ranges stays limited to
the block of numbers this node was using, so it can't hand out a number another
node may have. worf logs a warning for each one. Once that block is used up,
`nextval` fails. When no other node uses the sequence anymore, widen its limits
with `ALTER SEQUENCE`.

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
