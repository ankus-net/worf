---
title: Additional functions
description: Replicate DDL and wait for replication.
---

- `worf.replicate_ddl_command(command text, replication_sets text[])`
  Execute locally and then send the specified command to the replication queue
  for execution on subscribers which are subscribed to one of the specified
  `replication_sets`.

  Parameters:
  - `command` - DDL query to execute
  - `replication_sets` - array of replication sets which this command should be
    associated with, default "{ddl_sql}"

- `worf.release_fence(fence_id bigint)`
  Lets local writes and DDL through the fence that `worf.global_ddl_lock` put
  up for DDL from another node, or through every fence in this database when
  `fence_id` is NULL, as when that DDL won't reach this node because its node
  is gone for good. Returns the number of tables let through.

- `worf.synchronize_sequence(relation regclass)`
  Push sequence state to all subscribers. Unlike the subscription and table
  synchronization function, this function should be run on provider. It forces
  update of the tracked sequence state which will be consumed by all
  subscribers (replication set filtering still applies) once they replicate the
  transaction in which this function has been executed.

  Parameters:
  - `relation` - name of existing sequence, optionally qualified
