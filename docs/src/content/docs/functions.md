---
title: Additional functions
description: Replicate DDL and wait for replication.
---

- `worf.replicate_ddl_command(command text, replication_sets text[])`
  Execute locally and then send the specified command to the replication queue
  for execution on subscribers which are subscribed to one of the specified
  `replication_sets`. The command runs as the role that called the
  function, here and on the other nodes, with an empty search path. It can
  switch roles on the other nodes only as it could here; changing the session
  authorization fails there unless the role is a superuser. Settings it makes
  last until it ends, and temporary objects it creates on the other nodes are
  dropped when it ends. It can't control the transaction: `BEGIN`,
  `COMMIT`, `SAVEPOINT` and the like fail, and a DO block or procedure in it
  runs atomically.

  Parameters:
  - `command` - DDL query to execute
  - `replication_sets` - array of replication sets which this command should be
    associated with, default "{ddl_sql}"

- `worf.release_fence(fence_id bigint)`
  Unblocks local writes and DDL that `worf.global_ddl_lock` blocked for DDL
  from another node, once the transaction commits. Use it when that DDL will
  never arrive, for example because its node is gone for good. With a NULL
  `fence_id`, it unblocks everything in this database. Returns the number of
  tables unblocked.

- `worf.synchronize_sequence(relation regclass)`
  Push sequence state to all subscribers. Unlike the subscription and table
  synchronization function, this function should be run on provider. It forces
  update of the tracked sequence state which will be consumed by all
  subscribers (replication set filtering still applies) once they replicate the
  transaction in which this function has been executed.

  Parameters:
  - `relation` - name of existing sequence, optionally qualified
