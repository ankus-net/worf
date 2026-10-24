---
title: Row filtering
description: Replicate only some of a table's rows.
---

worf allows row based filtering both on provider side and the subscriber
side.

## Row Filtering on Provider

On the provider the row filtering can be done by specifying `row_filter`
parameter for the `worf.replication_set_add_table` function. The
`row_filter` is normal PostgreSQL expression which has the same limitations
on what's allowed as the `CHECK` constraint.

Simple `row_filter` would look something like `row_filter := 'id > 0'` which
would ensure that only rows where values of `id` column is bigger than zero
will be replicated.

It's allowed to use volatile function inside `row_filter` but caution must
be exercised with regard to writes as any expression which will do writes
will throw error and stop replication.

It's also worth noting that the `row_filter` is running inside the replication
session so session specific expressions such as `CURRENT_USER` will have
values of the replication session and not the session which did the writes.

## Row Filtering on Subscriber

On the subscriber the row based filtering can be implemented using standard
`BEFORE TRIGGER` mechanism.

It is required to mark any such triggers as either `ENABLE REPLICA` or
`ENABLE ALWAYS` otherwise they will not be executed by the replication
process.
