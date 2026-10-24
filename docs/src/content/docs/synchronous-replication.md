---
title: Synchronous replication
description: Make commits wait for a subscriber.
---

Synchronous replication is supported using same standard mechanism provided
by PostgreSQL for physical replication.

The `synchronous_commit` and `synchronous_standby_names` settings will affect
when `COMMIT` command reports success to client if worf subscription
name is used in `synchronous_standby_names`. Refer to PostgreSQL
documentation for more info about how to configure these two variables.
