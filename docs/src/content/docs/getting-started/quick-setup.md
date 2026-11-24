---
title: Quick setup
description: Configure PostgreSQL, create nodes and subscribe.
---

First the PostgreSQL server has to be properly configured to support logical
decoding:

```ini
wal_level = 'logical'
max_worker_processes = 10   # one per database needed on provider node
                            # one per node needed on subscriber node
max_replication_slots = 10  # one per node needed on provider node
max_wal_senders = 10        # one per node needed on provider node
shared_preload_libraries = 'worf'
```

A subscriber also needs room for its replication origins. From PostgreSQL 18
that's `max_active_replication_origins`; before, it's `max_replication_slots`.
Each subscription uses one, each table it is synchronizing uses one, and so does
each other node whose changes its provider forwards (with `forward_origins`
`{all}`).

When there aren't enough:

- worf frees the origins of forwarded nodes that no subscription is using at
  the moment. A forwarded node keeps its origin for a minute after its last
  change, so nodes that take turns don't make worf create and drop an origin
  for every transaction. Applying gets slower.
- If an apply or table sync still can't get an origin within 10 seconds, it
  stops with an error and retries, as PostgreSQL's own workers do.
- worf also frees forwarded nodes' origins for a PostgreSQL subscription that
  needs one. It checks every 10 seconds, or every twice
  `wal_retrieve_retry_interval` if that's longer. If that subscription still
  doesn't take a free origin, worf stops freeing origins for it for 10 minutes,
  then twice as long each time, up to a day.

Raise the limit if this happens. Other features that use origins need it
raised too.

`worf.origin_writer` records which node and subscription each origin is for,
so conflicts can tell where a row came from. Old records are removed after
`vacuum_freeze_min_age` transactions. A conflict on a row older than that is
treated as coming from the node its origin applies for now.

If you want to handle conflict resolution with last/first update wins (see
[Conflicts](../../conflicts/)), you can add this additional option to
postgresql.conf:

```ini
track_commit_timestamp = on # needed for last/first update wins conflict resolution
```

PostgreSQL 15.19, 16.15, 17.11, 18.6, 19 and later releases load only the
logical decoding output plugins that `output_plugin_libraries` lists, so the
provider must list `worf` there as well:

```ini
output_plugin_libraries = 'pgoutput, test_decoding, worf'
```

`pg_hba.conf` has to allow logical replication connections from
localhost. Logical replication connections are treated by `pg_hba.conf`
as regular connections to the provider database.

Next the `worf` extension has to be installed on all nodes:

```sql
CREATE EXTENSION worf;
```

Now create the provider node:

```sql
SELECT worf.create_node(
    node_name := 'provider1',
    dsn := 'host=providerhost port=5432 dbname=db'
);
```

Add all tables in `public` schema to the `default` replication set.

```sql
SELECT worf.replication_set_add_all_tables('default', ARRAY['public']);
```

Optionally you can also create additional replication sets and add tables to
them (see [Replication sets](../../replication-sets/)).

It's usually better to create replication sets before subscribing so that all
tables are synchronized during initial replication setup in a single initial
transaction. However, users of bigger databases may instead wish to create them
incrementally for better control.

Once the provider node is setup, subscribers can be subscribed to it. First the
subscriber node must be created:

```sql
SELECT worf.create_node(
    node_name := 'subscriber1',
    dsn := 'host=thishost port=5432 dbname=db'
);
```

And finally on the subscriber node you can create the subscription which will
start synchronization and replication process in the background:

```sql
SELECT worf.create_subscription(
    subscription_name := 'subscription1',
    provider_dsn := 'host=providerhost port=5432 dbname=db'
);

SELECT worf.wait_for_subscription_sync_complete('subscription1');
```
