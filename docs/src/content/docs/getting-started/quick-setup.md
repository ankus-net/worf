---
title: Quick setup
description: Configure PostgreSQL, create nodes and subscribe.
---

First the PostgreSQL server has to be properly configured to support logical
decoding:

    wal_level = 'logical'
    max_worker_processes = 10   # one per database needed on provider node
                                # one per node needed on subscriber node
    max_replication_slots = 10  # one per node needed on provider node
    max_wal_senders = 10        # one per node needed on provider node
    shared_preload_libraries = 'worf'

On a subscriber, PostgreSQL tracks each replication origin in use in one of
`max_active_replication_origins` slots from PostgreSQL 18, or
`max_replication_slots` before. Each subscription takes one, each table it's
synchronizing another, and each other node whose changes its provider forwards
with `forward_origins` `{all}` another. When one that needs a slot finds none
free, subscriptions give up the slots of forwarded nodes' origins they no longer
need, which slows apply; failing that within 10 seconds, its apply or table
sync stops with an error and retries. They also give them up, within 10
seconds, to another user of origins, such as a PostgreSQL subscription, whose
origin has none.

If you want to handle conflict resolution with last/first update wins (see
[Conflicts](../../conflicts/)), you can add this additional option to
postgresql.conf:

    track_commit_timestamp = on # needed for last/first update wins conflict resolution

PostgreSQL 15.19, 16.15, 17.11, 18.6, 19 and later releases load only the
logical decoding output plugins that `output_plugin_libraries` lists, so the
provider must list `worf` there as well:

    output_plugin_libraries = 'pgoutput, test_decoding, worf'

`pg_hba.conf` has to allow logical replication connections from
localhost. Logical replication connections are treated by `pg_hba.conf`
as regular connections to the provider database.

Next the `worf` extension has to be installed on all nodes:

    CREATE EXTENSION worf;

Now create the provider node:

    SELECT worf.create_node(
        node_name := 'provider1',
        dsn := 'host=providerhost port=5432 dbname=db'
    );

Add all tables in `public` schema to the `default` replication set.

    SELECT worf.replication_set_add_all_tables('default', ARRAY['public']);

Optionally you can also create additional replication sets and add tables to
them (see [Replication sets](../../replication-sets/)).

It's usually better to create replication sets before subscribing so that all
tables are synchronized during initial replication setup in a single initial
transaction. However, users of bigger databases may instead wish to create them
incrementally for better control.

Once the provider node is setup, subscribers can be subscribed to it. First the
subscriber node must be created:

    SELECT worf.create_node(
        node_name := 'subscriber1',
        dsn := 'host=thishost port=5432 dbname=db'
    );

And finally on the subscriber node you can create the subscription which will
start synchronization and replication process in the background:

    SELECT worf.create_subscription(
        subscription_name := 'subscription1',
        provider_dsn := 'host=providerhost port=5432 dbname=db'
    );

    SELECT worf.wait_for_subscription_sync_complete('subscription1');
