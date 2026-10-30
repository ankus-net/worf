---
title: Subscription management
description: Create, alter, synchronize and drop subscriptions.
---

- `worf.create_subscription(subscription_name name, provider_dsn text,
  replication_sets text[], synchronize_structure boolean,
  synchronize_data boolean, forward_origins text[], apply_delay interval)`
  Creates a subscription from current node to the provider node. Command does
  not block, just initiates the action.

  Parameters:
  - `subscription_name` - name of the subscription, must be unique
  - `provider_dsn` - connection string to a provider
  - `replication_sets` - array of replication sets to subscribe to, these must
    already exist, default is "{default,default_insert_only,ddl_sql}"
  - `synchronize_structure` - specifies if to synchronize structure from
    provider to the subscriber, default false
  - `synchronize_data` - specifies if to synchronize data from provider to
    the subscriber, default true
  - `forward_origins` - array of origin names to forward, currently only
    supported values are empty array meaning don't forward any changes
    that didn't originate on provider node (this is useful for two-way
    replication between the nodes), or "{all}" which means replicate all
    changes no matter what is their origin, default is "{all}"; a node
    skips its own changes when another node forwards them back
  - `apply_delay` - how much to delay replication, default is 0 seconds
  - `force_text_transfer` - force the provider to replicate all columns
    using a text representation (which is slower), default is false

  The `subscription_name` is used as `application_name` by the replication
  connection. This means that it's visible in the `pg_stat_replication`
  monitoring view. It can also be used in `synchronous_standby_names` when
  worf is used as part of
  [synchronous replication](../synchronous-replication/) setup.

  Use `worf.wait_for_subscription_sync_complete(sub_name)` to wait for the
  subscription to asynchronously start replicating and complete any needed
  schema and/or data sync.

- `worf.drop_subscription(subscription_name name, ifexists bool)`
  Disconnects the subscription and removes it from the catalog.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `ifexists` - if true, error is not thrown when subscription does not exist,
    default is false

- `worf.alter_subscription_disable(subscription_name name, immediate bool)`
   Disables a subscription and disconnects it from the provider.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `immediate` - if true, the subscription is stopped immediately, otherwise
    it will be only stopped at the end of current transaction, default is false

- `worf.alter_subscription_enable(subscription_name name, immediate bool)`
  Enables disabled subscription.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `immediate` - if true, the subscription is started immediately, otherwise
    it will be only started at the end of current transaction, default is false

- `worf.alter_subscription_interface(subscription_name name, interface_name name)`
  Switch the subscription to use different interface to connect to provider
  node.

  Parameters:
  - `subscription_name` - name of an existing subscription
  - `interface_name` - name of an existing interface of the current provider
    node

- `worf.alter_subscription_synchronize(subscription_name name, truncate bool)`
  All unsynchronized tables in all sets are synchronized in a single operation.
  Tables are copied and synchronized one by one. Command does not block, just
  initiates the action. Use `worf.wait_for_subscription_sync_complete`
  to wait for completion.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `truncate` - if true, tables will be truncated before copy, default false

- `worf.alter_subscription_resynchronize_table(subscription_name name,
  relation regclass)`
  Resynchronize one existing table. The table may not be the target of any
  foreign key constraints.
  **WARNING: This function will truncate the table immediately, and only then
  begin synchronising it, so it will be empty while being synced**

  Does not block, use `worf.wait_for_table_sync_complete` to wait for
  completion.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `relation` - name of existing table, optionally qualified

- `worf.wait_for_subscription_sync_complete(subscription_name name)`

   Wait for a subscription or to finish synchronization after a
   `worf.create_subscription` or `worf.alter_subscription_synchronize`.

  This function waits until the subscription's initial schema/data sync,
  if any, are done, and until any tables pending individual resynchronisation
  have also finished synchronising.

  For best results, run `SELECT worf.wait_slot_confirm_lsn(NULL, NULL)` on the
  provider after any replication set changes that requested resyncs, and only
  then call `worf.wait_for_subscription_sync_complete` on the subscriber.

- `worf.wait_for_table_sync_complete(subscription_name name, relation regclass)`

  Same as `worf.wait_for_subscription_sync_complete`, but waits only for
  the subscription's initial sync and the named table. Other tables pending
  resynchronisation are ignored.

- `worf.wait_slot_confirm_lsn`

  `SELECT worf.wait_slot_confirm_lsn(NULL, NULL)`

  Wait until all replication slots on the current node have replayed up to the
  xlog insert position at time of call on all providers. Returns when
  all slots' `confirmed_flush_lsn` passes the `pg_current_wal_insert_lsn()` at
  time of call.

  Optionally may wait for only one replication slot (first argument).
  Optionally may wait for an arbitrary LSN passed instead of the insert lsn
  (second argument). Both are usually just left null.

  This function is very useful to ensure all subscribers have received changes
  up to a certain point on the provider.

- `worf.show_subscription_status(subscription_name name)`
  Shows status and basic information about subscription.

  Parameters:
  - `subscription_name` - optional name of the existing subscription, when no
    name was provided, the function will show status for all subscriptions on
    local node

- `worf.show_subscription_table(subscription_name name,
  relation regclass)`
  Shows synchronization status of a table.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `relation` - name of existing table, optionally qualified

- `worf.alter_subscription_add_replication_set(subscription_name name,
  replication_set name)`
  Adds one replication set into a subscriber. Does not synchronize, only
  activates consumption of events.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `replication_set` - name of replication set to add

- `worf.alter_subscription_remove_replication_set(subscription_name name,
  replication_set name)`
  Removes one replication set from a subscriber.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `replication_set` - name of replication set to remove


There is also a `postgresql.conf` parameter,
`worf.extra_connection_options`, that may be set to assign connection
options that apply to all connections made by worf. This can be a useful
place to set up custom keepalive options, etc.

worf defaults to enabling TCP keepalives to ensure that it notices
when the upstream server disappears unexpectedly. To disable them add
`keepalives = 0` to `worf.extra_connection_options`.
