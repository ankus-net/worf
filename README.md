# worf

worf provides logical streaming replication for PostgreSQL, using a
publish/subscribe model. It is built with C# and .NET Native AOT on
[Ankus](https://github.com/ankus-net/ankus). See the
[documentation](https://ankus-net.github.io/worf/).

We use the following terms to describe data streams between nodes, deliberately
reused from the earlier Slony technology:
* Nodes - PostgreSQL database instances
* Providers and Subscribers - roles taken by Nodes
* Replication Set - a collection of tables

worf runs on PostgreSQL 15 through 19, on Linux x64, macOS ARM64 and x64, and
Windows x64. Provider and subscriber nodes may run different PostgreSQL major
versions (see [PostgreSQL Version differences](#postgresql-version-differences)).

Use cases supported are:
* Upgrades between major versions (given the above restrictions)
* Full database replication
* Selective replication of sets of tables using replication sets
* Selective replication of table rows at either publisher or subscriber side (row_filter)
* Selective replication of table columns at publisher side
* Data gather/merge from multiple upstream servers

Architectural details:
* worf works on a per-database level, not whole server level like
  physical streaming replication
* One Provider may feed multiple Subscribers without incurring additional disk
  write overhead
* One Subscriber can merge changes from several origins and detect conflict
  between changes with automatic and configurable conflict resolution (some,
  but not all aspects required for multi-master).
* Cascading replication is implemented in the form of changeset forwarding.

## Requirements

To use worf the provider and subscriber must be running PostgreSQL 15 or newer.

The `worf` extension must be installed on both provider and subscriber.
You must `CREATE EXTENSION worf` on both.

Tables on the provider and subscriber must have the same names and be in the
same schema. Future revisions may add mapping features.

Tables on the provider and subscriber must have the same columns, with the same
data types in each column. `CHECK` constraints, `NOT NULL` constraints, etc., must
be the same or weaker (more permissive) on the subscriber than the provider.
A subscriber ignores a column it dropped in rows from a provider that still
has it.

Tables must have the same `PRIMARY KEY`s. It is not recommended to add additional
`UNIQUE` constraints other than the `PRIMARY KEY` (see below).

Some additional requirements are covered in [Limitations and Restrictions](#limitations-and-restrictions).

## Installation

### Packages

Each [release](https://github.com/ankus-net/worf/releases) has a package for
every supported PostgreSQL major version and platform, named
`worf-<version>-pg<major>-<platform>`:

- `linux-x64`, `macos-arm64` and `macos-x64`: a `.tar.gz` archive
- `windows-x64`: a `.zip` archive and an `.msi` installer

Every package holds the extension library, its control and SQL files, and
`worf_create_subscriber`, laid out as `bin/`, `lib/` and `share/extension/`.
Copy each directory's files into the matching directory of your PostgreSQL
installation, as `pg_config --bindir`, `--pkglibdir` and `--sharedir` report
them:

    cp bin/* "$(pg_config --bindir)/"
    cp lib/* "$(pg_config --pkglibdir)/"
    cp share/extension/* "$(pg_config --sharedir)/extension/"

You might need to use `sudo` for these steps. A package works with any build of
the PostgreSQL major version it names. On Windows, the MSI finds your PostgreSQL
installation and installs the files there; see [Windows](https://ankus-net.github.io/worf/windows/).
`SHA256SUMS` lists every file's checksum.

### From source code

worf builds with the [.NET 10 SDK](https://dotnet.microsoft.com/download) and
[Ankus](https://ankus.net), which needs LLVM Clang 20 or later and the server
headers of the PostgreSQL release you build for. From a clone of this
repository:

    dotnet tool restore
    dotnet ankus init --pg17 "$(command -v pg_config)"
    dotnet ankus install --pg 17 --project src/Worf/Worf.csproj

`ankus install` builds the extension with Native AOT and installs it into that
PostgreSQL installation. `dotnet ankus package --pg 17 --project src/Worf/Worf.csproj --output dist`
instead writes the files to `dist`. Build `worf_create_subscriber` with
`dotnet publish src/Worf.CreateSubscriber -c Release -r <runtime identifier>`,
and install it beside your PostgreSQL programs.

## Usage

This section describes basic usage of the worf replication extension.

### Quick setup

First the PostgreSQL server has to be properly configured to support logical
decoding:

    wal_level = 'logical'
    max_worker_processes = 10   # one per database needed on provider node
                                # one per node needed on subscriber node
    max_replication_slots = 10  # one per node needed on provider node
    max_wal_senders = 10        # one per node needed on provider node
    shared_preload_libraries = 'worf'

If you want to handle conflict resolution with last/first update wins (see
[Conflicts](#conflicts)), you can add this additional option to
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
them (see [Replication sets](#replication-sets)).

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

### Creating subscriber nodes with base backups

In addition to the SQL-level node and subscription creation, worf also
supports creating a subscriber by cloning the provider with `pg_basebackup` and
starting it up as a worf subscriber. This is done with the
`worf_create_subscriber` tool; see the `--help` output.

Unlike `worf.create_subscription`'s data sync options, this clone ignores
replication sets and copies all tables on all databases. However, it's often
much faster, especially over high-bandwidth links.

### Node management

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

### Subscription management

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
    using a text representation (which is slower, but may be used to
    change the type of a replicated column on the subscriber), default
    is false

  The `subscription_name` is used as `application_name` by the replication
  connection. This means that it's visible in the `pg_stat_replication`
  monitoring view. It can also be used in `synchronous_standby_names` when
  worf is used as part of
  [synchronous replication](#synchronous-replication) setup.

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

### Replication sets

Replication sets provide a mechanism to control which tables in the database
will be replicated and which actions on those tables will be replicated.

Each replicated set can specify individually if `INSERTs`, `UPDATEs`,
`DELETEs` and `TRUNCATEs` on the set are replicated. Every table can be in
multiple replication sets and every subscriber can subscribe to multiple
replication sets as well. The resulting set of tables and actions replicated
is the union of the sets the table is in. The tables are not replicated until
they are added into a replication set.

There are three preexisting replication sets named "default",
"default_insert_only" and "ddl_sql". The "default" replication set is defined
to replicate all changes to tables in it. The "default_insert_only" only
replicates INSERTs and is meant for tables that don't have primary key (see
[Limitations](#primary-key-required) section for details).
The "ddl_sql" replication set is defined to replicate schema changes specified by
`worf.replicate_ddl_command`

The following functions are provided for managing the replication sets:

- `worf.create_replication_set(set_name name, replicate_insert bool, replicate_update bool, replicate_delete bool, replicate_truncate bool)`
  This function creates a new replication set.

  Parameters:
  - `set_name` - name of the set, must be unique
  - `replicate_insert` - specifies if `INSERT` is replicated, default true
  - `replicate_update` - specifies if `UPDATE` is replicated, default true
  - `replicate_delete` - specifies if `DELETE` is replicated, default true
  - `replicate_truncate` - specifies if `TRUNCATE` is replicated, default true

- `worf.alter_replication_set(set_name name, replicate_inserts bool, replicate_updates bool, replicate_deletes bool, replicate_truncate bool)`
  This function changes the parameters of the existing replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `replicate_insert` - specifies if `INSERT` is replicated, default true
  - `replicate_update` - specifies if `UPDATE` is replicated, default true
  - `replicate_delete` - specifies if `DELETE` is replicated, default true
  - `replicate_truncate` - specifies if `TRUNCATE` is replicated, default true

- `worf.drop_replication_set(set_name text)`
  Removes the replication set.

  Parameters:
  - `set_name` - name of the existing replication set

- `worf.replication_set_add_table(set_name name, relation regclass, synchronize_data boolean, columns text[], row_filter text)`
  Adds a table to replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `relation` - name or OID of the table to be added to the set
  - `synchronize_data` - if true, the table data is synchronized on all
    subscribers which are subscribed to given replication set, default false
  - `columns` - list of columns to replicate. Normally when all columns
    should be replicated, this will be set to NULL which is the
    default
  - `row_filter` - row filtering expression, default NULL (no filtering),
    see [Row Filtering](#row-filtering) for more info.
  **WARNING: Use caution when synchronizing data with a valid row filter.**
Using `synchronize_data=true` with a valid `row_filter` is like a one-time operation for a table.
Executing it again with modified `row_filter` won't synchronize data to subscriber. Subscribers
may need to call `worf.alter_subscription_resynchronize_table()` to fix it.

- `worf.replication_set_add_all_tables(set_name name, schema_names text[], synchronize_data boolean)`
  Adds all tables in given schemas. Only existing tables are added, table that
  will be created in future will not be added automatically. For how to ensure
  that tables created in future are added to correct replication set, see
  [Automatic assignment of replication sets for new tables](#automatic-assignment-of-replication-sets-for-new-tables).

  Parameters:
  - `set_name` - name of the existing replication set
  - `schema_names` - array of names name of existing schemas from which tables
    should be added
  - `synchronize_data` - if true, the table data is synchronized on all
    subscribers which are subscribed to given replication set, default false

- `worf.replication_set_remove_table(set_name name, relation regclass)`
  Remove a table from replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `relation` - name or OID of the table to be removed from the set

- `worf.replication_set_add_sequence(set_name name, relation regclass, synchronize_data boolean)`
  Adds a sequence to a replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `relation` - name or OID of the sequence to be added to the set
  - `synchronize_data` - if true, the sequence value will be synchronized immediately, default false

- `worf.replication_set_add_all_sequences(set_name name, schema_names text[], synchronize_data boolean)`
  Adds all sequences from the given schemas. Only existing sequences are added, any sequences that
  will be created in future will not be added automatically.

  Parameters:
  - `set_name` - name of the existing replication set
  - `schema_names` - array of names name of existing schemas from which tables
    should be added
  - `synchronize_data` - if true, the sequence value will be synchronized immediately, default false

- `worf.replication_set_remove_sequence(set_name name, relation regclass)`
  Remove a sequence from a replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `relation` - name or OID of the sequence to be removed from the set

You can view the information about which table is in which set by querying the
`worf.tables` view.

#### Automatic assignment of replication sets for new tables

The event trigger facility can be used for describing rules which define
replication sets for newly created tables.

Example:

    CREATE OR REPLACE FUNCTION worf_assign_repset()
    RETURNS event_trigger AS $$
    DECLARE obj record;
    BEGIN
        FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            IF obj.object_type = 'table' THEN
                IF obj.schema_name = 'config' THEN
                    PERFORM worf.replication_set_add_table('configuration', obj.objid);
                ELSIF NOT obj.in_extension THEN
                    PERFORM worf.replication_set_add_table('default', obj.objid);
                END IF;
            END IF;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;

    CREATE EVENT TRIGGER worf_assign_repset_trg
        ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS')
        EXECUTE PROCEDURE worf_assign_repset();

The above example will put all new tables created in schema `config` into
replication set `configuration` and all other new tables which are not created
by extensions will go to `default` replication set.

### Additional functions

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

### Row Filtering

worf allows row based filtering both on provider side and the subscriber
side.

#### Row Filtering on Provider

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

#### Row Filtering on Subscriber

On the subscriber the row based filtering can be implemented using standard
`BEFORE TRIGGER` mechanism.

It is required to mark any such triggers as either `ENABLE REPLICA` or
`ENABLE ALWAYS` otherwise they will not be executed by the replication
process.

### Sequence ranges

Sequence ranges give each node blocks of a sequence's values that no other node
uses, so rows inserted on different nodes never share a key, with no change to
the tables. On the node that will own the values, name the other nodes that
issue values from the sequence:

```sql
SELECT worf.enable_sequence_range('inventory_id_seq', participants := '{node_beta}');
```

The other nodes learn of the sequence over their subscriptions. `nextval`
fails with SQLSTATE `2200H` until a node has its first block, and a trigger
rejects values written by hand outside this node's blocks with SQLSTATE
`23514`. `worf.sequence_ranges` shows each sequence's state and blocks.

- `worf.enable_sequence_range(sequence regclass, participants text[], range_size bigint, max_range_size bigint, pool_start bigint, force boolean)`
  Puts a sequence under range management, owned by this node.
- `worf.activate_sequence_range(sequence regclass)`
  Starts granting with the nodes that have reported so far, or lifts a fence.
- `worf.alter_sequence_range(sequence regclass, range_size bigint, max_range_size bigint)`
  Changes the block sizes.
- `worf.disable_sequence_range(sequence regclass)`
  Stops granting. Each node keeps using its blocks. Enabling it on another
  node then moves the owner.
- `worf.reset_sequence_range(sequence regclass)`
  Abandons this node's blocks and requests new ones, as after a restore.
- `worf.reserve_sequence_range(sequence regclass, count bigint, timeout interval)`
  Returns once the active block has at least `count` values left, before a
  load that numbers its rows with the sequence.
- `worf.reserve_sequence_values(sequence regclass, lo bigint, hi bigint, timeout interval)`
  Gives this node exactly `[lo, hi]`, for rows that already carry those keys.

Where the schema can change, Snowflake IDs need no blocks. Run
`worf.enable_snowflake(participants text[])` on one node, naming the others,
and use `worf.snowflake_id()` as a `bigint` column's default. An ID holds the
milliseconds since 2025 in its top 41 bits, the node's number from the same
allocator in the next 10 and a counter in the last 12, and each node's IDs
only grow, even when its clock goes back across a restart.
`worf.snowflake_node()` shows this node's number.

## Synchronous Replication

Synchronous replication is supported using same standard mechanism provided
by PostgreSQL for physical replication.

The `synchronous_commit` and `synchronous_standby_names` settings will affect
when `COMMIT` command reports success to client if worf subscription
name is used in `synchronous_standby_names`. Refer to PostgreSQL
documentation for more info about how to configure these two variables.

## Conflicts

In case the node is subscribed to multiple providers, or when local writes
happen on a subscriber, conflicts can arise for the incoming changes. These
are automatically detected and can be acted on depending on the configuration.

The configuration of the conflicts resolver is done via the
`worf.conflict_resolution` setting.

The resolved conflicts are logged using the log level set using
`worf.conflict_log_level`. This parameter defaults to `LOG`. If set to
lower level than `log_min_messages` the resolved conflicts won't appear in
the server log.

## Configuration options

Some aspects of worf can be configured using configuration options that
can be either set in `postgresql.conf` or via `ALTER SYSTEM SET`.

- `worf.conflict_resolution`
  Sets the resolution method for any detected conflicts between local data
  and incoming changes.

  Possible values:
  - `error` - the replication will stop on error if conflict is detected and
    manual action is needed for resolving
  - `apply_remote` - always apply the change that's conflicting with local
    data
  - `keep_local` - keep the local version of the data and ignore the
     conflicting change that is coming from the remote node
  - `last_update_wins` - the version of data with newest commit timestamp
     will be kept (this can be either local or remote version)
  - `first_update_wins` - the version of the data with oldest timestamp will
     be kept (this can be either local or remote version)

  When the timestamps are equal, `last_update_wins` and `first_update_wins`
  keep the change written by the node with the higher node id, including
  changes another node forwarded, so every node keeps the same version.
  Nodes that replicate both ways need the same one of these two settings to
  keep the same rows, and `create_subscription` warns when they don't.

  The default value is `apply_remote`.

  The `keep_local`, `last_update_wins` and `first_update_wins` settings
  require `track_commit_timestamp` PostgreSQL setting to be enabled.

- `worf.conflict_log_level`
  Sets the log level for reporting detected conflicts when the
  `worf.conflict_resolution` is set to anything else than `error`.

  Main use for this setting is to suppress logging of conflicts.

  Possible values are same as for `log_min_messages` PostgreSQL setting.

  The default is `LOG`.

- `worf.max_clock_skew`
  How far ahead of the local clock a remote transaction's commit time can be
  for apply to wait until the local clock passes it, so that local changes
  made afterwards commit later and win under `last_update_wins`. A remote
  transaction further ahead applies at once, with a warning at most once a
  minute. `-1` never waits. The default is `1s`.

- `worf.exception_behaviour`
  What apply does with a remote transaction that fails, as on a constraint
  only the subscriber has:
  - `error` - stop, and apply the transaction again when the worker
    restarts, until the error is fixed
  - `discard` - apply the transaction again change by change, skipping the
    changes that fail, or the whole transaction when it fails at commit, as
    on a deferred constraint
  - `transdiscard` - skip the whole transaction
  - `sub_disable` - disable the subscription, which applies the transaction
    again once it's enabled

  Errors that don't come from the data, such as deadlocks, serialization
  failures, lock and statement timeouts, cancellations, shutdowns and running
  out of resources, are retried as under `error`. Skipped changes and
  disabled subscriptions are recorded in `worf.exception_log`, with the error,
  the change's rows or queued DDL, and the transaction it belongs to. The
  default is `error`.

- `worf.enable_ddl_replication`
  Replicates DDL as it runs, through the `ddl_sql` replication set as
  `worf.replicate_ddl_command` does, with the settings that change what it
  means: `search_path`, `DateStyle`, `IntervalStyle`, `TimeZone`,
  `standard_conforming_strings`, `default_tablespace`,
  `default_table_access_method` and `default_toast_compression`. Statements
  about databases, tablespaces, roles, server settings and PostgreSQL's own
  publications and subscriptions stay on their node, as do statements that use
  the session's temporary objects or prepared statements, and what they create.
  A `DROP` of temporary and other objects drops the others everywhere. worf
  itself can only be dropped in a statement of its own. Other nodes run
  concurrent statements, such as `CREATE INDEX CONCURRENTLY`, in their apply
  transaction. The default is `off`.

- `worf.include_ddl_repset`
  Adds the tables DDL creates to the `default` replication set, or to
  `default_insert_only` when they have no replica identity, and moves a table
  and its partitions between the two as they gain or lose one. A table that
  becomes unlogged leaves its replication sets, since its changes can't
  replicate, and a dropped table leaves them too. The default is `off`.

- `worf.allow_ddl_from_functions`
  Replicates DDL that functions and procedures run too. The default is
  `off`.

- `worf.global_ddl_lock`
  With `worf.enable_ddl_replication`, locks every node for DDL that can't
  apply in any order: changing a column's type, renaming a table, column or
  schema, moving a table to another schema, adding a constraint, a unique
  index or a `NOT NULL` column without a default, dropping a table or a schema
  with `CASCADE`, adding a domain constraint, renaming an enum value, or
  changing a composite type. It covers a partitioned table's partitions and
  every table with a column of a changed type. In a transaction block, any
  DDL takes the lock for its tables, so later DDL there needn't wait. Before
  the DDL runs, each node this node subscribes to that receives its DDL
  through `ddl_sql` holds back writes to those tables it replicates, which
  fail with SQLSTATE `55P03`, and DDL on them, until the DDL has applied
  there. Every node applies the others' changes up to that point first. A
  node keeps holding back across a restart, and lets go once the DDL applies
  or the transaction that ran it aborts. Only the table's, schema's or type's
  owner takes the lock. `CREATE UNIQUE INDEX CONCURRENTLY`, which commits
  partway, can't take it. The user in each provider connection string needs
  `EXECUTE` on `worf.fence_writes` and `worf.keep_fence`, which `PUBLIC`
  doesn't have. The default is `on`.

- `worf.sequence_range_check_interval`
  How often the range worker checks each managed sequence, switching it to
  its next block when the active one runs low. The default is `1s`.

- `worf.sequence_range_exchange_interval`
  How often each node exchanges sequence range reports with its neighbors
  when no request, grant or reset is waiting. The default is `1min`.

- `worf.sequence_range_timeout`
  The connection and statement timeout of a sequence range exchange. The
  default is `10s`.

- `worf.ddl_lock_timeout`
  How long DDL waits for the other nodes to hold back their writes and catch
  up before it fails. The default is `1min`.

- `worf.max_fenced_tables`
  How many tables other nodes' DDL can hold back on this server at once.
  Takes effect at server start. The default is `4096`.

- `worf.batch_inserts`
  Tells worf to use batch insert mechanism if possible. Batch mechanism
  uses PostgreSQL internal batch insert mode which is also used by `COPY`
  command.

  The batch inserts will improve replication performance of transactions that
  did many inserts into one table. worf will switch to batch mode when
  transaction did more than 5 INSERTs.

  It's only possible to switch to batch mode when there are no
  `INSTEAD OF INSERT` and `BEFORE INSERT` triggers on the table and when
  there are no defaults with volatile expressions for columns of the table.
  Also the batch mode will only work when `worf.conflict_resolution` is
  set to `error`.

  The default is `true`.

- `worf.use_spi`
  Tells worf to use SPI interface to form actual SQL
  (`INSERT`, `UPDATE`, `DELETE`) statements to apply incoming changes instead
  of using internal low level interface.

  This is mainly useful for debugging purposes.

  The default is `false`.

  This can be set to `true` only when `worf.conflict_resolution` is set to `error`.
In this state, conflicts are not detected.

- `worf.temp_directory`
  Defines system path where to put temporary files needed for schema
  synchronization. This path need to exist and be writable by user running
  Postgres.

  Default is empty, which tells worf to use default temporary directory
  based on environment and operating system settings.

- `worf.synchronous_commit`
  Sets the `synchronous_commit` of the apply and sync workers' transactions:
  `local` when enabled, `off` when not. Committing asynchronously is safe,
  because worf confirms changes to the provider only once they are flushed
  locally.

  The default is `false`. It can only be set at server start.

- `worf.extra_connection_options`
  Connection options to add to all peer node connections; see
  [Subscription management](#subscription-management).

  The default is empty.

- `worf.conflict_history_enabled`
  Records each resolved conflict in the `worf.conflict_history` table; see
  [Conflict history](https://ankus-net.github.io/worf/conflict-history/).

  The default is `false`.

- `worf.conflict_history_store_tuples`
  Stores the local and remote rows of each recorded conflict as `jsonb`.

  The default is `true`.

- `worf.conflict_history_max_tuple_size`
  The maximum bytes of each column value stored with a recorded conflict, from
  64 to 65536. Longer values are truncated.

  The default is `1024`.

## Limitations and restrictions

### Superuser is required

Currently worf replication and administration requires superuser
privileges. It may be later extended to more granular privileges.

### `UNLOGGED` and `TEMPORARY` not replicated

`UNLOGGED` and `TEMPORARY` tables will not and cannot be replicated, much like
with physical streaming replication.

### One database at a time

To replicate multiple databases you must set up individual provider/subscriber
relationships for each. There is no way to configure replication for all databases
in a PostgreSQL install at once.

### PRIMARY KEY or REPLICA IDENTITY required

`UPDATE`s and `DELETE`s cannot be replicated for tables that lack a `PRIMARY
KEY` or other valid replica identity such as using an index, which must be unique,
not partial, not deferrable, and include only columns marked NOT NULL.
Replication has no way to find the tuple that should be updated/deleted since
there is no unique identifier.
`REPLICA IDENTITY FULL` is not supported yet.


### Only one unique index/constraint/PK

If more than one upstream is configured or the downstream accepts local writes
then only one `UNIQUE` index should be present on downstream replicated tables.
Conflict resolution can only use one index at a time so conflicting rows may
`ERROR` if a row satisfies the `PRIMARY KEY` but violates a `UNIQUE` constraint
on the downstream side. This will stop replication until the downstream table
is modified to remove the violation.

It's fine to have extra unique constraints on an upstream if the downstream only
gets writes from that upstream and nowhere else. The rule is that the downstream
constraints must *not be more restrictive* than those on the upstream(s).

Partial secondary unique indexes are permitted, but will be ignored for
conflict resolution purposes.

### Unique constraints must not be deferrable

On the downstream end worf does not support index-based constraints
defined as `DEFERRABLE`. It will emit the error

    ERROR: worf doesn't support index rechecks needed for deferrable indexes
    DETAIL: relation "public"."test_relation" has deferrable indexes: "index1", "index2"

if such an index is present when it attempts to apply changes to a table.

### DDL

With `worf.enable_ddl_replication` on, DDL replicates as it runs, and
`worf.include_ddl_repset` adds the tables it creates to replication sets.
Otherwise, managing DDL so that the provider and subscriber database(s) remain
compatible is the responsibility of the user.

worf provides the `worf.replicate_ddl_command` function to allow DDL
to be run on the provider and subscriber at a consistent point.

A replicated `ALTER TABLE ... ADD COLUMN` with a volatile default, such as
`now()` or `gen_random_uuid()`, fills existing rows with each node's own
values.

With `worf.global_ddl_lock`, a statement in the same transaction that writes
rows another node is about to change, before DDL that takes the lock, can
make the DDL wait until `worf.ddl_lock_timeout`. A node notices within about a
minute that a node holding back its writes for DDL died before the DDL
committed.

### No replication queue flush

There's no support for freezing transactions on the master and waiting until
all pending queued xacts are replayed from slots. Support for making the
upstream read-only for this will be added in a future release.

This means that care must be taken when applying table structure changes. If
there are committed transactions that aren't yet replicated and the table
structure of the provider and subscriber are changed at the same time in a way
that makes the subscriber table incompatible with the queued transactions
replication will stop.

Administrators should either ensure that writes to the master are stopped
before making schema changes, or use the `worf.replicate_ddl_command`
function to queue schema changes so they're replayed at a consistent point
on the replica.

Once multi-master replication support is added then using
`worf.replicate_ddl_command` will not be enough, as the subscriber may be
generating new xacts with the old structure after the schema change is
committed on the publisher. Users will have to ensure writes are stopped on all
nodes and all slots are caught up before making schema changes.

### FOREIGN KEYS

Foreign keys constraints are not enforced for the replication process - what
succeeds on provider side gets applied to subscriber even if the `FOREIGN KEY`
would be violated.

### TRUNCATE

Using `TRUNCATE ... CASCADE` will only apply the `CASCADE` option on the
provider side.

(Properly handling this would probably require the addition of `ON TRUNCATE CASCADE`
support for foreign keys in PostgreSQL).

`TRUNCATE ... RESTART IDENTITY` is not supported. The identity restart step is
not replicated to the replica.

### Sequences

The state of sequences added to replication sets is replicated periodically
and not in real-time. Dynamic buffer is used for the value being replicated so
that the subscribers actually receive future state of the sequence. This
minimizes the chance of subscriber's notion of sequence's `last_value` falling
behind but does not completely eliminate the possibility.

It might be desirable to call `synchronize_sequence` to ensure all subscribers
have up to date information about given sequence after "big events" in the
database such as data loading or during the online upgrade.

It's generally recommended to use `bigserial` and `bigint` types for sequences
on multi-node systems as smaller sequences might reach end of the sequence
space fast.

Nodes that insert into the same table can give each node its own blocks of a
sequence's values with `worf.enable_sequence_range`, described under
[Sequence ranges](#sequence-ranges).

Users who want to have independent sequences on provider and subscriber can
also avoid adding sequences to replication sets and create sequences with step
interval equal to or greater than the number of nodes. And then setting a
different offset on each node. Use the `INCREMENT BY` option for
`CREATE SEQUENCE` or `ALTER SEQUENCE`, and use `setval(...)` to set the start
point.

### Triggers

Apply process and the initial COPY process both run with
`session_replication_role` set to `replica` which means that `ENABLE REPLICA`
and `ENABLE ALWAYS` triggers will be fired.

### PostgreSQL Version differences

worf can replicate across PostgreSQL major versions. Despite that, long
term cross-version replication is not considered a design target, though it may
often work. Issues where changes are valid on the provider but not on the
subscriber are more likely to arise when replicating across versions.

It is safer to replicate from an old version to a newer version since PostgreSQL
maintains solid backward compatibility but only limited forward compatibility.
Initial schema synchronization is only supported when replicating between same
version of PostgreSQL or from lower version to higher version.

Replicating between different minor versions makes no difference at all.

### Database encoding differences

worf does not support replication between databases with different
encoding. We recommend using `UTF-8` encoding in all replicated databases.

### Large objects

PostgreSQL's logical decoding facility does not support decoding changes
to large objects, so worf cannot replicate large objects.
