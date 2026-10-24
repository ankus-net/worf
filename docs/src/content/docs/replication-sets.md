---
title: Replication sets
description: Choose which tables and actions replicate.
---

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
[Limitations](../limitations/#primary-key-or-replica-identity-required) section for details).
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
    see [Row Filtering](../row-filtering/) for more info.
  **WARNING: Use caution when synchronizing data with a valid row filter.**
Using `synchronize_data=true` with a valid `row_filter` is like a one-time operation for a table.
Executing it again with modified `row_filter` won't synchronize data to subscriber. Subscribers
may need to call `worf.alter_subscription_resynchronize_table()` to fix it.

- `worf.replication_set_add_all_tables(set_name name, schema_names text[], synchronize_data boolean)`
  Adds all tables in given schemas. Only existing tables are added, table that
  will be created in future will not be added automatically. For how to ensure
  that tables created in future are added to correct replication set, see
  [Automatic assignment of replication sets for new tables](../replication-sets/#automatic-assignment-of-replication-sets-for-new-tables).

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

## Automatic assignment of replication sets for new tables

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
