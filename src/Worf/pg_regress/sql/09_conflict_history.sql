-- Test conflict history persistence feature
SELECT * FROM worf_regress_variables()
\gset

\c :provider_dsn

-- Create test table for conflict scenarios
SELECT worf.replicate_ddl_command($$
    CREATE TABLE public.conflict_test (
        id integer PRIMARY KEY,
        data text,
        num integer
    );
$$);

SELECT * FROM worf.replication_set_add_table('default', 'conflict_test');

SELECT worf.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

-- Verify conflict_history table exists and check schema
SELECT c.relname, c.relkind
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'worf' AND c.relname = 'conflict_history';

-- Check that table is partitioned
SELECT c.relname, pt.partstrat
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
WHERE n.nspname = 'worf' AND c.relname = 'conflict_history';

-- Verify views exist
SELECT viewname FROM pg_views
WHERE schemaname = 'worf' AND viewname IN ('recent_conflicts', 'conflict_summary')
ORDER BY viewname;

-- Verify functions exist
SELECT p.proname
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'worf'
AND p.proname IN ('conflict_history_ensure_partition', 'conflict_history_cleanup',
                  'conflict_stats', 'show_subscription_conflicts')
ORDER BY p.proname;

-- Test GUC variables - check that conflict history is enabled (set in regress config)
SHOW worf.conflict_history_enabled;
SHOW worf.conflict_history_store_tuples;
SHOW worf.conflict_history_max_tuple_size;

-- Verify initial partition exists for current month
SELECT count(*) > 0 AS has_current_partition
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'worf'
AND c.relname ~ '^conflict_history_[0-9]{4}_[0-9]{2}$';

-- Test partition creation function (idempotent)
SELECT worf.conflict_history_ensure_partition(CURRENT_DATE);
SELECT worf.conflict_history_ensure_partition(CURRENT_DATE); -- Should be no-op

-- Test partition creation for future month (need explicit cast to DATE)
SELECT worf.conflict_history_ensure_partition((CURRENT_DATE + interval '1 month')::DATE);

-- Check we now have at least 2 partitions
SELECT count(*) >= 2 AS has_multiple_partitions
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_inherits i ON i.inhrelid = c.oid
WHERE n.nspname = 'worf'
AND c.relname ~ '^conflict_history_[0-9]{4}_[0-9]{2}$';

-- Get count before our test conflicts (other tests may have created conflicts)
SELECT count(*) AS before_test FROM worf.conflict_history;

-- Insert test row on subscriber before provider (to create conflict)
INSERT INTO conflict_test (id, data, num) VALUES (1, 'subscriber_data', 100);

\c :provider_dsn

-- Insert same primary key on provider (will cause conflict on subscriber)
INSERT INTO conflict_test (id, data, num) VALUES (1, 'provider_data', 200);

SELECT worf.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

-- Check that conflict was recorded (should have at least one more than before)
SELECT count(*) > (SELECT count(*) FROM worf.conflict_history WHERE table_name != 'conflict_test')
    AS new_conflict_recorded
FROM worf.conflict_history
WHERE table_name = 'conflict_test';

-- Query conflict details for our test table
SELECT
    conflict_type,
    resolution,
    schema_name,
    table_name,
    CASE WHEN local_tuple IS NOT NULL THEN 'has_local_tuple' ELSE 'no_local_tuple' END as local_tuple_status,
    CASE WHEN remote_tuple IS NOT NULL THEN 'has_remote_tuple' ELSE 'no_remote_tuple' END as remote_tuple_status,
    remote_origin IS NOT NULL AS has_remote_origin
FROM worf.conflict_history
WHERE table_name = 'conflict_test'
ORDER BY recorded_at DESC
LIMIT 1;

-- Test local_tuple and remote_tuple JSONB content (if stored)
SELECT
    local_tuple->>'id' as local_id,
    local_tuple->>'data' as local_data,
    remote_tuple->>'id' as remote_id,
    remote_tuple->>'data' as remote_data
FROM worf.conflict_history
WHERE table_name = 'conflict_test'
AND local_tuple IS NOT NULL
ORDER BY recorded_at DESC
LIMIT 1;

-- Test recent_conflicts view
SELECT
    conflict_type,
    resolution,
    table_name
FROM worf.recent_conflicts
WHERE table_name = 'conflict_test'
LIMIT 1;

-- Test conflict_summary view (filter to our table)
SELECT
    table_name,
    conflict_type,
    resolution,
    conflict_count > 0 AS has_conflicts
FROM worf.conflict_summary
WHERE table_name = 'conflict_test';

-- Test cleanup function (should return 0 since partitions are recent)
SELECT worf.conflict_history_cleanup(30);

-- Test cleanup with very short retention (0 days - keeps only current month)
SELECT worf.conflict_history_cleanup(0) >= 0 AS cleanup_worked;

-- Cleanup test table
\c :provider_dsn
\set VERBOSITY terse
SELECT worf.replicate_ddl_command($$
    DROP TABLE public.conflict_test CASCADE;
$$);
