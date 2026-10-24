-- row based filtering
SELECT * FROM worf_regress_variables()
\gset

\c :provider_dsn
-- testing volatile sampling function in row_filter
SELECT worf.replicate_ddl_command($$
	CREATE TABLE public.test_tablesample (id int primary key, name text) WITH (fillfactor=10);
$$);
-- use fillfactor so we don't have to load too much data to get multiple pages
INSERT INTO test_tablesample
  SELECT i, repeat(i::text, 200) FROM generate_series(0, 9) s(i);

create or replace function funcn_get_system_sample_count(integer, integer) returns bigint as
$$ (SELECT count(*) FROM test_tablesample TABLESAMPLE SYSTEM ($1) REPEATABLE ($2)); $$
language sql volatile;

create or replace function funcn_get_bernoulli_sample_count(integer, integer) returns bigint as
$$ (SELECT count(*) FROM test_tablesample TABLESAMPLE BERNOULLI ($1) REPEATABLE ($2)); $$
language sql volatile;

SELECT * FROM worf.replication_set_add_table('default', 'test_tablesample', false, row_filter := $rf$id > funcn_get_system_sample_count(100, 3) $rf$);
SELECT * FROM worf.replication_set_remove_table('default', 'test_tablesample');
SELECT * FROM worf.replication_set_add_table('default', 'test_tablesample', true, row_filter := $rf$id > funcn_get_bernoulli_sample_count(10, 0) $rf$);

SELECT * FROM test_tablesample ORDER BY id limit 5;
SELECT worf.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

BEGIN;
SET LOCAL statement_timeout = '180s';
SELECT worf.wait_for_table_sync_complete('test_subscription', 'test_tablesample');
COMMIT;

SELECT sync_kind, sync_nspname, sync_relname, sync_status FROM worf.local_sync_status WHERE sync_relname = 'test_tablesample';

SELECT * FROM test_tablesample ORDER BY id limit 5;

\c :provider_dsn
\set VERBOSITY terse
DROP FUNCTION funcn_get_system_sample_count(integer, integer);
DROP FUNCTION funcn_get_bernoulli_sample_count(integer, integer);
SELECT worf.replicate_ddl_command($$
	DROP TABLE public.test_tablesample CASCADE;
$$);

CREATE TABLE sample_rowfilter_table(id int primary key, name text);

\COPY sample_rowfilter_table(id, name) FROM STDIN WITH CSV
1,John
2,Jane
3,Bob
4,Alice
5,Charlie
6,Eve
\.

SELECT worf.create_replication_set('sample_publisher_set', true, true, true, true);

SELECT worf.replication_set_add_table(set_name := 'sample_publisher_set', relation := 'public.sample_rowfilter_table', row_filter := $$ id >= 1  and id <= 3 $$);
SELECT * FROM worf.table_data_filtered(NULL::"public"."sample_rowfilter_table", '"public"."sample_rowfilter_table"'::regclass, ARRAY['sample_publisher_set']);

-- Try to trigger cache invalidation for the sample_rowfilter_table relation
-- while program execution is inside create_estate_for_relation(), called from
-- worf_table_data_filtered().  To reach that reliably, one can run the
-- test suite under debug_discard_caches=1.  This helps verify row_filter is
-- applied correctly even during cache invalidation.

SELECT worf.replication_set_remove_table('sample_publisher_set', '"public"."sample_rowfilter_table"'::regclass);
SELECT worf.replication_set_add_table(set_name := 'sample_publisher_set', relation := 'public.sample_rowfilter_table', row_filter := $$ id >= 4  and id <= 6 $$);

SELECT * FROM worf.table_data_filtered(NULL::"public"."sample_rowfilter_table", '"public"."sample_rowfilter_table"'::regclass, ARRAY['sample_publisher_set']);

SELECT worf.drop_replication_set('sample_publisher_set');
DROP TABLE sample_rowfilter_table;
