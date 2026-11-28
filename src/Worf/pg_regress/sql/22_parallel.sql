SELECT * FROM worf_regress_variables()
\gset

\c :provider_dsn

SELECT * FROM worf.create_replication_set('parallel');

\c :subscriber_dsn

SELECT * FROM worf.create_subscription(
    subscription_name := 'test_subscription_parallel',
    provider_dsn := (SELECT provider_dsn FROM worf_regress_variables()) || ' user=super',
	replication_sets := '{parallel,default}',
	forward_origins := '{}',
	synchronize_structure := false,
	synchronize_data := false
);

SELECT * FROM worf.create_subscription(
    subscription_name := 'test_subscription_parallel',
    provider_dsn := (SELECT provider_dsn FROM worf_regress_variables()) || ' user=super',
	replication_sets := '{parallel}',
	forward_origins := '{}',
	synchronize_structure := false,
	synchronize_data := false
);

BEGIN;
SET LOCAL statement_timeout = '180s';
SELECT worf.wait_for_subscription_sync_complete('test_subscription_parallel');
COMMIT;

SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status IN ('y', 'r') FROM worf.local_sync_status ORDER BY 2,3,4;

SELECT * FROM worf.show_subscription_status();

-- Make sure we see the slot and active connection
\c :provider_dsn
SELECT plugin, slot_type, database, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

SELECT worf.replicate_ddl_command($$
    CREATE TABLE public.basic_dml1 (
        id serial primary key,
        other integer,
        data text,
        something interval
    );
    CREATE TABLE public.basic_dml2 (
        id serial primary key,
        other integer,
        data text,
        something interval
    );
$$);

SELECT * FROM worf.replication_set_add_table('default', 'basic_dml1');
SELECT * FROM worf.replication_set_add_table('parallel', 'basic_dml2');

SELECT worf.wait_slot_confirm_lsn(NULL, NULL);

WITH one AS (
INSERT INTO basic_dml1(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL)
RETURNING *
)
INSERT INTO basic_dml2 SELECT * FROM one;

BEGIN;
UPDATE basic_dml1 SET other = id, something = something - '10 seconds'::interval WHERE id < 3;
DELETE FROM basic_dml2 WHERE id < 3;
COMMIT;

SELECT worf.wait_slot_confirm_lsn(NULL, NULL);

SELECT * FROM basic_dml1;
SELECT * FROM basic_dml2;

\c :subscriber_dsn

SELECT * FROM basic_dml1;
SELECT * FROM basic_dml2;

-- A set dropped and created again under its name while the subscription is disabled is the one the walsender follows
-- past the drop.
SELECT worf.alter_subscription_disable('test_subscription_parallel', true);

\c :provider_dsn

DO $$
BEGIN
	FOR i IN 1..100 LOOP
		IF NOT EXISTS (SELECT 1 FROM pg_stat_replication WHERE application_name = 'test_subscription_parallel') THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SELECT * FROM worf.drop_replication_set('parallel');
SELECT * FROM worf.create_replication_set('parallel');
SELECT * FROM worf.replication_set_add_table('parallel', 'basic_dml2');
INSERT INTO basic_dml2 VALUES (6, 6, 'recreated', NULL);

\c :subscriber_dsn

SELECT worf.alter_subscription_enable('test_subscription_parallel', true);

\c :provider_dsn

BEGIN;
SET LOCAL statement_timeout = '60s';
SELECT worf.wait_slot_confirm_lsn(NULL, NULL);
COMMIT;

\c :subscriber_dsn

SELECT * FROM basic_dml2 WHERE id = 6;

SELECT worf.drop_subscription('test_subscription_parallel');

\c :provider_dsn
\set VERBOSITY terse
SELECT * FROM worf.drop_replication_set('parallel');

SELECT worf.replicate_ddl_command($$
    DROP TABLE public.basic_dml1 CASCADE;
    DROP TABLE public.basic_dml2 CASCADE;
$$);
