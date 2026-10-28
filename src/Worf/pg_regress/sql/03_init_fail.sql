
SELECT * FROM worf_regress_variables()
\gset

\c :provider_dsn
SET client_min_messages = 'warning';
DROP ROLE IF EXISTS nonreplica;
CREATE USER nonreplica;

DO $$
BEGIN
	IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') = 904 THEN
		CREATE EXTENSION IF NOT EXISTS worf_origin;
	END IF;
END;$$;
CREATE EXTENSION IF NOT EXISTS worf;
GRANT ALL ON SCHEMA worf TO nonreplica;
GRANT ALL ON ALL TABLES IN SCHEMA worf TO nonreplica;

\c :subscriber_dsn
SET client_min_messages = 'warning';
\set VERBOSITY terse
DO $$
BEGIN
	IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') = 904 THEN
		CREATE EXTENSION IF NOT EXISTS worf_origin;
	END IF;
END;$$;

DO $$
BEGIN
        IF version() ~ 'Postgres-XL' THEN
                CREATE EXTENSION IF NOT EXISTS worf;
        ELSE
                CREATE EXTENSION IF NOT EXISTS worf VERSION '0.1.0';
        END IF;
END;
$$;
ALTER EXTENSION worf UPDATE;

-- fail (local node not existing)
SELECT * FROM worf.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := (SELECT provider_dsn FROM worf_regress_variables()) || ' user=nonreplica',
	forward_origins := '{}');

-- succeed
SELECT * FROM worf.create_node(node_name := 'test_subscriber', dsn := (SELECT subscriber_dsn FROM worf_regress_variables()) || ' user=nonreplica');

-- fail (can't connect to remote)
DO $$
BEGIN
    SELECT * FROM worf.create_subscription(
        subscription_name := 'test_subscription',
        provider_dsn := (SELECT provider_dsn FROM worf_regress_variables()) || ' user=nonexisting',
        forward_origins := '{}');
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '%:%', split_part(SQLERRM, ':', 1), (regexp_matches(SQLERRM, '^.*( FATAL:.*role.*)$'))[1];
END;
$$;

-- fail (remote node not existing)
SELECT * FROM worf.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := (SELECT provider_dsn FROM worf_regress_variables()) || ' user=nonreplica',
	forward_origins := '{}');

\c :provider_dsn
-- succeed
SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := (SELECT provider_dsn FROM worf_regress_variables()) || ' user=nonreplica');

\c :subscriber_dsn
\set VERBOSITY terse

-- fail (can't connect with replication connection to remote)
DO $$
BEGIN
    SELECT * FROM worf.create_subscription(
        subscription_name := 'test_subscription',
        provider_dsn := (SELECT provider_dsn FROM worf_regress_variables()) || ' user=nonreplica',
            forward_origins := '{}');
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '%', split_part(SQLERRM, ':', 1);
END;
$$;
-- cleanup

SELECT * FROM worf.drop_node('test_subscriber');
DROP EXTENSION worf;

\c :provider_dsn
SELECT * FROM worf.drop_node('test_provider');

SET client_min_messages = 'warning';
DROP OWNED BY nonreplica;
DROP ROLE IF EXISTS nonreplica;
DROP EXTENSION worf;
