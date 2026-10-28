DO $$
BEGIN
	IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') = 904 THEN
		CREATE EXTENSION IF NOT EXISTS worf_origin;
	END IF;
END;$$;
CREATE EXTENSION worf;

SELECT worf.worf_max_proto_version();

SELECT worf.worf_min_proto_version();

-- test extension version
SELECT worf.worf_version() = extversion
FROM pg_extension
WHERE extname = 'worf';

DROP EXTENSION worf;

-- test upgrades
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

SELECT worf.worf_version() = extversion
FROM pg_extension
WHERE extname = 'worf';

DROP EXTENSION worf;
