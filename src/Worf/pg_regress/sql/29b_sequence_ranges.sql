-- Sequence range management on a node of its own: the owner draws from blocks above the values its table holds,
-- the guard checks hand-written values, and DDL that would let values escape is rejected.
SET client_min_messages = 'warning';
DROP DATABASE IF EXISTS sequence_ranges;
CREATE DATABASE sequence_ranges;

\c sequence_ranges
SET client_min_messages = 'warning';
CREATE EXTENSION worf;
SELECT worf.create_node(node_name := 'ranges', dsn := 'dbname=sequence_ranges') IS NOT NULL AS created;
RESET client_min_messages;

CREATE TABLE t (id bigserial PRIMARY KEY, v text);
INSERT INTO t (id, v) VALUES (500, 'legacy');
SELECT worf.enable_sequence_range('t_id_seq', participants := '{}', range_size := 1000);
SELECT state, active_lo, active_hi, next_lo, next_hi FROM worf.sequence_ranges;
SELECT tgname FROM pg_trigger WHERE tgrelid = 't'::regclass ORDER BY tgname;

-- Values from the sequence pass, and so does a value it has passed; a value ahead of it doesn't.
INSERT INTO t (v) VALUES ('a'), ('b') RETURNING id;
DELETE FROM t WHERE id = 502;
INSERT INTO t (id, v) VALUES (502, 'behind');
INSERT INTO t (id, v) VALUES (1000, 'ahead');
UPDATE t SET id = 1000 WHERE id = 502;
UPDATE t SET v = 'updated' WHERE id = 500;
INSERT INTO t (id, v) VALUES (500, 'upserted') ON CONFLICT (id) DO UPDATE SET v = excluded.v;
SELECT id, v FROM t ORDER BY id;

-- DDL that would let values escape.
ALTER SEQUENCE t_id_seq RESTART;
ALTER SEQUENCE t_id_seq RENAME TO renamed;
ALTER SEQUENCE t_id_seq SET UNLOGGED;
DROP TRIGGER worf_sequence_range_guard ON t;
CREATE OR REPLACE TRIGGER worf_sequence_range_guard AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger();
TRUNCATE t RESTART IDENTITY;
GRANT UPDATE ON SEQUENCE t_id_seq TO PUBLIC;
SELECT worf.alter_sequence_range('t_id_seq', 1);
SELECT worf.replication_set_add_sequence('default', 't_id_seq');

-- A column whose values the guard can't read.
CREATE SEQUENCE codes;
CREATE TABLE coded (code text PRIMARY KEY DEFAULT 'INV-' || nextval('codes'));
SELECT worf.enable_sequence_range('codes', participants := '{}');

-- Disabling keeps the blocks, and dropping the table drops the sequence's range state.
SELECT worf.disable_sequence_range('t_id_seq');
SELECT state FROM worf.sequence_ranges;
DROP TABLE t;
SELECT count(*) FROM worf.sequence_range;

\c regression
SET client_min_messages = 'warning';
DROP DATABASE sequence_ranges WITH (FORCE);
