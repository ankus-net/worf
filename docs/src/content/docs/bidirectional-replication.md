---
title: Bidirectional replication
description: Replicate between two nodes in both directions.
---

This guide covers setting up and testing bidirectional (multi-master) replication between two PostgreSQL nodes using worf, including conflict detection, resolution, and the conflict history feature.

## Prerequisites

1. **worf installed** on both nodes (see [Installation](../getting-started/installation/))

2. **postgresql.conf** on both nodes:

```ini
wal_level = 'logical'
max_worker_processes = 10
max_replication_slots = 10
max_wal_senders = 10
shared_preload_libraries = 'worf'

# Required for last_update_wins / first_update_wins conflict resolution
track_commit_timestamp = on
```

3. **pg_hba.conf** - Ensure replication connections are allowed between nodes

4. Restart PostgreSQL after configuration changes

## Understanding Loopback Prevention

In bidirectional replication, changes flow both directions:
- Node A sends changes to Node B
- Node B sends changes to Node A

Without loopback prevention, a change from A→B would then flow B→A→B→A... infinitely.

**Solution:** The `forward_origins` parameter controls which replication origins are forwarded:
- `'{all}'` (default) - Forward all changes (for cascading setups)
- `'{}'` (empty) - Only forward locally-originated changes (for bidirectional)

When `forward_origins` is empty, changes received from the remote node are applied locally but NOT re-forwarded back, breaking the loop.

## Setup Overview

```
┌─────────────────┐              ┌─────────────────┐
│    Node A       │              │    Node B       │
│   (alpha_db)    │◄────────────►│   (beta_db)     │
│                 │              │                 │
│ - Provider node │              │ - Provider node │
│ - Subscriber to │              │ - Subscriber to │
│   Node B        │              │   Node A        │
└─────────────────┘              └─────────────────┘
```

Both nodes act as provider AND subscriber simultaneously.

## Step-by-Step Setup

### 1. Create Databases

```sql
-- On the PostgreSQL instance
CREATE DATABASE alpha_db;
CREATE DATABASE beta_db;
```

### 2. Set Up Node A (alpha_db)

```sql
\c alpha_db

-- Install extension
CREATE EXTENSION worf;

-- Create the provider node
SELECT worf.create_node(
    node_name := 'node_alpha',
    dsn := 'host=localhost port=5432 dbname=alpha_db user=postgres password=postgres'
);

-- Create test table
CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT now()
);

-- Add to default replication set
SELECT worf.replication_set_add_table('default', 'inventory', true);

-- Insert initial data
INSERT INTO inventory (product_name, quantity) VALUES
    ('Widget A', 100),
    ('Widget B', 200),
    ('Widget C', 150);
```

### 3. Set Up Node B (beta_db)

```sql
\c beta_db

-- Install extension
CREATE EXTENSION worf;

-- Create the provider node
SELECT worf.create_node(
    node_name := 'node_beta',
    dsn := 'host=localhost port=5432 dbname=beta_db user=postgres password=postgres'
);

-- Create matching table structure (must match exactly)
CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT now()
);

-- Add to default replication set
SELECT worf.replication_set_add_table('default', 'inventory', true);
```

Both nodes number new rows from their own `inventory_id_seq`, so rows inserted on both would share keys. Once the subscriptions below are up, give each node its own blocks of keys with [sequence ranges](../sequence-ranges/):

```sql
-- On node_alpha
SELECT worf.enable_sequence_range('inventory_id_seq', participants := '{node_beta}');
```

### 4. Create Subscriptions (Both Directions)

**On Node B - Subscribe to Node A:**

```sql
\c beta_db

SELECT worf.create_subscription(
    subscription_name := 'sub_to_alpha',
    provider_dsn := 'host=localhost port=5432 dbname=alpha_db user=postgres password=postgres',
    synchronize_data := true,
    forward_origins := '{}'  -- CRITICAL: Prevents loopback
);

-- Wait for initial sync
SELECT worf.wait_for_subscription_sync_complete('sub_to_alpha');
```

**On Node A - Subscribe to Node B:**

```sql
\c alpha_db

SELECT worf.create_subscription(
    subscription_name := 'sub_to_beta',
    provider_dsn := 'host=localhost port=5432 dbname=beta_db user=postgres password=postgres',
    synchronize_data := false,  -- Data already exists from Node B's sync
    forward_origins := '{}'     -- CRITICAL: Prevents loopback
);
```

### 5. Verify Subscriptions

```sql
-- On both nodes
SELECT * FROM worf.show_subscription_status();
```

Expected output shows `status = 'replicating'` and `forward_origins = {}`.

## Testing Bidirectional Replication

### Test 1: Changes from Node A to Node B

```sql
-- On alpha_db
\c alpha_db
UPDATE inventory SET quantity = 150, last_updated = now() WHERE product_name = 'Widget A';

-- On beta_db (verify replication)
\c beta_db
SELECT * FROM inventory WHERE product_name = 'Widget A';
-- Should show quantity = 150
```

### Test 2: Changes from Node B to Node A

```sql
-- On beta_db
\c beta_db
UPDATE inventory SET quantity = 250, last_updated = now() WHERE product_name = 'Widget B';

-- On alpha_db (verify replication)
\c alpha_db
SELECT * FROM inventory WHERE product_name = 'Widget B';
-- Should show quantity = 250
```

### Test 3: Simultaneous Non-Conflicting Changes

```sql
-- On alpha_db (update Widget A)
\c alpha_db
UPDATE inventory SET quantity = 175 WHERE product_name = 'Widget A';

-- On beta_db (update Widget C - different row)
\c beta_db
UPDATE inventory SET quantity = 300 WHERE product_name = 'Widget C';

-- Both changes should replicate to the other node
-- Verify on both nodes:
SELECT * FROM inventory ORDER BY id;
```

## Conflict Resolution

### Configure Conflict Resolution Strategy

```sql
-- Available options:
-- 'apply_remote'      - Remote change always wins (default)
-- 'keep_local'        - Local change always wins
-- 'last_update_wins'  - Most recent change wins (requires track_commit_timestamp)
-- 'first_update_wins' - Oldest change wins (requires track_commit_timestamp)
-- 'error'             - Raise error on conflict (requires manual intervention)

-- Set for current session
SET worf.conflict_resolution = 'last_update_wins';

-- Set permanently in postgresql.conf
-- worf.conflict_resolution = 'last_update_wins'
```

### Test 4: Creating and Resolving Conflicts

**Setup: Enable conflict history first**

```sql
-- On both nodes
SET worf.conflict_resolution = 'last_update_wins';
SET worf.conflict_history_enabled = true;
```

**Create a conflict:**

```sql
-- Step 1: Disable subscription on alpha to create conflict scenario
\c alpha_db
SELECT worf.alter_subscription_disable('sub_to_beta', true);

-- Step 2: Update same row on both nodes
\c alpha_db
UPDATE inventory SET quantity = 500, last_updated = now() WHERE id = 1;

\c beta_db
UPDATE inventory SET quantity = 600, last_updated = now() WHERE id = 1;

-- Step 3: Re-enable subscription - conflict will occur
\c alpha_db
SELECT worf.alter_subscription_enable('sub_to_beta', true);

-- The conflict is resolved based on worf.conflict_resolution setting
```

## Conflict History Feature

The conflict history feature records all conflicts to a queryable table.

### Enable Conflict History

```sql
-- Enable recording (can be set in postgresql.conf for persistence)
SET worf.conflict_history_enabled = true;

-- Optional: Control tuple storage
SET worf.conflict_history_store_tuples = true;  -- Store tuple data as JSONB
SET worf.conflict_history_max_tuple_size = 1024; -- Truncate large columns
```

### Query Conflict History

```sql
-- View recent conflicts (last 24 hours)
SELECT * FROM worf.recent_conflicts;

-- View conflict summary (last 7 days)
SELECT * FROM worf.conflict_summary;

-- Query specific conflicts
SELECT
    recorded_at,
    conflict_type,
    resolution,
    schema_name,
    table_name,
    local_tuple,
    remote_tuple
FROM worf.conflict_history
WHERE recorded_at > now() - interval '1 hour'
ORDER BY recorded_at DESC;

-- Get conflict statistics
SELECT * FROM worf.conflict_stats();
```

### Conflict History Table Structure

```sql
-- Main table (partitioned by month)
\d worf.conflict_history

-- Key columns:
-- id                  - Unique conflict ID
-- recorded_at         - When conflict was detected
-- sub_name            - Subscription that detected it
-- conflict_type       - insert_insert, update_update, update_delete, delete_delete
-- resolution          - apply_remote, keep_local, skip
-- schema_name         - Schema of affected table
-- table_name          - Name of affected table
-- local_tuple         - Local row data (JSONB)
-- remote_tuple        - Remote row data (JSONB)
-- local_commit_ts     - When local tuple was committed
-- remote_commit_ts    - When remote tuple was committed
```

### Manage Conflict History Partitions

```sql
-- Partitions are created automatically (monthly)
-- View existing partitions
SELECT c.relname
FROM pg_class c
JOIN pg_inherits i ON c.oid = i.inhrelid
JOIN pg_class p ON i.inhparent = p.oid
WHERE p.relname = 'conflict_history';

-- Clean up old partitions (keeps last 30 days by default)
SELECT worf.conflict_history_cleanup(retention_days := 30);

-- Manually ensure partition exists for a date
SELECT worf.conflict_history_ensure_partition('2026-02-01'::date);
```

## Monitoring Bidirectional Replication

### Check Subscription Status

```sql
SELECT
    subscription_name,
    status,
    provider_node,
    slot_name,
    forward_origins
FROM worf.show_subscription_status();
```

### Monitor Replication Lag

```sql
-- On provider node, check slot status
SELECT
    slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag
FROM pg_replication_slots
WHERE plugin = 'worf';
```

### Check for Conflicts

```sql
-- Quick conflict check
SELECT
    conflict_type,
    resolution,
    COUNT(*) as count,
    MAX(recorded_at) as last_occurrence
FROM worf.conflict_history
WHERE recorded_at > now() - interval '24 hours'
GROUP BY conflict_type, resolution;
```

## Best Practices

1. **Always use `forward_origins := '{}'`** for true bidirectional replication to prevent loops

2. **Use `last_update_wins`** for most bidirectional scenarios - it provides deterministic conflict resolution

3. **Enable `track_commit_timestamp`** for timestamp-based conflict resolution

4. **Enable conflict history** in production to monitor and analyze conflicts

5. **Use identical table structures** on all nodes - including constraints, defaults, and indexes

6. **Avoid simultaneous DDL** - coordinate schema changes to prevent conflicts

7. **Monitor replication lag** - high lag can increase conflict likelihood

8. **Test conflict scenarios** before production deployment

## Cleanup

```sql
-- On alpha_db
\c alpha_db
SELECT worf.drop_subscription('sub_to_beta');
SELECT worf.drop_node('node_alpha');

-- On beta_db
\c beta_db
SELECT worf.drop_subscription('sub_to_alpha');
SELECT worf.drop_node('node_beta');

-- Drop databases
\c postgres
DROP DATABASE alpha_db;
DROP DATABASE beta_db;
```

## Troubleshooting

### Subscription Stuck in 'initializing'

```sql
-- Check worker status
SELECT * FROM pg_stat_activity WHERE application_name LIKE 'worf%';

-- Check for errors in PostgreSQL logs
-- Look for "worf" or "logical replication" messages
```

### Conflicts Not Being Recorded

```sql
-- Verify conflict history is enabled
SHOW worf.conflict_history_enabled;

-- Check if partition exists for current month
SELECT worf.conflict_history_ensure_partition(CURRENT_DATE);
```

### Loopback Occurring (Infinite Replication)

```sql
-- Verify forward_origins is empty on both subscriptions
SELECT subscription_name, forward_origins
FROM worf.show_subscription_status();

-- If not empty, recreate subscription with forward_origins := '{}'
```

### Timestamp-Based Resolution Not Working

```sql
-- Verify track_commit_timestamp is enabled
SHOW track_commit_timestamp;
-- Must be 'on'

-- Verify conflict_resolution setting
SHOW worf.conflict_resolution;
-- Must be 'last_update_wins' or 'first_update_wins'
```
