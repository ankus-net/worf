---
title: Conflict history
description: Record resolved conflicts in a table you can query.
---

The **Conflict History** feature records replication conflicts to a queryable table instead of just logging them to the PostgreSQL log files.

## Overview

When replication conflicts occur (e.g., the same row is modified on multiple nodes), worf detects and resolves them automatically. Previously, these conflicts were only logged to PostgreSQL's log files, making them difficult to query and analyze.

With Conflict History, conflicts are now recorded to the `worf.conflict_history` table, allowing DBAs to:

- Query conflicts using standard SQL
- Analyze conflict patterns over time
- Inspect the actual tuple data involved in conflicts
- Monitor replication health with aggregate statistics

## Enabling Conflict History

Conflict history recording is **disabled by default**. To enable it:

```sql
-- Enable on a running system (requires superuser)
ALTER SYSTEM SET worf.conflict_history_enabled = on;
SELECT pg_reload_conf();

-- Verify it's enabled
SHOW worf.conflict_history_enabled;
```

To enable permanently, add to `postgresql.conf`:

```
worf.conflict_history_enabled = on
```

**Note:** This setting must be enabled on each subscriber node where you want to record conflicts.

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `worf.conflict_history_enabled` | `off` | Enable/disable conflict recording |
| `worf.conflict_history_store_tuples` | `on` | Store local/remote tuple data as JSONB |
| `worf.conflict_history_max_tuple_size` | `1024` | Maximum bytes per tuple field (64-65536) |

### Example: Disable Tuple Storage

If you want to record conflicts but not store the actual tuple data (to save space):

```sql
ALTER SYSTEM SET worf.conflict_history_store_tuples = off;
SELECT pg_reload_conf();
```

## Querying Conflicts

### Basic Query

```sql
SELECT * FROM worf.conflict_history;
```

### View Recent Conflicts (Last 24 Hours)

```sql
SELECT * FROM worf.recent_conflicts;
```

### Conflict Summary by Table

```sql
SELECT * FROM worf.conflict_summary;
```

### Detailed Conflict with Tuple Data

```sql
SELECT
    recorded_at,
    conflict_type,
    resolution,
    schema_name,
    table_name,
    local_tuple,
    remote_tuple
FROM worf.conflict_history
WHERE table_name = 'orders'
ORDER BY recorded_at DESC
LIMIT 10;
```

### Extract Specific Fields from Tuple JSONB

```sql
SELECT
    recorded_at,
    conflict_type,
    local_tuple->>'id' AS local_id,
    local_tuple->>'amount' AS local_amount,
    remote_tuple->>'id' AS remote_id,
    remote_tuple->>'amount' AS remote_amount
FROM worf.conflict_history
WHERE table_name = 'orders';
```

### Conflicts by Subscription

```sql
SELECT * FROM worf.show_subscription_conflicts('my_subscription');

-- With time range
SELECT * FROM worf.show_subscription_conflicts(
    'my_subscription',
    now() - interval '1 hour',
    100
);
```

### Aggregate Statistics

```sql
SELECT * FROM worf.conflict_stats();
```

Returns:
- `total_conflicts` - Total conflicts ever recorded
- `last_24h` - Conflicts in the last 24 hours
- `last_hour` - Conflicts in the last hour
- `tables_affected` - Number of distinct tables with conflicts

## Table Schema

The `worf.conflict_history` table contains:

| Column | Type | Description |
|--------|------|-------------|
| `id` | `BIGINT` | Unique identifier |
| `recorded_at` | `TIMESTAMPTZ` | When the conflict was recorded |
| `sub_id` | `OID` | Subscription OID |
| `sub_name` | `NAME` | Subscription name |
| `conflict_type` | `TEXT` | Type: `insert_insert`, `update_update`, `update_delete`, `delete_delete` |
| `resolution` | `TEXT` | Resolution: `apply_remote`, `keep_local`, `skip` |
| `schema_name` | `NAME` | Schema of affected table |
| `table_name` | `NAME` | Name of affected table |
| `index_name` | `NAME` | Index where conflict was detected |
| `local_tuple` | `JSONB` | Local row data (if tuple storage enabled) |
| `local_xid` | `XID` | Transaction ID of local tuple |
| `local_origin` | `INTEGER` | Replication origin of local tuple |
| `local_commit_ts` | `TIMESTAMPTZ` | Commit timestamp of local tuple |
| `remote_tuple` | `JSONB` | Remote row data (if tuple storage enabled) |
| `remote_origin` | `INTEGER` | Replication origin of remote change |
| `remote_commit_ts` | `TIMESTAMPTZ` | Commit timestamp on remote |
| `remote_commit_lsn` | `PG_LSN` | LSN of remote commit |
| `has_before_triggers` | `BOOLEAN` | Whether BEFORE triggers modified the tuple |

## Partition Management

The `conflict_history` table is partitioned by month for efficient data management.

### Partitions are Created Automatically

When a conflict is recorded, worf automatically creates the partition for the current month if it doesn't exist.

### Manual Partition Creation

```sql
-- Create partition for a specific month
SELECT worf.conflict_history_ensure_partition('2025-02-01'::DATE);
```

### Cleanup Old Partitions

```sql
-- Remove partitions older than 30 days
SELECT worf.conflict_history_cleanup(30);

-- Remove partitions older than 90 days
SELECT worf.conflict_history_cleanup(90);
```

The cleanup function returns the number of partitions dropped.

## Conflict Types

| Type | Description |
|------|-------------|
| `insert_insert` | Same primary key inserted on multiple nodes |
| `update_update` | Same row updated on multiple nodes |
| `update_delete` | Row updated on one node, deleted on another |
| `delete_delete` | Row deleted on multiple nodes |

## Resolution Types

| Resolution | Description |
|------------|-------------|
| `apply_remote` | Remote change was applied (remote wins) |
| `keep_local` | Local data was kept (local wins) |
| `skip` | Change was skipped |

## Best Practices

1. **Enable on all subscriber nodes** - Each node records conflicts it detects when applying changes.

2. **Set up regular cleanup** - Use `conflict_history_cleanup()` in a scheduled job:
   ```sql
   -- Run weekly to keep 90 days of history
   SELECT worf.conflict_history_cleanup(90);
   ```

3. **Monitor conflict rates** - High conflict counts may indicate application issues:
   ```sql
   SELECT * FROM worf.conflict_stats();
   ```

4. **Investigate patterns** - Use the summary view to find problematic tables:
   ```sql
   SELECT * FROM worf.conflict_summary
   ORDER BY conflict_count DESC;
   ```

5. **Disable tuple storage for high-volume systems** - If conflicts are frequent and you don't need tuple data, disable storage to save space.

## Limitations

- Conflict history is recorded on the **subscriber** side only (where conflicts are detected during apply)
- Very large tuple values are truncated based on `worf.conflict_history_max_tuple_size`
- External TOAST values are stored as `"(unchanged-toast-datum)"` rather than the actual value
- Recording failures do not abort replication - they are logged as warnings and replication continues
