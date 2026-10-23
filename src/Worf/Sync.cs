[assembly: PgSql("local-sync-status-table", """
    CREATE TABLE worf.local_sync_status (
        sync_kind "char" NOT NULL CHECK (sync_kind IN ('i', 's', 'd', 'f')),
        sync_subid oid NOT NULL REFERENCES worf.subscription(sub_id),
        sync_nspname name,
        sync_relname name,
        sync_status "char" NOT NULL,
    	sync_statuslsn pg_lsn NOT NULL,
        UNIQUE (sync_subid, sync_nspname, sync_relname)
    );
    """, Requires = ["node-tables"])]

namespace Worf;

/// <summary>
/// worf initial synchronization.
/// </summary>
internal static class Sync
{
}
