using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Runs worf's queries over a user's table as the table's owner, in a security-restricted operation with the search
/// path set to <c>pg_catalog, pg_temp</c>, as VACUUM and ANALYZE do, and with the owner's own row-level security
/// lifted, as a foreign key check does. The functions the planner and executor run for the table, as from its
/// constraints and index predicates, then run with its owner's rights alone and can't change the session. Settings
/// changed inside are undone when it returns.
/// </summary>
internal static unsafe class TableOwner
{
    // SECURITY_LOCAL_USERID_CHANGE, SECURITY_RESTRICTED_OPERATION and SECURITY_NOFORCE_RLS from miscadmin.h.
    private const int SecurityLocalUseridChange = 0x0001;
    private const int SecurityRestrictedOperation = 0x0002;
    private const int SecurityNoforceRls = 0x0004;

    /// <summary>
    /// Runs work as a table's owner.
    /// </summary>
    /// <typeparam name="T">The work's result.</typeparam>
    /// <param name="relid">The table.</param>
    /// <param name="work">The work.</param>
    /// <returns>Its result.</returns>
    internal static T Run<T>(uint relid, Func<T> work)
    {
        uint owner = Worf.ReadScalar<uint?>("SELECT relowner FROM pg_catalog.pg_class WHERE oid = $1", SpiParameter.Create(relid))
            ?? throw new PgException(PgSqlStates.UndefinedTable, $"relation {relid} does not exist");
        uint userId;
        int secContext;
        NativeMethods.GetUserIdAndSecContext(&userId, &secContext);
        int nestLevel = NativeMethods.NewGUCNestLevel();
        NativeMethods.SetUserIdAndSecContext(owner, secContext | SecurityLocalUseridChange | SecurityRestrictedOperation | SecurityNoforceRls);
        try
        {
            fixed (byte* name = Worf.ToServerCString("search_path"))
            fixed (byte* value = Worf.ToServerCString("pg_catalog, pg_temp"))
            {
                _ = NativeMethods.set_config_option((sbyte*)name, (sbyte*)value, GucContext.PGC_USERSET, GucSource.PGC_S_SESSION,
                    GucAction.GUC_ACTION_SAVE, true, 0, false);
            }

            return work();
        }
        finally
        {
            NativeMethods.AtEOXact_GUC(true, nestLevel);
            NativeMethods.SetUserIdAndSecContext(userId, secContext);
        }
    }
}
