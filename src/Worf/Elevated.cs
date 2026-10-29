using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Runs worf's own catalog work as the bootstrap superuser from hooks and triggers that run with a user's permissions,
/// in a security-restricted operation with the search path set to <c>pg_catalog, pg_temp</c>, so the user's objects
/// can't take part. Settings changed inside are undone when it returns.
/// </summary>
internal static unsafe class Elevated
{
    // BOOTSTRAP_SUPERUSERID from catalog/pg_authid_d.h.
    private const uint BootstrapSuperuserId = 10;

    // SECURITY_LOCAL_USERID_CHANGE and SECURITY_RESTRICTED_OPERATION from miscadmin.h.
    private const int SecurityLocalUseridChange = 0x0001;
    private const int SecurityRestrictedOperation = 0x0002;

    /// <summary>
    /// Runs work elevated.
    /// </summary>
    /// <typeparam name="T">The work's result.</typeparam>
    /// <param name="work">The work.</param>
    /// <returns>Its result.</returns>
    internal static T Run<T>(Func<T> work)
    {
        uint userId;
        int secContext;
        NativeMethods.GetUserIdAndSecContext(&userId, &secContext);
        int nestLevel = NativeMethods.NewGUCNestLevel();
        NativeMethods.SetUserIdAndSecContext(BootstrapSuperuserId, secContext | SecurityLocalUseridChange | SecurityRestrictedOperation);
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

    /// <summary>
    /// Runs work elevated.
    /// </summary>
    /// <param name="work">The work.</param>
    internal static void Run(Action work) => _ = Run(() =>
    {
        work();
        return true;
    });
}
