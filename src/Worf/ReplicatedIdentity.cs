using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Runs SQL that another node queued as the role that ran it there: the role becomes the session user, as it was in
/// that node's session, so that the SQL, and the functions it calls, can set and reset the role as they could there,
/// and no further. Changing the session authorization, which apply's own superuser could do, is refused meanwhile,
/// unless the role is a superuser, as it would be refused there.
/// </summary>
internal static unsafe partial class ReplicatedIdentity
{
    // The check hook PostgreSQL gives session_authorization, which this one passes changes on to, and whether replicated
    // SQL runs now as a role that isn't a superuser.
    private static GucStringCheckHook s_check;
    private static bool s_installed;
    private static bool s_refusing;

    // The session user, and the user and security context, before the role took over, which a background worker's
    // session_authorization setting, never set, doesn't give back.
    private static uint s_sessionUser;
    private static bool s_sessionSuperuser;
    private static uint s_user;
    private static int s_securityContext;

    [PgNativeCallback(nameof(CheckSessionAuthorization))]
    private static partial GucStringCheckHook SessionAuthorizationCheck { get; }

    /// <summary>
    /// Makes a role the session user until <see cref="Leave"/>, at a GUC nesting level of its own.
    /// </summary>
    /// <param name="role">The role.</param>
    /// <returns>The nesting level, for <see cref="Leave"/>.</returns>
    internal static int Enter(string role)
    {
        Install();
        s_sessionUser = NativeMethods.GetSessionUserId();
        s_sessionSuperuser = NativeMethods.superuser_arg(s_sessionUser);
        uint user;
        int securityContext;
        NativeMethods.GetUserIdAndSecContext(&user, &securityContext);
        (s_user, s_securityContext) = (user, securityContext);
        int nestLevel = NativeMethods.NewGUCNestLevel();
        fixed (byte* name = Worf.ToServerCString("session_authorization"))
        fixed (byte* value = Worf.ToServerCString(role))
        {
            _ = NativeMethods.set_config_option((sbyte*)name, (sbyte*)value, GucContext.PGC_SUSET, GucSource.PGC_S_SESSION,
                GucAction.GUC_ACTION_SAVE, true, 0, false);
        }

        s_refusing = !NativeMethods.superuser_arg(NativeMethods.GetSessionUserId());
        return nestLevel;
    }

    /// <summary>
    /// Gives the session its user back, and undoes every setting made since <see cref="Enter"/>, as the role too.
    /// </summary>
    /// <param name="nestLevel">The nesting level <see cref="Enter"/> returned.</param>
    internal static void Leave(int nestLevel)
    {
        s_refusing = false;
        NativeMethods.AtEOXact_GUC(false, nestLevel);
        NativeMethods.SetSessionAuthorization(s_sessionUser, s_sessionSuperuser);
        NativeMethods.SetUserIdAndSecContext(s_user, s_securityContext);
    }

    /// <summary>
    /// Puts this hook in front of PostgreSQL's own for session_authorization, once in each process.
    /// </summary>
    private static void Install()
    {
        if (s_installed)
        {
            return;
        }

        GucStringCheckHook* hook = GucOption.CheckHook("session_authorization");
        s_check = *hook;
        *hook = SessionAuthorizationCheck;

        s_installed = true;
    }

    /// <summary>
    /// Refuses a change of the session authorization while replicated SQL runs as a role that isn't a superuser, as
    /// the role's own session refuses it, and otherwise checks it as PostgreSQL does.
    /// </summary>
    private static bool CheckSessionAuthorization(sbyte** newval, void** extra, GucSource source)
    {
        if (s_refusing)
        {
            NativeMethods.GUC_check_errcode(SqlStateCode("42501"));
            NativeGlobals.GUC_check_errmsg_string = Worf.PStrDup("permission denied to set session authorization in SQL another node replicated");
            return false;
        }

        return s_check.IsNull || s_check.Invoke(newval, extra, source);
    }

    /// <summary>
    /// Packs a SQLSTATE as PostgreSQL's MAKE_SQLSTATE does.
    /// </summary>
    private static int SqlStateCode(string state)
    {
        int code = 0;
        for (int i = 0; i < 5; i++)
        {
            code += ((state[i] - '0') & 0x3F) << (6 * i);
        }

        return code;
    }
}
