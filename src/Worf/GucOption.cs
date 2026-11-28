using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Finds the hooks of a string setting, which worf puts its own in front of: through <c>find_option</c> and
/// <c>config_string</c> from PostgreSQL 16, which PostgreSQL 19 keeps inside <c>config_generic</c>, and before 16, by
/// the setting's name among them all, and at the place PostgreSQL 15's <c>config_string</c> keeps them, after its two
/// pointers.
/// </summary>
internal static unsafe class GucOption
{
    /// <summary>
    /// Finds a string setting's check hook.
    /// </summary>
    /// <param name="name">The setting.</param>
    /// <returns>Where the hook is kept.</returns>
    internal static GucStringCheckHook* CheckHook(string name) => (GucStringCheckHook*)Hook(name, 0);

    /// <summary>
    /// Finds a string setting's show hook.
    /// </summary>
    /// <param name="name">The setting.</param>
    /// <returns>Where the hook is kept.</returns>
    internal static GucShowHook* ShowHook(string name) => (GucShowHook*)Hook(name, 2);

    /// <summary>
    /// Finds one of a string setting's hooks, by its place after the check hook.
    /// </summary>
    private static void* Hook(string name, int after)
    {
#if ANKUS_PG15
        config_generic** options = NativeMethods.get_guc_variables();
        int count = NativeMethods.GetNumConfigOptions();
        for (int i = 0; i < count; i++)
        {
            if (string.Equals(Worf.FromServer(options[i]->name), name, StringComparison.OrdinalIgnoreCase))
            {
                return (byte*)options[i] + sizeof(config_generic) + ((2 + after) * sizeof(nint));
            }
        }

        throw new PgException(PgSqlStates.UndefinedObject, $"unrecognized configuration parameter \"{name}\"");
#elif ANKUS_PG16 || ANKUS_PG17 || ANKUS_PG18
        fixed (byte* cname = Worf.ToServerCString(name))
        {
            config_string* option = (config_string*)NativeMethods.find_option((sbyte*)cname, false, false, 20);
            return after == 0 ? &option->check_hook : &option->show_hook;
        }
#else
        // From PostgreSQL 19, a setting's own part is in its config_generic, rather than around it.
        fixed (byte* cname = Worf.ToServerCString(name))
        {
            config_generic* option = NativeMethods.find_option((sbyte*)cname, false, false, 20);
            return after == 0 ? &option->_string.check_hook : &option->_string.show_hook;
        }
#endif
    }
}
