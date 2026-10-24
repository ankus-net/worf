using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Support for monitoring and progress tracking.
/// </summary>
internal static class Monitoring
{
    /// <summary>
    /// Wait for the confirmed_flush_lsn of the specified slot, or all logical slots if none given, to pass the
    /// supplied value. If no position is supplied the write position is used. No timeout is offered, use a
    /// statement_timeout.
    /// </summary>
    /// <param name="slotname">The slot, or null for every slot.</param>
    /// <param name="target">The position to wait for, or null.</param>
    [PgFunction(Sql = """
        CREATE FUNCTION
        worf.wait_slot_confirm_lsn(slotname name, target pg_lsn)
        RETURNS void LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void WaitSlotConfirmLsn([PgSqlType("name", Schema = "pg_catalog")] PgDatum? slotname,
        [PgSqlType("pg_lsn", Schema = "pg_catalog")] PgDatum? target)
    {
        string? slotName = slotname is null || slotname.IsNull ? null : slotname.ToPostgresString();
        ulong targetLsn;

        if (target is not null && !target.IsNull)
        {
            targetLsn = target.DangerousGetBits();
        }
        else
        {
            unsafe
            {
                targetLsn = NativeGlobals.XactLastCommitEnd == 0 ? NativeMethods.GetXLogInsertRecPtr() : NativeGlobals.XactLastCommitEnd;
            }
        }

        PgLog.Debug1($"waiting for {slotName ?? "all local slots"} to pass confirmed_flush position {Worf.FormatLsn(targetLsn)}");

        while (true)
        {
            ulong oldestConfirmedLsn = 0;
            string? oldestSlot = null;

            SpiResult slots = Spi.Select("SELECT slot_name::text, confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots");
            foreach (SpiRow s in slots)
            {
                string name = s.Get<string>(0);
                ulong confirmedFlush = Worf.TryParseLsn(s.Get<string?>(1) ?? "", out ulong lsn) ? lsn : 0;

                if (slotName is not null && slotName != name)
                {
                    continue;
                }

                if (oldestConfirmedLsn == 0 || (confirmedFlush != 0 && confirmedFlush < oldestConfirmedLsn))
                {
                    oldestConfirmedLsn = confirmedFlush;
                    oldestSlot = name;
                }
            }

            if (oldestSlot is not null)
            {
                PgLog.Debug2($"oldest confirmed lsn is {Worf.FormatLsn(oldestConfirmedLsn)} on slot '{oldestSlot}', {(uint)(targetLsn - oldestConfirmedLsn)} bytes left until {Worf.FormatLsn(targetLsn)}");
            }

            if (oldestConfirmedLsn >= targetLsn)
            {
                break;
            }

            // Poll for slot progress. On Windows, use a shorter interval for faster response times since IPC can be
            // slower.
            Worker.WaitLatch(OperatingSystem.IsWindows() ? 200 : 1000);

            PgInterrupts.Check();
        }
    }
}
