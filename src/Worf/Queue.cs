[assembly: PgSql("queue-table", """
    CREATE TABLE worf.queue (
        queued_at timestamp with time zone NOT NULL,
        role name NOT NULL,
        replication_sets text[],
        message_type "char" NOT NULL,
        message json NOT NULL
    );
    """)]

namespace Worf;

/// <summary>
/// worf queue of replicated commands.
/// </summary>
internal static class Queue
{
}
