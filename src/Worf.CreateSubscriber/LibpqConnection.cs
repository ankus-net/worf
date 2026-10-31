namespace Worf.CreateSubscriber;

/// <summary>
/// A libpq connection.
/// </summary>
internal sealed class LibpqConnection : IDisposable
{
    private nint _conn;

    /// <summary>
    /// Initializes a new instance of the <see cref="LibpqConnection"/> class.
    /// </summary>
    /// <param name="conn">The open connection.</param>
    internal LibpqConnection(nint conn) => _conn = conn;

    /// <summary>
    /// Gets a value indicating whether the connection is still good.
    /// </summary>
    public bool IsOk => _conn != 0 && Libpq.IsOk(_conn);

    /// <summary>
    /// Runs a command and reads every row it returns as text, as <c>PQexec</c> returns them.
    /// </summary>
    /// <param name="sql">The command.</param>
    /// <returns>The rows.</returns>
    /// <exception cref="LibpqException">The command failed, with libpq's message.</exception>
    public List<string?[]> Query(string sql)
    {
        ObjectDisposedException.ThrowIf(_conn == 0, this);
        return Libpq.Exec(_conn, sql);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_conn != 0)
        {
            Libpq.Finish(_conn);
            _conn = 0;
        }
    }
}
