namespace Worf.CreateSubscriber;

/// <summary>
/// A connection or command that failed, with libpq's message, as <c>PQerrorMessage</c> or
/// <c>PQresultErrorMessage</c> formats it.
/// </summary>
internal sealed class LibpqException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="LibpqException"/> class.
    /// </summary>
    public LibpqException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="LibpqException"/> class.
    /// </summary>
    /// <param name="message">libpq's message.</param>
    public LibpqException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="LibpqException"/> class.
    /// </summary>
    /// <param name="message">libpq's message.</param>
    /// <param name="innerException">The cause.</param>
    public LibpqException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
