namespace Worf;

/// <summary>
/// A remote column's type, as the provider's RELATION message names it: its OID, which is the same on every node for
/// PostgreSQL's own types, and for other types, which each node numbers itself, its schema and name.
/// </summary>
/// <param name="Oid">The type's OID on the provider.</param>
/// <param name="Nspname">The type's schema, for a type that isn't PostgreSQL's own.</param>
/// <param name="Typname">The type's name, for a type that isn't PostgreSQL's own.</param>
internal readonly record struct WorfRemoteType(uint Oid, string? Nspname, string? Typname);
