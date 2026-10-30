namespace Worf;

/// <summary>
/// The type a remote column's values are sent in, as the provider's RELATION message names it: its OID, which is the
/// same on every node for PostgreSQL's own types, and for other types, which each node numbers itself, its schema, name
/// and definition.
/// </summary>
/// <param name="Oid">The type's OID on the provider.</param>
/// <param name="Nspname">The type's schema, for a type that isn't PostgreSQL's own.</param>
/// <param name="Typname">The type's name, for a type that isn't PostgreSQL's own.</param>
/// <param name="Definition">What reading the type's values depends on, as <see cref="ProtoNative.TypeDefinition"/>
/// describes it, for a type that isn't PostgreSQL's own.</param>
internal readonly record struct WorfRemoteType(uint Oid, string? Nspname, string? Typname, string? Definition);
