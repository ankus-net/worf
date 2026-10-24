using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The additional state for each ObjectAddress that deletion processing plans to delete.
/// </summary>
/// <param name="Flags">Bitmask of the flag bits.</param>
/// <param name="Dependee">Object whose deletion forced this one.</param>
internal readonly record struct ObjectAddressExtra(int Flags, ObjectAddress Dependee);
