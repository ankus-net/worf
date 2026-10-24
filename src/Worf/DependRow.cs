using Ankus.Postgres;

namespace Worf;

/// <summary>
/// A worf.depend row found by a scan.
/// </summary>
/// <param name="Ctid">The row's location.</param>
/// <param name="ClassId">The other object's catalog.</param>
/// <param name="ObjectId">The other object.</param>
/// <param name="ObjectSubId">The other object's column, or 0.</param>
/// <param name="DepType">The dependency type.</param>
internal readonly record struct DependRow(ItemPointerData Ctid, uint ClassId, uint ObjectId, int ObjectSubId, sbyte DepType);
