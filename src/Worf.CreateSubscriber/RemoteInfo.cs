namespace Worf.CreateSubscriber;

/// <summary>
/// The provider node's identity.
/// </summary>
/// <param name="NodeId">The node's id.</param>
/// <param name="NodeName">The node's name.</param>
/// <param name="Sysid">The provider's system identifier.</param>
/// <param name="Dbname">The node's database.</param>
/// <param name="ReplicationSets">The node's replication sets.</param>
internal sealed record RemoteInfo(uint NodeId, string NodeName, string Sysid, string Dbname, string ReplicationSets);
