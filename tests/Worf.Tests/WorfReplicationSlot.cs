using Npgsql.Replication;
using Npgsql.Replication.Internal;

namespace Worf.Tests;

/// <summary>
/// A worf replication slot that a test streams from itself, as a role with REPLICATION can.
/// </summary>
/// <param name="name">The slot's name.</param>
internal sealed class WorfReplicationSlot(string name) : LogicalReplicationSlot("worf", new ReplicationSlotOptions(name));
