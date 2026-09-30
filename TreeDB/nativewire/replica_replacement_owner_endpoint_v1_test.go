package nativewire

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestImmutableOwnerReplacementPrivateEndpointV1(t *testing.T) {
	testImmutableOwnerReplacementPrivateSourceWarmV1(t, true)
}

func assertReplacementOwnerEndpointSearchV1(t *testing.T, ctx context.Context, client *FixedPeerTCPClientV1, target, oldOwner *FixedPeerTCPRuntimeV1, command raftplacement.ReplicaReplacementBeginV1) {
	t.Helper()
	_, record, err := target.replacementOwnerCatalogV1(ctx, command, target.localDataV1(command.GroupID))
	if err != nil {
		t.Fatal(err)
	}
	identity := command.OwnerPreparation
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	request.Database, request.Catalog, request.Collection = identity.Index.Collection.Database, identity.Index.Collection.Catalog, identity.Index.Collection.Collection
	request.IndexName, request.IndexDefinitionDigest = identity.Index.IndexName, identity.Index.IndexDefinitionDigest
	request.SourceGeneration, request.SourceChecksum, request.SourceSchemaHash, request.SourceRowCount = identity.Source.Generation, identity.Source.Checksum, identity.Source.SchemaHash, identity.Source.RowCount
	request.PartitionGeneration, request.RouterGeneration, request.ReadySetDigest = identity.Generation, identity.Generation, record.ReadySetDigest
	request.TargetGroupID, request.TargetNodeID = command.GroupID, command.NewPeer.ID
	endpoint := client.config.Vector.ShardAddresses[command.GroupID][command.NewPeer.ID]
	conn, err := client.peerTransport.dialScope(ctx, endpoint, command.NewPeer.ID, "shard:"+string(command.GroupID))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := vectorPartitionShardSearchTCPDeadlineV1(ctx, 0, conn); err != nil {
		t.Fatal(err)
	}
	if err := writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{Request: &request}, vectorPartitionShardSearchTCPMaxFrameBytesV1); err != nil {
		t.Fatal(err)
	}
	frame, err := readVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPMaxFrameBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Error == nil || frame.Error.Code != VectorPartitionShardSearchErrorNotLeaderV1 || frame.Response != nil {
		t.Fatalf("prepared NONVOTER search must be typed NOT_LEADER without hits: %+v", frame)
	}
	// Preauthorizing the replacement address must preserve the ordinary old-voter
	// endpoint and route. The immutable public constructor excludes standby keys.
	if _, err := oldOwner.vector.ensureImmutableTopologyV1(ctx); err != nil {
		t.Fatal(err)
	}
	old := target.config.Vector.ShardAddresses[command.GroupID][command.OldNodeID]
	if _, err := client.peerTransport.ProbeShardEndpointV1(ctx, old, command.OldNodeID, command.GroupID); err != nil {
		t.Fatalf("old voter endpoint=%v", err)
	}
}
