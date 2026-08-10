package nativewire

import (
	"bytes"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestVectorPartitionStrictSearchCapabilityRejectsMutationV1(t *testing.T) {
	key := bytes.Repeat([]byte{0x5a}, 32)
	request := VectorPartitionShardSearchRequestV1{
		Version: VectorPartitionShardSearchVersionV1, RequestID: "request", CancellationID: "cancel",
		Database: "default", Catalog: "default", Collection: "docs", IndexName: "embedding",
		IndexDefinitionDigest: string(bytes.Repeat([]byte{'a'}, 64)), ReadySetDigest: string(bytes.Repeat([]byte{'b'}, 64)),
		SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4,
		PartitionGeneration: 5, RouterGeneration: 5, TargetGroupID: "group-a", PartitionIDs: []uint32{1},
		Query: []float32{1, 2}, Metric: VectorPartitionShardSearchMetricCosineV1,
		Mode: VectorPartitionShardSearchModeNoDocumentV1, Consistency: VectorPartitionShardSearchConsistencySnapshotV1,
		StatsMode: VectorPartitionShardSearchStatsBasicV1, TopK: 1, EfSearch: 8,
		DeadlineUnixNano: time.Now().Add(time.Minute).UnixNano(), RequestBytesLimit: 4096, CandidateBytesLimit: 4096, ResponseBytesLimit: 4096,
	}
	identity := VectorPartitionServingSnapshotIdentityV1{
		ServingIdentityDigest: string(bytes.Repeat([]byte{'c'}, 64)), CatalogEpoch: 1,
		CatalogDigest: string(bytes.Repeat([]byte{'d'}, 64)),
	}
	proof := raftcluster.CatalogMetaReadProofV1{
		NodeID: "meta-a", GroupID: "meta", LeaderTerm: 2, CatalogAppliedIndex: 7,
		CommitIndex: 8, RaftAppliedIndex: 8, ValidThroughUnixNano: time.Now().Add(time.Minute).UnixNano(), QuorumVerified: true,
	}
	capability, err := newVectorPartitionStrictSearchCapabilityV1(request, identity, proof, 9, key)
	if err != nil {
		t.Fatal(err)
	}
	request.StrictCapability = capability
	if err := validateVectorPartitionStrictSearchCapabilityV1(request, key); err != nil {
		t.Fatal(err)
	}

	mutated := request
	mutated.PartitionGeneration++
	if err := validateVectorPartitionStrictSearchCapabilityV1(mutated, key); err == nil {
		t.Fatal("capability accepted another generation")
	}
	if err := validateVectorPartitionStrictSearchCapabilityV1(request, bytes.Repeat([]byte{0x6b}, 32)); err == nil {
		t.Fatal("capability accepted another server key")
	}
	expired := *capability
	expired.ValidThroughUnixNano = time.Now().Add(-time.Second).UnixNano()
	expired.MAC = ""
	request.StrictCapability = &expired
	expired.MAC, err = vectorPartitionStrictSearchCapabilityMACV1(request, expired, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateVectorPartitionStrictSearchCapabilityV1(request, key); err == nil {
		t.Fatal("capability accepted an expired proof")
	}
}
