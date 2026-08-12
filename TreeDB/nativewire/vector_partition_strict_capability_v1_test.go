package nativewire

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

var vectorPartitionStrictCapabilityBenchmarkSinkV1 *vectorPartitionStrictSearchCapabilityV1

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

	mutations := []struct {
		name   string
		mutate func(*VectorPartitionShardSearchRequestV1)
	}{
		{"deadline", func(r *VectorPartitionShardSearchRequestV1) { r.DeadlineUnixNano-- }},
		{"group", func(r *VectorPartitionShardSearchRequestV1) { r.TargetGroupID = "group-b" }},
		{"generation", func(r *VectorPartitionShardSearchRequestV1) { r.PartitionGeneration++ }},
		{"query", func(r *VectorPartitionShardSearchRequestV1) { r.Query = []float32{1, 3} }},
		{"request_limit", func(r *VectorPartitionShardSearchRequestV1) { r.RequestBytesLimit++ }},
		{"candidate_limit", func(r *VectorPartitionShardSearchRequestV1) { r.CandidateBytesLimit++ }},
		{"response_limit", func(r *VectorPartitionShardSearchRequestV1) { r.ResponseBytesLimit++ }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			mutated := request
			test.mutate(&mutated)
			if err := validateVectorPartitionStrictSearchCapabilityV1(mutated, key); err == nil {
				t.Fatal("capability accepted mutated request")
			}
		})
	}
	targetNode := request
	targetNode.TargetNodeID = "node-b"
	if err := validateVectorPartitionStrictSearchCapabilityV1(targetNode, key); err != nil {
		t.Fatalf("target-node exclusion changed: %v", err)
	}
	mutatedCapability := *request.StrictCapability
	mutatedCapability.ServingIdentityDigest = string(bytes.Repeat([]byte{'e'}, 64))
	request.StrictCapability = &mutatedCapability
	if err := validateVectorPartitionStrictSearchCapabilityV1(request, key); err == nil {
		t.Fatal("capability accepted mutated serving identity")
	}
	request.StrictCapability = capability
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

func BenchmarkVectorPartitionStrictSearchCapabilityV1(b *testing.B) {
	key := bytes.Repeat([]byte{0x5a}, sha256.Size)
	request := VectorPartitionShardSearchRequestV1{
		Version: VectorPartitionShardSearchVersionV1, RequestID: "request", CancellationID: "cancel",
		Database: "default", Catalog: "default", Collection: "docs", IndexName: "embedding",
		IndexDefinitionDigest: string(bytes.Repeat([]byte{'a'}, 64)), ReadySetDigest: string(bytes.Repeat([]byte{'b'}, 64)),
		SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 250_000,
		PartitionGeneration: 5, RouterGeneration: 5, TargetGroupID: "group-a", PartitionIDs: []uint32{1, 3},
		Query: make([]float32, 128), Metric: VectorPartitionShardSearchMetricCosineV1,
		Mode: VectorPartitionShardSearchModeNoDocumentV1, Consistency: VectorPartitionShardSearchConsistencySnapshotV1,
		StatsMode: VectorPartitionShardSearchStatsBasicV1, TopK: 10, EfSearch: 128,
		DeadlineUnixNano: time.Now().Add(time.Hour).UnixNano(), RequestBytesLimit: 4096, CandidateBytesLimit: 64 << 20, ResponseBytesLimit: 4096,
	}
	request.Query[0] = 1
	identity := VectorPartitionServingSnapshotIdentityV1{
		ServingIdentityDigest: string(bytes.Repeat([]byte{'c'}, 64)), CatalogEpoch: 1,
		CatalogDigest: string(bytes.Repeat([]byte{'d'}, 64)),
	}
	proof := raftcluster.CatalogMetaReadProofV1{
		NodeID: "meta-a", GroupID: "meta", LeaderTerm: 2, CatalogAppliedIndex: 7,
		CommitIndex: 8, RaftAppliedIndex: 8, ValidThroughUnixNano: time.Now().Add(time.Hour).UnixNano(), QuorumVerified: true,
	}
	b.Run("construct", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var err error
			vectorPartitionStrictCapabilityBenchmarkSinkV1, err = newVectorPartitionStrictSearchCapabilityV1(request, identity, proof, 9, key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	capability, err := newVectorPartitionStrictSearchCapabilityV1(request, identity, proof, 9, key)
	if err != nil {
		b.Fatal(err)
	}
	request.StrictCapability = capability
	b.Run("validate", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := validateVectorPartitionStrictSearchCapabilityV1(request, key); err != nil {
				b.Fatal(err)
			}
		}
	})
}
