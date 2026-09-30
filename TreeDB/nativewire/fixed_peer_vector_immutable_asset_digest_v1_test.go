package nativewire

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

var fixedPeerImmutableActiveValidationBenchmarkErrorV1 error

// This synthetic two-owner/64-pack fixture compares only ACTIVE record
// validation. It includes fresh ready-set digest recomputation in both arms;
// it excludes configuration setup, quorum reads, storage, and distributed search.
func BenchmarkFixedPeerImmutableActiveVectorRecordV1(b *testing.B) {
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection:            raftplacement.CollectionRefV1{Database: "db", Catalog: "catalog", Collection: "documents"},
			CollectionIncarnation: 1, IndexName: "embedding", IndexEpoch: 1, CatalogEpoch: 1,
			IndexDefinitionDigest: strings.Repeat("a", 64), CatalogDigest: strings.Repeat("b", 64),
		},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{
			Generation: 1, Checksum: 2, SchemaHash: 3, RowCount: 6400,
		},
		Generation: 1,
		Immutable: raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{
			ManifestDigest: strings.Repeat("c", 64), PlacementDigest: strings.Repeat("d", 64),
		},
	}
	vector := &FixedPeerTCPVectorConfigV1{Identity: identity}
	vector.Manifest.Generation, vector.Manifest.RouterGeneration = 1, 1
	vector.Manifest.RouterAsset = collections.VectorPartitionAssetV1{ID: "router", Bytes: 4096, Checksum: strings.Repeat("e", 64)}
	for partition := uint32(0); partition < 64; partition++ {
		owner := raftcluster.GroupID("group-b")
		if partition%2 != 0 {
			owner = "group-c"
		}
		vector.Placement.Partitions = append(vector.Placement.Partitions, raftplacement.VectorPartitionGroupV1{PartitionID: partition, GroupID: owner})
		vector.Manifest.Placements = append(vector.Manifest.Placements, collections.VectorPartitionPlacementV1{PartitionID: partition, GroupID: string(owner)})
		vector.Manifest.Assets = append(vector.Manifest.Assets, collections.VectorPartitionAssetV1{
			PartitionID: partition, ID: fmt.Sprintf("pack-%d", partition), Bytes: 8192, Checksum: strings.Repeat("f", 64),
		})
	}
	r := &FixedPeerTCPRuntimeV1{config: FixedPeerTCPConfigV1{Vector: cloneFixedPeerVectorConfigV1(vector)}}
	r.immutableVectorAssetDigests = fixedPeerImmutableVectorAssetDigestsV1(r.config.Vector)
	owners := fixedPeerVectorOwnerGroupsV1(r.config.Vector.Placement)
	record := raftplacement.VectorPartitionLifecycleRecordV1{
		Identity: identity, State: raftplacement.VectorPartitionLifecycleActiveV1, RequiredGroups: owners,
	}
	for _, owner := range owners {
		record.ReadyGroups = append(record.ReadyGroups, raftplacement.VectorPartitionLifecycleGroupReadyV1{
			GroupID: owner, AppliedIndex: 7, AssetSetDigest: vectorPartitionM8GroupAssetSetDigestV1(string(owner), vector.Manifest),
		})
	}
	var err error
	record.ReadySetDigest, err = raftplacement.VectorPartitionLifecycleReadySetDigestV1(identity, owners, record.ReadyGroups)
	if err != nil {
		b.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		validate func(raftplacement.VectorPartitionLifecycleRecordV1, []raftcluster.GroupID) error
	}{
		{"recomputed", r.validateImmutableActiveVectorRecordRecomputedV1},
		{"precomputed", r.validateImmutableActiveVectorRecordV1},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			var validationErr error
			for b.Loop() {
				validationErr = test.validate(record, owners)
				if validationErr != nil {
					b.Fatal(validationErr)
				}
			}
			fixedPeerImmutableActiveValidationBenchmarkErrorV1 = validationErr
		})
	}
}

// Exact validator body from signed base 8f43b4fc, retained only for the paired
// microbenchmark. No baseline implementation is selectable by production.
func (r *FixedPeerTCPRuntimeV1) validateImmutableActiveVectorRecordRecomputedV1(record raftplacement.VectorPartitionLifecycleRecordV1, owners []raftcluster.GroupID) error {
	identity := r.config.Vector.Identity
	if record.Identity != identity || record.Aborted ||
		record.State != raftplacement.VectorPartitionLifecycleActiveV1 || record.InvalidationEpoch != 0 ||
		!slices.Equal(record.RequiredGroups, owners) || len(record.ReadyGroups) != len(owners) || record.ReadySetDigest == "" {
		return ErrFixedPeerVectorProofStaleV1
	}
	for i, owner := range owners {
		ready := record.ReadyGroups[i]
		if ready.GroupID != owner || ready.AppliedIndex == 0 ||
			ready.AssetSetDigest != vectorPartitionM8GroupAssetSetDigestV1(string(owner), r.config.Vector.Manifest) {
			return ErrFixedPeerVectorProofStaleV1
		}
	}
	digest, err := raftplacement.VectorPartitionLifecycleReadySetDigestV1(identity, owners, record.ReadyGroups)
	if err != nil || digest != record.ReadySetDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	return nil
}
