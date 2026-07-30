package main

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func testM3VariantDescriptorV1(dir string) m3VariantDescriptorV1 {
	hash := strings.Repeat("a", 64)
	d := m3VariantDescriptorV1{
		SchemaVersion: 4, ResultKind: "m3_persistent_variant_descriptor_v4", VariantID: "graph-overlap-020-v1",
		AssignmentBasis: partitionAssignmentGraphV1, OverlapRatio: .2,
		FixtureChecksum: hash, ArtifactSHA256: hash, GraphArtifactSHA256: hash, ArtifactBackend: "reference", Source: vectorpartition.Source{SourceID: "fixture", Checksum: hash, Vectors: 8, Dimensions: 2, Metric: "cosine"},
		DatabaseDirectory: dir, ManifestIntegrity: hash, ReadySetDigest: hash, RouterAssetChecksum: hash, RouterModelDigest: hash,
		SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRows: 8, PartitionGeneration: 4, RouterGeneration: 4,
		Partitions: 4, IndexDefinitionDigest: hash, PartitionHNSWM: 16, Capacity: 3, OverlapRequested: 1, OverlapRealized: 1, OverlapRejected: 0, OverlapUseful: 1, OverlapUnusedCapacity: 3, EdgeCutBefore: 2, EdgeCutAfter: 1, PartitionLoads: []int{3, 2, 2, 2}, OverlapMemberships: 1, PersistentAssetBytes: 1024,
	}
	d.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(d)
	d.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: 3, Budget: 1, Realized: 1, BuildIdentityDigest: d.BuildIdentityDigest})
	return d
}

func TestM3VariantDescriptorRoundTripAndImmutableCreateV1(t *testing.T) {
	dir := t.TempDir()
	want := testM3VariantDescriptorV1(dir)
	if err := m3WriteVariantDescriptorV1(dir, want); err != nil {
		t.Fatal(err)
	}
	got, err := m3ReadVariantDescriptorV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.VariantID != want.VariantID || got.ReadySetDigest != want.ReadySetDigest || len(got.PartitionLoads) != 4 {
		t.Fatalf("descriptor=%+v", got)
	}
	if err := m3WriteVariantDescriptorV1(dir, want); err == nil {
		t.Fatal("overwrote immutable variant descriptor")
	}
	if err := os.WriteFile(filepath.Join(dir, m3VariantDescriptorFileV1), []byte("{} {}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m3ReadVariantDescriptorV1(dir); err == nil {
		t.Fatal("accepted trailing or malformed descriptor JSON")
	}
}

func TestM3VariantDescriptorRejectsMalformedOverlapPolicyAccountingV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	for name, policy := range map[string]string{
		"realized exceeds budget": strings.Replace(d.OverlapPolicy, "realized=1", "realized=2", 1),
		"unspent mismatch":        strings.Replace(d.OverlapPolicy, "unspent=0", "unspent=1", 1),
	} {
		t.Run(name, func(t *testing.T) {
			candidate := d
			candidate.OverlapPolicy = policy
			if err := validateM3VariantDescriptorV1(candidate); err == nil {
				t.Fatalf("accepted malformed policy %q", policy)
			}
		})
	}
}

func TestM3VariantDescriptorRequiresDerivedExactTargetV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	d.OverlapRequested = 0
	d.OverlapRealized = 0
	d.OverlapMemberships = 0
	d.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(d)
	d.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: uint64(d.Capacity), Budget: 0, Realized: 0, BuildIdentityDigest: d.BuildIdentityDigest})
	if err := validateM3VariantDescriptorV1(d); err == nil {
		t.Fatal("accepted descriptor whose self-consistent accounting misses the ratio-derived target")
	}
}

func TestM3VariantBuildIdentityBindsExactOverlapTargetAndCapacityV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	baseline, err := m3VariantBuildIdentityDigestV1(d)
	if err != nil {
		t.Fatal(err)
	}
	targetChanged := d
	targetChanged.OverlapRequested++
	targetDigest, err := m3VariantBuildIdentityDigestV1(targetChanged)
	if err != nil || targetDigest == baseline {
		t.Fatalf("target identity baseline=%s changed=%s err=%v", baseline, targetDigest, err)
	}
	capacityChanged := d
	capacityChanged.Capacity++
	capacityDigest, err := m3VariantBuildIdentityDigestV1(capacityChanged)
	if err != nil || capacityDigest == baseline {
		t.Fatalf("capacity identity baseline=%s changed=%s err=%v", baseline, capacityDigest, err)
	}
}

func TestM3OverlapCapacityUsesExactGlobalTargetV1(t *testing.T) {
	artifact := vectorpartition.Artifact{IDs: make([]string, 1_000_000), Config: vectorpartition.Config{Partitions: 16}, Metrics: vectorpartition.Metrics{Cap: 65_625}}
	capacity, err := m3OverlapCapacityV1(artifact, .2)
	if err != nil {
		t.Fatal(err)
	}
	if capacity != 75_000 {
		t.Fatalf("capacity=%d want 75000", capacity)
	}
	if capacity, err = m3OverlapCapacityV1(artifact, 0); err != nil || capacity != 65_625 {
		t.Fatalf("disjoint capacity=%d err=%v", capacity, err)
	}
}

func TestM3OverlapCapacityAvoidsCeilAdditionOverflowV1(t *testing.T) {
	capacity, err := m3OverlapCapacityForRequestedV1(math.MaxInt-1, 1, 2, 0)
	if err != nil || capacity != math.MaxInt/2+1 {
		t.Fatalf("capacity=%d err=%v", capacity, err)
	}
}

func TestM3VariantDescriptorBindsReadyManifestV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	manifest := collections.VectorPartitionManifestV1{
		IntegrityDigest: d.ManifestIntegrity, ReadySetDigest: d.ReadySetDigest,
		IndexDefinitionDigest: d.IndexDefinitionDigest,
		RouterAsset:           collections.VectorPartitionAssetV1{Checksum: d.RouterAssetChecksum},
		SourceGeneration:      d.SourceGeneration, SourceChecksum: d.SourceChecksum, SourceSchemaHash: d.SourceSchemaHash, SourceRowCount: d.SourceRows,
		Generation: d.PartitionGeneration, RouterGeneration: d.RouterGeneration, PartitionCount: d.Partitions, BalancePolicy: d.OverlapPolicy,
		Memberships:        []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 0}, {VectorOrdinal: 2, PartitionID: 1}, {VectorOrdinal: 3, PartitionID: 1}, {VectorOrdinal: 4, PartitionID: 2}, {VectorOrdinal: 5, PartitionID: 2}, {VectorOrdinal: 6, PartitionID: 3}, {VectorOrdinal: 7, PartitionID: 3}},
		OverlapMemberships: []collections.VectorPartitionMembershipV1{{VectorOrdinal: 7, PartitionID: 0}},
		Assets:             []collections.VectorPartitionAssetV1{{Bytes: 200}, {Bytes: 200}, {Bytes: 200}, {Bytes: 200}},
	}
	manifest.RouterAsset.Bytes = 224
	fixture := fixtureManifest{Checksum: d.FixtureChecksum}
	if err := m3DescriptorMatchesManifestV1(d, fixture, manifest, d.RouterModelDigest); err != nil {
		t.Fatal(err)
	}
	manifest.ReadySetDigest = strings.Repeat("b", 64)
	if err := m3DescriptorMatchesManifestV1(d, fixture, manifest, d.RouterModelDigest); err == nil {
		t.Fatal("accepted descriptor for a different ready manifest")
	}
	manifest.ReadySetDigest = d.ReadySetDigest
	for name, mutate := range map[string]func(*m3VariantDescriptorV1){
		"assignment relabel": func(candidate *m3VariantDescriptorV1) {
			candidate.AssignmentBasis = partitionAssignmentStableIDHashV1
			candidate.OverlapRatio = 0
			candidate.VariantID = "stable-id-hash-disjoint-v1"
		},
		"artifact":         func(candidate *m3VariantDescriptorV1) { candidate.ArtifactSHA256 = strings.Repeat("b", 64) },
		"index definition": func(candidate *m3VariantDescriptorV1) { candidate.IndexDefinitionDigest = strings.Repeat("b", 64) },
		"capacity":         func(candidate *m3VariantDescriptorV1) { candidate.Capacity++ },
		"loads":            func(candidate *m3VariantDescriptorV1) { candidate.PartitionLoads[0]-- },
		"asset bytes":      func(candidate *m3VariantDescriptorV1) { candidate.PersistentAssetBytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := d
			candidate.PartitionLoads = append([]int(nil), d.PartitionLoads...)
			mutate(&candidate)
			if err := m3DescriptorMatchesManifestV1(candidate, fixture, manifest, candidate.RouterModelDigest); err == nil {
				t.Fatal("accepted descriptor mutation not covered by retained state")
			}
		})
	}
	relocated := d
	relocated.DatabaseDirectory = filepath.Join(t.TempDir(), "relocated")
	if err := m3DescriptorMatchesManifestV1(relocated, fixture, manifest, relocated.RouterModelDigest); err != nil {
		t.Fatalf("portable descriptor rejected after directory relocation: %v", err)
	}
}

func TestM3VariantIdentityV1(t *testing.T) {
	for _, tc := range []struct {
		assignment string
		ratio      float64
		want       string
	}{
		{partitionAssignmentGraphV1, 0, "graph-disjoint-v1"},
		{partitionAssignmentGraphV1, .2, "graph-overlap-020-v1"},
		{partitionAssignmentStableIDHashV1, 0, "stable-id-hash-disjoint-v1"},
	} {
		got, err := m3VariantIDV1(tc.assignment, tc.ratio)
		if err != nil || got != tc.want {
			t.Fatalf("variant (%q,%v)=%q err=%v want %q", tc.assignment, tc.ratio, got, err, tc.want)
		}
	}
	if _, err := m3VariantIDV1(partitionAssignmentStableIDHashV1, .2); err == nil {
		t.Fatal("accepted overlapping stable-ID hash baseline")
	}
}
