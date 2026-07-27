package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func testM3VariantDescriptorV1(dir string) m3VariantDescriptorV1 {
	hash := strings.Repeat("a", 64)
	return m3VariantDescriptorV1{
		SchemaVersion: 1, ResultKind: "m3_persistent_variant_descriptor_v1", VariantID: "graph-overlap-020-v1",
		AssignmentBasis: partitionAssignmentGraphV1, OverlapRatio: .2, OverlapPolicy: "overlap-v1",
		FixtureChecksum: hash, ArtifactSHA256: hash, ArtifactBackend: "reference", Source: vectorpartition.Source{SourceID: "fixture", Checksum: hash, Vectors: 8, Dimensions: 2, Metric: "cosine"},
		DatabaseDirectory: dir, ManifestIntegrity: hash, ReadySetDigest: hash, RouterAssetChecksum: hash, RouterModelDigest: hash,
		SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRows: 8, PartitionGeneration: 4, RouterGeneration: 4,
		Partitions: 4, PartitionHNSWM: 16, Capacity: 3, PartitionLoads: []int{3, 2, 2, 2}, OverlapMemberships: 1, PersistentAssetBytes: 1024,
	}
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

func TestM3VariantDescriptorBindsReadyManifestV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	manifest := collections.VectorPartitionManifestV1{
		IntegrityDigest: d.ManifestIntegrity, ReadySetDigest: d.ReadySetDigest,
		RouterAsset:      collections.VectorPartitionAssetV1{Checksum: d.RouterAssetChecksum},
		SourceGeneration: d.SourceGeneration, SourceChecksum: d.SourceChecksum, SourceSchemaHash: d.SourceSchemaHash, SourceRowCount: d.SourceRows,
		Generation: d.PartitionGeneration, RouterGeneration: d.RouterGeneration, PartitionCount: d.Partitions, BalancePolicy: d.OverlapPolicy,
		OverlapMemberships: make([]collections.VectorPartitionMembershipV1, d.OverlapMemberships),
	}
	fixture := fixtureManifest{Checksum: d.FixtureChecksum}
	if err := m3DescriptorMatchesManifestV1(d, fixture, manifest, d.RouterModelDigest); err != nil {
		t.Fatal(err)
	}
	manifest.ReadySetDigest = strings.Repeat("b", 64)
	if err := m3DescriptorMatchesManifestV1(d, fixture, manifest, d.RouterModelDigest); err == nil {
		t.Fatal("accepted descriptor for a different ready manifest")
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
