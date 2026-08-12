package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalHNSWRepairCalibrationBindDescriptorV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	fixture := m8QualificationFixturesV1[0]
	fixture.Vectors, fixture.Dimensions, fixture.Queries = 256, 8, 8
	_, queries := fixtureData(fixture)
	fixture.Checksum = fixtureChecksumFromData(fixtureVectors(fixture), queries)
	dir := filepath.Join(t.TempDir(), "m3")
	testM8QualificationRetainedDescriptorV1(t, dir, strings.Repeat("a", 40), fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0)
	assets, err := openM8ProductionExistingAssetSetV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer assets.Close()
	if err := localHNSWRepairCalibrationBindDescriptorV1(assets, fixture); err != nil {
		t.Fatalf("offline descriptor binder: %v", err)
	}
	if err := m8BindRetainedM3DescriptorV1(assets, fixture); err != nil {
		t.Fatalf("production descriptor binder regressed: %v", err)
	}
	descriptor, err := m3ReadVariantDescriptorV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	descriptor.SourceOrdinalDigest = ""
	raw, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, m3VariantDescriptorFileV1), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m8BindRetainedM3DescriptorV1(assets, fixture); err == nil {
		t.Fatal("retained binder accepted missing source ordinal digest")
	}
}
