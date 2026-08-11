package main

import (
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
}
