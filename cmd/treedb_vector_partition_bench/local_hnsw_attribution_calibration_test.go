package main

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWAttributionCalibrationV1Build(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	fixture := fixtureManifest{SchemaVersion: 1, Fixture: "calibration-test", Generator: qualificationEmbeddingGeneratorV1, Arithmetic: fixtureArithmetic, Vectors: 16, Queries: 16, Dimensions: 3, Metric: "cosine", Seed: 4016, Checksum: "test"}
	vectors := fixtureVectors(fixture)
	source, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	var ordinals []int
	for ordinal := 0; ordinal < fixture.Queries && len(ordinals) < 2; ordinal++ {
		if localHNSWCalibrationOrdinalV1(ordinal) {
			ordinals = append(ordinals, ordinal)
		}
	}
	calibration, err := localHNSWAttributionCalibrationV1Build(source, fixture, ordinals)
	if err != nil {
		t.Fatal(err)
	}
	if calibration.Schema != localHNSWAttributionCalibrationSchemaV1 || len(calibration.Ordinals) != 2 || len(calibration.Queries) != 2 || len(calibration.Truth) != 2 || len(calibration.Truth[0]) != 10 || len(calibration.Queries[0]) != fixture.Dimensions {
		t.Fatalf("calibration=%+v", calibration)
	}
	bad := append([]int(nil), ordinals...)
	bad[1] = bad[0]
	if _, err := localHNSWAttributionCalibrationV1Build(source, fixture, bad); err == nil {
		t.Fatal("expected duplicate calibration ordinal rejection")
	}
}
