package main

import (
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWAttributionBuildVariantV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	vectors := make([][]float64, 16)
	for i := range vectors {
		vectors[i] = []float64{float64(i + 1), float64(i%3 + 1), 1}
	}
	source, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	sourceDir := source.dir
	source.owned = false
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(sourceDir)
	source, err = openM8ProductionExistingAssetSetV1(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	harness, evidence, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantNativeV1, 9989)
	if err != nil {
		t.Fatal(err)
	}
	defer harness.Close()
	if evidence.Schema != localHNSWAttributionBuildSchemaV1 || evidence.Variant != "native" || evidence.VariantIdentity == "" || evidence.FileID != 9989 || evidence.Partitions != 4 || evidence.ElapsedNanos <= 0 || evidence.CloneLogicalBytes <= 0 || evidence.PackBytes == 0 || evidence.MappedBytes == 0 || evidence.CPUAvailable && evidence.CPUDeltaNanos < 0 {
		t.Fatalf("evidence=%+v", evidence)
	}
	repaired, repairEvidence, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 9990)
	if err != nil {
		t.Fatal(err)
	}
	defer repaired.Close()
	if repairEvidence.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1) || repairEvidence.VariantIdentity == "" || repairEvidence.FileID != 9990 || repairEvidence.Partitions != 4 || repairEvidence.PackBytes == 0 {
		t.Fatalf("repair evidence=%+v", repairEvidence)
	}
	if _, _, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantV1("wrong"), 9991); err == nil {
		t.Fatal("expected invalid variant rejection")
	}
}
