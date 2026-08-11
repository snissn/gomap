package main

import (
	"context"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWOrderCalibrationQueryV1(t *testing.T) {
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
	control, _, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 4107001)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	hash, _, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationStableIDHashV1, 4107002)
	if err != nil {
		t.Fatal(err)
	}
	defer hash.Close()
	ordinal := 0
	for !localHNSWCalibrationOrdinalV1(ordinal) {
		ordinal++
	}
	query64 := []float64{1, 1, 1}
	truth, err := m8ExactTruthV1(source.collection, source.manifest, [][]float64{query64}, 10)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := localHNSWOrderCalibrationQueryV1Build(context.Background(), source, control, hash, ordinal, m8Query32V1(query64), truth[0])
	if err != nil {
		t.Fatal(err)
	}
	if !localHNSWOrderCalibrationQueryV1Valid(evidence, 4) || evidence.Schema != localHNSWOrderCalibrationSchemaV1 || len(evidence.SourceSearches) != 4 || len(evidence.HashSearches) != 4 {
		t.Fatalf("evidence=%+v", evidence)
	}
	for _, searches := range [][]localHNSWRepairCalibrationSearchV1{evidence.SourceSearches, evidence.HashSearches} {
		for _, search := range searches {
			if search.AuxiliaryEdges > search.NativeEdges+search.AuxiliaryEdges || search.AuxiliaryAdmissions > search.AuxiliaryCandidates {
				t.Fatalf("search=%+v", search)
			}
		}
	}
}
