package collections

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var stableColumnRequirementBenchmarkSink rootpublication.StableLogicalObligationRequirements

func TestStableColumnAppendRequirementProofWorkIsMutationLocal(t *testing.T) {
	const retained = 4096
	records := make([]columnManifestRecord, retained+1)
	for index := range records {
		asset := testColumnPublishPreparedAssetM10A()
		asset.Ref.Generation = 1
		asset.Ref.PartID = uint64(index + 1)
		asset.Ref.FileID = uint32(index + 1)
		asset.Ref.Offset = int64(index) * asset.Ref.Length
		asset.GenerationID = 1
		records[index] = encodeColumnManifestPartRecordForTest1787(t, asset)
	}
	current, next := records[:retained], records
	delta := ColumnManifestRootDelta{
		Records: next, Mutations: mustBuildColumnManifestMutationDelta(t, current, next), MutationDelta: true,
	}
	requirements, mutation, fallback, work, err := stableColumnManifestDurablePublication(current, delta, 1, "events/column-assets")
	if err != nil {
		t.Fatal(err)
	}
	if len(mutation.Added) != 1 || len(mutation.Removed) != 0 {
		t.Fatalf("mutation added=%d removed=%d", len(mutation.Added), len(mutation.Removed))
	}
	if len(requirements.ScopedFields) != 0 || fallback == nil {
		t.Fatalf("certified append eagerly materialized requirements=%d fallback_nil=%t", len(requirements.Obligations), fallback == nil)
	}
	if work.FinalRequirementRecordsDecoded > 1 || work.FinalRequirementObligationsMaterialized != 0 {
		t.Fatalf("certified append work=%+v want one changed record and no final requirement slice", work)
	}
}

func TestStableColumnRemovalKeepsExactRequirements4366(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	asset.Ref.Generation = 1
	asset.GenerationID = 1
	current := []columnManifestRecord{encodeColumnManifestPartRecordForTest1787(t, asset)}
	delta := ColumnManifestRootDelta{
		Records: nil, Mutations: mustBuildColumnManifestMutationDelta(t, current, nil), MutationDelta: true,
	}
	requirements, mutation, fallback, work, err := stableColumnManifestDurablePublication(current, delta, 1, "events/column-assets")
	if err != nil {
		t.Fatal(err)
	}
	if fallback != nil || len(mutation.Removed) != 1 || len(mutation.Added) != 0 {
		t.Fatalf("removal mutation added=%d removed=%d fallback_nil=%t", len(mutation.Added), len(mutation.Removed), fallback == nil)
	}
	if len(requirements.ScopedFields) == 0 || len(requirements.Obligations) != 0 {
		t.Fatalf("removal exact requirements fields=%d obligations=%d", len(requirements.ScopedFields), len(requirements.Obligations))
	}
	if work.FinalRequirementRecordsDecoded != 1 || work.FinalRequirementObligationsMaterialized != 0 {
		t.Fatalf("removal exact work=%+v", work)
	}
}

func TestStableColumnReplacementKeepsExactRequirements4366(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	asset.Ref.Generation = 1
	asset.GenerationID = 1
	replacement := asset
	replacement.Ref.FileID++
	current := []columnManifestRecord{encodeColumnManifestPartRecordForTest1787(t, asset)}
	next := []columnManifestRecord{encodeColumnManifestPartRecordForTest1787(t, replacement)}
	delta := ColumnManifestRootDelta{
		Records: next, Mutations: mustBuildColumnManifestMutationDelta(t, current, next), MutationDelta: true,
	}
	requirements, mutation, fallback, _, err := stableColumnManifestDurablePublication(current, delta, 1, "events/column-assets")
	if err != nil {
		t.Fatal(err)
	}
	if fallback != nil || len(mutation.Removed) != 1 || len(mutation.Added) != 1 || len(requirements.Obligations) != 1 {
		t.Fatalf("replacement requirements=%d added=%d removed=%d fallback_nil=%t", len(requirements.Obligations), len(mutation.Added), len(mutation.Removed), fallback == nil)
	}
}

func TestStableColumnNonMutationDeltaKeepsExactRequirements4366(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	asset.Ref.Generation = 1
	asset.GenerationID = 1
	records := []columnManifestRecord{encodeColumnManifestPartRecordForTest1787(t, asset)}
	requirements, mutation, fallback, work, err := stableColumnManifestDurablePublication(nil, ColumnManifestRootDelta{Records: records}, 1, "events/column-assets")
	if err != nil {
		t.Fatal(err)
	}
	if fallback != nil || len(mutation.ScopedFields) != 0 || len(requirements.Obligations) != 1 {
		t.Fatalf("non-mutation delta requirements=%d mutation_fields=%d fallback_nil=%t", len(requirements.Obligations), len(mutation.ScopedFields), fallback == nil)
	}
	if work.FinalRequirementRecordsDecoded != 1 || work.FinalRequirementObligationsMaterialized != 1 {
		t.Fatalf("non-mutation exact work=%+v", work)
	}
}

func TestStableColumnAppendIdentityMismatchRejectsBeforeRegistration4366(t *testing.T) {
	for _, tc := range []struct {
		name       string
		generation uint64
		namespace  string
	}{
		{name: "generation", generation: 2, namespace: "events/column-assets"},
		{name: "namespace", generation: 1, namespace: "wrong"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			asset := testColumnPublishPreparedAssetM10A()
			asset.Ref.Generation = tc.generation
			asset.Ref.Namespace = tc.namespace
			asset.GenerationID = tc.generation
			next := []columnManifestRecord{encodeColumnManifestPartRecordForTest1787(t, asset)}
			delta := ColumnManifestRootDelta{
				Records: next, Mutations: mustBuildColumnManifestMutationDelta(t, nil, next), MutationDelta: true,
			}
			_, _, fallback, _, err := stableColumnManifestDurablePublication(nil, delta, 1, "events/column-assets")
			if err == nil || fallback != nil {
				t.Fatalf("identity mismatch err=%v fallback_nil=%t", err, fallback == nil)
			}
		})
	}
}

func BenchmarkStableColumnFinalRequirements4366(b *testing.B) {
	for _, retained := range []int{32, 4096} {
		b.Run(fmt.Sprintf("retained_%d", retained), func(b *testing.B) {
			records := make([]columnManifestRecord, retained+1)
			for index := range records {
				asset := testColumnPublishPreparedAssetM10A()
				asset.Ref.Generation = 1
				asset.Ref.PartID = uint64(index + 1)
				asset.Ref.FileID = uint32(index + 1)
				asset.Ref.Offset = int64(index) * asset.Ref.Length
				asset.GenerationID = 1
				records[index] = encodeColumnManifestPartRecordForTest1787(b, asset)
			}
			current, next := records[:retained], records
			delta := ColumnManifestRootDelta{
				Records: next, Mutations: mustBuildColumnManifestMutationDelta(b, current, next), MutationDelta: true,
			}
			b.Run("certified", func(b *testing.B) {
				b.ReportAllocs()
				var decoded, materialized uint64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					requirements, _, fallback, work, err := stableColumnManifestDurablePublication(current, delta, 1, "events/column-assets")
					if err != nil || fallback == nil {
						b.Fatalf("certified publication fallback=%t err=%v", fallback != nil, err)
					}
					decoded += work.FinalRequirementRecordsDecoded
					materialized += work.FinalRequirementObligationsMaterialized
					stableColumnRequirementBenchmarkSink = requirements
				}
				b.ReportMetric(float64(decoded)/float64(b.N), "records_decoded/op")
				b.ReportMetric(float64(materialized)/float64(b.N), "obligations_materialized/op")
			})
			b.Run("forced_exact", func(b *testing.B) {
				b.ReportAllocs()
				var decoded, materialized uint64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _, fallback, work, err := stableColumnManifestDurablePublication(current, delta, 1, "events/column-assets")
					if err != nil || fallback == nil {
						b.Fatalf("forced exact publication fallback=%t err=%v", fallback != nil, err)
					}
					requirements, fallbackWork, err := fallback()
					if err != nil {
						b.Fatal(err)
					}
					work.Add(fallbackWork)
					decoded += work.FinalRequirementRecordsDecoded
					materialized += work.FinalRequirementObligationsMaterialized
					stableColumnRequirementBenchmarkSink = requirements
				}
				b.ReportMetric(float64(decoded)/float64(b.N), "records_decoded/op")
				b.ReportMetric(float64(materialized)/float64(b.N), "obligations_materialized/op")
			})
		})
	}
}
