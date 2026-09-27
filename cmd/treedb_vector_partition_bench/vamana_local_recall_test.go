package main

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestVamanaLocalRecallAtEF96V1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	fixture := fixtureManifest{
		SchemaVersion: schemaVersion,
		Fixture:       "vamana-local-recall-ef96",
		Generator:     qualificationEmbeddingGeneratorV1,
		// Four 10k-row packs reproduce the short-edge saturation that smaller
		// packs miss while keeping this regression gate under 20 seconds.
		Vectors:    40_000,
		Queries:    128,
		Dimensions: 64,
		Seed:       4016,
	}
	vectors, queries := fixtureData(fixture)
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer assets.Close()
	truth, err := m8ExactTruthV1(assets.collection, assets.manifest, queries, 10)
	if err != nil {
		t.Fatal(err)
	}
	searchers := make([]*collections.VectorPartitionLocalSearcherV1, 4)
	for partition := range searchers {
		searchers[partition], err = assets.collection.OpenVectorPartitionLocalSearcherForGenerationV1(partitionHNSWIndex, assets.manifest.Generation, uint32(partition))
		if err != nil {
			t.Fatal(err)
		}
		defer searchers[partition].Close()
	}
	hits := 0
	for queryIndex, query64 := range queries {
		query := make([]float32, len(query64))
		for i, value := range query64 {
			query[i] = float32(value)
		}
		merged := make([]neighbor, 0, 40)
		for _, searcher := range searchers {
			results, _, err := searcher.SearchWithOptionsV1(context.Background(), query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 96})
			if err != nil {
				t.Fatal(err)
			}
			for _, result := range results {
				merged = append(merged, neighbor{ID: result.ID, Distance: 1 - float64(result.Score)})
			}
		}
		got := canonicalExactNeighborsV1(merged, 10)
		want := make(map[string]struct{}, len(truth[queryIndex]))
		for _, result := range truth[queryIndex] {
			want[result.ID] = struct{}{}
		}
		for _, result := range got {
			if _, ok := want[result.ID]; ok {
				hits++
			}
		}
	}
	recall := float64(hits) / float64(len(queries)*10)
	t.Logf("recall@10=%0.6f hits=%d/%d", recall, hits, len(queries)*10)
	if recall < .95 {
		t.Fatalf("Vamana local recall=%0.6f want >=0.95", recall)
	}
}
