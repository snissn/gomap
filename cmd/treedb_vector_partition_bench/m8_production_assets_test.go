package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestM8ProductionMultiGroupAssetsCheckedIn10kCISmokeV1(t *testing.T) {
	fixture, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	if fixture.Vectors != 10_000 {
		t.Fatalf("fixture vectors=%d", fixture.Vectors)
	}
	vectors := deterministicVectors(fixture)
	groups := []string{"m8-data-group-a", "m8-data-group-b"}
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, groups, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := assets.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	if assets.status.Manifest.State != "ready" || assets.router == nil || len(assets.manifest.Placements) != 4 {
		t.Fatalf("fixture readiness status=%+v", assets.status)
	}
	counts := map[string]int{}
	covered := map[uint32]bool{}
	for _, placement := range assets.manifest.Placements {
		counts[placement.GroupID]++
		covered[placement.PartitionID] = true
	}
	if len(counts) != 2 || counts[groups[0]] == 0 || counts[groups[1]] == 0 || len(covered) != 4 {
		t.Fatalf("placement=%+v", assets.manifest.Placements)
	}
	for _, group := range groups {
		if assets.assetSetDigests[group] == "" {
			t.Fatalf("missing actual asset digest for %s", group)
		}
	}
	assetCoverage := map[uint32]int{}
	for _, asset := range assets.manifest.Assets {
		for _, placement := range assets.manifest.Placements {
			if asset.PartitionID == placement.PartitionID {
				assetCoverage[asset.PartitionID]++
			}
		}
	}
	for partition := 0; partition < 4; partition++ {
		if assetCoverage[uint32(partition)] != 1 {
			t.Fatalf("partition %d asset coverage=%d", partition, assetCoverage[uint32(partition)])
		}
	}
	query := make([]float32, len(vectors[0]))
	for i, value := range vectors[17] {
		query[i] = float32(value)
	}
	merged := make([]neighbor, 0, 40)
	gotScores := map[string]float32{}
	for partition := 0; partition < 4; partition++ {
		searcher, err := assets.collection.OpenVectorPartitionLocalSearcherForGenerationV1(partitionHNSWIndex, assets.manifest.Generation, uint32(partition))
		if err != nil {
			t.Fatalf("open partition %d: %v", partition, err)
		}
		if searcher.Status().SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 {
			t.Fatalf("partition %d route=%q", partition, searcher.Status().SearchRoute)
		}
		results, _, err := searcher.SearchWithOptionsV1(context.Background(), query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 4096})
		closeErr := searcher.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("search partition %d err=%v close=%v", partition, err, closeErr)
		}
		for _, result := range results {
			merged = append(merged, neighbor{ID: result.ID, Distance: 1 - float64(result.Score)})
			gotScores[result.ID] = result.Score
		}
	}
	sortNeighbors(merged)
	merged = dedupeSortedNeighbors(merged)
	merged = merged[:10]
	want := m8ExactFP32TopKV1(vectors, query, 10)
	for i := range want {
		if merged[i].ID != want[i].id || math.Float32bits(gotScores[merged[i].ID]) != math.Float32bits(want[i].score) {
			t.Fatalf("parity rank=%d got=%s score=%08x want=%s score=%08x got_all=%s", i, merged[i].ID, math.Float32bits(gotScores[merged[i].ID]), want[i].id, math.Float32bits(want[i].score), fmt.Sprint(merged))
		}
	}
}

type m8FP32NeighborV1 struct {
	id    string
	score float32
}

func m8ExactFP32TopKV1(vectors [][]float64, query []float32, topK int) []m8FP32NeighborV1 {
	norm, _ := m6QueryNormV1(query)
	norms, _ := m6VectorNormsV1(vectors, len(query))
	out := make([]m8FP32NeighborV1, len(vectors))
	for i := range vectors {
		score := m6CosineScoreV1(vectors[i], query, norm, norms[i])
		// Native packs use the persisted FP32 norm/dot path; its final division
		// rounds one ULP below the float64-reference conversion.
		out[i] = m8FP32NeighborV1{id: fmt.Sprintf("doc-%06d", i), score: math.Nextafter32(score, float32(math.Inf(-1)))}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].score > out[j].score || out[i].score == out[j].score && out[i].id < out[j].id
	})
	return out[:topK]
}
