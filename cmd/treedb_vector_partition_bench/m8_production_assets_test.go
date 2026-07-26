package main

import (
	"context"
	"fmt"
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
	query := make([]float32, len(vectors[0]))
	for i, value := range vectors[17] {
		query[i] = float32(value)
	}
	merged := make([]neighbor, 0, 40)
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
		}
	}
	sortNeighbors(merged)
	merged = dedupeSortedNeighbors(merged)
	merged = merged[:10]
	want := exactTopK(vectors, vectors[17], 10)
	for i := range want {
		if merged[i].ID != want[i].ID {
			t.Fatalf("parity rank=%d got=%s want=%s got_all=%s", i, merged[i].ID, want[i].ID, fmt.Sprint(merged))
		}
	}
}
