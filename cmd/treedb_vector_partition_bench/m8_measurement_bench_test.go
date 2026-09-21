package main

import (
	"context"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
)

// A complete 16-query cell through the native TCP/Raft-read coordinator. Setup,
// truth and warmup are excluded; response reduction/accounting is INCLUDED.
// This identical benchmark can be run at the base to measure harness overhead.
func BenchmarkM8MeasuredNativeCellV1(b *testing.B) {
	requireM8PersistentAssetSupportV1(b)
	f := m8QualificationFixturesV1[0]
	f.Vectors, f.Dimensions, f.Queries = 2048, 128, 16
	vectors, queries := fixtureData(f)
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		b.Fatal(err)
	}
	defer assets.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	topology, err := nativewire.NewVectorPartitionM8ProductionMultiGroupV1(ctx, nativewire.VectorPartitionM8ProductionMultiGroupOptionsV1{Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(), GroupAssetSetDigests: assets.assetSetDigests, Database: "default", Catalog: "default"})
	if err != nil {
		b.Fatal(err)
	}
	defer topology.Close()
	truth, err := m8ExactTruthV1(assets.collection, assets.manifest, queries, 10)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := m8WarmProductionTopologyV1(ctx, topology.Coordinator(), assets, queries, config{topK: 10, efSearch: []int{96}, concurrency: []int{1}, warmup: 16}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		row, _, _, err := m8RunProductionCellV1(ctx, topology.Coordinator(), assets, queries, truth, 4, 96, 1, 10, defaultRouterScoreBudgetV3, nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxCandidateBytes)
		if err != nil || row.Status != "pass" || row.RPCs == 0 || row.LocalScoreCalls == 0 {
			b.Fatalf("not a valid native measured cell: %v %+v", err, row)
		}
	}
}
