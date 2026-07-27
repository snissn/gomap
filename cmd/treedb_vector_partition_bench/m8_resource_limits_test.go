package main

import (
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

func TestM8RowCountersTrackTruePerRequestMaximaV1(t *testing.T) {
	var row m8ProductionRowV1
	m8AccumulateProductionRowCountersV1(&row, nativewire.VectorPartitionCoordinatorCountersV1{
		Requests: 2, RPCs: 3, Retries: 1, Redirects: 1,
		RequestBytes: 100, CandidateBytes: 200, ResponseBytes: 300, MergeEntries: 20,
		MaxShardPartitions: 2, MaxShardRequestBytes: 70, MaxShardCandidateBytes: 150, MaxShardResponseBytes: 220,
	})
	m8AccumulateProductionRowCountersV1(&row, nativewire.VectorPartitionCoordinatorCountersV1{
		Requests: 4, RPCs: 4, RequestBytes: 90, CandidateBytes: 400, ResponseBytes: 250, MergeEntries: 40,
		MaxShardPartitions: 4, MaxShardRequestBytes: 60, MaxShardCandidateBytes: 300, MaxShardResponseBytes: 180,
	})
	if row.RequestBytes != 190 || row.CandidateBytes != 600 || row.ResponseBytes != 550 || row.RPCs != 7 {
		t.Fatalf("aggregate counters=%+v", row)
	}
	if row.MaxRequests != 4 || row.MaxRPCs != 4 || row.MaxRetries != 1 || row.MaxRedirects != 1 ||
		row.MaxRequestBytes != 100 || row.MaxCandidateBytes != 400 || row.MaxResponseBytes != 300 || row.MaxMergeEntries != 40 ||
		row.MaxShardPartitions != 4 || row.MaxShardRequestBytes != 70 || row.MaxShardCandidateBytes != 300 || row.MaxShardResponseBytes != 220 {
		t.Fatalf("per-request maxima=%+v", row)
	}
}

func TestM8ObservedResourceMaximaUseRecordedMaximaNotAveragesV1(t *testing.T) {
	rows := []m8ProductionRowV1{{
		Samples: 2, RPCs: 10, RequestBytes: 1000, CandidateBytes: 2000, ResponseBytes: 3000,
		MaxRequests: 3, MaxRPCs: 6, MaxRetries: 2, MaxRedirects: 1,
		MaxRequestBytes: 900, MaxCandidateBytes: 1800, MaxResponseBytes: 2700, MaxMergeEntries: 25,
		MaxShardPartitions: 4, MaxShardRequestBytes: 800, MaxShardCandidateBytes: 1700, MaxShardResponseBytes: 2600,
	}}
	got := m8ObservedResourceMaximaV1(rows)
	if got.Requests != 3 || got.RPCs != 6 || got.Retries != 2 || got.Redirects != 1 ||
		got.RequestBytes != 900 || got.CandidateBytes != 1800 || got.ResponseBytes != 2700 || got.MergeEntries != 25 ||
		got.ShardPartitions != 4 || got.ShardRequestBytes != 800 || got.ShardCandidateBytes != 1700 || got.ShardResponseBytes != 2600 {
		t.Fatalf("observed maxima=%+v", got)
	}
}

func TestM8ProductionResourcesFailClosedForZeroPartitionsV1(t *testing.T) {
	assets := &m8ProductionMultiGroupAssetsV1{manifest: collections.VectorPartitionManifestV1{}}
	got := m8ProductionResourcesV1(config{}, fixtureManifest{}, assets, nil, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{})
	if got.MaxPartitionLoad != math.MaxUint64 || got.BalanceHardCap != 0 {
		t.Fatalf("zero-partition resources=%+v", got)
	}
}

func TestM8ConfiguredAggregateTaskLimitMatchesPerTaskEnforcementV1(t *testing.T) {
	if got, ok := m8ConfiguredAggregateTaskLimitV1(1, 2); !ok || got != 2 {
		t.Fatalf("aggregate retry limit=(%d,%v) want=(2,true)", got, ok)
	}
	if _, ok := m8ConfiguredAggregateTaskLimitV1(-1, 2); ok {
		t.Fatal("accepted negative per-task limit")
	}
	if _, ok := m8ConfiguredAggregateTaskLimitV1(1, 0); ok {
		t.Fatal("accepted zero observed task scope")
	}
	if _, ok := m8ConfiguredAggregateTaskLimitV1(2, math.MaxUint64); ok {
		t.Fatal("accepted overflowing aggregate task limit")
	}
}

func TestM8RetryRedirectEvidenceUsesAggregateShardRequestScopeV1(t *testing.T) {
	cfg := config{
		m8CoordinatorLimits: nativewire.DefaultVectorPartitionCoordinatorLimitsV1(),
		m8ShardLimits:       nativewire.DefaultVectorPartitionShardSearchLimitsV1(),
		m8MaxAssetBytes:     1,
		m8MaxRSSBytes:       math.MaxUint64,
		partitions:          1,
		topK:                1,
		routerCandidates:    1,
		concurrency:         []int{1},
	}
	assets := &m8ProductionMultiGroupAssetsV1{manifest: collections.VectorPartitionManifestV1{
		SourceRowCount: 1,
		PartitionCount: 1,
		Memberships:    []collections.VectorPartitionMembershipV1{{PartitionID: 0}},
	}}
	rows := []m8ProductionRowV1{{Status: "pass", Probes: 1, EfSearch: 1, MaxRequests: 2, MaxRetries: 2, MaxRedirects: 2}}
	got := m8ProductionResourcesV1(cfg, fixtureManifest{Vectors: 1, Dimensions: 1}, assets, rows, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{})
	seen := map[string]m8ProductionResourceLimitComparisonV1{}
	for _, comparison := range got.LimitComparisons {
		seen[comparison.Name] = comparison
	}
	for _, name := range []string{"coordinator_retries_across_shard_requests", "coordinator_redirects_across_shard_requests"} {
		comparison, ok := seen[name]
		if !ok || comparison.Configured != 2 || comparison.Observed != 2 || !comparison.Enforced || !comparison.Passed {
			t.Fatalf("%s comparison=%+v present=%v", name, comparison, ok)
		}
	}
}

func TestM8PartitionLimitEvidenceUsesActualShardRequestScopeV1(t *testing.T) {
	cfg := config{
		m8CoordinatorLimits: nativewire.DefaultVectorPartitionCoordinatorLimitsV1(),
		m8ShardLimits:       nativewire.DefaultVectorPartitionShardSearchLimitsV1(),
		m8MaxAssetBytes:     1,
		m8MaxRSSBytes:       math.MaxUint64,
		partitions:          64,
		topK:                1,
		routerCandidates:    64,
		concurrency:         []int{1},
	}
	memberships := make([]collections.VectorPartitionMembershipV1, 64)
	for i := range memberships {
		memberships[i].PartitionID = uint32(i)
	}
	assets := &m8ProductionMultiGroupAssetsV1{manifest: collections.VectorPartitionManifestV1{
		SourceRowCount: 64,
		PartitionCount: 64,
		Memberships:    memberships,
	}}
	rows := []m8ProductionRowV1{{Status: "pass", Probes: 64, EfSearch: 1, MaxShardPartitions: 32}}
	got := m8ProductionResourcesV1(cfg, fixtureManifest{Vectors: 64, Dimensions: 1}, assets, rows, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{})
	seen := map[string]m8ProductionResourceLimitComparisonV1{}
	for _, comparison := range got.LimitComparisons {
		seen[comparison.Name] = comparison
	}
	if comparison := seen["coordinator_selected_partitions"]; comparison.Observed != 64 || !comparison.Passed {
		t.Fatalf("selected partitions=%+v", comparison)
	}
	for _, name := range []string{"coordinator_partitions_per_request", "shard_partitions"} {
		comparison := seen[name]
		if comparison.Configured != 32 || comparison.Observed != 32 || !comparison.Passed {
			t.Fatalf("%s comparison=%+v", name, comparison)
		}
	}
}
