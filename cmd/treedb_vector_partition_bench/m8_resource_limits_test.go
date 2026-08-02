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

func TestM8RouterSessionIdentityResourceMaxUsesEverySessionV1(t *testing.T) {
	short := nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{Database: "d", Catalog: "c", Collection: "x", IndexName: "i", IndexDefinitionDigest: "h", ReadySetDigest: "r"}
	long := short
	long.Database = "a-much-longer-database"
	evidence := m8ProductionRouterSessionEvidenceV1{
		AfterWarmup:   []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{{Identity: short}},
		AfterMeasured: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{{Identity: short}, {Identity: long}},
	}
	want := m8RouterSessionIdentityBytesV1(long)
	if got, ok := m8RouterSessionIdentityMaxBytesV1(evidence); !ok || got != want {
		t.Fatalf("identity maximum=%d ok=%v want %d", got, ok, want)
	}
	resources := m8ProductionResourceEvidenceV1{LimitComparisons: []m8ProductionResourceLimitComparisonV1{
		{Name: "coordinator_identity_bytes", Configured: want, Enforced: true},
		{Name: "shard_identity_bytes", Configured: want, Enforced: true},
	}}
	m8ApplyRouterSessionIdentityResourceMaxV1(&resources, evidence)
	for _, comparison := range resources.LimitComparisons {
		if comparison.Observed != want || !comparison.Passed {
			t.Fatalf("identity comparison=%+v want observed/passed %d/true", comparison, want)
		}
	}
}

func TestM8WallClockEvidenceUsesActualMaximumNotP99V1(t *testing.T) {
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
	got := m8ProductionResourcesV1(cfg, fixtureManifest{Vectors: 1, Dimensions: 1}, assets, []m8ProductionRowV1{{Status: "pass", Probes: 1, EfSearch: 1, P99Nanos: 100, MaxTotalNanos: 200}}, m8ProductionFaultResourceBoundaryV1{}, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{}, m8ProductionFaultResourceBoundaryV1{})
	for _, comparison := range got.LimitComparisons {
		if comparison.Name == "coordinator_wall_clock" {
			if comparison.Observed != 200 {
				t.Fatalf("wall-clock comparison=%+v want actual maximum 200", comparison)
			}
			return
		}
	}
	t.Fatal("missing coordinator_wall_clock comparison")
}

func TestM8ProductionResourcesReportRequestRouterBudgetV1(t *testing.T) {
	cfg := config{
		m8CoordinatorLimits: nativewire.DefaultVectorPartitionCoordinatorLimitsV1(),
		m8ShardLimits:       nativewire.DefaultVectorPartitionShardSearchLimitsV1(),
		m8MaxAssetBytes:     1,
		m8MaxRSSBytes:       math.MaxUint64,
		partitions:          1,
		topK:                1,
		routerCandidates:    7,
		probes:              []int{1, 4},
		concurrency:         []int{1},
	}
	assets := &m8ProductionMultiGroupAssetsV1{
		manifest: collections.VectorPartitionManifestV1{
			SourceRowCount: 1,
			PartitionCount: 1,
			Memberships:    []collections.VectorPartitionMembershipV1{{PartitionID: 0}},
		},
		status: collections.VectorPartitionRouterRuntimeStatusV1{Representatives: 19},
	}
	request := m8ProductionApproximateRequestV1(assets, []float32{1}, "router-budget", 1, 1, 1, cfg.routerCandidates, 1)
	if request.RouterMode != collections.VectorPartitionRouterModeApproxV1 || request.RouterCandidateBudget != 7 {
		t.Fatalf("request=%+v want approximate router budget 7", request)
	}
	warmup := m8ProductionWarmupRequestV1(assets, []float32{1}, "warmup-budget", 1, cfg)
	if warmup.RouterMode != collections.VectorPartitionRouterModeApproxV1 || warmup.RouterCandidateBudget != 7 || warmup.PartitionProbes != 4 {
		t.Fatalf("warmup=%+v want approximate router budget 7", warmup)
	}
	preflight := m8ProductionRequestV1(assets, []float32{1}, "preflight-budget", 1, 1, 1, 1)
	if preflight.RouterMode != collections.VectorPartitionRouterModeExactV1 || preflight.RouterCandidateBudget != 19 {
		t.Fatalf("preflight=%+v want exact router budget 19", preflight)
	}
	got := m8ProductionResourcesV1(cfg, fixtureManifest{Vectors: 1, Dimensions: 1}, assets, nil, m8ProductionFaultResourceBoundaryV1{}, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{}, m8ProductionFaultResourceBoundaryV1{})
	for _, comparison := range got.LimitComparisons {
		if comparison.Name != "coordinator_router_candidates" {
			continue
		}
		if comparison.Observed != uint64(assets.status.Representatives) {
			t.Fatalf("router comparison=%+v want exact-control budget %d", comparison, assets.status.Representatives)
		}
		return
	}
	t.Fatal("missing coordinator_router_candidates comparison")
}

func TestM8ResourceEvidenceIncludesAllUntimedBoundariesV1(t *testing.T) {
	cfg := config{
		m8CoordinatorLimits: nativewire.DefaultVectorPartitionCoordinatorLimitsV1(),
		m8ShardLimits:       nativewire.DefaultVectorPartitionShardSearchLimitsV1(),
		m8MaxAssetBytes:     1,
		m8MaxRSSBytes:       math.MaxUint64,
		partitions:          4,
		topK:                1,
		routerCandidates:    1,
		concurrency:         []int{1},
	}
	assets := &m8ProductionMultiGroupAssetsV1{manifest: collections.VectorPartitionManifestV1{
		Collection: "collection", IndexName: "index", IndexDefinitionDigest: "definition", ReadySetDigest: "ready",
		SourceRowCount: 4,
		PartitionCount: 4,
		Memberships: []collections.VectorPartitionMembershipV1{
			{PartitionID: 0}, {PartitionID: 1}, {PartitionID: 2}, {PartitionID: 3},
		},
	}}
	rows := []m8ProductionRowV1{{
		Status: "pass", Probes: 1, EfSearch: 32, MaxTotalNanos: 100,
		MaxRequests: 1, MaxRPCs: 1, MaxRequestBytes: 10, MaxCandidateBytes: 20, MaxResponseBytes: 30,
		MaxMergeEntries: 1, MaxShardPartitions: 1, MaxShardRequestBytes: 9, MaxShardCandidateBytes: 19, MaxShardResponseBytes: 29,
	}}
	fault := m8ProductionFaultResourceBoundaryV1{
		SelectedPartitions: 4, EfSearch: 4096, WallClockNanos: 400,
		Maxima: m8ProductionResourceObservedMaximaV1{
			Requests: 4, RPCs: 4, Retries: 2, Redirects: 1,
			RequestBytes: 100, CandidateBytes: 200, ResponseBytes: 300, MergeEntries: 40,
			ShardPartitions: 2, ShardRequestBytes: 90, ShardCandidateBytes: 190, ShardResponseBytes: 290,
		},
	}
	preflight := m8ProductionResourceBoundaryV1{
		SelectedPartitions: 4, EfSearch: 4096, WallClockNanos: 500,
		Maxima: m8ProductionResourceObservedMaximaV1{
			Requests: 4, RPCs: 5, Retries: 1, Redirects: 0,
			RequestBytes: 110, CandidateBytes: 210, ResponseBytes: 310, MergeEntries: 41,
			ShardPartitions: 3, ShardRequestBytes: 91, ShardCandidateBytes: 191, ShardResponseBytes: 291,
		},
	}
	untimed := m8MergeProductionResourceBoundariesV1(preflight, fault)
	got := m8ProductionResourcesV1(cfg, fixtureManifest{Vectors: 4, Dimensions: 1}, assets, rows, untimed, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{}, fault)
	identity := nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{
		Database: "default", Catalog: "default", Collection: assets.manifest.Collection, IndexName: assets.manifest.IndexName,
		IndexDefinitionDigest: assets.manifest.IndexDefinitionDigest, ReadySetDigest: assets.manifest.ReadySetDigest,
	}
	sessions := m8ProductionRouterSessionEvidenceV1{AfterWarmup: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{{Identity: identity}}}
	m8ApplyRouterSessionIdentityResourceMaxV1(&got, sessions)
	report := m8ProductionReportV1{
		Dataset: fixtureManifest{Vectors: 4, Dimensions: 1}, Config: m8ProductionConfigEvidenceV1{RaftGroups: cfg.raftGroups, TopK: cfg.topK},
		Rows: rows, UntimedBoundary: untimed, Failure: m8ProductionFailureEvidenceV1{ResourceBoundary: fault},
		Resources: got, RouterSessions: sessions,
	}
	expected, ok := m8ExpectedResourceLimitObservationsV1(report)
	if !ok {
		t.Fatal("derive validator resource observations")
	}
	seen := map[string]m8ProductionResourceLimitComparisonV1{}
	for _, comparison := range got.LimitComparisons {
		seen[comparison.Name] = comparison
	}
	want := map[string]uint64{
		"coordinator_selected_partitions":             4,
		"coordinator_requests":                        4,
		"coordinator_rpcs_across_shard_requests":      5,
		"coordinator_retries_across_shard_requests":   2,
		"coordinator_redirects_across_shard_requests": 1,
		"coordinator_ef_search":                       4096,
		"coordinator_partitions_per_request":          3,
		"coordinator_merge_entries":                   41,
		"coordinator_request_bytes":                   110,
		"coordinator_candidate_bytes":                 210,
		"coordinator_response_bytes":                  310,
		"coordinator_wall_clock":                      500,
		"shard_partitions":                            3,
		"shard_ef_search":                             4096,
		"shard_request_bytes":                         91,
		"shard_candidate_bytes":                       191,
		"shard_response_bytes":                        291,
	}
	for name, observed := range want {
		if comparison, ok := seen[name]; !ok || comparison.Observed != observed {
			t.Fatalf("%s comparison=%+v present=%v want observed=%d", name, comparison, ok, observed)
		}
	}
	for name, observed := range expected {
		if comparison, ok := seen[name]; !ok || comparison.Observed != observed {
			t.Fatalf("producer/validator %s comparison=%+v present=%v want observed=%d", name, comparison, ok, observed)
		}
	}
}

func TestM8CanonicalCandidateBudgetCoversRequiredOverlapV1(t *testing.T) {
	required := uint64(1_000_000+200_000) * 64
	if m8ProductionCandidateBudgetBytesV1 < required {
		t.Fatalf("candidate budget=%d below required overlap floor=%d", m8ProductionCandidateBudgetBytesV1, required)
	}
	cfg, err := parseConfig([]string{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.m8CoordinatorLimits.MaxCandidateBytes != m8ProductionCandidateBudgetBytesV1 || cfg.m8ShardLimits.MaxCandidateBytes != m8ProductionCandidateBudgetBytesV1 {
		t.Fatalf("candidate limits coordinator=%d shard=%d", cfg.m8CoordinatorLimits.MaxCandidateBytes, cfg.m8ShardLimits.MaxCandidateBytes)
	}
	request := m8ProductionRequestV1(&m8ProductionMultiGroupAssetsV1{manifest: collections.VectorPartitionManifestV1{Collection: "docs", IndexName: "embedding"}}, []float32{1}, "candidate-budget", 16, 4096, 10, cfg.m8CoordinatorLimits.MaxCandidateBytes)
	if request.CandidateBytesLimit != m8ProductionCandidateBudgetBytesV1 {
		t.Fatalf("request candidate limit=%d", request.CandidateBytesLimit)
	}
}

func TestM8ProductionResourcesFailClosedForZeroPartitionsV1(t *testing.T) {
	assets := &m8ProductionMultiGroupAssetsV1{manifest: collections.VectorPartitionManifestV1{}}
	got := m8ProductionResourcesV1(config{}, fixtureManifest{}, assets, nil, m8ProductionFaultResourceBoundaryV1{}, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{}, m8ProductionFaultResourceBoundaryV1{})
	if got.MaxPartitionLoad != math.MaxUint64 || got.BalanceHardCap != 0 {
		t.Fatalf("zero-partition resources=%+v", got)
	}
}

func TestM8ProductionResourcesUsePersistedBalanceCapacityV1(t *testing.T) {
	policy, err := collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{
		Capacity: 75,
		Budget:   20,
		Realized: 0,
		Unspent:  20,
	})
	if err != nil {
		t.Fatal(err)
	}
	assets := &m8ProductionMultiGroupAssetsV1{manifest: collections.VectorPartitionManifestV1{
		SourceRowCount: 100,
		PartitionCount: 2,
		BalancePolicy:  policy,
		Memberships: []collections.VectorPartitionMembershipV1{
			{PartitionID: 0},
			{PartitionID: 1},
		},
	}}
	got := m8ProductionResourcesV1(config{}, fixtureManifest{}, assets, nil, m8ProductionFaultResourceBoundaryV1{}, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{}, m8ProductionFaultResourceBoundaryV1{})
	if got.BalanceHardCap != 75 {
		t.Fatalf("balance hard cap=%d want persisted capacity 75", got.BalanceHardCap)
	}
}

func TestM8ProductionResourcesPreserveBuiltInDisjointBalanceCapacityV1(t *testing.T) {
	assets := &m8ProductionMultiGroupAssetsV1{manifest: collections.VectorPartitionManifestV1{
		SourceRowCount: 100,
		PartitionCount: 2,
		BalancePolicy:  "round_robin_disjoint_v1",
		Memberships: []collections.VectorPartitionMembershipV1{
			{PartitionID: 0},
			{PartitionID: 1},
		},
	}}
	got := m8ProductionResourcesV1(config{}, fixtureManifest{}, assets, nil, m8ProductionFaultResourceBoundaryV1{}, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{}, m8ProductionFaultResourceBoundaryV1{})
	if got.BalanceHardCap != 53 {
		t.Fatalf("balance hard cap=%d want built-in disjoint capacity 53", got.BalanceHardCap)
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
	if got, ok := m8ConfiguredRPCsV1(2, 2, true); !ok || got != 4 {
		t.Fatalf("configured RPCs=(%d,%v) want=(4,true)", got, ok)
	}
	if _, ok := m8ConfiguredRPCsV1(math.MaxUint64, 1, true); ok {
		t.Fatal("accepted overflowing configured RPC total")
	}
	if _, ok := m8ConfiguredRPCsV1(1, 1, false); ok {
		t.Fatal("accepted RPC total with invalid retry scope")
	}
}

func TestM8ConcurrentRequestEvidenceFailsClosedWhenAggregateLimitOverflowsV1(t *testing.T) {
	cfg := config{
		m8CoordinatorLimits: nativewire.DefaultVectorPartitionCoordinatorLimitsV1(),
		m8ShardLimits:       nativewire.DefaultVectorPartitionShardSearchLimitsV1(),
		m8MaxAssetBytes:     1,
		m8MaxRSSBytes:       math.MaxUint64,
		partitions:          1,
		topK:                1,
		routerCandidates:    1,
		concurrency:         []int{2},
	}
	cfg.m8CoordinatorLimits.MaxConcurrentRequests = math.MaxInt
	assets := &m8ProductionMultiGroupAssetsV1{manifest: collections.VectorPartitionManifestV1{
		SourceRowCount: 1,
		PartitionCount: 1,
		Memberships:    []collections.VectorPartitionMembershipV1{{PartitionID: 0}},
	}}
	got := m8ProductionResourcesV1(cfg, fixtureManifest{Vectors: 1, Dimensions: 1}, assets, nil, m8ProductionFaultResourceBoundaryV1{}, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{}, m8ProductionFaultResourceBoundaryV1{})
	for _, comparison := range got.LimitComparisons {
		if comparison.Name != "coordinator_concurrent_requests_across_clients" {
			continue
		}
		if comparison.Configured != 0 || comparison.Enforced || comparison.Passed {
			t.Fatalf("overflowing concurrent-request comparison=%+v", comparison)
		}
		return
	}
	t.Fatal("missing coordinator concurrent-request comparison")
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
	got := m8ProductionResourcesV1(cfg, fixtureManifest{Vectors: 1, Dimensions: 1}, assets, rows, m8ProductionFaultResourceBoundaryV1{}, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{}, m8ProductionFaultResourceBoundaryV1{})
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
	got := m8ProductionResourcesV1(cfg, fixtureManifest{Vectors: 64, Dimensions: 1}, assets, rows, m8ProductionFaultResourceBoundaryV1{}, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{}, m8ProductionFaultResourceBoundaryV1{})
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
