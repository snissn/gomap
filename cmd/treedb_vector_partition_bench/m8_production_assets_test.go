package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

func TestM8BoundedWorkUsesFixedWorkerPoolV1(t *testing.T) {
	var active, peak int32
	m8RunBoundedWorkV1(32, 3, func(int) {
		current := atomic.AddInt32(&active, 1)
		for {
			observed := atomic.LoadInt32(&peak)
			if current <= observed || atomic.CompareAndSwapInt32(&peak, observed, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		atomic.AddInt32(&active, -1)
	})
	if peak != 3 {
		t.Fatalf("worker peak=%d want 3", peak)
	}
}

func TestM8BoundedWorkClampsInvalidConcurrencyV1(t *testing.T) {
	var ran int32
	m8RunBoundedWorkV1(4, 0, func(int) { atomic.AddInt32(&ran, 1) })
	if ran != 4 {
		t.Fatalf("ran=%d want 4", ran)
	}
}

func requireM8PersistentAssetSupportV1(t testing.TB) {
	t.Helper()
	if !collections.VectorPartitionNamespacePersistenceSupportedForTestingV1() {
		t.Skip("M8 persistent vector-partition assets unsupported on this platform")
	}
}

const m8ProductionTopologyTestTimeoutV1 = 80 * time.Second

func TestM8ProductionReportRejectsUnexercisedDataGroupV1(t *testing.T) {
	fixture, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	group := func(id string, hits uint64) nativewire.VectorPartitionM8ProductionGroupEvidenceV1 {
		return nativewire.VectorPartitionM8ProductionGroupEvidenceV1{
			GroupID: id, LeaderID: id + "-leader", NodeIDs: []string{id + "-a", id + "-b", id + "-c"},
			CommitIndex: 1, ReadIndex: 1, AppliedIndex: 1, ReadEvidenceKind: "production", ProvesProductionConsensus: true, EndpointHits: hits,
		}
	}
	loads := make([]uint64, 4)
	for row := 0; row < fixture.Vectors; row++ {
		loads[row%len(loads)]++
	}
	diagnostics := func(loads []uint64) []m8PartitionPackDiagnosticsV1 {
		out := make([]m8PartitionPackDiagnosticsV1, len(loads))
		for partition, load := range loads {
			out[partition] = m8PartitionPackDiagnosticsV1{PartitionID: uint32(partition), Rows: load, ReachableRows: load, TraversalRoots: 1}
		}
		return out
	}
	report := m8ProductionReportV1{
		SchemaVersion: 3, ResultKind: "m8_production_multi_group_evidence_v3", Mode: m8ProductionMultiGroupModeV1, ProductionEvidence: true,
		GeneratedAt: time.Now(), Command: []string{"m8-test"}, BaseSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", HeadSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Dataset: fixture, Config: m8ProductionConfigEvidenceV1{RaftGroups: 2, RaftNodesPerGroup: 3, Partitions: 4, TopK: 10, RouterCandidates: 4}, BuildNanos: 1,
		Topology:       nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{Network: "tcp_loopback_serialized_m5_v1", LifecycleState: "active", ReadySetDigest: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", MetaNodes: []string{"meta-a", "meta-b", "meta-c"}, Groups: []nativewire.VectorPartitionM8ProductionGroupEvidenceV1{group("group-a", 1), group("group-b", 1)}},
		RouterSessions: m8ProductionRouterSessionEvidenceV1{AfterWarmup: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{{Identity: nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{Database: "default", Catalog: "default", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: "index-digest", SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5, ReadySetDigest: "ready-digest", RouterModelDigest: "model-digest"}, ColdOpens: 1, ManifestOpenAttempts: 1, Misses: 1, ReaderPins: 1, LeasePins: 1, LeaseReleases: 1}}, AfterMeasured: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{{Identity: nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{Database: "default", Catalog: "default", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: "index-digest", SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5, ReadySetDigest: "ready-digest", RouterModelDigest: "model-digest"}, ColdOpens: 1, ManifestOpenAttempts: 1, Misses: 1, ReaderPins: 1, Hits: uint64(fixture.Queries), LeasePins: uint64(fixture.Queries) + 1, LeaseReleases: uint64(fixture.Queries) + 1}}},
		Rows: []m8ProductionRowV1{{Status: "pass", Probes: 4, EfSearch: 10, Concurrency: 1, Samples: fixture.Queries, RecallAtK: 1, QPS: 1, RouterMode: collections.VectorPartitionRouterModeApproxV1, RouterCandidates: 4, ExactParityChecked: true, ExactParityPassed: true, NoPartialResults: true, Attribution: m8ProductionAttributionV1{
			Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1, ExhaustivePartitionRecallAtK: 1,
			ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true,
			ExactRepresentativeRecallAtK: 1, ApproximateRepresentativeRecallAtK: 1, LocalHNSWRecallAtK: 1, ApproximateLocalHNSWRecallAtK: 1, EndToEndRecallAtK: 1,
			CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true,
			ApproximateRouterCandidateBudget: 4, ApproximateRouterPartitionCoverageComplete: true,
			LocalHNSWSearches: 4, LocalHNSWCandidates: 4, LocalHNSWEdges: 4,
			ApproximateLocalHNSWSearches: 4, ApproximateLocalHNSWCandidates: 4, ApproximateLocalHNSWEdges: 4,
			ResidualLossOwners: []string{"none_observed"},
		}}},
		PackDiagnostics: diagnostics(loads),
		UntimedBoundary: m8ProductionResourceBoundaryV1{SelectedPartitions: 4, EfSearch: 10, WallClockNanos: 1, Maxima: m8ProductionResourceObservedMaximaV1{Requests: 2, RPCs: 1, RequestBytes: 1, ShardPartitions: 2, ShardRequestBytes: 1}},
		Failure:         m8ProductionFailureEvidenceV1{Passed: true, Error: "unavailable group rejected", ResourceBoundary: m8ProductionFaultResourceBoundaryV1{SelectedPartitions: 4, EfSearch: 4096, WallClockNanos: 1, Maxima: m8ProductionResourceObservedMaximaV1{Requests: 2, RPCs: 1, RequestBytes: 1, ShardPartitions: 2, ShardRequestBytes: 1}}}, GateLedger: m8ProductionGateLedgerV1{FailureHonesty: "pass", PartitionPackReachability: "pass"},
		Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: 1, PartitionLoads: loads}, TimedBoundary: "measured", Limitations: []string{"test"},
	}
	variant := testM3VariantDescriptorV1(t.TempDir())
	variant.OverlapRatio, variant.OverlapRequested, variant.OverlapRealized, variant.OverlapMemberships = 0, 0, 0, 0
	variant.OverlapUseful, variant.OverlapFiller = 0, 0
	variant.PartitionLoads = make([]int, len(loads))
	for i, load := range loads {
		variant.PartitionLoads[i] = int(load)
		variant.SourceRows += uint64(load)
		variant.Capacity = max(variant.Capacity, int(load))
	}
	variant.SourceRows -= 8 // test descriptor starts with eight source rows.
	variant.Source.Vectors = int(variant.SourceRows)
	variant.OverlapUnusedCapacity = int(uint64(variant.Capacity)*uint64(variant.Partitions) - variant.SourceRows)
	variant.FixtureChecksum, variant.PersistentAssetBytes = fixture.Checksum, report.Resources.PersistentAssetBytes
	variant.VariantID, err = m3VariantIDV1(variant.AssignmentBasis, variant.OverlapRatio)
	if err != nil {
		t.Fatal(err)
	}
	variant.BuildIdentityDigest, err = m3VariantBuildIdentityDigestV1(variant)
	if err != nil {
		t.Fatal(err)
	}
	variant.OverlapPolicy, err = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: uint64(variant.Capacity), BuildIdentityDigest: variant.BuildIdentityDigest})
	if err != nil {
		t.Fatal(err)
	}
	report.Variant = &variant
	report.Config.Overlap = []float64{0}
	report.Rows[0].VariantID = variant.VariantID
	if err := validateM8ProductionReportV1(report); err != nil {
		t.Fatalf("valid endpoint coverage rejected: %v", err)
	}
	for name, mutate := range map[string]func(*m8ProductionRowV1){
		"exact_router_mode": func(row *m8ProductionRowV1) {
			row.RouterMode = collections.VectorPartitionRouterModeExactV1
		},
		"candidate_budget_above_config": func(row *m8ProductionRowV1) {
			row.RouterCandidates = 5
			row.Attribution.ApproximateRouterCandidateBudget = 5
		},
		"missing_exact_local_work": func(row *m8ProductionRowV1) {
			row.Attribution.LocalHNSWSearches = 0
		},
		"missing_approximate_local_candidates": func(row *m8ProductionRowV1) {
			row.Attribution.ApproximateLocalHNSWCandidates = 0
		},
	} {
		t.Run("rejects_"+name, func(t *testing.T) {
			invalid := report
			invalid.Rows = append([]m8ProductionRowV1(nil), report.Rows...)
			mutate(&invalid.Rows[0])
			if err := validateM8ProductionReportV1(invalid); err == nil {
				t.Fatalf("accepted measured row with %s", name)
			}
		})
	}
	zeroEdges := report
	zeroEdges.Rows = append([]m8ProductionRowV1(nil), report.Rows...)
	zeroEdges.Rows[0].Attribution.LocalHNSWEdges = 0
	zeroEdges.Rows[0].Attribution.ApproximateLocalHNSWEdges = 0
	if err := validateM8ProductionReportV1(zeroEdges); err != nil {
		t.Fatalf("valid zero-edge local searches rejected: %v", err)
	}
	shortfall := report
	shortfall.Rows = append([]m8ProductionRowV1(nil), report.Rows...)
	shortfall.Rows[0].Status = "candidate_coverage_shortfall"
	shortfall.Rows[0].RecallAtK, shortfall.Rows[0].QPS = 0, 0
	shortfall.Rows[0].ExactParityChecked, shortfall.Rows[0].ExactParityPassed, shortfall.Rows[0].NoPartialResults = false, false, false
	shortfall.Rows[0].Attribution.ApproximateRepresentativeRecallAtK = 0
	shortfall.Rows[0].Attribution.ApproximateLocalHNSWRecallAtK = 0
	shortfall.Rows[0].Attribution.ApproximateLocalHNSWSearches = 0
	shortfall.Rows[0].Attribution.ApproximateLocalHNSWCandidates = 0
	shortfall.Rows[0].Attribution.ApproximateLocalHNSWEdges = 0
	shortfall.Rows[0].Attribution.EndToEndRecallAtK = 0
	shortfall.Rows[0].Attribution.CoordinatorMergeIDParity, shortfall.Rows[0].Attribution.CoordinatorMergeScoreParity = false, false
	shortfall.Rows[0].Attribution.ApproximateRouterPartitionCoverageComplete = false
	shortfall.Rows[0].Attribution.ResidualLossOwners = []string{"approximate_representative_routing"}
	shortfall.Rows[0].Attribution.StageOwners = nil
	shortfall.GateLedger = m8ProductionGateLedgerForReportV1(shortfall)
	if err := validateM8ProductionReportV1(shortfall); err != nil {
		t.Fatalf("valid candidate-coverage shortfall rejected: %v", err)
	}
	shortfall.Rows[0].VariantID = "wrong-variant"
	if err := validateM8ProductionReportV1(shortfall); err == nil {
		t.Fatal("accepted shortfall row with mismatched variant identity")
	}
	shortfall.Rows[0].VariantID = variant.VariantID
	shortfall.Rows[0].NoPartialResults = true
	if err := validateM8ProductionReportV1(shortfall); err == nil {
		t.Fatal("accepted candidate-coverage shortfall with a partial-result claim")
	}
	for name, diagnostics := range map[string][]m8PartitionPackDiagnosticsV1{
		"missing":      report.PackDiagnostics[:3],
		"duplicate":    {report.PackDiagnostics[0], report.PackDiagnostics[0], report.PackDiagnostics[2], report.PackDiagnostics[3]},
		"disconnected": {report.PackDiagnostics[0], {PartitionID: 1, Rows: 1, ReachableRows: 0, TraversalRoots: 2}, report.PackDiagnostics[2], report.PackDiagnostics[3]},
		"row_mismatch": {report.PackDiagnostics[0], {PartitionID: 1, Rows: report.PackDiagnostics[1].Rows - 1, ReachableRows: report.PackDiagnostics[1].Rows - 1, TraversalRoots: 1}, report.PackDiagnostics[2], report.PackDiagnostics[3]},
	} {
		t.Run("rejects_"+name+"_pack_diagnostics", func(t *testing.T) {
			invalid := report
			invalid.PackDiagnostics = diagnostics
			invalid.GateLedger = m8ProductionGateLedgerForReportV1(invalid)
			if err := validateM8ProductionReportV1(invalid); err == nil {
				t.Fatalf("accepted %s partition-pack diagnostics", name)
			}
		})
	}
	measuredRows := report.Rows
	measuredAfter := report.RouterSessions.AfterMeasured
	report.Rows = []m8ProductionRowV1{{Status: "unsupported", UnsupportedReason: "overlap assets unavailable", Overlap: .2}}
	report.RouterSessions.AfterMeasured = append([]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1(nil), report.RouterSessions.AfterWarmup...)
	if err := validateM8ProductionReportV1(report); err != nil {
		t.Fatalf("unsupported-only report rejected: %v", err)
	}
	report.Rows = measuredRows
	report.RouterSessions.AfterMeasured = measuredAfter
	report.Resources.PeakRSSMeasured = true
	if err := validateM8ProductionReportV1(report); err == nil {
		t.Fatal("accepted measured peak RSS without an explicit scope")
	}
	report.Resources.PeakRSSScope = "forged measured boundary"
	if err := validateM8ProductionReportV1(report); err == nil {
		t.Fatal("accepted measured peak RSS with a forged scope")
	}
	report.Resources.PeakRSSScope = m8PeakRSSScopeV1
	report.Topology.Groups[1].EndpointHits = 0
	if err := validateM8ProductionReportV1(report); err == nil {
		t.Fatal("accepted report with an unexercised data-group endpoint")
	}
}

func TestM8PartitionPackDiagnosticsFailClosedV1(t *testing.T) {
	valid := []m8PartitionPackDiagnosticsV1{
		{PartitionID: 0, Rows: 3, ReachableRows: 3, TraversalRoots: 1},
		{PartitionID: 1, Rows: 2, ReachableRows: 2, TraversalRoots: 1},
	}
	if !validM8PartitionPackDiagnosticsV1(valid, 2, []uint64{3, 2}) {
		t.Fatal("rejected complete reachable diagnostics")
	}
	for name, diagnostics := range map[string][]m8PartitionPackDiagnosticsV1{
		"missing":      valid[:1],
		"duplicate":    {valid[0], {PartitionID: 0, Rows: 2, ReachableRows: 2, TraversalRoots: 1}},
		"disconnected": {valid[0], {PartitionID: 1, Rows: 2, ReachableRows: 1, TraversalRoots: 2}},
		"empty":        {{PartitionID: 0, Rows: 0, ReachableRows: 0, TraversalRoots: 1}, valid[1]},
		"row_mismatch": {valid[0], {PartitionID: 1, Rows: 1, ReachableRows: 1, TraversalRoots: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			if validM8PartitionPackDiagnosticsV1(diagnostics, 2, []uint64{3, 2}) {
				t.Fatalf("accepted %s diagnostics: %+v", name, diagnostics)
			}
		})
	}
}

func TestM8RouterSessionEvidenceRejectsColdWorkOrLeaseImbalanceV1(t *testing.T) {
	identity := nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{
		Database: "default", Catalog: "default", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: "index-digest",
		SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5,
		ReadySetDigest: "ready-digest", RouterModelDigest: "model-digest",
	}
	warm := nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{
		Identity: identity, ColdOpens: 1, ManifestOpenAttempts: 1, Misses: 1, ReaderPins: 1, LeasePins: 1, LeaseReleases: 1,
	}
	measured := warm
	measured.Hits, measured.LeasePins, measured.LeaseReleases = 1, 2, 2
	valid := m8ProductionRouterSessionEvidenceV1{AfterWarmup: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{warm}, AfterMeasured: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{measured}}
	if !validM8RouterSessionEvidenceV1(valid, 1) {
		t.Fatal("rejected stable warmed router evidence")
	}
	prewarmed := valid
	prewarmed.BeforeWarmup = append([]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1(nil), warm)
	if validM8RouterSessionEvidenceV1(prewarmed, 1) {
		t.Fatal("accepted nonempty pre-warm router evidence")
	}
	unsupportedOnly := valid
	unsupportedOnly.AfterMeasured = append([]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1(nil), valid.AfterWarmup...)
	if !validM8RouterSessionEvidenceV1(unsupportedOnly, 0) {
		t.Fatal("rejected unchanged unsupported-only router evidence")
	}
	if validM8RouterSessionEvidenceV1(unsupportedOnly, 1) {
		t.Fatal("accepted unchanged router evidence for measured rows")
	}
	unsupportedOnly.AfterMeasured[0].Hits++
	unsupportedOnly.AfterMeasured[0].LeasePins++
	unsupportedOnly.AfterMeasured[0].LeaseReleases++
	if validM8RouterSessionEvidenceV1(unsupportedOnly, 0) {
		t.Fatal("accepted measured deltas for unsupported-only rows")
	}
	for name, mutate := range map[string]func(*m8ProductionRouterSessionEvidenceV1){
		"new cold open":     func(e *m8ProductionRouterSessionEvidenceV1) { e.AfterMeasured[0].ColdOpens++ },
		"new manifest open": func(e *m8ProductionRouterSessionEvidenceV1) { e.AfterMeasured[0].ManifestOpenAttempts++ },
		"new miss":          func(e *m8ProductionRouterSessionEvidenceV1) { e.AfterMeasured[0].Misses++ },
		"new reader pin":    func(e *m8ProductionRouterSessionEvidenceV1) { e.AfterMeasured[0].ReaderPins++ },
		"lease imbalance":   func(e *m8ProductionRouterSessionEvidenceV1) { e.AfterMeasured[0].LeaseReleases-- },
		"identity replacement": func(e *m8ProductionRouterSessionEvidenceV1) {
			e.AfterMeasured[0].Identity.RouterModelDigest = "other-model"
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			candidate.AfterWarmup = append([]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1(nil), valid.AfterWarmup...)
			candidate.AfterMeasured = append([]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1(nil), valid.AfterMeasured...)
			mutate(&candidate)
			if validM8RouterSessionEvidenceV1(candidate, 1) {
				t.Fatal("accepted invalid router-session evidence")
			}
		})
	}
	if validM8RouterSessionEvidenceV1(valid, 2) {
		t.Fatal("accepted fewer measured router operations than report samples")
	}
	twoSamples := valid
	twoSamples.AfterMeasured = append([]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1(nil), valid.AfterMeasured...)
	twoSamples.AfterMeasured[0].Hits++
	twoSamples.AfterMeasured[0].LeasePins++
	twoSamples.AfterMeasured[0].LeaseReleases++
	if !validM8RouterSessionEvidenceV1(twoSamples, 2) {
		t.Fatal("rejected exact two-sample router accounting")
	}
}

func TestM8ProductionMultiGroupAssetsCheckedIn10kCISmokeV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
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
	truth, err := m8ExactTruthV1(assets.collection, assets.manifest, [][]float64{vectors[17]}, 10)
	if err != nil {
		t.Fatal(err)
	}
	exactUnion, err := m8ExactPartitionUnionV1(context.Background(), assets, vectors[17], 10)
	if err != nil {
		t.Fatal(err)
	}
	if idParity, scoreParity := m8CanonicalParityV1(truth[0], exactUnion); !idParity || !scoreParity {
		t.Fatalf("generation-pinned exact-union parity IDs=%v scores=%v\nwant=%+v\ngot=%+v", idParity, scoreParity, truth[0], exactUnion)
	}
	if len(exactUnion) < 2 || exactUnion[0].Score != exactUnion[1].Score || exactUnion[0].ID >= exactUnion[1].ID {
		t.Fatalf("exact-union fixture must preserve canonical FP32 ties: %+v", exactUnion)
	}
	seenExactIDs := make(map[string]struct{}, len(exactUnion))
	for _, result := range exactUnion {
		if _, duplicate := seenExactIDs[result.ID]; duplicate {
			t.Fatalf("exact union retained duplicate overlap ID %q: %+v", result.ID, exactUnion)
		}
		seenExactIDs[result.ID] = struct{}{}
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
	merged = canonicalExactNeighborsV1(merged, 10)
	for i := range truth[0] {
		if merged[i].ID != truth[0][i].ID {
			t.Fatalf("HNSW parity rank=%d got=%s want=%s got_all=%s", i, merged[i].ID, truth[0][i].ID, fmt.Sprint(merged))
		}
	}
}

func TestM8ExactPartitionUnionRejectsDuplicateOrMissingPlacementsV1(t *testing.T) {
	assets := &m8ProductionMultiGroupAssetsV1{}
	assets.manifest.PartitionCount = 2
	assets.manifest.Placements = []collections.VectorPartitionPlacementV1{{PartitionID: 0}, {PartitionID: 0}}
	if _, err := m8ExactPartitionUnionV1(context.Background(), assets, []float64{1}, 1); err == nil {
		t.Fatal("accepted duplicate placement")
	}
	assets.manifest.Placements = []collections.VectorPartitionPlacementV1{{PartitionID: 0}}
	if _, err := m8ExactPartitionUnionV1(context.Background(), assets, []float64{1}, 1); err == nil {
		t.Fatal("accepted missing placement")
	}
}

func TestM8ProductionMultiGroupTopology10kTCPV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	fixture, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	vectors := deterministicVectors(fixture)
	for i := range vectors {
		vectors[i] = append([]float64(nil), vectors[i]...)
	}
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"m8-data-group-a", "m8-data-group-b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer assets.Close()
	ctx, cancel := context.WithTimeout(context.Background(), m8ProductionTopologyTestTimeoutV1)
	defer cancel()
	topology, err := nativewire.NewVectorPartitionM8ProductionMultiGroupV1(ctx, nativewire.VectorPartitionM8ProductionMultiGroupOptionsV1{Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(), GroupAssetSetDigests: assets.assetSetDigests, Database: "default", Catalog: "default"})
	if err != nil {
		t.Fatal(err)
	}
	defer topology.Close()
	query := make([]float32, len(vectors[0]))
	for i, v := range vectors[17] {
		query[i] = float32(v)
	}
	untimedBoundary, err := m8WarmProductionTopologyV1(ctx, topology.Coordinator(), assets, [][]float64{vectors[17]}, config{topK: 10, efSearch: []int{4096}, warmup: 0})
	if err != nil {
		t.Fatalf("warmup=0 endpoint preflight: %v", err)
	}
	if untimedBoundary.SelectedPartitions != len(assets.manifest.Placements) || untimedBoundary.EfSearch != 4096 ||
		untimedBoundary.WallClockNanos == 0 || untimedBoundary.Maxima.Requests == 0 || untimedBoundary.Maxima.RPCs == 0 ||
		untimedBoundary.Maxima.RequestBytes == 0 || untimedBoundary.Maxima.ShardPartitions == 0 || untimedBoundary.Maxima.ShardRequestBytes == 0 {
		t.Fatalf("untimed preflight resource boundary=%+v", untimedBoundary)
	}
	for _, group := range topology.Evidence().Groups {
		if group.EndpointHits != 1 {
			t.Fatalf("preflight did not exercise group endpoint: %+v", group)
		}
	}
	lowProbe, err := topology.Coordinator().Search(ctx, m8ProductionRequestV1(assets, query, "m8-low-probe", 1, 4096, 10, nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxCandidateBytes))
	if err != nil {
		t.Fatal(err)
	}
	if len(lowProbe.ProbedGroups) != 1 {
		t.Fatalf("low-probe response=%+v", lowProbe)
	}
	response, err := topology.Coordinator().Search(ctx, nativewire.VectorPartitionCoordinatorRequestV1{Version: nativewire.VectorPartitionCoordinatorVersionV1, RequestID: "m8-e2e-000017", CancellationID: "m8-e2e-cancel", Database: "default", Catalog: "default", Collection: assets.manifest.Collection, IndexName: assets.manifest.IndexName, IndexDefinitionDigest: assets.manifest.IndexDefinitionDigest, Query: query, Metric: nativewire.VectorPartitionShardSearchMetricCosineV1, RouterMode: collections.VectorPartitionRouterModeExactV1, RouterCandidateBudget: 10_000, PartitionProbes: 4, Consistency: nativewire.VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: nativewire.VectorPartitionShardSearchStatsBasicV1, TopK: 10, EfSearch: 4096, DeadlineUnixNano: time.Now().Add(20 * time.Second).UnixNano(), RequestBytesLimit: 4 << 20, CandidateBytesLimit: 64 << 20, ResponseBytesLimit: 64 << 20, MergeEntriesLimit: 40})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Neighbors) != 10 || len(response.ProbedGroups) != 2 {
		t.Fatalf("response=%+v", response)
	}
	sessions := topology.Coordinator().Stats().RouterSessions
	if len(sessions) != 1 {
		t.Fatalf("router sessions=%+v", sessions)
	}
	session := sessions[0]
	if session.ColdOpens != 1 || session.ManifestOpenAttempts != 1 || session.Misses != 1 || session.Hits < 2 ||
		session.ReaderPins != 1 || session.ReaderReleases != 0 || session.LeasePins != session.LeaseReleases ||
		session.Identity.Collection != assets.manifest.Collection || session.Identity.IndexName != assets.manifest.IndexName ||
		session.Identity.PartitionGeneration != assets.manifest.Generation || session.Identity.ReadySetDigest == "" || session.Identity.RouterModelDigest == "" {
		t.Fatalf("router session=%+v", session)
	}
	// Compare the raw TCP result with independently opened partition searchers;
	// m8ExactTruthV1 below owns the full-source canonical oracle.
	direct := make([]neighbor, 0, 40)
	directScores := make(map[string]float32, 40)
	for partition := 0; partition < 4; partition++ {
		searcher, openErr := assets.collection.OpenVectorPartitionLocalSearcherForGenerationV1(partitionHNSWIndex, assets.manifest.Generation, uint32(partition))
		if openErr != nil {
			t.Fatal(openErr)
		}
		results, _, searchErr := searcher.SearchWithOptionsV1(ctx, query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 4096})
		closeErr := searcher.Close()
		if searchErr != nil || closeErr != nil {
			t.Fatalf("direct partition %d err=%v close=%v", partition, searchErr, closeErr)
		}
		for _, result := range results {
			direct = append(direct, neighbor{ID: result.ID, Distance: 1 - float64(result.Score)})
			directScores[result.ID] = result.Score
		}
	}
	sortNeighbors(direct)
	direct = dedupeSortedNeighbors(direct)[:10]
	for i, got := range response.Neighbors {
		if got.ID != direct[i].ID || math.Float32bits(got.Score) != math.Float32bits(directScores[direct[i].ID]) {
			t.Fatalf("tcp parity rank=%d got=%+v direct=%+v", i, got, direct[i])
		}
	}
	attributionQueries := [][]float64{vectors[17], vectors[18], vectors[19], vectors[20]}
	staleManifest := assets.manifest
	staleManifest.SourceGeneration++
	if _, err := m8ExactTruthV1(assets.collection, staleManifest, attributionQueries[:1], 10); err == nil {
		t.Fatal("canonical source oracle accepted a mismatched source generation")
	}
	truth, err := m8ExactTruthV1(assets.collection, assets.manifest, attributionQueries, 10)
	if err != nil {
		t.Fatal(err)
	}
	harness, err := newM8AttributionHarnessV1(assets)
	if err != nil {
		t.Fatal(err)
	}
	defer harness.Close()
	candidates := int(assets.status.Representatives)
	primaryHomes, finalMemberships, err := m8TruthPartitionMembershipsByDocumentIDV1(assets, truth)
	if err != nil {
		t.Fatal(err)
	}
	membershipOracles, err := m8MembershipOracleRecallCacheV1(truth, primaryHomes, finalMemberships, len(harness.searchers), 4)
	if err != nil {
		t.Fatal(err)
	}
	attribution, err := m8BuildAttributionV1(ctx, assets, primaryHomes, finalMemberships, attributionQueries, truth, membershipOracles, 4, 4096, 10, candidates, make([][]m8CanonicalResultV1, len(attributionQueries)), harness)
	if err != nil {
		t.Fatal(err)
	}
	row, coordinatorResults, err := m8RunProductionCellV1(ctx, topology.Coordinator(), assets, attributionQueries, truth, 4, 4096, 4, 10, 64, nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxCandidateBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := m8AttachAttributionV1(&row, attribution, coordinatorResults); err != nil {
		t.Fatal(err)
	}
	if row.RouterMode != collections.VectorPartitionRouterModeApproxV1 || row.RouterCandidates != candidates {
		t.Fatalf("row router=%s/%d want approximate/%d", row.RouterMode, row.RouterCandidates, candidates)
	}
	tiedVectors := make([][]float64, 64)
	for i := range tiedVectors {
		tiedVectors[i] = append([]float64(nil), vectors[0]...)
	}
	shortfallAssets, err := newM8ProductionMultiGroupAssetsV1(tiedVectors, []string{"m8-data-group-a", "m8-data-group-b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer shortfallAssets.Close()
	shortfallTopology, err := nativewire.NewVectorPartitionM8ProductionMultiGroupV1(ctx, nativewire.VectorPartitionM8ProductionMultiGroupOptionsV1{Collection: shortfallAssets.collection, Manifest: shortfallAssets.manifest, RouterSource: shortfallAssets.RouterSource(), GroupAssetSetDigests: shortfallAssets.assetSetDigests, Database: "default", Catalog: "default"})
	if err != nil {
		t.Fatal(err)
	}
	defer shortfallTopology.Close()
	shortfallQueries := [][]float64{tiedVectors[0], tiedVectors[0], tiedVectors[0], tiedVectors[0]}
	// The router contract accepts a positive approximate candidate budget below
	// probes and returns the typed coverage error when it cannot cover them.
	// One candidate can select at most one partition, so it deterministically
	// shortfalls four requested probes on every platform.
	shortfall, shortfallResults, err := m8RunProductionCellV1(ctx, shortfallTopology.Coordinator(), shortfallAssets, shortfallQueries, make([][]m8CanonicalResultV1, len(shortfallQueries)), 4, 4096, 4, 10, 1, nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxCandidateBytes)
	if err != nil {
		t.Fatal(err)
	}
	if shortfall.Status != "candidate_coverage_shortfall" || len(shortfallResults) != len(shortfallQueries) {
		t.Fatalf("candidate-coverage shortfall=%+v results=%d", shortfall, len(shortfallResults))
	}
	if shortfall.MaxTotalNanos == 0 || shortfall.RequestBytes == 0 || shortfall.RPCs == 0 || shortfall.MaxRequests == 0 || shortfall.MaxRPCs == 0 {
		t.Fatalf("candidate-coverage shortfall discarded coordinator work=%+v", shortfall)
	}
	for _, results := range shortfallResults {
		if results != nil {
			t.Fatalf("candidate-coverage shortfall retained a partial result: %+v", results)
		}
	}
	if row.Attribution.Contract != m8CanonicalResultContractV1 || row.Attribution.ExhaustivePartitionRecallAtK != 1 ||
		row.Attribution.ExactRepresentativeRecallAtK != 1 || row.Attribution.ApproximateRepresentativeRecallAtK != 1 ||
		row.Attribution.LocalHNSWRecallAtK != 1 || row.Attribution.ApproximateLocalHNSWRecallAtK != 1 || row.Attribution.EndToEndRecallAtK != 1 ||
		row.Attribution.LocalHNSWSearches == 0 || row.Attribution.LocalHNSWCandidates == 0 || row.Attribution.LocalHNSWEdges == 0 ||
		row.Attribution.ApproximateLocalHNSWSearches == 0 || row.Attribution.ApproximateLocalHNSWCandidates == 0 || row.Attribution.ApproximateLocalHNSWEdges == 0 ||
		!row.Attribution.ExhaustivePartitionIDParity || !row.Attribution.ExhaustivePartitionScoreParity ||
		!row.Attribution.CoordinatorMergeIDParity || !row.Attribution.CoordinatorMergeScoreParity || !row.NoPartialResults {
		t.Fatalf("attribution=%+v", row.Attribution)
	}
	evidence := topology.Evidence()
	if evidence.LifecycleState != "active" || len(evidence.Groups) != 2 {
		t.Fatalf("evidence=%+v", evidence)
	}
	for _, group := range evidence.Groups {
		if len(group.NodeIDs) != 3 || group.EndpointHits < 2 || group.CommitIndex == 0 || group.ReadIndex == 0 || group.AppliedIndex < group.ReadIndex || group.ReadEvidenceKind != "production" || !group.ProvesProductionConsensus {
			t.Fatalf("group evidence=%+v", group)
		}
	}
	failure, postFaultTopology := m8RunUnavailableGroupV1(ctx, topology, assets, vectors[17], 10, nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxCandidateBytes)
	if !failure.Passed || failure.StoppedGroup == "" || failure.Error == "" || failure.ReturnedNeighbors != 0 || failure.ReturnedGroups != 0 {
		t.Fatalf("stopped group failure evidence=%+v", failure)
	}
	if failure.ResourceBoundary.SelectedPartitions != len(assets.manifest.Placements) || failure.ResourceBoundary.EfSearch != 4096 ||
		failure.ResourceBoundary.WallClockNanos == 0 || failure.ResourceBoundary.Maxima.Requests == 0 ||
		failure.ResourceBoundary.Maxima.RPCs == 0 || failure.ResourceBoundary.Maxima.RequestBytes == 0 ||
		failure.ResourceBoundary.Maxima.ShardPartitions == 0 || failure.ResourceBoundary.Maxima.ShardRequestBytes == 0 {
		t.Fatalf("stopped group resource boundary=%+v", failure.ResourceBoundary)
	}
	if len(postFaultTopology.Groups) != len(evidence.Groups) || postFaultTopology.MaxConcurrentShardRequests < evidence.MaxConcurrentShardRequests {
		t.Fatalf("post-fault topology=%+v pre-fault topology=%+v", postFaultTopology, evidence)
	}
}

func TestM8ExistingAssetsRelabelsTopologyWithoutMutatingLocalPacksV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	fixture, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	vectors := deterministicVectors(fixture)
	local, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"local-a", "local-b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	dir, original := local.dir, local.manifest
	local.owned = false
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	groups := []string{"topology-a", "topology-b", "topology-c", "topology-d"}
	assets, err := openM8ProductionMultiGroupExistingAssetsV1(dir, groups, 4, fixture, vectors)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := assets.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	if assets.manifest.ReadySetDigest == original.ReadySetDigest {
		t.Fatal("topology manifest retained local ready-set digest after placement relabel")
	}
	for i, placement := range assets.manifest.Placements {
		if placement.GroupID != groups[int(placement.PartitionID)%len(groups)] || placement.GroupID == original.Placements[i].GroupID && len(groups) > 2 {
			t.Fatalf("relabeled placement=%+v original=%+v", placement, original.Placements[i])
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), m8ProductionTopologyTestTimeoutV1)
	defer cancel()
	topology, err := nativewire.NewVectorPartitionM8ProductionMultiGroupV1(ctx, nativewire.VectorPartitionM8ProductionMultiGroupOptionsV1{Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(), GroupAssetSetDigests: assets.assetSetDigests, Database: "default", Catalog: "default"})
	if err != nil {
		t.Fatal(err)
	}
	defer topology.Close()
	query := make([]float32, len(vectors[0]))
	for i, value := range vectors[17] {
		query[i] = float32(value)
	}
	response, searchErr := topology.Coordinator().Search(ctx, nativewire.VectorPartitionCoordinatorRequestV1{Version: nativewire.VectorPartitionCoordinatorVersionV1, RequestID: "m8-existing-assets", CancellationID: "m8-existing-assets-cancel", Database: "default", Catalog: "default", Collection: assets.manifest.Collection, IndexName: assets.manifest.IndexName, IndexDefinitionDigest: assets.manifest.IndexDefinitionDigest, Query: query, Metric: nativewire.VectorPartitionShardSearchMetricCosineV1, RouterMode: collections.VectorPartitionRouterModeExactV1, RouterCandidateBudget: 10_000, PartitionProbes: 4, Consistency: nativewire.VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: nativewire.VectorPartitionShardSearchStatsBasicV1, TopK: 10, EfSearch: 4096, DeadlineUnixNano: time.Now().Add(30 * time.Second).UnixNano(), RequestBytesLimit: 4 << 20, CandidateBytesLimit: 64 << 20, ResponseBytesLimit: 64 << 20, MergeEntriesLimit: 40})
	if searchErr != nil || len(response.Neighbors) != 10 || len(response.ProbedGroups) != 4 {
		t.Fatalf("relabelled topology response=%+v err=%v", response, searchErr)
	}
	if err := topology.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("existing asset directory removed or inaccessible after topology cleanup: %v", err)
	}
}

func TestM8ExistingAssetsRejectsDifferentFixtureV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	fixture, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	vectors := deterministicVectors(fixture)
	local, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"local-a", "local-b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	dir := local.dir
	local.owned = false
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	different := fixture
	different.Seed++
	if _, err = openM8ProductionMultiGroupExistingAssetsV1(dir, []string{"topology-a", "topology-b"}, 4, different, deterministicVectors(different)); err == nil {
		t.Fatal("accepted retained assets from a different fixture")
	}
}
