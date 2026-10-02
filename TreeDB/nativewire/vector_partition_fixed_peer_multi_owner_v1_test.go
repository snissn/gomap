package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// This is the deployment test, not a local topology simulation. In particular,
// a node may never receive another owner's graph segment during fixture setup.
func TestMultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1(t *testing.T) {
	testMultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1(t, false, false, false)
}

func TestMultiOwnerTCPDomainSearchWithSeparateCatalogAndSourceLeadersV1(t *testing.T) {
	testMultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1(t, true, false, false)
}

func TestMultiOwnerTCPDomainSearchWithCatalogLeaderOnSourceFollowerV1(t *testing.T) {
	testMultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1(t, true, true, false)
}

func TestMultiOwnerTCPDomainSearchRejectsMissingHostedOwnerAssetV1(t *testing.T) {
	testMultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1(t, false, false, true)
}

// This fresh fixture qualifies the accepted canonical graph model through the
// public lifecycle and TCP path. It does not qualify the retained epoch-zero
// 100K fixture, its D16/P64 geometry, recall, or performance.
func TestMultiOwnerTCPAcceptedModelFreshIndexEpochV1(t *testing.T) {
	testMultiOwnerTCPAcceptedModelFreshIndexEpochV1(t, 64, 1, 4, 8, 16)
}

// Exercise the real ACTIVE-to-observer setup ordering on the existing fresh64
// deployment, without enabling retained comparative collection or profiling.
func TestMultiOwnerTCPCostSetupAfterActiveFreshIndexEpochV1(t *testing.T) {
	t.Setenv("GOMAP_ACCEPTED_COMPARATIVE_COST_V1", "")
	t.Setenv("GOMAP_FIXED_PEER_COST_SETUP_PREFLIGHT_V1", "1")
	t.Log("cost observation setup mode: fresh64 ACTIVE-before-first-search; comparative collection disabled")
	testMultiOwnerTCPAcceptedModelFreshIndexEpochV1(t, 64, 1, 4, 8, 16)
}

// These declared deterministic queries check public/native parity, not held-out
// recall or performance. Both fresh domain graphs have more than L256 rows.
func TestMultiOwnerTCPAcceptedModelScaledCorrectnessV1(t *testing.T) {
	testMultiOwnerTCPAcceptedModelFreshIndexEpochV1(t, 1024, 32, 10, 96, 32)
}

func testMultiOwnerTCPAcceptedModelFreshIndexEpochV1(t *testing.T, rowCount, queryCount, topK, efSearch, mergeEntries int) {
	t.Helper()
	definition := collections.VectorIndexDefinition{
		Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine,
		Dimensions: 768, M: 16, EfConstruction: 128, EfSearch: 128,
		Encoding: collections.VectorIndexEncodingFloat32, Strategy: collections.VectorIndexStrategyColumnGraph,
		SchemaGeneration: 1,
	}
	// This value copy checks the historical definition binding; it never edits
	// the retained fixture or changes its accepted epoch-zero digest.
	historical := definition
	historical.SchemaGeneration = 0
	if digest := collections.VectorIndexDefinitionDigestV1(historical); digest != "acb460d2a9c2fe4932668e7d01cce03ac791579dd750180b78c814c86c64b87e" {
		t.Fatalf("accepted source definition parameters changed: %s", digest)
	}
	documents := make([]vectorPartitionLiveDocumentV1, rowCount)
	for i := range documents {
		vector := make([]float32, definition.Dimensions)
		for dimension := range vector {
			vector[dimension] = float32(((i+1)*(dimension+3)+i*i)%31-15) / 16
		}
		vector[0] += float32(i) / float32(rowCount)
		documents[i] = vectorPartitionLiveDocumentV1{id: fmt.Sprintf("fresh-%02d", i), vector: vector, home: uint32(i % 3)}
	}
	seed := newVectorPartitionLiveNativewireDocumentsWithDefinitionV1(t, documents, nil, [2]string{"group-b", "group-c"}, true, true, definition)
	t.Cleanup(func() { _ = seed.database.Close() })
	// Seed construction is synchronous setup, not part of the TCP correctness
	// deadline. Keep the real graph geometry and the same bounded operation budget.
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	persisted := seed.collection.MetaView()
	expectedDigest := collections.VectorIndexDefinitionDigestV1(definition)
	if len(persisted.VectorIndexes) != 1 || persisted.VectorIndexes[0].SchemaGeneration != 1 ||
		collections.VectorIndexDefinitionDigestV1(persisted.VectorIndexes[0]) != expectedDigest ||
		seed.manifest.IndexDefinitionDigest != expectedDigest || expectedDigest == collections.VectorIndexDefinitionDigestV1(historical) {
		t.Fatalf("fresh trusted genesis did not preserve its distinct epoch/definition binding: meta=%+v manifest=%+v", persisted, seed.manifest)
	}
	if seed.manifest.SourceRowCount != uint64(rowCount) || seed.manifest.PartitionCount != 3 || seed.manifest.DomainCount != 2 {
		t.Fatalf("fresh fixture geometry changed: %+v", seed.manifest)
	}
	graphs := 0
	for _, asset := range seed.manifest.Assets {
		if asset.GraphVariant == "" {
			continue // Physical chunks inherit their descriptor's graph identity.
		}
		if asset.GraphVariant != string(collections.VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1) {
			t.Fatalf("fresh asset %s uses another graph model: %s", asset.ID, asset.GraphVariant)
		}
		graphs++
	}
	if graphs != 2 {
		t.Fatalf("fresh fixture has %d canonical domain graph descriptors, want 2", graphs)
	}
	domainRows, err := collections.VectorPartitionDomainGraphRowCountsV1(ctx, seed.manifest)
	if err != nil || len(domainRows) != 3 || domainRows[0] != uint64(rowCount-rowCount/3) || domainRows[1] != 0 || domainRows[2] != uint64(rowCount/3) {
		t.Fatalf("fresh domain row counts: rows=%v err=%v", domainRows, err)
	}
	queries := make([][]float32, queryCount)
	for i := range queries {
		// The 64-row wrapper retains its original query at document 37.
		queries[i] = append([]float32(nil), documents[(37+29*i)%rowCount].vector...)
	}
	t.Logf("fresh public fixture: rows=%d dimensions=768 packs=3 domains=2 owners=2 epoch=1 definition=%s canonical_graphs=%d", rowCount, expectedDigest, graphs)
	t.Logf("declared public queries: count=%d domain_rows=%d/%d top_k=%d probes=2 ef_search=%d merge_entries=%d", queryCount, domainRows[0], domainRows[2], topK, efSearch, mergeEntries)
	testMultiOwnerTCPDomainSearchWithQueriesV1(t, ctx, seed, queries, topK, efSearch, mergeEntries, false, false, false)
}

func testMultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1(t *testing.T, separateLeaders, sourceFollower, missingOwnerAsset bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0},
		{id: "b", vector: []float32{.8, .2}, home: 1},
		{id: "c", vector: []float32{0, 1}, home: 2},
		{id: "d", vector: []float32{.2, .8}, home: 2},
	}, nil, [2]string{"group-b", "group-c"}, true, true)
	testMultiOwnerTCPDomainSearchWithSeedV1(t, ctx, seed, []float32{.7, .7}, separateLeaders, sourceFollower, missingOwnerAsset)
}

func testMultiOwnerTCPDomainSearchWithSeedV1(t *testing.T, ctx context.Context, seed vectorPartitionLiveProductionFixtureV1, query []float32, separateLeaders, sourceFollower, missingOwnerAsset bool) {
	t.Helper()
	testMultiOwnerTCPDomainSearchWithQueriesV1(t, ctx, seed, [][]float32{query}, 4, 8, 16, separateLeaders, sourceFollower, missingOwnerAsset)
}

func testMultiOwnerTCPDomainSearchWithSeedModeV1(t *testing.T, ctx context.Context, seed vectorPartitionLiveProductionFixtureV1, query []float32, separateLeaders, sourceFollower, missingOwnerAsset, consumerOwner, readCost bool) {
	t.Helper()
	testMultiOwnerTCPDomainSearchWithQueriesModeV1(t, ctx, seed, [][]float32{query}, 4, 8, 16, separateLeaders, sourceFollower, missingOwnerAsset, consumerOwner, readCost)
}

func testMultiOwnerTCPDomainSearchWithQueriesV1(t *testing.T, ctx context.Context, seed vectorPartitionLiveProductionFixtureV1, queries [][]float32, topK, efSearch, mergeEntries int, separateLeaders, sourceFollower, missingOwnerAsset bool) {
	t.Helper()
	testMultiOwnerTCPDomainSearchWithQueriesModeV1(t, ctx, seed, queries, topK, efSearch, mergeEntries, separateLeaders, sourceFollower, missingOwnerAsset, false, false)
}

func testMultiOwnerTCPDomainSearchWithQueriesModeV1(t *testing.T, ctx context.Context, seed vectorPartitionLiveProductionFixtureV1, queries [][]float32, topK, efSearch, mergeEntries int, separateLeaders, sourceFollower, missingOwnerAsset, consumerOwner, readCost bool) {
	t.Helper()
	testMultiOwnerTCPDomainSearchWithQualificationV1(t, ctx, seed, queries, topK, efSearch, mergeEntries, 2, 0, nil, separateLeaders, sourceFollower, missingOwnerAsset, consumerOwner, readCost)
}

func testMultiOwnerTCPDomainSearchWithQualificationV1(t *testing.T, ctx context.Context, seed vectorPartitionLiveProductionFixtureV1, queries [][]float32, topK, efSearch, mergeEntries, probes, warmQueries int, truth [][]VectorPartitionCoordinatorNeighborV1, separateLeaders, sourceFollower, missingOwnerAsset, consumerOwner, readCost bool) {
	t.Helper()
	if len(queries) == 0 || probes < 1 || probes > int(seed.manifest.DomainCount) || warmQueries < 0 || warmQueries > 16 || (truth != nil && len(truth) != len(queries)) {
		t.Fatal("public parity requires a declared query")
	}
	// The opt-in observes the existing correctness corpus once per arm. It is
	// not testing.AllocsPerRun (which changes GOMAXPROCS and repeats queries).
	costSetting := os.Getenv("GOMAP_ACCEPTED_COMPARATIVE_COST_V1")
	comparativeCost := costSetting == "1"
	if costSetting != "" && !comparativeCost {
		t.Fatal("GOMAP_ACCEPTED_COMPARATIVE_COST_V1 must be empty or 1")
	}
	if comparativeCost && (len(queries) != 512 || truth == nil || warmQueries != 16 || probes != 5 || topK != 10 || efSearch != 96 || mergeEntries != 256 || separateLeaders || sourceFollower || missingOwnerAsset || consumerOwner || readCost) {
		t.Fatal("comparative cost requires only the pinned accepted 512-query qualification")
	}
	setupSetting := os.Getenv("GOMAP_FIXED_PEER_COST_SETUP_PREFLIGHT_V1")
	costSetupPreflight := setupSetting == "1"
	if setupSetting != "" && !costSetupPreflight {
		t.Fatal("GOMAP_FIXED_PEER_COST_SETUP_PREFLIGHT_V1 must be empty or 1")
	}
	if costSetupPreflight && (comparativeCost || os.Getenv("GOMAP_SELECTED_LIVE_FIXTURE") != "" || os.Getenv("GOMAP_SELECTED_LIVE_RECEIPTS") != "" || seed.manifest.SourceRowCount != 64 || len(queries) != 1 || truth != nil || warmQueries != 0 || probes != 2 || topK != 4 || efSearch != 8 || mergeEntries != 16 || separateLeaders || sourceFollower || missingOwnerAsset || consumerOwner || readCost) {
		t.Fatal("cost setup preflight requires only the existing fresh64 qualification")
	}
	type costWindow struct {
		Before, After                 FixedPeerDiagnosticsV1
		ElapsedNanos                  int64
		BytesPerQuery, AllocsPerQuery float64
	}
	var localCost, tcpClientCost costWindow
	var profilePaths []string
	var profileBoundaries []fixedPeerCostBoundaryV1
	var frameBefore, frameAfter fixedPeerCostBoundaryV1
	var servingRSS fixedPeerCostRSSV1
	var rssObserver *fixedPeerCostRSSObserverV1
	profileDir := os.Getenv("GOMAP_SELECTED_LIVE_RECEIPTS")
	if comparativeCost && (!filepath.IsAbs(profileDir) || runtime.MemProfileRate <= 0) {
		t.Fatal("comparative cost requires absolute retained output directory and enabled sampled memory profiles")
	}
	profilePath := func(name string) string {
		path := filepath.Join(profileDir, "accepted-cost-"+name+".pprof")
		profilePaths = append(profilePaths, path)
		return path
	}
	// These two batch-boundary samples include all activity in this process.
	// OS observations use the existing diagnostics reader; no query is sampled.
	sampleParent := func(before bool) FixedPeerDiagnosticsV1 {
		var report FixedPeerDiagnosticsV1
		// Keep this parent's OS reader allocations outside both counter bounds.
		if before {
			peerOSDiagnosticsV1(&report, nil)
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		report.Process.SampleUnixNano = uint64(time.Now().UnixNano())
		report.Process.HeapAllocBytes, report.Process.HeapObjects = memory.HeapAlloc, memory.HeapObjects
		report.Process.TotalAllocBytes, report.Process.Mallocs, report.Process.Frees = memory.TotalAlloc, memory.Mallocs, memory.Frees
		report.Process.NumGC, report.Process.PauseTotalNanos = uint64(memory.NumGC), memory.PauseTotalNs
		report.Process.Goroutines, report.Process.LogicalCPUs, report.Process.GOMAXPROCS = uint64(runtime.NumGoroutine()), runtime.NumCPU(), runtime.GOMAXPROCS(0)
		report.Process.GoMemoryLimitBytes = debug.SetMemoryLimit(-1)
		if !before {
			peerOSDiagnosticsV1(&report, nil)
		}
		return report
	}
	finishCost := func(window *costWindow, started time.Time) {
		window.ElapsedNanos = time.Since(started).Nanoseconds()
		window.After = sampleParent(false)
		if window.After.Process.TotalAllocBytes < window.Before.Process.TotalAllocBytes || window.After.Process.Mallocs < window.Before.Process.Mallocs {
			t.Fatal("process allocation counters decreased")
		}
		window.BytesPerQuery = float64(window.After.Process.TotalAllocBytes-window.Before.Process.TotalAllocBytes) / float64(len(queries))
		window.AllocsPerQuery = float64(window.After.Process.Mallocs-window.Before.Process.Mallocs) / float64(len(queries))
	}
	query := queries[0] // One representative query covers refusal/loss/reopen.
	if err := seed.collection.EnsureVectorPartitionLiveBindingV1(ctx, seed.manifest); err != nil {
		t.Fatal(err)
	}
	appliedCommandLSN := seed.database.State().AppliedCommandLSN
	if appliedCommandLSN == 0 {
		t.Fatal("trusted seed has no command-WAL coverage")
	}
	// Only the schema/index definition is copied into each node's trusted
	// genesis. The builder's document rows and full ColumnGraph stay behind.
	meta := seed.collection.MetaView()
	if meta.Options.ColumnStore == nil {
		t.Fatal("source fixture has no column-store definition")
	}
	columnStore := *meta.Options.ColumnStore
	columnStore.ActiveManifest = nil // the active source descriptor belongs only to group-d
	columnStore.RecoveryAuthoritativeManifest = nil
	columnStore.RecoveryAuthoritativeAppliedCommandLSN = 0
	meta.Options.ColumnStore = &columnStore
	configs := fixedPeerMultiOwnerSearchConfigsV1(t, seed.manifest, meta)
	for i := range configs {
		configs[i].Vector.RequestBase.PartitionProbes = probes
		if probes == 5 {
			configs[i].Vector.RequestBase.RouterScoreBudget = 256
		}
		configs[i].Vector.RequestBase.TopK = topK
		configs[i].Vector.RequestBase.EfSearch = efSearch
		configs[i].Vector.RequestBase.MergeEntriesLimit = mergeEntries
	}
	if comparativeCost && (configs[0].Vector.RequestBase.RouterMode != collections.VectorPartitionRouterModeExactV1 || configs[0].Vector.RequestBase.RouterScoreBudget != 256) {
		t.Fatal("comparative cost requires exact accepted C256 routing")
	}
	if consumerOwner {
		fixedPeerCatalogConsumerOwnerConfigV1(t, configs)
	}
	metaLeader := raftcluster.NodeID("source-holder")
	if separateLeaders {
		metaLeader = "ingress"
		if sourceFollower {
			configs = fixedPeerAddSourceFollowerMetaLeaderV1(t, configs)
			metaLeader = "source-follower"
		}
	}
	if separateLeaders || consumerOwner || readCost {
		ca := newPeerCAFixtureV1(t)
		for i := range configs {
			configs[i].Catalog.BootstrapNode = metaLeader
			configs[i].ClusterID = "multi-owner-separate-leaders"
			configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
			if consumerOwner && configs[i].NodeID == "owner-b" {
				t.Log("catalog-consumer owner config preflight: node=owner-b catalog_voter=false immutable=true credentialed=true local_data_group=group-b")
			}
			validated, _, err := validateFixedPeerConfigV1(configs[i])
			if err != nil {
				if consumerOwner && configs[i].NodeID == "owner-b" {
					t.Fatalf("catalog-consumer owner config preflight rejected: %v", err)
				}
				t.Fatal(err)
			}
			raw, err := json.Marshal(validated)
			if err != nil {
				t.Fatal(err)
			}
			// Credentialed startup pairs empty roots before admitting data.
			// Record that identity before trusted genesis populates DataRoot.
			if err := preparePeerStorageV1(validated, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Search the same immutable generation locally before the source holder is
	// closed. This is a result reference, not a substitute for the hosted-only
	// process and wire assertions below.
	localFixture := seed
	resolvedCatalog, err := raftplacement.Validate(configs[0].Vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	localFixture.catalog = resolvedCatalog
	localFixture.placement = configs[0].Vector.Placement
	localServices, localSources := newVectorPartitionLiveProductionServicesV1(t, localFixture)
	localCoordinator, err := NewVectorPartitionCoordinatorForTopologyV1(
		vectorPartitionLiveCoordinatorTopologyV1(localFixture),
		CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: seed.collection},
		&vectorPartitionLiveProductionDispatcherV1{services: localServices}, VectorPartitionCoordinatorLimitsV1{},
	)
	if err != nil {
		t.Fatal(err)
	}
	localResults := make([]VectorPartitionCoordinatorResponseV1, len(queries))
	validateLocal := func(i int, localResult VectorPartitionCoordinatorResponseV1) {
		if localResult.SourceGeneration != seed.manifest.SourceGeneration || localResult.SourceChecksum != seed.manifest.SourceChecksum ||
			localResult.SourceSchemaHash != seed.manifest.SourceSchemaHash || localResult.SourceRowCount != seed.manifest.SourceRowCount ||
			localResult.PartitionGeneration != seed.manifest.Generation || localResult.Counters.SelectedDomains != uint64(probes) ||
			localResult.Counters.SelectedPartitions != uint64(probes) || localResult.Counters.HNSWServedPartitions != uint64(probes) || localResult.Counters.ExactScanPartitions != 0 ||
			len(localResult.Neighbors) != min(topK, int(seed.manifest.SourceRowCount)) {
			t.Fatalf("same-generation local reference query %d omitted a domain or result: %+v", i, localResult)
		}
	}
	var localRequests []VectorPartitionCoordinatorRequestV1
	var localStarted time.Time
	if comparativeCost {
		localRequests = make([]VectorPartitionCoordinatorRequestV1, len(queries))
		for i, query := range queries {
			localRequests[i] = configs[0].Vector.RequestBase
			localRequests[i].RequestID, localRequests[i].CancellationID = fmt.Sprintf("local-parity-%d", i), fmt.Sprintf("local-parity-cancel-%d", i)
			localRequests[i].Query = append([]float32(nil), query...)
		}
		for i := 0; i < warmQueries; i++ {
			warm := localRequests[i]
			warm.DeadlineUnixNano = time.Now().Add(30 * time.Second).UnixNano()
			result, err := localCoordinator.Search(ctx, warm)
			if err != nil {
				t.Fatalf("bounded local warm query %d: %v", i, err)
			}
			validateLocal(i, result)
		}
		if err := fixedPeerCostProfileV1(profilePath("local-before")); err != nil {
			t.Fatal(err)
		}
		localCost.Before = sampleParent(true)
		localStarted = time.Now()
	}
	for i, query := range queries {
		localRequest := configs[0].Vector.RequestBase
		if comparativeCost {
			localRequest = localRequests[i]
		} else {
			localRequest.RequestID, localRequest.CancellationID = fmt.Sprintf("local-parity-%d", i), fmt.Sprintf("local-parity-cancel-%d", i)
			localRequest.Query = append([]float32(nil), query...)
		}
		localRequest.DeadlineUnixNano = time.Now().Add(30 * time.Second).UnixNano()
		localResult, err := localCoordinator.Search(ctx, localRequest)
		if err != nil {
			t.Fatalf("same-generation local reference query %d: %v", i, err)
		}
		if !comparativeCost {
			validateLocal(i, localResult)
		}
		localResults[i] = localResult
	}
	if comparativeCost {
		finishCost(&localCost, localStarted)
		if err := fixedPeerCostProfileV1(profilePath("local-after")); err != nil {
			t.Fatal(err)
		}
		for i, result := range localResults {
			validateLocal(i, result)
		}
	}
	if truth != nil {
		retainLiveLifecycleJSONV1(t, "accepted-native-reference", localResults)
	}
	if err := localCoordinator.Close(); err != nil {
		t.Fatal(err)
	}
	for _, source := range localSources {
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.database.Close(); err != nil {
		t.Fatal(err)
	}

	// A domain's sections must remain in one owner. Separate domains must not
	// share a physical segment, or copying just hosted assets is impossible.
	graphSegments := map[raftcluster.GroupID]map[uint32]bool{
		"group-b": {}, "group-c": {},
	}
	placements := make(map[uint32]raftcluster.GroupID, len(seed.manifest.Placements))
	for _, placement := range seed.manifest.Placements {
		if _, exists := placements[placement.PartitionID]; exists {
			t.Fatalf("duplicate placement for domain %d", placement.PartitionID)
		}
		placements[placement.PartitionID] = raftcluster.GroupID(placement.GroupID)
	}
	chunksInMultiPackDomain := 0
	for _, asset := range seed.manifest.Assets {
		group, exists := placements[asset.PartitionID]
		if !exists || graphSegments[group] == nil {
			t.Fatalf("asset %q has no valid owner placement for domain %d", asset.ID, asset.PartitionID)
		}
		graphSegments[group][asset.Ref.FileID] = true
		if asset.PartitionID == 0 && strings.Contains(asset.ID, "/chunk/") {
			chunksInMultiPackDomain++
		}
	}
	if seed.manifest.PartitionCount == 3 && chunksInMultiPackDomain < 2 {
		t.Fatalf("domain 0 has %d physical chunks, want at least two", chunksInMultiPackDomain)
	}
	for fileID := range graphSegments["group-b"] {
		if graphSegments["group-c"][fileID] {
			t.Fatalf("owner-local publication gap: domains owned by group-b and group-c share physical segment %d", fileID)
		}
	}

	hostedFiles := make(map[raftcluster.GroupID]map[string]bool)
	for _, config := range configs {
		if config.NodeID == "source-holder" {
			// The non-serving source holder alone retains the full builder DB.
			// It is the catalog leader for a later source-validated BUILD.
			root := filepath.Join(config.DataRoot, "group-d")
			if err := os.CopyFS(root, os.DirFS(seed.dir)); err != nil {
				t.Fatal(err)
			}
			if err := backenddb.RebindDurableRootSnapshotV1(root); err != nil {
				t.Fatal(err)
			}
			bootstrapFixedPeerVectorTrustedGenesisV1(t, config, appliedCommandLSN)
			fixedPeerAssertSourceDocumentCountV1(t, root, seed.manifest.Collection, int(seed.manifest.SourceRowCount))
			continue
		}
		if config.NodeID == "source-follower" {
			fixedPeerBootstrapHostedVectorMetadataV1(t, config, meta, filepath.Join(config.DataRoot, "group-d"))
			continue
		}
		group := raftcluster.GroupID("group-a")
		switch config.NodeID {
		case "owner-b":
			group = "group-b"
		case "owner-c":
			group = "group-c"
		}
		root := filepath.Join(config.DataRoot, string(group))
		hostedFiles[group] = fixedPeerCopyHostedVectorAssetsV1(t, seed.dir, root, seed.manifest, graphSegments[group], group == "group-a")
		fixedPeerBootstrapHostedVectorMetadataV1(t, config, meta, root)
		// Metadata creation must not materialize a foreign graph or the full
		// builder source before any serving process is opened.
		fixedPeerAssertHostedVectorFilesV1(t, root, hostedFiles[group], false)
		fixedPeerAssertSourceDocumentCountV1(t, root, seed.manifest.Collection, 0)
	}
	processes := make([]*fixedPeerTestProcessV1, len(configs))
	if sourceFollower {
		// The meta bootstrap member must be running before quorum arrives.
		processes[len(configs)-1] = fixedPeerStartTestProcessV1(t, configs[len(configs)-1])
	}
	for i, config := range configs {
		if processes[i] != nil {
			continue
		}
		processes[i] = fixedPeerStartTestProcessV1(t, config)
	}
	client, err := NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, config := range configs {
			id := config.NodeID
			status, err := client.Status(ctx, id)
			if consumerOwner && id == "owner-b" {
				if err != nil || status.CatalogRole != "consumer" || status.Catalog.AppliedIndex != 0 || status.CatalogRaft.GroupID != "" || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" {
					return false
				}
				continue
			}
			if err != nil || status.CatalogRaft.LeaderID == "" || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" ||
				((separateLeaders || consumerOwner) && (status.CatalogRaft.LeaderID != metaLeader ||
					((id == "source-holder" || id == "source-follower") && status.Groups[0].LeaderID != "source-holder"))) {
				return false
			}
		}
		return true
	})
	record, err := raftplacement.NewCatalogMetaRecordV1(1, configs[0].Vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, metaLeader, command); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, config := range configs {
			id := config.NodeID
			status, err := client.Status(ctx, id)
			if consumerOwner && id == "owner-b" {
				if err != nil || status.CatalogRole != "consumer" || status.Catalog.AppliedIndex != 0 {
					return false
				}
				continue
			}
			if err != nil || status.Catalog.AppliedIndex == 0 || status.Catalog.Digest != record.Digest {
				return false
			}
		}
		return true
	})
	for _, node := range []raftcluster.NodeID{"ingress", "owner-b", "owner-c"} {
		if report, err := client.ReadinessV1(ctx, node); err == nil || report.Ready {
			t.Fatalf("%s reported immutable readiness before ACTIVE and listener warm: %+v err=%v", node, report, err)
		}
	}
	if separateLeaders || consumerOwner {
		// owner-b has a valid cluster certificate but is not the catalog
		// leader. It may not request a source capture or publish BUILD.
		wrongCaller, err := NewFixedPeerTCPClientV1(configs[1])
		if err != nil {
			t.Fatal(err)
		}
		_, captureErr := wrongCaller.call(ctx, "source-holder", "vector-lifecycle", fixedPeerRequestV1{
			VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorLifecycleCaptureSourceV1},
		}, false)
		wrongCaller.Close()
		if !errors.Is(captureErr, errPeerAuthenticationV1) {
			t.Fatalf("non-meta peer source capture: got %v, want peer authentication refusal", captureErr)
		}
	}
	if _, err := client.EnsureImmutableVectorLifecycleV1(ctx); err != nil {
		t.Fatalf("source-verified BUILD, hosted Stage, and ACTIVE: %v", err)
	}
	// ACTIVE creation warms the lazily constructed ingress topology without
	// opening shard connection pools. Install the observer before any search.
	if comparativeCost || costSetupPreflight {
		for _, process := range processes {
			setup := fixedPeerCostObserveV1(t, ctx, process, "setup", "")
			if setup.SampleUnixNano <= 0 || setup.MemoryProfileRate <= 0 || setup.RequestFrameBytes != 0 || setup.ResponseFrameBytes != 0 {
				t.Fatalf("child setup before first shard connection: %+v", setup)
			}
		}
	}
	if faultDir := os.Getenv("GOMAP_FIXED_PEER_ACTIVE_INVALIDATION_CONTROL"); faultDir != "" {
		fixedPeerWaitActiveInvalidationFileV1(t, ctx, faultDir, "hook-ready")
	}
	for _, node := range []raftcluster.NodeID{"ingress", "owner-b", "owner-c"} {
		if report, err := client.ReadinessV1(ctx, node); err != nil || !report.Ready {
			t.Fatalf("warmed %s readiness: %+v err=%v", node, report, err)
		}
		peer, err := DialContext(ctx, "tcp", configs[0].Vector.PublicAddresses[node])
		if err != nil {
			t.Fatalf("public listener for %s: %v", node, err)
		}
		status, statusErr := peer.VectorStatusV1(ctx)
		closeErr := peer.Close()
		if err := errors.Join(statusErr, closeErr); err != nil {
			t.Fatalf("warmed %s status: %v", node, err)
		}
		if !status.Health.Ready || status.Health.Reason != "ready" {
			t.Fatalf("warmed %s is not ready: %+v", node, status.Health)
		}
	}
	publicClient, err := DialContext(ctx, "tcp", configs[0].Vector.PublicAddresses["ingress"])
	if err != nil {
		t.Fatal(err)
	}
	defer publicClient.Close()
	request := public.SearchRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: seed.manifest.IndexName, Generation: seed.manifest.Generation},
		Query: append([]float32(nil), query...), Metric: public.MetricCosineV1, TopK: topK, Probes: probes, EfSearch: efSearch,
		Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits:      public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: mergeEntries},
		Deadline:    time.Now().Add(30 * time.Second),
	}
	for _, node := range []raftcluster.NodeID{"owner-b", "owner-c", "source-holder"} {
		peer, err := DialContext(ctx, "tcp", configs[0].Vector.PublicAddresses[node])
		if err != nil {
			t.Fatalf("public listener for %s: %v", node, err)
		}
		_, searchErr := peer.VectorSearchStrictV1(ctx, request)
		closeErr := peer.Close()
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if !hasPublicVectorErrorCodeV1(searchErr, public.ErrorUnavailableV1) {
			t.Fatalf("nonrouter %s accepted public strict search: %v", node, searchErr)
		}
	}
	for i := 0; i < warmQueries; i++ {
		warm := request
		warm.Query = queries[i%len(queries)]
		warm.Deadline = time.Now().Add(30 * time.Second)
		if _, err := publicClient.VectorSearchStrictV1(ctx, warm); err != nil {
			t.Fatalf("bounded warm query %d: %v", i, err)
		}
	}
	var response public.SearchResponseV1
	publicResults := make([]public.SearchResponseV1, len(queries))
	truthHits := 0
	validatePublic := func(queryIndex int, actual public.SearchResponseV1) {
		if truth != nil {
			retainLiveLifecycleJSONV1(t, fmt.Sprintf("accepted-query-%03d", queryIndex), struct {
				Native VectorPartitionCoordinatorResponseV1
				Public public.SearchResponseV1
			}{localResults[queryIndex], actual})
		}
		expectedGroups, expectedRPCs := uint64(2), uint64(2)
		if probes != 2 {
			expectedGroups, expectedRPCs = localResults[queryIndex].Counters.SelectedGroups, localResults[queryIndex].Counters.RPCs
		}
		if actual.Generation != request.Generation || actual.Counters.SelectedDomains != uint64(probes) || actual.Counters.SelectedGroups != expectedGroups || actual.Counters.RPCs != expectedRPCs ||
			actual.Counters.SelectedPartitions != uint64(probes) || actual.Counters.HNSWServedPartitions != uint64(probes) || actual.Counters.ExactScanPartitions != 0 {
			t.Fatalf("public query %d did not traverse each selected domain exactly once: %+v", queryIndex, actual)
		}
		localResult := localResults[queryIndex]
		if len(actual.Neighbors) == 0 || len(actual.Neighbors) != len(localResult.Neighbors) {
			t.Fatalf("public query %d cardinality differs from same-generation local search: remote=%+v local=%+v", queryIndex, actual.Neighbors, localResult.Neighbors)
		}
		for rank, remote := range actual.Neighbors {
			local := localResult.Neighbors[rank]
			scoreMismatch := math.Abs(float64(remote.Score-local.Score)) > 1e-5
			if truth != nil {
				scoreMismatch = math.Float32bits(remote.Score) != math.Float32bits(local.Score)
			}
			if remote.ID != local.ID || scoreMismatch {
				t.Fatalf("same-generation local parity query %d rank %d: remote=%+v local=%+v", queryIndex, rank, remote, local)
			}
		}
		a, b := actual.Counters, localResult.Counters
		if b.RouterScoreCalls == 0 || b.RouterCandidates == 0 ||
			a.RouterScoreCalls != b.RouterScoreCalls || a.RouterCandidates != b.RouterCandidates || a.RouterEdges != b.RouterEdges {
			t.Fatalf("router counter parity q=%d: public=%+v native=%+v", queryIndex, a, b)
		}
		if truth != nil {
			if len(truth[queryIndex]) != topK {
				t.Fatal("retained truth cardinality drift")
			}
			expected := make(map[string]bool, topK)
			for _, n := range truth[queryIndex] {
				if expected[n.ID] {
					t.Fatal("duplicate retained truth ID")
				}
				expected[n.ID] = true
			}
			seen := make(map[string]bool, topK)
			for _, n := range actual.Neighbors {
				if seen[n.ID] {
					t.Fatal("duplicate public result ID")
				}
				seen[n.ID] = true
				if expected[n.ID] {
					truthHits++
				}
			}
			if a.SelectedPacks != b.SelectedPacks || a.Candidates != b.Candidates || a.Edges != b.Edges || a.Retries != b.Retries || a.Redirects != b.Redirects {
				t.Fatalf("algorithm counter parity q=%d: public=%+v native=%+v", queryIndex, a, b)
			}
		}
	}
	type daemonCost struct {
		NodeID                        raftcluster.NodeID
		PID                           int
		Before, After                 FixedPeerDiagnosticsV1
		BytesPerQuery, AllocsPerQuery float64
		PeerStreamWrittenBytes        *uint64
	}
	var daemonCosts []daemonCost
	var publicRequests []public.SearchRequestV1
	var tcpStarted time.Time
	if comparativeCost {
		publicRequests = make([]public.SearchRequestV1, len(queries))
		for i, query := range queries {
			publicRequests[i] = request
			publicRequests[i].Query = append([]float32(nil), query...)
		}
		daemonCosts = make([]daemonCost, len(configs))
		for i, process := range processes {
			boundary := fixedPeerCostObserveV1(t, ctx, process, "before", profilePath(fmt.Sprintf("node-%d-before", i)))
			if boundary.MemoryProfileRate != runtime.MemProfileRate {
				t.Fatal("child allocation profile sampling rate mismatch")
			}
			profileBoundaries = append(profileBoundaries, boundary)
			if configs[i].NodeID == "ingress" {
				frameBefore = boundary
			}
		}
		for i, config := range configs {
			before, err := client.DiagnosticsV1(ctx, config.NodeID)
			if err != nil {
				t.Fatalf("before cost diagnostics %s: %v", config.NodeID, err)
			}
			daemonCosts[i] = daemonCost{NodeID: config.NodeID, PID: processes[i].command.Process.Pid, Before: before}
		}
		pids := make([]int, len(processes))
		for i, process := range processes {
			pids[i] = process.command.Process.Pid
		}
		rssObserver = fixedPeerStartCostRSSV1(ctx, pids)
		defer func() {
			if rssObserver != nil {
				rssObserver.stop()
			}
		}()
		tcpClientCost.Before = sampleParent(true)
		tcpStarted = time.Now()
	}
	for queryIndex, query := range queries {
		queryRequest := request
		if comparativeCost {
			queryRequest = publicRequests[queryIndex]
		} else {
			queryRequest.Query = append([]float32(nil), query...)
		}
		queryRequest.Deadline = time.Now().Add(30 * time.Second)
		actual, err := publicClient.VectorSearchStrictV1(ctx, queryRequest)
		if err != nil {
			t.Fatalf("public query %d: %v", queryIndex, err)
		}
		if !comparativeCost {
			validatePublic(queryIndex, actual)
		}
		publicResults[queryIndex] = actual
		if queryIndex == 0 {
			response = actual
		}
	}
	if comparativeCost {
		finishCost(&tcpClientCost, tcpStarted)
		servingRSS = rssObserver.stop()
		rssObserver = nil
		if servingRSS.Unavailable != "" || servingRSS.Samples == 0 {
			t.Fatalf("serving RSS observation unavailable: %+v", servingRSS)
		}
		for i := range daemonCosts {
			d := &daemonCosts[i]
			after, err := client.DiagnosticsV1(ctx, d.NodeID)
			if err != nil {
				t.Fatalf("after cost diagnostics %s: %v", d.NodeID, err)
			}
			d.After = after
			if after.Process.TotalAllocBytes < d.Before.Process.TotalAllocBytes || after.Process.Mallocs < d.Before.Process.Mallocs {
				t.Fatalf("daemon allocation counters decreased: %s", d.NodeID)
			}
			d.BytesPerQuery = float64(after.Process.TotalAllocBytes-d.Before.Process.TotalAllocBytes) / float64(len(queries))
			d.AllocsPerQuery = float64(after.Process.Mallocs-d.Before.Process.Mallocs) / float64(len(queries))
			beforeWritten := make(map[string]uint64, len(d.Before.Network))
			for _, n := range d.Before.Network {
				beforeWritten[n.RemoteIP] = n.WrittenBytes
			}
			// Absence of an enabled peer transport is unavailable, not zero.
			if len(d.Before.Network) > 0 && len(after.Network) > 0 {
				var written uint64
				for _, n := range after.Network {
					previous, exists := beforeWritten[n.RemoteIP]
					if !exists || n.WrittenBytes < previous {
						t.Fatalf("peer stream inventory/counter drift: %s/%s", d.NodeID, n.RemoteIP)
					}
					written += n.WrittenBytes - previous
					delete(beforeWritten, n.RemoteIP)
				}
				if len(beforeWritten) != 0 {
					t.Fatalf("peer stream inventory shrank: %s", d.NodeID)
				}
				d.PeerStreamWrittenBytes = &written
			}
		}
		for i, process := range processes {
			boundary := fixedPeerCostObserveV1(t, ctx, process, "after", profilePath(fmt.Sprintf("node-%d-after", i)))
			if boundary.MemoryProfileRate != runtime.MemProfileRate || boundary.SampleUnixNano <= profileBoundaries[i].SampleUnixNano {
				t.Fatal("child allocation profile boundary drift")
			}
			profileBoundaries = append(profileBoundaries, boundary)
			if configs[i].NodeID == "ingress" {
				frameAfter = boundary
			}
		}
		if frameAfter.SampleUnixNano <= frameBefore.SampleUnixNano ||
			frameAfter.RequestFrameBytes <= frameBefore.RequestFrameBytes || frameAfter.ResponseFrameBytes <= frameBefore.ResponseFrameBytes {
			t.Fatal("ingress shard frame counter boundary drift")
		}
		for i, result := range publicResults {
			validatePublic(i, result)
		}
	}
	t.Logf("public/native parity: queries=%d neighbors_per_query=%d probes=%d exact_scan_partitions=0", len(queries), len(response.Neighbors), probes)
	if truth != nil {
		recall := float64(truthHits) / float64(len(queries)*topK)
		retainLiveLifecycleJSONV1(t, "accepted-public-parity", struct {
			Manifest                                    collections.VectorPartitionManifestV1
			Native                                      []VectorPartitionCoordinatorResponseV1
			Public                                      []public.SearchResponseV1
			Recall                                      float64
			WarmQueries, Probes, EfSearch, MergeEntries int
		}{seed.manifest, localResults, publicResults, recall, warmQueries, probes, efSearch, mergeEntries})
		if recall < .95 {
			t.Fatalf("pinned512-query recall %.9f below .95; no retuning", recall)
		}
	}
	if comparativeCost {
		var daemonBytesPerQuery, daemonAllocsPerQuery float64
		var daemonRSSBefore, daemonRSSAfter, peerStreamWrittenBytes uint64
		allRSSAvailable, allStreamsAvailable := true, true
		unavailable := []string{"exact navigation allocations", "exact merge allocations", "wire bytes outside the declared ingress-to-owner plaintext shard frame boundary", "simultaneous all-daemon peak RSS", "query-attributed process allocations", "repeated-run latency statistics"}
		for _, d := range daemonCosts {
			daemonBytesPerQuery += d.BytesPerQuery
			daemonAllocsPerQuery += d.AllocsPerQuery
			daemonRSSBefore += d.Before.Process.RSSBytes
			daemonRSSAfter += d.After.Process.RSSBytes
			if d.Before.Process.RSSBytes == 0 || d.After.Process.RSSBytes == 0 {
				allRSSAvailable = false
			}
			if d.PeerStreamWrittenBytes == nil {
				allStreamsAvailable = false
			} else {
				peerStreamWrittenBytes += *d.PeerStreamWrittenBytes
			}
		}
		var rssBefore, rssAfter, streamWritten *uint64
		if allRSSAvailable {
			rssBefore, rssAfter = &daemonRSSBefore, &daemonRSSAfter
		} else {
			unavailable = append(unavailable, "all-daemon RSS snapshots: inspect each diagnostic Unavailable")
		}
		if allStreamsAvailable {
			streamWritten = &peerStreamWrittenBytes
		} else {
			unavailable = append(unavailable, "all-daemon peer transport stream writes: no enabled inventory on one or more nodes")
		}
		var localCandidateBytes, publicCandidateBytes uint64
		for i := range queries {
			localCandidateBytes += localResults[i].Counters.CandidateBytes
			publicCandidateBytes += publicResults[i].Counters.CandidateBytes
		}
		retainLiveLifecycleJSONV1(t, "accepted-comparative-cost", struct {
			Queries, WarmQueries, Probes, EfSearch, TopK, RouterScoreBudget, MergeEntries int
			Local, TCPClient                                                              costWindow
			Daemons                                                                       []daemonCost
			TCPAllProcessBytesPerQuery, TCPAllProcessAllocsPerQuery                       float64
			DaemonRSSBeforeBytes, DaemonRSSAfterBytes, PeerStreamWrittenBytes             *uint64
			LocalCandidateBytes, PublicCandidateBytes                                     uint64
			CounterReceipt, Scope                                                         string
			Unavailable                                                                   []string
			ShardFramesBefore, ShardFramesAfter                                           fixedPeerCostBoundaryV1
			ShardRequestFrameBytes, ShardResponseFrameBytes                               uint64
			ServingRSS                                                                    fixedPeerCostRSSV1
			AllocationProfiles                                                            []string
			DaemonProfileBoundaries                                                       []fixedPeerCostBoundaryV1
			MemoryProfileRate                                                             int
		}{len(queries), warmQueries, probes, efSearch, topK, 256, mergeEntries,
			localCost, tcpClientCost, daemonCosts,
			tcpClientCost.BytesPerQuery + daemonBytesPerQuery, tcpClientCost.AllocsPerQuery + daemonAllocsPerQuery,
			rssBefore, rssAfter, streamWritten, localCandidateBytes, publicCandidateBytes,
			"accepted-public-parity", "one ordered pass per arm; process-global allocations; daemon intervals are staggered and include diagnostics/background; RSS snapshots and process-lifetime peak RSS; peer transport stream writes include control/raft/diagnostics and exclude public nativewire sockets; candidate bytes and algorithm counters are semantic counters; instrumentation envelope includes atomic shard-frame counters and parent RSS observer allocations; per-node and aggregate RSS maxima are sampled lower bounds, with serial-round skew; sampled alloc-profile intervals include background and profile/control work, not exact stage attribution; framed plaintext bytes count ingress dispatcher writes and reads once, including length prefixes and retries, excluding warmup, HTTP/control/Raft, public-client socket, TLS/IP/TCP overhead",
			unavailable, frameBefore, frameAfter,
			frameAfter.RequestFrameBytes - frameBefore.RequestFrameBytes, frameAfter.ResponseFrameBytes - frameBefore.ResponseFrameBytes,
			servingRSS, profilePaths, profileBoundaries, runtime.MemProfileRate})
	}
	if readCost {
		fixedPeerImmutableOwnerReadCostV1(t, ctx, configs, publicClient, request, consumerOwner)
	}
	if faultDir := os.Getenv("GOMAP_FIXED_PEER_ACTIVE_INVALIDATION_CONTROL"); !consumerOwner && faultDir != "" {
		fixedPeerAssertInFlightActiveInvalidationV1(t, ctx, faultDir, client, publicClient, request)
		return
	}
	if (separateLeaders || consumerOwner) && !sourceFollower {
		// The valid public request above used both owner shards. A node with a
		// valid cluster certificate must still be unable to send group-c work
		// directly to owner-b's authenticated shard endpoint.
		transport, err := NewPeerTransportV1(configs[0])
		if err != nil {
			t.Fatal(err)
		}
		defer transport.Close()
		endpoint := configs[0].Vector.ShardAddresses["group-b"]["owner-b"]
		identity, err := transport.ProbeShardEndpointV1(ctx, endpoint, "owner-b", "group-b")
		if err != nil || identity.GroupID != "group-b" {
			t.Fatalf("owner-b authenticated shard probe: identity=%+v err=%v", identity, err)
		}
		conn, err := transport.dialScope(ctx, endpoint, "owner-b", "shard:group-b")
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		wrongOwner := vectorPartitionShardSearchRequestTestV1([]uint32{2})
		wrongOwner.TargetGroupID, wrongOwner.TargetNodeID = "group-c", "owner-c"
		wrongOwner.Database, wrongOwner.Catalog, wrongOwner.Collection = configs[0].Vector.Collection.Database, configs[0].Vector.Collection.Catalog, configs[0].Vector.Collection.Collection
		wrongOwner.IndexName, wrongOwner.IndexDefinitionDigest = seed.manifest.IndexName, seed.manifest.IndexDefinitionDigest
		wrongOwner.SourceGeneration, wrongOwner.SourceChecksum = seed.manifest.SourceGeneration, seed.manifest.SourceChecksum
		wrongOwner.SourceSchemaHash, wrongOwner.SourceRowCount = seed.manifest.SourceSchemaHash, seed.manifest.SourceRowCount
		wrongOwner.PartitionGeneration, wrongOwner.RouterGeneration = seed.manifest.Generation, seed.manifest.Generation
		wrongOwner.DeadlineUnixNano = time.Now().Add(5 * time.Second).UnixNano()
		if err := writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{Request: &wrongOwner}, vectorPartitionShardSearchTCPMaxFrameBytesV1); err != nil {
			_ = conn.Close()
			t.Fatalf("write authenticated wrong-owner frame: %v", err)
		}
		_, readErr := readVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPMaxFrameBytesV1)
		_ = conn.Close()
		if readErr == nil {
			t.Fatal("owner-b accepted authenticated group-c shard request")
		}
		var netErr net.Error
		if errors.As(readErr, &netErr) && netErr.Timeout() {
			t.Fatalf("owner-b did not promptly refuse wrong-owner shard request: %v", readErr)
		}
		request.Deadline = time.Now().Add(30 * time.Second)
		afterFault, err := publicClient.VectorSearchStrictV1(ctx, request)
		if err != nil || len(afterFault.Neighbors) != len(response.Neighbors) {
			t.Fatalf("strict public search after wrong-owner refusal: response=%+v err=%v", afterFault, err)
		}
		for i := range response.Neighbors {
			if afterFault.Neighbors[i] != response.Neighbors[i] {
				t.Fatalf("wrong-owner fault changed strict result at rank %d: before=%+v after=%+v", i, response.Neighbors[i], afterFault.Neighbors[i])
			}
		}
	}
	if consumerOwner && os.Getenv("GOMAP_FIXED_PEER_ACTIVE_INVALIDATION_CONTROL") != "" {
		fixedPeerCatalogConsumerCachedInvalidationV1(t, ctx, configs, client, publicClient, request)
		return
	}
	// Losing one selected owner must fail the whole public request; no partial
	// hits from the surviving owner may escape. Reopen that owner on its original
	// hosted-only assets and require the same generation/result again.
	lostOwner, lostProcess := raftcluster.NodeID("owner-b"), 1
	if probes != 2 && len(localResults[0].ProbedGroups) > 0 && localResults[0].ProbedGroups[0] == "group-c" {
		lostOwner, lostProcess = "owner-c", 2
	}
	processes[lostProcess].stop(t)
	request.Deadline = time.Now().Add(12 * time.Second)
	partial, searchErr := publicClient.VectorSearchStrictV1(ctx, request)
	if searchErr == nil || len(partial.Neighbors) != 0 {
		t.Fatalf("missing selected owner returned partial result: response=%+v err=%v", partial, searchErr)
	}
	missingAsset, missingAssetRel := "", ""
	if missingOwnerAsset {
		for _, asset := range seed.manifest.Assets {
			if placements[asset.PartitionID] != "group-b" {
				continue
			}
			rel := filepath.Join(filepath.FromSlash(asset.Ref.Namespace), "assets", "segments", fmt.Sprintf("segment-%06d.tca", asset.Ref.FileID))
			if hostedFiles["group-b"][rel] {
				missingAsset = filepath.Join(backenddb.ColumnAssetRootDirPath(filepath.Join(configs[1].DataRoot, "group-b")), rel)
				missingAssetRel = rel
				break
			}
		}
		if missingAsset == "" {
			t.Fatal("group-b has no declared hosted graph segment to remove")
		}
		if err := os.Remove(missingAsset); err != nil {
			t.Fatalf("remove declared group-b graph segment: %v", err)
		}
		if _, err := os.Stat(missingAsset); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("removed group-b graph segment still exists: %v", err)
		}
	}
	processes[lostProcess] = fixedPeerStartTestProcessV1(t, configs[lostProcess])
	fixedPeerWaitV1(t, ctx, func() bool {
		status, err := client.Status(ctx, lostOwner)
		if consumerOwner {
			return err == nil && status.CatalogRole == "consumer" && status.Catalog.AppliedIndex == 0 && len(status.Groups) == 1 && status.Groups[0].LeaderID != ""
		}
		return err == nil && status.CatalogRaft.LeaderID != "" && len(status.Groups) == 1 && status.Groups[0].LeaderID != "" && status.Catalog.AppliedIndex != 0
	})
	if consumerOwner {
		fixedPeerAssertColdCatalogConsumerOwnerV1(t, ctx, configs, client)
	}
	if missingOwnerAsset {
		_, recoveryErr := client.EnsureImmutableVectorLifecycleV1(ctx)
		var remoteErr *fixedPeerRemoteErrorV1
		missingFile := strings.ToLower(fmt.Sprint(recoveryErr))
		if !errors.As(recoveryErr, &remoteErr) || remoteErr.code != "rejected" ||
			(!strings.Contains(missingFile, "no such file") &&
				!strings.Contains(missingFile, "cannot find the file")) {
			t.Fatalf("ACTIVE recovery did not reject the missing declared group-b graph segment: %v", recoveryErr)
		}
		if _, err := os.Stat(missingAsset); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("missing group-b graph segment was replaced during recovery: %v", err)
		}
		remainingHosted := maps.Clone(hostedFiles["group-b"])
		delete(remainingHosted, missingAssetRel)
		fixedPeerAssertHostedVectorFilesV1(t, filepath.Join(configs[1].DataRoot, "group-b"), remainingHosted, false)
		if report, err := client.ReadinessV1(ctx, "owner-c"); err != nil || !report.Ready {
			t.Fatalf("unaffected owner-c lost readiness: %+v err=%v", report, err)
		}
		fixedPeerAssertHostedVectorFilesV1(t, filepath.Join(configs[2].DataRoot, "group-c"), hostedFiles["group-c"], false)
		request.Deadline = time.Now().Add(12 * time.Second)
		partial, searchErr := publicClient.VectorSearchStrictV1(ctx, request)
		if !hasPublicVectorErrorCodeV1(searchErr, public.ErrorUnavailableV1) || len(partial.Neighbors) != 0 {
			t.Fatalf("missing declared owner asset returned partial result: response=%+v err=%v", partial, searchErr)
		}
		if report, err := client.ReadinessV1(ctx, "owner-c"); err != nil || !report.Ready {
			t.Fatalf("unaffected owner-c lost readiness after failed strict search: %+v err=%v", report, err)
		}
		return
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		report, err := client.ReadinessV1(ctx, lostOwner)
		return err != nil && !report.Ready && len(report.Groups) == 1 && report.Groups[0].Ready &&
			strings.Contains(report.Error, "immutable vector listener")
	})
	if _, err := client.EnsureImmutableVectorLifecycleV1(ctx); err != nil {
		t.Fatalf("reopened owner lifecycle: %v", err)
	}
	if report, err := client.ReadinessV1(ctx, lostOwner); err != nil || !report.Ready {
		t.Fatalf("rewarmed owner readiness: %+v err=%v", report, err)
	}
	request.Deadline = time.Now().Add(12 * time.Second)
	reopened, err := publicClient.VectorSearchStrictV1(ctx, request)
	if err != nil {
		t.Fatalf("reopened owner search: %v", err)
	}
	if reopened.Generation != request.Generation || len(reopened.Neighbors) != len(response.Neighbors) || reopened.Counters.SelectedDomains != uint64(probes) || reopened.Counters.RPCs != response.Counters.RPCs ||
		reopened.Counters.SelectedPartitions != uint64(probes) || reopened.Counters.HNSWServedPartitions != uint64(probes) || reopened.Counters.ExactScanPartitions != 0 {
		t.Fatalf("reopened owner lost a domain or result: before=%+v after=%+v", response, reopened)
	}
	for i, neighbor := range reopened.Neighbors {
		scoreMismatch := math.Abs(float64(neighbor.Score-response.Neighbors[i].Score)) > 1e-5
		if truth != nil {
			scoreMismatch = math.Float32bits(neighbor.Score) != math.Float32bits(response.Neighbors[i].Score)
		}
		if neighbor.ID != response.Neighbors[i].ID || scoreMismatch {
			t.Fatalf("reopened owner result at rank %d: before=%+v after=%+v", i, response.Neighbors[i], neighbor)
		}
	}
	if separateLeaders && !sourceFollower {
		// ACTIVE recovery needs only the committed owner assets. The source
		// group has no leader after its sole member exits.
		processes[3].stop(t)
		if _, err := client.Status(ctx, "source-holder"); err == nil {
			t.Fatal("source holder remained available after shutdown")
		}
		if _, err := client.EnsureImmutableVectorLifecycleV1(ctx); err != nil {
			t.Fatalf("ACTIVE recovery with source group unavailable: %v", err)
		}
		request.Deadline = time.Now().Add(12 * time.Second)
		recovered, err := publicClient.VectorSearchStrictV1(ctx, request)
		if err != nil || len(recovered.Neighbors) != len(response.Neighbors) {
			t.Fatalf("ACTIVE source-outage search: response=%+v err=%v", recovered, err)
		}
		for i, neighbor := range recovered.Neighbors {
			if neighbor.ID != response.Neighbors[i].ID || math.Abs(float64(neighbor.Score-response.Neighbors[i].Score)) > 1e-5 {
				t.Fatalf("ACTIVE source-outage result at rank %d: before=%+v after=%+v", i, response.Neighbors[i], neighbor)
			}
		}
	}
	if consumerOwner {
		fixedPeerCatalogConsumerOwnerControlsV1(t, ctx, configs, processes, client, publicClient, request)
	}
	if err := publicClient.Close(); err != nil && !consumerOwner {
		t.Fatal(err)
	}
	client.Close()
	for _, process := range processes {
		process.stop(t)
	}
	for _, config := range configs {
		if config.NodeID == "source-holder" {
			fixedPeerAssertSourceDocumentCountV1(t, filepath.Join(config.DataRoot, "group-d"), seed.manifest.Collection, int(seed.manifest.SourceRowCount))
			continue
		}
		if config.NodeID == "source-follower" {
			continue // a source-group follower may acquire the replicated source
		}
		group := raftcluster.GroupID("group-a")
		switch config.NodeID {
		case "owner-b":
			group = "group-b"
		case "owner-c":
			group = "group-c"
		}
		fixedPeerAssertHostedVectorFilesV1(t, filepath.Join(config.DataRoot, string(group)), hostedFiles[group], false)
		fixedPeerAssertSourceDocumentCountV1(t, filepath.Join(config.DataRoot, string(group)), seed.manifest.Collection, 0)
	}
}

func TestMultiOwnerTCPDomainSearchRestagesElectedOwnerV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0},
		{id: "b", vector: []float32{.8, .2}, home: 1},
		{id: "c", vector: []float32{0, 1}, home: 2},
		{id: "d", vector: []float32{.2, .8}, home: 2},
	}, nil, [2]string{"group-b", "group-c"}, true, true)
	if err := seed.collection.EnsureVectorPartitionLiveBindingV1(ctx, seed.manifest); err != nil {
		t.Fatal(err)
	}
	appliedCommandLSN := seed.database.State().AppliedCommandLSN
	if appliedCommandLSN == 0 {
		t.Fatal("trusted source has no command-WAL coverage")
	}
	meta := seed.collection.MetaView()
	if meta.Options.ColumnStore == nil {
		t.Fatal("source has no column-store definition")
	}
	columnStore := *meta.Options.ColumnStore
	columnStore.ActiveManifest = nil
	columnStore.RecoveryAuthoritativeManifest = nil
	columnStore.RecoveryAuthoritativeAppliedCommandLSN = 0
	meta.Options.ColumnStore = &columnStore
	configs := fixedPeerMultiOwnerSearchConfigsWithOwnerBReplicasV1(t, seed.manifest, meta, 3)
	if err := seed.database.Close(); err != nil {
		t.Fatal(err)
	}
	graphSegments := map[raftcluster.GroupID]map[uint32]bool{"group-b": {}, "group-c": {}}
	placement := make(map[uint32]raftcluster.GroupID)
	for _, item := range seed.manifest.Placements {
		placement[item.PartitionID] = raftcluster.GroupID(item.GroupID)
	}
	for _, asset := range seed.manifest.Assets {
		group := placement[asset.PartitionID]
		if graphSegments[group] == nil {
			t.Fatalf("asset %q has no owner", asset.ID)
		}
		graphSegments[group][asset.Ref.FileID] = true
	}
	for _, config := range configs {
		if config.NodeID == "source-holder" {
			root := filepath.Join(config.DataRoot, "group-d")
			if err := os.CopyFS(root, os.DirFS(seed.dir)); err != nil {
				t.Fatal(err)
			}
			if err := backenddb.RebindDurableRootSnapshotV1(root); err != nil {
				t.Fatal(err)
			}
			bootstrapFixedPeerVectorTrustedGenesisV1(t, config, appliedCommandLSN)
			continue
		}
		group := raftcluster.GroupID("group-a")
		if strings.HasPrefix(string(config.NodeID), "owner-b") {
			group = "group-b"
		} else if config.NodeID == "owner-c" {
			group = "group-c"
		}
		root := filepath.Join(config.DataRoot, string(group))
		hosted := fixedPeerCopyHostedVectorAssetsV1(t, seed.dir, root, seed.manifest, graphSegments[group], group == "group-a")
		fixedPeerBootstrapHostedVectorMetadataV1(t, config, meta, root)
		fixedPeerAssertHostedVectorFilesV1(t, root, hosted, false)
		fixedPeerAssertSourceDocumentCountV1(t, root, seed.manifest.Collection, 0)
	}
	processes := make([]*fixedPeerTestProcessV1, len(configs))
	// Start the configured meta bootstrap first. Bringing a different voter
	// up before it can let that voter win as soon as the fourth node supplies
	// the 4-of-6 quorum, which would not exercise this V1 source-holder path.
	processes[3] = fixedPeerStartTestProcessV1(t, configs[3])
	defer func() {
		for _, process := range processes {
			if process != nil {
				process.stop(t)
			}
		}
	}()
	client, err := NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// Ensure the source-holder's Raft bootstrap and listener are actually
	// ready before other voters begin their election timers.
	fixedPeerWaitV1(t, ctx, func() bool {
		_, statusErr := client.Status(ctx, "source-holder")
		return statusErr == nil
	})
	for _, i := range []int{0, 1, 2} {
		processes[i] = fixedPeerStartTestProcessV1(t, configs[i])
	}
	// Four meta voters form a quorum before the two extra owner voters start.
	// The source holder must lead meta to run the trusted V1 BUILD callback.
	var observedMetaLeader raftcluster.NodeID
	for observedMetaLeader != "source-holder" {
		status, statusErr := client.Status(ctx, "source-holder")
		if statusErr == nil {
			observedMetaLeader = status.CatalogRaft.LeaderID
		}
		if observedMetaLeader == "source-holder" {
			break
		}
		select {
		case <-ctx.Done():
			for _, i := range []int{3, 0, 1, 2} {
				if process := processes[i]; process != nil {
					log, readErr := os.ReadFile(process.log.Name())
					if len(log) > 4096 {
						log = log[len(log)-4096:]
					}
					t.Logf("%s startup log tail (read error=%v): %s", configs[i].NodeID, readErr, log)
				}
			}
			t.Fatalf("source-holder meta leadership: observed=%q last status error=%v: %v", observedMetaLeader, statusErr, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	processes[4] = fixedPeerStartTestProcessV1(t, configs[4])
	// Two group-b voters form a quorum while the slower third voter is down.
	// This makes the configured old hint the first owner leader.
	fixedPeerWaitV1(t, ctx, func() bool {
		status, err := client.Status(ctx, "owner-b")
		return err == nil && len(status.Groups) == 1 && status.Groups[0].LeaderID == "owner-b"
	})
	processes[5] = fixedPeerStartTestProcessV1(t, configs[5])
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, config := range configs {
			status, err := client.Status(ctx, config.NodeID)
			if err != nil || status.CatalogRaft.LeaderID == "" || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" {
				return false
			}
		}
		return true
	})
	record, err := raftplacement.NewCatalogMetaRecordV1(1, configs[0].Vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, "source-holder", command); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, config := range configs {
			status, err := client.Status(ctx, config.NodeID)
			if err != nil || status.Catalog.AppliedIndex == 0 || status.Catalog.Digest != record.Digest {
				return false
			}
		}
		return true
	})
	if _, err := client.EnsureImmutableVectorLifecycleV1(ctx); err != nil {
		t.Fatalf("initial ACTIVE: %v", err)
	}
	// Owner followers do not open shard listeners during warm, but are ready
	// once their local Raft state and the immutable ACTIVE record are current.
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range []raftcluster.NodeID{"owner-b-2", "owner-b-3"} {
			report, err := client.ReadinessV1(ctx, node)
			if err != nil || !report.Ready || len(report.Groups) != 1 ||
				report.Groups[0].LeaderID != "owner-b" {
				return false
			}
		}
		return true
	})
	publicClient, err := DialContext(ctx, "tcp", configs[0].Vector.PublicAddresses["ingress"])
	if err != nil {
		t.Fatal(err)
	}
	defer publicClient.Close()
	request := public.SearchRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: seed.manifest.IndexName, Generation: seed.manifest.Generation},
		Query: []float32{.7, .7}, Metric: public.MetricCosineV1, TopK: 4, Probes: 2, EfSearch: 8,
		Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits:      public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 16},
		Deadline:    time.Now().Add(12 * time.Second),
	}
	before, err := publicClient.VectorSearchStrictV1(ctx, request)
	if err != nil || len(before.Neighbors) == 0 {
		t.Fatalf("old owner search=%+v err=%v", before, err)
	}
	processes[1].stop(t)
	processes[1] = nil
	var elected raftcluster.NodeID
	fixedPeerWaitV1(t, ctx, func() bool {
		status, err := client.Status(ctx, "owner-b-2")
		if err != nil || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" || status.Groups[0].LeaderID == "owner-b" {
			return false
		}
		elected = status.Groups[0].LeaderID
		return true
	})
	request.Deadline = time.Now().Add(12 * time.Second)
	partial, err := publicClient.VectorSearchStrictV1(ctx, request)
	if err == nil || len(partial.Neighbors) != 0 {
		t.Fatalf("unstaged elected owner %s returned partial hits: response=%+v err=%v", elected, partial, err)
	}
	if report, err := client.ReadinessV1(ctx, elected); err == nil || report.Ready {
		t.Fatalf("elected owner %s reported ready before listener warm: %+v err=%v", elected, report, err)
	}
	if _, err := client.EnsureImmutableVectorLifecycleV1(ctx); err != nil {
		t.Fatalf("ACTIVE recovery on elected owner %s: %v", elected, err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		report, err := client.ReadinessV1(ctx, elected)
		return err == nil && report.Ready && len(report.Groups) == 1 && report.Groups[0].LeaderID == elected
	})
	request.Deadline = time.Now().Add(12 * time.Second)
	after, err := publicClient.VectorSearchStrictV1(ctx, request)
	if err != nil || len(after.Neighbors) != len(before.Neighbors) || after.Counters.SelectedDomains != 2 ||
		after.Counters.Requests != 2 || after.Counters.RPCs != before.Counters.RPCs+1 ||
		after.Counters.Retries != 1 || after.Counters.Redirects != 1 {
		t.Fatalf("elected owner search before=%+v after=%+v err=%v", before, after, err)
	}
	for i := range before.Neighbors {
		if before.Neighbors[i].ID != after.Neighbors[i].ID || math.Abs(float64(before.Neighbors[i].Score-after.Neighbors[i].Score)) > 1e-5 {
			t.Fatalf("elected owner result at rank %d: before=%+v after=%+v", i, before.Neighbors[i], after.Neighbors[i])
		}
	}
}

func TestFixedPeerImmutableDefinitionAndMutationRefusalV1(t *testing.T) {
	definition := collections.VectorIndexDefinition{Name: "embedding", SchemaGeneration: 17}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection:            raftplacement.CollectionRefV1{Collection: "documents"},
			CollectionIncarnation: 1,
			IndexName:             "embedding",
			IndexEpoch:            17,
			IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(definition),
		},
		Immutable: raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{ManifestDigest: strings.Repeat("a", 64), PlacementDigest: strings.Repeat("b", 64)},
	}
	meta := collections.CollectionMeta{Name: "documents", VectorIndexes: []collections.VectorIndexDefinition{definition}}
	if err := fixedPeerVectorImmutableDefinitionV1(meta, identity); err != nil {
		t.Fatalf("current durable definition: %v", err)
	}
	for _, changed := range []struct {
		name string
		edit func(*raftplacement.VectorPartitionLifecycleIdentityV1)
	}{
		{name: "recreated-index-epoch", edit: func(i *raftplacement.VectorPartitionLifecycleIdentityV1) { i.Index.IndexEpoch++ }},
		{name: "unsupported-collection-incarnation", edit: func(i *raftplacement.VectorPartitionLifecycleIdentityV1) { i.Index.CollectionIncarnation++ }},
	} {
		t.Run(changed.name, func(t *testing.T) {
			stale := identity
			changed.edit(&stale)
			if !errors.Is(fixedPeerVectorImmutableDefinitionV1(meta, stale), ErrFixedPeerVectorProofStaleV1) {
				t.Fatal("stale immutable definition was admitted")
			}
		})
	}
	b := &fixedPeerVectorBackendV1{runtime: &fixedPeerVectorRuntimeV1{parent: &FixedPeerTCPRuntimeV1{config: FixedPeerTCPConfigV1{Vector: &FixedPeerTCPVectorConfigV1{Identity: identity}}}}}
	if _, err := b.InsertVectorPartitionV1(t.Context(), public.InsertRequestV1{}); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
		t.Fatalf("immutable insert should refuse before backend or wire: %v", err)
	}
	if _, err := b.PrepareVectorPartitionV1(t.Context(), public.GenerationIDV1{}); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
		t.Fatalf("immutable lifecycle mutation should refuse before backend: %v", err)
	}
}

// This mirrors the existing fixed-peer trusted-genesis fixture, but creates
// only collection metadata. Every serving asset is copied separately above
// and remains subject to the hosted-file inventory assertions.
func fixedPeerBootstrapHostedVectorMetadataV1(t testing.TB, config FixedPeerTCPConfigV1, meta collections.CollectionMeta, root string) {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{Dir: root, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(database)
	_, createErr := manager.CreateCollection(&meta)
	coverage := database.State().AppliedCommandLSN
	if err := errors.Join(createErr, database.Close()); err != nil {
		t.Fatal(err)
	}
	if coverage == 0 {
		t.Fatal("metadata-only trusted genesis has no command-WAL coverage")
	}
	bootstrapFixedPeerVectorTrustedGenesisV1(t, config, coverage)
}

// The source-holder retains the full builder rows. Every serving process must
// retain zero rows before startup and after search/reopen, so hosted graph
// assets cannot be mistaken for a copied full serving source.
func fixedPeerAssertSourceDocumentCountV1(t testing.TB, root, collectionName string, want int) {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{Dir: root, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	manager := collections.NewCollectionManager(database)
	all, err := manager.ListCollections()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].Name != collectionName {
		t.Fatalf("process %q has %d collections, want only metadata for %q", root, len(all), collectionName)
	}
	for _, meta := range all {
		if meta.Name != collectionName {
			continue
		}
		collection, err := manager.OpenCollection(collectionName)
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		if _, err := collection.ScanDocumentIDsFunc(want+1, func([]byte) (bool, error) {
			seen++
			return seen < want+1, nil
		}); err != nil {
			t.Fatal(err)
		}
		if seen != want {
			t.Fatalf("process %q has %d source documents, want %d", root, seen, want)
		}
	}
}

func fixedPeerMultiOwnerSearchConfigsV1(t testing.TB, manifest collections.VectorPartitionManifestV1, sourceMeta collections.CollectionMeta) []FixedPeerTCPConfigV1 {
	return fixedPeerMultiOwnerSearchConfigsWithOwnerBReplicasV1(t, manifest, sourceMeta, 1)
}

func TestFixedPeerImmutableVectorConfigRejectsSourceOwnerGroupV1(t *testing.T) {
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0},
		{id: "b", vector: []float32{.8, .2}, home: 1},
		{id: "c", vector: []float32{0, 1}, home: 2},
		{id: "d", vector: []float32{.2, .8}, home: 2},
	}, nil, [2]string{"group-b", "group-c"}, true, true)
	configs := fixedPeerMultiOwnerSearchConfigsV1(t, seed.manifest, seed.collection.MetaView())
	if err := seed.database.Close(); err != nil {
		t.Fatal(err)
	}
	config := configs[1] // owner-b becomes both source-group member and ANN owner
	localGroups := map[raftcluster.GroupID]bool{"meta": true, "group-b": true}
	if err := validateFixedPeerVectorConfigV1(config, localGroups); err != nil {
		t.Fatalf("valid separate source and owners rejected: %v", err)
	}
	vector := *config.Vector
	vector.Catalog.Placements = append([]raftplacement.CollectionPlacementV1(nil), vector.Catalog.Placements...)
	vector.Catalog.Placements[0].GroupID = "group-b"
	record, err := raftplacement.NewCatalogMetaRecordV1(vector.Identity.Index.CatalogEpoch, vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	vector.Identity.Index.CatalogDigest = record.Digest
	config.Vector = &vector
	if err := validateFixedPeerVectorConfigV1(config, localGroups); err == nil || err.Error() != "immutable vector source group must be separate from owner groups" {
		t.Fatalf("source group that also owns domains accepted: %v", err)
	}
}

func fixedPeerMultiOwnerSearchConfigsWithOwnerBReplicasV1(t testing.TB, manifest collections.VectorPartitionManifestV1, sourceMeta collections.CollectionMeta, ownerBReplicas int) []FixedPeerTCPConfigV1 {
	t.Helper()
	if ownerBReplicas != 1 && ownerBReplicas != 3 {
		t.Fatal("owner-b fixture requires one or three voters")
	}
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	address := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		return listener.Addr().String()
	}
	ids := []raftcluster.NodeID{"ingress", "owner-b", "owner-c", "source-holder"}
	for replica := 2; replica <= ownerBReplicas; replica++ {
		ids = append(ids, raftcluster.NodeID(fmt.Sprintf("owner-b-%d", replica)))
	}
	features := raftcluster.DefaultFeatureSet()
	features.Required = append(features.Required,
		raftcluster.RequiredFeature{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]},
		raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]},
	)
	meta := FixedPeerTCPGroupV1{ID: "meta", BootstrapNode: "source-holder", Features: features}
	groups := []FixedPeerTCPGroupV1{
		{ID: "group-a", BootstrapNode: "ingress"},
		{ID: "group-b", BootstrapNode: "owner-b"},
		{ID: "group-c", BootstrapNode: "owner-c"},
		{ID: "group-d", BootstrapNode: "source-holder"},
	}
	nodes := make([]FixedPeerTCPNodeV1, 0, len(ids))
	publicAddresses := make(map[raftcluster.NodeID]string, len(ids))
	shardAddresses := map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-b": {}, "group-c": {}}
	ownerBMembers := make([]raftcluster.NodeID, 0, ownerBReplicas)
	groupForNode := make(map[raftcluster.NodeID]raftcluster.GroupID, len(ids))
	groupRaftAddress := make(map[raftcluster.NodeID]string, len(ids))
	for _, id := range ids {
		group := raftcluster.GroupID("group-a")
		switch {
		case strings.HasPrefix(string(id), "owner-b"):
			group = "group-b"
			ownerBMembers = append(ownerBMembers, id)
		case id == "owner-c":
			group = "group-c"
		case id == "source-holder":
			group = "group-d"
		}
		groupForNode[id] = group
		nodes = append(nodes, FixedPeerTCPNodeV1{ID: id, Address: address()})
		meta.Peers = append(meta.Peers, raftcluster.Peer{ID: id, Address: address(), Capabilities: features})
		for i := range groups {
			if groups[i].ID != group {
				continue
			}
			groupRaftAddress[id] = address()
			groups[i].Peers = append(groups[i].Peers, raftcluster.Peer{ID: id, Address: groupRaftAddress[id]})
			break
		}
		if group == "group-b" || group == "group-c" {
			shardAddresses[group][id] = address()
		}
		publicAddresses[id] = address()
	}
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: manifest.Collection}
	catalogFeatures := raftplacement.DefaultFeatureSet()
	catalogFeatures.Required = append(catalogFeatures.Required, raftcluster.RequiredFeature{
		Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle],
	})
	catalog := raftplacement.CatalogV1{
		Features: catalogFeatures,
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"ingress"}, LeaderHint: "ingress"},
			{ID: "group-b", Members: ownerBMembers, LeaderHint: "owner-b"},
			{ID: "group-c", Members: []raftcluster.NodeID{"owner-c"}, LeaderHint: "owner-c"},
			{ID: "group-d", Members: []raftcluster.NodeID{"source-holder"}, LeaderHint: "source-holder"},
		},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-d", Mode: raftplacement.PlacementModeCollectionV1}},
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection: ref, IndexName: manifest.IndexName, IndexDefinitionDigest: manifest.IndexDefinitionDigest,
		SourceGeneration: manifest.SourceGeneration, SourceChecksum: manifest.SourceChecksum,
		SourceSchemaHash: manifest.SourceSchemaHash, SourceRowCount: manifest.SourceRowCount,
		PartitionGeneration: manifest.Generation, PartitionCount: manifest.PartitionCount,
		Partitions: make([]raftplacement.VectorPartitionGroupV1, 0, len(manifest.Placements)),
	}
	for _, p := range manifest.Placements {
		if p.GroupID != "group-b" && p.GroupID != "group-c" {
			t.Fatalf("unsupported public fixture owner %q", p.GroupID)
		}
		placement.Partitions = append(placement.Partitions, raftplacement.VectorPartitionGroupV1{PartitionID: p.PartitionID, GroupID: raftcluster.GroupID(p.GroupID)})
	}
	if len(placement.Partitions) != int(manifest.PartitionCount) {
		t.Fatal("incomplete public fixture placement")
	}
	var indexEpoch uint64
	for _, definition := range sourceMeta.VectorIndexes {
		if definition.Name == manifest.IndexName && collections.VectorIndexDefinitionDigestV1(definition) == manifest.IndexDefinitionDigest {
			indexEpoch = definition.SchemaGeneration
			break
		}
	}
	if indexEpoch == 0 {
		t.Fatal("source definition does not attest the immutable index epoch")
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection: ref, CollectionIncarnation: 1, IndexName: manifest.IndexName, IndexDefinitionDigest: manifest.IndexDefinitionDigest,
			IndexEpoch: indexEpoch, CatalogEpoch: record.Epoch, CatalogDigest: record.Digest,
		},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{
			Generation: manifest.SourceGeneration, Checksum: manifest.SourceChecksum,
			SchemaHash: manifest.SourceSchemaHash, RowCount: manifest.SourceRowCount,
		},
		Generation: manifest.Generation,
	}
	manifestBytes, err := collections.EncodeVectorPartitionManifestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	placementDigest, err := collections.VectorPartitionPlacementDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	identity.Immutable = raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{
		ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(manifestBytes)), PlacementDigest: placementDigest,
	}
	requestBase := VectorPartitionCoordinatorRequestV1{
		Version: VectorPartitionCoordinatorVersionV1, RequestID: "multi-owner-search", CancellationID: "multi-owner-search-cancel",
		Database: ref.Database, Catalog: ref.Catalog, Collection: ref.Collection, IndexName: manifest.IndexName,
		IndexDefinitionDigest: manifest.IndexDefinitionDigest, Metric: VectorPartitionShardSearchMetricCosineV1,
		RouterMode: collections.VectorPartitionRouterModeExactV1, RouterScoreBudget: max(1, len(manifest.Representatives)),
		PartitionProbes: 2, Consistency: VectorPartitionShardSearchConsistencySnapshotV1,
		StatsMode: VectorPartitionShardSearchStatsBasicV1, TopK: 4, EfSearch: 8,
		RequestBytesLimit: 1 << 20, CandidateBytesLimit: 8 << 20, ResponseBytesLimit: 1 << 20, MergeEntriesLimit: 16,
	}
	vector := &FixedPeerTCPVectorConfigV1{
		Collection: ref, Catalog: catalog, Manifest: manifest, Placement: placement, Identity: identity,
		RouterNodeID:    "ingress",
		PublicAddresses: publicAddresses, ShardAddresses: shardAddresses, RequestBase: requestBase, IndexedThrough: 1,
	}
	root := t.TempDir()
	configs := make([]FixedPeerTCPConfigV1, len(ids))
	for i, id := range ids {
		configs[i] = FixedPeerTCPConfigV1{
			NodeID: id, DataRoot: filepath.Join(root, string(id), "data"), RaftRoot: filepath.Join(root, string(id), "raft"),
			ListenAddress: nodes[i].Address, Nodes: nodes, Catalog: meta, Groups: groups,
			RequestTimeout: 12 * time.Second, RaftTimeout: 300 * time.Millisecond, Vector: vector,
			RaftListen: map[raftcluster.GroupID]string{"meta": meta.Peers[i].Address, groupForNode[id]: groupRaftAddress[id]},
		}
	}
	return configs
}

// Keep the router in group-a while the meta leader is a follower of group-d.
// The source-holder remains group-d's bootstrap leader. This exposes callback
// selection that incorrectly treats local group membership as leadership.
func fixedPeerAddSourceFollowerMetaLeaderV1(t testing.TB, configs []FixedPeerTCPConfigV1) []FixedPeerTCPConfigV1 {
	t.Helper()
	const follower raftcluster.NodeID = "source-follower"
	addresses := fixedPeerFixtureUnusedAddressesV1(t, configs, 4)
	metaAddress, dataAddress, controlAddress, publicAddress := addresses[0], addresses[1], addresses[2], addresses[3]
	meta := configs[0].Catalog
	meta.BootstrapNode = follower
	meta.Peers = append(append([]raftcluster.Peer(nil), meta.Peers...), raftcluster.Peer{
		ID: follower, Address: metaAddress, Capabilities: meta.Features,
	})
	groups := append([]FixedPeerTCPGroupV1(nil), configs[0].Groups...)
	for i := range groups {
		if groups[i].ID == "group-d" {
			groups[i].Peers = append(append([]raftcluster.Peer(nil), groups[i].Peers...), raftcluster.Peer{ID: follower, Address: dataAddress})
		}
	}
	nodes := append(append([]FixedPeerTCPNodeV1(nil), configs[0].Nodes...), FixedPeerTCPNodeV1{ID: follower, Address: controlAddress})
	vector := *configs[0].Vector
	vector.PublicAddresses = make(map[raftcluster.NodeID]string, len(configs[0].Vector.PublicAddresses)+1)
	for node, addr := range configs[0].Vector.PublicAddresses {
		vector.PublicAddresses[node] = addr
	}
	vector.PublicAddresses[follower] = publicAddress
	vector.Catalog.Groups = append([]raftplacement.GroupV1(nil), vector.Catalog.Groups...)
	for i := range vector.Catalog.Groups {
		if vector.Catalog.Groups[i].ID == "group-d" {
			vector.Catalog.Groups[i].Members = append(append([]raftcluster.NodeID(nil), vector.Catalog.Groups[i].Members...), follower)
		}
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(vector.Identity.Index.CatalogEpoch, vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	vector.Identity.Index.CatalogDigest = record.Digest
	for i := range configs {
		configs[i].Nodes, configs[i].Catalog, configs[i].Groups, configs[i].Vector = nodes, meta, groups, &vector
	}
	root := t.TempDir()
	return append(configs, FixedPeerTCPConfigV1{
		NodeID: follower, DataRoot: filepath.Join(root, "data"), RaftRoot: filepath.Join(root, "raft"),
		ListenAddress: controlAddress, Nodes: nodes, Catalog: meta, Groups: groups,
		RequestTimeout: configs[0].RequestTimeout, RaftTimeout: configs[0].RaftTimeout, Vector: &vector,
		RaftListen: map[raftcluster.GroupID]string{"meta": metaAddress, "group-d": dataAddress},
	})
}

// The fixture transfers only declared immutable search assets. In particular,
// it cannot smuggle the builder's document DB, ColumnGraph or source WAL into
// an owner process and then mistake a full-local open for hosted-only serving.
func fixedPeerCopyHostedVectorAssetsV1(t testing.TB, source, target string, manifest collections.VectorPartitionManifestV1, hosted map[uint32]bool, router bool) map[string]bool {
	t.Helper()
	allowed := make(map[string]bool)
	copyAsset := func(asset collections.VectorPartitionAssetV1) {
		rel := filepath.Join(filepath.FromSlash(asset.Ref.Namespace), "assets", "segments", fmt.Sprintf("segment-%06d.tca", asset.Ref.FileID))
		if allowed[rel] {
			return
		}
		from, to := filepath.Join(backenddb.ColumnAssetRootDirPath(source), rel), filepath.Join(backenddb.ColumnAssetRootDirPath(target), rel)
		if err := os.MkdirAll(filepath.Dir(to), 0700); err != nil {
			t.Fatal(err)
		}
		in, err := os.Open(from)
		if err != nil {
			t.Fatal(err)
		}
		out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			_ = in.Close()
			t.Fatal(err)
		}
		_, copyErr := io.Copy(out, in)
		if err := errors.Join(copyErr, in.Close(), out.Close()); err != nil {
			t.Fatal(err)
		}
		allowed[rel] = true
	}
	for _, asset := range manifest.Assets {
		if hosted[asset.Ref.FileID] {
			copyAsset(asset)
		}
	}
	if router {
		copyAsset(manifest.RouterAsset)
	}
	expectedFiles := len(hosted)
	if router {
		expectedFiles++
	}
	if len(allowed) != expectedFiles {
		t.Fatalf("hosted asset file coverage=%d want %d graph segments and router=%t", len(allowed), len(hosted), router)
	}
	fixedPeerAssertHostedVectorFilesV1(t, target, allowed, true)
	return allowed
}

func fixedPeerAssertHostedVectorFilesV1(t testing.TB, target string, allowed map[string]bool, allFiles bool) {
	t.Helper()
	missing := maps.Clone(allowed)
	walkRoot := backenddb.ColumnAssetRootDirPath(target)
	if allFiles {
		walkRoot = target
	}
	err := filepath.WalkDir(walkRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(backenddb.ColumnAssetRootDirPath(target), path)
		if err != nil {
			return err
		}
		if !allowed[rel] {
			return fmt.Errorf("non-hosted or source file in process root %q", rel)
		}
		delete(missing, rel)
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("invalid hosted asset file %q: %v", rel, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for rel := range missing {
		t.Fatalf("missing hosted asset file in process root %q", rel)
	}
}
