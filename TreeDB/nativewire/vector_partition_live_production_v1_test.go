package nativewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

type vectorPartitionLiveProductionFixtureV1 struct {
	dir        string
	database   *backenddb.DB
	collection *collections.Collection
	definition collections.VectorIndexDefinition
	manifest   collections.VectorPartitionManifestV1
	catalog    raftplacement.ResolvedCatalogV1
	placement  raftplacement.VectorPartitionPlacementRecordV1
}

type vectorPartitionLiveProductionDispatcherV1 struct {
	mu                    sync.Mutex
	services              map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1
	recordCalls           bool
	calls                 []VectorPartitionShardSearchRequestV1
	reportedPackHeapBytes uint64
	beforeDispatch        func(context.Context, VectorPartitionShardSearchRequestV1) error
}

func (d *vectorPartitionLiveProductionDispatcherV1) DispatchVectorPartitionShardSearchV1(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
	d.mu.Lock()
	if d.recordCalls {
		d.calls = append(d.calls, request)
	}
	service := d.services[request.TargetGroupID]
	beforeDispatch := d.beforeDispatch
	d.mu.Unlock()
	if service == nil {
		return VectorPartitionShardSearchResponseV1{}, ErrVectorPartitionCoordinatorUnavailable
	}
	if beforeDispatch != nil {
		if err := beforeDispatch(ctx, request); err != nil {
			return VectorPartitionShardSearchResponseV1{}, err
		}
	}
	response, err := service.Search(ctx, request)
	if err != nil {
		return VectorPartitionShardSearchResponseV1{}, err
	}
	var heapBytes uint64
	for _, partial := range response.Partials {
		heapBytes += partial.HeapBytes
	}
	d.mu.Lock()
	d.reportedPackHeapBytes += heapBytes
	d.mu.Unlock()
	return response, nil
}

func (d *vectorPartitionLiveProductionDispatcherV1) replace(services map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1) {
	d.mu.Lock()
	d.services = services
	d.mu.Unlock()
}

func (d *vectorPartitionLiveProductionDispatcherV1) setBeforeDispatch(fn func(context.Context, VectorPartitionShardSearchRequestV1) error) {
	d.mu.Lock()
	d.beforeDispatch = fn
	d.mu.Unlock()
}

func (d *vectorPartitionLiveProductionDispatcherV1) lastRequest() VectorPartitionShardSearchRequestV1 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls[len(d.calls)-1]
}

func (d *vectorPartitionLiveProductionDispatcherV1) requests(requestID string) []VectorPartitionShardSearchRequestV1 {
	d.mu.Lock()
	defer d.mu.Unlock()
	requests := make([]VectorPartitionShardSearchRequestV1, 0, len(d.calls))
	for _, request := range d.calls {
		if strings.HasPrefix(request.RequestID, requestID+"/") {
			requests = append(requests, request)
		}
	}
	return requests
}

func (d *vectorPartitionLiveProductionDispatcherV1) packHeapBytes() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reportedPackHeapBytes
}

func TestVectorPartitionLiveProductionCoordinatorMutationAndColdReloadV1(t *testing.T) {
	fixture := newVectorPartitionLiveNativewireFixtureV1(t)
	defer fixture.database.Close()

	newServices := func() (map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1, []*CollectionVectorPartitionGenerationSourceV1) {
		return newVectorPartitionLiveProductionServicesV1(t, fixture)
	}
	services, sources := newServices()
	defer func() {
		for _, source := range sources {
			_ = source.Close()
		}
	}()
	dispatcher := &vectorPartitionLiveProductionDispatcherV1{services: services, recordCalls: true}
	topology := vectorPartitionLiveCoordinatorTopologyV1(fixture)
	coordinator, err := NewVectorPartitionCoordinatorForTopologyV1(topology, CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: fixture.collection}, dispatcher, VectorPartitionCoordinatorLimitsV1{})
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()

	search := func(id string, query []float32) VectorPartitionCoordinatorResponseV1 {
		t.Helper()
		response, err := coordinator.Search(t.Context(), VectorPartitionCoordinatorRequestV1{
			Version: VectorPartitionCoordinatorVersionV1, RequestID: id, CancellationID: "cancel-" + id,
			Database: "default", Catalog: "default", Collection: "docs", IndexName: fixture.definition.Name,
			IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(fixture.definition),
			Query:                 query, Metric: VectorPartitionShardSearchMetricCosineV1,
			RouterMode: collections.VectorPartitionRouterModeExactV1, RouterScoreBudget: len(fixture.manifest.Representatives), PartitionProbes: 1,
			Consistency: VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: VectorPartitionShardSearchStatsBasicV1,
			TopK: 1, EfSearch: 8, RequestBytesLimit: 1 << 20, CandidateBytesLimit: 8 << 20,
			ResponseBytesLimit: 1 << 20, MergeEntriesLimit: 3,
		})
		if err != nil {
			t.Fatalf("search %s: %v", id, err)
		}
		if response.Counters.ExactScanPartitions != 0 || response.Counters.RequestPathFullRebuilds != 0 ||
			response.Counters.HNSWServedPartitions != response.Counters.SelectedPartitions {
			t.Fatalf("search %s fallback/rebuild counters=%+v", id, response.Counters)
		}
		return response
	}

	initial := search("initial", []float32{1, 0})
	if len(initial.Neighbors) != 1 || initial.Neighbors[0].ID != "a" || initial.Counters.SelectedPacks != 2 || initial.Counters.LiveDomainsSearched != 1 {
		t.Fatalf("initial response=%+v", initial)
	}
	initialRequests := dispatcher.requests("initial")
	liveAssignments := 0
	for _, request := range initialRequests {
		if len(request.LiveDomainIDs) == 0 {
			continue
		}
		if len(request.LiveDomainIDs) != 1 || request.LiveDomainIDs[0] != 0 {
			t.Fatalf("initial live-domain request=%+v", request.LiveDomainIDs)
		}
		liveAssignments++
	}
	if len(initialRequests) != 2 || liveAssignments != 1 {
		t.Fatalf("initial request assignments=%+v", initialRequests)
	}
	initialStats := []CollectionVectorPartitionGenerationCacheStatsV1{sources[0].Stats(), sources[1].Stats()}

	insertDone := make(chan error, 1)
	var dispatchOnce sync.Once
	var dispatchErr error
	dispatcher.setBeforeDispatch(func(ctx context.Context, _ VectorPartitionShardSearchRequestV1) error {
		dispatchOnce.Do(func() {
			document, err := json.Marshal(map[string]any{"embedding": []float32{.9, .1}})
			if err != nil {
				dispatchErr = err
				return
			}
			insertStarted := make(chan struct{})
			go func() {
				close(insertStarted)
				_, err := fixture.collection.Insert([]byte("0-concurrent"), document)
				insertDone <- err
			}()
			select {
			case <-insertStarted:
			case <-ctx.Done():
				dispatchErr = ctx.Err()
				return
			}
			select {
			case err := <-insertDone:
				dispatchErr = fmt.Errorf("concurrent insert crossed coordinator pin: %v", err)
			case <-time.After(20 * time.Millisecond):
				stored, getErr := fixture.collection.Get([]byte("0-concurrent"))
				if getErr != nil {
					dispatchErr = getErr
				} else if stored != nil {
					dispatchErr = errors.New("concurrent insert published before the coordinator pin released")
				}
			}
		})
		return dispatchErr
	})
	pinned := search("concurrent-pinned", []float32{.9, .1})
	dispatcher.setBeforeDispatch(nil)
	if len(pinned.Neighbors) != 1 || pinned.Neighbors[0].ID != "a" || pinned.LiveRevision != initial.LiveRevision || pinned.LiveCoverage != initial.LiveCoverage {
		t.Fatalf("concurrent pinned response=%+v initial=%+v", pinned, initial)
	}
	select {
	case err := <-insertDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent insert remained blocked after coordinator search")
	}
	concurrent := search("concurrent-visible", []float32{.9, .1})
	if len(concurrent.Neighbors) != 1 || concurrent.Neighbors[0].ID != "0-concurrent" || concurrent.LiveRevision <= pinned.LiveRevision || concurrent.LiveCoverage <= pinned.LiveCoverage {
		t.Fatalf("concurrent visible response=%+v pinned=%+v", concurrent, pinned)
	}

	insertVectorPartitionLiveDocumentV1(t, fixture.collection, "0", []float32{1, 0})
	inserted := search("insert", []float32{1, 0})
	if len(inserted.Neighbors) != 1 || inserted.Neighbors[0].ID != "0" || inserted.Counters.DeltaResults != 1 || inserted.Counters.LiveDomainsSearched != 1 {
		t.Fatalf("insert response=%+v", inserted)
	}
	insertLiveAssignments := 0
	for _, request := range dispatcher.requests("insert") {
		if len(request.LiveDomainIDs) == 0 {
			continue
		}
		measured, err := services[request.TargetGroupID].Search(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		var baseScoreCalls uint64
		for _, partial := range measured.Partials {
			baseScoreCalls += partial.ScoreCalls
		}
		if measured.ScoreCalls <= baseScoreCalls {
			t.Fatalf("live score calls=%d want greater than immutable partial sum=%d", measured.ScoreCalls, baseScoreCalls)
		}
		exactBudget := request
		exactBudget.ScoreCallsLimit = measured.ScoreCalls
		if response, err := services[request.TargetGroupID].Search(t.Context(), exactBudget); err != nil || response.ScoreCalls != measured.ScoreCalls {
			t.Fatalf("exact live score budget response=%+v err=%v want score calls=%d", response, err, measured.ScoreCalls)
		}
		exactBudget.ScoreCallsLimit--
		if _, err := services[request.TargetGroupID].Search(t.Context(), exactBudget); err == nil {
			t.Fatal("under-budget live search succeeded")
		} else {
			assertVectorPartitionShardSearchCodeV1(t, err, VectorPartitionShardSearchErrorAssetsUnavailableV1)
		}
		insertLiveAssignments++
	}
	if insertLiveAssignments != 1 {
		t.Fatalf("insert live assignments=%d", insertLiveAssignments)
	}
	for i, source := range sources {
		if got := source.Stats(); got.GenerationMisses != initialStats[i].GenerationMisses || got.PartitionMisses != initialStats[i].PartitionMisses {
			t.Fatalf("mutation reloaded warm generation source=%d before=%+v after=%+v", i, initialStats[i], got)
		}
	}

	replaceVectorPartitionLiveDocumentV1(t, fixture.collection, "a", []float32{.1, .9})
	replaced := search("replace-worse", []float32{1, 0})
	if len(replaced.Neighbors) != 1 || replaced.Neighbors[0].ID != "0" ||
		replaced.LiveRevision <= inserted.LiveRevision || replaced.LiveCoverage <= inserted.LiveCoverage ||
		replaced.Counters.BaseCandidates == 0 || replaced.Counters.DeltaCandidates == 0 ||
		replaced.Counters.DeltaResults != 1 || replaced.Counters.LiveDomainsSearched != 1 {
		t.Fatalf("stale nearest was admitted response=%+v", replaced)
	}
	replaceVectorPartitionLiveDocumentV1(t, fixture.collection, "0", []float32{0, 1})
	movedB := search("move-b", []float32{0, 1})
	if len(movedB.Neighbors) != 1 || movedB.Neighbors[0].ID != "0" {
		t.Fatalf("move to domain B response=%+v", movedB)
	}
	replaceVectorPartitionLiveDocumentV1(t, fixture.collection, "0", []float32{1, 0})
	movedA := search("move-a", []float32{1, 0})
	if len(movedA.Neighbors) != 1 || movedA.Neighbors[0].ID != "0" {
		t.Fatalf("move back to domain A response=%+v", movedA)
	}
	if err := fixture.collection.Delete([]byte("0")); err != nil {
		t.Fatal(err)
	}
	deleted := search("delete", []float32{1, 0})
	if len(deleted.Neighbors) != 1 || deleted.Neighbors[0].ID == "0" {
		t.Fatalf("delete response=%+v", deleted)
	}

	for _, source := range sources {
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
	}
	services, sources = newServices()
	dispatcher.replace(services)
	cold := search("cold-reload", []float32{1, 0})
	if len(cold.Neighbors) != 1 || cold.LiveRevision == 0 || cold.LiveCoverage <= fixture.manifest.SourceGeneration {
		t.Fatalf("cold reload response=%+v", cold)
	}

	if _, err := collections.NewCollectionManager(fixture.database).CreateCollection(&collections.CollectionMeta{Name: "unrelated"}); err != nil {
		t.Fatal(err)
	}
	unrelated := search("unrelated-state-refresh", []float32{1, 0})
	if len(unrelated.Neighbors) != 1 || unrelated.LiveRevision != cold.LiveRevision || unrelated.LiveCoverage != cold.LiveCoverage {
		t.Fatalf("unrelated publication response=%+v cold=%+v", unrelated, cold)
	}

	bad := dispatcher.lastRequest()
	bad.RequestID = "mismatch"
	bad.LiveRevision++
	if _, err := services[bad.TargetGroupID].Search(t.Context(), bad); !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
		t.Fatalf("mismatched live identity err=%v", err)
	}
}

func TestVectorPartitionLiveProductionCheckpointCloseReopenV1(t *testing.T) {
	fixture := newVectorPartitionLiveNativewireFixtureV1(t)
	t.Cleanup(func() {
		if fixture.database != nil {
			_ = fixture.database.Close()
		}
	})

	search := func(id string) VectorPartitionCoordinatorResponseV1 {
		t.Helper()
		services, sources := newVectorPartitionLiveProductionServicesV1(t, fixture)
		defer func() {
			for _, source := range sources {
				_ = source.Close()
			}
		}()
		coordinator, err := NewVectorPartitionCoordinatorForTopologyV1(
			vectorPartitionLiveCoordinatorTopologyV1(fixture),
			CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: fixture.collection},
			&vectorPartitionLiveProductionDispatcherV1{services: services},
			VectorPartitionCoordinatorLimitsV1{},
		)
		if err != nil {
			t.Fatal(err)
		}
		defer coordinator.Close()
		response, err := coordinator.Search(t.Context(), VectorPartitionCoordinatorRequestV1{
			Version: VectorPartitionCoordinatorVersionV1, RequestID: id, CancellationID: "cancel-" + id,
			Database: "default", Catalog: "default", Collection: "docs", IndexName: fixture.definition.Name,
			IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(fixture.definition),
			Query:                 []float32{1, 0}, Metric: VectorPartitionShardSearchMetricCosineV1,
			RouterMode: collections.VectorPartitionRouterModeExactV1, RouterScoreBudget: len(fixture.manifest.Representatives), PartitionProbes: 1,
			Consistency: VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: VectorPartitionShardSearchStatsBasicV1,
			TopK: 1, EfSearch: 8, RequestBytesLimit: 1 << 20, CandidateBytesLimit: 8 << 20,
			ResponseBytesLimit: 1 << 20, MergeEntriesLimit: 3,
		})
		if err != nil {
			t.Fatalf("search %s: %v", id, err)
		}
		if response.Counters.ExactScanPartitions != 0 || response.Counters.RequestPathFullRebuilds != 0 ||
			response.Counters.HNSWServedPartitions != response.Counters.SelectedPartitions {
			t.Fatalf("search %s fallback/rebuild counters=%+v", id, response.Counters)
		}
		return response
	}

	initial := search("reopen-initial")
	insertVectorPartitionLiveDocumentV1(t, fixture.collection, "0-reopen", []float32{1, 0})
	replaceVectorPartitionLiveDocumentV1(t, fixture.collection, "a", []float32{0, 1})
	replaceVectorPartitionLiveDocumentV1(t, fixture.collection, "0-reopen", []float32{0, 1})
	replaceVectorPartitionLiveDocumentV1(t, fixture.collection, "0-reopen", []float32{1, 0})
	insertVectorPartitionLiveDocumentV1(t, fixture.collection, "0-deleted-reopen", []float32{1, 0})
	if err := fixture.collection.Delete([]byte("0-deleted-reopen")); err != nil {
		t.Fatal(err)
	}
	mutated := search("reopen-mutated")
	if len(mutated.Neighbors) != 1 || mutated.Neighbors[0].ID != "0-reopen" || mutated.LiveRevision <= initial.LiveRevision || mutated.LiveCoverage <= initial.LiveCoverage {
		t.Fatalf("mutated response=%+v initial=%+v", mutated, initial)
	}
	ordinaryPlan, err := collections.NewVectorPartitionGenerationSearchOpenPlanWithContextV1(t.Context(), fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	ordinaryPin, err := fixture.collection.AcquireVectorPartitionReaderPinWithContextV1(t.Context(), fixture.definition.Name, fixture.manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.collection.OpenVectorPartitionLocalSearcherForGenerationSearchPlanWithContextV1(t.Context(), fixture.definition.Name, fixture.manifest.Generation, fixture.manifest.Assets[0].PartitionID, ordinaryPlan, ordinaryPin); !errors.Is(err, collections.ErrVectorPartitionSearchUnavailable) {
		ordinaryPin.Release()
		t.Fatalf("ordinary stale-source open err=%v", err)
	}
	ordinaryPin.Release()
	if err := fixture.database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.database.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.database = nil

	database, err := backenddb.Open(backenddb.Options{Dir: fixture.dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	fixture.database = database
	collection, err := collections.NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	fixture.collection = collection
	reopened := search("reopened")
	if len(reopened.Neighbors) != 1 || reopened.Neighbors[0].ID != "0-reopen" ||
		reopened.LiveRevision != mutated.LiveRevision || reopened.LiveCoverage != mutated.LiveCoverage {
		t.Fatalf("reopened response=%+v mutated=%+v", reopened, mutated)
	}

	wrongManifest := fixture.manifest
	wrongManifest.Placements = append([]collections.VectorPartitionPlacementV1(nil), fixture.manifest.Placements...)
	wrongManifest.Placements[0].GroupID = "substituted-group"
	wrongManifest.Canonicalize()
	if _, err := fixture.collection.NewVectorPartitionGenerationLiveSearchOpenPlanWithContextV1(t.Context(), wrongManifest); !errors.Is(err, collections.ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("non-authoritative live manifest err=%v", err)
	}

	openPlan, err := fixture.collection.NewVectorPartitionGenerationLiveSearchOpenPlanWithContextV1(t.Context(), fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	generationPin, err := fixture.collection.AcquireVectorPartitionReaderPinWithContextV1(t.Context(), fixture.definition.Name, fixture.manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	defer generationPin.Release()
	asset := fixture.manifest.Assets[0]
	assetPath := filepath.Join(fixture.database.ColumnAssetRootDir(), filepath.FromSlash(asset.Ref.Namespace), "assets", "segments", fmt.Sprintf("segment-%06d.tca", asset.Ref.FileID))
	file, err := os.OpenFile(assetPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{'X'}, asset.Ref.Offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.collection.OpenVectorPartitionLocalSearcherForGenerationLiveSearchPlanWithContextV1(t.Context(), fixture.definition.Name, fixture.manifest.Generation, asset.PartitionID, openPlan, generationPin); !errors.Is(err, collections.ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("corrupt immutable live pack err=%v", err)
	}
}

func BenchmarkVectorPartitionLiveProductionCoordinatorV1(b *testing.B) {
	for _, cell := range []struct {
		name       string
		mutatedIDs int
		wantFirst  string
	}{
		{name: "live_overlay_1_id_update_search_1_to_1", mutatedIDs: 1, wantFirst: "0-live"},
		{name: "live_overlay_1024_ids_update_search_1_to_1", mutatedIDs: 1024, wantFirst: "0-live"},
	} {
		b.Run(cell.name, func(b *testing.B) {
			fixture := newVectorPartitionLiveNativewireFixtureV1(b)
			defer fixture.database.Close()
			services, sources := newVectorPartitionLiveProductionServicesV1(b, fixture)
			defer func() {
				for _, source := range sources {
					_ = source.Close()
				}
			}()
			dispatcher := &vectorPartitionLiveProductionDispatcherV1{services: services}
			coordinator, err := NewVectorPartitionCoordinatorForTopologyV1(vectorPartitionLiveCoordinatorTopologyV1(fixture), CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: fixture.collection}, dispatcher, VectorPartitionCoordinatorLimitsV1{})
			if err != nil {
				b.Fatal(err)
			}
			defer coordinator.Close()

			request := VectorPartitionCoordinatorRequestV1{
				Version: VectorPartitionCoordinatorVersionV1, RequestID: "benchmark", CancellationID: "cancel-benchmark",
				Database: "default", Catalog: "default", Collection: "docs", IndexName: fixture.definition.Name,
				IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(fixture.definition),
				Query:                 []float32{1, 0}, Metric: VectorPartitionShardSearchMetricCosineV1,
				RouterMode: collections.VectorPartitionRouterModeExactV1, RouterScoreBudget: len(fixture.manifest.Representatives), PartitionProbes: 1,
				Consistency: VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: VectorPartitionShardSearchStatsBasicV1,
				TopK: 4, EfSearch: 16, RequestBytesLimit: 1 << 20, CandidateBytesLimit: 8 << 20,
				ResponseBytesLimit: 1 << 20, MergeEntriesLimit: 8,
			}
			if _, err := coordinator.Search(b.Context(), request); err != nil {
				b.Fatalf("warm search: %v", err)
			}
			if cell.mutatedIDs > 0 {
				insertVectorPartitionLiveDocumentV1(b, fixture.collection, "0-live", []float32{1, 0})
			}
			for i := 1; i < cell.mutatedIDs; i++ {
				insertVectorPartitionLiveDocumentV1(b, fixture.collection, fmt.Sprintf("live-%04d", i), []float32{.8, .2})
			}

			latencies := make([]uint64, b.N)
			var writes, searches, searchErrors, correctResults uint64
			var baseCandidates, deltaCandidates, baseResults, deltaResults uint64
			var domains, packs, exactFallbacks, rebuilds, failures uint64
			var lastCutovers, liveMutatedIDs, liveIDs uint64
			heapBefore := dispatcher.packHeapBytes()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vector := []float32{1, 0}
				if i&1 != 0 {
					vector = []float32{0, 1}
				}
				replaceVectorPartitionLiveDocumentV1(b, fixture.collection, "0-live", vector)
				writes++
				request.Query = vector
				started := time.Now()
				response, err := coordinator.Search(b.Context(), request)
				latencies[i] = uint64(time.Since(started))
				if err != nil {
					searchErrors++
					continue
				}
				searches++
				if len(response.Neighbors) > 0 && response.Neighbors[0].ID == cell.wantFirst {
					correctResults++
				}
				baseCandidates += response.Counters.BaseCandidates
				deltaCandidates += response.Counters.DeltaCandidates
				baseResults += response.Counters.BaseResults
				deltaResults += response.Counters.DeltaResults
				domains += response.Counters.LiveDomainsSearched
				packs += response.Counters.SelectedPacks
				exactFallbacks += response.Counters.ExactScanPartitions
				rebuilds += response.Counters.RequestPathFullRebuilds
				failures += response.Counters.Failures
				lastCutovers = response.Counters.Cutovers
				liveMutatedIDs = response.Counters.LiveMutatedIDs
				liveIDs = response.Counters.LiveIDs
			}
			b.StopTimer()

			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			p99 := latencies[(len(latencies)-1)*99/100]
			storageBytes, err := vectorPartitionLiveProductionStorageBytesV1(fixture.dir)
			if err != nil {
				b.Fatal(err)
			}
			runtime.GC()
			var liveHeap runtime.MemStats
			runtime.ReadMemStats(&liveHeap)
			operations := float64(max(b.N, 1))
			elapsed := b.Elapsed().Seconds()
			b.ReportMetric(float64(writes)/elapsed, "writes/s")
			b.ReportMetric(float64(searches)/elapsed, "searches/s")
			b.ReportMetric(float64(p99), "p99-search-ns")
			b.ReportMetric(float64(correctResults)/operations, "correct-results/op")
			b.ReportMetric(float64(correctResults)/operations, "recall-at-1")
			b.ReportMetric(float64(baseCandidates)/operations, "base-candidates/op")
			b.ReportMetric(float64(deltaCandidates)/operations, "delta-candidates/op")
			b.ReportMetric(float64(baseResults)/operations, "base-results/op")
			b.ReportMetric(float64(deltaResults)/operations, "delta-results/op")
			b.ReportMetric(float64(domains)/operations, "live-domains/op")
			b.ReportMetric(float64(packs)/operations, "packs/op")
			b.ReportMetric(float64(dispatcher.packHeapBytes()-heapBefore)/operations, "response-pack-heap-B/op")
			b.ReportMetric(float64(liveHeap.HeapAlloc), "reachable-process-heap-B")
			b.ReportMetric(float64(storageBytes), "storage-B")
			b.ReportMetric(float64(lastCutovers), "cutovers")
			b.ReportMetric(float64(liveMutatedIDs), "live-mutated-ids")
			b.ReportMetric(float64(liveIDs), "live-ids")
			b.ReportMetric(float64(exactFallbacks), "exact-fallbacks")
			b.ReportMetric(float64(rebuilds), "request-rebuilds")
			b.ReportMetric(float64(failures), "reported-failures")
			b.ReportMetric(float64(searchErrors), "search-errors")
			if searchErrors != 0 || failures != 0 || exactFallbacks != 0 || rebuilds != 0 || searches != uint64(b.N) {
				b.Fatalf("unexpected result searches=%d correct=%d errors=%d failures=%d exact=%d rebuilds=%d", searches, correctResults, searchErrors, failures, exactFallbacks, rebuilds)
			}
		})
	}
}

func newVectorPartitionLiveNativewireFixtureV1(t testing.TB) vectorPartitionLiveProductionFixtureV1 {
	return newVectorPartitionLiveNativewireDocumentsV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0, overlap: true},
		{id: "b", vector: []float32{.8, .2}, home: 1},
		{id: "c", vector: []float32{0, 1}, home: 2},
		{id: "d", vector: []float32{.2, .8}, home: 2},
	}, nil)
}

type vectorPartitionLiveDocumentV1 struct {
	id      string
	vector  []float32
	home    uint32
	overlap bool
}

func newVectorPartitionLiveNativewireDocumentsV1(t testing.TB, documents []vectorPartitionLiveDocumentV1, columns *collections.ColumnStoreConfig) vectorPartitionLiveProductionFixtureV1 {
	t.Helper()
	if !collections.VectorPartitionNamespacePersistenceSupportedForTestingV1() {
		t.Skip("vector partition namespace persistence unsupported on this platform")
	}
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	database, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	dimensions := len(documents[0].vector)
	definition := collections.VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: dimensions, M: 2, EfConstruction: 8, EfSearch: 8, Strategy: collections.VectorIndexStrategyColumnGraph}
	if len(documents) > 4 {
		definition.M, definition.EfConstruction, definition.EfSearch = 16, 128, 96
	}
	if columns == nil {
		columns = &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: dimensions}}}
	}
	meta := collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: columns}, VectorIndexes: []collections.VectorIndexDefinition{definition}}
	manager := collections.NewCollectionManager(database)
	if _, err := manager.CreateCollection(&meta); err != nil {
		database.Close()
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("docs")
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	byID := make(map[string]vectorPartitionLiveDocumentV1, len(documents))
	for _, document := range documents {
		insertVectorPartitionLiveDocumentV1(t, collection, document.id, document.vector)
		byID[document.id] = document
	}
	if _, err := collection.RebuildVectorIndex(definition.Name); err != nil {
		database.Close()
		t.Fatal(err)
	}
	source, rows, err := collection.ReadVectorPartitionRouterSourceRowsV1(definition.Name)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	manifest := collections.VectorPartitionManifestV1{
		State: "building", Collection: "docs", IndexName: definition.Name,
		IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(definition),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 3, DomainCount: 2,
		DomainPacks:   []collections.VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 0, PackID: 1}, {DomainID: 1, PackID: 2}},
		BalancePolicy: "disjoint_v1",
		Placements:    []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}, {PartitionID: 2, GroupID: "group-b"}},
	}
	parts := []internalrouter.RouterPartitionV1{{PartitionID: 0}, {PartitionID: 1}, {PartitionID: 2}}
	for _, row := range rows {
		document := byID[string(row.DocumentID)]
		home := document.home
		manifest.Memberships = append(manifest.Memberships, collections.VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: home})
		parts[home].Vectors = append(parts[home].Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(collections.VectorPartitionMembershipHomeV1)})
		if document.overlap {
			manifest.OverlapMemberships = append(manifest.OverlapMemberships, collections.VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: 1})
			parts[1].Vectors = append(parts[1].Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(collections.VectorPartitionMembershipOverlapV1)})
		}
	}
	manifest.Canonicalize()
	inputs := make([]collections.VectorPartitionSearchAssetV1, 3)
	for partition := range inputs {
		inputs[partition] = collections.VectorPartitionSearchAssetV1{Source: source, Generation: manifest.Generation, PartitionID: uint32(partition), Dimensions: definition.Dimensions}
	}
	assets, resources, err := collection.MaterializeVectorPartitionLocalSearchAssetsV1(definition.Name, manifest, 9101, inputs)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	defer resources.Release()
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		database.Close()
		t.Fatal(err)
	}
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.BranchFactor, cfg.LeafSize, cfg.RepresentativeBudget = 2, 1, len(parts)
	cfg.MaxDepth, cfg.MaxIterations, cfg.MaxVectors = 4, 8, max(8, len(documents)*2)
	cfg.MaxDimensions, cfg.MaxRepresentatives, cfg.MaxScalarWork = max(8, dimensions), 32, 100_000_000
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(t.Context(), manifest, parts, collections.VectorPartitionRouterBuildOptionsV1{Config: cfg, AssetFileID: 9102, AssetPartID: 1, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		database.Close()
		t.Fatal(err)
	}
	ready, err := collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), definition.Name, manifest.Generation)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Features: raftplacement.DefaultFeatureSet(), Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}, {ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: definition.Name, IndexDefinitionDigest: ready.IndexDefinitionDigest, SourceGeneration: ready.SourceGeneration, SourceChecksum: ready.SourceChecksum, SourceSchemaHash: ready.SourceSchemaHash, SourceRowCount: ready.SourceRowCount, PartitionGeneration: ready.Generation, PartitionCount: ready.PartitionCount, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}, {PartitionID: 2, GroupID: "group-b"}}}
	return vectorPartitionLiveProductionFixtureV1{dir: dir, database: database, collection: collection, definition: definition, manifest: ready, catalog: catalog, placement: placement}
}

func vectorPartitionLiveCoordinatorTopologyV1(f vectorPartitionLiveProductionFixtureV1) VectorPartitionCoordinatorTopologyV1 {
	group, err := f.catalog.Resolve(f.placement.Collection)
	if err != nil {
		panic(err) // All callers construct a validated fixture catalog.
	}
	topology := VectorPartitionCoordinatorTopologyV1{Database: "default", Catalog: "default", Collection: f.manifest.Collection, CollectionGroupID: string(group), IndexName: f.definition.Name, IndexDefinitionDigest: f.manifest.IndexDefinitionDigest, SourceGeneration: f.manifest.SourceGeneration, SourceChecksum: f.manifest.SourceChecksum, SourceSchemaHash: f.manifest.SourceSchemaHash, SourceRowCount: f.manifest.SourceRowCount, PartitionGeneration: f.manifest.Generation}
	for _, group := range f.catalog.Groups {
		members := make([]string, len(group.Members))
		for i, member := range group.Members {
			members[i] = string(member)
		}
		topology.Groups = append(topology.Groups, VectorPartitionCoordinatorTopologyGroupV1{ID: string(group.ID), Members: members, LeaderHint: string(group.LeaderHint)})
	}
	for _, partition := range f.placement.Partitions {
		topology.Partitions = append(topology.Partitions, VectorPartitionCoordinatorTopologyPartitionV1{PartitionID: partition.PartitionID, GroupID: string(partition.GroupID)})
	}
	return topology
}

func newVectorPartitionLiveProductionServicesV1(t testing.TB, fixture vectorPartitionLiveProductionFixtureV1) (map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1, []*CollectionVectorPartitionGenerationSourceV1) {
	t.Helper()
	services := make(map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1, 2)
	sources := make([]*CollectionVectorPartitionGenerationSourceV1, 0, 2)
	for _, group := range fixture.catalog.Groups {
		source, err := NewCollectionVectorPartitionGenerationSourceV1(fixture.collection)
		if err != nil {
			t.Fatal(err)
		}
		read := &fakeVectorPartitionReadCoordinatorV1{
			proof:    raftcluster.ReadIndexProof{NodeID: group.LeaderHint, GroupID: group.ID, Term: 1, Index: 1, HasQuorum: true, EvidenceKind: raftcluster.ReadIndexEvidenceProduction},
			progress: raftcluster.AppliedProgress{NodeID: group.LeaderHint, GroupID: group.ID, Term: 1, Index: 1, HasApplied: true},
		}
		service, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
			Catalog: fixture.catalog, Placement: fixture.placement, LocalNodeID: group.LeaderHint,
			LocalGroupID: group.ID, ReadCoordinator: read, GenerationSource: source,
		})
		if err != nil {
			_ = source.Close()
			for _, opened := range sources {
				_ = opened.Close()
			}
			t.Fatal(err)
		}
		services[group.ID] = service
		sources = append(sources, source)
	}
	return services, sources
}

func vectorPartitionLiveProductionStorageBytesV1(root string) (uint64, error) {
	var total uint64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += uint64(info.Size())
		}
		return nil
	})
	return total, err
}

func insertVectorPartitionLiveDocumentV1(t testing.TB, collection *collections.Collection, id string, vector []float32) {
	t.Helper()
	document, err := json.Marshal(map[string]any{"embedding": vector, "time_us": 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Insert([]byte(id), document); err != nil {
		t.Fatalf("insert %s: %v", id, err)
	}
}

func replaceVectorPartitionLiveDocumentV1(t testing.TB, collection *collections.Collection, id string, vector []float32) {
	t.Helper()
	document, err := json.Marshal(map[string]any{"embedding": vector})
	if err != nil {
		t.Fatal(err)
	}
	matched, err := collection.Replace([]byte(id), document)
	if err != nil || !matched {
		t.Fatalf("replace %s matched=%v: %v", id, matched, err)
	}
}
