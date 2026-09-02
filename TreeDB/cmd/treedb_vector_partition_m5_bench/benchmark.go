package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	servicewire "github.com/snissn/gomap/TreeDB/nativewire"
)

type benchmarkEnvironment struct {
	collection *collections.Collection
	status     collections.VectorPartitionStatusV1
	cluster    *localRaftCluster
	catalog    raftplacement.ResolvedCatalogV1
	placement  raftplacement.VectorPartitionPlacementRecordV1
	request    servicewire.VectorPartitionShardSearchRequestV1
}

type stageSamples struct {
	routeOwner     []uint64
	readIndexApply []uint64
	generationOpen []uint64
	search         []uint64
	responseCopy   []uint64
	total          []uint64
}

type serviceMeasurement struct {
	phase  servicePhaseReport
	stages stageSamples
}

type baselineMeasurement struct {
	phase baselineReport
}

type serviceAccumulators struct {
	wallNanos  []uint64
	stages     stageSamples
	elapsed    time.Duration
	allocBytes uint64
	allocs     uint64
	responses  uint64
	candidates uint64
	edges      uint64
	mapped     uint64
	heap       uint64
	pack       uint64
	route      string
}

func newBenchmarkEnvironment(collection *collections.Collection, status collections.VectorPartitionStatusV1, cluster *localRaftCluster, partition uint32, query []float32, cfg config) (*benchmarkEnvironment, error) {
	if collection == nil || cluster == nil || cluster.leader == "" {
		return nil, errors.New("incomplete M5 benchmark environment")
	}
	ref := raftplacement.CollectionRefV1{
		Database:   raftplacement.DefaultDatabase,
		Catalog:    raftplacement.DefaultCatalog,
		Collection: status.Manifest.Collection,
	}
	groups := make([]raftplacement.GroupV1, 0, len(status.Manifest.Placements))
	seen := make(map[raftcluster.GroupID]struct{}, len(status.Manifest.Placements))
	parts := make([]raftplacement.VectorPartitionGroupV1, len(status.Manifest.Placements))
	for i, item := range status.Manifest.Placements {
		groupID := raftcluster.GroupID(item.GroupID)
		parts[i] = raftplacement.VectorPartitionGroupV1{PartitionID: item.PartitionID, GroupID: groupID}
		if _, exists := seen[groupID]; exists {
			continue
		}
		seen[groupID] = struct{}{}
		members := []raftcluster.NodeID{"node-a"}
		leader := raftcluster.NodeID("node-a")
		if groupID == cluster.groupID {
			members = []raftcluster.NodeID{"node-a", "node-b", "node-c"}
			leader = cluster.leader
		}
		groups = append(groups, raftplacement.GroupV1{ID: groupID, Members: members, LeaderHint: leader})
	}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Features: raftplacement.DefaultFeatureSet(),
		Groups:   groups,
		Placements: []raftplacement.CollectionPlacementV1{{
			Collection: ref,
			GroupID:    cluster.groupID,
			Mode:       raftplacement.PlacementModeCollectionV1,
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("build benchmark catalog: %w", err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection:            ref,
		IndexName:             status.Manifest.IndexName,
		IndexDefinitionDigest: status.Manifest.IndexDefinitionDigest,
		SourceGeneration:      status.Manifest.SourceGeneration,
		SourceChecksum:        status.Manifest.SourceChecksum,
		SourceSchemaHash:      status.Manifest.SourceSchemaHash,
		SourceRowCount:        status.Manifest.SourceRowCount,
		PartitionGeneration:   status.Manifest.Generation,
		PartitionCount:        status.Manifest.PartitionCount,
		Partitions:            parts,
	}
	request := servicewire.VectorPartitionShardSearchRequestV1{
		Version:               servicewire.VectorPartitionShardSearchVersionV1,
		RequestID:             "m5-real-raft-1m-v1",
		CancellationID:        "m5-real-raft-1m-v1-cancel",
		Database:              ref.Database,
		Catalog:               ref.Catalog,
		Collection:            ref.Collection,
		IndexName:             status.Manifest.IndexName,
		IndexDefinitionDigest: status.Manifest.IndexDefinitionDigest,
		SourceGeneration:      status.Manifest.SourceGeneration,
		SourceChecksum:        status.Manifest.SourceChecksum,
		SourceSchemaHash:      status.Manifest.SourceSchemaHash,
		SourceRowCount:        status.Manifest.SourceRowCount,
		PartitionGeneration:   status.Manifest.Generation,
		RouterGeneration:      status.Manifest.RouterGeneration,
		ReadySetDigest:        status.Manifest.ReadySetDigest,
		TargetGroupID:         cluster.groupID,
		TargetNodeID:          cluster.leader,
		PartitionIDs:          []uint32{partition},
		Query:                 append([]float32(nil), query...),
		Metric:                servicewire.VectorPartitionShardSearchMetricCosineV1,
		Mode:                  servicewire.VectorPartitionShardSearchModeNoDocumentV1,
		Consistency:           servicewire.VectorPartitionShardSearchConsistencySnapshotV1,
		StatsMode:             servicewire.VectorPartitionShardSearchStatsBasicV1,
		TopK:                  cfg.topK,
		EfSearch:              cfg.efSearch,
		RequestBytesLimit:     64 << 10,
		CandidateBytesLimit:   64 << 20,
		ResponseBytesLimit:    64 << 20,
	}
	return &benchmarkEnvironment{
		collection: collection,
		status:     status,
		cluster:    cluster,
		catalog:    catalog,
		placement:  placement,
		request:    request,
	}, nil
}

func (e *benchmarkEnvironment) Close() error {
	return nil
}

func (e *benchmarkEnvironment) newService(source *servicewire.CollectionVectorPartitionGenerationSourceV1) (*servicewire.VectorPartitionShardSearchServiceV1, error) {
	return servicewire.NewVectorPartitionShardSearchServiceV1(servicewire.VectorPartitionShardSearchServiceOptionsV1{
		Catalog:          e.catalog,
		Placement:        e.placement,
		LocalNodeID:      e.cluster.leader,
		LocalGroupID:     e.cluster.groupID,
		ReadCoordinator:  e.cluster.readCoordinator(),
		GenerationSource: source,
	})
}

func (e *benchmarkEnvironment) measureCold(ctx context.Context, samples int) (serviceMeasurement, error) {
	var accum serviceAccumulators
	for i := 0; i < samples; i++ {
		source, err := servicewire.NewCollectionVectorPartitionGenerationSourceV1(e.collection)
		if err != nil {
			return serviceMeasurement{}, err
		}
		service, err := e.newService(source)
		if err != nil {
			_ = source.Close()
			return serviceMeasurement{}, err
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		response, err := service.Search(ctx, e.request)
		elapsed := time.Since(started)
		runtime.ReadMemStats(&after)
		if err != nil {
			_ = source.Close()
			return serviceMeasurement{}, fmt.Errorf("cold sample %d: %w", i, err)
		}
		if err := accum.add(response, elapsed); err != nil {
			_ = source.Close()
			return serviceMeasurement{}, err
		}
		accum.allocBytes += after.TotalAlloc - before.TotalAlloc
		accum.allocs += after.Mallocs - before.Mallocs
		if err := source.Close(); err != nil {
			return serviceMeasurement{}, err
		}
	}
	return accum.serviceMeasurement(samples, requestBytes(e.request)), nil
}

func (e *benchmarkEnvironment) measureWarmAndBaseline(ctx context.Context, cfg config) (serviceMeasurement, baselineMeasurement, sourceCacheReport, error) {
	source, err := servicewire.NewCollectionVectorPartitionGenerationSourceV1(e.collection)
	if err != nil {
		return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, err
	}
	defer source.Close()
	service, err := e.newService(source)
	if err != nil {
		return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, err
	}
	for i := 0; i < cfg.warmup; i++ {
		response, searchErr := service.Search(ctx, e.request)
		if searchErr != nil {
			return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, fmt.Errorf("warmup service request %d: %w", i, searchErr)
		}
		if err := validateResponseRoute(response); err != nil {
			return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, err
		}
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var warm serviceAccumulators
	for i := 0; i < cfg.warmSamples; i++ {
		started := time.Now()
		response, searchErr := service.Search(ctx, e.request)
		elapsed := time.Since(started)
		if searchErr != nil {
			return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, fmt.Errorf("warm service request %d: %w", i, searchErr)
		}
		if err := warm.add(response, elapsed); err != nil {
			return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, err
		}
	}
	runtime.ReadMemStats(&after)
	warm.allocBytes = after.TotalAlloc - before.TotalAlloc
	warm.allocs = after.Mallocs - before.Mallocs

	pinned, err := source.PinVectorPartitionGenerationV1(ctx, e.request.IndexName, e.request.PartitionGeneration)
	if err != nil {
		return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, err
	}
	if pinned == nil {
		return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, errors.New("generation source returned a nil pinned generation")
	}
	defer pinned.Close()
	lease, err := pinned.OpenPartition(ctx, e.request.PartitionIDs[0])
	if err != nil {
		return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, err
	}
	if lease == nil || lease.Searcher == nil {
		if lease != nil {
			_ = lease.Close()
		}
		return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, errors.New("pinned generation returned a nil partition lease or searcher")
	}
	defer lease.Close()
	if lease.Searcher.Status().SearchRoute != m5RequiredRoute {
		return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, fmt.Errorf("direct baseline route=%q want %q", lease.Searcher.Status().SearchRoute, m5RequiredRoute)
	}
	searchOpts := collections.VectorPartitionSearchOptionsV1{TopK: cfg.topK, EfSearch: cfg.efSearch}
	for i := 0; i < cfg.baselineWarmup; i++ {
		if _, metrics, searchErr := lease.Searcher.SearchWithOptionsV1(ctx, e.request.Query, searchOpts); searchErr != nil {
			return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, fmt.Errorf("baseline warmup %d: %w", i, searchErr)
		} else if metrics.Route != m5RequiredRoute {
			return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, fmt.Errorf("baseline warmup route=%q", metrics.Route)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&before)
	baselineSamples := make([]uint64, 0, cfg.baseline)
	var baselineElapsed time.Duration
	var candidates, edges uint64
	for i := 0; i < cfg.baseline; i++ {
		started := time.Now()
		_, metrics, searchErr := lease.Searcher.SearchWithOptionsV1(ctx, e.request.Query, searchOpts)
		elapsed := time.Since(started)
		if searchErr != nil {
			return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, fmt.Errorf("baseline sample %d: %w", i, searchErr)
		}
		if metrics.Route != m5RequiredRoute {
			return serviceMeasurement{}, baselineMeasurement{}, sourceCacheReport{}, fmt.Errorf("baseline route=%q want %q", metrics.Route, m5RequiredRoute)
		}
		baselineSamples = append(baselineSamples, uint64(elapsed.Nanoseconds()))
		baselineElapsed += elapsed
		candidates += metrics.Candidates
		edges += metrics.Edges
	}
	runtime.ReadMemStats(&after)
	baseline := baselineMeasurement{phase: baselineReport{
		Latency: distributionOf(baselineSamples, baselineElapsed),
		Allocations: allocationMetrics{
			BytesPerOp:  float64(after.TotalAlloc-before.TotalAlloc) / float64(cfg.baseline),
			AllocsPerOp: float64(after.Mallocs-before.Mallocs) / float64(cfg.baseline),
			Scope:       "process-wide runtime counters across the timed batch; live Raft background activity is included",
		},
		Candidates:  float64(candidates) / float64(cfg.baseline),
		Edges:       float64(edges) / float64(cfg.baseline),
		SearchRoute: m5RequiredRoute,
	}}
	return warm.serviceMeasurement(cfg.warmSamples, requestBytes(e.request)), baseline, sourceCache(source.Stats()), nil
}

func (a *serviceAccumulators) add(response servicewire.VectorPartitionShardSearchResponseV1, wall time.Duration) error {
	if err := validateResponseRoute(response); err != nil {
		return err
	}
	a.wallNanos = append(a.wallNanos, uint64(wall.Nanoseconds()))
	a.elapsed += wall
	a.stages.routeOwner = append(a.stages.routeOwner, response.Timing.RouteOwnerNanos)
	a.stages.readIndexApply = append(a.stages.readIndexApply, response.Timing.ReadIndexApplyNanos)
	a.stages.generationOpen = append(a.stages.generationOpen, response.Timing.GenerationOpenNanos)
	a.stages.search = append(a.stages.search, response.Timing.SearchNanos)
	a.stages.responseCopy = append(a.stages.responseCopy, response.Timing.ResponseCopyNanos)
	a.stages.total = append(a.stages.total, response.Timing.TotalNanos)
	a.responses += response.ResponseBytes
	a.candidates += response.Candidates
	a.edges += response.Edges
	partial := response.Partials[0]
	a.mapped = partial.MappedBytes
	a.heap = partial.HeapBytes
	a.pack = partial.PackBytes
	a.route = partial.SearchRoute
	return nil
}

func (a serviceAccumulators) serviceMeasurement(samples int, requestBytes uint64) serviceMeasurement {
	return serviceMeasurement{
		phase: servicePhaseReport{
			Latency: distributionOf(a.wallNanos, a.elapsed),
			Stages: stageDistributions{
				RouteOwner:     distributionOf(a.stages.routeOwner, durationSum(a.stages.routeOwner)),
				ReadIndexApply: distributionOf(a.stages.readIndexApply, durationSum(a.stages.readIndexApply)),
				GenerationOpen: distributionOf(a.stages.generationOpen, durationSum(a.stages.generationOpen)),
				Search:         distributionOf(a.stages.search, durationSum(a.stages.search)),
				ResponseCopy:   distributionOf(a.stages.responseCopy, durationSum(a.stages.responseCopy)),
				Total:          distributionOf(a.stages.total, durationSum(a.stages.total)),
			},
			Allocations: allocationMetrics{
				BytesPerOp:  float64(a.allocBytes) / float64(samples),
				AllocsPerOp: float64(a.allocs) / float64(samples),
				Scope:       "process-wide runtime counters across timed requests; live Raft background activity is included",
			},
			RequestBytes:  requestBytes,
			ResponseBytes: float64(a.responses) / float64(samples),
			Candidates:    float64(a.candidates) / float64(samples),
			Edges:         float64(a.edges) / float64(samples),
			MappedBytes:   a.mapped,
			HeapBytes:     a.heap,
			PackBytes:     a.pack,
			SearchRoute:   a.route,
		},
		stages: a.stages,
	}
}

func validateResponseRoute(response servicewire.VectorPartitionShardSearchResponseV1) error {
	if len(response.Partials) != 1 || response.Partials[0].SearchRoute != m5RequiredRoute {
		return fmt.Errorf("service response route is not the required %q", m5RequiredRoute)
	}
	if response.Proof.ReadTerm == 0 || response.Proof.ReadIndex == 0 ||
		response.Proof.AppliedTerm == 0 || response.Proof.AppliedIndex < response.Proof.ReadIndex {
		return errors.New("service response lacks a non-zero read-index/apply proof")
	}
	return nil
}

func durationSum(samples []uint64) time.Duration {
	var total uint64
	for _, sample := range samples {
		total += sample
	}
	return time.Duration(total)
}
