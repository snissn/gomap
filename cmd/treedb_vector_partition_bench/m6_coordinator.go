package main

// M6 evidence deliberately uses the real persisted M4 router and production
// M6 coordinator with an in-process dispatcher that speaks the M5 request /
// response contract. It is local-service simulation, not production network,
// Raft read-proof, or M8 acceptance evidence.

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

const (
	m6CoordinatorStageV1            = "distributed_simulation_or_cluster"
	m6CoordinatorAttributionStageV1 = "distributed_coordinator_local_service"
	m6CoordinatorEvidenceKindV1     = "m6_local_service_simulation_v1"
	m6CoordinatorResultKindV1       = "coordinator_local_service_simulation"
	m6CoordinatorMeasurementV1      = "coordinator_local_service_simulation_not_production"
	m6CoordinatorMaxGroupsV1        = 64
)

type m6LatencySummaryV1 struct {
	P50Nanos uint64 `json:"p50_nanos"`
	P95Nanos uint64 `json:"p95_nanos"`
	P99Nanos uint64 `json:"p99_nanos"`
}

type m6CoordinatorCounterEvidenceV1 struct {
	SelectedPartitions uint64 `json:"selected_partitions"`
	SelectedGroups     uint64 `json:"selected_groups"`
	Requests           uint64 `json:"requests"`
	RPCs               uint64 `json:"rpcs"`
	Retries            uint64 `json:"retries"`
	Redirects          uint64 `json:"redirects"`
	Cancellations      uint64 `json:"cancellations"`
	Failures           uint64 `json:"failures"`
	QueryBytes         uint64 `json:"query_bytes"`
	RequestBytes       uint64 `json:"request_bytes"`
	ResponseBytes      uint64 `json:"response_bytes"`
	CandidateBytes     uint64 `json:"candidate_bytes"`
	Candidates         uint64 `json:"candidates"`
	Edges              uint64 `json:"edges"`
	MergeEntries       uint64 `json:"merge_entries"`
	Duplicates         uint64 `json:"duplicates"`
	ScoreDisagreements uint64 `json:"score_disagreements"`
}

type m6CoordinatorEvidenceV1 struct {
	EvidenceKind       string `json:"evidence_kind"`
	ProductionEvidence bool   `json:"production_evidence"`
	Transport          string `json:"transport"`
	ReadProof          string `json:"read_proof"`
	Network            string `json:"network"`
	Consistency        string `json:"consistency"`

	Queries            int  `json:"queries"`
	Probes             int  `json:"probes"`
	ShardSamples       int  `json:"shard_samples"`
	SourceHNSWDegree   int  `json:"source_hnsw_degree"`
	ExactParityChecked bool `json:"exact_all_partition_parity_checked"`
	ExactParityPassed  bool `json:"exact_all_partition_parity_passed"`

	RecallAtK           float64 `json:"recall_at_k"`
	RecallAt1           float64 `json:"recall_at_1"`
	RecallAt10          float64 `json:"recall_at_10"`
	RecallAt100         float64 `json:"recall_at_100"`
	RecallAt10Measured  bool    `json:"recall_at_10_measured"`
	RecallAt100Measured bool    `json:"recall_at_100_measured"`
	QPS                 float64 `json:"qps"`
	BytesPerOp          float64 `json:"bytes_per_op"`
	AllocsPerOp         float64 `json:"allocs_per_op"`
	PeakRSSBytes        int64   `json:"peak_rss_bytes"`
	PeakRSSMeasured     bool    `json:"peak_rss_measured"`
	MappedBytes         uint64  `json:"mapped_bytes"`

	RouterOpen    m6LatencySummaryV1 `json:"router_open"`
	RouterSearch  m6LatencySummaryV1 `json:"router_search"`
	Placement     m6LatencySummaryV1 `json:"placement"`
	Queue         m6LatencySummaryV1 `json:"queue"`
	RPC           m6LatencySummaryV1 `json:"rpc"`
	NetworkTiming m6LatencySummaryV1 `json:"network_timing"`
	ReadProofTime m6LatencySummaryV1 `json:"read_proof_timing"`
	ShardSearch   m6LatencySummaryV1 `json:"shard_search"`
	Response      m6LatencySummaryV1 `json:"response"`
	Dedupe        m6LatencySummaryV1 `json:"dedupe"`
	Merge         m6LatencySummaryV1 `json:"merge"`
	EndToEnd      m6LatencySummaryV1 `json:"end_to_end"`

	Counters                 m6CoordinatorCounterEvidenceV1 `json:"counters"`
	CoordinatorOverheadRatio float64                        `json:"placement_dedupe_merge_ratio"`
	Experimental             bool                           `json:"experimental"`
	ExperimentalReason       string                         `json:"experimental_reason"`
}

type m6CoordinatorHarnessV1 struct {
	coordinator      *nativewire.VectorPartitionCoordinatorV1
	dispatcher       *m6LocalShardDispatcherV1
	routerCandidates int
	modelDigest      string
	readySetDigest   string
	collection       string
	indexName        string
	indexDigest      string
}

type m6LocalShardDispatcherV1 struct {
	vectors          [][]float64
	vectorNorms      []float32
	partitions       int
	groupByPartition []string
	nodeByGroup      map[string]string
	timingMu         sync.Mutex
	durations        []uint64
}

func (h *m6CoordinatorHarnessV1) Close() error {
	if h == nil || h.coordinator == nil {
		return nil
	}
	return h.coordinator.Close()
}

func newM6CoordinatorHarnessV1(router *treeDBRepresentativeRouter, vectors [][]float64, partitions int) (*m6CoordinatorHarnessV1, error) {
	if router == nil || router.router == nil || router.collection == nil {
		return nil, errors.New("persisted M4 router is unavailable")
	}
	if len(vectors) < 1 || len(vectors[0]) < 1 {
		return nil, errors.New("M6 local-service evidence requires nonempty vectors")
	}
	dimensions := len(vectors[0])
	vectorNorms, err := m6VectorNormsV1(vectors, dimensions)
	if err != nil {
		return nil, err
	}
	if partitions < 1 || partitions > m6CoordinatorMaxGroupsV1 {
		return nil, fmt.Errorf("M6 local-service evidence requires 1..%d one-owner groups", m6CoordinatorMaxGroupsV1)
	}
	status := router.router.Status()
	if status.Manifest.PartitionCount != uint32(partitions) ||
		len(status.Manifest.Placements) != partitions ||
		status.Representatives < 1 || status.Representatives > 1_000_000 {
		return nil, errors.New("persisted M4 status is outside M6 evidence bounds")
	}
	topology := nativewire.VectorPartitionCoordinatorTopologyV1{
		Database: "default", Catalog: "default", Collection: status.Manifest.Collection,
		IndexName: status.Manifest.IndexName, IndexDefinitionDigest: status.Manifest.IndexDefinitionDigest,
		SourceGeneration: status.Manifest.SourceGeneration, SourceChecksum: status.Manifest.SourceChecksum,
		SourceSchemaHash: status.Manifest.SourceSchemaHash, SourceRowCount: status.Manifest.SourceRowCount,
		PartitionGeneration: status.Manifest.Generation,
		Partitions:          make([]nativewire.VectorPartitionCoordinatorTopologyPartitionV1, partitions),
	}
	groupByPartition := make([]string, partitions)
	nodeByGroup := make(map[string]string, partitions)
	for i, placement := range status.Manifest.Placements {
		if placement.PartitionID != uint32(i) || placement.GroupID == "" {
			return nil, errors.New("persisted M4 placement is not canonical")
		}
		nodeID := "local-node-" + placement.GroupID
		topology.Groups = append(topology.Groups, nativewire.VectorPartitionCoordinatorTopologyGroupV1{
			ID: placement.GroupID, LeaderHint: nodeID, Members: []string{nodeID},
		})
		topology.Partitions[i] = nativewire.VectorPartitionCoordinatorTopologyPartitionV1{
			PartitionID: placement.PartitionID, GroupID: placement.GroupID,
		}
		groupByPartition[i] = placement.GroupID
		nodeByGroup[placement.GroupID] = nodeID
	}
	topology.CollectionGroupID = topology.Groups[0].ID
	dispatcher := &m6LocalShardDispatcherV1{
		vectors: vectors, vectorNorms: vectorNorms, partitions: partitions,
		groupByPartition: groupByPartition, nodeByGroup: nodeByGroup,
	}
	coordinator, err := nativewire.NewVectorPartitionCoordinatorForTopologyV1(
		topology,
		nativewire.CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: router.collection},
		dispatcher,
		nativewire.VectorPartitionCoordinatorLimitsV1{},
	)
	if err != nil {
		return nil, err
	}
	return &m6CoordinatorHarnessV1{
		coordinator: coordinator, dispatcher: dispatcher, collection: status.Manifest.Collection,
		indexName: status.Manifest.IndexName, indexDigest: status.Manifest.IndexDefinitionDigest,
		routerCandidates: int(status.Representatives),
		modelDigest:      status.ModelDigest, readySetDigest: status.Manifest.ReadySetDigest,
	}, nil
}

func (h *m6CoordinatorHarnessV1) search(ctx context.Context, query []float64, probes, topK, queryIndex int) (nativewire.VectorPartitionCoordinatorResponseV1, error) {
	if h == nil || h.coordinator == nil || probes < 1 || probes > m6CoordinatorMaxGroupsV1 ||
		topK < 1 || topK > nativewire.DefaultVectorPartitionShardSearchLimitsV1().MaxTopK {
		return nativewire.VectorPartitionCoordinatorResponseV1{}, errors.New("M6 local-service query is outside bounded limits")
	}
	query32 := make([]float32, len(query))
	for i, value := range query {
		query32[i] = float32(value)
	}
	return h.coordinator.Search(ctx, nativewire.VectorPartitionCoordinatorRequestV1{
		Version:        nativewire.VectorPartitionCoordinatorVersionV1,
		RequestID:      fmt.Sprintf("m6-query-%06d-probes-%06d", queryIndex, probes),
		CancellationID: fmt.Sprintf("m6-cancel-%06d-probes-%06d", queryIndex, probes),
		Database:       "default", Catalog: "default", Collection: h.collection,
		IndexName: h.indexName, IndexDefinitionDigest: h.indexDigest,
		Query: query32, Metric: nativewire.VectorPartitionShardSearchMetricCosineV1,
		RouterMode:            collections.VectorPartitionRouterModeExactV1,
		RouterCandidateBudget: h.routerCandidates, PartitionProbes: probes,
		Consistency: nativewire.VectorPartitionShardSearchConsistencySnapshotV1,
		StatsMode:   nativewire.VectorPartitionShardSearchStatsBasicV1,
		TopK:        topK, EfSearch: max(topK, 128),
		DeadlineUnixNano:  time.Now().Add(30 * time.Second).UnixNano(),
		RequestBytesLimit: 4 << 20, CandidateBytesLimit: 64 << 20,
		ResponseBytesLimit: 64 << 20, MergeEntriesLimit: probes * topK,
	})
}

func (d *m6LocalShardDispatcherV1) DispatchVectorPartitionShardSearchV1(ctx context.Context, request nativewire.VectorPartitionShardSearchRequestV1) (nativewire.VectorPartitionShardSearchResponseV1, error) {
	started := time.Now()
	if err := ctx.Err(); err != nil {
		return nativewire.VectorPartitionShardSearchResponseV1{}, err
	}
	groupID := string(request.TargetGroupID)
	if d == nil || request.Version != nativewire.VectorPartitionShardSearchVersionV1 ||
		groupID == "" || string(request.TargetNodeID) != d.nodeByGroup[groupID] ||
		len(request.PartitionIDs) < 1 || len(request.Query) != len(d.vectors[0]) {
		return nativewire.VectorPartitionShardSearchResponseV1{}, errors.New("invalid local M5 dispatch")
	}
	queryNorm, err := m6QueryNormV1(request.Query)
	if err != nil {
		return nativewire.VectorPartitionShardSearchResponseV1{}, err
	}
	partials := make([]nativewire.VectorPartitionShardSearchPartialV1, len(request.PartitionIDs))
	var candidates, searchNanos uint64
	for i, partitionID := range request.PartitionIDs {
		if int(partitionID) >= d.partitions || d.groupByPartition[partitionID] != groupID {
			return nativewire.VectorPartitionShardSearchResponseV1{}, errors.New("local M5 route mismatch")
		}
		searchStarted := time.Now()
		neighbors, visited, err := d.partitionTopKV1(ctx, request.Query, queryNorm, partitionID, request.TopK)
		searchNanos += elapsedM6NanosV1(searchStarted)
		if err != nil {
			return nativewire.VectorPartitionShardSearchResponseV1{}, err
		}
		candidates += visited
		partials[i] = nativewire.VectorPartitionShardSearchPartialV1{
			PartitionID: partitionID, Neighbors: neighbors, Candidates: visited,
			SearchRoute: collections.VectorPartitionSearchRouteExactFP32ScanV1,
		}
	}
	candidateBytes := candidates * 64
	if candidateBytes > request.CandidateBytesLimit {
		return nativewire.VectorPartitionShardSearchResponseV1{}, errors.New("local M5 candidate budget exceeded")
	}
	responseStarted := time.Now()
	responseBytes, err := nativewire.MeasureVectorPartitionShardSearchResponseBytesV1(partials)
	if err != nil || responseBytes > request.ResponseBytesLimit {
		return nativewire.VectorPartitionShardSearchResponseV1{}, errors.New("local M5 response budget exceeded")
	}
	responseNanos := elapsedM6NanosV1(responseStarted)
	totalNanos := elapsedM6NanosV1(started)
	d.timingMu.Lock()
	d.durations = append(d.durations, totalNanos)
	d.timingMu.Unlock()
	return nativewire.VectorPartitionShardSearchResponseV1{
		Version: nativewire.VectorPartitionShardSearchVersionV1, RequestID: request.RequestID,
		Proof: nativewire.VectorPartitionShardSearchProofV1{
			ServingNode: request.TargetNodeID, LeaderNode: request.TargetNodeID, GroupID: request.TargetGroupID,
			ReadySetDigest: request.ReadySetDigest,
			ReadTerm:       1, ReadIndex: 1, AppliedTerm: 1, AppliedIndex: 1,
			SourceGeneration: request.SourceGeneration, SourceChecksum: request.SourceChecksum,
			SourceSchemaHash: request.SourceSchemaHash, SourceRowCount: request.SourceRowCount,
			PartitionGeneration: request.PartitionGeneration, RouterGeneration: request.RouterGeneration,
		},
		Partials: partials, Partitions: uint64(len(partials)),
		Candidates: candidates, ResponseBytes: responseBytes,
		Timing: nativewire.VectorPartitionShardSearchTimingV1{
			SearchNanos: searchNanos, ResponseCopyNanos: responseNanos, TotalNanos: totalNanos,
		},
	}, nil
}

func (d *m6LocalShardDispatcherV1) timingCursorV1() int {
	d.timingMu.Lock()
	defer d.timingMu.Unlock()
	return len(d.durations)
}

func (d *m6LocalShardDispatcherV1) durationsSinceV1(cursor int) ([]uint64, error) {
	d.timingMu.Lock()
	defer d.timingMu.Unlock()
	if cursor < 0 || cursor > len(d.durations) {
		return nil, errors.New("invalid local M5 timing cursor")
	}
	return append([]uint64(nil), d.durations[cursor:]...), nil
}

type m6LocalCandidateV1 struct {
	ID    string
	Score float32
}

type m6LocalWorstHeapV1 []m6LocalCandidateV1

func (h m6LocalWorstHeapV1) Len() int      { return len(h) }
func (h m6LocalWorstHeapV1) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h m6LocalWorstHeapV1) Less(i, j int) bool {
	return m6LocalCandidateWorseV1(h[i], h[j])
}
func (h *m6LocalWorstHeapV1) Push(value any) {
	*h = append(*h, value.(m6LocalCandidateV1))
}
func (h *m6LocalWorstHeapV1) Pop() any {
	old := *h
	value := old[len(old)-1]
	*h = old[:len(old)-1]
	return value
}

func m6LocalCandidateBetterV1(left, right m6LocalCandidateV1) bool {
	return left.Score > right.Score || left.Score == right.Score && left.ID < right.ID
}

func m6LocalCandidateWorseV1(left, right m6LocalCandidateV1) bool {
	return left.Score < right.Score || left.Score == right.Score && left.ID > right.ID
}

func (d *m6LocalShardDispatcherV1) partitionTopKV1(ctx context.Context, query []float32, queryNorm float64, partitionID uint32, topK int) ([]nativewire.VectorPartitionShardSearchNeighborV1, uint64, error) {
	h := make(m6LocalWorstHeapV1, 0, topK)
	heap.Init(&h)
	var visited uint64
	for ordinal, values := range d.vectors {
		if ordinal%d.partitions != int(partitionID) {
			continue
		}
		if visited&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}
		}
		visited++
		candidate := m6LocalCandidateV1{
			ID:    fmt.Sprintf("doc-%06d", ordinal),
			Score: m6CosineScoreV1(values, query, queryNorm, d.vectorNorms[ordinal]),
		}
		if len(h) < topK {
			heap.Push(&h, candidate)
		} else if m6LocalCandidateBetterV1(candidate, h[0]) {
			h[0] = candidate
			heap.Fix(&h, 0)
		}
	}
	sort.Slice(h, func(i, j int) bool { return m6LocalCandidateBetterV1(h[i], h[j]) })
	out := make([]nativewire.VectorPartitionShardSearchNeighborV1, len(h))
	for i, candidate := range h {
		out[i] = nativewire.VectorPartitionShardSearchNeighborV1{ID: candidate.ID, Score: candidate.Score}
	}
	return out, visited, nil
}

func m6QueryNormV1(query []float32) (float64, error) {
	var norm float64
	for _, value := range query {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return 0, errors.New("nonfinite local M5 query")
		}
		norm += float64(value) * float64(value)
	}
	if norm == 0 {
		return 0, errors.New("zero local M5 query")
	}
	return math.Sqrt(norm), nil
}

func m6VectorNormsV1(vectors [][]float64, dimensions int) ([]float32, error) {
	norms := make([]float32, len(vectors))
	for ordinal, vector := range vectors {
		if len(vector) != dimensions {
			return nil, errors.New("M6 local-service vectors have mixed dimensions")
		}
		var squared float64
		for _, value := range vector {
			value32 := float32(value)
			if math.IsNaN(float64(value32)) || math.IsInf(float64(value32), 0) {
				return nil, errors.New("M6 local-service vectors contain a nonfinite value")
			}
			squared += float64(value32) * float64(value32)
		}
		norms[ordinal] = float32(math.Sqrt(squared))
		if norms[ordinal] == 0 || math.IsInf(float64(norms[ordinal]), 0) {
			return nil, errors.New("M6 local-service vectors contain an invalid norm")
		}
	}
	return norms, nil
}

func m6CosineScoreV1(vector []float64, query []float32, queryNorm float64, vectorNorm float32) float32 {
	var dot float64
	for dimension, value := range vector {
		dot += float64(float32(value)) * float64(query[dimension])
	}
	return float32(dot / (queryNorm * float64(vectorNorm)))
}

type m6EvidenceAccumulatorV1 struct {
	routerOpen, routerSearch, placement, queue, rpc, network []uint64
	readProof, shardSearch, response, dedupe, merge, total   []uint64
	counters                                                 m6CoordinatorCounterEvidenceV1
	allocBytes, allocs                                       uint64
	recallK, recall1, recall10, recall100                    float64
}

func (a *m6EvidenceAccumulatorV1) add(
	response nativewire.VectorPartitionCoordinatorResponseV1,
	shardDurations []uint64,
	allocBytes, allocs uint64,
	recallK, recall1, recall10, recall100 float64,
) {
	timing := response.Timing
	a.routerOpen = append(a.routerOpen, timing.RouterOpenNanos)
	a.routerSearch = append(a.routerSearch, timing.RouterSearchNanos)
	a.placement = append(a.placement, timing.PlacementNanos)
	a.queue = append(a.queue, timing.QueueNanos)
	a.rpc = append(a.rpc, timing.RPCNanos)
	a.network = append(a.network, timing.NetworkNanos)
	a.readProof = append(a.readProof, timing.ReadIndexApplyNanos)
	a.shardSearch = append(a.shardSearch, shardDurations...)
	a.response = append(a.response, timing.ResponseNanos)
	a.dedupe = append(a.dedupe, timing.DedupeNanos)
	a.merge = append(a.merge, timing.MergeNanos)
	a.total = append(a.total, timing.TotalNanos)
	c := response.Counters
	a.counters.SelectedPartitions += c.SelectedPartitions
	a.counters.SelectedGroups += c.SelectedGroups
	a.counters.Requests += c.Requests
	a.counters.RPCs += c.RPCs
	a.counters.Retries += c.Retries
	a.counters.Redirects += c.Redirects
	a.counters.Cancellations += c.Cancellations
	a.counters.Failures += c.Failures
	a.counters.QueryBytes += c.QueryBytes
	a.counters.RequestBytes += c.RequestBytes
	a.counters.ResponseBytes += c.ResponseBytes
	a.counters.CandidateBytes += c.CandidateBytes
	a.counters.Candidates += c.Candidates
	a.counters.Edges += c.Edges
	a.counters.MergeEntries += c.MergeEntries
	a.counters.Duplicates += c.Duplicates
	a.counters.ScoreDisagreements += c.ScoreDisagreements
	a.allocBytes += allocBytes
	a.allocs += allocs
	a.recallK += recallK
	a.recall1 += recall1
	a.recall10 += recall10
	a.recall100 += recall100
}

func (a m6EvidenceAccumulatorV1) evidence(cfg config, queries int, probes int, parityChecked, parityPassed bool) m6CoordinatorEvidenceV1 {
	n := float64(queries)
	var totalNanos, overheadNanos uint64
	for i := range a.total {
		totalNanos += a.total[i]
		overheadNanos += a.placement[i] + a.dedupe[i] + a.merge[i]
	}
	evidence := m6CoordinatorEvidenceV1{
		EvidenceKind: m6CoordinatorEvidenceKindV1, ProductionEvidence: false,
		Transport:   "in_process_transport_neutral_m5_contract_simulation",
		ReadProof:   "synthetic_local_proof_not_measured",
		Network:     "in_process_no_production_network",
		Consistency: string(nativewire.VectorPartitionShardSearchConsistencySnapshotV1),
		Queries:     queries, Probes: probes, ShardSamples: len(a.shardSearch),
		SourceHNSWDegree:   cfg.router.sourceHNSWDegree,
		ExactParityChecked: parityChecked, ExactParityPassed: parityPassed,
		RecallAtK: a.recallK / n, RecallAt1: a.recall1 / n,
		RecallAt10: a.recall10 / n, RecallAt100: a.recall100 / n,
		RecallAt10Measured: cfg.topK >= 10, RecallAt100Measured: cfg.topK >= 100,
		BytesPerOp: float64(a.allocBytes) / n, AllocsPerOp: float64(a.allocs) / n,
		MappedBytes: cfg.router.open.MappedBytes,
		RouterOpen:  latencySummaryM6V1(a.routerOpen), RouterSearch: latencySummaryM6V1(a.routerSearch),
		Placement: latencySummaryM6V1(a.placement), Queue: latencySummaryM6V1(a.queue),
		RPC: latencySummaryM6V1(a.rpc), NetworkTiming: latencySummaryM6V1(a.network),
		ReadProofTime: latencySummaryM6V1(a.readProof), ShardSearch: latencySummaryM6V1(a.shardSearch),
		Response: latencySummaryM6V1(a.response), Dedupe: latencySummaryM6V1(a.dedupe),
		Merge: latencySummaryM6V1(a.merge), EndToEnd: latencySummaryM6V1(a.total),
		Counters:           a.counters,
		Experimental:       true,
		ExperimentalReason: "local-service simulation is not matched 1M/16 production M5 network and Raft evidence",
	}
	if totalNanos > 0 {
		evidence.QPS = n / (float64(totalNanos) / float64(time.Second))
		evidence.CoordinatorOverheadRatio = float64(overheadNanos) / float64(totalNanos)
	}
	if peakRSS, available := vectorPartitionBenchmarkPeakRSS(); available {
		evidence.PeakRSSBytes = peakRSS
		evidence.PeakRSSMeasured = true
	}
	return evidence
}

func latencySummaryM6V1(samples []uint64) m6LatencySummaryV1 {
	return m6LatencySummaryV1{
		P50Nanos: routerPercentileNanos(samples, 50),
		P95Nanos: routerPercentileNanos(samples, 95),
		P99Nanos: routerPercentileNanos(samples, 99),
	}
}

func simulateM6CoordinatorV1(cfg config, m fixtureManifest, vectors, queries [][]float64, probes int, overlap float64) (runResult, error) {
	if cfg.coordinator == nil || cfg.router == nil {
		return runResult{}, errors.New("M6 coordinator stage is not initialized")
	}
	if overlap != 0 {
		return runResult{}, errors.New("M6 local-service evidence currently declares only the disjoint persisted M4 fixture")
	}
	var accumulator m6EvidenceAccumulatorV1
	parityChecked := probes == cfg.partitions
	parityPassed := parityChecked
	for queryIndex, query := range queries {
		truth := exactTopK(vectors, query, cfg.topK)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		timingCursor := cfg.coordinator.dispatcher.timingCursorV1()
		response, err := cfg.coordinator.search(context.Background(), query, probes, cfg.topK, queryIndex)
		runtime.ReadMemStats(&after)
		if err != nil {
			return runResult{}, err
		}
		shardDurations, err := cfg.coordinator.dispatcher.durationsSinceV1(timingCursor)
		if err != nil {
			return runResult{}, err
		}
		if response.RouterModelDigest != cfg.coordinator.modelDigest ||
			response.ReadySetDigest != cfg.coordinator.readySetDigest {
			return runResult{}, errors.New("M6 response generation digest drift")
		}
		got := make([]neighbor, len(response.Neighbors))
		for i, result := range response.Neighbors {
			got[i] = neighbor{ID: result.ID, Distance: 1 - float64(result.Score)}
		}
		recallK := recall(truth, got)
		recall1 := recallPrefixM6V1(truth, got, 1)
		recall10 := 0.0
		if cfg.topK >= 10 {
			recall10 = recallPrefixM6V1(truth, got, 10)
		}
		recall100 := 0.0
		if cfg.topK >= 100 {
			recall100 = recallPrefixM6V1(truth, got, 100)
		}
		if parityChecked {
			query32 := make([]float32, len(query))
			for i, value := range query {
				query32[i] = float32(value)
			}
			oracle, err := cfg.coordinator.dispatcher.globalTopKV1(context.Background(), query32, cfg.topK)
			if err != nil {
				return runResult{}, err
			}
			if !equalM6CoordinatorNeighborsV1(response.Neighbors, oracle) {
				parityPassed = false
			}
		}
		accumulator.add(
			response, shardDurations,
			after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs,
			recallK, recall1, recall10, recall100,
		)
	}
	if parityChecked && !parityPassed {
		return runResult{}, errors.New("M6 all-partition exact local-service parity failure")
	}
	evidence := accumulator.evidence(cfg, len(queries), probes, parityChecked, parityPassed)
	stage := stageResult{
		Name:    m6CoordinatorAttributionStageV1,
		Method:  "persisted_m4_m6_in_process_m5_contract_simulation_v1",
		Enabled: true, Lossy: probes < cfg.partitions,
		RecallAtK: evidence.RecallAtK, Queries: len(queries), Probes: probes, Available: true,
		RouteKind: "local_service_simulation",
		Searches:  uint64(len(queries)), Candidates: evidence.Counters.Candidates,
		Edges:       evidence.Counters.Edges,
		SearchNanos: sumM6V1(accumulator.total),
		P50Nanos:    evidence.EndToEnd.P50Nanos, P95Nanos: evidence.EndToEnd.P95Nanos,
		P99Nanos:   evidence.EndToEnd.P99Nanos,
		BytesPerOp: evidence.BytesPerOp, AllocsPerOp: evidence.AllocsPerOp,
	}
	metrics := metricsV1{
		MeasurementStatus: m6CoordinatorMeasurementV1,
		BuildWallNanos:    int64(cfg.router.build.BuildNanos),
		BuildCPUNanos:     cfg.router.buildCPU, BuildCPUAvailable: cfg.router.buildCPUOK,
		PeakRSSBytes: evidence.PeakRSSBytes, PeakRSSAvailable: evidence.PeakRSSMeasured,
		TemporaryBytes: int64(cfg.router.build.RouterBytes), FinalBytes: int64(cfg.router.build.RouterBytes),
		BytesPerVector: float64(cfg.router.build.RouterBytes) / float64(len(vectors)),
		Balance:        1, MaxPartitionSize: (len(vectors) + cfg.partitions - 1) / cfg.partitions,
		ReplicationFactor: 1, RepresentativeCount: int(cfg.router.open.Representatives),
		SourceHNSWDegree:   cfg.router.sourceHNSWDegree,
		LloydIterations:    int(cfg.router.build.LloydIterations),
		EmptyRepairs:       int(cfg.router.build.EmptyRepairs),
		MinRepresentatives: int(cfg.router.build.MinRepresentativesPerPartition),
		MaxRepresentatives: int(cfg.router.build.MaxRepresentativesPerPartition),
		RouterBytes:        int64(cfg.router.build.RouterBytes),
		MappedBytes:        int64(cfg.router.open.MappedBytes),
		HeapCopyBytes:      int64(cfg.router.open.HeapCopyBytes),
		SelectedPartitions: probes,
		SelectedGroups:     int(evidence.Counters.SelectedGroups / uint64(len(queries))),
		RPCs:               int(evidence.Counters.RPCs / uint64(len(queries))),
		RequestBytes:       int64(evidence.Counters.RequestBytes / uint64(len(queries))),
		ResponseBytes:      int64(evidence.Counters.ResponseBytes / uint64(len(queries))),
		RoutingLatencyNanos: int64(
			sumM6V1(accumulator.routerSearch) / uint64(len(queries)),
		),
		ShardP50Nanos: int64(evidence.ShardSearch.P50Nanos),
		ShardP95Nanos: int64(evidence.ShardSearch.P95Nanos),
		ShardP99Nanos: int64(evidence.ShardSearch.P99Nanos),
		MergeDedupeNanos: int64(
			(sumM6V1(accumulator.dedupe) + sumM6V1(accumulator.merge)) / uint64(len(queries)),
		),
		Cancellations: int64(evidence.Counters.Cancellations), Failures: int64(evidence.Counters.Failures),
		QPS: evidence.QPS, P50Nanos: int64(evidence.EndToEnd.P50Nanos),
		P95Nanos: int64(evidence.EndToEnd.P95Nanos), P99Nanos: int64(evidence.EndToEnd.P99Nanos),
		RecallAt1: evidence.RecallAt1, RecallAt10: evidence.RecallAt10,
		RecallAt100: evidence.RecallAt100, BytesPerOp: evidence.BytesPerOp,
		AllocsPerOp: evidence.AllocsPerOp,
	}
	goMaxProcs, goMemoryLimitBytes := benchmarkRuntimeLimits()
	return runResult{
		SchemaVersion: schemaVersion, ResultKind: m6CoordinatorResultKindV1,
		ProductionEvidence: false, Command: cfg.command, BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA,
		GoVersion: runtime.Version(), Hardware: runtime.GOARCH + "/" + runtime.GOOS,
		GOMAXPROCS: goMaxProcs, GoMemoryLimitBytes: goMemoryLimitBytes,
		Dataset: m, Partitions: cfg.partitions, Overlap: overlap, Probes: probes, TopK: cfg.topK,
		RecallTarget: cfg.recallTarget, Seed: cfg.seed,
		MemoryBudgetBytes: cfg.maxBytes, ModeledPeakBytes: cfg.memory.ModeledPeakBytes,
		MemoryBudgetScope: memoryBudgetScope, Samples: len(queries),
		TimedBoundary: "persisted local M4 open/search plus production M6 planning/fanout/dedupe/merge and in-process simulated M5 contract; excludes production transport, Raft read proof, remote service, and M8 acceptance",
		Stages:        []stageResult{stage}, Metrics: metrics, Coordinator: &evidence,
	}, nil
}

func (d *m6LocalShardDispatcherV1) globalTopKV1(ctx context.Context, query []float32, topK int) ([]nativewire.VectorPartitionShardSearchNeighborV1, error) {
	queryNorm, err := m6QueryNormV1(query)
	if err != nil {
		return nil, err
	}
	h := make(m6LocalWorstHeapV1, 0, topK)
	heap.Init(&h)
	for ordinal, values := range d.vectors {
		if ordinal&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		candidate := m6LocalCandidateV1{
			ID:    fmt.Sprintf("doc-%06d", ordinal),
			Score: m6CosineScoreV1(values, query, queryNorm, d.vectorNorms[ordinal]),
		}
		if len(h) < topK {
			heap.Push(&h, candidate)
		} else if m6LocalCandidateBetterV1(candidate, h[0]) {
			h[0] = candidate
			heap.Fix(&h, 0)
		}
	}
	sort.Slice(h, func(i, j int) bool { return m6LocalCandidateBetterV1(h[i], h[j]) })
	out := make([]nativewire.VectorPartitionShardSearchNeighborV1, len(h))
	for i, candidate := range h {
		out[i] = nativewire.VectorPartitionShardSearchNeighborV1{ID: candidate.ID, Score: candidate.Score}
	}
	return out, nil
}

func equalM6CoordinatorNeighborsV1(
	got []nativewire.VectorPartitionCoordinatorNeighborV1,
	want []nativewire.VectorPartitionShardSearchNeighborV1,
) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].ID != want[i].ID ||
			math.Float32bits(got[i].Score) != math.Float32bits(want[i].Score) {
			return false
		}
	}
	return true
}

func recallPrefixM6V1(want, got []neighbor, k int) float64 {
	k = min(k, len(want))
	if k == 0 {
		return 1
	}
	return recall(want[:k], got[:min(k, len(got))])
}

func sumM6V1(values []uint64) uint64 {
	var total uint64
	for _, value := range values {
		total += value
	}
	return total
}

func elapsedM6NanosV1(started time.Time) uint64 {
	nanos := time.Since(started).Nanoseconds()
	if nanos < 1 {
		return 1
	}
	return uint64(nanos)
}

func validateM6CoordinatorEvidenceV1(r runResult) error {
	evidence := r.Coordinator
	if evidence == nil ||
		evidence.EvidenceKind != m6CoordinatorEvidenceKindV1 ||
		evidence.ProductionEvidence ||
		evidence.Transport != "in_process_transport_neutral_m5_contract_simulation" ||
		evidence.ReadProof != "synthetic_local_proof_not_measured" ||
		evidence.Network != "in_process_no_production_network" ||
		evidence.Consistency != string(nativewire.VectorPartitionShardSearchConsistencySnapshotV1) ||
		evidence.Queries != r.Samples || evidence.Queries < 1 ||
		evidence.Probes != r.Probes || evidence.Probes < 1 ||
		evidence.SourceHNSWDegree < 1 ||
		evidence.SourceHNSWDegree > maxSourceHNSWDegree ||
		evidence.SourceHNSWDegree != r.Metrics.SourceHNSWDegree ||
		evidence.ShardSamples != evidence.Queries*evidence.Probes {
		return errors.New("invalid M6 local-service evidence identity or labeling")
	}
	parityExpected := r.Probes == r.Partitions
	if evidence.ExactParityChecked != parityExpected ||
		evidence.ExactParityPassed != parityExpected {
		return errors.New("invalid M6 exact all-partition parity evidence")
	}
	for _, value := range []float64{
		evidence.RecallAtK, evidence.RecallAt1, evidence.RecallAt10, evidence.RecallAt100,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
			return errors.New("invalid M6 recall evidence")
		}
	}
	for _, value := range []float64{
		evidence.QPS, evidence.BytesPerOp, evidence.AllocsPerOp, evidence.CoordinatorOverheadRatio,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return errors.New("invalid M6 performance evidence")
		}
	}
	if evidence.QPS == 0 || evidence.CoordinatorOverheadRatio > 1 ||
		evidence.MappedBytes == 0 ||
		evidence.PeakRSSMeasured && evidence.PeakRSSBytes <= 0 ||
		!evidence.Experimental || evidence.ExperimentalReason == "" {
		return errors.New("incomplete M6 bounded experimental evidence")
	}
	for name, latency := range map[string]m6LatencySummaryV1{
		"router_open": evidence.RouterOpen, "router_search": evidence.RouterSearch,
		"placement": evidence.Placement, "rpc": evidence.RPC,
		"shard_search": evidence.ShardSearch, "response": evidence.Response,
		"dedupe": evidence.Dedupe, "merge": evidence.Merge, "end_to_end": evidence.EndToEnd,
	} {
		if latency.P50Nanos == 0 ||
			latency.P50Nanos > latency.P95Nanos ||
			latency.P95Nanos > latency.P99Nanos {
			return fmt.Errorf("invalid M6 %s latency evidence: %+v", name, latency)
		}
	}
	if evidence.ReadProofTime != (m6LatencySummaryV1{}) {
		return errors.New("synthetic local M6 read proof must not be reported as measured")
	}
	expectedFanout := uint64(evidence.Queries * evidence.Probes)
	counters := evidence.Counters
	if counters.SelectedPartitions != expectedFanout ||
		counters.SelectedGroups != expectedFanout ||
		counters.Requests != expectedFanout ||
		counters.RPCs != expectedFanout ||
		counters.Retries != 0 || counters.Redirects != 0 ||
		counters.Cancellations != 0 || counters.Failures != 0 ||
		counters.QueryBytes == 0 || counters.RequestBytes == 0 ||
		counters.ResponseBytes == 0 ||
		counters.CandidateBytes != counters.Candidates*64 ||
		counters.Candidates == 0 ||
		counters.Candidates > uint64(evidence.Queries*r.Dataset.Vectors) ||
		counters.MergeEntries == 0 ||
		counters.MergeEntries > uint64(evidence.Queries*evidence.Probes*r.TopK) {
		return fmt.Errorf("invalid M6 counter evidence: %+v", counters)
	}
	if len(r.Stages) != 1 {
		return errors.New("M6 result must contain one isolated attribution stage")
	}
	stage := r.Stages[0]
	if stage.Name != m6CoordinatorAttributionStageV1 ||
		stage.Method != "persisted_m4_m6_in_process_m5_contract_simulation_v1" ||
		stage.RouteKind != "local_service_simulation" ||
		stage.Searches != uint64(evidence.Queries) ||
		stage.P50Nanos != evidence.EndToEnd.P50Nanos ||
		stage.P95Nanos != evidence.EndToEnd.P95Nanos ||
		stage.P99Nanos != evidence.EndToEnd.P99Nanos {
		return errors.New("invalid M6 isolated stage evidence")
	}
	metrics := r.Metrics
	if metrics.MeasurementStatus != m6CoordinatorMeasurementV1 ||
		metrics.SelectedPartitions != r.Probes ||
		metrics.SelectedGroups != r.Probes ||
		metrics.RPCs != r.Probes ||
		metrics.RoutingLatencyNanos <= 0 ||
		metrics.ShardP50Nanos <= 0 ||
		metrics.ShardP50Nanos > metrics.ShardP95Nanos ||
		metrics.ShardP95Nanos > metrics.ShardP99Nanos ||
		metrics.Cancellations != 0 || metrics.Timeouts != 0 || metrics.Failures != 0 {
		return errors.New("invalid M6 report metrics")
	}
	return nil
}
