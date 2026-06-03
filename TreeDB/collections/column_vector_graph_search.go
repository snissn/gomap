package collections

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

// Keep modest scratch overgrowth to avoid realloc churn when callers vary
// TopK/EfSearch slightly, while still releasing oversized scratch after large
// probes.
const columnVectorGraphNativeScratchOversizeSlack = 16

// Default TopK values are small; insertion order avoids sort overhead there.
// Larger result sets switch to sort.Slice so result ordering does not go O(k^2).
const columnVectorGraphNativeResultOrderInsertionSortLimit = 32

// Frontier traversal uses a max-heap ordered by score/ordinal. A modest fanout
// lowers sift depth while preserving the same total comparator and pop order.
const columnVectorGraphNativeFrontierHeapFanout = 4

var (
	errColumnVectorGraphNativeSearchScratchRequired            = errors.New("collections: column_graph native search requires caller-owned scratch")
	errColumnVectorGraphNativeSearchQueryDimensionMismatch     = errors.New("collections: column_graph native search query dimension mismatch")
	errColumnVectorGraphNativeSearchQueryNormInvalid           = errors.New("collections: column_graph native search query norm invalid")
	errColumnVectorGraphNativeSearchTopKNegative               = errors.New("collections: column_graph native search top_k cannot be negative")
	errColumnVectorGraphNativeSearchEfSearchNegative           = errors.New("collections: column_graph native search ef_search cannot be negative")
	errColumnVectorGraphNativeSearchCandidateDimensionMismatch = errors.New("collections: column_graph native search candidate dimension mismatch")
	errColumnVectorGraphNativeSearchTraversalModeInvalid       = errors.New("collections: column_graph native search traversal mode invalid")
	errColumnVectorGraphNativeSearchWavefrontWidthInvalid      = errors.New("collections: column_graph native search wavefront width invalid")
	errColumnVectorGraphNativeSearchWavefrontRequiresPrepared  = errors.New("collections: column_graph native search wavefront requires prepared typed-column graph search")
	errColumnVectorGraphNativeSearchWavefrontWidthWithoutMode  = errors.New("collections: column_graph native search wavefront width requires wavefront traversal mode")
	errColumnVectorGraphNativeSearchQueryModeInvalid           = errors.New("collections: column_graph native search query mode invalid")
	errColumnVectorGraphNativeSearchQuantizedRerankLimit       = errors.New("collections: column_graph quantized rerank candidate limit invalid")
)

type columnVectorGraphScoreBatchMode uint8

type columnVectorGraphNativeSearchTraversalMode uint8

type columnVectorGraphNativeSearchStatsMode uint8

type columnVectorGraphNativeSearchQueryMode uint8

const (
	columnVectorGraphScoreBatchModeDefault columnVectorGraphScoreBatchMode = iota
	columnVectorGraphScoreBatchModeScalar
	columnVectorGraphScoreBatchModeIndexed
)

const (
	columnVectorGraphNativeSearchTraversalModeDefault columnVectorGraphNativeSearchTraversalMode = iota
	columnVectorGraphNativeSearchTraversalModeExact
	columnVectorGraphNativeSearchTraversalModeWavefront
)

const (
	columnVectorGraphNativeSearchStatsModeDefault columnVectorGraphNativeSearchStatsMode = iota
	columnVectorGraphNativeSearchStatsModeMinimal
	columnVectorGraphNativeSearchStatsModeFullDiagnostics
	columnVectorGraphNativeSearchStatsModeBenchmarkDebug
)

const (
	columnVectorGraphNativeSearchQueryModeExact columnVectorGraphNativeSearchQueryMode = iota
	columnVectorGraphNativeSearchQueryModeQuantizedOnly
	columnVectorGraphNativeSearchQueryModeQuantizedRerank
)

func (m columnVectorGraphScoreBatchMode) indexedEnabled() bool {
	return m == columnVectorGraphScoreBatchModeIndexed
}

func (m columnVectorGraphScoreBatchMode) String() string {
	switch m {
	case columnVectorGraphScoreBatchModeIndexed:
		return "indexed"
	case columnVectorGraphScoreBatchModeScalar:
		return "scalar"
	default:
		return "default"
	}
}

func columnVectorGraphScoreBatchModeForSearchPlan(requested columnVectorGraphScoreBatchMode, plan *columnVectorGraphSearchPlan) columnVectorGraphScoreBatchMode {
	switch requested {
	case columnVectorGraphScoreBatchModeScalar, columnVectorGraphScoreBatchModeIndexed:
		return requested
	default:
		if plan != nil && plan.preparedIndexedScoringDefaultEligible() {
			return columnVectorGraphScoreBatchModeIndexed
		}
		return columnVectorGraphScoreBatchModeScalar
	}
}

func (m columnVectorGraphNativeSearchTraversalMode) normalized() (columnVectorGraphNativeSearchTraversalMode, error) {
	switch m {
	case columnVectorGraphNativeSearchTraversalModeDefault, columnVectorGraphNativeSearchTraversalModeExact:
		return columnVectorGraphNativeSearchTraversalModeExact, nil
	case columnVectorGraphNativeSearchTraversalModeWavefront:
		return columnVectorGraphNativeSearchTraversalModeWavefront, nil
	default:
		return columnVectorGraphNativeSearchTraversalModeExact, errColumnVectorGraphNativeSearchTraversalModeInvalid
	}
}

func (m columnVectorGraphNativeSearchTraversalMode) String() string {
	switch m {
	case columnVectorGraphNativeSearchTraversalModeWavefront:
		return "wavefront"
	case columnVectorGraphNativeSearchTraversalModeExact:
		return "exact"
	default:
		return "default"
	}
}

func columnVectorGraphNativeSearchTraversalOptions(opts columnVectorGraphNativeSearchOptions) (columnVectorGraphNativeSearchTraversalMode, int, error) {
	mode, err := opts.TraversalMode.normalized()
	if err != nil {
		return columnVectorGraphNativeSearchTraversalModeExact, 0, err
	}
	if mode != columnVectorGraphNativeSearchTraversalModeWavefront {
		if opts.WavefrontWidth != 0 {
			return columnVectorGraphNativeSearchTraversalModeExact, 0, errColumnVectorGraphNativeSearchWavefrontWidthWithoutMode
		}
		return mode, 0, nil
	}
	if opts.WavefrontWidth < 2 {
		return mode, 0, errColumnVectorGraphNativeSearchWavefrontWidthInvalid
	}
	return mode, opts.WavefrontWidth, nil
}

func columnVectorGraphNativeSearchScoreTileCapacity(degree, efSearch, wavefrontWidth int) int {
	if degree < 0 {
		degree = 0
	}
	if wavefrontWidth <= 1 || efSearch <= degree {
		return degree
	}
	return efSearch
}

func (p *columnVectorGraphSearchPlan) preparedIndexedScoringDefaultEligible() bool {
	return p != nil && p.preparedSearch != nil && p.preparedSearch.indexedScoringDefaultEligible()
}

func (m columnVectorGraphNativeSearchStatsMode) normalized() columnVectorGraphNativeSearchStatsMode {
	switch m {
	case columnVectorGraphNativeSearchStatsModeMinimal, columnVectorGraphNativeSearchStatsModeFullDiagnostics, columnVectorGraphNativeSearchStatsModeBenchmarkDebug:
		return m
	default:
		return columnVectorGraphNativeSearchStatsModeFullDiagnostics
	}
}

func (m columnVectorGraphNativeSearchStatsMode) minimal() bool {
	return m.normalized() == columnVectorGraphNativeSearchStatsModeMinimal
}

func (m columnVectorGraphNativeSearchStatsMode) String() string {
	switch m.normalized() {
	case columnVectorGraphNativeSearchStatsModeMinimal:
		return "minimal"
	case columnVectorGraphNativeSearchStatsModeBenchmarkDebug:
		return "benchmark_debug"
	default:
		return "full_diagnostics"
	}
}

func (m columnVectorGraphNativeSearchQueryMode) normalized() (columnVectorGraphNativeSearchQueryMode, error) {
	switch m {
	case columnVectorGraphNativeSearchQueryModeExact:
		return columnVectorGraphNativeSearchQueryModeExact, nil
	case columnVectorGraphNativeSearchQueryModeQuantizedOnly:
		return columnVectorGraphNativeSearchQueryModeQuantizedOnly, nil
	case columnVectorGraphNativeSearchQueryModeQuantizedRerank:
		return columnVectorGraphNativeSearchQueryModeQuantizedRerank, nil
	default:
		return columnVectorGraphNativeSearchQueryModeExact, errColumnVectorGraphNativeSearchQueryModeInvalid
	}
}

func (m columnVectorGraphNativeSearchQueryMode) quantized() bool {
	return m == columnVectorGraphNativeSearchQueryModeQuantizedOnly || m == columnVectorGraphNativeSearchQueryModeQuantizedRerank
}

func (m columnVectorGraphNativeSearchQueryMode) String() string {
	switch m {
	case columnVectorGraphNativeSearchQueryModeQuantizedOnly:
		return "quantized_only"
	case columnVectorGraphNativeSearchQueryModeQuantizedRerank:
		return "quantized_rerank"
	default:
		return "exact"
	}
}

type columnVectorGraphNativeSearchOptions struct {
	TopK     int
	EfSearch int

	ScoreBatchMode columnVectorGraphScoreBatchMode
	QueryMode      columnVectorGraphNativeSearchQueryMode
	// QuantizedIndexName selects a named derived score plane for quantized query
	// modes. Exact mode must leave it empty.
	QuantizedIndexName string
	// QuantizedRerankCandidates bounds the candidate set exact-reranked after
	// quantized traversal. Zero means TopK.
	QuantizedRerankCandidates int
	// TraversalMode selects the layer-0 traversal scheduler. The default and
	// explicit exact modes preserve current HNSW candidate order. Wavefront is an
	// internal, non-default relaxed traversal experiment that stages candidates
	// across multiple frontier pops and is only valid with a positive
	// WavefrontWidth.
	TraversalMode  columnVectorGraphNativeSearchTraversalMode
	WavefrontWidth int
	// StatsMode selects how much graph-search telemetry is collected. The zero
	// value preserves full diagnostics; minimal mode keeps production source
	// health, fallback, admission, and result counters while avoiding
	// per-edge/per-candidate diagnostics on the healthy combined prepared path.
	StatsMode columnVectorGraphNativeSearchStatsMode

	// OmitResultMaterialization is an internal benchmark/profiling hook for the
	// graph-only boundary. It preserves traversal/scoring/top-k work but skips
	// final result-ID and row-ref materialization; public search paths must leave
	// this false so returned IDs and row refs are populated and counted.
	OmitResultMaterialization bool

	// CandidateRows is an optional pre-composed row-domain filter over graph
	// ordinals. It is intentionally internal until public metadata predicate
	// planning is designed; callers that set it should compose predicate and
	// visibility masks through typedcolumn.ComposeRowSelections first.
	CandidateRows    typedcolumn.RowSelection
	HasCandidateRows bool
}

func (r *columnVectorGraphPhysicalRowReader) validateQuantizedNativeSearchOptions(mode columnVectorGraphNativeSearchQueryMode, opts columnVectorGraphNativeSearchOptions) error {
	if mode == columnVectorGraphNativeSearchQueryModeExact {
		if opts.QuantizedIndexName != "" {
			return errors.New("collections: exact column_graph search cannot select a quantized index")
		}
		if opts.QuantizedRerankCandidates != 0 {
			return errors.New("collections: exact column_graph search cannot set quantized rerank candidates")
		}
		return nil
	}
	if opts.QuantizedIndexName == "" {
		return errors.New("collections: column_graph quantized search requires a quantized index name")
	}
	if _, ok := findQuantizedVectorIndex(r.def, opts.QuantizedIndexName); !ok {
		return fmt.Errorf("%w: column_graph %q quantized index %q is not declared", ErrVectorIndexSearchUnavailable, r.def.Name, opts.QuantizedIndexName)
	}
	if mode == columnVectorGraphNativeSearchQueryModeQuantizedOnly && opts.QuantizedRerankCandidates != 0 {
		return errors.New("collections: column_graph quantized_only search cannot set rerank candidates")
	}
	if mode == columnVectorGraphNativeSearchQueryModeQuantizedRerank {
		if opts.QuantizedRerankCandidates < 0 {
			return errColumnVectorGraphNativeSearchQuantizedRerankLimit
		}
		if opts.QuantizedRerankCandidates != 0 && opts.TopK > 0 && opts.QuantizedRerankCandidates < opts.TopK {
			return errColumnVectorGraphNativeSearchQuantizedRerankLimit
		}
	}
	return fmt.Errorf("%w: column_graph %q query_mode=%s quantized index %q has no loaded quantized score-plane asset", ErrVectorIndexSearchUnavailable, r.def.Name, mode.String(), opts.QuantizedIndexName)
}

type columnVectorGraphAdjacencySourceCounterSnapshot struct {
	AdjacencyBytesRead                   uint64
	AdjacencyDirectViews                 uint64
	AdjacencyMmapDirectViews             uint64
	AdjacencyHeapCopyTypedViews          uint64
	AdjacencyScratchDecodes              uint64
	AdjacencyPreparedCSRDirectViews      uint64
	AdjacencyPreparedCSRMmapDirectViews  uint64
	AdjacencyTypedListDirectViews        uint64
	AdjacencyTypedListMmapDirectViews    uint64
	AdjacencyTypedListHeapCopyTypedViews uint64
	AdjacencyTypedListScratchDecodes     uint64
}

func (c *columnVectorGraphAdjacencySourceCounterSnapshot) addOutcome(adjacencyLen int, outcome columnVectorGraphLayer0AdjacencySourceOutcome) {
	if c == nil {
		return
	}
	c.AdjacencyBytesRead += uint64(adjacencyLen) * 4
	switch outcome {
	case columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect:
		c.AdjacencyDirectViews++
		c.AdjacencyMmapDirectViews++
	case columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView:
		c.AdjacencyHeapCopyTypedViews++
	case columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect:
		c.AdjacencyDirectViews++
		c.AdjacencyMmapDirectViews++
		c.AdjacencyPreparedCSRDirectViews++
		c.AdjacencyPreparedCSRMmapDirectViews++
	case columnVectorGraphLayer0AdjacencySourceOutcomeTypedListMmapDirect:
		c.AdjacencyDirectViews++
		c.AdjacencyMmapDirectViews++
		c.AdjacencyTypedListDirectViews++
		c.AdjacencyTypedListMmapDirectViews++
	case columnVectorGraphLayer0AdjacencySourceOutcomeTypedListHeapCopyTypedView:
		c.AdjacencyHeapCopyTypedViews++
		c.AdjacencyTypedListHeapCopyTypedViews++
	case columnVectorGraphLayer0AdjacencySourceOutcomeTypedListScratchDecode:
		if adjacencyLen > 0 {
			c.AdjacencyScratchDecodes++
			c.AdjacencyTypedListScratchDecodes++
		}
	default:
		if adjacencyLen > 0 {
			c.AdjacencyScratchDecodes++
		}
	}
}

type columnVectorGraphNativeSearchStats struct {
	CandidateRows                        uint64
	Candidates                           uint64
	Edges                                uint64
	VisitedNodes                         uint64
	VisitedEdges                         uint64
	VectorBytesRead                      uint64
	NormBytesRead                        uint64
	AdjacencyBytesRead                   uint64
	CandidateFetches                     uint64
	ExpansionFetches                     uint64
	ResultFetches                        uint64
	ScoreBatches                         uint64
	OrdinalsGrouped                      uint64
	ScoreBatchCalls                      uint64
	ScoreBatchCandidates                 uint64
	ScoreBatchMaxTileSize                uint64
	ScoreBatchOptimizedCalls             uint64
	ScoreBatchScalarFallbackCalls        uint64
	PreparedScoreCalls                   uint64
	ScoreFloat64Fallbacks                uint64
	BlockViewHits                        uint64
	BlockViewMisses                      uint64
	BlockViewBuilds                      uint64
	AdjacencyExpansions                  uint64
	AdjacencyScratchDecodes              uint64
	AdjacencyDirectViews                 uint64
	AdjacencyMmapDirectViews             uint64
	AdjacencyHeapCopyTypedViews          uint64
	AdjacencyPreparedCSRDirectViews      uint64
	AdjacencyPreparedCSRMmapDirectViews  uint64
	AdjacencyTypedListDirectViews        uint64
	AdjacencyTypedListMmapDirectViews    uint64
	AdjacencyTypedListHeapCopyTypedViews uint64
	AdjacencyTypedListScratchDecodes     uint64
	AdjacencyLegacyFallbacks             uint64
	AdjacencySourceUnavailable           uint64
	AdjacencySourceFallbacks             uint64
	AdjacencyCertificationFailures       uint64
	AdjacencyValidationFailures          uint64
	AdjacencyAbsoluteOffsetUnaligned     uint64
	AdjacencyActualPointerUnaligned      uint64
	AdjacencyStaleHandles                uint64
	NormDirectViews                      uint64
	NormMmapDirectViews                  uint64
	NormHeapCopyTypedViews               uint64
	NormScratchDecodes                   uint64
	NormPreparedDirectViews              uint64
	NormSourceUnavailable                uint64
	NormSourceFallbacks                  uint64
	NormValidationFailures               uint64
	NormAbsoluteOffsetUnaligned          uint64
	NormActualPointerUnaligned           uint64
	NormStaleHandles                     uint64
	NormMappedBytes                      uint64
	NormHeapCopyBytes                    uint64
	NormDecodedBytes                     uint64
	NormActiveHandles                    int64
	NormDeniedResources                  uint64
	VectorDirectViews                    uint64
	VectorMmapDirectViews                uint64
	VectorHeapCopyTypedViews             uint64
	VectorScratchDecodes                 uint64
	VectorPreparedDirectViews            uint64
	VectorPreparedIdentityMappings       uint64
	VectorPreparedRowRefMappings         uint64
	VectorCertificationFailures          uint64
	VectorAbsoluteOffsetUnaligned        uint64
	VectorActualPointerUnaligned         uint64
	VectorStaleHandles                   uint64
	TypedColumnMappedBytes               uint64
	TypedColumnHeapCopyBytes             uint64
	TypedColumnDecodedBytes              uint64
	TypedColumnActiveHandles             int64
	TypedColumnDeniedResources           uint64
	TypedColumnFallbacks                 uint64
	RowRefVectorSourceState              uint64
	RowRefVectorSourceLegacyGraphIDs     uint64
	RowRefStatePreparedViews             uint64
	RowRefStateMmapDirectFields          uint64
	RowRefStateResultRefs                uint64
	RowRefStateSourceUnavailable         uint64
	RowRefStateSourceFallbacks           uint64
	ResultIDPreparedBytesViews           uint64
	ResultIDTypedBytesState              uint64
	ResultIDGraphFallbacks               uint64
	ResultIDStateValidationFailures      uint64
	PreparedGraphSearchViews             uint64
	GraphRowFallbacks                    uint64

	WavefrontSearches        uint64
	WavefrontWidth           uint64
	WavefrontRounds          uint64
	WavefrontCandidatePops   uint64
	WavefrontStagedNeighbors uint64
	WavefrontMaxTileSize     uint64

	// Benchmark/debug-only HNSW control-flow counters. These are populated only
	// when columnVectorGraphNativeSearchStatsModeBenchmarkDebug is selected so
	// steady-state full/minimal stats avoid the detailed counter increments.
	BenchmarkDebugSearches uint64

	NeighborTiles          uint64
	NeighborTileNeighbors  uint64
	NeighborTileMaxSize    uint64
	NeighborTileSize0      uint64
	NeighborTileSize1      uint64
	NeighborTileSize2To4   uint64
	NeighborTileSize5To8   uint64
	NeighborTileSize9To16  uint64
	NeighborTileSize17Plus uint64

	ScoreBatchSingletons      uint64
	ScoreBatchSize2To4        uint64
	ScoreBatchSize5To8        uint64
	ScoreBatchSize9To16       uint64
	ScoreBatchSize17Plus      uint64
	ScoredNeighbors           uint64
	SkippedNeighbors          uint64
	AlreadyVisitedSkips       uint64
	FilterSkips               uint64
	UpperLayerScores          uint64
	UpperLayerEntryScores     uint64
	UpperLayerNeighborScores  uint64
	UpperLayerEdgeVisits      uint64
	UpperLayerScoredNeighbors uint64
	UpperLayerFilterSkips     uint64
	Layer0Scores              uint64
	Layer0SeedScores          uint64
	Layer0NeighborScores      uint64
	Layer0EdgeVisits          uint64
	Layer0ScoredNeighbors     uint64
	Layer0AlreadyVisitedSkips uint64
	Layer0FilterSkips         uint64

	FrontierPushes        uint64
	FrontierPops          uint64
	FrontierPopMisses     uint64
	FrontierSiftUpSteps   uint64
	FrontierSiftDownSteps uint64

	TopKInsertAttempts   uint64
	TopKInsertSuccesses  uint64
	TopKInsertRejections uint64
	TopKShiftSteps       uint64

	VisitedMarkChecks uint64
	VisitedMarkHits   uint64
	VisitedMarkMisses uint64

	ExactModeSearches                     uint64
	ExactCandidateOrderObservations       uint64
	ExactCandidateOrderTransitions        uint64
	ExactCandidateOrderAdjacentForward    uint64
	ExactCandidateOrderNonAdjacentForward uint64
	ExactCandidateOrderBackwardJumps      uint64
	ExactCandidateOrderMaxForwardRun      uint64
}

type columnVectorGraphNativeSearchLoopCounters struct {
	Edges        uint64
	VisitedEdges uint64
}

func (c *columnVectorGraphNativeSearchLoopCounters) publish(stats *columnVectorGraphNativeSearchStats, candidates uint64) {
	if c == nil || stats == nil {
		return
	}
	stats.Candidates += candidates
	stats.Edges += c.Edges
	stats.VisitedEdges += c.VisitedEdges
}

type columnVectorGraphNativeSearchScoreContext uint8

const (
	columnVectorGraphNativeSearchScoreContextUpperEntry columnVectorGraphNativeSearchScoreContext = iota
	columnVectorGraphNativeSearchScoreContextUpperNeighbor
	columnVectorGraphNativeSearchScoreContextLayer0Seed
	columnVectorGraphNativeSearchScoreContextLayer0Neighbor
)

type columnVectorGraphNativeSearchDebugCounters struct {
	stats *columnVectorGraphNativeSearchStats

	exactMode              bool
	exactOrderStarted      bool
	exactLastOrdinal       int
	exactCurrentForwardRun uint64
}

func newColumnVectorGraphNativeSearchDebugCounters(stats *columnVectorGraphNativeSearchStats, exactMode bool) *columnVectorGraphNativeSearchDebugCounters {
	if stats == nil {
		return nil
	}
	stats.BenchmarkDebugSearches = 1
	if exactMode {
		stats.ExactModeSearches = 1
	}
	return &columnVectorGraphNativeSearchDebugCounters{stats: stats, exactMode: exactMode}
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordNeighborTile(size int) {
	if d == nil || d.stats == nil {
		return
	}
	if size < 0 {
		size = 0
	}
	s := d.stats
	s.NeighborTiles++
	s.NeighborTileNeighbors += uint64(size)
	if uint64(size) > s.NeighborTileMaxSize {
		s.NeighborTileMaxSize = uint64(size)
	}
	switch {
	case size == 0:
		s.NeighborTileSize0++
	case size == 1:
		s.NeighborTileSize1++
	case size <= 4:
		s.NeighborTileSize2To4++
	case size <= 8:
		s.NeighborTileSize5To8++
	case size <= 16:
		s.NeighborTileSize9To16++
	default:
		s.NeighborTileSize17Plus++
	}
}

func recordColumnVectorGraphScoreBatchDebugStats(stats *columnVectorGraphNativeSearchStats, tileSize int) {
	if stats == nil || stats.BenchmarkDebugSearches == 0 || tileSize <= 0 {
		return
	}
	switch {
	case tileSize == 1:
		stats.ScoreBatchSingletons++
	case tileSize <= 4:
		stats.ScoreBatchSize2To4++
	case tileSize <= 8:
		stats.ScoreBatchSize5To8++
	case tileSize <= 16:
		stats.ScoreBatchSize9To16++
	default:
		stats.ScoreBatchSize17Plus++
	}
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordUpperLayerEdge() {
	if d == nil || d.stats == nil {
		return
	}
	d.stats.UpperLayerEdgeVisits++
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordLayer0Edge() {
	if d == nil || d.stats == nil {
		return
	}
	d.stats.Layer0EdgeVisits++
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordFilterSkip(layer0 bool) {
	if d == nil || d.stats == nil {
		return
	}
	s := d.stats
	s.FilterSkips++
	s.SkippedNeighbors++
	if layer0 {
		s.Layer0FilterSkips++
	} else {
		s.UpperLayerFilterSkips++
	}
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordAlreadyVisitedSkip() {
	if d == nil || d.stats == nil {
		return
	}
	s := d.stats
	s.AlreadyVisitedSkips++
	s.SkippedNeighbors++
	s.Layer0AlreadyVisitedSkips++
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordVisitedMark(hit bool) {
	if d == nil || d.stats == nil {
		return
	}
	d.stats.VisitedMarkChecks++
	if hit {
		d.stats.VisitedMarkHits++
		return
	}
	d.stats.VisitedMarkMisses++
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordScores(context columnVectorGraphNativeSearchScoreContext, ordinals []int) {
	if d == nil || d.stats == nil || len(ordinals) == 0 {
		return
	}
	d.recordScoresCount(context, len(ordinals))
	if context == columnVectorGraphNativeSearchScoreContextLayer0Seed || context == columnVectorGraphNativeSearchScoreContextLayer0Neighbor {
		for _, ordinal := range ordinals {
			d.recordExactCandidateOrder(ordinal)
		}
	}
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordScore(context columnVectorGraphNativeSearchScoreContext, ordinal int) {
	if d == nil || d.stats == nil {
		return
	}
	d.recordScoresCount(context, 1)
	if context == columnVectorGraphNativeSearchScoreContextLayer0Seed || context == columnVectorGraphNativeSearchScoreContextLayer0Neighbor {
		d.recordExactCandidateOrder(ordinal)
	}
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordScoresCount(context columnVectorGraphNativeSearchScoreContext, count int) {
	if d == nil || d.stats == nil || count <= 0 {
		return
	}
	s := d.stats
	count64 := uint64(count)
	switch context {
	case columnVectorGraphNativeSearchScoreContextUpperEntry:
		s.UpperLayerScores += count64
		s.UpperLayerEntryScores += count64
	case columnVectorGraphNativeSearchScoreContextUpperNeighbor:
		s.UpperLayerScores += count64
		s.UpperLayerNeighborScores += count64
		s.UpperLayerScoredNeighbors += count64
		s.ScoredNeighbors += count64
	case columnVectorGraphNativeSearchScoreContextLayer0Seed:
		s.Layer0Scores += count64
		s.Layer0SeedScores += count64
	case columnVectorGraphNativeSearchScoreContextLayer0Neighbor:
		s.Layer0Scores += count64
		s.Layer0NeighborScores += count64
		s.Layer0ScoredNeighbors += count64
		s.ScoredNeighbors += count64
	}
}

func (d *columnVectorGraphNativeSearchDebugCounters) recordExactCandidateOrder(ordinal int) {
	if d == nil || d.stats == nil || !d.exactMode {
		return
	}
	s := d.stats
	s.ExactCandidateOrderObservations++
	if !d.exactOrderStarted {
		d.exactOrderStarted = true
		d.exactLastOrdinal = ordinal
		d.exactCurrentForwardRun = 1
		if s.ExactCandidateOrderMaxForwardRun < 1 {
			s.ExactCandidateOrderMaxForwardRun = 1
		}
		return
	}
	s.ExactCandidateOrderTransitions++
	switch {
	case ordinal == d.exactLastOrdinal+1:
		s.ExactCandidateOrderAdjacentForward++
		d.exactCurrentForwardRun++
		if d.exactCurrentForwardRun > s.ExactCandidateOrderMaxForwardRun {
			s.ExactCandidateOrderMaxForwardRun = d.exactCurrentForwardRun
		}
	case ordinal > d.exactLastOrdinal:
		s.ExactCandidateOrderNonAdjacentForward++
		d.exactCurrentForwardRun = 1
	default:
		s.ExactCandidateOrderBackwardJumps++
		d.exactCurrentForwardRun = 1
	}
	d.exactLastOrdinal = ordinal
}

type columnVectorGraphPreparedMinimalSearchCounters struct {
	dims                  int
	vectorIdentityMapping bool

	ScoreBatches                        uint64
	OrdinalsGrouped                     uint64
	ScoreBatchCalls                     uint64
	ScoreBatchCandidates                uint64
	ScoreBatchMaxTileSize               uint64
	ScoreBatchOptimizedCalls            uint64
	ScoreBatchScalarFallbackCalls       uint64
	PreparedScoreCalls                  uint64
	AdjacencyBytesRead                  uint64
	ExpansionFetches                    uint64
	AdjacencyExpansions                 uint64
	AdjacencyDirectViews                uint64
	AdjacencyMmapDirectViews            uint64
	AdjacencyPreparedCSRDirectViews     uint64
	AdjacencyPreparedCSRMmapDirectViews uint64
}

func newColumnVectorGraphPreparedMinimalSearchCounters(view *columnVectorGraphPreparedSearchView) *columnVectorGraphPreparedMinimalSearchCounters {
	if view == nil {
		return nil
	}
	return &columnVectorGraphPreparedMinimalSearchCounters{dims: view.dims, vectorIdentityMapping: view.vectorIdentityMapping}
}

func (c *columnVectorGraphPreparedMinimalSearchCounters) recordPreparedScores(count int, optimized bool, scalarFallback bool) {
	if c == nil || count <= 0 {
		return
	}
	count64 := uint64(count)
	c.ScoreBatches++
	c.OrdinalsGrouped += count64
	c.ScoreBatchCalls++
	c.ScoreBatchCandidates += count64
	if count64 > c.ScoreBatchMaxTileSize {
		c.ScoreBatchMaxTileSize = count64
	}
	if optimized {
		c.ScoreBatchOptimizedCalls++
	}
	if scalarFallback {
		c.ScoreBatchScalarFallbackCalls++
	}
	c.PreparedScoreCalls += count64
}

func (p *columnVectorGraphSearchPlan) recordPreparedMinimalScoreBatch(counters *columnVectorGraphPreparedMinimalSearchCounters, ordinals []int) {
	if counters == nil || len(ordinals) == 0 {
		return
	}
	if p == nil || p.preparedSearch == nil || !p.scoreBatchMode.indexedEnabled() || len(ordinals) <= 1 {
		counters.recordPreparedScores(len(ordinals), false, true)
		return
	}
	p.preparedSearch.recordIndexedScoreBatchMinimalCounters(counters, ordinals)
}

func (c *columnVectorGraphPreparedMinimalSearchCounters) recordPreparedAdjacency(adjacencyLen int) {
	if c == nil {
		return
	}
	c.ExpansionFetches++
	c.AdjacencyExpansions++
	c.AdjacencyBytesRead += uint64(adjacencyLen) * 4
	c.AdjacencyDirectViews++
	c.AdjacencyMmapDirectViews++
	c.AdjacencyPreparedCSRDirectViews++
	c.AdjacencyPreparedCSRMmapDirectViews++
}

func (c *columnVectorGraphPreparedMinimalSearchCounters) recordAdjacencyCounterSnapshot(counters columnVectorGraphAdjacencySourceCounterSnapshot) {
	if c == nil {
		return
	}
	c.AdjacencyBytesRead += counters.AdjacencyBytesRead
	c.AdjacencyDirectViews += counters.AdjacencyDirectViews
	c.AdjacencyMmapDirectViews += counters.AdjacencyMmapDirectViews
	c.AdjacencyPreparedCSRDirectViews += counters.AdjacencyPreparedCSRDirectViews
	c.AdjacencyPreparedCSRMmapDirectViews += counters.AdjacencyPreparedCSRMmapDirectViews
}

func (c *columnVectorGraphPreparedMinimalSearchCounters) publish(stats *columnVectorGraphNativeSearchStats) {
	if c == nil || stats == nil {
		return
	}
	stats.ScoreBatches += c.ScoreBatches
	stats.OrdinalsGrouped += c.OrdinalsGrouped
	stats.ScoreBatchCalls += c.ScoreBatchCalls
	stats.ScoreBatchCandidates += c.ScoreBatchCandidates
	if c.ScoreBatchMaxTileSize > stats.ScoreBatchMaxTileSize {
		stats.ScoreBatchMaxTileSize = c.ScoreBatchMaxTileSize
	}
	stats.ScoreBatchOptimizedCalls += c.ScoreBatchOptimizedCalls
	stats.ScoreBatchScalarFallbackCalls += c.ScoreBatchScalarFallbackCalls
	stats.PreparedScoreCalls += c.PreparedScoreCalls
	stats.VisitedNodes += c.PreparedScoreCalls
	stats.VectorBytesRead += c.PreparedScoreCalls * uint64(c.dims) * 4
	stats.NormBytesRead += c.PreparedScoreCalls * 4
	stats.AdjacencyBytesRead += c.AdjacencyBytesRead
	stats.CandidateFetches += c.PreparedScoreCalls
	stats.ExpansionFetches += c.ExpansionFetches
	stats.AdjacencyExpansions += c.AdjacencyExpansions
	stats.AdjacencyDirectViews += c.AdjacencyDirectViews
	stats.AdjacencyMmapDirectViews += c.AdjacencyMmapDirectViews
	stats.AdjacencyPreparedCSRDirectViews += c.AdjacencyPreparedCSRDirectViews
	stats.AdjacencyPreparedCSRMmapDirectViews += c.AdjacencyPreparedCSRMmapDirectViews
	stats.VectorDirectViews += c.PreparedScoreCalls
	stats.VectorMmapDirectViews += c.PreparedScoreCalls
	stats.VectorPreparedDirectViews += c.PreparedScoreCalls
	if c.vectorIdentityMapping {
		stats.VectorPreparedIdentityMappings += c.PreparedScoreCalls
	} else {
		stats.VectorPreparedRowRefMappings += c.PreparedScoreCalls
	}
	stats.NormDirectViews += c.PreparedScoreCalls
	stats.NormMmapDirectViews += c.PreparedScoreCalls
	stats.NormPreparedDirectViews += c.PreparedScoreCalls
}

// columnVectorGraphNativeSearchResult aliases buffers owned by the search
// scratch. Callers must copy the returned result slice and any retained result
// IDs before the next search with the same scratch.
type columnVectorGraphNativeSearchResult struct {
	Ordinal   int
	ID        []byte
	RowRef    DocumentRowRef
	HasRowRef bool
	Score     float64
}

type columnVectorGraphSearchCandidate struct {
	ordinal int
	score   float64
}

// columnVectorGraphNativeSearchScratch is caller-owned mutable search state.
// It is not concurrency-safe. Parallel searches over immutable graph assets are
// valid with one reader and one scratch per worker.
type columnVectorGraphNativeSearchScratch struct {
	scoreScratch        columnPhysicalRowReaderScratch
	expandScratch       columnPhysicalRowReaderScratch
	resultScratch       columnPhysicalRowReaderScratch
	visitMarks          []uint64
	visitEpoch          uint64
	frontier            []columnVectorGraphSearchCandidate
	top                 []columnVectorGraphSearchCandidate
	results             []columnVectorGraphNativeSearchResult
	idBuffers           [][]byte
	resultOrder         []int
	resultOrdinals      []int
	resultRowRefs       []DocumentRowRef
	resultHasRefs       []bool
	scoreTileOrdinals   []int
	scoreTileScores     []float64
	scoreTileRowIDs     []uint32
	scoreTileDots       []float32
	wavefrontCandidates []columnVectorGraphSearchCandidate
	searchPlan          columnVectorGraphSearchPlan
}

func (s *columnVectorGraphNativeSearchScratch) prepare(rowCount, dimensions, degree, topK, efSearch, scoreTileCapacity, wavefrontWidth int) error {
	if s == nil {
		return errColumnVectorGraphNativeSearchScratchRequired
	}
	if rowCount < 0 || dimensions < 0 || degree < 0 || topK < 0 || efSearch < 0 || scoreTileCapacity < 0 || wavefrontWidth < 0 {
		return fmt.Errorf("collections: column_graph native search received negative sizing input: rowCount=%d dimensions=%d degree=%d topK=%d efSearch=%d scoreTileCapacity=%d wavefrontWidth=%d", rowCount, dimensions, degree, topK, efSearch, scoreTileCapacity, wavefrontWidth)
	}
	if scoreTileCapacity < degree {
		scoreTileCapacity = degree
	}
	for _, rowScratch := range []*columnPhysicalRowReaderScratch{&s.scoreScratch, &s.expandScratch, &s.resultScratch} {
		prepareColumnVectorGraphNativeRowScratch(rowScratch, dimensions, degree)
	}
	s.visitMarks = resizeColumnVectorGraphNativeUint64Scratch(s.visitMarks, rowCount)
	s.visitEpoch++
	if s.visitEpoch == 0 {
		clear(s.visitMarks)
		s.visitEpoch = 1
	}
	frontierCap := efSearch
	if frontierCap > rowCount {
		frontierCap = rowCount
	}
	if frontierCap < topK {
		frontierCap = topK
	}
	s.frontier = resizeColumnVectorGraphNativeCandidateScratch(s.frontier, frontierCap)
	s.top = resizeColumnVectorGraphNativeCandidateScratch(s.top, topK)
	s.results = resizeColumnVectorGraphNativeResultScratch(s.results, topK)
	s.idBuffers = resizeColumnVectorGraphNativeIDBuffersScratch(s.idBuffers, topK)
	s.resultOrder = resizeColumnVectorGraphNativeIntScratch(s.resultOrder, topK)
	s.resultOrdinals = resizeColumnVectorGraphNativeIntScratch(s.resultOrdinals, topK)
	s.resultRowRefs = resizeColumnVectorGraphNativeRowRefScratch(s.resultRowRefs, topK)
	s.resultHasRefs = resizeColumnVectorGraphNativeBoolScratch(s.resultHasRefs, topK)
	s.scoreTileOrdinals = resizeColumnVectorGraphNativeIntScratch(s.scoreTileOrdinals, scoreTileCapacity)
	s.scoreTileScores = resizeColumnVectorGraphNativeFloat64Scratch(s.scoreTileScores, scoreTileCapacity)
	s.scoreTileRowIDs = resizeColumnVectorGraphNativeUint32Scratch(s.scoreTileRowIDs, scoreTileCapacity)
	s.scoreTileDots = resizeColumnVectorGraphNativeFloat32Scratch(s.scoreTileDots, scoreTileCapacity)
	s.wavefrontCandidates = resizeColumnVectorGraphNativeCandidateScratch(s.wavefrontCandidates, wavefrontWidth)
	return nil
}

func prepareColumnVectorGraphNativeRowScratch(s *columnPhysicalRowReaderScratch, dimensions, degree int) {
	if cap(s.Values) < columnVectorGraphPhysicalRowValueCount {
		s.Values = make([]columnDeclaredValue, 0, columnVectorGraphPhysicalRowValueCount)
	} else {
		clear(s.Values)
		s.Values = s.Values[:0]
	}
	if cap(s.Float32Values) < dimensions {
		s.Float32Values = make([]float32, 0, dimensions)
	} else {
		s.Float32Values = s.Float32Values[:0]
	}
	if cap(s.Uint32Values) < degree {
		s.Uint32Values = make([]uint32, 0, degree)
	} else {
		s.Uint32Values = s.Uint32Values[:0]
	}
}

func resizeColumnVectorGraphNativeCandidateScratch(dst []columnVectorGraphSearchCandidate, target int) []columnVectorGraphSearchCandidate {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]columnVectorGraphSearchCandidate, 0, target)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeResultScratch(dst []columnVectorGraphNativeSearchResult, target int) []columnVectorGraphNativeSearchResult {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		clear(dst)
		return make([]columnVectorGraphNativeSearchResult, 0, target)
	}
	if len(dst) > 0 {
		clear(dst)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeIntScratch(dst []int, target int) []int {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]int, 0, target)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeUint64Scratch(dst []uint64, target int) []uint64 {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]uint64, target)
	}
	return dst[:target]
}

func resizeColumnVectorGraphNativeUint32Scratch(dst []uint32, target int) []uint32 {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]uint32, 0, target)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeFloat32Scratch(dst []float32, target int) []float32 {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]float32, 0, target)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeFloat64Scratch(dst []float64, target int) []float64 {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]float64, 0, target)
	}
	return dst[:0]
}

func ensureColumnVectorGraphNativeIntScratch(dst []int, target int) []int {
	if cap(dst) < target {
		return make([]int, 0, target)
	}
	return dst[:0]
}

func ensureColumnVectorGraphNativeUint32Scratch(dst []uint32, target int) []uint32 {
	if cap(dst) < target {
		return make([]uint32, 0, target)
	}
	return dst[:0]
}

func ensureColumnVectorGraphNativeFloat32Scratch(dst []float32, target int) []float32 {
	if cap(dst) < target {
		return make([]float32, 0, target)
	}
	return dst[:0]
}

func ensureColumnVectorGraphNativeFloat64Scratch(dst []float64, target int) []float64 {
	if cap(dst) < target {
		return make([]float64, 0, target)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeRowRefScratch(dst []DocumentRowRef, target int) []DocumentRowRef {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]DocumentRowRef, target)
	}
	if len(dst) > 0 {
		clear(dst)
	}
	return dst[:target]
}

func resizeColumnVectorGraphNativeBoolScratch(dst []bool, target int) []bool {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]bool, target)
	}
	if len(dst) > 0 {
		clear(dst)
	}
	return dst[:target]
}

func resizeColumnVectorGraphNativeIDBuffersScratch(dst [][]byte, target int) [][]byte {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		next := make([][]byte, target)
		copy(next, dst)
		return next
	}
	if len(dst) < target {
		oldLen := len(dst)
		dst = dst[:target]
		clear(dst[oldLen:target])
		return dst
	}
	for i := target; i < len(dst); i++ {
		dst[i] = nil
	}
	return dst[:target]
}

func columnVectorGraphNativeScratchCapOversized(capacity, target int) bool {
	if target < 0 {
		return true
	}
	if target > (math.MaxInt-columnVectorGraphNativeScratchOversizeSlack)/2 {
		return false
	}
	return capacity > target*2+columnVectorGraphNativeScratchOversizeSlack
}

// SearchCosine traverses the persisted column graph through bound TVIS/base
// typed-column sources. Legacy graph rows are read only through explicit
// compatibility fallback when a physical row asset is present. Document
// materialization stays outside this kernel. Returned results and result IDs
// alias scratch-owned buffers and must be copied before the next SearchCosine
// call with the same scratch.
func (r *columnVectorGraphPhysicalRowReader) SearchCosine(query []float32, opts columnVectorGraphNativeSearchOptions, scratch *columnVectorGraphNativeSearchScratch) (results []columnVectorGraphNativeSearchResult, stats columnVectorGraphNativeSearchStats, err error) {
	if r == nil {
		return nil, columnVectorGraphNativeSearchStats{}, errNilColumnVectorGraphPhysicalRowReader
	}
	if r.reader == nil && columnVectorGraphManifestHasPhysicalAsset(r.graph) {
		return nil, columnVectorGraphNativeSearchStats{}, errNilColumnVectorGraphPhysicalRowReader
	}
	if len(query) != r.def.Dimensions {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q query dims=%d want %d: %w", r.def.Name, len(query), r.def.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	queryMode, err := opts.QueryMode.normalized()
	if err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search query mode: %w", r.def.Name, err)
	}
	if err := r.validateQuantizedNativeSearchOptions(queryMode, opts); err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, err
	}
	rowCount := r.RowCount()
	topK := opts.TopK
	if topK < 0 {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search top_k cannot be negative: %w", r.def.Name, errColumnVectorGraphNativeSearchTopKNegative)
	}
	efSearch := opts.EfSearch
	if efSearch < 0 {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search ef_search cannot be negative: %w", r.def.Name, errColumnVectorGraphNativeSearchEfSearchNegative)
	}
	traversalMode, wavefrontWidth, err := columnVectorGraphNativeSearchTraversalOptions(opts)
	if err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search traversal: %w", r.def.Name, err)
	}
	if topK == 0 || rowCount == 0 {
		return nil, columnVectorGraphNativeSearchStats{}, nil
	}
	candidateRows, hasCandidateRows, err := columnVectorGraphSearchCandidateRows(opts, rowCount)
	if err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q candidate rows: %w", r.def.Name, err)
	}
	candidateRowCount := rowCount
	if hasCandidateRows {
		candidateRowCount = candidateRows.Count()
	}
	stats.CandidateRows = uint64(candidateRowCount)
	if candidateRowCount == 0 {
		return nil, stats, nil
	}
	if scratch == nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q: %w", r.def.Name, errColumnVectorGraphNativeSearchScratchRequired)
	}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q query norm: %w: %w", r.def.Name, errColumnVectorGraphNativeSearchQueryNormInvalid, err)
	}
	if topK > candidateRowCount {
		topK = candidateRowCount
	}
	if efSearch == 0 {
		efSearch = r.def.EfSearch
	}
	if efSearch < topK {
		efSearch = topK
	}
	if efSearch > candidateRowCount {
		efSearch = candidateRowCount
	}
	if traversalMode == columnVectorGraphNativeSearchTraversalModeWavefront && wavefrontWidth > efSearch {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search wavefront width=%d exceeds normalized ef_search=%d: %w", r.def.Name, wavefrontWidth, efSearch, errColumnVectorGraphNativeSearchWavefrontWidthInvalid)
	}
	degree := r.def.M
	if degree < 0 {
		degree = 0
	}
	scoreTileCapacity := columnVectorGraphNativeSearchScoreTileCapacity(degree, efSearch, wavefrontWidth)
	if err := scratch.prepare(rowCount, r.def.Dimensions, degree, topK, efSearch, scoreTileCapacity, wavefrontWidth); err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search scratch prepare: %w", r.def.Name, err)
	}

	statsMode := opts.StatsMode.normalized()
	candidateLimit := uint64(efSearch)
	var visitedCandidates uint64
	var loopEdgeVisits uint64
	var plan *columnVectorGraphSearchPlan
	var loopCounters *columnVectorGraphNativeSearchLoopCounters
	if !statsMode.minimal() {
		loopCounters = &columnVectorGraphNativeSearchLoopCounters{}
	}
	var preparedMinimalCounters *columnVectorGraphPreparedMinimalSearchCounters
	var debugCounters *columnVectorGraphNativeSearchDebugCounters
	var debugStats *columnVectorGraphNativeSearchStats
	if statsMode == columnVectorGraphNativeSearchStatsModeBenchmarkDebug {
		exactTraversalForDebug := traversalMode == columnVectorGraphNativeSearchTraversalModeExact && candidateLimit == uint64(candidateRowCount)
		debugCounters = newColumnVectorGraphNativeSearchDebugCounters(&stats, exactTraversalForDebug)
		if debugCounters != nil {
			debugStats = debugCounters.stats
		}
	}
	defer func() {
		if loopCounters != nil {
			loopCounters.Edges += loopEdgeVisits
			loopCounters.VisitedEdges += loopEdgeVisits
			loopCounters.publish(&stats, visitedCandidates)
		}
		if preparedMinimalCounters != nil {
			preparedMinimalCounters.publish(&stats)
		}
		r.populateTypedColumnVectorSearchStats(&stats)
		r.populateInvNormStateSearchStats(&stats)
		r.populateRowRefStateSearchStats(&stats)
		r.populateDocumentIDStateSearchStats(&stats)
		r.populateLayer0AdjacencySourceSearchStats(&stats)
		if plan != nil {
			plan.scoreSource.populateConstructionStats(&stats)
		}
	}()
	plan, err = scratch.prepareSearchPlanForNativeSearch(r)
	if err != nil {
		return nil, stats, err
	}
	plan.scoreBatchMode = columnVectorGraphScoreBatchModeForSearchPlan(opts.ScoreBatchMode, plan)
	if traversalMode == columnVectorGraphNativeSearchTraversalModeWavefront {
		if plan.preparedSearch == nil {
			return nil, stats, fmt.Errorf("collections: column_graph %q native search wavefront: %w", r.def.Name, errColumnVectorGraphNativeSearchWavefrontRequiresPrepared)
		}
		stats.WavefrontSearches = 1
		stats.WavefrontWidth = uint64(wavefrontWidth)
	}
	if loopCounters == nil && plan.preparedSearch == nil {
		loopCounters = &columnVectorGraphNativeSearchLoopCounters{}
	}
	countLoopEdges := loopCounters != nil
	if plan.preparedSearch != nil {
		stats.PreparedGraphSearchViews = 1
		if statsMode.minimal() {
			preparedMinimalCounters = newColumnVectorGraphPreparedMinimalSearchCounters(plan.preparedSearch)
		}
	}
	hotStats := &stats
	if preparedMinimalCounters != nil {
		hotStats = nil
	}
	var singleBlockView *columnVectorGraphBlockView
	if plan.physicalReader != nil && len(plan.physicalReader.ranges) == 1 && (plan.scoreSource.vectorKind == columnVectorGraphSearchVectorSourceGraphRows || plan.scoreSource.normKind == columnVectorGraphSearchNormSourceGraphRows) {
		singleBlockView, err = plan.blockViewForAssetOrdinal(0)
		if err != nil {
			return nil, stats, err
		}
	}
	visitMarks := scratch.visitMarks
	visitEpoch := scratch.visitEpoch
	entryOrdinal, ok := columnVectorGraphNextCandidateSeed(0, rowCount, candidateRows, hasCandidateRows, visitMarks, visitEpoch)
	if !ok {
		return scratch.results, stats, nil
	}
	maxLayer, err := r.maxAdjacencyLayer(plan, singleBlockView, entryOrdinal, scratch, hotStats, preparedMinimalCounters)
	if err != nil {
		return nil, stats, err
	}
	for layer := maxLayer; layer > 0; layer-- {
		entryOrdinal, err = r.greedyNearestAtLayer(plan, singleBlockView, query, queryInvNorm, entryOrdinal, layer, candidateRows, hasCandidateRows, scratch, hotStats, countLoopEdges, &loopEdgeVisits, preparedMinimalCounters, debugCounters)
		if err != nil {
			return nil, stats, err
		}
	}
	visitMarks[entryOrdinal] = visitEpoch
	if err := r.scoreAndPushFrontierVisited(plan, singleBlockView, query, queryInvNorm, entryOrdinal, topK, scratch, hotStats, &visitedCandidates, preparedMinimalCounters, debugCounters, columnVectorGraphNativeSearchScoreContextLayer0Seed); err != nil {
		return nil, stats, err
	}
	nextSeed := 0
	if traversalMode == columnVectorGraphNativeSearchTraversalModeWavefront {
		if err := r.searchLayer0Wavefront(plan, singleBlockView, query, queryInvNorm, topK, rowCount, candidateLimit, wavefrontWidth, candidateRows, hasCandidateRows, scratch, hotStats, &stats, &visitedCandidates, preparedMinimalCounters, debugCounters, countLoopEdges, &loopEdgeVisits, &nextSeed); err != nil {
			return nil, stats, err
		}
	} else {
		for visitedCandidates < candidateLimit {
			var candidate columnVectorGraphSearchCandidate
			var ok bool
			if debugCounters != nil {
				candidate, ok = scratch.popFrontierDebug(debugCounters)
			} else {
				candidate, ok = scratch.popFrontier()
			}
			if !ok {
				seed, ok := columnVectorGraphNextCandidateSeed(nextSeed, rowCount, candidateRows, hasCandidateRows, visitMarks, visitEpoch)
				if !ok {
					break
				}
				nextSeed = seed + 1
				visitMarks[seed] = visitEpoch
				if err := r.scoreAndPushFrontierVisited(plan, singleBlockView, query, queryInvNorm, seed, topK, scratch, hotStats, &visitedCandidates, preparedMinimalCounters, debugCounters, columnVectorGraphNativeSearchScoreContextLayer0Seed); err != nil {
					return nil, stats, err
				}
				continue
			}
			adjacency, err := r.expandCandidateAdjacencyLayer(plan, singleBlockView, candidate.ordinal, 0, scratch, hotStats, preparedMinimalCounters, debugCounters)
			if err != nil {
				return nil, stats, err
			}
			if plan.scoreBatchMode.indexedEnabled() {
				for i := 0; i < len(adjacency) && visitedCandidates < candidateLimit; {
					remaining := int(candidateLimit - visitedCandidates)
					if remaining <= 0 {
						break
					}
					tileCap := len(adjacency) - i
					if tileCap > remaining {
						tileCap = remaining
					}
					scratch.scoreTileOrdinals = ensureColumnVectorGraphNativeIntScratch(scratch.scoreTileOrdinals, tileCap)
					tile := scratch.scoreTileOrdinals[:0]
					for i < len(adjacency) && len(tile) < remaining {
						neighborIndex := i
						neighbor := adjacency[i]
						i++
						if countLoopEdges {
							loopEdgeVisits++
						}
						if debugStats != nil {
							debugStats.Layer0EdgeVisits++
						}
						if uint64(neighbor) >= uint64(rowCount) {
							return nil, stats, fmt.Errorf("collections: column_graph %q ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", r.def.Name, candidate.ordinal, neighborIndex, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
						}
						neighborOrdinal := int(neighbor)
						if visitMarks[neighborOrdinal] == visitEpoch {
							if debugStats != nil {
								debugStats.VisitedMarkChecks++
								debugStats.VisitedMarkHits++
								debugStats.AlreadyVisitedSkips++
								debugStats.SkippedNeighbors++
								debugStats.Layer0AlreadyVisitedSkips++
							}
							continue
						}
						if debugStats != nil {
							debugStats.VisitedMarkChecks++
							debugStats.VisitedMarkMisses++
						}
						if !columnVectorGraphCandidateRowAllowed(candidateRows, hasCandidateRows, neighborOrdinal) {
							if debugCounters != nil {
								debugCounters.recordFilterSkip(true)
							}
							visitMarks[neighborOrdinal] = visitEpoch
							continue
						}
						visitMarks[neighborOrdinal] = visitEpoch
						tile = append(tile, neighborOrdinal)
					}
					if len(tile) == 0 {
						continue
					}
					if err := r.scoreAndPushFrontierVisitedTile(plan, singleBlockView, query, queryInvNorm, tile, topK, scratch, hotStats, &visitedCandidates, preparedMinimalCounters, debugCounters, columnVectorGraphNativeSearchScoreContextLayer0Neighbor); err != nil {
						return nil, stats, err
					}
				}
				continue
			}
			for i, neighbor := range adjacency {
				if visitedCandidates >= candidateLimit {
					break
				}
				if countLoopEdges {
					loopEdgeVisits++
				}
				if debugStats != nil {
					debugStats.Layer0EdgeVisits++
				}
				if uint64(neighbor) >= uint64(rowCount) {
					return nil, stats, fmt.Errorf("collections: column_graph %q ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", r.def.Name, candidate.ordinal, i, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
				}
				neighborOrdinal := int(neighbor)
				if visitMarks[neighborOrdinal] == visitEpoch {
					if debugStats != nil {
						debugStats.VisitedMarkChecks++
						debugStats.VisitedMarkHits++
						debugStats.AlreadyVisitedSkips++
						debugStats.SkippedNeighbors++
						debugStats.Layer0AlreadyVisitedSkips++
					}
					continue
				}
				if debugStats != nil {
					debugStats.VisitedMarkChecks++
					debugStats.VisitedMarkMisses++
				}
				if !columnVectorGraphCandidateRowAllowed(candidateRows, hasCandidateRows, neighborOrdinal) {
					if debugCounters != nil {
						debugCounters.recordFilterSkip(true)
					}
					visitMarks[neighborOrdinal] = visitEpoch
					continue
				}
				visitMarks[neighborOrdinal] = visitEpoch
				if err := r.scoreAndPushFrontierVisited(plan, singleBlockView, query, queryInvNorm, neighborOrdinal, topK, scratch, hotStats, &visitedCandidates, preparedMinimalCounters, debugCounters, columnVectorGraphNativeSearchScoreContextLayer0Neighbor); err != nil {
					return nil, stats, err
				}
			}
		}
	}

	if len(scratch.top) == 0 {
		return scratch.results, stats, nil
	}
	if opts.OmitResultMaterialization {
		stats.BlockViewHits = plan.hits
		stats.BlockViewMisses = plan.misses
		stats.BlockViewBuilds = plan.builds
		for _, candidate := range scratch.top {
			scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{
				Ordinal: candidate.ordinal,
				Score:   candidate.score,
			})
		}
		return scratch.results, stats, nil
	}
	if err := r.fetchTopSearchResults(plan, singleBlockView, scratch, &stats); err != nil {
		return nil, stats, err
	}
	return scratch.results, stats, nil
}

func (r *columnVectorGraphPhysicalRowReader) searchLayer0Wavefront(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, topK int, rowCount int, candidateLimit uint64, wavefrontWidth int, candidateRows typedcolumn.RowSelection, hasCandidateRows bool, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, wavefrontStats *columnVectorGraphNativeSearchStats, visitedCandidates *uint64, preparedMinimal *columnVectorGraphPreparedMinimalSearchCounters, debugCounters *columnVectorGraphNativeSearchDebugCounters, countLoopEdges bool, loopEdgeVisits *uint64, nextSeed *int) error {
	if wavefrontWidth < 2 {
		return errColumnVectorGraphNativeSearchWavefrontWidthInvalid
	}
	visitMarks := scratch.visitMarks
	visitEpoch := scratch.visitEpoch
	for *visitedCandidates < candidateLimit {
		scratch.wavefrontCandidates = ensureColumnVectorGraphNativeCandidateScratch(scratch.wavefrontCandidates, wavefrontWidth)
		wave := scratch.wavefrontCandidates[:0]
		for len(wave) < wavefrontWidth && *visitedCandidates < candidateLimit {
			var candidate columnVectorGraphSearchCandidate
			var ok bool
			if debugCounters != nil {
				candidate, ok = scratch.popFrontierDebug(debugCounters)
			} else {
				candidate, ok = scratch.popFrontier()
			}
			if !ok {
				if len(wave) > 0 {
					break
				}
				seed, seedOK := columnVectorGraphNextCandidateSeed(*nextSeed, rowCount, candidateRows, hasCandidateRows, visitMarks, visitEpoch)
				if !seedOK {
					break
				}
				*nextSeed = seed + 1
				visitMarks[seed] = visitEpoch
				if err := r.scoreAndPushFrontierVisited(plan, singleBlockView, query, queryInvNorm, seed, topK, scratch, stats, visitedCandidates, preparedMinimal, debugCounters, columnVectorGraphNativeSearchScoreContextLayer0Seed); err != nil {
					return err
				}
				continue
			}
			wave = append(wave, candidate)
		}
		if len(wave) == 0 {
			if *visitedCandidates >= candidateLimit {
				break
			}
			seed, seedOK := columnVectorGraphNextCandidateSeed(*nextSeed, rowCount, candidateRows, hasCandidateRows, visitMarks, visitEpoch)
			if !seedOK {
				break
			}
			*nextSeed = seed + 1
			visitMarks[seed] = visitEpoch
			if err := r.scoreAndPushFrontierVisited(plan, singleBlockView, query, queryInvNorm, seed, topK, scratch, stats, visitedCandidates, preparedMinimal, debugCounters, columnVectorGraphNativeSearchScoreContextLayer0Seed); err != nil {
				return err
			}
			continue
		}
		remaining := int(candidateLimit - *visitedCandidates)
		if remaining <= 0 {
			break
		}
		scratch.scoreTileOrdinals = ensureColumnVectorGraphNativeIntScratch(scratch.scoreTileOrdinals, remaining)
		tile := scratch.scoreTileOrdinals[:0]
		for _, candidate := range wave {
			if len(tile) >= remaining {
				break
			}
			adjacency, err := r.expandCandidateAdjacencyLayer(plan, singleBlockView, candidate.ordinal, 0, scratch, stats, preparedMinimal, debugCounters)
			if err != nil {
				return err
			}
			for i, neighbor := range adjacency {
				if len(tile) >= remaining {
					break
				}
				if countLoopEdges {
					(*loopEdgeVisits)++
				}
				if debugCounters != nil {
					debugCounters.recordLayer0Edge()
				}
				if uint64(neighbor) >= uint64(rowCount) {
					return fmt.Errorf("collections: column_graph %q ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", r.def.Name, candidate.ordinal, i, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
				}
				neighborOrdinal := int(neighbor)
				if visitMarks[neighborOrdinal] == visitEpoch {
					if debugCounters != nil {
						debugCounters.recordVisitedMark(true)
						debugCounters.recordAlreadyVisitedSkip()
					}
					continue
				}
				if debugCounters != nil {
					debugCounters.recordVisitedMark(false)
				}
				if !columnVectorGraphCandidateRowAllowed(candidateRows, hasCandidateRows, neighborOrdinal) {
					if debugCounters != nil {
						debugCounters.recordFilterSkip(true)
					}
					visitMarks[neighborOrdinal] = visitEpoch
					continue
				}
				visitMarks[neighborOrdinal] = visitEpoch
				tile = append(tile, neighborOrdinal)
			}
		}
		if wavefrontStats != nil {
			wavefrontStats.WavefrontRounds++
			wavefrontStats.WavefrontCandidatePops += uint64(len(wave))
			wavefrontStats.WavefrontStagedNeighbors += uint64(len(tile))
			if uint64(len(tile)) > wavefrontStats.WavefrontMaxTileSize {
				wavefrontStats.WavefrontMaxTileSize = uint64(len(tile))
			}
		}
		if len(tile) == 0 {
			continue
		}
		if err := r.scoreAndPushFrontierVisitedTile(plan, singleBlockView, query, queryInvNorm, tile, topK, scratch, stats, visitedCandidates, preparedMinimal, debugCounters, columnVectorGraphNativeSearchScoreContextLayer0Neighbor); err != nil {
			return err
		}
	}
	return nil
}

func ensureColumnVectorGraphNativeCandidateScratch(dst []columnVectorGraphSearchCandidate, target int) []columnVectorGraphSearchCandidate {
	if cap(dst) < target {
		return make([]columnVectorGraphSearchCandidate, 0, target)
	}
	return dst[:0]
}

func columnVectorGraphSearchCandidateRows(opts columnVectorGraphNativeSearchOptions, rowCount int) (typedcolumn.RowSelection, bool, error) {
	if !opts.HasCandidateRows {
		return typedcolumn.RowSelection{}, false, nil
	}
	if opts.CandidateRows.Rows() != rowCount {
		return typedcolumn.RowSelection{}, false, fmt.Errorf("selection rows=%d want graph rows=%d", opts.CandidateRows.Rows(), rowCount)
	}
	return opts.CandidateRows, true, nil
}

func composeColumnVectorGraphCandidateRowSelection(rowCount int, predicate *typedcolumn.RowSelection, visibility *typedcolumn.RowSelection, scratch *typedcolumn.RowSelectionScratch) (typedcolumn.RowSelection, bool, error) {
	if predicate == nil && visibility == nil {
		selection, err := typedcolumn.NewAllRowSelection(rowCount)
		return selection, false, err
	}
	selection, err := typedcolumn.ComposeRowSelectionsInto(rowCount, typedcolumn.RowSelectionComponents{Predicate: predicate, Visibility: visibility}, scratch)
	return selection, true, err
}

func columnVectorGraphCandidateRowAllowed(selection typedcolumn.RowSelection, hasSelection bool, ordinal int) bool {
	return !hasSelection || selection.Contains(ordinal)
}

func columnVectorGraphNextCandidateSeed(start int, rowCount int, selection typedcolumn.RowSelection, hasSelection bool, visitMarks []uint64, visitEpoch uint64) (int, bool) {
	if start < 0 {
		start = 0
	}
	for ordinal := start; ordinal < rowCount; ordinal++ {
		if len(visitMarks) > ordinal && visitMarks[ordinal] == visitEpoch {
			continue
		}
		if !columnVectorGraphCandidateRowAllowed(selection, hasSelection, ordinal) {
			continue
		}
		return ordinal, true
	}
	return 0, false
}

func (r *columnVectorGraphPhysicalRowReader) maxAdjacencyLayer(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, preparedMinimal *columnVectorGraphPreparedMinimalSearchCounters) (int, error) {
	if plan != nil && plan.preparedSearch != nil {
		layer, counters, err := plan.preparedSearch.maxAdjacencyLayerForOrdinal(ordinal)
		if preparedMinimal != nil {
			preparedMinimal.recordAdjacencyCounterSnapshot(counters)
		} else {
			recordColumnVectorGraphAdjacencySourceCounterSnapshotStats(stats, counters)
		}
		return layer, err
	}
	if layer, _, counters, fallbackReason, ok := r.maxDirectAdjacencyLayerForOrdinal(ordinal); ok {
		recordColumnVectorGraphAdjacencySourceCounterSnapshotStats(stats, counters)
		return layer, nil
	} else if fallbackReason != "" {
		recordColumnVectorGraphAdjacencySourceCounterSnapshotStats(stats, counters)
		recordColumnVectorGraphAdjacencyFallbackReasonStats(stats, fallbackReason)
		if stats != nil {
			stats.AdjacencySourceFallbacks++
		}
	}
	adjacency, direct, err := r.rawCandidateAdjacencyWithDirectView(plan, singleBlockView, ordinal, scratch)
	if err != nil {
		return 0, err
	}
	recordColumnVectorGraphAdjacencySourceStats(stats, len(adjacency), direct)
	return columnVectorGraphAdjacencyMaxLayer(adjacency)
}

func (r *columnVectorGraphPhysicalRowReader) greedyNearestAtLayer(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, entryOrdinal int, layer int, candidateRows typedcolumn.RowSelection, hasCandidateRows bool, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, countLoopEdges bool, loopEdgeVisits *uint64, preparedMinimal *columnVectorGraphPreparedMinimalSearchCounters, debugCounters *columnVectorGraphNativeSearchDebugCounters) (int, error) {
	best := entryOrdinal
	bestScore, err := r.scoreOrdinal(plan, singleBlockView, query, queryInvNorm, best, scratch, stats)
	if err != nil {
		return 0, err
	}
	if debugCounters != nil {
		debugCounters.recordScore(columnVectorGraphNativeSearchScoreContextUpperEntry, best)
	}
	if preparedMinimal != nil {
		preparedMinimal.recordPreparedScores(1, false, true)
	}
	changed := true
	for changed {
		changed = false
		adjacency, err := r.expandCandidateAdjacencyLayer(plan, singleBlockView, best, layer, scratch, stats, preparedMinimal, debugCounters)
		if err != nil {
			return 0, err
		}
		if plan.scoreBatchMode.indexedEnabled() {
			scratch.scoreTileOrdinals = ensureColumnVectorGraphNativeIntScratch(scratch.scoreTileOrdinals, len(adjacency))
			tile := scratch.scoreTileOrdinals[:0]
			for i, neighbor := range adjacency {
				if countLoopEdges {
					(*loopEdgeVisits)++
				}
				if debugCounters != nil {
					debugCounters.recordUpperLayerEdge()
				}
				if uint64(neighbor) >= uint64(r.RowCount()) {
					return 0, fmt.Errorf("collections: column_graph %q ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", r.def.Name, best, i, neighbor, r.RowCount(), errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
				}
				neighborOrdinal := int(neighbor)
				if !columnVectorGraphCandidateRowAllowed(candidateRows, hasCandidateRows, neighborOrdinal) {
					if debugCounters != nil {
						debugCounters.recordFilterSkip(false)
					}
					continue
				}
				tile = append(tile, neighborOrdinal)
			}
			if len(tile) == 0 {
				continue
			}
			scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(tile))
			scores, err := r.scoreOrdinals(plan, singleBlockView, query, queryInvNorm, tile, scratch.scoreTileScores, scratch, stats)
			if err != nil {
				return 0, err
			}
			if debugCounters != nil {
				debugCounters.recordScores(columnVectorGraphNativeSearchScoreContextUpperNeighbor, tile)
			}
			if preparedMinimal != nil {
				plan.recordPreparedMinimalScoreBatch(preparedMinimal, tile)
			}
			for i, neighborOrdinal := range tile {
				score := scores[i]
				if score > bestScore || (score == bestScore && neighborOrdinal < best) {
					best = neighborOrdinal
					bestScore = score
					changed = true
				}
			}
			continue
		}
		for i, neighbor := range adjacency {
			if countLoopEdges {
				(*loopEdgeVisits)++
			}
			if debugCounters != nil {
				debugCounters.recordUpperLayerEdge()
			}
			if uint64(neighbor) >= uint64(r.RowCount()) {
				return 0, fmt.Errorf("collections: column_graph %q ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", r.def.Name, best, i, neighbor, r.RowCount(), errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
			neighborOrdinal := int(neighbor)
			if !columnVectorGraphCandidateRowAllowed(candidateRows, hasCandidateRows, neighborOrdinal) {
				if debugCounters != nil {
					debugCounters.recordFilterSkip(false)
				}
				continue
			}
			score, err := r.scoreOrdinal(plan, singleBlockView, query, queryInvNorm, neighborOrdinal, scratch, stats)
			if err != nil {
				return 0, err
			}
			if debugCounters != nil {
				debugCounters.recordScore(columnVectorGraphNativeSearchScoreContextUpperNeighbor, neighborOrdinal)
			}
			if preparedMinimal != nil {
				preparedMinimal.recordPreparedScores(1, false, true)
			}
			if score > bestScore || (score == bestScore && neighborOrdinal < best) {
				best = neighborOrdinal
				bestScore = score
				changed = true
			}
		}
	}
	return best, nil
}

func (r *columnVectorGraphPhysicalRowReader) fetchTopSearchResults(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	if plan != nil && plan.preparedSearch != nil {
		return r.fetchTopPreparedSearchResults(plan.preparedSearch, plan, scratch, stats)
	}
	n := len(scratch.top)
	scratch.resultOrder = scratch.resultOrder[:n]
	scratch.resultOrdinals = scratch.resultOrdinals[:n]
	for i := 0; i < n; i++ {
		scratch.resultOrder[i] = i
	}
	sortColumnVectorGraphResultOrderByOrdinal(scratch.resultOrder, scratch.top)
	for fetchPos, topIndex := range scratch.resultOrder {
		scratch.resultOrdinals[fetchPos] = scratch.top[topIndex].ordinal
	}
	scratch.resultRowRefs = scratch.resultRowRefs[:n]
	scratch.resultHasRefs = scratch.resultHasRefs[:n]
	clear(scratch.resultRowRefs)
	clear(scratch.resultHasRefs)
	resultIDStateValidationFailure := false
	for resultPos, topIndex := range scratch.resultOrder {
		ordinal := scratch.resultOrdinals[resultPos]
		id, fromDocumentIDState := r.documentIDForOrdinal(ordinal)
		if !fromDocumentIDState {
			if r.documentIDStateFallbackReason != "" {
				resultIDStateValidationFailure = true
			}
			if plan == nil || plan.physicalReader == nil {
				return fmt.Errorf("collections: column_graph %q result-id graph-row fallback unavailable for ordinal=%d", r.def.Name, ordinal)
			}
			view := singleBlockView
			rowIndex := ordinal
			if view == nil {
				refView, ref, err := plan.blockViewForOrdinal(ordinal)
				if err != nil {
					return err
				}
				view = refView
				rowIndex = ref.rowIndex
			}
			var err error
			id, err = view.id(rowIndex)
			if err != nil {
				return err
			}
			stats.ResultIDGraphFallbacks++
			stats.GraphRowFallbacks++
		} else {
			stats.ResultIDTypedBytesState++
		}
		if cap(scratch.idBuffers[topIndex]) < len(id) {
			scratch.idBuffers[topIndex] = make([]byte, len(id))
		} else if columnVectorGraphNativeScratchCapOversized(cap(scratch.idBuffers[topIndex]), len(id)) {
			scratch.idBuffers[topIndex] = make([]byte, len(id))
		}
		scratch.idBuffers[topIndex] = scratch.idBuffers[topIndex][:len(id)]
		copy(scratch.idBuffers[topIndex], id)
		if rowRef, ok := r.rowRefForOrdinal(ordinal); ok {
			rowRef.DocumentID = scratch.idBuffers[topIndex]
			scratch.resultRowRefs[topIndex] = rowRef
			scratch.resultHasRefs[topIndex] = true
			stats.RowRefStateResultRefs++
		}
	}
	stats.ResultFetches += uint64(n)
	if resultIDStateValidationFailure {
		stats.ResultIDStateValidationFailures++
	}
	stats.BlockViewHits = plan.hits
	stats.BlockViewMisses = plan.misses
	stats.BlockViewBuilds = plan.builds
	for i, candidate := range scratch.top {
		scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{
			Ordinal:   candidate.ordinal,
			ID:        scratch.idBuffers[i],
			RowRef:    scratch.resultRowRefs[i],
			HasRowRef: scratch.resultHasRefs[i],
			Score:     candidate.score,
		})
	}
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) fetchTopPreparedSearchResults(view *columnVectorGraphPreparedSearchView, plan *columnVectorGraphSearchPlan, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	if view == nil || !view.ready() {
		return errors.New("collections: column_graph prepared result view is unavailable")
	}
	n := len(scratch.top)
	scratch.resultOrder = scratch.resultOrder[:n]
	scratch.resultOrdinals = scratch.resultOrdinals[:n]
	for i := 0; i < n; i++ {
		scratch.resultOrder[i] = i
	}
	sortColumnVectorGraphResultOrderByOrdinal(scratch.resultOrder, scratch.top)
	for fetchPos, topIndex := range scratch.resultOrder {
		scratch.resultOrdinals[fetchPos] = scratch.top[topIndex].ordinal
	}
	scratch.resultRowRefs = scratch.resultRowRefs[:n]
	scratch.resultHasRefs = scratch.resultHasRefs[:n]
	clear(scratch.resultRowRefs)
	clear(scratch.resultHasRefs)
	for resultPos, topIndex := range scratch.resultOrder {
		ordinal := scratch.resultOrdinals[resultPos]
		id, ok := view.documentIDForOrdinal(ordinal)
		if !ok {
			return fmt.Errorf("collections: column_graph %q prepared result-id unavailable for ordinal=%d", r.def.Name, ordinal)
		}
		if cap(scratch.idBuffers[topIndex]) < len(id) {
			scratch.idBuffers[topIndex] = make([]byte, len(id))
		} else if columnVectorGraphNativeScratchCapOversized(cap(scratch.idBuffers[topIndex]), len(id)) {
			scratch.idBuffers[topIndex] = make([]byte, len(id))
		}
		scratch.idBuffers[topIndex] = scratch.idBuffers[topIndex][:len(id)]
		copy(scratch.idBuffers[topIndex], id)
		stats.ResultIDTypedBytesState++
		if rowRef, ok := view.rowRefForOrdinal(ordinal); ok {
			rowRef.DocumentID = scratch.idBuffers[topIndex]
			scratch.resultRowRefs[topIndex] = rowRef
			scratch.resultHasRefs[topIndex] = true
			stats.RowRefStateResultRefs++
		} else {
			return fmt.Errorf("collections: column_graph %q prepared row-ref unavailable for ordinal=%d", r.def.Name, ordinal)
		}
	}
	stats.ResultFetches += uint64(n)
	stats.BlockViewHits = plan.hits
	stats.BlockViewMisses = plan.misses
	stats.BlockViewBuilds = plan.builds
	for i, candidate := range scratch.top {
		scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{
			Ordinal:   candidate.ordinal,
			ID:        scratch.idBuffers[i],
			RowRef:    scratch.resultRowRefs[i],
			HasRowRef: scratch.resultHasRefs[i],
			Score:     candidate.score,
		})
	}
	return nil
}

func sortColumnVectorGraphResultOrderByOrdinal(order []int, top []columnVectorGraphSearchCandidate) {
	if len(order) <= columnVectorGraphNativeResultOrderInsertionSortLimit {
		for i := 1; i < len(order); i++ {
			item := order[i]
			ordinal := top[item].ordinal
			j := i - 1
			for j >= 0 && top[order[j]].ordinal > ordinal {
				order[j+1] = order[j]
				j--
			}
			order[j+1] = item
		}
		return
	}
	sort.Slice(order, func(i, j int) bool {
		return top[order[i]].ordinal < top[order[j]].ordinal
	})
}

func (r *columnVectorGraphPhysicalRowReader) scoreAndPushFrontierVisited(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinal, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, visitedCandidates *uint64, preparedMinimal *columnVectorGraphPreparedMinimalSearchCounters, debugCounters *columnVectorGraphNativeSearchDebugCounters, scoreContext columnVectorGraphNativeSearchScoreContext) error {
	score, err := r.scoreOrdinal(plan, singleBlockView, query, queryInvNorm, ordinal, scratch, stats)
	if err != nil {
		return err
	}
	if debugCounters != nil {
		debugCounters.recordScore(scoreContext, ordinal)
	}
	if preparedMinimal != nil {
		preparedMinimal.recordPreparedScores(1, false, true)
	}
	(*visitedCandidates)++
	candidate := columnVectorGraphSearchCandidate{
		ordinal: ordinal,
		score:   score,
	}
	if debugCounters != nil {
		scratch.insertTopDebug(topK, candidate, debugCounters)
		scratch.pushFrontierDebug(candidate, debugCounters)
	} else {
		scratch.insertTop(topK, candidate)
		scratch.pushFrontier(candidate)
	}
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) scoreAndPushFrontierVisitedTile(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinals []int, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, visitedCandidates *uint64, preparedMinimal *columnVectorGraphPreparedMinimalSearchCounters, debugCounters *columnVectorGraphNativeSearchDebugCounters, scoreContext columnVectorGraphNativeSearchScoreContext) error {
	if len(ordinals) == 0 {
		return nil
	}
	scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(ordinals))
	scores, err := r.scoreOrdinals(plan, singleBlockView, query, queryInvNorm, ordinals, scratch.scoreTileScores, scratch, stats)
	if err != nil {
		return err
	}
	if debugCounters != nil {
		debugCounters.recordScores(scoreContext, ordinals)
	}
	if preparedMinimal != nil {
		plan.recordPreparedMinimalScoreBatch(preparedMinimal, ordinals)
	}
	for i, ordinal := range ordinals {
		(*visitedCandidates)++
		candidate := columnVectorGraphSearchCandidate{ordinal: ordinal, score: scores[i]}
		if debugCounters != nil {
			scratch.insertTopDebug(topK, candidate, debugCounters)
			scratch.pushFrontierDebug(candidate, debugCounters)
		} else {
			scratch.insertTop(topK, candidate)
			scratch.pushFrontier(candidate)
		}
	}
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) scoreOrdinal(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if plan != nil && plan.preparedSearch != nil {
		return plan.preparedSearch.scoreOrdinal(plan, query, queryInvNorm, ordinal, stats)
	}
	if plan != nil && plan.scoreSource.reader != nil {
		return plan.scoreSource.scoreOrdinal(plan, singleBlockView, query, queryInvNorm, ordinal, scratch, stats)
	}
	return r.scoreOrdinalLegacy(plan, singleBlockView, query, queryInvNorm, ordinal, scratch, stats)
}

func (r *columnVectorGraphPhysicalRowReader) scoreOrdinals(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if plan != nil && plan.preparedSearch != nil {
		return plan.preparedSearch.scoreOrdinals(plan, query, queryInvNorm, ordinals, dst, scratch, stats)
	}
	if plan == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	return plan.scoreSource.scoreOrdinals(plan, singleBlockView, query, queryInvNorm, ordinals, dst, scratch, stats)
}

func (r *columnVectorGraphPhysicalRowReader) scoreOrdinalLegacy(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, query []float32, queryInvNorm float32, ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if plan == nil || plan.physicalReader == nil {
		return 0, fmt.Errorf("collections: column_graph %q graph-row score fallback unavailable", r.def.Name)
	}
	view := singleBlockView
	rowIndex := ordinal
	if view == nil {
		refView, ref, err := plan.blockViewForOrdinal(ordinal)
		if err != nil {
			return 0, err
		}
		view = refView
		rowIndex = ref.rowIndex
	}
	if stats != nil {
		recordColumnVectorGraphScoreBatchStats(stats, 1, false, true)
		stats.VisitedNodes++
		stats.BlockViewHits = plan.hits
		stats.BlockViewMisses = plan.misses
		stats.BlockViewBuilds = plan.builds
	}
	var vector []float32
	if typedVector, outcome, fallbackReason, ok := r.typedVectorForOrdinal(ordinal); ok {
		vector = typedVector
		recordColumnVectorGraphVectorSourceStats(stats, outcome, fallbackReason)
	} else {
		scratch.scoreScratch.Float32Values = scratch.scoreScratch.Float32Values[:0]
		var vectorScratch []float32
		vector, vectorScratch = view.vectorUnchecked(rowIndex, scratch.scoreScratch.Float32Values)
		scratch.scoreScratch.Float32Values = vectorScratch
		if stats != nil {
			stats.VectorScratchDecodes++
			stats.GraphRowFallbacks++
		}
	}
	invNorm, normOutcome, normFallbackReason, normOK := r.invNormForOrdinal(ordinal)
	if normOK {
		recordColumnVectorGraphInvNormSourceStats(stats, normOutcome, normFallbackReason)
	} else {
		if normFallbackReason != "" {
			recordColumnVectorGraphInvNormFallbackReasonStats(stats, normFallbackReason)
			if stats != nil {
				stats.NormSourceFallbacks++
			}
		}
		var err error
		invNorm, err = view.legacyInvNorm(rowIndex)
		if stats != nil {
			stats.GraphRowFallbacks++
		}
		if err != nil {
			return 0, err
		}
	}
	if stats != nil {
		stats.CandidateFetches++
		stats.VectorBytesRead += uint64(len(vector)) * 4
		stats.NormBytesRead += 4
	}
	score, err := columnVectorGraphNativeCosineScoreVector(query, queryInvNorm, ordinal, vector, invNorm)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, fmt.Errorf("collections: column_graph %q candidate ordinal=%d cosine score is not finite", r.def.Name, ordinal)
	}
	return score, nil
}

func (r *columnVectorGraphPhysicalRowReader) expandCandidateAdjacencyLayer(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, ordinal int, layer int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, preparedMinimal *columnVectorGraphPreparedMinimalSearchCounters, debugCounters *columnVectorGraphNativeSearchDebugCounters) ([]uint32, error) {
	if plan != nil && plan.preparedSearch != nil {
		layerAdjacency, outcome, err := plan.preparedSearch.adjacencyLayerForOrdinal(ordinal, layer)
		if err != nil {
			return nil, err
		}
		if debugCounters != nil {
			debugCounters.recordNeighborTile(len(layerAdjacency))
		}
		if preparedMinimal != nil {
			preparedMinimal.recordPreparedAdjacency(len(layerAdjacency))
		} else if stats != nil {
			stats.ExpansionFetches++
			stats.AdjacencyExpansions++
			recordColumnVectorGraphAdjacencySourceOutcomeStats(stats, len(layerAdjacency), outcome)
			stats.BlockViewHits = plan.hits
			stats.BlockViewMisses = plan.misses
			stats.BlockViewBuilds = plan.builds
		}
		return layerAdjacency, nil
	}
	if layerAdjacency, outcome, fallbackReason, ok := r.directAdjacencyLayerForOrdinal(ordinal, layer); ok {
		if debugCounters != nil {
			debugCounters.recordNeighborTile(len(layerAdjacency))
		}
		if stats != nil {
			stats.ExpansionFetches++
			stats.AdjacencyExpansions++
			recordColumnVectorGraphAdjacencySourceOutcomeStats(stats, len(layerAdjacency), outcome)
			stats.BlockViewHits = plan.hits
			stats.BlockViewMisses = plan.misses
			stats.BlockViewBuilds = plan.builds
		}
		return layerAdjacency, nil
	} else if fallbackReason != "" {
		recordColumnVectorGraphAdjacencyFallbackReasonStats(stats, fallbackReason)
		if stats != nil {
			stats.AdjacencySourceFallbacks++
		}
	}
	adjacency, direct, err := r.rawCandidateAdjacencyWithDirectView(plan, singleBlockView, ordinal, scratch)
	if err != nil {
		return nil, err
	}
	layerAdjacency, err := columnVectorGraphAdjacencyLayer(adjacency, layer)
	if err != nil {
		return nil, fmt.Errorf("collections: column_graph %q ordinal=%d malformed adjacency layer=%d: %w", r.def.Name, ordinal, layer, err)
	}
	if debugCounters != nil {
		debugCounters.recordNeighborTile(len(layerAdjacency))
	}
	if stats != nil {
		stats.ExpansionFetches++
		stats.AdjacencyExpansions++
		recordColumnVectorGraphAdjacencySourceStats(stats, len(adjacency), direct)
		stats.BlockViewHits = plan.hits
		stats.BlockViewMisses = plan.misses
		stats.BlockViewBuilds = plan.builds
	}
	return layerAdjacency, nil
}

func recordColumnVectorGraphInvNormSourceStats(stats *columnVectorGraphNativeSearchStats, outcome columnVectorGraphInvNormStateOutcome, fallbackReason typeddecode.Reason) {
	if stats == nil {
		return
	}
	switch outcome {
	case columnVectorGraphInvNormStateOutcomeMmapDirect:
		stats.NormMmapDirectViews++
		stats.NormDirectViews++
	case columnVectorGraphInvNormStateOutcomeHeapCopyTypedView:
		stats.NormHeapCopyTypedViews++
	case columnVectorGraphInvNormStateOutcomeScratchDecode:
		stats.NormScratchDecodes++
	default:
		stats.NormScratchDecodes++
	}
	recordColumnVectorGraphInvNormFallbackReasonStats(stats, fallbackReason)
}

func recordColumnVectorGraphInvNormFallbackReasonStats(stats *columnVectorGraphNativeSearchStats, reason typeddecode.Reason) {
	if stats == nil || reason == "" || reason == typeddecode.ReasonSupported {
		return
	}
	switch reason {
	case typeddecode.ReasonAbsoluteOffsetUnaligned, typeddecode.ReasonUnaligned:
		stats.NormAbsoluteOffsetUnaligned++
	case typeddecode.ReasonActualPointerUnaligned:
		stats.NormActualPointerUnaligned++
	case typeddecode.ReasonNilHandle, typeddecode.ReasonStaleHandle:
		stats.NormStaleHandles++
	case typeddecode.ReasonNotWriterCertified, typeddecode.ReasonWrongEndian, typeddecode.ReasonLengthMultipleMismatch, typeddecode.ReasonPayloadLengthMismatch, typeddecode.ReasonRowCountMismatch, typeddecode.ReasonDimensionMismatch, typeddecode.ReasonCompressed, typeddecode.ReasonNullableWrapper, typeddecode.ReasonValidationFailed:
		stats.NormValidationFailures++
	}
}

func recordColumnVectorGraphVectorSourceStats(stats *columnVectorGraphNativeSearchStats, outcome columnVectorGraphTypedColumnVectorOutcome, fallbackReason typeddecode.Reason) {
	if stats == nil {
		return
	}
	switch outcome {
	case columnVectorGraphTypedColumnVectorOutcomeMmapDirect:
		stats.VectorMmapDirectViews++
		stats.VectorDirectViews++
	case columnVectorGraphTypedColumnVectorOutcomeHeapCopyTypedView:
		stats.VectorHeapCopyTypedViews++
	case columnVectorGraphTypedColumnVectorOutcomeScratchDecode:
		stats.VectorScratchDecodes++
	default:
		stats.VectorScratchDecodes++
	}
	recordColumnVectorGraphVectorFallbackReasonStats(stats, fallbackReason)
}

func recordColumnVectorGraphVectorFallbackReasonStats(stats *columnVectorGraphNativeSearchStats, reason typeddecode.Reason) {
	if stats == nil || reason == "" || reason == typeddecode.ReasonSupported {
		return
	}
	switch reason {
	case typeddecode.ReasonAbsoluteOffsetUnaligned, typeddecode.ReasonUnaligned:
		stats.VectorAbsoluteOffsetUnaligned++
	case typeddecode.ReasonActualPointerUnaligned:
		stats.VectorActualPointerUnaligned++
	case typeddecode.ReasonStaleHandle:
		stats.VectorStaleHandles++
	case typeddecode.ReasonNotWriterCertified, typeddecode.ReasonWrongEndian, typeddecode.ReasonLengthMultipleMismatch, typeddecode.ReasonPayloadLengthMismatch, typeddecode.ReasonRowCountMismatch, typeddecode.ReasonDimensionMismatch, typeddecode.ReasonCompressed, typeddecode.ReasonNullableWrapper, typeddecode.ReasonValidationFailed:
		stats.VectorCertificationFailures++
	}
}

func recordColumnVectorGraphAdjacencySourceStats(stats *columnVectorGraphNativeSearchStats, adjacencyLen int, direct bool) {
	if stats == nil {
		return
	}
	stats.AdjacencyBytesRead += uint64(adjacencyLen) * 4
	stats.AdjacencyLegacyFallbacks++
	stats.GraphRowFallbacks++
	if adjacencyLen == 0 {
		return
	}
	if direct {
		stats.AdjacencyDirectViews++
		return
	}
	stats.AdjacencyScratchDecodes++
}

func recordColumnVectorGraphAdjacencySourceOutcomeStats(stats *columnVectorGraphNativeSearchStats, adjacencyLen int, outcome columnVectorGraphLayer0AdjacencySourceOutcome) {
	var counters columnVectorGraphAdjacencySourceCounterSnapshot
	counters.addOutcome(adjacencyLen, outcome)
	recordColumnVectorGraphAdjacencySourceCounterSnapshotStats(stats, counters)
}

func recordColumnVectorGraphAdjacencySourceCounterSnapshotStats(stats *columnVectorGraphNativeSearchStats, counters columnVectorGraphAdjacencySourceCounterSnapshot) {
	if stats == nil {
		return
	}
	stats.AdjacencyBytesRead += counters.AdjacencyBytesRead
	stats.AdjacencyDirectViews += counters.AdjacencyDirectViews
	stats.AdjacencyMmapDirectViews += counters.AdjacencyMmapDirectViews
	stats.AdjacencyHeapCopyTypedViews += counters.AdjacencyHeapCopyTypedViews
	stats.AdjacencyScratchDecodes += counters.AdjacencyScratchDecodes
	stats.AdjacencyPreparedCSRDirectViews += counters.AdjacencyPreparedCSRDirectViews
	stats.AdjacencyPreparedCSRMmapDirectViews += counters.AdjacencyPreparedCSRMmapDirectViews
	stats.AdjacencyTypedListDirectViews += counters.AdjacencyTypedListDirectViews
	stats.AdjacencyTypedListMmapDirectViews += counters.AdjacencyTypedListMmapDirectViews
	stats.AdjacencyTypedListHeapCopyTypedViews += counters.AdjacencyTypedListHeapCopyTypedViews
	stats.AdjacencyTypedListScratchDecodes += counters.AdjacencyTypedListScratchDecodes
}

func recordColumnVectorGraphAdjacencyFallbackReasonStats(stats *columnVectorGraphNativeSearchStats, reason typeddecode.Reason) {
	if stats == nil || reason == "" || reason == typeddecode.ReasonSupported {
		return
	}
	switch reason {
	case typeddecode.ReasonAbsoluteOffsetUnaligned, typeddecode.ReasonUnaligned:
		stats.AdjacencyAbsoluteOffsetUnaligned++
	case typeddecode.ReasonActualPointerUnaligned:
		stats.AdjacencyActualPointerUnaligned++
	case typeddecode.ReasonNilHandle, typeddecode.ReasonStaleHandle:
		stats.AdjacencyStaleHandles++
	case typeddecode.ReasonValidationFailed:
		stats.AdjacencyValidationFailures++
	case typeddecode.ReasonNotWriterCertified, typeddecode.ReasonWrongEndian, typeddecode.ReasonLengthMultipleMismatch, typeddecode.ReasonPayloadLengthMismatch, typeddecode.ReasonRowCountMismatch, typeddecode.ReasonDimensionMismatch, typeddecode.ReasonCompressed, typeddecode.ReasonNullableWrapper, typeddecode.ReasonOffsetsCountMismatch, typeddecode.ReasonOffsetsStartMismatch, typeddecode.ReasonOffsetsNonMonotonic, typeddecode.ReasonOffsetsGoIntRange, typeddecode.ReasonValuesLengthMismatch:
		stats.AdjacencyCertificationFailures++
	}
}

func (r *columnVectorGraphPhysicalRowReader) maxDirectAdjacencyLayerForOrdinal(ordinal int) (int, []uint32, columnVectorGraphAdjacencySourceCounterSnapshot, typeddecode.Reason, bool) {
	if r == nil || r.adjacencyLayerSources == nil || !r.adjacencyLayerSources.allLayers {
		return 0, nil, columnVectorGraphAdjacencySourceCounterSnapshot{}, "", false
	}
	return r.adjacencyLayerSources.MaxLayerForOrdinal(ordinal)
}

func (r *columnVectorGraphPhysicalRowReader) directAdjacencyLayerForOrdinal(ordinal int, layer int) ([]uint32, columnVectorGraphLayer0AdjacencySourceOutcome, typeddecode.Reason, bool) {
	if r == nil {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, "", false
	}
	if r.adjacencyLayerSources != nil {
		return r.adjacencyLayerSources.Neighbors(layer, ordinal)
	}
	if layer == 0 && r.layer0AdjacencySource != nil {
		return r.layer0AdjacencySource.Neighbors(ordinal)
	}
	return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, "", false
}

func (r *columnVectorGraphPhysicalRowReader) layer0AdjacencyForOrdinal(ordinal int) ([]uint32, columnVectorGraphLayer0AdjacencySourceOutcome, typeddecode.Reason, bool) {
	return r.directAdjacencyLayerForOrdinal(ordinal, 0)
}

func (r *columnVectorGraphPhysicalRowReader) populateLayer0AdjacencySourceSearchStats(stats *columnVectorGraphNativeSearchStats) {
	if r == nil || stats == nil || r.adjacencyLayerSources != nil || r.layer0AdjacencySource != nil {
		return
	}
	stats.AdjacencySourceUnavailable = 1
	if r.layer0AdjacencySourceUnavailable {
		stats.AdjacencySourceFallbacks = 1
		return
	}
	if r.layer0AdjacencySourceFallbackReason != "" {
		stats.AdjacencySourceFallbacks = 1
		recordColumnVectorGraphAdjacencyFallbackReasonStats(stats, r.layer0AdjacencySourceFallbackReason)
	}
}

func (r *columnVectorGraphPhysicalRowReader) rawCandidateAdjacency(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, ordinal int, scratch *columnVectorGraphNativeSearchScratch) ([]uint32, error) {
	adjacency, _, err := r.rawCandidateAdjacencyWithDirectView(plan, singleBlockView, ordinal, scratch)
	return adjacency, err
}

func (r *columnVectorGraphPhysicalRowReader) rawCandidateAdjacencyWithDirectView(plan *columnVectorGraphSearchPlan, singleBlockView *columnVectorGraphBlockView, ordinal int, scratch *columnVectorGraphNativeSearchScratch) ([]uint32, bool, error) {
	if plan == nil || plan.physicalReader == nil {
		return nil, false, fmt.Errorf("collections: column_graph %q adjacency graph-row fallback unavailable", r.def.Name)
	}
	view := singleBlockView
	rowIndex := ordinal
	if view == nil {
		refView, ref, err := plan.blockViewForOrdinal(ordinal)
		if err != nil {
			return nil, false, err
		}
		view = refView
		rowIndex = ref.rowIndex
	}
	scratch.expandScratch.Uint32Values = scratch.expandScratch.Uint32Values[:0]
	adjacency, adjacencyScratch, direct, err := view.adjacency(rowIndex, scratch.expandScratch.Uint32Values)
	if err != nil {
		return nil, false, err
	}
	scratch.expandScratch.Uint32Values = adjacencyScratch
	return adjacency, direct, nil
}

func (s *columnVectorGraphNativeSearchScratch) markVisited(ordinal int) bool {
	if ordinal < 0 || ordinal >= len(s.visitMarks) || s.visitMarks[ordinal] == s.visitEpoch {
		return false
	}
	s.visitMarks[ordinal] = s.visitEpoch
	return true
}

func (s *columnVectorGraphNativeSearchScratch) pushFrontier(candidate columnVectorGraphSearchCandidate) {
	s.frontier = append(s.frontier, columnVectorGraphSearchCandidate{})
	s.frontierSiftUp(len(s.frontier)-1, candidate)
}

func (s *columnVectorGraphNativeSearchScratch) pushFrontierDebug(candidate columnVectorGraphSearchCandidate, debugCounters *columnVectorGraphNativeSearchDebugCounters) {
	debugCounters.stats.FrontierPushes++
	s.frontier = append(s.frontier, columnVectorGraphSearchCandidate{})
	s.frontierSiftUpDebug(len(s.frontier)-1, candidate, debugCounters)
}

func (s *columnVectorGraphNativeSearchScratch) popFrontier() (columnVectorGraphSearchCandidate, bool) {
	if len(s.frontier) == 0 {
		return columnVectorGraphSearchCandidate{}, false
	}
	lastIdx := len(s.frontier) - 1
	best := s.frontier[0]
	last := s.frontier[lastIdx]
	s.frontier = s.frontier[:lastIdx]
	if len(s.frontier) > 0 {
		s.frontierSiftDown(0, last)
	}
	return best, true
}

func (s *columnVectorGraphNativeSearchScratch) popFrontierDebug(debugCounters *columnVectorGraphNativeSearchDebugCounters) (columnVectorGraphSearchCandidate, bool) {
	if len(s.frontier) == 0 {
		debugCounters.stats.FrontierPopMisses++
		return columnVectorGraphSearchCandidate{}, false
	}
	debugCounters.stats.FrontierPops++
	lastIdx := len(s.frontier) - 1
	best := s.frontier[0]
	last := s.frontier[lastIdx]
	s.frontier = s.frontier[:lastIdx]
	if len(s.frontier) > 0 {
		s.frontierSiftDownDebug(0, last, debugCounters)
	}
	return best, true
}

func (s *columnVectorGraphNativeSearchScratch) frontierSiftUp(idx int, candidate columnVectorGraphSearchCandidate) {
	frontier := s.frontier
	for idx > 0 {
		parent := (idx - 1) / columnVectorGraphNativeFrontierHeapFanout
		parentCandidate := frontier[parent]
		if !columnVectorGraphSearchCandidateBetter(candidate, parentCandidate) {
			break
		}
		frontier[idx] = parentCandidate
		idx = parent
	}
	frontier[idx] = candidate
}

func (s *columnVectorGraphNativeSearchScratch) frontierSiftUpDebug(idx int, candidate columnVectorGraphSearchCandidate, debugCounters *columnVectorGraphNativeSearchDebugCounters) {
	frontier := s.frontier
	var steps uint64
	for idx > 0 {
		parent := (idx - 1) / columnVectorGraphNativeFrontierHeapFanout
		parentCandidate := frontier[parent]
		if !columnVectorGraphSearchCandidateBetter(candidate, parentCandidate) {
			break
		}
		steps++
		frontier[idx] = parentCandidate
		idx = parent
	}
	frontier[idx] = candidate
	debugCounters.stats.FrontierSiftUpSteps += steps
}

func (s *columnVectorGraphNativeSearchScratch) frontierSiftDown(idx int, candidate columnVectorGraphSearchCandidate) {
	frontier := s.frontier
	n := len(frontier)
	for {
		firstChild := idx*columnVectorGraphNativeFrontierHeapFanout + 1
		if firstChild >= n {
			break
		}
		child := firstChild
		childCandidate := frontier[firstChild]
		lastChild := firstChild + columnVectorGraphNativeFrontierHeapFanout
		if lastChild > n {
			lastChild = n
		}
		for next := firstChild + 1; next < lastChild; next++ {
			if columnVectorGraphSearchCandidateBetter(frontier[next], childCandidate) {
				child = next
				childCandidate = frontier[next]
			}
		}
		if !columnVectorGraphSearchCandidateBetter(childCandidate, candidate) {
			break
		}
		frontier[idx] = childCandidate
		idx = child
	}
	frontier[idx] = candidate
}

func (s *columnVectorGraphNativeSearchScratch) frontierSiftDownDebug(idx int, candidate columnVectorGraphSearchCandidate, debugCounters *columnVectorGraphNativeSearchDebugCounters) {
	frontier := s.frontier
	n := len(frontier)
	var steps uint64
	for {
		firstChild := idx*columnVectorGraphNativeFrontierHeapFanout + 1
		if firstChild >= n {
			break
		}
		child := firstChild
		childCandidate := frontier[firstChild]
		lastChild := firstChild + columnVectorGraphNativeFrontierHeapFanout
		if lastChild > n {
			lastChild = n
		}
		for next := firstChild + 1; next < lastChild; next++ {
			if columnVectorGraphSearchCandidateBetter(frontier[next], childCandidate) {
				child = next
				childCandidate = frontier[next]
			}
		}
		if !columnVectorGraphSearchCandidateBetter(childCandidate, candidate) {
			break
		}
		steps++
		frontier[idx] = childCandidate
		idx = child
	}
	frontier[idx] = candidate
	debugCounters.stats.FrontierSiftDownSteps += steps
}

func (s *columnVectorGraphNativeSearchScratch) insertTop(limit int, candidate columnVectorGraphSearchCandidate) {
	if limit <= 0 {
		return
	}
	pos := len(s.top)
	for pos > 0 && columnVectorGraphSearchCandidateBetter(candidate, s.top[pos-1]) {
		pos--
	}
	if pos >= limit {
		return
	}
	if len(s.top) < limit {
		s.top = append(s.top, columnVectorGraphSearchCandidate{})
	}
	copy(s.top[pos+1:], s.top[pos:len(s.top)-1])
	s.top[pos] = candidate
}

func (s *columnVectorGraphNativeSearchScratch) insertTopDebug(limit int, candidate columnVectorGraphSearchCandidate, debugCounters *columnVectorGraphNativeSearchDebugCounters) {
	debugCounters.stats.TopKInsertAttempts++
	if limit <= 0 {
		debugCounters.stats.TopKInsertRejections++
		return
	}
	pos := len(s.top)
	for pos > 0 && columnVectorGraphSearchCandidateBetter(candidate, s.top[pos-1]) {
		pos--
	}
	if pos >= limit {
		debugCounters.stats.TopKInsertRejections++
		return
	}
	debugCounters.stats.TopKInsertSuccesses++
	shiftSteps := len(s.top) - pos
	if len(s.top) >= limit && shiftSteps > 0 {
		shiftSteps--
	}
	debugCounters.stats.TopKShiftSteps += uint64(shiftSteps)
	if len(s.top) < limit {
		s.top = append(s.top, columnVectorGraphSearchCandidate{})
	}
	copy(s.top[pos+1:], s.top[pos:len(s.top)-1])
	s.top[pos] = candidate
}

func columnVectorGraphSearchCandidateBetter(left, right columnVectorGraphSearchCandidate) bool {
	if left.score == right.score {
		return left.ordinal < right.ordinal
	}
	return left.score > right.score
}

func recordColumnVectorGraphScoreBatchStats(stats *columnVectorGraphNativeSearchStats, tileSize int, optimized bool, scalarFallback bool) {
	if stats == nil || tileSize <= 0 {
		return
	}
	stats.ScoreBatches++
	stats.OrdinalsGrouped += uint64(tileSize)
	stats.ScoreBatchCalls++
	stats.ScoreBatchCandidates += uint64(tileSize)
	if uint64(tileSize) > stats.ScoreBatchMaxTileSize {
		stats.ScoreBatchMaxTileSize = uint64(tileSize)
	}
	if stats.BenchmarkDebugSearches != 0 {
		recordColumnVectorGraphScoreBatchDebugStats(stats, tileSize)
	}
	if optimized {
		stats.ScoreBatchOptimizedCalls++
	}
	if scalarFallback {
		stats.ScoreBatchScalarFallbackCalls++
	}
}

func columnVectorGraphNativeCosineScore(query []float32, queryInvNorm float32, row columnVectorGraphPhysicalRow) (float64, error) {
	return columnVectorGraphNativeCosineScoreVector(query, queryInvNorm, row.Ordinal, row.Vector, row.InvNorm)
}

func columnVectorGraphNativeCosineScoreVector(query []float32, queryInvNorm float32, ordinal int, vector []float32, invNorm float32) (float64, error) {
	if len(vector) != len(query) {
		return 0, fmt.Errorf("collections: column_graph candidate ordinal=%d vector dims=%d want %d: %w", ordinal, len(vector), len(query), errColumnVectorGraphNativeSearchCandidateDimensionMismatch)
	}
	dot := float64(vectorDotProductFloat32(query, vector))
	return columnVectorGraphNativeCosineScoreDot(query, queryInvNorm, ordinal, dot, vector, invNorm)
}

func columnVectorGraphNativeCosineScoreDot(query []float32, queryInvNorm float32, ordinal int, dot float64, vector []float32, invNorm float32) (float64, error) {
	if len(vector) != len(query) {
		return 0, fmt.Errorf("collections: column_graph candidate ordinal=%d vector dims=%d want %d: %w", ordinal, len(vector), len(query), errColumnVectorGraphNativeSearchCandidateDimensionMismatch)
	}
	if math.IsInf(dot, 0) || math.IsNaN(dot) {
		dot = columnVectorGraphNativeDotProductFloat64(query, vector)
	}
	score := dot * float64(queryInvNorm) * float64(invNorm)
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0, fmt.Errorf("collections: column_graph candidate ordinal=%d cosine score is not finite", ordinal)
	}
	return score, nil
}

func columnVectorGraphNativeDotProductFloat64(left, right []float32) float64 {
	var dot float64
	for i, v := range left {
		dot += float64(v) * float64(right[i])
	}
	return dot
}
