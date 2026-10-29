package collections

import (
	"bytes"
	"context"

	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"

	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"

	"github.com/snissn/gomap/TreeDB/internal/vectorops"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	defaultVectorIndexM                       = 16
	defaultVectorIndexEfConstruction          = 128
	defaultVectorIndexEfSearch                = 64
	defaultVectorIndexFetchMultiple           = 4
	defaultVectorIndexRebuildPPM              = 250_000
	defaultVectorIndexExactFilterMax          = 1024
	defaultVectorRecallBatchCells             = 1 << 20
	defaultVectorIndexLiveDeltaRows           = 128 << 10
	maxVectorIndexEagerNeighborCap            = 64
	minVectorIndexParallelReciprocalNeighbors = 17
)

const vectorIndexConstructionDecisionPairSlots = 4096

const (
	vectorIndexConstructionDecisionPlanning = iota
	vectorIndexConstructionDecisionReciprocal
	vectorIndexConstructionDecisionPhaseCount
)

// vectorIndexConstructionDecisionObserverV1 is a bounded sink for the
// production frozen-prefix constructor. Collection rebuilds install it only
// when their manager explicitly enables construction diagnostics.
type vectorIndexConstructionDecisionObserverV1 struct {
	phases [vectorIndexConstructionDecisionPhaseCount]vectorIndexConstructionDecisionPhaseV1
}

type vectorIndexConstructionDecisionPhaseV1 struct {
	digestXOR, digestSum                                          uint64
	decisions, accepted, rejected                                 uint64
	directExactFP32Rows, directExactFP32Calls                     uint64
	indexedExactFP32Rows, indexedExactFP32Calls                   uint64
	approximateScoreRows, approximateScoreCalls                   uint64
	exactFP32Dimensions, diversityPredicates, diversityCandidates uint64
	diversityComparisonsRequested, diversityComparisonsExecuted   uint64
	uniqueRowPairs, repeatedRowPairs, rowPairReplacements         uint64
	activeWallNanos                                               uint64
	saturated                                                     bool
	candidateCount, selectedCount, earlyExit, groupSize           [16]uint64
	pruneSurvivors                                                [16]uint64
	rowPairs                                                      [vectorIndexConstructionDecisionPairSlots]uint64
}

type vectorIndexConstructionDecisionContextV1 struct {
	observer   *vectorIndexConstructionDecisionObserverV1
	phase      int
	source     int
	layer      int
	dimensions int
}

// VectorIndexConstructionDecisionSnapshot is bounded, process-local evidence
// from one completed column_graph adjacency build. Planning covers frozen-prefix
// candidate search/selection; Reciprocal covers reciprocal-link pruning.
type VectorIndexConstructionDecisionSnapshot struct {
	Planning   VectorIndexConstructionDecisionPhaseSnapshot `json:"planning"`
	Reciprocal VectorIndexConstructionDecisionPhaseSnapshot `json:"reciprocal"`
}

// VectorIndexConstructionDecisionPhaseSnapshot reports exact work accounting
// for one construction phase. Row fields count scored vector rows, Calls count
// scoring invocations, and ExactFP32Dimensions is the sum of scored row widths.
// Accepted and Rejected classify diversity predicates. DiversityCandidates is
// the exact candidate count presented to selection. Comparison counts separate
// the requested selected-row fanout from comparisons executed before early exit;
// indexed kernels may score every requested row before the predicate exits.
// Approximate score rows/calls are reserved for an explicitly instrumented future scorer
// and are zero for the current exact route. Active wall nanoseconds sum observed
// function time and may exceed elapsed adjacency wall when reciprocal work runs
// concurrently. Histogram buckets are powers of two (0, 1, 2, 3-4, ...),
// capped at bucket 15. Row-pair counters describe worker-local fixed 4096-slot
// replacement sketches reduced in worker order; cross-worker boundaries compare
// the retained final pair in each occupied slot. Saturated means at least one
// counter overflowed.
type VectorIndexConstructionDecisionPhaseSnapshot struct {
	DigestXOR                     uint64     `json:"digest_xor"`
	DigestSum                     uint64     `json:"digest_sum"`
	Decisions                     uint64     `json:"decisions"`
	Accepted                      uint64     `json:"accepted"`
	Rejected                      uint64     `json:"rejected"`
	DirectExactFP32Rows           uint64     `json:"direct_exact_fp32_rows"`
	DirectExactFP32Calls          uint64     `json:"direct_exact_fp32_calls"`
	IndexedExactFP32Rows          uint64     `json:"indexed_exact_fp32_rows"`
	IndexedExactFP32Calls         uint64     `json:"indexed_exact_fp32_calls"`
	ApproximateScoreRows          uint64     `json:"approximate_score_rows"`
	ApproximateScoreCalls         uint64     `json:"approximate_score_calls"`
	ExactFP32Dimensions           uint64     `json:"exact_fp32_dimensions"`
	DiversityPredicates           uint64     `json:"diversity_predicates"`
	DiversityCandidates           uint64     `json:"diversity_candidates"`
	DiversityComparisonsRequested uint64     `json:"diversity_comparisons_requested"`
	DiversityComparisonsExecuted  uint64     `json:"diversity_comparisons_executed"`
	UniqueRowPairs                uint64     `json:"unique_row_pairs"`
	RepeatedRowPairs              uint64     `json:"repeated_row_pairs"`
	RowPairReplacements           uint64     `json:"row_pair_replacements"`
	ActiveWallNanos               uint64     `json:"active_wall_nanos"`
	Saturated                     bool       `json:"saturated"`
	CandidateCountHistogram       [16]uint64 `json:"candidate_count_histogram"`
	SelectedCountHistogram        [16]uint64 `json:"selected_count_histogram"`
	DiversityEarlyExitHistogram   [16]uint64 `json:"diversity_early_exit_histogram"`
	ReciprocalGroupHistogram      [16]uint64 `json:"reciprocal_group_histogram"`
	PruneSurvivorHistogram        [16]uint64 `json:"prune_survivor_histogram"`
}

func (observer *vectorIndexConstructionDecisionObserverV1) snapshot() *VectorIndexConstructionDecisionSnapshot {
	if observer == nil {
		return nil
	}
	return &VectorIndexConstructionDecisionSnapshot{
		Planning:   observer.phaseSnapshot(vectorIndexConstructionDecisionPlanning),
		Reciprocal: observer.phaseSnapshot(vectorIndexConstructionDecisionReciprocal),
	}
}

func (observer *vectorIndexConstructionDecisionObserverV1) phaseSnapshot(phase int) VectorIndexConstructionDecisionPhaseSnapshot {
	source := &observer.phases[phase]
	target := VectorIndexConstructionDecisionPhaseSnapshot{
		DigestXOR:                     source.digestXOR,
		DigestSum:                     source.digestSum,
		Decisions:                     source.decisions,
		Accepted:                      source.accepted,
		Rejected:                      source.rejected,
		DirectExactFP32Rows:           source.directExactFP32Rows,
		DirectExactFP32Calls:          source.directExactFP32Calls,
		IndexedExactFP32Rows:          source.indexedExactFP32Rows,
		IndexedExactFP32Calls:         source.indexedExactFP32Calls,
		ApproximateScoreRows:          source.approximateScoreRows,
		ApproximateScoreCalls:         source.approximateScoreCalls,
		ExactFP32Dimensions:           source.exactFP32Dimensions,
		DiversityPredicates:           source.diversityPredicates,
		DiversityCandidates:           source.diversityCandidates,
		DiversityComparisonsRequested: source.diversityComparisonsRequested,
		DiversityComparisonsExecuted:  source.diversityComparisonsExecuted,
		UniqueRowPairs:                source.uniqueRowPairs,
		RepeatedRowPairs:              source.repeatedRowPairs,
		RowPairReplacements:           source.rowPairReplacements,
		ActiveWallNanos:               source.activeWallNanos,
		Saturated:                     source.saturated,
	}
	for bucket := range target.CandidateCountHistogram {
		target.CandidateCountHistogram[bucket] = source.candidateCount[bucket]
		target.SelectedCountHistogram[bucket] = source.selectedCount[bucket]
		target.DiversityEarlyExitHistogram[bucket] = source.earlyExit[bucket]
		target.ReciprocalGroupHistogram[bucket] = source.groupSize[bucket]
		target.PruneSurvivorHistogram[bucket] = source.pruneSurvivors[bucket]
	}
	return target
}

func (observer *vectorIndexConstructionDecisionObserverV1) merge(source *vectorIndexConstructionDecisionObserverV1) {
	if observer == nil || source == nil {
		return
	}
	for phase := range observer.phases {
		target, local := &observer.phases[phase], &source.phases[phase]
		target.digestXOR ^= local.digestXOR
		target.digestSum += local.digestSum
		for _, values := range []struct {
			target, local *uint64
		}{
			{&target.decisions, &local.decisions}, {&target.accepted, &local.accepted}, {&target.rejected, &local.rejected},
			{&target.directExactFP32Rows, &local.directExactFP32Rows}, {&target.directExactFP32Calls, &local.directExactFP32Calls},
			{&target.indexedExactFP32Rows, &local.indexedExactFP32Rows}, {&target.indexedExactFP32Calls, &local.indexedExactFP32Calls},
			{&target.approximateScoreRows, &local.approximateScoreRows}, {&target.approximateScoreCalls, &local.approximateScoreCalls},
			{&target.exactFP32Dimensions, &local.exactFP32Dimensions}, {&target.diversityPredicates, &local.diversityPredicates},
			{&target.diversityCandidates, &local.diversityCandidates}, {&target.diversityComparisonsRequested, &local.diversityComparisonsRequested},
			{&target.diversityComparisonsExecuted, &local.diversityComparisonsExecuted}, {&target.uniqueRowPairs, &local.uniqueRowPairs},
			{&target.repeatedRowPairs, &local.repeatedRowPairs}, {&target.rowPairReplacements, &local.rowPairReplacements},
			{&target.activeWallNanos, &local.activeWallNanos},
		} {
			vectorIndexConstructionDecisionAddV1(values.target, *values.local, &target.saturated)
		}
		// Classify each worker-local sketch boundary in worker order. The sketch
		// intentionally retains only the final pair per occupied slot.
		for pair := range target.rowPairs {
			if local.rowPairs[pair] == 0 {
				continue
			}
			if target.rowPairs[pair] != 0 {
				target.uniqueRowPairs--
				if target.rowPairs[pair] == local.rowPairs[pair] {
					vectorIndexConstructionDecisionAddV1(&target.repeatedRowPairs, 1, &target.saturated)
				} else {
					vectorIndexConstructionDecisionAddV1(&target.rowPairReplacements, 1, &target.saturated)
				}
			}
			target.rowPairs[pair] = local.rowPairs[pair]
		}
		for bucket := range target.candidateCount {
			for _, values := range []struct {
				target, local *uint64
			}{
				{&target.candidateCount[bucket], &local.candidateCount[bucket]}, {&target.selectedCount[bucket], &local.selectedCount[bucket]},
				{&target.earlyExit[bucket], &local.earlyExit[bucket]}, {&target.groupSize[bucket], &local.groupSize[bucket]},
				{&target.pruneSurvivors[bucket], &local.pruneSurvivors[bucket]},
			} {
				vectorIndexConstructionDecisionAddV1(values.target, *values.local, &target.saturated)
			}
		}
		target.saturated = target.saturated || local.saturated
	}
}

func vectorIndexConstructionDecisionHashV1(hash, value uint64) uint64 {
	hash ^= value + 0x9e3779b97f4a7c15 + hash<<6 + hash>>2
	return hash
}

func vectorIndexConstructionDecisionMixV1(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ value>>31
}

func vectorIndexConstructionDecisionBucketV1(value int) int {
	bucket := 0
	for value > 1 && bucket < 15 {
		value = (value + 1) >> 1
		bucket++
	}
	return bucket
}

func vectorIndexConstructionDecisionAddV1(counter *uint64, delta uint64, saturated *bool) {
	if *counter == ^uint64(0) {
		*saturated = true
		return
	}
	next := *counter + delta
	if next < *counter {
		next = ^uint64(0)
		*saturated = true
	}
	*counter = next
}

func vectorIndexConstructionDecisionXORV1(counter *uint64, value uint64) {
	*counter ^= value
}

func (context *vectorIndexConstructionDecisionContextV1) phaseStats() *vectorIndexConstructionDecisionPhaseV1 {
	if context == nil || context.observer == nil || context.phase < 0 || context.phase >= len(context.observer.phases) {
		return nil
	}
	return &context.observer.phases[context.phase]
}

func (context *vectorIndexConstructionDecisionContextV1) recordRowFrom(source, nodeID int, indexed bool) {
	stats := context.phaseStats()
	if stats == nil {
		return
	}
	rows := &stats.directExactFP32Rows
	if indexed {
		rows = &stats.indexedExactFP32Rows
	} else {
		vectorIndexConstructionDecisionAddV1(&stats.directExactFP32Calls, 1, &stats.saturated)
	}
	vectorIndexConstructionDecisionAddV1(rows, 1, &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.exactFP32Dimensions, uint64(context.dimensions), &stats.saturated)
	pair := vectorIndexConstructionDecisionMixV1(uint64(source + 1))
	pair = vectorIndexConstructionDecisionHashV1(pair, uint64(nodeID+1)) | 1
	slot := &stats.rowPairs[pair&(vectorIndexConstructionDecisionPairSlots-1)]
	seen := *slot
	*slot = pair
	switch seen {
	case 0:
		vectorIndexConstructionDecisionAddV1(&stats.uniqueRowPairs, 1, &stats.saturated)
	case pair:
		vectorIndexConstructionDecisionAddV1(&stats.repeatedRowPairs, 1, &stats.saturated)
	default:
		vectorIndexConstructionDecisionAddV1(&stats.rowPairReplacements, 1, &stats.saturated)
	}
}

func (context *vectorIndexConstructionDecisionContextV1) recordRow(nodeID int, indexed bool) {
	context.recordRowFrom(context.source, nodeID, indexed)
}

func (context *vectorIndexConstructionDecisionContextV1) recordIndexedExactFP32Call() {
	stats := context.phaseStats()
	if stats != nil {
		vectorIndexConstructionDecisionAddV1(&stats.indexedExactFP32Calls, 1, &stats.saturated)
	}
}

// recordApproximateScoreCall is the private ownership seam for a future
// approximate construction scorer. The current exact constructor never calls it.
func (context *vectorIndexConstructionDecisionContextV1) recordApproximateScoreCall(rows int) {
	stats := context.phaseStats()
	if stats == nil || rows <= 0 {
		return
	}
	vectorIndexConstructionDecisionAddV1(&stats.approximateScoreCalls, 1, &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.approximateScoreRows, uint64(rows), &stats.saturated)
}

func (context *vectorIndexConstructionDecisionContextV1) recordEarlyExit(executed, requested int, accepted bool) {
	stats := context.phaseStats()
	if stats == nil {
		return
	}
	vectorIndexConstructionDecisionAddV1(&stats.earlyExit[vectorIndexConstructionDecisionBucketV1(executed)], 1, &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.diversityPredicates, 1, &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.diversityComparisonsRequested, uint64(requested), &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.diversityComparisonsExecuted, uint64(executed), &stats.saturated)
	if accepted {
		vectorIndexConstructionDecisionAddV1(&stats.accepted, 1, &stats.saturated)
	} else {
		vectorIndexConstructionDecisionAddV1(&stats.rejected, 1, &stats.saturated)
	}

}

func (context *vectorIndexConstructionDecisionContextV1) recordDecision(orderedHash uint64, candidates, selected int, selectedIDs []vectorIndexCandidate) {
	stats := context.phaseStats()
	if stats == nil {
		return
	}
	hash := vectorIndexConstructionDecisionHashV1(orderedHash, uint64(selected))
	for _, candidate := range selectedIDs {
		hash = vectorIndexConstructionDecisionHashV1(hash, uint64(candidate.nodeID+1))
	}
	hash = vectorIndexConstructionDecisionMixV1(hash)
	vectorIndexConstructionDecisionXORV1(&stats.digestXOR, hash)
	stats.digestSum += hash
	vectorIndexConstructionDecisionAddV1(&stats.decisions, 1, &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.diversityCandidates, uint64(candidates), &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.candidateCount[vectorIndexConstructionDecisionBucketV1(candidates)], 1, &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.selectedCount[vectorIndexConstructionDecisionBucketV1(selected)], 1, &stats.saturated)
}

func (context *vectorIndexConstructionDecisionContextV1) recordGroup(groupSize, candidates, survivors int) {
	stats := context.phaseStats()
	if stats == nil {
		return
	}
	vectorIndexConstructionDecisionAddV1(&stats.groupSize[vectorIndexConstructionDecisionBucketV1(groupSize)], 1, &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.candidateCount[vectorIndexConstructionDecisionBucketV1(candidates)], 1, &stats.saturated)
	vectorIndexConstructionDecisionAddV1(&stats.pruneSurvivors[vectorIndexConstructionDecisionBucketV1(survivors)], 1, &stats.saturated)
}

func (context *vectorIndexConstructionDecisionContextV1) recordWall(elapsed time.Duration) {
	stats := context.phaseStats()
	if stats != nil && elapsed >= 0 {
		vectorIndexConstructionDecisionAddV1(&stats.activeWallNanos, uint64(max(elapsed, time.Nanosecond)), &stats.saturated)
	}
}

// vectorIndexLayer0ConstructionPolicyV1 selects the canonical construction
// profile or an offline experiment while keeping reciprocal capacity and
// pruning at the canonical 2M limit.
type vectorIndexLayer0ConstructionPolicyV1 struct {
	initialSelectionFactor int
	backfill               bool
	preserveSearchSet      bool
	qualityPostfill        bool
	robustPruneRefinement  bool
}

// vectorIndexConstructionTraceV1 is deliberately private: construction
// provenance is benchmark evidence, not a runtime index contract.
type vectorIndexConstructionTraceV1 struct {
	// detailed is enabled only for predeclared offline diagnostic partitions.
	// Compact traces retain live origins/final survivors but never allocate a
	// historical event per reciprocal maintenance operation.
	detailed                bool
	selections              []vectorIndexConstructionSelectionV1
	events                  []vectorIndexConstructionEdgeEventV1
	pending                 map[vectorIndexConstructionEdgeKeyV1]string
	origins                 map[vectorIndexConstructionEdgeKeyV1]string
	sampleIDs               map[string]struct{}
	nativeInsertionOrdinals []int
	pruneKeeps              uint64
	compactLifecycle        VectorPartitionConstructionCompactLifecycleV1
	postfillEdges           uint64
}

type vectorIndexConstructionSelectionV1 struct {
	Node, Layer, Candidates, Selected, DiversitySelected, BackfillSelected int
	CandidateNodes                                                         []int
	Sampled                                                                bool
}

type vectorIndexConstructionEdgeKeyV1 struct{ From, To, Layer int }

type vectorIndexConstructionEdgeEventV1 struct {
	From, To, Layer, InsertionOrdinal int
	Origin, Action                    string
}

func vectorIndexConstructionOriginIndexV1(origin string) (int, bool) {
	switch origin {
	case "diversity_selected":
		return 0, true
	case "nearest_backfill":
		return 1, true
	case "reciprocal_add":
		return 2, true
	case "reciprocity_repair":
		return 3, true
	case "overlay_rewrite":
		return 4, true
	case "quality_postfill":
		return 5, true
	case "robust_prune_refinement":
		return 6, true
	case "robust_prune_residual_fill":
		return 7, true
	}
	return 0, false
}

func (t *vectorIndexConstructionTraceV1) countLifecycle(origin, action string) {
	originIndex, ok := vectorIndexConstructionOriginIndexV1(origin)
	if !ok {
		return
	}
	switch action {
	case "initial_add":
		t.compactLifecycle.InitialAdd[originIndex]++
	case "reciprocal_add":
		t.compactLifecycle.ReciprocalAdd[originIndex]++
	case "reciprocal_prune_keep":
		t.pruneKeeps++
		t.compactLifecycle.PruneKeep[originIndex]++
	case "reciprocal_prune_drop":
		t.compactLifecycle.PruneDrop[originIndex]++
	case "reciprocity_repair_add", "overlay_rewrite_add":
		t.compactLifecycle.VariantAdd[originIndex]++
	case "reciprocity_repair_drop", "overlay_rewrite_drop":
		t.compactLifecycle.VariantDrop[originIndex]++
	case "robust_prune_add", "robust_prune_residual_fill_add":
		t.compactLifecycle.VariantAdd[originIndex]++
	case "robust_prune_drop":
		t.compactLifecycle.VariantDrop[originIndex]++
	case "quality_postfill_add":
		t.compactLifecycle.QualityPostfillAdd[originIndex]++
	}
}

func (t *vectorIndexConstructionTraceV1) selectEdge(from, to, layer int, origin string) {
	if t == nil {
		return
	}
	t.init()
	t.pending[vectorIndexConstructionEdgeKeyV1{from, to, layer}] = origin
}

func (t *vectorIndexConstructionTraceV1) init() {
	if t.pending == nil {
		t.pending = make(map[vectorIndexConstructionEdgeKeyV1]string)
	}
	if t.origins == nil {
		t.origins = make(map[vectorIndexConstructionEdgeKeyV1]string)
	}
}

func (t *vectorIndexConstructionTraceV1) record(from, to, layer int, origin, action string) {
	if t == nil {
		return
	}
	if action != "final_survivor" {
		t.countLifecycle(origin, action)
	}
	if action == "reciprocal_prune_keep" {
		return
	}
	insertionOrdinal := from
	if to > insertionOrdinal {
		insertionOrdinal = to
	}
	// Variant rewrites are recorded after BFS locality remapping. Translate
	// those persisted ordinals back to causal insertion order; pre-remap build
	// events intentionally retain their native node ordinals directly.
	if len(t.nativeInsertionOrdinals) != 0 && from >= 0 && to >= 0 && from < len(t.nativeInsertionOrdinals) && to < len(t.nativeInsertionOrdinals) {
		insertionOrdinal = t.nativeInsertionOrdinals[from]
		if t.nativeInsertionOrdinals[to] > insertionOrdinal {
			insertionOrdinal = t.nativeInsertionOrdinals[to]
		}
	}
	if !t.detailed && action != "final_survivor" {
		return
	}
	if !t.detailed { // compact final survivors live in origins, not events.
		return
	}
	t.events = append(t.events, vectorIndexConstructionEdgeEventV1{From: from, To: to, Layer: layer, InsertionOrdinal: insertionOrdinal, Origin: origin, Action: action})
}

type vectorIndexNode struct {
	documentID    []byte
	vector        []float32
	quantized     []int8
	quantScale    float32
	normSquared   float64
	cachedInvNorm float64
	level         int
	neighbors     [][]vectorIndexNeighbor
	deleted       bool
}

type vectorIndexNeighbor struct {
	nodeID   uint32
	distance float32
}

func vectorIndexNodeOrdinalsFitUint32(existing, additional uint64) bool {
	return additional == 0 || (existing <= math.MaxUint32 && additional-1 <= math.MaxUint32-existing)
}

const (
	nativeVectorFrozenPrefixBatchWidth   = 16
	nativeVectorFrozenPrefixBatchMinimum = 32
)

type vectorIndexFrozenPrefixInsert struct {
	documentID []byte
	vector     []float32
	level      int
	neighbors  [][]int
}

type vectorIndexFrozenPrefixReciprocalLink struct {
	fromNodeID int
	toNodeID   int
	layer      int
}

type vectorIndexFrozenPrefixReciprocalGroup struct {
	start int
	end   int
}

type vectorIndexFrozenPrefixCommitScratch struct {
	links             []vectorIndexFrozenPrefixReciprocalLink
	groups            []vectorIndexFrozenPrefixReciprocalGroup
	pruned            []bool
	dots              []vectorIndexFrozenPrefixDiversityScratch
	decisionObservers []vectorIndexConstructionDecisionObserverV1
}

type vectorIndexFrozenPrefixDiversityScratch struct {
	neighbors      []vectorIndexNeighbor
	rowIDs         []uint32
	dots           []float32
	indexedBatches uint64
}

func (scratch *vectorIndexFrozenPrefixCommitScratch) decisionObserverWorkers(workers int) []vectorIndexConstructionDecisionObserverV1 {
	if cap(scratch.decisionObservers) < workers {
		observers := make([]vectorIndexConstructionDecisionObserverV1, workers)
		copy(observers, scratch.decisionObservers)
		scratch.decisionObservers = observers
	} else if len(scratch.decisionObservers) < workers {
		scratch.decisionObservers = scratch.decisionObservers[:workers]
	}
	return scratch.decisionObservers[:workers]
}

func vectorIndexReciprocalLinkWorkerCount(neighbors int) int {
	if neighbors < minVectorIndexParallelReciprocalNeighbors {
		return 1
	}
	return minInt(runtime.GOMAXPROCS(0), neighbors)
}

func vectorIndexNeighborIDsDistinct(neighbors []int) bool {
	for i := range neighbors {
		for j := 0; j < i; j++ {
			if neighbors[i] == neighbors[j] {
				return false
			}
		}
	}
	return true
}

func (node *vectorIndexNode) matchesVector(vector []float32) bool {
	if node == nil {
		return false
	}
	return slices.Equal(node.vector, vector)
}

func (node *vectorIndexNode) matchesInt8SourceVector(vector []float32) bool {
	if node == nil || len(node.quantized) != len(vector) {
		return false
	}
	scale := vectorIndexInt8Scale(vector)
	if node.quantScale != scale {
		return false
	}
	for i, value := range vector {
		if node.quantized[i] != quantizeVectorIndexInt8Value(value, scale) {
			return false
		}
	}
	return true
}

func quantizeVectorIndexInt8(vector []float32) ([]int8, float32) {
	out := make([]int8, 0, len(vector))
	return quantizeVectorIndexInt8Into(out, vector)
}

func quantizeVectorIndexInt8Into(dst []int8, vector []float32) ([]int8, float32) {
	scale := vectorIndexInt8Scale(vector)
	if cap(dst) < len(vector) {
		dst = make([]int8, len(vector))
	} else {
		dst = dst[:len(vector)]
	}
	for i, value := range vector {
		dst[i] = quantizeVectorIndexInt8Value(value, scale)
	}
	return dst, scale
}

func vectorIndexInt8Scale(vector []float32) float32 {
	var maxAbs float32
	for _, value := range vector {
		abs := value
		if abs < 0 {
			abs = -abs
		}
		if abs > maxAbs {
			maxAbs = abs
		}
	}
	scale := maxAbs / 127
	if scale == 0 {
		scale = 1
	}
	return scale
}

func quantizeVectorIndexInt8Value(value, scale float32) int8 {
	q := int(math.Round(float64(value / scale)))
	if q > 127 {
		q = 127
	} else if q < -127 {
		q = -127
	}
	return int8(q)
}

func normalizeVectorIndexEdgeDistance(distance float32) (float32, bool) {
	if math.IsNaN(float64(distance)) || math.IsInf(float64(distance), 1) {
		return 0, false
	}
	if math.IsInf(float64(distance), -1) {
		return -math.MaxFloat32, true
	}
	return distance, true
}

func prepareVectorIndexGraphOnlyQuery(query []float32, metric VectorMetric, dimensions int) (float64, preparedFloat32CosineQuery, bool, error) {
	if len(query) == 0 {
		return 0, preparedFloat32CosineQuery{}, false, errors.New("collections: vector query cannot be empty")
	}
	if err := validateFloat32Vector(query); err != nil {
		return 0, preparedFloat32CosineQuery{}, false, fmt.Errorf("collections: vector query: %w", err)
	}
	queryNorm := float64(-1)
	var prepared preparedFloat32CosineQuery
	if metric == VectorMetricCosine {
		queryNorm = vectorNormSquared(query)
		if queryNorm == 0 {
			return 0, preparedFloat32CosineQuery{}, false, errors.New("collections: cosine vector query cannot have zero magnitude")
		}
		var err error
		prepared, err = prepareFloat32CosineQuery(query, queryNorm)
		if err != nil {
			return 0, preparedFloat32CosineQuery{}, false, err
		}
	}
	if dimensions != 0 && len(query) != dimensions {
		return 0, preparedFloat32CosineQuery{}, false, fmt.Errorf("collections: vector query has dimension %d, want %d", len(query), dimensions)
	}
	return queryNorm, prepared, metric == VectorMetricCosine, nil
}

func vectorDocumentIDSet(ids [][]byte) map[string]struct{} {
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		out[string(id)] = struct{}{}
	}
	return out
}

func collectionGetUnsafeAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, key []byte) ([]byte, bool, error) {
	if snap == nil {
		return nil, false, backenddb.ErrClosed
	}
	if len(catalog.overlayRootIDs(rootName)) == 0 {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			return nil, false, nil
		}
		out, err := snap.GetUnsafeAtRoot(rootID, key)
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, false, nil
		}
		return out, err == nil, err
	}
	useOverlayFilters := catalog.rootID(rootName) != 0
	for _, rootID := range catalog.rootStack(rootName) {
		if useOverlayFilters && !catalog.overlayRootMayContainKey(rootName, rootID, key) {
			continue
		}
		entry, err := snap.GetEntryAtRoot(rootID, key)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			return nil, false, nil
		}
		out, err := snap.GetUnsafeAtRoot(rootID, key)
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, false, nil
		}
		return out, err == nil, err
	}
	return nil, false, nil
}

func cloneVectorSearchResultDocumentIDs(results []VectorSearchResult) {
	total := 0
	for _, result := range results {
		total += len(result.DocumentID)
	}
	if total == 0 {
		return
	}
	arena := make([]byte, 0, total)
	for i := range results {
		start := len(arena)
		arena = append(arena, results[i].DocumentID...)
		results[i].DocumentID = arena[start:len(arena):len(arena)]
	}
}

func vectorIndexConstructionOrdinalOffset(nodeID, sourceNodeID int) int {
	if nodeID < sourceNodeID {
		return sourceNodeID - nodeID
	}
	return nodeID - sourceNodeID
}

func vectorIndexRobustPruneOccludesV1(alphaSquared, chosenToCandidate, sourceToCandidate float32) bool {
	return alphaSquared*chosenToCandidate <= sourceToCandidate
}

func vectorDistanceToStoredNode(query []float32, node *vectorIndexNode, metric VectorMetric) (float32, error) {
	return vectorDistanceToStoredNodeWithQueryNorm(query, -1, node, metric)
}

func vectorDistanceToStoredNodeWithQueryNorm(query []float32, queryNormSquared float64, node *vectorIndexNode, metric VectorMetric) (float32, error) {
	dims := node.vectorDimensions()
	if len(query) != dims {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", len(query), dims)
	}
	switch metric {
	case VectorMetricCosine:
		if len(node.vector) > 0 {
			return vectorDistanceToFloat32NodeCosine(query, queryNormSquared, node)
		}
		var dot float64
		for i, left := range query {
			right := node.vectorValueAt(i)
			dot += float64(left * right)
		}
		leftNorm := queryNormSquared
		if leftNorm < 0 {
			leftNorm = vectorNormSquared(query)
		}
		rightNorm := node.cachedNormSquared()
		if leftNorm == 0 || rightNorm == 0 {
			return 0, errors.New("collections: cosine vector cannot have zero magnitude")
		}
		return float32(1 - dot/(math.Sqrt(leftNorm)*math.Sqrt(rightNorm))), nil
	case VectorMetricL2:
		var sum float64
		for i, left := range query {
			diff := float64(left - node.vectorValueAt(i))
			sum += diff * diff
		}
		return float32(math.Sqrt(sum)), nil
	case VectorMetricInnerProduct:
		var dot float64
		for i, left := range query {
			dot += float64(left * node.vectorValueAt(i))
		}
		return float32(-dot), nil
	default:
		return 0, fmt.Errorf("collections: unsupported vector metric %d", metric)
	}
}

func vectorDistanceBetweenStoredNodes(left, right *vectorIndexNode, metric VectorMetric) (float32, error) {
	dims := left.vectorDimensions()
	rightDims := right.vectorDimensions()
	if dims != rightDims {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", dims, rightDims)
	}
	switch metric {
	case VectorMetricCosine:
		if len(left.vector) > 0 && len(right.vector) > 0 {
			return vectorDistanceBetweenFloat32NodesCosine(left, right)
		}
		var dot float64
		for i := 0; i < dims; i++ {
			leftValue := left.vectorValueAt(i)
			rightValue := right.vectorValueAt(i)
			dot += float64(leftValue * rightValue)
		}
		leftNorm := left.cachedNormSquared()
		rightNorm := right.cachedNormSquared()
		if leftNorm == 0 || rightNorm == 0 {
			return 0, errors.New("collections: cosine vector cannot have zero magnitude")
		}
		return float32(1 - dot/(math.Sqrt(leftNorm)*math.Sqrt(rightNorm))), nil
	case VectorMetricL2:
		var sum float64
		for i := 0; i < dims; i++ {
			diff := float64(left.vectorValueAt(i) - right.vectorValueAt(i))
			sum += diff * diff
		}
		return float32(math.Sqrt(sum)), nil
	case VectorMetricInnerProduct:
		var dot float64
		for i := 0; i < dims; i++ {
			dot += float64(left.vectorValueAt(i) * right.vectorValueAt(i))
		}
		return float32(-dot), nil
	default:
		return 0, fmt.Errorf("collections: unsupported vector metric %d", metric)
	}
}

type preparedFloat32CosineQuery struct {
	vector  []float32
	invNorm float64
}

func prepareFloat32CosineQuery(query []float32, queryNormSquared float64) (preparedFloat32CosineQuery, error) {
	leftNorm := queryNormSquared
	if leftNorm < 0 {
		leftNorm = vectorNormSquared(query)
	}
	if leftNorm == 0 {
		return preparedFloat32CosineQuery{}, errors.New("collections: cosine vector cannot have zero magnitude")
	}
	return preparedFloat32CosineQuery{
		vector:  query,
		invNorm: 1 / math.Sqrt(leftNorm),
	}, nil
}

func vectorDistanceToFloat32NodeCosine(query []float32, queryNormSquared float64, node *vectorIndexNode) (float32, error) {
	prepared, err := prepareFloat32CosineQuery(query, queryNormSquared)
	if err != nil {
		return 0, err
	}
	return vectorDistanceToFloat32NodeCosinePrepared(prepared, node)
}

func vectorDistanceToFloat32NodeCosinePrepared(query preparedFloat32CosineQuery, node *vectorIndexNode) (float32, error) {
	if len(query.vector) != len(node.vector) {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", len(query.vector), len(node.vector))
	}
	if node.cachedInvNorm == 0 {
		return 0, errors.New("collections: cosine vector cannot have zero magnitude")
	}
	return vectorDistanceToFloat32NodeCosineUnchecked(query, node), nil
}

func vectorDistanceToFloat32NodeCosineUnchecked(query preparedFloat32CosineQuery, node *vectorIndexNode) float32 {
	return vectorops.CosineDistanceFloat32Normalized(query.vector, node.vector, query.invNorm, node.cachedInvNorm)
}

func vectorDistanceBetweenFloat32NodesCosine(left, right *vectorIndexNode) (float32, error) {
	if len(left.vector) != len(right.vector) {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", len(left.vector), len(right.vector))
	}
	if !canUseUncheckedFloat32NodeCosine(left, right, 0) {
		return 0, errors.New("collections: cosine vector cannot have zero magnitude")
	}
	return vectorDistanceBetweenFloat32NodesCosineUnchecked(left, right), nil
}

func canUseUncheckedFloat32NodeCosine(left, right *vectorIndexNode, dimensions int) bool {
	return len(left.vector) > 0 &&
		len(left.vector) == len(right.vector) &&
		(dimensions == 0 || len(left.vector) == dimensions) &&
		left.cachedInvNorm != 0 &&
		right.cachedInvNorm != 0
}

func vectorDistanceBetweenFloat32NodesCosineUnchecked(left, right *vectorIndexNode) float32 {
	return vectorops.CosineDistanceFloat32Normalized(left.vector, right.vector, left.cachedInvNorm, right.cachedInvNorm)
}

func safeFloat32DotProductForCosine(leftNormSquared, rightNormSquared float64) bool {
	if leftNormSquared <= 0 || rightNormSquared <= 0 {
		return false
	}
	const maxDot = float64(math.MaxFloat32)
	return leftNormSquared <= maxDot*maxDot/rightNormSquared
}

func (node *vectorIndexNode) vectorDimensions() int {
	if len(node.vector) > 0 {
		return len(node.vector)
	}
	return len(node.quantized)
}

func (node *vectorIndexNode) vectorValueAt(i int) float32 {
	if len(node.vector) > 0 {
		return node.vector[i]
	}
	return float32(node.quantized[i]) * node.quantScale
}

func (node *vectorIndexNode) cachedNormSquared() float64 {
	if node.normSquared > 0 {
		return node.normSquared
	}
	return node.storedNormSquared()
}

func (node *vectorIndexNode) cacheVectorNorms() {
	node.normSquared = node.storedNormSquared()
	if node.normSquared > 0 {
		node.cachedInvNorm = 1 / math.Sqrt(node.normSquared)
	} else {
		node.cachedInvNorm = 0
	}
}

func (node *vectorIndexNode) storedNormSquared() float64 {
	var norm float64
	dims := node.vectorDimensions()
	for i := 0; i < dims; i++ {
		value := float64(node.vectorValueAt(i))
		norm += value * value
	}
	return norm
}

func (node *vectorIndexNode) vectorBytes() int {
	if len(node.quantized) > 0 {
		return len(node.quantized) + 4
	}
	return len(node.vector) * 4
}

type vectorIndexCandidate struct {
	nodeID   int
	distance float32
}

type vectorIndexSearchScratch struct {
	context             context.Context
	contextErr          error
	visitedEpochs       []uint32
	visitedEpoch        uint32
	explorationLimit    int
	explored            int
	queue               vectorIndexMinCandidateHeap
	best                vectorIndexMaxCandidateHeap
	liveBest            vectorIndexMaxCandidateHeap
	out                 []vectorIndexCandidate
	resumeCandidates    []vectorIndexCandidate
	resumeDeferred      []vectorIndexCandidate
	resumeVisitedMark   uint32
	resumeEnabled       bool
	resumeRequested     bool
	resumed             bool
	scoreTracking       bool
	scoreLimit          int
	scoreCalls          int
	scoreBudgetExceeded bool
}

func (scratch *vectorIndexSearchScratch) setContext(ctx context.Context) {
	scratch.context = ctx
	scratch.contextErr = nil
}

func (scratch *vectorIndexSearchScratch) clearContext() {
	scratch.context = nil
	scratch.contextErr = nil
}

func (scratch *vectorIndexSearchScratch) finalContextErr() error {
	if scratch == nil || scratch.contextErr != nil {
		if scratch == nil {
			return nil
		}
		return scratch.contextErr
	}
	if scratch.context != nil {
		scratch.contextErr = scratch.context.Err()
	}
	return scratch.contextErr
}

func (scratch *vectorIndexSearchScratch) startScoreTracking(limit int) {
	scratch.scoreTracking = true
	scratch.scoreLimit = limit
	scratch.scoreCalls = 0
	scratch.scoreBudgetExceeded = false
}

func (scratch *vectorIndexSearchScratch) stopScoreTracking() {
	scratch.scoreTracking = false
	scratch.scoreLimit = 0
	scratch.scoreCalls = 0
	scratch.scoreBudgetExceeded = false
}

func (scratch *vectorIndexSearchScratch) finalScoreBudgetErr() error {
	if scratch == nil || !scratch.scoreBudgetExceeded {
		return nil
	}
	return fmt.Errorf("%w: live native score budget=%d exhausted", ErrVectorPartitionSearchUnavailable, scratch.scoreLimit)
}

func (scratch *vectorIndexSearchScratch) startResumableSearch() {
	scratch.resumeCandidates = scratch.resumeCandidates[:0]
	scratch.resumeDeferred = scratch.resumeDeferred[:0]
	scratch.resumeVisitedMark = 0
	scratch.resumeEnabled = true
	scratch.resumeRequested = false
	scratch.resumed = false
}

func (scratch *vectorIndexSearchScratch) resumeSearch() {
	scratch.resumeRequested = true
	scratch.resumed = false
}

func (scratch *vectorIndexSearchScratch) stopResumableSearch() {
	scratch.resumeCandidates = scratch.resumeCandidates[:0]
	scratch.resumeDeferred = scratch.resumeDeferred[:0]
	scratch.resumeEnabled = false
	scratch.resumeRequested = false
	scratch.resumed = false
	scratch.resumeVisitedMark = 0
}

func (scratch *vectorIndexSearchScratch) nextVisitedEpoch(nodes int) ([]uint32, uint32) {
	if cap(scratch.visitedEpochs) < nodes {
		scratch.visitedEpochs = make([]uint32, nodes, growVectorIndexScratchCapacity(cap(scratch.visitedEpochs), nodes))
	} else {
		scratch.visitedEpochs = scratch.visitedEpochs[:nodes]
	}
	scratch.visitedEpoch++
	if scratch.visitedEpoch == 0 {
		clear(scratch.visitedEpochs)
		scratch.visitedEpoch = 1
	}
	return scratch.visitedEpochs, scratch.visitedEpoch
}

func growVectorIndexScratchCapacity(current int, required int) int {
	next := current
	if next < 64 {
		next = 64
	}
	for next < required {
		next *= 2
	}
	return next
}

func sortVectorIndexCandidates(candidates []vectorIndexCandidate) {
	slices.SortFunc(candidates, func(left, right vectorIndexCandidate) int {
		if left.distance < right.distance {
			return -1
		}
		if left.distance > right.distance {
			return 1
		}
		if left.nodeID < right.nodeID {
			return -1
		}
		if left.nodeID > right.nodeID {
			return 1
		}
		return 0
	})
}

func vectorIndexCandidateLess(left, right vectorIndexCandidate) bool {
	if left.distance != right.distance {
		return left.distance < right.distance
	}
	return left.nodeID < right.nodeID
}

func vectorIndexCandidateWorse(left, right vectorIndexCandidate) bool {
	return vectorIndexCandidateLess(right, left)
}

type vectorIndexMinCandidateHeap []vectorIndexCandidate

func (h *vectorIndexMinCandidateHeap) push(candidate vectorIndexCandidate) {
	*h = append(*h, candidate)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if !vectorIndexCandidateLess((*h)[child], (*h)[parent]) {
			break
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h *vectorIndexMinCandidateHeap) pushBy(candidate vectorIndexCandidate, less func(vectorIndexCandidate, vectorIndexCandidate) bool) {
	*h = append(*h, candidate)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if !less((*h)[child], (*h)[parent]) {
			break
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h *vectorIndexMinCandidateHeap) pop() vectorIndexCandidate {
	out := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	h.down(0)
	return out
}

func (h *vectorIndexMinCandidateHeap) popBy(less func(vectorIndexCandidate, vectorIndexCandidate) bool) vectorIndexCandidate {
	out := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	h.downBy(0, less)
	return out
}

func (h vectorIndexMinCandidateHeap) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && vectorIndexCandidateLess(h[right], h[left]) {
			child = right
		}
		if !vectorIndexCandidateLess(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

func (h vectorIndexMinCandidateHeap) downBy(parent int, less func(vectorIndexCandidate, vectorIndexCandidate) bool) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && less(h[right], h[left]) {
			child = right
		}
		if !less(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

type vectorIndexMaxCandidateHeap []vectorIndexCandidate

func (h *vectorIndexMaxCandidateHeap) pushBounded(candidate vectorIndexCandidate, limit int) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.up(len(*h) - 1)
		return
	}
	if !vectorIndexCandidateLess(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.down(0)
}

func (h *vectorIndexMaxCandidateHeap) pushBoundedBy(candidate vectorIndexCandidate, limit int, less func(vectorIndexCandidate, vectorIndexCandidate) bool) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.upBy(len(*h)-1, less)
		return
	}
	if !less(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.downBy(0, less)
}

func (h *vectorIndexMaxCandidateHeap) up(child int) {
	for child > 0 {
		parent := (child - 1) / 2
		if !vectorIndexCandidateWorse((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h *vectorIndexMaxCandidateHeap) upBy(child int, less func(vectorIndexCandidate, vectorIndexCandidate) bool) {
	for child > 0 {
		parent := (child - 1) / 2
		if !less((*h)[parent], (*h)[child]) {
			return
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h vectorIndexMaxCandidateHeap) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && vectorIndexCandidateWorse(h[right], h[left]) {
			child = right
		}
		if !vectorIndexCandidateWorse(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

func (h vectorIndexMaxCandidateHeap) downBy(parent int, less func(vectorIndexCandidate, vectorIndexCandidate) bool) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && less(h[left], h[right]) {
			child = right
		}
		if !less(h[parent], h[child]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

func cosineRecallBatchSafe(queries [][]float32, documentNorms []float64) bool {
	for _, query := range queries {
		queryNorm := vectorNormSquared(query)
		for _, documentNorm := range documentNorms {
			if !safeFloat32DotProductForCosine(queryNorm, documentNorm) {
				return false
			}
		}
	}
	return true
}

func vectorFromStoredDocument(materializer *StoredDocumentJSONMaterializer, document []byte, fieldPath []string) ([]float32, bool, error) {
	if materializer != nil && materializer.DocumentFormat() == DocumentFormatBSON {
		return vectorFromBSONField(document, fieldPath)
	}
	if materializer == nil {
		return nil, false, errors.New("collections: nil stored document materializer")
	}
	if materializer.DocumentFormat() != DocumentFormatJSON {
		workstats.IndexedJSON.MaterializationRows.Add(1)
	}
	jsonDoc, err := materializer.StoredDocumentJSON(document)
	if err != nil {
		return nil, false, err
	}
	workstats.IndexedJSON.VectorRows.Add(1)
	return vectorFromJSONField(jsonDoc, fieldPath)
}

func cloneVectorIndexNodes(in []vectorIndexNode) []vectorIndexNode {
	out := make([]vectorIndexNode, len(in))
	for i := range in {
		out[i] = vectorIndexNode{
			documentID:    bytes.Clone(in[i].documentID),
			vector:        append([]float32(nil), in[i].vector...),
			quantized:     append([]int8(nil), in[i].quantized...),
			quantScale:    in[i].quantScale,
			normSquared:   in[i].normSquared,
			cachedInvNorm: in[i].cachedInvNorm,
			level:         in[i].level,
			deleted:       in[i].deleted,
			neighbors:     make([][]vectorIndexNeighbor, len(in[i].neighbors)),
		}
		for layer := range in[i].neighbors {
			out[i].neighbors[layer] = append([]vectorIndexNeighbor(nil), in[i].neighbors[layer]...)
		}
	}
	return out
}

func cloneVectorIndexCurrentNode(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
