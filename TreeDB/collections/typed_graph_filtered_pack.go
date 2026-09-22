package collections

import (
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// Prepared filters use sparse/all/range shapes. Enumerate selected ordinals
// directly; the generic graph seed finder scans the corpus and is not used.
func typedGraphFilterOrdinalAt(selection typedcolumn.RowSelection, position int) (int, bool) {
	if position < 0 || position >= selection.Count() {
		return 0, false
	}
	if selection.IsAll() {
		return position, true
	}
	if start, _, ok := selection.SingleRange(); ok {
		return start + position, true
	}
	rows := selection.SparseRows()
	if position < len(rows) {
		return rows[position], true
	}
	return 0, false
}

// typedGraphScalarU8TraversalAdmission owns typed selection and local-to-base
// mapping for the private scalar-u8 collector. The shared HNSW traversal sees
// only its ordinal admission/seed interface, keeping typedcolumn at this
// approved typed-graph seam. The fields borrow immutable plan/navigation state
// for one caller-scratch request.
type typedGraphScalarU8TraversalAdmission struct {
	candidateRows        typedcolumn.RowSelection
	hasCandidateRows     bool
	selectedSeeds        int
	excludedBaseOrdinals []int
	localToBase          []uint32
	baseRows             int
	eligible             int
}

func typedGraphScalarU8PrepareBaseAdmission(plan *typedGraphPreparedFilter, rows int, dst *typedGraphScalarU8TraversalAdmission) (int, error) {
	if rows < 0 || dst == nil {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	*dst = typedGraphScalarU8TraversalAdmission{}
	if plan == nil {
		return rows, nil
	}
	eligible, err := typedGraphScalarU8EligibleCount(rows, plan.base, true, plan.excludedBase)
	if err != nil {
		return 0, err
	}
	*dst = typedGraphScalarU8TraversalAdmission{
		candidateRows:        plan.base,
		hasCandidateRows:     true,
		selectedSeeds:        plan.base.Count(),
		excludedBaseOrdinals: plan.excludedBase,
		baseRows:             rows,
		eligible:             eligible,
	}
	return eligible, nil
}

// typedGraphScalarU8BindNavigationAdmission keeps the already validated base
// selection count/exclusions while changing admission to the local navigation
// ordinal domain. The navigation view itself is the immutable selected domain.
func typedGraphScalarU8BindNavigationAdmission(dst *typedGraphScalarU8TraversalAdmission, localToBase []uint32) {
	if dst == nil {
		return
	}
	dst.candidateRows = typedcolumn.RowSelection{}
	dst.hasCandidateRows = false
	dst.selectedSeeds = 0
	dst.localToBase = localToBase
}

func (f *typedGraphScalarU8TraversalAdmission) enabled() bool {
	return f != nil && (f.hasCandidateRows || len(f.excludedBaseOrdinals) != 0 || f.localToBase != nil)
}

func (f *typedGraphScalarU8TraversalAdmission) validate(rowCount int) error {
	if !f.enabled() {
		return nil
	}
	if rowCount < 0 {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	if f.hasCandidateRows && f.candidateRows.Rows() != rowCount {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal candidate rows=%d want %d", f.candidateRows.Rows(), rowCount)
	}
	if f.hasCandidateRows && (f.selectedSeeds < 0 || f.selectedSeeds != f.candidateRows.Count()) {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal selected seed count=%d want %d", f.selectedSeeds, f.candidateRows.Count())
	}
	if f.localToBase != nil && len(f.localToBase) != rowCount {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal local ordinal map rows=%d want %d", len(f.localToBase), rowCount)
	}
	baseRows := f.baseRows
	if baseRows == 0 {
		baseRows = rowCount
	}
	if baseRows < 0 {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	for i, ordinal := range f.excludedBaseOrdinals {
		if ordinal < 0 || ordinal >= baseRows {
			return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal excluded ordinal[%d]=%d outside rows=%d", i, ordinal, baseRows)
		}
		if i > 0 && ordinal <= f.excludedBaseOrdinals[i-1] {
			return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal excluded ordinals are not strictly increasing at %d", i)
		}
	}
	if f.eligible < 0 || f.eligible > rowCount {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal eligible count=%d outside rows=%d", f.eligible, rowCount)
	}
	return nil
}

func (f *typedGraphScalarU8TraversalAdmission) eligibleCount() int {
	if f == nil {
		return 0
	}
	return f.eligible
}

func (f *typedGraphScalarU8TraversalAdmission) hasSelectedSeeds() bool {
	return f != nil && f.hasCandidateRows
}

func (f *typedGraphScalarU8TraversalAdmission) selectedSeedCount() int {
	if f == nil || !f.hasCandidateRows {
		return 0
	}
	return f.selectedSeeds
}

func (f *typedGraphScalarU8TraversalAdmission) selectedSeedAt(position int) (int, bool) {
	if f == nil || !f.hasCandidateRows {
		return 0, false
	}
	return typedGraphFilterOrdinalAt(f.candidateRows, position)
}

func (f *typedGraphScalarU8TraversalAdmission) admits(ordinal int) bool {
	if f == nil || !f.enabled() {
		return true
	}
	if ordinal < 0 {
		return false
	}
	if f.hasCandidateRows && !f.candidateRows.Contains(ordinal) {
		return false
	}
	baseOrdinal := ordinal
	if f.localToBase != nil {
		if ordinal >= len(f.localToBase) {
			return false
		}
		baseOrdinal = int(f.localToBase[ordinal])
	}
	i := sort.SearchInts(f.excludedBaseOrdinals, baseOrdinal)
	return i >= len(f.excludedBaseOrdinals) || f.excludedBaseOrdinals[i] != baseOrdinal
}

func typedGraphScalarU8EligibleCount(rows int, selection typedcolumn.RowSelection, hasSelection bool, excluded []int) (int, error) {
	if rows < 0 {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	count := rows
	if hasSelection {
		if selection.Rows() != rows {
			return 0, ErrVectorIndexSnapshotMismatch
		}
		count = selection.Count()
	}
	for i, ordinal := range excluded {
		if ordinal < 0 || ordinal >= rows || (i > 0 && ordinal <= excluded[i-1]) {
			return 0, ErrVectorIndexSnapshotMismatch
		}
		if !hasSelection || selection.Contains(ordinal) {
			count--
		}
	}
	if count < 0 {
		return 0, fmt.Errorf("collections: typed scalar_u8 eligible count underflow")
	}
	return count, nil
}

func typedGraphFilteredAdmit(candidate columnVectorGraphSearchCandidate, eligible typedcolumn.RowSelection, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) {
	if eligible.Contains(candidate.ordinal) {
		if scratch.insertTop(topK, candidate) {
			scratch.pushFrontierAccounting(candidate, stats)
		}
	} else {
		stats.FilteredIneligibleScores++
		// Ineligible nodes still navigate the original graph, never the induced
		// subgraph of a dispersed filter. Only result admission is restricted.
		scratch.pushFrontierAccounting(candidate, stats)
	}
	stats.FilteredFrontierPeak = max(stats.FilteredFrontierPeak, uint64(len(scratch.frontier)))
}

func (v *columnHNSWSearchPackPreparedView) scoreFilteredSeed(query []float32, ordinal, topK int, opts columnVectorGraphNativeSearchOptions, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, visited *uint64) error {
	score, err := v.scoreOrdinal(query, ordinal, opts.ScoreBatchMode, scratch, stats)
	if err != nil {
		return err
	}
	*visited++
	typedGraphFilteredAdmit(columnVectorGraphSearchCandidate{ordinal: ordinal, score: score}, opts.CandidateRows, topK, scratch, stats)
	return nil
}

func (v *columnHNSWSearchPackPreparedView) scoreFilteredTile(query []float32, rowIDs []uint32, topK int, opts columnVectorGraphNativeSearchOptions, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, visited *uint64) error {
	scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(rowIDs))
	scores, err := v.scoreRowIDs(query, rowIDs, scratch.scoreTileScores, opts.ScoreBatchMode, scratch, stats)
	*visited += uint64(len(scores))
	if err != nil {
		return err
	}
	for i, ordinal := range rowIDs {
		typedGraphFilteredAdmit(columnVectorGraphSearchCandidate{ordinal: int(ordinal), score: scores[i]}, opts.CandidateRows, topK, scratch, stats)
	}
	return nil
}
