package collections

import "github.com/snissn/gomap/TreeDB/internal/typedcolumn"

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
	if err != nil {
		return err
	}
	*visited += uint64(len(rowIDs))
	for i, ordinal := range rowIDs {
		typedGraphFilteredAdmit(columnVectorGraphSearchCandidate{ordinal: int(ordinal), score: scores[i]}, opts.CandidateRows, topK, scratch, stats)
	}
	return nil
}
