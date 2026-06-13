package collections

import (
	"errors"
	"testing"
)

func TestScalarU8PreparedTraversalGreedyUpperLayerTie2659(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{1, 0, 0}},
		{id: "doc-c", vector: []float32{1, 0, 0}},
	}
	plane := prepareScalarU8PreparedGreedyPlaneForTest2659(t, rows, []float32{1, 0, 0})

	var scoreScratch columnVectorGraphNativeSearchScratch
	lowerScore, err := plane.scoreOrdinal(1, &scoreScratch, nil)
	if err != nil {
		t.Fatalf("score lower ordinal: %v", err)
	}
	higherScore, err := plane.scoreOrdinal(2, &scoreScratch, nil)
	if err != nil {
		t.Fatalf("score higher ordinal: %v", err)
	}
	if lowerScore != higherScore {
		t.Fatalf("fixture scores lower=%v higher=%v want exact scalar_u8 tie", lowerScore, higherScore)
	}

	for _, tc := range []struct {
		name      string
		entry     int
		neighbors []uint32
		want      int
		changed   bool
	}{
		{name: "lower_neighbor_wins_equal_score", entry: 2, neighbors: []uint32{1}, want: 1, changed: true},
		{name: "higher_neighbor_does_not_replace_lower_best", entry: 1, neighbors: []uint32{2}, want: 1, changed: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pack := scalarU8UpperLayerGreedyPackForTest2659(len(rows), tc.entry, tc.neighbors)
			var scratch columnVectorGraphNativeSearchScratch
			if err := plane.prepareForHNSWPreparedTraversal(pack, []float32{1, 0, 0}, columnHNSWPreparedTraversalOptions{}, &scratch); err != nil {
				t.Fatalf("prepare traversal plane: %v", err)
			}
			var stats columnVectorGraphNativeSearchStats
			var loopEdges uint64
			got, err := pack.greedyNearestAtLayerPreparedScorePlane(plane, plane, tc.entry, 1, &scratch, &stats, true, &loopEdges)
			if err != nil {
				t.Fatalf("greedy upper layer: %v", err)
			}
			if got != tc.want {
				t.Fatalf("greedy best=%d want %d", got, tc.want)
			}
			if cap(scratch.scoreTileScores) != 0 {
				t.Fatalf("scoreTileScores cap=%d want 0; scalar_u8 greedy seam should avoid float64 score staging", cap(scratch.scoreTileScores))
			}
			if stats.QuantizedScoreCalls != 2 {
				t.Fatalf("QuantizedScoreCalls=%d want 2 (entry + one upper-layer neighbor)", stats.QuantizedScoreCalls)
			}
			if loopEdges != 1 {
				t.Fatalf("loopEdges=%d want 1", loopEdges)
			}

			bestScore := lowerScore
			if tc.changed {
				bestScore = higherScore
			}
			var helperScratch columnVectorGraphNativeSearchScratch
			newBest, newBestScore, helperChanged, err := plane.scoreGreedyBestRowIDsPrevalidated(tc.neighbors, tc.entry, bestScore, &helperScratch, nil)
			if err != nil {
				t.Fatalf("scoreGreedyBestRowIDsPrevalidated: %v", err)
			}
			if newBest != tc.want || helperChanged != tc.changed {
				t.Fatalf("direct greedy best=%d changed=%v want best=%d changed=%v", newBest, helperChanged, tc.want, tc.changed)
			}
			if newBestScore != lowerScore {
				t.Fatalf("direct greedy score=%v want public scalar_u8 score=%v", newBestScore, lowerScore)
			}
		})
	}
}

func TestScalarU8PreparedTraversalGreedyUpperLayerBoundsFailClosed2659(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{1, 0, 0}},
		{id: "doc-c", vector: []float32{1, 0, 0}},
	}
	plane := prepareScalarU8PreparedGreedyPlaneForTest2659(t, rows, []float32{1, 0, 0})
	pack := scalarU8UpperLayerGreedyPackForTest2659(len(rows), 2, []uint32{uint32(len(rows))})
	var scratch columnVectorGraphNativeSearchScratch
	if err := plane.prepareForHNSWPreparedTraversal(pack, []float32{1, 0, 0}, columnHNSWPreparedTraversalOptions{}, &scratch); err != nil {
		t.Fatalf("prepare traversal plane: %v", err)
	}
	var stats columnVectorGraphNativeSearchStats
	_, err := pack.greedyNearestAtLayerPreparedScorePlane(plane, plane, 2, 1, &scratch, &stats, true, nil)
	if !errors.Is(err, errColumnVectorGraphAdjacencyOrdinalOutOfBounds) {
		t.Fatalf("greedy invalid adjacency err=%v want %v", err, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
	}
	if stats.QuantizedScoreCalls != 1 {
		t.Fatalf("QuantizedScoreCalls=%d want only entry scored before fail-closed adjacency validation", stats.QuantizedScoreCalls)
	}
	if cap(scratch.scoreTileScores) != 0 {
		t.Fatalf("scoreTileScores cap=%d want 0 on fail-closed greedy path", cap(scratch.scoreTileScores))
	}
}

func prepareScalarU8PreparedGreedyPlaneForTest2659(tb testing.TB, rows []columnGraphRebuildInputRowV2A, query []float32) *columnHNSWPreparedScalarU8ScorePlane {
	tb.Helper()
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(tb, rows)
	tb.Cleanup(func() { _ = d.Close() })
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		tb.Fatalf("open reader: %v", err)
	}
	tb.Cleanup(func() { _ = reader.Close() })
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		tb.Fatalf("query inv norm: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	scorer, err := reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query, queryInvNorm, &scratch)
	if err != nil {
		tb.Fatalf("prepare scalar_u8 scorer: %v", err)
	}
	return &columnHNSWPreparedScalarU8ScorePlane{scorer: scorer, ready: true}
}

func scalarU8UpperLayerGreedyPackForTest2659(rowCount int, adjacencyOrdinal int, neighbors []uint32) *columnHNSWSearchPackPreparedView {
	upperOffsets := make([]uint64, rowCount+1)
	upperNeighbors := make([]uint32, 0, len(neighbors))
	for ordinal := 0; ordinal < rowCount; ordinal++ {
		upperOffsets[ordinal] = uint64(len(upperNeighbors))
		if ordinal == adjacencyOrdinal {
			upperNeighbors = append(upperNeighbors, neighbors...)
		}
	}
	upperOffsets[rowCount] = uint64(len(upperNeighbors))
	return &columnHNSWSearchPackPreparedView{
		Header: columnHNSWSearchPackHeader{
			Rows:                rowCount,
			Dimensions:          3,
			VectorStride:        3,
			M:                   3,
			EfSearch:            rowCount,
			EntryOrdinal:        adjacencyOrdinal,
			MaxLayer:            1,
			AdjacencyLayerCount: 2,
		},
		Levels: make([]uint16, rowCount),
		AdjacencyLayers: []columnHNSWSearchPackPreparedLayer{
			{Offsets: make([]uint64, rowCount+1)},
			{Offsets: upperOffsets, Neighbors: upperNeighbors},
		},
	}
}
