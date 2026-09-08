package collections

// A finite budget must retain upper navigation and charge its actual work.
// The synthetic connected pack isolates traversal from graph construction.
import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedGraphPreparedFilterBudgetPreservesUpperNavigationV1(t *testing.T) {
	const n, ef, firstGood = 4097, 2048, 2049
	input := testColumnHNSWSearchPackInput2312()
	input.Rows, input.Dimensions = n, 8
	stride, err := columnHNSWSearchPackVectorStrideForDimensions(input.Dimensions)
	if err != nil {
		t.Fatal(err)
	}
	input.VectorStride = stride
	input.M, input.EfSearch = 16, ef
	input.EntryOrdinal, input.MaxLayer = 0, 1
	input.NormalizedVectors = make([]float32, n*stride)
	input.Levels = make([]uint16, n)
	input.Levels[0], input.Levels[firstGood] = 1, 1
	input.AdjacencyLayers = make([]columnHNSWSearchPackLayerInput, 2)
	for layer := range input.AdjacencyLayers {
		input.AdjacencyLayers[layer].Offsets = make([]uint64, n+1)
	}
	input.RowRefGenerations = make([]int64, n)
	input.RowRefPartIDs = make([]int64, n)
	input.RowRefRowIndexes = make([]int64, n)
	input.RowRefAppliedCommandLSN = make([]int64, n)
	input.DocumentIDOffsets = make([]uint64, n+1)
	input.DocumentIDBytes = nil
	for i := 0; i < n; i++ {
		score := -0.5
		if i == ef {
			score = -0.9 // Worse bridge after exactly EF eligible mediocre rows.
		} else if i >= firstGood {
			score = 0.8 + 0.1*float64(i-firstGood)/float64(n-firstGood-1)
		}
		vector := []float32{float32(score), float32(math.Sqrt(1 - score*score)), 0, 0, 0, 0, 0, 0}
		invNorm, normErr := columnVectorGraphInvNorm(vector)
		if normErr != nil {
			t.Fatal(normErr)
		}
		for dim, value := range vector {
			input.NormalizedVectors[i*stride+dim] = value * invNorm
		}
		// Connected, reciprocal layer-0 chain; maximum degree two (< 2M).
		layer0 := &input.AdjacencyLayers[0]
		if i > 0 {
			layer0.Neighbors = append(layer0.Neighbors, uint32(i-1))
		}
		if i+1 < n {
			layer0.Neighbors = append(layer0.Neighbors, uint32(i+1))
		}
		layer0.Offsets[i+1] = uint64(len(layer0.Neighbors))
		// Both endpoints have level one; the upper edge is reciprocal.
		layer1 := &input.AdjacencyLayers[1]
		if i == 0 {
			layer1.Neighbors = append(layer1.Neighbors, firstGood)
		} else if i == firstGood {
			layer1.Neighbors = append(layer1.Neighbors, 0)
		}
		layer1.Offsets[i+1] = uint64(len(layer1.Neighbors))
		input.RowRefGenerations[i], input.RowRefPartIDs[i] = 11, 1
		input.RowRefRowIndexes[i], input.RowRefAppliedCommandLSN[i] = int64(i), 101
		input.DocumentIDBytes = append(input.DocumentIDBytes, fmt.Sprintf("row-%04d", i)...)
		input.DocumentIDOffsets[i+1] = uint64(len(input.DocumentIDBytes))
	}
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	pack, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	defer pack.Close()
	selected, err := typedcolumn.NewAllRowSelection(n)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		cap      int
		filtered bool
	}{
		{"ordinary_upper_control", 0, false},
		{"budgeted_unfiltered", n, false},
		{"budgeted_all_selected", n, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var scratch columnVectorGraphNativeSearchScratch
			opts := columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: ef, CandidateLimit: tc.cap}
			if tc.filtered {
				opts.CandidateRows, opts.HasCandidateRows = selected, true
			}
			got, stats, err := pack.searchCosine([]float32{1, 0, 0, 0, 0, 0, 0, 0}, opts, &scratch)
			if err != nil || len(got) != 5 {
				t.Fatalf("results=%d candidates=%d err=%v", len(got), stats.Candidates, err)
			}
			if tc.cap > 0 && (stats.PreparedScoreCalls >= uint64(tc.cap) || stats.Candidates > stats.PreparedScoreCalls) {
				t.Fatalf("fixture exhausted cap or miscounted layer 0: scores=%d candidates=%d cap=%d", stats.PreparedScoreCalls, stats.Candidates, tc.cap)
			}
			for rank, result := range got {
				wantOrdinal := n - 1 - rank
				wantID := fmt.Sprintf("row-%04d", wantOrdinal)
				if result.Ordinal != wantOrdinal || string(result.ID) != wantID || result.Score <= 0.89 {
					t.Errorf("rank=%d ordinal=%d id=%q score=%g; want ordinal=%d id=%q score>0.89; candidates=%d cap=%d seed_inspections=%d", rank, result.Ordinal, result.ID, result.Score, wantOrdinal, wantID, stats.Candidates, tc.cap, stats.FilteredSeedInspections)
				}
			}
		})
	}
}

// Repeated upper scores consume the same finite allowance as layer zero.
// The three-row fixture needs three upper scores and three layer-zero scores.
func TestTypedGraphPreparedFilterUpperBudgetPrefixV1(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	input.Levels = []uint16{1, 1, 0}
	input.AdjacencyLayers[1] = columnHNSWSearchPackLayerInput{Offsets: []uint64{0, 1, 2, 2}, Neighbors: []uint32{1, 0}}
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	pack, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	defer pack.Close()
	selected, err := typedcolumn.NewAllRowSelection(input.Rows)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"unfiltered", "filtered", "trace"} {
		t.Run(mode, func(t *testing.T) {
			var scratch columnVectorGraphNativeSearchScratch
			run := func(ctx context.Context, limit int) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
				opts := columnVectorGraphNativeSearchOptions{TopK: 3, EfSearch: 2048, CandidateLimit: limit}
				if mode == "filtered" {
					opts.CandidateRows, opts.HasCandidateRows = selected, true
				}
				if mode == "trace" {
					var trace columnHNSWSearchPackAttributionTrace
					got, stats, err := pack.searchCosineWithContextTrace(ctx, []float32{0, 1, 0}, opts, &scratch, &trace)
					if errors.Is(err, errTypedGraphSearchBudget) && trace.Termination != "candidate_limit" {
						t.Fatalf("budget trace termination=%q", trace.Termination)
					}
					if uint64(len(trace.ScoreOrdinals)) != stats.PreparedScoreCalls {
						t.Fatalf("trace score prefix=%d actual=%d", len(trace.ScoreOrdinals), stats.PreparedScoreCalls)
					}
					return got, stats, err
				}
				return pack.searchCosineWithContext(ctx, []float32{0, 1, 0}, opts, &scratch)
			}
			for limit := 1; limit < 6; limit++ {
				got, stats, err := run(context.Background(), limit)
				if !errors.Is(err, errTypedGraphSearchBudget) || len(got) != 0 || stats.PreparedScoreCalls != uint64(limit) || stats.Candidates != uint64(max(0, limit-3)) || stats.ResultFetches != 0 {
					t.Fatalf("limit=%d results=%d scores=%d layer0=%d fetches=%d err=%v", limit, len(got), stats.PreparedScoreCalls, stats.Candidates, stats.ResultFetches, err)
				}
			}
			// Reuse the same scratch after every failed call; no stale output.
			got, stats, err := run(context.Background(), 6)
			if err != nil || len(got) != 3 || string(got[0].ID) != "doc-b" || stats.PreparedScoreCalls != 6 || stats.Candidates != 3 {
				t.Fatalf("retry results=%v scores=%d layer0=%d err=%v", got, stats.PreparedScoreCalls, stats.Candidates, err)
			}
			upperCanceled := false
			for check := 1; check < 60; check++ {
				ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: check}
				got, stats, err := run(ctx, 6)
				if errors.Is(err, context.Canceled) && stats.PreparedScoreCalls > 0 && stats.Candidates == 0 {
					if len(got) != 0 || stats.PreparedScoreCalls > 3 || stats.ResultFetches != 0 {
						t.Fatalf("upper cancellation lost prefix/results: %+v %v", stats, got)
					}
					upperCanceled = true
					break
				}
			}
			if !upperCanceled {
				t.Fatal("no nonzero upper-layer cancellation prefix")
			}
			if got, _, err := run(context.Background(), 6); err != nil || len(got) != 3 {
				t.Fatalf("retry after cancellation: %d %v", len(got), err)
			}
		})
	}
}

func TestTypedGraphPreparedFilterPreservesIneligibleBridgeV1(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	input.MaxLayer = 0
	input.Levels = []uint16{0, 0, 0}
	input.AdjacencyLayers = []columnHNSWSearchPackLayerInput{{Offsets: []uint64{0, 1, 3, 4}, Neighbors: []uint32{1, 0, 2, 1}}}
	input.NormalizedVectors = []float32{0, 1, 0, 0, -1, 0, 0, 0, 1, 0, 0, 0}
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	pack, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	defer pack.Close()
	selected, err := typedcolumn.NewSparseRowSelectionNoCopy(3, []int{2})
	if err != nil {
		t.Fatal(err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := pack.searchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 2048, CandidateLimit: 3, CandidateRows: selected, HasCandidateRows: true}, &scratch)
	if err != nil || len(got) != 1 || string(got[0].ID) != "doc-c" || stats.Candidates != 3 || stats.FilteredIneligibleScores != 2 || stats.FilteredSeedInspections != 0 {
		t.Fatalf("ineligible bridge results=%v stats=%+v err=%v", got, stats, err)
	}
}

func TestTypedGraphPreparedFilterUpperTileBudgetPrefixV1(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	input.Levels = []uint16{1, 1, 1}
	input.AdjacencyLayers[1] = columnHNSWSearchPackLayerInput{Offsets: []uint64{0, 2, 3, 4}, Neighbors: []uint32{1, 2, 0, 0}}
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	pack, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	defer pack.Close()
	for _, traced := range []bool{false, true} {
		var scratch columnVectorGraphNativeSearchScratch
		opts := columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 2048, CandidateLimit: 2}
		var got []columnVectorGraphNativeSearchResult
		var stats columnVectorGraphNativeSearchStats
		var trace columnHNSWSearchPackAttributionTrace
		if traced {
			got, stats, err = pack.searchCosineWithContextTrace(context.Background(), []float32{0, 1, 0}, opts, &scratch, &trace)
		} else {
			got, stats, err = pack.searchCosine([]float32{0, 1, 0}, opts, &scratch)
		}
		if !errors.Is(err, errTypedGraphSearchBudget) || len(got) != 0 || stats.PreparedScoreCalls != 2 || stats.Candidates != 0 || stats.Edges != 1 || stats.ResultFetches != 0 {
			t.Fatalf("traced=%t results=%v scores=%d candidates=%d edges=%d err=%v", traced, got, stats.PreparedScoreCalls, stats.Candidates, stats.Edges, err)
		}
		if traced && (len(trace.ScoreOrdinals) != 2 || len(trace.EdgeEvents) != 1 || trace.EdgeEvents[0].DestinationOrdinal != 1) {
			t.Fatalf("truncated tile trace=%+v", trace)
		}
	}
}
