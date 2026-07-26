package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestVectorPartitionLocalSearcherV1ExactStableIDsAndPins(t *testing.T) {
	a := VectorPartitionSearchAssetV1{ManifestChecksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Generation: 1, PartitionID: 2, Dimensions: 2, IDs: []string{"bb", "a"}, Vectors: [][]float32{{1, 0}, {.9, .1}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1, VectorPartitionMembershipOverlapV1}, Adjacency: [][]uint32{{1}, {0}}}
	s, e := OpenVectorPartitionLocalSearcherV1(a)
	if e != nil {
		t.Fatal(e)
	}
	got, e := s.Search([]float32{1, 0}, 2)
	if e != nil {
		t.Fatal(e)
	}
	if got[0].ID != "bb" || got[0].Score <= got[1].Score {
		t.Fatalf("%+v", got)
	}
	if status := s.Status(); status.MaxStableIDBytes != 2 {
		t.Fatalf("max stable ID bytes=%d want 2", status.MaxStableIDBytes)
	}
	if _, _, err := s.SearchWithOptionsV1(context.Background(), []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, MaxStableIDBytes: 0}); err != nil {
		t.Fatalf("uncapped stable ID search: %v", err)
	}
	if _, _, err := s.SearchWithOptionsV1(context.Background(), []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, MaxStableIDBytes: 1}); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("capped stable ID search err=%v", err)
	}
	if _, err := s.SearchScratchBytesV1(VectorPartitionSearchOptionsV1{TopK: 1, MaxStableIDBytes: 1}); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("capped stable ID preflight err=%v", err)
	}
	if status, _, err := s.SearchPreflightV1(VectorPartitionSearchOptionsV1{TopK: 1, MaxStableIDBytes: 1}); !errors.Is(err, ErrVectorPartitionSearchUnavailable) || status.MaxStableIDBytes != 2 {
		t.Fatalf("coherent capped preflight status=%+v err=%v", status, err)
	}
	if e := s.Acquire(); e != nil {
		t.Fatal(e)
	}
	if e := s.Retire(); !errors.Is(e, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("retire=%v", e)
	}
	s.Release()
	if e := s.Retire(); e != nil {
		t.Fatal(e)
	}
}

func TestVectorPartitionLocalSearcherV1ExactTopKPreservesCutoffTieOrder(t *testing.T) {
	asset := VectorPartitionSearchAssetV1{
		ManifestChecksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Generation:       1,
		PartitionID:      2,
		Dimensions:       2,
		IDs:              []string{"z", "b", "a", "c"},
		Vectors:          [][]float32{{1, 0}, {1, 0}, {1, 0}, {0, 1}},
		Kinds: []VectorPartitionMembershipKindV1{
			VectorPartitionMembershipHomeV1,
			VectorPartitionMembershipHomeV1,
			VectorPartitionMembershipHomeV1,
			VectorPartitionMembershipHomeV1,
		},
	}
	searcher, err := OpenVectorPartitionLocalSearcherV1(asset)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = searcher.Close() })

	results, metrics, err := searcher.SearchWithOptionsV1(
		context.Background(), []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].ID != "a" || results[1].ID != "b" ||
		results[0].Score != results[1].Score {
		t.Fatalf("cutoff tie results=%+v want a,b in stable-ID order", results)
	}
	if metrics.Candidates != 4 || metrics.Route != VectorPartitionSearchRouteExactFP32ScanV1 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestVectorPartitionLocalSearcherV1HNSWCanonicalizesFP32TieOrder(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	input.NormalizedVectors = []float32{
		1, 0, 0, 0,
		1, 0, 0, 0,
		0, 1, 0, 0,
	}
	input.DocumentIDOffsets = []uint64{0, 1, 2, 3}
	input.DocumentIDBytes = []byte("zac")
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, handle := testColumnHNSWSearchPackPreparedViewFromBytes2314(
		t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity,
	)
	searcher := &VectorPartitionLocalSearcherV1{
		asset:            VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 2, Dimensions: 3},
		prepared:         view,
		opened:           1,
		homeMemberships:  input.Rows,
		packBytes:        uint64(len(raw)),
		heapBytes:        uint64(len(raw)),
		searchRoute:      VectorPartitionSearchRouteHNSWSearchPackV1,
		maxStableIDBytes: 1,
	}

	results, metrics, err := searcher.SearchWithOptionsV1(
		context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 2, EfSearch: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].ID != "a" || results[1].ID != "z" ||
		results[0].Score != results[1].Score {
		t.Fatalf("HNSW tie results=%+v want a,z in public FP32/stable-ID order", results)
	}
	if metrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 {
		t.Fatalf("metrics=%+v", metrics)
	}
	if err := searcher.Close(); err != nil {
		t.Fatal(err)
	}
	if !handle.Released() {
		t.Fatal("HNSW tie search retained prepared resource")
	}
}

func TestVectorPartitionLocalSearcherV1PinnedExactPackScanV1(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	input.NormalizedVectors = []float32{1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0}
	input.DocumentIDOffsets = []uint64{0, 4, 5, 6}
	input.DocumentIDBytes = []byte("longac")
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, handle := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	searcher := &VectorPartitionLocalSearcherV1{asset: VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 2, Dimensions: 3}, prepared: view, opened: 1, maxStableIDBytes: 4}
	results, metrics, err := searcher.SearchExactWithOptionsV1(context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 2})
	if err != nil || len(results) != 2 || results[0].ID != "a" || results[1].ID != "long" || metrics.Route != VectorPartitionSearchRouteExactFP32ScanV1 {
		t.Fatalf("results=%+v metrics=%+v err=%v", results, metrics, err)
	}
	if _, _, err := searcher.SearchExactWithOptionsV1(context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 1, MaxStableIDBytes: 3}); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("stable-ID cap err=%v", err)
	}
	if _, _, err := searcher.SearchExactWithOptionsV1(context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: -1}); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("negative ef_search err=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := searcher.SearchExactWithOptionsV1(canceled, []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context err=%v", err)
	}
	if status := searcher.Status(); status.Searches != 1 || status.Candidates != 3 || status.Failures != 2 || status.ActivePins != 0 {
		t.Fatalf("exact diagnostic status=%+v", status)
	}
	if err := searcher.Close(); err != nil || !handle.Released() {
		t.Fatalf("close=%v released=%v", err, handle.Released())
	}
	if len(results) != 2 || results[0].ID != "a" || results[1].ID != "long" {
		t.Fatalf("caller-owned results changed after close: %+v", results)
	}
}

func TestVectorPartitionNativeResultsCanonicalizeCollapsedFP32TieV1(t *testing.T) {
	higherFloat64 := math.Nextafter(1, 2)
	if float32(higherFloat64) != float32(1) {
		t.Fatal("test scores do not collapse to one float32 value")
	}
	prepared := &columnHNSWSearchPackPreparedView{
		Header:            columnHNSWSearchPackHeader{Rows: 2, Dimensions: 2, VectorStride: 2},
		NormalizedVectors: []float32{1, 0, 1, 0},
		DocumentIDOffsets: []uint64{0, 1, 2},
		DocumentIDBytes:   []byte("za"),
	}
	results, err := canonicalizeVectorPartitionNativeResultsV1(context.Background(), prepared, []float32{1, 0}, []columnVectorGraphNativeSearchResult{
		{Ordinal: 0, ID: []byte("z"), Score: higherFloat64},
		{Ordinal: 1, ID: []byte("a"), Score: -1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].ID != "a" || results[1].ID != "z" ||
		results[0].Score != results[1].Score {
		t.Fatalf("collapsed FP32 tie results=%+v want a,z", results)
	}
}

func TestCanonicalVectorPartitionCosineScoreV1NormalizesInFP32V1(t *testing.T) {
	query := []float32{3, 4}
	vector := []float32{6, 8}
	got, err := CanonicalVectorPartitionCosineScoreV1(query, vector)
	if err != nil || got != 1 {
		t.Fatalf("score=%v err=%v want 1", got, err)
	}
	if _, err := CanonicalVectorPartitionCosineScoreV1(query, []float32{0, 0}); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("zero-vector err=%v", err)
	}
}

func TestVectorPartitionLocalSearcherV1ScratchBoundIncludesRowSizedHNSWState(t *testing.T) {
	searcher := &VectorPartitionLocalSearcherV1{
		asset: VectorPartitionSearchAssetV1{Generation: 1, PartitionID: 2, Dimensions: 128},
		prepared: &columnHNSWSearchPackPreparedView{Header: columnHNSWSearchPackHeader{
			Rows:         1_000_000,
			Dimensions:   128,
			VectorStride: 128,
			M:            16,
			EfSearch:     128,
		}},
	}
	got, err := searcher.SearchScratchBytesV1(VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 16})
	if err != nil {
		t.Fatal(err)
	}
	if got <= 8_000_000 {
		t.Fatalf("scratch bytes=%d want row-count visit bitmap plus candidate queues", got)
	}
	if got <= 16*64 {
		t.Fatalf("scratch bytes=%d incorrectly modeled only ef_search candidates", got)
	}
}

func TestVectorPartitionHNSWSearchScratchIncludesConvertedResultsV1(t *testing.T) {
	const (
		rows         = 17
		vectorStride = 32
		degree       = 8
		topK         = 7
		efSearch     = 11
	)
	got, err := vectorPartitionHNSWSearchScratchBytesV1(rows, vectorStride, degree, topK, efSearch)
	if err != nil {
		t.Fatal(err)
	}
	frontier := columnVectorGraphNativeSearchFrontierCapacity(rows, degree, topK, efSearch)
	withoutConverted := uint64(rows)*uint64(unsafe.Sizeof(uint64(0))) +
		uint64(frontier)*uint64(unsafe.Sizeof(columnVectorGraphSearchCandidate{})) +
		uint64(efSearch)*uint64(unsafe.Sizeof(columnVectorGraphSearchCandidate{})) +
		uint64(topK)*uint64(unsafe.Sizeof(columnVectorGraphNativeSearchResult{})) +
		uint64(topK)*uint64(unsafe.Sizeof([]byte(nil))) +
		uint64(topK)*uint64(unsafe.Sizeof(int(0)))*2 +
		uint64(topK)*uint64(unsafe.Sizeof(DocumentRowRef{})) +
		uint64(topK)*uint64(unsafe.Sizeof(bool(false))) +
		uint64(degree)*uint64(unsafe.Sizeof(float64(0))) +
		uint64(degree)*uint64(unsafe.Sizeof(uint32(0))) +
		uint64(degree)*uint64(unsafe.Sizeof(float32(0))) +
		uint64(vectorStride)*uint64(unsafe.Sizeof(float32(0)))
	wantConverted := uint64(topK) * uint64(unsafe.Sizeof(VectorPartitionSearchResultV1{}))
	if got-withoutConverted != wantConverted {
		t.Fatalf("converted result scratch=%d want=%d (total=%d)", got-withoutConverted, wantConverted, got)
	}
}

func TestVectorPartitionConservativeSearchScratchCoversActualRoutesV1(t *testing.T) {
	opts := VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 16}
	conservative, err := VectorPartitionConservativeSearchScratchBytesV1(17, 4096, opts)
	if err != nil {
		t.Fatal(err)
	}
	prepared := &columnHNSWSearchPackPreparedView{Header: columnHNSWSearchPackHeader{
		Rows: 17, Dimensions: 4096, VectorStride: 4096, M: 256, EfSearch: 128,
	}}
	actualHNSW, err := vectorPartitionSearchScratchBytesV1(opts, prepared, 0, 0, prepared.Header)
	if err != nil {
		t.Fatal(err)
	}
	actualExact, err := vectorPartitionSearchScratchBytesV1(opts, nil, 17, 0, columnHNSWSearchPackHeader{})
	if err != nil {
		t.Fatal(err)
	}
	if conservative < actualHNSW || conservative < actualExact {
		t.Fatalf("conservative=%d actual hnsw=%d exact=%d", conservative, actualHNSW, actualExact)
	}
}

func TestVectorPartitionLocalSearcherV1ExplicitOptionsAndCancellation(t *testing.T) {
	asset := VectorPartitionSearchAssetV1{
		ManifestChecksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Generation:       1,
		PartitionID:      2,
		Dimensions:       2,
		IDs:              []string{"a", "b"},
		Vectors:          [][]float32{{1, 0}, {0, 1}},
		Kinds:            []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1, VectorPartitionMembershipHomeV1},
	}
	searcher, err := OpenVectorPartitionLocalSearcherV1(asset)
	if err != nil {
		t.Fatal(err)
	}
	results, metrics, err := searcher.SearchWithOptionsV1(context.Background(), []float32{1, 0}, VectorPartitionSearchOptionsV1{
		TopK:     1,
		EfSearch: 2,
	})
	if err != nil || len(results) != 1 || results[0].ID != "a" || metrics.Candidates != 2 {
		t.Fatalf("results=%+v metrics=%+v err=%v", results, metrics, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := searcher.SearchWithOptionsV1(ctx, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 2}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled search err=%v", err)
	}
	if status := searcher.Status(); status.ActivePins != 0 {
		t.Fatalf("canceled search leaked pins: %+v", status)
	}
}

type vectorPartitionLocalSearcherDeadlineAfterErrContextV1 struct {
	context.Context
	calls         int
	deadlineAfter int
}

func (c *vectorPartitionLocalSearcherDeadlineAfterErrContextV1) Err() error {
	c.calls++
	if c.calls >= c.deadlineAfter {
		return context.DeadlineExceeded
	}
	return nil
}

func TestVectorPartitionLocalSearcherV1DeadlineInterruptsExactRankingAndReleasesResources(t *testing.T) {
	const rows = 4096
	asset := VectorPartitionSearchAssetV1{
		ManifestChecksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Generation:       11,
		PartitionID:      2,
		Dimensions:       1,
		IDs:              make([]string, rows),
		Vectors:          make([][]float32, rows),
		Kinds:            make([]VectorPartitionMembershipKindV1, rows),
	}
	for i := range rows {
		asset.IDs[i] = fmt.Sprintf("id-%04d", rows-i)
		asset.Vectors[i] = []float32{1}
		asset.Kinds[i] = VectorPartitionMembershipHomeV1
	}
	for _, tc := range []struct {
		name          string
		deadlineAfter int
	}{
		{name: "mid_heap_drain", deadlineAfter: 19},
		{name: "before_stats_publication", deadlineAfter: 34},
	} {
		t.Run(tc.name, func(t *testing.T) {
			searcher, err := OpenVectorPartitionLocalSearcherV1(asset)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = searcher.Close() })

			// Preflight and the exact scan make 17 cancellation polls. Heap
			// draining polls 16 more times and the final poll precedes stats
			// publication. The old full sort made only 18 total polls and
			// would incorrectly return success for both cases.
			ctx := &vectorPartitionLocalSearcherDeadlineAfterErrContextV1{
				Context: context.Background(), deadlineAfter: tc.deadlineAfter,
			}
			results, metrics, err := searcher.SearchWithOptionsV1(ctx, []float32{1}, VectorPartitionSearchOptionsV1{
				TopK: rows,
			})
			if !errors.Is(err, context.DeadlineExceeded) || len(results) != 0 || metrics != (VectorPartitionSearchMetricsV1{}) {
				t.Fatalf("results=%+v metrics=%+v err=%v want deadline and no partial result", results, metrics, err)
			}
			if ctx.calls != ctx.deadlineAfter {
				t.Fatalf("deadline polls=%d want %d", ctx.calls, ctx.deadlineAfter)
			}
			if status := searcher.Status(); status.ActivePins != 0 || status.Searches != 0 || status.Failures != 1 {
				t.Fatalf("deadline return retained exact-search pin or stats: %+v", status)
			}
		})
	}
}

func TestVectorPartitionLocalSearcherV1DeadlineInterruptsNativeTraversalAndReleasesResources(t *testing.T) {
	const rows = 4096
	input := columnHNSWSearchPackBuildInput{
		Rows: rows, Dimensions: 1, VectorStride: 4,
		M: 1, EfConstruction: rows, EfSearch: rows,
		EntryOrdinal: 0, MaxLayer: 0,
		BaseIdentity: columnHNSWSearchPackBaseIdentity{
			ManifestGeneration: 11,
			ManifestChecksum:   12,
			SchemaHash:         13,
		},
		NormalizedVectors:       make([]float32, rows*4),
		Levels:                  make([]uint16, rows),
		AdjacencyLayers:         []columnHNSWSearchPackLayerInput{{Offsets: make([]uint64, rows+1)}},
		RowRefGenerations:       make([]int64, rows),
		RowRefPartIDs:           make([]int64, rows),
		RowRefRowIndexes:        make([]int64, rows),
		RowRefAppliedCommandLSN: make([]int64, rows),
		DocumentIDOffsets:       make([]uint64, rows+1),
		DocumentIDBytes:         bytes.Repeat([]byte{'x'}, rows),
	}
	for ordinal := range rows {
		input.NormalizedVectors[ordinal*4] = 1
		input.RowRefGenerations[ordinal] = 11
		input.RowRefPartIDs[ordinal] = 1
		input.RowRefRowIndexes[ordinal] = int64(ordinal)
		input.RowRefAppliedCommandLSN[ordinal] = 1
		input.DocumentIDOffsets[ordinal] = uint64(ordinal)
	}
	input.DocumentIDOffsets[rows] = uint64(rows)
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, handle := testColumnHNSWSearchPackPreparedViewFromBytes2314(
		t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity,
	)
	searcher := &VectorPartitionLocalSearcherV1{
		asset:            VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 2, Dimensions: 1},
		prepared:         view,
		opened:           1,
		homeMemberships:  rows,
		packBytes:        uint64(len(raw)),
		heapBytes:        uint64(len(raw)),
		searchRoute:      VectorPartitionSearchRouteHNSWSearchPackV1,
		maxStableIDBytes: 1,
	}
	t.Cleanup(func() { _ = searcher.Close() })

	// Four polls cover local-search preflight plus pack scratch/query setup.
	// The ninth poll fires only after the disconnected layer-0 traversal has
	// seeded multiple rows, making this a deterministic mid-traversal deadline.
	ctx := &vectorPartitionLocalSearcherDeadlineAfterErrContextV1{
		Context: context.Background(), deadlineAfter: 9,
	}
	results, metrics, err := searcher.SearchWithOptionsV1(ctx, []float32{1}, VectorPartitionSearchOptionsV1{
		TopK: 1, EfSearch: rows,
	})
	if !errors.Is(err, context.DeadlineExceeded) || len(results) != 0 || metrics != (VectorPartitionSearchMetricsV1{}) {
		t.Fatalf("results=%+v metrics=%+v err=%v want deadline and no partial result", results, metrics, err)
	}
	if ctx.calls != ctx.deadlineAfter {
		t.Fatalf("deadline polls=%d want %d", ctx.calls, ctx.deadlineAfter)
	}
	if status := searcher.Status(); status.ActivePins != 0 || status.Searches != 0 || status.Failures != 1 {
		t.Fatalf("deadline return retained search pin or stats: %+v", status)
	}
	if err := searcher.Close(); err != nil {
		t.Fatal(err)
	}
	if !handle.Released() {
		t.Fatal("deadline return retained prepared search resource after lease close")
	}
}

func TestVectorPartitionLocalSearcherV1FailsClosed(t *testing.T) {
	_, e := OpenVectorPartitionLocalSearcherV1(VectorPartitionSearchAssetV1{ManifestChecksum: "bad", Generation: 1, Dimensions: 1, IDs: []string{"x", "x"}, Vectors: [][]float32{{1}, {1}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1, VectorPartitionMembershipHomeV1}})
	if !errors.Is(e, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("%v", e)
	}
}
