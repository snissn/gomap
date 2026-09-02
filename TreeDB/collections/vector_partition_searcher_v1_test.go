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
	if status := s.Status(); status.MaxStableIDBytes != 2 || status.HeapBytes != 27 || status.PackBytes != 27 {
		t.Fatalf("exact in-memory status=%+v want max ID 2 and 27-byte cloned+inverse-norm payload", status)
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
	failures := s.Status().Failures
	if _, _, _, err := s.SearchWithAttributionV1(context.Background(), []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1}); !errors.Is(err, ErrVectorPartitionSearchUnavailable) || s.Status().Failures != failures+1 {
		t.Fatalf("exact attribution err=%v status=%+v", err, s.Status())
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
		context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "a" {
		t.Fatalf("HNSW cutoff-tie results=%+v want stable-ID winner a", results)
	}
	if metrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 {
		t.Fatalf("metrics=%+v", metrics)
	}
	first := results[0]
	repeated, repeatedMetrics, err := searcher.SearchWithOptionsV1(
		context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 3},
	)
	if err != nil || len(repeated) != 1 || repeated[0] != first || repeatedMetrics != metrics {
		t.Fatalf("first=%+v repeated=%+v metrics=%+v/%+v err=%v", first, repeated, metrics, repeatedMetrics, err)
	}
	attributed, attributedMetrics, attribution, err := searcher.SearchWithAttributionV1(
		context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 3},
	)
	if err != nil || len(attributed) != 1 || attributed[0] != first || attributedMetrics != metrics ||
		attribution.VisitedRows == 0 {
		t.Fatalf("attributed=%+v metrics=%+v attribution=%+v err=%v", attributed, attributedMetrics, attribution, err)
	}
	checkedOutA := searcher.scratch.Get().(*columnVectorGraphNativeSearchScratch)
	checkedOutB := searcher.scratch.Get().(*columnVectorGraphNativeSearchScratch)
	if checkedOutA == checkedOutB {
		t.Fatal("concurrent scratch checkouts aliased")
	}
	searcher.scratch.Put(checkedOutA)
	searcher.scratch.Put(checkedOutB)
	type searchResult struct {
		results []VectorPartitionSearchResultV1
		metrics VectorPartitionSearchMetricsV1
		err     error
	}
	start, concurrent := make(chan struct{}), make(chan searchResult, 2)
	for range 2 {
		go func() {
			<-start
			results, metrics, err := searcher.SearchWithOptionsV1(
				context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 3},
			)
			concurrent <- searchResult{results: results, metrics: metrics, err: err}
		}()
	}
	close(start)
	for range 2 {
		got := <-concurrent
		if got.err != nil || len(got.results) != 1 || got.results[0] != first || got.metrics != metrics {
			t.Fatalf("concurrent results=%+v metrics=%+v err=%v", got.results, got.metrics, got.err)
		}
	}
	if err := searcher.Close(); err != nil {
		t.Fatal(err)
	}
	if searcher.scratchReady {
		t.Fatal("retired searcher retained its scratch pool")
	}
	if !handle.Released() {
		t.Fatal("HNSW tie search retained prepared resource")
	}
}

func BenchmarkVectorPartitionLocalSearcherV1HNSWScratchV1(b *testing.B) {
	const rows = 4096
	input := testVectorPartitionLocalSearcherDisconnectedHNSWInputV1(rows)
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		b.Fatal(err)
	}
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(
		b, raw, mappedresource.SourceHeapCopy, input.BaseIdentity,
	)
	searcher := &VectorPartitionLocalSearcherV1{
		asset:    VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 2, Dimensions: input.Dimensions},
		prepared: view, opened: 1, homeMemberships: input.Rows,
		searchRoute: VectorPartitionSearchRouteHNSWSearchPackV1, maxStableIDBytes: 1,
	}
	b.Cleanup(func() { _ = searcher.Close() })
	query := []float32{1}
	opts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: rows}
	ctx := context.Background()
	var results []VectorPartitionSearchResultV1
	var metrics VectorPartitionSearchMetricsV1
	var total uint64
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		results, metrics, err = searcher.SearchWithOptionsV1(ctx, query, opts)
		if err != nil {
			b.Fatal(err)
		}
		total += uint64(len(results))
	}
	b.StopTimer()
	if total != uint64(b.N) || len(results) != 1 || results[0].ID == "" || metrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 {
		b.Fatalf("total=%d results=%+v metrics=%+v", total, results, metrics)
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
	results, err := canonicalizeVectorPartitionNativeResultsV1(context.Background(), prepared, []float32{1, 0}, []columnVectorGraphSearchCandidate{
		{ordinal: 0, score: higherFloat64},
		{ordinal: 1, score: -1},
	}, 2)
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
	scorer, err := NewCanonicalVectorPartitionCosineScorerV1(query)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := scorer.ScoreV1(vector)
	if err != nil || math.Float32bits(prepared) != math.Float32bits(got) {
		t.Fatalf("prepared score=%v bits=%#x err=%v want=%v bits=%#x", prepared, math.Float32bits(prepared), err, got, math.Float32bits(got))
	}
	var scoreErr error
	allocs := testing.AllocsPerRun(100, func() {
		_, scoreErr = scorer.ScoreV1(vector)
	})
	if scoreErr != nil || allocs != 0 {
		t.Fatalf("prepared score err=%v allocs/run=%.1f want zero", scoreErr, allocs)
	}
}

func TestCanonicalVectorPartitionAccumulateV1RoundsProductAndSumV1(t *testing.T) {
	sum := math.Float64frombits(0x3ff0000000000001)
	left := math.Float32frombits(0x3f7fffff)
	right := math.Float32frombits(0x3f000001)
	product := math.Float64frombits(math.Float64bits(float64(left) * float64(right)))
	want := math.Float64frombits(math.Float64bits(sum + product))
	got := canonicalVectorPartitionAccumulateV1(sum, left, right)
	if math.Float64bits(got) != math.Float64bits(want) {
		t.Fatalf("canonical accumulation bits=%#x want=%#x", math.Float64bits(got), math.Float64bits(want))
	}
}

func TestVectorPartitionLocalSearcherV1ExactScanAllocationsBoundedByTopKV1(t *testing.T) {
	const (
		rows = 512
		topK = 5
	)
	vectors := make([]float32, rows*4)
	offsets := make([]uint64, rows+1)
	ids := make([]byte, 0, rows*8)
	for row := 0; row < rows; row++ {
		vectors[row*4] = 1
		ids = fmt.Appendf(ids, "%08d", row)
		offsets[row+1] = uint64(len(ids))
	}
	searcher := &VectorPartitionLocalSearcherV1{
		asset: VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 2, Dimensions: 2},
		prepared: &columnHNSWSearchPackPreparedView{
			Header:            columnHNSWSearchPackHeader{Rows: rows, Dimensions: 2, VectorStride: 4},
			NormalizedVectors: vectors,
			DocumentIDOffsets: offsets,
			DocumentIDBytes:   ids,
		},
		opened:           1,
		maxStableIDBytes: 8,
	}
	var searchErr error
	allocs := testing.AllocsPerRun(10, func() {
		var results []VectorPartitionSearchResultV1
		results, _, searchErr = searcher.SearchExactWithOptionsV1(
			context.Background(), []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: topK},
		)
		if len(results) != topK {
			searchErr = fmt.Errorf("results=%d want=%d", len(results), topK)
		}
	})
	if searchErr != nil {
		t.Fatal(searchErr)
	}
	if allocs >= 32 {
		t.Fatalf("exact scan allocs/run=%.1f want O(topK), not O(rows=%d)", allocs, rows)
	}
}

func TestCanonicalizeVectorPartitionNativeResultsAllocationsBoundedByTopKV1(t *testing.T) {
	const (
		rows = 512
		topK = 5
	)
	vectors := make([]float32, rows*2)
	offsets := make([]uint64, rows+1)
	ids := make([]byte, 0, rows*8)
	candidates := make([]columnVectorGraphSearchCandidate, rows)
	for row := 0; row < rows; row++ {
		vectors[row*2] = 1
		ids = fmt.Appendf(ids, "%08d", rows-row-1)
		offsets[row+1] = uint64(len(ids))
		candidates[row] = columnVectorGraphSearchCandidate{ordinal: row}
	}
	prepared := &columnHNSWSearchPackPreparedView{
		Header:            columnHNSWSearchPackHeader{Rows: rows, Dimensions: 2, VectorStride: 2},
		NormalizedVectors: vectors,
		DocumentIDOffsets: offsets,
		DocumentIDBytes:   ids,
	}
	var canonicalErr error
	allocs := testing.AllocsPerRun(10, func() {
		var got []VectorPartitionSearchResultV1
		got, canonicalErr = canonicalizeVectorPartitionNativeResultsV1(context.Background(), prepared, []float32{1, 0}, candidates, topK)
		if len(got) != topK || got[0].ID != "00000000" {
			canonicalErr = fmt.Errorf("canonical results=%+v", got)
		}
	})
	if canonicalErr != nil || allocs >= 32 {
		t.Fatalf("canonical native results err=%v allocs/run=%.1f want O(topK), not O(rows=%d)", canonicalErr, allocs, rows)
	}
}

func TestVectorPartitionLocalSearcherV1ExactScanRejectsPreparedShapeMismatchV1(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dimensions int
		stride     int
	}{
		{name: "dimensions", dimensions: 2, stride: 4},
		{name: "stride", dimensions: 3, stride: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			searcher := &VectorPartitionLocalSearcherV1{
				asset: VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 2, Dimensions: 3},
				prepared: &columnHNSWSearchPackPreparedView{
					Header:            columnHNSWSearchPackHeader{Rows: 1, Dimensions: tc.dimensions, VectorStride: tc.stride},
					NormalizedVectors: make([]float32, tc.stride),
					DocumentIDOffsets: []uint64{0, 1},
					DocumentIDBytes:   []byte("a"),
				},
				opened:           1,
				maxStableIDBytes: 1,
			}
			results, metrics, err := searcher.SearchExactWithOptionsV1(
				context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 1},
			)
			if !errors.Is(err, ErrVectorPartitionSearchUnavailable) || results != nil || metrics != (VectorPartitionSearchMetricsV1{}) {
				t.Fatalf("results=%+v metrics=%+v err=%v want unavailable and no partial result", results, metrics, err)
			}
			if status := searcher.Status(); status.Failures != 1 || status.Searches != 0 || status.ActivePins != 0 {
				t.Fatalf("status=%+v want one failure, no search, released pin", status)
			}
		})
	}
}

func TestVectorPartitionLocalSearcherV1ExactScanAcceptsAlignedOverpaddedStrideV1(t *testing.T) {
	searcher := &VectorPartitionLocalSearcherV1{
		asset: VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 2, Dimensions: 3},
		prepared: &columnHNSWSearchPackPreparedView{
			Header:            columnHNSWSearchPackHeader{Rows: 1, Dimensions: 3, VectorStride: 8},
			NormalizedVectors: []float32{1, 0, 0, 0, 0, 0, 0, 0},
			DocumentIDOffsets: []uint64{0, 1},
			DocumentIDBytes:   []byte("a"),
		},
		opened:           1,
		maxStableIDBytes: 1,
	}
	results, metrics, err := searcher.SearchExactWithOptionsV1(
		context.Background(), []float32{1, 0, 0}, VectorPartitionSearchOptionsV1{TopK: 1},
	)
	if err != nil || len(results) != 1 || results[0].ID != "a" || results[0].Score != 1 ||
		metrics.Route != VectorPartitionSearchRouteExactFP32ScanV1 {
		t.Fatalf("results=%+v metrics=%+v err=%v want valid overpadded exact result", results, metrics, err)
	}
}

func TestVectorPartitionLocalSearcherV1ExactScanUsesCanonicalScoreBitsV1(t *testing.T) {
	query := []float32{-17132608, 10416778, -5245998.5}
	vector := []float32{40529192, 32163242, -2387782.25}
	want, err := CanonicalVectorPartitionCosineScoreV1(query, vector)
	if err != nil {
		t.Fatal(err)
	}
	searcher, err := OpenVectorPartitionLocalSearcherV1(VectorPartitionSearchAssetV1{
		ManifestChecksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Generation:       1,
		PartitionID:      2,
		Dimensions:       len(query),
		IDs:              []string{"a"},
		Vectors:          [][]float32{vector},
		Kinds:            []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = searcher.Close() })
	// The searcher owns its vector snapshot and cached inverse norms; caller mutation after Open
	// cannot change the generation-pinned score bits.
	vector[0] = 0
	got, _, err := searcher.SearchWithOptionsV1(context.Background(), query, VectorPartitionSearchOptionsV1{TopK: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || math.Float32bits(got[0].Score) != math.Float32bits(want) {
		t.Fatalf("exact score=%v bits=%#x want=%v bits=%#x", got, math.Float32bits(got[0].Score), want, math.Float32bits(want))
	}
}

func TestCanonicalizeVectorPartitionNativeResultsRejectsUntrustedIdentityV1(t *testing.T) {
	prepared := &columnHNSWSearchPackPreparedView{
		Header:            columnHNSWSearchPackHeader{Rows: 2, Dimensions: 2, VectorStride: 2},
		NormalizedVectors: []float32{1, 0, 0, 1},
		DocumentIDOffsets: []uint64{0, 1, 2},
		DocumentIDBytes:   []byte("ab"),
	}
	for _, tc := range []struct {
		name      string
		candidate columnVectorGraphSearchCandidate
	}{
		{name: "negative ordinal", candidate: columnVectorGraphSearchCandidate{ordinal: -1}},
		{name: "out of range ordinal", candidate: columnVectorGraphSearchCandidate{ordinal: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := canonicalizeVectorPartitionNativeResultsV1(context.Background(), prepared, []float32{1, 0}, []columnVectorGraphSearchCandidate{tc.candidate}, 1); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
				t.Fatalf("err=%v want unavailable", err)
			}
		})
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
	if got <= 2_000_000 || got >= 3_000_000 {
		t.Fatalf("scratch bytes=%d want compact row-count visit marks plus candidate queues", got)
	}
	if got <= 16*64 {
		t.Fatalf("scratch bytes=%d incorrectly modeled only ef_search candidates", got)
	}
}

func TestVectorPartitionHNSWSearchScratchIncludesOrdinalHandoffResultsV1(t *testing.T) {
	const (
		rows         = 17
		dimensions   = 29
		vectorStride = 32
		degree       = 8
		topK         = 7
		efSearch     = 11
	)
	prepared := &columnHNSWSearchPackPreparedView{Header: columnHNSWSearchPackHeader{
		Rows: rows, Dimensions: dimensions, VectorStride: vectorStride, M: degree / 2, EfSearch: efSearch,
	}}
	got, err := vectorPartitionSearchScratchBytesV1(VectorPartitionSearchOptionsV1{TopK: topK, EfSearch: efSearch}, prepared, 0, dimensions, 0, prepared.Header)
	if err != nil {
		t.Fatal(err)
	}
	frontier := columnVectorGraphNativeSearchFrontierCapacity(rows, degree, topK, efSearch)
	withoutCanonical := uint64(rows)*uint64(unsafe.Sizeof(uint16(0))) +
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
	wantConverted := uint64(topK) * uint64(unsafe.Sizeof(columnVectorGraphSearchCandidate{})+unsafe.Sizeof(VectorPartitionSearchResultV1{}))
	wantCanonicalQuery := uint64(dimensions) * uint64(unsafe.Sizeof(float32(0)))
	if got-withoutCanonical != wantConverted+wantCanonicalQuery {
		t.Fatalf("ordinal result and canonical query scratch=%d want=%d (total=%d)", got-withoutCanonical, wantConverted+wantCanonicalQuery, got)
	}
}

func TestVectorPartitionHNSWSearchScratchIncludesAuxiliaryFanoutV3(t *testing.T) {
	const rows = 64
	base := columnHNSWSearchPackHeader{Rows: rows, Dimensions: 3, VectorStride: 4, M: 2, EfSearch: 8}
	v2 := &columnHNSWSearchPackPreparedView{Header: base}
	offsets := make([]uint64, rows+1)
	for ordinal := 1; ordinal <= rows; ordinal++ {
		offsets[ordinal] = 9
	}
	v3 := &columnHNSWSearchPackPreparedView{Header: base, AuxiliaryNavigation: columnHNSWSearchPackPreparedLayer{Offsets: offsets, Neighbors: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9}}}
	v3.Header.HasAuxiliaryNavigation = true
	if got, err := v2.layer0ExpansionDegreeV1(); err != nil || got != 4 {
		t.Fatalf("v2 expansion=%d err=%v want 4", got, err)
	}
	if got, err := v3.layer0ExpansionDegreeV1(); err != nil || got != 13 {
		t.Fatalf("v3 expansion=%d err=%v want 13", got, err)
	}
	opts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8}
	v2Bytes, err := vectorPartitionSearchScratchBytesV1(opts, v2, 0, 3, 0, v2.Header)
	if err != nil {
		t.Fatal(err)
	}
	v3Bytes, err := vectorPartitionSearchScratchBytesV1(opts, v3, 0, 3, 0, v3.Header)
	if err != nil || v3Bytes <= v2Bytes {
		t.Fatalf("v2=%d v3=%d err=%v", v2Bytes, v3Bytes, err)
	}
	searcher := &VectorPartitionLocalSearcherV1{asset: VectorPartitionSearchAssetV1{Dimensions: 3}, prepared: v3, opened: 1}
	_, preflightBytes, err := searcher.SearchPreflightV1(opts)
	if err != nil || preflightBytes != v3Bytes {
		t.Fatalf("preflight=%d want=%d err=%v", preflightBytes, v3Bytes, err)
	}
}

func TestVectorPartitionExactSearchScratchIncludesCanonicalQueryV1(t *testing.T) {
	const (
		rows       = 3
		dimensions = 29
		topK       = 3
	)
	got, err := vectorPartitionSearchScratchBytesV1(VectorPartitionSearchOptionsV1{TopK: topK}, nil, rows, dimensions, 0, columnHNSWSearchPackHeader{})
	if err != nil {
		t.Fatal(err)
	}
	withoutCanonicalQuery := uint64(topK*2) * uint64(unsafe.Sizeof(VectorPartitionSearchResultV1{}))
	wantCanonicalQuery := uint64(dimensions) * uint64(unsafe.Sizeof(float32(0)))
	if got-withoutCanonicalQuery != wantCanonicalQuery {
		t.Fatalf("canonical query scratch=%d want=%d (total=%d)", got-withoutCanonicalQuery, wantCanonicalQuery, got)
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
	actualHNSW, err := vectorPartitionSearchScratchBytesV1(opts, prepared, 0, 4096, 0, prepared.Header)
	if err != nil {
		t.Fatal(err)
	}
	actualExact, err := vectorPartitionSearchScratchBytesV1(opts, nil, 17, 4096, 0, columnHNSWSearchPackHeader{})
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
	input := testVectorPartitionLocalSearcherDisconnectedHNSWInputV1(rows)
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
	retry, retryMetrics, retryErr := searcher.SearchWithOptionsV1(context.Background(), []float32{1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: rows})
	if retryErr != nil || len(retry) != 1 || retryMetrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 {
		t.Fatalf("retry=%+v metrics=%+v err=%v", retry, retryMetrics, retryErr)
	}
	if err := searcher.Close(); err != nil {
		t.Fatal(err)
	}
	if !handle.Released() {
		t.Fatal("deadline return retained prepared search resource after lease close")
	}
}

func testVectorPartitionLocalSearcherDisconnectedHNSWInputV1(rows int) columnHNSWSearchPackBuildInput {
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
	return input
}

func TestVectorPartitionLocalSearcherV1FailsClosed(t *testing.T) {
	_, e := OpenVectorPartitionLocalSearcherV1(VectorPartitionSearchAssetV1{ManifestChecksum: "bad", Generation: 1, Dimensions: 1, IDs: []string{"x", "x"}, Vectors: [][]float32{{1}, {1}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1, VectorPartitionMembershipHomeV1}})
	if !errors.Is(e, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("%v", e)
	}
}
