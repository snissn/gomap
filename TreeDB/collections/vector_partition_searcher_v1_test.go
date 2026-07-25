package collections

import (
	"bytes"
	"context"
	"errors"
	"testing"

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
