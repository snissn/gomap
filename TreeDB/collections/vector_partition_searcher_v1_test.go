package collections

import (
	"context"
	"errors"
	"testing"
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

func TestVectorPartitionLocalSearcherV1FailsClosed(t *testing.T) {
	_, e := OpenVectorPartitionLocalSearcherV1(VectorPartitionSearchAssetV1{ManifestChecksum: "bad", Generation: 1, Dimensions: 1, IDs: []string{"x", "x"}, Vectors: [][]float32{{1}, {1}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1, VectorPartitionMembershipHomeV1}})
	if !errors.Is(e, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("%v", e)
	}
}
