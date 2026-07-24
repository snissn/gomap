package collections

import (
	"errors"
	"testing"
)

func TestVectorPartitionLocalSearcherV1ExactStableIDsAndPins(t *testing.T) {
	a := VectorPartitionSearchAssetV1{ManifestChecksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Generation: 1, PartitionID: 2, Dimensions: 2, IDs: []string{"b", "a"}, Vectors: [][]float32{{1, 0}, {.9, .1}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1, VectorPartitionMembershipOverlapV1}, Adjacency: [][]uint32{{1}, {0}}}
	s, e := OpenVectorPartitionLocalSearcherV1(a)
	if e != nil {
		t.Fatal(e)
	}
	got, e := s.Search([]float32{1, 0}, 2)
	if e != nil {
		t.Fatal(e)
	}
	if got[0].ID != "b" || got[0].Score <= got[1].Score {
		t.Fatalf("%+v", got)
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
func TestVectorPartitionLocalSearcherV1FailsClosed(t *testing.T) {
	_, e := OpenVectorPartitionLocalSearcherV1(VectorPartitionSearchAssetV1{ManifestChecksum: "bad", Generation: 1, Dimensions: 1, IDs: []string{"x", "x"}, Vectors: [][]float32{{1}, {1}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1, VectorPartitionMembershipHomeV1}})
	if !errors.Is(e, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("%v", e)
	}
}
