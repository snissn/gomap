package collections

import (
	"bytes"
	"testing"
)

func TestNativeHNSWPublicSearchOwnsSelectedResults(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 8, 2)
	closed := false
	defer func() {
		if !closed {
			closeAll()
		}
	}()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	var held []VectorSearchResult
	for _, ef := range []int{64, int(^uint(0) >> 1)} {
		rows, _, err := idx.Search([]float32{1, 2}, VectorIndexSearchOptions{TopK: 1, EfSearch: ef, DisableExactFallback: true})
		if err != nil || len(rows) != 1 {
			t.Fatalf("EfSearch=%d: rows=%d err=%v", ef, len(rows), err)
		}
		if cap(rows) != len(rows) {
			t.Fatalf("selected results expose discarded candidates: len=%d cap=%d", len(rows), cap(rows))
		}
		id, document := bytes.Clone(rows[0].DocumentID), bytes.Clone(rows[0].Document)
		if len(id) == 0 || len(document) == 0 {
			t.Fatal("missing selected row")
		}
		rows[0].DocumentID[0] ^= 1
		rows[0].Document[0] ^= 1
		stored, err := c.Get(id)
		if err != nil || !bytes.Equal(stored, document) {
			t.Fatalf("caller mutation changed stored row: %v", err)
		}
		held, _, err = idx.Search([]float32{1, 2}, VectorIndexSearchOptions{TopK: 1, EfSearch: ef, DisableExactFallback: true})
		if err != nil || len(held) != 1 || !bytes.Equal(held[0].DocumentID, id) || !bytes.Equal(held[0].Document, document) {
			t.Fatalf("caller mutation changed runtime or next result: %v", err)
		}
	}
	id, document := bytes.Clone(held[0].DocumentID), bytes.Clone(held[0].Document)
	closeAll()
	closed = true
	if !bytes.Equal(held[0].DocumentID, id) || !bytes.Equal(held[0].Document, document) {
		t.Fatal("selected result lost ownership after DB Close")
	}
}
