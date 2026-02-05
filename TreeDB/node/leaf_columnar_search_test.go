package node

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafColumnarSearchMatchesPlainLeaf(t *testing.T) {
	const keys = 128

	plainBuf := make([]byte, page.PageSize)
	plainB := NewBuilderWithOptions(plainBuf, page.PageTypeLeaf, BuilderOptions{})
	plainB.SetPageID(1)

	colBuf := make([]byte, page.PageSize)
	colB := NewBuilderWithOptions(colBuf, page.PageTypeLeaf, BuilderOptions{LeafColumnar: true})
	colB.SetPageID(2)

	colPackedBuf := make([]byte, page.PageSize)
	colPackedB := NewBuilderWithOptions(colPackedBuf, page.PageTypeLeaf, BuilderOptions{
		LeafColumnar:   true,
		PackedValuePtr: true,
	})
	colPackedB.SetPageID(3)

	var k [8]byte
	for i := 0; i < keys; i++ {
		binary.BigEndian.PutUint64(k[:], uint64(i))
		key := k[:]
		if i%2 == 0 {
			if err := plainB.AddLeafEntry(key, []byte("v"), FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("plain add inline: %v", err)
			}
			if err := colB.AddLeafEntry(key, []byte("v"), FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("columnar add inline: %v", err)
			}
			if err := colPackedB.AddLeafEntry(key, []byte("v"), FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("columnar packed add inline: %v", err)
			}
		} else {
			ptr := page.ValuePtr{Offset: uint64(i), Length: 1, FileID: 1}
			if err := plainB.AddLeafEntry(key, nil, FlagPointer, ptr); err != nil {
				t.Fatalf("plain add pointer: %v", err)
			}
			if err := colB.AddLeafEntry(key, nil, FlagPointer, ptr); err != nil {
				t.Fatalf("columnar add pointer: %v", err)
			}
			if err := colPackedB.AddLeafEntry(key, nil, FlagPointer, ptr); err != nil {
				t.Fatalf("columnar packed add pointer: %v", err)
			}
		}
	}

	plain := plainB.Finish()
	columnar := colB.Finish()
	columnarPacked := colPackedB.Finish()
	if !columnar.leafColumnar() {
		t.Fatalf("expected columnar leaf flag set")
	}
	if !columnarPacked.leafColumnar() || !columnarPacked.leafPackedValuePtr() {
		t.Fatalf("expected columnar packed leaf flags set")
	}

	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5_000; i++ {
		q := rng.Intn(keys * 2)
		binary.BigEndian.PutUint64(k[:], uint64(q))

		pIdx, pFound, err := plain.SearchLeaf(k[:])
		if err != nil {
			t.Fatalf("plain search: %v", err)
		}
		cIdx, cFound, err := columnar.SearchLeaf(k[:])
		if err != nil {
			t.Fatalf("columnar search: %v", err)
		}
		cpIdx, cpFound, err := columnarPacked.SearchLeaf(k[:])
		if err != nil {
			t.Fatalf("columnar packed search: %v", err)
		}

		if pIdx != cIdx || pFound != cFound {
			t.Fatalf("query=%d mismatch: plain idx=%d found=%v; columnar idx=%d found=%v", q, pIdx, pFound, cIdx, cFound)
		}
		if pIdx != cpIdx || pFound != cpFound {
			t.Fatalf("query=%d mismatch: plain idx=%d found=%v; columnar packed idx=%d found=%v", q, pIdx, pFound, cpIdx, cpFound)
		}

		if pIdx < plain.Count() {
			pKey, _, _, _, err := plain.GetLeafEntryView(pIdx)
			if err != nil {
				t.Fatalf("plain get view: %v", err)
			}
			cKey, _, _, _, err := columnar.GetLeafEntryView(cIdx)
			if err != nil {
				t.Fatalf("columnar get view: %v", err)
			}
			if !bytes.Equal(pKey, cKey) {
				t.Fatalf("query=%d key mismatch at idx=%d: plain=%x columnar=%x", q, pIdx, pKey, cKey)
			}

			cpKey, _, _, _, err := columnarPacked.GetLeafEntryView(cpIdx)
			if err != nil {
				t.Fatalf("columnar packed get view: %v", err)
			}
			if !bytes.Equal(pKey, cpKey) {
				t.Fatalf("query=%d key mismatch at idx=%d: plain=%x columnar_packed=%x", q, pIdx, pKey, cpKey)
			}
		}
	}
}

func TestLeafColumnarSearchDoesNotUseLeafEntryKeyAt(t *testing.T) {
	const keys = smallSearchThreshold + 4
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafColumnar: true})
	b.SetPageID(1)

	var k [8]byte
	for i := 0; i < keys; i++ {
		binary.BigEndian.PutUint64(k[:], uint64(i))
		if err := b.AddLeafEntry(k[:], []byte("v"), FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("add leaf entry: %v", err)
		}
	}

	n := b.Finish()
	n.leafValid = true

	binary.BigEndian.PutUint64(k[:], uint64(keys/2))
	if _, _, err := n.SearchLeaf(k[:]); err != nil {
		t.Fatalf("search leaf: %v", err)
	}
	if !n.leafValid {
		t.Fatalf("SearchLeaf cleared leafValid; expected columnar search to avoid leafEntryKeyAt")
	}
}
