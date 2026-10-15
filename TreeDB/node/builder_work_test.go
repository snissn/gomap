package node

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func TestLeafBuilderWorkSharesEncoderAndRefusesBeforeEffects(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		var a, b [page.PageSize]byte
		opts := BuilderOptions{LeafPrefixCompression: prefix, PackedValuePtr: true, EntryRevisions: true}
		x, y := NewBuilderWithOptions(a[:], page.PageTypeLeaf, opts), NewBuilderWithOptions(b[:], page.PageTypeLeaf, opts)
		for i, r := range []LeafEntry{{Key: []byte("aa"), Value: []byte("value"), Revision: 17}, {Key: []byte("ab"), Flags: FlagPointer, ValuePtr: page.ValuePtr{FileID: 2, Offset: 37, Length: 19}, Revision: 29}} {
			before := a
			done, e := x.AddLeafEntryWithRevisionWithWork(r.Key, r.Value, r.Flags, r.ValuePtr, r.Revision, &iterator.OrdinalScanWork{RecordLimit: 0, ByteLimit: 1 << 20})
			var large *iterator.OrdinalUnitTooLarge
			if done || !errors.As(e, &large) || a != before || x.Count() != uint16(i) {
				t.Fatal("refusal changed original", done, e)
			}
			done, e = x.AddLeafEntryWithRevisionWithWork(r.Key, r.Value, r.Flags, r.ValuePtr, r.Revision, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			if !done || e != nil {
				t.Fatal(done, e)
			}
			if e = y.AddLeafEntryWithRevision(r.Key, r.Value, r.Flags, r.ValuePtr, r.Revision); e != nil {
				t.Fatal(e)
			}
		}
		if done, e := x.FinishNoNodeWithWork(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !done || e != nil {
			t.Fatal(done, e)
		}
		y.FinishNoNode()
		if !bytes.Equal(a[:], b[:]) {
			t.Fatal("different ordinary encoder bytes")
		}
	}
}
