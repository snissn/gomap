package node

import (
	"bytes"
	"errors"
	"fmt"
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

// The existing work-bearing API must encode the actual combined profile rather
// than reject it or silently choose a plain leaf. Long common prefixes force a
// real comparison continuation; final packing must also resume under Q32.
func TestLeafBuilderCombinedWorkRetainsPacking(t *testing.T) {
	for _, mode := range []string{"inline", "pointer", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			var image, ordinary [page.PageSize]byte
			opts := BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, PackedValuePtr: true, EntryRevisions: true}
			b := NewBuilderWithOptions(image[:], page.PageTypeLeaf, opts)
			ref := NewBuilderWithOptions(ordinary[:], page.PageTypeLeaf, opts)
			rows := make([]LeafEntry, 12)
			for i := range rows {
				rows[i] = LeafEntry{Key: []byte(fmt.Sprintf("%s-%03d", bytes.Repeat([]byte("a"), 96), i)), Value: []byte(fmt.Sprintf("value-%d", i)), Revision: page.EntryRevision(i + 17)}
				if mode == "pointer" || mode == "mixed" && i%3 == 0 {
					rows[i].Flags = FlagPointer
					rows[i].ValuePtr = page.ValuePtr{FileID: 3, Offset: uint64(37 + i*23), Length: 19}
					rows[i].Value = nil
				}
				if mode == "mixed" && i%3 == 1 {
					rows[i].Flags = FlagTombstone
					rows[i].Value = nil
				}
			}
			yields := 0
			for _, r := range rows {
				for tries := 0; ; tries++ {
					if tries > 128 {
						t.Fatal("entry did not progress")
					}
					work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
					done, e := b.AddLeafEntryWithRevisionWithWork(r.Key, r.Value, r.Flags, r.ValuePtr, r.Revision, work)
					if e != nil {
						t.Fatal("actual combined work path", e)
					}
					if work.Records > 32 || work.Bytes > 1<<20 {
						t.Fatal("overspent")
					}
					if done {
						break
					}
					yields++
				}
				if e := ref.AddLeafEntryWithRevision(r.Key, r.Value, r.Flags, r.ValuePtr, r.Revision); e != nil {
					t.Fatal(e)
				}
			}
			paused := false
			for tries := 0; ; tries++ {
				if tries > 128 {
					t.Fatal("finish did not progress")
				}
				done, e := b.FinishNoNodeWithWork(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
				if e != nil {
					t.Fatal(e)
				}
				if done {
					break
				}
				paused = true
				if b.leafColumnarPrefixV2Entries == nil {
					t.Fatal("packing did not retain original scratch")
				}
				if e := b.AddLeafEntry([]byte("z"), nil, 0, page.ValuePtr{}); e == nil {
					t.Fatal("packing allowed mutation", e)
				}
			}
			ref.FinishNoNode()
			if yields == 0 || !paused {
				t.Fatal("missing real continuation", yields, paused)
			}
			if !bytes.Equal(image[:], ordinary[:]) || !page.VerifyChecksumNonMutating(image[:]) {
				t.Fatal("original encoder bytes/checksum changed")
			}
			n := NewNodeView(image[:])
			for i, r := range rows {
				key, val, ptr, flags, e := n.GetLeafEntryView(uint16(i))
				if e != nil || !bytes.Equal(key, r.Key) || !bytes.Equal(val, r.Value) || flags != r.Flags || r.Flags&FlagPointer != 0 && ptr != r.ValuePtr {
					t.Fatalf("entry %d: %q %q %+v flags=%x err=%v", i, key, val, ptr, flags, e)
				}
			}
		})
	}
}
