package tree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func pointMetadataTree(tb testing.TB) (*Tree, [][]byte) {
	tb.Helper()
	p, err := pager.Open(filepath.Join(tb.TempDir(), "index.db"), 65536)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = p.Close() })
	if _, err := p.Alloc(1); err != nil {
		tb.Fatal(err)
	}
	data, err := p.GetForWrite(0)
	if err != nil {
		tb.Fatal(err)
	}
	b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true, EntryRevisions: true, PackedValuePtr: true})
	b.SetPageID(0)
	reader := newMapValueReader()
	keys := make([][]byte, 32)
	for i := range keys {
		keys[i] = append([]byte("arbitrary\x00namespace:"), make([]byte, 8)...)
		binary.BigEndian.PutUint64(keys[i][len(keys[i])-8:], uint64(i*2))
		flags, val := byte(node.FlagInline), []byte("value")
		if i == 2 {
			val = nil
		}
		if i == 3 {
			flags, val = node.FlagTombstone, nil
		}
		var ptr page.ValuePtr
		if i == 4 {
			flags, val, ptr = node.FlagPointer, nil, reader.Add([]byte("value"))
		}
		if err := b.AddLeafEntryWithRevision(keys[i], val, flags, ptr, page.EntryRevision(100+i)); err != nil {
			tb.Fatal(err)
		}
	}
	b.FinishNoNode()
	return New(p, reader, 0), keys
}

func TestPointValueMetadataOwnedAndRevision(t *testing.T) {
	tr, keys := pointMetadataTree(t)
	for i, key := range keys {
		out, rev, err := tr.GetVersionedAppend(key, []byte("prefix:"))
		if rev != page.EntryRevision(100+i) {
			t.Fatalf("revision %d: %d", i, rev)
		}
		if i == 3 {
			if !errors.Is(err, ErrKeyNotFound) || string(out) != "prefix:" {
				t.Fatalf("tombstone: %q %v", out, err)
			}
			continue
		}
		want := "prefix:value"
		if i == 2 {
			want = "prefix:"
		}
		if err != nil || string(out) != want {
			t.Fatalf("read %d: %q %v", i, out, err)
		}
	}
	for _, i := range []int{1, 2, 4} {
		out, err := tr.GetUnsafe(keys[i])
		want := []byte("value")
		if i == 2 {
			want = nil
		}
		if err != nil || !bytes.Equal(out, want) {
			t.Fatalf("unsafe %d: %q %v", i, out, err)
		}
	}
	out, err := tr.Get(keys[1])
	if err != nil {
		t.Fatal(err)
	}
	out[0] = 'X'
	again, err := tr.Get(keys[1])
	if err != nil || !bytes.Equal(again, []byte("value")) {
		t.Fatalf("owned: %q %v", again, err)
	}
	for _, i := range []int{1, 2, 3} {
		present, err := tr.Has(keys[i])
		if err != nil || present != (i != 3) {
			t.Fatalf("has %d: %v %v", i, present, err)
		}
	}
	missing := append([]byte(nil), keys[1]...)
	missing[len(missing)-1]++
	if _, rev, err := tr.GetVersionedAppend(missing, nil); !errors.Is(err, ErrKeyNotFound) || rev != page.LegacyEntryRevision {
		t.Fatalf("missing: %d %v", rev, err)
	}
}

func TestPointValueMetadataNoKeyAllocation(t *testing.T) {
	tr, keys := pointMetadataTree(t)
	dst := make([]byte, 0, 16)
	allocs := testing.AllocsPerRun(100, func() {
		out, rev, err := tr.GetVersionedAppend(keys[1], dst)
		if err != nil || len(out) != 5 || rev != 101 {
			panic("point read")
		}
	})
	if allocs != 0 {
		t.Fatalf("point metadata allocated %.0f times; matched key must not be reconstructed", allocs)
	}
}

func BenchmarkPointValueMetadata(b *testing.B) {
	tr, keys := pointMetadataTree(b)
	dst := make([]byte, 0, 16)
	for _, mode := range []string{"hit", "miss", "mixed"} {
		queries := make([][]byte, 32)
		for i := range queries {
			queries[i] = append([]byte(nil), keys[i]...)
			if mode == "miss" || mode == "mixed" && i%2 == 1 {
				queries[i][len(queries[i])-1]++
			}
		}
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, err := tr.GetVersionedAppend(queries[i%len(queries)], dst)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					b.Fatal(err)
				}
			}
		})
	}
}
