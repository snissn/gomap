package node

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestBuilderAddInternalLeafLogChildFromNodeCopiesEncodedEntry(t *testing.T) {
	srcBuilder := NewBuilder(make([]byte, page.PageSize), page.PageTypeInternal)
	refs := []page.ChildRef{
		page.LeafLogChildRef(page.LogRecordRef{FileID: 1, Offset: 128, RecordLengthHint: 4096}),
		page.LeafLogChildRef(page.LogRecordRef{FileID: 1, Offset: 8192, RecordLengthHint: 2048, SubIndex: 2}),
		page.LeafLogChildRef(page.LogRecordRef{FileID: 2, Offset: 16384, RecordLengthHint: 1024}),
	}
	keys := [][]byte{nil, []byte("city:hnl"), []byte("city:sea")}
	for i := range refs {
		if err := srcBuilder.AddInternalChildRef(keys[i], refs[i]); err != nil {
			t.Fatalf("source add %d: %v", i, err)
		}
	}
	src := srcBuilder.Finish()
	if !src.InternalLeafLogRefsEnabled() {
		t.Fatal("source did not finish as leaf-log-ref internal page")
	}

	dstBuilder := NewBuilder(make([]byte, page.PageSize), page.PageTypeInternal)
	for i := range refs {
		if err := dstBuilder.AddInternalLeafLogChildFromNode(src, uint16(i)); err != nil {
			t.Fatalf("copy add %d: %v", i, err)
		}
	}
	dst := dstBuilder.Finish()
	if !dst.InternalLeafLogRefsEnabled() {
		t.Fatal("destination did not finish as leaf-log-ref internal page")
	}
	if got, want := dst.Count(), src.Count(); got != want {
		t.Fatalf("count=%d want %d", got, want)
	}
	for i := range refs {
		key, ref, err := dst.GetInternalEntryRefView(uint16(i))
		if err != nil {
			t.Fatalf("dst entry %d: %v", i, err)
		}
		if !bytes.Equal(key, keys[i]) {
			t.Fatalf("dst key %d=%q want %q", i, key, keys[i])
		}
		if ref != refs[i] {
			t.Fatalf("dst ref %d=%+v want %+v", i, ref, refs[i])
		}
	}
}

func TestBuilderAddInternalLeafLogChildFromNodeRejectsWrongSource(t *testing.T) {
	srcBuilder := NewBuilder(make([]byte, page.PageSize), page.PageTypeInternal)
	if err := srcBuilder.AddInternalChild([]byte{}, 7); err != nil {
		t.Fatalf("source add page child: %v", err)
	}
	src := srcBuilder.Finish()

	dstBuilder := NewBuilder(make([]byte, page.PageSize), page.PageTypeInternal)
	err := dstBuilder.AddInternalLeafLogChildFromNode(src, 0)
	if err != ErrInvalidType {
		t.Fatalf("error=%v want %v", err, ErrInvalidType)
	}
}

func BenchmarkBuilderAddInternalLeafLogChildRef(b *testing.B) {
	refs, _, src := benchmarkLeafLogInternalEntries(b)
	b.Run("decode_reencode", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			dstBuilder := NewBuilder(make([]byte, page.PageSize), page.PageTypeInternal)
			for i := range refs {
				key, ref, err := src.GetInternalEntryRefView(uint16(i))
				if err != nil {
					b.Fatalf("source entry %d: %v", i, err)
				}
				if err := dstBuilder.AddInternalChildRef(key, ref); err != nil {
					b.Fatalf("add %d: %v", i, err)
				}
			}
			_ = dstBuilder.Finish()
		}
	})
	b.Run("copy_encoded", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			dstBuilder := NewBuilder(make([]byte, page.PageSize), page.PageTypeInternal)
			for i := range refs {
				if err := dstBuilder.AddInternalLeafLogChildFromNode(src, uint16(i)); err != nil {
					b.Fatalf("copy %d: %v", i, err)
				}
			}
			_ = dstBuilder.Finish()
		}
	})
}

func benchmarkLeafLogInternalEntries(tb testing.TB) ([]page.ChildRef, [][]byte, *Node) {
	tb.Helper()
	refs := make([]page.ChildRef, 96)
	keys := make([][]byte, len(refs))
	srcBuilder := NewBuilder(make([]byte, page.PageSize), page.PageTypeInternal)
	for i := range refs {
		refs[i] = page.LeafLogChildRef(page.LogRecordRef{
			FileID:           uint32(1 + i/32),
			Offset:           uint64(4096 * i),
			RecordLengthHint: 4096,
			SubIndex:         uint16(i % 8),
		})
		if i > 0 {
			keys[i] = []byte(fmt.Sprintf("key:%04d", i))
		}
		if err := srcBuilder.AddInternalChildRef(keys[i], refs[i]); err != nil {
			tb.Fatalf("source add %d: %v", i, err)
		}
	}
	src := srcBuilder.Finish()
	if !src.InternalLeafLogRefsEnabled() {
		tb.Fatal("source did not finish as leaf-log-ref internal page")
	}
	return refs, keys, src
}
