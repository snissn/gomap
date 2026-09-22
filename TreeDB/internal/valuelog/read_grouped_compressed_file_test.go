package valuelog

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func openGroupedCompressedFileReadFallbackFixture(tb testing.TB) (*File, []page.ValuePtr, [][]byte) {
	tb.Helper()

	dir := tb.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		tb.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		tb.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	records := make([]Record, 8)
	want := make([][]byte, len(records))
	for i := range records {
		v := make([]byte, 32<<10)
		copy(v, []byte(fmt.Sprintf("record-%02d:", i)))
		for j := 32; j < len(v); j++ {
			v[j] = 'x'
		}
		records[i] = Record{RID: uint64(i + 1), Value: v}
		want[i] = append([]byte(nil), v...)
	}

	ptrScratch := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, ptrScratch)
	if err != nil {
		_ = writer.Close()
		tb.Fatalf("append frame: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		tb.Fatalf("expected block-compressed frame to be kept")
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("close writer: %v", err)
	}

	f, err := openFile(path, fileID, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		tb.Fatalf("open file: %v", err)
	}
	tb.Cleanup(func() { _ = f.Close() })
	f.setGroupedFrameCacheEntries(0)

	return f, ptrs, want
}

func TestFileReadGroupedCompressedFromFileTo_UsesDstWithCacheDisabled(t *testing.T) {
	f, ptrs, want := openGroupedCompressedFileReadFallbackFixture(t)

	dst := make([]byte, 0, len(want[0]))
	dstBacking := dst[:1]
	got, usedDst, err, ok := f.readGroupedCompressedFromFileTo(ptrs[0], dst)
	if err != nil {
		t.Fatalf("readGroupedCompressedFromFileTo: %v", err)
	}
	if !ok {
		t.Fatalf("expected grouped compressed fast path")
	}
	if !usedDst {
		t.Fatalf("expected usedDst=true")
	}
	if len(got) != len(want[0]) {
		t.Fatalf("got len=%d want=%d", len(got), len(want[0]))
	}
	if &got[0] != &dstBacking[0] {
		t.Fatalf("expected returned slice to be backed by dst")
	}
	if !bytes.Equal(got, want[0]) {
		t.Fatalf("value mismatch")
	}
}

func TestFileReadGroupedCompressedFromFileTo_ReadNilDstWithCacheDisabled(t *testing.T) {
	f, ptrs, want := openGroupedCompressedFileReadFallbackFixture(t)

	got, usedDst, err, ok := f.readGroupedCompressedFromFileTo(ptrs[0], nil)
	if err != nil {
		t.Fatalf("readGroupedCompressedFromFileTo: %v", err)
	}
	if !ok {
		t.Fatalf("expected grouped compressed fast path")
	}
	if usedDst {
		t.Fatalf("expected usedDst=false")
	}
	if len(got) != len(want[0]) {
		t.Fatalf("got len=%d want=%d", len(got), len(want[0]))
	}
	if cap(got) != len(got) {
		t.Fatalf("expected tight copy for nil dst (cap=%d len=%d)", cap(got), len(got))
	}
	if !bytes.Equal(got, want[0]) {
		t.Fatalf("value mismatch")
	}
}

func BenchmarkFileReadGroupedCompressedFromFileTo(b *testing.B) {
	f, ptrs, want := openGroupedCompressedFileReadFallbackFixture(b)

	b.Run("nil_dst", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, usedDst, err, ok := f.readGroupedCompressedFromFileTo(ptrs[i%len(ptrs)], nil)
			if err != nil {
				b.Fatalf("readGroupedCompressedFromFileTo: %v", err)
			}
			if !ok {
				b.Fatal("expected grouped compressed fast path")
			}
			if usedDst {
				b.Fatal("expected usedDst=false")
			}
			if len(got) != len(want[0]) {
				b.Fatalf("got len=%d want=%d", len(got), len(want[0]))
			}
		}
	})

	b.Run("reused_dst", func(b *testing.B) {
		dst := make([]byte, 0, len(want[0]))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, usedDst, err, ok := f.readGroupedCompressedFromFileTo(ptrs[i%len(ptrs)], dst[:0])
			if err != nil {
				b.Fatalf("readGroupedCompressedFromFileTo: %v", err)
			}
			if !ok {
				b.Fatal("expected grouped compressed fast path")
			}
			if !usedDst {
				b.Fatal("expected usedDst=true")
			}
			if len(got) != len(want[0]) {
				b.Fatalf("got len=%d want=%d", len(got), len(want[0]))
			}
		}
	})
}
