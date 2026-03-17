package valuelog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func benchmarkFallbackFileForReadAppend(b *testing.B, records []Record, compress bool) (*File, []page.ValuePtr, [][]byte) {
	b.Helper()

	dir := b.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		b.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}
	if compress {
		writer.SetBlockCompression(BlockCodecSnappy, true)
	}

	want := make([][]byte, len(records))
	ptrs := make([]page.ValuePtr, len(records))
	if compress {
		out, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, ptrs)
		if err != nil {
			_ = writer.Close()
			b.Fatalf("AppendFrameWithStatsInto: %v", err)
		}
		if !stats.Kept {
			_ = writer.Close()
			b.Fatalf("expected compressed frame to be kept")
		}
		ptrs = out
	} else {
		for i := range records {
			ptr, err := writer.Append(0, nil, records[i].RID, records[i].Value)
			if err != nil {
				_ = writer.Close()
				b.Fatalf("Append: %v", err)
			}
			ptrs[i] = ptr
		}
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("Close writer: %v", err)
	}

	for i := range records {
		want[i] = append([]byte(nil), records[i].Value...)
	}

	fh, err := os.Open(path)
	if err != nil {
		b.Fatalf("Open(%s): %v", path, err)
	}
	b.Cleanup(func() { _ = fh.Close() })

	f := &File{ID: fileID, Path: path, File: fh}
	// Force ReadAppend down the ReadAt fallback path without triggering remaps.
	mapped := []byte{0}
	f.mmapData.Store(mapped)
	f.deadMappingsCount.Store(uint64(effectiveMaxDeadMappings(len(mapped))))

	return f, ptrs, want
}

func BenchmarkFileReadAppend_Fallback(b *testing.B) {
	b.Run("plain", func(b *testing.B) {
		records := []Record{{RID: 1, Value: bytes.Repeat([]byte("a"), 256)}}
		f, ptrs, want := benchmarkFallbackFileForReadAppend(b, records, false)
		buf := make([]byte, 0, len(want[0]))

		b.ReportAllocs()
		b.SetBytes(int64(len(want[0])))
		b.ResetTimer()

		var sink byte
		for i := 0; i < b.N; i++ {
			got, err := f.ReadAppend(ptrs[0], false, buf[:0])
			if err != nil {
				b.Fatalf("ReadAppend: %v", err)
			}
			if !bytes.Equal(got, want[0]) {
				b.Fatalf("ReadAppend mismatch: got=%q want=%q", got, want[0])
			}
			buf = got
			sink ^= got[0]
		}
		if sink == 0xff {
			b.Fatalf("sink")
		}
	})

	b.Run("grouped_compressed", func(b *testing.B) {
		records := make([]Record, 16)
		for i := range records {
			v := make([]byte, 320)
			copy(v, []byte(fmt.Sprintf("record-%02d:", i)))
			for j := 32; j < len(v); j++ {
				v[j] = 'x'
			}
			records[i] = Record{RID: uint64(i + 1), Value: v}
		}
		f, ptrs, want := benchmarkFallbackFileForReadAppend(b, records, true)
		buf := make([]byte, 0, len(want[0]))

		b.ReportAllocs()
		b.SetBytes(int64(len(want[0])))
		b.ResetTimer()

		var sink byte
		for i := 0; i < b.N; i++ {
			idx := i % len(ptrs)
			got, err := f.ReadAppend(ptrs[idx], false, buf[:0])
			if err != nil {
				b.Fatalf("ReadAppend: %v", err)
			}
			if !bytes.Equal(got, want[idx]) {
				b.Fatalf("ReadAppend mismatch at %d: got=%q want=%q", idx, got, want[idx])
			}
			buf = got
			sink ^= got[0]
		}
		if sink == 0xff {
			b.Fatalf("sink")
		}
	})
}
