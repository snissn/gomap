package commitlog

import (
	"bytes"
	"path/filepath"
	"testing"
)

func BenchmarkWriterAppendBatchCompressedSmall(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "commit.log")
	w, err := NewWriterWithOptions(path, Options{Compress: true})
	if err != nil {
		b.Fatalf("NewWriterWithOptions: %v", err)
	}
	b.Cleanup(func() { _ = w.Close() })

	records := []Record{{
		Op:    OpSetInline,
		Key:   []byte("k"),
		Value: bytes.Repeat([]byte("x"), 1024),
		Seq:   1,
	}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records[0].Seq = uint64(i + 1)
		if err := w.AppendBatch(records); err != nil {
			b.Fatalf("AppendBatch: %v", err)
		}
	}
}

func BenchmarkWriterAppendBatchCompressedLarge(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "commit.log")
	w, err := NewWriterWithOptions(path, Options{Compress: true})
	if err != nil {
		b.Fatalf("NewWriterWithOptions: %v", err)
	}
	b.Cleanup(func() { _ = w.Close() })

	records := []Record{{
		Op:    OpSetInline,
		Key:   []byte("k"),
		Value: bytes.Repeat([]byte("A"), defaultCompressMinLen),
		Seq:   1,
	}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records[0].Seq = uint64(i + 1)
		if err := w.AppendBatch(records); err != nil {
			b.Fatalf("AppendBatch: %v", err)
		}
	}
}
