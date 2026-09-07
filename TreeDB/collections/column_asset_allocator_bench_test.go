package collections

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkColumnAssetAllocatorFresh(b *testing.B) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		b.Fatal(err)
	}
	root := b.TempDir()
	a, err := newNextColumnPhysicalAssetSegmentAppender(root, *cfg)
	if err != nil {
		b.Fatal(err)
	}
	if err := a.abort(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a, err := newNextColumnPhysicalAssetSegmentAppender(root, *cfg)
		if err != nil {
			b.Fatal(err)
		}
		if err := a.abort(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColumnAssetAllocatorColdScan(b *testing.B) {
	dir := b.TempDir()
	for i := uint32(1); i <= 257; i++ {
		if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(i)), nil, 0600); err != nil {
			b.Fatal(err)
		}
	}
	ns := columnAssetManagerNamespace{SegmentDir: dir}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if id, err := nextColumnAssetSegmentFileID(ns); err != nil || id != 258 {
			b.Fatalf("id=%d err=%v", id, err)
		}
	}
}
