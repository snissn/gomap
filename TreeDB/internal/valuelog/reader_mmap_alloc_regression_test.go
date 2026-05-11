package valuelog

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func appendCompressedFrameForCacheTestsTB(tb testing.TB, writer *Writer, frame int, n int) ([]page.ValuePtr, [][]byte) {
	tb.Helper()
	records := make([]Record, n)
	want := make([][]byte, n)
	for i := 0; i < n; i++ {
		v := make([]byte, 384)
		copy(v, []byte(fmt.Sprintf("frame-%02d-record-%02d:", frame, i)))
		for j := 48; j < len(v); j++ {
			v[j] = byte('a' + frame)
		}
		records[i] = Record{RID: uint64(frame*100 + i + 1), Value: v}
		want[i] = append([]byte(nil), v...)
	}
	dst := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, dst)
	if err != nil {
		tb.Fatalf("append frame %d: %v", frame, err)
	}
	if !stats.Kept {
		tb.Fatalf("expected block-compressed frame %d to be kept", frame)
	}
	return ptrs, want
}

func benchmarkRandomReadGroupedFrameReadUnsafeTo(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip("mmap not supported on windows")
	}

	dir := b.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		b.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		b.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)
	const frames = 512
	const perFrame = 4
	ptrs := make([]page.ValuePtr, 0, frames*perFrame)
	for i := 0; i < frames; i++ {
		fp, _ := appendCompressedFrameForCacheTestsTB(b, writer, i, perFrame)
		ptrs = append(ptrs, fp...)
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		b.Fatalf("new manager: %v", err)
	}
	b.Cleanup(func() { _ = m.Close() })
	m.SetDisableReadChecksum(true)
	m.SetGroupedFrameCacheEntries(4096)
	f := m.files[fileID]
	f.remapToFileSize()

	// Warm grouped-frame decode cache.
	dst := make([]byte, 0, 512)
	for _, ptr := range ptrs {
		var used bool
		dst, used, err = f.ReadUnsafeTo(ptr, false, dst[:0])
		if err != nil {
			b.Fatalf("warm read: %v", err)
		}
		if !used {
			b.Fatalf("expected warm ReadUnsafeTo to reuse dst")
		}
	}

	rng := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ptr := ptrs[rng.Intn(len(ptrs))]
		var used bool
		dst, used, err = f.ReadUnsafeTo(ptr, false, dst[:0])
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		if !used {
			b.Fatalf("expected ReadUnsafeTo to reuse dst")
		}
	}
}

func BenchmarkValueLogRandomReadGroupedFrame_ReadUnsafeTo(b *testing.B) {
	benchmarkRandomReadGroupedFrameReadUnsafeTo(b)
}

func TestBenchmarkValueLogRandomReadGroupedFrame_ReadUnsafeTo_AllocsBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skip benchmark alloc gate in short mode")
	}
	res := testing.Benchmark(func(b *testing.B) {
		benchmarkRandomReadGroupedFrameReadUnsafeTo(b)
	})
	if allocs := res.AllocsPerOp(); allocs > 0 {
		t.Fatalf("alloc regression: allocs/op=%d want 0", allocs)
	}
}

func TestFileReadViaMmapViewTo_CachesGroupedFrameWithNilDst(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)
	ptrs, want := appendCompressedFrameForCacheTestsTB(t, writer, 0, 4)
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	m.SetDisableReadChecksum(true)
	m.SetGroupedFrameCacheEntries(8)

	f := m.files[fileID]
	f.remapToFileSize()

	hits0, misses0, _, _ := f.groupedFrameCacheStats()
	got1, usedDst1, err, ok := f.readViaMmapViewTo(ptrs[0], false, nil)
	if err != nil || !ok {
		t.Fatalf("first read failed: ok=%v err=%v", ok, err)
	}
	if usedDst1 {
		t.Fatalf("expected usedDst=false with nil dst")
	}
	if !bytes.Equal(got1, want[0]) {
		t.Fatalf("first read mismatch")
	}

	hits1, misses1, entries1, _ := f.groupedFrameCacheStats()
	if misses1 <= misses0 {
		t.Fatalf("expected grouped-frame miss on first read: before=%d after=%d", misses0, misses1)
	}
	if hits1 != hits0 {
		t.Fatalf("unexpected grouped-frame hit on first read: before=%d after=%d", hits0, hits1)
	}
	if entries1 == 0 {
		t.Fatalf("expected grouped-frame cache entry after first read")
	}

	got2, usedDst2, err, ok := f.readViaMmapViewTo(ptrs[1], false, nil)
	if err != nil || !ok {
		t.Fatalf("second read failed: ok=%v err=%v", ok, err)
	}
	if usedDst2 {
		t.Fatalf("expected usedDst=false with nil dst")
	}
	if !bytes.Equal(got2, want[1]) {
		t.Fatalf("second read mismatch")
	}

	hits2, misses2, _, _ := f.groupedFrameCacheStats()
	if hits2 <= hits1 {
		t.Fatalf("expected grouped-frame cache hit on second read: before=%d after=%d", hits1, hits2)
	}
	if misses2 != misses1 {
		t.Fatalf("unexpected grouped-frame miss increase on second read: before=%d after=%d", misses1, misses2)
	}
}

func TestValueLogManager_RandomReadGroupedFrameAllocBudget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)
	const frames = 512
	const perFrame = 4
	ptrs := make([]page.ValuePtr, 0, frames*perFrame)
	want := make([][]byte, 0, frames*perFrame)
	for i := 0; i < frames; i++ {
		fp, fv := appendCompressedFrameForCacheTestsTB(t, writer, i, perFrame)
		ptrs = append(ptrs, fp...)
		want = append(want, fv...)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	m.SetDisableReadChecksum(true)
	m.SetGroupedFrameCacheEntries(4096)

	f := m.files[fileID]
	f.remapToFileSize()

	// Warm grouped-frame decode cache.
	for _, ptr := range ptrs {
		v, err := m.ReadUnsafe(ptr)
		if err != nil {
			t.Fatalf("warm read: %v", err)
		}
		if len(v) == 0 {
			t.Fatalf("warm read returned empty value")
		}
	}
	hitsWarm, missesWarm, _, _ := f.groupedFrameCacheStats()
	if hitsWarm == 0 || missesWarm == 0 {
		t.Fatalf("expected grouped-frame cache warm-up to produce hits/misses, hits=%d misses=%d", hitsWarm, missesWarm)
	}

	const readsPerRun = 4000
	allocsPerRun := testing.AllocsPerRun(20, func() {
		rng := rand.New(rand.NewSource(1))
		dst := make([]byte, 0, 512)
		for i := 0; i < readsPerRun; i++ {
			idx := rng.Intn(len(ptrs))
			v, usedDst, err := f.ReadUnsafeTo(ptrs[idx], false, dst[:0])
			if err != nil {
				t.Fatalf("read idx=%d: %v", idx, err)
			}
			if !usedDst {
				t.Fatalf("expected ReadUnsafeTo to reuse dst on grouped frame path")
			}
			if !bytes.Equal(v, want[idx]) {
				t.Fatalf("value mismatch idx=%d", idx)
			}
			dst = v[:0]
		}
	})
	allocsPerRead := allocsPerRun / float64(readsPerRun)
	if allocsPerRead > 0.005 {
		t.Fatalf("alloc regression: allocs/read=%f (allocs/run=%f, reads/run=%d)", allocsPerRead, allocsPerRun, readsPerRun)
	}
}
