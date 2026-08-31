package valuelog

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"sync"
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

func TestFileReadViaMmapViewTo_DoesNotCacheGroupedFrameWithNilDst(t *testing.T) {
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
	if hits1 != hits0 {
		t.Fatalf("unexpected grouped-frame hit on first read: before=%d after=%d", hits0, hits1)
	}
	if misses1 != misses0 {
		t.Fatalf("nil-dst miss should not create grouped cache stats: before=%d after=%d", misses0, misses1)
	}
	if entries1 != 0 {
		t.Fatalf("expected no grouped-frame cache entry for nil dst read, got entries=%d", entries1)
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

	hits2, misses2, entries2, _ := f.groupedFrameCacheStats()
	if hits2 != hits1 {
		t.Fatalf("unexpected grouped-frame cache hit on second nil-dst read: before=%d after=%d", hits1, hits2)
	}
	if misses2 != misses1 {
		t.Fatalf("second nil-dst miss should not create grouped cache stats: before=%d after=%d", misses1, misses2)
	}
	if entries2 != 0 {
		t.Fatalf("expected nil-dst reads to avoid grouped-frame entries, got entries=%d", entries2)
	}
}

func TestValueLogGroupedReadParallelThenAppend_NoCorruption(t *testing.T) {
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

	const frames = 256
	const perFrame = 4
	ptrs := make([]page.ValuePtr, 0, frames*perFrame)
	want := make([][]byte, 0, frames*perFrame)
	for i := 0; i < frames; i++ {
		fp, fv := appendCompressedFrameForCacheTestsTB(t, writer, i, perFrame)
		ptrs = append(ptrs, fp...)
		want = append(want, fv...)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	m.SetDisableReadChecksum(true)
	m.SetGroupedFrameCacheEntries(64)
	f := m.files[fileID]
	f.remapToFileSize()

	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < len(ptrs); i += workers {
				got, _, readErr := f.ReadUnsafeTo(ptrs[i], false, nil)
				if readErr != nil {
					errCh <- fmt.Errorf("unsafe read[%d]: %w", i, readErr)
					return
				}
				if !bytes.Equal(got, want[i]) {
					errCh <- fmt.Errorf("unsafe read[%d]: payload mismatch", i)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for readErr := range errCh {
		if readErr != nil {
			t.Fatal(readErr)
		}
	}

	var dst []byte
	for i := range ptrs {
		dst, err = m.ReadAppend(ptrs[i], dst[:0])
		if err != nil {
			t.Fatalf("append read[%d]: %v", i, err)
		}
		if !bytes.Equal(dst, want[i]) {
			t.Fatalf("append read[%d]: payload mismatch", i)
		}
	}
}

func TestValueLogReadAppendDoesNotDoubleOwnGroupedCacheScratch(t *testing.T) {
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

	ptrs1, want1 := appendCompressedFrameForCacheTestsTB(t, writer, 1, 4)
	ptrs2, want2 := appendCompressedFrameForCacheTestsTB(t, writer, 2, 4)
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
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

	dst := make([]byte, 0, 512)
	dst, err = m.ReadAppend(ptrs1[0], dst[:0])
	if err != nil {
		t.Fatalf("first append read: %v", err)
	}
	if !bytes.Equal(dst, want1[0]) {
		t.Fatalf("first append read mismatch")
	}

	dst, err = m.ReadAppend(ptrs2[0], dst[:0])
	if err != nil {
		t.Fatalf("second append read: %v", err)
	}
	if !bytes.Equal(dst, want2[0]) {
		t.Fatalf("second append read mismatch")
	}

	got, usedDst, readErr := f.ReadUnsafeTo(ptrs1[1], false, dst[:0])
	if readErr != nil {
		t.Fatalf("grouped cache read: %v", readErr)
	}
	if !usedDst {
		t.Fatalf("expected grouped cache read to reuse dst")
	}
	if !bytes.Equal(got, want1[1]) {
		t.Fatalf("grouped cache read mismatch after second append read")
	}
}

func TestValueLogReadAppendCurrentWritableMmapGroupedCacheHitReusesDst(t *testing.T) {
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
	defer func() { _ = writer.Close() }()
	writer.SetBlockCompression(BlockCodecSnappy, true)
	ptrs, want := appendCompressedFrameForCacheTestsTB(t, writer, 1, 4)
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush writer: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	m.SetGroupedFrameCacheEntries(8)
	m.SetCurrentWritableMmapEnabled(true)
	if err := m.RegisterSegment(path, fileID); err != nil {
		t.Fatalf("register segment: %v", err)
	}
	if err := m.PromoteCurrentWritable(fileID); err != nil {
		t.Fatalf("promote current writable: %v", err)
	}

	f := m.files[fileID]
	if !f.currentWritable.Load() {
		t.Fatalf("segment is not current writable")
	}
	f.remapToFileSize()
	if data, _ := f.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("current writable segment is not mmapped")
	}

	prefix := []byte("caller-prefix:")
	backing := make([]byte, len(prefix), len(prefix)+512)
	copy(backing, prefix)
	expected := make([]byte, len(prefix)+len(want[1]))
	copy(expected, prefix)
	copy(expected[len(prefix):], want[1])

	if _, err := m.ReadAppend(ptrs[0], backing[:len(prefix)]); err != nil {
		t.Fatalf("warm grouped cache: %v", err)
	}
	hitsBefore, _, _, _ := f.groupedFrameCacheStats()
	mmapHitsBefore := f.mmapReadHits.Load()
	fallbacksBefore := f.mmapReadFallbackReadAt.Load()
	got, err := m.ReadAppend(ptrs[1], backing[:len(prefix)])
	if err != nil {
		t.Fatalf("warm cache hit: %v", err)
	}
	hitsAfter, _, _, _ := f.groupedFrameCacheStats()
	if hitsAfter != hitsBefore+1 {
		t.Fatalf("expected grouped cache hit: before=%d after=%d", hitsBefore, hitsAfter)
	}
	if mmapHitsAfter := f.mmapReadHits.Load(); mmapHitsAfter != mmapHitsBefore+1 {
		t.Fatalf("expected one mmap hit: before=%d after=%d", mmapHitsBefore, mmapHitsAfter)
	}
	if fallbacksAfter := f.mmapReadFallbackReadAt.Load(); fallbacksAfter != fallbacksBefore {
		t.Fatalf("unexpected ReadAt fallback: before=%d after=%d", fallbacksBefore, fallbacksAfter)
	}
	if !bytes.Equal(got, expected) {
		t.Fatalf("ReadAppend bytes mismatch: got=%q want=%q", got, expected)
	}
	if len(got) == 0 || &got[0] != &backing[0] {
		t.Fatalf("ReadAppend did not reuse caller destination")
	}

	const readsPerRun = 1000
	allocsPerRun := testing.AllocsPerRun(20, func() {
		for range readsPerRun {
			var readErr error
			got, readErr = m.ReadAppend(ptrs[1], backing[:len(prefix)])
			if readErr != nil {
				t.Fatalf("cache-hit ReadAppend: %v", readErr)
			}
			if !bytes.Equal(got, expected) {
				t.Fatalf("cache-hit ReadAppend bytes mismatch")
			}
		}
	})
	if allocsPerRead := allocsPerRun / readsPerRun; allocsPerRead > 0.005 {
		t.Fatalf("alloc regression: allocs/read=%f (allocs/run=%f, reads/run=%d)", allocsPerRead, allocsPerRun, readsPerRun)
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
