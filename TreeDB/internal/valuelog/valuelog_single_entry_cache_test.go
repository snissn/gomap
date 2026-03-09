package valuelog

import (
	"bytes"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogManager_MmapReadAppendCompressedGroupedSingleEntrySkipsCache(t *testing.T) {
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

	value := bytes.Repeat([]byte("single-entry-cache-bypass-"), 16)
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, []Record{{RID: 1, Value: value}}, make([]page.ValuePtr, 1))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append frame: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected block-compressed single-entry frame to be kept")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetDisableReadChecksum(true)
	defer func() { _ = m.Close() }()

	f := m.files[fileID]
	f.remapToFileSize()

	var got []byte
	for i := 0; i < 2; i++ {
		got, err = m.ReadAppend(ptrs[0], got[:0])
		if err != nil {
			t.Fatalf("read append %d: %v", i, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("value mismatch at %d", i)
		}
	}

	hits, misses, entries, capacity := f.groupedFrameCacheStats()
	if hits != 0 {
		t.Fatalf("expected single-entry grouped reads to skip grouped cache hits, got=%d", hits)
	}
	if misses != 0 {
		t.Fatalf("expected single-entry grouped reads to skip grouped cache misses, got=%d", misses)
	}
	if entries != 0 {
		t.Fatalf("expected no grouped cache entries for single-entry frames, got=%d", entries)
	}
	if capacity == 0 {
		t.Fatalf("expected grouped cache to remain configured")
	}

	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if f.cacheFlags != 0 || f.cacheK != 0 || f.cacheLen != 0 || len(f.cacheRaw) != 0 || f.cacheStart.Load() != 0 {
		t.Fatalf("expected single-entry fast path to skip prefix cache publication, flags=%d cacheK=%d cacheLen=%d raw=%d start=%d", f.cacheFlags, f.cacheK, f.cacheLen, len(f.cacheRaw), f.cacheStart.Load())
	}
}
