package valuelog

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestNewWriter_LazyScratchBuffers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if cap(writer.scratch) != 0 {
		t.Fatalf("cap(scratch)=%d want 0", cap(writer.scratch))
	}
	if cap(writer.rawScratch) != 0 {
		t.Fatalf("cap(rawScratch)=%d want 0", cap(writer.rawScratch))
	}
	if cap(writer.encScratch) != 0 {
		t.Fatalf("cap(encScratch)=%d want 0", cap(writer.encScratch))
	}
	if cap(writer.blockScratch) != 0 {
		t.Fatalf("cap(blockScratch)=%d want 0", cap(writer.blockScratch))
	}

	sinkWriter := NewWriterWithSink(io.Discard, page.ValueLogFileID(2))
	if cap(sinkWriter.scratch) != 0 {
		t.Fatalf("cap(sink scratch)=%d want 0", cap(sinkWriter.scratch))
	}
}

func TestWriterFlush_TrimsOversizedScratchBuffers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	oversized := writerScratchTrimCap + 1
	writer.scratch = make([]byte, 32, oversized)
	writer.rawScratch = make([]byte, 16, oversized)
	writer.encScratch = make([]byte, 8, oversized)
	writer.blockScratch = make([]byte, 4, oversized)
	writer.encLimiter.buf = writer.encScratch
	writer.encLimiter.limit = oversized

	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(writer.scratch) != 0 || cap(writer.scratch) != writerScratchKeepCap {
		t.Fatalf("scratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.scratch), cap(writer.scratch), writerScratchKeepCap)
	}
	if len(writer.rawScratch) != 0 || cap(writer.rawScratch) != writerScratchKeepCap {
		t.Fatalf("rawScratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.rawScratch), cap(writer.rawScratch), writerScratchKeepCap)
	}
	if len(writer.encScratch) != 0 || cap(writer.encScratch) != writerScratchKeepCap {
		t.Fatalf("encScratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.encScratch), cap(writer.encScratch), writerScratchKeepCap)
	}
	if len(writer.blockScratch) != 0 || cap(writer.blockScratch) != writerScratchKeepCap {
		t.Fatalf("blockScratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.blockScratch), cap(writer.blockScratch), writerScratchKeepCap)
	}
	if writer.encLimiter.buf != nil || writer.encLimiter.limit != 0 {
		t.Fatalf("encLimiter not cleared after Flush: buf=%v limit=%d", writer.encLimiter.buf != nil, writer.encLimiter.limit)
	}
}

func TestWriterFlush_KeepsScratchWithinHysteresis(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	scratchCap := writerScratchTrimCap
	rawCap := writerScratchTrimCap - 1
	writer.scratch = make([]byte, 32, scratchCap)
	writer.rawScratch = make([]byte, 16, rawCap)

	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(writer.scratch) != 0 || cap(writer.scratch) != scratchCap {
		t.Fatalf("scratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.scratch), cap(writer.scratch), scratchCap)
	}
	if len(writer.rawScratch) != 0 || cap(writer.rawScratch) != rawCap {
		t.Fatalf("rawScratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.rawScratch), cap(writer.rawScratch), rawCap)
	}
}

func TestWriterRotateTo_TrimsOversizedScratchBuffers(t *testing.T) {
	writer := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))

	oversized := writerScratchTrimCap + 1
	writer.scratch = make([]byte, 32, oversized)
	writer.rawScratch = make([]byte, 16, oversized)
	writer.encScratch = make([]byte, 8, oversized)
	writer.blockScratch = make([]byte, 4, oversized)
	writer.encLimiter.buf = writer.encScratch
	writer.encLimiter.limit = oversized

	dir := t.TempDir()
	path := filepath.Join(dir, "value-000002.log")
	if err := writer.RotateTo(path, page.ValueLogFileID(2)); err != nil {
		t.Fatalf("RotateTo: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if len(writer.scratch) != 0 || cap(writer.scratch) != writerScratchKeepCap {
		t.Fatalf("scratch after RotateTo = len %d cap %d, want len 0 cap %d", len(writer.scratch), cap(writer.scratch), writerScratchKeepCap)
	}
	if len(writer.rawScratch) != 0 || cap(writer.rawScratch) != writerScratchKeepCap {
		t.Fatalf("rawScratch after RotateTo = len %d cap %d, want len 0 cap %d", len(writer.rawScratch), cap(writer.rawScratch), writerScratchKeepCap)
	}
	if len(writer.encScratch) != 0 || cap(writer.encScratch) != writerScratchKeepCap {
		t.Fatalf("encScratch after RotateTo = len %d cap %d, want len 0 cap %d", len(writer.encScratch), cap(writer.encScratch), writerScratchKeepCap)
	}
	if len(writer.blockScratch) != 0 || cap(writer.blockScratch) != writerScratchKeepCap {
		t.Fatalf("blockScratch after RotateTo = len %d cap %d, want len 0 cap %d", len(writer.blockScratch), cap(writer.blockScratch), writerScratchKeepCap)
	}
	if writer.encLimiter.buf != nil || writer.encLimiter.limit != 0 {
		t.Fatalf("encLimiter not cleared after RotateTo: buf=%v limit=%d", writer.encLimiter.buf != nil, writer.encLimiter.limit)
	}
}

func TestWriterClose_ReleasesScratchBuffers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	writer.scratch = make([]byte, 32, writerScratchTrimCap+1)
	writer.rawScratch = make([]byte, 16, writerScratchTrimCap+1)
	writer.encScratch = make([]byte, 8, writerScratchTrimCap+1)
	writer.blockScratch = make([]byte, 4, writerScratchTrimCap+1)
	writer.encLimiter.buf = writer.encScratch
	writer.encLimiter.limit = writerScratchTrimCap + 1

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if writer.scratch != nil {
		t.Fatalf("scratch not released on Close")
	}
	if writer.rawScratch != nil {
		t.Fatalf("rawScratch not released on Close")
	}
	if writer.encScratch != nil {
		t.Fatalf("encScratch not released on Close")
	}
	if writer.blockScratch != nil {
		t.Fatalf("blockScratch not released on Close")
	}
	if writer.encLimiter.buf != nil || writer.encLimiter.limit != 0 {
		t.Fatalf("encLimiter not cleared on Close: buf=%v limit=%d", writer.encLimiter.buf != nil, writer.encLimiter.limit)
	}
}

func TestNewWriter_DefersDirSyncUntilSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	origSyncDir := syncDirFn
	var syncDirCalls int
	syncDirFn = func(path string) error {
		syncDirCalls++
		return nil
	}
	defer func() { syncDirFn = origSyncDir }()

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if syncDirCalls != 0 {
		t.Fatalf("syncDir calls after NewWriter = %d want 0", syncDirCalls)
	}
	if _, err := writer.Append(0, nil, 1, []byte("value")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if syncDirCalls != 1 {
		t.Fatalf("syncDir calls after first Sync = %d want 1", syncDirCalls)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("second Sync: %v", err)
	}
	if syncDirCalls != 1 {
		t.Fatalf("syncDir calls after second Sync = %d want 1", syncDirCalls)
	}
}

func TestWriterRotateTo_DefersDirSyncUntilSync(t *testing.T) {
	origSyncDir := syncDirFn
	var syncDirCalls int
	syncDirFn = func(path string) error {
		syncDirCalls++
		return nil
	}
	defer func() { syncDirFn = origSyncDir }()

	writer := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))
	dir := t.TempDir()
	path1 := filepath.Join(dir, "value-000001.log")
	path2 := filepath.Join(dir, "value-000002.log")
	if err := writer.RotateTo(path1, page.ValueLogFileID(1)); err != nil {
		t.Fatalf("RotateTo(path1): %v", err)
	}
	if syncDirCalls != 0 {
		t.Fatalf("syncDir calls after RotateTo(path1) = %d want 0", syncDirCalls)
	}
	if _, err := writer.Append(0, nil, 1, []byte("value")); err != nil {
		t.Fatalf("Append(path1): %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync(path1): %v", err)
	}
	if syncDirCalls != 1 {
		t.Fatalf("syncDir calls after Sync(path1) = %d want 1", syncDirCalls)
	}
	if err := writer.RotateTo(path2, page.ValueLogFileID(2)); err != nil {
		t.Fatalf("RotateTo(path2): %v", err)
	}
	if syncDirCalls != 1 {
		t.Fatalf("syncDir calls after RotateTo(path2) = %d want 1", syncDirCalls)
	}
	if _, err := writer.Append(0, nil, 2, []byte("value")); err != nil {
		t.Fatalf("Append(path2): %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync(path2): %v", err)
	}
	if syncDirCalls != 2 {
		t.Fatalf("syncDir calls after Sync(path2) = %d want 2", syncDirCalls)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWriterRotateTo_DeferSealedSyncSyncsSealedFilesAtSync(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "value-000001.log")
	path2 := filepath.Join(dir, "value-000002.log")

	writer, err := NewWriter(path1, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	var syncCalls int
	writer.syncFn = func(file *os.File) error {
		syncCalls++
		return nil
	}
	writer.SetDeferSealedSync(true)

	if _, err := writer.Append(0, nil, 1, []byte("value-1")); err != nil {
		t.Fatalf("Append(path1): %v", err)
	}
	if err := writer.RotateTo(path2, page.ValueLogFileID(2)); err != nil {
		t.Fatalf("RotateTo(path2): %v", err)
	}
	if syncCalls != 0 {
		t.Fatalf("sync calls after RotateTo = %d want 0", syncCalls)
	}
	if len(writer.sealedFiles) != 1 {
		t.Fatalf("sealed files after RotateTo = %d want 1", len(writer.sealedFiles))
	}
	if _, err := writer.Append(0, nil, 2, []byte("value-2")); err != nil {
		t.Fatalf("Append(path2): %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if syncCalls != 2 {
		t.Fatalf("sync calls after Sync = %d want 2 (sealed + current)", syncCalls)
	}
	if len(writer.sealedFiles) != 0 {
		t.Fatalf("sealed files after Sync = %d want 0", len(writer.sealedFiles))
	}
}

func TestWriterSync_SkipsRedundantSyncWhenClean(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	var syncCalls int
	writer.syncFn = func(file *os.File) error {
		syncCalls++
		return nil
	}
	if _, err := writer.Append(0, nil, 1, []byte("value")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("first Sync: %v", err)
	}
	if syncCalls != 1 {
		t.Fatalf("sync calls after first Sync = %d want 1", syncCalls)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("second Sync: %v", err)
	}
	if syncCalls != 1 {
		t.Fatalf("sync calls after second Sync = %d want 1", syncCalls)
	}
}
