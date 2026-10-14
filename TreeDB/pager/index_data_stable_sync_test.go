package pager

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestDirtyChunkSyncRetainsFlushAndRetriesStableFileBarrier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	chunkSize := syncPagesTestChunkSize(1)
	p, err := Open(path, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetSyncConcurrency(2)
	pagesPerChunk := int(chunkSize / int64(page.PageSize))
	if _, err := p.Alloc(pagesPerChunk + 1); err != nil {
		t.Fatal(err)
	}
	stable, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stable.Close()
	beforeSize := p.durableFileSize.Load()
	originalFile := syncPageFileFn
	fileCalls := 0
	failSync := true
	wantErr := errors.New("injected index-data sync failure")
	syncPageFileFn = func(file *os.File) error {
		fileCalls++
		if file != stable {
			t.Fatal("sync did not use the exact retained file")
		}
		if failSync {
			return wantErr
		}
		return originalFile(file)
	}
	t.Cleanup(func() { syncPageFileFn = originalFile })
	for _, id := range []uint64{0, uint64(pagesPerChunk)} {
		if err := p.Write(id, bytes.Repeat([]byte{0x5a}, page.PageSize)); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.FlushDirtyChunksFrom(1); err != nil {
		t.Fatal(err)
	}
	lowerDirty := p.dirtyChunks.contains(0)
	upperDirty := p.dirtyChunks.contains(1)
	if !lowerDirty || upperDirty || fileCalls != 0 || p.durableFileSize.Load() != beforeSize {
		t.Fatalf("flush-only state: lower=%t upper=%t file_calls=%d durable_size=%d", lowerDirty, upperDirty, fileCalls, p.durableFileSize.Load())
	}
	want := bytes.Repeat([]byte{0xa5}, page.PageSize)
	if err := p.Write(uint64(pagesPerChunk), want); err != nil {
		t.Fatal(err)
	}
	if err := p.SyncIndexDataWithStableFile(stable); !errors.Is(err, wantErr) {
		t.Fatalf("failed file barrier=%v want %v", err, wantErr)
	}
	if p.dirtyChunks.count != 2 || fileCalls != 1 || p.durableFileSize.Load() != beforeSize {
		t.Fatalf("failed barrier: dirty=%d file_calls=%d durable_size=%d", p.dirtyChunks.count, fileCalls, p.durableFileSize.Load())
	}
	failSync = false
	if err := p.SyncIndexDataWithStableFile(stable); err != nil {
		t.Fatal(err)
	}
	if p.dirtyChunks.count != 0 || fileCalls != 2 {
		t.Fatalf("retry barrier: dirty=%d file_calls=%d", p.dirtyChunks.count, fileCalls)
	}
	info, err := stable.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if got := p.durableFileSize.Load(); got != info.Size() {
		t.Fatalf("durable file size=%d want %d", got, info.Size())
	}
	got := make([]byte, page.PageSize)
	for _, id := range []uint64{0, uint64(pagesPerChunk)} {
		if _, err := stable.ReadAt(got, int64(id)*int64(page.PageSize)); err != nil {
			t.Fatal(err)
		}
		expected := want
		if id == 0 {
			expected = bytes.Repeat([]byte{0x5a}, page.PageSize)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("retained file page %d differs after retry", id)
		}
	}
}

func TestSyncIndexDataWithStableFileRejectsNilBeforeDurabilityCut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var points []durabilitycut.Point
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceIndex {
			points = append(points, event.Point)
		}
		return nil
	})
	defer restore()

	if err := p.SyncIndexDataWithStableFile(nil); err == nil {
		t.Fatal("nil stable target unexpectedly entered durability barrier")
	}
	if len(points) != 0 {
		t.Fatalf("nil stable target emitted durability cuts=%v want none", points)
	}
}

func TestSyncIndexDataWithStableFileDrainsLiveMappingsAndSurvivesClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	pageID, err := p.Alloc(1)
	if err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if err := p.Write(pageID, bytes.Repeat([]byte{0x5a}, page.PageSize)); err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if p.dirtyChunks.count == 0 {
		_ = p.Close()
		t.Fatal("live pager has no dirty mmap chunk before index-data barrier")
	}
	stable, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	defer stable.Close()
	if err := p.SyncIndexDataWithStableFile(stable); err != nil {
		_ = p.Close()
		t.Fatalf("live stable-file barrier: %v", err)
	}
	if p.dirtyChunks.count != 0 {
		_ = p.Close()
		t.Fatalf("dirty chunks after live barrier=%d want 0", p.dirtyChunks.count)
	}
	info, err := stable.Stat()
	if err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if got := p.durableFileSize.Load(); got != info.Size() {
		_ = p.Close()
		t.Fatalf("durable file size=%d want %d", got, info.Size())
	}
	if err := p.Write(pageID, bytes.Repeat([]byte{0xa5}, page.PageSize)); err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	closedTarget, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if err := closedTarget.Close(); err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if err := p.SyncIndexDataWithStableFile(closedTarget); err == nil {
		_ = p.Close()
		t.Fatal("closed stable target unexpectedly completed durability barrier")
	}
	if p.dirtyChunks.count == 0 {
		_ = p.Close()
		t.Fatal("failed stable-file sync did not restore dirty mmap bookkeeping")
	}
	if err := p.SyncIndexDataWithStableFile(stable); err != nil {
		_ = p.Close()
		t.Fatalf("retry stable-file barrier: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.SyncIndexDataWithStableFile(stable); err != nil {
		t.Fatalf("retained stable-file barrier after pager close: %v", err)
	}
}

func TestSyncPagesWithStableFileUsesPinnedIdentityAfterPathReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")
	p, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(2); err != nil {
		t.Fatal(err)
	}
	if err := p.Write(0, bytes.Repeat([]byte{0x6d}, page.PageSize)); err != nil {
		t.Fatal(err)
	}

	pinned, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	moved := filepath.Join(dir, "index.original")
	if err := os.Rename(path, moved); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("Windows open index handles prevent path replacement: %v", err)
		}
		t.Fatal(err)
	}
	replacement, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := replacement.Truncate(int64(2 * page.PageSize)); err != nil {
		t.Fatal(err)
	}

	if err := p.SyncPagesWithStableFile(replacement, []uint64{0}); err == nil {
		t.Fatal("replacement handle unexpectedly crossed mapped-page barrier")
	}
	if err := p.SyncPagesWithStableFile(pinned, []uint64{0}); err != nil {
		t.Fatalf("pinned mapped-page barrier after path replacement: %v", err)
	}
}
