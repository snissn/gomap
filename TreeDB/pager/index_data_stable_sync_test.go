package pager

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

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
	if len(p.dirtyChunks) == 0 {
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
	if len(p.dirtyChunks) != 0 {
		_ = p.Close()
		t.Fatalf("dirty chunks after live barrier=%d want 0", len(p.dirtyChunks))
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
	if len(p.dirtyChunks) == 0 {
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
