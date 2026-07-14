package pager

import (
	"bytes"
	"os"
	"path/filepath"
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
