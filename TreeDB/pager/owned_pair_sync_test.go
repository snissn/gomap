package pager

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOwnedPairFencePreservesBorrowedAndUnrelatedDirtyRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db.primary")
	chunkSize := syncPagesTestChunkSize(4)
	p, err := Open(path, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	count := int(chunkSize/page.PageSize) + 1
	if _, err = p.Alloc(count); err != nil {
		t.Fatal(err)
	}
	// Retain a mutable borrow before the pair takes pager.mu. Its independent
	// mutation must remain eligible for a later ordinary dirty-file barrier.
	borrowed, err := p.GetForWrite(2)
	if err != nil {
		t.Fatal(err)
	}
	other := uint64(count - 1)
	if err = p.Write(other, bytes.Repeat([]byte{0x39}, page.PageSize)); err != nil {
		t.Fatal(err)
	}
	left, right := bytes.Repeat([]byte{0x11}, page.PageSize), bytes.Repeat([]byte{0x22}, page.PageSize)
	short := &iterator.OrdinalScanWork{RecordLimit: 6, ByteLimit: 1 << 20}
	if _, _, ok, err := p.WriteAdjacentDurableViewsWithWork(0, left, right, short); ok || err == nil {
		t.Fatalf("undersized indivisible admission=%t %v", ok, err)
	}
	actual, err := p.Get(0)
	if err != nil || actual[0] != 0 {
		t.Fatalf("short admission wrote page: %v", err)
	}
	original := syncPageFileFn
	injected := errors.New("owned pair file barrier failed")
	failed := true
	calls := 0
	syncPageFileFn = func(file *os.File) error {
		calls++
		if file != p.file {
			t.Fatal("pair fenced a different physical handle")
		}
		borrowed[0] = 0x7a
		if failed {
			return injected
		}
		return original(file)
	}
	t.Cleanup(func() { syncPageFileFn = original })
	before := p.durableFileSize.Load()
	var cuts []durabilitycut.Point
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Path == path && e.Resource == durabilitycut.ResourceIndex {
			if e.StablePath != p.file {
				t.Fatal("cut did not identify retained physical file")
			}
			cuts = append(cuts, e.Point)
		}
		return nil
	})
	defer func() {
		if restore != nil {
			restore()
		}
	}()
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if _, _, ok, err := p.WriteAdjacentDurableViewsWithWork(0, left, right, work); ok || !errors.Is(err, injected) {
		t.Fatalf("failed pair fence=%t %v", ok, err)
	}
	if p.durableFileSize.Load() != before || !p.dirtyChunks.contains(0) || !p.dirtyChunks.contains(1) {
		t.Fatal("failed pair lost dirty retry or advanced durable extent")
	}
	failed = false
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	l, rr, ok, err := p.WriteAdjacentDurableViewsWithWork(0, left, right, work)
	if !ok || err != nil {
		t.Fatalf("pair retry=%t %v", ok, err)
	}
	left[0], right[0] = 0x55, 0x66
	if l[0] != 0x11 || rr[0] != 0x22 {
		t.Fatal("physical pair aliased reused source scratch")
	}
	if !p.dirtyChunks.contains(0) || !p.dirtyChunks.contains(1) {
		t.Fatal("owned pair cleared independent dirty membership")
	}
	if calls != 2 || len(cuts) != 3 || cuts[0] != durabilitycut.BeforeIndexDataSync || cuts[1] != durabilitycut.BeforeIndexDataSync || cuts[2] != durabilitycut.AfterIndexDataSync {
		t.Fatalf("actual fence/cuts=%d %v", calls, cuts)
	}
	info, err := p.file.Stat()
	if err != nil || p.durableFileSize.Load() != info.Size() {
		t.Fatalf("pair extent=%d %v", p.durableFileSize.Load(), err)
	}
	restore()
	restore = nil
	syncPageFileFn = original
	if err = p.SyncIndexData(); err != nil {
		t.Fatal(err)
	}
	if p.dirtyChunks.count != 0 {
		t.Fatal("ordinary retry did not drain retained membership")
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	image := make([]byte, page.PageSize)
	for id, want := range map[uint64]byte{0: 0x11, 1: 0x22, 2: 0x7a, other: 0x39} {
		if _, err = f.ReadAt(image, int64(id)*page.PageSize); err != nil || image[0] != want {
			t.Fatalf("retained page %d=%x want%x %v", id, image[0], want, err)
		}
	}
	t.Logf("owned pair actual work: %d records %d bytes", work.Records, work.Bytes)
}
