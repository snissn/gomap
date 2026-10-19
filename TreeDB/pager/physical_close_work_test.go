package pager

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
)

func TestPhysicalCloseLargeConstructorMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(4 << 20); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := OpenReadOnly(path, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	handle := p.file
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = handle.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("physical file not closed", err)
	}
	if p.file != nil || p.chunks != nil || p.mappings != nil || p.closeWork.view != nil {
		t.Fatal("terminal aliases retained")
	}
	if err = p.Close(); err != nil {
		t.Fatal("terminal replay", err)
	}
}
func TestPhysicalCloseOwnedChildRefundAfterAliasScrub(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Pager{})))
	if err := owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "owned-child")
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenOwnedFileWithMetadata(f, f.Name(), 4<<20, OpenOptions{}, &owner, charge)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != 0 || p.ownedFileCharge != 0 || p.file != nil || p.path != "" || p.ownedFileMetadata != nil {
		t.Fatal("physical child did not scrub/refund original contribution")
	}
}

// The retained publication handle must still fence writes after the original
// pager unmaps and closes its own descriptor.
func TestPhysicalCloseRetainedHandleSyncAfterDirtyWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 4096)
	data[0], data[len(data)-1] = 0x51, 0x72
	if err := p.Write(id, data); err != nil {
		t.Fatal(err)
	}
	retained, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.SyncIndexDataWithStableFile(retained); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	if _, err := retained.ReadAt(got, int64(id)*4096); err != nil {
		t.Fatal(err)
	}
	if got[0] != data[0] || got[len(got)-1] != data[len(data)-1] {
		t.Fatal("retained barrier lost original mapped writes")
	}
	if p.dirtyChunks.count != 0 || p.dirtyChunks.low != 0 || p.dirtyChunks.high != 0 || p.dirtyChunks.words != nil {
		t.Fatal("terminal dirty membership retained disposed mapping bounds")
	}
	if err := retained.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.SyncIndexDataWithStableFile(retained); err == nil {
		t.Fatalf("closed retained handle sync = %v", err)
	}
}

func TestPhysicalCloseRetainedSyncFailureDoesNotRestoreDisposedMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Write(id, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	retained, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	injected := errors.New("retained barrier interrupted after original close")
	closed := false
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point != durabilitycut.BeforeIndexDataSync || e.Path != path {
			return nil
		}
		if err := p.Close(); err != nil {
			return err
		}
		closed = true
		return injected
	})
	err = p.SyncIndexDataWithStableFile(retained)
	restore()
	if !closed || !errors.Is(err, injected) {
		t.Fatalf("original close/failure = %v, %v", closed, err)
	}
	if p.dirtyChunks.words != nil || p.dirtyChunks.count != 0 || p.dirtyChunks.low != 0 || p.dirtyChunks.high != 0 {
		t.Fatal("failed barrier resurrected disposed mappings")
	}
	if err := p.SyncIndexDataWithStableFile(retained); err != nil {
		t.Fatal(err)
	}
}

// Ordinary foreground allocation must not allocate a private Growth when the
// original mapping and bookkeeping backings already cover the logical pages.
func TestPagerLogicalAllocationWithinExistingCapacity(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "logical.db"), 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.QuiesceBackgroundGrowth()
	if _, err = p.Alloc(1); err != nil {
		t.Fatal(err)
	}
	start := p.PageCount()
	allocations := testing.AllocsPerRun(64, func() {
		if _, err = p.Alloc(1); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("logical allocation created private control: %g allocations", allocations)
	}
	if p.PageCount() != start+65 {
		t.Fatal("logical publication did not advance exact count")
	}
	// A real backing expansion continues through the constructor and preserves
	// the original allocated page and normal reopen extent.
	prior := p.PageCount()
	id, err := p.Alloc(1024)
	if err != nil || id != prior || p.PageCount() != prior+1024 {
		t.Fatalf("expansion: %d %v", id, err)
	}
}
