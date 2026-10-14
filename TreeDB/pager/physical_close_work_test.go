package pager

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"unsafe"
)

func TestPhysicalCloseWorkLargeConstructorMapping(t *testing.T) {
	if !nativePartialMappingSupported() {
		t.Skip("native partial mapping unsupported")
	}
	path := filepath.Join(t.TempDir(), "index.db")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Truncate(4 << 20); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	p, e := OpenReadOnly(path, 4<<20)
	if e != nil {
		t.Fatal(e)
	}
	handle := p.file
	zero := &iterator.OrdinalScanWork{}
	if ready, e := p.CloseWork(zero); ready || e != nil || p.closing {
		t.Fatalf("refusal changed close: %v %v", ready, e)
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if ready, e := p.CloseWork(w); ready || e != nil {
		t.Fatalf("close start: %v %v", ready, e)
	}
	if e = p.Close(); !errors.Is(e, ErrCloseWorkRequired) {
		t.Fatalf("ordinary stole cleanup: %v", e)
	}
	if _, e = handle.Stat(); e != nil {
		t.Fatalf("handle closed before mappings: %v", e)
	}
	sawPartial := false
	for n := 0; n < 1000; n++ {
		w = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, e := p.CloseWork(w)
		if e != nil {
			t.Fatal(e)
		}
		if !ready && p.closeWork.offset > 0 {
			sawPartial = true
		}
		if ready {
			if !sawPartial {
				t.Fatal("no partial physical unmap")
			}
			break
		}
		if n == 999 {
			t.Fatal("close did not finish")
		}
	}
	if _, e = handle.Stat(); !errors.Is(e, os.ErrClosed) {
		t.Fatalf("physical file not closed: %v", e)
	}
	if p.file != nil || p.chunks != nil || p.mappings != nil {
		t.Fatal("terminal aliases retained")
	}
	if ready, e := p.CloseWork(zero); !ready || e != nil {
		t.Fatalf("terminal replay: %v %v", ready, e)
	}
}

func TestPhysicalCloseWorkOwnedChildRefundUsesExternalReceipt(t *testing.T) {
	if !nativePartialMappingSupported() {
		t.Skip("native partial mapping unsupported")
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	// The original producer admits this exact child's containing wrapper.
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
	if done, err := p.CloseWork(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); done || !errors.Is(err, ErrCloseWorkRequired) || p.closing {
		t.Fatal("self-refund lacked original parent receipt", done, err)
	}
	var refund retainedalloc.RemovalWork
	for n := 0; ; n++ {
		if n > 1000 {
			t.Fatal("owned child did not finish")
		}
		w := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
		done, err := p.CloseWorkInto(w, &refund)
		if err != nil {
			t.Fatal(err)
		}
		if p.closeWork.phase == 6 && (p.file != nil || p.path != "" || p.ownedFileMetadata != nil) {
			t.Fatal("containing charge changed before original child aliases scrubbed")
		}
		if done {
			break
		}
		runtime.Gosched()
	}
	if owner.Bytes() != 0 || p.ownedFileCharge != 0 {
		t.Fatal("physical child did not refund original contribution")
	}
	if done, err := refund.ContinueWork(&iterator.OrdinalScanWork{}); !done || err != nil {
		t.Fatal("terminal refund replay", done, err)
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
