//go:build !windows

package pager

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
)

func TestPhysicalCloseWorkFirstUnmapFailureCustody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Truncate(2 << 20); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	p, e := OpenReadOnly(path, 2<<20)
	if e != nil {
		t.Fatal(e)
	}
	handle := p.file
	physical := unmapConstructorPhysical
	failure := errors.New("physical unmap failure")
	calls := 0
	unmapConstructorPhysical = func(address unsafe.Pointer, length uintptr) error { calls++; return failure }
	defer func() {
		unmapConstructorPhysical = physical
		_ = physical(unsafe.Pointer(unsafe.SliceData(p.mappings[0].data)), p.mappings[0].extent)
		_ = handle.Close()
	}()
	w := &iterator.OrdinalScanWork{RecordLimit: 42, ByteLimit: 1 << 20}
	if ready, e := p.CloseWork(w); ready || !errors.Is(e, failure) {
		t.Fatalf("first outcome: %v %v", ready, e)
	}
	if p.mappings[0].offset != 0 || p.chunks[0] == nil || p.file != handle {
		t.Fatal("failed mapping lost custody")
	}
	if _, e = handle.Stat(); e != nil {
		t.Fatal("file closed after failed unmap", e)
	}
	for i := 0; i < 3; i++ {
		if ready, e := p.CloseWork(&iterator.OrdinalScanWork{RecordLimit: 42, ByteLimit: 1 << 20}); ready || !errors.Is(e, failure) {
			t.Fatal("failure changed", e)
		}
	}
	if calls != 1 {
		t.Fatal("uncertain physical unmap replayed", calls)
	}
}

func TestGrowthPrivateLargeMappingCancelWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, e := Open(path, 4<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	p.QuiesceBackgroundGrowth()
	g, ready, e := NewGrowthForPager(p, 2, nil)
	if !ready || e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 100; i++ {
		ready, e = g.Step(p, &iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20})
		if e != nil {
			t.Fatal(e)
		}
		if g.mapped > g.oldChunks {
			break
		}
		if ready {
			t.Fatal("installed before private mapping yield")
		}
		if i == 99 {
			t.Fatalf("private mapping not constructed: phase=%d mapped=%d old=%d total=%d target=%d failure=%v", g.phase, g.mapped, g.oldChunks, g.totalChunks, g.targetPages, g.failure)
		}
	}
	if g.installed || len(p.chunks) != 0 {
		t.Fatal("private mapping published")
	}
	if ready, e = g.Cancel(&iterator.OrdinalScanWork{}); ready || e != nil {
		t.Fatal("zero-work cancel", e)
	}
	sawPartial := false
	for i := 0; i < 100; i++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 42, ByteLimit: 1 << 20}
		ready, e = g.Cancel(w)
		if e != nil {
			t.Fatal(e)
		}
		if !ready && g.mappings[g.mapped-1].offset > 0 {
			sawPartial = true
		}
		if ready {
			break
		}
		if i == 99 {
			t.Fatal("cancel did not finish")
		}
	}
	if !sawPartial || g.mappings != nil || g.primary != nil || !g.cancelled {
		t.Fatal("private physical mapping/aliases survived cancel")
	}
}
