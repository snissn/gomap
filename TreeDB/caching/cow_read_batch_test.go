package caching

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// These cases use the portable compressed-pointer fixture, independently of
// the dictionary-leaf producer fixtures in cow_read_batch_dictionary_test.go.
func TestCOWReadBatchFreshCompressedValueRefund(t *testing.T) {
	db, want := cowSuccessorCompressedPointerFixture(t)
	warm, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	if _, _, _, e = warm.SeekGEVersionRange([]byte("a"), []byte("b")); e != nil {
		t.Fatal(e)
	}
	if e = warm.Close(); e != nil {
		t.Fatal(e)
	}
	s, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	basis, budget := s.cowCut.basis, db.cow.budget
	basis.readMu.Lock()
	basis.reader.close()
	basis.reader = nil
	basis.readMu.Unlock()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	workspace := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge() + overhead
	point := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{}))) + overhead
	output := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadAllocation{}))) + overhead + memtable.COWAllocationCharge(cowReadCapacity(uint64(len(want))))
	stage := memtable.COWAllocationCharge(uint64(unsafe.Sizeof([32]cowReadBatchStage{}))) + overhead
	before := budget.Stats()
	pressure, e := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - workspace - point - output - stage - overhead)
	if e != nil {
		t.Fatal(e)
	}
	defer pressure.Close()
	a := [][]byte{[]byte("a"), []byte("a")}
	b := [][]byte{[]byte("b"), []byte("b")}
	r := make([]mvcckey.VersionRangeResult, 2)
	if e = s.SeekGEVersionRanges(a, b, r); e != nil {
		t.Fatal(e)
	}
	for _, v := range r {
		if v.Err != nil || !bytes.Equal(v.Value, want) {
			t.Fatal(v)
		}
	}
	if basis.reader != nil || s.cowReader == nil || s.cowReader.point != nil {
		t.Fatal("fresh scratch did not refund before original value admission")
	}
}

func TestCOWReadBatchExistingCustomValueBound(t *testing.T) {
	db, _ := cowSuccessorCompressedPointerFixture(t)
	s, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.cowReadMu.Lock()
	w, e := s.cowWorkspaceLocked()
	if e == nil {
		_, e = w.pointScratch()
		w.limits.MaxValueBytes = 4096
	}
	s.cowReadMu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	before := db.cow.budget.Stats()
	a := [][]byte{[]byte("a"), []byte("a")}
	b := [][]byte{[]byte("b"), []byte("b")}
	r := make([]mvcckey.VersionRangeResult, 2)
	if e = s.SeekGEVersionRanges(a, b, r); e != nil {
		t.Fatal(e)
	}
	for _, v := range r {
		if !errors.Is(v.Err, valuelog.ErrCOWReadCapacity) || v.Found || v.Value != nil {
			t.Fatal(v)
		}
	}
	if after := db.cow.budget.Stats(); after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases || after.ReservedBytes != before.ReservedBytes {
		t.Fatalf("refused value leaked: before%+v after%+v", before, after)
	}
}
