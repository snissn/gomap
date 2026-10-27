//go:build !windows

package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// These cases share the existing !windows dictionary-leaf producer/reopen
// fixtures. Portable compressed-pointer refund and value-bound cases remain in
// cow_read_batch_test.go so Windows keeps that coverage.
func batchRawProbes(n int) ([][]byte, [][]byte, []mvcckey.VersionRangeResult) {
	a, b := make([][]byte, n), make([][]byte, n)
	for i := range a {
		a[i] = []byte(fmt.Sprintf("a%03d", i))
		b[i] = append(append([]byte(nil), a[i]...), 0)
	}
	return a, b, make([]mvcckey.VersionRangeResult, n)
}

func TestCOWReadBatchDictionaryReopenCloseAndPressure(t *testing.T) {
	t.Logf("bounded stack stage: item=%d array=%d charged=%d", unsafe.Sizeof(cowReadBatchStage{}), unsafe.Sizeof([mvcckey.MaxReadBatch]cowReadBatchStage{}), memtable.COWAllocationCharge(uint64(unsafe.Sizeof([mvcckey.MaxReadBatch]cowReadBatchStage{}))))
	for _, entries := range []int{-1, 128} {
		t.Run(fmt.Sprint(entries), func(t *testing.T) {
			db, _, want, _ := cowLeafMissFixture(t, entries, 64)
			s, e := db.AcquireMVCCReadCut()
			if e != nil {
				t.Fatal(e)
			}
			a, b, r := batchRawProbes(32)
			for j := 0; j < 2; j++ {
				if e = s.SeekGEVersionRanges(a, b, r); e != nil {
					t.Fatal(e)
				}
				for i, v := range r {
					if v.Err != nil || !v.Found || !bytes.Equal(v.Key, a[i]) || !bytes.Equal(v.Value, want) {
						t.Fatalf("%d: %+v", i, v)
					}
				}
			}
			before := db.cow.budget.Stats()
			// Refuse only new batch metadata. Existing scalar workspace remains usable.
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			pressure, e := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
			if e != nil {
				t.Fatal(e)
			}
			pressured := db.cow.budget.Stats()
			if e = s.SeekGEVersionRanges(a, b, r); e != nil {
				t.Fatal(e)
			}
			for _, v := range r {
				if v.Err != nil || !bytes.Equal(v.Value, want) {
					t.Fatal(v)
				}
			}
			if got := db.cow.budget.Stats(); got != pressured {
				t.Fatalf("fallback leaked accounting: before%+v after%+v", pressured, got)
			}
			pressure.Close()
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			if e = s.SeekGEVersionRanges(a, b, r); e == nil {
				t.Fatal("closed snapshot read")
			}
		})
	}
}

func TestCOWReadBatchWarmHeaderCorruptionAndRepair(t *testing.T) {
	db, _, want, ptr := cowLeafMissFixture(t, 128, 64)
	s, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	a, b, r := batchRawProbes(16)
	if e = s.SeekGEVersionRanges(a, b, r); e != nil {
		t.Fatal(e)
	}
	f, _, e := s.cowCut.basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if e != nil {
		t.Fatal(e)
	}
	writer, e := os.OpenFile(f.Name(), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer writer.Close()
	offset := int64(ptr.Offset)
	var version [1]byte
	if _, e = writer.ReadAt(version[:], offset); e != nil {
		t.Fatal(e)
	}
	if _, e = writer.WriteAt([]byte{version[0] ^ 1}, offset); e != nil {
		t.Fatal(e)
	}
	defer writer.WriteAt(version[:], offset)
	if e = s.SeekGEVersionRanges(a, b, r); e != nil {
		t.Fatal(e)
	}
	if !errors.Is(r[0].Err, valuelog.ErrCorrupt) {
		t.Fatalf("warm cached header bypassed: %+v", r[0])
	}
	if _, e = writer.WriteAt(version[:], offset); e != nil {
		t.Fatal(e)
	}
	if e = s.SeekGEVersionRanges(a, b, r); e != nil {
		t.Fatal(e)
	}
	for _, v := range r {
		if v.Err != nil || !bytes.Equal(v.Value, want) {
			t.Fatal(v)
		}
	}
}

func TestCOWReadBatchContendedSlotsFallback(t *testing.T) {
	db, _, want, _ := cowLeafMissFixture(t, 128, 64)
	s, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	basis := s.cowCut.basis
	basis.readMu.Lock()
	defer basis.readMu.Unlock()
	for i := range basis.readSlots {
		basis.readSlots[i].mu.Lock()
		defer basis.readSlots[i].mu.Unlock()
	}
	a, b, r := batchRawProbes(32)
	if e = s.SeekGEVersionRanges(a, b, r); e != nil {
		t.Fatal(e)
	}
	for _, v := range r {
		if v.Err != nil || !bytes.Equal(v.Value, want) {
			t.Fatal(v)
		}
	}
}

// The batch must preserve the original fresh dictionary handoff: a cold shared
// lane cannot strand the already-admitted complete-cut decoder's point scratch.
func TestCOWReadBatchCapturedDictionaryFreshRefund(t *testing.T) {
	db, want, ptr := cowSuccessorDictionaryLeafFixture(t)
	s, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.cowReadMu.Lock()
	w, e := s.cowWorkspaceLocked()
	var pageScratch [4096]byte
	if e == nil {
		f, fe := w.file(ptr.ValuePtr())
		e = fe
		if e == nil {
			shape, se := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), w.limits)
			e = se
			if e == nil {
				_, e = w.readInspected(f, ptr.ValuePtr(), shape, true, pageScratch[:], nil)
			}
		}
	}
	s.cowReadMu.Unlock()
	if e != nil || w.point != nil || len(w.codecs) == 0 {
		t.Fatal("warm real local decoder", e)
	}
	store := db.dictStore
	db.dictStore = cowCapturedDictionaryOnlyStore{store}
	defer func() { db.dictStore = store }()
	budget := db.cow.budget
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	workspace := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge() + overhead
	point := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{}))) + overhead
	stage := memtable.COWAllocationCharge(uint64(unsafe.Sizeof([32]cowReadBatchStage{}))) + overhead
	pressure, e := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - workspace - point - stage - overhead)
	if e != nil {
		t.Fatal(e)
	}
	defer pressure.Close()
	a, b, r := batchRawProbes(2)
	if e = s.SeekGEVersionRanges(a, b, r); e != nil {
		t.Fatal(e)
	}
	for _, v := range r {
		if v.Err != nil || !bytes.Equal(v.Value, want) {
			t.Fatalf("fresh dictionary handoff stranded cut-local point: %+v", v)
		}
	}
}

func TestCOWReadBatchColdCRCFailure(t *testing.T) {
	db, _, want, ptr := cowLeafMissFixture(t, -1, 64)
	s, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	f, _, e := s.cowCut.basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if e != nil {
		t.Fatal(e)
	}
	writer, e := os.OpenFile(f.Name(), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer writer.Close()
	offset := int64(ptr.Offset - 4)
	var crc [1]byte
	if _, e = writer.ReadAt(crc[:], offset); e != nil {
		t.Fatal(e)
	}
	if _, e = writer.WriteAt([]byte{crc[0] ^ 1}, offset); e != nil {
		t.Fatal(e)
	}
	defer writer.WriteAt(crc[:], offset)
	a, b, r := batchRawProbes(2)
	if e = s.SeekGEVersionRanges(a, b, r); e != nil {
		t.Fatal(e)
	}
	for _, v := range r {
		if !errors.Is(v.Err, valuelog.ErrCorrupt) {
			t.Fatalf("cold CRC bypassed: %+v", v)
		}
	}
	if _, e = writer.WriteAt(crc[:], offset); e != nil {
		t.Fatal(e)
	}
	if e = s.SeekGEVersionRanges(a, b, r); e != nil {
		t.Fatal(e)
	}
	for _, v := range r {
		if v.Err != nil || !bytes.Equal(v.Value, want) {
			t.Fatal(v)
		}
	}
}

func TestCOWReadBatchConcurrentSnapshotClose(t *testing.T) {
	db, _, _, _ := cowLeafMissFixture(t, 128, 64)
	s, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	a, b, r := batchRawProbes(32)
	readDone, closeDone := make(chan error, 1), make(chan error, 1)
	// Original decoder ownership provides contention; no production hook or
	// borrowed page is injected. Both operations must join before fixture Close.
	db.cow.physicalMu.Lock()
	go func() {
		e := s.SeekGEVersionRanges(a, b, r)
		if errors.Is(e, backenddb.ErrClosed) {
			e = nil
		}
		readDone <- e
	}()
	go func() { closeDone <- s.Close() }()
	db.cow.physicalMu.Unlock()
	cowWait(t, readDone)
	cowWait(t, closeDone)
	for _, v := range r {
		if v.Err != nil && !errors.Is(v.Err, backenddb.ErrClosed) {
			t.Fatal(v.Err)
		}
	}
}
