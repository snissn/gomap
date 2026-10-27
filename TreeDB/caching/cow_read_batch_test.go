package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func batchRawProbes(n int) ([][]byte, [][]byte, []mvcckey.VersionRangeResult) {
	a, b := make([][]byte, n), make([][]byte, n)
	for i := range a {
		a[i] = []byte(fmt.Sprintf("a%03d", i))
		b[i] = append(append([]byte(nil), a[i]...), 0)
	}
	return a, b, make([]mvcckey.VersionRangeResult, n)
}

func TestCOWReadBatchDictionaryReopenCloseAndPressure(t *testing.T) {
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
