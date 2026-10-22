package caching

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// This representation regression deliberately holds the legacy shared scratch
// locks while exercising real exported snapshot APIs. It proves independent
// retained readers use their admitted owners; timing throughput is measured
// separately. Disabled decoded caching forces an actual compressed leaf read.
func TestCOWRetainedReadersOwnPhysicalScratch(t *testing.T) {
	for _, operation := range []string{"point", "successor", "iterator"} {
		t.Run(operation, func(t *testing.T) {
			db, _, want := cowLeafCacheFixture(t, -1, true)
			s, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			basis := s.cowCut.basis
			basis.readMu.Lock()
			db.cow.physicalMu.Lock()
			done := make(chan error, 1)
			go func() {
				var got []byte
				var err error
				switch operation {
				case "point":
					got, err = s.GetAppend([]byte("a000"), nil)
				case "successor":
					var key []byte
					var found bool
					key, got, found, err = s.SeekGEVersionRange([]byte("a000"), nil)
					if err == nil && (!found || string(key) != "a000") {
						err = fmt.Errorf("successor key=%q found=%t", key, found)
					}
				case "iterator":
					it, openErr := s.Iterator([]byte("a000"), nil)
					if openErr != nil {
						err = openErr
						break
					}
					if it.Valid() {
						got = append([]byte(nil), it.Value()...)
					}
					err = it.Error()
					if closeErr := it.Close(); err == nil {
						err = closeErr
					}
				}
				if err == nil && !bytes.Equal(got, want) {
					err = fmt.Errorf("physical value=%q want=%q", got, want)
				}
				done <- err
			}()
			var result error
			timedOut := false
			select {
			case result = <-done:
			case <-time.After(2 * time.Second):
				timedOut = true
			}
			db.cow.physicalMu.Unlock()
			basis.readMu.Unlock()
			if timedOut {
				// Join the admitted operation before cleanup even on baseline.
				cowWait(t, done)
				t.Fatal("retained read waited for another owner's physical scratch")
			}
			if result != nil {
				t.Fatal(result)
			}
		})
	}
}

func TestCOWRetainedPhysicalScratchAdmissionAndRelease(t *testing.T) {
	db, _, want := cowLeafCacheFixture(t, -1, true)
	budget := db.cow.budget
	before := budget.Stats()
	a, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	read := func(s *Snapshot) []byte {
		t.Helper()
		got, err := s.GetAppend([]byte("a000"), nil)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("physical read=%q error=%v", got, err)
		}
		return got
	}
	read(a)
	workspace, point, codec := a.cowReader, a.cowReader.point, a.cowReader.codecs[0]
	warm := budget.Stats()
	for range 8 {
		read(a)
	}
	if a.cowReader != workspace || workspace.point != point || workspace.codecs[0] != codec || budget.Stats() != warm {
		t.Fatal("retained physical reads did not reuse their admitted capacity")
	}
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - warm.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	pressured := budget.Stats()
	got, readErr := b.GetAppend([]byte("a000"), []byte("keep"))
	if !errors.Is(readErr, memtable.ErrCOWCapacity) || string(got) != "keep" || b.cowReader != nil || budget.Stats() != pressured {
		pressure.Close()
		t.Fatalf("independent scratch refusal changed ownership: got=%q error=%v", got, readErr)
	}
	pressure.Close()
	owned := read(b)
	if b.cowReader == workspace || b.cowReader.point == point || b.cowReader.codecs[0] == codec {
		t.Fatal("distinct retained readers shared mutable physical scratch")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	read(b)
	if !bytes.Equal(owned, want) {
		t.Fatal("reader close/reuse changed an owned result")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if after := budget.Stats(); after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases || after.ReservedBytes != before.ReservedBytes || after.Views != before.Views {
		t.Fatalf("retained physical owners did not refund: before=%+v after=%+v", before, after)
	}
}
