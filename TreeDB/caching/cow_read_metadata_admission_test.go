package caching

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCOWReadWorkspaceMetadataAdmissionRefusalAndDrain(t *testing.T) {
	db := cowPointerFixture(t)
	key := []byte("metadata-admission")
	value := bytes.Repeat([]byte("pointer-value"), 1024)
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	budget := db.cow.budget
	before := budget.Stats()
	metadata := uint64(0)
	for _, size := range valuelog.COWInspectionMetadataAllocationSizes() {
		metadata += memtable.COWAllocationCharge(size)
	}
	if metadata != 0 && metadata != 4144 {
		t.Fatalf("metadata envelope=%d want three separately rounded buffers4144", metadata)
	}
	leaseOverhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	required := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) +
		memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(uintptr(0)))) + metadata + leaseOverhead
	// Leave exactly one byte less than the entire workspace envelope. This
	// uses the real shared budget, including the pressure lease's own charge.
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - required + 1 - leaseOverhead)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	pressured := budget.Stats()
	w, err := snapshot.cowWorkspaceLocked()
	if !errors.Is(err, memtable.ErrCOWCapacity) || w != nil || snapshot.cowReader != nil {
		t.Fatalf("whole metadata admission: workspace%v error%v", w, err)
	}
	if after := budget.Stats(); after != pressured {
		t.Fatalf("refusal changed ownership: before%+v after%+v", pressured, after)
	}
	if got, err := snapshot.Get(key); !errors.Is(err, memtable.ErrCOWCapacity) || got != nil || snapshot.cowReader != nil {
		t.Fatalf("real pointer read bypassed whole metadata admission: value%v error%v", got, err)
	}
	if after := budget.Stats(); after != pressured {
		t.Fatalf("refused pointer read changed ownership: before%+v after%+v", pressured, after)
	}
	pressure.Close()
	w, err = snapshot.cowWorkspaceLocked()
	if err != nil {
		t.Fatal(err)
	}
	if delta := budget.Stats().ReservedBytes - before.ReservedBytes; delta != required {
		t.Fatalf("workspace charge=%d want%d (metadata%d)", delta, required, metadata)
	}
	if again, err := snapshot.cowWorkspaceLocked(); err != nil || again != w || budget.Stats().ReservedBytes-before.ReservedBytes != required {
		t.Fatalf("serialized workspace reuse allocated another envelope: %v", err)
	}
	if got, err := snapshot.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("pointer read after pressure release: value length%d error%v", len(got), err)
	}
	readBaseline := budget.Stats()
	if got, err := snapshot.Get(key); err != nil || !bytes.Equal(got, value) || budget.Stats() != readBaseline {
		t.Fatalf("repeated pointer read changed the admitted workspace: value length%d error%v", len(got), err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	cowPressureEvictIdleReadScratch(t, budget)
	if got := budget.Stats(); got.ExternalBytes != before.ExternalBytes || got.ExternalLeases != before.ExternalLeases || got.ReservedBytes != before.ReservedBytes || got.Views != before.Views-1 {
		t.Fatalf("metadata ownership did not return to the retained-cut baseline: before%+v after%+v", before, got)
	}
}

func TestCOWGetManyViewReleasesResultOnLookupPressure(t *testing.T) {
	for _, test := range []struct {
		name   string
		refuse bool
	}{
		{name: "retained-result-blocks-lookup"},
		{name: "lookup-does-not-fit-without-result", refuse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := cowPointerFixture(t)
			settled, dirty := []byte("settled-empty"), []byte("dirty-inline")
			if err := db.Set(settled, []byte{}); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := db.Set(dirty, []byte("x")); err != nil {
				t.Fatal(err)
			}
			snapshot, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			budget := db.cow.budget
			cowPressureEvictIdleReadScratch(t, budget)
			before := budget.Stats()
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			workspace := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge() + overhead
			point := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{}))) + overhead
			// The empty result fits after the physical lookup, with no allowance
			// for the prior nonempty copy. The control also starves traversal.
			remaining := workspace + point + overhead
			if test.refuse {
				remaining = workspace + point - 1
			}
			pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - remaining - overhead)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			pressured := budget.Stats()
			calls := 0
			err = snapshot.GetManyView([][]byte{dirty, settled}, func(i int, key, value []byte, found bool) error {
				if i != calls || !found || cap(value) != len(value) {
					t.Fatalf("callback index=%d calls=%d found=%v len/cap=%d/%d", i, calls, found, len(value), cap(value))
				}
				calls++
				if i == 0 {
					if !bytes.Equal(key, dirty) || string(value) != "x" || snapshot.cowReader != nil {
						t.Fatalf("expected dirty inline hit without workspace: key=%q value=%q", key, value)
					}
					// With the callback's actual result charged, the same snapshot's
					// physical read admits the workspace but refuses point scratch.
					got, err := snapshot.Get(settled)
					if got != nil || !errors.Is(err, memtable.ErrCOWCapacity) || snapshot.cowReader == nil || snapshot.cowReader.point != nil {
						t.Fatalf("nested physical lookup under pressure: value=%v err=%v workspace=%v", got, err, snapshot.cowReader != nil)
					}
					if string(value) != "x" {
						t.Fatal("nested lookup changed the callback value")
					}
				} else if !bytes.Equal(key, settled) || value == nil || len(value) != 0 {
					t.Fatalf("present empty callback: key=%q value=%v", key, value)
				}
				return nil
			})
			leases, readCharge := 2, workspace+point
			if test.refuse {
				if !errors.Is(err, memtable.ErrCOWCapacity) || calls != 1 {
					t.Fatalf("true pressure: callbacks=%d err=%v", calls, err)
				}
				leases, readCharge = 1, workspace
			} else if err != nil || calls != 2 {
				t.Fatalf("lookup after dropping result: callbacks=%d err=%v", calls, err)
			}
			if after := budget.Stats(); after.ExternalLeases-pressured.ExternalLeases != leases || after.ReservedBytes-pressured.ReservedBytes != readCharge {
				t.Fatalf("callback retained budget debt: before=%+v after=%+v", pressured, after)
			}
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if after := budget.Stats(); after.ExternalLeases != pressured.ExternalLeases || after.ReservedBytes != pressured.ReservedBytes {
				t.Fatalf("snapshot did not refund read workspace: before=%+v after=%+v", pressured, after)
			}
		})
	}
}
