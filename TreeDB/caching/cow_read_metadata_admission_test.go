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
	if got := budget.Stats(); got.ExternalBytes != before.ExternalBytes || got.ExternalLeases != before.ExternalLeases || got.ReservedBytes != before.ReservedBytes || got.Views != before.Views-1 {
		t.Fatalf("metadata ownership did not return to the retained-cut baseline: before%+v after%+v", before, got)
	}
}
