package caching

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestCOWLazyReadWorkspaceAdmission(t *testing.T) {
	db := cowPointerFixture(t)
	db.valueLogThreshold = 64
	if err := db.Set([]byte("backend"), []byte("basis")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"inline", "gone"} {
		if err := db.Set([]byte(key), []byte("small")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	pointerValue := bytes.Repeat([]byte("pointer"), 256)
	if err := db.Set([]byte("pointer"), pointerValue); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	before := db.cow.budget.Stats()
	checkInline := func() {
		t.Helper()
		value, revision, err := snapshot.GetVersionedAppend([]byte("inline"), nil)
		if err != nil || string(value) != "small" || revision == 0 {
			t.Fatalf("inline value=%q revision=%d err=%v", value, revision, err)
		}
		for _, key := range []string{"inline", "pointer"} {
			found, err := snapshot.Has([]byte(key))
			if err != nil || !found {
				t.Fatalf("Has(%q)=%t err=%v", key, found, err)
			}
			entry, err := snapshot.GetEntryExact([]byte(key))
			if err != nil || entry.Revision == 0 || string(entry.Key) != key {
				t.Fatalf("GetEntryExact(%q)=%+v err=%v", key, entry, err)
			}
			if key == "pointer" && entry.Flags&node.FlagPointer == 0 {
				t.Fatal("fixture pointer was inline")
			}
		}
		if _, _, err := snapshot.GetVersioned([]byte("gone")); !errors.Is(err, tree.ErrKeyNotFound) {
			t.Fatalf("tombstone: %v", err)
		}
		if found, err := snapshot.Has([]byte("gone")); err != nil || found {
			t.Fatalf("tombstone Has=%t err=%v", found, err)
		}
		if snapshot.cowReader != nil {
			t.Fatal("cached inline/tombstone/metadata allocated decoder workspace")
		}
	}
	checkInline()
	if after := db.cow.budget.Stats(); after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
		t.Fatalf("metadata reads changed retained admission: before=%+v after=%+v", before, after)
	}
	// Fill the same authoritative in-flight budget completely after capture.
	// Cached metadata still needs no scratch; actual pointer/backend reads and
	// the internally owned View copy must refuse before allocation/callback.
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	checkInline()
	for _, key := range []string{"pointer", "backend"} {
		if _, err := snapshot.Get([]byte(key)); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("needed workspace %q: %v", key, err)
		}
	}
	called := false
	if err := snapshot.GetManyView([][]byte{[]byte("inline")}, func(_ int, _, _ []byte, _ bool) error {
		called = true
		return nil
	}); !errors.Is(err, memtable.ErrCOWCapacity) || called {
		t.Fatalf("View copy refusal err=%v callback=%t", err, called)
	}
	if snapshot.cowReader != nil {
		t.Fatal("refused workspace became visible")
	}
	pressure.Close()
	if value, err := snapshot.Get([]byte("pointer")); err != nil || !bytes.Equal(value, pointerValue) {
		t.Fatalf("pointer retry len=%d err=%v", len(value), err)
	}
	if snapshot.cowReader == nil {
		t.Fatal("pointer read did not acquire its workspace")
	}
	if value, err := snapshot.Get([]byte("backend")); err != nil || string(value) != "basis" {
		t.Fatalf("backend retry value=%q err=%v", value, err)
	}
}
