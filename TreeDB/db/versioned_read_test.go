package db

import (
	"bytes"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestGetVersionedPreservesBatchEntryRevision(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	b := db.NewBatch().(*Batch)
	if err := b.SetWithRevision([]byte("k"), []byte("value"), page.EntryRevision(101)); err != nil {
		t.Fatalf("SetWithRevision: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}

	val, revision, err := db.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if !bytes.Equal(val, []byte("value")) || revision != 101 {
		t.Fatalf("GetVersioned=(%q,%d), want (value,101)", val, revision)
	}

	dst := []byte("prefix:")
	out, revision, err := db.GetVersionedAppend([]byte("k"), dst)
	if err != nil {
		t.Fatalf("GetVersionedAppend: %v", err)
	}
	if !bytes.Equal(out, []byte("prefix:value")) || revision != 101 {
		t.Fatalf("GetVersionedAppend=(%q,%d), want (prefix:value,101)", out, revision)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	snapVal, snapRevision, err := snap.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("Snapshot.GetVersioned: %v", err)
	}
	if !bytes.Equal(snapVal, []byte("value")) || snapRevision != 101 {
		t.Fatalf("Snapshot.GetVersioned=(%q,%d), want (value,101)", snapVal, snapRevision)
	}

	snapEntry, err := snap.GetEntry([]byte("k"))
	if err != nil {
		t.Fatalf("Snapshot.GetEntry: %v", err)
	}
	if !bytes.Equal(snapEntry.Value, []byte("value")) || snapEntry.Revision != 101 {
		t.Fatalf("Snapshot.GetEntry=(%q,%d), want (value,101)", snapEntry.Value, snapEntry.Revision)
	}
}

func TestGetVersionedAssignsDurableEntryRevision(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	b := db.NewBatch().(*Batch)
	if err := b.Set([]byte("k"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}

	val, revision, err := db.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if !bytes.Equal(val, []byte("value")) || revision == page.LegacyEntryRevision {
		t.Fatalf("GetVersioned=(%q,%d), want (value,non-legacy)", val, revision)
	}
	if got := db.State().MaxEntryRevision; got < revision {
		t.Fatalf("MaxEntryRevision=%d, want >= assigned revision %d", got, revision)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := reopen.Close(); err != nil {
			t.Fatalf("Close reopen: %v", err)
		}
	}()
	val, reopenedRevision, err := reopen.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("reopen GetVersioned: %v", err)
	}
	if !bytes.Equal(val, []byte("value")) || reopenedRevision != revision {
		t.Fatalf("reopen GetVersioned=(%q,%d), want (value,%d)", val, reopenedRevision, revision)
	}
	if got := reopen.State().MaxEntryRevision; got < revision {
		t.Fatalf("reopen MaxEntryRevision=%d, want >= assigned revision %d", got, revision)
	}
}

func TestEntryRevisionFloorAdvancesAfterExplicitRevision(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	b := db.NewBatch().(*Batch)
	if err := b.SetWithRevision([]byte("explicit"), []byte("value"), page.EntryRevision(100)); err != nil {
		t.Fatalf("SetWithRevision: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync explicit: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close explicit batch: %v", err)
	}

	b = db.NewBatch().(*Batch)
	if err := b.Set([]byte("next"), []byte("value")); err != nil {
		t.Fatalf("Set next: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync next: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close next batch: %v", err)
	}

	_, revision, err := db.GetVersioned([]byte("next"))
	if err != nil {
		t.Fatalf("GetVersioned next: %v", err)
	}
	if revision <= 100 {
		t.Fatalf("next revision=%d, want > explicit floor 100", revision)
	}
	if got := db.State().MaxEntryRevision; got < revision {
		t.Fatalf("MaxEntryRevision=%d, want >= next revision %d", got, revision)
	}
}

func TestAssignBatchEntryRevisionsReservesExplicitOnlyFloor(t *testing.T) {
	db := &DB{}
	b := batchpkg.New(nil, -1)
	defer b.Close()
	if err := b.SetWithRevision([]byte("explicit"), []byte("value"), page.EntryRevision(100)); err != nil {
		t.Fatalf("SetWithRevision: %v", err)
	}

	if got := db.assignBatchEntryRevisions(b); got != page.EntryRevision(100) {
		t.Fatalf("assignBatchEntryRevisions=%d, want 100", got)
	}
	if got := db.nextEntryRevision(); got <= page.EntryRevision(100) {
		t.Fatalf("nextEntryRevision=%d, want > explicit floor 100", got)
	}
}
