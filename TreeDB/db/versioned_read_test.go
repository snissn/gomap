package db

import (
	"bytes"
	"testing"

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
