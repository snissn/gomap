package caching

import (
	"bytes"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestGetVersionedPreservesMutableMemtableRevision(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("backend Close: %v", err)
		}
	}()

	db, err := Open(filepath.Join(dir, "cache"), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "skiplist",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	b := db.NewBatch()
	if err := b.SetWithRevision([]byte("k"), []byte("value"), page.EntryRevision(201)); err != nil {
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
	if !bytes.Equal(val, []byte("value")) || revision != 201 {
		t.Fatalf("GetVersioned=(%q,%d), want (value,201)", val, revision)
	}

	dst := []byte("prefix:")
	out, revision, err := db.GetVersionedAppend([]byte("k"), dst)
	if err != nil {
		t.Fatalf("GetVersionedAppend: %v", err)
	}
	if !bytes.Equal(out, []byte("prefix:value")) || revision != 201 {
		t.Fatalf("GetVersionedAppend=(%q,%d), want (prefix:value,201)", out, revision)
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
	if !bytes.Equal(snapVal, []byte("value")) || snapRevision != 201 {
		t.Fatalf("Snapshot.GetVersioned=(%q,%d), want (value,201)", snapVal, snapRevision)
	}

	snapEntry, err := snap.GetEntry([]byte("k"))
	if err != nil {
		t.Fatalf("Snapshot.GetEntry: %v", err)
	}
	if !bytes.Equal(snapEntry.Value, []byte("value")) || snapEntry.Revision != 201 {
		t.Fatalf("Snapshot.GetEntry=(%q,%d), want (value,201)", snapEntry.Value, snapEntry.Revision)
	}
}

func TestGetVersionedAssignsMutableMemtableRevision(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold: 1 << 30,
		MemtableMode:   "skiplist",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	if err := db.Set([]byte("k"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, revision, err := db.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if !bytes.Equal(val, []byte("value")) || revision == page.LegacyEntryRevision {
		t.Fatalf("GetVersioned=(%q,%d), want (value,non-legacy)", val, revision)
	}

	if err := db.Set([]byte("next"), []byte("value")); err != nil {
		t.Fatalf("Set next: %v", err)
	}
	_, nextRevision, err := db.GetVersioned([]byte("next"))
	if err != nil {
		t.Fatalf("GetVersioned next: %v", err)
	}
	if nextRevision <= revision {
		t.Fatalf("next revision=%d, want > first revision %d", nextRevision, revision)
	}
}

func TestCommandWALPointCallbacksReceiveVisibleRevision(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "skiplist",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	var setRevision page.EntryRevision
	if err := db.SetAfterCommandWALAppendWithRevision([]byte("k"), []byte("value"), func(revision page.EntryRevision) error {
		setRevision = revision
		return nil
	}); err != nil {
		t.Fatalf("SetAfterCommandWALAppendWithRevision: %v", err)
	}
	val, revision, err := db.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if !bytes.Equal(val, []byte("value")) || revision == page.LegacyEntryRevision || revision != setRevision {
		t.Fatalf("GetVersioned=(%q,%d), callback revision=%d; want (value,same non-legacy)", val, revision, setRevision)
	}

	var deleteRevision page.EntryRevision
	if err := db.DeleteAfterCommandWALAppendWithRevision([]byte("k"), func(revision page.EntryRevision) error {
		deleteRevision = revision
		return nil
	}); err != nil {
		t.Fatalf("DeleteAfterCommandWALAppendWithRevision: %v", err)
	}
	val, revision, err = db.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersioned after delete: %v", err)
	}
	if val != nil || revision == page.LegacyEntryRevision || revision != deleteRevision || revision <= setRevision {
		t.Fatalf("delete GetVersioned=(%q,%d), callback revision=%d set=%d; want tombstone same higher non-legacy revision", val, revision, deleteRevision, setRevision)
	}
}

func TestGetVersionedPreservesMutableMemtableDeleteRevision(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "skiplist",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	b := db.NewBatch()
	if err := b.SetWithRevision([]byte("k"), []byte("value"), page.EntryRevision(201)); err != nil {
		t.Fatalf("SetWithRevision: %v", err)
	}
	if err := b.DeleteWithRevision([]byte("k"), page.EntryRevision(202)); err != nil {
		t.Fatalf("DeleteWithRevision: %v", err)
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
	if val != nil || revision != 202 {
		t.Fatalf("GetVersioned after delete=(%q,%d), want (nil,202)", val, revision)
	}

	dst := []byte("prefix:")
	out, revision, err := db.GetVersionedAppend([]byte("k"), dst)
	if err != tree.ErrKeyNotFound {
		t.Fatalf("GetVersionedAppend err=%v, want ErrKeyNotFound", err)
	}
	if !bytes.Equal(out, dst) || revision != 202 {
		t.Fatalf("GetVersionedAppend after delete=(%q,%d), want (%q,202)", out, revision, dst)
	}
}
