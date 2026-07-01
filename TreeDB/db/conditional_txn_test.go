package db

import (
	"bytes"
	"errors"
	"testing"
)

func requireVersionedEntry(t *testing.T, db *DB, key, want []byte) VersionedEntry {
	t.Helper()
	entry, found, err := db.GetVersioned(key)
	if err != nil {
		t.Fatalf("GetVersioned(%q): %v", key, err)
	}
	if !found {
		t.Fatalf("GetVersioned(%q) found=false, want true", key)
	}
	if !bytes.Equal(entry.Value, want) {
		t.Fatalf("GetVersioned(%q) value=%q want %q", key, entry.Value, want)
	}
	return entry
}

func requireVersionedMissing(t *testing.T, db *DB, key []byte) {
	t.Helper()
	entry, found, err := db.GetVersioned(key)
	if err != nil {
		t.Fatalf("GetVersioned(%q): %v", key, err)
	}
	if found {
		t.Fatalf("GetVersioned(%q) found=true entry=%+v, want missing", key, entry)
	}
	if entry.Revision != 0 {
		t.Fatalf("GetVersioned(%q) missing revision=%d want 0", key, entry.Revision)
	}
}

func TestRawKVGetVersionedRevisionPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Set([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Set k=v1: %v", err)
	}
	first := requireVersionedEntry(t, db, []byte("k"), []byte("v1"))
	if first.Revision == 0 {
		t.Fatalf("initial revision=0, want durable commit revision")
	}

	if err := db.Set([]byte("other"), []byte("x")); err != nil {
		t.Fatalf("Set other: %v", err)
	}
	afterUnrelated := requireVersionedEntry(t, db, []byte("k"), []byte("v1"))
	if afterUnrelated.Revision != first.Revision {
		t.Fatalf("revision changed after unrelated write: got %d want %d", afterUnrelated.Revision, first.Revision)
	}

	if err := db.Set([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Set k=v2: %v", err)
	}
	updated := requireVersionedEntry(t, db, []byte("k"), []byte("v2"))
	if updated.Revision <= first.Revision {
		t.Fatalf("updated revision=%d want > %d", updated.Revision, first.Revision)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedEntry := requireVersionedEntry(t, reopened, []byte("k"), []byte("v2"))
	if reopenedEntry.Revision != updated.Revision {
		t.Fatalf("reopened revision=%d want %d", reopenedEntry.Revision, updated.Revision)
	}
	requireVersionedMissing(t, reopened, []byte("missing"))
}

func TestRawKVGetVersionedDeleteRangeRemovesRevisionMetadata(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, kv := range []struct {
		key string
		val string
	}{
		{"a", "va"},
		{"b", "vb"},
		{"c", "vc"},
	} {
		if err := db.Set([]byte(kv.key), []byte(kv.val)); err != nil {
			t.Fatalf("Set %s: %v", kv.key, err)
		}
	}
	cBefore := requireVersionedEntry(t, db, []byte("c"), []byte("vc"))

	batch := db.NewBatch()
	if err := batch.DeleteRange([]byte("a"), []byte("c")); err != nil {
		_ = batch.Close()
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := batch.Write(); err != nil {
		_ = batch.Close()
		t.Fatalf("Write delete range: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}

	requireVersionedMissing(t, db, []byte("a"))
	requireVersionedMissing(t, db, []byte("b"))
	cAfter := requireVersionedEntry(t, db, []byte("c"), []byte("vc"))
	if cAfter.Revision != cBefore.Revision {
		t.Fatalf("exclusive range end revision changed: got %d want %d", cAfter.Revision, cBefore.Revision)
	}
}

func TestRawKVRevisionMetadataSurvivesSystemRootPublish(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	before := requireVersionedEntry(t, db, []byte("k"), []byte("v"))

	if _, err := db.PublishSystemRootIterator(mustFrozenSystemMemtable(t, "sys/a", "1").NewIterator(nil, nil)); err != nil {
		t.Fatalf("PublishSystemRootIterator: %v", err)
	}

	after := requireVersionedEntry(t, db, []byte("k"), []byte("v"))
	if after.Revision != before.Revision {
		t.Fatalf("revision after system publish=%d want %d", after.Revision, before.Revision)
	}
}

func TestConditionalTxnDetectsWriteCommittedDuringCommitAttempt(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k"), []byte("base")); err != nil {
		t.Fatalf("Set base: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	got, err := tx.Get([]byte("k"))
	if err != nil {
		t.Fatalf("tx.Get: %v", err)
	}
	if !bytes.Equal(got, []byte("base")) {
		t.Fatalf("tx.Get=%q want base", got)
	}
	if err := tx.Set([]byte("x"), []byte("tx")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}

	var hookErr error
	db.testAfterOptimisticApplyHook = func() {
		db.testAfterOptimisticApplyHook = nil
		hookErr = db.Set([]byte("k"), []byte("other"))
	}
	err = tx.Commit()
	if hookErr != nil {
		t.Fatalf("hook Set: %v", hookErr)
	}
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Commit error=%v want ErrConcurrentModification", err)
	}
	if got, err := db.Get([]byte("x")); err != nil {
		t.Fatalf("Get x: %v", err)
	} else if got != nil {
		t.Fatalf("x=%q want missing after failed conditional commit", got)
	}
	requireBackendRawKVValue(t, db, []byte("k"), []byte("other"))
}

func TestConditionalTxnAllowsDisjointConcurrentWrite(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("a"), []byte("base")); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, err := tx.Get([]byte("a")); err != nil {
		t.Fatalf("tx.Get: %v", err)
	}
	if err := db.Set([]byte("b"), []byte("other")); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	if err := tx.Set([]byte("c"), []byte("tx")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	requireBackendRawKVValue(t, db, []byte("c"), []byte("tx"))
}

func TestConditionalTxnTracksEmptyKeyConflicts(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set(nil, []byte("base")); err != nil {
		t.Fatalf("Set empty key: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if got, err := tx.Get(nil); err != nil {
		t.Fatalf("tx.Get empty key: %v", err)
	} else if !bytes.Equal(got, []byte("base")) {
		t.Fatalf("tx.Get empty key=%q want base", got)
	}
	if err := db.Set([]byte{}, []byte("other")); err != nil {
		t.Fatalf("Set empty key other: %v", err)
	}
	if err := tx.Set([]byte("x"), []byte("tx")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Commit error=%v want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnMissingReadConflictsWithInsertDeleteCycle(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if got, err := tx.Get([]byte("missing")); err != nil {
		t.Fatalf("tx.Get missing: %v", err)
	} else if got != nil {
		t.Fatalf("tx.Get missing=%q want nil", got)
	}
	if err := db.Set([]byte("missing"), []byte("temp")); err != nil {
		t.Fatalf("Set missing: %v", err)
	}
	if err := db.Delete([]byte("missing")); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
	if err := tx.Set([]byte("x"), []byte("tx")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Commit error=%v want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnReadConflictsWithConcurrentDeleteRange(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("b"), []byte("base")); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if got, err := tx.Get([]byte("b")); err != nil {
		t.Fatalf("tx.Get b: %v", err)
	} else if !bytes.Equal(got, []byte("base")) {
		t.Fatalf("tx.Get b=%q want base", got)
	}

	b := db.NewBatch()
	if err := b.DeleteRange([]byte("a"), []byte("c")); err != nil {
		_ = b.Close()
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write range delete: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := tx.Set([]byte("x"), []byte("tx")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Commit error=%v want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnEmptyKeyReadConflictsWithConcurrentDeleteRange(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set(nil, []byte("base")); err != nil {
		t.Fatalf("Set empty key: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if got, err := tx.Get(nil); err != nil {
		t.Fatalf("tx.Get empty key: %v", err)
	} else if !bytes.Equal(got, []byte("base")) {
		t.Fatalf("tx.Get empty key=%q want base", got)
	}

	b := db.NewBatch()
	if err := b.DeleteRange(nil, []byte("a")); err != nil {
		_ = b.Close()
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write range delete: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := tx.Set([]byte("x"), []byte("tx")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Commit error=%v want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnDeleteRangeFailsClosed(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer func() { _ = tx.Close() }()

	if err := tx.DeleteRange([]byte("a"), []byte("z")); !errors.Is(err, ErrConditionalRangeUnsupported) {
		t.Fatalf("DeleteRange error=%v want ErrConditionalRangeUnsupported", err)
	}
}
