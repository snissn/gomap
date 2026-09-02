package treedb

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
)

func TestPublicConditionalTxnCachedHandleCommitsThroughMemtable(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	got, revision, err := tx.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("tx.GetVersioned: %v", err)
	}
	if !bytes.Equal(got, []byte("before")) || revision == LegacyEntryRevision {
		t.Fatalf("tx.GetVersioned=(%q,%d), want before with native revision", got, revision)
	}
	if err := tx.Set([]byte("k"), []byte("after")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}
	got, revision, err = db.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if !bytes.Equal(got, []byte("after")) || revision == LegacyEntryRevision {
		t.Fatalf("GetVersioned=(%q,%d), want after with native revision", got, revision)
	}
	stats := db.Stats()
	assertUintStatAtLeast(t, stats, "treedb.cache.conditional_txn.started_total", 1)
	assertUintStatAtLeast(t, stats, "treedb.cache.conditional_txn.commits_total", 1)
	assertUintStatAtLeast(t, stats, "treedb.cache.conditional_txn.read_set.entries_total", 1)
	assertUintStatEquals(t, stats, "treedb.cache.conditional_txn.active", 0)
}

func TestPublicConditionalTxnCachedHandleConflictsOnConcurrentWrite(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if _, _, err := tx.GetVersioned([]byte("k")); err != nil {
		t.Fatalf("tx.GetVersioned: %v", err)
	}
	if err := db.Set([]byte("k"), []byte("outside")); err != nil {
		t.Fatalf("outside Set: %v", err)
	}
	if err := tx.Set([]byte("k"), []byte("inside")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("tx.Commit error=%v, want ErrConcurrentModification", err)
	}
	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("outside")) {
		t.Fatalf("Get=%q, want outside", got)
	}
}

func TestPublicConditionalTxnCachedHandleConflictsWhenKeyChangesBeforeFirstRead(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if err := db.Set([]byte("k"), []byte("outside")); err != nil {
		t.Fatalf("outside Set: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("k")); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("tx.GetVersioned error=%v, want ErrConcurrentModification", err)
	}
}

func TestPublicConditionalTxnCachedHandleReadsStagedWrites(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if err := tx.Set([]byte("k"), []byte("staged")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if got, _, err := tx.GetVersionedAppend([]byte("k"), []byte("prefix:")); err != nil || !bytes.Equal(got, []byte("prefix:staged")) {
		t.Fatalf("tx.GetVersionedAppend=(%q,%v), want prefix:staged,nil", got, err)
	}
	if got, err := tx.Get([]byte("k")); err != nil || !bytes.Equal(got, []byte("staged")) {
		t.Fatalf("tx.Get=(%q,%v), want staged,nil", got, err)
	}
	if found, err := tx.Has([]byte("k")); err != nil || !found {
		t.Fatalf("tx.Has=(%t,%v), want true,nil", found, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}
	if got, err := db.Get([]byte("k")); err != nil || !bytes.Equal(got, []byte("staged")) {
		t.Fatalf("Get=(%q,%v), want staged,nil", got, err)
	}
}

func TestPublicConditionalTxnCachedHandleReadsStagedDelete(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if err := tx.Delete([]byte("k")); err != nil {
		t.Fatalf("tx.Delete: %v", err)
	}
	if found, err := tx.Has([]byte("k")); err != nil || found {
		t.Fatalf("tx.Has=(%t,%v), want false,nil", found, err)
	}
	if got, err := tx.Get([]byte("k")); err != nil || got != nil {
		t.Fatalf("tx.Get=(%q,%v), want nil,nil", got, err)
	}
	prefix := []byte("prefix:")
	got, _, err := tx.GetVersionedAppend([]byte("k"), append([]byte(nil), prefix...))
	if !errors.Is(err, ErrKeyNotFound) || !bytes.Equal(got, prefix) {
		t.Fatalf("tx.GetVersionedAppend=(%q,%v), want prefix, ErrKeyNotFound", got, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}
	if got, err := db.Get([]byte("k")); err != nil || got != nil {
		t.Fatalf("Get=(%q,%v), want nil,nil", got, err)
	}
}

func TestPublicConditionalTxnCachedHandleConflictsOnAbsentInsertDelete(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if found, err := tx.Has([]byte("missing")); err != nil || found {
		t.Fatalf("tx.Has=(%t,%v), want false,nil", found, err)
	}
	if err := db.Set([]byte("missing"), []byte("outside")); err != nil {
		t.Fatalf("outside Set: %v", err)
	}
	if err := db.Delete([]byte("missing")); err != nil {
		t.Fatalf("outside Delete: %v", err)
	}
	if err := tx.Set([]byte("missing"), []byte("inside")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("tx.Commit error=%v, want ErrConcurrentModification", err)
	}
	if got, err := db.Get([]byte("missing")); err != nil || got != nil {
		t.Fatalf("Get after outside delete=(%q,%v), want nil,nil", got, err)
	}
}

func TestPublicConditionalTxnCachedHandleConflictsOnDeleteRange(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if found, err := tx.Has([]byte("k")); err != nil || !found {
		t.Fatalf("tx.Has=(%t,%v), want true,nil", found, err)
	}
	wb := db.NewBatch()
	if wb == nil {
		t.Fatal("NewBatch returned nil")
	}
	if err := wb.DeleteRange([]byte("a"), []byte("z")); err != nil {
		_ = wb.Close()
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := wb.Write(); err != nil {
		_ = wb.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := wb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Set([]byte("k"), []byte("inside")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("tx.Commit error=%v, want ErrConcurrentModification", err)
	}
}

func TestPublicConditionalTxnCachedHandleReusableStorage(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	var tx ConditionalTxn
	if err := db.InitConditionalTxn(&tx); err != nil {
		t.Fatalf("InitConditionalTxn: %v", err)
	}
	if err := db.InitConditionalTxn(&tx); !errors.Is(err, ErrConditionalTxnClosed) {
		_ = tx.Close()
		t.Fatalf("active InitConditionalTxn error=%v, want ErrConditionalTxnClosed", err)
	}
	if _, _, err := tx.GetVersioned([]byte("k")); err != nil {
		_ = tx.Close()
		t.Fatalf("tx.GetVersioned: %v", err)
	}
	if err := tx.Set([]byte("k"), []byte("middle")); err != nil {
		_ = tx.Close()
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}
	if err := db.InitConditionalTxn(&tx); err != nil {
		t.Fatalf("second InitConditionalTxn: %v", err)
	}
	if got, _, err := tx.GetVersioned([]byte("k")); err != nil || !bytes.Equal(got, []byte("middle")) {
		_ = tx.Close()
		t.Fatalf("second tx.GetVersioned=(%q,%v), want middle,nil", got, err)
	}
	if err := tx.Set([]byte("k"), []byte("after")); err != nil {
		_ = tx.Close()
		t.Fatalf("second tx.Set: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("second tx.Commit: %v", err)
	}
	if got, err := db.Get([]byte("k")); err != nil || !bytes.Equal(got, []byte("after")) {
		t.Fatalf("Get=(%q,%v), want after,nil", got, err)
	}
	assertUintStatEquals(t, db.Stats(), "treedb.cache.conditional_txn.active", 0)
}

func TestPublicConditionalTxnCommandWALCachedHandleFailsClosed(t *testing.T) {
	db, err := Open(OptionsFor(ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.NewConditionalTxn(); !errors.Is(err, ErrConditionalTxnUnsupported) {
		t.Fatalf("NewConditionalTxn error=%v, want ErrConditionalTxnUnsupported", err)
	}
}

func assertUintStatAtLeast(t *testing.T, stats map[string]string, key string, want uint64) {
	t.Helper()
	got := parseUintStatForTest(t, stats, key)
	if got < want {
		t.Fatalf("%s=%d, want >= %d", key, got, want)
	}
}

func assertUintStatEquals(t *testing.T, stats map[string]string, key string, want uint64) {
	t.Helper()
	got := parseUintStatForTest(t, stats, key)
	if got != want {
		t.Fatalf("%s=%d, want %d", key, got, want)
	}
}

func parseUintStatForTest(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %s", key)
	}
	got, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return got
}
