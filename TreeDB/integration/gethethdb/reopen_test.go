package gethethdb

import (
	"bytes"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func openAtPath(t testing.TB, dir string) *Database {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	// Force non-empty raw-KV values through the persistent value log so this
	// adapter reopen test covers pointer-backed values, not just inline payloads.
	opts.ValueLog.PointerThreshold = 1
	db, err := OpenWithOptions(opts)
	if err != nil {
		t.Fatalf("OpenWithOptions(%s): %v", dir, err)
	}
	return db
}

func TestCommandWALReopenPersistsBorrowedBatchPut(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "treedb")
	db := openAtPath(t, dir)

	batch, ok := db.NewBatch().(*Batch)
	if !ok {
		t.Fatalf("NewBatch type=%T want *Batch", db.NewBatch())
	}
	key := []byte("borrowed-key")
	value := []byte("borrowed-value")
	if err := batch.Put(key, value); err != nil {
		t.Fatalf("batch Put: %v", err)
	}
	if !batch.hasBorrowedOps {
		t.Skip("local TreeDB command-WAL replay-byte optimization unavailable without a gomap module replace")
	}
	copy(key, "mutated-key-")
	copy(value, "mutated-value-")
	if err := batch.Write(); err != nil {
		t.Fatalf("batch Write: %v", err)
	}
	if err := db.SyncKeyValue(); err != nil {
		t.Fatalf("SyncKeyValue: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openAtPath(t, dir)
	defer reopened.Close()
	assertValue(t, reopened, []byte("borrowed-key"), []byte("borrowed-value"))
	assertMissing(t, reopened, []byte("mutated-key-"))
}

func TestCommandWALReopenPersistsAdapterOperations(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "treedb")
	db := openAtPath(t, dir)

	if err := db.Put([]byte("point"), []byte("value")); err != nil {
		t.Fatalf("Put point: %v", err)
	}
	if err := db.Put(nil, nil); err != nil {
		t.Fatalf("Put empty key nil value: %v", err)
	}

	batch := db.NewBatch()
	if err := batch.Put([]byte("batch"), []byte("value")); err != nil {
		t.Fatalf("batch Put: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("batch Write: %v", err)
	}

	for _, key := range []string{"dbdr/1", "dbdr/2", "dbdr/3", "batchdr/1", "batchdr/2", "batchdr/3"} {
		if err := db.Put([]byte(key), []byte("range-value")); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	if err := db.DeleteRange([]byte("dbdr/1"), []byte("dbdr/3")); err != nil {
		t.Fatalf("DB DeleteRange: %v", err)
	}

	rangeBatch := db.NewBatch()
	if err := rangeBatch.DeleteRange([]byte("batchdr/1"), []byte("batchdr/3")); err != nil {
		t.Fatalf("batch DeleteRange: %v", err)
	}
	if err := rangeBatch.Write(); err != nil {
		t.Fatalf("batch DeleteRange Write: %v", err)
	}

	stats := db.TreeDB().Stats()
	if got := stats["treedb.write_path.mode"]; got != "command_wal_cached" {
		t.Fatalf("write_path.mode=%q want command_wal_cached", got)
	}
	if err := db.SyncKeyValue(); err != nil {
		t.Fatalf("SyncKeyValue: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openAtPath(t, dir)
	defer reopened.Close()
	assertValue(t, reopened, []byte("point"), []byte("value"))
	assertValue(t, reopened, []byte("batch"), []byte("value"))
	assertValue(t, reopened, nil, []byte{})
	assertMissing(t, reopened, []byte("dbdr/1"))
	assertMissing(t, reopened, []byte("dbdr/2"))
	assertValue(t, reopened, []byte("dbdr/3"), []byte("range-value"))
	assertMissing(t, reopened, []byte("batchdr/1"))
	assertMissing(t, reopened, []byte("batchdr/2"))
	assertValue(t, reopened, []byte("batchdr/3"), []byte("range-value"))
}

func assertValue(t testing.TB, db *Database, key, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q)=%q want %q", key, got, want)
	}
}

func assertMissing(t testing.TB, db *Database, key []byte) {
	t.Helper()
	has, err := db.Has(key)
	if err != nil {
		t.Fatalf("Has(%q): %v", key, err)
	}
	if has {
		t.Fatalf("Has(%q)=true want false", key)
	}
	if got, err := db.Get(key); err == nil || got != nil {
		t.Fatalf("Get(%q) got=%q err=%v, want not-found error", key, got, err)
	}
}
