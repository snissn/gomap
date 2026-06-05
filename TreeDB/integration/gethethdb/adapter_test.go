package gethethdb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
	treedb "github.com/snissn/gomap/TreeDB"
)

func openTestDB(t testing.TB) *Database {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "treedb"), &OpenOptions{Profile: treedb.ProfileCommandWALRelaxed})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestDatabaseSuite(t *testing.T) {
	dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
		return openTestDB(t)
	})
}

func TestOpenDefaultsToCommandWALDurable(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "treedb"), nil)
	if err != nil {
		t.Fatalf("Open default: %v", err)
	}
	defer db.Close()
	stats := db.TreeDB().Stats()
	if got := stats["treedb.write_path.mode"]; got != "command_wal_cached" {
		t.Fatalf("write_path.mode=%q want command_wal_cached", got)
	}
	if got := stats["treedb.durability_mode"]; got != "wal_on_sync" {
		t.Fatalf("durability_mode=%q, want wal_on_sync", got)
	}
}

func TestOpenRejectsWritableNonCommandWALOptions(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileBench, filepath.Join(t.TempDir(), "treedb"))
	if _, err := OpenWithOptions(opts); err == nil {
		t.Fatal("OpenWithOptions accepted writable non-command-WAL options")
	}
	if _, err := Open(filepath.Join(t.TempDir(), "treedb"), &OpenOptions{Profile: treedb.ProfileBench}); err == nil {
		t.Fatal("Open accepted bench profile without command WAL")
	}
}

func TestNativeEmptyNilKeyAndNilValueParity(t *testing.T) {
	db := openTestDB(t)

	if err := db.Put(nil, []byte("nil-key")); err != nil {
		t.Fatalf("Put nil key: %v", err)
	}
	if got, err := db.Get([]byte{}); err != nil || !bytes.Equal(got, []byte("nil-key")) {
		t.Fatalf("Get empty after nil key got=%q err=%v", got, err)
	}
	if has, err := db.Has(nil); err != nil || !has {
		t.Fatalf("Has nil key has=%v err=%v", has, err)
	}
	if err := db.Put([]byte{}, []byte("empty-key")); err != nil {
		t.Fatalf("Put empty key: %v", err)
	}
	if got, err := db.Get(nil); err != nil || !bytes.Equal(got, []byte("empty-key")) {
		t.Fatalf("Get nil after empty key got=%q err=%v", got, err)
	}

	if err := db.Put([]byte("nil-value"), nil); err != nil {
		t.Fatalf("Put nil value: %v", err)
	}
	got, err := db.Get([]byte("nil-value"))
	if err != nil {
		t.Fatalf("Get nil-value: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("nil value length=%d want 0", len(got))
	}
}

func TestDeleteRangeNilVsEmptyBounds(t *testing.T) {
	db := openTestDB(t)
	for _, key := range [][]byte{nil, []byte("a"), []byte("b")} {
		if err := db.Put(key, []byte("v")); err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
	}
	if err := db.DeleteRange(nil, []byte{}); err != nil {
		t.Fatalf("DeleteRange(nil, empty): %v", err)
	}
	if has, err := db.Has(nil); err != nil || !has {
		t.Fatalf("empty key affected by nil..empty range has=%v err=%v", has, err)
	}
	if err := db.DeleteRange([]byte{}, []byte("b")); err != nil {
		t.Fatalf("DeleteRange(empty, b): %v", err)
	}
	for _, key := range [][]byte{nil, []byte("a")} {
		if has, err := db.Has(key); err != nil || has {
			t.Fatalf("key %q has=%v err=%v, want deleted", key, has, err)
		}
	}
	if has, err := db.Has([]byte("b")); err != nil || !has {
		t.Fatalf("exclusive end key b has=%v err=%v", has, err)
	}
}

func TestBatchReplayPreservesDeleteRangeOrder(t *testing.T) {
	t.Run("put-then-range-delete", func(t *testing.T) {
		db := openTestDB(t)
		batch := db.NewBatch()
		if err := batch.Put([]byte("m"), []byte("new")); err != nil {
			t.Fatal(err)
		}
		if err := batch.DeleteRange([]byte("a"), []byte("z")); err != nil {
			t.Fatal(err)
		}
		if err := batch.Replay(db); err != nil {
			t.Fatal(err)
		}
		if has, err := db.Has([]byte("m")); err != nil || has {
			t.Fatalf("m has=%v err=%v, want deleted by replayed later range", has, err)
		}
	})

	t.Run("range-delete-then-put", func(t *testing.T) {
		db := openTestDB(t)
		if err := db.Put([]byte("m"), []byte("old")); err != nil {
			t.Fatal(err)
		}
		batch := db.NewBatch()
		if err := batch.DeleteRange([]byte("a"), []byte("z")); err != nil {
			t.Fatal(err)
		}
		if err := batch.Put([]byte("m"), []byte("new")); err != nil {
			t.Fatal(err)
		}
		if err := batch.Replay(db); err != nil {
			t.Fatal(err)
		}
		if got, err := db.Get([]byte("m")); err != nil || !bytes.Equal(got, []byte("new")) {
			t.Fatalf("m got=%q err=%v, want replayed later put", got, err)
		}
	})
}

func TestIteratorPrefixStart(t *testing.T) {
	db := openTestDB(t)
	for _, key := range []string{"ka1", "ka2", "ka3", "ka4", "ka5", "kb1"} {
		if err := db.Put([]byte(key), []byte("v-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	it := db.NewIterator([]byte("ka"), []byte("3"))
	got := collectIteratorKeys(it)
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"ka3", "ka4", "ka5"}; !slices.Equal(got, want) {
		t.Fatalf("iterator keys=%v want %v", got, want)
	}
}

func TestOperationsAfterClose(t *testing.T) {
	db := openTestDB(t)
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := db.Get([]byte("key")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after close err=%v want ErrClosed", err)
	}
	if _, err := db.Has([]byte("key")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Has after close err=%v want ErrClosed", err)
	}
	if err := db.Put([]byte("key2"), []byte("value2")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after close err=%v want ErrClosed", err)
	}
	if err := db.Delete([]byte("key")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete after close err=%v want ErrClosed", err)
	}
	if err := db.DeleteRange(nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("DeleteRange after close err=%v want ErrClosed", err)
	}
	if _, err := db.Stat(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Stat after close err=%v want ErrClosed", err)
	}
	if err := db.SyncKeyValue(); !errors.Is(err, ErrClosed) {
		t.Fatalf("SyncKeyValue after close err=%v want ErrClosed", err)
	}
	if err := db.Compact(nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Compact after close err=%v want ErrClosed", err)
	}
	it := db.NewIterator(nil, nil)
	if it.Next() {
		t.Fatal("closed iterator returned an item")
	}
	if err := it.Error(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed iterator err=%v want ErrClosed", err)
	}
	it.Release()
	it.Release()

	batch := db.NewBatch()
	if err := batch.Put([]byte("batchkey"), []byte("batchval")); err != nil {
		t.Fatalf("batch.Put after close err=%v, want nil", err)
	}
	if err := batch.DeleteRange([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("batch.DeleteRange after close err=%v, want nil", err)
	}
	if got := batch.ValueSize(); got == 0 {
		t.Fatal("closed batch ValueSize=0 after queued ops")
	}
	if err := batch.Write(); !errors.Is(err, ErrClosed) {
		t.Fatalf("batch.Write after close err=%v want ErrClosed", err)
	}
	batch.Reset()
	if got := batch.ValueSize(); got != 0 {
		t.Fatalf("closed batch ValueSize after Reset=%d want 0", got)
	}
}

func TestDBLevelDeleteRangeCallsPublicTreeDBDeleteRangeDirectly(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	sourcePath := filepath.Join(filepath.Dir(file), "adapter.go")
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("read adapter source: %v", err)
	}
	body := string(source)
	start := strings.Index(body, "func (d *Database) DeleteRange(start, end []byte) error {")
	if start < 0 {
		t.Fatal("Database.DeleteRange implementation not found")
	}
	end := strings.Index(body[start:], "\n}\n\n// Stat")
	if end < 0 {
		t.Fatal("Database.DeleteRange implementation end not found")
	}
	method := body[start : start+end]
	if !strings.Contains(method, "return tdb.DeleteRange(start, end)") {
		t.Fatalf("Database.DeleteRange does not directly call public TreeDB DB.DeleteRange; body:\n%s", method)
	}
	if strings.Contains(method, "NewBatch") {
		t.Fatalf("Database.DeleteRange contains adapter-side batch path; body:\n%s", method)
	}
}

func collectIteratorKeys(it interface {
	Next() bool
	Key() []byte
	Release()
}) []string {
	defer it.Release()
	var out []string
	for it.Next() {
		out = append(out, string(it.Key()))
	}
	return out
}
