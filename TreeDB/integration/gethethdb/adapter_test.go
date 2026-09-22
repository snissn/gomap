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
	treebatch "github.com/snissn/gomap/TreeDB/batch"
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

func TestResolveTreeDBOptionsReadIntegrityEnv(t *testing.T) {
	clearReadIntegrityEnv(t)
	dir := filepath.Join(t.TempDir(), "treedb")
	opts, err := resolveTreeDBOptions(dir, nil)
	if err != nil {
		t.Fatalf("resolve default options: %v", err)
	}
	if got := opts.ValueLog.ReadIntegrity; got != treedb.IntegrityVerify {
		t.Fatalf("default read integrity=%d want verify", got)
	}

	t.Setenv(EnvReadIntegrity, "unsafe-skip-checksums")
	opts, err = resolveTreeDBOptions(dir, nil)
	if err != nil {
		t.Fatalf("resolve skip-checksum options: %v", err)
	}
	if got := opts.ValueLog.ReadIntegrity; got != treedb.IntegritySkipChecksums {
		t.Fatalf("env read integrity=%d want skip-checksums", got)
	}

	t.Setenv(EnvReadIntegrity, "verify")
	relaxed := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts, err = resolveTreeDBOptions(dir, &OpenOptions{Options: &relaxed})
	if err != nil {
		t.Fatalf("resolve verify override options: %v", err)
	}
	if got := opts.ValueLog.ReadIntegrity; got != treedb.IntegrityVerify {
		t.Fatalf("env verify override read integrity=%d want verify", got)
	}
}

func TestResolveTreeDBOptionsReadIntegrityFallbackEnv(t *testing.T) {
	clearReadIntegrityEnv(t)
	t.Setenv(EnvReadIntegrityFallback, "unsafe-skip-checksums")
	opts, err := resolveTreeDBOptions(filepath.Join(t.TempDir(), "treedb"), nil)
	if err != nil {
		t.Fatalf("resolve fallback read integrity: %v", err)
	}
	if got := opts.ValueLog.ReadIntegrity; got != treedb.IntegritySkipChecksums {
		t.Fatalf("fallback env read integrity=%d want skip-checksums", got)
	}

	t.Setenv(EnvReadIntegrity, "verify")
	opts, err = resolveTreeDBOptions(filepath.Join(t.TempDir(), "treedb"), nil)
	if err != nil {
		t.Fatalf("resolve specific read integrity: %v", err)
	}
	if got := opts.ValueLog.ReadIntegrity; got != treedb.IntegrityVerify {
		t.Fatalf("specific env read integrity=%d want verify", got)
	}
}

func TestResolveTreeDBOptionsReadIntegrityEnvRejectsInvalid(t *testing.T) {
	clearReadIntegrityEnv(t)
	t.Setenv(EnvReadIntegrity, "fast-but-maybe-safe")
	_, err := resolveTreeDBOptions(filepath.Join(t.TempDir(), "treedb"), nil)
	if err == nil || !strings.Contains(err.Error(), EnvReadIntegrity) {
		t.Fatalf("resolve invalid read integrity err=%v, want %s error", err, EnvReadIntegrity)
	}
}

func clearReadIntegrityEnv(t *testing.T) {
	t.Helper()
	t.Setenv(EnvReadIntegrity, "")
	t.Setenv(EnvReadIntegrityFallback, "")
}

func TestOpenDefaultsUseGethSizedCommandWALSegments(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "treedb")
	opts, err := resolveTreeDBOptions(dir, nil)
	if err != nil {
		t.Fatalf("resolve default options: %v", err)
	}
	if got := opts.WALMaxSegmentBytes; got != defaultGethCommandWALMaxSegmentBytes {
		t.Fatalf("default WALMaxSegmentBytes=%d want %d", got, defaultGethCommandWALMaxSegmentBytes)
	}
	if got, ok := int64OptionField(opts, "CommandWALSegmentTargetBytes"); ok && got != defaultGethCommandWALSegmentTargetBytes {
		t.Fatalf("default CommandWALSegmentTargetBytes=%d want %d", got, defaultGethCommandWALSegmentTargetBytes)
	}
	if got := opts.MaxWALBytes; got != defaultGethCommandWALCheckpointMaxWALBytes {
		t.Fatalf("default MaxWALBytes=%d want %d", got, defaultGethCommandWALCheckpointMaxWALBytes)
	}

	custom := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	custom.WALMaxSegmentBytes = 1024
	_ = setInt64OptionFieldDefault(&custom, "CommandWALSegmentTargetBytes", 2048)
	custom.MaxWALBytes = 4096
	opts, err = resolveTreeDBOptions(dir, &OpenOptions{Options: &custom})
	if err != nil {
		t.Fatalf("resolve custom options: %v", err)
	}
	if got := opts.WALMaxSegmentBytes; got != 1024 {
		t.Fatalf("explicit WALMaxSegmentBytes=%d want preserved 1024", got)
	}
	if got, ok := int64OptionField(opts, "CommandWALSegmentTargetBytes"); ok && got != 2048 {
		t.Fatalf("explicit CommandWALSegmentTargetBytes=%d want preserved 2048", got)
	}
	if got := opts.MaxWALBytes; got != 4096 {
		t.Fatalf("explicit MaxWALBytes=%d want preserved 4096", got)
	}
}

func TestOpenWithOptionsAppliesGethSizedCommandWALSegments(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, filepath.Join(t.TempDir(), "treedb"))
	opts.WALMaxSegmentBytes = 0
	db, err := OpenWithOptions(opts)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer db.Close()
	if got := db.walMaxSegmentBytes; got != defaultGethCommandWALMaxSegmentBytes {
		t.Fatalf("OpenWithOptions WALMaxSegmentBytes=%d want %d", got, defaultGethCommandWALMaxSegmentBytes)
	}
	if _, ok := int64OptionField(treedb.Options{}, "CommandWALSegmentTargetBytes"); ok && db.commandWALSegmentTargetBytes != defaultGethCommandWALSegmentTargetBytes {
		got := db.commandWALSegmentTargetBytes
		t.Fatalf("OpenWithOptions CommandWALSegmentTargetBytes=%d want %d", got, defaultGethCommandWALSegmentTargetBytes)
	}
	if got := db.maxWALBytes; got != defaultGethCommandWALCheckpointMaxWALBytes {
		t.Fatalf("OpenWithOptions MaxWALBytes=%d want %d", got, defaultGethCommandWALCheckpointMaxWALBytes)
	}
}

func TestOpenWithOptionsAppliesGethSizedWALSegmentsBeforeReadOnlyFormatActivation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "treedb")
	writable, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Open writable: %v", err)
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("close writable: %v", err)
	}

	readOnly, err := OpenWithOptions(treedb.Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("OpenWithOptions read-only: %v", err)
	}
	defer readOnly.Close()
	if got := readOnly.walMaxSegmentBytes; got != defaultGethCommandWALMaxSegmentBytes {
		t.Fatalf("read-only WALMaxSegmentBytes=%d want %d", got, defaultGethCommandWALMaxSegmentBytes)
	}
	if _, ok := int64OptionField(treedb.Options{}, "CommandWALSegmentTargetBytes"); ok && readOnly.commandWALSegmentTargetBytes != defaultGethCommandWALSegmentTargetBytes {
		got := readOnly.commandWALSegmentTargetBytes
		t.Fatalf("read-only CommandWALSegmentTargetBytes=%d want %d", got, defaultGethCommandWALSegmentTargetBytes)
	}
}

func TestOpenRejectsWritableNonCommandWALOptions(t *testing.T) {
	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, filepath.Join(t.TempDir(), "treedb"))
	if _, err := OpenWithOptions(opts); err == nil {
		t.Fatal("OpenWithOptions accepted writable non-command-WAL options")
	}
	if _, err := Open(filepath.Join(t.TempDir(), "treedb"), &OpenOptions{Profile: treedb.ProfileBench}); err == nil {
		t.Fatal("Open accepted bench profile without command WAL")
	}
}

func TestOpenReadOnlyMissingPathDoesNotCreateDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("precondition stat(%q) err=%v, want not exist", missing, err)
	}
	if db, err := Open(missing, &OpenOptions{ReadOnly: true}); err == nil {
		_ = db.Close()
		t.Fatal("read-only Open on missing path succeeded, want error")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only Open created %q or returned unexpected stat err=%v", missing, err)
	}
}

func TestOpenReadOnlyRejectsFilePathWithoutMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(path, []byte("not a TreeDB dir"), 0o644); err != nil {
		t.Fatalf("write file path: %v", err)
	}
	if db, err := Open(path, &OpenOptions{ReadOnly: true}); err == nil {
		_ = db.Close()
		t.Fatal("read-only Open on file path succeeded, want error")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat file path after read-only Open: %v", err)
	}
	if info.IsDir() {
		t.Fatalf("read-only Open converted file path %q into directory", path)
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

func TestGetMissingReturnsNotFoundAndZeroLengthValuesRemainReadable(t *testing.T) {
	db := openTestDB(t)

	if got, err := db.Get([]byte("missing")); !errors.Is(err, ErrNotFound) || got != nil {
		t.Fatalf("Get missing got=%q err=%v, want ErrNotFound", got, err)
	}
	for _, key := range [][]byte{[]byte("nil-value"), []byte("empty-value")} {
		var value []byte
		if string(key) == "empty-value" {
			value = []byte{}
		}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get %q: %v", key, err)
		}
		if len(got) != 0 {
			t.Fatalf("Get %q length=%d want 0", key, len(got))
		}
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
	t.Run("point-submission-order-and-duplicates", func(t *testing.T) {
		db := openTestDB(t)
		batch := db.NewBatch()
		if err := batch.Put([]byte("b"), []byte("1")); err != nil {
			t.Fatal(err)
		}
		if err := batch.Put([]byte("a"), []byte("1")); err != nil {
			t.Fatal(err)
		}
		if err := batch.Put([]byte("b"), []byte("2")); err != nil {
			t.Fatal(err)
		}
		if err := batch.Delete([]byte("a")); err != nil {
			t.Fatal(err)
		}
		var rec replayRecorder
		if err := batch.Replay(&rec); err != nil {
			t.Fatal(err)
		}
		want := []string{"put:b=1", "put:a=1", "put:b=2", "delete:a"}
		if !slices.Equal(rec.ops, want) {
			t.Fatalf("replay ops=%v want %v", rec.ops, want)
		}
	})

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

func TestBatchWriteWithoutResetPreservesContents(t *testing.T) {
	db := openTestDB(t)
	batch := db.NewBatch()
	if err := batch.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	initialSize := batch.ValueSize()
	if initialSize == 0 {
		t.Fatal("initial ValueSize=0, want queued bytes")
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	assertValue(t, db, []byte("a"), []byte("1"))
	if got := batch.ValueSize(); got != initialSize {
		t.Fatalf("ValueSize after Write=%d want %d", got, initialSize)
	}

	if err := db.Delete([]byte("a")); err != nil {
		t.Fatalf("external Delete: %v", err)
	}
	assertMissing(t, db, []byte("a"))
	if err := batch.Write(); err != nil {
		t.Fatalf("second Write without Reset: %v", err)
	}
	assertValue(t, db, []byte("a"), []byte("1"))

	if err := batch.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatalf("second external Delete: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("third Write without Reset: %v", err)
	}
	assertValue(t, db, []byte("a"), []byte("1"))
	assertValue(t, db, []byte("b"), []byte("2"))

	batch.Reset()
	if got := batch.ValueSize(); got != 0 {
		t.Fatalf("ValueSize after Reset=%d want 0", got)
	}
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatalf("delete a after Reset: %v", err)
	}
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatalf("delete b after Reset: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("empty Write after Reset: %v", err)
	}
	assertMissing(t, db, []byte("a"))
	assertMissing(t, db, []byte("b"))
}

func TestBatchWriteThenImmediateResetSkipsLazyRebuild(t *testing.T) {
	inner := &countingTreeBatch{}
	batch := &Batch{db: &Database{}, inner: inner}
	if err := batch.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := inner.applyCalls(); got != 1 {
		t.Fatalf("inner apply calls before Write=%d want 1", got)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !batch.needsRebuild {
		t.Fatal("batch should mark inner rebuild as lazy after Write")
	}
	batch.Reset()
	if batch.needsRebuild {
		t.Fatal("Reset should discard lazy rebuild requirement")
	}
	if got := batch.ValueSize(); got != 0 {
		t.Fatalf("ValueSize after Reset=%d want 0", got)
	}
	if got := len(batch.ops); got != 0 {
		t.Fatalf("ops after Reset len=%d want 0", got)
	}
	if got := inner.applyCalls(); got != 1 {
		t.Fatalf("inner apply calls after Write+Reset=%d want 1 (no rebuild)", got)
	}
	if inner.writeCalls != 1 {
		t.Fatalf("inner Write calls=%d want 1", inner.writeCalls)
	}
	if inner.resetCalls != 1 {
		t.Fatalf("inner Reset calls=%d want 1", inner.resetCalls)
	}
}

func TestBatchWriteThenReplayPreservesOperations(t *testing.T) {
	db := openTestDB(t)
	batch := db.NewBatch()
	if err := batch.Put([]byte("b"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := batch.DeleteRange([]byte("m"), []byte("z")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var rec replayRecorder
	if err := batch.Replay(&rec); err != nil {
		t.Fatalf("Replay after Write: %v", err)
	}
	want := []string{"put:b=1", "delete:a", "delete-range:m..z"}
	if !slices.Equal(rec.ops, want) {
		t.Fatalf("replay after Write ops=%v want %v", rec.ops, want)
	}
}

func TestBatchClonesCallerBuffersForWriteReplayAndReuse(t *testing.T) {
	db := openTestDB(t)
	batch := db.NewBatch()
	key := []byte("key-a")
	value := []byte("value-a")
	if err := batch.Put(key, value); err != nil {
		t.Fatalf("Put: %v", err)
	}
	copy(key, "key-b")
	copy(value, "value-b")
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	assertValue(t, db, []byte("key-a"), []byte("value-a"))
	assertMissing(t, db, []byte("key-b"))

	var rec replayRecorder
	if err := batch.Replay(&rec); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if want := []string{"put:key-a=value-a"}; !slices.Equal(rec.ops, want) {
		t.Fatalf("replay ops=%v want %v", rec.ops, want)
	}

	if err := db.Delete([]byte("key-a")); err != nil {
		t.Fatalf("external Delete: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("repeated Write: %v", err)
	}
	assertValue(t, db, []byte("key-a"), []byte("value-a"))
}

func TestBatchReplayBeforeWriteUsesCommandWALOwnedBytes(t *testing.T) {
	db := openTestDB(t)
	batch, ok := db.NewBatch().(*Batch)
	if !ok {
		t.Fatalf("NewBatch type=%T want *Batch", db.NewBatch())
	}
	key := []byte("key-a")
	value := []byte("value-a")
	if err := batch.Put(key, value); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !batch.hasBorrowedOps {
		t.Skip("local TreeDB command-WAL replay-byte optimization unavailable without a gomap module replace")
	}
	copy(key, "key-b")
	copy(value, "value-b")
	var rec replayRecorder
	if err := batch.Replay(&rec); err != nil {
		t.Fatalf("Replay before Write: %v", err)
	}
	if want := []string{"put:key-a=value-a"}; !slices.Equal(rec.ops, want) {
		t.Fatalf("replay before Write ops=%v want %v", rec.ops, want)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !batch.hasBorrowedOps {
		t.Fatal("Write+pending replay should retain borrowed op-log bytes until Reset/rebuild")
	}
	batch.Reset()
	if batch.hasBorrowedOps || len(batch.ops) != 0 {
		t.Fatalf("Reset left borrowed=%t ops=%d", batch.hasBorrowedOps, len(batch.ops))
	}
}

func TestBatchBorrowedReplayBytesHandleReusedZeroValueBuffer(t *testing.T) {
	db := openTestDB(t)
	batch := db.NewBatch().(*Batch)
	value := make([]byte, len("value-b"))
	if err := batch.Put([]byte("key-a"), value); err != nil {
		t.Fatalf("Put zero value: %v", err)
	}
	if !batch.hasBorrowedOps {
		t.Skip("local TreeDB command-WAL replay-byte optimization unavailable without a gomap module replace")
	}
	copy(value, "value-b")
	if err := batch.Put([]byte("key-b"), value); err != nil {
		t.Fatalf("Put reused nonzero value: %v", err)
	}
	var rec replayRecorder
	if err := batch.Replay(&rec); err != nil {
		t.Fatalf("Replay before Write: %v", err)
	}
	if want := []string{"put:key-a=\x00\x00\x00\x00\x00\x00\x00", "put:key-b=value-b"}; !slices.Equal(rec.ops, want) {
		t.Fatalf("replay ops=%q want %q", rec.ops, want)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	assertValue(t, db, []byte("key-a"), make([]byte, len("value-b")))
	assertValue(t, db, []byte("key-b"), []byte("value-b"))
}

func TestBatchRepeatedWriteDetachesCommandWALBorrowedReplayBytes(t *testing.T) {
	db := openTestDB(t)
	batch := db.NewBatch().(*Batch)
	if err := batch.Put([]byte("key-a"), []byte("value-a")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if !batch.hasBorrowedOps {
		t.Skip("local TreeDB command-WAL replay-byte optimization unavailable without a gomap module replace")
	}
	if err := db.Delete([]byte("key-a")); err != nil {
		t.Fatalf("external Delete: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if batch.hasBorrowedOps {
		t.Fatal("repeated Write should detach borrowed replay bytes before rebuilding")
	}
	assertValue(t, db, []byte("key-a"), []byte("value-a"))
}

func TestBatchReplayAfterCloseDetachesCommandWALBorrowedReplayBytes(t *testing.T) {
	db := openTestDB(t)
	batch := db.NewBatch().(*Batch)
	if err := batch.Put([]byte("key-a"), []byte("value-a")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !batch.hasBorrowedOps {
		t.Skip("local TreeDB command-WAL replay-byte optimization unavailable without a gomap module replace")
	}
	closer, ok := any(batch).(interface{ Close() })
	if !ok {
		t.Fatal("batch does not expose Close")
	}
	closer.Close()
	if batch.hasBorrowedOps {
		t.Fatal("Close should detach borrowed replay bytes")
	}
	var rec replayRecorder
	if err := batch.Replay(&rec); err != nil {
		t.Fatalf("Replay after Close: %v", err)
	}
	if want := []string{"put:key-a=value-a"}; !slices.Equal(rec.ops, want) {
		t.Fatalf("replay after Close ops=%v want %v", rec.ops, want)
	}
}

func TestBatchDeleteAndDeleteRangeCloneCallerBuffersForReplayAndReuse(t *testing.T) {
	db := openTestDB(t)
	for _, key := range []string{"aa", "bb", "bc", "za", "zb"} {
		if err := db.Put([]byte(key), []byte("v-"+key)); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	batch := db.NewBatch()
	deleteKey := []byte("aa")
	rangeStart := []byte("bb")
	rangeEnd := []byte("bd")
	if err := batch.Delete(deleteKey); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := batch.DeleteRange(rangeStart, rangeEnd); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	copy(deleteKey, "za")
	copy(rangeStart, "za")
	copy(rangeEnd, "zz")
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for _, key := range [][]byte{[]byte("aa"), []byte("bb"), []byte("bc")} {
		assertMissing(t, db, key)
	}
	for _, key := range [][]byte{[]byte("za"), []byte("zb")} {
		assertValue(t, db, key, append([]byte("v-"), key...))
	}

	var rec replayRecorder
	if err := batch.Replay(&rec); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	want := []string{"delete:aa", "delete-range:bb..bd"}
	if !slices.Equal(rec.ops, want) {
		t.Fatalf("replay ops=%v want %v", rec.ops, want)
	}

	for _, key := range []string{"aa", "bb", "bc"} {
		if err := db.Put([]byte(key), []byte("again-"+key)); err != nil {
			t.Fatalf("restore %s: %v", key, err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("repeated Write: %v", err)
	}
	for _, key := range [][]byte{[]byte("aa"), []byte("bb"), []byte("bc")} {
		assertMissing(t, db, key)
	}
	for _, key := range [][]byte{[]byte("za"), []byte("zb")} {
		assertValue(t, db, key, append([]byte("v-"), key...))
	}
}

func TestBatchCloseKeepsClosedMutationCompatibility(t *testing.T) {
	db := openTestDB(t)
	batch := db.NewBatch()
	if err := batch.Put([]byte("before-close"), []byte("v")); err != nil {
		t.Fatalf("Put before close: %v", err)
	}
	closer, ok := batch.(interface{ Close() })
	if !ok {
		t.Fatal("batch does not expose Close")
	}
	closer.Close()
	if err := batch.Put([]byte("after-close"), []byte("v")); err != nil {
		t.Fatalf("Put after batch Close err=%v, want nil", err)
	}
	if err := batch.DeleteRange([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("DeleteRange after batch Close err=%v, want nil", err)
	}
	if got := batch.ValueSize(); got == 0 {
		t.Fatal("closed batch ValueSize=0 after queued ops")
	}
	if err := batch.Write(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed batch Write err=%v want ErrClosed", err)
	}
	batch.Reset()
	if got := batch.ValueSize(); got != 0 {
		t.Fatalf("closed batch ValueSize after Reset=%d want 0", got)
	}
	var rec replayRecorder
	if err := batch.Replay(&rec); err != nil {
		t.Fatalf("Replay after Reset: %v", err)
	}
	if len(rec.ops) != 0 {
		t.Fatalf("Replay after Reset ops=%v want none", rec.ops)
	}
}

func TestAdapterBatchOpReserveHint(t *testing.T) {
	for _, tc := range []struct {
		size int
		want int
	}{
		{size: -1, want: 0},
		{size: 0, want: 0},
		{size: 16, want: 16},
		{size: 8 * 1024, want: 8 * 1024},
		{size: 102400, want: 400},
	} {
		if got := adapterBatchOpReserveHint(tc.size); got != tc.want {
			t.Fatalf("adapterBatchOpReserveHint(%d)=%d want %d", tc.size, got, tc.want)
		}
	}
}

func TestNewBatchWithSizePreallocatesAdapterOpLogConservatively(t *testing.T) {
	db := openTestDB(t)
	small, ok := db.NewBatchWithSize(16).(*Batch)
	if !ok {
		t.Fatalf("NewBatchWithSize type=%T want *Batch", db.NewBatchWithSize(16))
	}
	if got := cap(small.ops); got < 16 {
		t.Fatalf("small op log cap=%d want at least 16", got)
	}
	large := db.NewBatchWithSize(102400).(*Batch)
	if got, want := cap(large.ops), adapterBatchOpReserveHint(102400); got != want {
		t.Fatalf("large byte-hint op log cap=%d want %d", got, want)
	}
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

func TestIteratorNextAfterExhaustionIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	for _, key := range []string{"a", "b", "c"} {
		if err := db.Put([]byte(key), []byte("v-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	it := db.NewIterator(nil, nil)
	var got []string
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("iterator keys=%v want %v", got, want)
	}
	for i := 0; i < 3; i++ {
		if it.Next() {
			t.Fatalf("exhausted iterator Next #%d returned true", i+1)
		}
		if key := it.Key(); key != nil {
			t.Fatalf("exhausted iterator Key #%d=%q want nil", i+1, key)
		}
		if value := it.Value(); value != nil {
			t.Fatalf("exhausted iterator Value #%d=%q want nil", i+1, value)
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("exhausted iterator err=%v", err)
	}
	it.Release()
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

type countingTreeBatch struct {
	setCalls         int
	setViewCalls     int
	deleteCalls      int
	deleteViewCalls  int
	deleteRangeCalls int
	writeCalls       int
	writeSyncCalls   int
	resetCalls       int
	closeCalls       int
}

func (b *countingTreeBatch) applyCalls() int {
	return b.setCalls + b.setViewCalls + b.deleteCalls + b.deleteViewCalls + b.deleteRangeCalls
}

func (b *countingTreeBatch) Set(_, _ []byte) error {
	b.setCalls++
	return nil
}

func (b *countingTreeBatch) SetView(_, _ []byte) error {
	b.setViewCalls++
	return nil
}

func (b *countingTreeBatch) Delete(_ []byte) error {
	b.deleteCalls++
	return nil
}

func (b *countingTreeBatch) DeleteView(_ []byte) error {
	b.deleteViewCalls++
	return nil
}

func (b *countingTreeBatch) DeleteRange(_, _ []byte) error {
	b.deleteRangeCalls++
	return nil
}

func (b *countingTreeBatch) Write() error {
	b.writeCalls++
	return nil
}

func (b *countingTreeBatch) WriteSync() error {
	b.writeSyncCalls++
	return nil
}

func (b *countingTreeBatch) Close() error {
	b.closeCalls++
	return nil
}

func (b *countingTreeBatch) Reset() {
	b.resetCalls++
}

func (b *countingTreeBatch) Replay(func(treebatch.Entry) error) error {
	return nil
}

func (b *countingTreeBatch) GetByteSize() (int, error) {
	return 0, nil
}

type replayRecorder struct {
	ops []string
}

func (r *replayRecorder) Put(key []byte, value []byte) error {
	r.ops = append(r.ops, "put:"+string(key)+"="+string(value))
	return nil
}

func (r *replayRecorder) Delete(key []byte) error {
	r.ops = append(r.ops, "delete:"+string(key))
	return nil
}

func (r *replayRecorder) DeleteRange(start, end []byte) error {
	r.ops = append(r.ops, "delete-range:"+string(start)+".."+string(end))
	return nil
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
