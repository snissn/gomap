package db

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

func writeSystemTreeEntryForTest(t *testing.T, d *DB, key, value []byte) {
	t.Helper()
	if d == nil {
		t.Fatal("nil db")
	}

	state := d.State()
	if state == nil {
		t.Fatal("db state is nil")
	}
	idx := d.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}

	b := batch.New(d.valueLogManager, d.InlineThresholdForKey(key))
	if err := b.Set(key, value); err != nil {
		t.Fatalf("set system batch entry: %v", err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			t.Fatalf("close internal batch: %v", err)
		}
	}()

	newSysRoot, retired, _, err := idx.zipper.Apply(state.SystemRootPageID, b)
	if err != nil {
		t.Fatalf("apply system batch: %v", err)
	}
	if err := d.finalizeCommit(state.RootPageID, newSysRoot, retired, true, adaptive.Metrics{}, nil, true, nil); err != nil {
		t.Fatalf("finalize system-root commit: %v", err)
	}
}

func getSystemTreeValue(t *testing.T, d *DB, key []byte) []byte {
	t.Helper()
	state := d.State()
	if state == nil {
		t.Fatalf("db state is nil")
	}
	tr := tree.New(d.Pager(), newValueReader(state.ValueLogSet), state.SystemRootPageID)
	entry, err := tr.GetEntry(key)
	if err != nil {
		if err == tree.ErrKeyNotFound {
			return nil
		}
		t.Fatalf("system tree get %q: %v", string(key), err)
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil
	}
	return append([]byte(nil), entry.Value...)
}

func TestOpenThenReadSystemMetadataFromSystemTree(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	metaKey, err := collections.SystemCollectionMetaKey("users")
	if err != nil {
		t.Fatalf("system meta key: %v", err)
	}
	meta := &collections.CollectionMeta{Name: "users"}
	encoded, err := meta.Encode()
	if err != nil {
		t.Fatalf("encode collection meta: %v", err)
	}

	writeSystemTreeEntryForTest(t, db, metaKey, encoded)

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got := getSystemTreeValue(t, reopen, metaKey)
	if got == nil {
		t.Fatalf("expected system key %q after reopen", string(metaKey))
	}
	if !bytes.Equal(got, encoded) {
		t.Fatalf("system metadata mismatch: got=%q want=%q", string(got), string(encoded))
	}
}

func TestCatalogStoreRoundTripInSystemRoot_WithoutUserMutation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("tenant:001:user:1"), []byte("u1")); err != nil {
		t.Fatalf("write user data: %v", err)
	}

	indexKey, err := collections.SystemIndexKey("users", "by_name")
	if err != nil {
		t.Fatalf("system index key: %v", err)
	}
	indexValue := []byte("index:users:name")
	writeSystemTreeEntryForTest(t, db, indexKey, indexValue)

	prefixKey, err := collections.SystemCollectionPrefix("users")
	if err != nil {
		t.Fatalf("system collection prefix: %v", err)
	}
	prefixValue := []byte("prefix")
	writeSystemTreeEntryForTest(t, db, prefixKey, prefixValue)

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	v, err := reopen.Get([]byte("tenant:001:user:1"))
	if err != nil {
		t.Fatalf("user get after reopen: %v", err)
	}
	if !bytes.Equal(v, []byte("u1")) {
		t.Fatalf("unexpected user value: %q", v)
	}

	if got := getSystemTreeValue(t, reopen, indexKey); !bytes.Equal(got, indexValue) {
		t.Fatalf("index system value changed: got=%q want=%q", got, indexValue)
	}
	if got := getSystemTreeValue(t, reopen, prefixKey); !bytes.Equal(got, prefixValue) {
		t.Fatalf("prefix system value changed: got=%q want=%q", got, prefixValue)
	}
}

func TestNoopUpgradePreservesSystemRootID(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	state0 := db.State()
	if state0 == nil {
		t.Fatal("state nil")
	}
	rootBefore := state0.SystemRootPageID

	if err := db.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("write sync: %v", err)
	}

	state1 := db.State()
	if state1 == nil {
		t.Fatal("missing state after write")
	}
	if state1.SystemRootPageID != rootBefore {
		t.Fatalf("unexpected system root change on user-only write: before=%d after=%d", rootBefore, state1.SystemRootPageID)
	}
}

func TestSystemBatchWrite_DoesNotReplaceUserRoot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("user:k"), []byte("user:v")); err != nil {
		t.Fatalf("write user key: %v", err)
	}

	state0 := db.State()
	if state0 == nil {
		t.Fatal("missing initial state")
	}
	userRootBefore := state0.RootPageID
	systemRootBefore := state0.SystemRootPageID

	sysBatch := db.NewSystemBatch()
	if sysBatch == nil {
		t.Fatal("expected system batch")
	}
	defer sysBatch.Close()

	key, err := collections.SystemCollectionMetaKey("users")
	if err != nil {
		t.Fatalf("system key: %v", err)
	}
	if err := sysBatch.Set(key, []byte("meta")); err != nil {
		t.Fatalf("system batch set: %v", err)
	}
	if err := sysBatch.Write(); err != nil {
		t.Fatalf("system batch write: %v", err)
	}

	state1 := db.State()
	if state1 == nil {
		t.Fatal("missing state after system write")
	}
	if state1.RootPageID != userRootBefore {
		t.Fatalf("system batch replaced user root: before=%d after=%d", userRootBefore, state1.RootPageID)
	}
	if state1.SystemRootPageID == systemRootBefore {
		t.Fatalf("expected system batch to advance system root")
	}

	got, err := db.Get([]byte("user:k"))
	if err != nil {
		t.Fatalf("get user key: %v", err)
	}
	if !bytes.Equal(got, []byte("user:v")) {
		t.Fatalf("unexpected user value: got=%q want=%q", got, []byte("user:v"))
	}
}
