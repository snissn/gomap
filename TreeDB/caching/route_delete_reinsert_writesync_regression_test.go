package caching_test

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func assertSingleKeyParity(t *testing.T, db *treedb.DB, key, want []byte) {
	t.Helper()

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("get mismatch key=%q got=%x want=%x", key, got, want)
	}

	has, err := db.Has(key)
	if err != nil {
		t.Fatalf("has %q: %v", key, err)
	}
	if !has {
		t.Fatalf("has(%q)=false want true", key)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	if !it.Valid() {
		_ = it.Close()
		t.Fatalf("iterator empty; want key %q", key)
	}
	if !bytes.Equal(it.Key(), key) || !bytes.Equal(it.Value(), want) {
		_ = it.Close()
		t.Fatalf("iterator mismatch got key=%q val=%x want key=%q val=%x", it.Key(), it.Value(), key, want)
	}
	it.Next()
	if it.Valid() {
		k := append([]byte(nil), it.Key()...)
		_ = it.Close()
		t.Fatalf("iterator extra key=%q", k)
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		t.Fatalf("iterator error: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	rit, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	if !rit.Valid() {
		_ = rit.Close()
		t.Fatalf("reverse iterator empty; want key %q", key)
	}
	if !bytes.Equal(rit.Key(), key) || !bytes.Equal(rit.Value(), want) {
		_ = rit.Close()
		t.Fatalf("reverse iterator mismatch got key=%q val=%x want key=%q val=%x", rit.Key(), rit.Value(), key, want)
	}
	rit.Next()
	if rit.Valid() {
		k := append([]byte(nil), rit.Key()...)
		_ = rit.Close()
		t.Fatalf("reverse iterator extra key=%q", k)
	}
	if err := rit.Error(); err != nil {
		_ = rit.Close()
		t.Fatalf("reverse iterator error: %v", err)
	}
	if err := rit.Close(); err != nil {
		t.Fatalf("reverse iterator close: %v", err)
	}
}

// Regression: in v1_leaflog_route with separate Batch.WriteSync calls, deleting
// and then reinserting the same key must not drop the reinserted value.
func TestRegression_RouteMode_DeleteReinsertSeparateWriteSyncVisibleAfterReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	// Force pointer eligibility so deferred route materialization is exercised.
	opts.ValueLog.PointerThreshold = 1
	if opts.MemtableMode == "" || opts.MemtableMode == "adaptive" {
		opts.MemtableMode = "adaptive:hash_sorted"
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	key := []byte("k050")
	v1 := bytes.Repeat([]byte{0xAA}, 64)
	v2 := bytes.Repeat([]byte{0xBB}, 64)

	set1 := db.NewBatch()
	if err := set1.Set(key, v1); err != nil {
		_ = set1.Close()
		t.Fatalf("set1: %v", err)
	}
	if err := set1.WriteSync(); err != nil {
		_ = set1.Close()
		t.Fatalf("set1 writesync: %v", err)
	}
	if err := set1.Close(); err != nil {
		t.Fatalf("set1 close: %v", err)
	}

	del := db.NewBatch()
	if err := del.Delete(key); err != nil {
		_ = del.Close()
		t.Fatalf("delete: %v", err)
	}
	if err := del.WriteSync(); err != nil {
		_ = del.Close()
		t.Fatalf("delete writesync: %v", err)
	}
	if err := del.Close(); err != nil {
		t.Fatalf("delete close: %v", err)
	}

	set2 := db.NewBatch()
	if err := set2.Set(key, v2); err != nil {
		_ = set2.Close()
		t.Fatalf("set2: %v", err)
	}
	if err := set2.WriteSync(); err != nil {
		_ = set2.Close()
		t.Fatalf("set2 writesync: %v", err)
	}
	if err := set2.Close(); err != nil {
		t.Fatalf("set2 close: %v", err)
	}

	assertSingleKeyParity(t, db, key, v2)

	if err := db.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	assertSingleKeyParity(t, db, key, v2)
}
