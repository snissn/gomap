package caching_test

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Regression: strict route mode point lookups must remain exact even when a
// batch writes keys that have many persisted keys between them in sort order.
func TestRegression_RouteExactLookupWithPersistedGapAfterWriteSync(t *testing.T) {
	dir := t.TempDir()

	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	if opts.MemtableMode == "" || opts.MemtableMode == "adaptive" {
		opts.MemtableMode = "adaptive:hash_sorted"
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	seed := db.NewBatch()
	for i := 1000; i < 2000; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		val := bytes.Repeat([]byte{byte((i%251)+1)}, 96)
		if err := seed.Set(key, val); err != nil {
			_ = seed.Close()
			t.Fatalf("seed set %q: %v", key, err)
		}
	}
	if err := seed.WriteSync(); err != nil {
		_ = seed.Close()
		t.Fatalf("seed writesync: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	leftKey := []byte("k0900")
	rightKey := []byte("k2500")
	leftVal := []byte("left-value")
	rightVal := []byte("right-value")

	b := db.NewBatch()
	if err := b.Set(leftKey, leftVal); err != nil {
		_ = b.Close()
		t.Fatalf("left set: %v", err)
	}
	if err := b.Set(rightKey, rightVal); err != nil {
		_ = b.Close()
		t.Fatalf("right set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("writesync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	gotLeft, err := db.Get(leftKey)
	if err != nil {
		t.Fatalf("get left: %v", err)
	}
	if !bytes.Equal(gotLeft, leftVal) {
		t.Fatalf("left value mismatch: got=%q want=%q", gotLeft, leftVal)
	}
	gotRight, err := db.Get(rightKey)
	if err != nil {
		t.Fatalf("get right: %v", err)
	}
	if !bytes.Equal(gotRight, rightVal) {
		t.Fatalf("right value mismatch: got=%q want=%q", gotRight, rightVal)
	}

	hasLeft, err := db.Has(leftKey)
	if err != nil {
		t.Fatalf("has left: %v", err)
	}
	if !hasLeft {
		t.Fatalf("expected left key to exist")
	}
	hasRight, err := db.Has(rightKey)
	if err != nil {
		t.Fatalf("has right: %v", err)
	}
	if !hasRight {
		t.Fatalf("expected right key to exist")
	}
}

