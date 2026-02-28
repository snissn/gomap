package caching_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func routeParityWideKey(i int) string {
	return fmt.Sprintf("k%06d/%s", i, strings.Repeat("x", 96))
}

func routeParityWideVal(tag byte, i int) []byte {
	v := bytes.Repeat([]byte{tag}, 64)
	binary.BigEndian.PutUint32(v[len(v)-4:], uint32(i))
	return v
}

func assertRouteParityState(t *testing.T, db *treedb.DB, expected map[string][]byte) {
	t.Helper()

	keys := make([]string, 0, len(expected))
	for k := range expected {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		want := expected[k]
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get mismatch key=%q got=%x want=%x", k, got, want)
		}
		has, err := db.Has([]byte(k))
		if err != nil {
			t.Fatalf("has %q: %v", k, err)
		}
		if !has {
			t.Fatalf("has(%q)=false want true", k)
		}
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	idx := 0
	for it.Valid() {
		if idx >= len(keys) {
			t.Fatalf("iterator extra key %q", it.Key())
		}
		gotKey := string(it.Key())
		wantKey := keys[idx]
		if gotKey != wantKey {
			t.Fatalf("iterator key[%d]=%q want %q", idx, gotKey, wantKey)
		}
		if !bytes.Equal(it.Value(), expected[wantKey]) {
			t.Fatalf("iterator value mismatch key=%q", wantKey)
		}
		idx++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	if idx != len(keys) {
		t.Fatalf("iterator count=%d want=%d", idx, len(keys))
	}

	rit, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	idx = len(keys) - 1
	for rit.Valid() {
		if idx < 0 {
			t.Fatalf("reverse iterator extra key %q", rit.Key())
		}
		gotKey := string(rit.Key())
		wantKey := keys[idx]
		if gotKey != wantKey {
			t.Fatalf("reverse iterator key[%d]=%q want %q", idx, gotKey, wantKey)
		}
		if !bytes.Equal(rit.Value(), expected[wantKey]) {
			t.Fatalf("reverse iterator value mismatch key=%q", wantKey)
		}
		idx--
		rit.Next()
	}
	if err := rit.Error(); err != nil {
		t.Fatalf("reverse iterator error: %v", err)
	}
	if err := rit.Close(); err != nil {
		t.Fatalf("reverse iterator close: %v", err)
	}
	if idx != -1 {
		t.Fatalf("reverse iterator count mismatch, idx=%d", idx)
	}
}

// Regression: route mode read APIs must stay in parity after write churn
// (split-prone seed + delete + append) and after reopen.
func TestRegression_RouteMode_ReadParityAfterDeleteAppendReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 512
	opts.ValueLog.OuterLeafBlockTargetBytes = 512
	if opts.MemtableMode == "" || opts.MemtableMode == "adaptive" {
		opts.MemtableMode = "adaptive:hash_sorted"
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	flush := func(b treedb.Batch, label string) {
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("%s writesync: %v", label, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("%s close: %v", label, err)
		}
	}

	expected := make(map[string][]byte, 2600)

	seed := db.NewBatch()
	for i := 0; i < 2400; i++ {
		k := routeParityWideKey(i)
		v := routeParityWideVal('s', i)
		if err := seed.Set([]byte(k), v); err != nil {
			_ = seed.Close()
			t.Fatalf("seed set %q: %v", k, err)
		}
		expected[k] = append([]byte(nil), v...)
	}
	flush(seed, "seed")

	del := db.NewBatch()
	for i := 400; i < 800; i++ {
		k := routeParityWideKey(i)
		if err := del.Delete([]byte(k)); err != nil {
			_ = del.Close()
			t.Fatalf("delete %q: %v", k, err)
		}
		delete(expected, k)
	}
	flush(del, "delete")

	appendBatch := db.NewBatch()
	for i := 3000; i < 3300; i++ {
		k := routeParityWideKey(i)
		v := routeParityWideVal('n', i)
		if err := appendBatch.Set([]byte(k), v); err != nil {
			_ = appendBatch.Close()
			t.Fatalf("append %q: %v", k, err)
		}
		expected[k] = append([]byte(nil), v...)
	}
	flush(appendBatch, "append")

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	assertRouteParityState(t, db, expected)

	if err := db.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	assertRouteParityState(t, reopened, expected)
}
