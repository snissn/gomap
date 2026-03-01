package caching_test

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Regression for issue #657: RID allocation must remain globally monotonic
// across reopen/write sessions. Reusing RID=1 in a later value-log segment
// makes subsequent WAL/recovery open fail with "valuelog: duplicate rid 1".
func TestRegression_ReopenWriteSessions_DoNotReuseValueLogRID(t *testing.T) {
	dir := t.TempDir()

	openFast := func() *treedb.DB {
		db, err := treedb.Open(treedb.Options{
			Dir:                dir,
			Durability:         treedb.DurabilityWALOffRelaxed,
			ValueLog: treedb.ValueLogOptions{
				PointerThreshold: 1,
			},
		})
		if err != nil {
			t.Fatalf("open fast: %v", err)
		}
		return db
	}

	writeOne := func(db *treedb.DB, key string) {
		b := db.NewBatch()
		defer func() { _ = b.Close() }()
		if err := b.Set([]byte(key), bytes.Repeat([]byte("x"), 600)); err != nil {
			t.Fatalf("batch set %q: %v", key, err)
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("batch writesync %q: %v", key, err)
		}
	}

	// Session 1 writes pointer-backed value(s).
	db := openFast()
	writeOne(db, "k1")
	if err := db.Close(); err != nil {
		t.Fatalf("close session 1: %v", err)
	}

	// Session 2 writes again after reopen.
	db = openFast()
	writeOne(db, "k2")
	if err := db.Close(); err != nil {
		t.Fatalf("close session 2: %v", err)
	}

	// Correct behavior: DB should still open cleanly in default (WAL/recovery)
	// mode. Current buggy behavior fails with "valuelog: duplicate rid 1".
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after two sessions should succeed (RIDs must be unique across segments): %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, key := range []string{"k1", "k2"} {
		val, getErr := db.Get([]byte(key))
		if getErr != nil {
			t.Fatalf("get %q: %v", key, getErr)
		}
		if len(val) == 0 {
			t.Fatalf("expected value for key %q", key)
		}
		if !bytes.Equal(val, bytes.Repeat([]byte("x"), 600)) {
			t.Fatalf("value mismatch for key %q: got_len=%d", key, len(val))
		}
	}

	stats := db.Stats()
	if len(stats) == 0 {
		t.Fatalf("expected non-empty stats")
	}
}
