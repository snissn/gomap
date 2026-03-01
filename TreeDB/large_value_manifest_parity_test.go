package treedb

import (
	"bytes"
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/limits"
)

func TestLargeValueManifestParity_WALOnFast(t *testing.T) {
	runLargeValueManifestParity(t, ProfileWALOnFast)
}

func TestLargeValueManifestParity_WALOffFast(t *testing.T) {
	runLargeValueManifestParity(t, ProfileFast)
}

func runLargeValueManifestParity(t *testing.T, profile Profile) {
	t.Helper()

	oldMax := limits.MaxRecordSize
	limits.MaxRecordSize = 2 << 20
	t.Cleanup(func() {
		limits.MaxRecordSize = oldMax
	})

	opts := OptionsFor(profile, t.TempDir())
	opts.IndexOuterLeafMode = IndexOuterLeafModeV1
	opts.DisableBackgroundPrune = true
	opts.BackgroundValueLogGCInterval = -1
	opts.BackgroundValueLogRewriteInterval = -1
	opts.KeepRecent = 100000

	key := []byte("k-large-manifest")
	value := bytes.Repeat([]byte("large-manifest|"), 400000) // ~5.3MiB, forces chunking with MaxRecordSize=2MiB.

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", profile, err)
	}
	defer func() { _ = db.Close() }()

	if err := db.SetSync(key, value); err != nil {
		t.Fatalf("SetSync(%s): %v", profile, err)
	}
	assertValueEqual(t, db, key, value, "post-write")

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(%s): %v", profile, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close before reopen(%s): %v", profile, err)
	}

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Reopen(%s): %v", profile, err)
	}
	assertValueEqual(t, db, key, value, "post-reopen")

	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC(%s): %v", profile, err)
	}
	assertValueEqual(t, db, key, value, "post-gc")

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     64,
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline(%s): %v", profile, err)
	}
	assertValueEqual(t, db, key, value, "post-rewrite")
}

func assertValueEqual(t *testing.T, db *DB, key, want []byte, stage string) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("%s: Get(%q): %v", stage, key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: value mismatch got=%d want=%d", stage, len(got), len(want))
	}
}
