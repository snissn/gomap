package treedb

import (
	"fmt"
	"os"
	"testing"
)

const envLargeValueSweep = "TREEDB_TEST_LARGE_VALUE_SWEEP"

// TestLargeValueSweepCelestiaOptions is an opt-in diagnostic that finds the
// first value size that fails under Celestia-style TreeDB options.
//
// Enable with:
//
//	TREEDB_TEST_LARGE_VALUE_SWEEP=1 go test ./TreeDB -run TestLargeValueSweepCelestiaOptions -v
func TestLargeValueSweepCelestiaOptions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large value sweep in -short mode")
	}
	if os.Getenv(envLargeValueSweep) == "" {
		t.Skipf("set %s=1 to run large value sweep", envLargeValueSweep)
	}

	const (
		startSize = 1 << 10
		maxSize   = 256 << 20
	)

	opts := OptionsFor(ProfileWALOnFast, t.TempDir())
	opts.KeepRecent = 100000
	opts.IndexOuterLeafMode = IndexOuterLeafModeV1LeafLogRoute
	opts.DisableBackgroundPrune = true
	opts.BackgroundValueLogGCInterval = -1
	opts.BackgroundValueLogRewriteInterval = -1

	db := openSweepDB(t, opts)
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	key := []byte("large-value-sweep")

	for size := startSize; size <= maxSize; size <<= 1 {
		fill := byte((size / startSize) & 0xff)
		val := make([]byte, size)
		for i := range val {
			val[i] = fill
		}

		t.Logf("try size=%s", formatBytes(size))

		if err := db.SetSync(key, val); err != nil {
			t.Fatalf("first failure at size=%s: SetSync: %v", formatBytes(size), err)
		}

		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("first failure at size=%s: Get: %v", formatBytes(size), err)
		}
		verifyUniformValue(t, got, len(val), fill, size, "post-write")

		if err := db.Checkpoint(); err != nil {
			t.Fatalf("first failure at size=%s: Checkpoint: %v", formatBytes(size), err)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("first failure at size=%s: Close: %v", formatBytes(size), err)
		}
		db = nil

		db = openSweepDB(t, opts)
		got, err = db.Get(key)
		if err != nil {
			t.Fatalf("first failure at size=%s: Get after reopen: %v", formatBytes(size), err)
		}
		verifyUniformValue(t, got, len(val), fill, size, "post-reopen")
	}
}

func openSweepDB(t *testing.T, opts Options) *DB {
	t.Helper()
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func verifyUniformValue(t *testing.T, got []byte, wantLen int, wantFill byte, size int, stage string) {
	t.Helper()
	if len(got) != wantLen {
		t.Fatalf("%s size=%s: len=%d want=%d", stage, formatBytes(size), len(got), wantLen)
	}
	if len(got) == 0 {
		return
	}
	indices := []int{0, len(got) / 2, len(got) - 1}
	for _, idx := range indices {
		if got[idx] != wantFill {
			t.Fatalf("%s size=%s: got[%d]=%d want=%d", stage, formatBytes(size), idx, got[idx], wantFill)
		}
	}
}

func formatBytes(n int) string {
	const (
		kib = 1 << 10
		mib = 1 << 20
	)
	switch {
	case n >= mib:
		return fmt.Sprintf("%dMiB", n/mib)
	case n >= kib:
		return fmt.Sprintf("%dKiB", n/kib)
	default:
		return fmt.Sprintf("%dB", n)
	}
}
