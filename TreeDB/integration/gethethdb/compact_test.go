package gethethdb

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func openCompactTestDBAt(t testing.TB, dir string) *Database {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	// Force non-empty values through TreeDB's persistent value log so adapter
	// compaction coverage includes value-log pointer preservation.
	opts.ValueLog.PointerThreshold = 1
	db, err := OpenWithOptions(opts)
	if err != nil {
		t.Fatalf("OpenWithOptions(%s): %v", dir, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func compactTestValue(i int) []byte {
	return []byte(fmt.Sprintf("value-%03d-abcdefghijklmnopqrstuvwxyz", i))
}

func compactCommitSeq(t testing.TB, db *Database) uint64 {
	t.Helper()
	stats := db.TreeDB().Stats()
	raw, ok := stats["treedb.commit_seq"]
	if !ok {
		t.Fatalf("treedb.commit_seq missing from stats")
	}
	seq, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse treedb.commit_seq=%q: %v", raw, err)
	}
	return seq
}

func TestCompactWholeDBCommandWALPreservesDataAfterReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "treedb")
	db := openCompactTestDBAt(t, dir)

	for i := 0; i < 64; i++ {
		key := []byte(fmt.Sprintf("compact/live/%03d", i))
		if err := db.Put(key, compactTestValue(i)); err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
	}
	for i := 0; i < 16; i++ {
		key := []byte(fmt.Sprintf("compact/live/%03d", i))
		if err := db.Delete(key); err != nil {
			t.Fatalf("Delete %q: %v", key, err)
		}
	}

	if err := db.Compact(nil, nil); err != nil {
		t.Fatalf("Compact(nil, nil): %v", err)
	}
	for i := 0; i < 64; i++ {
		key := []byte(fmt.Sprintf("compact/live/%03d", i))
		if i < 16 {
			assertMissing(t, db, key)
			continue
		}
		assertValue(t, db, key, compactTestValue(i))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close after compact: %v", err)
	}

	reopened := openCompactTestDBAt(t, dir)
	for i := 0; i < 64; i++ {
		key := []byte(fmt.Sprintf("compact/live/%03d", i))
		if i < 16 {
			assertMissing(t, reopened, key)
			continue
		}
		assertValue(t, reopened, key, compactTestValue(i))
	}
}

func TestCompactBoundedRangeIsAdvisoryNoOp(t *testing.T) {
	db := openCompactTestDBAt(t, filepath.Join(t.TempDir(), "treedb"))
	before := compactCommitSeq(t, db)
	if err := db.Put([]byte("pending/bounded"), compactTestValue(1)); err != nil {
		t.Fatalf("Put pending: %v", err)
	}
	afterPut := compactCommitSeq(t, db)
	if afterPut != before {
		t.Fatalf("Put advanced commit seq from %d to %d; test requires pending cached write", before, afterPut)
	}

	if err := db.Compact([]byte{0x00}, []byte{0x10}); err != nil {
		t.Fatalf("bounded Compact: %v", err)
	}
	afterBounded := compactCommitSeq(t, db)
	if afterBounded != afterPut {
		t.Fatalf("bounded Compact advanced commit seq from %d to %d; want advisory no-op", afterPut, afterBounded)
	}
	assertValue(t, db, []byte("pending/bounded"), compactTestValue(1))
}

func TestCompactGethRangeSweepsOnlyCompactAtNilLimitTail(t *testing.T) {
	db := openCompactTestDBAt(t, filepath.Join(t.TempDir(), "treedb"))
	compactions := 0
	db.compactStorage = func(ctx context.Context, tdb *treedb.DB) error {
		compactions++
		return tdb.Checkpoint()
	}

	if err := db.Put([]byte("pending/range16"), compactTestValue(16)); err != nil {
		t.Fatalf("Put range16 pending: %v", err)
	}
	before16 := compactCommitSeq(t, db)
	for b := 0x00; b <= 0xe0; b += 0x10 {
		start := []byte{byte(b)}
		limit := []byte{byte(b + 0x10)}
		if err := db.Compact(start, limit); err != nil {
			t.Fatalf("16-range bounded Compact %#x-%#x: %v", start, limit, err)
		}
		if got := compactCommitSeq(t, db); got != before16 {
			t.Fatalf("16-range bounded Compact %#x-%#x advanced commit seq from %d to %d", start, limit, before16, got)
		}
	}
	if compactions != 0 {
		t.Fatalf("16-range bounded compactions=%d want 0", compactions)
	}
	if err := db.Compact([]byte{0xf0}, nil); err != nil {
		t.Fatalf("16-range tail Compact: %v", err)
	}
	if compactions != 1 {
		t.Fatalf("16-range tail compactions=%d want 1", compactions)
	}
	if got := compactCommitSeq(t, db); got <= before16 {
		t.Fatalf("16-range tail Compact did not run full storage compaction/checkpoint: before=%d after=%d", before16, got)
	}
	assertValue(t, db, []byte("pending/range16"), compactTestValue(16))

	if err := db.Put([]byte("pending/range256"), compactTestValue(255)); err != nil {
		t.Fatalf("Put range256 pending: %v", err)
	}
	before256 := compactCommitSeq(t, db)
	for b := 0; b < 255; b++ {
		start := []byte{byte(b)}
		limit := []byte{byte(b + 1)}
		if err := db.Compact(start, limit); err != nil {
			t.Fatalf("256-range bounded Compact %#x-%#x: %v", start, limit, err)
		}
		if got := compactCommitSeq(t, db); got != before256 {
			t.Fatalf("256-range bounded Compact %#x-%#x advanced commit seq from %d to %d", start, limit, before256, got)
		}
	}
	if compactions != 1 {
		t.Fatalf("256-range bounded compactions=%d want 1", compactions)
	}
	if err := db.Compact([]byte{0xff}, nil); err != nil {
		t.Fatalf("256-range tail Compact: %v", err)
	}
	if compactions != 2 {
		t.Fatalf("256-range tail compactions=%d want 2", compactions)
	}
	if got := compactCommitSeq(t, db); got <= before256 {
		t.Fatalf("256-range tail Compact did not run full storage compaction/checkpoint: before=%d after=%d", before256, got)
	}
	assertValue(t, db, []byte("pending/range256"), compactTestValue(255))
}
