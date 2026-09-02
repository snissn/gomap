package db

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

func leafGenerationStatInt64(t *testing.T, stats map[string]string, key string) int64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stats key %q", key)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return v
}

func leafGenerationStatBool(t *testing.T, stats map[string]string, key string) bool {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stats key %q", key)
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return v
}

func TestStats_LeafGenerationLifecycle(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	manifestBefore := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}

	if err := leafLog.rotateLeaf(); err != nil {
		closeNoErr(t, snap)
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')

	stats := db.Stats()
	if !leafGenerationStatBool(t, stats, "treedb.leaf_generation.enabled") {
		t.Fatalf("expected leaf generation stats to be enabled")
	}
	if got, want := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.total"), int64(2); got != want {
		t.Fatalf("generations.total=%d, want %d", got, want)
	}
	if got, want := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.sealed"), int64(1); got != want {
		t.Fatalf("generations.sealed=%d, want %d", got, want)
	}
	if got, want := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.writable"), int64(1); got != want {
		t.Fatalf("generations.writable=%d, want %d", got, want)
	}
	if got, want := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.pinned"), int64(1); got != want {
		t.Fatalf("generations.pinned=%d, want %d", got, want)
	}
	if got, want := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.pins.total"), int64(1); got != want {
		t.Fatalf("pins.total=%d, want %d", got, want)
	}
	if got := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.bytes.total"); got <= 0 {
		t.Fatalf("bytes.total=%d, want > 0", got)
	}
	if got := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.plan_cache.subtree_pages"); got != 0 {
		t.Fatalf("plan_cache.subtree_pages before planning=%d, want 0", got)
	}
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		closeNoErr(t, snap)
		t.Fatalf("LeafGenerationPlan: %v", err)
	}
	stats = db.Stats()
	if got := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.plan_cache.subtree_pages"); got <= 0 {
		closeNoErr(t, snap)
		t.Fatalf("plan_cache.subtree_pages after planning=%d, want > 0", got)
	}

	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		closeNoErr(t, snap)
		t.Fatalf("LeafGenerationGC while pinned: %v", err)
	}
	stats = db.Stats()
	if got, want := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.retiring"), int64(1); got != want {
		closeNoErr(t, snap)
		t.Fatalf("generations.retiring=%d, want %d", got, want)
	}
	if got := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.sealed"); got != 0 {
		closeNoErr(t, snap)
		t.Fatalf("generations.sealed=%d, want 0 after retire classification", got)
	}
	if got, want := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.pinned"), int64(1); got != want {
		closeNoErr(t, snap)
		t.Fatalf("generations.pinned=%d, want %d", got, want)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if got := db.leafGenerationPinCountForTesting(gen1.GenerationID); got != 0 {
		t.Fatalf("pin count after close=%d, want 0", got)
	}
	advancePastRetainedDurableSlotForTest(t, db)
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC after close: %v", err)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
	stats = db.Stats()
	if got, want := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.total"), int64(1); got != want {
		t.Fatalf("generations.total=%d, want %d", got, want)
	}
	if got := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.retiring"); got != 0 {
		t.Fatalf("generations.retiring=%d, want 0", got)
	}
	if got := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.deleted"); got != 0 {
		t.Fatalf("generations.deleted=%d, want 0 after prune", got)
	}
	if got := leafGenerationStatInt64(t, stats, "treedb.leaf_generation.generations.pinned"); got != 0 {
		t.Fatalf("generations.pinned=%d, want 0", got)
	}
}
