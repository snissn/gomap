package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestCompactStorageExhaustiveDefaultWriteProducedLeafPointersSurviveReopenGC(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	ctx := context.Background()
	dir := t.TempDir()
	opts := compactPersistenceProofOptions(dir)

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open default-write compact fixture: %v", err)
	}
	expected, deleted := writeCompactPersistenceFixture(t, db)
	requireCompactPersistenceSupport(t, db)
	requireCompactPersistenceAdmissionMetadata(t, db)
	requireCompactPersistenceLeafLogRoot(t, db)
	assertCompactPersistenceValues(t, db, expected, deleted)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint default-write compact fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close default-write compact fixture: %v", err)
	}

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Reopen before compact: %v", err)
	}
	requireCompactPersistenceSupport(t, db)
	requireCompactPersistenceLeafLogRoot(t, db)
	assertCompactPersistenceValues(t, db, expected, deleted)

	compactStats, err := db.CompactStorage(ctx, CompactStorageOptions{
		Mode:                           CompactStorageExhaustive,
		SyncEachPhase:                  true,
		ValueLogRewriteBatchSize:       128,
		ValueLogRewriteMaxSegmentBytes: 64 << 10,
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("CompactStorage exhaustive on supported default-write fixture: %v", err)
	}
	for _, phase := range []string{
		"value-log-rewrite",
		"value-log-gc",
		"seal-current-leaf-generation",
		"leaf-generation-gc",
		"index-vacuum",
	} {
		if !compactStoragePhaseSeen(compactStats.Phases, phase) {
			_ = db.Close()
			t.Fatalf("CompactStorage exhaustive missing phase %q: %+v", phase, compactStats.Phases)
		}
	}
	requireCompactPersistenceLeafLogRoot(t, db)
	assertCompactPersistenceValues(t, db, expected, deleted)
	if err := db.Close(); err != nil {
		t.Fatalf("Close after compact: %v", err)
	}

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Reopen after compact: %v", err)
	}
	defer func() { _ = db.Close() }()
	requireCompactPersistenceSupport(t, db)
	requireCompactPersistenceLeafLogRoot(t, db)
	assertCompactPersistenceValues(t, db, expected, deleted)

	if _, err := db.ValueLogGC(ctx, ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC after compact/reopen: %v", err)
	}
	requireCompactPersistenceLeafLogRoot(t, db)
	assertCompactPersistenceValues(t, db, expected, deleted)

	if _, err := db.LeafGenerationGC(ctx, LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC after compact/reopen: %v", err)
	}
	requireCompactPersistenceLeafLogRoot(t, db)
	assertCompactPersistenceValues(t, db, expected, deleted)

	plan, err := db.CompactStoragePlan(ctx, CompactStorageOptions{Mode: CompactStorageExhaustive})
	if err != nil {
		t.Fatalf("CompactStoragePlan after compact/reopen/GC checks: %v", err)
	}
	if plan.Mode != CompactStorageExhaustive || !plan.DryRun {
		t.Fatalf("unexpected final compact plan mode/dry-run: mode=%q dryRun=%t", plan.Mode, plan.DryRun)
	}
}

func compactPersistenceProofOptions(dir string) Options {
	return Options{
		Dir:                        dir,
		CommandWAL:                 true,
		Durability:                 DurabilityWALOnRelaxed,
		DisableBackgroundPrune:     true,
		DisableSideStores:          true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		FlushAdmissionPolicy:       FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:      4,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushApplySpanNative:       true,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionOff,
		},
	}
}

func writeCompactPersistenceFixture(t *testing.T, db *DB) (map[string][]byte, [][]byte) {
	t.Helper()
	expected := make(map[string][]byte)
	deleted := make([][]byte, 0, 192)

	writeSetBatch := func(label string, start, count int, generation string) {
		t.Helper()
		b := db.NewBatch()
		for i := start; i < start+count; i++ {
			key := compactPersistenceKey(i)
			value := compactPersistenceValue(i, generation)
			if err := b.Set(key, value); err != nil {
				_ = b.Close()
				t.Fatalf("%s Set(%q): %v", label, key, err)
			}
			expected[string(key)] = append([]byte(nil), value...)
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("%s WriteSync: %v", label, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("%s Close: %v", label, err)
		}
	}

	writeDeleteBatch := func(label string, start, count int) {
		t.Helper()
		b := db.NewBatch()
		for i := start; i < start+count; i++ {
			key := compactPersistenceKey(i)
			if err := b.Delete(key); err != nil {
				_ = b.Close()
				t.Fatalf("%s Delete(%q): %v", label, key, err)
			}
			delete(expected, string(key))
			deleted = append(deleted, append([]byte(nil), key...))
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("%s WriteSync: %v", label, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("%s Close: %v", label, err)
		}
	}

	writeSetBatch("seed compact persistence fixture", 0, 4096, "seed")
	writeSetBatch("update compact persistence fixture", 0, 384, "update")
	writeDeleteBatch("delete compact persistence fixture", 768, 192)
	writeSetBatch("extend compact persistence fixture", 4096, 512, "extend")
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after default-write fixture: %v", err)
	}
	return expected, deleted
}

func compactPersistenceKey(i int) []byte {
	return []byte(fmt.Sprintf("compact-persist/key-%06d", i))
}

func compactPersistenceValue(i int, generation string) []byte {
	payload := fmt.Sprintf("compact-persist/value/%s/%06d/", generation, i)
	return bytes.Repeat([]byte(payload), 4)
}

func requireCompactPersistenceSupport(t *testing.T, db *DB) {
	t.Helper()
	if db.leafPageLog == nil {
		t.Fatal("supported compact fixture opened without a leaf-page log")
	}
	classification := compactStorageClassifyLeafPageLogOwner(db.leafPageLog, CompactStorageLifecycleExclusiveMaintenance)
	if classification.OwnerClass != CompactStorageLeafPageLogOwnerInternalHiddenByWrapper ||
		classification.Status != CompactStorageOwnerStatusSupportedTarget ||
		!classification.Replaceable ||
		classification.RequiresQuiescence {
		t.Fatalf("unsupported compact fixture owner classification: %+v", classification)
	}
}

func requireCompactPersistenceAdmissionMetadata(t *testing.T, db *DB) {
	t.Helper()
	stats := db.Stats()
	for key, want := range map[string]string{
		"treedb.flush_admission.policy":                             FlushAdmissionPolicyExplicit.String(),
		"treedb.flush_admission.admitted":                           "true",
		"treedb.flush_admission.flush_apply_concurrency_configured": "4",
		"treedb.flush_admission.flush_apply_concurrency":            fmt.Sprintf("%d", compactPersistenceExpectedEffectiveConcurrency(4)),
		"treedb.flush_admission.flush_apply_concurrency_defaulted":  "false",
		"treedb.flush_admission.flush_apply_span_native":            "true",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("admission metadata %s=%q want %q (stats=%+v)", key, got, want, stats)
		}
	}
	if got := requireDBStatUint64(t, db, "treedb.flush_apply.span_native.used_ops_total"); got == 0 {
		t.Fatalf("default-write fixture did not use span-native apply: used_ops_total=%d", got)
	}
}

func compactPersistenceExpectedEffectiveConcurrency(configured int) int {
	if configured <= 1 {
		return 0
	}
	gomax := runtime.GOMAXPROCS(0)
	if gomax < 1 {
		gomax = 1
	}
	if configured > gomax {
		configured = gomax
	}
	if configured <= 1 {
		return 0
	}
	return configured
}

func requireCompactPersistenceLeafLogRoot(t *testing.T, db *DB) []page.LeafLogPtr {
	t.Helper()
	state := db.State()
	if state == nil || state.RootPageID == 0 {
		t.Fatalf("missing user root state for compact persistence fixture: %#v", state)
	}
	ptrs := requireLeafLogRootChildren(t, db, state.RootPageID)
	for _, ptr := range ptrs {
		if ptr.FileID == 0 {
			t.Fatalf("leaf-log child has zero file id: %+v", ptr)
		}
		path := leafLogSegmentPath(t, db.dir, ptr.FileID)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("leaf-log child segment missing for ptr %+v at %s: %v", ptr, path, err)
		}
	}
	return ptrs
}

func assertCompactPersistenceValues(t *testing.T, db *DB, expected map[string][]byte, deleted [][]byte) {
	t.Helper()
	for rawKey, want := range expected {
		key := []byte(rawKey)
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) mismatch: got len=%d want len=%d", key, len(got), len(want))
		}
	}
	for _, key := range deleted {
		got, err := db.Get(key)
		if errors.Is(err, tree.ErrKeyNotFound) || (err == nil && len(got) == 0) {
			continue
		}
		t.Fatalf("Get(%q) after delete got value len=%d err=%v, want no readable value", key, len(got), err)
	}
}
