package treedb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestCompactStorageExhaustiveDefaultWriteProducedValueLogPointersSurviveBackendReopenGCStatus(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	ctx := context.Background()
	dir := t.TempDir()
	opts := compactPersistencePublicOptions(dir)

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open public default-write fixture: %v", err)
	}
	expected, deleted := writeCompactPersistencePublicFixture(t, db)
	requireCompactPersistencePublicAdmissionMetadata(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("Close public default-write fixture: %v", err)
	}

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir, DisableSideStores: true})
	if err != nil {
		t.Fatalf("OpenBackend for default-write fixture: %v", err)
	}
	cleanupDone := false
	defer func() {
		if !cleanupDone {
			_ = cleanup()
		}
	}()
	requireCompactPersistenceBackendSupport(t, backend)
	pointerKey := []byte("compact-public/key-000001")
	requireCompactPersistenceBackendPointer(t, backend, pointerKey)
	assertCompactPersistenceBackendValues(t, backend, expected, deleted)

	compactStats, err := backend.CompactStorage(ctx, backenddb.CompactStorageOptions{
		Mode:                           backenddb.CompactStorageExhaustive,
		SyncEachPhase:                  true,
		ValueLogRewriteBatchSize:       128,
		ValueLogRewriteMaxSegmentBytes: 64 << 10,
	})
	if err != nil {
		t.Fatalf("CompactStorage exhaustive on backend-opened default-write fixture: %v", err)
	}
	for _, phase := range []string{
		"value-log-rewrite",
		"value-log-gc",
		"seal-current-leaf-generation",
		"leaf-generation-gc",
		"index-vacuum",
	} {
		if !compactStoragePublicPhaseSeen(compactStats.Phases, phase) {
			t.Fatalf("CompactStorage exhaustive missing phase %q: %+v", phase, compactStats.Phases)
		}
	}
	requireCompactPersistenceBackendPointer(t, backend, pointerKey)
	assertCompactPersistenceBackendValues(t, backend, expected, deleted)

	gcStats, err := backend.ValueLogGC(ctx, backenddb.ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC after backend compact: %v", err)
	}
	if gcStats.SegmentsReferenced == 0 {
		t.Fatalf("ValueLogGC after backend compact saw no referenced value-log segments: %+v", gcStats)
	}
	requireCompactPersistenceBackendPointer(t, backend, pointerKey)
	assertCompactPersistenceBackendValues(t, backend, expected, deleted)

	plan, err := backend.CompactStoragePlan(ctx, backenddb.CompactStorageOptions{Mode: backenddb.CompactStorageExhaustive})
	if err != nil {
		t.Fatalf("CompactStoragePlan after backend compact/GC checks: %v", err)
	}
	if plan.Mode != backenddb.CompactStorageExhaustive || !plan.DryRun {
		t.Fatalf("unexpected final backend compact plan mode/dry-run: mode=%q dryRun=%t", plan.Mode, plan.DryRun)
	}
	if plan.ValueLogRewritePlan.SegmentsTotal == 0 {
		t.Fatalf("CompactStoragePlan after backend compact/GC saw no value-log rewrite status: %+v", plan.ValueLogRewritePlan)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("Close backend maintenance fixture: %v", err)
	}
	cleanupDone = true

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Reopen public default-write fixture after backend maintenance: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	assertCompactPersistencePublicValues(t, reopened, expected, deleted)
}

func compactPersistencePublicOptions(dir string) treedb.Options {
	opts := treedb.Options{
		Dir:                        dir,
		FlushThreshold:             64 << 20,
		IndexOuterLeavesInValueLog: true,
	}
	opts.DisableBackgroundPrune = true
	opts.DisableSideStores = true
	opts.FlushAdmissionPolicy = treedb.FlushAdmissionPolicyExplicit
	opts.FlushApplyConcurrency = 4
	opts.FlushApplyMinEntries = 1
	opts.FlushApplyMinSpans = 1
	opts.FlushApplyMinBytes = 1
	opts.FlushApplySpanNative = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	return opts
}

func writeCompactPersistencePublicFixture(t *testing.T, db *treedb.DB) (map[string][]byte, [][]byte) {
	t.Helper()
	expected := make(map[string][]byte)
	deleted := make([][]byte, 0, 128)

	setRange := func(label string, start, count int, generation string) {
		t.Helper()
		for i := start; i < start+count; i++ {
			key := compactPersistencePublicKey(i)
			value := compactPersistencePublicValue(i, generation)
			if err := db.Set(key, value); err != nil {
				t.Fatalf("%s Set(%q): %v", label, key, err)
			}
			expected[string(key)] = append([]byte(nil), value...)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("%s Checkpoint: %v", label, err)
		}
	}

	deleteRange := func(label string, start, count int) {
		t.Helper()
		for i := start; i < start+count; i++ {
			key := compactPersistencePublicKey(i)
			if err := db.Delete(key); err != nil {
				t.Fatalf("%s Delete(%q): %v", label, key, err)
			}
			delete(expected, string(key))
			deleted = append(deleted, append([]byte(nil), key...))
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("%s Checkpoint: %v", label, err)
		}
	}

	setRange("seed public default-write fixture", 0, 2048, "seed")
	setRange("update public default-write fixture", 0, 256, "update")
	deleteRange("delete public default-write fixture", 512, 128)
	setRange("extend public default-write fixture", 2048, 256, "extend")
	return expected, deleted
}

func compactPersistencePublicKey(i int) []byte {
	return []byte(fmt.Sprintf("compact-public/key-%06d", i))
}

func compactPersistencePublicValue(i int, generation string) []byte {
	payload := fmt.Sprintf("compact-public/value/%s/%06d/", generation, i)
	return bytes.Repeat([]byte(payload), 32)
}

func requireCompactPersistencePublicAdmissionMetadata(t *testing.T, db *treedb.DB) {
	t.Helper()
	stats := db.Stats()
	for key, want := range map[string]string{
		"treedb.flush_admission.policy":                             treedb.FlushAdmissionPolicyExplicit.String(),
		"treedb.flush_admission.admitted":                           "true",
		"treedb.flush_admission.flush_apply_concurrency_configured": "4",
		"treedb.flush_admission.flush_apply_concurrency":            fmt.Sprintf("%d", compactPersistenceExpectedEffectiveConcurrency(4)),
		"treedb.flush_admission.flush_apply_concurrency_defaulted":  "false",
		"treedb.flush_admission.flush_apply_span_native":            "true",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("public admission metadata %s=%q want %q (stats=%+v)", key, got, want, stats)
		}
	}
	if got := requirePublicStatUint64(t, db, "treedb.flush_apply.span_native.used_ops_total"); got == 0 {
		t.Fatalf("public default-write fixture did not use span-native apply: used_ops_total=%d", got)
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

func requireCompactPersistenceBackendSupport(t *testing.T, backend *backenddb.DB) {
	t.Helper()
	classification := backend.CompactStorageLeafPageLogOwnerClassification(backenddb.CompactStorageLifecycleExclusiveMaintenance)
	if classification.Status != backenddb.CompactStorageOwnerStatusSupportedTarget || classification.RequiresQuiescence {
		t.Fatalf("backend-opened default-write fixture has unsupported compact owner classification: %+v", classification)
	}
}

func requireCompactPersistenceBackendPointer(t *testing.T, backend *backenddb.DB, key []byte) page.ValuePtr {
	t.Helper()
	it, err := backend.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("backend pointer projection iterator: %v", err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if !bytes.Equal(it.UnsafeKey(), key) {
			continue
		}
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 {
			t.Fatalf("backend key %q projection flags=%08b want value-log pointer", key, flags)
		}
		if ptr.FileID == 0 || ptr.Length == 0 {
			t.Fatalf("backend key %q invalid value-log pointer: %+v", key, ptr)
		}
		return ptr
	}
	if err := it.Error(); err != nil {
		t.Fatalf("backend pointer projection iterator error: %v", err)
	}
	t.Fatalf("backend pointer projection missing key %q", key)
	return page.ValuePtr{}
}

func assertCompactPersistenceBackendValues(t *testing.T, backend *backenddb.DB, expected map[string][]byte, deleted [][]byte) {
	t.Helper()
	for rawKey, want := range expected {
		key := []byte(rawKey)
		got, err := backend.Get(key)
		if err != nil {
			t.Fatalf("backend Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("backend Get(%q) mismatch: got len=%d want len=%d", key, len(got), len(want))
		}
	}
	for _, key := range deleted {
		got, err := backend.Get(key)
		if errors.Is(err, tree.ErrKeyNotFound) || (err == nil && len(got) == 0) {
			continue
		}
		t.Fatalf("backend Get(%q) after delete got value len=%d err=%v, want no readable value", key, len(got), err)
	}
}

func assertCompactPersistencePublicValues(t *testing.T, db *treedb.DB, expected map[string][]byte, deleted [][]byte) {
	t.Helper()
	for rawKey, want := range expected {
		key := []byte(rawKey)
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("public Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("public Get(%q) mismatch: got len=%d want len=%d", key, len(got), len(want))
		}
	}
	for _, key := range deleted {
		got, err := db.Get(key)
		if errors.Is(err, treedb.ErrKeyNotFound) || (err == nil && len(got) == 0) {
			continue
		}
		t.Fatalf("public Get(%q) after delete got value len=%d err=%v, want no readable value", key, len(got), err)
	}
}

func compactStoragePublicPhaseSeen(phases []backenddb.CompactStoragePhaseStats, name string) bool {
	for _, phase := range phases {
		if phase.Name == name {
			return true
		}
	}
	return false
}
