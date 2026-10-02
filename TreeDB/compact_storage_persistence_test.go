package treedb_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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

// Keep the public command-WAL/auto-compression path: lookup bytes alone are
// insufficient authority for packed leaves after the cached owner closes.
func TestCompactStorageExhaustiveCommandWALRandom4KOffline(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	for _, count := range []int{20000, 100000} {
		t.Run(fmt.Sprintf("keys%d", count), func(t *testing.T) {
			if testing.Short() && count == 100000 {
				t.Skip("original-sized maintenance fixture")
			}
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
			opts.FlushThreshold = 64 << 20
			opts.ChunkSize = 256 << 10
			opts.IndexOuterLeavesInValueLog = true
			opts.LeafPrefixCompression = true
			opts.IndexColumnarLeaves = true
			opts.IndexPackedValuePtr = true
			opts.LeafPageReadCacheWriteAdmission = treedb.LeafPageReadCacheWriteAdmissionAdaptive
			opts.ValueLog.Compression = treedb.ValueLogCompressionAuto
			t.Logf("durable command WAL: keys=%d, value=4096, batch=1000, updates=%d, compression=auto, outer-leaves=true, pointer-threshold=default(512)", count, count*2/5)
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if database != nil {
					if err := database.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			write := func(start, end int, update bool) {
				t.Helper()
				for start < end {
					batch := database.NewBatch()
					limit := min(start+1000, end)
					for j := start; j < limit; j++ {
						i, version := j, uint64(0)
						if update {
							i = (j * 7919) % count
							version = 1
						}
						id := uint64(i * 2)
						if err := batch.Set(compactRandom4KKey(id), compactRandom4KValue(id, version)); err != nil {
							t.Fatal(errors.Join(err, batch.Close()))
						}
					}
					if err := errors.Join(batch.WriteSync(), batch.Close()); err != nil {
						t.Fatal(err)
					}
					start = limit
					if update && start%(count/10) == 0 {
						if err := database.Checkpoint(); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			closePublic := func() {
				t.Helper()
				if err := errors.Join(database.Checkpoint(), database.Close()); err != nil {
					t.Fatal(err)
				}
				database = nil
			}
			write(0, count, false)
			closePublic()
			database, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			write(0, count*2/5, true)
			closePublic()
			// Reopen/close the cached owner before the exclusive maintenance handoff.
			database, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closePublic()
			verifyReadOnly := func() {
				t.Helper()
				database, err = treedb.Open(treedb.Options{Dir: dir, ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				changed := make([]bool, count)
				for j := 0; j < count*2/5; j++ {
					changed[(j*7919)%count] = true
				}
				for i := 0; i < count; i++ {
					id, version := uint64(i*2), uint64(0)
					if changed[i] {
						version = 1
					}
					got, err := database.Get(compactRandom4KKey(id))
					if err != nil || !bytes.Equal(got, compactRandom4KValue(id, version)) {
						t.Fatalf("read-only key=%d mismatch: %v", id, err)
					}
				}
				for i := 0; i < min(count, 10000); i++ {
					got, err := database.Get(compactRandom4KKey(uint64((i*7919%count)*2 + 1)))
					if err != nil || got != nil {
						t.Fatalf("read-only missing key %d: len=%d err=%v", i, len(got), err)
					}
				}
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database = nil
			}
			if count == 20000 {
				// An unresolved packed dependency fails after earlier phases have committed.
				// Cleanup must release owners so a read-only reopen and a fresh retry work.
				backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir})
				if err != nil {
					t.Fatal(err)
				}
				backend.SetStableDictionaryResourceProvider(nil)
				stats, compactErr := backend.CompactStorage(context.Background(), backenddb.CompactStorageOptions{Mode: backenddb.CompactStorageExhaustive, SyncEachPhase: true})
				closeErr := cleanup()
				if !errors.Is(compactErr, rootpublication.ErrUnresolvedResource) || closeErr != nil {
					t.Fatalf("partial compact: operation=%v cleanup=%v", compactErr, closeErr)
				}
				if !compactStoragePublicPhaseSeen(stats.Phases, "value-log-gc") {
					t.Fatal("failure did not follow committed maintenance phases")
				}
				verifyReadOnly()
			}
			for pass := 0; pass < 2; pass++ {
				backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				stats, compactErr := backend.CompactStorage(ctx, backenddb.CompactStorageOptions{Mode: backenddb.CompactStorageExhaustive, SyncEachPhase: true})
				if compactErr == nil {
					_, compactErr = backend.ValueLogGC(ctx, backenddb.ValueLogGCOptions{})
				}
				if compactErr == nil {
					_, compactErr = backend.LeafGenerationGC(ctx, backenddb.LeafGenerationGCOptions{})
				}
				cancel()
				err = errors.Join(compactErr, cleanup())
				if err != nil {
					t.Fatalf("offline compact/GC pass %d: %v", pass, err)
				}
				vacuumApplied := false
				for _, phase := range stats.Phases {
					if phase.Name == "index-vacuum" {
						vacuumApplied = phase.Status == backenddb.CompactStoragePhaseStatusSucceeded ||
							(pass > 0 && phase.Status == backenddb.CompactStoragePhaseStatusNotRequired)
					}
				}
				if !vacuumApplied || stats.RemainingDebt.IndexVacuumRequired {
					t.Fatalf("unfinished vacuum: phases=%+v debt=%+v", stats.Phases, stats.RemainingDebt)
				}
				if pass == 0 {
					ran := false
					for _, pack := range stats.LeafGenerationPacks {
						ran = ran || pack.Ran
					}
					if !ran {
						t.Fatal("fixture did not exercise leaf packing")
					}
				}
				if stats.ByteMinimized && (stats.RemainingDebt.ValueLogRewriteBytes != 0 || stats.RemainingDebt.LeafGCBytes != 0) {
					t.Fatalf("false byte-minimized claim: %+v", stats.RemainingDebt)
				}
				t.Logf("pass=%d byte-minimized=%t debt=%+v", pass, stats.ByteMinimized, stats.RemainingDebt)
				verifyReadOnly()
			}
		})
	}
}

func compactRandom4KKey(id uint64) []byte {
	key := make([]byte, 32)
	copy(key, "zone/settings/config/v1/")
	binary.BigEndian.PutUint64(key[24:], id)
	return key
}

func compactRandom4KValue(id, version uint64) []byte {
	value := make([]byte, 4096)
	binary.BigEndian.PutUint64(value, id)
	binary.BigEndian.PutUint64(value[8:], version)
	source := rand.New(rand.NewPCG(id+51, 99))
	for offset := 16; offset < len(value); offset += 8 {
		binary.LittleEndian.PutUint64(value[offset:], source.Uint64())
	}
	return value
}
