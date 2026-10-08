package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Three equal-work public lifecycle epochs. The driver must bind fixedCalls to
// a completed baseline discovery before any candidate is measured. This test
// records evidence; the finite and storage attribution gates remain external.
func TestR1LeafDefaultFixedEpochs5098(t *testing.T) {
	out := os.Getenv("GOMAP_R1_S_SUPPORT_OUT")
	if out == "" {
		t.Skip("opt-in source-bound public producer lifecycle packet")
	}
	fixedCalls, parseErr := strconv.Atoi(os.Getenv("GOMAP_R1_S_FIXED_EPOCH_CALLS"))
	if parseErr != nil || fixedCalls < 2048 || fixedCalls > 131072 || fixedCalls%2048 != 0 {
		t.Fatal("fixed epoch calls must come from the accepted bounded baseline discovery")
	}
	role := os.Getenv("GOMAP_R1_S_ARM_ROLE")
	if role != "O" && role != "C" && role != "F" {
		t.Fatal("explicit source-bound O/C/F role required")
	}
	rolloverQualifiedAllEpochs := true
	currentEpoch := 0
	for _, key := range []string{"TREEDB_VLOG_GENERATION_LEAF_SEGMENT_TARGET_BYTES", "TREEDB_VLOG_GENERATION_HOT_SEGMENT_TARGET_BYTES", "TREEDB_VLOG_GENERATION_WARM_SEGMENT_TARGET_BYTES", "TREEDB_VLOG_GENERATION_COLD_SEGMENT_TARGET_BYTES"} {
		if value := os.Getenv(key); value != "" {
			t.Fatalf("default producer probe refuses override %s=%s", key, value)
		}
	}
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(out, "stages.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	emit := func(stage string, data any, err error, elapsed time.Duration, before, after runtime.MemStats) {
		event := map[string]any{"epoch": currentEpoch, "stage": stage, "at": time.Now().UTC().Format(time.RFC3339Nano), "data": data, "elapsed_ns": elapsed.Nanoseconds(), "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "scope": "diagnostic inclusive update blocks; update fixture preparation is timed, lifecycle oracles/census are outside operation timers"}
		if err != nil {
			event["error"] = err.Error()
		}
		if e := json.NewEncoder(f).Encode(event); e != nil {
			t.Fatal(e)
		}
		if e := f.Sync(); e != nil {
			t.Fatal(e)
		}
	}
	run := func(stage string, fn func() (any, error)) {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		data, e := fn()
		elapsed := time.Since(started)
		runtime.ReadMemStats(&after)
		emit(stage, data, e, elapsed, before, after)
		if e != nil {
			t.Fatalf("%s: %v", stage, e)
		}
	}
	tree, err := openR1Tree(r1Config{Durability: "durable"}, "typed-row")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if t.Failed() {
			// Keep the attributable DB on a failed discriminator for diagnosis.
			if tree.cleanup != nil {
				if e := tree.cleanup(); e != nil {
					t.Error(e)
				}
				tree.cleanup = nil
			}
			return
		}
		if e := tree.close(); e != nil {
			t.Error(e)
		}
	}()
	// Reopen the exact same public owner through its stats-returning wrapper.
	// That wrapper delegates to the same OpenBackendWithCachedLeafLog path;
	// no producer replacement or threshold adjustment is introduced.
	if err := tree.cleanup(); err != nil {
		t.Fatal(err)
	}
	tree.cleanup = nil
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, tree.dir)
	var cacheStats func() map[string]string
	tree.db, tree.cleanup, cacheStats, err = treedb.OpenBackendWithCachedLeafLogStats(opts)
	if err != nil {
		t.Fatal(err)
	}
	tree.manager = collections.NewCollectionManager(tree.db)
	tree.col, err = tree.manager.OpenCollection("r1")
	if err != nil {
		t.Fatal(err)
	}
	stats := cacheStats()
	if stats["treedb.cache.physical_vlog_writers.configured"] == "" {
		t.Fatal("shared all-physical value-log observer missing")
	}
	if stats["treedb.cache.vlog_generation.leaf.segment_target_bytes"] != "33554432" || stats["treedb.cache.vlog_generation.hot.segment_target_bytes"] != "268435456" {
		t.Fatalf("default producer contract changed: leaf=%q hot=%q", stats["treedb.cache.vlog_generation.leaf.segment_target_bytes"], stats["treedb.cache.vlog_generation.hot.segment_target_bytes"])
	}
	emit("producer", map[string]any{"opener": "OptionsFor(CommandWALDurable)+OpenBackendWithCachedLeafLog, reopened through Stats wrapper", "db_dir": tree.dir, "profile": tree.db.ResolvedProfile(), "durability": tree.db.DurabilityMode(), "cached_stats": stats, "exhaustive_owner": tree.db.CompactStorageLeafPageLogOwnerClassification(backenddb.CompactStorageLifecycleQuiescedMaintenance), "fixture_sha256": r1FixtureHash(r1Fixture(4096)), "population": 4096, "hot": 64, "batch": 32, "discovery_block_calls": 2048, "discovery_cap_calls": 131072, "source_role": "source-bound fixed-work lane", "arm_role": role, "fixed_calls_per_epoch": fixedCalls, "epochs": 3, "default_rollover_acceptance": false}, nil, 0, runtime.MemStats{}, runtime.MemStats{})
	rows := r1Fixture(4096)
	known := make(map[string]bool, len(rows)+192*32)
	for _, row := range rows {
		known[row.Email] = true
	}
	for start := 0; start < len(rows); start += 32 {
		if err := tree.insert(rows[start : start+32]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tree.transition("checkpointed"); err != nil {
		t.Fatal(err)
	}
	held, err := tree.openReader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if held != nil {
			if e := held.close(); e != nil {
				t.Error(e)
			}
		}
	}()
	old := append([]r1Document(nil), rows...)
	previousFiles := make(map[string]map[string]any)
	census := func(stage string) {
		files := make(map[string]map[string]any)
		manifestObservations := make(map[string]map[string]any)
		walFiles := make(map[string]map[string]any)
		var persistentBytes, walBytes, persistentAllocatedBytes, walAllocatedBytes int64
		if err := filepath.WalkDir(tree.dir, func(path string, entry os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if entry.IsDir() {
				return nil
			}
			info, e := entry.Info()
			if e != nil {
				return e
			}
			rel, e := filepath.Rel(tree.dir, path)
			if e != nil {
				return e
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			if strings.Contains(entry.Name(), "manifest") && strings.HasSuffix(entry.Name(), ".json") {
				raw, e := os.ReadFile(path)
				if e != nil {
					return e
				}
				var value any
				if e := json.Unmarshal(raw, &value); e != nil {
					return e
				}
				manifestObservations[rel] = map[string]any{"sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "observed_json": value, "deletion_authority": false}
			}
			if strings.Contains(path, string(filepath.Separator)+"wal"+string(filepath.Separator)) || strings.HasSuffix(path, "-wal") {
				stat, ok := info.Sys().(*syscall.Stat_t)
				if !ok {
					return fmt.Errorf("physical stat unavailable for %s", rel)
				}
				allocated := stat.Blocks * 512
				walFiles[rel] = map[string]any{"apparent_bytes": info.Size(), "allocated_bytes": allocated, "device": stat.Dev, "inode": stat.Ino}
				walBytes += info.Size()
				walAllocatedBytes += allocated
			} else {
				stat, ok := info.Sys().(*syscall.Stat_t)
				if !ok {
					return fmt.Errorf("physical stat unavailable for %s", rel)
				}
				allocated := stat.Blocks * 512
				files[rel] = map[string]any{"apparent_bytes": info.Size(), "allocated_bytes": allocated, "device": stat.Dev, "inode": stat.Ino}
				persistentBytes += info.Size()
				persistentAllocatedBytes += allocated
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		absent := make(map[string]map[string]any)
		for rel, old := range previousFiles {
			if _, exists := files[rel]; exists {
				continue
			}
			if _, e := os.Lstat(filepath.Join(tree.dir, rel)); !os.IsNotExist(e) {
				t.Fatalf("prior path absent from census but physical absence unproved %s: %v", rel, e)
			}
			absent[rel] = old
		}
		closure, e := tree.db.CaptureRecoverableRootSetForInspection(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		roots := closure.Roots()
		e = closure.Revalidate()
		closure.Release()
		if e != nil {
			t.Fatal(e)
		}
		previousFiles = files
		emit(stage, map[string]any{"observed_manifests": manifestObservations, "recoverable_roots_for_inspection": roots, "previous_paths_now_absent": absent, "physical_absence_is_not_deletion_authority": true, "durable_files_excluding_wal": files, "durable_bytes_excluding_wal": persistentBytes, "durable_allocated_bytes_excluding_wal": persistentAllocatedBytes, "wal_files": walFiles, "wal_bytes": walBytes, "wal_allocated_bytes": walAllocatedBytes, "backend_stats": tree.db.Stats(), "cached_stats": cacheStats()}, nil, 0, runtime.MemStats{}, runtime.MemStats{})
	}
	verify := func(reader r1Reader, want []r1Document) {
		for start := 0; start < len(want); start += 32 {
			end := start + 32
			if end > len(want) {
				end = len(want)
			}
			docs, _, e := reader.fetch(r1IDs(want[start:end]))
			if e != nil {
				t.Fatal(e)
			}
			if len(docs) != end-start {
				t.Fatal("complete-row count mismatch")
			}
			for i, raw := range docs {
				if e := r1VerifyDocument(raw, want[start+i]); e != nil {
					t.Fatal(e)
				}
			}
		}
		view := reader.(*r1TreeReader).view
		check := func(index, value string, expected []string) {
			var actual []string
			if err := view.VisitIndexValueIDs(index, value, func(id []byte) error { actual = append(actual, string(id)); return nil }); err != nil {
				t.Fatal(err)
			}
			sort.Strings(actual)
			sort.Strings(expected)
			if len(actual) != len(expected) || len(actual) > 0 && !reflect.DeepEqual(actual, expected) {
				t.Fatalf("%s=%q postings: got %v want %v", index, value, actual, expected)
			}
		}
		emails := make(map[string][]string, len(want))
		cities := make(map[string][]string, 8)
		for _, row := range want {
			emails[row.Email] = append(emails[row.Email], row.ID)
			cities[row.City] = append(cities[row.City], row.ID)
		}
		for value := range known {
			check("email", value, emails[value])
		}
		for city := range 8 {
			value := fmt.Sprintf("city-%02d", city)
			check("city", value, cities[value])
		}
	}
	oracle := func(stage string) {
		current, e := tree.openReader()
		if e != nil {
			t.Fatal(e)
		}
		var closeErr error
		func() { defer func() { closeErr = current.close() }(); verify(current, rows) }()
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if held != nil {
			verify(held, old)
		}
		emit(stage, map[string]any{"complete_current": true, "complete_held": held != nil, "current_and_historical_postings": true, "known_email_values": len(known)}, nil, 0, runtime.MemStats{}, runtime.MemStats{})
	}
	for currentEpoch = 0; currentEpoch < 3; currentEpoch++ {
		if currentEpoch > 0 {
			held, err = tree.openReader()
			if err != nil {
				t.Fatal(err)
			}
			old = append([]r1Document(nil), rows...)
		}
		census("loaded")
		oracle("loaded_oracle")
		// The physical set includes every worker used before or during discovery.
		loadedStats := cacheStats()
		physicalEpoch, physicalEpochErr := s5098NewPhysicalThresholdEpoch(loadedStats)
		if physicalEpochErr != nil {
			t.Fatal(physicalEpochErr)
		}
		thresholds := func(stats map[string]string) (map[string]uint64, bool) {
			counts, passed, err := physicalEpoch.observe(stats)
			if err != nil {
				t.Fatal(err)
			}
			return counts, passed
		}
		operationLatency := make([]int64, fixedCalls)
		calls := 0
		qualified := false
		for block := 0; block < fixedCalls/2048; block++ {
			run(fmt.Sprintf("discovery_block_%02d", block), func() (any, error) {
				for n := 0; n < 2048; n++ {
					call := calls
					start := (call % 2) * 32
					update := append([]r1Document(nil), rows[start:start+32]...)
					for i := range update {
						update[i].Email = fmt.Sprintf("s5098-e0-c%d-r%d@example.test", call%8, start+i)
						update[i].City = fmt.Sprintf("city-%02d", (start+i+call+1)%8)
						update[i].Bio = strings.Repeat(fmt.Sprintf("%02d", call%100), 48)
						update[i].Revision++
						known[update[i].Email] = true
					}
					operationStarted := time.Now()
					if e := tree.replace(update); e != nil {
						return map[string]any{"completed_calls": calls}, e
					}
					operationLatency[calls] = time.Since(operationStarted).Nanoseconds()
					copy(rows[start:start+32], update)
					calls++
				}
				return map[string]any{"completed_calls": calls, "batch": 32}, nil
			})
			stats := cacheStats()
			counts, passed := thresholds(stats)
			handoffDeltas := make(map[string]uint64)
			for prefix := range counts {
				key := prefix + ".maintenance_handoffs_total"
				current, e := strconv.ParseUint(stats[key], 10, 64)
				if e != nil {
					t.Fatal(e)
				}
				var initial uint64
				if raw := loadedStats[key]; raw != "" {
					initial, e = strconv.ParseUint(raw, 10, 64)
				}
				if e != nil {
					t.Fatal(e)
				}
				if current < initial {
					t.Fatal("handoff counter regressed")
				}
				handoffDeltas[prefix] = current - initial
			}
			emit("physical_rollover_progress", map[string]any{"completed_calls": calls, "threshold_delta_per_exercised_physical_worker": counts, "cached_stats": stats, "all_workers_two_threshold_rotations": passed, "no_explicit_maintenance": true, "observed_maintenance_handoffs_per_worker": handoffDeltas, "automatic_maintenance_policy": "unchanged defaults; no absence claim"}, nil, 0, runtime.MemStats{}, runtime.MemStats{})
			qualified = passed
		}
		orderedLatency := append([]int64(nil), operationLatency[:calls]...)
		sort.Slice(orderedLatency, func(i, j int) bool { return orderedLatency[i] < orderedLatency[j] })
		var median float64
		if calls%2 == 0 {
			median = float64(orderedLatency[calls/2-1])/2 + float64(orderedLatency[calls/2])/2
		} else {
			median = float64(orderedLatency[calls/2])
		}
		p95 := orderedLatency[(95*calls+99)/100-1]
		emit("public_update_latency", map[string]any{"calls": calls, "raw_operation_ns": operationLatency[:calls], "median_ns": median, "p95_ns": p95, "p95_definition": "nearest rank ceil(0.95*N)", "scope": "public ReplaceBatch only; prepared fixture, counters, oracle, census and sorting outside timer; identical two clock reads and preallocated latency store in every arm"}, nil, 0, runtime.MemStats{}, runtime.MemStats{})
		oracle("after_discovery_oracle")
		census("after_discovery")
		counts, _ := thresholds(cacheStats())
		emit("discovery_complete", map[string]any{"completed_calls": calls, "fixed_subsequent_calls_per_epoch": calls, "threshold_delta_per_exercised_physical_worker": counts, "rollover_discovery_qualified": qualified, "fixed_work_complete": calls == fixedCalls, "finite_admission": false, "performance_acceptance": false, "retention_plateau_acceptance": false}, nil, 0, runtime.MemStats{}, runtime.MemStats{})
		rolloverQualifiedAllEpochs = rolloverQualifiedAllEpochs && qualified
		if !qualified && role != "O" {
			t.Fatal("C/F fixed-work epoch failed two natural threshold rotations for every exercised physical producer")
		}
		if !qualified {
			emit("original_arm_unqualified_continue", map[string]any{"arm_role": role, "rollover_qualified": false, "full_work_and_lifecycle_required": true}, nil, 0, runtime.MemStats{}, runtime.MemStats{})
		}
		ctx := context.Background()
		run("discovery_checkpoint", func() (any, error) { return nil, tree.transition("checkpointed") })
		run("discovery_column_fold", func() (any, error) { return tree.col.ColumnStoreCompact(ctx, collections.ColumnStoreCompactOptions{}) })
		run("discovery_overlay_fold", func() (any, error) { return tree.col.CompactRootOverlays(ctx) })
		run("discovery_fold_checkpoint", func() (any, error) { return nil, tree.transition("checkpointed") })
		run("discovery_leaf_plan", func() (any, error) {
			return tree.db.LeafGenerationPlan(ctx, backenddb.LeafGenerationPlanOptions{Force: true})
		})
		run("discovery_leaf_pack", func() (any, error) {
			return tree.db.LeafGenerationPackFromPlan(ctx, backenddb.LeafGenerationPackFromPlanOptions{Force: true, Sync: true, MaxGenerations: 8})
		})
		oracle("discovery_packed_oracle")
		run("discovery_held_gc", func() (any, error) { return tree.db.LeafGenerationGC(ctx, backenddb.LeafGenerationGCOptions{}) })
		oracle("discovery_held_after_gc_oracle")
		census("discovery_held_after_gc")
		if err := held.close(); err != nil {
			t.Fatal(err)
		}
		held = nil
		run("discovery_fallback_refresh", func() (any, error) { return nil, tree.db.RefreshCommandWALCheckpointFallback() })
		run("discovery_drained_gc", func() (any, error) { return tree.db.LeafGenerationGC(ctx, backenddb.LeafGenerationGCOptions{}) })
		oracle("discovery_drained_oracle")
		census("discovery_drained")
		if err := tree.cleanup(); err != nil {
			t.Fatal(err)
		}
		tree.cleanup = nil
		tree.db, tree.cleanup, cacheStats, err = treedb.OpenBackendWithCachedLeafLogStats(opts)
		if err != nil {
			t.Fatal(err)
		}
		tree.manager = collections.NewCollectionManager(tree.db)
		tree.col, err = tree.manager.OpenCollection("r1")
		if err != nil {
			t.Fatal(err)
		}
		oracle("reopen_oracle")
		census("reopened")
		// Reopen is followed by the next equal-work mutation epoch before its
		// maintenance. No fresh post-reopen maintenance is inserted here.
		if currentEpoch < 2 {
			continue
		}
		run("reopened_fallback_refresh", func() (any, error) { return nil, tree.db.RefreshCommandWALCheckpointFallback() })
		run("reopened_leaf_plan", func() (any, error) {
			return tree.db.LeafGenerationPlan(ctx, backenddb.LeafGenerationPlanOptions{Force: true})
		})
		run("reopened_leaf_gc", func() (any, error) { return tree.db.LeafGenerationGC(ctx, backenddb.LeafGenerationGCOptions{}) })
		oracle("reopened_after_gc_oracle")
		census("reopened_after_gc")
	}
	emit("complete", map[string]any{"arm_role": role, "rollover_qualified_all_epochs": rolloverQualifiedAllEpochs, "fixed_epochs_completed": 3, "fixed_calls_per_epoch": fixedCalls, "post_reopen_mutation_epochs": 2, "default_rollover_acceptance": false, "finite_admission": false, "performance_acceptance": false, "retention_plateau_acceptance": false}, nil, 0, runtime.MemStats{}, runtime.MemStats{})
}
