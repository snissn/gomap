package treedb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// One cell per fresh process. GC samples are diagnostics outside throughput
// phases. Cache capacities describe the main DB, not equal physical RAM.
func BenchmarkMemoryBudgetWorkflow(b *testing.B) {
	if b.N != 1 {
		b.Fatal("requires one fixed workflow: -benchtime=1x")
	}
	keys, updates := 250000, 40000
	pilot := os.Getenv("TREEDB_MEMORY_PILOT") == "1"
	if pilot {
		keys, updates = 8192, 8000
	}
	leafMiB := memoryBudgetEnv(b, "TREEDB_MEMORY_LEAF_MIB", 0, 16, 32, 48, 64)
	valueBytes := memoryBudgetEnv(b, "TREEDB_MEMORY_VALUE_BYTES", 256, 4096)
	threshold := memoryBudgetEnv(b, "TREEDB_MEMORY_POINTER_THRESHOLD", 1, 1024)
	binaryHash := memoryBudgetBinaryHash(b)
	environment := os.Environ()
	sort.Strings(environment)
	opts := memoryBudgetOptions(b.TempDir(), leafMiB, threshold)
	d, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if d != nil {
			if err := d.Close(); err != nil {
				b.Error(err)
			}
		}
	}()
	stats := d.Stats()
	memoryBudgetCheckConfiguration(b, stats, leafMiB)
	packet := map[string]any{
		"schema": "memory-budget-v1", "pilot": pilot, "keys": keys, "updates": updates,
		"key_bytes": 32, "shared_prefix_bytes": 24, "value_bytes": valueBytes, "pointer_threshold": threshold,
		"main_leaf_budget_bytes": leafMiB << 20, "main_frame_budget_bytes": (64 - leafMiB) << 20,
		"combined_configured_main_budget_bytes": 64 << 20, "gc_is_diagnostic": true,
		"rss_available": runtime.GOOS == "linux" && memoryBudgetStat(b, stats, "treedb.process.memory.rss_bytes") > 0, "go_version": runtime.Version(),
		"goos": runtime.GOOS, "goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0),
		"gomemlimit": os.Getenv("GOMEMLIMIT"), "negative_filter_bytes": opts.NegativeLookupFilterBytes,
		"crc_disabled": false, "side_store_limits_changed": false,
		"background_maintenance_disabled": true, "batch_size": 1000, "update_stride": 7919,
		"binary_sha256": binaryHash, "process_id": os.Getpid(), "process_argv": os.Args,
		"environment_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(environment, "\x00")))),
		"initial_stats":      stats,
		"configuration": map[string]any{
			"command_wal": opts.CommandWAL, "command_wal_stats_scan": opts.CommandWALStatsScan,
			"keep_recent": opts.KeepRecent, "flush_threshold": opts.FlushThreshold,
			"outer_leaves_in_value_log": opts.IndexOuterLeavesInValueLog, "leaf_prefix_compression": opts.LeafPrefixCompression,
			"columnar_leaves": opts.IndexColumnarLeaves, "packed_value_ptr": opts.IndexPackedValuePtr,
			"leaf_cache_entries":                  opts.LeafPageReadCacheEntries,
			"background_checkpoint_interval":      int64(opts.BackgroundCheckpointInterval),
			"background_checkpoint_idle_duration": int64(opts.BackgroundCheckpointIdleDuration),
			"max_wal_bytes":                       opts.MaxWALBytes, "background_index_vacuum_interval": int64(opts.BackgroundIndexVacuumInterval),
			"disable_background_prune": opts.DisableBackgroundPrune,
		},
	}
	phases := make([]memoryBudgetPhase, 0, 11)
	measure := func(name string, operations int, run func()) {
		beforeStats := d.Stats()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		run()
		elapsed := time.Since(start).Nanoseconds()
		runtime.ReadMemStats(&after)
		phases = append(phases, memoryBudgetPhase{
			Name: name, Operations: operations, ElapsedNS: elapsed,
			AllocatedBytes: after.TotalAlloc - before.TotalAlloc, Allocations: after.Mallocs - before.Mallocs,
			HeapBytes: after.HeapAlloc, BeforeStats: beforeStats, Stats: d.Stats(),
			Files: memoryBudgetFiles(b, opts.Dir),
		})
	}
	// Generate only one batch's values at a time; the caller does not keep a
	// second complete dataset live while measuring engine retention.
	measure("load_sync", keys, func() { memoryBudgetWrite(b, d, keys, valueBytes, 0, keys) })
	measure("checkpoint", 1, func() {
		if err := d.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	})
	pointerEntries := memoryBudgetPlacement(b, d, keys)
	wantPointers := threshold == 1 || valueBytes == 4096
	if wantPointers && pointerEntries != keys || !wantPointers && pointerEntries != 0 {
		b.Fatal("unexpected persisted placement", pointerEntries, keys, threshold, valueBytes)
	}
	packet["verified_pointer_entries"] = pointerEntries
	for _, name := range []string{"owned_get_sweep", "reused_append_sweep"} {
		measure(name, keys, func() { memoryBudgetRead(b, d, keys, valueBytes, 0, name == "reused_append_sweep", false) })
	}
	for _, name := range []string{"read_gc1", "read_gc2"} {
		runtime.GC()
		var sample runtime.MemStats
		runtime.ReadMemStats(&sample)
		phases = append(phases, memoryBudgetPhase{Name: name, HeapBytes: sample.HeapAlloc, Stats: d.Stats(), Files: memoryBudgetFiles(b, opts.Dir)})
	}
	measure("updates_sync", updates, func() { memoryBudgetWrite(b, d, keys, valueBytes, 1, updates) })
	measure("update_checkpoint", 1, func() {
		if err := d.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	})
	closure := d.Stats()
	if memoryBudgetStat(b, closure, "treedb.command_wal.applied_lsn") < memoryBudgetStat(b, closure, "treedb.command_wal.live_accepted_max_lsn") {
		b.Fatal("checkpoint does not cover acknowledged LSN")
	}
	measure("verify_before_close", keys*2, func() { memoryBudgetRead(b, d, keys, valueBytes, updates, true, true) })
	if err := d.Close(); err != nil {
		b.Fatal(err)
	}
	d = nil
	d, err = Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	memoryBudgetCheckConfiguration(b, d.Stats(), leafMiB)
	measure("verify_reopen", keys*2, func() { memoryBudgetRead(b, d, keys, valueBytes, updates, true, true) })
	packet["closure_stats"] = closure
	packet["reopen_stats"] = d.Stats()
	packet["phases"] = phases
	packet["all_values_and_interleaved_misses_verified"] = true
	if err := d.Close(); err != nil {
		b.Fatal(err)
	}
	d = nil
	packet["final_close_checked"] = true
	packet["closed_files"] = memoryBudgetFiles(b, opts.Dir)
	if memoryBudgetBinaryHash(b) != binaryHash {
		b.Fatal("benchmark executable changed during workflow")
	}
	encoded, err := json.Marshal(packet)
	if err != nil {
		b.Fatal(err)
	}
	// Separate from Go's stdout benchmark framing, including diagnostics.
	fmt.Fprintf(os.Stderr, "TREEDB_MEMORY_PACKET %s\n", encoded)
}

type memoryBudgetPhase struct {
	Name           string            `json:"name"`
	Operations     int               `json:"operations"`
	ElapsedNS      int64             `json:"elapsed_ns"`
	AllocatedBytes uint64            `json:"allocated_bytes"`
	Allocations    uint64            `json:"allocations"`
	HeapBytes      uint64            `json:"heap_bytes"`
	BeforeStats    map[string]string `json:"before_stats,omitempty"`
	Stats          map[string]string `json:"stats"`
	Files          map[string]int64  `json:"logical_file_bytes"`
}

func memoryBudgetBinaryHash(tb testing.TB) string {
	tb.Helper()
	path, err := os.Executable()
	if err != nil {
		tb.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	h := sha256.New()
	_, readErr := io.Copy(h, f)
	closeErr := f.Close()
	if readErr != nil || closeErr != nil {
		tb.Fatal(readErr, closeErr)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// File lengths are logical storage bytes, not allocated blocks or RAM. Keep
// filenames so WAL, persistent value/leaf logs, indexes and side stores cannot
// silently become one misleading storage number.
func memoryBudgetFiles(tb testing.TB, dir string) map[string]int64 {
	tb.Helper()
	files := make(map[string]int64)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unexpected nonregular storage file: %s", path)
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = info.Size()
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return files
}

func memoryBudgetEnv(tb testing.TB, name string, allowed ...int) int {
	tb.Helper()
	n, err := strconv.Atoi(os.Getenv(name))
	for _, valid := range allowed {
		if err == nil && n == valid {
			return n
		}
	}
	tb.Fatalf("%s must be one of %v", name, allowed)
	return 0
}

func memoryBudgetOptions(dir string, leafMiB, threshold int) Options {
	opts := Options{Dir: dir, CommandWAL: true, CommandWALStatsScan: true, KeepRecent: 10000,
		IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true, IndexColumnarLeaves: true,
		IndexPackedValuePtr: true, FlushThreshold: 64 << 20, LeafPageReadCacheEntries: -1,
		BackgroundCheckpointInterval: -1, BackgroundCheckpointIdleDuration: -1, MaxWALBytes: -1,
		BackgroundIndexVacuumInterval: -1, DisableBackgroundPrune: true}
	if leafMiB > 0 {
		opts.LeafPageReadCacheEntries = leafMiB << 20 / page.PageSize
	}
	opts.ValueLog.PointerThreshold = threshold
	return opts
}

func memoryBudgetCheckConfiguration(tb testing.TB, stats map[string]string, leafMiB int) {
	tb.Helper()
	if stats["treedb.vlog.read_integrity"] != "verify" || memoryBudgetStat(tb, stats, "treedb.negative_lookup_filter.active_bytes") != 0 {
		tb.Fatal("requires default checksums and negative filtering off")
	}
	if memoryBudgetStat(tb, stats, "treedb.process.read_path.outer_leaf.cache.capacity") != uint64(leafMiB<<20/page.PageSize) ||
		memoryBudgetStat(tb, stats, "treedb.vlog.grouped_frame_cache.budget_bytes") != uint64((64-leafMiB)<<20) {
		tb.Fatal("main cache budget does not match frozen overlay", leafMiB)
	}
	if leafMiB == 64 && memoryBudgetStat(tb, stats, "treedb.vlog.grouped_frame_cache.capacity") != 0 {
		tb.Fatal("zero-byte frame cohort must also disable entries")
	}
}

func memoryBudgetStat(tb testing.TB, stats map[string]string, name string) uint64 {
	tb.Helper()
	n, err := strconv.ParseUint(stats[name], 10, 64)
	if err != nil {
		tb.Fatalf("missing/invalid stat %s: %v", name, err)
	}
	return n
}

func memoryBudgetKey(id int, dst []byte) []byte {
	dst = dst[:32]
	copy(dst, "memory/shared/prefix/0000")
	binary.BigEndian.PutUint64(dst[24:], uint64(id))
	return dst
}

func memoryBudgetValue(id, generation int, dst []byte) []byte {
	if len(dst) == 256 {
		for i := range dst {
			dst[i] = 'v'
		}
	} else {
		x := uint64(id+1)*0x9e3779b97f4a7c15 + uint64(generation)
		for i := range dst {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			dst[i] = byte(x)
		}
	}
	binary.LittleEndian.PutUint64(dst, uint64(id))
	binary.LittleEndian.PutUint64(dst[8:], uint64(generation))
	return dst
}

func memoryBudgetWrite(tb testing.TB, d *DB, keys, valueBytes, generation, count int) {
	tb.Helper()
	for base := 0; base < count; base += 1000 {
		batch := d.NewBatchWithSize(1000)
		for i := base; i < min(base+1000, count); i++ {
			id := i
			if generation != 0 {
				id = i * 7919 % keys
			}
			if err := batch.Set(memoryBudgetKey(id*2, make([]byte, 32)), memoryBudgetValue(id, generation, make([]byte, valueBytes))); err != nil {
				_ = batch.Close()
				tb.Fatal(err)
			}
		}
		writeErr := batch.WriteSync()
		closeErr := batch.Close()
		if writeErr != nil || closeErr != nil {
			tb.Fatal(writeErr, closeErr)
		}
	}
}

func memoryBudgetRead(tb testing.TB, d *DB, keys, valueBytes, updates int, appendRead, checkMisses bool) {
	tb.Helper()
	// A tiny per-key expected-generation table avoids retaining payloads.
	var updated []bool
	if updates > 0 {
		updated = make([]bool, keys)
	}
	for i := range updates {
		updated[i*7919%keys] = true
	}
	key, expected, dst := make([]byte, 32), make([]byte, valueBytes), make([]byte, 0, valueBytes)
	for id := range keys {
		generation := 0
		if updates > 0 && updated[id] {
			generation = 1
		}
		memoryBudgetKey(id*2, key)
		var value []byte
		var err error
		if appendRead {
			value, err = d.GetAppend(key, dst[:0])
		} else {
			value, err = d.Get(key)
		}
		if checkErr := memoryBudgetValidate(value, memoryBudgetValue(id, generation, expected), err); checkErr != nil {
			tb.Fatalf("value %d generation %d: %v", id, generation, checkErr)
		}
		if checkMisses {
			value, err = d.Get(memoryBudgetKey(id*2+1, key))
			if checkErr := memoryBudgetValidate(value, nil, err); checkErr != nil {
				tb.Fatalf("interleaved miss %d: %v", id, checkErr)
			}
		}
	}
}

func memoryBudgetValidate(value, expected []byte, err error) error {
	if err != nil {
		return err
	}
	if expected == nil && value != nil || !bytes.Equal(value, expected) {
		return fmt.Errorf("full value/miss mismatch")
	}
	return nil
}

func memoryBudgetPlacement(tb testing.TB, d *DB, keys int) int {
	tb.Helper()
	snapshot := d.backend.AcquireSnapshot()
	if snapshot == nil {
		tb.Fatal("no checkpoint snapshot")
	}
	defer snapshot.Close()
	key := make([]byte, 32)
	pointers := 0
	for id := range keys {
		entry, err := snapshot.GetEntry(memoryBudgetKey(id*2, key))
		if err != nil || entry.Flags&node.FlagTombstone != 0 {
			tb.Fatalf("placement %d: %v", id, err)
		}
		if entry.Flags&node.FlagPointer != 0 {
			pointers++
		}
	}
	return pointers
}

func TestMemoryBudgetFixture(t *testing.T) {
	for _, valueBytes := range []int{256, 4096} {
		for _, threshold := range []int{1, 1024} {
			t.Run(fmt.Sprintf("bytes=%d/threshold=%d", valueBytes, threshold), func(t *testing.T) {
				opts := memoryBudgetOptions(t.TempDir(), 0, threshold)
				d, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if d != nil {
						if err := d.Close(); err != nil {
							t.Error(err)
						}
					}
				}()
				memoryBudgetWrite(t, d, 32, valueBytes, 0, 32)
				if err := d.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				pointers := memoryBudgetPlacement(t, d, 32)
				if pointers != 0 && pointers != 32 || (pointers == 32) != (threshold == 1 || valueBytes == 4096) {
					t.Fatal("unexpected placement", pointers)
				}
				memoryBudgetRead(t, d, 32, valueBytes, 0, false, true)
				memoryBudgetWrite(t, d, 32, valueBytes, 1, 16)
				if err := d.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				d = nil
				d, err = Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				memoryBudgetRead(t, d, 32, valueBytes, 16, true, true)
				want := memoryBudgetValue(0, 1, make([]byte, valueBytes))
				corrupt := bytes.Clone(want)
				corrupt[len(corrupt)-1] ^= 1
				if memoryBudgetValidate(corrupt, want, nil) == nil || memoryBudgetValidate(nil, want, nil) == nil || memoryBudgetValidate([]byte{}, nil, nil) == nil {
					t.Fatal("full-byte guard accepted corruption, absent hit or present miss")
				}
			})
		}
	}
}
