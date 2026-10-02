package treedb

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// This fixed-work public workflow reuses the memory/placement fixture. Read
// throughput includes CRC32 consumption; read latency covers the API request.
// Setup, full-byte proof, checkpoints and GC have separately named phases.
func BenchmarkQuicksilverWorkflow(b *testing.B) {
	if b.N != 1 {
		b.Fatal("fixed workflow requires -benchtime=1x")
	}
	pilot := os.Getenv("TREEDB_MEMORY_PILOT") == "1"
	keys, updates, reads := 250000, 40000, 256000
	if pilot {
		keys, updates, reads = 8192, 8000, 6400
	}
	leaf := memoryBudgetEnv(b, "TREEDB_MEMORY_LEAF_MIB", 0, 16, 32, 48, 64)
	size := memoryBudgetEnv(b, "TREEDB_MEMORY_VALUE_BYTES", 256, 4096)
	threshold := memoryBudgetEnv(b, "TREEDB_MEMORY_POINTER_THRESHOLD", 1, 1024)
	miss := memoryBudgetEnv(b, "TREEDB_QUICKSILVER_MISS_PERCENT", 0, 50, 90, 99)
	batchSize := memoryBudgetEnv(b, "TREEDB_QUICKSILVER_READ_BATCH", 1, 64)
	filterBytes := memoryBudgetEnv(b, "TREEDB_QUICKSILVER_FILTER_BYTES", 0, ((keys+1)*10+63)/64*8)
	distribution := os.Getenv("TREEDB_QUICKSILVER_DISTRIBUTION")
	if distribution != "uniform" && distribution != "zipf" {
		b.Fatal("explicit uniform or zipf distribution required")
	}
	queries := quicksilverQueries(keys, reads, miss, distribution, size)
	opts := memoryBudgetOptions(b.TempDir(), leaf, threshold)
	opts.NegativeLookupFilterBytes = filterBytes
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
	quicksilverCheckConfiguration(b, d.Stats(), leaf, filterBytes)
	binaryHash := memoryBudgetBinaryHash(b)
	initial := d.Stats()
	phases := make([]memoryBudgetPhase, 0, 15)
	measure := func(name string, operations int, run func()) {
		beforeStats := d.Stats()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		run()
		elapsed := time.Since(start).Nanoseconds()
		runtime.ReadMemStats(&after)
		phases = append(phases, memoryBudgetPhase{Name: name, Operations: operations, ElapsedNS: elapsed,
			AllocatedBytes: after.TotalAlloc - before.TotalAlloc, Allocations: after.Mallocs - before.Mallocs,
			HeapBytes: after.HeapAlloc, BeforeStats: beforeStats, Stats: d.Stats(), Files: memoryBudgetFiles(b, opts.Dir)})
	}
	checkpoint := func() {
		if err := d.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	}
	measure("load_sync", keys, func() { memoryBudgetWrite(b, d, keys, size, 0, keys) })
	// A separate sentinel exercises present-empty semantics without changing the
	// regular even-hit/odd-miss domain or its operation counts.
	if err := d.SetSync([]byte("quicksilver/empty/sentinel"), nil); err != nil {
		b.Fatal(err)
	}
	measure("initial_checkpoint", 1, checkpoint)
	pointers := memoryBudgetPlacement(b, d, keys)
	if (threshold == 1 || size == 4096) && pointers != keys || threshold != 1 && size != 4096 && pointers != 0 {
		b.Fatal("wrong persisted placement", pointers)
	}
	measure("verify_initial", keys*2+1, func() { quicksilverVerify(b, d, keys, size, 0) })
	var latency []int64
	var checksum uint64
	measure("owned_warm_reads", reads, func() {
		latency, checksum = quicksilverRead(b, d, queries, batchSize)
	})
	// Four identical synchronous update intervals, each ending at a checkpoint.
	// Values are generated only one batch at a time, outside any reader loop.
	for part := range 4 {
		measure(fmt.Sprintf("updates_%d", part+1), updates/4, func() {
			for base := part * (updates / 4); base < (part+1)*(updates/4); base += 1000 {
				batch := d.NewBatchWithSize(1000)
				for i := base; i < base+1000; i++ {
					id := i * 7919 % keys
					if err := batch.Set(memoryBudgetKey(id*2, make([]byte, 32)), memoryBudgetValue(id, 1, make([]byte, size))); err != nil {
						_ = batch.Close()
						b.Fatal(err)
					}
				}
				writeErr, closeErr := batch.WriteSync(), batch.Close()
				if writeErr != nil || closeErr != nil {
					b.Fatal(writeErr, closeErr)
				}
			}
		})
		measure(fmt.Sprintf("checkpoint_%d", part+1), 1, checkpoint)
	}
	closure := d.Stats()
	if memoryBudgetStat(b, closure, "treedb.command_wal.applied_lsn") < memoryBudgetStat(b, closure, "treedb.command_wal.live_accepted_max_lsn") {
		b.Fatal("checkpoint does not cover acknowledged LSN")
	}
	measure("verify_before_close", keys*2+1, func() { quicksilverVerify(b, d, keys, size, updates) })
	if err := d.Close(); err != nil {
		b.Fatal(err)
	}
	d = nil
	reopenStart := time.Now()
	d, err = Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	reopenNS := time.Since(reopenStart).Nanoseconds()
	quicksilverCheckConfiguration(b, d.Stats(), leaf, filterBytes)
	measure("verify_reopen", keys*2+1, func() { quicksilverVerify(b, d, keys, size, updates) })
	reopenStats := d.Stats()
	runtime.GC()
	var retained runtime.MemStats
	runtime.ReadMemStats(&retained)
	retainedStats := d.Stats()
	if err := d.Close(); err != nil {
		b.Fatal(err)
	}
	d = nil
	if memoryBudgetBinaryHash(b) != binaryHash {
		b.Fatal("benchmark executable changed")
	}
	environment := os.Environ()
	sort.Strings(environment)
	packet := map[string]any{"schema": "quicksilver-workflow-v1", "pilot": pilot,
		"keys": keys, "updates": updates, "read_keys": reads, "value_bytes": size, "key_bytes": 32,
		"pointer_threshold": threshold, "verified_pointer_entries": pointers, "miss_percent": miss,
		"distribution": distribution, "read_batch": batchSize, "negative_filter_bytes": filterBytes,
		"main_leaf_budget_bytes": leaf << 20, "main_frame_budget_bytes": (64 - leaf) << 20,
		"combined_configured_main_budget_bytes": 64 << 20, "batch_size": 1000, "update_stride": 7919,
		"update_checkpoints": 4, "crc_disabled": false, "sentinel_keys": 1,
		"shared_prefix_bytes": 24, "background_maintenance_disabled": true, "side_store_limits_changed": false,
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
		"warm_state":                            "after checkpoint, persisted-placement scan and full value/miss verification",
		"throughput_includes_crc32_consumption": true, "latency_unit": "ns per owned API request",
		"query_table_bytes": len(queries) * 8, "read_checksum": checksum, "latency_samples": len(latency),
		"read_api_requests": reads / batchSize, "read_hits": reads * (100 - miss) / 100, "read_misses": reads * miss / 100,
		"latency_sample_stride_requests": 16, "read_p99_ns": algorithmQuantile(latency, .99),
		"read_p999_ns": algorithmQuantile(latency, .999), "read_max_ns": algorithmQuantile(latency, 1), "reopen_ns": reopenNS,
		"initial_stats": initial, "closure_stats": closure, "reopen_stats": reopenStats,
		"post_reopen_gc_heap_bytes": retained.HeapAlloc, "post_reopen_gc_stats": retainedStats,
		"phases": phases, "closed_files": memoryBudgetFiles(b, opts.Dir),
		"all_values_and_interleaved_misses_verified": true, "present_empty_verified": true, "final_close_checked": true,
		"binary_sha256": binaryHash, "go_version": runtime.Version(), "goos": runtime.GOOS,
		"goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "gomemlimit": os.Getenv("GOMEMLIMIT"),
		"process_id": os.Getpid(), "process_argv": os.Args,
		"environment_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(environment, "\x00"))))}
	encoded, err := json.Marshal(packet)
	if err != nil {
		b.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "TREEDB_QUICKSILVER_PACKET %s\n", encoded)
}

type quicksilverQuery struct {
	KeyID uint32
	CRC   uint32
}

func quicksilverQueries(keys, count, miss int, distribution string, size int) []quicksilverQuery {
	rng := rand.New(rand.NewPCG(51, 99))
	zipf := rand.NewZipf(rng, 1.1, 1, uint64(keys-1))
	queries := make([]quicksilverQuery, count)
	scratch := make([]byte, size)
	for i := range queries {
		id := rng.IntN(keys)
		if distribution == "zipf" {
			id = int(zipf.Uint64())
		}
		queries[i].KeyID = uint32(id * 2)
		if i*37%100 < miss {
			queries[i].KeyID++
		} else {
			queries[i].CRC = crc32.ChecksumIEEE(memoryBudgetValue(id, 0, scratch))
		}
	}
	return queries
}

func quicksilverRead(tb testing.TB, d *DB, queries []quicksilverQuery, batchSize int) ([]int64, uint64) {
	tb.Helper()
	if (batchSize != 1 && batchSize != 64) || len(queries)%batchSize != 0 {
		tb.Fatal("requires complete single or 64-key read requests")
	}
	keys := make([][]byte, batchSize)
	for i := range keys {
		keys[i] = make([]byte, 32)
	}
	samples := make([]int64, 0, (len(queries)/batchSize+15)/16)
	var checksum uint64
	for base := 0; base < len(queries); base += batchSize {
		for i := range keys {
			memoryBudgetKey(int(queries[base+i].KeyID), keys[i])
		}
		start := time.Now()
		var values [][]byte
		var err error
		if batchSize == 1 {
			var value []byte
			value, err = d.Get(keys[0])
			values = [][]byte{value}
		} else {
			values, err = d.GetMany(keys)
		}
		elapsed := time.Since(start).Nanoseconds()
		if err != nil || len(values) != batchSize {
			tb.Fatal("owned read failed", err, len(values), batchSize)
		}
		for i, value := range values {
			query := queries[base+i]
			if query.KeyID%2 == 1 {
				if value != nil {
					tb.Fatal("present value for deterministic miss")
				}
			} else if value == nil || crc32.ChecksumIEEE(value) != query.CRC {
				tb.Fatal("owned value checksum mismatch", query.KeyID)
			}
			checksum += uint64(query.CRC)
		}
		if base/batchSize%16 == 0 {
			samples = append(samples, elapsed)
		}
	}
	return samples, checksum
}

func quicksilverCheckConfiguration(tb testing.TB, stats map[string]string, leafMiB, filterBytes int) {
	tb.Helper()
	if stats["treedb.vlog.read_integrity"] != "verify" ||
		memoryBudgetStat(tb, stats, "treedb.negative_lookup_filter.active_bytes") != uint64(filterBytes) ||
		memoryBudgetStat(tb, stats, "treedb.process.read_path.outer_leaf.cache.capacity")*4096 != uint64(leafMiB<<20) ||
		memoryBudgetStat(tb, stats, "treedb.vlog.grouped_frame_cache.budget_bytes") != uint64((64-leafMiB)<<20) ||
		leafMiB == 64 && memoryBudgetStat(tb, stats, "treedb.vlog.grouped_frame_cache.capacity") != 0 {
		tb.Fatal("integrity/filter/main-cache configuration does not match frozen cell")
	}
}

func quicksilverVerify(tb testing.TB, d *DB, keys, size, updates int) {
	tb.Helper()
	memoryBudgetRead(tb, d, keys, size, updates, true, true)
	empty, err := d.Get([]byte("quicksilver/empty/sentinel"))
	if err != nil || empty == nil || !bytes.Equal(empty, []byte{}) {
		tb.Fatal("present-empty sentinel", err, empty)
	}
}

func TestQuicksilverWorkflowFixture(t *testing.T) {
	for _, distribution := range []string{"uniform", "zipf"} {
		for _, miss := range []int{0, 50, 90, 99} {
			queries := quicksilverQueries(100, 6400, miss, distribution, 256)
			missing := 0
			for _, query := range queries {
				if query.KeyID >= 200 {
					t.Fatal("out-of-domain query", query)
				}
				if query.KeyID%2 == 1 {
					missing++
				}
			}
			if missing != 6400*miss/100 {
				t.Fatal("wrong deterministic miss count", missing)
			}
		}
	}
	opts := memoryBudgetOptions(t.TempDir(), 0, 1)
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
	memoryBudgetWrite(t, d, 100, 256, 0, 100)
	if err := d.SetSync([]byte("quicksilver/empty/sentinel"), nil); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	quicksilverVerify(t, d, 100, 256, 0)
	for _, batch := range []int{1, 64} {
		quicksilverRead(t, d, quicksilverQueries(100, 6400, 90, "zipf", 256), batch)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	d, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	quicksilverVerify(t, d, 100, 256, 0)
}
