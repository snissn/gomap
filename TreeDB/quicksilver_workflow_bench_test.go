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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	processIOBefore := quicksilverIO()
	initial := d.Stats()
	phaseIO := make(map[string]any)
	phases := make([]memoryBudgetPhase, 0, 15)
	measure := func(name string, operations int, run func()) {
		ioBefore := quicksilverIO()
		beforeStats := d.Stats()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		run()
		elapsed := time.Since(start).Nanoseconds()
		runtime.ReadMemStats(&after)
		phaseIO[name] = map[string]any{"before": ioBefore, "after": quicksilverIO()}
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
	// The concurrent reader has fixed present-key Get composition, separately
	// labeled from the warm query table. Cleanup joins it before any owner closes.
	reader := quicksilverStartReader(d.Get, keys, size, 65536)
	defer reader.finish()
	if err := <-reader.started; err != nil {
		b.Fatal(err)
	}
	ackSamples := make([]int64, 0, updates/1000)
	var concurrent quicksilverConcurrentResult
	// Four identical synchronous update intervals, each ending at a checkpoint.
	// Values are generated only one batch at a time, outside any reader loop.
	for part := range 4 {
		measure(fmt.Sprintf("updates_%d", part+1), updates/4, func() {
			for base := part * (updates / 4); base < (part+1)*(updates/4); base += 1000 {
				if reader.failed.Load() {
					b.Fatal("concurrent reader failed")
				}
				batch := d.NewBatchWithSize(1000)
				for i := base; i < base+1000; i++ {
					id := i * 7919 % keys
					if err := batch.Set(memoryBudgetKey(id*2, make([]byte, 32)), memoryBudgetValue(id, 1, make([]byte, size))); err != nil {
						_ = batch.Close()
						b.Fatal(err)
					}
				}
				ackStart := time.Now()
				writeErr := batch.WriteSync()
				ackNS := time.Since(ackStart).Nanoseconds()
				closeErr := batch.Close()
				if writeErr != nil || closeErr != nil {
					b.Fatal(writeErr, closeErr)
				}
				ackSamples = append(ackSamples, ackNS)
			}
		})
		measure(fmt.Sprintf("checkpoint_%d", part+1), 1, func() {
			checkpoint()
			if part == 3 {
				concurrent = reader.finish()
				if concurrent.Error != "" || concurrent.Reads == 0 || len(concurrent.Samples) == 0 {
					b.Fatal("concurrent reader incomplete", concurrent.Error)
				}
			}
		})
	}
	// Observe the complete lifetime after checkpoint_4 phase capture. This
	// includes quantile completion, channel synchronization, join and phase reporting.
	concurrent.ElapsedNS = time.Since(reader.start).Nanoseconds()
	if len(ackSamples) != updates/1000 {
		b.Fatal("incomplete update acknowledgements")
	}
	var ackSum int64
	for _, ns := range ackSamples {
		if ns <= 0 {
			b.Fatal("nonpositive acknowledgement")
		}
		ackSum += ns
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
		"latency_sample_stride_requests": quicksilverLatencyStride, "read_p99_ns": algorithmQuantile(latency, .99),
		"read_p999_ns": algorithmQuantile(latency, .999), "read_max_ns": algorithmQuantile(latency, 1), "reopen_ns": reopenNS,
		"initial_stats": initial, "closure_stats": closure, "reopen_stats": reopenStats,
		"post_reopen_gc_heap_bytes": retained.HeapAlloc, "post_reopen_gc_stats": retainedStats,
		"phases": phases, "closed_files": memoryBudgetFiles(b, opts.Dir),
		"update_ack_samples_ns": ackSamples, "update_ack_count": len(ackSamples), "update_ack_batch_ops": 1000,
		"update_ack_ns": ackSum, "update_ack_latency_unit": "ns per 1000-key WriteSync",
		"update_ack_p99_ns": quicksilverQuantile(ackSamples, .99), "update_ack_p999_ns": quicksilverQuantile(ackSamples, .999),
		"update_ack_max_ns": quicksilverQuantile(ackSamples, 1), "concurrent_owned_reads": concurrent,
		"phase_process_io": phaseIO, "process_io": map[string]any{"before": processIOBefore, "after": quicksilverIO()},
		"process_io_scope":                           "kernel process counters; includes concurrent owner and helper work, not device writes",
		"final_checkpoint_includes_reader_join":      true,
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

// Coprime to both the 100-key miss period and the 25-request GetMany64 period.
const quicksilverLatencyStride = 17

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
	samples := make([]int64, 0, (len(queries)/batchSize+quicksilverLatencyStride-1)/quicksilverLatencyStride)
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
		if base/batchSize%quicksilverLatencyStride == 0 {
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
			for _, batch := range []int{1, 64} {
				hits, misses := 0, 0
				for base := 0; base < len(queries); base += batch * quicksilverLatencyStride {
					for _, query := range queries[base : base+batch] {
						if query.KeyID%2 == 0 {
							hits++
						} else {
							misses++
						}
					}
				}
				if hits == 0 || miss > 0 && misses == 0 {
					t.Fatal("latency sample omitted hit/miss population", distribution, miss, batch, hits, misses)
				}
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

// Quantile sorts its input; retain chronological raw samples in the packet.
func quicksilverQuantile(samples []int64, fraction float64) int64 {
	return algorithmQuantile(append([]int64(nil), samples...), fraction)
}

type quicksilverConcurrentResult struct {
	API                 string  `json:"api"`
	Readers             int     `json:"readers"`
	KeyDomain           string  `json:"key_domain"`
	Generations         []int   `json:"allowed_generations"`
	Stride              int     `json:"sample_stride"`
	Capacity            int     `json:"sample_capacity"`
	Reads               uint64  `json:"reads"`
	Samples             []int64 `json:"samples_ns"`
	MaxNS               int64   `json:"max_ns"`
	P99NS               int64   `json:"p99_ns"`
	P999NS              int64   `json:"p999_ns"`
	ElapsedNS           int64   `json:"elapsed_ns"`
	FullValuesValidated bool    `json:"full_values_validated"`
	Joined              bool    `json:"joined"`
	Error               string  `json:"error"`
}

type quicksilverReader struct {
	start   time.Time
	stop    atomic.Bool
	failed  atomic.Bool
	started chan error
	done    chan quicksilverConcurrentResult
	once    sync.Once
	result  quicksilverConcurrentResult
}

func quicksilverStartReader(get func([]byte) ([]byte, error), keys, size, capacity int) *quicksilverReader {
	return quicksilverStartReaderMeasured(get, keys, size, capacity, func(start time.Time) int64 { return time.Since(start).Nanoseconds() })
}

// Inject only the elapsed observation for mocked unit reads. Native captures
// always use the real monotonic API timer above and reject nonpositive samples.
func quicksilverStartReaderMeasured(get func([]byte) ([]byte, error), keys, size, capacity int, elapsed func(time.Time) int64) *quicksilverReader {
	r := &quicksilverReader{start: time.Now(), started: make(chan error, 1), done: make(chan quicksilverConcurrentResult, 1)}
	go func() {
		result := quicksilverConcurrentResult{API: "owned Get", Readers: 1, KeyDomain: "present even keys; permutation 7919", Generations: []int{0, 1}, Stride: quicksilverLatencyStride, Capacity: capacity, Samples: make([]int64, 0, capacity)}
		key, old, updated := make([]byte, 32), make([]byte, size), make([]byte, size)
		announced := false
		for !r.stop.Load() {
			id := int(result.Reads * 7919 % uint64(keys))
			memoryBudgetKey(id*2, key)
			memoryBudgetValue(id, 0, old)
			memoryBudgetValue(id, 1, updated)
			readStart := time.Now()
			value, err := get(key)
			ns := elapsed(readStart)
			if err != nil || !quicksilverConcurrentValid(value, old, updated) || ns <= 0 {
				result.Error = fmt.Sprintf("concurrent Get invalid id=%d err=%v ns=%d", id, err, ns)
				break
			}
			if ns > result.MaxNS {
				result.MaxNS = ns
			}
			if result.Reads%quicksilverLatencyStride == 0 {
				if len(result.Samples) == cap(result.Samples) {
					result.Error = "concurrent sample capacity exceeded"
					break
				}
				result.Samples = append(result.Samples, ns)
			}
			result.Reads++
			if !announced {
				r.started <- nil
				announced = true
			}
		}
		if !announced {
			r.started <- fmt.Errorf("concurrent reader did not validate first read: %s", result.Error)
		}
		result.P99NS, result.P999NS = quicksilverQuantile(result.Samples, .99), quicksilverQuantile(result.Samples, .999)
		result.FullValuesValidated = result.Error == "" && result.Reads > 0
		if result.Error != "" {
			r.failed.Store(true)
		}
		r.done <- result
	}()
	return r
}

func quicksilverConcurrentValid(value, old, updated []byte) bool {
	return len(value) > 0 && (bytes.Equal(value, old) || bytes.Equal(value, updated))
}

func (r *quicksilverReader) finish() quicksilverConcurrentResult {
	r.once.Do(func() {
		r.stop.Store(true)
		r.result = <-r.done
		r.result.Joined = true
		r.result.ElapsedNS = time.Since(r.start).Nanoseconds()
	})
	return r.result
}

// Keep this helper inside H: the original-product overlay has five additions
// and does not import A's Go file. Missing /proc data is explicit, never zero.
func quicksilverIO() map[string]any {
	counters := map[string]uint64{}
	raw, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return map[string]any{"supported": false, "counters": counters}
	}
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := strings.Cut(line, ": ")
		if ok {
			n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
			if err == nil {
				counters[key] = n
			}
		}
	}
	return map[string]any{"supported": true, "counters": counters}
}

func TestQuicksilverUpdateReader(t *testing.T) {
	// Returning tiny in-memory mock values can occupy zero clock ticks on Windows.
	// Lifecycle/value tests use known positive samples, not scheduler timing.
	startReader := func(get func([]byte) ([]byte, error), keys, size, capacity int) *quicksilverReader {
		return quicksilverStartReaderMeasured(get, keys, size, capacity, func(time.Time) int64 { return 1 })
	}
	old, updated := memoryBudgetValue(0, 0, make([]byte, 256)), memoryBudgetValue(0, 1, make([]byte, 256))
	for _, value := range [][]byte{old, updated} {
		if !quicksilverConcurrentValid(value, old, updated) {
			t.Fatal("valid generation rejected")
		}
	}
	zero := quicksilverStartReaderMeasured(func([]byte) ([]byte, error) { return old, nil }, 1, 256, 65536, func(time.Time) int64 { return 0 })
	if err := <-zero.started; err == nil {
		t.Fatal("zero-duration valid mock read announced success")
	}
	if result := zero.finish(); result.Error == "" || !strings.Contains(result.Error, "ns=0") || !result.Joined || !zero.failed.Load() || result.FullValuesValidated || result.Reads != 0 || len(result.Samples) != 0 {
		t.Fatal("zero-duration read accepted", result)
	}
	corrupt := append([]byte(nil), old...)
	corrupt[len(corrupt)-1] ^= 1
	for _, value := range [][]byte{nil, {}, old[:16], corrupt} {
		if quicksilverConcurrentValid(value, old, updated) {
			t.Fatal("invalid full value accepted")
		}
	}
	for _, fail := range []bool{false, true} {
		for _, value := range [][]byte{nil, {}, corrupt} {
			failed := startReader(func([]byte) ([]byte, error) { return value, nil }, 1, 256, 65536)
			if err := <-failed.started; err == nil {
				t.Fatal("invalid read announced success")
			}
			if result := failed.finish(); result.Error == "" || !result.Joined || result.FullValuesValidated {
				t.Fatal("invalid reader accepted", result)
			}
		}
		// Deferred joins also run on an early writer/checkpoint failure return.
		for _, stage := range []string{"write", "checkpoint"} {
			reader := startReader(func([]byte) ([]byte, error) { return old, nil }, 1, 256, 65536)
			if err := <-reader.started; err != nil {
				t.Fatal(err)
			}
			func() { defer reader.finish(); _ = fmt.Errorf("injected %s failure", stage); return }()
			if !reader.result.Joined || !reader.stop.Load() {
				t.Fatal("failure cleanup did not join", stage)
			}
		}
		r := startReader(func([]byte) ([]byte, error) {
			if fail {
				return nil, fmt.Errorf("injected read error")
			}
			return old, nil
		}, 1, 256, 65536)
		first := <-r.started
		result := r.finish()
		if !result.Joined || (first != nil) != fail || (result.Error != "") != fail || result.FullValuesValidated == fail {
			t.Fatal("reader lifecycle", first, result)
		}
		if again := r.finish(); again.Reads != result.Reads || !again.Joined {
			t.Fatal("join not idempotent")
		}
	}
	r := startReader(func([]byte) ([]byte, error) { return old, nil }, 1, 256, 1)
	if err := <-r.started; err != nil {
		t.Fatal(err)
	}
	// Wait for overflow without relying on arbitrary sleeps.
	result := <-r.done
	r.done <- result
	result = r.finish()
	if result.Error != "concurrent sample capacity exceeded" || !result.Joined {
		t.Fatal("overflow accepted", result)
	}
	// A blocked owned Get keeps finish blocked until the reader completes. The
	// lifetime includes that wait and reader-side quantiles/channel completion.
	entered, release := make(chan struct{}), make(chan struct{})
	joined := make(chan quicksilverConcurrentResult, 1)
	delayed := startReader(func([]byte) ([]byte, error) {
		close(entered)
		<-release
		return old, nil
	}, 1, 256, 65536)
	<-entered
	go func() { joined <- delayed.finish() }()
	for !delayed.stop.Load() {
		runtime.Gosched()
	}
	select {
	case <-joined:
		t.Fatal("joined before Get completed")
	default:
	}
	minimum := time.Since(delayed.start).Nanoseconds()
	close(release)
	observed := <-joined
	if observed.ElapsedNS < minimum || observed.ElapsedNS > time.Since(delayed.start).Nanoseconds() || !observed.Joined {
		t.Fatal("reader lifetime omitted join", observed)
	}
	samples := []int64{9, 1, 7}
	if quicksilverQuantile(samples, .99) != 9 || samples[0] != 9 || samples[1] != 1 {
		t.Fatal("raw chronology changed")
	}
}
