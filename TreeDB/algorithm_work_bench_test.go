package treedb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// This fixed-work workflow is intentionally run with -benchtime=1x. Counters,
// process allocations and read validation include harness work; ns/op is the
// complete write/ack/checkpoint interval, not an isolated engine operation.
func BenchmarkAlgorithmSparseUpdates(b *testing.B) {
	keys, updates := 250000, 40000
	if os.Getenv("TREEDB_ALGORITHM_PILOT") == "1" {
		keys, updates = 8192, 8000
	}
	provenance := algorithmProvenance(b)
	for _, pointer := range []bool{false, true} {
		for _, checkpoints := range []int{4, 1} {
			for _, wide := range []bool{false, true} {
				b.Run(fmt.Sprintf("pointer=%v/checkpoints=%d/wide=%v", pointer, checkpoints, wide), func(b *testing.B) {
					if b.N != 1 {
						b.Fatal("fixed-work workflow requires -benchtime=1x")
					}
					opts := algorithmOptions(b.TempDir(), pointer, wide)
					d := algorithmFixture(b, opts, keys)
					defer func() {
						if d != nil {
							if err := d.Close(); err != nil {
								b.Error(err)
							}
						}
					}()
					algorithmVerify(b, d, keys, 0)
					updateKeys, updateValues := make([][]byte, updates), make([][]byte, updates)
					for i := range updateKeys {
						id := i * 7919 % keys
						updateKeys[i], updateValues[i] = algorithmKey(id*2), algorithmValue(id, 1)
					}
					before := algorithmStats(d.Stats())
					var memBefore, memAfter runtime.MemStats
					runtime.ReadMemStats(&memBefore)
					ioBefore := algorithmIO()
					var stop atomic.Bool
					ready, release, done := make(chan struct{}), make(chan struct{}), make(chan algorithmReadResult, 1)
					go func() {
						r := algorithmReadResult{Samples: make([]int64, 0, 65536)}
						close(ready)
						<-release
						for !stop.Load() {
							id := int(r.Reads * 7919 % uint64(keys))
							key := algorithmKey(id * 2)
							start := time.Now()
							value, err := d.Get(key)
							ns := time.Since(start).Nanoseconds()
							if err != nil || !algorithmValid(value, id, -1) {
								r.Error = fmt.Sprintf("read id=%d err=%v invalid=%v", id, err, !algorithmValid(value, id, -1))
								stop.Store(true)
								break
							}
							r.Reads++
							if ns > r.MaxNS {
								r.MaxNS = ns
							}
							// A fixed stride over the entire interval, with an explicit
							// capacity gate: overflow fails instead of truncating late tails.
							if r.Reads%16 == 0 {
								if len(r.Samples) == cap(r.Samples) {
									r.Error = "sample capacity exceeded"
									stop.Store(true)
									break
								}
								r.Samples = append(r.Samples, ns)
							}
						}
						done <- r
					}()
					<-ready
					b.ReportAllocs()
					b.ResetTimer()
					start := time.Now()
					close(release)
					var ackNS, checkpointNS int64
					var writeErr error
					commits, completedCheckpoints := 0, 0
					for base := 0; base < updates && writeErr == nil && !stop.Load(); base += 1000 {
						batch := d.NewBatchWithSize(1000)
						end := min(base+1000, updates)
						for i := base; i < end; i++ {
							if writeErr = batch.Set(updateKeys[i], updateValues[i]); writeErr != nil {
								break
							}
						}
						if writeErr == nil {
							ackStart := time.Now()
							writeErr = batch.WriteSync()
							ackNS += time.Since(ackStart).Nanoseconds()
							commits++
						}
						if err := batch.Close(); writeErr == nil {
							writeErr = err
						}
						if writeErr == nil && end%(updates/checkpoints) == 0 {
							cpStart := time.Now()
							writeErr = d.Checkpoint()
							checkpointNS += time.Since(cpStart).Nanoseconds()
							completedCheckpoints++
						}
					}
					stop.Store(true)
					reads := <-done
					elapsed := time.Since(start)
					b.StopTimer()
					ioAfter := algorithmIO()
					runtime.ReadMemStats(&memAfter)
					if writeErr != nil || reads.Error != "" {
						b.Fatal(writeErr, reads.Error)
					}
					if commits != updates/1000 || completedCheckpoints != checkpoints || len(reads.Samples) == 0 {
						b.Fatal("incomplete operations", commits, completedCheckpoints, reads.Reads)
					}
					after := algorithmStats(d.Stats())
					disk := algorithmDisk(b, opts.Dir)
					if algorithmUint(after, "treedb.command_wal.applied_lsn") < algorithmUint(after, "treedb.command_wal.live_accepted_max_lsn") {
						b.Fatal("checkpoint left acknowledged LSN uncovered")
					}
					if err := d.Close(); err != nil {
						b.Fatal(err)
					}
					d = nil
					reopened, err := Open(opts)
					if err != nil {
						b.Fatal(err)
					}
					d = reopened
					algorithmVerify(b, d, keys, updates)
					runtime.GC()
					var retained runtime.MemStats
					runtime.ReadMemStats(&retained)
					b.ReportMetric(float64(updates)/elapsed.Seconds(), "updates/s")
					b.ReportMetric(float64(reads.Reads)/elapsed.Seconds(), "reads/s")
					b.ReportMetric(float64(algorithmQuantile(reads.Samples, .99)), "read-p99-ns")
					b.ReportMetric(float64(algorithmQuantile(reads.Samples, .999)), "read-p999-ns")
					if err := d.Close(); err != nil {
						b.Fatal(err)
					}
					d = nil
					packet := map[string]any{"schema": "algorithm-work-v1", "provenance": provenance, "keys": keys, "updates": updates, "commits": commits, "ack_batch_ops": 1000, "permutation_multiplier": 7919, "key_bytes": 32, "value_bytes": 256, "allowed_concurrent_generations": []int{0, 1}, "pointer_threshold": opts.ValueLog.PointerThreshold, "flush_threshold": opts.FlushThreshold, "coalescing_wide": wide, "checkpoints": completedCheckpoints, "interval_ns": elapsed.Nanoseconds(), "ack_ns": ackNS, "checkpoint_ns": checkpointNS, "read_count": reads.Reads, "read_samples": len(reads.Samples), "read_sample_stride": 16, "read_p99_ns": algorithmQuantile(reads.Samples, .99), "read_p999_ns": algorithmQuantile(reads.Samples, .999), "read_max_ns": reads.MaxNS, "allocated_bytes": memAfter.TotalAlloc - memBefore.TotalAlloc, "allocations": memAfter.Mallocs - memBefore.Mallocs, "post_reopen_gc_heap_bytes": retained.HeapAlloc, "kernel_process_io_before": ioBefore, "kernel_process_io_after": ioAfter, "file_logical_bytes": disk, "before": before, "after": after, "validated_all_values_and_misses": true, "final_close_checked": true}
					encoded, err := json.Marshal(packet)
					if err != nil {
						b.Fatal(err)
					}
					b.Log(string(encoded))
				})
			}
		}
	}
}

type algorithmReadResult struct {
	Reads   uint64
	Samples []int64
	MaxNS   int64
	Error   string
}

func algorithmOptions(dir string, pointer, wide bool) Options {
	opts := Options{Dir: dir, CommandWAL: true, CommandWALStatsScan: true, PublicBatchWriteSyncPhaseStats: true, KeepRecent: 10000, IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true, BackgroundCheckpointInterval: -1, BackgroundCheckpointIdleDuration: -1, MaxWALBytes: -1, BackgroundIndexVacuumInterval: -1, DisableBackgroundPrune: true}
	if pointer {
		opts.ValueLog.PointerThreshold = 1
	} else {
		opts.ValueLog.PointerThreshold = 16384
	}
	opts.FlushThreshold = 64 << 20
	switch os.Getenv("TREEDB_ALGORITHM_SMALL_FLUSH") {
	case "", "0":
	case "1":
		opts.FlushThreshold = 1 << 20
	default:
		panic("TREEDB_ALGORITHM_SMALL_FLUSH must be 0 or 1")
	}
	if wide {
		opts.FlushBacklogCoalescingMaxMemtables = 128
		opts.FlushBacklogCoalescingMaxOps = 4 << 20
		opts.FlushBacklogCoalescingMaxBytes = 1 << 30
	}
	return opts
}

func algorithmKey(id int) []byte {
	key := make([]byte, 32)
	copy(key, "algorithm/shared/prefix/")
	binary.BigEndian.PutUint64(key[24:], uint64(id))
	return key
}
func algorithmValue(id, generation int) []byte {
	value := bytes.Repeat([]byte{'v'}, 256)
	binary.LittleEndian.PutUint64(value, uint64(id))
	binary.LittleEndian.PutUint64(value[8:], uint64(generation))
	return value
}
func algorithmValid(value []byte, id, generation int) bool {
	if len(value) != 256 || binary.LittleEndian.Uint64(value) != uint64(id) {
		return false
	}
	g := binary.LittleEndian.Uint64(value[8:])
	if (generation >= 0 && g != uint64(generation)) || g > 1 {
		return false
	}
	for _, v := range value[16:] {
		if v != 'v' {
			return false
		}
	}
	return true
}

func algorithmFixture(tb testing.TB, opts Options, keys int) *DB {
	tb.Helper()
	d, err := Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = d.Close() })
	for base := 0; base < keys; base += 1000 {
		batch := d.NewBatchWithSize(1000)
		for i := base; i < min(base+1000, keys); i++ {
			if err := batch.Set(algorithmKey(i*2), algorithmValue(i, 0)); err != nil {
				tb.Fatal(err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			tb.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		tb.Fatal(err)
	}
	return d
}
func algorithmVerify(tb testing.TB, d *DB, keys, updates int) {
	tb.Helper()
	changed := make([]bool, keys)
	for i := 0; i < updates; i++ {
		changed[i*7919%keys] = true
	}
	for id := 0; id < keys; id++ {
		generation := 0
		if changed[id] {
			generation = 1
		}
		value, err := d.Get(algorithmKey(id * 2))
		if err != nil || !algorithmValid(value, id, generation) {
			tb.Fatalf("reopen id=%d generation=%d err=%v", id, generation, err)
		}
		missing, err := d.Get(algorithmKey(id*2 + 1))
		if err != nil || missing != nil {
			tb.Fatalf("reopen missing=%d err=%v", id, err)
		}
	}
}
func algorithmStats(stats map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range stats {
		if strings.Contains(key, "flush") || strings.Contains(key, "checkpoint") || strings.Contains(key, "command_wal") || strings.Contains(key, "memory") || strings.Contains(key, "cache") {
			out[key] = value
		}
	}
	return out
}

func algorithmProvenance(tb testing.TB) map[string]any {
	tb.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("source identity unavailable")
	}
	raw, err := os.ReadFile(source)
	if err != nil {
		tb.Fatal(err)
	}
	head := os.Getenv("TREEDB_ALGORITHM_RUNTIME_HEAD")
	pilot := os.Getenv("TREEDB_ALGORITHM_PILOT") == "1"
	var binaryHash string
	var runtimeTree string
	if !pilot {
		root := filepath.Dir(filepath.Dir(source))
		actual, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
		if err != nil {
			tb.Fatal(err)
		}
		if err := algorithmCheckHead(head, strings.TrimSpace(string(actual))); err != nil {
			tb.Fatal(err)
		}
		dirty, err := exec.Command("git", "-C", root, "status", "--porcelain", "--untracked-files=normal", "--", "TreeDB", "go.mod", "go.sum", "scripts/treedb_algorithm_work_overlay.py").Output()
		if err != nil || len(dirty) != 0 {
			tb.Fatal("unfrozen runtime/harness source", err, string(dirty))
		}
		var freeze struct {
			RuntimeHead   string `json:"runtime_head"`
			RuntimeTree   string `json:"runtime_tree"`
			HarnessSHA256 string `json:"harness_sha256"`
			BinarySHA256  string `json:"binary_sha256"`
		}
		freezeBytes, err := os.ReadFile(os.Getenv("TREEDB_ALGORITHM_FREEZE_FILE"))
		if err != nil {
			tb.Fatal("external freeze required", err)
		}
		if err := json.Unmarshal(freezeBytes, &freeze); err != nil {
			tb.Fatal(err)
		}
		treeBytes, err := exec.Command("git", "-C", root, "rev-parse", "HEAD:TreeDB").Output()
		if err != nil {
			tb.Fatal(err)
		}
		runtimeTree = strings.TrimSpace(string(treeBytes))
		executable, err := os.Executable()
		if err != nil {
			tb.Fatal(err)
		}
		file, err := os.Open(executable)
		if err != nil {
			tb.Fatal(err)
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			tb.Fatal(copyErr, closeErr)
		}
		binaryHash = fmt.Sprintf("%x", hash.Sum(nil))
		if freeze.RuntimeHead != head || freeze.RuntimeTree != runtimeTree || freeze.HarnessSHA256 != fmt.Sprintf("%x", sha256.Sum256(raw)) || freeze.BinarySHA256 != binaryHash {
			tb.Fatal("external freeze does not bind runtime/harness/binary")
		}
	}
	host, err := os.Hostname()
	if err != nil {
		tb.Fatal(err)
	}
	return map[string]any{"runtime_head": head, "runtime_tree": runtimeTree, "harness_sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "binary_sha256": binaryHash, "go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "gomemlimit": os.Getenv("GOMEMLIMIT"), "host": host, "argv": os.Args, "pilot": pilot}
}

func algorithmCheckHead(claimed, actual string) error {
	if len(claimed) != 40 || claimed != actual {
		return fmt.Errorf("stale or missing runtime head: claimed=%s actual=%s", claimed, actual)
	}
	return nil
}

func algorithmDisk(tb testing.TB, dir string) map[string]int64 {
	tb.Helper()
	out := map[string]int64{}
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			relative, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			out[relative] = info.Size()
		}
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return out
}
func algorithmUint(stats map[string]string, key string) uint64 {
	n, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		panic(fmt.Sprintf("missing/invalid required stat %s: %v", key, err))
	}
	return n
}
func algorithmIO() map[string]uint64 {
	out := map[string]uint64{}
	raw, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := strings.Cut(line, ": ")
		if ok {
			n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
			if err == nil {
				out[key] = n
			}
		}
	}
	return out
}
func algorithmQuantile(samples []int64, fraction float64) int64 {
	if len(samples) == 0 {
		return 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[min(int(float64(len(samples))*fraction), len(samples)-1)]
}

func algorithmBatches(keys int, shape string) [][][]byte {
	rng := rand.New(rand.NewSource(4893))
	batches := make([][][]byte, 128)
	for batch := range batches {
		batches[batch] = make([][]byte, 64)
		start := rng.Intn(keys - 128)
		for i := range batches[batch] {
			id := start + i
			if shape == "uniform" {
				id = rng.Intn(keys)
			}
			if shape == "clustered" {
				id = start + rng.Intn(128)
			}
			physical := id * 2
			if i%8 == 0 {
				physical++
			}
			batches[batch][i] = algorithmKey(physical)
		}
		batches[batch][63] = batches[batch][1]
		if shape == "sorted" {
			sort.Slice(batches[batch], func(i, j int) bool { return bytes.Compare(batches[batch][i], batches[batch][j]) < 0 })
		}
	}
	return batches
}

// Natural batches are supplied by this caller. It does not buffer scalar reads.
func BenchmarkAlgorithmGetMany(b *testing.B) {
	keys := 250000
	if os.Getenv("TREEDB_ALGORITHM_PILOT") == "1" {
		keys = 8192
	}
	b.Log(algorithmProvenance(b))
	for _, pointer := range []bool{false, true} {
		b.Run(fmt.Sprintf("pointer=%v", pointer), func(b *testing.B) {
			d := algorithmFixture(b, algorithmOptions(b.TempDir(), pointer, false), keys)
			defer func() {
				if err := d.Close(); err != nil {
					b.Error(err)
				}
			}()
			algorithmVerify(b, d, keys, 0)
			for _, shape := range []string{"sorted", "clustered", "uniform"} {
				batches := algorithmBatches(keys, shape)
				for _, view := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/view=%v", shape, view), func(b *testing.B) {
						consume := func(_ int, key, value []byte, found bool) error {
							physical := int(binary.BigEndian.Uint64(key[24:]))
							if physical%2 == 1 {
								if found || value != nil {
									return fmt.Errorf("found missing key")
								}
								return nil
							}
							if !found || !algorithmValid(value, physical/2, 0) {
								return fmt.Errorf("invalid value")
							}
							return nil
						}
						for _, batch := range batches {
							out, err := d.GetMany(batch)
							if err != nil || len(out) != len(batch) {
								b.Fatal("batch warmup", err)
							}
							for j, value := range out {
								if err := consume(j, batch[j], value, value != nil); err != nil {
									b.Fatal("batch warmup", err)
								}
							}
							if err := d.GetManyView(batch, consume); err != nil {
								b.Fatal("callback warmup", err)
							}
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							batch := batches[i%len(batches)]
							if view {
								if err := d.GetManyView(batch, consume); err != nil {
									b.Fatal(err)
								}
							} else {
								out, err := d.GetMany(batch)
								if err != nil || len(out) != len(batch) {
									b.Fatal(err)
								}
								for j, value := range out {
									if err := consume(j, batch[j], value, value != nil); err != nil {
										b.Fatal(err)
									}
								}
							}
						}
						b.StopTimer()
						b.ReportMetric(float64(b.N*64)/b.Elapsed().Seconds(), "keys/s")
						b.ReportMetric(64, "keys/op")
					})
				}
			}
		})
	}
}

func TestAlgorithmWorkFixture(t *testing.T) {
	t.Run("invalid_flush_control", func(t *testing.T) {
		t.Setenv("TREEDB_ALGORITHM_SMALL_FLUSH", "invalid")
		defer func() {
			if recover() == nil {
				t.Fatal("invalid flush control accepted")
			}
		}()
		_ = algorithmOptions("unused", false, false)
	})
	validHead := strings.Repeat("a", 40)
	if algorithmCheckHead(validHead, validHead) != nil || algorithmCheckHead(strings.Repeat("b", 40), validHead) == nil || algorithmCheckHead("", validHead) == nil {
		t.Fatal("runtime head gate did not reject stale/missing identity")
	}
	d := algorithmFixture(t, algorithmOptions(t.TempDir(), true, false), 256)
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	algorithmVerify(t, d, 256, 0)
	for _, shape := range []string{"sorted", "clustered", "uniform"} {
		batches := algorithmBatches(256, shape)
		for _, batch := range batches[:2] {
			got, err := d.GetMany(batch)
			if err != nil || len(got) != 64 {
				t.Fatal(err)
			}
			for i, value := range got {
				id := int(binary.BigEndian.Uint64(batch[i][24:]))
				if id%2 == 1 {
					if value != nil {
						t.Fatal("missing key found")
					}
				} else if !algorithmValid(value, id/2, 0) {
					t.Fatal("invalid owned batch value")
				}
			}
			if err := d.GetManyView(batch, func(i int, key, value []byte, found bool) error {
				if !bytes.Equal(key, batch[i]) || found != (got[i] != nil) || !bytes.Equal(value, got[i]) {
					return fmt.Errorf("callback differs from owned result")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	old := d.AcquireSnapshot()
	if old == nil {
		t.Fatal("missing snapshot")
	}
	defer func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	}()
	batch := d.NewBatch()
	if err := batch.Set(algorithmKey(2), algorithmValue(1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	value, err := old.Get(algorithmKey(2))
	if err != nil || !algorithmValid(value, 1, 0) {
		t.Fatal("old generation changed", err)
	}
	current, err := d.GetMany([][]byte{algorithmKey(2), algorithmKey(3), algorithmKey(2)})
	if err != nil || len(current) != 3 || !algorithmValid(current[0], 1, 1) || current[1] != nil || !algorithmValid(current[2], 1, 1) {
		t.Fatal("new generation invalid", err)
	}
	current[0][16] = 'x'
	value, err = d.Get(algorithmKey(2))
	if err != nil || !algorithmValid(value, 1, 1) {
		t.Fatal("owned output mutation reached storage", err)
	}
}

// The child exits after a synchronous acknowledgement without Close or another
// checkpoint. This is process-crash replay proof, not power-loss simulation.
func TestAlgorithmWorkAcknowledgedReplay(t *testing.T) {
	if dir := os.Getenv("TREEDB_ALGORITHM_CRASH_DIR"); dir != "" {
		d := algorithmFixture(t, algorithmOptions(dir, true, false), 256)
		batch := d.NewBatch()
		for i := 0; i < 64; i++ {
			id := i * 7919 % 256
			if err := batch.Set(algorithmKey(id*2), algorithmValue(id, 1)); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
		stats := algorithmStats(d.Stats())
		if algorithmUint(stats, "treedb.command_wal.live_accepted_max_lsn") <= algorithmUint(stats, "treedb.command_wal.applied_lsn") {
			t.Fatal("fixture checkpointed acknowledged updates before crash")
		}
		os.Exit(0)
	}
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestAlgorithmWorkAcknowledgedReplay$", "-test.count=1")
	cmd.Env = append(os.Environ(), "TREEDB_ALGORITHM_CRASH_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v %s", err, output)
	}
	d, err := Open(algorithmOptions(dir, true, false))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	algorithmVerify(t, d, 256, 64)
	stats := algorithmStats(d.Stats())
	if algorithmUint(stats, "treedb.command_wal.applied_lsn") < algorithmUint(stats, "treedb.command_wal.max_lsn") {
		t.Fatal("replay incomplete")
	}
}
