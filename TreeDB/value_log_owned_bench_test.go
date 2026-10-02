package treedb

import (
	"bytes"
	"encoding/binary"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/tree"
)

// This reuses the canonical pointer fixture; setup and warmup are outside the
// timed region. Cold route setup is measured separately by ReadAppendOwned.
func BenchmarkDBOwnedValueLogRoute(b *testing.B) {
	if os.Getenv("TREEDB_HOT_PATH_STATS") == "" {
		b.Fatal("TREEDB_HOT_PATH_STATS=1 is required to verify pointer placement")
	}
	db, keys := openSnapshotValueLogBenchDB(b)
	dir := db.dir
	if err := db.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	var err error
	db, err = Open(Options{Dir: dir, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	runtime.GC()
	var cold, warm runtime.MemStats
	runtime.ReadMemStats(&cold)
	want := make([]byte, 256)
	for i := range want {
		want[i] = byte(i)
	}
	for i, key := range keys {
		binary.BigEndian.PutUint64(want[len(want)-8:], uint64(i))
		out, err := db.Get(key)
		if err != nil || !bytes.Equal(out, want) {
			b.Fatalf("fixture value %d: length=%d err=%v", i, len(out), err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&warm)
	before := db.Stats()
	pathBefore := tree.ReadPathStatsSnapshot()
	b.ReportAllocs()
	b.SetBytes(256)
	b.ResetTimer()
	for i := range b.N {
		out, err := db.Get(keys[i%len(keys)])
		if err != nil || len(out) != 256 {
			b.Fatalf("owned Get: length=%d err=%v", len(out), err)
		}
		snapshotReadBenchSink = out
	}
	b.StopTimer()
	after := db.Stats()
	pathAfter := tree.ReadPathStatsSnapshot()
	route := make(map[string]uint64, 4)
	for metric, key := range map[string]string{
		"mmap_hits/op": "treedb.vlog.mmap_read.hits", "fallbacks/op": "treedb.vlog.mmap_read.fallback_readat",
		"cache_hits/op": "treedb.vlog.grouped_frame_cache.hits", "crc_checks/op": "treedb.vlog.read.crc32_checks_total",
	} {
		prior, err := strconv.ParseUint(before[key], 10, 64)
		if err != nil {
			b.Fatalf("missing route counter %s: %v", key, err)
		}
		current, err := strconv.ParseUint(after[key], 10, 64)
		if err != nil {
			b.Fatalf("missing route counter %s: %v", key, err)
		}
		route[metric] = current - prior
		b.ReportMetric(float64(current-prior)/float64(b.N), metric)
	}
	// Outer leaf loads can add value-log reads to the one value pointer per
	// Get. Check placement exactly and classify the observed file routes.
	if pathAfter.GetAppendPointerHitsTotal-pathBefore.GetAppendPointerHitsTotal != uint64(b.N) || pathAfter.GetAppendInlineHitsTotal != pathBefore.GetAppendInlineHitsTotal {
		b.Fatal("fixture no longer performs exactly one pointer read per Get")
	}
	if route["mmap_hits/op"]+route["fallbacks/op"] < uint64(b.N) || route["crc_checks/op"] < uint64(b.N) || route["cache_hits/op"] > route["mmap_hits/op"] {
		b.Fatalf("public route counters are inconsistent: %v", route)
	}
	for metric, key := range map[string]string{
		"retained_raw_B": "treedb.vlog.grouped_frame_cache.retained_bytes",
		"mapped_B":       "treedb.process.memory.vlog_mmap_active_bytes",
	} {
		n, err := strconv.ParseUint(after[key], 10, 64)
		if err != nil {
			b.Fatalf("missing memory counter %s: %v", key, err)
		}
		b.ReportMetric(float64(n), metric)
	}
	b.ReportMetric(float64(pathAfter.GetAppendPointerHitsTotal-pathBefore.GetAppendPointerHitsTotal)/float64(b.N), "pointer_hits/op")
	b.ReportMetric(float64(pathAfter.GetAppendInlineHitsTotal-pathBefore.GetAppendInlineHitsTotal)/float64(b.N), "inline_hits/op")
	b.ReportMetric(float64(int64(warm.HeapAlloc)-int64(cold.HeapAlloc)), "warm_heap_delta_B")
}
