//go:build vlogprof

package main

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/kvstore"
)

func waitForDictAppliedStrict(db kvstore.DB, timeout time.Duration) bool {
	const interval = 50 * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats := getTreeDBStats(db)
		if parseUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id") != 0 {
			return true
		}
		// If dict compression is paused before a dict is applied, return false so
		// callers can decide whether to continue warming or fail.
		if parseUint(stats, "treedb.cache.vlog_dict.pause_remaining_bytes") != 0 {
			return false
		}
		time.Sleep(interval)
	}
	stats := getTreeDBStats(db)
	return parseUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id") != 0
}

func TestProfileVlogDict_WALOff_DictOn_Ultra_1024(t *testing.T) {
	// Steady-state CPU profile (post-warmup) for:
	// - wal_off (DisableWAL=true)
	// - dict enabled
	// - ultra_compressible_repeat, 1KiB values
	//
	// Run:
	//   go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_WALOff_DictOn_Ultra_1024 -count=1 -v
	// Optional:
	//   VLOG_DICT_CPUPROFILE=/tmp/vlog_dict_cpu.pprof
	const (
		warmupBytes  = int64(16 << 20)  // trains + publishes dict
		measureBytes = int64(512 << 20) // enough samples for stable profiles
		batchSize    = 1000
	)

	restore := snapshotTreeDBFlags()
	defer restore.restore()

	dictBytes := 40 << 10
	if raw := os.Getenv("VLOG_DICT_BYTES"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			dictBytes = v
		}
	}
	dictOff := os.Getenv("VLOG_DICT_DISABLE") == "1"

	tc := valueLogDictSuiteCase{
		mode:     "wal_off",
		dictOn:   !dictOff,
		pattern:  "ultra_compressible_repeat",
		valueSz:  1024,
		trainB:   4 << 20,
		dictB:    dictBytes,
		warmupB:  warmupBytes,
		measureB: measureBytes,
	}
	if dictOff {
		tc.trainB = -1
		tc.dictB = 0
	}
	applyValueLogDictSuiteFlags(tc)

	factory, err := GetDBFactory("treedb")
	if err != nil {
		t.Fatalf("GetDBFactory(treedb): %v", err)
	}
	dir := t.TempDir()

	db, err := factory(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	closeDB := func() {
		if db != nil {
			_ = db.Close()
			db = nil
		}
	}
	defer closeDB()

	batcher, ok := db.(kvstore.Batcher)
	if !ok {
		t.Fatalf("treedb does not implement kvstore.Batcher")
	}

	warmupKeys := int((tc.warmupB + int64(tc.valueSz) - 1) / int64(tc.valueSz))
	measureKeys := int((tc.measureB + int64(tc.valueSz) - 1) / int64(tc.valueSz))

	values := makeValuePool(1, tc.pattern, tc.valueSz, 2048)
	keyBase := 0
	valPos := 0
	valPos, err = writeBatches(batcher, keyBase, warmupKeys, batchSize, values, valPos)
	if err != nil {
		t.Fatalf("warmup write: %v", err)
	}
	keyBase += warmupKeys

	if !dictOff {
		if ok := waitForDictAppliedStrict(db, 10*time.Second); !ok {
			// Dict training is gated by sampling stride and queue limits; under
			// high-throughput conditions it may not reach the training target during
			// the first warmup window. Extend warmup in bounded chunks until the dict
			// is applied.
			const extraWarmupBytesMax = int64(256 << 20)
			const extraWarmupChunk = int64(16 << 20)
			extraBytes := int64(0)
			for extraBytes < extraWarmupBytesMax && !waitForDictAppliedStrict(db, 250*time.Millisecond) {
				chunkKeys := int((extraWarmupChunk + int64(tc.valueSz) - 1) / int64(tc.valueSz))
				valPos, err = writeBatches(batcher, keyBase, chunkKeys, batchSize, values, valPos)
				if err != nil {
					t.Fatalf("extra warmup write: %v", err)
				}
				keyBase += chunkKeys
				extraBytes += extraWarmupChunk
			}
		}
		stats := getTreeDBStats(db)
		if parseUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id") == 0 {
			t.Fatalf("expected dict to be applied after warmup stats=%v", stats)
		}
	}

	profilePath := os.Getenv("VLOG_DICT_CPUPROFILE")
	if profilePath == "" {
		profilePath = "/tmp/vlog_dict_wal_off_ultra_1024_cpu.pprof"
	}
	f, err := os.Create(profilePath)
	if err != nil {
		t.Fatalf("create cpu profile: %v", err)
	}
	defer func() { _ = f.Close() }()

	// Reduce log noise in the profile.
	oldLogOut := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldLogOut)

	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatalf("StartCPUProfile: %v", err)
	}
	start := time.Now()
	_, err = writeBatches(batcher, keyBase, measureKeys, batchSize, values, valPos)
	pprof.StopCPUProfile()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("measure write: %v", err)
	}

	opsPerSec := float64(measureKeys) / elapsed.Seconds()
	mbPerSec := float64(measureKeys*tc.valueSz) / elapsed.Seconds() / (1024 * 1024)

	// Force a value-log flush by appending additional data beyond the append buffer.
	const flushBytes = int64(32 << 20)
	flushKeys := int((flushBytes + int64(tc.valueSz) - 1) / int64(tc.valueSz))
	if flushKeys > 0 {
		if _, err := writeBatches(batcher, keyBase+measureKeys, flushKeys, batchSize, values, valPos); err != nil {
			t.Fatalf("flush write: %v", err)
		}
	}

	if cp, ok := db.(interface{ Checkpoint() error }); ok {
		if err := cp.Checkpoint(); err != nil {
			t.Fatalf("checkpoint before read: %v", err)
		}
	}
	closeDB()
	db, err = factory(dir)
	if err != nil {
		t.Fatalf("reopen for read: %v", err)
	}

	readStart := time.Now()
	var key [8]byte
	for i := 0; i < measureKeys; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(keyBase+i))
		if _, err := db.Get(key[:]); err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	readElapsed := time.Since(readStart)
	readOps := float64(measureKeys) / readElapsed.Seconds()
	readMB := float64(measureKeys*tc.valueSz) / readElapsed.Seconds() / (1024 * 1024)

	t.Logf("write elapsed=%s ops/s=%.0f MB/s=%.1f profile=%s", elapsed, opsPerSec, mbPerSec, profilePath)
	t.Logf("read elapsed=%s ops/s=%.0f MB/s=%.1f", readElapsed, readOps, readMB)
}

func TestProfileVlogDict_WALOn_DictOn_Ultra_1024(t *testing.T) {
	// Steady-state CPU profile (post-warmup) for:
	// - wal_on (DisableWAL=false)
	// - dict enabled
	// - ultra_compressible_repeat, 1KiB values
	//
	// Run:
	//   go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_WALOn_DictOn_Ultra_1024 -count=1 -v
	// Optional:
	//   VLOG_DICT_CPUPROFILE=/tmp/vlog_dict_cpu.pprof
	const (
		warmupBytes  = int64(16 << 20)
		measureBytes = int64(512 << 20)
		batchSize    = 1000 // below streaming cutoff (1MiB, 4096 entries)
	)

	restore := snapshotTreeDBFlags()
	defer restore.restore()

	dictBytes := 40 << 10
	if raw := os.Getenv("VLOG_DICT_BYTES"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			dictBytes = v
		}
	}
	dictOff := os.Getenv("VLOG_DICT_DISABLE") == "1"

	tc := valueLogDictSuiteCase{
		mode:     "wal_on",
		dictOn:   !dictOff,
		pattern:  "ultra_compressible_repeat",
		valueSz:  1024,
		trainB:   4 << 20,
		dictB:    dictBytes,
		warmupB:  warmupBytes,
		measureB: measureBytes,
	}
	if dictOff {
		tc.trainB = -1
		tc.dictB = 0
	}
	applyValueLogDictSuiteFlags(tc)

	factory, err := GetDBFactory("treedb")
	if err != nil {
		t.Fatalf("GetDBFactory(treedb): %v", err)
	}
	dir := t.TempDir()

	db, err := factory(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	closeDB := func() {
		if db != nil {
			_ = db.Close()
			db = nil
		}
	}
	defer closeDB()

	batcher, ok := db.(kvstore.Batcher)
	if !ok {
		t.Fatalf("treedb does not implement kvstore.Batcher")
	}

	warmupKeys := int((tc.warmupB + int64(tc.valueSz) - 1) / int64(tc.valueSz))
	measureKeys := int((tc.measureB + int64(tc.valueSz) - 1) / int64(tc.valueSz))

	values := makeValuePool(1, tc.pattern, tc.valueSz, 2048)
	keyBase := 0
	valPos := 0
	valPos, err = writeBatches(batcher, keyBase, warmupKeys, batchSize, values, valPos)
	if err != nil {
		t.Fatalf("warmup write: %v", err)
	}
	keyBase += warmupKeys

	if !dictOff {
		if ok := waitForDictAppliedStrict(db, 10*time.Second); !ok {
			const extraWarmupBytesMax = int64(256 << 20)
			const extraWarmupChunk = int64(16 << 20)
			extraBytes := int64(0)
			for extraBytes < extraWarmupBytesMax && !waitForDictAppliedStrict(db, 250*time.Millisecond) {
				chunkKeys := int((extraWarmupChunk + int64(tc.valueSz) - 1) / int64(tc.valueSz))
				valPos, err = writeBatches(batcher, keyBase, chunkKeys, batchSize, values, valPos)
				if err != nil {
					t.Fatalf("extra warmup write: %v", err)
				}
				keyBase += chunkKeys
				extraBytes += extraWarmupChunk
			}
		}
		stats := getTreeDBStats(db)
		if parseUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id") == 0 {
			t.Fatalf("expected dict to be applied after warmup stats=%v", stats)
		}
	}

	profilePath := os.Getenv("VLOG_DICT_CPUPROFILE")
	if profilePath == "" {
		profilePath = "/tmp/vlog_dict_wal_on_ultra_1024_cpu.pprof"
	}
	f, err := os.Create(profilePath)
	if err != nil {
		t.Fatalf("create cpu profile: %v", err)
	}
	defer func() { _ = f.Close() }()

	oldLogOut := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldLogOut)

	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatalf("StartCPUProfile: %v", err)
	}
	start := time.Now()
	_, err = writeBatches(batcher, keyBase, measureKeys, batchSize, values, valPos)
	pprof.StopCPUProfile()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("measure write: %v", err)
	}

	opsPerSec := float64(measureKeys) / elapsed.Seconds()
	mbPerSec := float64(measureKeys*tc.valueSz) / elapsed.Seconds() / (1024 * 1024)

	// Force a value-log flush by appending additional data beyond the append buffer.
	const flushBytes = int64(32 << 20)
	flushKeys := int((flushBytes + int64(tc.valueSz) - 1) / int64(tc.valueSz))
	if flushKeys > 0 {
		if _, err := writeBatches(batcher, keyBase+measureKeys, flushKeys, batchSize, values, valPos); err != nil {
			t.Fatalf("flush write: %v", err)
		}
	}

	if cp, ok := db.(interface{ Checkpoint() error }); ok {
		if err := cp.Checkpoint(); err != nil {
			t.Fatalf("checkpoint before read: %v", err)
		}
	}
	closeDB()
	db, err = factory(dir)
	if err != nil {
		t.Fatalf("reopen for read: %v", err)
	}

	readStart := time.Now()
	var key [8]byte
	for i := 0; i < measureKeys; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(keyBase+i))
		if _, err := db.Get(key[:]); err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	readElapsed := time.Since(readStart)
	readOps := float64(measureKeys) / readElapsed.Seconds()
	readMB := float64(measureKeys*tc.valueSz) / readElapsed.Seconds() / (1024 * 1024)

	t.Logf("write elapsed=%s ops/s=%.0f MB/s=%.1f profile=%s", elapsed, opsPerSec, mbPerSec, profilePath)
	t.Logf("read elapsed=%s ops/s=%.0f MB/s=%.1f", readElapsed, readOps, readMB)
}
