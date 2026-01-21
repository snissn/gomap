//go:build vlogprof

package main

import (
	"io"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/kvstore"
)

func TestProfileVlogDict_Mode4_DictOn_Ultra_1024(t *testing.T) {
	// Steady-state CPU profile (post-warmup) for:
	// - mode4 (DisableJournal=true)
	// - dict enabled
	// - ultra_compressible_repeat, 1KiB values
	//
	// Run:
	//   go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode4_DictOn_Ultra_1024 -count=1 -v
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
		mode:     "mode4",
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
	defer func() { _ = db.Close() }()

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
		if err := waitForDictPublish(db, 10*time.Second); err != nil {
			t.Fatalf("waitForDictPublish: %v", err)
		}
		stats := getTreeDBStats(db)
		if parseUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id") == 0 {
			t.Fatalf("expected dict to be applied after warmup")
		}
	}

	profilePath := os.Getenv("VLOG_DICT_CPUPROFILE")
	if profilePath == "" {
		profilePath = "/tmp/vlog_dict_mode4_ultra_1024_cpu.pprof"
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
	t.Logf("elapsed=%s ops/s=%.0f MB/s=%.1f profile=%s", elapsed, opsPerSec, mbPerSec, profilePath)
}

func TestProfileVlogDict_Mode3_DictOn_Ultra_1024(t *testing.T) {
	// Steady-state CPU profile (post-warmup) for:
	// - mode3 (DisableJournal=false)
	// - dict enabled
	// - ultra_compressible_repeat, 1KiB values
	//
	// Run:
	//   go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=1 -v
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
		mode:     "mode3",
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
	defer func() { _ = db.Close() }()

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
		if err := waitForDictPublish(db, 10*time.Second); err != nil {
			t.Fatalf("waitForDictPublish: %v", err)
		}
		stats := getTreeDBStats(db)
		if parseUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id") == 0 {
			t.Fatalf("expected dict to be applied after warmup")
		}
	}

	profilePath := os.Getenv("VLOG_DICT_CPUPROFILE")
	if profilePath == "" {
		profilePath = "/tmp/vlog_dict_mode3_ultra_1024_cpu.pprof"
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
	t.Logf("elapsed=%s ops/s=%.0f MB/s=%.1f profile=%s", elapsed, opsPerSec, mbPerSec, profilePath)
}
