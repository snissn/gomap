package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/snissn/gomap/kvstore"
)

type valueLogDictSuiteCase struct {
	mode     string // wal_on (journal enabled) or wal_off (DisableWAL)
	dictOn   bool
	pattern  string
	valueSz  int
	trainB   int
	dictB    int
	warmupB  int64
	measureB int64
}

type valueLogDictSuiteResult struct {
	mode    string
	dict    string
	pattern string
	valueSz int

	warmupKeys  int
	measureKeys int

	opsPerSec float64
	mbPerSec  float64

	observedRatioWarmup  float64
	observedRatioTotal   float64
	observedRatioMeasure float64

	attemptedFrac float64
	keptFrac      float64
	dictID        uint64
	k             uint64
	pauseBytes    int64

	indexBytes  int64
	dictdbBytes int64
	walBytes    walDirBytes
	walMeasure  walDirBytes
}

type walDirBytes struct {
	Commit int64
	WAL    int64
	Value  int64
	Other  int64
	Total  int64
}

type dictFrameCounters struct {
	Total     uint64
	Attempted uint64
	Kept      uint64
}

func runValueLogDictSuite(baseCfg BenchConfig) (string, error) {
	// Keep this suite quick and focused: small warmup to trigger dict training,
	// then a steady-state write phase for throughput + metrics.
	const (
		// Warmup is written before a dict becomes active. For fast write paths,
		// warmup can complete long before background training/publish finishes,
		// so the warmup bytes are often effectively "uncompressed". Keep warmup
		// small so overall ratios don't get dominated by this fixed window.
		warmupBytes  = int64(4 << 20)  // 4MiB
		measureBytes = int64(32 << 20) // 32MiB
	)

	cases := []valueLogDictSuiteCase{
		// WAL on: journal enabled.
		{mode: "wal_on", dictOn: false, pattern: "ultra_compressible_repeat", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: true, pattern: "ultra_compressible_repeat", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: false, pattern: "highly_compressible_tail64", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: true, pattern: "highly_compressible_tail64", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: false, pattern: "incompressible", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: true, pattern: "incompressible", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: false, pattern: "ultra_compressible_repeat", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: true, pattern: "ultra_compressible_repeat", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: false, pattern: "highly_compressible_tail64", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: true, pattern: "highly_compressible_tail64", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: false, pattern: "incompressible", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_on", dictOn: true, pattern: "incompressible", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},

		// WAL off: journal disabled (value-log pointers still enabled). Unsafe; requires AllowUnsafe.
		{mode: "wal_off", dictOn: false, pattern: "ultra_compressible_repeat", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: true, pattern: "ultra_compressible_repeat", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: false, pattern: "highly_compressible_tail64", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: true, pattern: "highly_compressible_tail64", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: false, pattern: "incompressible", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: true, pattern: "incompressible", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: false, pattern: "ultra_compressible_repeat", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: true, pattern: "ultra_compressible_repeat", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: false, pattern: "highly_compressible_tail64", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: true, pattern: "highly_compressible_tail64", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: false, pattern: "incompressible", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "wal_off", dictOn: true, pattern: "incompressible", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
	}

	results := make([]valueLogDictSuiteResult, 0, len(cases))
	seed := int64(baseCfg.SeedUsed)
	batchSize := baseCfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	orig := snapshotTreeDBFlags()
	defer orig.restore()

	for _, tc := range cases {
		res, err := runValueLogDictSuiteCase(tc, seed, batchSize)
		if err != nil {
			return "", err
		}
		results = append(results, res)
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: vlog_dict\n\n")
	sb.WriteString(fmt.Sprintf("- seed: %d\n", seed))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", batchSize))
	sb.WriteString(fmt.Sprintf("- warmup bytes: %s\n", formatFloat(float64(warmupBytes))))
	sb.WriteString(fmt.Sprintf("- measure bytes: %s\n\n", formatFloat(float64(measureBytes))))

	sb.WriteString("| wal | dict | pattern | valsize | ops/sec | MB/s | observed_ratio_warmup | observed_ratio_total | observed_ratio_measure | attempted_frac | kept_frac | dict_id | k | pause_bytes | wal_commit_bytes_total | wal_value_bytes_total | wal_value_bytes_measure | wal_total_bytes_total | index_bytes | dictdb_bytes |\n")
	sb.WriteString("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, r := range results {
		sb.WriteString("| ")
		sb.WriteString(r.mode)
		sb.WriteString(" | ")
		sb.WriteString(r.dict)
		sb.WriteString(" | ")
		sb.WriteString(r.pattern)
		sb.WriteString(" | ")
		sb.WriteString(fmt.Sprintf("%d", r.valueSz))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(r.opsPerSec))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(r.mbPerSec))
		sb.WriteString(" | ")
		if math.IsNaN(r.observedRatioWarmup) {
			sb.WriteString("-")
		} else {
			sb.WriteString(fmt.Sprintf("%.6f", r.observedRatioWarmup))
		}
		sb.WriteString(" | ")
		if math.IsNaN(r.observedRatioTotal) {
			sb.WriteString("-")
		} else {
			sb.WriteString(fmt.Sprintf("%.6f", r.observedRatioTotal))
		}
		sb.WriteString(" | ")
		if math.IsNaN(r.observedRatioMeasure) {
			sb.WriteString("-")
		} else {
			sb.WriteString(fmt.Sprintf("%.6f", r.observedRatioMeasure))
		}
		sb.WriteString(" | ")
		if math.IsNaN(r.attemptedFrac) {
			sb.WriteString("-")
		} else {
			sb.WriteString(fmt.Sprintf("%.6f", r.attemptedFrac))
		}
		sb.WriteString(" | ")
		if math.IsNaN(r.keptFrac) {
			sb.WriteString("-")
		} else {
			sb.WriteString(fmt.Sprintf("%.6f", r.keptFrac))
		}
		sb.WriteString(" | ")
		sb.WriteString(fmt.Sprintf("%d", r.dictID))
		sb.WriteString(" | ")
		sb.WriteString(fmt.Sprintf("%d", r.k))
		sb.WriteString(" | ")
		sb.WriteString(fmt.Sprintf("%d", r.pauseBytes))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(float64(r.walBytes.Commit)))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(float64(r.walBytes.Value)))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(float64(r.walMeasure.Value)))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(float64(r.walBytes.Total)))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(float64(r.indexBytes)))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(float64(r.dictdbBytes)))
		sb.WriteString(" |\n")
	}
	sb.WriteString("\n")

	return sb.String(), nil
}

func runValueLogDictSuiteCase(tc valueLogDictSuiteCase, seed int64, batchSize int) (valueLogDictSuiteResult, error) {
	restore := snapshotTreeDBFlags()
	defer restore.restore()

	applyValueLogDictSuiteFlags(tc)

	factory, err := GetDBFactory("treedb")
	if err != nil {
		return valueLogDictSuiteResult{}, err
	}
	dir, err := os.MkdirTemp("", "bench-vlog-dict-*")
	if err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	db, err := factory(dir)
	if err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: open: %w", err)
	}
	defer func() { _ = db.Close() }()

	batcher, ok := db.(kvstore.Batcher)
	if !ok {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: treedb does not implement kvstore.Batcher")
	}

	warmupKeys := int((tc.warmupB + int64(tc.valueSz) - 1) / int64(tc.valueSz))
	measureKeys := int((tc.measureB + int64(tc.valueSz) - 1) / int64(tc.valueSz))

	poolSize := 2048
	if strings.Contains(strings.ToLower(tc.pattern), "incompressible") {
		// Avoid repeated random values for "incompressible" cases: a small pool can
		// accidentally introduce compressibility. Use a pool large enough to cover
		// the full warmup+measure stream with unique values.
		poolSize = warmupKeys + measureKeys
	}
	values := makeValuePool(seed, tc.pattern, tc.valueSz, poolSize)

	keyBase := 0
	valPos := 0
	valPos, err = writeBatches(batcher, keyBase, warmupKeys, batchSize, values, valPos)
	if err != nil {
		return valueLogDictSuiteResult{}, err
	}
	keyBase += warmupKeys

	if tc.dictOn {
		// Best-effort wait for a dictionary to be trained + published.
		if err := waitForDictPublish(db, 5*time.Second); err != nil {
			return valueLogDictSuiteResult{}, err
		}
	}

	// Ensure warmup bytes are flushed to disk before we snapshot "before" sizes.
	// For small values the value-log writer can keep warmup in an in-memory
	// append buffer until later, which makes observed_ratio_warmup/measure
	// misleading.
	if cp, ok := db.(interface{ Checkpoint() error }); ok {
		if err := cp.Checkpoint(); err != nil {
			return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: warmup checkpoint: %w", err)
		}
	}

	walBefore, err := walDirBreakdown(filepath.Join(dir, "maindb", "wal"))
	if err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: wal size (before): %w", err)
	}

	stats0 := getTreeDBStats(db)
	c0 := parseDictCounters(stats0)

	start := time.Now()
	valPos, err = writeBatches(batcher, keyBase, measureKeys, batchSize, values, valPos)
	if err != nil {
		return valueLogDictSuiteResult{}, err
	}
	elapsed := time.Since(start)

	stats1 := getTreeDBStats(db)
	c1 := parseDictCounters(stats1)

	dictID := parseUint(stats1, "treedb.cache.vlog_dict.last_applied_dict_id")
	k := parseUint(stats1, "treedb.cache.vlog_dict.current_k")
	pauseBytes := parseInt64(stats1, "treedb.cache.vlog_dict.pause_remaining_bytes")

	totalDelta := c1.Total - c0.Total
	attemptedDelta := c1.Attempted - c0.Attempted
	keptDelta := c1.Kept - c0.Kept

	attemptedFrac := math.NaN()
	keptFrac := math.NaN()
	if totalDelta > 0 {
		attemptedFrac = float64(attemptedDelta) / float64(totalDelta)
		keptFrac = float64(keptDelta) / float64(totalDelta)
	}

	opsPerSec := float64(measureKeys) / elapsed.Seconds()
	mbPerSec := float64(measureKeys*tc.valueSz) / elapsed.Seconds() / (1024 * 1024)

	// Close to flush buffers before inspecting on-disk sizes.
	if err := db.Close(); err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: close: %w", err)
	}

	walAfter, err := walDirBreakdown(filepath.Join(dir, "maindb", "wal"))
	if err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: wal size (after): %w", err)
	}

	indexBytes, err := fileSize(filepath.Join(dir, "maindb", "index.db"))
	if err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: index.db size: %w", err)
	}
	dictdbBytes, err := dirSize(filepath.Join(dir, "dictdb"))
	if err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: dictdb size: %w", err)
	}

	rawBytesWarmup := int64(warmupKeys * tc.valueSz)
	rawBytesMeasure := int64(measureKeys * tc.valueSz)
	rawBytesTotal := rawBytesWarmup + rawBytesMeasure

	observedRatioWarmup := math.NaN()
	if rawBytesWarmup > 0 && walBefore.Value > 0 {
		observedRatioWarmup = float64(walBefore.Value) / float64(rawBytesWarmup)
	}
	observedRatioTotal := math.NaN()
	if rawBytesTotal > 0 && walAfter.Value > 0 {
		observedRatioTotal = float64(walAfter.Value) / float64(rawBytesTotal)
	}
	observedRatioMeasure := math.NaN()
	if rawBytesMeasure > 0 && walAfter.Value > 0 && walAfter.Value >= walBefore.Value {
		observedRatioMeasure = float64(walAfter.Value-walBefore.Value) / float64(rawBytesMeasure)
	}

	return valueLogDictSuiteResult{
		mode:                 tc.mode,
		dict:                 boolLabel(tc.dictOn, "on", "off"),
		pattern:              tc.pattern,
		valueSz:              tc.valueSz,
		warmupKeys:           warmupKeys,
		measureKeys:          measureKeys,
		opsPerSec:            opsPerSec,
		mbPerSec:             mbPerSec,
		observedRatioWarmup:  observedRatioWarmup,
		observedRatioTotal:   observedRatioTotal,
		observedRatioMeasure: observedRatioMeasure,
		attemptedFrac:        attemptedFrac,
		keptFrac:             keptFrac,
		dictID:               dictID,
		k:                    k,
		pauseBytes:           pauseBytes,
		indexBytes:           indexBytes,
		dictdbBytes:          dictdbBytes,
		walBytes:             walAfter,
		walMeasure: walDirBytes{
			Commit: walAfter.Commit - walBefore.Commit,
			WAL:    walAfter.WAL - walBefore.WAL,
			Value:  walAfter.Value - walBefore.Value,
			Other:  walAfter.Other - walBefore.Other,
			Total:  walAfter.Total - walBefore.Total,
		},
	}, nil
}

func makeValuePool(seed int64, pattern string, size int, poolSize int) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	if poolSize <= 0 {
		poolSize = 1
	}
	out := make([][]byte, poolSize)
	mode := strings.ToLower(strings.TrimSpace(pattern))
	for i := 0; i < poolSize; i++ {
		buf := make([]byte, size)
		switch mode {
		case "zero", "zeros":
			// Leave zeroed.
		case "ultra_compressible", "ultra_compressible_repeat":
			// Keep values ultra-compressible but not identical so dict training triggers.
			// A tiny random tail avoids degenerate "all samples identical" dictionaries
			// while keeping the stream near the best-case for compression ratios.
			fillRepeatTail(rng, buf, 4, []byte("{\"key\":\"value\",\"active\":true}"))
		case "highly_compressible_notail":
			fillRepeatTail(rng, buf, 0, []byte("{\"key\":\"value\",\"active\":true}"))
		case "", "repeat", "repeat_tail64", "highly_compressible", "highly_compressible_tail64":
			fillRepeatTail(rng, buf, 64, []byte("{\"key\":\"value\",\"active\":true}"))
		case "half_repeat_half_random":
			fillRepeatTail(rng, buf, 0, []byte("{\"key\":\"value\",\"active\":true}"))
			if len(buf) > 0 {
				half := len(buf) / 2
				_, _ = rng.Read(buf[half:])
			}
		case "medium_compressible", "medium_compressible_sparse":
			fillSparseNoise(rng, buf, 256, 16, []byte("abcd1234"))
		case "celestia_height_prefix_fill":
			fillCelestiaHeightPrefix(buf, i)
		case "incompressible", "random":
			_, _ = rng.Read(buf)
		default:
			_, _ = rng.Read(buf)
		}
		out[i] = buf
	}
	return out
}

func fillCelestiaHeightPrefix(dst []byte, idx int) {
	if len(dst) == 0 {
		return
	}
	for i := range dst {
		dst[i] = 'a'
	}
	prefix := append([]byte("celestia/height/"), strconv.AppendInt(nil, int64(idx), 10)...)
	if len(prefix) >= len(dst) {
		copy(dst, prefix[:len(dst)])
		return
	}
	copy(dst, prefix)
}

func fillRepeatTail(rng *rand.Rand, dst []byte, tail int, pattern []byte) {
	if len(dst) == 0 {
		return
	}
	for i := 0; i < len(dst); {
		n := copy(dst[i:], pattern)
		i += n
	}
	if tail <= 0 {
		return
	}
	if tail > len(dst) {
		tail = len(dst)
	}
	if tail > 0 {
		_, _ = rng.Read(dst[len(dst)-tail:])
	}
}

func fillSparseNoise(rng *rand.Rand, dst []byte, stride, noise int, pattern []byte) {
	fillRepeatTail(rng, dst, 0, pattern)
	if stride <= 0 {
		stride = 256
	}
	if noise <= 0 {
		noise = 16
	}
	for off := 0; off < len(dst); off += stride {
		end := off + noise
		if end > len(dst) {
			end = len(dst)
		}
		_, _ = rng.Read(dst[off:end])
	}
}

func writeBatches(batcher kvstore.Batcher, keyBase, count, batchSize int, values [][]byte, valPos int) (int, error) {
	total := count
	var k [8]byte
	for i := 0; i < total; i += batchSize {
		batch, err := batcher.NewBatch()
		if err != nil {
			return 0, fmt.Errorf("vlog_dict: new batch: %w", err)
		}
		end := i + batchSize
		if end > total {
			end = total
		}
		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(k[:], uint64(keyBase+j))
			value := values[valPos%len(values)]
			valPos++
			if err := batch.Set(k[:], value); err != nil {
				_ = batch.Close()
				return 0, fmt.Errorf("vlog_dict: set: %w", err)
			}
		}
		if err := batch.Commit(); err != nil {
			_ = batch.Close()
			return 0, fmt.Errorf("vlog_dict: commit: %w", err)
		}
		if err := batch.Close(); err != nil {
			return 0, fmt.Errorf("vlog_dict: close batch: %w", err)
		}
	}
	return valPos, nil
}

func waitForDictPublish(db kvstore.DB, timeout time.Duration) error {
	const (
		interval = 50 * time.Millisecond
	)
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats := getTreeDBStats(db)
		if parseUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id") != 0 {
			return nil
		}
		// If dict compression is paused before a dict is published (common on
		// incompressible streams), don't burn the full timeout waiting.
		if parseUint(stats, "treedb.cache.vlog_dict.pause_remaining_bytes") != 0 {
			return nil
		}
		time.Sleep(interval)
	}
	// Some workloads may legitimately refuse to publish dicts (e.g. no-op dict,
	// invalid dict). Do not fail the suite.
	return nil
}

func walDirBreakdown(path string) (walDirBytes, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return walDirBytes{}, nil
		}
		return walDirBytes{}, err
	}

	var b walDirBytes
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return walDirBytes{}, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		size := info.Size()
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, "commit-"):
			b.Commit += size
		case strings.HasPrefix(name, "wal-"):
			b.WAL += size
		case strings.HasPrefix(name, "value-") || strings.HasPrefix(name, "vlog-"):
			b.Value += size
		default:
			b.Other += size
		}
	}
	b.Total = b.Commit + b.WAL + b.Value + b.Other
	return b, nil
}

func boolLabel(v bool, t, f string) string {
	if v {
		return t
	}
	return f
}

type treedbFlagSnapshot struct {
	disableWAL                 bool
	relaxedSync                bool
	disableReadChecksum        bool
	allowUnsafe                bool
	valueLogThreshold          int
	journalLanes               int
	vlogCompressionAutotune    string
	vlogDictTrainBytes         int
	vlogDictDictBytes          int
	vlogDictAdaptiveRatio      float64
	vlogDictMetricsWindowBytes int
	vlogDictMetricsMinRecords  int
	vlogDictMetricsPauseBytes  int
	vlogDictMinSavingsRatio    float64
}

func snapshotTreeDBFlags() treedbFlagSnapshot {
	return treedbFlagSnapshot{
		disableWAL:                 *treedbDisableWAL,
		relaxedSync:                *treedbRelaxedSync,
		disableReadChecksum:        *treedbDisableReadChecksum,
		allowUnsafe:                *treedbAllowUnsafe,
		valueLogThreshold:          *treedbValueLogThreshold,
		journalLanes:               *treedbJournalLanes,
		vlogCompressionAutotune:    *treedbVlogCompressionAutotune,
		vlogDictTrainBytes:         *treedbVlogDictTrainBytes,
		vlogDictDictBytes:          *treedbVlogDictDictBytes,
		vlogDictAdaptiveRatio:      *treedbVlogDictAdaptiveRatio,
		vlogDictMetricsWindowBytes: *treedbVlogDictMetricsWindow,
		vlogDictMetricsMinRecords:  *treedbVlogDictMetricsMinRecords,
		vlogDictMetricsPauseBytes:  *treedbVlogDictMetricsPauseBytes,
		vlogDictMinSavingsRatio:    *treedbVlogDictMinSavingsRatio,
	}
}

func (s treedbFlagSnapshot) restore() {
	*treedbDisableWAL = s.disableWAL
	*treedbRelaxedSync = s.relaxedSync
	*treedbDisableReadChecksum = s.disableReadChecksum
	*treedbAllowUnsafe = s.allowUnsafe
	*treedbValueLogThreshold = s.valueLogThreshold
	*treedbJournalLanes = s.journalLanes
	*treedbVlogCompressionAutotune = s.vlogCompressionAutotune
	*treedbVlogDictTrainBytes = s.vlogDictTrainBytes
	*treedbVlogDictDictBytes = s.vlogDictDictBytes
	*treedbVlogDictAdaptiveRatio = s.vlogDictAdaptiveRatio
	*treedbVlogDictMetricsWindow = s.vlogDictMetricsWindowBytes
	*treedbVlogDictMetricsMinRecords = s.vlogDictMetricsMinRecords
	*treedbVlogDictMetricsPauseBytes = s.vlogDictMetricsPauseBytes
	*treedbVlogDictMinSavingsRatio = s.vlogDictMinSavingsRatio
}

func applyValueLogDictSuiteFlags(tc valueLogDictSuiteCase) {
	// Keep the value-log path enabled.
	*treedbDisableWAL = false
	*treedbValueLogThreshold = 1

	// Favor throughput to isolate compression overhead.
	*treedbRelaxedSync = true
	*treedbDisableReadChecksum = true
	*treedbAllowUnsafe = true

	// Keep lanes at default unless user overrides.
	if *treedbJournalLanes <= 0 {
		*treedbJournalLanes = 0
	}

	switch tc.mode {
	case "wal_off":
		*treedbDisableWAL = true
	case "wal_on":
		*treedbDisableWAL = false
	default:
		*treedbDisableWAL = false
	}

	// The suite expects to compare dict compression on/off. Ensure TreeDB's
	// compression autotune is enabled when dict is on, and disabled when dict is
	// off, regardless of global CLI defaults.
	if tc.dictOn {
		*treedbVlogCompressionAutotune = "medium"
	} else {
		*treedbVlogCompressionAutotune = "off"
	}

	*treedbVlogDictTrainBytes = tc.trainB
	if tc.dictOn {
		*treedbVlogDictDictBytes = tc.dictB
	} else {
		*treedbVlogDictDictBytes = 0
	}
}

func getTreeDBStats(db kvstore.DB) map[string]string {
	if db == nil {
		return nil
	}
	if s, ok := db.(interface{ Stats() map[string]string }); ok {
		return s.Stats()
	}
	return nil
}

func parseDictCounters(stats map[string]string) dictFrameCounters {
	return dictFrameCounters{
		Total:     parseUint(stats, "treedb.cache.vlog_dict.frames_total"),
		Attempted: parseUint(stats, "treedb.cache.vlog_dict.frames_attempted"),
		Kept:      parseUint(stats, "treedb.cache.vlog_dict.frames_kept"),
	}
}

func parseUint(stats map[string]string, key string) uint64 {
	if stats == nil {
		return 0
	}
	raw, ok := stats[key]
	if !ok {
		return 0
	}
	n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseInt64(stats map[string]string, key string) int64 {
	if stats == nil {
		return 0
	}
	raw, ok := stats[key]
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return n
}
