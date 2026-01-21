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
	mode     string // mode3 (journal on) or mode4 (DisableJournal)
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

	observedRatio float64

	attemptedFrac float64
	keptFrac      float64
	dictID        uint64
	k             uint64
	pauseBytes    int64

	indexBytes  int64
	dictdbBytes int64
	walBytes    walDirBytes
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
		warmupBytes  = int64(16 << 20) // 16MiB
		measureBytes = int64(32 << 20) // 32MiB
	)

	cases := []valueLogDictSuiteCase{
		// Mode 3: journal on.
		{mode: "mode3", dictOn: false, pattern: "highly_compressible_tail64", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode3", dictOn: true, pattern: "highly_compressible_tail64", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode3", dictOn: false, pattern: "incompressible", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode3", dictOn: true, pattern: "incompressible", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode3", dictOn: false, pattern: "highly_compressible_tail64", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode3", dictOn: true, pattern: "highly_compressible_tail64", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode3", dictOn: false, pattern: "incompressible", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode3", dictOn: true, pattern: "incompressible", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},

		// Mode 4: journal disabled (value-log pointers still enabled). Unsafe; requires AllowUnsafe.
		{mode: "mode4", dictOn: false, pattern: "highly_compressible_tail64", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode4", dictOn: true, pattern: "highly_compressible_tail64", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode4", dictOn: false, pattern: "incompressible", valueSz: 1024, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode4", dictOn: true, pattern: "incompressible", valueSz: 1024, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode4", dictOn: false, pattern: "highly_compressible_tail64", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode4", dictOn: true, pattern: "highly_compressible_tail64", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode4", dictOn: false, pattern: "incompressible", valueSz: 16 << 10, trainB: -1, dictB: 0, warmupB: warmupBytes, measureB: measureBytes},
		{mode: "mode4", dictOn: true, pattern: "incompressible", valueSz: 16 << 10, trainB: 4 << 20, dictB: 40 << 10, warmupB: warmupBytes, measureB: measureBytes},
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

	sb.WriteString("| mode | dict | pattern | valsize | ops/sec | MB/s | observed_ratio | attempted_frac | kept_frac | dict_id | k | pause_bytes | wal_commit_bytes | wal_value_bytes | wal_total_bytes | index_bytes | dictdb_bytes |\n")
	sb.WriteString("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
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
		if math.IsNaN(r.observedRatio) {
			sb.WriteString("-")
		} else {
			sb.WriteString(fmt.Sprintf("%.6f", r.observedRatio))
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

	val := makeValuePattern(seed, tc.pattern, tc.valueSz)

	warmupKeys := int((tc.warmupB + int64(tc.valueSz) - 1) / int64(tc.valueSz))
	measureKeys := int((tc.measureB + int64(tc.valueSz) - 1) / int64(tc.valueSz))

	keyBase := 0
	if err := writeBatches(batcher, keyBase, warmupKeys, batchSize, val); err != nil {
		return valueLogDictSuiteResult{}, err
	}
	keyBase += warmupKeys

	if tc.dictOn {
		// Best-effort wait for a dictionary to be trained + applied. If training
		// is async, we may need a small amount of additional writes to observe an
		// applied dict id.
		keyBase, err = ensureDictApplied(db, batcher, keyBase, batchSize, val)
		if err != nil {
			return valueLogDictSuiteResult{}, err
		}
	}

	stats0 := getTreeDBStats(db)
	c0 := parseDictCounters(stats0)

	start := time.Now()
	if err := writeBatches(batcher, keyBase, measureKeys, batchSize, val); err != nil {
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

	indexBytes, err := fileSize(filepath.Join(dir, "maindb", "index.db"))
	if err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: index.db size: %w", err)
	}
	dictdbBytes, err := dirSize(filepath.Join(dir, "dictdb"))
	if err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: dictdb size: %w", err)
	}
	walBytes, err := walDirBreakdown(filepath.Join(dir, "maindb", "wal"))
	if err != nil {
		return valueLogDictSuiteResult{}, fmt.Errorf("vlog_dict: wal size: %w", err)
	}

	rawBytesTotal := int64((keyBase + measureKeys) * tc.valueSz)
	observedRatio := math.NaN()
	if rawBytesTotal > 0 && walBytes.Value > 0 {
		observedRatio = float64(walBytes.Value) / float64(rawBytesTotal)
	}

	return valueLogDictSuiteResult{
		mode:          tc.mode,
		dict:          boolLabel(tc.dictOn, "on", "off"),
		pattern:       tc.pattern,
		valueSz:       tc.valueSz,
		warmupKeys:    warmupKeys,
		measureKeys:   measureKeys,
		opsPerSec:     opsPerSec,
		mbPerSec:      mbPerSec,
		observedRatio: observedRatio,
		attemptedFrac: attemptedFrac,
		keptFrac:      keptFrac,
		dictID:        dictID,
		k:             k,
		pauseBytes:    pauseBytes,
		indexBytes:    indexBytes,
		dictdbBytes:   dictdbBytes,
		walBytes:      walBytes,
	}, nil
}

func makeValuePattern(seed int64, pattern string, size int) []byte {
	rng := rand.New(rand.NewSource(seed))
	buf := make([]byte, size)
	switch strings.ToLower(strings.TrimSpace(pattern)) {
	case "", "repeat", "highly_compressible", "highly_compressible_tail64":
		for i := range buf {
			buf[i] = 0x61
		}
		if len(buf) > 64 {
			_, _ = rng.Read(buf[len(buf)-64:])
		}
	case "medium_compressible", "medium_compressible_sparse":
		for i := range buf {
			buf[i] = 0x61
		}
		if len(buf) > 0 {
			half := len(buf) / 2
			_, _ = rng.Read(buf[half:])
		}
	case "incompressible", "random":
		_, _ = rng.Read(buf)
	default:
		_, _ = rng.Read(buf)
	}
	return buf
}

func writeBatches(batcher kvstore.Batcher, keyBase, count, batchSize int, val []byte) error {
	total := count
	var k [8]byte
	for i := 0; i < total; i += batchSize {
		batch, err := batcher.NewBatch()
		if err != nil {
			return fmt.Errorf("vlog_dict: new batch: %w", err)
		}
		end := i + batchSize
		if end > total {
			end = total
		}
		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(k[:], uint64(keyBase+j))
			if err := batch.Set(k[:], val); err != nil {
				_ = batch.Close()
				return fmt.Errorf("vlog_dict: set: %w", err)
			}
		}
		if err := batch.Commit(); err != nil {
			_ = batch.Close()
			return fmt.Errorf("vlog_dict: commit: %w", err)
		}
		if err := batch.Close(); err != nil {
			return fmt.Errorf("vlog_dict: close batch: %w", err)
		}
	}
	return nil
}

func ensureDictApplied(db kvstore.DB, batcher kvstore.Batcher, keyBase int, batchSize int, val []byte) (int, error) {
	const (
		timeout  = 20 * time.Second
		interval = 50 * time.Millisecond
	)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats := getTreeDBStats(db)
		if parseUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id") != 0 {
			return keyBase, nil
		}

		// Nudge the system with a small amount of write traffic to ensure that if
		// the dictionary is ready, it is actually applied on a frame before the
		// measured phase begins.
		nudge := batchSize
		if nudge <= 0 {
			nudge = 128
		}
		if err := writeBatches(batcher, keyBase, nudge, nudge, val); err != nil {
			return 0, err
		}
		keyBase += nudge
		time.Sleep(interval)
	}
	// Incompressible streams can legitimately refuse to publish/apply dicts. Do
	// not fail the suite; proceed without a dict.
	return keyBase, nil
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
	disableJournal             bool
	disableValueLog            bool
	splitValueLog              bool
	memtableValueLogPointers   bool
	relaxedSync                bool
	disableReadChecksum        bool
	allowUnsafe                bool
	valueLogThreshold          int
	journalLanes               int
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
		disableJournal:             *treedbDisableJournal,
		disableValueLog:            *treedbDisableValueLog,
		splitValueLog:              *treedbSplitValueLog,
		memtableValueLogPointers:   *treedbMemtableValueLogPointers,
		relaxedSync:                *treedbRelaxedSync,
		disableReadChecksum:        *treedbDisableReadChecksum,
		allowUnsafe:                *treedbAllowUnsafe,
		valueLogThreshold:          *treedbValueLogThreshold,
		journalLanes:               *treedbJournalLanes,
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
	*treedbDisableJournal = s.disableJournal
	*treedbDisableValueLog = s.disableValueLog
	*treedbSplitValueLog = s.splitValueLog
	*treedbMemtableValueLogPointers = s.memtableValueLogPointers
	*treedbRelaxedSync = s.relaxedSync
	*treedbDisableReadChecksum = s.disableReadChecksum
	*treedbAllowUnsafe = s.allowUnsafe
	*treedbValueLogThreshold = s.valueLogThreshold
	*treedbJournalLanes = s.journalLanes
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
	*treedbDisableValueLog = false
	*treedbSplitValueLog = true
	*treedbMemtableValueLogPointers = true
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
	case "mode3":
		*treedbDisableJournal = false
	case "mode4":
		*treedbDisableJournal = true
	default:
		*treedbDisableJournal = false
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
