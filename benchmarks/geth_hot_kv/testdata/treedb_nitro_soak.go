package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/node"
)

// treedb_nitro_soak is a small integrated geth/Nitro hot-KV benchmark.
//
// Run it from a go-ethereum or Nitro go-ethereum checkout so it uses that
// checkout's node.OpenDatabase implementation and selected --db.engine path:
//
//   cd go-ethereum
//   go run /path/to/treedb_nitro_soak.go -n 30000 -reads 12000 -out /tmp/treedb_soak_results.json
//
// The workload intentionally mirrors the historical #2392 shape without relying
// on a full node sync: deterministic geth-like binary/prefix keys, batched
// writes, random point reads, full ordered iteration, DB-level DeleteRange plus
// batch DeleteRange, close/reopen verification, and engine directory sizing.

const (
	phaseWrite       = "write"
	phaseRead        = "read"
	phaseIterate     = "iterate"
	phaseDeleteRange = "delete_range"
	phaseReopen      = "reopen_verify"

	iterationModeValue   = "value"
	iterationModeKeyOnly = "key-only"

	readIntegrityVerify     = "verify"
	readIntegrityUnsafeSkip = "unsafe-skip-checksums"
)

type config struct {
	N                    int      `json:"n"`
	Reads                int      `json:"reads"`
	Engines              []string `json:"engines"`
	KeyShape             string   `json:"key_shape"`
	ValueShape           string   `json:"value_shape"`
	ValueSize            int      `json:"value_size"`
	BatchTargetBytes     int      `json:"batch_target_bytes"`
	DeleteRangeWidth     int      `json:"delete_range_width"`
	DeleteRangesPerBatch int      `json:"delete_ranges_per_batch"`
	TreeDBReadIntegrity  string   `json:"treedb_read_integrity"`
	IterationMode        string   `json:"iteration_mode"`
	Seed                 int64    `json:"seed"`
	WorkDir              string   `json:"workdir"`
	Keep                 bool     `json:"keep"`
	ProfileDir           string   `json:"profile_dir,omitempty"`
	ProfileEngines       []string `json:"profile_engines,omitempty"`
	CacheMB              int      `json:"cache_mb"`
	Handles              int      `json:"handles"`
}

type phaseResult struct {
	DurationMillis int64            `json:"duration_millis"`
	Ops            int              `json:"ops"`
	OpsPerSec      float64          `json:"ops_per_sec"`
	StatDelta      map[string]int64 `json:"stat_delta,omitempty"`
}

type memStatsDelta struct {
	Phase             string `json:"phase"`
	Engine            string `json:"engine"`
	TotalAllocBytes   uint64 `json:"total_alloc_bytes"`
	Mallocs           uint64 `json:"mallocs"`
	Frees             uint64 `json:"frees"`
	HeapAllocBefore   uint64 `json:"heap_alloc_before"`
	HeapAllocAfter    uint64 `json:"heap_alloc_after"`
	HeapObjectsBefore uint64 `json:"heap_objects_before"`
	HeapObjectsAfter  uint64 `json:"heap_objects_after"`
	PauseTotalNs      uint64 `json:"pause_total_ns"`
	NumGCDelta        uint32 `json:"num_gc_delta"`
}

type runResult struct {
	Engine                string                 `json:"engine"`
	DBPath                string                 `json:"db_path"`
	ReadIntegrity         string                 `json:"read_integrity"`
	IterationMode         string                 `json:"iteration_mode"`
	WriteOpsPerSec        float64                `json:"write_ops_sec"`
	ReadOpsPerSec         float64                `json:"read_ops_sec"`
	IterateKeysPerSec     float64                `json:"iterate_keys_sec"`
	DeleteRangeKeysPerSec float64                `json:"delete_range_keys_sec"`
	SizeBytes             int64                  `json:"size_bytes"`
	PostDeleteSizeBytes   int64                  `json:"post_delete_size_bytes"`
	KeysWritten           int                    `json:"keys_written"`
	Reads                 int                    `json:"reads"`
	IteratedKeys          int                    `json:"iterated_keys"`
	DeleteRangeKeys       int                    `json:"delete_range_keys"`
	Phases                map[string]phaseResult `json:"phases"`
}

type output struct {
	Config  config      `json:"config"`
	Runs    []runResult `json:"runs"`
	Summary string      `json:"summary_markdown"`
}

type benchmarkData struct {
	Keys   [][]byte
	Values [][]byte
	Order  []int
}

type openedDB struct {
	stack *node.Node
	db    ethdb.Database
}

func main() {
	var enginesCSV string
	var profileEnginesCSV string
	cfg := config{}
	createdTempWorkDir := false
	flag.IntVar(&cfg.N, "n", 30000, "number of hot KV records")
	flag.IntVar(&cfg.Reads, "reads", 12000, "number of deterministic random point reads")
	flag.StringVar(&enginesCSV, "engines", "pebble,leveldb,treedb", "comma-separated db.engine values")
	flag.StringVar(&cfg.KeyShape, "key-shape", "geth-mixed", "key shape: geth-mixed|state-heavy|single-prefix|sequential")
	flag.StringVar(&cfg.ValueShape, "value-shape", "geth-mixed", "value shape: geth-mixed|fixed")
	flag.IntVar(&cfg.ValueSize, "value-size", 128, "base value size in bytes")
	flag.IntVar(&cfg.BatchTargetBytes, "batch-target-bytes", ethdb.IdealBatchSize, "flush write batches after this queued value size; <=0 writes one final batch")
	flag.IntVar(&cfg.DeleteRangeWidth, "delete-range-width", 100, "keys covered by each DeleteRange call")
	flag.IntVar(&cfg.DeleteRangesPerBatch, "delete-ranges-per-batch", 100, "batch DeleteRange calls per batch.Write")
	flag.StringVar(&cfg.TreeDBReadIntegrity, "treedb-read-integrity", readIntegrityVerify, "TreeDB value-log read integrity: verify|unsafe-skip-checksums (unsafe mode is benchmark-only)")
	flag.StringVar(&cfg.IterationMode, "iteration-mode", iterationModeValue, "iteration mode: value|key-only")
	flag.Int64Var(&cfg.Seed, "seed", 1, "deterministic seed")
	flag.StringVar(&cfg.WorkDir, "workdir", "", "base workdir; defaults to os.MkdirTemp")
	flag.BoolVar(&cfg.Keep, "keep", false, "keep the workdir after completion")
	flag.StringVar(&cfg.ProfileDir, "profile-dir", "", "write per-phase pprof artifacts under this directory")
	flag.StringVar(&profileEnginesCSV, "profile-engines", "treedb", "comma-separated engines to profile when -profile-dir is set; empty profiles all")
	flag.IntVar(&cfg.CacheMB, "cache", 1024, "node.OpenDatabase cache MB")
	flag.IntVar(&cfg.Handles, "handles", 1024, "node.OpenDatabase file handles")
	outPath := flag.String("out", "", "write JSON results to this path")
	flag.Parse()

	var err error
	cfg.Engines = splitCSV(enginesCSV)
	cfg.ProfileEngines = splitCSV(profileEnginesCSV)
	if normalized, err := normalizeReadIntegrity(cfg.TreeDBReadIntegrity); err != nil {
		fatalf("%v", err)
	} else {
		cfg.TreeDBReadIntegrity = normalized
	}
	if normalized, err := normalizeIterationMode(cfg.IterationMode); err != nil {
		fatalf("%v", err)
	} else {
		cfg.IterationMode = normalized
	}
	if cfg.N <= 0 || cfg.Reads < 0 || cfg.ValueSize <= 0 || cfg.DeleteRangeWidth <= 0 || cfg.DeleteRangesPerBatch <= 0 {
		fatalf("invalid non-positive benchmark parameter")
	}
	if len(cfg.Engines) == 0 {
		fatalf("no engines requested")
	}
	if cfg.WorkDir == "" {
		cfg.WorkDir, err = os.MkdirTemp("", "nitro_treedb_soak_*")
		if err != nil {
			fatalf("create workdir: %v", err)
		}
		createdTempWorkDir = true
	} else if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		fatalf("create workdir %s: %v", cfg.WorkDir, err)
	}
	if !cfg.Keep && createdTempWorkDir && !pathWithin(cfg.WorkDir, *outPath) && !pathWithin(cfg.WorkDir, cfg.ProfileDir) {
		defer os.RemoveAll(cfg.WorkDir)
	}
	if cfg.ProfileDir != "" {
		if err := os.MkdirAll(cfg.ProfileDir, 0o755); err != nil {
			fatalf("create profile dir: %v", err)
		}
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}

	data, err := prepareData(cfg)
	if err != nil {
		fatalf("prepare data: %v", err)
	}
	var runs []runResult
	for _, engine := range cfg.Engines {
		run, err := runEngine(cfg, engine, data)
		if err != nil {
			fatalf("%s: %v", engine, err)
		}
		runs = append(runs, run)
	}
	result := output{Config: cfg, Runs: runs}
	result.Summary = renderSummary(result)
	fmt.Print(result.Summary)
	if *outPath != "" {
		if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil && filepath.Dir(*outPath) != "." {
			fatalf("create output dir: %v", err)
		}
		blob, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			fatalf("marshal results: %v", err)
		}
		if err := os.WriteFile(*outPath, append(blob, '\n'), 0o644); err != nil {
			fatalf("write results: %v", err)
		}
	}
	if cfg.ProfileDir != "" {
		writeRuntimeProfile(filepath.Join(cfg.ProfileDir, "block.pprof"), "block")
		writeRuntimeProfile(filepath.Join(cfg.ProfileDir, "mutex.pprof"), "mutex")
	}
}

func runEngine(cfg config, engine string, data benchmarkData) (runResult, error) {
	dbRoot := filepath.Join(cfg.WorkDir, engine)
	if err := os.RemoveAll(dbRoot); err != nil {
		return runResult{}, err
	}
	if err := os.MkdirAll(dbRoot, 0o755); err != nil {
		return runResult{}, err
	}
	opened, err := openEngine(dbRoot, engine, false, cfg)
	if err != nil {
		return runResult{}, err
	}
	defer opened.close()

	run := runResult{
		Engine:        engine,
		DBPath:        filepath.Join(dbRoot, "geth", "chaindata"),
		ReadIntegrity: readIntegrityForRun(cfg, engine),
		IterationMode: cfg.IterationMode,
		KeysWritten:   len(data.Keys),
		Reads:         cfg.Reads,
		Phases:        make(map[string]phaseResult),
	}
	profiler := phaseProfiler{cfg: cfg, engine: engine}

	writePhase, err := timeDBPhase(profiler, opened.db, phaseWrite, func() (int, error) {
		return writeData(opened.db, cfg, data)
	})
	if err != nil {
		return runResult{}, err
	}
	run.Phases[phaseWrite] = writePhase
	run.WriteOpsPerSec = writePhase.OpsPerSec

	readPhase, err := timeDBPhase(profiler, opened.db, phaseRead, func() (int, error) {
		return readData(opened.db, cfg, data)
	})
	if err != nil {
		return runResult{}, err
	}
	run.Phases[phaseRead] = readPhase
	run.ReadOpsPerSec = readPhase.OpsPerSec

	iteratePhase, err := timeDBPhase(profiler, opened.db, phaseIterate, func() (int, error) {
		return iterateData(opened.db, len(data.Keys), cfg.IterationMode)
	})
	if err != nil {
		return runResult{}, err
	}
	run.Phases[phaseIterate] = iteratePhase
	run.IterateKeysPerSec = iteratePhase.OpsPerSec
	run.IteratedKeys = iteratePhase.Ops
	loadedSize, err := dirSize(run.DBPath)
	if err != nil {
		return runResult{}, err
	}
	run.SizeBytes = loadedSize

	deletePhase, err := timeDBPhase(profiler, opened.db, phaseDeleteRange, func() (int, error) {
		return deleteRanges(opened.db, cfg, data)
	})
	if err != nil {
		return runResult{}, err
	}
	run.Phases[phaseDeleteRange] = deletePhase
	run.DeleteRangeKeysPerSec = deletePhase.OpsPerSec
	run.DeleteRangeKeys = deletePhase.Ops

	if err := opened.close(); err != nil {
		return runResult{}, err
	}

	reopenPhase, err := timeDBPhase(profiler, nil, phaseReopen, func() (int, error) {
		reopened, err := openEngine(dbRoot, engine, false, cfg)
		if err != nil {
			return 0, err
		}
		defer reopened.close()
		return verifyDeleted(reopened.db, data)
	})
	if err != nil {
		return runResult{}, err
	}
	run.Phases[phaseReopen] = reopenPhase

	postDeleteSize, err := dirSize(run.DBPath)
	if err != nil {
		return runResult{}, err
	}
	run.PostDeleteSizeBytes = postDeleteSize
	return run, nil
}

func readIntegrityForRun(cfg config, engine string) string {
	if !isTreeDBEngine(engine) {
		return "n/a"
	}
	return cfg.TreeDBReadIntegrity
}

func isTreeDBEngine(engine string) bool {
	return strings.EqualFold(strings.TrimSpace(engine), "treedb")
}

func applyTreeDBReadIntegrityEnv(engine, mode string) func() {
	if !isTreeDBEngine(engine) {
		return func() {}
	}
	return withEnv(map[string]string{
		"TREEDB_GETH_READ_INTEGRITY": mode,
		"TREEDB_READ_INTEGRITY":      mode,
	})
}

func withEnv(values map[string]string) func() {
	type prior struct {
		value string
		ok    bool
	}
	old := make(map[string]prior, len(values))
	for key, value := range values {
		prev, ok := os.LookupEnv(key)
		old[key] = prior{value: prev, ok: ok}
		if value == "" {
			_ = os.Unsetenv(key)
		} else {
			_ = os.Setenv(key, value)
		}
	}
	return func() {
		for key, prev := range old {
			if prev.ok {
				_ = os.Setenv(key, prev.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}
}

func openEngine(root, engine string, readonly bool, cfg config) (*openedDB, error) {
	restoreEnv := applyTreeDBReadIntegrityEnv(engine, cfg.TreeDBReadIntegrity)
	defer restoreEnv()

	stack, err := node.New(&node.Config{
		DataDir:  root,
		Name:     "geth",
		DBEngine: engine,
		IPCPath:  "",
	})
	if err != nil {
		return nil, err
	}
	db, err := stack.OpenDatabase("chaindata", cfg.CacheMB, cfg.Handles, "", readonly)
	if err != nil {
		_ = stack.Close()
		return nil, err
	}
	return &openedDB{stack: stack, db: db}, nil
}

func (o *openedDB) close() error {
	if o == nil {
		return nil
	}
	var errs []error
	if o.db != nil {
		if err := o.db.Close(); err != nil {
			errs = append(errs, err)
		}
		o.db = nil
	}
	if o.stack != nil {
		if err := o.stack.Close(); err != nil && !errors.Is(err, node.ErrNodeStopped) {
			errs = append(errs, err)
		}
		o.stack = nil
	}
	return errors.Join(errs...)
}

func writeData(db ethdb.Database, cfg config, data benchmarkData) (int, error) {
	batch := db.NewBatch()
	defer batch.Close()
	written := 0
	for i := range data.Keys {
		if err := batch.Put(data.Keys[i], data.Values[i]); err != nil {
			return written, err
		}
		written++
		if cfg.BatchTargetBytes > 0 && batch.ValueSize() >= cfg.BatchTargetBytes {
			if err := batch.Write(); err != nil {
				return written, err
			}
			batch.Reset()
		}
	}
	if batch.ValueSize() > 0 {
		if err := batch.Write(); err != nil {
			return written, err
		}
	}
	if err := db.SyncKeyValue(); err != nil {
		return written, err
	}
	return written, nil
}

func readData(db ethdb.Database, cfg config, data benchmarkData) (int, error) {
	rng := rand.New(rand.NewSource(cfg.Seed ^ 0x5eed))
	read := 0
	for i := 0; i < cfg.Reads; i++ {
		idx := data.Order[rng.Intn(len(data.Order))]
		got, err := db.Get(data.Keys[idx])
		if err != nil {
			return read, fmt.Errorf("get key %d: %w", idx, err)
		}
		if !equalBytes(got, data.Values[idx]) {
			return read, fmt.Errorf("value mismatch for key %d", idx)
		}
		read++
	}
	return read, nil
}

func iterateData(db ethdb.Database, want int, mode string) (int, error) {
	it := db.NewIterator(nil, nil)
	defer it.Release()
	count := 0
	for it.Next() {
		count++
		_ = it.Key()
		if mode == iterationModeValue {
			_ = it.Value()
		}
	}
	if err := it.Error(); err != nil {
		return count, err
	}
	if count != want {
		return count, fmt.Errorf("iterator count=%d want=%d", count, want)
	}
	return count, nil
}

func deleteRanges(db ethdb.Database, cfg config, data benchmarkData) (int, error) {
	keys := data.Keys
	mid := len(keys) / 2
	deleted := 0
	for start := 0; start < mid; start += cfg.DeleteRangeWidth {
		end := min(start+cfg.DeleteRangeWidth, mid)
		if err := db.DeleteRange(keys[start], rangeLimit(keys[end-1])); err != nil {
			return deleted, fmt.Errorf("db DeleteRange [%d,%d): %w", start, end, err)
		}
		deleted += end - start
	}
	batch := db.NewBatch()
	defer batch.Close()
	batchedRanges := 0
	for start := mid; start < len(keys); start += cfg.DeleteRangeWidth {
		end := min(start+cfg.DeleteRangeWidth, len(keys))
		if err := batch.DeleteRange(keys[start], rangeLimit(keys[end-1])); err != nil {
			return deleted, fmt.Errorf("batch DeleteRange [%d,%d): %w", start, end, err)
		}
		deleted += end - start
		batchedRanges++
		if batchedRanges >= cfg.DeleteRangesPerBatch {
			if err := batch.Write(); err != nil {
				return deleted, err
			}
			batch.Reset()
			batchedRanges = 0
		}
	}
	if batchedRanges > 0 {
		if err := batch.Write(); err != nil {
			return deleted, err
		}
	}
	if err := db.SyncKeyValue(); err != nil {
		return deleted, err
	}
	return deleted, nil
}

func verifyDeleted(db ethdb.Database, data benchmarkData) (int, error) {
	checked := 0
	for i, key := range data.Keys {
		has, err := db.Has(key)
		if err != nil {
			return checked, fmt.Errorf("has key %d: %w", i, err)
		}
		if has {
			return checked, fmt.Errorf("key %d remains after DeleteRange", i)
		}
		checked++
	}
	it := db.NewIterator(nil, nil)
	defer it.Release()
	if it.Next() {
		return checked, fmt.Errorf("iterator saw key after DeleteRange: %x", it.Key())
	}
	return checked, it.Error()
}

func prepareData(cfg config) (benchmarkData, error) {
	keys := make([][]byte, cfg.N)
	values := make([][]byte, cfg.N)
	for i := 0; i < cfg.N; i++ {
		keys[i] = keyFor(cfg.KeyShape, i)
		values[i] = valueFor(cfg.ValueShape, cfg.ValueSize, i, keys[i])
	}
	idx := make([]int, cfg.N)
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(i, j int) bool { return strings.Compare(string(keys[idx[i]]), string(keys[idx[j]])) < 0 })
	sortedKeys := make([][]byte, cfg.N)
	sortedVals := make([][]byte, cfg.N)
	for out, in := range idx {
		sortedKeys[out] = keys[in]
		sortedVals[out] = values[in]
		if out > 0 && equalBytes(sortedKeys[out-1], sortedKeys[out]) {
			return benchmarkData{}, fmt.Errorf("duplicate generated key at sorted positions %d/%d", out-1, out)
		}
	}
	order := make([]int, cfg.N)
	for i := range order {
		order[i] = i
	}
	return benchmarkData{Keys: sortedKeys, Values: sortedVals, Order: order}, nil
}

func keyFor(shape string, i int) []byte {
	switch shape {
	case "geth-mixed":
		return gethMixedKey(i)
	case "state-heavy":
		switch i % 10 {
		case 0, 1, 2:
			return prefixedHashKey('a', i, 0)
		case 3, 4, 5:
			return storageKey('o', i)
		case 6, 7:
			return prefixedHashKey('A', i, 1)
		case 8:
			return storageKey('O', i)
		default:
			return prefixedHashKey('c', i, 2)
		}
	case "single-prefix":
		return prefixedHashKey('a', i, 0)
	case "sequential":
		key := make([]byte, 9)
		key[0] = 'k'
		binary.BigEndian.PutUint64(key[1:], uint64(i))
		return key
	default:
		fatalf("unknown key-shape %q", shape)
	}
	panic("unreachable")
}

func gethMixedKey(i int) []byte {
	num := uint64(i / 10)
	switch i % 10 {
	case 0:
		return numberHashKey('h', num, i, 0) // header
	case 1:
		key := make([]byte, 10) // header hash: h + number + n
		key[0] = 'h'
		binary.BigEndian.PutUint64(key[1:9], num)
		key[9] = 'n'
		return key
	case 2:
		return prefixedHashKey('H', i, 1) // hash -> number
	case 3:
		return numberHashKey('b', num, i, 2) // body
	case 4:
		return numberHashKey('r', num, i, 3) // receipts
	case 5:
		return prefixedHashKey('l', i, 4) // tx lookup
	case 6:
		return prefixedHashKey('a', i, 5) // snapshot account
	case 7:
		return storageKey('o', i) // snapshot storage
	case 8:
		return prefixedHashKey('c', i, 6) // code
	default:
		return prefixedHashKey('A', i, 7) // account trie node-ish path
	}
}

func numberHashKey(prefix byte, number uint64, i int, salt byte) []byte {
	key := make([]byte, 1+8+32)
	key[0] = prefix
	binary.BigEndian.PutUint64(key[1:9], number)
	hash := digest(i, salt)
	copy(key[9:], hash[:])
	return key
}

func prefixedHashKey(prefix byte, i int, salt byte) []byte {
	key := make([]byte, 1+32)
	key[0] = prefix
	hash := digest(i, salt)
	copy(key[1:], hash[:])
	return key
}

func storageKey(prefix byte, i int) []byte {
	key := make([]byte, 1+32+32)
	key[0] = prefix
	account := digest(i, 8)
	slot := digest(i, 9)
	copy(key[1:33], account[:])
	copy(key[33:], slot[:])
	return key
}

func valueFor(shape string, baseSize, i int, key []byte) []byte {
	size := baseSize
	if shape == "geth-mixed" {
		switch key[0] {
		case 'h':
			size = max(96, baseSize*2)
		case 'b':
			size = max(192, baseSize*4)
		case 'r':
			size = max(256, baseSize*6)
		case 'c':
			size = max(512, baseSize*8)
		case 'l':
			size = max(24, baseSize/4)
		case 'a', 'A', 'o', 'O':
			size = max(32, baseSize)
		default:
			size = baseSize
		}
	} else if shape != "fixed" {
		fatalf("unknown value-shape %q", shape)
	}
	value := make([]byte, size)
	seed := digest(i, 0x42)
	for off := 0; off < len(value); off += len(seed) {
		copy(value[off:], seed[:])
		seed = sha256.Sum256(seed[:])
	}
	return value
}

func digest(i int, salt byte) [32]byte {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(i))
	binary.BigEndian.PutUint64(buf[8:], uint64(salt))
	return sha256.Sum256(buf[:])
}

func rangeLimit(key []byte) []byte {
	limit := append([]byte(nil), key...)
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] != 0xff {
			limit[i]++
			return limit[:i+1]
		}
	}
	return nil
}

func pathWithin(root, path string) bool {
	if root == "" || path == "" {
		return false
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absRoot, absPath)
	if err != nil {
		return false
	}
	return rel == "." || (rel != "" && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func normalizeReadIntegrity(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", readIntegrityVerify, "verified", "checksum-verify", "checksum-verified", "checksums", "on", "true", "1":
		return readIntegrityVerify, nil
	case readIntegrityUnsafeSkip, "skip-checksums", "skip", "no-checksums", "none", "off", "false", "0":
		return readIntegrityUnsafeSkip, nil
	default:
		return "", fmt.Errorf("unknown -treedb-read-integrity %q (use verify or unsafe-skip-checksums)", raw)
	}
}

func normalizeIterationMode(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", iterationModeValue, "values", "value-materialized", "value-iteration":
		return iterationModeValue, nil
	case iterationModeKeyOnly, "keys", "key":
		return iterationModeKeyOnly, nil
	default:
		return "", fmt.Errorf("unknown -iteration-mode %q (use value or key-only)", raw)
	}
}

func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

type dbStatter interface {
	Stat() (string, error)
}

var interestingStatKeys = []string{
	"treedb.vlog.read.crc32_checks_total",
	"treedb.cache.vlog_read.crc32_checks_total",
	"treedb.vlog.grouped_frame_cache.hits",
	"treedb.vlog.grouped_frame_cache.misses",
	"treedb.vlog.grouped_frame_cache.stores",
	"treedb.cache.vlog_grouped_frame_cache.hits",
	"treedb.cache.vlog_grouped_frame_cache.misses",
	"treedb.cache.vlog_grouped_frame_cache.stores",
	"treedb.vlog.mmap_read.hits",
	"treedb.vlog.mmap_read.miss_out_of_range",
	"treedb.vlog.mmap_read.miss_no_mapping",
	"treedb.vlog.mmap_read.miss_dead_mapping_cap",
	"treedb.vlog.mmap_read.fallback_readat",
	"treedb.process.read_path.outer_leaf.loads_total",
	"treedb.process.read_path.outer_leaf.point_loads_total",
	"treedb.process.read_path.outer_leaf.iterator_loads_total",
	"treedb.process.read_path.outer_leaf.checksum.verifications_total",
	"treedb.process.read_path.outer_leaf.checksum.skips_total",
	"treedb.cache.vlog_mmap.read.hits",
	"treedb.cache.vlog_mmap.read.miss_out_of_range",
	"treedb.cache.vlog_mmap.read.miss_no_mapping",
	"treedb.cache.vlog_mmap.read.miss_dead_mapping_cap",
	"treedb.cache.vlog_mmap.read.fallback_readat",
	"treedb.cache.delete_range.calls_total",
	"treedb.cache.delete_range.batch_calls_total",
	"treedb.cache.delete_range.batch_writes_total",
	"treedb.cache.delete_range.input_ranges_total",
	"treedb.cache.delete_range.effective_ranges_total",
	"treedb.cache.delete_range.coalesced_ranges_total",
	"treedb.cache.delete_range.iterators_total",
	"treedb.cache.delete_range.snapshot_iterators_total",
	"treedb.cache.delete_range.backend_iterators_total",
	"treedb.cache.delete_range.memtable_iterators_total",
	"treedb.cache.delete_range.queue_iterators_total",
	"treedb.cache.delete_range.visited_keys_total",
	"treedb.cache.delete_range.tombstone_keys_total",
	"treedb.cache.delete_range.materialized_keys_total",
	"treedb.cache.delete_range.materialized_key_bytes_total",
	"treedb.cache.delete_range.fast_path_clears_total",
	"treedb.cache.delete_range.backend_direct_batches_total",
	"treedb.cache.delete_range.backend_direct_keys_total",
	"treedb.cache.range_span.layers_total",
	"treedb.cache.range_span.active_layers",
	"treedb.cache.range_span.active_spans",
	"treedb.cache.range_span.input_total",
	"treedb.cache.range_span.effective_total",
	"treedb.cache.range_span.keys_materialized_total",
	"treedb.cache.range_span.point_overrides_total",
	"treedb.cache.range_span.point_probes_total",
	"treedb.cache.range_span.point_hits_total",
	"treedb.cache.range_span.iterator_probes_total",
	"treedb.cache.range_span.iterator_skips_total",
	"treedb.cache.range_span.range_only_queued_units_total",
	"treedb.cache.range_span.range_only_flushed_total",
	"treedb.cache.range_span.spans_flushed_total",
	"treedb.cache.range_span.flush_batches_total",
}

var interestingStatPrefixes = []string{
	"treedb.command_wal.",
	"treedb.cache.command_wal.",
	"treedb.cache.checkpoint.",
	"treedb.cache.vlog_auto.",
	"treedb.cache.vlog_block.",
	"treedb.cache.vlog_outer_leaf_codec.",
	"treedb.cache.vlog_payload_kind.",
	"treedb.cache.vlog_payload_split.",
	"treedb.cache.vlog_queue.",
	"treedb.cache.vlog_shape.",
	"treedb.cache.vlog_write_mode.",
	"treedb.cache.batch_arena.",
	"treedb.cache.memtable_adaptive.",
	"treedb.cache.memtable_stats.",
	"treedb.cache.memtable_view.",
	"treedb.cache.range_span.",
	"treedb.freelist.",
	"treedb.graveyard.",
	"treedb.pages.",
	"treedb.publish.ordered_root_delta_group.",
	"treedb.vlog.decode_buffer_grow.",
}

var interestingStatKeySet = func() map[string]struct{} {
	out := make(map[string]struct{}, len(interestingStatKeys))
	for _, key := range interestingStatKeys {
		out[key] = struct{}{}
	}
	return out
}()

func timeDBPhase(profiler phaseProfiler, db ethdb.Database, phase string, fn func() (int, error)) (phaseResult, error) {
	before := statSnapshot(db)
	result, err := profiler.time(phase, fn)
	after := statSnapshot(db)
	result.StatDelta = statDelta(before, after)
	return result, err
}

func statSnapshot(db ethdb.Database) map[string]int64 {
	statter, ok := db.(dbStatter)
	if !ok || statter == nil {
		return nil
	}
	raw, err := statter.Stat()
	if err != nil || raw == "" {
		return nil
	}
	parsed := parseKeyValueStats(raw)
	out := make(map[string]int64, len(parsed))
	for key, value := range parsed {
		if interestingStatKey(key) {
			out[key] = value
		}
	}
	return out
}

func interestingStatKey(key string) bool {
	if _, ok := interestingStatKeySet[key]; ok {
		return true
	}
	for _, prefix := range interestingStatPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func parseKeyValueStats(raw string) map[string]int64 {
	out := make(map[string]int64)
	for _, line := range strings.Split(raw, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			out[key] = n
		}
	}
	return out
}

func statDelta(before, after map[string]int64) map[string]int64 {
	if len(after) == 0 {
		return nil
	}
	out := make(map[string]int64, len(after))
	for key, post := range after {
		out[key] = post - before[key]
	}
	return out
}

type phaseProfiler struct {
	cfg    config
	engine string
}

func (p phaseProfiler) time(phase string, fn func() (int, error)) (phaseResult, error) {
	if p.cfg.ProfileDir != "" && p.shouldProfile() {
		cpuPath := filepath.Join(p.cfg.ProfileDir, fmt.Sprintf("cpu_%s_%s.pprof", phase, p.engine))
		f, err := os.Create(cpuPath)
		if err != nil {
			return phaseResult{}, err
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return phaseResult{}, err
		}
		start := time.Now()
		ops, runErr := fn()
		elapsed := time.Since(start)
		pprof.StopCPUProfile()
		runtime.ReadMemStats(&after)
		if err := f.Close(); err != nil && runErr == nil {
			runErr = err
		}
		writeMemStatsDelta(filepath.Join(p.cfg.ProfileDir, fmt.Sprintf("memstats_%s_%s.json", phase, p.engine)), phase, p.engine, before, after)
		writeRuntimeProfile(filepath.Join(p.cfg.ProfileDir, fmt.Sprintf("allocs_cumulative_%s_%s.pprof", phase, p.engine)), "allocs")
		return phaseResult{DurationMillis: elapsed.Milliseconds(), Ops: ops, OpsPerSec: perSec(ops, elapsed)}, runErr
	}
	start := time.Now()
	ops, err := fn()
	elapsed := time.Since(start)
	return phaseResult{DurationMillis: elapsed.Milliseconds(), Ops: ops, OpsPerSec: perSec(ops, elapsed)}, err
}

func (p phaseProfiler) shouldProfile() bool {
	if len(p.cfg.ProfileEngines) == 0 {
		return true
	}
	for _, engine := range p.cfg.ProfileEngines {
		if engine == p.engine {
			return true
		}
	}
	return false
}

func writeMemStatsDelta(path, phase, engine string, before, after runtime.MemStats) {
	delta := memStatsDelta{
		Phase:             phase,
		Engine:            engine,
		TotalAllocBytes:   after.TotalAlloc - before.TotalAlloc,
		Mallocs:           after.Mallocs - before.Mallocs,
		Frees:             after.Frees - before.Frees,
		HeapAllocBefore:   before.HeapAlloc,
		HeapAllocAfter:    after.HeapAlloc,
		HeapObjectsBefore: before.HeapObjects,
		HeapObjectsAfter:  after.HeapObjects,
		PauseTotalNs:      after.PauseTotalNs - before.PauseTotalNs,
		NumGCDelta:        after.NumGC - before.NumGC,
	}
	blob, err := json.MarshalIndent(delta, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "memstats %s: %v\n", path, err)
		return
	}
	if err := os.WriteFile(path, append(blob, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "memstats %s: %v\n", path, err)
	}
}

func writeRuntimeProfile(path, name string) {
	runtime.GC()
	prof := pprof.Lookup(name)
	if prof == nil {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "profile %s: %v\n", path, err)
		return
	}
	defer f.Close()
	_ = prof.WriteTo(f, 0)
}

func renderSummary(out output) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "# geth/Nitro hot KV soak\n\n")
	fmt.Fprintf(&sb, "- n: %d\n", out.Config.N)
	fmt.Fprintf(&sb, "- reads: %d\n", out.Config.Reads)
	fmt.Fprintf(&sb, "- key-shape: %s\n", out.Config.KeyShape)
	fmt.Fprintf(&sb, "- value-shape: %s\n", out.Config.ValueShape)
	fmt.Fprintf(&sb, "- value-size: %d\n", out.Config.ValueSize)
	fmt.Fprintf(&sb, "- batch-target-bytes: %d\n", out.Config.BatchTargetBytes)
	fmt.Fprintf(&sb, "- delete-range-width: %d\n", out.Config.DeleteRangeWidth)
	fmt.Fprintf(&sb, "- TreeDB read-integrity: %s\n", out.Config.TreeDBReadIntegrity)
	fmt.Fprintf(&sb, "- iteration-mode: %s\n", out.Config.IterationMode)
	fmt.Fprintf(&sb, "- workdir: %s\n\n", out.Config.WorkDir)
	sb.WriteString("`size bytes` is measured after write/read/iterate and before the destructive DeleteRange phase; `post-delete bytes` is measured after close/reopen verification.\n\n")
	sb.WriteString("| engine | read integrity | iteration mode | write ops/sec | read ops/sec | iterate keys/sec | DeleteRange keys/sec | size bytes | post-delete bytes |\n")
	sb.WriteString("|---|---|---|---:|---:|---:|---:|---:|---:|\n")
	for _, run := range out.Runs {
		fmt.Fprintf(&sb, "| %s | %s | %s | %.0f | %.0f | %.0f | %.0f | %d | %d |\n", run.Engine, run.ReadIntegrity, run.IterationMode, run.WriteOpsPerSec, run.ReadOpsPerSec, run.IterateKeysPerSec, run.DeleteRangeKeysPerSec, run.SizeBytes, run.PostDeleteSizeBytes)
	}
	writeStatDeltaSummary(&sb, out.Runs)
	return sb.String()
}

func writeStatDeltaSummary(sb *strings.Builder, runs []runResult) {
	if hasReadCounterDeltas(runs) {
		sb.WriteString("\n## TreeDB value-log read counters\n\n")
		sb.WriteString("Per-phase deltas from TreeDB `Stat()` output. CRC counts are value-log record CRC32 computations performed by the read path; grouped-frame counters report the current grouped-frame cache behavior used by follow-up #2678. Outer-leaf counters identify B-tree leaf pages read from the value-log-backed leaf log. Broader write-path stat deltas remain available in the JSON output and matrix `phase_stat_deltas.tsv`.\n\n")
		sb.WriteString("| engine | read integrity | iteration mode | phase | crc32 checks | grouped hits | grouped misses | grouped stores | mmap hits | mmap miss out-of-range | mmap miss no mapping | mmap miss dead cap | mmap ReadAt fallbacks | outer leaf loads | outer leaf point loads | outer leaf iterator loads | outer leaf checksum verifies | outer leaf checksum skips |\n")
		sb.WriteString("|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, run := range runs {
			for _, phase := range []string{phaseWrite, phaseRead, phaseIterate, phaseDeleteRange, phaseReopen} {
				result, ok := run.Phases[phase]
				if !ok || len(result.StatDelta) == 0 {
					continue
				}
				counters, ok := readCounterDeltas(result.StatDelta)
				if !ok {
					continue
				}
				fmt.Fprintf(sb, "| %s | %s | %s | %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |\n",
					run.Engine,
					run.ReadIntegrity,
					run.IterationMode,
					phase,
					counters.crc32Checks,
					counters.groupedHits,
					counters.groupedMisses,
					counters.groupedStores,
					counters.mmapHits,
					counters.mmapMissOutOfRange,
					counters.mmapMissNoMapping,
					counters.mmapMissDeadCap,
					counters.mmapFallbackReadAt,
					counters.outerLeafLoads,
					counters.outerLeafPointLoads,
					counters.outerLeafIteratorLoads,
					counters.outerLeafChecksumVerifications,
					counters.outerLeafChecksumSkips,
				)
			}
		}
	}

	if !hasDeleteRangeCounterDeltas(runs) {
		return
	}
	sb.WriteString("\n## TreeDB DeleteRange counters\n\n")
	sb.WriteString("Per-phase DeleteRange deltas from TreeDB `Stat()` output. `input ranges` counts submitted non-empty ranges; `effective ranges` counts ranges after exact adjacent/overlap coalescing in the write plan; materialized keys are copied into point tombstones by the cached fallback. Span columns report command-WAL range-span overlay activity.\n\n")
	sb.WriteString("| engine | read integrity | iteration mode | phase | db calls | batch calls | batch writes | input ranges | effective ranges | coalesced ranges | visited keys | materialized keys | materialized key bytes | tombstone keys | iterators | snapshot iters | backend iters | memtable iters | queue iters | fast clears | backend direct batches | backend direct keys | span layers | span active | span input | span effective | span materialized keys | span iter probes | span iter skips | span queued units | span flushed | spans flushed | span flush batches |\n")
	sb.WriteString("|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, run := range runs {
		for _, phase := range []string{phaseWrite, phaseRead, phaseIterate, phaseDeleteRange, phaseReopen} {
			result, ok := run.Phases[phase]
			if !ok || len(result.StatDelta) == 0 {
				continue
			}
			if !hasAnyStatDeltaValue(result.StatDelta, deleteRangeCounterKeys...) {
				continue
			}
			fmt.Fprintf(sb, "| %s | %s | %s | %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |\n",
				run.Engine,
				run.ReadIntegrity,
				run.IterationMode,
				phase,
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.calls_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.batch_calls_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.batch_writes_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.input_ranges_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.effective_ranges_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.coalesced_ranges_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.visited_keys_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.materialized_keys_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.materialized_key_bytes_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.tombstone_keys_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.iterators_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.snapshot_iterators_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.backend_iterators_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.memtable_iterators_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.queue_iterators_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.fast_path_clears_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.backend_direct_batches_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.delete_range.backend_direct_keys_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.layers_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.active_spans"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.input_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.effective_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.keys_materialized_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.iterator_probes_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.iterator_skips_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.range_only_queued_units_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.range_only_flushed_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.spans_flushed_total"),
				statDeltaValue(result.StatDelta, "treedb.cache.range_span.flush_batches_total"),
			)
		}
	}
}

type readCounters struct {
	crc32Checks                    int64
	groupedHits                    int64
	groupedMisses                  int64
	groupedStores                  int64
	mmapHits                       int64
	mmapMissOutOfRange             int64
	mmapMissNoMapping              int64
	mmapMissDeadCap                int64
	mmapFallbackReadAt             int64
	outerLeafLoads                 int64
	outerLeafPointLoads            int64
	outerLeafIteratorLoads         int64
	outerLeafChecksumVerifications int64
	outerLeafChecksumSkips         int64
}

func hasReadCounterDeltas(runs []runResult) bool {
	for _, run := range runs {
		for _, phase := range run.Phases {
			if _, ok := readCounterDeltas(phase.StatDelta); ok {
				return true
			}
		}
	}
	return false
}

var deleteRangeCounterKeys = []string{
	"treedb.cache.delete_range.calls_total",
	"treedb.cache.delete_range.batch_calls_total",
	"treedb.cache.delete_range.batch_writes_total",
	"treedb.cache.delete_range.input_ranges_total",
	"treedb.cache.delete_range.effective_ranges_total",
	"treedb.cache.delete_range.coalesced_ranges_total",
	"treedb.cache.delete_range.iterators_total",
	"treedb.cache.delete_range.snapshot_iterators_total",
	"treedb.cache.delete_range.backend_iterators_total",
	"treedb.cache.delete_range.memtable_iterators_total",
	"treedb.cache.delete_range.queue_iterators_total",
	"treedb.cache.delete_range.visited_keys_total",
	"treedb.cache.delete_range.tombstone_keys_total",
	"treedb.cache.delete_range.materialized_keys_total",
	"treedb.cache.delete_range.materialized_key_bytes_total",
	"treedb.cache.delete_range.fast_path_clears_total",
	"treedb.cache.delete_range.backend_direct_batches_total",
	"treedb.cache.delete_range.backend_direct_keys_total",
	"treedb.cache.range_span.layers_total",
	"treedb.cache.range_span.active_layers",
	"treedb.cache.range_span.active_spans",
	"treedb.cache.range_span.input_total",
	"treedb.cache.range_span.effective_total",
	"treedb.cache.range_span.keys_materialized_total",
	"treedb.cache.range_span.iterator_probes_total",
	"treedb.cache.range_span.iterator_skips_total",
	"treedb.cache.range_span.range_only_queued_units_total",
	"treedb.cache.range_span.range_only_flushed_total",
	"treedb.cache.range_span.spans_flushed_total",
	"treedb.cache.range_span.flush_batches_total",
}

func hasDeleteRangeCounterDeltas(runs []runResult) bool {
	for _, run := range runs {
		for _, phase := range run.Phases {
			if hasAnyStatDeltaValue(phase.StatDelta, deleteRangeCounterKeys...) {
				return true
			}
		}
	}
	return false
}

func readCounterDeltas(delta map[string]int64) (readCounters, bool) {
	counters := readCounters{
		crc32Checks:                    statDeltaValue(delta, "treedb.vlog.read.crc32_checks_total", "treedb.cache.vlog_read.crc32_checks_total"),
		groupedHits:                    statDeltaValue(delta, "treedb.vlog.grouped_frame_cache.hits", "treedb.cache.vlog_grouped_frame_cache.hits"),
		groupedMisses:                  statDeltaValue(delta, "treedb.vlog.grouped_frame_cache.misses", "treedb.cache.vlog_grouped_frame_cache.misses"),
		groupedStores:                  statDeltaValue(delta, "treedb.vlog.grouped_frame_cache.stores", "treedb.cache.vlog_grouped_frame_cache.stores"),
		mmapHits:                       statDeltaValue(delta, "treedb.vlog.mmap_read.hits", "treedb.cache.vlog_mmap.read.hits"),
		mmapMissOutOfRange:             statDeltaValue(delta, "treedb.vlog.mmap_read.miss_out_of_range", "treedb.cache.vlog_mmap.read.miss_out_of_range"),
		mmapMissNoMapping:              statDeltaValue(delta, "treedb.vlog.mmap_read.miss_no_mapping", "treedb.cache.vlog_mmap.read.miss_no_mapping"),
		mmapMissDeadCap:                statDeltaValue(delta, "treedb.vlog.mmap_read.miss_dead_mapping_cap", "treedb.cache.vlog_mmap.read.miss_dead_mapping_cap"),
		mmapFallbackReadAt:             statDeltaValue(delta, "treedb.vlog.mmap_read.fallback_readat", "treedb.cache.vlog_mmap.read.fallback_readat"),
		outerLeafLoads:                 statDeltaValue(delta, "treedb.process.read_path.outer_leaf.loads_total"),
		outerLeafPointLoads:            statDeltaValue(delta, "treedb.process.read_path.outer_leaf.point_loads_total"),
		outerLeafIteratorLoads:         statDeltaValue(delta, "treedb.process.read_path.outer_leaf.iterator_loads_total"),
		outerLeafChecksumVerifications: statDeltaValue(delta, "treedb.process.read_path.outer_leaf.checksum.verifications_total"),
		outerLeafChecksumSkips:         statDeltaValue(delta, "treedb.process.read_path.outer_leaf.checksum.skips_total"),
	}
	return counters, counters != (readCounters{})
}

func statDeltaValue(delta map[string]int64, keys ...string) int64 {
	for _, key := range keys {
		if value, ok := delta[key]; ok {
			return value
		}
	}
	return 0
}

func hasAnyStatDeltaValue(delta map[string]int64, keys ...string) bool {
	for _, key := range keys {
		if value, ok := delta[key]; ok && value != 0 {
			return true
		}
	}
	return false
}

func perSec(ops int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(ops) / d.Seconds()
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
