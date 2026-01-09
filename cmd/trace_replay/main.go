package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
)

type distSummary struct {
	Count int     `json:"count"`
	Min   int     `json:"min"`
	P50   int     `json:"p50"`
	P90   int     `json:"p90"`
	P99   int     `json:"p99"`
	Max   int     `json:"max"`
	Avg   float64 `json:"avg"`
}

type phaseSummary struct {
	Ops          map[string]int `json:"ops"`
	BatchOps     distSummary    `json:"batch_ops"`
	BatchBytes   distSummary    `json:"batch_bytes"`
	IterNexts    distSummary    `json:"iter_nexts"`
	GetKeyLens   distSummary    `json:"get_key_lens"`
	GetValueLens distSummary    `json:"get_value_lens"`
	SetKeyLens   distSummary    `json:"set_key_lens"`
	SetValueLens distSummary    `json:"set_value_lens"`
	IterStartLen distSummary    `json:"iter_start_lens"`
	IterEndLen   distSummary    `json:"iter_end_lens"`
}

type summary struct {
	TotalEvents int                     `json:"total_events"`
	Phases      map[string]phaseSummary `json:"phases"`
}

func main() {
	summaryPath := flag.String("summary", "", "Path to trace_bench JSON summary")
	outDir := flag.String("dir", "", "DB directory (default: temp)")
	seed := flag.Int64("seed", 1, "RNG seed")
	scale := flag.Float64("scale", 1.0, "Scale factor for counts in summary")
	mode := flag.String("mode", "cached", "TreeDB mode: cached or backend")
	disableWAL := flag.Bool("disable-wal", false, "Disable WAL")
	disableVlog := flag.Bool("disable-vlog", false, "Disable value log")
	flushThreshold := flag.Int("flush-threshold", 32*1024*1024, "Flush threshold bytes")
	memtableShards := flag.Int("memtable-shards", 0, "Memtable shards (0 = default)")
	flag.Parse()

	if *summaryPath == "" {
		fmt.Fprintln(os.Stderr, "missing -summary")
		os.Exit(2)
	}

	var s summary
	data, err := os.ReadFile(*summaryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read summary: %v\n", err)
		os.Exit(2)
	}
	if err := json.Unmarshal(data, &s); err != nil {
		fmt.Fprintf(os.Stderr, "parse summary: %v\n", err)
		os.Exit(2)
	}

	dir := *outDir
	if dir == "" {
		tmp, err := os.MkdirTemp("", "trace-replay-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "temp dir: %v\n", err)
			os.Exit(2)
		}
		dir = tmp
	}
	fmt.Printf("trace_replay: dir=%s\n", dir)

	modeVal := treedb.ModeCached
	switch strings.ToLower(*mode) {
	case "backend", "uncached", "raw":
		modeVal = treedb.ModeBackend
	}

	opts := treedb.Options{
		Dir:                 dir,
		Mode:                modeVal,
		DisableWAL:          *disableWAL,
		DisableValueLog:     *disableVlog,
		FlushThreshold:      int64(*flushThreshold),
		MemtableShards:      *memtableShards,
		AllowUnsafe:         true,
		DisableReadChecksum: true,
	}
	db, err := treedb.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open treedb: %v\n", err)
		os.Exit(2)
	}
	defer func() { _ = db.Close() }()

	rng := rand.New(rand.NewSource(*seed))
	keyspace := make([][]byte, 0, 1<<20)
	keyIndex := make(map[string]struct{})

	phases := s.Phases
	if len(phases) == 0 {
		fmt.Fprintln(os.Stderr, "no phases in summary")
		os.Exit(2)
	}

	for phase, p := range phases {
		fmt.Printf("phase=%s\n", phase)
		if err := runPhase(db, rng, p, *scale, &keyspace, keyIndex); err != nil {
			fmt.Fprintf(os.Stderr, "phase %s: %v\n", phase, err)
			os.Exit(2)
		}
	}

	fmt.Printf("trace_replay: done\n")
	fmt.Printf("trace_replay: db=%s\n", filepath.Base(dir))
}

func runPhase(db *treedb.DB, rng *rand.Rand, p phaseSummary, scale float64, keyspace *[][]byte, keyIndex map[string]struct{}) error {
	batchWrites := scaledCount(p.Ops["batch_write"], scale)
	if batchWrites == 0 {
		return nil
	}

	getsTotal := scaledCount(p.Ops["get"], scale)
	iterCreates := scaledCount(p.Ops["iter_create"], scale)

	getsPerBatch := float64(getsTotal) / float64(batchWrites)
	itersPerBatch := float64(iterCreates) / float64(batchWrites)

	getDebt := 0.0
	iterDebt := 0.0

	for i := 0; i < batchWrites; i++ {
		ops := sampleDist(rng, p.BatchOps, 1)
		avgValueLen := estimateValueLen(p, ops)
		if err := runBatch(db, rng, p, ops, avgValueLen, keyspace, keyIndex); err != nil {
			return err
		}

		getDebt += getsPerBatch
		for getDebt >= 1.0 {
			getDebt -= 1.0
			if len(*keyspace) == 0 {
				break
			}
			key := (*keyspace)[rng.Intn(len(*keyspace))]
			if _, err := db.Get(key); err != nil {
				return err
			}
		}

		iterDebt += itersPerBatch
		for iterDebt >= 1.0 {
			iterDebt -= 1.0
			if len(*keyspace) == 0 {
				break
			}
			nexts := sampleDist(rng, p.IterNexts, 0)
			it, err := db.Iterator(nil, nil)
			if err != nil {
				return err
			}
			for j := 0; j < nexts && it.Valid(); j++ {
				it.Next()
			}
			if err := it.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func runBatch(db *treedb.DB, rng *rand.Rand, p phaseSummary, ops int, avgVal int, keyspace *[][]byte, keyIndex map[string]struct{}) error {
	batch := db.NewBatch()
	if batch == nil {
		return fmt.Errorf("batch unavailable")
	}
	defer func() { _ = batch.Close() }()

	for i := 0; i < ops; i++ {
		key := makeKey(rng, p, len(*keyspace))
		val := makeValue(rng, p, avgVal)
		if err := batch.Set(key, val); err != nil {
			return err
		}
		if _, ok := keyIndex[string(key)]; !ok {
			keyIndex[string(key)] = struct{}{}
			*keyspace = append(*keyspace, key)
		}
	}
	return batch.Write()
}

func makeKey(rng *rand.Rand, p phaseSummary, idx int) []byte {
	l := sampleDist(rng, p.GetKeyLens, 16)
	if l < 8 {
		l = 8
	}
	key := make([]byte, l)
	copy(key, fmt.Sprintf("k%08x", idx))
	for i := 8; i < l; i++ {
		key[i] = byte(rng.Intn(26) + 'a')
	}
	return key
}

func makeValue(rng *rand.Rand, p phaseSummary, avgVal int) []byte {
	l := avgVal
	if l <= 0 {
		l = sampleDist(rng, p.GetValueLens, 64)
	}
	if l < 8 {
		l = 8
	}
	val := make([]byte, l)
	for i := range val {
		val[i] = byte(rng.Intn(26) + 'a')
	}
	return val
}

func scaledCount(v int, scale float64) int {
	if scale <= 0 {
		return 0
	}
	return int(math.Round(float64(v) * scale))
}

func estimateValueLen(p phaseSummary, ops int) int {
	if p.BatchBytes.Count > 0 && ops > 0 {
		return int(p.BatchBytes.Avg / float64(ops))
	}
	if p.GetValueLens.Count > 0 {
		return int(p.GetValueLens.Avg)
	}
	return 64
}

func sampleDist(rng *rand.Rand, d distSummary, fallback int) int {
	if d.Count == 0 {
		return fallback
	}
	r := rng.Float64()
	switch {
	case r < 0.50:
		return uniformInt(rng, d.Min, d.P50)
	case r < 0.90:
		return uniformInt(rng, d.P50, d.P90)
	case r < 0.99:
		return uniformInt(rng, d.P90, d.P99)
	default:
		return uniformInt(rng, d.P99, d.Max)
	}
}

func uniformInt(rng *rand.Rand, min, max int) int {
	if max <= min {
		return min
	}
	return rng.Intn(max-min+1) + min
}
