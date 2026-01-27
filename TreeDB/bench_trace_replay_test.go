package treedb

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type traceDistSummary struct {
	Count int     `json:"count"`
	Min   int     `json:"min"`
	P50   int     `json:"p50"`
	P90   int     `json:"p90"`
	P99   int     `json:"p99"`
	Max   int     `json:"max"`
	Avg   float64 `json:"avg"`
}

type tracePhaseSummary struct {
	Ops          map[string]int `json:"ops"`
	BatchOps     traceDistSummary
	BatchBytes   traceDistSummary
	IterNexts    traceDistSummary
	GetKeyLens   traceDistSummary
	GetValueLens traceDistSummary
	SetKeyLens   traceDistSummary
	SetValueLens traceDistSummary
	IterStartLen traceDistSummary
	IterEndLen   traceDistSummary
	IterKinds    map[string]int `json:"iter_create_kind"`
}

type traceSummary struct {
	TotalEvents int                          `json:"total_events"`
	Phases      map[string]tracePhaseSummary `json:"phases"`
}

func BenchmarkTraceReplay(b *testing.B) {
	summaryPath := os.Getenv("TREEDB_TRACE_SUMMARY")
	if summaryPath == "" {
		b.Skip("TREEDB_TRACE_SUMMARY not set")
	}
	data, err := os.ReadFile(summaryPath)
	if err != nil {
		b.Fatal(err)
	}
	var s traceSummary
	if err := json.Unmarshal(data, &s); err != nil {
		b.Fatal(err)
	}
	if len(s.Phases) == 0 {
		b.Fatal("trace summary has no phases")
	}

	disableWAL := parseBoolEnv("TREEDB_TRACE_DISABLE_WAL", false)
	scale := parseFloatEnv("TREEDB_TRACE_SCALE", 1.0)
	flushThreshold := parseIntEnv("TREEDB_TRACE_FLUSH_THRESHOLD", 32*1024*1024)
	memtableShards := parseIntEnv("TREEDB_TRACE_MEMTABLE_SHARDS", 0)
	seed := parseInt64Env("TREEDB_TRACE_SEED", 1)

	totalOps := scaledTotalOps(s, scale)
	if totalOps > 0 {
		b.ReportMetric(float64(totalOps), "ops/iter")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir, err := os.MkdirTemp("", "treedb-trace-replay-*")
		if err != nil {
			b.Fatal(err)
		}

		opts := Options{
			Dir:            dir,
			FlushThreshold: int64(flushThreshold),
			MemtableShards: memtableShards,
			Durability: func() DurabilityMode {
				if disableWAL {
					return DurabilityWALOffRelaxed
				}
				return DurabilityWALOnRelaxed
			}(),
			ValueLog: ValueLogOptions{
				ReadIntegrity: IntegritySkipChecksums,
			},
		}
		db, err := Open(opts)
		if err != nil {
			_ = os.RemoveAll(dir)
			b.Fatal(err)
		}

		rng := rand.New(rand.NewSource(seed + int64(i)))
		keyspace := make([][]byte, 0, 1<<20)
		keyIndex := make(map[string]struct{})

		for _, phase := range orderedTracePhases(s.Phases) {
			if err := runTracePhase(db, rng, s.Phases[phase], scale, &keyspace, keyIndex); err != nil {
				_ = db.Close()
				_ = os.RemoveAll(dir)
				b.Fatalf("phase %s: %v", phase, err)
			}
		}

		if err := db.Close(); err != nil {
			_ = os.RemoveAll(dir)
			b.Fatal(err)
		}
		_ = os.RemoveAll(dir)
	}
}

func parseBoolEnv(key string, def bool) bool {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	switch strings.ToLower(val) {
	case "1", "true", "t", "yes", "y":
		return true
	case "0", "false", "f", "no", "n":
		return false
	default:
		return def
	}
}

func parseIntEnv(key string, def int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return n
}

func parseInt64Env(key string, def int64) int64 {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func parseFloatEnv(key string, def float64) float64 {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	n, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return def
	}
	return n
}

func orderedTracePhases(phases map[string]tracePhaseSummary) []string {
	order := make([]string, 0, len(phases))
	seen := make(map[string]struct{}, len(phases))
	for _, phase := range []string{"restore", "catchup"} {
		if _, ok := phases[phase]; ok {
			order = append(order, phase)
			seen[phase] = struct{}{}
		}
	}
	rest := make([]string, 0, len(phases)-len(order))
	for phase := range phases {
		if _, ok := seen[phase]; ok {
			continue
		}
		rest = append(rest, phase)
	}
	sort.Strings(rest)
	return append(order, rest...)
}

func scaledTotalOps(s traceSummary, scale float64) int {
	total := 0
	for _, phase := range s.Phases {
		for _, count := range phase.Ops {
			total += scaledCount(count, scale)
		}
	}
	return total
}

func runTracePhase(db *DB, rng *rand.Rand, p tracePhaseSummary, scale float64, keyspace *[][]byte, keyIndex map[string]struct{}) error {
	batchWrites := scaledCount(p.Ops["batch_write"], scale)
	if batchWrites == 0 {
		return nil
	}

	getsTotal := scaledCount(p.Ops["get"], scale)
	hasTotal := scaledCount(p.Ops["has"], scale)
	iterCreates := scaledCount(p.Ops["iter_create"], scale)
	deletesTotal := scaledCount(p.Ops["delete"], scale)

	getsPerBatch := float64(getsTotal) / float64(batchWrites)
	hasPerBatch := float64(hasTotal) / float64(batchWrites)
	itersPerBatch := float64(iterCreates) / float64(batchWrites)
	deletesPerBatch := float64(deletesTotal) / float64(batchWrites)

	getDebt := 0.0
	hasDebt := 0.0
	iterDebt := 0.0
	deleteDebt := 0.0

	for i := 0; i < batchWrites; i++ {
		ops := sampleDist(rng, p.BatchOps, 1)
		targetBytes := sampleDist(rng, p.BatchBytes, 0)
		if err := runTraceBatch(db, rng, p, ops, targetBytes, keyspace, keyIndex); err != nil {
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

		hasDebt += hasPerBatch
		for hasDebt >= 1.0 {
			hasDebt -= 1.0
			if len(*keyspace) == 0 {
				break
			}
			key := (*keyspace)[rng.Intn(len(*keyspace))]
			if _, err := db.Has(key); err != nil {
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
			start, end := pickRange(rng, p, *keyspace)
			if useReverseIter(rng, p.IterKinds) {
				it, err := db.ReverseIterator(start, end)
				if err != nil {
					return err
				}
				for j := 0; j < nexts && it.Valid(); j++ {
					it.Next()
				}
				if err := it.Close(); err != nil {
					return err
				}
				continue
			}
			it, err := db.Iterator(start, end)
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

		deleteDebt += deletesPerBatch
		for deleteDebt >= 1.0 {
			deleteDebt -= 1.0
			if len(*keyspace) == 0 {
				break
			}
			idx := rng.Intn(len(*keyspace))
			key := (*keyspace)[idx]
			if err := db.Delete(key); err != nil {
				return err
			}
			delete(keyIndex, string(key))
			(*keyspace)[idx] = (*keyspace)[len(*keyspace)-1]
			*keyspace = (*keyspace)[:len(*keyspace)-1]
		}
	}
	return nil
}

func runTraceBatch(db *DB, rng *rand.Rand, p tracePhaseSummary, ops int, targetBytes int, keyspace *[][]byte, keyIndex map[string]struct{}) error {
	batch := db.NewBatch()
	if batch == nil {
		return fmt.Errorf("batch unsupported")
	}
	usedBytes := 0
	for i := 0; i < ops; i++ {
		key := randomKey(rng, p.SetKeyLens)
		value := randomValue(rng, p.SetValueLens)
		usedBytes += len(key) + len(value)

		if _, ok := keyIndex[string(key)]; !ok {
			keyIndex[string(key)] = struct{}{}
			*keyspace = append(*keyspace, key)
		}
		if err := batch.Set(key, value); err != nil {
			return err
		}
		if targetBytes > 0 && usedBytes >= targetBytes {
			break
		}
	}
	if err := batch.Write(); err != nil {
		return err
	}
	if err := batch.Close(); err != nil {
		return err
	}
	return nil
}

func randomKey(rng *rand.Rand, dist traceDistSummary) []byte {
	n := sampleDist(rng, dist, 16)
	if n <= 0 {
		n = 16
	}
	key := make([]byte, n)
	for i := range key {
		key[i] = byte(rng.Intn(256))
	}
	return key
}

func randomValue(rng *rand.Rand, dist traceDistSummary) []byte {
	n := sampleDist(rng, dist, 32)
	if n <= 0 {
		n = 32
	}
	value := make([]byte, n)
	for i := range value {
		value[i] = byte(rng.Intn(256))
	}
	return value
}

func pickRange(rng *rand.Rand, p tracePhaseSummary, keyspace [][]byte) ([]byte, []byte) {
	startLen := sampleDist(rng, p.IterStartLen, 0)
	endLen := sampleDist(rng, p.IterEndLen, 0)
	if startLen == 0 && endLen == 0 {
		key := keyspace[rng.Intn(len(keyspace))]
		return key, nil
	}
	key := keyspace[rng.Intn(len(keyspace))]
	start := key
	end := key
	if startLen > 0 && startLen < len(key) {
		start = key[:startLen]
	}
	if endLen > 0 && endLen < len(key) {
		end = key[:endLen]
	}
	if endLen == 0 {
		end = nil
	}
	return start, end
}

func useReverseIter(rng *rand.Rand, kinds map[string]int) bool {
	if kinds == nil {
		return false
	}
	total := 0
	for _, v := range kinds {
		total += v
	}
	if total == 0 {
		return false
	}
	rev := kinds["reverse"]
	if rev <= 0 {
		return false
	}
	return rng.Intn(total) < rev
}

func sampleDist(rng *rand.Rand, dist traceDistSummary, fallback int) int {
	if dist.Count == 0 {
		return fallback
	}
	p := rng.Float64()
	switch {
	case p <= 0.50:
		return dist.P50
	case p <= 0.90:
		return dist.P90
	case p <= 0.99:
		return dist.P99
	default:
		return dist.Max
	}
}

func scaledCount(count int, scale float64) int {
	if count == 0 {
		return 0
	}
	if scale <= 0 {
		scale = 1
	}
	return int(math.Max(1, float64(count)*scale))
}
