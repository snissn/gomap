package treedb

import (
	"bufio"
	"encoding/json"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

type timelineTraceEvent struct {
	TS         int64  `json:"ts_unix_nano"`
	Op         string `json:"op"`
	Phase      string `json:"phase"`
	IterID     uint64 `json:"iter_id"`
	KeyLen     int    `json:"key_len"`
	ValueLen   int    `json:"value_len"`
	BatchOps   int    `json:"batch_ops"`
	BatchBytes int    `json:"batch_bytes"`
	IterKind   string `json:"iter_kind"`
	IterNexts  int    `json:"iter_nexts"`
	IterMillis int64  `json:"iter_ms"`
	StartLen   int    `json:"iter_start_len"`
	EndLen     int    `json:"iter_end_len"`
}

type timelineIter struct {
	id        uint64
	kind      string
	startTS   int64
	endTS     int64
	startLen  int
	endLen    int
	nexts     int
	iterMilli int64
}

type timelineEvent struct {
	ts         int64
	op         string
	batchOps   int
	batchBytes int
	keyLen     int
	valueLen   int
	iter       *timelineIter
}

type timelinePhase struct {
	name   string
	minTS  int64
	maxTS  int64
	events []timelineEvent
}

func BenchmarkTraceReplayTimeline(b *testing.B) {
	summaryPath := os.Getenv("TREEDB_TRACE_SUMMARY")
	if summaryPath == "" {
		b.Skip("TREEDB_TRACE_SUMMARY not set")
	}
	tracePath := os.Getenv("TREEDB_TRACE_JSONL")
	if tracePath == "" {
		b.Skip("TREEDB_TRACE_JSONL not set")
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
	phaseDurationMs := parseIntEnv("TREEDB_TRACE_TIMELINE_DURATION_MS", 1000)

	timeline, err := loadTraceTimeline(tracePath)
	if err != nil {
		b.Fatal(err)
	}
	if len(timeline) == 0 {
		b.Fatal("timeline trace has no phases")
	}

	totalOps := scaledTotalOps(s, scale)
	if totalOps > 0 {
		b.ReportMetric(float64(totalOps), "ops/iter")
	}

	opts := Options{
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir, err := os.MkdirTemp("", "treedb-trace-timeline-*")
		if err != nil {
			b.Fatal(err)
		}
		opts.Dir = dir
		db, err := Open(opts)
		if err != nil {
			_ = os.RemoveAll(dir)
			b.Fatal(err)
		}

		rng := rand.New(rand.NewSource(seed + int64(i)))
		keyspace := make([][]byte, 0, 1<<20)
		keyIndex := make(map[string]struct{})

		for _, phaseName := range orderedTracePhases(s.Phases) {
			phaseSummary, ok := s.Phases[phaseName]
			if !ok {
				continue
			}
			phaseTimeline, ok := timeline[phaseName]
			if !ok {
				continue
			}
			if err := runTraceTimelinePhase(db, rng, phaseSummary, phaseTimeline, phaseDurationMs, scale, &keyspace, keyIndex); err != nil {
				_ = db.Close()
				_ = os.RemoveAll(dir)
				b.Fatalf("phase %s: %v", phaseName, err)
			}
		}

		if err := db.Close(); err != nil {
			_ = os.RemoveAll(dir)
			b.Fatal(err)
		}
		_ = os.RemoveAll(dir)
	}
}

func loadTraceTimeline(path string) (map[string]*timelinePhase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	phases := make(map[string]*timelinePhase)
	iters := make(map[uint64]*timelineIter)
	var syntheticIterID uint64

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 8*1024*1024)
	for scanner.Scan() {
		var ev timelineTraceEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue
		}
		if ev.Op == "phase" {
			continue
		}
		phase := ev.Phase
		if phase == "" {
			phase = "unknown"
		}
		p := phases[phase]
		if p == nil {
			p = &timelinePhase{name: phase}
			phases[phase] = p
		}
		if p.minTS == 0 || ev.TS < p.minTS {
			p.minTS = ev.TS
		}
		if ev.TS > p.maxTS {
			p.maxTS = ev.TS
		}

		switch ev.Op {
		case "iter_create":
			if ev.IterID == 0 {
				syntheticIterID++
				ev.IterID = syntheticIterID
			}
			iter := iters[ev.IterID]
			if iter == nil {
				iter = &timelineIter{id: ev.IterID}
				iters[ev.IterID] = iter
			}
			iter.kind = ev.IterKind
			iter.startTS = ev.TS
			iter.startLen = ev.StartLen
			iter.endLen = ev.EndLen
		case "iter_close":
			if ev.IterID == 0 {
				syntheticIterID++
				ev.IterID = syntheticIterID
			}
			iter := iters[ev.IterID]
			if iter == nil {
				iter = &timelineIter{id: ev.IterID}
				iters[ev.IterID] = iter
			}
			iter.endTS = ev.TS
			iter.nexts = ev.IterNexts
			iter.iterMilli = ev.IterMillis
		case "batch_write", "get", "has":
			p.events = append(p.events, timelineEvent{
				ts:         ev.TS,
				op:         ev.Op,
				batchOps:   ev.BatchOps,
				batchBytes: ev.BatchBytes,
				keyLen:     ev.KeyLen,
				valueLen:   ev.ValueLen,
			})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	for _, iter := range iters {
		phase := "unknown"
		if iter.startTS == 0 && iter.endTS == 0 {
			continue
		}
		if iter.startTS == 0 {
			iter.startTS = iter.endTS - iter.iterMilli*int64(time.Millisecond)
		}
		if iter.endTS == 0 {
			iter.endTS = iter.startTS + iter.iterMilli*int64(time.Millisecond)
		}
		for name, p := range phases {
			if iter.startTS >= p.minTS && iter.startTS <= p.maxTS {
				phase = name
				break
			}
		}
		p := phases[phase]
		if p == nil {
			p = &timelinePhase{name: phase}
			phases[phase] = p
		}
		if p.minTS == 0 || iter.startTS < p.minTS {
			p.minTS = iter.startTS
		}
		if iter.endTS > p.maxTS {
			p.maxTS = iter.endTS
		}
		p.events = append(p.events, timelineEvent{ts: iter.startTS, op: "iter", iter: iter})
	}

	for _, p := range phases {
		sort.Slice(p.events, func(i, j int) bool { return p.events[i].ts < p.events[j].ts })
	}

	return phases, nil
}

func runTraceTimelinePhase(db *DB, rng *rand.Rand, summary tracePhaseSummary, phase *timelinePhase, phaseDurationMs int, scale float64, keyspace *[][]byte, keyIndex map[string]struct{}) error {
	if phase == nil {
		return nil
	}
	span := phase.maxTS - phase.minTS
	var timeScale float64
	if phaseDurationMs > 0 && span > 0 {
		timeScale = float64(phaseDurationMs) * float64(time.Millisecond) / float64(span)
	}

	start := time.Now()
	var wg sync.WaitGroup

	for _, ev := range phase.events {
		if timeScale > 0 {
			target := start.Add(time.Duration(float64(ev.ts-phase.minTS) * timeScale))
			sleepUntil(target)
		}
		switch ev.op {
		case "batch_write":
			ops := ev.batchOps
			if ops <= 0 {
				ops = sampleDist(rng, summary.BatchOps, 1)
			}
			bytes := ev.batchBytes
			if bytes <= 0 {
				bytes = sampleDist(rng, summary.BatchBytes, 0)
			}
			if err := runTraceBatch(db, rng, summary, ops, bytes, keyspace, keyIndex); err != nil {
				return err
			}
		case "get":
			if len(*keyspace) == 0 {
				continue
			}
			key := (*keyspace)[rng.Intn(len(*keyspace))]
			if _, err := db.Get(key); err != nil {
				return err
			}
		case "has":
			if len(*keyspace) == 0 {
				continue
			}
			key := (*keyspace)[rng.Intn(len(*keyspace))]
			if _, err := db.Has(key); err != nil {
				return err
			}
		case "iter":
			iter := ev.iter
			if iter == nil {
				continue
			}
			startKey, endKey := pickRangeFromLens(rng, *keyspace, iter.startLen, iter.endLen)
			nexts := iter.nexts
			if nexts <= 0 {
				nexts = sampleDist(rng, summary.IterNexts, 0)
			}
			dur := time.Duration(iter.endTS-iter.startTS) * time.Nanosecond
			if dur < 0 {
				dur = 0
			}
			if timeScale > 0 {
				dur = time.Duration(math.Max(0, float64(dur)*timeScale))
			}
			wg.Add(1)
			go func(kind string, nexts int, start, end []byte, duration time.Duration) {
				defer wg.Done()
				if len(*keyspace) == 0 {
					return
				}
				if kind == "reverse" {
					it, err := db.ReverseIterator(start, end)
					if err != nil {
						return
					}
					consumeIterator(it, nexts, duration)
					_ = it.Close()
					return
				}
				it, err := db.Iterator(start, end)
				if err != nil {
					return
				}
				consumeIterator(it, nexts, duration)
				_ = it.Close()
			}(iter.kind, nexts, startKey, endKey, dur)
		}
	}

	wg.Wait()
	return nil
}

func sleepUntil(t time.Time) {
	for {
		now := time.Now()
		if !now.Before(t) {
			return
		}
		if remain := time.Until(t); remain > 0 {
			time.Sleep(remain)
		}
	}
}

func pickRangeFromLens(rng *rand.Rand, keyspace [][]byte, startLen, endLen int) ([]byte, []byte) {
	if len(keyspace) == 0 {
		return nil, nil
	}
	startKey := keyspace[rng.Intn(len(keyspace))]
	endKey := keyspace[rng.Intn(len(keyspace))]
	return trimKey(startKey, startLen), trimKey(endKey, endLen)
}

func trimKey(key []byte, n int) []byte {
	if n <= 0 {
		return nil
	}
	if n > len(key) {
		n = len(key)
	}
	out := make([]byte, n)
	copy(out, key[:n])
	return out
}

func consumeIterator(it Iterator, nexts int, duration time.Duration) {
	if nexts <= 0 {
		return
	}
	if duration <= 0 {
		for i := 0; i < nexts && it.Valid(); i++ {
			it.Next()
		}
		return
	}
	step := duration / time.Duration(nexts)
	for i := 0; i < nexts && it.Valid(); i++ {
		if step > 0 {
			time.Sleep(step)
		}
		it.Next()
	}
}
