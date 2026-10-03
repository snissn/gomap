package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
)

var (
	quicksilverCase      = flag.String("quicksilver-case", "random4k", "quicksilver fixture: random4k (100k keys) or structured256 (250k keys)")
	quicksilverReads     = flag.Int("quicksilver-reads", 2000000, "quicksilver aggregate operations per fixed read phase")
	quicksilverReadBatch = flag.Int("quicksilver-read-batch", 64, "quicksilver reads per owned-read snapshot; 1 uses ordinary Get")
	quicksilverDuration  = flag.Duration("quicksilver-duration", 4*time.Second, "quicksilver concurrent read/update duration")
	quicksilverUpdates   = flag.Int("quicksilver-updates", 40000, "quicksilver distinct updated keys (default capped at key count)")
)

const quicksilverTraceLength = 65536
const quicksilverSampleLimit = 1000000

var quicksilverPhaseNames = []string{"quicksilver_hits", "quicksilver_misses", "quicksilver_mixed", "quicksilver_concurrent"}

type quicksilverConfig struct {
	Case      string        `json:"case"`
	Keys      int           `json:"keys"`
	Reads     int           `json:"aggregate_reads"`
	Workers   int           `json:"workers"`
	ReadBatch int           `json:"reads_per_snapshot"`
	Duration  time.Duration `json:"concurrent_duration_ns"`
	Updates   int           `json:"updates"`
}

func (c quicksilverConfig) valueSize() int {
	if c.Case == "random4k" {
		return 4096
	}
	return 256
}
func (c quicksilverConfig) validate() error {
	if c.Case != "random4k" && c.Case != "structured256" {
		return fmt.Errorf("quicksilver: unknown case %q", c.Case)
	}
	if c.Keys < 1 || c.Keys > 10000000 || c.Reads < 1 || c.Reads > 1000000000 || c.Workers < 1 || c.Workers > 1024 || c.ReadBatch < 1 || c.ReadBatch > 1000000 || c.Duration <= 0 || c.Duration > time.Hour || c.Updates < 1 || c.Updates > c.Keys {
		return fmt.Errorf("quicksilver: invalid configuration: %+v", c)
	}
	return nil
}
func resolveQuicksilverConfig(base BenchConfig, isSet map[string]bool) (quicksilverConfig, error) {
	c := quicksilverConfig{Case: *quicksilverCase, Keys: base.Keys, Reads: *quicksilverReads, Workers: base.ReadWorkers, ReadBatch: *quicksilverReadBatch, Duration: *quicksilverDuration, Updates: *quicksilverUpdates}
	if !isSet["keys"] {
		c.Keys = 100000
		if c.Case == "structured256" {
			c.Keys = 250000
		}
	}
	if !isSet["read-workers"] {
		c.Workers = 4
	}
	if !isSet["quicksilver-updates"] {
		c.Updates = min(c.Updates, c.Keys)
	}
	for _, name := range []string{"test", "seed", "keycounts", "keyscale", "keys-min", "keys-max", "key-shape", "val-pattern", "val-pool-size", "read-require-hit", "checkpoint-between-tests", "checkpoint-every-ops", "checkpoint-every-bytes", "vacuum-between-tests", "settle-before-scans", "treedb-vlog-rewrite-after-run", "treedb-vacuum-after-vlog-rewrite-run", "checkpoint-settle-before-tests", "checkpoint-settle-timeout", "range-queries", "range-span", "write-workers", "batch-delete-range-width", "batch-delete-ranges-per-batch", "batch-delete-range-validate", "batch-delete-range-refill", "batch-write-steady-checkpoint-bytes", "batch-write-dict-warmup", "outdir", "format", "flushdrain-checkpoint-max", "treedb-cache-stats-before-reads", "treedb-cache-stats-after-tests"} {
		if isSet[name] {
			return c, fmt.Errorf("quicksilver: -%s does not apply to this fixed workflow", name)
		}
	}
	if isSet["valsize"] && base.ValueSize != c.valueSize() {
		return c, fmt.Errorf("quicksilver: -valsize must match %s (%d)", c.Case, c.valueSize())
	}
	if isSet["batchsize"] && base.BatchSize != 1000 {
		return c, fmt.Errorf("quicksilver: load/update batchsize is fixed at 1000")
	}
	return c, c.validate()
}

type quicksilverPhase struct {
	Name               string            `json:"name"`
	Ops                int               `json:"ops"`
	Seconds            float64           `json:"seconds"`
	CompositionSeconds float64           `json:"composition_seconds"`
	OpsPerSec          float64           `json:"ops_per_sec"`
	Samples            int               `json:"samples"`
	P50US              float64           `json:"p50_us"`
	P99US              float64           `json:"p99_us"`
	P999US             float64           `json:"p999_us"`
	MaxUS              float64           `json:"max_us"`
	AllocatedBytes     uint64            `json:"process_allocated_bytes"`
	Mallocs            uint64            `json:"process_mallocs"`
	BytesPerOp         float64           `json:"process_bytes_per_op"`
	AllocsPerOp        float64           `json:"process_allocs_per_op"`
	HeapBefore         uint64            `json:"process_heap_alloc_before"`
	HeapAfter          uint64            `json:"process_heap_alloc_after"`
	GCPauseNS          uint64            `json:"process_gc_pause_ns"`
	GCCycles           uint32            `json:"process_gc_cycles"`
	StatsBefore        map[string]string `json:"stats_before,omitempty"`
	StatsAfter         map[string]string `json:"stats_after,omitempty"`
}
type quicksilverResult struct {
	wrapper             kvstore.DB
	Engine              string             `json:"engine"`
	DBName              string             `json:"db_name"`
	Config              quicksilverConfig  `json:"config"`
	GOMAXPROCS          int                `json:"gomaxprocs"`
	Profiled            bool               `json:"profiled"`
	UpdateStride        int                `json:"update_stride"`
	TraceBytes          int                `json:"trace_bytes"`
	SampleCapacity      int                `json:"sample_capacity"`
	SampleBytes         int                `json:"sample_bytes"`
	LoadSeconds         float64            `json:"load_seconds"`
	InitialCheckpointMS float64            `json:"initial_checkpoint_ms"`
	ReopenMS            float64            `json:"reopen_ms"`
	FinalCheckpointMS   float64            `json:"final_checkpoint_ms"`
	FinalReopenMS       float64            `json:"final_reopen_ms"`
	Phases              []quicksilverPhase `json:"phases"`
	UpdateBatches       []float64          `json:"update_batch_ms"`
	Checkpoints         []float64          `json:"checkpoint_ms"`
	UpdatedKeys         int                `json:"updated_keys"`
	VerifiedKeys        int                `json:"verified_keys"`
	VerifiedMisses      int                `json:"verified_misses"`
	InitialStats        map[string]string  `json:"initial_stats,omitempty"`
	FinalStats          map[string]string  `json:"final_stats,omitempty"`
	InitialFiles        map[string]int64   `json:"initial_files"`
	FinalFiles          map[string]int64   `json:"final_files"`
	Flags               map[string]string  `json:"registered_cli_flags,omitempty"`
	DataDir             string             `json:"data_dir,omitempty"`
}

func quicksilverKey(id uint64) [32]byte {
	var k [32]byte
	copy(k[:], "zone/settings/config/v1/")
	binary.BigEndian.PutUint64(k[24:], id)
	return k
}
func quicksilverValue(dst []byte, id, gen uint64, random bool) {
	if random {
		r := rand.New(rand.NewPCG(id+51, 99))
		for p := 16; p < len(dst); p += 8 {
			x := r.Uint64()
			for j := 0; j < 8 && p+j < len(dst); j++ {
				dst[p+j] = byte(x >> (8 * j))
			}
		}
	} else {
		const text = `{"enabled":true,"action":"allow","ttl":300,"region":"global"}`
		for p := 16; p < len(dst); p++ {
			dst[p] = text[(p-16)%len(text)]
		}
	}
	binary.BigEndian.PutUint64(dst, id)
	binary.BigEndian.PutUint64(dst[8:], gen)
}
func quicksilverUpdateStride(n int) int {
	gcd := func(a, b int) int {
		for b != 0 {
			a, b = b, a%b
		}
		return a
	}
	stride := 7919
	for gcd(stride, n) != 1 {
		stride++
	}
	return stride
}
func quicksilverStats(db kvstore.DB) map[string]string {
	if s, ok := db.(kvstore.StatsProvider); ok {
		return s.Stats()
	}
	return nil
}
func quicksilverFiles(dir string) (map[string]int64, error) {
	out := map[string]int64{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, e := d.Info()
			if e != nil {
				return e
			}
			rel, e := filepath.Rel(dir, path)
			if e != nil {
				return e
			}
			out[rel] = info.Size()
		}
		return nil
	})
	return out, err
}
func quicksilverWrite(db kvstore.DB, c quicksilverConfig, offset, count, stride int, update bool) (err error) {
	// Normal LMDB batches require creation, staging and commit on one OS thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	b, err := db.(kvstore.Batcher).NewBatch()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, b.Close()) }()
	for j := 0; j < count; j++ {
		i := offset + j
		var gen uint64
		if update {
			i = int(int64(i) * int64(stride) % int64(c.Keys))
			gen = 1
		}
		id := uint64(i) * 2
		k := quicksilverKey(id)
		v := make([]byte, c.valueSize())
		quicksilverValue(v, id, gen, c.Case == "random4k")
		if err = b.Set(k[:], v); err != nil {
			return err
		}
	}
	return b.CommitSync()
}
func quicksilverGet(get func([]byte) ([]byte, error), key []byte) ([]byte, error) {
	v, err := get(key)
	if errors.Is(err, treedb.ErrKeyNotFound) {
		return nil, nil
	}
	return v, err
}
func quicksilverCheckValue(id uint64, v []byte, size int, concurrent bool) error {
	if id%2 != 0 {
		if v != nil {
			return fmt.Errorf("quicksilver: absent key %d returned bytes", id)
		}
		return nil
	}
	if len(v) != size || binary.BigEndian.Uint64(v) != id {
		return fmt.Errorf("quicksilver: bad identity/length at %d (length %d)", id, len(v))
	}
	gen := binary.BigEndian.Uint64(v[8:])
	if gen > 1 || (!concurrent && gen != 0) {
		return fmt.Errorf("quicksilver: bad generation %d at %d", gen, id)
	}
	return nil
}

type quicksilverReader struct {
	samples []int64
	count   int
	maximum int64
	err     error
}
type quicksilverFixture struct {
	keys    [][32]byte
	ids     []uint64
	readers []quicksilverReader
	samples []int64
}

func newQuicksilverFixture(c quicksilverConfig) *quicksilverFixture {
	f := &quicksilverFixture{keys: make([][32]byte, 2*quicksilverTraceLength), ids: make([]uint64, quicksilverTraceLength), readers: make([]quicksilverReader, c.Workers), samples: make([]int64, quicksilverSampleLimit)}
	r := rand.New(rand.NewPCG(24, 91))
	for i := range f.ids {
		id := uint64(r.IntN(c.Keys)) * 2
		f.ids[i] = id
		f.keys[2*i] = quicksilverKey(id)
		f.keys[2*i+1] = quicksilverKey(id + 1)
	}
	offset := 0
	for w := range f.readers {
		size := quicksilverSampleLimit / c.Workers
		if w < quicksilverSampleLimit%c.Workers {
			size++
		}
		f.readers[w].samples = f.samples[offset : offset : offset+size]
		offset += size
	}

	return f
}

// Every worker and the paced writer use the same barrier; errors cancel waits,
// then all goroutines join before this returns and the owner can close the DB.
func quicksilverReadPhase(db kvstore.DB, c quicksilverConfig, f *quicksilverFixture, mode int, guard *benchGuard, writer func(context.Context) error, stopProfile func()) (quicksilverPhase, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	ready.Add(c.Workers)
	start := make(chan struct{})
	var writerErr error
	writerDone := make(chan struct{})
	for w := range f.readers {
		s := &f.readers[w]
		s.samples = s.samples[:0]
		s.count = 0
		s.maximum = 0
		s.err = nil
		wg.Add(1)
		go func(w int, s *quicksilverReader) {
			defer wg.Done()
			var snap kvstore.ReadSnapshot
			defer func() {
				if snap != nil {
					s.err = errors.Join(s.err, snap.Close())
					if s.err != nil {
						cancel()
					}
				}
			}()
			ready.Done()
			<-start
			deadline := time.Now().Add(c.Duration)
			getter := db.Get
			limit := c.Reads / c.Workers
			if w < c.Reads%c.Workers {
				limit++
			}
			for i := 0; mode == 3 || i < limit; i++ {
				if i%256 == 0 {
					if ctx.Err() != nil {
						return
					}
					if e := guard.Checkpoint(); e != nil {
						s.err = e
						cancel()
						return
					}
					if mode == 3 && time.Now().After(deadline) {
						return
					}
				}
				global := i*c.Workers + w
				pos := global % len(f.ids)
				miss := mode == 1 || (mode >= 2 && global%11 != 0)
				idx := pos * 2
				id := f.ids[pos]
				if miss {
					idx++
					id++
				}
				t := time.Now()
				if c.ReadBatch > 1 && i%c.ReadBatch == 0 {
					if snap != nil {
						if e := snap.Close(); e != nil {
							snap = nil
							s.err = e
							cancel()
							return
						}
						snap = nil
					}
					var e error
					snap, e = db.(kvstore.ReadSnapshotter).AcquireReadSnapshot()
					if e != nil {
						s.err = e
						cancel()
						return
					}
					if snap == nil {
						s.err = errors.New("quicksilver: nil snapshot")
						cancel()
						return
					}
					getter = snap.Get
				}
				v, e := quicksilverGet(getter, f.keys[idx][:])
				ns := time.Since(t).Nanoseconds()
				if e == nil {
					e = quicksilverCheckValue(id, v, c.valueSize(), mode == 3)
				}
				if e != nil {
					s.err = e
					cancel()
					return
				}
				s.count++
				if ns > s.maximum {
					s.maximum = ns
				}
				stride := 4
				if mode == 3 {
					stride = 16
				}
				if (i^(i>>6)^(i>>12))%stride == 0 && len(s.samples) < cap(s.samples) {
					s.samples = append(s.samples, ns)
				}
			}
		}(w, s)
	}
	ready.Wait()
	if writer != nil {
		go func() {
			defer close(writerDone)
			<-start
			writerErr = writer(ctx)
			if writerErr != nil {
				cancel()
			}
		}()
	} else {
		close(writerDone)
	}
	p := quicksilverPhase{Name: quicksilverPhaseNames[mode], StatsBefore: quicksilverStats(db)}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	clock := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(clock)
	runtime.ReadMemStats(&after)
	if stopProfile != nil {
		stopProfile()
	}
	p.StatsAfter = quicksilverStats(db)
	<-writerDone
	p.CompositionSeconds = time.Since(clock).Seconds()
	p.Seconds = elapsed.Seconds()
	p.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
	p.Mallocs = after.Mallocs - before.Mallocs
	p.GCCycles = after.NumGC - before.NumGC
	p.HeapBefore = before.HeapAlloc
	p.HeapAfter = after.HeapAlloc
	p.GCPauseNS = after.PauseTotalNs - before.PauseTotalNs
	// Merge in the existing fixed-capacity backing buffers. Sorting is outside read timers.
	all := f.samples[:0]
	err := writerErr
	for _, s := range f.readers {
		err = errors.Join(err, s.err)
		p.Ops += s.count
		p.MaxUS = max(p.MaxUS, float64(s.maximum)/1000)
		n := len(all)
		all = all[:n+len(s.samples)]
		copy(all[n:], s.samples)
	}
	if err != nil {
		return p, err
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p.Samples = len(all)
	q := func(frac float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(float64(len(all)-1)*frac)]) / 1000
	}
	p.P50US = q(.5)
	p.P99US = q(.99)
	p.P999US = q(.999)
	if p.Ops > 0 {
		p.OpsPerSec = float64(p.Ops) / p.Seconds
		p.BytesPerOp = float64(p.AllocatedBytes) / float64(p.Ops)
		p.AllocsPerOp = float64(p.Mallocs) / float64(p.Ops)
	}
	return p, nil
}

func quicksilverProfilePhase(cfg BenchConfig, name, engine string, fn func(func()) (quicksilverPhase, error)) (p quicksilverPhase, err error) {
	hooks := profileHooksFromConfig(cfg)
	var base string
	if shouldAllocsProfile(cfg, name) {
		base, err = hooks.writeAllocsSnapshotTemp("quicksilver_allocs_base")
		if err != nil {
			return p, err
		}
		defer os.Remove(base)
	}
	var cpu *os.File
	if shouldCPUProfile(cfg, name) {
		cpu, err = os.Create(cfg.CPUProfile + "_" + name + "_" + engine + ".pprof")
		if err != nil {
			return p, err
		}
		if err = hooks.startCPUProfile(cpu); err != nil {
			return p, errors.Join(err, cpu.Close())
		}
	}
	stopped := false
	stop := func() {
		if cpu != nil && !stopped {
			hooks.stopCPUProfile()
			stopped = true
		}
	}
	p, err = fn(stop)
	stop()
	if cpu != nil {
		err = errors.Join(err, cpu.Close())
	}
	if base != "" {
		after, e := hooks.writeAllocsSnapshotTemp("quicksilver_allocs_after")
		if e != nil {
			return p, errors.Join(err, e)
		}
		defer os.Remove(after)
		err = errors.Join(err, hooks.writeAllocsDeltaProfile(base, after, cfg.AllocsProfile+"_"+name+"_"+engine+".pprof"))
	}
	return p, err
}
func quicksilverCheckpoint(db kvstore.DB, cfg BenchConfig, engine, label string) (elapsed time.Duration, err error) {
	hooks := profileHooksFromConfig(cfg)
	// Concurrent readers and a writer may have their own CPU capture; checkpoint
	// capture is a separate final boundary, never nested with a read CPU profile.
	var f *os.File
	if shouldCheckpointCPUProfile(cfg, label) {
		f, err = startCheckpointCPUProfile(cfg, hooks, label, engine)
	}
	if err != nil {
		return 0, err
	}
	t := time.Now()
	err = db.(checkpointer).Checkpoint()
	elapsed = time.Since(t)
	if f != nil {
		hooks.stopCPUProfile()
		err = errors.Join(err, f.Close())
	}
	return elapsed, err
}
func quicksilverVerify(db kvstore.DB, c quicksilverConfig, stride int, guard *benchGuard) (keys, misses int, err error) {
	changed := make([]bool, c.Keys)
	for i := 0; i < c.Updates; i++ {
		changed[int(int64(i)*int64(stride)%int64(c.Keys))] = true
	}
	expected := make([]byte, c.valueSize())
	for i := 0; i < c.Keys; i++ {
		if i%256 == 0 {
			if err = guard.Checkpoint(); err != nil {
				return
			}
		}
		id := uint64(i) * 2
		k := quicksilverKey(id)
		var gen uint64
		if changed[i] {
			gen = 1
		}
		quicksilverValue(expected, id, gen, c.Case == "random4k")
		var v []byte
		v, err = quicksilverGet(db.Get, k[:])
		if err != nil {
			return
		}
		if !bytes.Equal(v, expected) {
			err = fmt.Errorf("quicksilver: reopen byte mismatch key %d", id)
			return
		}
		keys++
		k = quicksilverKey(id + 1)
		v, err = quicksilverGet(db.Get, k[:])
		if err != nil {
			return
		}
		if v != nil {
			err = fmt.Errorf("quicksilver: reopen absent key %d returned bytes", id+1)
			return
		}
		misses++
	}
	return
}
func runQuicksilverEngine(cfg BenchConfig, c quicksilverConfig, engine string, open DBFactory, dir string) (res quicksilverResult, err error) {
	if err = c.validate(); err != nil {
		return
	}
	res = quicksilverResult{Engine: engine, Config: c, GOMAXPROCS: runtime.GOMAXPROCS(0), Profiled: benchConfigHasAnyProfileOutput(cfg), UpdateStride: quicksilverUpdateStride(c.Keys), TraceBytes: quicksilverTraceLength * (64 + 8), SampleCapacity: quicksilverSampleLimit, SampleBytes: quicksilverSampleLimit * 8}
	guard := newBenchGuard(cfg)
	if err = guard.Checkpoint(); err != nil {
		return
	}
	var db kvstore.DB
	db, err = open(dir)
	if err != nil {
		return
	}
	res.DBName = db.Name()
	res.wrapper = db
	defer func() {
		if db != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	if _, ok := db.(kvstore.Batcher); !ok {
		return res, fmt.Errorf("quicksilver: %s lacks CommitSync batches", engine)
	}
	if _, ok := db.(checkpointer); !ok {
		return res, fmt.Errorf("quicksilver: %s lacks Checkpoint", engine)
	}
	if c.ReadBatch > 1 {
		if _, ok := db.(kvstore.ReadSnapshotter); !ok {
			return res, fmt.Errorf("quicksilver: %s lacks read snapshots; use -quicksilver-read-batch=1 for ordinary Get", engine)
		}
	}
	t := time.Now()
	for i := 0; i < c.Keys; i += 1000 {
		if err = guard.Checkpoint(); err != nil {
			return
		}
		if err = quicksilverWrite(db, c, i, min(1000, c.Keys-i), res.UpdateStride, false); err != nil {
			return
		}
	}
	res.LoadSeconds = time.Since(t).Seconds()
	elapsed, e := quicksilverCheckpoint(db, cfg, engine, "quicksilver_initial")
	if e != nil {
		return res, e
	}
	res.InitialCheckpointMS = float64(elapsed) / float64(time.Millisecond)
	res.InitialStats = quicksilverStats(db)
	if err = db.Close(); err != nil {
		db = nil
		return
	}
	db = nil
	if res.InitialFiles, err = quicksilverFiles(dir); err != nil {
		return
	}
	t = time.Now()
	db, err = open(dir)
	res.ReopenMS = float64(time.Since(t)) / float64(time.Millisecond)
	if err != nil {
		return
	}
	// Identical deterministic untimed warmup; no cold-device claim.
	for i := 0; i < 50000; i++ {
		if i%256 == 0 {
			if err = guard.Checkpoint(); err != nil {
				return
			}
		}
		id := uint64(int64(i)*7919%int64(c.Keys)) * 2
		if i%11 != 0 {
			id++
		}
		k := quicksilverKey(id)
		var v []byte
		v, err = quicksilverGet(db.Get, k[:])
		if err == nil {
			err = quicksilverCheckValue(id, v, c.valueSize(), false)
		}
		if err != nil {
			return
		}
	}
	fixture := newQuicksilverFixture(c)
	defer installAllocsProfileRateForEnabled(cfg.AllocsProfile != "", cfg.AllocsProfileRate)()
	for mode := 0; mode < 3; mode++ {
		p, e := quicksilverProfilePhase(cfg, quicksilverPhaseNames[mode], engine, func(stop func()) (quicksilverPhase, error) {
			return quicksilverReadPhase(db, c, fixture, mode, guard, nil, stop)
		})
		if e != nil {
			return res, e
		}
		res.Phases = append(res.Phases, p)
	}
	writer := func(ctx context.Context) error {
		batches := (c.Updates + 999) / 1000
		t := time.Now()
		for i := 0; i < batches; i++ {
			timer := time.NewTimer(time.Until(t.Add(time.Duration(i) * c.Duration / time.Duration(batches))))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			if e := guard.Checkpoint(); e != nil {
				return e
			}
			start := time.Now()
			count := min(1000, c.Updates-i*1000)
			if e := quicksilverWrite(db, c, i*1000, count, res.UpdateStride, true); e != nil {
				return e
			}
			res.UpdateBatches = append(res.UpdateBatches, float64(time.Since(start))/float64(time.Millisecond))
			res.UpdatedKeys += count
			// Four checkpoints at approximately quarter intervals (all four at defaults).
			if (i+1)*4/batches > i*4/batches {
				start = time.Now()
				if e := db.(checkpointer).Checkpoint(); e != nil {
					return e
				}
				res.Checkpoints = append(res.Checkpoints, float64(time.Since(start))/float64(time.Millisecond))
			}
		}
		return nil
	}
	p, e := quicksilverProfilePhase(cfg, quicksilverPhaseNames[3], engine, func(stop func()) (quicksilverPhase, error) {
		return quicksilverReadPhase(db, c, fixture, 3, guard, writer, stop)
	})
	if e != nil {
		return res, e
	}
	res.Phases = append(res.Phases, p)
	if res.UpdatedKeys != c.Updates {
		return res, fmt.Errorf("quicksilver: incomplete updates %d/%d", res.UpdatedKeys, c.Updates)
	}
	elapsed, e = quicksilverCheckpoint(db, cfg, engine, "quicksilver_final")
	if e != nil {
		return res, e
	}
	res.FinalCheckpointMS = float64(elapsed) / float64(time.Millisecond)
	res.FinalStats = quicksilverStats(db)
	if err = db.Close(); err != nil {
		db = nil
		return
	}
	db = nil
	if res.FinalFiles, err = quicksilverFiles(dir); err != nil {
		return
	}
	t = time.Now()
	db, err = open(dir)
	res.FinalReopenMS = float64(time.Since(t)) / float64(time.Millisecond)
	if err != nil {
		return
	}
	res.VerifiedKeys, res.VerifiedMisses, err = quicksilverVerify(db, c, res.UpdateStride, guard)
	return
}

func runQuicksilverSuite(cfg BenchConfig, c quicksilverConfig, profileDir string) (out string, err error) {
	if err := c.validate(); err != nil {
		return "", err
	}
	if cfg.MaxWall < 0 || cfg.MaxRSSMB < 0 {
		return "", fmt.Errorf("quicksilver: guard limits must be nonnegative")
	}
	for _, name := range parseList(cfg.DBsArg) {
		if name != "all" {
			if _, err := GetDBFactory(name); err != nil {
				return "", err
			}
		}
	}
	names := resolveDBs(cfg.DBsArg, cfg.DBsExcludeArg)
	names, err = applyCompressionVariants(names, cfg.DBsExcludeArg)
	if err != nil {
		return "", err
	}
	if len(names) == 0 {
		return "", errors.New("quicksilver: no DBs selected")
	}
	for _, selection := range []map[string]struct{}{cfg.CPUProfileTests, cfg.AllocsProfileTests} {
		for name := range selection {
			if !contains(quicksilverPhaseNames, name) {
				return "", fmt.Errorf("quicksilver: unsupported profile phase %q", name)
			}
		}
	}
	for name := range cfg.CheckpointCPUProfileTests {
		if name != "quicksilver_initial" && name != "quicksilver_final" {
			return "", fmt.Errorf("quicksilver: unsupported checkpoint profile phase %q", name)
		}
	}
	runtimeCfg := cfg
	runtimeCfg.AllocsProfile = ""
	finish, e := startSuiteRuntimeProfiles(runtimeCfg, "quicksilver")
	if e != nil {
		return "", e
	}
	defer func() { err = errors.Join(err, finish()) }()
	reports := make([]quicksilverResult, 0, len(names))
	flags := map[string]string{}
	flag.VisitAll(func(f *flag.Flag) { flags[f.Name] = f.Value.String() })
	for _, name := range names {
		open, e := GetDBFactory(name)
		if e != nil {
			return "", e
		}
		dir, e := os.MkdirTemp("", "bench-quicksilver-"+name+"-")
		if e != nil {
			return "", e
		}
		report, e := runQuicksilverEngine(cfg, c, name, open, dir)
		if e != nil {
			return "", fmt.Errorf("quicksilver %s (failed DB retained at %s): %w", name, dir, e)
		}
		report.Flags = flags
		if cfg.KeepDir {
			report.DataDir = dir
		} else {
			if e = os.RemoveAll(dir); e != nil {
				return "", e
			}
		}
		reports = append(reports, report)
	}
	if err = finish(); err != nil {
		return "", err
	}
	raw, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		return "", err
	}
	if profileDir != "" {
		if err = os.WriteFile(filepath.Join(profileDir, "quicksilver_results.json"), raw, 0644); err != nil {
			return "", err
		}
		if err = writeBenchprofArtifacts(profileDir, *pathLabel, quicksilverBenchprofRuns(cfg, c, reports)); err != nil {
			return "", err
		}
		if err = runBenchprofStrict(profileDir); err != nil {
			return "", err
		}
	}
	return string(raw) + "\n", nil
}

// A fixed workload is one canonical run with a column for every selected engine.
// Separate runs represent key-count sweeps and would lose later-engine columns.
func quicksilverBenchprofRuns(cfg BenchConfig, c quicksilverConfig, reports []quicksilverResult) []BenchRun {
	cfg.Keys = c.Keys
	cfg.ValueSize = c.valueSize()
	cfg.ReadWorkers = c.Workers
	cfg.BatchSize = 1000
	cfg.KeyShape = "shared_prefix24_be8"
	cfg.ValuePattern = c.Case
	cfg.SeedUsed = 24
	cfg.TestsArg = strings.Join(quicksilverPhaseNames, ",")
	run := BenchRun{Config: cfg, Instances: make([]*DBInstance, 0, len(reports)), TestOrder: quicksilverPhaseNames, DisplayNames: map[string]string{}, Results: map[string]map[string]float64{}, TreeDBStats: map[string]map[string]string{}, CheckpointDurations: map[string]map[string]time.Duration{"quicksilver_initial": {}, "quicksilver_final": {}}}
	for _, report := range reports {
		run.Instances = append(run.Instances, &DBInstance{Name: report.Engine, Wrapper: report.wrapper, Dir: report.DataDir})
		run.TreeDBStats[report.DBName] = report.FinalStats
		run.CheckpointDurations["quicksilver_initial"][report.DBName] = time.Duration(report.InitialCheckpointMS * float64(time.Millisecond))
		run.CheckpointDurations["quicksilver_final"][report.DBName] = time.Duration(report.FinalCheckpointMS * float64(time.Millisecond))
		for _, p := range report.Phases {
			if run.Results[p.Name] == nil {
				run.Results[p.Name] = map[string]float64{}
			}
			run.Results[p.Name][report.DBName] = p.OpsPerSec
			run.DisplayNames[p.Name] = p.Name
		}
	}
	return []BenchRun{run}
}
