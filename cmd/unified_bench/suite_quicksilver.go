package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/bits"
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
	quicksilverVerifyDir   = flag.String("quicksilver-verify-dir", "", "Verify an existing final Quicksilver database without rerunning the workload (one DB, same fixture flags)")
	quicksilverCase        = flag.String("quicksilver-case", "realistic", "quicksilver fixture: realistic (3M keys), random4k (100k keys), structured256 (250k keys)")
	quicksilverReads       = flag.Int("quicksilver-reads", 2000000, "quicksilver aggregate operations per fixed read phase")
	quicksilverReadBatch   = flag.Int("quicksilver-read-batch", 64, "quicksilver reads per owned-read snapshot; 1 uses ordinary Get")
	quicksilverDuration    = flag.Duration("quicksilver-duration", 4*time.Second, "quicksilver concurrent read/update duration")
	quicksilverUpdates     = flag.Int("quicksilver-updates", 40000, "quicksilver mutation targets (default capped at key count); realistic mixes updates/deletes/inserts/overwrite bursts")
	quicksilverCommit      = flag.String("quicksilver-commit", "auto", "quicksilver batch API: auto (ordinary for realistic, sync for historical cases), ordinary, sync")
	quicksilverWorkingSet  = flag.String("quicksilver-working-set", "uniform", "quicksilver realistic read working set: uniform, 1%, 20%")
	quicksilverMissPercent = flag.Int("quicksilver-miss-percent", 90, "quicksilver realistic mixed/concurrent absent-key request percentage (0..100)")
	quicksilverMixture     = flag.String("quicksilver-mixture", "primary", "quicksilver realistic fixture mixture: primary, holdout (different key/value/content weights)")
)

const quicksilverTraceLength = 65536
const quicksilverSampleLimit = 1000000

var quicksilverPhaseNames = []string{"quicksilver_hits", "quicksilver_misses", "quicksilver_mixed", "quicksilver_concurrent"}

type quicksilverConfig struct {
	Case                string        `json:"case"`
	Keys                int           `json:"keys"`
	Reads               int           `json:"aggregate_reads"`
	Workers             int           `json:"workers"`
	ReadBatch           int           `json:"reads_per_snapshot"`
	Duration            time.Duration `json:"concurrent_duration_ns"`
	Updates             int           `json:"updates"`
	Mixture             string        `json:"mixture"`
	Seed                int64         `json:"seed"`
	CommitMode          string        `json:"commit_mode"`
	BarrierPolicy       string        `json:"barrier_policy"`
	WorkingSet          string        `json:"working_set"`
	MissPercent         int           `json:"miss_percent"`
	Generation          string        `json:"generation"`
	KeyDistribution     string        `json:"key_distribution"`
	ValueDistribution   string        `json:"value_distribution"`
	ContentDistribution string        `json:"content_distribution"`
}

func (c quicksilverConfig) valueSize() int {
	if c.Case == "random4k" {
		return 4096
	}
	return 256
}
func (c quicksilverConfig) validate() error {
	if c.Case != "random4k" && c.Case != "structured256" && c.Case != "realistic" {
		return fmt.Errorf("quicksilver: unknown case %q", c.Case)
	}
	if c.Keys < 1 || c.Keys > 10000000 || c.Reads < 1 || c.Reads > 1000000000 || c.Workers < 1 || c.Workers > 1024 || c.ReadBatch < 1 || c.ReadBatch > 1000000 || c.Duration <= 0 || c.Duration > time.Hour || c.Updates < 1 || c.Updates > c.Keys {
		return fmt.Errorf("quicksilver: invalid configuration: %+v", c)
	}
	if c.CommitMode != "" && c.CommitMode != "ordinary" && c.CommitMode != "sync" {
		return fmt.Errorf("quicksilver: invalid commit mode %q", c.CommitMode)
	}
	if c.Case == "realistic" && (c.MissPercent < 0 || c.MissPercent > 100 || (c.Mixture != "" && c.Mixture != "primary" && c.Mixture != "holdout") || (c.WorkingSet != "" && c.WorkingSet != "uniform" && c.WorkingSet != "1%" && c.WorkingSet != "20%")) {
		return fmt.Errorf("quicksilver: invalid read distribution")
	}
	if int64((c.Keys*5+63)/64)*8*int64(c.Workers+1) > 512<<20 {
		return fmt.Errorf("quicksilver: distinct-access tracking exceeds 512 MiB; reduce read-workers or keys")
	}
	return nil
}
func resolveQuicksilverConfig(base BenchConfig, isSet map[string]bool) (quicksilverConfig, error) {
	c := quicksilverConfig{Case: *quicksilverCase, Keys: base.Keys, Reads: *quicksilverReads, Workers: base.ReadWorkers, ReadBatch: *quicksilverReadBatch, Duration: *quicksilverDuration, Updates: *quicksilverUpdates, Seed: 24, CommitMode: *quicksilverCommit, WorkingSet: *quicksilverWorkingSet, MissPercent: *quicksilverMissPercent, Mixture: *quicksilverMixture}
	if c.CommitMode == "auto" {
		c.CommitMode = ""
	} else if c.CommitMode != "ordinary" && c.CommitMode != "sync" {
		return c, fmt.Errorf("quicksilver: unknown -quicksilver-commit %q", c.CommitMode)
	}
	if c.Case == "realistic" {
		c.Seed = base.SeedUsed
	} else if isSet["seed"] || isSet["quicksilver-working-set"] || isSet["quicksilver-miss-percent"] || isSet["quicksilver-mixture"] {
		return c, fmt.Errorf("quicksilver: seed/working-set/miss-percent/mixture apply only to realistic; historical trace is fixed")
	}
	c = c.resolved()
	if !isSet["keys"] {
		c.Keys = 100000
		if c.Case == "realistic" {
			c.Keys = 3000000
		}
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
	for _, name := range []string{"test", "keycounts", "keyscale", "keys-min", "keys-max", "key-shape", "val-pattern", "val-pool-size", "read-require-hit", "checkpoint-between-tests", "checkpoint-every-ops", "checkpoint-every-bytes", "vacuum-between-tests", "settle-before-scans", "treedb-vlog-rewrite-after-run", "treedb-vacuum-after-vlog-rewrite-run", "checkpoint-settle-before-tests", "checkpoint-settle-timeout", "range-queries", "range-span", "write-workers", "batch-delete-range-width", "batch-delete-ranges-per-batch", "batch-delete-range-validate", "batch-delete-range-refill", "batch-write-steady-checkpoint-bytes", "batch-write-dict-warmup", "outdir", "format", "flushdrain-checkpoint-max", "treedb-cache-stats-before-reads", "treedb-cache-stats-after-tests"} {
		if isSet[name] {
			return c, fmt.Errorf("quicksilver: -%s does not apply to this fixed workflow", name)
		}
	}
	if isSet["valsize"] && c.Case == "realistic" {
		return c, fmt.Errorf("quicksilver: -valsize does not apply to realistic variable-length values")
	}
	if isSet["valsize"] && base.ValueSize != c.valueSize() {
		return c, fmt.Errorf("quicksilver: -valsize must match %s (%d)", c.Case, c.valueSize())
	}
	if isSet["batchsize"] && base.BatchSize != 1000 {
		return c, fmt.Errorf("quicksilver: load/update batchsize is fixed at 1000")
	}
	c = c.resolved()
	return c, c.validate()
}

type quicksilverPhase struct {
	Name                    string            `json:"name"`
	DistinctAccesses        int               `json:"distinct_accesses"`
	DistinctPresentRequests int               `json:"distinct_present_requests"`
	DistinctAbsentRequests  int               `json:"distinct_absent_requests"`
	RequestedPresent        int               `json:"requested_present"`
	RequestedAbsent         int               `json:"requested_absent"`
	ObservedHits            int               `json:"observed_hits"`
	MissKinds               [3]int            `json:"miss_kind_requests_arbitrary_common_prefix_deleted"`
	Ops                     int               `json:"ops"`
	Seconds                 float64           `json:"seconds"`
	CompositionSeconds      float64           `json:"composition_seconds"`
	OpsPerSec               float64           `json:"ops_per_sec"`
	Samples                 int               `json:"samples"`
	P50US                   float64           `json:"p50_us"`
	P95US                   float64           `json:"p95_us"`
	P99US                   float64           `json:"p99_us"`
	P999US                  float64           `json:"p999_us"`
	MaxUS                   float64           `json:"max_us"`
	AllocatedBytes          uint64            `json:"process_allocated_bytes"`
	Mallocs                 uint64            `json:"process_mallocs"`
	BytesPerOp              float64           `json:"process_bytes_per_op"`
	AllocsPerOp             float64           `json:"process_allocs_per_op"`
	HeapBefore              uint64            `json:"process_heap_alloc_before"`
	HeapAfter               uint64            `json:"process_heap_alloc_after"`
	GCPauseNS               uint64            `json:"process_gc_pause_ns"`
	GCCycles                uint32            `json:"process_gc_cycles"`
	StatsBefore             map[string]string `json:"stats_before,omitempty"`
	StatsAfter              map[string]string `json:"stats_after,omitempty"`
}
type quicksilverResult struct {
	wrapper                   kvstore.DB
	Engine                    string                     `json:"engine"`
	DBName                    string                     `json:"db_name"`
	Config                    quicksilverConfig          `json:"config"`
	GOMAXPROCS                int                        `json:"gomaxprocs"`
	Profiled                  bool                       `json:"profiled"`
	UpdateStride              int                        `json:"update_stride"`
	TraceBytes                int                        `json:"trace_bytes"`
	OracleStateBytes          int                        `json:"oracle_state_bytes"`
	AccessGeneration          string                     `json:"access_generation"`
	SampleCapacity            int                        `json:"sample_capacity"`
	SampleBytes               int                        `json:"sample_bytes"`
	LoadSeconds               float64                    `json:"load_seconds"`
	DeletedPreparationSeconds float64                    `json:"deleted_preparation_seconds"`
	FixtureAnalysisSeconds    float64                    `json:"fixture_analysis_seconds"`
	MutationCommitBatches     int                        `json:"mutation_commit_batches"`
	InitialCheckpointMS       float64                    `json:"initial_checkpoint_ms"`
	ReopenMS                  float64                    `json:"reopen_ms"`
	FinalCheckpointMS         float64                    `json:"final_checkpoint_ms"`
	FinalReopenMS             float64                    `json:"final_reopen_ms"`
	Phases                    []quicksilverPhase         `json:"phases"`
	UpdateBatches             []float64                  `json:"update_batch_ms"`
	Checkpoints               []float64                  `json:"checkpoint_ms"`
	UpdatedKeys               int                        `json:"updated_keys"`
	Mutations                 quicksilverMutations       `json:"mutations"`
	Fixture                   quicksilverDistribution    `json:"loaded_distribution"`
	Compressibility           quicksilverCompressibility `json:"compressibility"`
	DistinctTrackingBytes     int                        `json:"distinct_tracking_bytes"`
	InitialVerifiedKeys       int                        `json:"initial_verified_keys"`
	InitialVerifiedMisses     int                        `json:"initial_verified_misses"`
	Correctness               string                     `json:"correctness"`
	VerifiedKeys              int                        `json:"verified_keys"`
	VerifiedMisses            int                        `json:"verified_misses"`
	InitialStats              map[string]string          `json:"initial_stats,omitempty"`
	FinalStats                map[string]string          `json:"final_stats,omitempty"`
	InitialFiles              map[string]int64           `json:"initial_files"`
	FinalFiles                map[string]int64           `json:"final_files"`
	Flags                     map[string]string          `json:"registered_cli_flags,omitempty"`
	DataDir                   string                     `json:"data_dir,omitempty"`
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
	if c.Case == "realistic" {
		return quicksilverRealisticWrite(db, c, offset, count, stride, update)
	}
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
	return quicksilverCommitBatch(b, c)
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
	samples   []int64
	count     int
	maximum   int64
	err       error
	distinct  []uint64
	present   int
	absent    int
	hits      int
	missKinds [3]int
}
type quicksilverFixture struct {
	keys     [][32]byte
	ids      []uint64
	readers  []quicksilverReader
	samples  []int64
	distinct []uint64
	states   []uint8
}

func newQuicksilverFixture(c quicksilverConfig) *quicksilverFixture {
	f := &quicksilverFixture{readers: make([]quicksilverReader, c.Workers), samples: make([]int64, quicksilverSampleLimit)}
	if c.Case != "realistic" {
		f.keys = make([][32]byte, 2*quicksilverTraceLength)
		f.ids = make([]uint64, quicksilverTraceLength)
	}
	if c.Case == "realistic" {
		f.states = make([]uint8, c.Keys)
		stride := quicksilverUpdateStride(c.Keys)
		for j := 0; j < c.Updates; j++ {
			i := int(int64(j) * int64(stride) % int64(c.Keys))
			switch j % 4 {
			case 0:
				f.states[i] = 1
			case 1:
				f.states[i] = 255
			case 3:
				f.states[i] = 4
			}
		}
	}
	words := (c.Keys*5 + 63) / 64
	f.distinct = make([]uint64, words)
	for w := range f.readers {
		f.readers[w].distinct = make([]uint64, words)
	}
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
	c = c.resolved()
	readStride := quicksilverUpdateStride(c.Keys)
	readOffset := int(quicksilverMix(uint64(c.Seed)) % uint64(c.Keys))
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
		clear(s.distinct)
		s.present, s.absent, s.hits = 0, 0, 0
		s.missKinds = [3]int{}
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
			var rng *rand.Rand
			var keyScratch []byte
			if c.Case == "realistic" {
				rng = rand.New(rand.NewPCG(uint64(c.Seed), uint64(w)+91))
				keyScratch = make([]byte, 0, 128)
			}
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
				var key []byte
				var id uint64
				var absent bool
				var distinctIndex int
				kind := 0
				if c.Case == "realistic" {
					id, absent, kind, distinctIndex = quicksilverAccess(&c, rng, mode, i+w, readStride, readOffset)
					key = quicksilverLookupKey(keyScratch[:0], id, c.Seed, c.Mixture, absent && kind == 0)
				} else {
					global := i*c.Workers + w
					pos := global % len(f.ids)
					absent = mode == 1 || (mode >= 2 && global%11 != 0)
					idx := pos * 2
					id = f.ids[pos]
					if absent {
						idx++
						id++
						kind = 1
					}
					key = f.keys[idx][:]
					distinctIndex = int(id/2) * 5
					if absent {
						distinctIndex += 2
					}
				}
				s.distinct[distinctIndex/64] |= uint64(1) << uint(distinctIndex%64)
				if absent {
					s.absent++
					s.missKinds[kind]++
				} else {
					s.present++
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
				v, e := quicksilverGet(getter, key)
				ns := time.Since(t).Nanoseconds()
				if e == nil {
					state := uint8(0)
					if c.Case == "realistic" && mode == 3 && !absent {
						if id/2 >= uint64(2*c.Keys) {
							state = 5
						} else {
							state = f.states[id/2]
						}
					}
					e = quicksilverCheckRead(&c, id, v, absent, mode == 3, state)
				}
				if e != nil {
					s.err = e
					cancel()
					return
				}
				if len(v) != 0 {
					s.hits++
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
	<-writerDone
	p.StatsAfter = quicksilverStats(db)
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
	clear(f.distinct)
	err := writerErr
	for _, s := range f.readers {
		err = errors.Join(err, s.err)
		p.Ops += s.count
		p.RequestedPresent += s.present
		p.RequestedAbsent += s.absent
		p.ObservedHits += s.hits
		for j := range p.MissKinds {
			p.MissKinds[j] += s.missKinds[j]
		}
		for j, word := range s.distinct {
			f.distinct[j] |= word
		}
		p.MaxUS = max(p.MaxUS, float64(s.maximum)/1000)
		n := len(all)
		all = all[:n+len(s.samples)]
		copy(all[n:], s.samples)
	}
	if err != nil {
		return p, err
	}
	// Slots 0/4 of each five-key group are present-class identities. The
	// pattern repeats every five words; count both classes without visiting bits.
	presentMasks := [5]uint64{0x18c6318c6318c631, 0x318c6318c6318c63, 0x6318c6318c6318c6, 0xc6318c6318c6318c, 0x8c6318c6318c6318}
	for j, word := range f.distinct {
		p.DistinctAccesses += bits.OnesCount64(word)
		p.DistinctPresentRequests += bits.OnesCount64(word & presentMasks[j%5])
	}
	p.DistinctAbsentRequests = p.DistinctAccesses - p.DistinctPresentRequests

	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p.Samples = len(all)
	q := func(frac float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(float64(len(all)-1)*frac)]) / 1000
	}
	p.P50US = q(.5)
	p.P95US = q(.95)
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
	if c.Case == "realistic" {
		return quicksilverRealisticVerify(db, c, stride, guard, true)
	}
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
	c = c.resolved()
	if err = c.validate(); err != nil {
		return
	}
	res = quicksilverResult{Correctness: "owned read identity/length/generation; full bytes and all miss classes after durable checkpoint/reopen", DistinctTrackingBytes: ((c.Keys*5 + 63) / 64) * 8 * (c.Workers + 1), Engine: engine, Config: c, GOMAXPROCS: runtime.GOMAXPROCS(0), Profiled: benchConfigHasAnyProfileOutput(cfg), UpdateStride: quicksilverUpdateStride(c.Keys), TraceBytes: quicksilverTraceLength * (64 + 8), SampleCapacity: quicksilverSampleLimit, SampleBytes: quicksilverSampleLimit * 8}
	res.AccessGeneration = "historical repeated 65536-entry trace"
	if c.Case == "realistic" {
		res.TraceBytes = 0
		res.OracleStateBytes = c.Keys
		res.AccessGeneration = "per-worker PCG, full-duration stream, fixed 128-byte key scratch; PRNG/key construction and exact distinct bitmap writes included in read timer"
	}
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
		return res, fmt.Errorf("quicksilver: %s lacks ordinary/sync batches", engine)
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
	if c.Case == "realistic" {
		t = time.Now()
		if err = quicksilverPrepareDeleted(db, c, guard); err != nil {
			return
		}
		res.DeletedPreparationSeconds = time.Since(t).Seconds()
		t = time.Now()
		res.Fixture = quicksilverLoadedDistribution(c)
		if res.Compressibility, err = quicksilverMeasureCompressibility(c); err != nil {
			return
		}
		res.FixtureAnalysisSeconds = time.Since(t).Seconds()
	}
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
	if c.Case == "realistic" {
		res.InitialVerifiedKeys, res.InitialVerifiedMisses, err = quicksilverRealisticVerify(db, c, res.UpdateStride, guard, false)
		if err != nil {
			return
		}
	}
	// Identical deterministic untimed warmup; no cold-device claim.
	warmRNG := rand.New(rand.NewPCG(uint64(c.Seed), 91))
	var warmKey [128]byte
	for i := 0; i < 50000; i++ {
		if i%256 == 0 {
			if err = guard.Checkpoint(); err != nil {
				return
			}
		}
		if c.Case == "realistic" {
			id, absent, kind, _ := quicksilverAccess(&c, warmRNG, 2, i, res.UpdateStride, int(quicksilverMix(uint64(c.Seed))%uint64(c.Keys)))
			k := quicksilverLookupKey(warmKey[:0], id, c.Seed, c.Mixture, absent && kind == 0)
			v, e := quicksilverGet(db.Get, k)
			if e == nil {
				e = quicksilverCheckRead(&c, id, v, absent, false, 0)
			}
			if e != nil {
				return res, e
			}
			continue
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
			if c.Case == "realistic" {
				res.Mutations.add(i*1000, count)
				res.MutationCommitBatches++
				if count >= 4 {
					res.MutationCommitBatches += 3
				}
			}
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

type quicksilverVerificationResult struct {
	VerificationOnly bool              `json:"verification_only"`
	Engine           string            `json:"engine"`
	DataDir          string            `json:"data_dir"`
	Config           quicksilverConfig `json:"config"`
	VerifiedKeys     int               `json:"verified_keys"`
	VerifiedMisses   int               `json:"verified_misses"`
}

func verifyQuicksilverRetained(cfg BenchConfig, c quicksilverConfig, names []string, profileDir string) (out string, err error) {
	if len(names) != 1 || profileDir != "" || benchConfigHasAnyProfileOutput(cfg) {
		return "", errors.New("quicksilver verification requires one DB and no profiling outputs")
	}
	var marker string
	switch {
	case names[0] == "treedb_backend" || names[0] == "treedb_backend_command_wal":
		marker = "index.db"
	case strings.HasPrefix(names[0], "treedb"):
		marker = "maindb/index.db"
	case names[0] == "lmdb":
		marker = "data.mdb"
	case names[0] == "rocksdb":
		marker = "CURRENT"
	default:
		return "", errors.New("quicksilver verification supports TreeDB, LMDB and RocksDB")
	}
	info, err := os.Stat(filepath.Join(cfg.QuicksilverVerifyDir, marker))
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return "", errors.New("quicksilver verification requires an existing nonempty database marker")
	}
	open, err := GetDBFactory(names[0])
	if err != nil {
		return "", err
	}
	db, err := open(cfg.QuicksilverVerifyDir)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	report := quicksilverVerificationResult{VerificationOnly: true, Engine: names[0], DataDir: cfg.QuicksilverVerifyDir, Config: c}
	report.VerifiedKeys, report.VerifiedMisses, err = quicksilverVerify(db, c, quicksilverUpdateStride(c.Keys), newBenchGuard(cfg))
	if err != nil {
		return "", err
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	return string(raw), err
}

func runQuicksilverSuite(cfg BenchConfig, c quicksilverConfig, profileDir string) (out string, err error) {
	c = c.resolved()
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
	if cfg.QuicksilverVerifyDir != "" {
		return verifyQuicksilverRetained(cfg, c, names, profileDir)
	}
	// These adapters checkpoint by replacing a live handle, which is unsafe
	// during the suite's concurrent readers. Reject the whole selection early.
	for _, name := range names {
		switch name {
		case "leveldb", "leveldb_block_comp_on", "leveldb_block_comp_off":
			return "", fmt.Errorf("quicksilver: %s is unsupported: its checkpoint closes/reopens the DB handle while concurrent readers are active", name)
		case "lmdb":
			// Each worker can hold one read transaction; the adapter uses LMDB's default reader table.
			if c.Workers > 126 {
				return "", fmt.Errorf("quicksilver: LMDB supports at most 126 read workers (default reader slots); got %d", c.Workers)
			}
		}
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
	c = c.resolved()
	cfg.ValueSize = c.valueSize()
	if c.Case == "realistic" {
		cfg.ValueSize = 0
	}
	cfg.ReadWorkers = c.Workers
	cfg.BatchSize = 1000
	cfg.KeyShape = "shared_prefix24_be8"
	cfg.ValuePattern = c.Case
	cfg.SeedUsed = c.Seed
	if c.Case == "realistic" {
		cfg.KeyShape = c.KeyDistribution
		cfg.ValuePattern = c.ValueDistribution + "; " + c.ContentDistribution
	}
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
