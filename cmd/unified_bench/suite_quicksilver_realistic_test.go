package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/kvstore"
)

func quicksilverRealisticSmokeConfig() quicksilverConfig {
	return quicksilverConfig{Case: "realistic", Keys: 73, Reads: 1001, Workers: 4, ReadBatch: 64, Duration: 100 * time.Millisecond, Updates: 73, Seed: 24, WorkingSet: "uniform", MissPercent: 90}.resolved()
}
func TestQuicksilverGenericFixture(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.Keys = 50000
	seen := make(map[string]bool, c.Keys*4)
	var scratch, other [128]byte
	for i := 0; i < c.Keys; i++ {
		// Live, common-prefix miss, deleted, inserted and arbitrary domains remain disjoint.
		for _, id := range []uint64{uint64(i) * 2, uint64(i)*2 + 1, uint64(c.Keys+i) * 2, uint64(2*c.Keys+i) * 2} {
			k := quicksilverGenericKey(scratch[:0], id, c.Seed, c.Mixture)
			if seen[string(k)] {
				t.Fatalf("collision at id %d", id)
			}
			seen[string(k)] = true
			if !bytes.Equal(k, quicksilverGenericKey(other[:0], id, c.Seed, c.Mixture)) {
				t.Fatal("nondeterministic key")
			}
			if bytes.Equal(k, quicksilverGenericKey(other[:0], id, c.Seed+1, c.Mixture)) {
				t.Fatal("seed ignored")
			}
		}
		k := quicksilverLookupKey(scratch[:0], uint64(i)*2, c.Seed, c.Mixture, true)
		if seen[string(k)] {
			t.Fatal("arbitrary miss collision")
		}
		seen[string(k)] = true
	}
	d := quicksilverLoadedDistribution(c)
	if d.MinKeyBytes >= d.MaxKeyBytes || d.MinValueBytes != 32 || d.MaxValueBytes < 30000 {
		t.Fatalf("fixed lengths: %+v", d)
	}
	for i, want := range []float64{.80, .18, .02} {
		ratio := float64(d.ValueBuckets[i]) / float64(c.Keys)
		if ratio < want-.01 || ratio > want+.01 {
			t.Fatalf("value mixture: %+v", d)
		}
		if d.KeyKinds[i] < 16000 {
			t.Fatalf("missing key family: %+v", d)
		}
	}
	if d.OpaqueValues < 24000 || d.StructuredValues < 24000 {
		t.Fatalf("content mixture: %+v", d)
	}
	for _, id := range []uint64{0, 2, 174, 12134} {
		n := quicksilverRealisticSize(&c, id)
		a, b := make([]byte, n), make([]byte, n)
		quicksilverRealisticValue(a, c, id, 0)
		quicksilverRealisticValue(b, c, id, 0)
		if !bytes.Equal(a, b) {
			t.Fatal("nondeterministic value")
		}
		quicksilverRealisticValue(b, c, id, 1)
		if bytes.Equal(a, b) {
			t.Fatal("generation unchanged")
		}
	}
}

func TestQuicksilverCommonPrefixMisses(t *testing.T) {
	for _, mixture := range []string{"primary", "holdout"} {
		for _, seed := range []int64{24, 91} {
			t.Run(mixture+"/"+strconv.FormatInt(seed, 10), func(t *testing.T) {
				const keys = 50000
				seen := make(map[string]bool, keys*5)
				var liveScratch, missScratch, domainScratch [128]byte
				families := [3]int{}
				for i := 0; i < keys; i++ {
					id := uint64(i) * 2
					live := quicksilverLookupKey(liveScratch[:0], id, seed, mixture, false)
					miss := quicksilverLookupKey(missScratch[:0], id+1, seed, mixture, false)
					// Infer prefixes from actual bytes, independently of the family selector.
					var prefix []byte
					switch live[0] {
					case 'n':
						prefix = []byte("namespace/")
						families[0]++
					case 'h':
						prefix = []byte{'h'}
						families[1]++
					case 0x80:
						prefix = []byte{0x80}
						families[2]++
					default:
						t.Fatalf("unexpected live prefix %x", live)
					}
					if !bytes.HasPrefix(live, prefix) || !bytes.HasPrefix(miss, prefix) || bytes.Equal(live, miss) {
						t.Fatalf("id %d: common-prefix miss %x does not preserve source %x", id, miss, live)
					}
					// Check actual byte-key disjointness across all generated identity domains.
					for _, domainID := range []uint64{id, id + 1, uint64(keys+i) * 2, uint64(2*keys+i) * 2} {
						key := quicksilverLookupKey(domainScratch[:0], domainID, seed, mixture, false)
						if seen[string(key)] {
							t.Fatalf("collision at id %d", domainID)
						}
						seen[string(key)] = true
					}
					arbitrary := quicksilverLookupKey(domainScratch[:0], id, seed, mixture, true)
					if seen[string(arbitrary)] {
						t.Fatalf("arbitrary key collision at id %d", id)
					}
					seen[string(arbitrary)] = true
				}
				for _, count := range families {
					if count == 0 {
						t.Fatal("missing actual key family")
					}
				}
			})
		}
	}
}

func TestQuicksilverRealisticWorkflow(t *testing.T) {
	for _, commit := range []string{"ordinary", "sync"} {
		t.Run(commit, func(t *testing.T) {
			c := quicksilverRealisticSmokeConfig()
			c.CommitMode = commit
			r, e := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			if r.Config.CommitMode != commit || r.Config.Generation != "generic-v1" || r.Config.Seed != c.Seed || r.TraceBytes != 0 {
				t.Fatalf("missing provenance: %+v", r.Config)
			}
			m := r.Mutations
			if m.Updates != 19 || m.Deletes != 18 || m.Inserts != 18 || m.OverwriteTargets != 18 || m.OverwriteSets != 72 {
				t.Fatalf("mutation schedule: %+v", m)
			}
			if r.InitialVerifiedKeys != c.Keys || r.VerifiedKeys != c.Keys-m.Deletes+m.Inserts || r.VerifiedMisses != 2*c.Keys+quicksilverDeletedKeys(c)+m.Deletes {
				t.Fatalf("reopen oracle counts: %+v", r)
			}
			for _, p := range r.Phases[:3] {
				if p.P95US < p.P50US || p.P95US > p.P99US {
					t.Fatalf("invalid p95 quantile: %+v", p)
				}
				if p.Ops != c.Reads || p.RequestedPresent+p.RequestedAbsent != p.Ops || p.DistinctAccesses != p.DistinctPresentRequests+p.DistinctAbsentRequests || p.DistinctAccesses > p.Ops {
					t.Fatalf("invalid read accounting: %+v", p)
				}
			}
			p := r.Phases[1]
			if p.MissKinds[0] == 0 || p.MissKinds[1] == 0 || p.MissKinds[2] == 0 {
				t.Fatalf("miss classes absent: %+v", p)
			}
			raw, e := json.Marshal(r)
			if e != nil {
				t.Fatal(e)
			}
			for _, field := range []string{"commit_mode", "barrier_policy", "generation", "loaded_distribution", "distinct_accesses", "initial_verified_keys", "mutations"} {
				if !strings.Contains(string(raw), `"`+field+`"`) {
					t.Fatalf("missing artifact field %s", field)
				}
			}
		})
	}
}

type quicksilverObserveMissDB struct {
	fixedNameDB
	seen map[string]struct{}
}

func (d *quicksilverObserveMissDB) Get(key []byte) ([]byte, error) {
	d.seen[string(key)] = struct{}{}
	return nil, nil
}
func TestQuicksilverActualDistinctAccesses(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.Keys = 100000
	c.Reads = 180000
	c.Workers = 1
	c.ReadBatch = 1
	for _, working := range []string{"uniform", "1%", "20%"} {
		c.WorkingSet = working
		d := &quicksilverObserveMissDB{seen: map[string]struct{}{}}
		p, e := quicksilverReadPhase(d, c, newQuicksilverFixture(c), 1, nil, nil, nil)
		if e != nil {
			t.Fatal(e)
		}
		if p.DistinctAccesses != len(d.seen) {
			t.Fatalf("%s: counted %d, actual %d", working, p.DistinctAccesses, len(d.seen))
		}
		if working == "uniform" && p.DistinctAccesses <= quicksilverTraceLength {
			t.Fatalf("short repeated trace remains: %+v", p)
		}
		if working != "uniform" && p.DistinctAccesses > 2*quicksilverWorkingKeys(c)+quicksilverDeletedKeys(c) {
			t.Fatalf("working set escaped: %+v", p)
		}
	}
}
func TestQuicksilverMissRateAndWorkingSetIndependent(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.ReadBatch = 1
	c.Reads = 10000
	c.MissPercent = 100
	for _, working := range []string{"uniform", "1%", "20%"} {
		c.WorkingSet = working
		p, e := quicksilverReadPhase(&fixedNameDB{}, c, newQuicksilverFixture(c), 2, nil, nil, nil)
		if e != nil {
			t.Fatal(e)
		}
		if p.RequestedPresent != 0 || p.RequestedAbsent != c.Reads {
			t.Fatalf("%s: miss rate changed by locality: %+v", working, p)
		}
	}
	c.MissPercent = -1
	if c.validate() == nil {
		t.Fatal("invalid percentage accepted")
	}
	c.MissPercent = 50
	c.WorkingSet = "unknown"
	if c.validate() == nil {
		t.Fatal("unknown working set accepted")
	}
	c.WorkingSet = "uniform"
	c.Keys = 10000000
	c.Workers = 1024
	if c.validate() == nil {
		t.Fatal("unbounded tracking allocation accepted")
	}
}

func TestQuicksilverConcurrentMissClassesPerReader(t *testing.T) {
	for _, mixture := range []string{"primary", "holdout"} {
		for _, workers := range []int{3, 6, 7, 9, 12, 4, 8, 16, 32, 64} {
			t.Run(mixture+"/"+strconv.Itoa(workers), func(t *testing.T) {
				c := quicksilverRealisticSmokeConfig()
				c.Mixture, c.Workers, c.MissPercent, c.ReadBatch = mixture, workers, 100, 1
				c.Duration = 20 * time.Millisecond
				f := newQuicksilverFixture(c)
				p, err := quicksilverReadPhase(&fixedNameDB{}, c, f, 3, nil, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				if p.RequestedPresent != 0 || p.RequestedAbsent != p.Ops {
					t.Fatalf("incorrect all-miss phase accounting: %+v", p)
				}
				for w, reader := range f.readers {
					counts := reader.missKinds
					if reader.count == 0 || counts[0]+counts[1]+counts[2] != reader.count || max(counts[0], counts[1], counts[2])-min(counts[0], counts[1], counts[2]) > 1 {
						t.Fatalf("reader %d: %d actual reads have skewed miss classes %v", w, reader.count, counts)
					}
				}
			})
		}
	}
}

func TestQuicksilverRealisticConcurrentValidation(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	id := uint64(2)
	if quicksilverCheckRead(&c, id, nil, false, true, 0) == nil {
		t.Fatal("missing unchanged key accepted")
	}
	if e := quicksilverCheckRead(&c, id, nil, false, true, 255); e != nil {
		t.Fatal(e)
	}
	v := make([]byte, quicksilverRealisticSize(&c, id))
	quicksilverRealisticValue(v, c, id, 4)
	if quicksilverCheckRead(&c, id, v, false, true, 1) == nil {
		t.Fatal("update accepted overwrite generation")
	}
	if e := quicksilverCheckRead(&c, id, v, false, true, 4); e != nil {
		t.Fatal(e)
	}
	if quicksilverCheckRead(&c, id, v, true, true, 0) == nil {
		t.Fatal("miss returned bytes")
	}
}

func TestQuicksilverRealisticFullByteOracle(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	opens := 0
	open := func(dir string) (kvstore.DB, error) {
		d, e := NewTreeDBPublicCommandWAL(dir)
		opens++
		if opens == 3 && e == nil {
			return quicksilverCorruptDB{d}, nil
		}
		return d, e
	}
	_, e := runQuicksilverEngine(BenchConfig{}, c, "treedb", open, t.TempDir())
	if e == nil || !strings.Contains(e.Error(), "reopen byte mismatch") {
		t.Fatalf("payload corruption accepted: %v", e)
	}
}

type quicksilverCommitProbeDB struct {
	*batchDeleteRangeMemoryDB
	ordinary, sync int
}

func (d *quicksilverCommitProbeDB) NewBatch() (kvstore.Batch, error) {
	b, e := d.batchDeleteRangeMemoryDB.NewBatch()
	return &quicksilverCommitProbeBatch{Batch: b, d: d}, e
}

type quicksilverCommitProbeBatch struct {
	kvstore.Batch
	d *quicksilverCommitProbeDB
}

func (b *quicksilverCommitProbeBatch) Commit() error     { b.d.ordinary++; return b.Batch.Commit() }
func (b *quicksilverCommitProbeBatch) CommitSync() error { b.d.sync++; return b.Batch.CommitSync() }
func TestQuicksilverCommitDispatch(t *testing.T) {
	for _, fixture := range []string{"realistic", "random4k", "structured256"} {
		for _, mode := range []string{"", "ordinary", "sync"} {
			c := quicksilverRealisticSmokeConfig()
			c.Case = fixture
			c.CommitMode = mode
			db := &quicksilverCommitProbeDB{batchDeleteRangeMemoryDB: newBatchDeleteRangeMemoryDB("memory")}
			if e := quicksilverWrite(db, c, 0, 4, quicksilverUpdateStride(c.Keys), false); e != nil {
				t.Fatal(e)
			}
			if c.resolved().CommitMode == "ordinary" {
				if db.ordinary != 1 || db.sync != 0 {
					t.Fatalf("wrong ordinary dispatch %s/%s", fixture, mode)
				}
			} else if db.ordinary != 0 || db.sync != 1 {
				t.Fatalf("wrong sync dispatch %s/%s", fixture, mode)
			}
			if e := quicksilverWrite(db, c, 0, 4, quicksilverUpdateStride(c.Keys), true); e != nil {
				t.Fatal(e)
			}
			want := 2
			if fixture == "realistic" {
				want = 5
			}
			if c.resolved().CommitMode == "ordinary" {
				if db.ordinary != want || db.sync != 0 {
					t.Fatal("overwrite generations collapsed into a batch")
				}
			} else if db.sync != want || db.ordinary != 0 {
				t.Fatal("overwrite generations collapsed into a batch")
			}

		}
	}
}

func TestQuicksilverRealisticHarnessAllocations(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.ReadBatch = 1
	c.Reads = 100000
	p, e := quicksilverReadPhase(&fixedNameDB{}, c, newQuicksilverFixture(c), 1, nil, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	if p.AllocsPerOp > .01 || p.BytesPerOp > 8 {
		t.Fatalf("avoidable generic per-read allocation: %+v", p)
	}
	t.Logf("realistic harness %.5f allocs/read %.3f B/read", p.AllocsPerOp, p.BytesPerOp)
}

func BenchmarkQuicksilverGenericKey(b *testing.B) {
	var scratch [128]byte
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := quicksilverGenericKey(scratch[:0], uint64(i), 24, "primary")
		if len(k) == 0 {
			b.Fatal("empty key")
		}
	}
}

func TestQuicksilverHoldoutAndCompressibility(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.Keys = 50000
	c.Mixture = "holdout"
	c = c.resolved()
	d := quicksilverLoadedDistribution(c)
	for i, want := range []float64{.50, .30, .20} {
		got := float64(d.KeyKinds[i]) / float64(c.Keys)
		if got < want-.01 || got > want+.01 {
			t.Fatalf("holdout keys: %+v", d)
		}
	}
	for i, want := range []float64{.95, .04, .01} {
		got := float64(d.ValueBuckets[i]) / float64(c.Keys)
		if got < want-.01 || got > want+.01 {
			t.Fatalf("holdout values: %+v", d)
		}
	}
	if d.OpaqueValues < 37000 || d.OpaqueValues > 38000 {
		t.Fatalf("holdout contents: %+v", d)
	}
	out, err := quicksilverMeasureCompressibility(c)
	if err != nil {
		t.Fatal(err)
	}
	again, err := quicksilverMeasureCompressibility(c)
	if err != nil {
		t.Fatal(err)
	}
	if out != again || out.Total.Records != 4096 || out.Total.RawBytes != out.Structured.RawBytes+out.Opaque.RawBytes || out.Total.CompressedBytes != out.Structured.CompressedBytes+out.Opaque.CompressedBytes {
		t.Fatalf("invalid compression sample: %+v", out)
	}
	if out.Structured.Ratio >= out.Opaque.Ratio {
		t.Fatalf("compression labels unsupported: %+v", out)
	}
	t.Logf("holdout DEFLATE sample=%d structured ratio=%.3f opaque ratio=%.3f total ratio=%.3f", out.Total.Records, out.Structured.Ratio, out.Opaque.Ratio, out.Total.Ratio)
}

func TestQuicksilverRealisticCLIContract(t *testing.T) {
	binaryPath := filepath.Join(t.TempDir(), "unified-bench")
	build := exec.Command("go", "build", "-o", binaryPath, ".")
	build.Env = append(os.Environ(), "GOWORK=off")
	if out, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build %v: %s", e, out)
	}
	base := []string{"-suite=quicksilver", "-dbs=treedb", "-keys=17", "-quicksilver-reads=41", "-quicksilver-duration=10ms"}
	for _, bad := range [][]string{{"-quicksilver-commit=bad"}, {"-quicksilver-working-set=bad"}, {"-quicksilver-miss-percent=-1"}, {"-quicksilver-mixture=bad"}, {"-valsize=256"}, {"-quicksilver-case=random4k", "-seed=2"}} {
		args := append(append([]string{}, base...), bad...)
		out, e := exec.Command(binaryPath, args...).CombinedOutput()
		if e == nil || strings.Contains(string(out), "failed DB retained") {
			t.Fatalf("invalid config opened engine: %v/%v: %s", bad, e, out)
		}
	}
	dir := t.TempDir()
	args := append(base, "-seed=91", "-quicksilver-mixture=holdout", "-quicksilver-working-set=20%", "-quicksilver-miss-percent=70", "-profile-dir="+dir)
	cmd := exec.Command(binaryPath, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	raw, e := cmd.Output()
	if e != nil {
		t.Fatalf("CLI %v: %s", e, stderr.String())
	}
	var reports []quicksilverResult
	if e = json.Unmarshal(raw, &reports); e != nil {
		t.Fatal(e)
	}
	r := reports[0]
	if r.Config.Seed != 91 || r.Config.Case != "realistic" || r.Config.Mixture != "holdout" || r.Config.CommitMode != "ordinary" || r.Config.WorkingSet != "20%" || r.Config.MissPercent != 70 || r.Compressibility.Total.Records != 17 {
		t.Fatalf("CLI resolution lost: %+v", r)
	}
	if !strings.Contains(stderr.String(), "commit=ordinary seed=91 working_set=20% miss_percent=70") || strings.Contains(stderr.String(), "key_bytes=32 shared_prefix=24") {
		t.Fatalf("misleading banner: %s", stderr.String())
	}
	sidecar, e := os.ReadFile(filepath.Join(dir, "quicksilver_results.json"))
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(bytes.TrimSpace(raw), bytes.TrimSpace(sidecar)) {
		t.Fatal("sidecar differs from stdout contract")
	}
	for _, file := range []string{"benchprof_results.json", "benchprof_results.md", "insights.json", "cpu_quicksilver_hits_treedb.pprof", "allocs_quicksilver_hits_treedb.pprof"} {
		info, e := os.Stat(filepath.Join(dir, file))
		if e != nil || info.Size() == 0 {
			t.Fatalf("missing consumer artifact %s: %v", file, e)
		}
	}
}
