package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/kvstore"
)

func quicksilverSmokeConfig() quicksilverConfig {
	return quicksilverConfig{Case: "random4k", Keys: 73, Reads: 101, Workers: 4, ReadBatch: 64, Duration: 20 * time.Millisecond, Updates: 73}
}
func TestQuicksilverConfig(t *testing.T) {
	c := quicksilverSmokeConfig()
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*quicksilverConfig){func(c *quicksilverConfig) { c.Keys = 0 }, func(c *quicksilverConfig) { c.Reads = 0 }, func(c *quicksilverConfig) { c.Workers = 0 }, func(c *quicksilverConfig) { c.ReadBatch = 0 }, func(c *quicksilverConfig) { c.Duration = 0 }, func(c *quicksilverConfig) { c.Updates = c.Keys + 1 }, func(c *quicksilverConfig) { c.Case = "bad" }} {
		bad := c
		mutate(&bad)
		if bad.validate() == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
}
func TestQuicksilverRejectsUnusedWorkflowFlags(t *testing.T) {
	for _, name := range []string{"checkpoint-settle-before-tests", "checkpoint-settle-timeout", "range-span", "range-queries", "write-workers", "batch-delete-range-width", "batch-write-dict-warmup", "outdir", "format"} {
		_, err := resolveQuicksilverConfig(BenchConfig{}, map[string]bool{name: true})
		if err == nil || !strings.Contains(err.Error(), "-"+name+" does not apply") {
			t.Fatalf("silently ignored -%s: %v", name, err)
		}
	}
}

func TestQuicksilverWorkflow(t *testing.T) {
	c := quicksilverSmokeConfig()
	r, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if r.VerifiedKeys != c.Keys || r.VerifiedMisses != c.Keys || r.UpdatedKeys != c.Updates {
		t.Fatalf("oracle counts: %+v", r)
	}
	for _, p := range r.Phases[:3] {
		if p.Ops != c.Reads {
			t.Fatalf("aggregate remainder lost: %+v", p)
		}
	}
}

func TestQuicksilverRetainedVerification(t *testing.T) {
	c := quicksilverSmokeConfig()
	dir := t.TempDir()
	if _, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, dir); err != nil {
		t.Fatal(err)
	}
	cfg := BenchConfig{DBsArg: "treedb", QuicksilverVerifyDir: dir}
	raw, err := runQuicksilverSuite(cfg, c, "")
	if err != nil {
		t.Fatal(err)
	}
	var report quicksilverVerificationResult
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		t.Fatal(err)
	}
	if !report.VerificationOnly || report.VerifiedKeys != c.Keys || report.VerifiedMisses != c.Keys || report.DataDir != dir {
		t.Fatalf("incomplete verification: %+v", report)
	}
	db, err := NewTreeDBPublicCommandWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := quicksilverKey(0)
	if err := db.Delete(key[:]); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := runQuicksilverSuite(cfg, c, ""); err == nil {
		t.Fatal("accepted a deleted live key")
	}
}

func TestQuicksilverRetainedVerificationRejectsBeforeOpen(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	for _, dir := range []string{missing, t.TempDir()} {
		if _, err := runQuicksilverSuite(BenchConfig{DBsArg: "treedb", QuicksilverVerifyDir: dir}, quicksilverSmokeConfig(), ""); err == nil {
			t.Fatal("accepted absent/empty database")
		}
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("initialized a missing database: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sentinel"), []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []BenchConfig{
		{DBsArg: "treedb,hashdb", QuicksilverVerifyDir: dir},
		{DBsArg: "treedb", QuicksilverVerifyDir: dir, CPUProfile: "unused.pprof"},
		{DBsArg: "treedb", QuicksilverVerifyDir: dir},
	} {
		if _, err := runQuicksilverSuite(cfg, quicksilverSmokeConfig(), ""); err == nil {
			t.Fatal("accepted incompatible verification mode")
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "sentinel" {
		t.Fatalf("opened a rejected directory: %v %v", entries, err)
	}
}

func TestQuicksilverRetainedRealisticVerification(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.Case, c.Seed = "realistic", 24
	dir := t.TempDir()
	initial, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := BenchConfig{DBsArg: "treedb", QuicksilverVerifyDir: dir}
	raw, err := runQuicksilverSuite(cfg, c, "")
	if err != nil {
		t.Fatal(err)
	}
	var report quicksilverVerificationResult
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		t.Fatal(err)
	}
	if report.VerifiedKeys != initial.VerifiedKeys || report.VerifiedMisses != initial.VerifiedMisses {
		t.Fatalf("verification differs from suite: %+v", report)
	}
	c.Seed = 91
	if _, err := runQuicksilverSuite(cfg, c, ""); err == nil {
		t.Fatal("accepted the wrong fixture seed")
	}
}

func TestQuicksilverRetainedBackendVerification(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.Case = "structured256"
	dir := t.TempDir()
	if _, err := runQuicksilverEngine(BenchConfig{}, c, "treedb_backend", NewTreeDBBackend, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := runQuicksilverSuite(BenchConfig{DBsArg: "treedb_backend", QuicksilverVerifyDir: dir}, c, ""); err != nil {
		t.Fatal(err)
	}
	publicDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(publicDir, "maindb"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publicDir, "maindb", "index.db"), []byte("public marker"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := runQuicksilverSuite(BenchConfig{DBsArg: "treedb_backend", QuicksilverVerifyDir: publicDir}, c, ""); err == nil {
		t.Fatal("accepted a public database as a backend database")
	}
	if _, err := os.Stat(filepath.Join(publicDir, "index.db")); !os.IsNotExist(err) {
		t.Fatalf("initialized the wrong layout: %v", err)
	}
}

func TestQuicksilverOracle(t *testing.T) {
	for _, n := range []int{1, 73, 7919, 15838} {
		stride := quicksilverUpdateStride(n)
		seen := make(map[int]bool)
		for i := 0; i < n; i++ {
			id := int(int64(i) * int64(stride) % int64(n))
			if seen[id] {
				t.Fatalf("duplicate update for n=%d", n)
			}
			seen[id] = true
		}
	}
	v := make([]byte, 256)
	quicksilverValue(v, 2, 0, false)
	if err := quicksilverCheckValue(2, v, 256, false); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []struct {
		id uint64
		v  []byte
	}{{3, v}, {4, v}, {2, v[:15]}} {
		if quicksilverCheckValue(bad.id, bad.v, 256, false) == nil {
			t.Fatal("bad value accepted")
		}
	}
	quicksilverValue(v, 2, 1, false)
	if quicksilverCheckValue(2, v, 256, false) == nil {
		t.Fatal("generation1 accepted before updates")
	}
	quicksilverValue(v, 2, 2, false)
	if quicksilverCheckValue(2, v, 256, true) == nil {
		t.Fatal("generation2 accepted")
	}
}

type quicksilverNoSnapshotDB struct{ *batchDeleteRangeMemoryDB }

func (*quicksilverNoSnapshotDB) Checkpoint() error { return nil }
func TestQuicksilverCapabilityFailure(t *testing.T) {
	c := quicksilverSmokeConfig()
	open := func(string) (kvstore.DB, error) {
		return &quicksilverNoSnapshotDB{newBatchDeleteRangeMemoryDB("NoSnapshot")}, nil
	}
	_, err := runQuicksilverEngine(BenchConfig{}, c, "fake", open, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "lacks read snapshots") {
		t.Fatalf("silent fallback: %v", err)
	}
	_, err = runQuicksilverSuite(BenchConfig{DBsArg: "treedb,unknown"}, c, "")
	if err == nil || !strings.Contains(err.Error(), "unknown DB") {
		t.Fatalf("unknown name dropped: %v", err)
	}
}

type quicksilverFailSnapshotDB struct {
	errorGetDB
	active  atomic.Int32
	entered chan struct{}
	once    sync.Once
	read    func([]byte) ([]byte, error)
}

func (d *quicksilverFailSnapshotDB) AcquireReadSnapshot() (kvstore.ReadSnapshot, error) {
	d.active.Add(1)
	return &quicksilverFailSnapshot{d: d}, nil
}

type quicksilverFailSnapshot struct{ d *quicksilverFailSnapshotDB }

func (s *quicksilverFailSnapshot) Get(key []byte) ([]byte, error) {
	s.d.once.Do(func() { close(s.d.entered) })
	if s.d.read != nil {
		return s.d.read(key)
	}
	return nil, errors.New("injected read failure")
}
func (s *quicksilverFailSnapshot) GetAppend(k, dst []byte) ([]byte, error) { return s.Get(k) }
func (s *quicksilverFailSnapshot) Close() error                            { s.d.active.Add(-1); return nil }
func TestQuicksilverErrorJoins(t *testing.T) {
	c := quicksilverSmokeConfig()
	d := &quicksilverFailSnapshotDB{entered: make(chan struct{})}
	joined := make(chan struct{})
	writer := func(ctx context.Context) error { defer close(joined); <-ctx.Done(); return nil }
	_, err := quicksilverReadPhase(d, c, newQuicksilverFixture(c), 3, nil, writer, nil)
	if err == nil || !strings.Contains(err.Error(), "injected read failure") {
		t.Fatalf("failure lost: %v", err)
	}
	select {
	case <-joined:
	default:
		t.Fatal("writer did not join")
	}
	if d.active.Load() != 0 {
		t.Fatal("snapshot leaked")
	}
	// Successful reads cannot independently cancel the writer-error path.
	c.Workers, c.Reads = 1, 257
	memory := newBatchDeleteRangeMemoryDB("memory")
	if err := quicksilverWrite(memory, c, 0, c.Keys, quicksilverUpdateStride(c.Keys), false); err != nil {
		t.Fatal(err)
	}
	resume := make(chan struct{})
	d = &quicksilverFailSnapshotDB{entered: make(chan struct{}), read: func(key []byte) ([]byte, error) {
		<-resume
		return memory.Get(key)
	}}
	failureCtx := make(chan context.Context, 1)
	done := make(chan struct{})
	var phase quicksilverPhase
	go func() {
		defer close(done)
		phase, err = quicksilverReadPhase(d, c, newQuicksilverFixture(c), 0, nil, func(ctx context.Context) error {
			<-d.entered
			failureCtx <- ctx
			return errors.New("injected writer failure")
		}, nil)
	}()
	var ctx context.Context
	select {
	case ctx = <-failureCtx:
	case <-time.After(5 * time.Second):
		close(resume)
		t.Fatal("writer did not reach the valid-read barrier")
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Error("writer failure did not cancel the live reader context")
	}
	close(resume) // Let the valid first read finish after writer cancellation.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("readers did not join after writer failure")
	}
	// Cancellation is polled every 256 reads; the 257th read must not execute.
	if err == nil || err.Error() != "injected writer failure" || phase.Ops != 256 || d.active.Load() != 0 {
		t.Fatalf("writer failure cleanup: ops=%d active=%d err=%v", phase.Ops, d.active.Load(), err)
	}
}

type quicksilverCorruptDB struct{ kvstore.DB }

func (d quicksilverCorruptDB) Get(k []byte) ([]byte, error) {
	v, e := d.DB.Get(k)
	if len(v) > 16 {
		v[16] ^= 1
	}
	return v, e
}
func TestQuicksilverFullByteOracle(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.Case = "structured256"
	opens := 0
	factory := func(dir string) (kvstore.DB, error) {
		d, e := NewTreeDBPublicCommandWAL(dir)
		opens++
		if opens == 3 && e == nil {
			return quicksilverCorruptDB{d}, nil
		}
		return d, e
	}
	_, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", factory, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "reopen byte mismatch") {
		t.Fatalf("payload corruption accepted: %v", err)
	}
}

// Isolates harness overhead with a no-allocation missing-key adapter. The trace,
// keys and sample buffers are already built, as in the production read phase.
func TestQuicksilverHarnessAllocations(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.ReadBatch = 1
	c.Reads = 10000
	f := newQuicksilverFixture(c)
	d := &fixedNameDB{name: "misses"}
	p, err := quicksilverReadPhase(d, c, f, 1, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Ops != c.Reads || p.AllocsPerOp > .01 || p.BytesPerOp > 8 {
		t.Fatalf("avoidable per-read allocation: %+v", p)
	}
	t.Logf("harness observed %.4f allocs/read, %.2f bytes/read; fixture=%d bytes samples=%d bytes", p.AllocsPerOp, p.BytesPerOp, quicksilverTraceLength*72, quicksilverSampleLimit*8)
}

type quicksilverTailStatsDB struct {
	fixedNameDB
	tailComplete atomic.Bool
	statsCalled  chan struct{}
}

func (d *quicksilverTailStatsDB) Stats() map[string]string {
	state := "pending"
	if d.tailComplete.Load() {
		state = "complete"
	}
	d.statsCalled <- struct{}{}
	return map[string]string{"writer_tail": state}
}

func TestQuicksilverStatsAfterWriterDrain(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.Workers, c.ReadBatch, c.Reads = 1, 1, 1
	d := &quicksilverTailStatsDB{statsCalled: make(chan struct{}, 2)}
	release := make(chan struct{})
	readersJoined := make(chan struct{})
	done := make(chan struct{})
	var releaseOnce sync.Once
	var phase quicksilverPhase
	var err error
	go func() {
		defer close(done)
		phase, err = quicksilverReadPhase(d, c, newQuicksilverFixture(c), 1, nil, func(context.Context) error {
			<-release
			d.tailComplete.Store(true)
			return nil
		}, func() { close(readersJoined) })
	}()
	defer func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("phase did not join after releasing writer tail")
		}
	}()
	select {
	case <-readersJoined:
	case <-time.After(5 * time.Second):
		t.Fatal("reader profile did not stop before writer tail")
	}
	<-d.statsCalled // StatsBefore precedes reader start.
	select {
	case <-d.statsCalled:
		t.Error("StatsAfter sampled before writer tail joined")
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("phase did not join after releasing writer tail")
	}
	if err != nil || phase.StatsBefore["writer_tail"] != "pending" || phase.StatsAfter["writer_tail"] != "complete" {
		t.Fatalf("stats do not bracket completed writer: before=%v after=%v err=%v", phase.StatsBefore, phase.StatsAfter, err)
	}
}

func TestQuicksilverReaderTimerExcludesWriterDrain(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.ReadBatch = 1
	c.Duration = time.Millisecond
	c.Case = "structured256"
	d := newBatchDeleteRangeMemoryDB("memory")
	if err := quicksilverWrite(d, c, 0, c.Keys, quicksilverUpdateStride(c.Keys), false); err != nil {
		t.Fatal(err)
	}
	p, err := quicksilverReadPhase(d, c, newQuicksilverFixture(c), 3, nil, func(context.Context) error { time.Sleep(50 * time.Millisecond); return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Seconds >= p.CompositionSeconds/2 || p.CompositionSeconds < .04 {
		t.Fatalf("writer drain entered reader denominator: %+v", p)
	}
}

func TestQuicksilverCheckpointNanosecondRoundTrip(t *testing.T) {
	for _, elapsed := range []time.Duration{1, 32259, 1633660, 100001} {
		t.Run(elapsed.String(), func(t *testing.T) {
			// The measured duration travels through milliseconds in the suite JSON
			// before the canonical exporter reconstructs a nanosecond duration.
			ms := float64(elapsed) / float64(time.Millisecond)
			reports := []quicksilverResult{{Engine: "treedb", DBName: "TreeDB", InitialCheckpointMS: ms, FinalCheckpointMS: ms}}
			run := quicksilverBenchprofRuns(BenchConfig{}, quicksilverSmokeConfig(), reports)[0]
			for _, phase := range []string{"quicksilver_initial", "quicksilver_final"} {
				if got := run.CheckpointDurations[phase]["TreeDB"]; got != elapsed {
					t.Fatalf("%s: duration round trip lost a nanosecond: got %s, want %s", phase, got, elapsed)
				}
			}
		})
	}
}

func TestQuicksilverMultiEngineMarkdown(t *testing.T) {
	c := quicksilverSmokeConfig()
	reports := []quicksilverResult{
		{Engine: "treedb", InitialCheckpointMS: 10, FinalCheckpointMS: 20},
		{Engine: "treedb_bench_unsafe", InitialCheckpointMS: 30, FinalCheckpointMS: 40},
	}
	for engine := range reports {
		open, err := GetDBFactory(reports[engine].Engine)
		if err != nil {
			t.Fatal(err)
		}
		db, err := open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		reports[engine].wrapper = db
		reports[engine].DBName = db.Name()
		reports[engine].FinalStats = quicksilverStats(db)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		for phase, name := range quicksilverPhaseNames {
			reports[engine].Phases = append(reports[engine].Phases, quicksilverPhase{Name: name, OpsPerSec: float64((engine+1)*100 + phase)})
		}
	}
	dir := t.TempDir()
	if err := writeBenchprofArtifacts(dir, "", quicksilverBenchprofRuns(BenchConfig{}, c, reports)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "benchprof_results.md"))
	if err != nil {
		t.Fatal(err)
	}
	lines := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		lines[strings.Join(strings.Fields(line), " ")] = true
	}
	if !lines["Test TreeDB TreeDB (bench_unsafe)"] {
		t.Fatalf("missing engine columns: %s", raw[:min(len(raw), 1500)])
	}
	for phase, name := range quicksilverPhaseNames {
		expected := name + " " + formatFloat(float64(100+phase)) + " " + formatFloat(float64(200+phase))
		if !lines[expected] {
			t.Fatalf("missing actual engine values %q: %s", expected, raw[:min(len(raw), 1500)])
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
	if err != nil {
		t.Fatal(err)
	}
	var exported benchprofExport
	if err := json.Unmarshal(data, &exported); err != nil {
		t.Fatal(err)
	}
	if len(exported.Runs) != 1 || len(exported.Runs[0].Results[quicksilverPhaseNames[0]]) != 2 {
		t.Fatalf("fixed workload must export one run containing both engines: %s", data)
	}
	run := quicksilverBenchprofRuns(BenchConfig{}, c, reports)[0]
	if run.TreeDBStats["TreeDB"]["treedb.profile.bench_unsafe"] != "false" || run.TreeDBStats["TreeDB (bench_unsafe)"]["treedb.profile.bench_unsafe"] != "true" {
		t.Fatalf("variant settings lost: %+v", run.TreeDBStats)
	}
	if len(run.TreeDBStats) != 2 || len(run.CheckpointDurations["quicksilver_initial"]) != 2 || len(run.CheckpointDurations["quicksilver_final"]) != 2 {
		t.Fatalf("engine metadata dropped: %+v", run)
	}
}

func TestQuicksilverRejectsLevelDBBeforeOpen(t *testing.T) {
	original := dbFactories
	originalMode := *leveldbBlockCompressionMode
	t.Cleanup(func() { dbFactories = original; *leveldbBlockCompressionMode = originalMode })
	calls := 0
	dbFactories = make(map[string]DBFactory, len(original))
	for name := range original {
		dbFactories[name] = func(string) (kvstore.DB, error) { calls++; return nil, errors.New("unexpected engine open") }
	}
	for _, mode := range []string{"default", "both"} {
		*leveldbBlockCompressionMode = mode
		for _, batch := range []int{1, 64} {
			c := quicksilverSmokeConfig()
			c.ReadBatch = batch
			for _, names := range []string{"leveldb", "leveldb_block_comp_on", "leveldb_block_comp_off", "treedb,leveldb", "all"} {
				_, err := runQuicksilverSuite(BenchConfig{DBsArg: names}, c, "")
				if err == nil || !strings.Contains(err.Error(), "checkpoint closes/reopens") || calls != 0 {
					t.Fatalf("selection=%s compression=%s readBatch=%d opened=%d: %v", names, mode, batch, calls, err)
				}
			}
		}
	}
}
