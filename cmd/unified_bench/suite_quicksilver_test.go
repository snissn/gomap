package main

import (
	"context"
	"errors"
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
}

func (d *quicksilverFailSnapshotDB) AcquireReadSnapshot() (kvstore.ReadSnapshot, error) {
	d.active.Add(1)
	return &quicksilverFailSnapshot{d: d}, nil
}

type quicksilverFailSnapshot struct{ d *quicksilverFailSnapshotDB }

func (s *quicksilverFailSnapshot) Get([]byte) ([]byte, error) {
	s.d.once.Do(func() { close(s.d.entered) })
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
	// The sibling failure path cancels and joins readers when the writer fails.
	_, err = quicksilverReadPhase(d, c, newQuicksilverFixture(c), 3, nil, func(context.Context) error { return errors.New("injected writer failure") }, nil)
	if err == nil || !strings.Contains(err.Error(), "injected writer failure") || d.active.Load() != 0 {
		t.Fatalf("writer failure cleanup: %v", err)
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
