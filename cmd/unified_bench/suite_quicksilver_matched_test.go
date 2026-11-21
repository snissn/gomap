package main

import (
	"context"
	"errors"
	"github.com/snissn/gomap/kvstore"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestQuicksilverFixedWorkQuotas(t *testing.T) {
	for _, reads := range []int{101, 2} {
		t.Run(string(rune('a'+reads%26)), func(t *testing.T) {
			c := quicksilverSmokeConfig()
			c.ReadBatch = 1 // The in-memory fixture has no snapshot interface.
			c.ConcurrentMode, c.Reads = "fixed-work", reads
			db := newBatchDeleteRangeMemoryDB("memory")
			if err := quicksilverWrite(db, c, 0, c.Keys, quicksilverUpdateStride(c.Keys), false); err != nil {
				t.Fatal(err)
			}
			progress := &quicksilverWriterProgress{}
			f := newQuicksilverFixture(c)
			p, err := quicksilverReadPhase(db, c, f, 3, nil, func(context.Context) error {
				progress.commit(c.Updates, 0)
				progress.group(c.Updates)
				progress.checkpoint(true)
				return nil
			}, nil, progress)
			if err != nil {
				t.Fatal(err)
			}
			if p.Ops != reads || !p.ConcurrentWork.Completed || !p.ConcurrentWork.ReaderJoined || !p.ConcurrentWork.WriterJoined {
				t.Fatalf("incomplete: %+v", p)
			}
			for w, r := range f.readers {
				want := reads / c.Workers
				if w < reads%c.Workers {
					want++
				}
				if r.count != want {
					t.Fatalf("worker%d: got%d want%d", w, r.count, want)
				}
			}
			if p.ConcurrentWork.BothJoin == nil || p.ConcurrentWork.BothJoin.AllocatedBytes < p.AllocatedBytes {
				t.Fatalf("cut order: %+v", p.ConcurrentWork)
			}
		})
	}
}

func TestQuicksilverFixedWorkWriterTailCut(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.ConcurrentMode = "fixed-work"
	c.ReadBatch = 1 // The in-memory fixture has no snapshot interface.
	c.Workers = 1
	c.Reads = 1
	db := newBatchDeleteRangeMemoryDB("memory")
	if err := quicksilverWrite(db, c, 0, c.Keys, quicksilverUpdateStride(c.Keys), false); err != nil {
		t.Fatal(err)
	}
	progress := &quicksilverWriterProgress{}
	tail := make(chan []byte, 1)
	stopped := make(chan struct{}, 1)
	p, err := quicksilverReadPhase(db, c, newQuicksilverFixture(c), 3, nil, func(ctx context.Context) error {
		deadline := time.Now().Add(5 * time.Second)
		for !progress.readerCutTaken.Load() {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if time.Now().After(deadline) {
				return errors.New("reader cut not observed")
			}
			runtime.Gosched()
		}
		select {
		case <-stopped:
			return errors.New("CPU stopped before writer tail")
		default:
		}
		b := make([]byte, 1<<20)
		b[0] = 1
		tail <- b
		progress.commit(c.Updates, 0)
		progress.group(c.Updates)
		progress.checkpoint(true)
		return nil
	}, func() { stopped <- struct{}{} }, progress)
	if err != nil {
		t.Fatal(err)
	}
	b := <-tail
	runtime.KeepAlive(b)
	if p.ConcurrentWork.BothJoin.AllocatedBytes-p.AllocatedBytes < 1<<20 {
		t.Fatalf("tail omitted: %+v", p.ConcurrentWork)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("CPU profile did not stop after both joined")
	}
	if p.ConcurrentWork.ReaderCutProgress.After.Groups != 0 || p.ConcurrentWork.AfterBothJoin.Groups != 1 {
		t.Fatalf("wrong actual progress: %+v", p.ConcurrentWork)
	}
}

func TestQuicksilverFixedWorkReadErrorJoins(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.ConcurrentMode = "fixed-work"
	db := &quicksilverFailSnapshotDB{entered: make(chan struct{})}
	joined := make(chan struct{})
	p, err := quicksilverReadPhase(db, c, newQuicksilverFixture(c), 3, nil, func(ctx context.Context) error { defer close(joined); <-ctx.Done(); return ctx.Err() }, nil, &quicksilverWriterProgress{})
	if err == nil || !strings.Contains(err.Error(), "injected read failure") || db.active.Load() != 0 {
		t.Fatalf("lost failure/owner: %+v %v", p, err)
	}
	<-joined
	if p.ConcurrentWork.Completed || !p.ConcurrentWork.ReaderJoined || !p.ConcurrentWork.WriterJoined || p.ConcurrentWork.Error == "" {
		t.Fatalf("false completion: %+v", p.ConcurrentWork)
	}
}

func TestQuicksilverFixedWorkWriterErrorJoins(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.ConcurrentMode = "fixed-work"
	db := &quicksilverFailSnapshotDB{entered: make(chan struct{}), read: func([]byte) ([]byte, error) { return nil, nil }}
	progress := &quicksilverWriterProgress{}
	p, err := quicksilverReadPhase(db, c, newQuicksilverFixture(c), 3, nil, func(ctx context.Context) error { progress.commit(1, 0); return errors.New("injected commit error") }, nil, progress)
	if err == nil || !strings.Contains(err.Error(), "injected commit error") || db.active.Load() != 0 {
		t.Fatalf("lost writer failure: %+v %v", p, err)
	}
	if p.ConcurrentWork.Completed || p.ConcurrentWork.AfterBothJoin.Commits != 1 || p.ConcurrentWork.AfterBothJoin.Groups != 0 {
		t.Fatalf("partial commits estimated: %+v", p.ConcurrentWork)
	}
}

func TestQuicksilverConcurrentModeConfig(t *testing.T) {
	c := quicksilverSmokeConfig()
	if c.resolved().ConcurrentMode != "duration" {
		t.Fatal("default duration changed")
	}
	c.ConcurrentMode = "invalid"
	if c.validate() == nil {
		t.Fatal("accepted unknown mode")
	}
	c.ConcurrentMode = "fixed-work"
	if c.validate() != nil {
		t.Fatal("rejected fixed-work")
	}
	if _, err := runQuicksilverSuite(BenchConfig{QuicksilverVerifyDir: t.TempDir(), DBsArg: "treedb"}, c, ""); err == nil || !strings.Contains(err.Error(), "concurrent mode") {
		t.Fatalf("ignored verify-only mode: %v", err)
	}
}

// The same production paced writer, ordinary batch API and reopen oracle run in
// this small control. It covers a remainder group and actual quarter checkpoints.
func TestQuicksilverFixedWorkWorkflow(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.ConcurrentMode, c.Keys, c.Updates, c.Reads = "fixed-work", 2001, 2001, 13
	c.Duration = 10 * time.Millisecond
	r, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := r.Phases[3].ConcurrentWork
	if !w.Completed || w.CompletedReads != 13 || w.AfterBothJoin.Commits != 9 || w.AfterBothJoin.Groups != 3 || w.AfterBothJoin.Checkpoints != 3 || w.AfterBothJoin.Sets != 3001 || w.AfterBothJoin.Deletes != 500 {
		t.Fatalf("actual writer work: %+v", w)
	}
	if r.UpdatedKeys != 2001 || r.MutationCommitBatches != 9 || len(r.Checkpoints) != 3 || r.VerifiedKeys != 2001 || r.VerifiedMisses != 4522 {
		t.Fatalf("lost original writer/oracle: %+v", r)
	}
}

type quicksilverProgressFaultDB struct {
	*batchDeleteRangeMemoryDB
	batches, failCommit, failClose int
}

func (d *quicksilverProgressFaultDB) NewBatch() (kvstore.Batch, error) {
	b, err := d.batchDeleteRangeMemoryDB.NewBatch()
	d.batches++
	return &quicksilverProgressFaultBatch{Batch: b, owner: d, number: d.batches}, err
}

type quicksilverProgressFaultBatch struct {
	kvstore.Batch
	owner  *quicksilverProgressFaultDB
	number int
}

func (b *quicksilverProgressFaultBatch) Commit() error {
	if b.number == b.owner.failCommit {
		return errors.New("injected commit failure")
	}
	return b.Batch.Commit()
}
func (b *quicksilverProgressFaultBatch) Close() error {
	err := b.Batch.Close()
	if b.number == b.owner.failClose {
		return errors.Join(err, errors.New("injected Close failure"))
	}
	return err
}
func TestQuicksilverWriterProgressPartialCommit(t *testing.T) {
	for _, fault := range []string{"commit", "Close"} {
		t.Run(fault, func(t *testing.T) {
			c := quicksilverRealisticSmokeConfig()
			c.CommitMode = "ordinary"
			db := &quicksilverProgressFaultDB{batchDeleteRangeMemoryDB: newBatchDeleteRangeMemoryDB("memory")}
			if fault == "commit" {
				db.failCommit = 2
			} else {
				db.failClose = 1
			}
			progress := &quicksilverWriterProgress{}
			err := quicksilverWrite(db, c, 0, 4, quicksilverUpdateStride(c.Keys), true, progress)
			if err == nil || !strings.Contains(err.Error(), fault+" failure") {
				t.Fatalf("lost error: %v", err)
			}
			s := progress.snapshot(time.Now())
			if s.Commits != 1 || s.Sets != 3 || s.Deletes != 1 || s.Groups != 0 || s.Targets != 0 {
				t.Fatalf("estimated failed work: %+v", s)
			}
		})
	}
}
func TestQuicksilverWriterProgressSnapshots(t *testing.T) {
	p := &quicksilverWriterProgress{}
	clock := time.Now()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for i := 0; i < 1000; i++ {
			p.commit(5, 2)
		}
	}()
	for {
		s := p.snapshot(clock)
		if s.Sets != 5*s.Commits || s.Deletes != 2*s.Commits {
			t.Fatalf("torn progress: %+v", s)
		}
		select {
		case <-done:
			wg.Wait()
			goto checkpoint
		default:
		}
	}
checkpoint:
	p.startCheckpoint()
	partial := p.snapshot(clock)
	if !partial.CheckpointActive || partial.Checkpoints != 0 {
		t.Fatalf("checkpoint not bracketed: %+v", partial)
	}
	p.checkpoint(false)
	partial = p.snapshot(clock)
	if partial.CheckpointActive || partial.Checkpoints != 0 {
		t.Fatalf("failed checkpoint counted: %+v", partial)
	}
	p.startCheckpoint()
	p.checkpoint(true)
	if s := p.snapshot(clock); s.Checkpoints != 1 || s.CheckpointActive {
		t.Fatalf("successful checkpoint lost: %+v", s)
	}
}
func TestQuicksilverExpectedMatchedWork(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.Updates = 40000
	g, m, h, sets, deletes := quicksilverExpectedWriter(c)
	if g != 40 || m != 160 || h != 4 || sets != 60000 || deletes != 10000 {
		t.Fatalf("changed 40000 targets: %d %d %d %d %d", g, m, h, sets, deletes)
	}
}

func TestQuicksilverFixedWorkRejectsMissingOwner(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.ConcurrentMode = "fixed-work"
	db := newBatchDeleteRangeMemoryDB("memory")
	_, err := quicksilverReadPhase(db, c, newQuicksilverFixture(c), 3, nil, func(context.Context) error { return nil }, nil)
	if err == nil || !strings.Contains(err.Error(), "owned writer progress") {
		t.Fatalf("missing owner accepted: %v", err)
	}
}
