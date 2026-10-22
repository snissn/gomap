package caching

import (
	"bytes"
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/tree"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// Deliberately reduced valid test-only limits isolate each finite dimension.
// Pressure comes from actual external ownership, never rewritten budget stats.
func TestCOWPreflightIncludesCanonicalControlHeadroom(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			for _, dimension := range []string{"retirement", "total"} {
				t.Run(dimension, func(t *testing.T) {
					limits := memtable.DefaultCOWLimits()
					limits.MaxGenerationBytes = 1 << 20
					limits.MaxTotalBytes, limits.MaxRetiredBytes, limits.MaxInFlightBytes = 8<<20, 8<<20, 8<<20
					if dimension == "retirement" {
						limits.MaxRetiredBytes = 4 << 20
					} else {
						limits.MaxTotalBytes = 4 << 20
					}
					db, backend := cowSourcePressureFixture(t, mode, 1, limits)
					b := db.NewBatchWithSize(1)
					defer b.Close()
					key, value := []byte("late-control"), bytes.Repeat([]byte("v"), 128)
					if err := b.Set(key, value); err != nil {
						t.Fatal(err)
					}
					if err := b.cowPrepareWriteStorage(); err != nil {
						t.Fatal(err)
					}
					charge, err := db.cow.writers[0].Estimate([]memtable.COWMutation{{Key: key, Value: value, Flags: node.FlagInline}}, memtable.COWPrepareOptions{})
					if err != nil {
						t.Fatal(err)
					}
					// A strict sub-bound of known late control: canonical scratch alone.
					overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchPreparation{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWPrepareOptions{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
					stats := db.COWMemoryStats()
					used, limit := stats.HistoryBytes+stats.ReservedBytes+stats.DeferredBytes, limits.MaxRetiredBytes
					if dimension == "total" {
						used, limit = stats.TotalBytes, limits.MaxTotalBytes
					}
					headroom := charge.Total() + overhead/2
					leaseOverhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
					if used+headroom+leaseOverhead >= limit {
						t.Fatal("fixture lacks headroom")
					}
					filler, err := db.cow.budget.AcquireExternal(limit - used - headroom - leaseOverhead)
					if err != nil {
						t.Fatal(err)
					}
					defer filler.Close()
					stats = db.COWMemoryStats()
					used = stats.HistoryBytes + stats.ReservedBytes + stats.DeferredBytes
					if dimension == "total" {
						used = stats.TotalBytes
					}
					if used+charge.Total() > limit || used+charge.Total()+overhead <= limit {
						t.Fatalf("not missing-control-only pressure: stats=%+v charge=%+v overhead=%d", stats, charge, overhead)
					}
					seq, lsn := db.cow.cut.logicalSeq, backend.CommandWALNextLSN()
					db.cow.writerMu.Lock()
					r, pressure, attempt, predictErr := b.cowGenerationRollover(true)
					// Exercise the old failure mechanism directly, without revisions or WAL:
					// the same actual staged command cannot satisfy canonical late control.
					canonicalErr := b.PrepareExternalCommandWALPublication()
					cleanup := b.cancelCOWPublication()
					db.cow.writerMu.Unlock()
					if cleanup != nil {
						if err := cleanup.drainCancelled(); err != nil {
							t.Fatal(err)
						}
					}
					if !errors.Is(canonicalErr, memtable.ErrCOWCapacity) {
						t.Fatalf("canonical capacity origin not reproduced: %v", canonicalErr)
					}
					if db.cow.cut.logicalSeq != seq || backend.CommandWALNextLSN() != lsn || b.COWReceipt().Assigned || b.COWReceipt().LogicalInstalled {
						t.Fatal("refusal assigned or published")
					}
					if predictErr != nil || r != nil || !pressure || attempt {
						t.Fatalf("late %s control missing from preproducer pressure: rollover=%t pressure=%t optional=%t err=%v", dimension, r != nil, pressure, attempt, predictErr)
					}
				})
			}
		})
	}
}

func cowControlSequence(c *cowCache) uint64 {
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	return c.cut.logicalSeq
}

// All limits below are valid reduced TEST limits. The real command-WAL owner,
// journal, history, views and external leases retain their normal accounting.
func newCOWControlHeadroomFixture(t *testing.T, mode, dimension string, warm, reclaimable bool) (*cowPressureRolloverFixture, uint64) {
	t.Helper()
	limits := memtable.DefaultCOWLimits()
	limits.MaxGenerationBytes = 1 << 20
	limits.MaxTotalBytes, limits.MaxRetiredBytes, limits.MaxInFlightBytes = 8<<20, 8<<20, 8<<20
	if dimension == "retirement" {
		limits.MaxRetiredBytes = 4 << 20
	} else {
		limits.MaxTotalBytes = 4 << 20
	}
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend"), CommandWAL: true, Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{TransactionOwner: owner, MemtableMode: mode, DisableWAL: false, ExternalCommandWAL: true, MemtableShards: 1, COWMemtableLimits: limits, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1 << 30, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		t.Fatal(err)
	}
	f := &cowPressureRolloverFixture{db: db, backend: backend, value: bytes.Repeat([]byte("n"), 128)}
	t.Cleanup(func() {
		f.releasePressure()
		if f.batch != nil {
			_ = f.batch.Close()
		}
		if f.old != nil {
			_ = f.old.Close()
		}
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
		if stats := db.COWMemoryStats(); stats.TotalBytes != 0 {
			t.Errorf("control fixture retained parent after Close: %+v", stats)
		}
	})
	db.flushMu.Lock()
	f.locked = true
	write := func(b *Batch) {
		t.Helper()
		if err := f.write(b, true, nil); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if warm {
		initial := db.NewBatchWithSize(1)
		if err := initial.Set([]byte("anchor"), []byte("old")); err != nil {
			t.Fatal(err)
		}
		write(initial)
	}
	f.old, err = db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	if reclaimable {
		// The held view pins only the earlier anchor generation, leaving the real
		// later seed generation reclaimable by the one ordinary checkpoint join.
		if err := db.rolloverCOWForFlush(); err != nil {
			t.Fatal(err)
		}
		seed := db.NewBatchWithSize(128)
		for i := 0; i < 128; i++ {
			if err := seed.Set([]byte(fmt.Sprintf("seed-%03d", i)), bytes.Repeat([]byte("s"), 128)); err != nil {
				t.Fatal(err)
			}
		}
		write(seed)
	}
	b := db.NewBatchWithSize(1)
	f.batch = b
	if err := b.Set([]byte("target"), f.value); err != nil {
		t.Fatal(err)
	}
	if err := b.AuthorizeCOWAppend(owner); err != nil {
		t.Fatal(err)
	}
	before := b.cowState.storageBytes
	if err := b.cowPrepareWriteStorage(); err != nil {
		t.Fatal(err)
	}
	scratch := b.cowState.storageBytes - before
	groups := [][]memtable.COWMutation{{{Key: []byte("target"), Value: f.value, Flags: node.FlagInline}}}
	charge, err := db.cow.writers[0].Estimate(groups[0], memtable.COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	control, err := b.cowKnownControlCharge(groups)
	if err != nil {
		t.Fatal(err)
	}
	stats := db.COWMemoryStats()
	used, limit := stats.HistoryBytes+stats.ReservedBytes+stats.DeferredBytes, limits.MaxRetiredBytes
	if dimension == "total" {
		used, limit = stats.TotalBytes, limits.MaxTotalBytes
	}
	// Write will stage another already-owned scratch backing before prediction.
	// Then row estimate fits, but its absent known control does not.
	headroom := scratch + charge.Total() + control/2
	lease := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	if control == 0 || used+headroom+lease >= limit {
		t.Fatalf("invalid control fixture: control=%d stats=%+v", control, stats)
	}
	f.filler, err = db.cow.budget.AcquireExternal(limit - used - headroom - lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("actual known-control fixture warm=%t reclaimable=%t dimension=%s estimate=%d control=%d repeatScratch=%d stats=%+v", warm, reclaimable, dimension, charge.Total(), control, scratch, db.COWMemoryStats())
	return f, control
}

func TestCOWKnownControlPressureJoinsBeforeOneWALPublication(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		for _, dimension := range []string{"retirement", "total"} {
			t.Run(mode+"/"+dimension, func(t *testing.T) {
				f, _ := newCOWControlHeadroomFixture(t, mode, dimension, true, true)
				c := f.db.cow
				seq, lsn, runs, pubs := c.cut.logicalSeq, f.backend.CommandWALNextLSN(), f.db.checkpointRuns.Load(), c.publications.Load()
				calls := cowPressureCalls{}
				done := make(chan error, 1)
				go func() { done <- f.write(f.batch, true, &calls) }()
				select {
				case err := <-done:
					t.Fatalf("write escaped reclaimable-history fence: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
				c.writerMu.Lock()
				observed := c.cut.logicalSeq
				c.writerMu.Unlock()
				if observed != seq || f.backend.CommandWALNextLSN() != lsn {
					t.Fatal("producer advanced before control pressure join")
				}
				f.db.flushMu.Unlock()
				f.locked = false
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("control pressure join stalled")
				}
				receipt := f.batch.COWReceipt()
				if f.db.checkpointRuns.Load() != runs+1 || c.publications.Load() != pubs+1 || cowControlSequence(c) != seq+1 || calls.append != 1 || calls.prepare != 1 || f.backend.CommandWALNextLSN() != lsn+1 || !receipt.Assigned || !receipt.LogicalInstalled {
					t.Fatalf("not one preproducer join/WAL/publication: calls=%+v receipt=%+v checkpoints=%d", calls, receipt, f.db.checkpointRuns.Load()-runs)
				}
				if got, err := f.old.Get([]byte("anchor")); err != nil || string(got) != "old" {
					t.Fatalf("held cut lost anchor: %q %v", got, err)
				}
				if got, err := f.old.Get([]byte("target")); !errors.Is(err, tree.ErrKeyNotFound) || got != nil {
					t.Fatalf("held cut saw target: %q %v", got, err)
				}
				for _, key := range []string{"target", "seed-000", "anchor"} {
					got, err := f.db.Get([]byte(key))
					if err != nil || len(got) == 0 {
						t.Fatalf("current %s: %q %v", key, got, err)
					}
				}
			})
		}
	}
}

func TestCOWKnownControlExternalOwnerRefusalIsFinite(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		for _, dimension := range []string{"retirement", "total"} {
			for _, warm := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/warm=%t", mode, dimension, warm), func(t *testing.T) {
					f, _ := newCOWControlHeadroomFixture(t, mode, dimension, warm, false)
					f.db.flushMu.Unlock()
					f.locked = false
					c := f.db.cow
					seq, lsn, runs, pubs := cowControlSequence(c), f.backend.CommandWALNextLSN(), f.db.checkpointRuns.Load(), c.publications.Load()
					calls := cowPressureCalls{}
					err := f.write(f.batch, true, &calls)
					receipt := f.batch.COWReceipt()
					if !errors.Is(err, memtable.ErrCOWCapacity) || f.db.checkpointRuns.Load() != runs+1 || c.publications.Load() != pubs || cowControlSequence(c) != seq || f.backend.CommandWALNextLSN() != lsn || calls.append != 0 || calls.prepare != 0 || receipt.Assigned || receipt.LogicalInstalled {
						t.Fatalf("nonreclaimable control pressure accepted/replayed: calls=%+v receipt=%+v err=%v joins=%d", calls, receipt, err, f.db.checkpointRuns.Load()-runs)
					}
					if !warm && c.journal != nil {
						t.Fatal("cold refused command initialized journal after producer seam")
					}
					f.filler.Close()
					f.filler = nil
					// This is an explicit caller resubmission of an unassigned command.
					if err := f.write(f.batch, true, &calls); err != nil {
						t.Fatal(err)
					}
					if calls.append != 1 || calls.prepare != 1 || f.backend.CommandWALNextLSN() != lsn+1 || cowControlSequence(c) != seq+1 {
						t.Fatal("resubmission did not publish exactly once")
					}
				})
			}
		}
	}
}

func TestCOWKnownControlIncludedInPositivePrediction(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	b := db.NewBatchWithSize(1)
	defer b.Close()
	key := cowOrderedLifecycleKey(t, "control-proof", 9)
	if err := b.SetWithMVCCInput(key, []byte("value"), owner.Input(key, 9, 0)); err != nil {
		t.Fatal(err)
	}
	if err := b.cowPrepareWriteStorage(); err != nil {
		t.Fatal(err)
	}
	groups := make([][]memtable.COWMutation, len(db.cow.writers))
	i := db.shardIndex(key)
	groups[i] = []memtable.COWMutation{{Key: key, Value: []byte("value"), Flags: node.FlagInline, MVCC: true, Timestamp: 9}}
	db.cow.writerMu.Lock()
	defer db.cow.writerMu.Unlock()
	if !b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("ample typed proof did not activate")
	}
	bound, ok := db.cow.writers[i].OrderedPredictionUpperBound(groups[i])
	if !ok {
		t.Fatal("missing typed bound")
	}
	control, err := b.cowKnownControlCharge(groups)
	if err != nil {
		t.Fatal(err)
	}
	stats := db.COWMemoryStats()
	limits := db.cow.budget.Limits()
	lease := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	remaining := bound.Total() + control/2
	if stats.ReservedBytes+remaining+lease >= limits.MaxInFlightBytes {
		t.Fatal("fixture lacks headroom")
	}
	filler, err := db.cow.budget.AcquireExternal(limits.MaxInFlightBytes - stats.ReservedBytes - remaining - lease)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	if b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("positive proof omitted late known control")
	}
	filler.Close()
	if !b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("proof did not recover after external owner released")
	}
}

func TestCOWKnownControlFrameCaptureMatchesRealCharge(t *testing.T) {
	for _, points := range []int{1, 64} {
		t.Run(fmt.Sprint(points), func(t *testing.T) {
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			before := budget.Stats()
			capture, err := newCOWFrameCapture(budget, points)
			if err != nil {
				t.Fatal(err)
			}
			after := budget.Stats()
			want := cowFrameCaptureCharge(points) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			if after.TotalBytes-before.TotalBytes != want || after.ReservedBytes-before.ReservedBytes != want {
				t.Fatalf("frame helper disagrees with real owner: before=%+v after=%+v want=%d", before, after, want)
			}
			capture.close()
			if budget.Stats().TotalBytes != before.TotalBytes {
				t.Fatal("frame owner was not refunded")
			}
		})
	}
}

func TestCOWKnownControlPreparedCutMatchesRealCharge(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		for _, state := range []string{"empty", "parked", "frozen"} {
			t.Run(mode+"/"+state, func(t *testing.T) {
				db, _ := cowSourcePressureFixture(t, mode, 2, memtable.DefaultCOWLimits())
				db.flushMu.Lock()
				defer db.flushMu.Unlock()
				keys := cowSourcePressureKeys(t, db)
				if state != "empty" {
					seed := db.NewBatchWithSize(2)
					for _, key := range keys {
						if err := seed.Set(key, []byte("seed")); err != nil {
							t.Fatal(err)
						}
					}
					if err := seed.Write(); err != nil {
						t.Fatal(err)
					}
					_ = seed.Close()
					if state == "frozen" {
						if err := db.rolloverCOWForFlush(); err != nil {
							t.Fatal(err)
						}
					}
				}
				groups := make([][]memtable.COWMutation, 2)
				for i, key := range keys {
					groups[i] = []memtable.COWMutation{{Key: key, Value: []byte("next"), Flags: node.FlagInline}}
				}
				c := db.cow
				c.writerMu.Lock()
				needs := c.successors[0].writer == nil || c.successors[1].writer == nil
				if needs != (state != "parked") || (len(c.cut.frozen) > 0) != (state == "frozen") {
					c.writerMu.Unlock()
					t.Fatal("wrong actual cut basis")
				}
				before := c.budget.Stats()
				want := cowPreparedCutCharge(2, len(c.cut.frozen), needs) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
				p, err := c.prepareNative(groups, make([]memtable.COWPrepareOptions, 2), nil)
				if err != nil {
					c.writerMu.Unlock()
					t.Fatal(err)
				}
				for _, staged := range p.prepared {
					if staged != nil {
						want += staged.Charge().Total()
					}
				}
				after := c.budget.Stats()
				p.cancel()
				c.writerMu.Unlock()
				if err := p.drainCancelled(); err != nil {
					t.Fatal(err)
				}
				if after.TotalBytes-before.TotalBytes != want || after.ReservedBytes-before.ReservedBytes != want {
					t.Fatalf("cut helper disagrees with real parent owners: before=%+v after=%+v want=%d", before, after, want)
				}
			})
		}
	}
}
