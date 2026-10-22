package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

type cowPressureRolloverFixture struct {
	db                              *DB
	backend                         *backenddb.DB
	batch                           *Batch
	old                             *Snapshot
	filler                          *memtable.COWExternalLease
	fillerBytes                     uint64
	locked                          bool
	keys                            [][]byte
	oldValue, value                 []byte
	groups                          [][]memtable.COWMutation
	current, fresh, extra, headroom uint64
}

func (f *cowPressureRolloverFixture) releasePressure() {
	if f.filler != nil {
		f.filler.Close()
		f.filler = nil
	}
	if f.locked {
		f.db.flushMu.Unlock()
		f.locked = false
	}
}

// Only real staged allocations and an explicitly retained external owner create
// pressure. The limits, current history and accounting are never rewritten.
func newCOWPressureRolloverFixture(t *testing.T, mode string, wal bool, dirtyShards int) *cowPressureRolloverFixture {
	t.Helper()
	dir := t.TempDir()
	durability := backenddb.DurabilityWALOffRelaxed
	if wal {
		durability = backenddb.DurabilityWALOnRelaxed
	}
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend"), CommandWAL: wal, Durability: durability, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	limits := memtable.DefaultCOWLimits()
	db, err := Open(dir, backend, Options{TransactionOwner: owner, MemtableMode: mode, DisableWAL: !wal, AllowUnsafe: true, ExternalCommandWAL: wal, MemtableShards: 4, COWMemtableLimits: limits, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1 << 30, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		t.Fatal(err)
	}
	f := &cowPressureRolloverFixture{db: db, backend: backend, oldValue: bytes.Repeat([]byte("s"), 128), value: bytes.Repeat([]byte("t"), 128)}
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
	})
	db.flushMu.Lock()
	f.locked = true
	seed := db.NewBatchWithSize(4096)
	counts := [4]int{}
	seedKeys := make([][]byte, 4)
	for candidate, n := uint64(0), 0; candidate < 100000 && n < 1024*dirtyShards; candidate++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, candidate)
		shard := db.shardIndex(key)
		if shard >= dirtyShards || counts[shard] == 1024 {
			continue
		}
		if err := seed.Set(key, f.oldValue); err != nil {
			t.Fatal(err)
		}
		if seedKeys[shard] == nil {
			seedKeys[shard] = key
		}
		counts[shard]++
		n++
	}
	for i, count := range counts {
		want := 0
		if i < dirtyShards {
			want = 1024
		}
		if count != want {
			t.Fatal("seed shard distribution", counts)
		}
	}
	if err := f.write(seed, wal, nil); err != nil {
		t.Fatal("seed", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	f.old, err = db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	b := db.NewBatchWithSize(64)
	f.batch = b
	f.groups = make([][]memtable.COWMutation, 4)
	add := func(key []byte) {
		if err := b.Set(key, f.value); err != nil {
			t.Fatal(err)
		}
		i := db.shardIndex(key)
		f.groups[i] = append(f.groups[i], memtable.COWMutation{Key: key, Value: f.value, Flags: node.FlagInline})
		f.keys = append(f.keys, key)
	}
	for _, key := range seedKeys {
		if key != nil {
			add(key)
		}
	}
	for candidate := uint64(1000000); candidate < 1100000 && len(f.keys) < 64; candidate++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, candidate)
		if len(f.groups[db.shardIndex(key)]) < 16 {
			add(key)
		}
	}
	if len(f.keys) != 64 {
		t.Fatal("target shard distribution")
	}
	if wal {
		if err := b.AuthorizeCOWAppend(owner); err != nil {
			t.Fatal(err)
		}
	}
	beforeScratch := b.cowState.storageBytes
	if err := b.cowPrepareWriteStorage(); err != nil {
		t.Fatal(err)
	}
	writeScratch := b.cowState.storageBytes - beforeScratch // Write stages another owned backing.
	db.cow.writerMu.Lock()
	for i, group := range f.groups {
		if i < dirtyShards {
			if db.cow.cut.shards[i].Len() == 0 || db.cow.successors[i].writer == nil {
				t.Fatal("missing real dirty/promised basis", i)
			}
		} else if db.cow.cut.shards[i].Len() != 0 || db.cow.successors[i].writer != nil {
			t.Fatal("untouched shard has a fabricated promise", i)
		}
		current, err := db.cow.writers[i].Estimate(group, memtable.COWPrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		freshWriter := db.cow.successors[i].writer
		if freshWriter == nil {
			freshWriter = db.cow.writers[i]
		}
		fresh, err := freshWriter.Estimate(group, memtable.COWPrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		trigger := limits.MaxGenerationBytes - limits.MaxGenerationBytes/4
		used := db.cow.cut.shards[i].history
		if used > trigger || current.History() > trigger-used {
			t.Fatal("fixture requires existing generation rollover")
		}
		f.current += current.Total()
		f.fresh += fresh.Total()
	}
	if b.cowPredictionCanSkipEstimate(f.groups) {
		t.Fatal("raw input used typed shortcut")
	}
	db.cow.writerMu.Unlock()
	if db.mutableBytes.Load() > db.mutableFlushThreshold() {
		t.Fatal("fixture requires mutable rollover")
	}
	// Parent allocations still ahead: repeated write scratch; inline preparation
	// scratch; new cut/prepared/retired/successor vectors; journal key arena. The
	// generation constructors and rollover use the already reserved child lane.
	allocation := memtable.COWAllocationCharge
	leaseOverhead := allocation(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	preparation := allocation(uint64(unsafe.Sizeof(cowBatchPreparation{}))) + allocation(4*uint64(unsafe.Sizeof(memtable.COWPrepareOptions{})))
	cut := cowCutCharge(4, 4) + allocation(uint64(unsafe.Sizeof(cowPreparedCut{}))) + allocation(4*uint64(unsafe.Sizeof((*memtable.COWPrepared)(nil)))) + allocation(4*uint64(unsafe.Sizeof(memtable.COWRetirement{}))) + allocation(4*uint64(unsafe.Sizeof(cowRawSuccessor{})))
	var keyBytes uint64
	for _, key := range f.keys {
		keyBytes += uint64(len(key))
	}
	f.extra = writeScratch + preparation + cut + 2*leaseOverhead + allocation(keyBytes) + leaseOverhead
	f.headroom = f.fresh + (f.current-f.fresh)/2
	if f.fresh >= f.current || f.fresh+f.extra >= f.headroom {
		t.Fatalf("fixture lacks proved fresh headroom: current=%d fresh=%d extra=%d H=%d", f.current, f.fresh, f.extra, f.headroom)
	}
	stats := db.COWMemoryStats()
	if stats.ReservedBytes+leaseOverhead+f.headroom >= limits.MaxInFlightBytes {
		t.Fatalf("fixture saturation unavailable: %+v", stats)
	}
	f.fillerBytes = limits.MaxInFlightBytes - stats.ReservedBytes - leaseOverhead - f.headroom
	f.filler, err = db.cow.budget.AcquireExternal(f.fillerBytes)
	if err != nil {
		t.Fatal(err)
	}
	stats = db.COWMemoryStats()
	if stats.ReservedBytes+f.headroom != limits.MaxInFlightBytes || stats.TotalBytes+f.current >= limits.MaxTotalBytes || stats.HistoryBytes+stats.ReservedBytes+stats.DeferredBytes+f.current >= limits.MaxRetiredBytes || stats.Sources+4 > limits.MaxSources || stats.Generations+4 > limits.MaxGenerations {
		t.Fatalf("not isolated in-flight pressure: stats=%+v current=%d fresh=%d extra=%d H=%d", stats, f.current, f.fresh, f.extra, f.headroom)
	}
	t.Logf("actual default-limit fixture: current=%d fresh=%d extra=%d H=%d stats=%+v", f.current, f.fresh, f.extra, f.headroom, stats)
	return f
}

type cowPressureCalls struct{ append, prepare int }

// This is the same real owner/guard/prepared append path as prefix-reuse tests.
// count observes the callback before revision assignment; no append is stubbed.
func (f *cowPressureRolloverFixture) write(b *Batch, wal bool, count *cowPressureCalls) error {
	if !wal {
		return b.Write()
	}
	owner := f.db.cow.txnOwner
	if err := b.AuthorizeCOWAppend(owner); err != nil {
		return err
	}
	err := b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error {
		if count != nil {
			count.append++
		}
		if err := b.AcquireCOWAppendGuard(owner); err != nil {
			return err
		}
		prepare := func() error {
			if count != nil {
				count.prepare++
			}
			b.AssignExternalCommandWALPointRevisions()
			return b.PrepareExternalCommandWALPublication()
		}
		lsn, err := f.backend.AppendPreparedRawKVUnderGuard(b.COWAppendGuard(), prepare, b.ExternalCommandWALFinalizer(), b.Replay, len(b.entries), backenddb.RawKVCommandWALAppendRelaxed)
		receiptErr := b.RecordCOWAppend(owner, lsn, err, false)
		if err != nil {
			return err
		}
		return receiptErr
	})
	b.CompleteCOWAcknowledgement(owner, false, err)
	return err
}

func TestCOWInFlightPressureChoosesPromisedDirtyRollover(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			f := newCOWPressureRolloverFixture(t, mode, false, 4)
			if err := f.db.beginDirectWrite(); err != nil {
				t.Fatal(err)
			}
			f.db.cow.writerMu.Lock()
			rollover, pressure, pressureRollover, err := f.batch.cowGenerationRollover(true)
			if err == nil && rollover != nil {
				rollover.install()
				f.db.mutableBytes.Store(0)
			}
			f.db.cow.writerMu.Unlock()
			f.db.writeMu.RUnlock()
			if rollover != nil {
				if cleanupErr := rollover.drain(); cleanupErr != nil {
					t.Fatal(cleanupErr)
				}
			}
			if err != nil || rollover == nil || pressure || !pressureRollover {
				t.Fatalf("wanted real pre-producer rollover, got rollover=%t checkpoint=%t err=%v", rollover != nil, pressure, err)
			}
		})
	}
}

func TestCOWInFlightPressureRollsBeforeSinglePublication(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		for _, wal := range []bool{false, true} {
			for _, dirtyShards := range []int{3, 4} {
				t.Run(fmt.Sprintf("%s/wal=%t/dirty=%d", mode, wal, dirtyShards), func(t *testing.T) {
					f := newCOWPressureRolloverFixture(t, mode, wal, dirtyShards)
					c := f.db.cow
					seq, lsn, runs, rollovers, publishes, prepares, handoffs := c.cut.logicalSeq, f.backend.CommandWALNextLSN(), f.db.checkpointRuns.Load(), c.rollovers.Load(), c.publications.Load(), c.prepareCalls.Load(), c.handoffs.Load()
					calls := cowPressureCalls{}
					done := make(chan error, 1)
					go func() { done <- f.write(f.batch, wal, &calls) }()
					select {
					case err := <-done:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(5 * time.Second):
						f.releasePressure()
						err := <-done
						t.Fatalf("write requested fenced checkpoint instead of promised rollover: err=%v", err)
					}
					receipt := f.batch.COWReceipt()
					if f.db.checkpointRuns.Load() != runs || c.rollovers.Load() != rollovers+1 || c.publications.Load() != publishes+1 || c.prepareCalls.Load() != prepares+1 || c.handoffs.Load() != handoffs || c.cut.logicalSeq != seq+1 || !receipt.LogicalInstalled {
						t.Fatalf("not one rollover/publication without checkpoint: receipt=%+v checkpoint=%d rollover=%d publication=%d prepare=%d handoff=%d", receipt, f.db.checkpointRuns.Load()-runs, c.rollovers.Load()-rollovers, c.publications.Load()-publishes, c.prepareCalls.Load()-prepares, c.handoffs.Load()-handoffs)
					}
					if wal {
						if calls.append != 1 || calls.prepare != 1 || f.backend.CommandWALNextLSN() != lsn+1 || !receipt.Assigned {
							t.Fatalf("WAL not assigned exactly once: calls=%+v lsn=%d before=%d receipt=%+v", calls, f.backend.CommandWALNextLSN(), lsn, receipt)
						}
					} else if calls.append != 0 || calls.prepare != 0 || f.backend.CommandWALNextLSN() != lsn || receipt.Assigned {
						t.Fatal("NoWAL invented an assignment")
					}
					for i, key := range f.keys {
						if got, err := f.db.Get(key); err != nil || !bytes.Equal(got, f.value) {
							t.Fatalf("latest %x: %q %v", key, got, err)
						}
						got, err := f.old.Get(key)
						if (i < dirtyShards && (err != nil || !bytes.Equal(got, f.oldValue))) || (i >= dirtyShards && (!errors.Is(err, tree.ErrKeyNotFound) || got != nil)) {
							t.Fatalf("old cut %x: %q %v", key, got, err)
						}
					}
					if c.budget.Limits() != memtable.DefaultCOWLimits() {
						t.Fatal("limits changed")
					}
					f.releasePressure()
					if err := f.old.Close(); err != nil {
						t.Fatal(err)
					}
					f.old = nil
					if err := f.batch.Close(); err != nil {
						t.Fatal(err)
					}
					f.batch = nil
					if err := f.db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					if err := f.db.Close(); err != nil {
						t.Fatal(err)
					}
					if stats := f.db.COWMemoryStats(); stats.TotalBytes != 0 {
						t.Fatalf("parent retained after Close: %+v", stats)
					}
				})
			}
		}
	}
}

// Re-entry after one optional transition cannot ask for another, even if the
// actual current basis remains useful. No current metadata is forged.
func TestCOWInFlightPressureRolloverAttemptIsFinite(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			f := newCOWPressureRolloverFixture(t, mode, false, 4)
			c := f.db.cow
			seq, lsn, rotations := c.cut.logicalSeq, f.backend.CommandWALNextLSN(), c.rollovers.Load()
			c.writerMu.Lock()
			r, pressure, attempt, err := f.batch.cowGenerationRollover(false)
			c.writerMu.Unlock()
			if err != nil || r != nil || !pressure || attempt {
				t.Fatalf("spent token reattempted rollover: rollover=%t pressure=%t attempt=%t err=%v", r != nil, pressure, attempt, err)
			}
			if c.cut.logicalSeq != seq || f.backend.CommandWALNextLSN() != lsn || c.rollovers.Load() != rotations {
				t.Fatal("prediction assigned or installed work")
			}
		})
	}
}

// Another writer can enter while the original command drains its first real
// rollover. A new dirty/promised basis does not replenish the original token.
func TestCOWInFlightPressureRolloverTokenSurvivesInterference(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			f := newCOWPressureRolloverFixture(t, mode, false, 4)
			c := f.db.cow
			if err := f.db.beginDirectWrite(); err != nil {
				t.Fatal(err)
			}
			c.writerMu.Lock()
			r, pressure, consumed, err := f.batch.cowGenerationRollover(true)
			if err == nil && r != nil {
				r.install()
				f.db.mutableBytes.Store(0)
			}
			c.writerMu.Unlock()
			f.db.writeMu.RUnlock()
			if r != nil {
				if cleanupErr := r.drain(); cleanupErr != nil {
					t.Fatal(cleanupErr)
				}
			}
			if err != nil || r == nil || pressure || !consumed {
				t.Fatalf("first attempt did not consume a real rollover: r=%t pressure=%t consumed=%t err=%v", r != nil, pressure, consumed, err)
			}
			other := f.db.NewBatchWithSize(4)
			defer other.Close()
			for _, key := range f.keys[:4] { // One actual interfering write per shard.
				if err := other.Set(key, f.oldValue); err != nil {
					t.Fatal(err)
				}
			}
			if err := other.Write(); err != nil {
				t.Fatal("interfering writer", err)
			}
			if err := other.Close(); err != nil {
				t.Fatal(err)
			}
			var estimate uint64
			c.writerMu.Lock()
			for i, group := range f.groups {
				if c.cut.shards[i].Len() == 0 || c.successors[i].writer == nil {
					t.Fatal("interference did not create a real eligible basis", i)
				}
				charge, err := c.writers[i].Estimate(group, memtable.COWPrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				estimate += charge.Total()
			}
			c.writerMu.Unlock()
			stats := f.db.COWMemoryStats()
			remaining := c.budget.Limits().MaxInFlightBytes - stats.ReservedBytes
			wantHeadroom := estimate / 2
			if wantHeadroom >= remaining {
				t.Fatal("interference fixture cannot tighten real reservation")
			}
			if err := f.filler.Resize(f.fillerBytes + remaining - wantHeadroom); err != nil {
				t.Fatal(err)
			}
			seq, lsn, rotations, checkpoints := c.cut.logicalSeq, f.backend.CommandWALNextLSN(), c.rollovers.Load(), f.db.checkpointRuns.Load()
			c.writerMu.Lock()
			r, pressure, attempt, err := f.batch.cowGenerationRollover(!consumed)
			c.writerMu.Unlock()
			if err != nil || r != nil || !pressure || attempt {
				t.Fatalf("interference replenished token: r=%t pressure=%t attempt=%t err=%v", r != nil, pressure, attempt, err)
			}
			if c.cut.logicalSeq != seq || f.backend.CommandWALNextLSN() != lsn || c.rollovers.Load() != rotations || f.db.checkpointRuns.Load() != checkpoints || f.batch.COWReceipt().Assigned || f.batch.COWReceipt().LogicalInstalled {
				t.Fatal("re-entry assigned, published or repeated work")
			}
		})
	}
}
