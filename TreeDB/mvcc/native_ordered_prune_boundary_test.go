package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// The probe decorates the public boundaries rather than bypassing native
// preparation or injecting a second publication owner.
type nativePruneBoundaryDB struct {
	*treedb.DB
	t                             *testing.T
	begins, completes, attempts   int
	relaxedAttempts, syncAttempts int
	failAt                        int
	acceptFailure                 bool
	writeErr, prepareErr, endErr  error
	onCapture                     func() error
	iteratorClosed, cutClosed     bool
}

func (p *nativePruneBoundaryDB) PrepareMVCCPrune() (func() error, error) {
	p.begins++
	if p.prepareErr != nil {
		return nil, p.prepareErr
	}
	complete, err := p.DB.PrepareMVCCPrune()
	if err != nil || complete == nil {
		return complete, err
	}
	return func() error {
		p.completes++
		if !p.iteratorClosed || !p.cutClosed {
			p.t.Error("prune boundary completed before iterator/cut cleanup")
		}
		return errors.Join(complete(), p.endErr)
	}, nil
}

func (p *nativePruneBoundaryDB) AcquireMVCCReadCut() (treedb.MVCCReadSnapshot, error) {
	cut, err := p.DB.AcquireMVCCReadCut()
	if err != nil {
		return nil, err
	}
	p.iteratorClosed, p.cutClosed = false, false
	return &nativePruneBoundaryCut{MVCCReadSnapshot: cut, owner: p}, nil
}

type nativePruneBoundaryCut struct {
	treedb.MVCCReadSnapshot
	owner *nativePruneBoundaryDB
}

func (c *nativePruneBoundaryCut) Close() error {
	err := c.MVCCReadSnapshot.Close()
	c.owner.cutClosed = true
	return err
}

func (c *nativePruneBoundaryCut) ReverseIterator(start, end []byte) (treedb.Iterator, error) {
	it, err := c.MVCCReadSnapshot.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	if hook := c.owner.onCapture; hook != nil {
		c.owner.onCapture = nil
		if err := hook(); err != nil {
			return nil, errors.Join(err, it.Close())
		}
	}
	return &nativePruneBoundaryIterator{Iterator: it, owner: c.owner}, nil
}

type nativePruneBoundaryIterator struct {
	treedb.Iterator
	owner *nativePruneBoundaryDB
}

func (it *nativePruneBoundaryIterator) Close() error {
	err := it.Iterator.Close()
	it.owner.iteratorClosed = true
	return err
}

func (p *nativePruneBoundaryDB) NewBatchWithSize(size int) treedb.Batch {
	return &nativePruneBoundaryBatch{Batch: p.DB.NewBatchWithSize(size), owner: p}
}

type nativePruneBoundaryBatch struct {
	treedb.Batch
	owner   *nativePruneBoundaryDB
	deletes bool
}

func (b *nativePruneBoundaryBatch) Delete(key []byte) error {
	b.deletes = true
	return b.Batch.Delete(key)
}

func (b *nativePruneBoundaryBatch) write(sync bool) error {
	write := b.Batch.Write
	if sync {
		write = b.Batch.WriteSync
	}
	if b.deletes {
		b.owner.attempts++
		if sync {
			b.owner.syncAttempts++
		} else {
			b.owner.relaxedAttempts++
		}
		if b.owner.failAt == b.owner.attempts {
			if b.owner.acceptFailure {
				return errors.Join(write(), b.owner.writeErr)
			}
			return b.owner.writeErr
		}
	}
	return write()
}

func (b *nativePruneBoundaryBatch) Write() error     { return b.write(false) }
func (b *nativePruneBoundaryBatch) WriteSync() error { return b.write(true) }

func nativePruneBoundaryRequireDrained(t *testing.T, db *treedb.DB) {
	t.Helper()
	stats := db.Stats()
	for _, name := range []string{"treedb.cache.mutable_bytes", "treedb.cache.cow.frozen_roots", "treedb.cache.cow.ordered.physical_versions"} {
		if stats[name] != "0" {
			t.Fatalf("completed prune retained active overlay work: %s=%q", name, stats[name])
		}
	}
}

func TestNativeOrderedPruneBoundaryDrainsWholeHistoryPreservesCutAndTail(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileNoWALFast, treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		for _, mode := range []CommitMode{CommitRelaxed, CommitDurable} {
			t.Run(fmt.Sprintf("%s/mode%d", profile, mode), func(t *testing.T) {
				dir := t.TempDir()
				db := nativeOrderedPruneOpen(t, profile, dir)
				defer func() {
					if db != nil {
						_ = db.Close()
					}
				}()
				store := New(db)
				const floor = 97
				groups := make([]CommitGroup, floor)
				for i := range groups {
					ts := uint64(i + 1)
					groups[i] = CommitGroup{Timestamp: ts, Mutations: []Mutation{
						{Key: []byte("dead"), Value: []byte{byte(ts)}, Delete: ts == floor},
						{Key: []byte("live"), Value: []byte{byte(ts)}},
					}}
				}
				if err := store.CommitGroupAt(groups, CommitRelaxed); err != nil {
					t.Fatal(err)
				}
				old, err := store.IterateVersions(VersionIteratorOptions{ReadTimestamp: 32})
				if err != nil {
					t.Fatal(err)
				}
				defer old.Close()
				if err := store.AdvanceDiscardFloor(floor, mode); err != nil {
					t.Fatal(err)
				}
				probe := &nativePruneBoundaryDB{DB: db, t: t}
				// This admitted tail arrives after the prune cut. It must survive
				// the exact source-prefix handoff along with the selected anchor.
				tailKey := []byte{0xff, 0}
				probe.onCapture = func() error {
					return store.CommitAt(floor+1, []Mutation{{Key: []byte("live"), Value: []byte("tail")}, {Key: tailKey, Value: []byte("binary")}}, mode)
				}
				stats, err := newStore(probe).PruneVersions(PruneOptions{BatchSize: 17, Mode: mode})
				if err != nil || stats.Pruned != 2*floor-1 || probe.begins != 1 || probe.completes != 1 {
					t.Fatalf("prune=%+v begins=%d completes=%d err=%v", stats, probe.begins, probe.completes, err)
				}
				if (mode == CommitDurable && probe.syncAttempts != probe.attempts) || (mode == CommitRelaxed && probe.relaxedAttempts != probe.attempts) {
					t.Fatalf("delete acknowledgement mode changed: mode=%d relaxed=%d sync=%d", mode, probe.relaxedAttempts, probe.syncAttempts)
				}
				nativePruneBoundaryRequireDrained(t, db)
				requireResult(t, store, []byte("dead"), floor+1, Absent, 0, nil)
				requireResult(t, store, []byte("live"), floor+1, Present, floor+1, []byte("tail"))
				requireResult(t, store, tailKey, floor+1, Present, floor+1, []byte("binary"))
				versions := collectVersions(t, old)
				if len(versions) != 64 {
					t.Fatalf("old cut retained %d versions, want64", len(versions))
				}
				for _, v := range versions {
					if v.State != Present || v.Timestamp > 32 || !bytes.Equal(v.Value, []byte{byte(v.Timestamp)}) {
						t.Fatalf("old cut changed: %+v", v)
					}
				}
				if _, err := store.PruneVersions(PruneOptions{Mode: mode}); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db = nil
				db = nativeOrderedPruneOpen(t, profile, dir)
				store = New(db)
				requireResult(t, store, []byte("dead"), floor+1, Absent, 0, nil)
				requireResult(t, store, []byte("live"), floor+1, Present, floor+1, []byte("tail"))
				requireResult(t, store, tailKey, floor+1, Present, floor+1, []byte("binary"))
			})
		}
	}
}

func TestNativeOrderedPruneBoundaryCompletesInterruptedAcceptedPrefix(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("ambiguous%t", accepted), func(t *testing.T) {
			dir := t.TempDir()
			db := nativeOrderedPruneOpen(t, treedb.ProfileCommandWALDurable, dir)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			store := New(db)
			groups := make([]CommitGroup, 66)
			for i := range groups {
				ts := uint64(i + 1)
				groups[i] = CommitGroup{Timestamp: ts, Mutations: []Mutation{{Key: []byte("k"), Value: []byte{byte(ts)}, Delete: ts == 65}}}
			}
			if err := store.CommitGroupAt(groups, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if err := store.AdvanceDiscardFloor(65, CommitDurable); err != nil {
				t.Fatal(err)
			}
			writeErr, endErr := errors.New("prune write interruption"), errors.New("prune completion reporting failure")
			probe := &nativePruneBoundaryDB{DB: db, t: t, failAt: 3, acceptFailure: accepted, writeErr: writeErr, endErr: endErr}
			stats, err := newStore(probe).PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable})
			if !errors.Is(err, writeErr) || !errors.Is(err, endErr) || stats.Pruned != 2 || probe.completes != 1 {
				t.Fatalf("partial prune=%+v completion=%d error=%v", stats, probe.completes, err)
			}
			nativePruneBoundaryRequireDrained(t, db)
			requireResult(t, store, []byte("k"), 66, Present, 66, []byte{66})
			remaining := nativePruneCollect(t, store, VersionIteratorOptions{})
			wantCount := 64
			if accepted {
				wantCount--
			}
			if len(remaining) != wantCount {
				t.Fatalf("accepted prefix remaining=%d want%d", len(remaining), wantCount)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			db = nativeOrderedPruneOpen(t, treedb.ProfileCommandWALDurable, dir)
			store = New(db)
			if _, err := store.PruneVersions(PruneOptions{BatchSize: 13, Mode: CommitDurable}); err != nil {
				t.Fatal(err)
			}
			nativePruneBoundaryRequireDrained(t, db)
			nativePruneRequireVersions(t, nativePruneCollect(t, store, VersionIteratorOptions{}), []Version{{Key: []byte("k"), Timestamp: 66, State: Present, Value: []byte{66}}})
		})
	}
}

func TestNativeOrderedPruneBoundaryNoDeleteDoesNotComplete(t *testing.T) {
	db := nativeOrderedPruneOpen(t, treedb.ProfileNoWALFast, t.TempDir())
	defer db.Close()
	store := New(db)
	if err := store.CommitAt(10, []Mutation{{Key: []byte("k"), Value: []byte("anchor")}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceDiscardFloor(10, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	probe := &nativePruneBoundaryDB{DB: db, t: t}
	stats, err := newStore(probe).PruneVersions(PruneOptions{Mode: CommitRelaxed})
	if err != nil || stats.Pruned != 0 || probe.begins != 1 || probe.completes != 0 || probe.attempts != 0 {
		t.Fatalf("no-op prune=%+v begin=%d complete=%d attempts=%d error=%v", stats, probe.begins, probe.completes, probe.attempts, err)
	}
	probe.prepareErr = errors.New("prepare prune refusal")
	if _, err := newStore(probe).PruneVersions(PruneOptions{Mode: CommitRelaxed}); !errors.Is(err, probe.prepareErr) || probe.attempts != 0 {
		t.Fatalf("preparation refusal performed deletes: attempts=%d error=%v", probe.attempts, err)
	}
}

func TestNativeOrderedPruneBoundaryUnsupportedModeNoCheckpoint(t *testing.T) {
	opts := managedHistoryOptions(treedb.ProfileCommandWALRelaxed, t.TempDir(), false)
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	if err := store.CommitAt(10, []Mutation{{Key: []byte("k"), Value: []byte("value")}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	before := db.Stats()
	complete, err := db.PrepareMVCCPrune()
	if err != nil || complete != nil {
		t.Fatalf("legacy boundary=%v error=%v", complete != nil, err)
	}
	after := db.Stats()
	for _, name := range []string{"treedb.cache.mutable_bytes", "treedb.cache.cow.rollovers_total", "treedb.cache.cow.handoffs_total"} {
		if before[name] != after[name] {
			t.Fatalf("unsupported mode checkpointed: %s changed %q→%q", name, before[name], after[name])
		}
	}
}
