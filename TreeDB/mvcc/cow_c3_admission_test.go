package mvcc

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func c3ReadDB(t testing.TB, profile treedb.Profile, pointers bool) *treedb.DB {
	t.Helper()
	opts := treedb.OptionsFor(profile, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 4
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.ForcePointers = pointers
	opts.ValueLog.PointerThreshold = 1 << 30
	if pointers {
		opts.ValueLog.PointerThreshold = 1
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

// Use the accepted harness for every goroutine-owning case: release, bounded
// common-deadline joins, then Close and owned-directory removal.
func c3ReadHarness(t *testing.T, pointers bool) *c3COWHarness {
	t.Helper()
	dir, err := os.MkdirTemp("", "gomap-c3-read-admission-")
	if err != nil {
		t.Fatal(err)
	}
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 4
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.ForcePointers = pointers
	opts.ValueLog.PointerThreshold = 1 << 30
	if pointers {
		opts.ValueLog.PointerThreshold = 1
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v; retain %s", err, dir)
	}
	h := &c3COWHarness{t: t, db: db, dir: dir}
	t.Cleanup(h.cleanup)
	return h
}

type c3CutDB struct {
	*treedb.DB
	enter, release                     chan struct{}
	iterator                           bool
	captureErr, constructErr, closeErr error
	captures, closes                   int
	mu                                 sync.Mutex
}

func (db *c3CutDB) AcquireMVCCReadCut() (treedb.MVCCReadSnapshot, error) {
	db.mu.Lock()
	db.captures++
	db.mu.Unlock()
	if db.captureErr != nil {
		return nil, db.captureErr
	}
	snap, err := db.DB.AcquireMVCCReadCut()
	if err != nil {
		return nil, err
	}
	return &c3Cut{MVCCReadSnapshot: snap, owner: db}, nil
}

type c3Cut struct {
	treedb.MVCCReadSnapshot
	owner *c3CutDB
}

func (c *c3Cut) pause() {
	if c.owner.enter != nil {
		close(c.owner.enter)
		<-c.owner.release
	}
}
func (c *c3Cut) SeekGEVersionRange(a, b []byte) ([]byte, []byte, bool, error) {
	c.pause()
	return c.MVCCReadSnapshot.SeekGEVersionRange(a, b)
}
func (c *c3Cut) Iterator(a, b []byte) (treedb.Iterator, error) {
	if c.owner.iterator {
		c.pause()
	}
	if c.owner.constructErr != nil {
		return nil, c.owner.constructErr
	}
	return c.MVCCReadSnapshot.Iterator(a, b)
}
func (c *c3Cut) Close() error {
	c.owner.mu.Lock()
	c.owner.closes++
	c.owner.mu.Unlock()
	return errors.Join(c.MVCCReadSnapshot.Close(), c.owner.closeErr)
}

func TestC3CapturedCutMaterializesOutsideFloorAdmission(t *testing.T) {
	for _, pointers := range []bool{false, true} {
		for _, history := range []bool{false, true} {
			name := "point"
			if history {
				name = "versions"
			}
			if pointers {
				name += "/pointer"
			}
			t.Run(name, func(t *testing.T) {
				h := c3ReadHarness(t, pointers)
				db := h.db
				paused := &c3CutDB{DB: db, enter: make(chan struct{}), release: make(chan struct{}), iterator: history}
				var once sync.Once
				release := func() { once.Do(func() { close(paused.release) }) }
				h.release = release
				store := newStore(paused)
				key := []byte("a")
				old := []byte("old")
				if err := store.CommitAt(10, []Mutation{{Key: key, Value: old}}, CommitRelaxed); err != nil {
					t.Fatal(err)
				}
				read := h.start("captured materialization", func() c3COWOutcome {
					if history {
						v, e := c3COWReadVersions(store, key)
						return c3COWOutcome{versions: v, err: e}
					} else {
						r, e := store.GetAt(key, 100)
						return c3COWOutcome{point: r, err: e}
					}
				})
				select {
				case <-paused.enter:
				case <-time.After(c3COWCallBound):
					t.Fatal("cut materialization not reached")
				}
				writes := h.start("same-ts/history/floor/maintenance", func() c3COWOutcome {
					// Replace the same timestamp and insert late historical data after capture.
					err := store.CommitGroupAt([]CommitGroup{{Timestamp: 10, Mutations: []Mutation{{Key: key, Value: []byte("replacement")}}}, {Timestamp: 5, Mutations: []Mutation{{Key: key, Value: []byte("late")}}}, {Timestamp: 20, Mutations: []Mutation{{Key: key, Delete: true}}}, {Timestamp: 30, Mutations: []Mutation{{Key: []byte("ab"), Value: []byte{}}}}}, CommitRelaxed)
					if err == nil {
						err = store.AdvanceDiscardFloor(50, CommitRelaxed)
					}
					if err == nil && pointers {
						err = db.Checkpoint()
					}
					if err == nil && pointers {
						_, err = db.ValueLogRewriteOnline(context.Background(), treedb.ValueLogRewriteOnlineOptions{BatchSize: 1, SyncEachBatch: true})
					}
					if err == nil && pointers {
						_, err = db.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{})
					}
					return c3COWOutcome{err: err}
				})
				if !writes.join(c3COWCallBound) {
					t.Fatal("floor/maintenance retained materialization admission")
				}
				h.require(writes)
				release()
				out := h.require(read)
				if history {
					if err := c3COWCheckVersions(out.versions, key, old, nil, false); err != nil {
						t.Fatal(err)
					}
				} else if err := c3COWCheckPoint(out.point, 10, old); err != nil {
					t.Fatal(err)
				}
				if paused.captures != 1 || paused.closes != 1 {
					t.Fatalf("capture/close = %d/%d", paused.captures, paused.closes)
				}
				// Independent calls remain fresh despite the earlier pinned physical cut.
				paused.enter = nil
				fresh, err := store.GetAt(key, 100)
				if err != nil || fresh.State != Tombstone || fresh.Timestamp != 20 {
					t.Fatalf("fresh=%+v err=%v", fresh, err)
				}
				if _, err := store.GetAt(key, 50); !errors.Is(err, ErrReadBeforeDiscardFloor) {
					t.Fatalf("floor equality: %v", err)
				}
			})
		}
	}
}

func TestC3ReadCutFailureAndClose(t *testing.T) {
	injected := errors.New("injected cut failure")
	for _, stage := range []string{"capture", "point_close", "iterator_build", "iterator_close"} {
		t.Run(stage, func(t *testing.T) {
			db := c3ReadDB(t, treedb.ProfileNoWALFast, true)
			adapter := &c3CutDB{DB: db}
			store := newStore(adapter)
			if err := store.CommitAt(10, []Mutation{{Key: []byte("a"), Value: []byte("v")}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			switch stage {
			case "capture":
				adapter.captureErr = injected
			case "point_close", "iterator_close":
				adapter.closeErr = injected
			case "iterator_build":
				adapter.constructErr = injected
			}
			var err error
			if stage == "iterator_build" || stage == "iterator_close" {
				var it *VersionIterator
				it, err = store.IterateVersions(VersionIteratorOptions{ExactKey: []byte("a")})
				if err == nil {
					err = it.Close()
				}
			} else {
				_, err = store.GetAt([]byte("a"), 100)
			}
			if !errors.Is(err, injected) {
				t.Fatalf("lost error: %v", err)
			}
			want := 1
			if stage == "capture" {
				want = 0
			}
			if adapter.closes != want {
				t.Fatalf("closed %d want %d", adapter.closes, want)
			}
			if err := store.AdvanceDiscardFloor(50, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if stats := db.Stats(); stats["treedb.cache.cow.views"] != "0" {
				t.Fatalf("leaked views: %s", stats["treedb.cache.cow.views"])
			}
		})
	}
}

func TestC3COWPruneRefusesBeforeFloorWALEffects(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			db := c3ReadDB(t, profile, false)
			store := New(db)
			if err := store.CommitAt(10, []Mutation{{Key: []byte("a"), Value: []byte("v")}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if err := store.AdvanceDiscardFloor(20, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			before := db.Stats()
			if _, err := store.PruneVersions(PruneOptions{Mode: CommitDurable}); !errors.Is(err, caching.ErrCOWUnsupported) {
				t.Fatalf("prune=%v", err)
			}
			after := db.Stats()
			if floor, err := store.DiscardFloor(); err != nil || floor != 20 {
				t.Fatalf("refused prune floor=%d err=%v", floor, err)
			}
			for _, stat := range []string{"treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total", "treedb.cache.cow.publications_total", "treedb.cache.cow.capture_calls_total"} {
				if before[stat] != after[stat] {
					t.Fatalf("prune changed %s: %s -> %s", stat, before[stat], after[stat])
				}
			}
			r, err := store.GetAt([]byte("a"), 100)
			if err != nil || !bytes.Equal(r.Value, []byte("v")) {
				t.Fatalf("retained=%+v %v", r, err)
			}
		})
	}
}

type c3WritePauseDB struct {
	*treedb.DB
	entered, release chan struct{}
	atomic           bool
}

func (db *c3WritePauseDB) SupportsMVCCReadCut() bool { return db.atomic && db.DB.SupportsMVCCReadCut() }
func (db *c3WritePauseDB) NewBatchWithSize(n int) treedb.Batch {
	return &c3WritePauseBatch{Batch: db.DB.NewBatchWithSize(n), owner: db}
}

type c3WritePauseBatch struct {
	treedb.Batch
	owner *c3WritePauseDB
}

func (b *c3WritePauseBatch) Write() error {
	close(b.owner.entered)
	<-b.owner.release
	return b.Batch.Write()
}
func TestC3GroupAdmissionCapabilityAndACK(t *testing.T) {
	for _, atomic := range []bool{false, true} {
		name := "legacy_fence"
		if atomic {
			name = "atomic_cut"
		}
		t.Run(name, func(t *testing.T) {
			h := c3ReadHarness(t, false)
			db := h.db
			adapter := &c3WritePauseDB{DB: db, entered: make(chan struct{}), release: make(chan struct{}), atomic: atomic}
			store := newStore(adapter)
			// Warm floor without a batch so only the tested group reaches the pause.
			if _, err := store.DiscardFloor(); err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			release := func() { once.Do(func() { close(adapter.release) }) }
			h.release = release
			done := h.start("qualified group ACK", func() c3COWOutcome {
				return c3COWOutcome{err: store.CommitGroupAt([]CommitGroup{{Timestamp: 10, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("a")}, {Key: []byte("b"), Value: []byte("b")}}}}, CommitRelaxed)}
			})
			select {
			case <-adapter.entered:
			case <-time.After(c3COWCallBound):
				t.Fatal("group ACK pause not entered")
			}
			shared := store.mu.TryRLock()
			if shared {
				store.mu.RUnlock()
			}
			if shared != atomic {
				t.Fatalf("shared admission=%t capability=%t", shared, atomic)
			}
			if store.mu.TryLock() {
				store.mu.Unlock()
				t.Fatal("active commit did not exclude floor advancement")
			}
			release()
			h.require(done)
			// Change wrapper after joined group, so floor's one-record batch is unpaused.
			store.db = db
			if err := store.AdvanceDiscardFloor(20, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitAt(20, []Mutation{{Key: []byte("late")}}, CommitRelaxed); !errors.Is(err, ErrVersionBelowDiscardFloor) {
				t.Fatalf("commit at floor=%v", err)
			}
		})
	}
}

func TestC3ReadCutCapacityRefusalHasNoFallback(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxViews = 4
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	if _, err := store.DiscardFloor(); err != nil {
		t.Fatal(err)
	}
	var cuts []treedb.MVCCReadSnapshot
	defer func() {
		for _, cut := range cuts {
			_ = cut.Close()
		}
	}()
	for len(cuts) < 8 {
		cut, e := db.AcquireMVCCReadCut()
		if e != nil {
			err = e
			break
		}
		cuts = append(cuts, cut)
	}
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("admission refusal: %v", err)
	}
	if _, err := store.GetAt([]byte("a"), 100); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("point masked admission: %v", err)
	}
	if _, err := store.IterateVersions(VersionIteratorOptions{}); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("iterator masked admission: %v", err)
	}
	for _, cut := range cuts {
		if err := cut.Close(); err != nil {
			t.Fatal(err)
		}
	}
	cuts = nil
	if _, err := store.GetAt([]byte("a"), 100); err != nil {
		t.Fatalf("read did not resume: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetAt([]byte("a"), 100); !errors.Is(err, treedb.ErrClosed) {
		t.Fatalf("closed DB read: %v", err)
	}
}
