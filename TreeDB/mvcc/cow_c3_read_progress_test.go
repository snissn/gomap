package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

const (
	c3COWCallBound     = 10 * time.Second
	c3COWProgressBound = time.Second
)

// This desired-progress regression was RED with the legacy exclusive Store
// group fence. Qualified atomic-cut producers must satisfy it without changing
// its publication window; passing does not qualify native floor/prune work.
// Only bounded completion, not universal liveness, is observed. The existing publication hook fixes the writer's position; reader
// started signals acknowledge launch, not entry into the Store's read lock.
func TestC3COWStoreReadersProgressDuringPreparedGroup(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, pointers := range []bool{false, true} {
			shape := "inline"
			if pointers {
				shape = "forced_pointer"
			}
			t.Run(string(profile)+"/"+shape, func(t *testing.T) {
				c3COWPreparedGroup(t, profile, pointers)
			})
		}
	}
}

type c3COWOutcome struct {
	point    Result
	versions []Version
	err      error
}

type c3COWCall struct {
	label   string
	started chan struct{}
	done    chan c3COWOutcome
	joined  bool
	out     c3COWOutcome
}

// Only the test goroutine mutates the harness/call bookkeeping. Worker
// goroutines own their iterators and return errors; none call testing.Fatal.
type c3COWHarness struct {
	t       *testing.T
	db      *treedb.DB
	dir     string
	calls   []*c3COWCall
	release func()
	restore func()
	cleaned bool
}

func (h *c3COWHarness) start(label string, fn func() c3COWOutcome) *c3COWCall {
	c := &c3COWCall{label: label, started: make(chan struct{}), done: make(chan c3COWOutcome, 1)}
	h.calls = append(h.calls, c)
	go func() {
		close(c.started)
		c.done <- fn()
	}()
	return c
}

func (c *c3COWCall) join(bound time.Duration) bool {
	if c.joined {
		return true
	}
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case c.out = <-c.done:
		c.joined = true
		return true
	case <-timer.C:
		return false
	}
}

func (h *c3COWHarness) require(c *c3COWCall) c3COWOutcome {
	h.t.Helper()
	if !c.join(c3COWCallBound) {
		h.t.Fatalf("%s did not join within %s; owned directory %s", c.label, c3COWCallBound, h.dir)
	}
	if c.out.err != nil {
		h.t.Fatalf("%s: %v", c.label, c.out.err)
	}
	return c.out
}

func (h *c3COWHarness) cleanup() {
	if h.cleaned {
		return
	}
	h.cleaned = true
	if h.release != nil {
		h.release()
	}
	// One common join deadline bounds failure cleanup, even if several calls
	// are stuck. Unjoined users prevent Close and directory removal.
	deadline := time.Now().Add(c3COWCallBound)
	joined := true
	for _, c := range h.calls {
		if !c.join(time.Until(deadline)) {
			joined = false
			h.t.Errorf("cleanup: %s unjoined; preserve owned directory %s", c.label, h.dir)
		}
	}
	if h.restore != nil {
		h.restore()
	}
	if !joined {
		return
	}
	closed := make(chan error, 1)
	go func() { closed <- h.db.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			h.t.Errorf("Close: %v; preserve owned directory %s", err, h.dir)
			return
		}
		if err := os.RemoveAll(h.dir); err != nil {
			h.t.Errorf("remove closed owned directory %s: %v", h.dir, err)
		} else {
			h.t.Logf("owned cleanup: all %d calls joined, observer restored, Close completed", len(h.calls))
		}
	case <-time.After(c3COWCallBound):
		h.t.Errorf("Close did not complete within %s; preserve owned directory %s", c3COWCallBound, h.dir)
	}
}

func c3COWPreparedGroup(t *testing.T, profile treedb.Profile, pointers bool) {
	t.Helper()
	// Avoid TempDir's automatic removal if a genuinely stuck call still owns
	// the DB. The harness removes this private directory only after Close.
	dir, err := os.MkdirTemp("", "gomap-c3-read-progress-")
	if err != nil {
		t.Fatal(err)
	}
	opts := treedb.OptionsFor(profile, dir)
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
		t.Fatalf("Open: %v; preserve owned directory %s", err, dir)
	}
	h := &c3COWHarness{t: t, db: db, dir: dir}
	defer h.cleanup()
	stats := db.Stats()
	t.Logf("receipt requested_profile=%s resolved_profile=%s memtable_mode=%s ordinary_ack=%s force_pointers=%t pointer_threshold=%d dir=%s", profile, stats["treedb.profile.resolved"], stats["treedb.cache.memtable_mode"], stats["treedb.profile.ordinary_ack_class"], pointers, opts.ValueLog.PointerThreshold, dir)
	if stats["treedb.profile.resolved"] != string(profile) || stats["treedb.cache.memtable_mode"] != "cow_btree" || stats["treedb.profile.ordinary_ack_class"] != profile.OrdinaryAckClass() {
		t.Fatal("resolved profile/mode/ordinary ACK does not match requested COW fixture")
	}
	store := New(db)
	keys := [][]byte{[]byte("c3-a"), []byte("c3-b")}
	old, fresh := []byte("old-value"), []byte("new-value")
	h.require(h.start("seed", func() c3COWOutcome {
		return c3COWOutcome{err: store.CommitAt(10, []Mutation{{Key: keys[0], Value: old}, {Key: keys[1], Value: old}}, CommitRelaxed)}
	}))
	h.require(h.start("warm floor", func() c3COWOutcome {
		r, err := store.GetAt(keys[0], 100)
		return c3COWOutcome{err: errors.Join(err, c3COWCheckPoint(r, 10, old))}
	}))

	// DisableSideStores keeps the main DB at dir. Public Open passes that
	// main directory to caching.Open, which stores its own dir as <dir>/wal
	// even when WAL is disabled. BeforeCOWCutSwap reports that cached root.
	cacheRoot := filepath.Join(filepath.Clean(dir), "wal")
	entered, release := make(chan durabilitycut.Event, 1), make(chan struct{})
	var once, releaseOnce sync.Once
	h.release = func() { releaseOnce.Do(func() { close(release) }) }
	h.restore = durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWCutSwap && e.Resource == durabilitycut.ResourceAuxiliary && e.Root == cacheRoot {
			once.Do(func() { entered <- e; <-release })
		}
		return nil // An accepted publication cannot be revoked by an observer.
	})
	writer := h.start("group writer", func() c3COWOutcome {
		return c3COWOutcome{err: store.CommitGroupAt([]CommitGroup{
			{Timestamp: 20, Mutations: []Mutation{{Key: keys[0], Value: fresh}}},
			{Timestamp: 20, Mutations: []Mutation{{Key: keys[1], Value: fresh}}},
		}, CommitRelaxed)}
	})
	select {
	case e := <-entered:
		t.Logf("publication hook entered point=%s resource=%s root=%s", e.Point, e.Resource, e.Root)
	case writer.out = <-writer.done:
		writer.joined = true
		t.Fatalf("group writer returned before matching BeforeCOWCutSwap root=%s: err=%v", cacheRoot, writer.out.err)
	case <-time.After(c3COWCallBound):
		t.Fatalf("group writer did not enter BeforeCOWCutSwap root=%s within %s", cacheRoot, c3COWCallBound)
	}
	// Read-only raw control uses generic public snapshot/iterator APIs. All
	// physical MVCC writes remain owned by this actual Store.
	h.require(h.start("paused raw snapshot control", func() c3COWOutcome {
		return c3COWOutcome{err: c3COWRawOld(db, keys, old)}
	}))
	t.Log("paused raw snapshot control completed on old cut")
	point := h.start("Store.GetAt", func() c3COWOutcome {
		r, err := store.GetAt(keys[0], 100)
		return c3COWOutcome{point: r, err: err}
	})
	history := h.start("Store.IterateVersions", func() c3COWOutcome {
		v, err := c3COWReadVersions(store, keys[0])
		return c3COWOutcome{versions: v, err: err}
	})
	for _, c := range []*c3COWCall{point, history} {
		select {
		case <-c.started:
		case <-time.After(c3COWCallBound):
			t.Fatalf("%s launch not acknowledged", c.label)
		}
	}
	// Both readers share one observation window. Receipt of started is not a
	// proof that a goroutine has reached the Store lock; scheduler delay can
	// also produce this bounded nonprogress result on an overloaded runner.
	pointDone, historyDone := point.done, history.done
	pointBefore, historyBefore := false, false
	timer := time.NewTimer(c3COWProgressBound)
	for pointDone != nil || historyDone != nil {
		select {
		case point.out = <-pointDone:
			point.joined, pointBefore, pointDone = true, true, nil
		case history.out = <-historyDone:
			history.joined, historyBefore, historyDone = true, true, nil
		case <-timer.C:
			pointDone, historyDone = nil, nil
		}
	}
	timer.Stop()
	h.release()
	h.require(writer)
	p, v := h.require(point), h.require(history)
	// Calls completing after release may capture either complete cut. Only
	// pre-release completions must return the old cut.
	if pointBefore {
		if err := c3COWCheckPoint(p.point, 10, old); err != nil {
			t.Error(err)
		}
	} else if err := c3COWCheckPointEither(p.point, old, fresh); err != nil {
		t.Error(err)
	}
	if historyBefore {
		if err := c3COWCheckVersions(v.versions, keys[0], old, fresh, false); err != nil {
			t.Error(err)
		}
	} else if err := c3COWCheckVersionsEither(v.versions, keys[0], old, fresh); err != nil {
		t.Error(err)
	}
	h.require(h.start("complete publication oracle", func() c3COWOutcome {
		for _, key := range keys {
			r, err := store.GetAt(key, 100)
			if err = errors.Join(err, c3COWCheckPoint(r, 20, fresh)); err != nil {
				return c3COWOutcome{err: err}
			}
			versions, err := c3COWReadVersions(store, key)
			if err = errors.Join(err, c3COWCheckVersions(versions, key, old, fresh, true)); err != nil {
				return c3COWOutcome{err: err}
			}
		}
		return c3COWOutcome{}
	}))
	t.Log("after-release complete group and retained history oracle passed")
	h.cleanup()
	if !pointBefore || !historyBefore {
		t.Errorf("desired reader progress RED: while BeforeCOWCutSwap was withheld for %s, GetAt completed=%t IterateVersions completed=%t; raw old-cut control and after-release complete publication passed", c3COWProgressBound, pointBefore, historyBefore)
	}
}

func c3COWRawOld(db *treedb.DB, keys [][]byte, old []byte) (err error) {
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		return errors.New("raw control snapshot unavailable")
	}
	defer func() { err = errors.Join(err, snapshot.Close()) }()
	for _, key := range keys {
		lower, encodeErr := mvcckey.Encode(key, 100)
		upper, upperErr := mvcckey.AppendKeyVersionsUpper(nil, key)
		wantKey, wantErr := mvcckey.Encode(key, 10)
		if encodeErr != nil || upperErr != nil || wantErr != nil {
			return errors.Join(encodeErr, upperErr, wantErr)
		}
		it, openErr := snapshot.Iterator(lower, upper)
		if openErr != nil {
			return openErr
		}
		if it == nil {
			return errors.New("raw control iterator unavailable")
		}
		var checkErr error
		if !it.Valid() || !bytes.Equal(it.Key(), wantKey) || !bytes.Equal(it.Value(), append([]byte{recordValueV1}, old...)) {
			checkErr = fmt.Errorf("raw control %q did not return old encoded record", key)
		} else {
			it.Next()
			if it.Valid() {
				checkErr = fmt.Errorf("raw control %q saw an unexpected additional record", key)
			}
		}
		if closeErr := errors.Join(checkErr, it.Error(), it.Close()); closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func c3COWReadVersions(store *Store, key []byte) (versions []Version, err error) {
	it, err := store.IterateVersions(VersionIteratorOptions{ExactKey: key, ReadTimestamp: 100})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	for it.Valid() {
		versions = append(versions, it.Entry())
		it.Next()
	}
	return versions, it.Error()
}

func c3COWCheckPoint(r Result, timestamp uint64, value []byte) error {
	if r.State != Present || r.Timestamp != timestamp || !bytes.Equal(r.Value, value) {
		return fmt.Errorf("point = %+v; want Present timestamp %d value %q", r, timestamp, value)
	}
	return nil
}

func c3COWCheckPointEither(r Result, old, fresh []byte) error {
	if c3COWCheckPoint(r, 10, old) == nil || c3COWCheckPoint(r, 20, fresh) == nil {
		return nil
	}
	return fmt.Errorf("after-release reader returned neither complete old nor new point: %+v", r)
}

func c3COWCheckVersions(v []Version, key, old, fresh []byte, published bool) error {
	want := []Version{{Key: key, State: Present, Timestamp: 10, Value: old}}
	if published {
		want = append([]Version{{Key: key, State: Present, Timestamp: 20, Value: fresh}}, want...)
	}
	if len(v) != len(want) {
		return fmt.Errorf("history for %q has %d versions; want %d", key, len(v), len(want))
	}
	for i := range want {
		if !bytes.Equal(v[i].Key, want[i].Key) || v[i].State != want[i].State || v[i].Timestamp != want[i].Timestamp || !bytes.Equal(v[i].Value, want[i].Value) {
			return fmt.Errorf("history for %q version %d = %+v; want %+v", key, i, v[i], want[i])
		}
	}
	return nil
}

func c3COWCheckVersionsEither(v []Version, key, old, fresh []byte) error {
	if c3COWCheckVersions(v, key, old, fresh, false) == nil || c3COWCheckVersions(v, key, old, fresh, true) == nil {
		return nil
	}
	return fmt.Errorf("after-release reader returned neither complete old nor new history: %+v", v)
}
