package db

import (
	"bytes"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestOneShotReadAllocationBudget(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	key := []byte("key")
	if err := d.SetSync(key, []byte("value123")); err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 64)
	for _, tc := range []struct {
		name   string
		read   func() error
		budget float64
	}{
		{"Get", func() error { _, err := d.Get(key); return err }, 1},
		{"GetAppend", func() error { _, err := d.GetAppend(key, dst); return err }, 0},
		{"GetVersioned", func() error { _, _, err := d.GetVersioned(key); return err }, 1},
		{"GetVersionedAppend", func() error { _, _, err := d.GetVersionedAppend(key, dst); return err }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			epoch := d.snapshotAcquireEpoch.Load()
			allocs := testing.AllocsPerRun(100, func() {
				if err := tc.read(); err != nil {
					t.Fatal(err)
				}
			})
			if acquired := d.snapshotAcquireEpoch.Load() - epoch; acquired != 202 {
				t.Fatalf("capture epoch advanced %d, want 202 (101 capture/release pairs)", acquired)
			}
			if d.MinPinnedSnapshotCommitSeq() != math.MaxUint64 {
				t.Fatal("read leaked a registry pin")
			}
			t.Logf("allocations/read=%.0f capture/release pairs=101 live registry pins=0", allocs)
			if allocs > tc.budget {
				t.Fatalf("%.0f allocations/read, want <= %.0f (owned result only)", allocs, tc.budget)
			}
		})
	}
}

func TestOneShotReadOwnedMissingEmptyAndNilKey(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, key := range [][]byte{nil, []byte("empty"), []byte("owned")} {
		value := []byte{}
		if bytes.Equal(key, []byte("owned")) {
			value = []byte("value123")
		}
		if err := d.SetSync(key, value); err != nil {
			t.Fatal(err)
		}
	}
	stale := d.AcquireSnapshot()
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{nil, {}, []byte("empty"), []byte("owned"), []byte("missing")} {
		got, rev, err := d.GetVersioned(key)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := d.Get(key)
		if err != nil || !bytes.Equal(plain, got) || (plain == nil) != (got == nil) {
			t.Fatalf("Get(%q)=%q, %v; versioned=%q", key, plain, err, got)
		}
		base := []byte("prefix:")
		out, appendRev, appendErr := d.GetVersionedAppend(key, base)
		wantErr := error(nil)
		if got == nil {
			wantErr = tree.ErrKeyNotFound
		}
		if !errors.Is(appendErr, wantErr) || appendRev != rev || !bytes.Equal(out, append(append([]byte{}, base...), got...)) {
			t.Fatalf("GetVersionedAppend(%q)=%q,%d,%v; value=%q revision=%d", key, out, appendRev, appendErr, got, rev)
		}
		out, appendErr = d.GetAppend(key, base)
		if !errors.Is(appendErr, wantErr) || !bytes.Equal(out, append(append([]byte{}, base...), got...)) {
			t.Fatalf("GetAppend(%q)=%q,%v", key, out, appendErr)
		}
		if len(plain) > 0 {
			plain[0] ^= 0xff
			again, err := d.Get(key)
			if err != nil || !bytes.Equal(again, got) {
				t.Fatalf("returned bytes alias pooled state: %q, %v", again, err)
			}
		}
	}
	if _, err := stale.Get([]byte("owned")); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed exported alias became readable: %v", err)
	}
}

func TestOneShotReadCapturePublicationAndRelease(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)
	d := &DB{snapPool: NewSnapshotPool()}
	d.idx.Store(idx)
	state1 := &DBState{CommitSeq: 11, RootPageID: 1}
	state2 := &DBState{CommitSeq: 22, RootPageID: 2}
	d.publishSnapshotView(idx, state1, nil, nil)

	// A pending capture must see the view published under the exclusive lock.
	d.valueLogPublicationMu.Lock()
	started := make(chan struct{})
	done := make(chan *oneShotRead)
	go func() {
		close(started)
		r, err := d.acquireOneShotReadOrErr()
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	<-started
	d.publishSnapshotView(idx, state2, nil, nil)
	d.valueLogPublicationMu.Unlock()
	r := <-done
	if r == nil {
		t.Fatal("capture failed")
	}
	if r.snapshot.state != state2 || idx.registry.MinPinnedSeq() != state2.CommitSeq || d.snapshotAcquireInFlight() != 0 {
		t.Fatal("capture did not register the coherent published view")
	}
	if err := r.close(); err != nil {
		t.Fatal(err)
	}
	if idx.registry.MinPinnedSeq() != math.MaxUint64 || r.snapshot.db != nil || r.snapshot.idx != nil || r.snapshot.state != nil || r.snapshot.reader.vlogs != nil || r.snapshot.treePager != nil || r.snapshot.treeRoot != 0 || r.snapshot.registryID != 0 {
		t.Fatal("released guard retained capture resources")
	}

	// Closing while a capture is waiting for root reuse rejects the capture.
	d.rootReuseMu.Lock()
	closed := make(chan error)
	go func() {
		r, err := d.acquireOneShotReadOrErr()
		if r != nil {
			_ = r.close()
		}
		closed <- err
	}()
	d.closing.Store(true)
	d.rootReuseMu.Unlock()
	if err := <-closed; !errors.Is(err, ErrClosed) {
		t.Fatalf("capture during close: %v", err)
	}
	d.closing.Store(false)
	d.publicationPoisoned.Store(true)
	if r, err := d.acquireOneShotReadOrErr(); r != nil || err == nil {
		t.Fatalf("poisoned capture=%v, %v", r, err)
	}
	if idx.registry.MinPinnedSeq() != math.MaxUint64 {
		t.Fatal("rejected capture leaked registry pin")
	}
}

func TestOneShotReadRejectedCaptureReleasesValueLogPin(t *testing.T) {
	idx := &indexGen{}
	idx.refs.Store(1)
	segment := &valuelog.File{}
	segment.RefCount.Store(1)
	set := &valuelog.Set{Files: map[uint32]*valuelog.File{1: segment}}
	d := &DB{snapPool: NewSnapshotPool()}
	d.publishSnapshotView(idx, &DBState{CommitSeq: 1, RootPageID: 1, ValueLogSet: set}, &valuelog.Manager{}, nil)
	if read, err := d.acquireOneShotReadOrErr(); read != nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("capture without registry=%v, %v", read, err)
	}
	if set.RefCount.Load() != 0 || segment.RefCount.Load() != 0 || d.snapshotAcquireInFlight() != 0 {
		t.Fatal("failed capture leaked a value-log or acquisition pin")
	}
}

func TestOneShotReadAlternateRootLeavesPooledRootTreesClean(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("primary-key"), []byte("primary-value")); err != nil {
		t.Fatal(err)
	}
	table := memtable.NewAppendOnlyWithCapacity(4)
	table.Set([]byte("alt-key"), []byte("alt-value-payload"))
	table.Freeze()
	_, rootIDs, err := d.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
		Iter: table.NewIterator(nil, nil),
	}})
	if err != nil {
		t.Fatalf("PublishOrderedRootGroup: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want 1 nonzero root", rootIDs)
	}
	altRootID := rootIDs[0]

	var begins, ends atomic.Int64
	unregister := d.RegisterForegroundReadObserverHooks(
		func() {},
		func() { begins.Add(1) },
		func() { ends.Add(1) },
	)
	defer unregister()

	var (
		captured  *oneShotRead
		staleView OneShotReadView
		gotVal    []byte
		found     bool
	)
	dst := make([]byte, 0, 64)
	if err := d.WithOneShotRead(func(view OneShotReadView) error {
		captured = view.r
		staleView = view
		state, ok := view.StateToken()
		if !ok || state.RootPageID == 0 || state.RootPageID == altRootID {
			t.Fatalf("unexpected state token: %+v ok=%v altRoot=%d", state, ok, altRootID)
		}
		if view.Pager() == nil {
			t.Fatal("Pager() returned nil")
		}
		var readErr error
		gotVal, found, readErr = view.GetAppendAtRoot(altRootID, []byte("alt-key"), dst[:0])
		if readErr != nil {
			return readErr
		}
		// Verify alternate-root read never dirtied rootTreeFirst or rootTrees.
		if view.r.snapshot.rootTreeFirst[0].root != 0 || len(view.r.snapshot.rootTrees) != 0 || cap(view.r.snapshot.rootTrees) != 0 {
			t.Fatalf("alternate-root read dirtied pooled rootTrees: first=%+v len=%d cap=%d",
				view.r.snapshot.rootTreeFirst[0].root, len(view.r.snapshot.rootTrees), cap(view.r.snapshot.rootTrees))
		}
		// Verify missing key and zero root semantics.
		miss, missFound, missErr := view.GetAppendAtRoot(altRootID, []byte("missing"), dst[:0])
		if missErr != nil || missFound || len(miss) != 0 {
			t.Fatalf("missing key=(%q, %v, %v), want (\"\", false, nil)", miss, missFound, missErr)
		}
		zeroOut, zeroFound, zeroErr := view.GetAppendAtRoot(0, []byte("alt-key"), dst[:0])
		if zeroErr != nil || zeroFound || len(zeroOut) != 0 {
			t.Fatalf("zero root=(%q, %v, %v), want (\"\", false, nil)", zeroOut, zeroFound, zeroErr)
		}
		return nil
	}); err != nil {
		t.Fatalf("WithOneShotRead: %v", err)
	}
	if !found || !bytes.Equal(gotVal, []byte("alt-value-payload")) {
		t.Fatalf("alt-key=(%q, %v), want (%q, true)", gotVal, found, "alt-value-payload")
	}
	if captured == nil {
		t.Fatal("expected captured oneShotRead")
	}
	if captured.snapshot.rootTreeFirst[0].root != 0 || len(captured.snapshot.rootTrees) != 0 || cap(captured.snapshot.rootTrees) != 0 {
		t.Fatalf("returned oneShotRead retained rootTrees: first=%d len=%d cap=%d",
			captured.snapshot.rootTreeFirst[0].root, len(captured.snapshot.rootTrees), cap(captured.snapshot.rootTrees))
	}
	if begins.Load() != 1 || ends.Load() != 1 {
		t.Fatalf("foreground observer begins=%d ends=%d, want 1/1", begins.Load(), ends.Load())
	}

	// Retained view must stay closed even after another WithOneShotRead reuses the pool.
	if err := d.WithOneShotRead(func(view OneShotReadView) error {
		if _, ok := staleView.StateToken(); ok {
			t.Fatal("stale OneShotReadView StateToken succeeded during reused capture")
		}
		if staleView.Pager() != nil {
			t.Fatal("stale OneShotReadView Pager returned non-nil during reused capture")
		}
		if _, _, err := staleView.GetAppendAtRoot(altRootID, []byte("alt-key"), nil); !errors.Is(err, ErrClosed) {
			t.Fatalf("stale OneShotReadView GetAppendAtRoot err=%v, want ErrClosed", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("second WithOneShotRead: %v", err)
	}

	// Zero-allocation budget for alternate-root WithOneShotRead with hooks observer.
	key := []byte("alt-key")
	allocs := testing.AllocsPerRun(100, func() {
		if err := d.WithOneShotRead(func(view OneShotReadView) error {
			var ok bool
			var readErr error
			dst, ok, readErr = view.GetAppendAtRoot(altRootID, key, dst[:0])
			if readErr != nil || !ok {
				t.Fatalf("GetAppendAtRoot=(%v, %v)", ok, readErr)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 0 {
		t.Fatalf("WithOneShotRead alternate-root allocs/op=%.0f, want 0", allocs)
	}
}

func TestForegroundReadObserverHooksConcurrentSwapAndUnregister(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	type counterObserver struct {
		begins   atomic.Int64
		ends     atomic.Int64
		active   atomic.Int64
		underflo atomic.Bool
	}
	newHooks := func(c *counterObserver) (func(), func()) {
		return func() {
				c.begins.Add(1)
				c.active.Add(1)
			}, func() {
				c.ends.Add(1)
				if c.active.Add(-1) < 0 {
					c.underflo.Store(true)
				}
			}
	}
	obsA := &counterObserver{}
	obsB := &counterObserver{}
	obsLegacy := &counterObserver{}
	beginA, endA := newHooks(obsA)
	beginB, endB := newHooks(obsB)
	beginLegacy := func() func() {
		obsLegacy.begins.Add(1)
		obsLegacy.active.Add(1)
		var once sync.Once
		return func() {
			once.Do(func() {
				obsLegacy.ends.Add(1)
				if obsLegacy.active.Add(-1) < 0 {
					obsLegacy.underflo.Store(true)
				}
			})
		}
	}

	stop := make(chan struct{})
	var swapWG sync.WaitGroup
	swapWG.Add(1)
	go func() {
		defer swapWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var unreg func()
			switch i % 3 {
			case 0:
				unreg = d.RegisterForegroundReadObserverHooks(func() {}, beginA, endA)
			case 1:
				unreg = d.RegisterForegroundReadObserverHooks(func() {}, beginB, endB)
			default:
				unreg = d.RegisterForegroundReadObserver(func() {}, beginLegacy)
			}
			if i%2 == 0 {
				unreg()
			}
		}
	}()

	var readerWG sync.WaitGroup
	for g := 0; g < 4; g++ {
		readerWG.Add(1)
		go func(id int) {
			defer readerWG.Done()
			dst := make([]byte, 0, 32)
			for i := 0; i < 400; i++ {
				switch (id + i) % 4 {
				case 0:
					snap := d.AcquireSnapshot()
					if snap == nil {
						t.Errorf("AcquireSnapshot=nil")
						return
					}
					snap.MarkForegroundRead()
					snap.MarkForegroundRead()
					if err := snap.Close(); err != nil {
						t.Errorf("Snapshot.Close: %v", err)
						return
					}
				case 1:
					snap := d.AcquireSnapshot()
					if snap == nil {
						t.Errorf("AcquireSnapshot=nil")
						return
					}
					snap.MarkForegroundRead()
					snap.DetachForegroundRead()
					snap.DetachForegroundRead()
					if err := snap.Close(); err != nil {
						t.Errorf("Snapshot.Close: %v", err)
						return
					}
				case 2:
					end := d.BeginForegroundRead()
					end()
					end()
				case 3:
					if err := d.WithOneShotRead(func(view OneShotReadView) error {
						state, ok := view.StateToken()
						if !ok {
							return errors.New("missing state token")
						}
						var readErr error
						dst, _, readErr = view.GetAppendAtRoot(state.RootPageID, []byte("k"), dst[:0])
						return readErr
					}); err != nil {
						t.Errorf("WithOneShotRead: %v", err)
						return
					}
				}
			}
		}(g)
	}

	readerWG.Wait()
	close(stop)
	swapWG.Wait()

	for name, obs := range map[string]*counterObserver{
		"A":      obsA,
		"B":      obsB,
		"legacy": obsLegacy,
	} {
		if obs.underflo.Load() {
			t.Fatalf("observer %s underflowed active count", name)
		}
		if got := obs.active.Load(); got != 0 {
			t.Fatalf("observer %s active=%d want 0 (begins=%d ends=%d)", name, got, obs.begins.Load(), obs.ends.Load())
		}
		if obs.begins.Load() != obs.ends.Load() {
			t.Fatalf("observer %s begins=%d != ends=%d", name, obs.begins.Load(), obs.ends.Load())
		}
	}
}
