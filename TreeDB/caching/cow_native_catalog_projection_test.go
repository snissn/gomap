package caching

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type testNativeCatalogProjection interface {
	NativeCatalogProjection(func(backenddb.NativeSemanticCapture) (backenddb.NativeSemanticState, error)) (backenddb.NativeSemanticState, error)
}

func TestCOWNativeCatalogProjectionOriginalCutLifetime(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	cut := c.retainCut()
	cap := &NativeSemanticCapture{cache: c, cut: cut}
	defer cap.Close()
	provider, ok := any(cap).(testNativeCatalogProjection)
	if !ok {
		t.Fatal("original capture lacks cut-owned derived catalog capability")
	}
	var calls atomic.Int32
	state := newTestSemanticState(nil)
	build := func(backenddb.NativeSemanticCapture) (backenddb.NativeSemanticState, error) {
		calls.Add(1)
		return state, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := provider.NativeCatalogProjection(build)
			if err != nil || got != state {
				t.Errorf("projection=%v err=%v", got, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 || state.refs.Load() != 1 || cap.State() != nil {
		t.Fatal("projection not single-winner or mutated published authority")
	}
	// A raw successor must not inherit a projection of the previous cut.
	c.writerMu.Lock()
	prepared := cowCutPrepare(t, c, "successor")
	retired := prepared.publish()
	c.writerMu.Unlock()
	if retired != nil {
		if err := retired.drain(); err != nil {
			t.Fatal(err)
		}
	}
	next := &NativeSemanticCapture{cache: c, cut: c.retainCut()}
	defer next.Close()
	nextProvider := any(next).(testNativeCatalogProjection)
	refusal := memtable.ErrCOWCapacity
	if got, err := nextProvider.NativeCatalogProjection(func(backenddb.NativeSemanticCapture) (backenddb.NativeSemanticState, error) { return nil, refusal }); got != nil || !errors.Is(err, refusal) {
		t.Fatal("failed build installed projection", got, err)
	}
	// Partial failed import is returned to its original caller, never cached.
	partial := newTestSemanticState(nil)
	failure := errors.New("catalog validation failure")
	failed, err := nextProvider.NativeCatalogProjection(func(backenddb.NativeSemanticCapture) (backenddb.NativeSemanticState, error) { return partial, failure })
	if failed != partial || !errors.Is(err, failure) {
		t.Fatal("partial failure custody", failed, err)
	}
	record := failed.Release()
	failed = nil
	completion, err := record.Finish()
	record = nil
	if err != nil {
		t.Fatal(err)
	}
	completion.Complete()
	if partial.calls != 1 || partial.resource != nil {
		t.Fatal("partial failed import not disposed once")
	}
	nextState := newTestSemanticState(nil)
	if got, err := nextProvider.NativeCatalogProjection(func(backenddb.NativeSemanticCapture) (backenddb.NativeSemanticState, error) { return nextState, nil }); err != nil || got != nextState {
		t.Fatal(got, err)
	}
	if state.calls != 0 || state.refs.Load() != 1 {
		t.Fatal("old cut projection retired while captured")
	}
	if err := cap.Close(); err != nil {
		t.Fatal(err)
	}
	db.drainSemanticCleanup()
	if state.calls != 1 || state.resource != nil || nextState.calls != 0 {
		t.Fatal("old projection cleanup crossed cut or missing")
	}
	if got, err := provider.NativeCatalogProjection(build); got != nil || !errors.Is(err, backenddb.ErrClosed) {
		t.Fatal("closed capture reused projection", got, err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	db.drainSemanticCleanup()
	if nextState.calls != 1 || nextState.resource != nil {
		t.Fatal("last-cut projection not cleaned once")
	}
	if !db.semanticCleanupTerminal() {
		t.Fatal("last-cut cleanup did not reach original terminal owner")
	}
	if s := c.budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
		t.Fatalf("last owner receipts: %+v", s)
	}
}
