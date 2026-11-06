package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type testBasisDiagnosticState struct {
	*testBasisRefreshState
	observed uint64
	call     valuelog.COWBasisCall
}

func (s *testBasisDiagnosticState) PrepareBasisRefreshDiagnostic(before, after *backenddb.Snapshot, call valuelog.COWBasisCall) (backenddb.NativeSemanticState, error) {
	s.observed++
	s.call = call
	return s.PrepareBasisRefresh(before, after)
}

func TestCOWBasisRefusalExactInvocationAndPartial(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "frozen"}[frozen], func(t *testing.T) {
			var observer valuelog.COWBasisRefusalObserver
			owner := transactionowner.New()
			if !observer.Bind(owner) {
				t.Fatal("bind")
			}
			opening := newTestSemanticState(nil)
			partial := newTestSemanticState(nil)
			failure := errors.New("original refusal")
			state := &testBasisDiagnosticState{testBasisRefreshState: &testBasisRefreshState{testSemanticState: opening, partial: partial, failure: failure}}
			before, after := &backenddb.Snapshot{}, &backenddb.Snapshot{}
			result, err := refreshCOWSemanticBasisDiagnostic(state, before, after, observer.Begin(owner, frozen))
			if result != partial || err != failure || state.before != before || state.after != after || state.observed != 1 || opening.refs.Load() != 1 || partial.refs.Load() != 1 {
				t.Fatal("diagnostic changed original invocation/error/partial ownership")
			}
			word := observer.Snapshot().First
			if (word&(1<<24) != 0) != frozen || (word>>29)&15 != uint64(valuelog.COWBasisRefresh) || observer.Snapshot().Completed != 1 || observer.Snapshot().Missing {
				t.Fatalf("refresh tuple=%x snapshot=%+v", word, observer.Snapshot())
			}
			state.before, state.after, state.partial, state.call = nil, nil, nil, valuelog.COWBasisCall{}
			for _, original := range []*testSemanticState{opening, partial} {
				cleanup := original.Release()
				if cleanup == nil {
					t.Fatal("missing original cleanup")
				}
				completion, err := cleanup.Finish()
				if err != nil {
					t.Fatal(err)
				}
				completion.Complete()
			}
		})
	}
}

func TestCOWBasisRefusalExclusiveCacheBinding(t *testing.T) {
	// Registration predicates only: this control does not open or activate a DB.
	owner := transactionowner.New()
	cache := &DB{cow: &cowCache{txnOwner: owner}}
	var observer valuelog.COWBasisRefusalObserver
	if cache.EnableNativeBasisRefusalObserverForTest(transactionowner.New(), &observer) {
		t.Fatal("foreign opening identity accepted")
	}
	if !cache.EnableNativeBasisRefusalObserverForTest(owner, &observer) || cache.EnableNativeBasisRefusalObserverForTest(owner, &observer) {
		t.Fatal("binding not one-shot")
	}
	other := &DB{cow: &cowCache{txnOwner: transactionowner.New()}}
	if other.EnableNativeBasisRefusalObserverForTest(other.cow.txnOwner, &observer) {
		t.Fatal("receipt migrated to another owner")
	}
	call := cache.nativeBasisRefusalObserver.Load().Begin(owner, false)
	if !call.Enabled() || observer.Begin(other.cow.txnOwner, false).Enabled() {
		t.Fatal("invocation identity mismatch")
	}
}

func TestCOWBasisRefusalMissingSeamPreservesOriginal(t *testing.T) {
	var observer valuelog.COWBasisRefusalObserver
	owner := transactionowner.New()
	if !observer.Bind(owner) {
		t.Fatal("bind")
	}
	original := newTestSemanticState(nil)
	failure := errors.New("unchanged fallback refusal")
	state := &testBasisRefreshState{testSemanticState: original, failure: failure}
	before, after := &backenddb.Snapshot{}, &backenddb.Snapshot{}
	result, err := refreshCOWSemanticBasisDiagnostic(state, before, after, observer.Begin(owner, false))
	if result != nil || err != failure || state.before != before || state.after != after || !observer.Snapshot().Missing {
		t.Fatal("missing observer seam changed original call/result")
	}
	state.before, state.after = nil, nil
	cleanup := original.Release()
	completion, err := cleanup.Finish()
	if err != nil {
		t.Fatal(err)
	}
	completion.Complete()
}
