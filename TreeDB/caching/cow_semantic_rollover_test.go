package caching

import (
	"errors"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type semanticRolloverState struct {
	*testSemanticState
	next             backenddb.NativeSemanticState
	failure          error
	rolloverCalls    int
	before           func()
	built            *atomic.Bool
	refuseUntilBuilt bool
}

func (s *semanticRolloverState) PrepareRollover() (backenddb.NativeSemanticState, error) {
	s.rolloverCalls++
	if s.before != nil {
		s.before()
	}
	if s.refuseUntilBuilt && !s.built.Load() {
		return nil, memtable.ErrCOWCapacity
	}
	if s.next != nil {
		next := s.next
		s.next = nil
		return next, s.failure
	}
	if s.failure != nil {
		return nil, s.failure
	}
	s.Retain()
	return s, nil
}
func (s *semanticRolloverState) PrepareBuild(*backenddb.RootPublicationBuildGroup, *backenddb.Snapshot, *backenddb.StableResourceCaptureLease) error {
	if s.built != nil {
		s.built.Store(true)
	}
	return nil
}

func TestCOWSemanticRolloverOwnsDistinctSuccessorAndPrivateError(t *testing.T) {
	for _, mode := range []string{"install", "cancel", "partial-error"} {
		t.Run(mode, func(t *testing.T) {
			db, c := cowCutFixture(t)
			db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
			c.flushWake = make(chan struct{}, 1)
			original := &semanticRolloverState{testSemanticState: newTestSemanticState(nil)}
			successor := newTestSemanticState(func() error {
				if !c.writerMu.TryLock() {
					t.Error("destructor under writer owner")
				} else {
					c.writerMu.Unlock()
				}
				if !c.cutMu.TryLock() {
					t.Error("destructor under cut latch")
				} else {
					c.cutMu.Unlock()
				}
				return nil
			})
			original.next = successor
			if mode == "partial-error" {
				original.failure = errors.New("partial original successor")
			}
			original.before = func() {
				if c.writerMu.TryLock() {
					c.writerMu.Unlock()
					t.Error("rollover lacks writer owner")
				}
				if !c.cutMu.TryLock() {
					t.Error("rollover runs under cut latch")
				} else {
					c.cutMu.Unlock()
				}
			}
			c.cut.semanticRootSet = original
			c.cut.logicalSeq = 1
			opening := c.cut
			c.writerMu.Lock()
			p, err := c.prepareRollover()
			if mode == "partial-error" {
				if err != original.failure || p.semantic != successor || p.next != nil {
					t.Fatalf("partial result lost: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if p.next.semanticRootSet != successor || p.next.frozenSemanticRootSet != original || p.next.logicalSeq != 1 || len(p.next.frozen) != 0 {
					t.Fatal("whole semantic cut mismatch")
				}
				if original.refs.Load() != 2 || successor.refs.Load() != 1 {
					t.Fatal("successor edges not adopted once")
				}
				if mode == "install" {
					p.install()
				}
			}
			c.writerMu.Unlock()
			if mode != "install" {
				if err := c.close(); err != nil {
					t.Fatal(err)
				}
				if c.activeCuts.Load() != 1 || opening.refs != 1 || db.semanticCleanupTerminal() {
					t.Fatal("opening cut withdrew before private cleanup")
				}
			}
			if err := p.drain(); err != nil {
				t.Fatal(err)
			}
			if mode == "install" {
				if c.cut.semanticRootSet != successor || c.cut.frozenSemanticRootSet != original || original.refs.Load() != 1 {
					t.Fatal("installed captured state lost")
				}
				if err := c.close(); err != nil {
					t.Fatal(err)
				}
			}
			if c.activeCuts.Load() != 0 || c.semanticHead == nil || db.semanticCleanupTerminal() {
				t.Fatal("worker terminal before original cleanup")
			}
			db.drainSemanticCleanup()
			if original.calls != 1 || successor.calls != 1 || !db.semanticCleanupTerminal() {
				t.Fatal("original cleanup did not run once")
			}
		})
	}
}

func TestCOWNativeOnlyCapturedPrefixDrainsBeforeRolloverCapacityRetry(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	var built atomic.Bool
	var first *semanticRolloverState
	for i := 0; i < 2; i++ {
		capture, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		state := &semanticRolloverState{testSemanticState: nativeBridgeState(t, capture), built: &built, refuseUntilBuilt: i == 1}
		if i == 0 {
			first = state
		}
		result, err := backend.WriteNativePublication(&backenddb.NativePreparedPublication{Basis: capture.State(), State: state, StartSequence: capture.LogicalSequence()}, nil, false)
		if err != nil || !result.LogicalInstalled {
			t.Fatalf("native acceptance=%+v: %v", result, err)
		}
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if err := cache.rolloverCOWForFlush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(cache.cow.cut.frozen) != 0 || cache.cow.cut.frozenLogicalSeq <= cache.cow.cut.basis.logicalSeq || built.Load() {
		t.Fatal("fixture lacks native-only captured prefix")
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if !built.Load() || first.rolloverCalls != 1 || cache.cow.cut.basis.logicalSeq != 2 || cache.cow.cut.logicalSeq != 2 {
		t.Fatal("finite complete-prefix retry failed")
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}
