package caching

import (
	"errors"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Keep this witness separate from reentry controls: backend EnterCleanup entry
// coverage counts only the real drain, including its empty and one-record cases.
func TestCOWSemanticCleanupGuardDrainBatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
	}{{"empty", 0}, {"one", 1}, {"many", 64}} {
		t.Run(tc.name, func(t *testing.T) {
			db, c := cowCutFixture(t)
			guard := db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
			db.ownedCloseGuard = guard
			before := c.budget.Stats()
			states := make([]*testSemanticState, tc.n)
			refunds := make([]*observingSemanticRefund, tc.n)
			for i := range states {
				lease, err := c.budget.AcquireExternal(8192)
				if err != nil {
					t.Fatal(err)
				}
				refunds[i] = &observingSemanticRefund{lease: lease, observe: func() {}}
				states[i] = newTestSemanticState(nil)
				states[i].refund = refunds[i]
				c.semanticMu.Lock()
				c.enqueueSemanticLocked(states[i].Release())
				c.semanticMu.Unlock()
			}
			db.drainSemanticCleanup()
			for i, state := range states {
				if state.calls != 1 || state.resource != nil || state.cleanup.Work != nil || refunds[i].calls != 1 {
					t.Fatalf("record %d did not finish/refund exactly once", i)
				}
			}
			if c.semanticHead != nil || c.semanticTail != nil || c.semanticFailed != nil || c.semanticRunning || c.semanticCompleted != uint64(tc.n) || c.semanticEnqueued != uint64(tc.n) {
				t.Fatal("drain lost original queue/frontier state")
			}
			if after := c.budget.Stats(); after.ExternalBytes != before.ExternalBytes || after.ReservedBytes != before.ReservedBytes {
				t.Fatalf("unbalanced original refunds: before=%+v after=%+v", before, after)
			}
			if err := db.joinSemanticCleanupFrontier(); err != nil {
				t.Fatal(err)
			}
			if err := guard.CheckClose(); err != nil {
				t.Fatal("cleanup marker survived drain", err)
			}
			if err := guard.Enter(); err != nil {
				t.Fatal("same-caller publication entry remained blocked", err)
			}
			guard.Leave()
		})
	}
}

func TestCOWSemanticCleanupGuardDrainScope(t *testing.T) {
	t.Run("nested-refusal", func(t *testing.T) {
		db, c := cowCutFixture(t)
		guard := db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
		db.ownedCloseGuard = guard
		nested := newTestSemanticState(nil)
		notifications := 0
		db.notifyError = func(error) { notifications++ }
		outer := newTestSemanticState(func() error {
			c.semanticMu.Lock()
			c.enqueueSemanticLocked(nested.Release())
			c.semanticMu.Unlock()
			db.drainSemanticCleanup()
			if err := guard.CheckClose(); err == nil {
				t.Error("refused nested drain cleared outer marker")
			}
			if err := guard.EnterCleanup(); !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
				t.Errorf("nested cleanup entry=%v", err)
			}
			if err := guard.Enter(); !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
				t.Errorf("nested publication entry=%v", err)
			}
			return nil
		})
		c.semanticMu.Lock()
		c.enqueueSemanticLocked(outer.Release())
		c.semanticMu.Unlock()
		db.drainSemanticCleanup()
		if outer.calls != 1 || nested.calls != 0 || nested.cleanup.Work != nested || c.semanticFailed != &nested.cleanup || notifications != 0 || !errors.Is(db.backgroundError(), rootpublication.ErrDurableRootOwnership) {
			t.Fatal("nested refusal lost original failure custody")
		}
		if err := guard.CheckClose(); err != nil {
			t.Fatal("outer marker survived drain", err)
		}
		if err := guard.Enter(); err != nil {
			t.Fatal("same-worker publication still blocked", err)
		}
		guard.Leave()
	})
	t.Run("concurrent-lane", func(t *testing.T) {
		db, c := cowCutFixture(t)
		guard := db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
		db.ownedCloseGuard = guard
		started := []chan struct{}{make(chan struct{}), make(chan struct{})}
		release := []chan struct{}{make(chan struct{}), make(chan struct{})}
		states := make([]*testSemanticState, 2)
		for i := range states {
			states[i] = newTestSemanticState(func() error {
				close(started[i])
				<-release[i]
				return nil
			})
			c.semanticMu.Lock()
			c.enqueueSemanticLocked(states[i].Release())
			c.semanticMu.Unlock()
		}
		done := make(chan struct{})
		returned := make(chan error, 1)
		go func() {
			defer close(done)
			db.drainSemanticCleanup()
			if err := guard.CheckClose(); err != nil {
				returned <- err
				return
			}
			err := guard.Enter()
			if err == nil {
				guard.Leave()
			}
			returned <- err
		}()
		defer func() {
			for _, ch := range release {
				select {
				case <-ch:
				default:
					close(ch)
				}
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("cleanup worker did not return")
			}
		}()
		for i := range states {
			select {
			case <-started[i]:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup record did not start")
			}
			if err := guard.CheckClose(); err != nil {
				t.Fatal("unrelated caller refused", err)
			}
			if err := guard.Enter(); err != nil {
				t.Fatal("independent publication lane refused", err)
			}
			guard.Leave()
			close(release[i])
		}
		select {
		case err := <-returned:
			if err != nil {
				t.Fatal("marker leaked beyond drain", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cleanup did not complete")
		}
		for _, state := range states {
			if state.calls != 1 || state.resource != nil {
				t.Fatal("original work replayed or retained")
			}
		}
	})
}
