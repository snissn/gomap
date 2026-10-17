package db

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// OwnedCallbackCloseGuard is the original, immutable Close capability shared
// by the public, cache and backend handles. It retains only this tiny caller
// cell, never a containing DB, and remains valid after those handles detach.
type OwnedCallbackCloseGuard struct {
	caller atomic.Uint64
	scope  sync.Mutex
}

// CheckClose must precede any Close side effect or wait.
func (g *OwnedCallbackCloseGuard) CheckClose() error {
	if g != nil {
		if caller := g.caller.Load(); caller != 0 && caller == currentGoroutineID() {
			return errPublicationCallbackCloseV6
		}
	}
	return nil
}

// Enter marks the actual destructor/error notification invocation. The worker
// owns serialized callbacks; nested/concurrent invocation is a protocol error.
func (g *OwnedCallbackCloseGuard) Enter() error {
	if g == nil {
		return rootpublication.ErrPublisherProtocol
	}
	caller := currentGoroutineID()
	if caller == 0 {
		return rootpublication.ErrPublisherProtocol
	}
	if !g.scope.TryLock() {
		return rootpublication.ErrDurableRootOwnership
	}
	if !g.caller.CompareAndSwap(0, caller) {
		g.scope.Unlock()
		return rootpublication.ErrDurableRootOwnership
	}
	return nil
}

// EnterCleanup waits only outside progress-critical gates for an independent
// original publisher callback to finish. Same-caller recursion still refuses.
func (g *OwnedCallbackCloseGuard) EnterCleanup() error {
	if g == nil {
		return rootpublication.ErrPublisherProtocol
	}
	caller := currentGoroutineID()
	if caller == 0 {
		return rootpublication.ErrPublisherProtocol
	}
	if g.caller.Load() == caller {
		return rootpublication.ErrDurableRootOwnership
	}
	g.scope.Lock()
	g.caller.Store(caller)
	return nil
}
func (g *OwnedCallbackCloseGuard) Leave() { g.caller.Store(0); g.scope.Unlock() }

// OwnedCallbackCloseGuard returns the capability bound when this DB opened.
func (db *DB) OwnedCallbackCloseGuard() *OwnedCallbackCloseGuard {
	if db == nil {
		return nil
	}
	return db.ownedFinishGuardV6
}

func (r *rootPublicationRuntimeV1) EnterOwnedFinishCallbackV6(w *iterator.OrdinalScanWork) (bool, error) {
	if r == nil || r.db == nil || w == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if !w.Reserve(2, 128+2*uint64(unsafe.Sizeof(OwnedCallbackCloseGuard{}))) {
		return false, nil
	}
	if err := r.db.ownedFinishGuardV6.Enter(); err != nil {
		return false, err
	}
	return true, nil
}
func (r *rootPublicationRuntimeV1) LeaveOwnedFinishCallbackV6() { r.db.ownedFinishGuardV6.Leave() }

var errPublicationCallbackCloseV6 = errors.New("treedb: Close from active publication Finish callback")
