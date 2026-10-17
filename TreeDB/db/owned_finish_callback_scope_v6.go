package db

import (
	"errors"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// OwnedCallbackCloseGuard is the original, immutable Close capability shared
// by the public, cache and backend handles. It retains only this tiny caller
// cell, never a containing DB, and remains valid after those handles detach.
type OwnedCallbackCloseGuard struct{ caller atomic.Uint64 }

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
	if !g.caller.CompareAndSwap(0, caller) {
		return rootpublication.ErrDurableRootOwnership
	}
	return nil
}
func (g *OwnedCallbackCloseGuard) Leave() { g.caller.Store(0) }

// OwnedCallbackCloseGuard returns the capability bound when this DB opened.
func (db *DB) OwnedCallbackCloseGuard() *OwnedCallbackCloseGuard {
	if db == nil {
		return nil
	}
	return db.ownedFinishGuardV6
}

func (r *rootPublicationRuntimeV1) EnterOwnedFinishCallbackV6() error {
	if r == nil || r.db == nil {
		return rootpublication.ErrDurableRootOwnership
	}
	return r.db.ownedFinishGuardV6.Enter()
}
func (r *rootPublicationRuntimeV1) LeaveOwnedFinishCallbackV6() { r.db.ownedFinishGuardV6.Leave() }

var errPublicationCallbackCloseV6 = errors.New("treedb: Close from active publication Finish callback")
