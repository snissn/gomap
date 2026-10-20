package db

import (
	"errors"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// OwnedCallbackCloseGuard is the original, immutable Close capability shared
// by the public, cache and backend handles. It retains only this tiny caller
// cell, never a containing DB, and remains valid after those handles detach.
type OwnedCallbackCloseGuard struct {
	caller  atomic.Uint64 // existing serialized publication Finish lane
	cleanup atomic.Uint64 // existing serialized cache cleanup worker lane
}

// CheckClose must precede any Close side effect or wait.
func (g *OwnedCallbackCloseGuard) CheckClose() error {
	if g != nil {
		if caller := currentGoroutineID(); caller != 0 && (g.caller.Load() == caller || g.cleanup.Load() == caller) {
			return errPublicationCallbackCloseV6
		}
	}
	return nil
}

// Enter marks the existing serialized publication Finish lane. Independent
// cleanup is allowed to overlap: blocking here would invert coordinator.mu
// against a destructor that consults the original coordinator.
func (g *OwnedCallbackCloseGuard) Enter() error {
	if g == nil {
		return rootpublication.ErrPublisherProtocol
	}
	caller := currentGoroutineID()
	if caller == 0 {
		return rootpublication.ErrPublisherProtocol
	}
	if g.cleanup.Load() == caller || !g.caller.CompareAndSwap(0, caller) {
		return rootpublication.ErrDurableRootOwnership
	}
	return nil
}

// EnterCleanup marks only the existing flush worker lane. Each fixed lane owns
// its marker; nested callbacks on either lane refuse without a mutex or wait.
func (g *OwnedCallbackCloseGuard) EnterCleanup() error {
	if g == nil {
		return rootpublication.ErrPublisherProtocol
	}
	caller := currentGoroutineID()
	if caller == 0 {
		return rootpublication.ErrPublisherProtocol
	}
	if g.caller.Load() == caller || !g.cleanup.CompareAndSwap(0, caller) {
		return rootpublication.ErrDurableRootOwnership
	}
	return nil
}
func (g *OwnedCallbackCloseGuard) Leave()        { g.caller.Store(0) }
func (g *OwnedCallbackCloseGuard) LeaveCleanup() { g.cleanup.Store(0) }

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
