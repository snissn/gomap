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
	caller     atomic.Uint64 // existing serialized publication Finish lane
	cleanup    atomic.Uint64 // existing serialized cache cleanup worker lane
	checkpoint atomic.Uint64 // existing size-maintenance worker's NotifyError
}

// CheckClose must precede any Close side effect or wait.
func (g *OwnedCallbackCloseGuard) CheckClose() error {
	if g != nil {
		if caller := currentGoroutineID(); caller != 0 && (g.caller.Load() == caller || g.cleanup.Load() == caller || g.checkpoint.Load() == caller) {
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

// CurrentGoroutineID returns the caller's immutable runtime goroutine ID.
// Dedicated long-lived worker goroutines can capture it once at startup and
// pass it to EnterCleanupWithGoroutineID to avoid repeated runtime.Stack calls.
func CurrentGoroutineID() uint64 {
	return currentGoroutineID()
}

// EnterCleanup marks only the existing flush worker lane. Each fixed lane owns
// its marker; nested callbacks on either lane refuse without a mutex or wait.
func (g *OwnedCallbackCloseGuard) EnterCleanup() error {
	return g.EnterCleanupWithGoroutineID(0)
}

// EnterCleanupWithGoroutineID marks only the existing flush worker lane using
// a caller-cached goroutine ID when non-zero, falling back to
// currentGoroutineID() when caller is zero.
func (g *OwnedCallbackCloseGuard) EnterCleanupWithGoroutineID(caller uint64) error {
	if g == nil {
		return rootpublication.ErrPublisherProtocol
	}
	if caller == 0 {
		caller = currentGoroutineID()
		if caller == 0 {
			return rootpublication.ErrPublisherProtocol
		}
	}
	if g.caller.Load() == caller || !g.cleanup.CompareAndSwap(0, caller) {
		return rootpublication.ErrDurableRootOwnership
	}
	return nil
}

// EnterCheckpointNotification marks the size worker's synchronous NotifyError.
// Close joins this worker, so callback reentry must be rejected before any
// Close side effect. Other callback lanes can remain active independently.
func (g *OwnedCallbackCloseGuard) EnterCheckpointNotification() error {
	if g == nil {
		return rootpublication.ErrPublisherProtocol
	}
	caller := currentGoroutineID()
	if caller == 0 || !g.checkpoint.CompareAndSwap(0, caller) {
		return rootpublication.ErrDurableRootOwnership
	}
	return nil
}

func (g *OwnedCallbackCloseGuard) LeaveCheckpointNotification() { g.checkpoint.Store(0) }

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
