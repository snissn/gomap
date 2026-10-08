package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"sync"
)

const (
	snapshotVMRoleV1 uint64 = 1 << iota
	snapshotIndexRoleV1
	snapshotLeafRoleV1
	snapshotCounterRoleV1
	snapshotCallbackRoleV1
	snapshotScrubRoleV1
	snapshotControlRoleV1
)
const snapshotAllRolesV1 = (snapshotControlRoleV1 << 1) - 1

// OriginalSnapshotCleanupV1 survives the original handle scrub. Its immutable
// identity and original Scope, never a pooled Snapshot's CAS, prove completion.
// The failure link is actual synchronous DB custody, not a scheduled job.
type OriginalSnapshotCleanupV1 struct {
	mu             sync.Mutex
	snapshot       *Snapshot
	creator        *residentcredit.Scope
	privateBacking residentcredit.PrivateSnapshotBackingV1
	refs           uint64
	outcome        rootpublication.StableCleanupOutcomeV1
	fault          error // ordinary errors/hidden graphs remain uncertified
	control        *valuelog.SnapshotSetTerminalRetentions
	next           *OriginalSnapshotCleanupV1
	attached       bool
	invoking       bool // exact synchronous cleanup invocation; protected by mu
}

func (c *OriginalSnapshotCleanupV1) RetainOriginalCleanupV1() error {
	if c == nil {
		return ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refs == 0 || c.creator == nil || c.refs == ^uint64(0) {
		return ErrClosed
	}
	if err := c.creator.RetainOriginalLifetime(); err != nil {
		return err
	}
	c.refs++
	return nil
}
func (c *OriginalSnapshotCleanupV1) ReleaseOriginalCleanupV1() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.refs == 0 {
		c.mu.Unlock()
		return
	}
	c.refs--
	creator := c.creator
	var backing residentcredit.PrivateSnapshotBackingV1
	if c.refs == 0 {
		c.creator = nil
		c.fault = nil
		backing = c.privateBacking
		c.privateBacking = nil
	}
	c.mu.Unlock()
	creator.ReleaseStableMetadata()
	creator = nil
	if backing != nil {
		backing.ReleasePrivateSnapshotBackingV1()
		backing = nil
	}
}
func (c *OriginalSnapshotCleanupV1) ObserveOriginalCleanupV1() rootpublication.StableCleanupOutcomeV1 {
	if c == nil {
		return rootpublication.StableCleanupOutcomeV1{Phase: rootpublication.StableCleanupCompleteV1}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outcome
}
func (c *OriginalSnapshotCleanupV1) consumed(role uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outcome.ConsumedRoles&role != 0
}
func (c *OriginalSnapshotCleanupV1) start(role uint64) {
	c.mu.Lock()
	c.outcome.StartedRoles |= role
	c.mu.Unlock()
}
func (c *OriginalSnapshotCleanupV1) consume(role uint64) {
	c.mu.Lock()
	c.outcome.ConsumedRoles |= role
	c.mu.Unlock()
}
func (c *OriginalSnapshotCleanupV1) AdvanceOriginalCleanupV1() (out rootpublication.StableCleanupOutcomeV1, err error) {
	if c == nil {
		return rootpublication.StableCleanupOutcomeV1{}, ErrClosed
	}
	c.mu.Lock()
	if c.outcome.Complete() {
		out = c.outcome
		c.mu.Unlock()
		return out, nil
	}
	if c.outcome.Phase == rootpublication.StableCleanupRunningV1 || c.outcome.Phase == rootpublication.StableCleanupUncertainV1 {
		out = c.outcome
		c.mu.Unlock()
		return out, rootpublication.ErrStableResourceOperationBusy
	}
	s := c.snapshot
	if s == nil {
		c.mu.Unlock()
		return out, ErrClosed
	}
	c.outcome.Phase = rootpublication.StableCleanupRunningV1
	c.mu.Unlock()
	// No waits: a callback/reentrant Close observes running and returns busy.
	defer func() {
		c.mu.Lock()
		completed := c.outcome.ConsumedRoles == snapshotAllRolesV1
		c.mu.Unlock()
		if completed {
			s.finalized.Store(true)
		}
		s = nil
		c.mu.Lock()
		if v := recover(); v != nil {
			c.outcome.Phase = rootpublication.StableCleanupUncertainV1
			c.outcome.Debt = rootpublication.StableCleanupUnknownDebtV1
			c.outcome.PendingRoles = snapshotAllRolesV1 &^ c.outcome.ConsumedRoles
			c.mu.Unlock()
			panic(v)
		}
		if err != nil {
			c.fault = errors.Join(c.fault, err)
			c.outcome.FaultCode = 1
		}
		if c.outcome.ConsumedRoles == snapshotAllRolesV1 {
			c.outcome.Phase = rootpublication.StableCleanupCompleteV1
			if c.outcome.Debt != rootpublication.StableCleanupNamespaceDebtV1 {
				c.outcome.Debt = rootpublication.StableCleanupNoDebtV1
			}
			c.outcome.PendingRoles = 0
			c.snapshot = nil
		} else if c.outcome.Phase == rootpublication.StableCleanupRunningV1 {
			c.outcome.Phase = rootpublication.StableCleanupPendingV1
			c.outcome.Debt = rootpublication.StableCleanupExactHoldsV1
			c.outcome.PendingRoles = snapshotAllRolesV1 &^ c.outcome.ConsumedRoles
		}
		out = c.outcome
		releaseOriginal := completed && !c.attached
		c.mu.Unlock()
		if releaseOriginal {
			c.ReleaseOriginalCleanupV1()
		} // original handle, only after returned role/local scrub
	}()
	err = s.finalizeCloseEffectsV1(c)
	return
}

// attachFailedSnapshotCleanupV1 transfers the private handle's EXISTING
// original edge into the actual DB holder. No fallible retain after cleanup can
// drop the only remaining owner. Completion keeps that edge until DB unlink.
func (db *DB) attachFailedSnapshotCleanupV1(c *OriginalSnapshotCleanupV1) {
	if db == nil || c == nil {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.attached || c.outcome.Complete() {
		return
	}
	c.attached = true
	c.next = db.failedSnapshotCleanup
	db.failedSnapshotCleanup = c
}

// The actual original edge is published BEFORE callbacks or physical release.
// A reentrant Close sees this running owner and refuses before teardown.
func (db *DB) beginSnapshotCleanupInvocationV1(c *OriginalSnapshotCleanupV1) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if db.snapshotPhysicalCloseRunning || c.invoking || c.outcome.Phase == rootpublication.StableCleanupRunningV1 {
		return rootpublication.ErrStableResourceOperationBusy
	}
	if c.outcome.Complete() {
		return nil
	}
	if !c.attached {
		c.attached = true
		c.next = db.failedSnapshotCleanup
		db.failedSnapshotCleanup = c
	}
	c.invoking = true
	return nil
}
func (db *DB) endSnapshotCleanupInvocationV1(c *OriginalSnapshotCleanupV1) {
	db.mu.Lock()
	c.mu.Lock()
	c.invoking = false
	done := c.outcome.Complete()
	removed := false
	if done && c.attached {
		link := &db.failedSnapshotCleanup
		for *link != nil && *link != c {
			link = &(*link).next
		}
		if *link == c {
			*link = c.next
			c.next = nil
			c.attached = false
			removed = true
		}
	}
	c.mu.Unlock()
	db.mu.Unlock()
	if removed {
		c.ReleaseOriginalCleanupV1()
	}
}
func (db *DB) checkSnapshotPhysicalCloseV1Locked() error {
	if db.snapshotPhysicalCloseRunning {
		return rootpublication.ErrStableResourceOperationBusy
	}
	for c := db.failedSnapshotCleanup; c != nil; c = c.next {
		c.mu.Lock()
		busy := c.invoking || c.outcome.Phase == rootpublication.StableCleanupRunningV1 || c.outcome.Phase == rootpublication.StableCleanupUncertainV1
		c.mu.Unlock()
		if busy {
			return rootpublication.ErrStableResourceOperationBusy
		}
	}
	return nil
}
func (db *DB) checkSnapshotPhysicalCloseV1() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.checkSnapshotPhysicalCloseV1Locked()
}
func (db *DB) beginSnapshotPhysicalCloseV1() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.checkSnapshotPhysicalCloseV1Locked(); err != nil {
		return err
	}
	db.snapshotPhysicalCloseRunning = true
	return nil
}
func (db *DB) endSnapshotPhysicalCloseV1() {
	db.mu.Lock()
	db.snapshotPhysicalCloseRunning = false
	db.mu.Unlock()
}
func (db *DB) hasFailedSnapshotCleanupV1() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.failedSnapshotCleanup != nil || db.failedSnapshotCleanupDraining
}

// Claim an observer while the actual DB link still protects its original edge.
// No copied list or callback is retained beyond this synchronous drain.
func (db *DB) drainFailedSnapshotCleanupV1() error {
	db.mu.Lock()
	if db.failedSnapshotCleanupDraining || db.snapshotPhysicalCloseRunning {
		db.mu.Unlock()
		return rootpublication.ErrStableResourceOperationBusy
	}
	db.failedSnapshotCleanupDraining = true
	current := db.failedSnapshotCleanup
	var result error
	var next *OriginalSnapshotCleanupV1
	if current != nil {
		if err := current.RetainOriginalCleanupV1(); err != nil {
			result = err
			current = nil
		}
	}
	db.mu.Unlock()
	defer func() {
		if current != nil {
			current.ReleaseOriginalCleanupV1()
			current = nil
		}
		if next != nil {
			next.ReleaseOriginalCleanupV1()
			next = nil
		}
		db.mu.Lock()
		db.failedSnapshotCleanupDraining = false
		db.mu.Unlock()
	}()
	for current != nil {
		// Retain the next exact observer before current may unlink itself.
		db.mu.Lock()
		next = current.next
		if next != nil {
			if err := next.RetainOriginalCleanupV1(); err != nil {
				result = errors.Join(result, err)
				next = nil
			}
		}
		db.mu.Unlock()
		func() {
			c := current
			current = nil
			admitted := false
			defer func() {
				if admitted {
					db.endSnapshotCleanupInvocationV1(c)
				}
				c.ReleaseOriginalCleanupV1()
				c = nil
			}()
			if err := db.beginSnapshotCleanupInvocationV1(c); err != nil {
				result = errors.Join(result, err)
				return
			}
			admitted = true
			_, err := c.AdvanceOriginalCleanupV1()
			result = errors.Join(result, err)
		}()
		current = next
		next = nil
	}
	if db.hasFailedSnapshotCleanupV1ExceptDrain() {
		result = errors.Join(result, rootpublication.ErrStableResourceOperationBusy)
	}
	return result
}
func (db *DB) hasFailedSnapshotCleanupV1ExceptDrain() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.failedSnapshotCleanup != nil
}

func (s *Snapshot) CleanupCompleteV1() bool {
	if s == nil {
		return true
	}
	s.iteratorMu.Lock()
	c := s.originalCleanup
	done := s.finalized.Load() && c == nil
	s.iteratorMu.Unlock()
	if c != nil {
		return c.ObserveOriginalCleanupV1().Complete()
	}
	return done
}
func (db *DB) closePrivateSnapshotV1(holder **Snapshot) (err error) {
	if holder == nil || *holder == nil {
		return nil
	}
	s := *holder
	*holder = nil
	holder = nil
	s.iteratorMu.Lock()
	c := s.originalCleanup
	s.iteratorMu.Unlock()
	if c == nil {
		return s.Close()
	}
	if err = c.RetainOriginalCleanupV1(); err != nil {
		return err
	}
	defer func() {
		if !c.ObserveOriginalCleanupV1().Complete() {
			db.attachFailedSnapshotCleanupV1(c)
		}
		s = nil
		c.ReleaseOriginalCleanupV1()
		c = nil
	}()
	return s.Close()
}
