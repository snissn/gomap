package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Queue metadata is part of the original admitted cowCache control. Only the
// existing flushLoop consumes it. The same latch serializes enqueue, active-cut
// withdrawal and the cleanup-only worker's terminal decision.
func (c *cowCache) enqueueSemanticLocked(record *rootpublication.SemanticCleanup) {
	if record == nil {
		return
	}
	if !record.EnqueueOnce() {
		panic("semantic cleanup record transferred twice")
	}
	if c.semanticEnqueued == ^uint64(0) {
		panic("semantic cleanup frontier exhausted")
	}
	c.semanticEnqueued++
	record.Next = nil
	if c.semanticTail == nil {
		c.semanticHead = record
	} else {
		c.semanticTail.Next = record
	}
	c.semanticTail = record
}
func (c *cowCache) wakeSemanticCleanup() {
	wakeSemanticCleanup(c.flushWake)
}

func wakeSemanticCleanup(wake chan struct{}) {
	if wake != nil {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func (cut *cowReadCut) releaseSemanticState() {
	c := cut.cache
	cut.cache = nil
	// Native Release only detaches one original record. It cannot traverse
	// catalogs, invoke destructors, block, or allocate.
	var semantic, frozen *rootpublication.SemanticCleanup
	if cut.semanticRootSet != nil {
		semantic = cut.semanticRootSet.Release()
		cut.semanticRootSet = nil
	}
	if cut.frozenSemanticRootSet != nil {
		frozen = cut.frozenSemanticRootSet.Release()
		cut.frozenSemanticRootSet = nil
	}
	var refund *memtable.COWExternalLease
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(semantic)
	c.enqueueSemanticLocked(frozen)
	// Enqueue MUST precede the last cut leaving the terminal predicate.
	remaining := c.activeCuts.Add(-1)
	if c.flushWake == nil && c.closeCleanupDone && c.readClosed.Load() && c.activeCuts.Load() == 0 && c.semanticHead == nil && c.semanticFailed == nil && !c.semanticRunning && (c.prefix == nil || !c.prefix.AcceptedCleanupFailed()) {
		refund = c.lease
		c.lease = nil
	}
	// Raw publications and retained semantic edges need no worker turn. The
	// final closed cut must still wake the cleanup-only worker even without a
	// record, so it can make the serialized terminal decision.
	var wake chan struct{}
	if semantic != nil || frozen != nil || (remaining == 0 && c.readClosed.Load()) {
		wake = c.flushWake
	}
	c.semanticMu.Unlock()
	wakeSemanticCleanup(wake)
	if refund != nil {
		c.closePhysicalReader()
	}
	refund.Close()
}

func retainCOWSemanticState(cut *cowReadCut) {
	if cut.semanticRootSet != nil {
		cut.semanticRootSet.Retain()
	}
	if cut.frozenSemanticRootSet != nil {
		cut.frozenSemanticRootSet.Retain()
	}
}

func (db *DB) drainSemanticCleanup() {
	c := db.cow
	if c == nil {
		return
	}
	for {
		c.semanticMu.Lock()
		record := c.semanticHead
		if record == nil {
			c.semanticMu.Unlock()
			return
		}
		c.semanticHead = record.Next
		if c.semanticHead == nil {
			c.semanticTail = nil
		}
		record.Next = nil
		c.semanticRunning = true
		c.semanticMu.Unlock()
		// Guard actual destruction AND the synchronous NotifyError callback. Close
		// from this caller must refuse before public/cache waits on this worker.
		var completion rootpublication.SemanticCleanupCompletion
		err := db.ownedCloseGuard.EnterCleanup()
		entered := err == nil
		if err == nil {
			completion, err = record.Finish()
			if err != nil {
				c.semanticMu.Lock()
				if c.prefix != nil {
					c.prefix.PoisonAccepted(err)
				}
				c.semanticMu.Unlock()
				db.retainOriginalCleanupError(err)
				if db.notifyError != nil {
					db.notifyError(err)
				}
			}
		} else {
			// No protected callback scope exists on this protocol failure. Publish the
			// terminal error and wake prefix waiters without recursively notifying.
			c.semanticMu.Lock()
			if c.prefix != nil {
				c.prefix.PoisonAccepted(err)
			}
			c.semanticMu.Unlock()
			db.retainOriginalCleanupError(err)
		}
		c.semanticMu.Lock()
		if err != nil {
			// Parking retains this SAME record, Work, failure and charge. Future
			// wakes drain eligible siblings; this record is never runnable again.
			if record.Err == nil {
				record.Err = err
			}
			record.Next = c.semanticFailed
			c.semanticFailed = record
		}
		c.semanticMu.Unlock()
		// The record can be embedded in the refunded EngineVersion. No record
		// or Work access may follow completion of the independent receipt.
		record = nil
		completion.Complete()
		if entered {
			db.ownedCloseGuard.LeaveCleanup()
		}
		c.semanticMu.Lock()
		c.semanticRunning = false
		c.semanticCompleted++
		c.semanticCond.Broadcast()
		c.semanticMu.Unlock()
	}
}

// Join only work already transferred after the producer's checkpoint. The
// existing FIFO flush worker remains the sole destructor; retained cuts and
// cleanup queued by later producers are outside this fixed frontier.
func (db *DB) joinSemanticCleanupFrontier() error {
	c := db.cow
	c.semanticMu.Lock()
	target := c.semanticEnqueued
	c.wakeSemanticCleanup()
	defer c.semanticMu.Unlock()
	for {
		if c.semanticFailed != nil {
			return c.semanticFailed.Err
		}
		if c.semanticCompleted >= target {
			return nil
		}
		if c.semanticStopped {
			return memtable.ErrCOWClosed
		}
		c.semanticCond.Wait()
	}
}

func (db *DB) semanticCleanupTerminal() bool {
	c := db.cow
	if c == nil {
		return true
	}
	c.semanticMu.Lock()
	if !c.readClosed.Load() || !c.closeCleanupDone || c.activeCuts.Load() != 0 || c.semanticHead != nil || c.semanticRunning {
		c.semanticMu.Unlock()
		return false
	}
	c.semanticStopped = true
	c.semanticCond.Broadcast()
	var refund *memtable.COWExternalLease
	// Failed original work remains in terminal custody and keeps its charge.
	if c.semanticFailed == nil && (c.prefix == nil || !c.prefix.AcceptedCleanupFailed()) {
		refund = c.lease
		c.lease = nil
	}
	c.semanticMu.Unlock()
	if refund != nil {
		c.closePhysicalReader()
	}
	refund.Close()
	return true
}

// A private successor has no activeCuts edge. Its opening retained cut remains
// alive until cancellation has released/enqueued these private semantic edges.
func (c *cowCache) releasePrivateSemanticState(cut *cowReadCut) {
	var semantic, frozen *rootpublication.SemanticCleanup
	if cut.semanticRootSet != nil {
		semantic = cut.semanticRootSet.Release()
		cut.semanticRootSet = nil
	}
	if cut.frozenSemanticRootSet != nil {
		frozen = cut.frozenSemanticRootSet.Release()
		cut.frozenSemanticRootSet = nil
	}
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(semantic)
	c.enqueueSemanticLocked(frozen)
	c.semanticMu.Unlock()
	c.wakeSemanticCleanup()
}
