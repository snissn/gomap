package rootpublication

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// BindFinishV6 transfers the SAME finishing transaction to the original
// caller's already admitted disposal receipt. The caller clears its transaction
// alias before ContinueFinishWorkV6. It does not settle the coordinator join.
func (s *PrimaryConstructionReleaseWork) BindFinishV6(t *DurableRootTransaction, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || t == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.transaction != nil || s.phase != 0 || s.finishing {
		return false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*t))) {
		return false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if t.phase != durableRootFinishing || t.owner != ResourceOwnerCoordinator ||
		c == nil || !c.nativeWorkOwned || c.cleanupReceipt != nil {
		return false, ErrDurableRootOwnership
	}
	c.cleanupReceipt = s
	s.transaction, s.finishing = t, true
	return true, nil
}

// ContinueFinishWorkV6 drives the original construction queue before the
// original Set's typed release. release must admit the actual Set-edge and
// physical work, including synchronized coordinator pin consumption. It must
// not use the ordinary synchronous Set release as a bounded callback.
//
// When the original constructor's shared Set edge was consumed against its
// exact visible clone, release is nil: only the SAME private arena queue and
// by-value Set completion remain. Otherwise release supplies actual typed work.
//
// Once physical disposal has returned, this method detaches the transaction
// and returns false. A later call with release == nil consumes only the
// admitted accounting receipt. This prevents callback/transaction aliases from
// overlapping the containing refund. The coordinator's physical join and prune
// ACK remain the outer caller's responsibility.
func (s *PrimaryConstructionReleaseWork) ContinueFinishWorkV6(w *iterator.OrdinalScanWork, release func(*StableResourceSet, *iterator.OrdinalScanWork) (bool, error)) (bool, error) {
	if s == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.finishing {
		return false, ErrDurableRootOwnership
	}
	if s.phase == 2 {
		return true, nil
	}
	if s.phase == 1 {
		// No caller-owned physical callback may remain in this refund invocation.
		if release != nil {
			return false, ErrDurableRootOwnership
		}
		ready, err := s.removal.ContinueWork(w)
		if ready && err == nil {
			s.phase = 2
		}
		return ready, err
	}
	if s.phase == 0 {
		t := s.transaction
		if t == nil {
			return false, ErrDurableRootOwnership
		}
		t.mu.Lock()
		c := t.constructionV6
		if c == nil || c.cleanupReceipt != s || (!c.setReleaseConsumed && release == nil) {
			t.mu.Unlock()
			return false, ErrDurableRootOwnership
		}
		t.mu.Unlock()
		terminal, ready, err := t.finishPrimaryStepWithWorkV6(w, nil,
			func(set *StableResourceSet, _ map[ResourceKind]uint64, work *iterator.OrdinalScanWork) (bool, error) {
				// This queue belongs to the original native construction. An ordinary
				// release driver must never drain it on the native caller's behalf.
				for {
					ready, progress, err := c.arena.ReleaseQueueStep(&c.cleanupQueue, work)
					if !ready || err != nil {
						return false, err
					}
					if !progress {
						break
					}
				}
				if c.setReleaseConsumed {
					// Its exact protected shared edges were already consumed
					// during the original publication turn, including pin counts.
					return c.setRelease.ContinueWork(work)
				}
				return release(set, work)
			})
		c, t = nil, nil
		if !ready || err != nil {
			return false, err
		}
		s.terminal, s.phase = terminal, 3
	}
	if s.phase == 3 {
		ready, err := bindPrimaryTerminalWorkV6(s.terminal, &s.removal, w)
		if !ready || err != nil {
			return false, err
		}
		node := s.terminal
		// RemovalWork already owns only the by-value accounting data.
		_, _ = popPrimaryTerminalCompletionV6(node)
		node = nil
		s.terminal, s.transaction, s.phase = nil, nil, 1
		// Return through the actual disposing call before any containing refund.
		return false, nil
	}
	return false, ErrDurableRootOwnership
}

// FinishDetachedV6 reports that the receipt has ended its physical aliases.
// The caller ends its callback scope before invoking accounting-only progress.
func (s *PrimaryConstructionReleaseWork) FinishDetachedV6() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finishing && (s.phase == 1 || s.phase == 2)
}

// consumePrimarySharedResourcesV6 is called under the original coordinator
// mutex, during this transaction's admitted final publication turn. It does
// not advance a frontier, consume the transaction, release the turn, or refund
// any storage. The final driver must admit this operation BEFORE its fence;
// the work-bearing callback contract alone is not that admission proof.
func (t *DurableRootTransaction) consumePrimarySharedResourcesV6(visible *StableResourceSet, counts map[ResourceKind]uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || !c.nativeWorkOwned ||
		t.owner != ResourceOwnerCoordinator || t.phase != durableRootActivated ||
		c.resources == nil || c.finishReleaseStarted || c.setReleaseConsumed || t.finishBusy {
		return false, ErrDurableRootOwnership
	}
	ready, err := c.setRelease.consumeSharedWithVisibleV6(c.resources, visible, counts, c.arena.MetadataOwner(), w)
	if ready && err == nil {
		c.setReleaseConsumed = true
		c.finishReleaseStarted = true
	}
	return ready, err
}
