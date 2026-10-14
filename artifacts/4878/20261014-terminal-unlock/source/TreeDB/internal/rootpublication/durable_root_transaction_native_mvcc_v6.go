package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"sync"
	"unsafe"
)

// SealPrimaryBaseAbsenceV6 continues this SAME transaction's construction.
// The physical selection proof and fresh current-root cut belong to its DB
// caller. It creates neither a publication issuer nor a replacement owner.
func (t *DurableRootTransaction) SealPrimaryBaseAbsenceV6(current primaryarena.Ref, entry node.PrimaryDirectoryEntry, base node.PrimaryOperand, sequence uint64, scratch []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || c.cleanupPhase != 0 || t.phase != durableRootConstructingV6 || c.certificate != nil || c.componentCount != 0 {
		return false, ErrDurableRootOwnership
	}
	cert, ready, err := c.arena.SealConstructedBaseAbsenceV6(c.root, current, entry, base, sequence, scratch, t, w)
	if ready && err == nil {
		c.certificate = cert
	}
	return ready, err
}

var ErrNativeMVCCRootWorkRequiredV6 = errors.New("native MVCC transaction requires its original work-bearing caller")

// PrimaryConstructionReleaseWork belongs to the disposing original parent.
// Its containing allocation is admitted by that parent before Bind. It survives
// detaching the original transaction and its private constructor.
type PrimaryConstructionReleaseWork struct {
	mu          sync.Mutex
	transaction *DurableRootTransaction
	removal     retainedalloc.RemovalWork
	phase       uint8
}
type primaryConstructionReleaseState struct {
	owner  *retainedalloc.Owner
	charge uint64
}

func (s *PrimaryConstructionReleaseWork) Bind(t *DurableRootTransaction, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || t == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.transaction != nil || s.phase != 0 {
		return false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*t))) {
		return false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phase != durableRootConstructingV6 || t.constructionV6 == nil || !t.constructionV6.nativeWorkOwned || t.constructionV6.cleanupReceipt != nil {
		return false, ErrDurableRootOwnership
	}
	t.constructionV6.cleanupReceipt = s
	s.transaction = t
	return true, nil
}
func (s *PrimaryConstructionReleaseWork) ContinueWork(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase == 2 {
		return true, nil
	}
	if s.phase == 1 {
		ready, err := s.removal.ContinueWork(w)
		if ready {
			s.phase = 2
		}
		return ready, err
	}
	t := s.transaction
	if t == nil {
		return false, ErrDurableRootOwnership
	}
	t.mu.Lock()
	var terminal primaryConstructionReleaseState
	ready, err := t.abortNativeMVCCConstructionStepLockedV6(w, &terminal, &s.removal)
	if ready && err == nil {
		s.transaction = nil
		s.phase = 1
	}
	t.mu.Unlock()
	// The original transaction is no longer accessed after its active call joins.
	return false, err
}

// The old receiver-only native path cannot retain a receipt after refund of
// this receiver. Production callers must bind their admitted external receipt.
func (t *DurableRootTransaction) AbortNativeMVCCConstructionStepV6(w *iterator.OrdinalScanWork) (bool, error) {
	return false, ErrNativeMVCCRootWorkRequiredV6
}

// Ordinary Abort loops these exact phases under its ordinary unlimited policy.
// The native path has no opaque callbacks: the unbound private constructor has
// not installed any, and later visibility Finish supplies work-bearing hooks.
func (t *DurableRootTransaction) abortNativeMVCCConstructionStepLockedV6(w *iterator.OrdinalScanWork, terminal *primaryConstructionReleaseState, removal *retainedalloc.RemovalWork) (bool, error) {
	if t.physicalCleanupError != nil {
		return false, t.physicalCleanupError
	}
	c := t.constructionV6
	if c == nil || (t.phase != durableRootConstructingV6 && t.phase != durableRootPrepared) {
		return false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*t))+2*uint64(unsafe.Sizeof(*c))) {
		return false, nil
	}
	if c.cleanupPhase == 0 {
		c.cleanupPhase = 1
	}
	for c.cleanupPhase == 1 && c.componentCount > 0 {
		r := c.components[c.componentCount-1]
		ready, err := c.arena.DropToReleaseQueue(r, &c.cleanupQueue, w)
		if err != nil {
			return false, t.recordPhysicalCleanupFailureLocked(err)
		}
		if !ready {
			return false, nil
		}
		c.componentCount--
		c.components[c.componentCount] = primaryarena.Ref{}
	}
	if c.cleanupPhase == 1 {
		c.cleanupPhase = 2
	}
	if c.cleanupPhase == 2 {
		if c.root != (primaryarena.Ref{}) {
			ready, err := c.arena.DropNativeMVCCConstructionToReleaseQueueV6(c.root, t, &c.cleanupQueue, w)
			if err != nil {
				return false, t.recordPhysicalCleanupFailureLocked(err)
			}
			if !ready {
				return false, nil
			}
			c.root = primaryarena.Ref{}
		}
		c.cleanupPhase = 3
	}
	if c.cleanupPhase == 3 {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*t))+2*uint64(unsafe.Sizeof(*c))) {
			return false, nil
		}
		if t.abortComplete != nil {
			if c.nativeWorkOwned {
				return false, t.recordPhysicalCleanupFailureLocked(ErrNativeMVCCRootWorkRequiredV6)
			}
			if err := t.abortComplete(t.input); err != nil {
				return false, t.recordPhysicalCleanupFailureLocked(err)
			}
		}
		// Dispose every certificate/image/callback alias BEFORE the bank's
		// physical release refunds its backing. These descriptors confer no
		// remaining edge; resources retain their own original owner cell.
		c.certificate, c.promotion = nil, nil
		c.candidate, c.members = PreparedRootCandidate{}, [1]*DurableRootTransaction{}
		t.input = DurableRootCallbackInput{}
		t.activate, t.consume, t.abort, t.abortComplete, t.fail = nil, nil, nil, nil, nil
		c.cleanupPhase = 4
	}
	for c.cleanupPhase == 4 {
		ready, progress, err := c.arena.ReleaseQueueStep(&c.cleanupQueue, w)
		if err != nil {
			return false, t.recordPhysicalCleanupFailureLocked(err)
		}
		if !ready {
			return false, nil
		}
		if !progress {
			c.cleanupPhase = 5
		}
	}
	owner, charge := c.arena.MetadataOwner(), c.metadataCharge
	if removal != nil {
		ready, err := removal.BindPendingWork(owner, charge, w)
		if !ready || err != nil {
			return false, err
		}
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*t))+2*uint64(unsafe.Sizeof(*c))) {
		return false, nil
	}
	t.constructionV6 = nil
	t.input = DurableRootCallbackInput{}
	t.activate, t.consume, t.abort, t.abortComplete, t.fail = nil, nil, nil, nil, nil
	*c = primaryTransactionConstructionV6{}
	t.owner, t.phase = ResourceOwnerReleased, durableRootConsumed
	terminal.owner, terminal.charge = owner, charge
	return true, nil
}
