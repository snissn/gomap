package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"time"
)

// This fixed node is born/admitted with the original constructor. It holds
// accounting custody after the constructor's last caller has returned, and
// deliberately has no transaction, candidate, root or callback backpointer.
type primaryTerminalCompletionV6 struct {
	owner  *retainedalloc.Owner
	charge uint64
	next   *primaryTerminalCompletionV6
}

// Pop returns only by-value accounting data. The caller clears its node alias
// before any containing refund; native parents bind their admitted RemovalWork
// first and continue that same original accounting operation after detachment.
func popPrimaryTerminalCompletionV6(n *primaryTerminalCompletionV6) (*primaryTerminalCompletionV6, primaryConstructionReleaseState) {
	next, terminal := n.next, primaryConstructionReleaseState{owner: n.owner, charge: n.charge}
	*n = primaryTerminalCompletionV6{}
	return next, terminal
}

func completePrimaryTerminalsV6(head **primaryTerminalCompletionV6) {
	for *head != nil {
		node := *head
		next, terminal := popPrimaryTerminalCompletionV6(node)
		*head = next
		node = nil
		terminal.owner.RemovePending(terminal.charge)
	}
}

type primaryPublicationCompletionV6 struct {
	head                        *primaryTerminalCompletionV6
	frontier                    Frontier
	lineage                     DurableRootLineageID
	sequence, oldest, groupSize uint64
	service                     time.Duration
	success                     bool
	active                      bool
	detached                    bool
}

func (c *Coordinator) removeCompletedPendingPrefixV6(n int, bytes uint64) {
	if n == 0 {
		return
	}
	c.pending = append([]pendingEntry(nil), c.pending[n:]...)
	if bytes <= c.pendingBytes {
		c.pendingBytes -= bytes
	} else {
		c.pendingBytes = 0
	}
}

func (c *Coordinator) completePublicationLockedV6(done primaryPublicationCompletionV6) {
	c.completePublicationLockedWithNativeV6(done, nil, nil)
}

func (c *Coordinator) completePublicationLockedWithNativeV6(done primaryPublicationCompletionV6, stop *nativeTimerStopPermitV6, notify *nativeNotifyPermitV6) {
	c.durable, c.oldestRecoverable = done.frontier, done.oldest
	c.reachabilityEpoch++
	if done.sequence > c.durableRootSequence {
		c.durableRootLineage, c.durableRootSequence = done.lineage, done.sequence
	}
	c.lastGroupSize, c.lastService = done.groupSize, done.service
	if c.ewmaService == 0 {
		c.ewmaService = done.service
	} else {
		c.ewmaService = (7*c.ewmaService + done.service) / 8
	}
	c.lastRetryableError = nil
	c.retryAttempt = PublishAttempt{}
	c.publishing = false
	c.satisfyWaitersLocked()
	c.recomputePublishRequestLockedWithNativeV6(true, stop)
	if notify != nil {
		notify.commit(c)
	} else {
		c.notifyLocked()
	}
}

func bindPrimaryTerminalWorkV6(n *primaryTerminalCompletionV6, removal *retainedalloc.RemovalWork, w *iterator.OrdinalScanWork) (bool, error) {
	if n == nil || removal == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	return removal.BindPendingWork(n.owner, n.charge, w)
}

// The original DATA constructor shares the same outside-callback terminal
// receipt and ACK barrier as PRIMARY. Physical Set release has already completed.
func (t *DurableRootTransaction) finishOrdinaryDataMetadataV6() (*primaryTerminalCompletionV6, error) {
	if t == nil {
		return nil, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.terminalMetadataV6 == nil {
		return nil, nil
	}
	if t.physicalCleanupError != nil {
		return nil, t.physicalCleanupError
	}
	if t.phase != durableRootFinishing || t.ownerLockedV6() != ResourceOwnerCoordinator {
		return nil, ErrDurableRootOwnership
	}
	node := t.terminalMetadataV6
	t.terminalMetadataV6 = nil
	t.input = DurableRootCallbackInput{Lineage: t.input.Lineage, Sequence: t.input.Sequence}
	t.clearDurableRootCallbacksV6()
	t.releaseOwnerLockedV6()
	t.phase = durableRootConsumed
	return node, nil
}
