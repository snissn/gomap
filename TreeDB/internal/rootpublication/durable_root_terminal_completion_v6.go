package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"time"
)

// This fixed node is born/admitted with the original constructor. It holds
// accounting custody after the constructor's last caller has returned, and
// deliberately has no transaction, candidate, root or callback backpointer.
type durableRootTerminalCompletionV6 struct {
	owner  *retainedalloc.Owner
	charge uint64
	next   *durableRootTerminalCompletionV6
}

// Pop returns only by-value accounting data. The caller clears its node alias
// before any containing refund; native parents bind their admitted RemovalWork
// first and continue that same original accounting operation after detachment.
func popRootTerminalCompletionV6(n *durableRootTerminalCompletionV6) (*durableRootTerminalCompletionV6, metadataReleaseStateV6) {
	next, terminal := n.next, metadataReleaseStateV6{owner: n.owner, charge: n.charge}
	*n = durableRootTerminalCompletionV6{}
	return next, terminal
}

func completeRootTerminalsV6(head **durableRootTerminalCompletionV6) {
	for *head != nil {
		node := *head
		next, terminal := popRootTerminalCompletionV6(node)
		*head = next
		node = nil
		terminal.owner.RemovePending(terminal.charge)
	}
}

type publicationCompletionV6 struct {
	head                        *durableRootTerminalCompletionV6
	scratch                     StableResourceTokenReleaseCompletionV6
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
	copy(c.pending, c.pending[n:])
	clear(c.pending[len(c.pending)-n:])
	c.pending = c.pending[:len(c.pending)-n]
	if bytes <= c.pendingBytes {
		c.pendingBytes -= bytes
	} else {
		c.pendingBytes = 0
	}
}

func (c *Coordinator) completePublicationLockedV6(done publicationCompletionV6) {
	c.durable, c.oldestRecoverable = done.frontier, done.oldest
	c.reachabilityEpoch++
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
	c.recomputePublishRequestLocked(true)
	c.notifyLocked()
}

// The original DATA constructor uses its outside-callback terminal receipt
// and ACK barrier. Physical Set release has already completed.
func (t *DurableRootTransaction) finishOrdinaryDataMetadataV6() (*durableRootTerminalCompletionV6, error) {
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
	t.dataCandidate = PreparedRootCandidate{}
	t.dataMember = [1]*DurableRootTransaction{}
	t.input = DurableRootCallbackInput{Lineage: t.input.Lineage, Sequence: t.input.Sequence}
	t.activate, t.consume, t.abort, t.abortComplete, t.fail = nil, nil, nil, nil, nil
	t.releaseOwnerLockedV6()
	t.phase = durableRootConsumed
	return node, nil
}

type metadataReleaseStateV6 struct {
	owner  *retainedalloc.Owner
	charge uint64
}

func (t *DurableRootTransaction) ownerLockedV6() ResourceOwnerState { return t.owner }
func (t *DurableRootTransaction) releaseOwnerLockedV6()             { t.owner = ResourceOwnerReleased }
