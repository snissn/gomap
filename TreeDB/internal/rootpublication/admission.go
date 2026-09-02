package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrBuilderLease rejects atomic handoff with a foreign, released, or
// non-final nested builder lease. Rejection never consumes the lease or moves
// candidate ownership.
var ErrBuilderLease = errors.New("invalid root publication builder lease")

// BuilderHandoffReceipt proves that EnqueueBuilt atomically accepted one
// candidate and consumed its final builder lease. A receipt is coordinator
// specific and separates irreversible ownership transfer from the optional
// post-acceptance hard-admission wait.
type BuilderHandoffReceipt struct {
	coordinator       *Coordinator
	sequence          uint64
	failureGeneration uint64
	hard              bool
}

// BuilderToken accounts one active candidate builder. Nested returns another
// handle to the same lease; the coordinator sees one active builder until the
// final handle is released.
type BuilderToken struct {
	handle *builderHandle
}

// builderHandle is separately allocated so copying the exported BuilderToken
// preserves one release/nesting identity instead of copying synchronization
// state and decrementing the shared lease twice.
type builderHandle struct {
	mu       sync.Mutex
	lease    *builderLease
	released bool
}

type builderLease struct {
	coordinator *Coordinator
	mu          sync.Mutex
	refs        uint64
	released    bool
}

func (c *Coordinator) AcquireBuilder(ctx context.Context) (*BuilderToken, error) {
	for {
		c.mu.Lock()
		if err := c.terminalErrorLocked(); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if !c.preparing {
			c.activeBuilders++
			lease := &builderLease{coordinator: c, refs: 1}
			c.mu.Unlock()
			return &BuilderToken{handle: &builderHandle{lease: lease}}, nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			// Recheck terminal state so Stop consistently reports ErrClosed and
			// poison retains its recovery error.
		case <-changed:
		}
	}
}

// EnqueueBuilt atomically moves a prepared candidate into pending publication
// and consumes the final builder lease. The handle -> lease -> coordinator lock
// order matches Nested and Release. Validation or activation failure preserves
// both candidate ownership and the live token. A successful return proves that
// ownership moved even when the receipt subsequently requires an admission
// wait.
func (c *Coordinator) EnqueueBuilt(candidate *PreparedRootCandidate, token *BuilderToken) (*BuilderHandoffReceipt, error) {
	if token == nil || token.handle == nil {
		return nil, ErrBuilderLease
	}
	handle := token.handle
	handle.mu.Lock()
	if handle.released || handle.lease == nil {
		handle.mu.Unlock()
		return nil, ErrBuilderLease
	}
	lease := handle.lease
	lease.mu.Lock()
	if lease.released || lease.coordinator != c || lease.refs != 1 {
		lease.mu.Unlock()
		handle.mu.Unlock()
		return nil, ErrBuilderLease
	}

	c.mu.Lock()
	if c.activeBuilders == 0 {
		c.mu.Unlock()
		lease.mu.Unlock()
		handle.mu.Unlock()
		return nil, fmt.Errorf("%w: coordinator has no active builder", ErrBuilderLease)
	}
	decision, err := c.enqueueLocked(candidate, false)
	if err != nil {
		c.mu.Unlock()
		lease.mu.Unlock()
		handle.mu.Unlock()
		return nil, err
	}

	// enqueueLocked has crossed its final fallible step. Consume the final
	// handle and lease before making activeBuilders zero under the same c.mu
	// transition that appended the candidate.
	handle.released = true
	handle.lease = nil
	lease.refs = 0
	lease.released = true
	c.activeBuilders--
	c.notifyLocked()
	c.mu.Unlock()
	lease.mu.Unlock()
	handle.mu.Unlock()
	c.signal()
	return &BuilderHandoffReceipt{
		coordinator: c, sequence: decision.sequence,
		failureGeneration: decision.failureGeneration, hard: decision.hard,
	}, nil
}

// WaitForAdmission completes the optional hard-admission wait associated with
// an accepted builder handoff. Errors from this method are always
// post-acceptance: the coordinator already owns the candidate and its debt.
func (c *Coordinator) WaitForAdmission(ctx context.Context, receipt *BuilderHandoffReceipt) error {
	if receipt == nil || receipt.coordinator != c {
		return ErrBuilderLease
	}
	if !receipt.hard {
		return nil
	}
	return c.waitForAdmission(ctx, receipt.sequence, receipt.failureGeneration)
}

func (t *BuilderToken) Nested() (*BuilderToken, error) {
	if t == nil || t.handle == nil {
		return nil, ErrClosed
	}
	handle := t.handle
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.released || handle.lease == nil {
		return nil, ErrClosed
	}
	lease := handle.lease
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return nil, ErrClosed
	}
	lease.refs++
	return &BuilderToken{handle: &builderHandle{lease: lease}}, nil
}

func (t *BuilderToken) Release() {
	if t == nil || t.handle == nil {
		return
	}
	handle := t.handle
	handle.mu.Lock()
	if handle.released || handle.lease == nil {
		handle.mu.Unlock()
		return
	}
	handle.released = true
	lease := handle.lease
	handle.lease = nil
	handle.mu.Unlock()

	lease.mu.Lock()
	if lease.refs > 0 {
		lease.refs--
	}
	last := lease.refs == 0 && !lease.released
	if last {
		lease.released = true
	}
	lease.mu.Unlock()
	if last {
		lease.coordinator.mu.Lock()
		lease.coordinator.activeBuilders--
		lease.coordinator.notifyLocked()
		lease.coordinator.mu.Unlock()
		lease.coordinator.signal()
	}
}
