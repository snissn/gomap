package rootpublication

import (
	"context"
	"sync"
)

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
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.terminalErrorLocked(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.activeBuilders++
	lease := &builderLease{coordinator: c, refs: 1}
	return &BuilderToken{handle: &builderHandle{lease: lease}}, nil
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
