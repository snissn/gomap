package rootpublication

import (
	"context"
	"sync"
)

// BuilderToken accounts one active candidate builder. Nested returns another
// handle to the same lease; the coordinator sees one active builder until the
// final handle is released.
type BuilderToken struct {
	lease *builderLease
	once  sync.Once
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
	return &BuilderToken{lease: &builderLease{coordinator: c, refs: 1}}, nil
}

func (t *BuilderToken) Nested() (*BuilderToken, error) {
	if t == nil || t.lease == nil {
		return nil, ErrClosed
	}
	t.lease.mu.Lock()
	defer t.lease.mu.Unlock()
	if t.lease.released {
		return nil, ErrClosed
	}
	t.lease.refs++
	return &BuilderToken{lease: t.lease}, nil
}

func (t *BuilderToken) Release() {
	if t == nil || t.lease == nil {
		return
	}
	t.once.Do(func() {
		lease := t.lease
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
	})
}
