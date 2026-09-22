package valuelog

import (
	"sync"
	"time"
)

// VirtualClock provides deterministic time control for tests.
type VirtualClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewVirtualClock(start time.Time) *VirtualClock {
	return &VirtualClock{now: start}
}

func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *VirtualClock) Advance(ns int64) {
	if ns == 0 {
		return
	}
	c.mu.Lock()
	c.now = c.now.Add(time.Duration(ns))
	c.mu.Unlock()
}
