package rootpublication

import (
	"sort"
	"sync"
	"time"
)

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(d time.Duration) Timer {
	return realTimer{timer: time.NewTimer(d)}
}

type realTimer struct{ timer *time.Timer }

func (t realTimer) C() <-chan time.Time { return t.timer.C }
func (t realTimer) Stop() bool          { return t.timer.Stop() }

// FakeClock is a deterministic clock seam for scheduler and service-time
// tests. Advance synchronously fires every timer due at the resulting time.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]struct{}
	nextID uint64
}

func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now, timers: make(map[*fakeTimer]struct{})}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	t := &fakeTimer{clock: c, when: c.now.Add(d), ch: make(chan time.Time, 1), active: true, id: c.nextID}
	c.timers[t] = struct{}{}
	return t
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	due := c.takeDueTimersLocked(now)
	c.mu.Unlock()
	for _, timer := range due {
		timer.ch <- timer.when
	}
}

func (c *FakeClock) takeDueTimersLocked(now time.Time) []*fakeTimer {
	var due []*fakeTimer
	for timer := range c.timers {
		if timer.active && !timer.when.After(now) {
			timer.active = false
			delete(c.timers, timer)
			due = append(due, timer)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].when.Equal(due[j].when) {
			return due[i].when.Before(due[j].when)
		}
		return due[i].id < due[j].id
	})
	return due
}

type fakeTimer struct {
	clock  *FakeClock
	when   time.Time
	ch     chan time.Time
	active bool
	id     uint64
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	if t.active {
		t.active = false
		delete(t.clock.timers, t)
	}
	return wasActive
}
