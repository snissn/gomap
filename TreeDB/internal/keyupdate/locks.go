package keyupdate

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

const stripes = 256

// Locks coordinates single-key read-modify-write operations. The zero value is
// ready for use.
type Locks struct {
	stripes [stripes]sync.Mutex
}

// Guard holds a locked update stripe. It is returned by value so callers can
// preserve defer-based panic safety without allocating an unlock closure.
type Guard struct {
	locks *Locks
	index uint64
}

// Unlock releases the stripe held by g. The zero value is a no-op guard.
func (g Guard) Unlock() {
	if g.locks == nil {
		return
	}
	g.locks.stripes[g.index].Unlock()
}

// Lock locks the stripe for key and returns a guard that unlocks it. Hash
// collisions only reduce concurrency; they do not affect correctness.
func (l *Locks) Lock(key []byte) Guard {
	idx := xxhash.Sum64(key) & (stripes - 1)
	l.stripes[idx].Lock()
	return Guard{locks: l, index: idx}
}
