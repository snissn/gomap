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

// Unlocker releases a lock acquired by Locks.Lock.
type Unlocker struct {
	mu *sync.Mutex
}

// Unlock releases the lock acquired by Locks.Lock.
func (u Unlocker) Unlock() {
	if u.mu == nil {
		return
	}
	u.mu.Unlock()
}

// Lock locks the stripe for key and returns its unlock handle. Hash collisions
// only reduce concurrency; they do not affect correctness.
func (l *Locks) Lock(key []byte) Unlocker {
	idx := xxhash.Sum64(key) & (stripes - 1)
	mu := &l.stripes[idx]
	mu.Lock()
	return Unlocker{mu: mu}
}
