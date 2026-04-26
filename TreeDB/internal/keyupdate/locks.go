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

// Lock locks the stripe for key and returns its unlock function. Hash collisions
// only reduce concurrency; they do not affect correctness.
func (l *Locks) Lock(key []byte) func() {
	mu := l.LockMutex(key)
	return mu.Unlock
}

// LockMutex locks the stripe for key and returns the locked mutex. It avoids
// allocating an unlock closure on hot paths that can call Unlock directly.
func (l *Locks) LockMutex(key []byte) *sync.Mutex {
	idx := xxhash.Sum64(key) & (stripes - 1)
	mu := &l.stripes[idx]
	mu.Lock()
	return mu
}
