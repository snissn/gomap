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
	idx := xxhash.Sum64(key) & (stripes - 1)
	mu := &l.stripes[idx]
	mu.Lock()
	return mu.Unlock
}
