package mvcc

import (
	"sync"
	"sync/atomic"
)

const maxUint64 = ^uint64(0)

// Registry tracks active reader commit sequences.
type Registry struct {
	currentSeq atomic.Uint64

	mu     sync.Mutex
	pinned map[uint64]uint64
}

func NewRegistry() *Registry {
	return &Registry{
		pinned: make(map[uint64]uint64),
	}
}

func (r *Registry) SetCurrentSeq(seq uint64) {
	if r == nil {
		return
	}
	r.currentSeq.Store(seq)
}

func (r *Registry) Pin(seq uint64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.pinned[seq]++
	r.mu.Unlock()
}

func (r *Registry) Unpin(seq uint64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if n, ok := r.pinned[seq]; ok {
		if n <= 1 {
			delete(r.pinned, seq)
		} else {
			r.pinned[seq] = n - 1
		}
	}
	r.mu.Unlock()
}

// MinPinnedSeq returns the minimum pinned sequence.
// If no readers are active, it returns CurrentSeq+1.
func (r *Registry) MinPinnedSeq() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pinned) == 0 {
		cur := r.currentSeq.Load()
		if cur == maxUint64 {
			return cur
		}
		return cur + 1
	}
	min := maxUint64
	for seq := range r.pinned {
		if seq < min {
			min = seq
		}
	}
	return min
}

