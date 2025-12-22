package lifecycle

import (
	"math"
	"sync"
)

type ReaderRegistry struct {
	mu sync.Mutex

	// seqs stores pinned commit seqs by handle index (handle = idx+1).
	// A value of 0 indicates a free slot.
	seqs []uint64
	free []int

	// min caches the lowest pinned seq when dirty=false. When there are no active
	// readers, min is math.MaxUint64.
	min   uint64
	dirty bool
}

func NewReaderRegistry() *ReaderRegistry {
	return &ReaderRegistry{
		min: math.MaxUint64,
	}
}

// Register adds a reader pinned to the given sequence.
// Returns a handle to be used for Unregister.
func (r *ReaderRegistry) Register(seq uint64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	var idx int
	if n := len(r.free); n > 0 {
		idx = r.free[n-1]
		r.free = r.free[:n-1]
	} else {
		idx = len(r.seqs)
		r.seqs = append(r.seqs, 0)
	}
	r.seqs[idx] = seq
	if seq < r.min {
		r.min = seq
	}
	return int64(idx + 1)
}

// Unregister removes a reader.
func (r *ReaderRegistry) Unregister(id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if id <= 0 {
		return
	}
	idx := int(id - 1)
	if idx < 0 || idx >= len(r.seqs) {
		return
	}
	old := r.seqs[idx]
	if old == 0 {
		return
	}
	r.seqs[idx] = 0
	r.free = append(r.free, idx)
	if old == r.min {
		r.dirty = true
	}
}

// MinPinnedSeq returns the lowest sequence number currently pinned by any reader.
// If no readers are active, returns MaxUint64 (meaning no restriction from readers).
func (r *ReaderRegistry) MinPinnedSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.dirty {
		return r.min
	}

	min := uint64(math.MaxUint64)
	for _, seq := range r.seqs {
		if seq != 0 && seq < min {
			min = seq
		}
	}
	r.min = min
	r.dirty = false
	if min == math.MaxUint64 {
		return math.MaxUint64
	}
	return min
}
