package lifecycle

import (
	"math"
	"sync"
	"sync/atomic"
)

const fastReaderHandle int64 = -1

type ReaderRegistry struct {
	mu sync.Mutex

	// seqs stores pinned commit seqs by handle index (handle = idx+1).
	// A value of math.MaxUint64 indicates a free slot.
	seqs []uint64
	free []int

	// min caches the lowest pinned seq when dirty=false. When there are no active
	// readers, min is math.MaxUint64.
	min   uint64
	dirty bool

	// fastSeq/fastCount model the common steady-state case where readers all pin
	// the same sequence. Register/Unregister can avoid mutex contention by using
	// fastReaderHandle in this path.
	fastSeq   atomic.Uint64
	fastCount atomic.Int64
}

func NewReaderRegistry() *ReaderRegistry {
	return &ReaderRegistry{
		min: math.MaxUint64,
	}
}

// Register adds a reader pinned to the given sequence.
// Returns a handle to be used for Unregister.
func (r *ReaderRegistry) Register(seq uint64) int64 {
	if r.fastSeq.Load() == seq {
		r.fastCount.Add(1)
		return fastReaderHandle
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.fastCount.Load() == 0 {
		r.fastSeq.Store(seq)
		r.fastCount.Store(1)
		return fastReaderHandle
	}
	if r.fastSeq.Load() == seq {
		r.fastCount.Add(1)
		return fastReaderHandle
	}

	var idx int
	if n := len(r.free); n > 0 {
		idx = r.free[n-1]
		r.free = r.free[:n-1]
	} else {
		idx = len(r.seqs)
		r.seqs = append(r.seqs, math.MaxUint64)
	}
	r.seqs[idx] = seq
	if seq < r.min {
		r.min = seq
	}
	return int64(idx + 1)
}

// Unregister removes a reader.
func (r *ReaderRegistry) Unregister(id int64) {
	if id == fastReaderHandle {
		for {
			cur := r.fastCount.Load()
			if cur <= 0 {
				return
			}
			if r.fastCount.CompareAndSwap(cur, cur-1) {
				return
			}
		}
	}

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
	if old == math.MaxUint64 {
		return
	}
	r.seqs[idx] = math.MaxUint64
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

	fastMin := uint64(math.MaxUint64)
	if r.fastCount.Load() > 0 {
		fastMin = r.fastSeq.Load()
	}

	if !r.dirty {
		if fastMin < r.min {
			return fastMin
		}
		return r.min
	}

	min := uint64(math.MaxUint64)
	for _, seq := range r.seqs {
		if seq < min {
			min = seq
		}
	}
	r.min = min
	r.dirty = false
	if fastMin < min {
		return fastMin
	}
	return min
}
