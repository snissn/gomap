package lifecycle

import (
	"math"
	"sync"
)

type ReaderRegistry struct {
	mu      sync.Mutex
	readers map[int64]uint64 // Handle -> CommitSeq
	nextID  int64
}

func NewReaderRegistry() *ReaderRegistry {
	return &ReaderRegistry{
		readers: make(map[int64]uint64),
		nextID:  1,
	}
}

// Register adds a reader pinned to the given sequence.
// Returns a handle to be used for Unregister.
func (r *ReaderRegistry) Register(seq uint64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	id := r.nextID
	r.nextID++
	r.readers[id] = seq
	return id
}

// Unregister removes a reader.
func (r *ReaderRegistry) Unregister(id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.readers, id)
}

// MinPinnedSeq returns the lowest sequence number currently pinned by any reader.
// If no readers are active, returns MaxUint64 (meaning no restriction from readers).
func (r *ReaderRegistry) MinPinnedSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if len(r.readers) == 0 {
		return math.MaxUint64
	}
	
	min := uint64(math.MaxUint64)
	for _, seq := range r.readers {
		if seq < min {
			min = seq
		}
	}
	return min
}
