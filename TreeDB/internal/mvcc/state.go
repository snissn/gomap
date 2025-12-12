package mvcc

import (
	"errors"
	"sync/atomic"

	"treedb/internal/page"
	"treedb/internal/slab"
)

// DBState is an immutable view of the database at a commit sequence.
// Writers publish a new DBState by atomic swap.
type DBState struct {
	CommitSeq        uint64
	UserRootPageID   page.PageID
	SystemRootPageID page.PageID
	SlabSet          *slab.SlabSet
}

var ErrNoState = errors.New("mvcc: no state published")

// StateHolder publishes DBState and owns the reader registry.
type StateHolder struct {
	ptr      atomic.Pointer[DBState]
	registry *Registry
}

func NewStateHolder(initial *DBState) *StateHolder {
	h := &StateHolder{
		registry: NewRegistry(),
	}
	if initial != nil {
		h.ptr.Store(initial)
		h.registry.SetCurrentSeq(initial.CommitSeq)
	}
	return h
}

// Load returns the currently published state.
func (h *StateHolder) Load() *DBState {
	if h == nil {
		return nil
	}
	return h.ptr.Load()
}

// Publish atomically swaps in a new state.
func (h *StateHolder) Publish(s *DBState) {
	if h == nil || s == nil {
		panic("mvcc: publish nil")
	}
	h.ptr.Store(s)
	h.registry.SetCurrentSeq(s.CommitSeq)
}

func (h *StateHolder) Registry() *Registry {
	if h == nil {
		return nil
	}
	return h.registry
}

// AcquireSnapshot captures the current state once and pins slabs and sequence.
func (h *StateHolder) AcquireSnapshot() (*Snapshot, error) {
	if h == nil {
		return nil, ErrNoState
	}
	st := h.ptr.Load()
	if st == nil {
		return nil, ErrNoState
	}
	seq := st.CommitSeq
	h.registry.Pin(seq)

	var pinned []*slab.SlabFile
	if st.SlabSet != nil {
		for _, id := range st.SlabSet.IDs() {
			f, ok := st.SlabSet.Get(id)
			if !ok || f == nil {
				continue
			}
			f.Pin()
			pinned = append(pinned, f)
		}
	}
	return &Snapshot{state: st, registry: h.registry, slabs: pinned}, nil
}

// Snapshot is a pinned, immutable view of DBState.
type Snapshot struct {
	state    *DBState
	registry *Registry
	slabs    []*slab.SlabFile
	closed   atomic.Bool
}

func (s *Snapshot) State() *DBState {
	if s == nil {
		return nil
	}
	return s.state
}

// Close releases slab pins and deregisters the reader sequence.
func (s *Snapshot) Close() error {
	if s == nil {
		return nil
	}
	if s.closed.Swap(true) {
		return nil
	}
	var firstErr error
	for _, f := range s.slabs {
		if f == nil {
			continue
		}
		if err := f.Unpin(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.registry != nil && s.state != nil {
		s.registry.Unpin(s.state.CommitSeq)
	}
	return firstErr
}

