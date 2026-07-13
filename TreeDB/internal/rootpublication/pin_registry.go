package rootpublication

import (
	"errors"
	"fmt"
	"sync"
)

var ErrUnbalancedResourcePin = errors.New("stable resource release without a matching pin")

// IdentityPinRegistry serializes stable-resource registration against physical
// deletion for every owner of the same open-file identity. Registries are
// explicitly DB-scoped and injected into each producer/deleter; there is no
// process-global lookup by path.
type IdentityPinRegistry struct {
	mu     sync.Mutex
	states map[StableIdentity]*identityPinState
}

type identityPinState struct {
	pins      uint64
	observers uint64
	deleting  bool
	retired   bool
	zero      chan struct{}
}

// IdentityDeleteLease excludes new pins while a deleter closes and unlinks the
// captured identity. End must be called whether deletion succeeds or fails.
type IdentityDeleteLease struct {
	registry *IdentityPinRegistry
	identity StableIdentity
	once     sync.Once
	retired  bool
}

func NewIdentityPinRegistry() *IdentityPinRegistry {
	return &IdentityPinRegistry{states: make(map[StableIdentity]*identityPinState)}
}

func (r *IdentityPinRegistry) Pin(identity StableIdentity) error {
	if r == nil || !identity.valid() {
		return fmt.Errorf("%w: invalid identity pin", ErrInvalidStableResource)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.stateLocked(identity)
	if state.deleting {
		return ErrResourcePinned
	}
	if state.retired {
		return ErrResourceConflict
	}
	if state.pins == 0 {
		state.zero = make(chan struct{})
	}
	state.pins++
	return nil
}

func (r *IdentityPinRegistry) Release(identity StableIdentity) error {
	if r == nil || !identity.valid() {
		return fmt.Errorf("%w: invalid identity release", ErrInvalidStableResource)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.states[identity]
	if state == nil || state.pins == 0 {
		return ErrUnbalancedResourcePin
	}
	state.pins--
	if state.pins == 0 {
		close(state.zero)
		state.zero = nil
		if !state.deleting && !state.retired && state.observers == 0 {
			delete(r.states, identity)
		}
	}
	return nil
}

// Observe records one live producer/deleter handle for identity. Retired
// identities reject new observations until every pre-existing observer has
// closed, preventing a stale descriptor from resurrecting a deleted inode.
func (r *IdentityPinRegistry) Observe(identity StableIdentity) error {
	if r == nil || !identity.valid() {
		return fmt.Errorf("%w: invalid identity observation", ErrInvalidStableResource)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.stateLocked(identity)
	if state.retired || state.deleting {
		return ErrResourceConflict
	}
	state.observers++
	return nil
}

func (r *IdentityPinRegistry) Unobserve(identity StableIdentity) error {
	if r == nil || !identity.valid() {
		return fmt.Errorf("%w: invalid identity unobserve", ErrInvalidStableResource)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.states[identity]
	if state == nil || state.observers == 0 {
		return ErrUnbalancedResourcePin
	}
	state.observers--
	if state.observers == 0 && state.pins == 0 && !state.deleting {
		delete(r.states, identity)
	}
	return nil
}

// BeginDelete atomically excludes later pins, or fails with
// ErrResourcePinned while an existing token still owns the identity.
func (r *IdentityPinRegistry) BeginDelete(identity StableIdentity) (*IdentityDeleteLease, error) {
	if r == nil || !identity.valid() {
		return nil, fmt.Errorf("%w: invalid delete identity", ErrInvalidStableResource)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.stateLocked(identity)
	if state.pins != 0 || state.deleting {
		return nil, ErrResourcePinned
	}
	if state.retired {
		return &IdentityDeleteLease{registry: r, identity: identity, retired: true}, nil
	}
	state.deleting = true
	return &IdentityDeleteLease{registry: r, identity: identity}, nil
}

// WaitUnpinned returns a channel closed after the current pins drain. The
// returned channel is already closed when no pins exist.
func (r *IdentityPinRegistry) WaitUnpinned(identity StableIdentity) <-chan struct{} {
	ready := make(chan struct{})
	if r == nil || !identity.valid() {
		close(ready)
		return ready
	}
	r.mu.Lock()
	state := r.states[identity]
	if state == nil || state.pins == 0 {
		r.mu.Unlock()
		close(ready)
		return ready
	}
	zero := state.zero
	r.mu.Unlock()
	return zero
}

func (r *IdentityPinRegistry) PinCount(identity StableIdentity) uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if state := r.states[identity]; state != nil {
		return state.pins
	}
	return 0
}

func (r *IdentityPinRegistry) stateLocked(identity StableIdentity) *identityPinState {
	if r.states == nil {
		r.states = make(map[StableIdentity]*identityPinState)
	}
	state := r.states[identity]
	if state == nil {
		state = &identityPinState{}
		r.states[identity] = state
	}
	return state
}

// Abort releases an unsuccessful delete attempt and permits later pins.
func (l *IdentityDeleteLease) Abort() {
	if l == nil || l.registry == nil {
		return
	}
	l.once.Do(func() {
		r := l.registry
		r.mu.Lock()
		state := r.states[l.identity]
		if state != nil && !l.retired {
			state.deleting = false
			if state.pins == 0 && state.observers == 0 && !state.retired {
				delete(r.states, l.identity)
			}
		}
		r.mu.Unlock()
	})
}

// CommitDeleted retires the identity. Existing observers may close it, but no
// stale descriptor may register a new token. The tombstone is removed after
// the final observer unregisters.
func (l *IdentityDeleteLease) CommitDeleted() {
	if l == nil || l.registry == nil {
		return
	}
	l.once.Do(func() {
		r := l.registry
		r.mu.Lock()
		state := r.stateLocked(l.identity)
		state.deleting = false
		state.retired = true
		if state.observers == 0 && state.pins == 0 {
			delete(r.states, l.identity)
		}
		r.mu.Unlock()
	})
}
