package rootpublication

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

var (
	ErrUnbalancedResourcePin      = errors.New("stable resource release without a matching pin")
	ErrResourceDeletionInProgress = errors.New("stable resource deletion in progress")
)

// IdentityPinRegistry is a DB-scoped gate shared by every producer and
// deleter that can reach the same physical files. Logical generations are not
// part of this key: deleting an inode must be blocked by every alias of it.
type IdentityPinRegistry struct {
	mu         sync.Mutex
	states     map[StableIdentity]*identityPinState
	namespaces map[string]bool
	activePins uint64
}

type identityPinState struct {
	pins      uint64
	observers uint64
	deleting  bool
	retired   bool
	zero      chan struct{}
}

// IdentityDeleteLease excludes later pins until deletion commits or aborts.
type IdentityDeleteLease struct {
	registry  *IdentityPinRegistry
	identity  StableIdentity
	namespace string
	once      sync.Once
}

// IdentityPin owns one registry reference and is safe to release repeatedly.
type IdentityPin struct {
	registry *IdentityPinRegistry
	identity StableIdentity
	once     sync.Once
}

func NewIdentityPinRegistry() *IdentityPinRegistry {
	return &IdentityPinRegistry{
		states:     make(map[StableIdentity]*identityPinState),
		namespaces: make(map[string]bool),
	}
}

func physicalStableIdentity(identity StableIdentity) StableIdentity {
	identity.Generation = 0
	return identity
}

// StableIdentityFromFile captures the physical identity of an already-open
// handle. Callers must not substitute a pathname lookup: the pathname can be
// rebound between discovery and deletion.
func StableIdentityFromFile(file *os.File) (StableIdentity, error) {
	if file == nil {
		return StableIdentity{}, fmt.Errorf("%w: nil stable resource handle", ErrUnresolvedResource)
	}
	return stableIdentityFromFile(file)
}

// SamePhysicalIdentity reports whether two identities name the same physical
// object, deliberately ignoring their logical generations.
func SamePhysicalIdentity(left, right StableIdentity) bool {
	left, leftErr := validateRegistryIdentity(left)
	right, rightErr := validateRegistryIdentity(right)
	return leftErr == nil && rightErr == nil && left == right
}

func validateRegistryIdentity(identity StableIdentity) (StableIdentity, error) {
	identity = physicalStableIdentity(identity)
	if !identity.valid() {
		return StableIdentity{}, fmt.Errorf("%w: invalid physical identity", ErrUnresolvedResource)
	}
	return identity, nil
}

func (registry *IdentityPinRegistry) Pin(identity StableIdentity) (*IdentityPin, error) {
	identity, err := validateRegistryIdentity(identity)
	if err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.stateLocked(identity)
	if state.deleting {
		return nil, ErrResourceDeletionInProgress
	}
	if state.retired {
		return nil, ErrResourceConflict
	}
	state.pins++
	registry.activePins++
	return &IdentityPin{registry: registry, identity: identity}, nil
}

func (registry *IdentityPinRegistry) release(identity StableIdentity) error {
	identity, err := validateRegistryIdentity(identity)
	if err != nil {
		return err
	}
	if registry == nil {
		return fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.states[identity]
	if state == nil || state.pins == 0 {
		return ErrUnbalancedResourcePin
	}
	state.pins--
	registry.activePins--
	if state.pins == 0 {
		if state.zero != nil {
			close(state.zero)
			state.zero = nil
		}
		registry.deleteIdleStateLocked(identity, state)
	}
	return nil
}

func (pin *IdentityPin) Release() {
	if pin == nil || pin.registry == nil {
		return
	}
	pin.once.Do(func() { _ = pin.registry.release(pin.identity) })
}

func (registry *IdentityPinRegistry) Observe(identity StableIdentity) error {
	identity, err := validateRegistryIdentity(identity)
	if err != nil {
		return err
	}
	if registry == nil {
		return fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.stateLocked(identity)
	if state.retired || state.deleting {
		return ErrResourceConflict
	}
	state.observers++
	return nil
}

func (registry *IdentityPinRegistry) Unobserve(identity StableIdentity) error {
	identity, err := validateRegistryIdentity(identity)
	if err != nil {
		return err
	}
	if registry == nil {
		return fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.states[identity]
	if state == nil || state.observers == 0 {
		return ErrUnbalancedResourcePin
	}
	state.observers--
	registry.deleteIdleStateLocked(identity, state)
	return nil
}

func (registry *IdentityPinRegistry) BeginDelete(identity StableIdentity) (*IdentityDeleteLease, error) {
	return registry.beginDelete(identity, "")
}

func (registry *IdentityPinRegistry) BeginDeleteAt(identity StableIdentity, namespace string) (*IdentityDeleteLease, error) {
	if namespace == "" {
		return nil, fmt.Errorf("%w: empty delete namespace", ErrUnresolvedResource)
	}
	return registry.beginDelete(identity, namespace)
}

func (registry *IdentityPinRegistry) beginDelete(identity StableIdentity, namespace string) (*IdentityDeleteLease, error) {
	identity, err := validateRegistryIdentity(identity)
	if err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.stateLocked(identity)
	if state.pins != 0 || state.deleting || (namespace != "" && registry.namespaces[namespace]) {
		return nil, ErrResourcePinned
	}
	if state.retired {
		return nil, ErrResourceConflict
	}
	state.deleting = true
	if namespace != "" {
		registry.namespaces[namespace] = true
	}
	return &IdentityDeleteLease{registry: registry, identity: identity, namespace: namespace}, nil
}

func (registry *IdentityPinRegistry) WaitUnpinned(identity StableIdentity) <-chan struct{} {
	ready := make(chan struct{})
	identity, err := validateRegistryIdentity(identity)
	if registry == nil || err != nil {
		close(ready)
		return ready
	}
	registry.mu.Lock()
	state := registry.states[identity]
	if state == nil || state.pins == 0 {
		registry.mu.Unlock()
		close(ready)
		return ready
	}
	if state.zero == nil {
		state.zero = make(chan struct{})
	}
	zero := state.zero
	registry.mu.Unlock()
	return zero
}

func (registry *IdentityPinRegistry) PinCount(identity StableIdentity) uint64 {
	identity, err := validateRegistryIdentity(identity)
	if registry == nil || err != nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if state := registry.states[identity]; state != nil {
		return state.pins
	}
	return 0
}

func (registry *IdentityPinRegistry) ActivePins() uint64 {
	if registry == nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.activePins
}

// ActiveIdentities reports live registry state for leak/stress gates.
func (registry *IdentityPinRegistry) ActiveIdentities() int {
	if registry == nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return len(registry.states)
}

func (registry *IdentityPinRegistry) stateLocked(identity StableIdentity) *identityPinState {
	if registry.states == nil {
		registry.states = make(map[StableIdentity]*identityPinState)
	}
	state := registry.states[identity]
	if state == nil {
		state = &identityPinState{}
		registry.states[identity] = state
	}
	return state
}

func (registry *IdentityPinRegistry) deleteIdleStateLocked(identity StableIdentity, state *identityPinState) {
	if state != nil && state.pins == 0 && state.observers == 0 && !state.deleting {
		delete(registry.states, identity)
	}
}

func (lease *IdentityDeleteLease) Abort() {
	if lease == nil || lease.registry == nil {
		return
	}
	lease.once.Do(func() {
		registry := lease.registry
		registry.mu.Lock()
		state := registry.states[lease.identity]
		if state != nil {
			state.deleting = false
			registry.deleteIdleStateLocked(lease.identity, state)
		}
		delete(registry.namespaces, lease.namespace)
		registry.mu.Unlock()
	})
}

func (lease *IdentityDeleteLease) CommitDeleted() {
	if lease == nil || lease.registry == nil {
		return
	}
	lease.once.Do(func() {
		registry := lease.registry
		registry.mu.Lock()
		state := registry.stateLocked(lease.identity)
		state.deleting = false
		state.retired = true
		registry.deleteIdleStateLocked(lease.identity, state)
		delete(registry.namespaces, lease.namespace)
		registry.mu.Unlock()
	})
}

// Commit is the deletion-owner spelling used after a successful unlink.
func (lease *IdentityDeleteLease) Commit() { lease.CommitDeleted() }
