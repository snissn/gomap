package rootpublication

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"unsafe"
)

var (
	ErrUnbalancedResourcePin      = errors.New("stable resource release without a matching pin")
	ErrResourceDeletionInProgress = errors.New("stable resource deletion in progress")
)

// IdentityPinRegistry is a DB-scoped gate shared by every producer and
// deleter that can reach the same physical files. Logical generations are not
// part of this key: deleting an inode must be blocked by every alias of it.
type IdentityPinRegistry struct {
	mu                   sync.Mutex
	backingCensus        BackingCensus
	directoryBirths      BackingCensus
	backingCertified     bool
	states               stableTable[StableIdentity, *identityPinState]
	namespaces           stableTable[string, bool]
	stableLinks          stableTable[stableNamespaceLink, struct{}]
	stableDirectoryLinks stableTable[stableNamespaceLink, stableDirectoryLinkAuthority]
	deleteGeneration     uint64
	activePins           uint64
	borrowers            *stableRegistryBorrower
	terminalReservations *StableTerminalDeleteReservation
	ready                chan struct{}
}

// IdentityPinRegistryStats is an atomic snapshot of the live physical-identity
// and namespace-proof state retained by one DB-scoped registry.
type IdentityPinRegistryStats struct {
	ActivePins                 uint64
	ActiveIdentities           int
	ActiveStableNamespaceLinks int
}

// stableNamespaceLink records that an exact parent/child/name binding has
// already survived the required parent-directory sync. Pathnames alone are
// insufficient because either component can be rebound between appends.
type stableNamespaceLink struct {
	parent StableIdentity
	child  StableIdentity
	name   string
}

// stableDirectoryLinkAuthority keeps both physical identities alive for as
// long as a directory-ancestry sync proof may be reused. Numeric file
// identities alone are insufficient because the filesystem may recycle them
// after deletion.
type stableDirectoryLinkAuthority struct {
	parent *os.File
	child  *os.File
	census BackingCensus
}

func (authority stableDirectoryLinkAuthority) close() {
	if authority.child != nil {
		_ = authority.child.Close()
	}
	if authority.parent != nil {
		_ = authority.parent.Close()
	}
}

type identityPinState struct {
	stamp            backingStamp
	metadataAccount  StableMetadataAccount
	zeroStamp        backingStamp
	pins             uint64
	observers        uint64
	deleteGeneration uint64
	deleting         bool
	retired          bool
	zero             chan struct{}
}

// IdentityDeleteLease excludes later pins until deletion commits or aborts.
type IdentityDeleteLease struct {
	registry        *IdentityPinRegistry
	identity        StableIdentity
	namespace       string
	generation      uint64
	metadataAccount StableMetadataAccount
	active          bool // protected by registry.mu
	once            sync.Once
}

// IdentityPin owns one registry reference and is safe to release repeatedly.
type IdentityPin struct {
	released bool // protected by registry.mu; terminal reservation membership
	registry *IdentityPinRegistry
	identity StableIdentity
	once     sync.Once
}

func NewIdentityPinRegistry() *IdentityPinRegistry {
	registry := &IdentityPinRegistry{
		states:               newStableIdentityTable(),
		namespaces:           newStableNamespaceTable(),
		stableLinks:          newStableLinkTable[struct{}](),
		stableDirectoryLinks: newStableLinkTable[stableDirectoryLinkAuthority](),
	}
	registry.backingCertified = finiteStablePlatform() == nil
	if stamp, err := prepareBackingStamp(uint64(unsafe.Sizeof(IdentityPinRegistry{})), true, nil); err == nil {
		registry.backingCensus.add(stamp)
	}
	registry.ready = make(chan struct{})
	close(registry.ready)
	if stamp, err := prepareBackingStamp(104, true, nil); err == nil {
		registry.backingCensus.add(stamp)
	}
	return registry
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
	state := registry.states.get(identity)
	if state == nil {
		return nil, ErrResourceConflict
	}
	if state.deleting {
		return nil, ErrResourceDeletionInProgress
	}
	if state.observers == 0 {
		return nil, ErrResourceConflict
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
	return registry.releaseLocked(identity)
}

// releaseLocked is shared by concrete pin controls and the checked ordinary
// release path. Every pin-state change, including terminal membership, uses mu.
func (registry *IdentityPinRegistry) releaseLocked(identity StableIdentity) error {
	state := registry.states.get(identity)
	if state == nil || state.pins == 0 {
		return ErrUnbalancedResourcePin
	}
	state.pins--
	registry.activePins--
	if state.pins == 0 {
		if state.zero != nil {
			close(state.zero)
			state.zero = nil
			registry.backingCensus.remove(state.zeroStamp)
			state.zeroStamp = backingStamp{}
		}
		registry.deleteIdleStateLocked(identity, state)
	}
	return nil
}

func (pin *IdentityPin) Release() {
	if pin == nil || pin.registry == nil {
		return
	}
	pin.once.Do(func() {
		registry := pin.registry
		registry.mu.Lock()
		defer registry.mu.Unlock()
		if pin.released {
			return
		}
		pin.released = true
		_ = registry.releaseLocked(pin.identity)
	})
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
	if registry.states.less == nil {
		registry.states = newStableIdentityTable()
	}
	prepared, err := registry.prepareObserveLocked(identity, nil, nil)
	if err != nil {
		return err
	}
	prepared.apply()
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
	state := registry.states.get(identity)
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
	return registry.beginDeleteLocked(identity, namespace, nil, nil)
}

func (registry *IdentityPinRegistry) beginDeleteLocked(identity StableIdentity, namespace string, ownedPins []*IdentityPin, account StableMetadataAccount) (*IdentityDeleteLease, error) {
	state := registry.states.get(identity)
	for i, pin := range ownedPins {
		if pin == nil || pin.registry != registry || pin.identity != identity || pin.released {
			return nil, ErrResourcePinned
		}
		for j := 0; j < i; j++ {
			if ownedPins[j] == pin {
				return nil, ErrResourcePinned
			}
		}
	}
	if state != nil && (state.pins != uint64(len(ownedPins)) || state.deleting) || state == nil && len(ownedPins) != 0 || namespace != "" && registry.namespaces.get(namespace) {
		return nil, ErrResourcePinned
	}
	if state != nil && state.retired {
		return nil, ErrResourceConflict
	}
	if registry.namespaces.less == nil {
		registry.namespaces = newStableNamespaceTable()
	}
	n, err := registry.namespaces.plannedInsertBytes(namespace)
	if namespace == "" {
		n = 0
	}
	if err != nil {
		return nil, err
	}
	if state == nil {
		if registry.states.less == nil {
			registry.states = newStableIdentityTable()
		}
		b, e := StableBackingClassBytes(uint64(unsafe.Sizeof(identityPinState{})), true)
		if e != nil && registry.borrowers != nil {
			return nil, e
		}
		key, e := registry.states.plannedInsertBytes(identity)
		if e != nil && registry.borrowers != nil {
			return nil, e
		}
		growth, e := finiteStableAdd(b, key)
		if e != nil {
			return nil, e
		}
		n, err = finiteStableAdd(n, growth)
		if err != nil {
			return nil, err
		}
	}
	if registry.deleteGeneration == ^uint64(0) {
		return nil, ErrStableMetadataShapeUnsupported
	}
	if account != nil {
		leaseBytes, e := StableBackingClassBytes(uint64(unsafe.Sizeof(IdentityDeleteLease{})), true)
		if e != nil {
			return nil, e
		}
		if e = account.ReserveStableMetadata(leaseBytes); e != nil {
			return nil, e
		}
		if e = account.RetainStableMetadata(); e != nil {
			return nil, e
		}
	}
	if err = registry.predebitBorrowersLocked(n, nil); err != nil {
		if account != nil {
			account.ReleaseStableMetadata()
		}
		return nil, err
	}
	// Every retained allocation is admitted before deletion state is changed.
	if state == nil {
		state = registry.stateLocked(identity)
	}
	if namespace != "" {
		registry.namespaces.set(namespace, true)
	}
	registry.deleteGeneration++
	state.deleteGeneration = registry.deleteGeneration
	state.deleting = true
	return &IdentityDeleteLease{registry: registry, identity: identity, namespace: namespace, generation: state.deleteGeneration, metadataAccount: account, active: true}, nil
}

// WaitUnpinned returns nil if a live finite borrower refuses channel growth.
// A refused channel cannot report that a still-pinned identity is ready.
func (registry *IdentityPinRegistry) WaitUnpinned(identity StableIdentity) <-chan struct{} {
	ready, _ := registry.WaitUnpinnedChecked(identity)
	return ready
}

// WaitUnpinnedChecked preserves an explicit refusal for consumers that can retry.
func (registry *IdentityPinRegistry) WaitUnpinnedChecked(identity StableIdentity) (<-chan struct{}, error) {
	identity, err := validateRegistryIdentity(identity)
	if registry == nil || err != nil {
		ready := make(chan struct{})
		close(ready)
		return ready, nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.states.get(identity)
	if state == nil || state.pins == 0 {
		if registry.ready == nil {
			b, e := StableBackingClassBytes(104, true)
			if e != nil && registry.borrowers != nil {
				return nil, e
			}
			if e = registry.predebitBorrowersLocked(b, nil); e != nil {
				return nil, e
			}
			registry.ready = make(chan struct{})
			close(registry.ready)
			stamp, e := prepareBackingStamp(104, true, nil)
			if e != nil {
				registry.backingCertified = false
			}
			registry.backingCensus.add(stamp)
		}
		return registry.ready, nil
	}
	if state.zero == nil {
		// Go1.26.3 amd64 runtime.hchan is 104 bytes, independently rounded.
		b, e := StableBackingClassBytes(104, true)
		if e != nil && registry.borrowers != nil {
			return nil, e
		}
		if e = registry.predebitBorrowersLocked(b, nil); e != nil {
			return nil, e
		}
		stamp, e := prepareBackingStamp(104, true, nil)
		if e != nil {
			registry.backingCertified = false
		}
		state.zero = make(chan struct{})
		state.zeroStamp = stamp
		registry.backingCensus.add(stamp)
	}
	return state.zero, nil
}

func (registry *IdentityPinRegistry) PinCount(identity StableIdentity) uint64 {
	identity, err := validateRegistryIdentity(identity)
	if registry == nil || err != nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if state := registry.states.get(identity); state != nil {
		return state.pins
	}
	return 0
}

// ObserverCount reports producer observations for one exact physical identity.
// It is intentionally identity-scoped so lifecycle checks do not confuse a
// resource with unrelated long-lived DB observers sharing this registry.
func (registry *IdentityPinRegistry) ObserverCount(identity StableIdentity) uint64 {
	identity, err := validateRegistryIdentity(identity)
	if registry == nil || err != nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if state := registry.states.get(identity); state != nil {
		return state.observers
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
	return registry.states.count
}

// Stats reports one internally consistent snapshot for lifecycle and leak
// gates. Prefer this to reading the individual counters when producers may be
// shutting down concurrently.
func (registry *IdentityPinRegistry) Stats() IdentityPinRegistryStats {
	if registry == nil {
		return IdentityPinRegistryStats{}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return IdentityPinRegistryStats{
		ActivePins:                 registry.activePins,
		ActiveIdentities:           registry.states.count,
		ActiveStableNamespaceLinks: registry.stableLinks.count,
	}
}

func stableNamespaceLinkFromFiles(parent, child *os.File, name string) (stableNamespaceLink, error) {
	if name == "" {
		return stableNamespaceLink{}, fmt.Errorf("%w: empty stable child name", ErrUnresolvedResource)
	}
	parentIdentity, err := StableIdentityFromFile(parent)
	if err != nil {
		return stableNamespaceLink{}, err
	}
	childIdentity, err := StableIdentityFromFile(child)
	if err != nil {
		return stableNamespaceLink{}, err
	}
	parentIdentity, err = validateRegistryIdentity(parentIdentity)
	if err != nil {
		return stableNamespaceLink{}, err
	}
	childIdentity, err = validateRegistryIdentity(childIdentity)
	if err != nil {
		return stableNamespaceLink{}, err
	}
	return stableNamespaceLink{parent: parentIdentity, child: childIdentity, name: name}, nil
}

// StableNamespaceLinkKnown reports whether this exact physical parent/child
// binding has already been made namespace-durable by this DB instance.
func (registry *IdentityPinRegistry) StableNamespaceLinkKnown(parent, child *os.File, name string) (bool, error) {
	if registry == nil {
		return false, fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	link, err := stableNamespaceLinkFromFiles(parent, child, name)
	if err != nil {
		return false, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	_, known := registry.stableLinks.lookup(link)
	return known, nil
}

// StableDirectoryLinkKnown reports whether this exact physical
// parent/child/name directory binding has already survived the platform's
// create-persistence operation. Each proof retains duplicate handles so its
// physical identities cannot be deleted and reused while sync elision remains
// possible. Directory-ancestry proofs are deliberately separate from
// stableLinks: they are a namespace-setup cache, not live publication resources
// owned by rollback and retirement.
func (registry *IdentityPinRegistry) StableDirectoryLinkKnown(parent, child *os.File, name string) (bool, error) {
	if registry == nil {
		return false, fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	link, err := stableNamespaceLinkFromFiles(parent, child, name)
	if err != nil {
		return false, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	_, known := registry.stableDirectoryLinks.lookup(link)
	return known, nil
}

// RememberStableDirectoryLink records a directory-ancestry proof only after
// the exact platform persistence handle has been synchronized. Rebinding a
// name to another child invalidates the previous proof for that parent/name.
func (registry *IdentityPinRegistry) RememberStableDirectoryLink(parent, child *os.File, name string) error {
	if registry == nil {
		return fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.stableDirectoryLinks.less == nil {
		registry.stableDirectoryLinks = newStableLinkTable[stableDirectoryLinkAuthority]()
	}
	if err := registry.predebitLinkBirthLocked(parent, child, name, true); err != nil {
		return err
	}
	link, err := stableNamespaceLinkFromFiles(parent, child, name)
	if err != nil {
		return err
	}
	retainedParent, err := duplicateStableFile(parent)
	if err != nil {
		return fmt.Errorf("retain stable directory parent: %w", err)
	}
	// Even a failed child dup retains the successful parent attempt in the census.
	var parentBacking backingLayout
	parentBacking.file(retainedParent)
	registry.directoryBirths.addGroup(parentBacking.census.AllocatedClassBytes, parentBacking.census.Allocations)
	retainedChild, err := duplicateStableFile(child)
	if err != nil {
		_ = retainedParent.Close()
		return fmt.Errorf("retain stable directory child: %w", err)
	}
	var childBacking backingLayout
	childBacking.file(retainedChild)
	registry.directoryBirths.addGroup(childBacking.census.AllocatedClassBytes, childBacking.census.Allocations)
	retained := stableDirectoryLinkAuthority{parent: retainedParent, child: retainedChild}
	var retainedBacking backingLayout
	retainedBacking.merge(parentBacking.census)
	retainedBacking.merge(childBacking.census)
	retained.census = retainedBacking.census
	if _, known := registry.stableDirectoryLinks.lookup(link); known {
		retained.close()
		return nil
	}
	insert, err := registry.stableDirectoryLinks.prepare(link, retained, nil)
	if err != nil {
		retained.close()
		return err
	}
	registry.stableDirectoryLinks.removeWhere(func(prior stableNamespaceLink, authority stableDirectoryLinkAuthority) bool {
		remove := prior.parent == link.parent && prior.name == link.name && prior.child != link.child
		if remove {
			authority.close()
		}
		return remove
	})
	insert.apply()
	return nil
}

// NewStableNamespaceTokenForKnownLink binds a publication token to an exact
// parent/child/name link whose namespace durability was already established by
// this registry. The registry proof is scoped to physical identities, while
// ParentGeneration remains the caller's logical publication generation.
//
// The returned token starts stable and performs no additional directory sync.
// This is only valid while the caller retains the exact child against deletion;
// a pathname lookup is never accepted as a substitute for the open handles.
func (registry *IdentityPinRegistry) NewStableNamespaceTokenForKnownLink(spec StableNamespaceSpec) (*StableNamespaceToken, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	if spec.LinkedResource == nil {
		return nil, fmt.Errorf("%w: known stable namespace link requires exact child handle", ErrUnresolvedResource)
	}
	link, err := stableNamespaceLinkFromFiles(spec.Parent, spec.LinkedResource, spec.NewName)
	if err != nil {
		return nil, err
	}
	registry.mu.Lock()
	_, known := registry.stableLinks.lookup(link)
	registry.mu.Unlock()
	if !known {
		return nil, fmt.Errorf("%w: exact namespace link is not known durable", ErrNamespaceUnstable)
	}
	token, err := newStableNamespaceToken(spec, nativeNamespaceAdapter{})
	if err != nil {
		return nil, err
	}
	token.state.Store(namespaceStable)
	return token, nil
}

// RememberStableNamespaceLink records proof only after the exact parent handle
// has been synced while the exact child binding was validated.
func (registry *IdentityPinRegistry) RememberStableNamespaceLink(parent, child *os.File, name string) error {
	if registry == nil {
		return fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.stableLinks.less == nil {
		registry.stableLinks = newStableLinkTable[struct{}]()
	}
	if err := registry.predebitLinkBirthLocked(parent, child, name, false); err != nil {
		return err
	}
	link, err := stableNamespaceLinkFromFiles(parent, child, name)
	if err != nil {
		return err
	}
	insert, err := registry.stableLinks.prepare(link, struct{}{}, nil)
	if err != nil {
		return err
	}
	registry.stableLinks.removeWhere(func(prior stableNamespaceLink, _ struct{}) bool {
		return prior.parent == link.parent && prior.name == link.name && prior.child != link.child
	})
	insert.apply()
	return nil
}

// ForgetStableNamespaceLink discards only the proof for this exact physical
// binding. A rebound child with the same pathname remains a distinct key.
func (registry *IdentityPinRegistry) ForgetStableNamespaceLink(parent, child *os.File, name string) error {
	if registry == nil {
		return fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	link, err := stableNamespaceLinkFromFiles(parent, child, name)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	registry.stableLinks.remove(link)
	registry.mu.Unlock()
	return nil
}

// ForgetStableNamespaceLinkIdentity is the post-close companion to
// ForgetStableNamespaceLink. Callers must have captured both identities from
// the exact handles before closing them.
func (registry *IdentityPinRegistry) ForgetStableNamespaceLinkIdentity(parent, child StableIdentity, name string) error {
	if registry == nil {
		return fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	parent, err := validateRegistryIdentity(parent)
	if err != nil {
		return err
	}
	child, err = validateRegistryIdentity(child)
	if err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("%w: empty stable child name", ErrUnresolvedResource)
	}
	registry.mu.Lock()
	registry.stableLinks.remove(stableNamespaceLink{parent: parent, child: child, name: name})
	registry.mu.Unlock()
	return nil
}

// ActiveStableNamespaceLinks reports retained identity-keyed sync proofs for
// leak and stress gates.
func (registry *IdentityPinRegistry) ActiveStableNamespaceLinks() int {
	if registry == nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.stableLinks.count
}

// CachedStableDirectoryLinks reports the DB-lifetime ancestry proofs retained
// as a sync-elision cache. These entries are not live resource ownership and
// are intentionally excluded from Stats and ActiveStableNamespaceLinks.
func (registry *IdentityPinRegistry) CachedStableDirectoryLinks() int {
	if registry == nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.stableDirectoryLinks.count
}

// ClearStableNamespaceLinks retires DB-lifetime namespace-sync proofs and
// directory-ancestry setup proofs during producer shutdown. Exact resource
// tokens retain their own parent handles and remain usable; a later DB instance
// must establish fresh namespace evidence.
func (registry *IdentityPinRegistry) ClearStableNamespaceLinks() {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	registry.stableLinks.clear()
	registry.stableDirectoryLinks.visit(func(_ stableNamespaceLink, authority stableDirectoryLinkAuthority) bool {
		authority.close()
		return true
	})
	registry.stableDirectoryLinks.clear()
	registry.mu.Unlock()
}

func (registry *IdentityPinRegistry) stateLocked(identity StableIdentity) *identityPinState {
	if registry.states.less == nil {
		registry.states = newStableIdentityTable()
	}
	state := registry.states.get(identity)
	if state == nil {
		stamp, err := prepareBackingStamp(uint64(unsafe.Sizeof(identityPinState{})), true, nil)
		if err != nil {
			registry.backingCertified = false
		}
		state = &identityPinState{stamp: stamp}
		registry.backingCensus.add(stamp)
		registry.states.set(identity, state)
	}
	return state
}

func (registry *IdentityPinRegistry) deleteIdleStateLocked(identity StableIdentity, state *identityPinState) {
	if state != nil && state.pins == 0 && state.observers == 0 && !state.deleting {
		registry.states.remove(identity)
		registry.backingCensus.remove(state.stamp)
		if state.metadataAccount != nil {
			account := state.metadataAccount
			state.metadataAccount = nil
			account.ReleaseStableMetadata()
		}
	}
}

// finishIdentityDeleteLocked is the same generation gate for ordinary leases
// and constructor-prepared terminal values. Reserved namespace nodes stay in
// the existing AVL with value false between uses; their actual backing remains
// visible to registry borrowers until the exact reservation closes.
func (registry *IdentityPinRegistry) finishIdentityDeleteLocked(identity StableIdentity, namespace string, generation uint64, deleted bool) bool {
	state := registry.states.get(identity)
	if state == nil || !state.deleting || state.deleteGeneration != generation {
		return false
	}
	if deleted && state.pins != 0 {
		panic("stable deletion committed with live pins")
	}
	state.deleting = false
	if deleted {
		state.retired = true
		registry.stableLinks.removeWhere(func(link stableNamespaceLink, _ struct{}) bool { return link.child == identity })
	}
	registry.deleteIdleStateLocked(identity, state)
	if registry.terminalNamespaceReservedLocked(namespace) {
		registry.namespaces.find(namespace).value = false
	} else {
		registry.namespaces.remove(namespace)
	}
	return true
}

func (lease *IdentityDeleteLease) Abort()         { lease.finish(false) }
func (lease *IdentityDeleteLease) CommitDeleted() { lease.finish(true) }
func (lease *IdentityDeleteLease) finish(deleted bool) {
	if lease == nil || lease.registry == nil {
		return
	}
	lease.once.Do(func() {
		registry := lease.registry
		registry.mu.Lock()
		if !lease.active || !registry.finishIdentityDeleteLocked(lease.identity, lease.namespace, lease.generation, deleted) {
			registry.mu.Unlock()
			return
		}
		lease.active = false
		account := lease.metadataAccount
		lease.metadataAccount = nil
		registry.mu.Unlock()
		if account != nil {
			account.ReleaseStableMetadata()
		}
	})
}

// Commit is the deletion-owner spelling used after a successful unlink.
func (lease *IdentityDeleteLease) Commit() { lease.CommitDeleted() }

func stableStringLess(a, b string) bool { return a < b }
func stableIdentityLess(a, b StableIdentity) bool {
	if a.Platform != b.Platform {
		return a.Platform < b.Platform
	}
	if a.VolumeID != b.VolumeID {
		return a.VolumeID < b.VolumeID
	}
	for i := range a.ObjectID {
		if a.ObjectID[i] != b.ObjectID[i] {
			return a.ObjectID[i] < b.ObjectID[i]
		}
	}
	return a.Generation < b.Generation
}
func stableNamespaceLinkLess(a, b stableNamespaceLink) bool {
	if a.parent != b.parent {
		return stableIdentityLess(a.parent, b.parent)
	}
	if a.name != b.name {
		return a.name < b.name
	}
	return stableIdentityLess(a.child, b.child)
}

func newStableIdentityTable() stableTable[StableIdentity, *identityPinState] {
	t := newStableTable[StableIdentity, *identityPinState](stableIdentityLess)
	t.keyPlan = func(k StableIdentity) (uint64, uint64, error) { return backingStringPlan(k.Platform) }
	t.ownKey = func(k StableIdentity) StableIdentity { k.Platform = finiteStableCopy(k.Platform); return k }
	return t
}
func newStableNamespaceTable() stableTable[string, bool] {
	t := newStableTable[string, bool](stableStringLess)
	t.keyPlan = func(k string) (uint64, uint64, error) { return backingStringPlan(k) }
	t.ownKey = finiteStableCopy
	return t
}
func newStableLinkTable[V any]() stableTable[stableNamespaceLink, V] {
	t := newStableTable[stableNamespaceLink, V](stableNamespaceLinkLess)
	t.keyPlan = func(k stableNamespaceLink) (uint64, uint64, error) {
		return backingStringPlan(k.parent.Platform, k.child.Platform, k.name)
	}
	t.ownKey = func(k stableNamespaceLink) stableNamespaceLink {
		k.parent.Platform = finiteStableCopy(k.parent.Platform)
		k.child.Platform = finiteStableCopy(k.child.Platform)
		k.name = finiteStableCopy(k.name)
		return k
	}
	return t
}

// RetainedBackingCensus exports independent numbers, never a table/control view.
// Unknown fabricated registries cannot supply allocation provenance for loans.
func (registry *IdentityPinRegistry) RetainedBackingCensus() (BackingCensus, error) {
	if registry == nil {
		return BackingCensus{}, ErrResourceOwnership
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.retainedBackingCensusLocked()
}

func (registry *IdentityPinRegistry) retainedBackingCensusLocked() (BackingCensus, error) {
	if !registry.backingCertified {
		return BackingCensus{}, ErrStableMetadataShapeUnsupported
	}
	var p backingLayout
	p.merge(registry.backingCensus)
	p.merge(registry.states.census)
	p.merge(registry.namespaces.census)
	p.merge(registry.stableLinks.census)
	p.merge(registry.stableDirectoryLinks.census)
	p.merge(BackingCensus{AllocatedClassBytes: registry.directoryBirths.AllocatedClassBytes, Allocations: registry.directoryBirths.Allocations})
	registry.stableDirectoryLinks.visit(func(_ stableNamespaceLink, a stableDirectoryLinkAuthority) bool {
		live := a.census
		live.AllocatedClassBytes, live.Allocations = 0, 0
		p.merge(live)
		return true
	})
	return p.census, p.err
}

// preparedStableRegistryObserve is held only by the concrete producer while the
// registry lock remains held. It prepays state/node/owned-key backing as one
// operation and retains the supplied resident destination before any allocation.
type preparedStableRegistryObserve struct {
	registry    *IdentityPinRegistry
	state       *identityPinState
	insert      stableTableInsert[StableIdentity, *identityPinState]
	newState    bool
	destination StableMetadataAccount
}

func (registry *IdentityPinRegistry) prepareObserveLocked(identity StableIdentity, operation, destination StableMetadataAccount) (preparedStableRegistryObserve, error) {
	var p preparedStableRegistryObserve
	state := registry.states.get(identity)
	if state != nil {
		if state.retired || state.deleting {
			return p, ErrResourceConflict
		}
		return preparedStableRegistryObserve{registry: registry, state: state}, nil
	}
	if operation != nil && destination == nil {
		return p, ErrStableMetadataShapeUnsupported
	}
	stateBytes, err := StableBackingClassBytes(uint64(unsafe.Sizeof(identityPinState{})), true)
	if err != nil && operation != nil {
		return p, err
	}
	nodeBytes, err := registry.states.plannedInsertBytes(identity)
	if err != nil && operation != nil {
		return p, err
	}
	total, err := finiteStableAdd(stateBytes, nodeBytes)
	if err != nil {
		return p, err
	}
	if err = registry.predebitBorrowersLocked(total, nil); err != nil {
		return p, err
	}
	if operation != nil && !registry.hasBorrowerLocked(operation) {
		if err = operation.ReserveStableMetadata(total); err != nil {
			return p, err
		}
	}
	if destination != nil {
		if err = destination.ReserveStableMetadata(total); err != nil {
			return p, err
		}
		if err = destination.RetainStableMetadata(); err != nil {
			return p, err
		}
	}
	stamp, err := prepareBackingStamp(uint64(unsafe.Sizeof(identityPinState{})), true, nil)
	if err != nil {
		if operation != nil {
			if destination != nil {
				destination.ReleaseStableMetadata()
			}
			return p, err
		}
		registry.backingCertified = false
	}
	state = &identityPinState{stamp: stamp, metadataAccount: destination}
	insert, err := registry.states.prepare(identity, state, nil)
	if err != nil {
		if destination != nil {
			destination.ReleaseStableMetadata()
		}
		return p, err
	}
	return preparedStableRegistryObserve{registry: registry, state: state, insert: insert, newState: true, destination: destination}, nil
}
func (p *preparedStableRegistryObserve) apply() {
	if p.newState {
		p.insert.apply()
		p.registry.backingCensus.add(p.state.stamp)
	}
	p.state.observers++
	// The real state now owns destination retention through its last actual pin/
	// observation/delete reference. No additional reserve or retain occurs here.
	*p = preparedStableRegistryObserve{}
}
func (p *preparedStableRegistryObserve) abort() {
	if p == nil {
		return
	}
	if p.newState && p.destination != nil {
		p.destination.ReleaseStableMetadata()
	}
	*p = preparedStableRegistryObserve{}
}

// The platform key length is intrinsic, so this complete plan precedes Stat,
// copies, FD duplication and removal of any prior namespace binding. A known
// key conservatively consumes the attempted plan as well; no debit is refunded.
func (r *IdentityPinRegistry) predebitLinkBirthLocked(parent, child *os.File, name string, directory bool) error {
	if r.borrowers == nil {
		return nil
	}
	if parent == nil || child == nil || name == "" {
		return ErrUnresolvedResource
	}
	var p stableBackingSizePlan
	// linux amd64 os.fileStat, two exact-handle identity queries.
	p.add(200, true)
	p.add(200, true)
	if directory {
		p.add(uint64(unsafe.Sizeof(stableTableNode[stableNamespaceLink, stableDirectoryLinkAuthority]{})), true)
	} else {
		p.add(uint64(unsafe.Sizeof(stableTableNode[stableNamespaceLink, struct{}]{})), true)
	}
	p.string(runtime.GOOS)
	p.string(runtime.GOOS)
	p.string(name)
	if p.err != nil {
		return p.err
	}
	n := p.bytes
	if directory {
		a, err := finiteStableHandleBytes(parent)
		if err != nil {
			return err
		}
		b, err := finiteStableHandleBytes(child)
		if err != nil {
			return err
		}
		n, err = finiteStableAdd(n, a)
		if err != nil {
			return err
		}
		n, err = finiteStableAdd(n, b)
		if err != nil {
			return err
		}
	}
	return r.predebitBorrowersLocked(n, nil)
}

// PrepareTerminalDeleteAt reserves the existing deletion gate while exactly
// these concrete group-owned pins remain live. The slice is used only under mu
// and never retained. New foreign pins/observations are excluded; ordinary
// Release drains the verified pins without changing deletion authority.
func (registry *IdentityPinRegistry) PrepareTerminalDeleteAt(identity StableIdentity, namespace string, ownedPins []*IdentityPin, account StableMetadataAccount) (*IdentityDeleteLease, error) {
	identity, err := validateRegistryIdentity(identity)
	if err != nil {
		return nil, err
	}
	if registry == nil || namespace == "" || account == nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.beginDeleteLocked(identity, namespace, ownedPins, account)
}

// CheckDrained must precede physical close/unlink. A reservation grants no
// destructive authority until its exact group-owned pin closure has drained.
func (lease *IdentityDeleteLease) CheckDrained() error {
	if lease == nil || lease.registry == nil {
		return ErrResourceOwnership
	}
	registry := lease.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.states.get(lease.identity)
	if !lease.active || state == nil || state.deleteGeneration != lease.generation || !state.deleting || state.retired || lease.namespace != "" && !registry.namespaces.get(lease.namespace) {
		return ErrResourceOwnership
	}
	if state.pins != 0 {
		return ErrResourcePinned
	}
	return nil
}
