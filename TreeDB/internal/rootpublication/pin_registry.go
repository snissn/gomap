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
	mu                   sync.Mutex
	states               map[StableIdentity]*identityPinState
	namespaces           map[string]bool
	stableLinks          map[stableNamespaceLink]struct{}
	stableDirectoryLinks map[stableNamespaceLink]stableDirectoryLinkAuthority
	activePins           uint64
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
		states:               make(map[StableIdentity]*identityPinState),
		namespaces:           make(map[string]bool),
		stableLinks:          make(map[stableNamespaceLink]struct{}),
		stableDirectoryLinks: make(map[stableNamespaceLink]stableDirectoryLinkAuthority),
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
	state := registry.states[identity]
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
	if state := registry.states[identity]; state != nil {
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
	return len(registry.states)
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
		ActiveIdentities:           len(registry.states),
		ActiveStableNamespaceLinks: len(registry.stableLinks),
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
	_, known := registry.stableLinks[link]
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
	_, known := registry.stableDirectoryLinks[link]
	return known, nil
}

// RememberStableDirectoryLink records a directory-ancestry proof only after
// the exact platform persistence handle has been synchronized. Rebinding a
// name to another child invalidates the previous proof for that parent/name.
func (registry *IdentityPinRegistry) RememberStableDirectoryLink(parent, child *os.File, name string) error {
	if registry == nil {
		return fmt.Errorf("%w: nil identity pin registry", ErrUnresolvedResource)
	}
	link, err := stableNamespaceLinkFromFiles(parent, child, name)
	if err != nil {
		return err
	}
	retainedParent, err := duplicateStableFile(parent)
	if err != nil {
		return fmt.Errorf("retain stable directory parent: %w", err)
	}
	retainedChild, err := duplicateStableFile(child)
	if err != nil {
		_ = retainedParent.Close()
		return fmt.Errorf("retain stable directory child: %w", err)
	}
	retained := stableDirectoryLinkAuthority{parent: retainedParent, child: retainedChild}
	var retired []stableDirectoryLinkAuthority
	registry.mu.Lock()
	if registry.stableDirectoryLinks == nil {
		registry.stableDirectoryLinks = make(map[stableNamespaceLink]stableDirectoryLinkAuthority)
	}
	if _, known := registry.stableDirectoryLinks[link]; known {
		registry.mu.Unlock()
		retained.close()
		return nil
	}
	for prior, authority := range registry.stableDirectoryLinks {
		if prior.parent == link.parent && prior.name == link.name && prior.child != link.child {
			delete(registry.stableDirectoryLinks, prior)
			retired = append(retired, authority)
		}
	}
	registry.stableDirectoryLinks[link] = retained
	registry.mu.Unlock()
	for _, authority := range retired {
		authority.close()
	}
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
	_, known := registry.stableLinks[link]
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
	link, err := stableNamespaceLinkFromFiles(parent, child, name)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	if registry.stableLinks == nil {
		registry.stableLinks = make(map[stableNamespaceLink]struct{})
	}
	for prior := range registry.stableLinks {
		if prior.parent == link.parent && prior.name == link.name && prior.child != link.child {
			delete(registry.stableLinks, prior)
		}
	}
	registry.stableLinks[link] = struct{}{}
	registry.mu.Unlock()
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
	delete(registry.stableLinks, link)
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
	delete(registry.stableLinks, stableNamespaceLink{parent: parent, child: child, name: name})
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
	return len(registry.stableLinks)
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
	return len(registry.stableDirectoryLinks)
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
	clear(registry.stableLinks)
	authorities := make([]stableDirectoryLinkAuthority, 0, len(registry.stableDirectoryLinks))
	for link, authority := range registry.stableDirectoryLinks {
		authorities = append(authorities, authority)
		delete(registry.stableDirectoryLinks, link)
	}
	registry.mu.Unlock()
	for _, authority := range authorities {
		authority.close()
	}
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
		for link := range registry.stableLinks {
			if link.child == lease.identity {
				delete(registry.stableLinks, link)
			}
		}
		registry.deleteIdleStateLocked(lease.identity, state)
		delete(registry.namespaces, lease.namespace)
		registry.mu.Unlock()
	})
}

// Commit is the deletion-owner spelling used after a successful unlink.
func (lease *IdentityDeleteLease) Commit() { lease.CommitDeleted() }
