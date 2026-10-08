package rootpublication

import "unsafe"

// StableTerminalDeleteBinding is a call-time exact installed identity/namespace
// input. The constructor copies namespace backing; it retains no input slice,
// request facet, registrar, File, Manager or callback.
type StableTerminalDeleteBinding struct {
	Identity  StableIdentity
	Namespace string
}

type stableTerminalDeleteBinding struct {
	identity  StableIdentity
	namespace string // aliases only the reservation-owned AVL key
}

// StableTerminalDeleteReservation owns precreated namespace claim capacity in
// the SAME registry AVL. It is an immutable set of actual installed bindings,
// not a second deletion gate or an authority inferred from registered counts.
// Every borrower sees the complete retained reservation, including inactive
// claims. Future identities require a new pre-WAL credited constructor.
type StableTerminalDeleteReservation struct {
	registry                    *IdentityPinRegistry
	next                        *StableTerminalDeleteReservation
	bindings                    []stableTerminalDeleteBinding
	creator                     StableMetadataAccount
	borrower                    *stableRegistryBorrower
	controlStamp, bindingsStamp backingStamp
	bytes                       uint64
	closed                      bool // protected by registry.mu
}

// NewStableTerminalDeleteReservation requires actual resident authority and a
// transient cumulative request facet. All bindings and all live borrower
// growth are admitted under registry.mu before any copy/node/control birth.
func (registry *IdentityPinRegistry) NewStableTerminalDeleteReservation(bindings []StableTerminalDeleteBinding, request, resident StableMetadataAccount) (*StableTerminalDeleteReservation, error) {
	if registry == nil || request == nil || resident == nil || len(bindings) == 0 || len(bindings) > int(^uint(0)>>1)/int(unsafe.Sizeof(stableTerminalDeleteBinding{})) {
		return nil, ErrStableMetadataShapeUnsupported
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if !registry.backingCertified {
		return nil, ErrStableMetadataShapeUnsupported
	}
	var plan stableBackingSizePlan
	plan.add(uint64(unsafe.Sizeof(StableTerminalDeleteReservation{})), true)
	plan.add(uint64(len(bindings))*uint64(unsafe.Sizeof(stableTerminalDeleteBinding{})), true)
	for i, binding := range bindings {
		identity, err := validateRegistryIdentity(binding.Identity)
		if err != nil || binding.Namespace == "" {
			return nil, ErrStableMetadataShapeUnsupported
		}
		state := registry.states.get(identity)
		if state == nil || state.observers == 0 || state.retired || state.deleting {
			return nil, ErrResourceConflict
		}
		if registry.namespaces.find(binding.Namespace) != nil {
			return nil, ErrResourceConflict
		}
		if _, known := registry.terminalDeleteNamespaceLocked(identity); known {
			return nil, ErrResourceConflict
		}
		for j := 0; j < i; j++ {
			if physicalStableIdentity(bindings[j].Identity) == identity || bindings[j].Namespace == binding.Namespace {
				return nil, ErrResourceConflict
			}
		}
		n, err := registry.namespaces.plannedInsertBytes(binding.Namespace)
		if err != nil {
			return nil, err
		}
		plan.bytes, plan.err = finiteStableAdd(plan.bytes, n)
		if plan.err != nil {
			return nil, plan.err
		}
	}
	if plan.err != nil {
		return nil, plan.err
	}
	// This reservation reaches the existing registry. Its intrinsic resident
	// creator therefore holds a real same-engine registry loan, including all
	// historical backing and every future growth admission while it survives.
	census, err := registry.retainedBackingCensusLocked()
	if err != nil {
		return nil, err
	}
	loanBytes := census.LiveClassBytes
	if !registry.hasBorrowerLocked(resident) {
		wrapper, err := StableBackingClassBytes(uint64(unsafe.Sizeof(stableRegistryBorrower{})), true)
		if err != nil {
			return nil, err
		}
		loanBytes, err = finiteStableAdd(loanBytes, wrapper)
		if err != nil {
			return nil, err
		}
	}
	sourceBytes, err := finiteStableAdd(plan.bytes, loanBytes)
	if err != nil {
		return nil, err
	}
	if err = request.ReserveStableMetadata(sourceBytes); err != nil {
		return nil, err
	}
	if err = resident.RetainStableMetadata(); err != nil {
		return nil, err
	}
	if err = registry.predebitBorrowersLocked(plan.bytes, resident); err != nil {
		resident.ReleaseStableMetadata()
		return nil, err
	}
	borrower, err := registry.acquireBorrowerWithBackingLocked(resident, plan.bytes, false)
	if err != nil {
		resident.ReleaseStableMetadata()
		return nil, err
	}
	control, _ := prepareBackingStamp(uint64(unsafe.Sizeof(StableTerminalDeleteReservation{})), true, nil)
	array, _ := prepareBackingStamp(uint64(len(bindings))*uint64(unsafe.Sizeof(stableTerminalDeleteBinding{})), true, nil)
	reservation := &StableTerminalDeleteReservation{registry: registry, bindings: make([]stableTerminalDeleteBinding, len(bindings)), creator: resident, borrower: borrower, controlStamp: control, bindingsStamp: array, bytes: plan.bytes}
	registry.backingCensus.add(control)
	registry.backingCensus.add(array)
	for i, binding := range bindings {
		// The same complete class/key plan was validated and prepaid above.
		insert, err := registry.namespaces.prepare(binding.Namespace, false, nil)
		if err != nil {
			panic("prepaid terminal namespace plan changed")
		}
		insert.apply()
		reservation.bindings[i] = stableTerminalDeleteBinding{identity: physicalStableIdentity(binding.Identity), namespace: insert.node.key}
	}
	reservation.next = registry.terminalReservations
	registry.terminalReservations = reservation
	return reservation, nil
}

func (registry *IdentityPinRegistry) terminalNamespaceReservedLocked(namespace string) bool {
	if namespace == "" {
		return false
	}
	for reservation := registry.terminalReservations; reservation != nil; reservation = reservation.next {
		for _, binding := range reservation.bindings {
			if binding.namespace == namespace {
				return true
			}
		}
	}
	return false
}

// MatchesRegistry compares actual retained registry identity, never an address
// converted to a scalar or a newly created registry with matching counts.
func (reservation *StableTerminalDeleteReservation) MatchesRegistry(registry *IdentityPinRegistry) bool {
	return reservation != nil && reservation.registry == registry
}

func (reservation *StableTerminalDeleteReservation) ClassBytes() uint64 {
	if reservation == nil {
		return 0
	}
	return reservation.bytes
}
func (reservation *StableTerminalDeleteReservation) Close() error {
	if reservation == nil || reservation.registry == nil {
		return nil
	}
	registry := reservation.registry
	registry.mu.Lock()
	if reservation.closed {
		registry.mu.Unlock()
		return nil
	}
	for _, binding := range reservation.bindings {
		if registry.namespaces.get(binding.namespace) {
			registry.mu.Unlock()
			return ErrResourceDeletionInProgress
		}
	}
	link := &registry.terminalReservations
	for *link != reservation {
		if *link == nil {
			registry.mu.Unlock()
			return ErrResourceOwnership
		}
		link = &(*link).next
	}
	*link = reservation.next
	for _, binding := range reservation.bindings {
		registry.namespaces.remove(binding.namespace)
	}
	clear(reservation.bindings)
	reservation.bindings = nil
	reservation.next = nil
	reservation.closed = true
	registry.backingCensus.remove(reservation.controlStamp)
	registry.backingCensus.remove(reservation.bindingsStamp)
	creator := reservation.creator
	reservation.creator = nil
	borrower := reservation.borrower
	reservation.borrower = nil
	registry.mu.Unlock()
	borrower.release()
	creator.ReleaseStableMetadata()
	return nil
}

// StableTerminalDeleteLease is an immutable CALLER-OWNED generation value.
// Copying a prior value never observes a later reused plan entry's generation.
// There is no pooled generic *IdentityDeleteLease or reset sync.Once. Its exact
// registry gate and namespace claim are the same as the ordinary lease path.
type StableTerminalDeleteLease struct {
	registry   *IdentityPinRegistry
	identity   StableIdentity
	generation uint64
}

func (reservation *StableTerminalDeleteReservation) PrepareTerminalDeleteAt(identity StableIdentity, namespace string, ownedPins []*IdentityPin) (StableTerminalDeleteLease, error) {
	var lease StableTerminalDeleteLease
	if reservation == nil || reservation.registry == nil {
		return lease, ErrStableMetadataShapeUnsupported
	}
	registry := reservation.registry
	identity, err := validateRegistryIdentity(identity)
	if err != nil {
		return lease, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if reservation.closed {
		return lease, ErrResourceOwnership
	}
	known := false
	for _, binding := range reservation.bindings {
		if binding.identity == identity && binding.namespace == namespace {
			namespace = binding.namespace
			known = true
			break
		}
	}
	if !known {
		return lease, ErrStableMetadataShapeUnsupported
	}
	state := registry.states.get(identity)
	if state == nil || state.retired || state.observers == 0 {
		return lease, ErrResourceConflict
	}
	for i, pin := range ownedPins {
		if pin == nil || pin.registry != registry || pin.identity != identity || pin.released {
			return lease, ErrResourcePinned
		}
		for j := 0; j < i; j++ {
			if ownedPins[j] == pin {
				return lease, ErrResourcePinned
			}
		}
	}
	if state.pins != uint64(len(ownedPins)) || state.deleting || registry.namespaces.get(namespace) {
		return lease, ErrResourcePinned
	}
	if registry.deleteGeneration == ^uint64(0) {
		return lease, ErrStableMetadataShapeUnsupported
	}
	// No allocation, credit callback, copy or constructor is allowed here.
	registry.deleteGeneration++
	state.deleteGeneration = registry.deleteGeneration
	state.deleting = true
	registry.namespaces.find(namespace).value = true
	return StableTerminalDeleteLease{registry: registry, identity: identity, generation: state.deleteGeneration}, nil
}

// Namespace lookup is synchronized and returns only a transient internal alias.
// A copied value retains no reservation control or owned pathname after Close.
func (registry *IdentityPinRegistry) terminalDeleteNamespaceLocked(identity StableIdentity) (string, bool) {
	for reservation := registry.terminalReservations; reservation != nil; reservation = reservation.next {
		for _, binding := range reservation.bindings {
			if binding.identity == identity {
				return binding.namespace, true
			}
		}
	}
	return "", false
}
func (lease StableTerminalDeleteLease) Valid() bool { return lease.registry != nil }
func (lease StableTerminalDeleteLease) CheckDrained() error {
	if lease.registry == nil {
		return ErrResourceOwnership
	}
	registry := lease.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.states.get(lease.identity)
	namespace, known := registry.terminalDeleteNamespaceLocked(lease.identity)
	if !known || state == nil || !state.deleting || state.retired || state.deleteGeneration != lease.generation || !registry.namespaces.get(namespace) {
		return ErrResourceOwnership
	}
	if state.pins != 0 {
		return ErrResourcePinned
	}
	return nil
}
func (lease StableTerminalDeleteLease) Abort()         { lease.finish(false) }
func (lease StableTerminalDeleteLease) CommitDeleted() { lease.finish(true) }
func (lease StableTerminalDeleteLease) finish(deleted bool) {
	if lease.registry == nil {
		return
	}
	registry := lease.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if namespace, known := registry.terminalDeleteNamespaceLocked(lease.identity); known {
		registry.finishIdentityDeleteLocked(lease.identity, namespace, lease.generation, deleted)
	}
}

// StableTerminalRegistryLoan is held only by the real publication member. It
// joins the existing live-borrower list; growth is admitted against the same
// destination before every registry birth. Close scrubs the control before
// releasing that destination. It exports no identity/pin/namespace authority.
type StableTerminalRegistryLoan struct {
	borrower *stableRegistryBorrower
	account  StableMetadataAccount
	stamp    backingStamp
}

func (reservation *StableTerminalDeleteReservation) RetainRegistryLoan(request, resident StableMetadataAccount) (*StableTerminalRegistryLoan, error) {
	if reservation == nil || reservation.registry == nil || request == nil || resident == nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	registry := reservation.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if reservation.closed {
		return nil, ErrResourceOwnership
	}
	census, err := registry.retainedBackingCensusLocked()
	if err != nil {
		return nil, err
	}
	control, err := StableBackingClassBytes(uint64(unsafe.Sizeof(StableTerminalRegistryLoan{})), true)
	if err != nil {
		return nil, err
	}
	source, err := finiteStableAdd(census.LiveClassBytes, control)
	if err != nil {
		return nil, err
	}
	if !registry.hasBorrowerLocked(resident) {
		wrapper, err := StableBackingClassBytes(uint64(unsafe.Sizeof(stableRegistryBorrower{})), true)
		if err != nil {
			return nil, err
		}
		source, err = finiteStableAdd(source, wrapper)
		if err != nil {
			return nil, err
		}
	}
	if err = request.ReserveStableMetadata(source); err != nil {
		return nil, err
	}
	if err = resident.RetainStableMetadata(); err != nil {
		return nil, err
	}
	if err = registry.predebitBorrowersLocked(control, resident); err != nil {
		resident.ReleaseStableMetadata()
		return nil, err
	}
	borrower, err := registry.acquireBorrowerWithBackingLocked(resident, control, false)
	if err != nil {
		resident.ReleaseStableMetadata()
		return nil, err
	}
	stamp, _ := prepareBackingStamp(uint64(unsafe.Sizeof(StableTerminalRegistryLoan{})), true, nil)
	registry.backingCensus.add(stamp)
	return &StableTerminalRegistryLoan{borrower: borrower, account: resident, stamp: stamp}, nil
}
func (loan *StableTerminalRegistryLoan) Close() {
	if loan == nil || loan.borrower == nil {
		return
	}
	borrower, account := loan.borrower, loan.account
	registry := borrower.registry
	registry.mu.Lock()
	registry.backingCensus.remove(loan.stamp)
	loan.borrower = nil
	loan.account = nil
	loan.stamp = backingStamp{}
	registry.mu.Unlock()
	borrower.release()
	account.ReleaseStableMetadata()
}

// RequireBindings is a whole-input pre-WAL check. Existing storage does not
// imply that a newly rotated or foreign-installed segment has prepaid claims.
func (reservation *StableTerminalDeleteReservation) RequireBindings(bindings []StableTerminalDeleteBinding) error {
	if reservation == nil || reservation.registry == nil || len(bindings) == 0 {
		return ErrStableMetadataShapeUnsupported
	}
	registry := reservation.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if reservation.closed {
		return ErrResourceOwnership
	}
	for _, binding := range bindings {
		identity, err := validateRegistryIdentity(binding.Identity)
		if err != nil {
			return err
		}
		known := false
		for _, owned := range reservation.bindings {
			if owned.identity == identity && owned.namespace == binding.Namespace {
				known = true
				break
			}
		}
		if !known {
			return ErrStableMetadataShapeUnsupported
		}
	}
	return nil
}
