package db

import (
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"unsafe"
)

// This owner is attached only to the existing runtime. Its immutable envelope
// bounds all later ordinary/finite member and seal additions. Request callbacks
// are transient; creator credit belongs to the runtime, never its first member.
// This is preparation storage, not a complete finite constructor certificate.
type rootPublicationTerminalArenaV1 struct {
	scratch                                 *rootpublication.StableTerminalScratch
	managerStorage                          *valuelog.StableSegmentTerminalStorage
	creator                                 rootpublication.StableMetadataAccount
	members, seals, debt, resources, prefix int
	bytes                                   uint64
	loans                                   int
	closed                                  bool
}

type rootPublicationTerminalLoanV1 struct {
	arena           *rootPublicationTerminalArenaV1
	account         rootpublication.StableMetadataAccount
	adopted, closed bool
	registryLoan    *rootpublication.StableTerminalRegistryLoan
}

func terminalArenaEnvelopeV1(l *PreparedRootPublicationLimits) (rootpublication.StableTerminalCapacity, error) {
	if l == nil || l.MaxVisibleMembers <= 0 || l.MaxSeals <= 0 || l.MaxAllocatorDebt <= 0 || l.MaxVisibleResources <= 0 || l.MaxSealPrefixEntries <= 0 {
		return rootpublication.StableTerminalCapacity{}, rootpublication.ErrStableMetadataShapeUnsupported
	}
	max := int(^uint(0) >> 1)
	// Publication needs M pending + one overwritten slot + 2(S-1) retired
	// seal roles + M previous member owners. Shutdown also includes both
	// durable slots, one pending and one ambiguous candidate (two roles each),
	// the runtime visible owner, both member roles, and M recovery roles.
	// Poison prevents a second ambiguous birth; admission rejects existing
	// ambiguous authority before creating this immutable envelope.
	if l.MaxSeals > (max-7)/2 || l.MaxVisibleMembers > (max-7-2*l.MaxSeals)/3 {
		return rootpublication.StableTerminalCapacity{}, rootpublication.ErrStableMetadataShapeUnsupported
	}
	roles := 3*l.MaxVisibleMembers + 2*l.MaxSeals + 7
	loose := l.MaxSeals + 2
	if roles > (max-loose)/l.MaxVisibleResources {
		return rootpublication.StableTerminalCapacity{}, rootpublication.ErrStableMetadataShapeUnsupported
	}
	return rootpublication.StableTerminalCapacity{Roles: roles, ExtraRoles: roles, LooseTokens: loose, Tokens: roles*l.MaxVisibleResources + loose}, nil
}

// terminalArenaClassBytesV1 counts the constructor's actual distinct backing
// classes. Registry histories/reservations and member-loan controls are separate
// real loans; callers must add them and cannot treat this inventory as full fit.
func terminalArenaClassBytesV1(l *PreparedRootPublicationLimits) (uint64, error) {
	c, err := terminalArenaEnvelopeV1(l)
	if err != nil {
		return 0, err
	}
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(rootPublicationTerminalArenaV1{})), true)
	if err != nil {
		return 0, err
	}
	scratch, err := rootpublication.StableTerminalScratchClassBytes(c)
	if err != nil {
		return 0, err
	}
	storage, err := valuelog.StableSegmentTerminalStorageClassBytes(c.Tokens, c.Tokens*2)
	if err != nil || scratch > ^uint64(0)-control || storage > ^uint64(0)-control-scratch {
		return 0, rootpublication.ErrStableMetadataShapeUnsupported
	}
	return control + scratch + storage, nil
}

func (a *rootPublicationTerminalArenaV1) fits(l *PreparedRootPublicationLimits) bool {
	return a != nil && !a.closed && l != nil && a.members == l.MaxVisibleMembers && a.seals == l.MaxSeals && a.debt == l.MaxAllocatorDebt && a.resources == l.MaxVisibleResources && a.prefix == l.MaxSealPrefixEntries
}

// The runtime mutex protects the loan count and all actual horizon fields.
func (runtime *rootPublicationRuntimeV1) checkTerminalEnvelopeLocked(resources *rootpublication.StableResourceSet, memberBirth, sealBirth bool) error {
	a := runtime.terminalArena
	if a == nil {
		return nil
	}
	if a.closed || len(runtime.visibleMembers) > a.members || len(runtime.seals) > a.seals || len(runtime.debt) > a.debt || memberBirth && (len(runtime.visibleMembers) >= a.members || len(runtime.debt) >= a.debt) || sealBirth && (len(runtime.seals) >= a.seals || len(runtime.debt) >= a.debt || len(runtime.debt) >= a.prefix) {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	check := func(s *rootpublication.StableResourceSet) bool { return s == nil || s.Len() <= a.resources }
	if !check(resources) || !check(runtime.visibleResources) {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	for _, m := range runtime.visibleMembers {
		if !check(m.resources) || !check(m.previousResources) {
			return rootpublication.ErrStableMetadataShapeUnsupported
		}
	}
	for _, s := range runtime.seals {
		if !check(s.resources) || !check(s.overwrittenResources) {
			return rootpublication.ErrStableMetadataShapeUnsupported
		}
	}
	return nil
}

// Called under the actual raw/write/commit serializer BEFORE append. Nothing
// here consumes a command identity, a page, a resource or an allocator prefix.
// Independent account callbacks cannot reenter the engine. A destination
// rejection never refunds the successful cumulative request debit.
func (db *DB) admitRootPublicationTerminalV1(limits *PreparedRootPublicationLimits, request, creator, borrower rootpublication.StableMetadataAccount) (*PreparedRootPublicationLimits, *rootPublicationTerminalLoanV1, error) {
	return db.admitRootPublicationTerminalWithBindingsV1(limits, request, creator, borrower, nil, false)
}

func (db *DB) admitRootPublicationTerminalWithBindingsV1(limits *PreparedRootPublicationLimits, request, creator, borrower rootpublication.StableMetadataAccount, bindings []rootpublication.StableTerminalDeleteBinding, requireBindings bool) (*PreparedRootPublicationLimits, *rootPublicationTerminalLoanV1, error) {
	if limits == nil || request == nil || creator == nil || borrower == nil || db.rootPublication == nil {
		return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	if requireBindings && (len(bindings) == 0 || db.valueLogManager == nil) {
		return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	capacity, err := terminalArenaEnvelopeV1(limits)
	if err != nil {
		return nil, nil, err
	}
	runtime := db.rootPublication
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.poison != nil {
		return nil, nil, runtime.poison
	}
	// Validate the entire existing closure before any credit or constructor.
	if a := runtime.terminalArena; a != nil && !a.fits(limits) {
		return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	for _, s := range db.durableRoot.slotResources {
		if s != nil && s.Len() > limits.MaxVisibleResources {
			return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
	}
	if db.durableRoot.pending != nil || len(db.durableRoot.ambiguous) != 0 || len(runtime.visibleMembers) >= limits.MaxVisibleMembers || len(runtime.seals) >= limits.MaxSeals || len(runtime.debt) >= limits.MaxAllocatorDebt {
		return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	for _, m := range runtime.visibleMembers {
		if m.resources != nil && m.resources.Len() > limits.MaxVisibleResources || m.previousResources != nil && m.previousResources.Len() > limits.MaxVisibleResources {
			return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
	}
	for _, s := range runtime.seals {
		if s.resources != nil && s.resources.Len() > limits.MaxVisibleResources || s.overwrittenResources != nil && s.overwrittenResources.Len() > limits.MaxVisibleResources {
			return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
	}
	if runtime.terminalArena == nil {
		if requireBindings && (len(bindings) == 0 || db.valueLogManager == nil) {
			return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
		}
		if runtime.terminalReporting {
			return nil, nil, rootpublication.ErrResourceOwnership
		}
		control, e := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(rootPublicationTerminalArenaV1{})), true)
		if e != nil {
			return nil, nil, e
		}
		if e = request.ReserveStableMetadata(control); e != nil {
			return nil, nil, e
		}
		if e = creator.ReserveStableMetadata(control); e != nil {
			return nil, nil, e
		}
		if e = creator.RetainStableMetadata(); e != nil {
			return nil, nil, e
		}
		scratch, e := rootpublication.NewStableTerminalScratch(capacity, request, creator)
		if e != nil {
			creator.ReleaseStableMetadata()
			return nil, nil, e
		}
		var reservation *rootpublication.StableTerminalDeleteReservation
		if requireBindings {
			reservation, e = db.valueLogManager.NewStableTerminalDeleteReservation(bindings, request, creator)
			if e != nil {
				_ = scratch.Close()
				creator.ReleaseStableMetadata()
				return nil, nil, e
			}
		}
		var storage *valuelog.StableSegmentTerminalStorage
		if reservation != nil {
			storage, e = valuelog.NewStableSegmentTerminalStorageWithReservation(capacity.Tokens, capacity.Tokens*2, reservation, request, creator)
		} else {
			storage, e = valuelog.NewStableSegmentTerminalStorage(capacity.Tokens, capacity.Tokens*2, request, creator)
		}
		if e != nil {
			if reservation != nil {
				_ = reservation.Close()
			}
			_ = scratch.Close()
			creator.ReleaseStableMetadata()
			return nil, nil, e
		}
		runtime.terminalArena = &rootPublicationTerminalArenaV1{scratch: scratch, managerStorage: storage, creator: creator, members: limits.MaxVisibleMembers, seals: limits.MaxSeals, debt: limits.MaxAllocatorDebt, resources: limits.MaxVisibleResources, prefix: limits.MaxSealPrefixEntries, bytes: control + scratch.ClassBytes() + storage.ClassBytes()}
	}
	a := runtime.terminalArena
	if requireBindings {
		if err := db.valueLogManager.ValidateStableTerminalDeleteBindings(a.managerStorage, bindings); err != nil {
			return nil, nil, err
		}
	}
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(rootPublicationTerminalLoanV1{})), true)
	if err != nil {
		return nil, nil, err
	}
	limitsClass, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(PreparedRootPublicationLimits{})), true)
	if err != nil || control > ^uint64(0)-limitsClass || a.bytes > ^uint64(0)-control-limitsClass {
		return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	if err = request.ReserveStableMetadata(a.bytes + control + limitsClass); err != nil {
		return nil, nil, err
	}
	if err = borrower.ReserveStableMetadata(a.bytes + control + limitsClass); err != nil {
		return nil, nil, err
	}
	if err = borrower.RetainStableMetadata(); err != nil {
		return nil, nil, err
	}
	var registryLoan *rootpublication.StableTerminalRegistryLoan
	if requireBindings {
		registryLoan, err = a.managerStorage.RetainRegistryLoan(request, borrower)
		if err != nil {
			borrower.ReleaseStableMetadata()
			return nil, nil, err
		}
	}
	loan := &rootPublicationTerminalLoanV1{arena: a, account: borrower, registryLoan: registryLoan}
	local := *limits
	local.terminalLoan = loan
	a.loans++
	return &local, loan, nil
}

// Call with runtime.mu held. Only exact member/operation terminal completion
// closes this loan; the first creator member cannot retire arena backing.
func (loan *rootPublicationTerminalLoanV1) closeLocked() {
	if loan == nil || loan.closed {
		return
	}
	a := loan.arena
	if a == nil || a.loans <= 0 {
		panic("terminal arena loan imbalance")
	}
	a.loans--
	account := loan.account
	registryLoan := loan.registryLoan
	loan.account = nil
	loan.arena = nil
	loan.registryLoan = nil
	loan.closed = true
	if registryLoan != nil {
		registryLoan.Close()
	}
	account.ReleaseStableMetadata()
}
func (runtime *rootPublicationRuntimeV1) closeTerminalArenaLocked() error {
	a := runtime.terminalArena
	if a == nil {
		return nil
	}
	if a.loans != 0 || runtime.terminalReporting {
		return rootpublication.ErrResourceOwnership
	}
	if err := a.managerStorage.Close(); err != nil {
		return err
	}
	if err := a.scratch.Close(); err != nil {
		return err
	}
	a.managerStorage = nil
	a.scratch = nil
	a.closed = true
	creator := a.creator
	a.creator = nil
	runtime.terminalArena = nil
	creator.ReleaseStableMetadata()
	return nil
}
func (runtime *rootPublicationRuntimeV1) TerminalScratch() *rootpublication.StableTerminalScratch {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.terminalArena == nil {
		return nil
	}
	return runtime.terminalArena.scratch
}
