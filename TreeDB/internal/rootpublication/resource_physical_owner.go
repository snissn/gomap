package rootpublication

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// StableSegmentOwner owns the exact immutable handle/namespace backing of one
// segment. It is ordinary producer backing, not a registry or a handle pool.
// Per-frontier tokens retain this SAME owner and their own identity pin.
// Accounted creation is refused until a real resident destination is supplied.
type StableSegmentOwner struct {
	mu       sync.Mutex
	refs     atomic.Int64
	released atomic.Bool
	token    *StableResourceToken
	census   BackingCensus
	resident StableMetadataAccount
}

// NewStableSegmentOwner consumes an ordinary exact token. Generic accounted
// tokens, callbacks, directory/obligation backing and unknown constructor
// provenance cannot become permanent writer backing.
func NewStableSegmentOwner(token *StableResourceToken) (*StableSegmentOwner, error) {
	if token == nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.callbackBacked || token.callbackCreator != nil || token.activeOperations != 0 || token.releasePending || token.cleanupRunning || token.cleanupUncertain || token.cleanupComplete || token.released.Load() || token.metadataAccount != nil || !token.wholeBackingOwned || token.directory != nil || len(token.logicalObligations) != 0 || token.frontier.exactRIDs != nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	if token.namespace != nil {
		if _, native := token.namespace.adapter.(nativeNamespaceAdapter); !native || token.namespace.metadataAccount != nil {
			return nil, ErrStableMetadataShapeUnsupported
		}
	}
	stamp, e := prepareBackingStamp(uint64(unsafe.Sizeof(StableSegmentOwner{})), true, nil)
	if e != nil {
		return nil, e
	}
	owner := &StableSegmentOwner{token: token}
	if e = token.transferLocked(ResourceOwnerToken, ResourceOwnerShared); e != nil {
		return nil, e
	}
	// The producer observation, exact FD and namespace authority keep the source
	// alive; per-frontier outputs each hold the normal deletion-exclusion pin.
	if token.identityPin != nil {
		token.identityPin.Release()
		token.identityPin = nil
		if token.backingCertified {
			b, _ := StableBackingClassBytes(uint64(unsafe.Sizeof(IdentityPin{})), true)
			token.backingCensus.LiveClassBytes -= b
			token.backingCensus.LiveAllocations--
		}
	}
	owner.census = token.backingCensus
	if token.namespace != nil {
		var p backingLayout
		p.merge(owner.census)
		p.merge(token.namespace.backingCensus)
		owner.census = p.census
	}
	owner.census.add(stamp)
	owner.refs.Store(1)
	return owner, nil
}
func (o *StableSegmentOwner) RetainedBackingCensus() (BackingCensus, error) {
	if o == nil {
		return BackingCensus{}, ErrResourceOwnership
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.token == nil || o.refs.Load() == 0 {
		return BackingCensus{}, ErrResourceOwnership
	}
	if !o.token.backingCertified || o.token.namespace != nil && !o.token.namespace.backingCertified {
		return BackingCensus{}, ErrStableMetadataShapeUnsupported
	}
	return o.census, nil
}

// Capture returns an owned per-frontier token, exposing no handle/control view.
// The producer must have drained/synced exactly the frontier it certifies.
// Borrowed ordinary backing is charged as a retained loan, before cloning or
// pinning, and the real source owner remains referenced through token terminal.
func (o *StableSegmentOwner) Capture(lane, id, path string, frontier DurableFrontier, reach ReachabilityField, contentSynced bool, account StableMetadataAccount) (*StableResourceToken, error) {
	return o.capture(lane, id, path, frontier, reach, nil, contentSynced, account)
}

// CaptureLogicalObligations retains the same exact physical owner for ordinary
// logical column references. The existing small normalization route owns and
// prepays every obligation array/string before any clone or registry pin. This
// token constructor does not admit the still-closed set/path publication route.
func (o *StableSegmentOwner) CaptureLogicalObligations(lane, id, path string, frontier DurableFrontier, reach ReachabilityField, obligations []StableLogicalObligation, contentSynced bool, account StableMetadataAccount) (*StableResourceToken, error) {
	return o.capture(lane, id, path, frontier, reach, obligations, contentSynced, account)
}
func (o *StableSegmentOwner) capture(lane, id, path string, frontier DurableFrontier, reach ReachabilityField, obligations []StableLogicalObligation, contentSynced bool, account StableMetadataAccount) (*StableResourceToken, error) {
	if o == nil {
		return nil, ErrResourceOwnership
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	source := o.token
	if source == nil || o.refs.Load() == 0 || source.reachability != reach || frontier.exactRIDs != nil {
		return nil, ErrStableMetadataShapeUnsupported
	}
	if e := validateDurableFrontier(frontier); e != nil {
		return nil, e
	}
	if e := validateDiagnosticPath(path); e != nil {
		return nil, e
	}
	var loanBytes uint64
	var borrower *stableRegistryBorrower
	if account != nil {
		if !source.backingCertified || source.namespace != nil && !source.namespace.backingCertified {
			return nil, ErrStableMetadataShapeUnsupported
		}
		for _, obligation := range obligations {
			if obligation.Reachability != reach {
				return nil, ErrResourceConflict
			}
		}
		obligationBytes, err := finiteStableObligationBytes(obligations)
		if err != nil {
			return nil, err
		}
		loanBytes = o.census.LiveClassBytes
		var plan stableBackingSizePlan
		plan.bytes = obligationBytes
		plan.add(uint64(unsafe.Sizeof(StableResourceToken{})), true)
		plan.add(uint64(unsafe.Sizeof(IdentityPin{})), true)
		plan.string(lane)
		plan.string(id)
		plan.string(path)
		plan.string(string(reach))
		if plan.err != nil {
			return nil, plan.err
		}
		extra, err := finiteStableAdd(loanBytes, plan.bytes)
		if err != nil {
			return nil, err
		}
		if source.pinRegistry != nil {
			borrower, err = source.pinRegistry.acquireBorrowerWithBacking(account, extra, true)
		} else {
			err = finiteStableBegin(account, extra)
		}
		if err != nil {
			return nil, err
		}
	}
	clone, e := source.cloneSharedPinnedAccountWithCredit(lane, id, path, frontier, reach, obligations, nil, nil, account, loanBytes, account != nil, o)
	if e != nil {
		borrower.release()
		return nil, e
	}
	clone.registryBorrower = borrower
	o.refs.Add(1)
	clone.segmentOwner = o
	// A new successful producer sync certifies this frontier; without it retain
	// only the original actual content certificate, never inflate its frontier.
	if contentSynced {
		clone.syncedFrontier = frontier
		clone.hasSyncedFrontier = true
		clone.metrics.physicalFileSyncs.Store(1)
	}
	return clone, nil
}
func (o *StableSegmentOwner) release() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.refs.Add(-1) < 0 {
		panic("stable segment owner reference imbalance")
	}
	if o.refs.Load() == 0 {
		token := o.token
		o.token = nil
		token.releaseFrom(ResourceOwnerShared)
		if o.resident != nil {
			account := o.resident
			o.resident = nil
			account.ReleaseStableMetadata()
		}
	}
}
func (o *StableSegmentOwner) Release() {
	if o != nil && !o.released.Swap(true) {
		o.release()
	}
}

// PreparedStableSegmentOwner holds an exclusive complete FD/namespace closure
// through pre-WAL destination preparation. Commit and Abort allocate no backing
// and request no credit. Generic surviving clones remain ineligible.
type PreparedStableSegmentOwner struct {
	transfer    preparedStableMetadataTransfer
	owner       *StableSegmentOwner
	destination StableMetadataAccount
	ready       bool
}

func PrepareStableSegmentOwner(token *StableResourceToken, destination StableMetadataAccount) (PreparedStableSegmentOwner, error) {
	var p PreparedStableSegmentOwner
	if token == nil {
		return p, ErrStableMetadataShapeUnsupported
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	if token.callbackBacked || token.callbackCreator != nil || token.activeOperations != 0 || token.releasePending || token.cleanupRunning || token.cleanupUncertain || token.cleanupComplete || token.released.Load() || token.metadataAccount == nil || destination == nil || !token.wholeBackingOwned || !token.backingCertified || token.directory != nil || len(token.logicalObligations) != 0 || token.frontier.exactRIDs != nil {
		return p, ErrStableMetadataShapeUnsupported
	}
	n, err := StableBackingClassBytes(uint64(unsafe.Sizeof(StableSegmentOwner{})), true)
	if err != nil {
		return p, err
	}
	if err = token.metadataAccount.ReserveStableMetadata(n); err != nil {
		return p, err
	}
	if err = destination.ReserveStableMetadata(n); err != nil {
		return p, err
	}
	if err = destination.RetainStableMetadata(); err != nil {
		return p, err
	}
	stamp, err := prepareBackingStamp(uint64(unsafe.Sizeof(StableSegmentOwner{})), true, nil)
	if err != nil {
		destination.ReleaseStableMetadata()
		return p, err
	}
	transfer, err := prepareStableMetadataTransferLocked(token, destination)
	if err != nil {
		destination.ReleaseStableMetadata()
		return p, err
	}
	census := token.backingCensus
	if token.namespace != nil {
		var layout backingLayout
		layout.merge(census)
		layout.merge(token.namespace.backingCensus)
		census = layout.census
	}
	census.add(stamp)
	owner := &StableSegmentOwner{token: token, census: census, resident: destination}
	owner.refs.Store(1)
	return PreparedStableSegmentOwner{transfer: transfer, owner: owner, destination: destination, ready: true}, nil
}
func (p *PreparedStableSegmentOwner) Commit() *StableSegmentOwner {
	if p == nil || !p.ready {
		return nil
	}
	p.transfer.commit()
	if err := p.owner.token.transfer(ResourceOwnerToken, ResourceOwnerShared); err != nil {
		panic("stable prepared physical owner lost token")
	}
	owner := p.owner
	*p = PreparedStableSegmentOwner{}
	return owner
}
func (p *PreparedStableSegmentOwner) Abort() {
	if p == nil || !p.ready {
		return
	}
	p.transfer.abort()
	p.owner.token = nil
	p.owner.resident = nil
	p.destination.ReleaseStableMetadata()
	*p = PreparedStableSegmentOwner{}
}
