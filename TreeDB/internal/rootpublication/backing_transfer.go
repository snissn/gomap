package rootpublication

import "reflect"

// preparedStableMetadataTransfer is a private engine value held by the concrete
// request owner. It is not an exported lease/control pointer or a generic view.
// Only a constructor-certified complete owned closure may transfer; current
// exclusive reference counts cannot certify a clone's shared allocations.
// Preparation claims the existing token and prepays exact destination backing;
// terminal commit/abort allocate nothing and never request additional credit.
type preparedStableMetadataTransfer struct {
	token               *StableResourceToken
	source, destination StableMetadataAccount
	namespace           *StableNamespaceToken
	ready               bool
	registryBorrower    *stableRegistryBorrower
}

func prepareStableMetadataTransfer(token *StableResourceToken, destination StableMetadataAccount) (preparedStableMetadataTransfer, error) {
	var p preparedStableMetadataTransfer
	if token == nil || destination == nil {
		return p, ErrStableMetadataShapeUnsupported
	}
	token.metadataMu.Lock()
	defer token.metadataMu.Unlock()
	return prepareStableMetadataTransferLocked(token, destination)
}

// Caller already holds the universal token admission gate.
func prepareStableMetadataTransferLocked(token *StableResourceToken, destination StableMetadataAccount) (preparedStableMetadataTransfer, error) {
	var p preparedStableMetadataTransfer
	if token.callbackBacked || token.callbackCreator != nil || token.activeOperations != 0 || token.releasePending || token.cleanupRunning || token.cleanupUncertain || token.cleanupComplete {
		return p, ErrStableMetadataShapeUnsupported
	}
	if token.metadataAccount == nil || !reflect.ValueOf(token.metadataAccount).Comparable() || !reflect.ValueOf(destination).Comparable() || token.released.Load() || token.transferPending || !token.backingCertified || !token.wholeBackingOwned || token.segmentOwner != nil ||
		token.pinnedRefs == nil || token.pinnedRefs.Load() != 1 || token.owner.Load() != uint32(ResourceOwnerToken) {
		return p, ErrStableMetadataShapeUnsupported
	}
	ns := token.namespace
	if ns != nil {
		ns.mu.Lock()
		defer ns.mu.Unlock()
		if ns.released.Load() || ns.refs.Load() != 1 || !ns.backingCertified || ns.metadataAccount != token.metadataAccount {
			return p, ErrStableMetadataShapeUnsupported
		}
	}
	n := token.backingCensus.LiveClassBytes
	if ns != nil {
		var err error
		n, err = finiteStableAdd(n, ns.backingCensus.LiveClassBytes)
		if err != nil {
			return p, err
		}
	}
	if err := destination.ReserveStableMetadata(n); err != nil {
		return p, err
	}
	if err := destination.RetainStableMetadata(); err != nil {
		return p, err
	}
	retained := 1
	if ns != nil {
		if err := destination.RetainStableMetadata(); err != nil {
			destination.ReleaseStableMetadata()
			return p, err
		}
		retained++
	}
	source := token.metadataAccount
	if err := source.RetainStableMetadata(); err != nil {
		for i := 0; i < retained; i++ {
			destination.ReleaseStableMetadata()
		}
		return p, err
	}
	var borrower *stableRegistryBorrower
	if token.registryBorrower != nil {
		var err error
		borrower, err = token.registryBorrower.registry.acquireBorrower(destination)
		if err != nil {
			source.ReleaseStableMetadata()
			for i := 0; i < retained; i++ {
				destination.ReleaseStableMetadata()
			}
			return p, err
		}
	}
	if err := token.transferLocked(ResourceOwnerToken, ResourceOwnerShared); err != nil {
		borrower.release()
		source.ReleaseStableMetadata()
		for i := 0; i < retained; i++ {
			destination.ReleaseStableMetadata()
		}
		return p, err
	}
	token.transferPending = true
	return preparedStableMetadataTransfer{token: token, source: source, destination: destination, namespace: ns, ready: true, registryBorrower: borrower}, nil
}

func (p *preparedStableMetadataTransfer) commit() {
	if p == nil || !p.ready {
		return
	}
	token := p.token
	token.metadataMu.Lock()
	if !token.transferPending || token.metadataAccount != p.source {
		panic("stable prepared transfer authority changed")
	}
	if p.namespace != nil {
		p.namespace.mu.Lock()
		p.namespace.metadataAccount = p.destination
		p.namespace.metadataBacking = p.namespace.backingCensus.LiveClassBytes
		p.namespace.mu.Unlock()
		p.source.ReleaseStableMetadata()
	}
	oldBorrower := token.registryBorrower
	token.registryBorrower = p.registryBorrower
	p.registryBorrower = nil
	token.metadataAccount = p.destination
	token.metadataBacking = token.backingCensus.LiveClassBytes
	token.transferPending = false
	if err := token.transferLocked(ResourceOwnerShared, ResourceOwnerToken); err != nil {
		panic("stable prepared transfer ownership changed")
	}
	token.metadataMu.Unlock()
	oldBorrower.release()
	// Existing object retentions move, then the preparation's old-owner hold ends.
	p.source.ReleaseStableMetadata()
	p.source.ReleaseStableMetadata()
	p.clear()
}
func (p *preparedStableMetadataTransfer) abort() {
	if p == nil || !p.ready {
		return
	}
	token := p.token
	token.metadataMu.Lock()
	if !token.transferPending || token.metadataAccount != p.source {
		panic("stable prepared transfer authority changed")
	}
	token.transferPending = false
	if err := token.transferLocked(ResourceOwnerShared, ResourceOwnerToken); err != nil {
		panic("stable prepared transfer ownership changed")
	}
	token.metadataMu.Unlock()
	if p.namespace != nil {
		p.destination.ReleaseStableMetadata()
	}
	p.destination.ReleaseStableMetadata()
	p.source.ReleaseStableMetadata()
	p.registryBorrower.release()
	p.clear()
}
func (p *preparedStableMetadataTransfer) clear() { *p = preparedStableMetadataTransfer{} }
