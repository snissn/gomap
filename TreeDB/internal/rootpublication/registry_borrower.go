package rootpublication

import (
	"reflect"
	"unsafe"
)

// A borrower is owned by actual engine tokens/collectors, never exported as a
// generic view. Its retained registry pointer excludes pointer/address reuse.
// Registry growth reaches every borrower; cumulative debit is never refunded.
type stableRegistryBorrower struct {
	registry *IdentityPinRegistry
	account  StableMetadataAccount
	next     *stableRegistryBorrower
	refs     uint64
	stamp    backingStamp
}

func stableMetadataAccountsEqual(a, b StableMetadataAccount) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if !reflect.ValueOf(a).Comparable() || !reflect.ValueOf(b).Comparable() {
		return false
	}
	return a == b
}
func (r *IdentityPinRegistry) hasBorrowerLocked(a StableMetadataAccount) bool {
	for b := r.borrowers; b != nil; b = b.next {
		if stableMetadataAccountsEqual(b.account, a) {
			return true
		}
	}
	return false
}
func (r *IdentityPinRegistry) predebitBorrowersLocked(n uint64, except StableMetadataAccount) error {
	if n == 0 {
		return nil
	}
	for b := r.borrowers; b != nil; b = b.next {
		if !stableMetadataAccountsEqual(b.account, except) {
			if err := b.account.ReserveStableMetadata(n); err != nil {
				return err
			}
		}
	}
	return nil
}
func (r *IdentityPinRegistry) acquireBorrower(account StableMetadataAccount) (*stableRegistryBorrower, error) {
	return r.acquireBorrowerWithBacking(account, 0, false)
}
func (r *IdentityPinRegistry) acquireBorrowerWithBacking(account StableMetadataAccount, extra uint64, tokenRetention bool) (*stableRegistryBorrower, error) {
	if r == nil || account == nil || !reflect.ValueOf(account).Comparable() {
		return nil, ErrStableMetadataShapeUnsupported
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acquireBorrowerWithBackingLocked(account, extra, tokenRetention)
}

// The same registry engine is reused by constructor-reserved terminal claims.
// Caller holds registry.mu; no nested lock or new borrower representation.
func (r *IdentityPinRegistry) acquireBorrowerWithBackingLocked(account StableMetadataAccount, extra uint64, tokenRetention bool) (*stableRegistryBorrower, error) {
	if r == nil || account == nil || !reflect.ValueOf(account).Comparable() {
		return nil, ErrStableMetadataShapeUnsupported
	}
	for b := r.borrowers; b != nil; b = b.next {
		if stableMetadataAccountsEqual(b.account, account) {
			if err := account.ReserveStableMetadata(extra); err != nil {
				return nil, err
			}
			if tokenRetention {
				if err := account.RetainStableMetadata(); err != nil {
					return nil, err
				}
			}
			b.refs++
			return b, nil
		}
	}
	census, err := r.retainedBackingCensusLocked()
	if err != nil {
		return nil, err
	}
	bytes, err := StableBackingClassBytes(uint64(unsafe.Sizeof(stableRegistryBorrower{})), true)
	if err != nil {
		return nil, err
	}
	total, err := finiteStableAdd(census.LiveClassBytes, bytes)
	if err != nil {
		return nil, err
	}
	total, err = finiteStableAdd(total, extra)
	if err != nil {
		return nil, err
	}
	if err = account.ReserveStableMetadata(total); err != nil {
		return nil, err
	}
	if err = account.RetainStableMetadata(); err != nil {
		return nil, err
	}
	if tokenRetention {
		if err = account.RetainStableMetadata(); err != nil {
			account.ReleaseStableMetadata()
			return nil, err
		}
	}
	if err = r.predebitBorrowersLocked(bytes, nil); err != nil {
		account.ReleaseStableMetadata()
		if tokenRetention {
			account.ReleaseStableMetadata()
		}
		return nil, err
	}
	stamp, err := prepareBackingStamp(uint64(unsafe.Sizeof(stableRegistryBorrower{})), true, nil)
	if err != nil {
		account.ReleaseStableMetadata()
		if tokenRetention {
			account.ReleaseStableMetadata()
		}
		return nil, err
	}
	b := &stableRegistryBorrower{registry: r, account: account, next: r.borrowers, refs: 1, stamp: stamp}
	r.borrowers = b
	r.backingCensus.add(stamp)
	return b, nil
}
func (b *stableRegistryBorrower) retain() {
	r := b.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if b.refs == 0 {
		panic("stable registry borrower revived")
	}
	b.refs++
}
func (b *stableRegistryBorrower) release() {
	if b == nil {
		return
	}
	r := b.registry
	r.mu.Lock()
	if b.refs == 0 {
		r.mu.Unlock()
		panic("stable registry borrower reference imbalance")
	}
	b.refs--
	if b.refs != 0 {
		r.mu.Unlock()
		return
	}
	link := &r.borrowers
	for *link != b {
		if *link == nil {
			r.mu.Unlock()
			panic("stable registry borrower missing")
		}
		link = &(*link).next
	}
	*link = b.next
	r.backingCensus.remove(b.stamp)
	account := b.account
	b.registry = nil
	b.account = nil
	b.next = nil
	r.mu.Unlock()
	account.ReleaseStableMetadata()
}

// ObserveWithMetadataAccount uses the same registry plan and state. An explicit
// resident destination is retained before birth; all live source loans predebit
// growth, including growth requested by ordinary producers.
func (r *IdentityPinRegistry) ObserveWithMetadataAccount(identity StableIdentity, operation, destination StableMetadataAccount) error {
	identity, err := validateRegistryIdentity(identity)
	if err != nil {
		return err
	}
	if r == nil || operation == nil || destination == nil {
		return ErrStableMetadataShapeUnsupported
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.backingCertified {
		return ErrStableMetadataShapeUnsupported
	}
	prepared, err := r.prepareObserveLocked(identity, operation, destination)
	if err != nil {
		return err
	}
	prepared.apply()
	return nil
}

// stableBackingSizePlan plans classes without constructing objects or stamps.
// Actual constructor census remains separate and is recorded only after debit.
type stableBackingSizePlan struct {
	bytes uint64
	err   error
}

func (p *stableBackingSizePlan) add(n uint64, scan bool) {
	if p.err != nil || n == 0 {
		return
	}
	p.bytes, p.err = finiteStableClassAdd(p.bytes, n, scan)
}
func (p *stableBackingSizePlan) string(s string) { p.add(uint64(len(s)), false) }
