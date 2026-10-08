package freelist

import "github.com/snissn/gomap/TreeDB/internal/allocatorownership"

// BindManagedIndexWriterV1 binds the actual DB-only index writer before any
// external DB export. Public New/Enable calls cannot construct this internal
// capability and confer no exclusive receiver ownership.
func (a *Allocator) BindManagedIndexWriterV1(authority *allocatorownership.ManagedWriter) error {
	if a == nil || authority == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.writerAuthority != nil || !authority.Claim() {
		return ErrCandidateConsumed
	}
	a.writerAuthority = authority
	return nil
}

// MarkOrdinaryWriterEscapeV1 runs under the index writer-field read lock.
// Existing finite-origin backing refuses the raw writer before exposure.
// An ordinary retaining export permanently prevents future finite adoption.
func (a *Allocator) MarkOrdinaryWriterEscapeV1() bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return false
	}
	if a.cow != nil && (a.cow.creator != nil || a.cow.generation.hasFiniteBackingV1() ||
		a.cow.txn != nil && (a.cow.txn.creator != nil || a.cow.txn.buildCreator != nil || stateTreeFiniteV1(a.cow.txn.root))) {
		return false
	}
	a.rawWriterEscaped = true
	return true
}

func (a *Allocator) CanDetachManagedIndexWriterV1(authority *allocatorownership.ManagedWriter) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed && authority != nil && a.writerAuthority == authority && !a.rawWriterEscaped
}

// DetachManagedIndexWriterV1 acknowledges that both index writer fields were
// removed at the joined producer terminal. Cuts and Cond borrowers still own
// the same aggregate header creating credit; publication is no shortcut.
func (a *Allocator) DetachManagedIndexWriterV1(authority *allocatorownership.ManagedWriter) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed || authority == nil || a.writerAuthority != authority || a.rawWriterEscaped {
		return
	}
	a.writerDetached = true
	a.releaseClosedCOWStateCreatorLockedV1()
	if a.cow == nil {
		a.writerAuthority = nil
	}
}
