package db

import "github.com/snissn/gomap/TreeDB/internal/transactionowner"

// LeafGenerationPackTransition attests one accepted private-COW leaf-pack
// publication. That producer copies decoded leaf entries without changing their
// values or embedded row-asset references, and replaces only physical child and
// collection-root references. It does not authorize a general value-log rewrite
// or column compaction, consume a logical source, or prove new build capacity.
//
// The fixed-size value owns no resources. Its private endpoints come from the
// retained copy snapshot and the actual accepted visible-state installation,
// never a later read of the backend's current state. Only the leaf-pack producer
// can construct it. Callers keep both original snapshots alive while using it.
type LeafGenerationPackTransition struct {
	db     *DB
	owner  *transactionowner.Owner
	idx    *indexGen
	before StateToken
	after  StateToken
}

// IsZero reports that no accepted private-COW leaf-pack transition was supplied.
func (t LeafGenerationPackTransition) IsZero() bool {
	return t == (LeafGenerationPackTransition{})
}

// Validate checks the exact borrowed physical snapshots without retaining them
// or allocating. Reversed, foreign, closed and intervening bases fail closed;
// a later backend publication does not invalidate the two original snapshots.
func (t LeafGenerationPackTransition) Validate(before, after *Snapshot) error {
	if t.db == nil || t.owner == nil || t.idx == nil || t.owner != t.db.privateCOWOwner ||
		t.after.CommitSeq <= t.before.CommitSeq || t.after.CommitSeq != t.before.CommitSeq+1 ||
		t.after.AppliedCommandLSN != t.before.AppliedCommandLSN ||
		t.after.MaxEntryRevision != t.before.MaxEntryRevision {
		return ErrRootPublicationBasisMismatch
	}
	if err := before.beginRead(); err != nil {
		return ErrRootPublicationBasisMismatch
	}
	defer before.endRead()
	if err := after.beginRead(); err != nil {
		return ErrRootPublicationBasisMismatch
	}
	defer after.endRead()
	if before.db != t.db || after.db != t.db || before.idx != t.idx || after.idx != t.idx {
		return ErrRootPublicationBasisMismatch
	}
	beforeToken, beforeOK := stateTokenFromState(before.state)
	afterToken, afterOK := stateTokenFromState(after.state)
	if !beforeOK || !afterOK || beforeToken != t.before || afterToken != t.after {
		return ErrRootPublicationBasisMismatch
	}
	return nil
}
