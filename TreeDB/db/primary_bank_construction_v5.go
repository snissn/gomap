package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"math"
	"unsafe"
)

// This is the actual private construction custody, shared by ordinary
// dependency-tree and manifest bank builders. It is transferred to a seal when
// needed; it is not another publication or retirement registry.
type primaryBankConstructionV5 struct {
	idx            *indexGen
	owner          *primaryArenaOwnerV5
	metadata       *retainedalloc.Owner
	refs           []primaryarena.Ref
	charge         uint64
	closed, failed bool
	cleanupErr     error
	failure        primaryCleanupFailureV5
	failedNext     *primaryBankConstructionV5
}

func newPrimaryBankConstructionV5(idx *indexGen) (*primaryBankConstructionV5, error) {
	if idx == nil || idx.primaryOwner == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	metadata := idx.primary.MetadataOwner()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryBankConstructionV5{})))
	if err := metadata.AddPending(charge); err != nil {
		return nil, err
	}
	ready, err := idx.primaryOwner.retain(nil)
	if !ready || err != nil {
		metadata.RemovePending(charge)
		return nil, errors.Join(err, rootpublication.ErrResourceOwnership)
	}
	return &primaryBankConstructionV5{idx: idx, owner: idx.primaryOwner, metadata: metadata, charge: charge}, nil
}

func (claims *primaryBankConstructionV5) ensureCapacity(count int) error {
	if claims.closed || count < len(claims.refs) {
		return rootpublication.ErrResourceOwnership
	}
	if count <= cap(claims.refs) {
		return nil
	}
	capacity := count
	if cap(claims.refs) <= math.MaxInt/2 && 2*cap(claims.refs) > capacity {
		capacity = 2 * cap(claims.refs)
	}
	if uint64(capacity) > math.MaxUint64/uint64(unsafe.Sizeof(primaryarena.Ref{})) {
		return retainedalloc.ErrCapacity
	}
	added := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(primaryarena.Ref{})))
	if err := claims.metadata.AddPending(added); err != nil {
		return err
	}
	next := make([]primaryarena.Ref, len(claims.refs), capacity)
	copy(next, claims.refs)
	old := retainedalloc.AllocationCharge(uint64(cap(claims.refs)) * uint64(unsafe.Sizeof(primaryarena.Ref{})))
	clear(claims.refs[:cap(claims.refs)])
	claims.refs = next
	claims.charge += added - old
	claims.metadata.RemovePending(old)
	return nil
}

func (claims *primaryBankConstructionV5) claim(class primaryarena.Class, count int) error {
	if count <= 0 || count > math.MaxInt-len(claims.refs) {
		return retainedalloc.ErrCapacity
	}
	start := len(claims.refs)
	if err := claims.ensureCapacity(start + count); err != nil {
		return err
	}
	refs := claims.refs[:start+count]
	if err := claims.idx.primary.ClaimMetadataBanksIntoOrdinary(class, refs[start:]); err != nil {
		return err
	}
	claims.refs = refs
	return nil
}

// Every consumed bank edge is cleared immediately. Failed cleanup retains the
// exact remaining references and embedded error on the existing physical owner.
func (claims *primaryBankConstructionV5) release() error {
	if claims == nil {
		return nil
	}
	if claims.closed {
		return claims.cleanupErr
	}
	claims.closed = true
	for i, ref := range claims.refs {
		if ref == (primaryarena.Ref{}) {
			continue
		}
		consumed, err := claims.idx.primary.Drop(ref, nil)
		if !consumed || err != nil {
			return claims.retainFailure(err)
		}
		claims.refs[i] = primaryarena.Ref{}
	}
	consumed, debt, err := claims.owner.releaseWithOutcome(nil)
	if !consumed || debt {
		return claims.retainFailure(err)
	}
	metadata, charge := claims.metadata, claims.charge
	clear(claims.refs[:cap(claims.refs)])
	claims.refs = nil
	claims.idx, claims.owner, claims.metadata, claims.charge = nil, nil, nil, 0
	claims.cleanupErr = err // prior sibling's already-owned error, not this refunded object
	metadata.RemovePending(charge)
	return err
}

func (claims *primaryBankConstructionV5) retainFailure(err error) error {
	claims.failure.causes = [3]error{err, rootpublication.ErrResourceOwnership}
	claims.cleanupErr = &claims.failure
	claims.metadata.CleanupFailed()
	owner := claims.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !claims.failed {
		claims.failed = true
		claims.failedNext = owner.failedBankConstructions
		owner.failedBankConstructions = claims
		claims.failure.causes[2] = owner.cleanupErr
		owner.cleanupErr = claims.cleanupErr
	}
	return claims.cleanupErr
}
