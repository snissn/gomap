package db

import (
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The actual original manifest construction retains every failed edge. The
// enclosing seal clears its claims alias before consuming the completion.
func releasePrimaryBankConstructionV6(claims *primaryBankConstructionV5) (primaryRuntimeDisposalCompletionV6, bool, error) {
	var empty primaryRuntimeDisposalCompletionV6
	if claims == nil {
		return empty, true, nil
	}
	claims.releaseMu.Lock()
	if claims.closed {
		err := claims.cleanupErr
		claims.releaseMu.Unlock()
		return empty, err == nil, err
	}
	fail := func(err error) (primaryRuntimeDisposalCompletionV6, bool, error) {
		claims.closed = true
		result := claims.retainFailure(err)
		claims.releaseMu.Unlock()
		return empty, false, result
	}
	if claims.owner == nil || claims.idx == nil || claims.idx.primary != claims.owner.arena {
		return fail(rootpublication.ErrResourceOwnership)
	}
	claims.releasePhase = 1
	for claims.releaseIndex < len(claims.refs) {
		i := claims.releaseIndex
		if claims.refs[i] != (primaryarena.Ref{}) {
			consumed, err := claims.idx.primary.Drop(claims.refs[i], nil)
			if err != nil {
				return fail(err)
			}
			if !consumed {
				return fail(rootpublication.ErrResourceOwnership)
			}
			claims.refs[i] = primaryarena.Ref{}
		}
		claims.releaseIndex++
	}
	consumed, debt, ownerCompletion, err := claims.owner.releaseOriginalWithOutcome()
	if !consumed || debt {
		return fail(err)
	}
	clear(claims.refs[:cap(claims.refs)])
	completion := primaryRuntimeDisposalCompletionV6{claims.metadata, claims.charge}
	claims.cleanupErr = err
	claims.refs, claims.idx, claims.owner, claims.metadata = nil, nil, nil, nil
	claims.charge, claims.releasePhase, claims.closed = 0, 6, true
	claims.releaseMu.Unlock()
	ownerCompletion.complete()
	return completion, true, err
}
