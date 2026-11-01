package db

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Called only after selection, source resolution and target preflight. The
// retained set pins exact source handles until leafGenerationPackLocked returns.
func (db *DB) handoffPrivateCOWLeafPackSources(ctx context.Context, owner *transactionowner.Owner, set *valuelog.Set, ids []uint32) (leafGenerationPackHandoffTransition, error) {
	var transition leafGenerationPackHandoffTransition
	if owner == nil || owner != db.privateCOWOwner {
		return transition, ErrNativeSemanticUnsupported
	}
	db.writeMu.RLock()
	log := db.leafPageLog
	for {
		wrapped, ok := log.(*leafPageLogWithRecordLengthHints)
		if !ok {
			break
		}
		log = wrapped.inner
	}
	db.writeMu.RUnlock()
	// Backend-owned rewrite fixtures have no cached active append owner.
	cached, ok := log.(LeafPageLogCachedWrapperOwner)
	if !ok || !cached.CompactStorageCachedWrapperOwner() {
		return transition, nil
	}
	handoff, ok := log.(LeafPageLogPrivateCOWPackHandoff)
	if !ok {
		return transition, fmt.Errorf("leaf generation pack: cached leaf-page-log owner %T lacks private source handoff", log)
	}
	sources := make([]LeafPageLogPackSource, 0, len(ids))
	for _, id := range ids {
		file := set.Files[page.ValueLogFileID(id)]
		if file == nil || file.File == nil {
			return transition, fmt.Errorf("%w: leaf pack source %d unavailable", rootpublication.ErrUnresolvedResource, id)
		}
		identity, err := rootpublication.StableIdentityFromFile(file.File)
		if err != nil {
			return transition, err
		}
		sources = append(sources, LeafPageLogPackSource{FileID: page.ValueLogFileID(id), Identity: identity})
	}
	// Capture one coherent original publication under the existing writer owner.
	// The installed handoff may rotate writers, but must not publish a backend
	// root or generation view before this producer reconciles its successors.
	db.writeMu.RLock()
	origin := db.snapshotViewRO.Load()
	if origin == nil || origin.idx == nil || origin.state == nil {
		db.writeMu.RUnlock()
		return transition, ErrClosed
	}
	transition.db, transition.owner, transition.idx = db, owner, origin.idx
	transition.before, _ = stateTokenFromState(origin.state)
	db.writeMu.RUnlock()
	if err := handoff.HandoffPrivateCOWLeafPackSources(ctx, owner, sources); err != nil {
		return leafGenerationPackHandoffTransition{}, err
	}
	// Rotation creates empty successor files before their first physical leaf
	// append. Register them under the original backend writer owner now, before
	// copy/publication captures its generation view. Otherwise the ensuing GC
	// would discover them and stale the cache's just-adopted Snapshot, making
	// that same basis pin the selected sources during its own retirement pass.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return leafGenerationPackHandoffTransition{}, err
	}
	current := db.snapshotViewRO.Load()
	if current == nil || current.idx != transition.idx || current.state == nil {
		return leafGenerationPackHandoffTransition{}, ErrRootPublicationBasisMismatch
	}
	currentToken, _ := stateTokenFromState(current.state)
	if currentToken != transition.before {
		return leafGenerationPackHandoffTransition{}, ErrRootPublicationBasisMismatch
	}
	changed, err := db.reconcileLeafGenerationManifestWithDirLocked(transition.before.CommitSeq)
	if err != nil {
		return leafGenerationPackHandoffTransition{}, err
	}
	// That producer publishes exactly one generation view iff changed. Derive
	// its exact expected endpoint from the original state, then check the actual
	// coherent publication. Never adopt an arbitrary observed version delta.
	transition.after, transition.changed = transition.before, changed
	if changed {
		transition.after.LeafGenerationStateVersion++
		if transition.after.LeafGenerationStateVersion == 0 {
			return leafGenerationPackHandoffTransition{}, ErrRootPublicationBasisMismatch
		}
	}
	current = db.snapshotViewRO.Load()
	if current == nil || current.idx != transition.idx || current.state == nil {
		return leafGenerationPackHandoffTransition{}, ErrRootPublicationBasisMismatch
	}
	currentToken, _ = stateTokenFromState(current.state)
	if currentToken != transition.after {
		return leafGenerationPackHandoffTransition{}, ErrRootPublicationBasisMismatch
	}
	return transition, nil
}
