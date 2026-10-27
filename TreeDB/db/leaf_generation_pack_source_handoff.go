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
func (db *DB) handoffPrivateCOWLeafPackSources(ctx context.Context, owner *transactionowner.Owner, set *valuelog.Set, ids []uint32) error {
	if owner == nil || owner != db.privateCOWOwner {
		return ErrNativeSemanticUnsupported
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
		return nil
	}
	handoff, ok := log.(LeafPageLogPrivateCOWPackHandoff)
	if !ok {
		return fmt.Errorf("leaf generation pack: cached leaf-page-log owner %T lacks private source handoff", log)
	}
	sources := make([]LeafPageLogPackSource, 0, len(ids))
	for _, id := range ids {
		file := set.Files[page.ValueLogFileID(id)]
		if file == nil || file.File == nil {
			return fmt.Errorf("%w: leaf pack source %d unavailable", rootpublication.ErrUnresolvedResource, id)
		}
		identity, err := rootpublication.StableIdentityFromFile(file.File)
		if err != nil {
			return err
		}
		sources = append(sources, LeafPageLogPackSource{FileID: page.ValueLogFileID(id), Identity: identity})
	}
	if err := handoff.HandoffPrivateCOWLeafPackSources(ctx, owner, sources); err != nil {
		return err
	}
	// Rotation creates empty successor files before their first physical leaf
	// append. Register them under the original backend writer owner now, before
	// copy/publication captures its generation view. Otherwise the ensuing GC
	// would discover them and stale the cache's just-adopted Snapshot, making
	// that same basis pin the selected sources during its own retirement pass.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	state := db.state.Load()
	if state == nil {
		return ErrClosed
	}
	_, err := db.reconcileLeafGenerationManifestWithDirLocked(state.CommitSeq)
	return err
}
