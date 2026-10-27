package caching

import (
	"context"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

var _ backenddb.LeafPageLogPrivateCOWPackHandoff = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogPrivateCOWPackHandoff = (*cachingLeafPageLog)(nil)

func (g *cachingLeafPageLogGroup) HandoffPrivateCOWLeafPackSources(ctx context.Context, owner *transactionowner.Owner, sources []backenddb.LeafPageLogPackSource) error {
	if g == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	return g.db.handoffPrivateCOWLeafPackSources(ctx, owner, sources)
}

func (l *cachingLeafPageLog) HandoffPrivateCOWLeafPackSources(ctx context.Context, owner *transactionowner.Owner, sources []backenddb.LeafPageLogPackSource) error {
	if l == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	return l.db.handoffPrivateCOWLeafPackSources(ctx, owner, sources)
}

// The original slice owns flushMu: physical checkpoint/maintenance builders
// cannot append leaves while we hand off selected sources. Ordinary private
// writers still append their WAL and mutable COW suffix. Leaf append paths lock
// vlogMu before borrowing l.vlog (prepared work holds payloads, not writers), so
// the existing rotation cannot strand a pending borrowed writer on the source.
func (db *DB) handoffPrivateCOWLeafPackSources(ctx context.Context, owner *transactionowner.Owner, sources []backenddb.LeafPageLogPackSource) error {
	if db == nil || db.cow == nil || owner == nil || owner != db.cow.txnOwner {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// This maintenance-only state is bounded by the selected source count,
	// never allocated or inspected by normal appenders.
	selected := make(map[uint32]rootpublication.StableIdentity, len(sources))
	for _, source := range sources {
		if source.FileID == 0 || source.Identity == (rootpublication.StableIdentity{}) {
			return fmt.Errorf("%w: invalid leaf pack source witness", rootpublication.ErrUnresolvedResource)
		}
		if _, duplicate := selected[source.FileID]; duplicate {
			return fmt.Errorf("%w: duplicate leaf pack source %d", rootpublication.ErrResourceConflict, source.FileID)
		}
		selected[source.FileID] = source.Identity
	}
	locked := make([]*lane, 0, len(sources))
	defer func() {
		for i := len(locked) - 1; i >= 0; i-- {
			locked[i].vlogMu.Unlock()
		}
	}()
	for _, lane := range db.leafLogAppendLanesSnapshot() {
		if lane == nil {
			continue
		}
		lane.vlogMu.Lock()
		if lane.vlog == nil {
			lane.vlogMu.Unlock()
			continue
		}
		id, err := valuelog.EncodeFileID(uint32(lane.id), uint32(lane.vlogSeq))
		want, matches := selected[id]
		if err != nil || !matches {
			lane.vlogMu.Unlock()
			if err != nil {
				return err
			}
			continue
		}
		locked = append(locked, lane)
		writer, ok := lane.vlog.(interface {
			StableIdentity() (rootpublication.StableIdentity, error)
		})
		if !ok {
			return fmt.Errorf("%w: selected leaf writer lacks stable identity", rootpublication.ErrUnresolvedResource)
		}
		actual, err := writer.StableIdentity()
		if err != nil {
			return err
		}
		if actual != want {
			return fmt.Errorf("%w: selected leaf pack source %d identity mismatch", rootpublication.ErrResourceConflict, id)
		}
	}
	// Validate every selected active identity before the first rotation. Locks
	// remain held through execution, so each proof still names that exact writer.
	for _, lane := range locked {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := db.rotateValueLogMuHeld(lane); err != nil {
			return err
		}
	}
	return nil
}
