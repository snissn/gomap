package collections

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// A paged build owns at most two private segments: final output and temporary
// intent. Each append closes its bounded producer authority; only exact physical
// identity and frontier survive between appends. No per-page refs are retained.
// Unreferenced segments left by a process crash use ordinary column-asset GC.
type vectorPartitionPrivateSegmentV2 struct {
	plan      columnAssetGCPlannedSegment
	dir       string
	uncertain bool
}

func (s *vectorPartitionPrivateSegmentV2) append(c *Collection, generation, part uint64, raw []byte, recovery StableResourceCaptureRecoveryRetainer) (ColumnAssetRef, error) {
	cfg := *c.meta.Options.ColumnStore
	registry := c.db.StableResourceIdentityPinRegistry()
	session := newColumnPhysicalAssetAppendSessionWithStableResources(c.db.ColumnAssetRootDir(), cfg, registry, recovery)
	var appender *columnPhysicalAssetSegmentAppender
	var err error
	if s.plan.entry.FileID == 0 {
		appender, err = session.freshAppender()
	} else {
		appender, err = session.appender(s.plan.entry.FileID)
	}
	if err != nil {
		return ColumnAssetRef{}, errors.Join(err, session.abort())
	}
	if s.plan.entry.FileID == 0 {
		s.plan = columnAssetGCPlannedSegment{entry: ColumnAssetReachabilitySegmentEntry{FileID: appender.fileID, Bytes: appender.offset}, parentIdentity: appender.stableParentIdentity, childIdentity: appender.stableChildIdentity}
		s.dir = appender.namespace.SegmentDir
	} else if !rootpublication.SamePhysicalIdentity(s.plan.parentIdentity, appender.stableParentIdentity) || !rootpublication.SamePhysicalIdentity(s.plan.childIdentity, appender.stableChildIdentity) || s.plan.entry.Bytes != appender.offset {
		return ColumnAssetRef{}, errors.Join(fmt.Errorf("%w: private projection segment identity/frontier changed", ErrColumnAssetGCPlanStale), session.abort())
	}
	refs, appendErr := session.appendKinds(appender.fileID, []columnPhysicalAssetAppendItem{{payload: raw, kind: ColumnAssetKindTCS1HNSWSearchPack, generation: generation, partID: part}})
	_, resources, closeErr := session.closeWithStableResourcesValidated(func(captured *rootpublication.StableResourceSet) error {
		if appendErr != nil {
			return appendErr // The existing exact rollback path handles partial writes.
		}
		if len(refs) != 1 {
			return errors.New("collections: private projection append count")
		}
		return validateStableColumnResourcesMatchPrepared([]ColumnPreparedAsset{{Ref: refs[0], Bytes: refs[0].Length}}, captured)
	})
	s.uncertain = appender.stableRecoveryRetained || errors.Is(closeErr, ErrRecoveryRequired)
	if appender.stableRollbackTruncated {
		s.plan.entry.Bytes = appender.appendStart
	} else {
		s.plan.entry.Bytes = appender.offset
	}
	if appendErr != nil || closeErr != nil {
		if resources != nil {
			resources.Release()
		}
		return ColumnAssetRef{}, errors.Join(appendErr, closeErr)
	}
	// Even a later sync failure can safely remove this private exact frontier.
	s.plan.entry.Bytes = appender.offset
	if resources == nil {
		return ColumnAssetRef{}, errors.New("collections: missing private projection authority")
	}
	defer resources.Release()
	if err := resources.SyncThrough(); err != nil {
		return ColumnAssetRef{}, err
	}
	return refs[0], nil
}

func (s *vectorPartitionPrivateSegmentV2) remove(registry *rootpublication.IdentityPinRegistry) error {
	if s.plan.entry.FileID == 0 || s.uncertain {
		return nil // Ambiguous append rollback belongs to the capture recovery owner.
	}
	deleter, err := newColumnAssetStableSegmentDeleter(s.dir, registry)
	if err != nil {
		return err
	}
	_, err = deleter.delete(s.plan, nil)
	if err == nil {
		// An earlier attempt may have unlinked the child but failed to sync
		// its parent. delete validates the exact parent even when the child
		// is absent; always finish that parent's sync before forgetting debt.
		deleter.removed = true
	}
	err = deleter.finish(err)
	if err == nil {
		s.plan.entry.FileID = 0
	}
	return err
}
