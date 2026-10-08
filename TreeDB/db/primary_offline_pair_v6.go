package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// Offline callers hold LOCK and have no outside readers. Construction still
// uses the ordinary fixed PRIMARY owner pair; logical commit/ACK frontiers do
// not advance merely because the physical files and pointers are replaced.
func (db *DB) newOfflinePrimaryPairV6(p *pager.Pager) (idx *indexGen, err error) {
	idx = newIndexGen(db.nextIndexID(), p, freelist.New(p, 0), nil)
	idx.zipper = zipper.New(p, idx.allocator)
	idx.zipper.SetLeafPrefixCompression(db.leafPrefixCompression)
	idx.zipper.SetIndexColumnarLeaves(db.indexColumnarLeaves)
	idx.zipper.SetIndexPackedValuePtr(db.indexPackedValuePtr)
	idx.zipper.SetIndexInternalBaseDelta(db.indexInternalBaseDelta)
	idx.zipper.SetAdaptiveLeafEncoding(db.indexAdaptiveLeafEncoding)
	idx.zipper.SetLeafPageReader(db.leafPageReader(db.valueLogManager))
	idx.zipper.SetLeafPageLog(db.leafPageLog)
	idx.zipper.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)
	defer func() {
		if err != nil {
			err = errors.Join(err, idx.close())
		}
	}()
	path := filepath.Join(db.dir, primaryNewFileName)
	if err = removePersistentFileBestEffort(db.dir, path, durabilitycut.ResourceIndex); err != nil {
		return idx, err
	}
	idx.primary, err = primaryarena.OpenCapsule(path)
	if err != nil {
		return idx, err
	}
	idx.primaryOwner, err = newPrimaryArenaOwnerV5(idx.primary)
	if err != nil {
		err = errors.Join(err, idx.primary.Close())
		idx.primary = nil
		return idx, err
	}
	if err = idx.primary.SetOwnedMetadataDecoder(rootpublication.PrimaryBankMetadataEdgesV5, rootpublication.PrimaryBankMetadataEdgesOwnedV5); err != nil {
		return idx, err
	}
	err = p.AttachPrimaryBankPager(idx.primary.Pager())
	idx.zipper.SetPrimaryArena(idx.primary)
	idx.zipper.SetPrimaryConstructorV6(nil)
	idx.zipper.SetPrimaryDirectory(false)
	return idx, err
}

func primaryOfflineRootV6(source recoverableDurableBasis, slot uint64) RecoverableRoot {
	r := source.slotRecord[slot]
	readRoot := r.UserRootPageID
	if source.primaryReadRoots[slot] != 0 {
		readRoot = source.primaryReadRoots[slot]
	}
	return RecoverableRoot{CommitSeq: r.CommitSeq, UserRootPageID: readRoot, SystemRootPageID: r.SystemRootPageID, AppliedCommandLSN: r.AppliedCommandLSN, MaxEntryRevision: r.MaxEntryRevision, Durable: true}
}

// produce and beforeClose are synchronous, borrowed operation callbacks, never
// retained by the pair. All slot/proof outputs are materialized by produce;
// source, producer and replacement owners close before namespace replacement.
func (db *DB) finishOfflinePrimaryPairV6(idx *indexGen, roots *RecoverableRootSet, produce func(RecoverableRoot) (rebuiltDurableRootV1, error), rewritePointers bool, beforeClose func() error) (err error) {
	if idx == nil || roots == nil || produce == nil {
		return rootpublication.ErrResourceOwnership
	}
	started := false
	defer func() {
		roots.Release()
		closeErr := idx.close()
		err = errors.Join(err, closeErr)
		if !started && closeErr == nil {
			err = errors.Join(err, removePersistentFileBestEffort(db.dir, filepath.Join(db.dir, indexNewFileName), durabilitycut.ResourceIndex), removePersistentFileBestEffort(db.dir, filepath.Join(db.dir, primaryNewFileName), durabilitycut.ResourceIndex))
		} else if err != nil {
			err = errors.Join(err, ErrRecoveryRequired)
		}
	}()
	source := roots.durable
	older, err := produce(primaryOfflineRootV6(source, source.slot^1))
	if err != nil {
		return err
	}
	defer older.resources.Release()
	older.meta.LastCommitHeight = source.slotRecord[source.slot^1].LastCommitHeight
	latest, err := produce(primaryOfflineRootV6(source, source.slot))
	if err != nil {
		return err
	}
	defer latest.resources.Release()
	latest.meta.LastCommitHeight = source.slotRecord[source.slot].LastCommitHeight
	selected, runtime, err := db.writeRebuiltPrimaryRootsWithProducerV5(context.Background(), idx, roots, older, latest, filepath.Join(db.dir, indexNewFileName), produce)
	if err != nil {
		return err
	}
	idx.unpublishedPrimary = runtime
	idx.unpublishedSelection = &selected
	if err = roots.Revalidate(); err != nil {
		return err
	}
	parent, err := rootpublication.OpenStableParent(db.dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, parent.Close()) }()
	decision, err := primaryJointDecisionForV6(parent, idx, selected)
	if err != nil {
		return err
	}
	// These are actual cleanup completions, not merely DB discovery removal.
	older.resources.Release()
	latest.resources.Release()
	stable := roots.stableSnapshot
	roots.Release()
	if stable != nil {
		if err = stable.Close(); err != nil {
			return err
		}
	}
	if beforeClose != nil {
		if err = beforeClose(); err != nil {
			return err
		}
	}
	if err = idx.close(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	if rewritePointers {
		if err = invalidateOfflineValueLogRefCountsV6(db.dir, parent); err != nil {
			return err
		}
	}
	started, err = writePrimaryJointCommitV6(db.dir, parent, decision)
	if err != nil {
		return err
	}
	if err = rollForwardPrimaryJointV6(context.Background(), db.dir, parent, decision); err != nil {
		return err
	}
	return deletePrimaryJointCommitV6(context.Background(), db.dir, parent)
}

// Refcounts are a derived scan cache keyed only by logical CommitSeq. A V6
// pointer rewrite preserves that sequence but replaces physical pointers, so
// the old cache cannot project subsequent publications. Removing it before the
// staging barrier makes either the old or committed pair rebuild exact counts.
func invalidateOfflineValueLogRefCountsV6(dir string, parent *os.File) (err error) {
	f, err := rootpublication.OpenStableChildFile(parent, valueLogRefCountsFileName, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err = rootpublication.ValidateStableChildLink(parent, f, valueLogRefCountsFileName); err != nil {
		return err
	}
	if err = rootpublication.RemoveStableChildFile(parent, valueLogRefCountsFileName); err != nil {
		return err
	}
	return observeStableNamespaceMutation(durabilitycut.NamespaceUnlink, durabilitycut.ResourceIndex, dir, filepath.Join(dir, valueLogRefCountsFileName), "", parent, f, valueLogRefCountsFileName, "")
}
