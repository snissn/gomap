package db

import (
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"path/filepath"
	"unsafe"

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
func (db *DB) captureOfflinePrimaryScopeV6() (idx *indexGen, err error) {
	source := db.idx.Load()
	if source == nil || source.pager == nil || source.primaryOwner == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	metadata := source.primary.MetadataOwner()
	fileCharge, err := rootpublication.StableFileMetadataCharge(0) // parent name borrows the immutable DB directory string
	if err != nil {
		return nil, err
	}
	controlCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(indexGen{})))
	charge := fileCharge + controlCharge
	if err = metadata.AddPending(charge); err != nil {
		return nil, err
	}
	idx = newIndexGen(db.nextIndexID(), nil, nil, nil)
	idx.offlineNamespaceScope = true
	idx.offlineScopeControlCharge = controlCharge
	idx.offlineScopePhysicalOwner = source.primaryOwner
	idx.stableNamespaceMetadata = metadata
	idx.stableNamespaceParentCharge = charge
	idx.stableNamespaceParent, err = rootpublication.OpenStableParent(db.dir)
	if err != nil {
		return idx, errors.Join(err, idx.releaseOfflineNamespaceScopeV6())
	}
	parent := idx.stableNamespaceParent
	err = source.pager.WithStableResourceFile(func(f *os.File) error { return rootpublication.ValidateStableChildLink(parent, f, indexFileName) })
	if err == nil {
		err = source.primary.Pager().WithStableResourceFile(func(f *os.File) error {
			return rootpublication.ValidateStableChildLink(parent, f, primaryIndexFileName)
		})
	}
	if err != nil {
		return idx, errors.Join(err, idx.releaseOfflineNamespaceScopeV6())
	}
	return idx, nil
}

// The private generation's physical Close leaves only its admitted operation
// parent live through COMMIT. This final namespace phase uses the original
// close operand and never treats a failed Close as disposal or refund.
func (idx *indexGen) releaseOfflineNamespaceScopeV6() error {
	if idx == nil {
		return nil
	}
	// Join the same physical close once before disposing namespace scope storage.
	// Its cached failure remains real custody; this phase does not retry it.
	physicalErr := idx.close()
	idx.stableNamespaceMu.Lock()
	defer idx.stableNamespaceMu.Unlock()
	if !idx.offlineNamespaceScope {
		return physicalErr
	}
	if idx.stableNamespaceParent != nil {
		if err := idx.stableNamespaceParent.Close(); err != nil {
			idx.stableNamespaceFailure.causes[1] = err
			idx.stableNamespaceMetadata.CleanupFailed()
			idx.closeFailure.causes[1] = &idx.stableNamespaceFailure
			// A failed physical close already links this generation to its actual
			// owner. Do not put its one intrusive link on a second failure list.
			if physicalErr == nil && !idx.offlineScopeRetained && idx.offlineScopePhysicalOwner != nil {
				idx.offlineScopePhysicalOwner.retainFailedDataGeneration(idx)
				idx.offlineScopeRetained = true
			}
			return errors.Join(physicalErr, &idx.stableNamespaceFailure, ErrRecoveryRequired)
		}
	}
	metadata, charge := idx.stableNamespaceMetadata, idx.stableNamespaceParentCharge
	idx.stableNamespaceParent = nil
	idx.stableNamespaceParentCharge = 0
	idx.offlineNamespaceScope = false
	if physicalErr != nil {
		// Parent-file cleanup completed, but the exact physical generation and its
		// controls remain reachable/charged on the existing physical failure owner.
		idx.stableNamespaceMetadata.CleanupFailed()
		metadata.RemovePending(charge - idx.offlineScopeControlCharge)
		return errors.Join(physicalErr, ErrRecoveryRequired)
	}
	idx.stableNamespaceMetadata = nil
	idx.offlineScopeControlCharge = 0
	idx.offlineScopePhysicalOwner = nil
	idx.pager, idx.allocator, idx.zipper = nil, nil, nil
	idx.primary, idx.primaryOwner = nil, nil
	idx.registry, idx.graveyard = nil, nil
	metadata.RemovePending(charge)
	return nil
}

func (db *DB) newOfflinePrimaryPairV6(idx *indexGen, chunkSize int64) (_ *indexGen, err error) {
	if idx == nil || !idx.offlineNamespaceScope || idx.stableNamespaceParent == nil {
		return idx, rootpublication.ErrResourceOwnership
	}
	parent := idx.stableNamespaceParent
	// Construct the ordinary owner before either transferred-file constructor.
	// PRIMARY is adopted first so partial DATA cleanup remains in this lifetime.
	var primaryID, dataID rootpublication.StableIdentity
	defer func() {
		if err != nil {
			err = errors.Join(err, idx.close())
			if idx.closeErr == nil {
				if rootpublication.SamePhysicalIdentity(primaryID, primaryID) {
					err = errors.Join(err, cleanupOfflinePrimaryStageV6(db.dir, parent, primaryNewFileName, primaryID))
				}
				if rootpublication.SamePhysicalIdentity(dataID, dataID) {
					err = errors.Join(err, cleanupOfflinePrimaryStageV6(db.dir, parent, indexNewFileName, dataID))
				}
			}
			if idx.closeErr != nil && idx.primaryOwner == nil {
				if source := db.idx.Load(); source != nil && source.primaryOwner != nil {
					source.primaryOwner.retainFailedDataGeneration(idx)
					source.primary.MetadataOwner().CleanupFailed()
				}
			}
		}
	}()
	if err = jointSwapHookV6("before-offline-primary-create"); err != nil {
		return idx, err
	}
	primaryFile, err := rootpublication.OpenStableChildFile(parent, primaryNewFileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return idx, err
	}
	primaryID, err = rootpublication.StableIdentityFromFile(primaryFile)
	observationErr := observeStableNamespaceMutation(durabilitycut.NamespaceCreate, durabilitycut.ResourceIndex, db.dir, "", filepath.Join(db.dir, primaryNewFileName), parent, primaryFile, "", primaryNewFileName)
	var constructorErr error
	idx.primary, constructorErr = primaryarena.OpenCapsuleOwnedFile(primaryFile, filepath.Join(db.dir, primaryNewFileName))
	err = errors.Join(err, observationErr, constructorErr)
	if idx.primary != nil {
		var ownerErr error
		idx.primaryOwner, ownerErr = newPrimaryArenaOwnerV5(idx.primary)
		err = errors.Join(err, ownerErr)
	}
	if err != nil {
		return idx, err
	}
	if err = idx.primary.SetOwnedMetadataDecoder(rootpublication.PrimaryBankMetadataEdgesV5, rootpublication.PrimaryBankMetadataEdgesOwnedV5); err != nil {
		return idx, err
	}
	if err = jointSwapHookV6("before-offline-data-create"); err != nil {
		return idx, err
	}
	dataFile, err := rootpublication.OpenStableChildFile(parent, indexNewFileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return idx, err
	}
	dataID, err = rootpublication.StableIdentityFromFile(dataFile)
	observationErr = observeStableNamespaceMutation(durabilitycut.NamespaceCreate, durabilitycut.ResourceIndex, db.dir, "", filepath.Join(db.dir, indexNewFileName), parent, dataFile, "", indexNewFileName)
	idx.pager, constructorErr = pager.OpenOwnedFileWithOptions(dataFile, filepath.Join(db.dir, indexNewFileName), chunkSize, pager.OpenOptions{})
	err = errors.Join(err, observationErr, constructorErr)
	if err != nil {
		return idx, err
	}
	if _, err = idx.pager.Alloc(2); err != nil {
		return idx, err
	}
	idx.allocator = freelist.New(idx.pager, 0)
	idx.zipper = zipper.New(idx.pager, idx.allocator)
	idx.zipper.SetLeafPrefixCompression(db.leafPrefixCompression)
	idx.zipper.SetIndexColumnarLeaves(db.indexColumnarLeaves)
	idx.zipper.SetIndexPackedValuePtr(db.indexPackedValuePtr)
	idx.zipper.SetIndexInternalBaseDelta(db.indexInternalBaseDelta)
	idx.zipper.SetAdaptiveLeafEncoding(db.indexAdaptiveLeafEncoding)
	idx.zipper.SetLeafPageReader(db.leafPageReader(db.valueLogManager))
	idx.zipper.SetLeafPageLog(db.leafPageLog)
	idx.zipper.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)
	err = idx.pager.AttachPrimaryBankPager(idx.primary.Pager())
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
func (db *DB) finishOfflinePrimaryPairV6(parent *os.File, idx *indexGen, roots *RecoverableRootSet, produce func(RecoverableRoot) (rebuiltDurableRootV1, error), rewritePointers bool, beforeClose func() error) (err error) {
	if parent == nil || idx == nil || roots == nil || produce == nil {
		return rootpublication.ErrResourceOwnership
	}
	started := false
	var staging primaryJointDecisionV6
	defer func() {
		roots.Release()
		closeErr := idx.close()
		err = errors.Join(err, closeErr)
		if !started && closeErr == nil {
			if rootpublication.SamePhysicalIdentity(staging.Data, staging.Data) {
				err = errors.Join(err, cleanupOfflinePrimaryStageV6(db.dir, parent, indexNewFileName, staging.Data))
			}
			if rootpublication.SamePhysicalIdentity(staging.Primary, staging.Primary) {
				err = errors.Join(err, cleanupOfflinePrimaryStageV6(db.dir, parent, primaryNewFileName, staging.Primary))
			}
		} else if err != nil {
			err = errors.Join(err, ErrRecoveryRequired)
		}
	}()
	staging, err = primaryJointDecisionForV6(parent, idx, durableRootSelectionV1{})
	if err != nil {
		return err
	}
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
	if err = idx.pager.WithStableResourceFile(func(f *os.File) error { return rootpublication.ValidateStableChildLink(parent, f, indexNewFileName) }); err != nil {
		return err
	}
	if err = idx.primary.Pager().WithStableResourceFile(func(f *os.File) error { return rootpublication.ValidateStableChildLink(parent, f, primaryNewFileName) }); err != nil {
		return err
	}
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

// Cleanup only the exact staged identity under the retained construction parent.
// A rebound diagnostic directory never supplies deletion authority.
func cleanupOfflinePrimaryStageV6(dir string, parent *os.File, name string, expected rootpublication.StableIdentity) (err error) {
	f, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	actual, err := rootpublication.StableIdentityFromFile(f)
	if err != nil {
		return err
	}
	if !rootpublication.SamePhysicalIdentity(actual, expected) {
		return rootpublication.ErrResourceOwnership
	}
	if err = rootpublication.ValidateStableChildLink(parent, f, name); err != nil {
		return err
	}
	if err = rootpublication.RemoveStableChildFile(parent, name); err != nil {
		return err
	}
	return observeStableNamespaceMutation(durabilitycut.NamespaceUnlink, durabilitycut.ResourceIndex, dir, filepath.Join(dir, name), "", parent, f, name, "")
}
