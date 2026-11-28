package db

import (
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// NoteLeafLogDirSyncPending records that a new leaf_vlog segment file was
// created with its parent directory sync deferred. The next same-directory
// leaf-generation manifest replacement sync (or the fail-closed pre-meta
// durable-root barrier if no manifest replacement runs) must sync leaf_vlog
// before any meta page referencing the segment becomes durable.
func (db *DB) NoteLeafLogDirSyncPending() {
	if db == nil {
		return
	}
	db.leafLogDirUnsyncedSeq.Add(1)
}

func (db *DB) markLeafLogDirSyncedThrough(seq uint64) {
	if db == nil || seq == 0 {
		return
	}
	for {
		cur := db.leafLogDirSyncedSeq.Load()
		if cur >= seq {
			return
		}
		if db.leafLogDirSyncedSeq.CompareAndSwap(cur, seq) {
			return
		}
	}
}

func (db *DB) syncPendingLeafLogDirIfNeeded() error {
	if db == nil || db.dir == "" {
		return nil
	}
	target := db.leafLogDirUnsyncedSeq.Load()
	if target == 0 || db.leafLogDirSyncedSeq.Load() >= target {
		return nil
	}
	leafDir := LeafLogDirPath(db.dir)
	if store := db.leafGenerationManifestStore; store != nil && store.mode == leafGenerationManifestStable {
		if err := store.syncParentDir(); err != nil {
			return err
		}
	} else {
		if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, leafDir, leafDir); err != nil {
			return err
		}
		if err := syncDirFn(leafDir); err != nil {
			return err
		}
		if err := durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, leafDir, leafDir); err != nil {
			return err
		}
	}
	db.markLeafLogDirSyncedThrough(target)
	return nil
}

type packedRootPublication struct {
	afterActivation          func()
	afterManifestPreparation func() error
}

type ordinaryLeafManifestPublication struct {
	staged                stagedLeafGenerationManifestResult
	prepared              *LeafGenerationManifestStablePreparedClosure
	dependenciesPreSynced bool
}

// The exact resource set belongs to the candidate after preparation. This
// separate owner keeps the compatibility rollback bytes until visible admission
// or terminal teardown, including while the same candidate retries a pre-meta cut.
type inlineLeafManifestPublication struct {
	candidate *durableRootPublishCandidateV1
	update    ordinaryLeafManifestPublication
}

// finishFailedInlineLeafManifest runs after admitted publishers have drained
// and before the stable manifest store closes its retained parent. Only the
// exact parked candidate with an untouched meta can be safely abandoned.
func (db *DB) finishFailedInlineLeafManifest() error {
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	inline := db.inlineLeafManifest
	db.inlineLeafManifest = nil
	if inline == nil {
		return nil
	}
	if db.durableRoot.pending == inline.candidate && !inline.candidate.metaMutated {
		return inline.update.prepared.abandonUnpublished()
	}
	// Preserve both immutable revisions when the root may already be durable.
	return inline.update.prepared.ReleaseWithError()
}

type ordinaryLeafManifestAuthority struct {
	revision uint64
	identity rootpublication.StableIdentity
	digest   [32]byte
}

type ordinaryLeafManifestCache struct {
	authority ordinaryLeafManifestAuthority
	view      *leafGenerationView
}

func (p ordinaryLeafManifestPublication) finish(keep bool) error {
	if p.prepared == nil {
		return nil
	}
	if keep {
		// The exact token has transferred. ReleaseWithError records any
		// remaining original cleanup failure for checked teardown.
		_ = p.prepared.ReleaseWithError()
		return nil
	}
	return p.prepared.abandonUnpublished()
}

func (p ordinaryLeafManifestPublication) bind(post *finalizeCommitPost, opts *finalizeCommitOptions) {
	opts.leafManifestAlreadyPersistent = true
	opts.ordinaryLeafManifestBound = true
	post.clearLeafGenerationPendingFileIDs = p.staged.pendingFileIDs
}

// bindPublicationLeafManifest checks the exact immutable generation authority in
// the captured candidate closure. An ordinary successor with no newly uncovered
// leaf files keeps its existing token. A pack stages promoted files and any
// late descriptor appends against its original copy basis. The
// exact staged coverage precedes build lock release, and persistence runs under
// the next-root gate. The superseded durable slot keeps its original authority.
// This function consumes resources on error or replacement.
func (db *DB) bindPublicationLeafManifest(resources *rootpublication.StableResourceSet, idx *indexGen, next page.MetaPageBody, pack *leafGenerationManifest, packRawFileIDs []uint32, opts *finalizeCommitOptions) (result *rootpublication.StableResourceSet, update ordinaryLeafManifestPublication, retErr error) {
	result = resources
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, db.releaseCapturedRootResources(result), update.finish(db.publicationPoisoned.Load()))
			result = nil
			update = ordinaryLeafManifestPublication{}
		}
	}()
	if db.leafGenerationManifestStore == nil || db.leafGenerationManifestStore.mode != leafGenerationManifestStable {
		if pack != nil {
			return result, update, fmt.Errorf("%w: stable manifest replacement mode unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
		}
		return resources, update, nil
	}

	base := pack
	if pack == nil {
		authority, err := exactOrdinaryLeafManifestAuthority(resources)
		if err != nil {
			return result, update, err
		}
		if authority.revision == 0 {
			return resources, update, nil
		}
		// A compatibility replacement or an abandoned candidate may leave or
		// reuse a revision number. Only the inherited token gives coverage credit.
		if db.ordinaryLeafManifestCache.authority != authority {
			var exact *leafGenerationManifest
			if err := resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
				var err error
				exact, _, err = loadSelectedDurableLeafGenerationManifestTokens(tokens)
				return err
			}); err != nil {
				return result, update, err
			}
			if exact == nil || exact.ManifestRevision != authority.revision {
				return result, update, fmt.Errorf("%w: missing inherited leaf-generation manifest revision %d", rootpublication.ErrResourceConflict, authority.revision)
			}
			db.ordinaryLeafManifestCache = ordinaryLeafManifestCache{authority: authority, view: newLeafGenerationView(exact)}
		}
		files := db.ordinaryLeafManifestCache.view.FileToGeneration
		missing, err := firstUncoveredOuterLeafFile(resources, func(fileID uint32) bool { return files[fileID] != 0 })
		if err != nil {
			return result, update, err
		}
		if missing == 0 {
			return result, update, nil
		}
		db.mu.RLock()
		base = db.leafGenerationManifest
		db.mu.RUnlock()
	}
	var err error
	update.staged, err = db.stagedLeafGenerationManifestWithPendingResultAndLimit(base, 0, next.CommitSeq, opts.preparedLimits, packRawFileIDs...)
	if err != nil {
		return result, update, err
	}
	if pack != nil && !update.staged.changed {
		return result, update, fmt.Errorf("%w: packed publication did not advance the leaf-generation manifest", rootpublication.ErrResourceConflict)
	}
	update.staged.rawFileIDs = append(update.staged.rawFileIDs, packRawFileIDs...)
	covered := func(fileID uint32) bool {
		return leafManifestContainsActiveFile(update.staged.manifest, fileID)
	}
	missing, err := firstUncoveredOuterLeafFile(resources, covered)
	if err != nil {
		return result, update, err
	}
	// GC can retire or delete an older raw or packed outer-leaf generation
	// without publishing a root commit, leaving its exact token inherited
	// conservatively from visibleBase across both ordinary and subsequent
	// packed publications. Project out only outer-leaf files that the final
	// candidate roots prove unreachable; any still-reachable omitted file
	// remains uncovered and fails closed below.
	if missing != 0 {
		projected, err := db.projectUncoveredPublicationOuterLeafFiles(resources, idx, next, covered, opts)
		if err != nil {
			return result, update, err
		}
		if projected != nil {
			result = projected
			if err := db.releaseCapturedRootResources(resources); err != nil {
				return result, update, err
			}
			resources = projected
			missing, err = firstUncoveredOuterLeafFile(resources, covered)
			if err != nil {
				return result, update, err
			}
		}
	}
	if missing != 0 {
		return result, update, fmt.Errorf("%w: publication leaf-generation manifest omits leaf file %d", rootpublication.ErrUnresolvedResource, missing)
	}
	// The private roots are built, reuse is still open and resources retain the
	// candidate's exact files. The publication gate keeps the predecessor and
	// operational manifest fixed while the staged successor does its file and
	// directory syncs. No code may reacquire build locks under that gate.
	opts.releaseRootConstruction()
	var syncDeps func() error
	if !durabilitycut.Enabled() && resources != nil {
		// Flush external dependency buffers (e.g. value-l255-*.log) to the OS
		// page cache BEFORE entering parallel manifest/dependency fsync so both
		// fsync calls reach the kernel simultaneously and coalesce into one
		// JBD2 commit instead of staggering across two commits.
		if flushErr := resources.FlushThrough(); flushErr != nil {
			return result, update, fmt.Errorf("flush durable-root external dependencies: %w", flushErr)
		}
		if db.leafGenerationManifestStore != nil && db.leafGenerationManifestStore.mode == leafGenerationManifestStable &&
			db.leafGenerationManifestStore.hooks.BeforeTempCreate == nil && db.leafGenerationManifestStore.hooks.BeforeRename == nil {
			syncDeps = func() error {
				if syncErr := resources.SyncThrough(); syncErr != nil {
					return fmt.Errorf("sync durable-root external dependencies: %w", syncErr)
				}
				return nil
			}
		} else {
			if syncErr := resources.SyncThrough(); syncErr != nil {
				return result, update, fmt.Errorf("sync durable-root external dependencies: %w", syncErr)
			}
		}
		update.dependenciesPreSynced = true
	}
	// Sample the deferred leaf_vlog directory-sync sequence strictly before the
	// stable manifest replacement syncs maindb/leaf_vlog. Any segment created
	// before this load is provably durable in leaf_vlog once replacement returns.
	leafDirSeq := db.leafLogDirUnsyncedSeq.Load()
	update.prepared, update.staged.manifest, err = db.prepareLeafGenerationManifestStableCandidateWithDeps(update.staged.manifest, syncDeps)
	if err != nil {
		return result, update, err
	}
	db.markLeafLogDirSyncedThrough(leafDirSeq)
	if pack != nil {
		if err := opts.packedRootPublication.afterManifestPreparation(); err != nil {
			return result, update, err
		}
	}
	manifestResources, err := update.prepared.TakeStableResources()
	if err != nil {
		return result, update, err
	}
	replacement, err := db.replaceDurableRootManifestResourcesV1(resources, manifestResources)
	if err = errors.Join(err, db.releaseCapturedRootResources(manifestResources)); err != nil {
		return result, update, errors.Join(err, db.releaseCapturedRootResources(replacement))
	}
	return replacement, update, db.releaseCapturedRootResources(resources)
}

// prepareSyncOperationalLeafManifest handles synchronous commits whose durable
// root does not yet inherit an immutable leaf-generation manifest token
// (authority.revision == 0, such as initial checkpoints before any pack).
// Running operational manifest replacement here — before EnqueueBuilt — lets
// its fsync(leaf_vlog) satisfy any deferred leaf_vlog segment creation barrier
// in one directory sync and pre-sync external dependencies (e.g.
// value-l255-*.log) sequentially before manifest replacement when
// durabilitycut is disabled so Publish() skips redundant re-syncing without
// ext4 JBD2/directory-lock contention.
func (db *DB) prepareSyncOperationalLeafManifest(
	resources *rootpublication.StableResourceSet,
	next page.MetaPageBody,
	syncWrite bool,
	post *finalizeCommitPost,
	opts *finalizeCommitOptions,
) (leafManifest *leafGenerationManifest, rawFileIDs []uint32, dependenciesPreSynced bool, err error) {
	if !syncWrite || db.leafPageLog == nil || opts.ordinaryLeafManifestBound {
		return nil, nil, false, nil
	}
	if db.leafGenerationManifestStore == nil || db.leafGenerationManifestStore.mode != leafGenerationManifestStable {
		return nil, nil, false, nil
	}
	db.mu.RLock()
	base := db.leafGenerationManifest
	db.mu.RUnlock()
	staged, err := db.stagedLeafGenerationManifestWithPendingResultAndLimit(base, 0, next.CommitSeq, opts.preparedLimits)
	if err != nil {
		return nil, nil, false, err
	}
	var toPersist *leafGenerationManifest
	if staged.changed {
		toPersist = staged.manifest
	} else if db.leafGenerationManifestDirty.Load() && base != nil {
		toPersist = base.clone()
	} else {
		return nil, nil, false, nil
	}
	opts.releaseRootConstruction()
	var syncDeps func() error
	overlapManifestIO := !durabilitycut.Enabled() &&
		db.leafGenerationManifestStore != nil &&
		db.leafGenerationManifestStore.mode == leafGenerationManifestStable &&
		db.leafGenerationManifestStore.hooks.BeforeTempCreate == nil &&
		db.leafGenerationManifestStore.hooks.BeforeRename == nil &&
		db.leafGenerationManifestStore.hooks.BeforeParentSync == nil
	if !durabilitycut.Enabled() && resources != nil {
		// Flush external dependency buffers (e.g. value-l255-*.log) to the OS
		// page cache BEFORE entering parallel manifest/dependency fsync so both
		// fsync calls reach the kernel simultaneously and coalesce into one
		// JBD2 commit instead of staggering across two commits.
		if flushErr := resources.FlushThrough(); flushErr != nil {
			return nil, nil, false, fmt.Errorf("flush durable-root external dependencies: %w", flushErr)
		}
		if overlapManifestIO {
			syncDeps = func() error {
				if syncErr := resources.SyncThrough(); syncErr != nil {
					return fmt.Errorf("sync durable-root external dependencies: %w", syncErr)
				}
				return nil
			}
		} else {
			if syncErr := resources.SyncThrough(); syncErr != nil {
				return nil, nil, false, fmt.Errorf("sync durable-root external dependencies: %w", syncErr)
			}
		}
		dependenciesPreSynced = true
	}
	if err := db.replaceLeafGenerationManifestOperationalWithOptions(toPersist, syncDeps, overlapManifestIO); err != nil {
		return nil, nil, false, err
	}
	db.leafGenerationManifestDirty.Store(false)
	if !staged.changed {
		return nil, nil, dependenciesPreSynced, nil
	}
	opts.leafManifestAlreadyPersistent = true
	opts.ordinaryLeafManifestBound = true
	post.clearLeafGenerationPendingFileIDs = append(post.clearLeafGenerationPendingFileIDs[:0], staged.pendingFileIDs...)
	return staged.manifest, staged.rawFileIDs, dependenciesPreSynced, nil
}

// GC can retire a raw or packed outer-leaf generation whose exact token is
// still retained conservatively by a predecessor. Remove uncovered outer-leaf
// identities from this successor only if the final roots prove they are
// unreachable. Predecessor slots and reachable identities retain their
// original authority.
func (db *DB) projectUncoveredPublicationOuterLeafFiles(resources *rootpublication.StableResourceSet, idx *indexGen, next page.MetaPageBody, covered func(uint32) bool, opts *finalizeCommitOptions) (*rootpublication.StableResourceSet, error) {
	references, err := db.scanCandidateValueLogReferencesWithCountsAndLimitsV1(idx, next, opts.valueLogPublicationLocked, nil, opts.preparedLimits)
	if err != nil {
		return nil, fmt.Errorf("project outer-leaf dependencies for manifest replacement: %w", err)
	}
	var selectedRaw, selectedPack []rootpublication.StableIdentity
	var droppedRaw, droppedPack bool
	kinds := [...]rootpublication.ResourceKind{
		rootpublication.ResourceOuterLeafLog,
		rootpublication.ResourceOuterLeafPack,
	}
	err = resources.WalkPhysicalResourceIdentities(kinds[:], func(kind rootpublication.ResourceKind, generation uint64, identity rootpublication.StableIdentity, _ [32]byte) error {
		if generation > math.MaxUint32 {
			return fmt.Errorf("%w: invalid physical leaf file ID %d", rootpublication.ErrResourceConflict, generation)
		}
		rawFileID, valid := rawLeafGenerationFileID(uint32(generation))
		if !valid {
			return fmt.Errorf("%w: invalid physical leaf file ID %d", rootpublication.ErrResourceConflict, generation)
		}
		_, reachable := references[page.ValueLogFileID(rawFileID)]
		keep := covered(rawFileID) || reachable
		switch kind {
		case rootpublication.ResourceOuterLeafLog:
			if keep {
				selectedRaw = append(selectedRaw, identity)
			} else {
				droppedRaw = true
			}
		case rootpublication.ResourceOuterLeafPack:
			if keep {
				selectedPack = append(selectedPack, identity)
			} else {
				droppedPack = true
			}
		}
		return nil
	})
	if err != nil || (!droppedRaw && !droppedPack) {
		return nil, err
	}
	cur := resources
	if droppedRaw {
		nextSet, err := rootpublication.CloneStableResourceSetSelectingPhysicalKind(cur, rootpublication.ResourceOuterLeafLog, selectedRaw)
		if err != nil {
			return nil, err
		}
		cur = nextSet
	}
	if droppedPack {
		nextSet, err := rootpublication.CloneStableResourceSetSelectingPhysicalKind(cur, rootpublication.ResourceOuterLeafPack, selectedPack)
		if err != nil {
			if cur != resources {
				err = errors.Join(err, db.releaseCapturedRootResources(cur))
			}
			return nil, err
		}
		if cur != resources {
			if releaseErr := db.releaseCapturedRootResources(cur); releaseErr != nil {
				return nil, errors.Join(releaseErr, db.releaseCapturedRootResources(nextSet))
			}
		}
		cur = nextSet
	}
	return cur, nil
}

func exactOrdinaryLeafManifestAuthority(resources *rootpublication.StableResourceSet) (ordinaryLeafManifestAuthority, error) {
	var authority ordinaryLeafManifestAuthority
	kinds := [...]rootpublication.ResourceKind{rootpublication.ResourceOuterLeafManifest}
	err := resources.WalkPhysicalResourceIdentities(kinds[:], func(_ rootpublication.ResourceKind, revision uint64, identity rootpublication.StableIdentity, digest [32]byte) error {
		if authority.revision != 0 || revision == 0 || identity == (rootpublication.StableIdentity{}) || digest == ([32]byte{}) {
			return fmt.Errorf("%w: publication requires one exact outer-leaf manifest", rootpublication.ErrResourceConflict)
		}
		authority = ordinaryLeafManifestAuthority{revision: revision, identity: identity, digest: digest}
		return nil
	})
	return authority, err
}

func firstUncoveredOuterLeafFile(resources *rootpublication.StableResourceSet, covered func(uint32) bool) (uint32, error) {
	var missing uint32
	kinds := [...]rootpublication.ResourceKind{rootpublication.ResourceOuterLeafLog, rootpublication.ResourceOuterLeafPack}
	err := resources.WalkPhysicalResourceIdentities(kinds[:], func(_ rootpublication.ResourceKind, generation uint64, _ rootpublication.StableIdentity, _ [32]byte) error {
		if generation > math.MaxUint32 {
			return fmt.Errorf("%w: invalid physical leaf file ID %d", rootpublication.ErrResourceConflict, generation)
		}
		fileID, valid := rawLeafGenerationFileID(uint32(generation))
		if !valid {
			return fmt.Errorf("%w: invalid physical leaf file ID %d", rootpublication.ErrResourceConflict, generation)
		}
		if missing == 0 && !covered(fileID) {
			missing = fileID
		}
		return nil
	})
	return missing, err
}

func leafManifestContainsActiveFile(manifest *leafGenerationManifest, fileID uint32) bool {
	if manifest != nil {
		for _, gen := range manifest.Generations {
			if gen.State == leafGenerationStateDeleted || gen.State == leafGenerationStateRetiring {
				continue
			}
			for _, existing := range gen.FileIDs {
				if existing == fileID {
					return true
				}
			}
		}
	}
	return false
}
