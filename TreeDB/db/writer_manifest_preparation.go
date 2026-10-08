package db

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// writerPublicationPreparation retains a private Apply's external authority
// while all root serialization is released. It is single-owner until acceptance.
// Its immutable replacement is never installed merely because preparation ran.
type writerPublicationPreparation struct {
	db              *DB
	idx             *indexGen
	runtime         *rootPublicationRuntimeV1
	basis           page.MetaPageBody
	manifestBasis   *leafGenerationManifest
	staged          stagedLeafGenerationManifestResult
	pending         []leafGenerationPendingFile
	baseResources   *rootpublication.StableResourceSet
	producerPins    *rootpublication.StableResourceSet
	producerIDs     map[uint32]struct{}
	manifestClosure *LeafGenerationManifestStablePreparedClosure
	resources       *rootpublication.StableResourceSet
	accepted        bool
}

// Called with writer serialization and the durable publication gate held.
func (db *DB) selectWriterPublicationPreparation(idx *indexGen, touched []uint32) (_ *writerPublicationPreparation, retErr error) {
	p := &writerPublicationPreparation{db: db, idx: idx, runtime: db.rootPublication}
	idx.acquire()
	defer func() {
		if retErr != nil {
			_ = p.release()
		}
	}()
	db.mu.RLock()
	p.basis = db.meta
	p.manifestBasis = db.leafGenerationManifest
	db.mu.RUnlock()
	if _, err := db.registerLeafPageLogSegmentsForPublish(); err != nil {
		return nil, err
	}
	var err error
	if p.runtime != nil {
		p.baseResources, err = p.runtime.cloneVisibleResources()
	} else {
		p.baseResources, err = rootpublication.CloneStableResourceSetExcludingKinds(db.durableRoot.slotResources[db.durableRoot.slot])
	}
	if err != nil {
		return nil, err
	}
	if len(touched) != 0 || db.indexOuterLeavesInValueLog {
		p.producerIDs = make(map[uint32]struct{}, len(touched))
	}
	for _, id := range touched {
		if id != 0 {
			p.producerIDs[id] = struct{}{}
		}
	}
	if db.indexOuterLeavesInValueLog && db.valueLogManager != nil {
		for _, id := range db.valueLogManager.CurrentWritableFileIDs() {
			lane, _ := valuelog.DecodeFileID(id)
			if lane == valuelog.ReservedLeafLogLaneID {
				p.producerIDs[id] = struct{}{}
			}
		}
	}
	if len(p.producerIDs) != 0 {
		p.producerPins, err = db.captureRegisteredDurableValueLogResourcesV1(p.producerIDs)
		if err != nil {
			return nil, err
		}
	}
	if p.manifestBasis != nil && db.leafGenerationManifestStore != nil && db.leafGenerationManifestStore.mode == leafGenerationManifestStable {
		p.pending = db.snapshotLeafGenerationPendingFileIDs(0)
		p.staged, err = db.stagedLeafGenerationManifestWithPendingResult(p.manifestBasis, 0, p.basis.CommitSeq+1)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

// No DB/write/commit/root-build/durable/group/publish-prepare lock may be held.
func (p *writerPublicationPreparation) prepare(syncWrite bool) error {
	dependencySync := syncWrite && p.runtime == nil
	if err := p.db.flushFinalizeCommitDurability(p.idx, p.db.currentValueLogAppender(), dependencySync); err != nil {
		return err
	}
	if p.staged.changed {
		var err error
		p.manifestClosure, p.staged.manifest, err = p.db.prepareLeafGenerationManifestStableCandidate(p.staged.manifest)
		if err != nil {
			return err
		}
		p.resources, err = p.manifestClosure.TakeStableResources()
		if err != nil {
			return err
		}
	} else if p.staged.manifest != nil {
		// A revision number from the operational view is not replacement authority.
		// Membership reuse is permitted only against the retained exact base token.
		found := false
		for _, token := range p.baseResources.Tokens() {
			if token.Kind() == rootpublication.ResourceOuterLeafManifest && token.Generation() == p.manifestBasis.ManifestRevision {
				found = true
			}
		}
		if !found && len(p.producerIDs) != 0 {
			return fmt.Errorf("%w: unchanged writer manifest lacks exact base authority", rootpublication.ErrUnresolvedResource)
		}
	}
	if p.staged.manifest != nil {
		for id := range p.producerIDs {
			lane, _ := valuelog.DecodeFileID(id)
			raw, leaf := rawLeafGenerationFileID(id)
			if lane == valuelog.ReservedLeafLogLaneID && leaf && !p.staged.manifest.hasNonDeletedFileID(raw) {
				return fmt.Errorf("%w: writer manifest omits producer %d", rootpublication.ErrUnresolvedResource, raw)
			}
		}
	}
	return nil
}

// Called after reacquiring the original serialization order, before capture.
func (p *writerPublicationPreparation) validate() error {
	db := p.db
	if err := db.checkReadAdmissionLocked(); err != nil {
		return err
	}
	// A vacuum cutover may release writeMu while it synchronizes. Never wait
	// for that interval while holding the durable gate or a shared write lock.
	if db.vacuumCutoverInProgress.Load() {
		return errDurableRootCandidateStale
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	db.mu.RLock()
	same := db.idx.Load() == p.idx && db.rootPublication == p.runtime && db.meta.UserRootPageID == p.basis.UserRootPageID && db.meta.SystemRootPageID == p.basis.SystemRootPageID && db.meta.CommitSeq == p.basis.CommitSeq && db.leafGenerationManifest == p.manifestBasis
	db.mu.RUnlock()
	if !same {
		return errDurableRootCandidateStale
	}
	if p.staged.manifest != nil {
		if !reflect.DeepEqual(p.pending, db.snapshotLeafGenerationPendingFileIDs(0)) {
			return errDurableRootCandidateStale
		}
		now, err := db.stagedLeafGenerationManifestWithPendingResult(p.manifestBasis, 0, p.basis.CommitSeq+1)
		if err != nil {
			return err
		}
		// The store alone chooses the actual immutable revision. All other contents
		// must be the identical semantic candidate selected before the wait.
		sameMembership := now.manifest == p.staged.manifest
		if p.staged.changed {
			candidate := *p.staged.manifest
			candidate.ManifestRevision = now.manifest.ManifestRevision
			sameMembership = reflect.DeepEqual(&candidate, now.manifest)
		}
		if now.changed != p.staged.changed || !sameMembership || !reflect.DeepEqual(now.pendingFileIDs, p.staged.pendingFileIDs) {
			return errDurableRootCandidateStale
		}
	}
	return nil
}

func (p *writerPublicationPreparation) hasManifestPreparation() bool {
	return p != nil && p.staged.manifest != nil
}

func (p *writerPublicationPreparation) bind(opts *finalizeCommitOptions) {
	opts.writerPreparation = p
	opts.durableIndex = p.idx
	opts.durableResources = p.resources
	p.resources = nil
	opts.leafManifestAlreadyPersistent = p.staged.changed
}

// Must run outside root serialization. Accepted authority is never abandoned,
// including when a later admission/durability wait returned an error.
func (p *writerPublicationPreparation) release() error {
	if p == nil {
		return nil
	}
	p.baseResources.Release()
	p.producerPins.Release()
	p.resources.Release()
	var err error
	if p.manifestClosure != nil {
		if !p.accepted {
			err = p.manifestClosure.abandonUnpublished()
		}
		p.manifestClosure.Release()
	}
	if p.idx != nil {
		p.db.releaseIndex(p.idx)
		p.idx = nil
	}
	return errors.Join(err)
}

// publishWriterOutput takes the caller's root serialization and durable gate.
// Every return releases them; private-output rollback remains with the caller.
func (b *Batch) publishWriterOutput(idx *indexGen, newRoot, systemRoot uint64, retired []uint64, syncWrite bool, metrics adaptive.Metrics, touched []uint32, delta *valueLogRefDelta, intent *commandWALBatchIntent, conditional *ConditionalTxn, opts finalizeCommitOptions, releaseRoot, reacquireRoot func()) (post finalizeCommitPost, retErr error) {
	db := b.db
	rootHeld, durableHeld := true, true
	release := func() {
		if durableHeld {
			db.durablePublishMu.Unlock()
			durableHeld = false
		}
		if rootHeld {
			releaseRoot()
			rootHeld = false
		}
	}
	reacquire := func() {
		reacquireRoot()
		rootHeld = true
		db.durablePublishMu.Lock()
		durableHeld = true
	}
	var p *writerPublicationPreparation
	defer func() {
		release()
		retErr = errors.Join(retErr, p.release())
	}()
	if b.physicalOnly && intent != nil && (intent.lsn == 0 || intent.fromReplay || len(intent.durablePrefixGroup) != 0) {
		return post, ErrCommandWALRejected
	}
	var err error
	p, err = db.selectWriterPublicationPreparation(idx, touched)
	if err != nil {
		return post, wrapFinalizeCommitError(err, true)
	}
	if p.basis.CommitSeq != opts.expectedBaseCommitSeq || p.basis.SystemRootPageID != systemRoot {
		return post, errDurableRootCandidateStale
	}
	release()
	if err := p.prepare(syncWrite); err != nil {
		return post, wrapFinalizeCommitError(err, true)
	}
	if hook := db.testWriterManifestPreparedHook; hook != nil {
		hook()
	}
	if !opts.writerSerialized {
		if hook := db.testAfterOptimisticPublishPrepareHook; hook != nil {
			hook()
		}
	}
	reacquire()
	guardedStart := time.Now()
	defer func() { db.observeFlushApplyGuardedPublish(time.Since(guardedStart), retErr == nil) }()
	validate := func() error {
		if err := p.validate(); err != nil {
			if errors.Is(err, errDurableRootCandidateStale) {
				db.observeFlushApplyMismatch()
			}
			return err
		}
		if conditional != nil {
			return conditional.validateReadSetAtPublish()
		}
		return nil
	}
	if err := validate(); err != nil {
		return post, wrapFinalizeCommitError(err, true)
	}
	// An ordinary unassigned intent retains Batch.write's outer publish guard.
	// Only that branch can perform dependency/journal synchronization here.
	if intent != nil && intent.lsn == 0 {
		release()
		_, err = db.appendRawKVCommandWALIntent(intent, syncWrite)
		reacquire()
		if err == nil {
			err = validate()
		}
		if err != nil {
			if intent.lsn != 0 {
				db.poisonCommandWALAfterPostAppendFailure(intent)
				err = errors.Join(err, ErrRecoveryRequired)
			}
			return post, wrapFinalizeCommitError(err, true)
		}
	} else if intent != nil {
		if _, err := db.appendRawKVCommandWALIntent(intent, syncWrite); err != nil {
			return post, wrapFinalizeCommitError(err, true)
		}
	}
	// No waits occur while this guard is held. Exact producer pins above protected
	// detached work; the guard now seals GC across capture and acceptance.
	db.publishPrepareMu.RLock()
	guard := &finalizeCommitPrepareGuard{db: db}
	defer guard.Release()
	if intent != nil {
		wal := commandWALFinalizeOptions(intent)
		opts.commandWALPublish = wal.commandWALPublish
		opts.appliedCommandLSN = wal.appliedCommandLSN
		opts.appliedRanges = wal.appliedRanges
	}
	opts.skipPrePublishFlush = true
	opts.durablePublishLocked = true
	opts.durablePublishRelease = func() {
		if durableHeld {
			db.durablePublishMu.Unlock()
			durableHeld = false
		}
	}
	opts.releaseRootSerialization = func() {
		if rootHeld {
			releaseRoot()
			rootHeld = false
		}
	}
	p.bind(&opts)
	var manifest *leafGenerationManifest
	if p.staged.changed {
		manifest = p.staged.manifest
	}
	post, err = db.finalizeCommitLockedWithOptions(newRoot, systemRoot, retired, syncWrite, metrics, touched, db.indexOuterLeavesInValueLog, delta, manifest, p.staged.rawFileIDs, opts)
	if err != nil && intent != nil && intent.lsn != 0 && !post.accepted {
		db.poisonCommandWALAfterPostAppendFailure(intent)
		err = errors.Join(err, ErrRecoveryRequired)
	}
	p.accepted = err == nil || post.accepted || errors.Is(err, ErrRecoveryRequired)
	return post, err
}

// consumePending runs at successful visibility while the publication gate is
// still held. Later registrations retain their own attribution and ownership.
func (p *writerPublicationPreparation) consumePending() {
	if p == nil {
		return
	}
	p.accepted = true
	if len(p.staged.pendingFileIDs) == 0 {
		return
	}
	db := p.db
	db.leafGenerationPendingMu.Lock()
	defer db.leafGenerationPendingMu.Unlock()
	acceptedIDs := make(map[uint32]struct{}, len(p.staged.pendingFileIDs))
	for _, id := range p.staged.pendingFileIDs {
		acceptedIDs[id] = struct{}{}
	}
	for _, item := range p.pending {
		if _, exists := db.leafGenerationPendingSet[item.rawFileID]; exists && db.leafGenerationPendingCommitSeq[item.rawFileID] == item.commitSeq {
			if _, accepted := acceptedIDs[item.rawFileID]; accepted {
				delete(db.leafGenerationPendingSet, item.rawFileID)
				delete(db.leafGenerationPendingCommitSeq, item.rawFileID)
			}
		}
	}
	keep := db.leafGenerationPendingFileIDs[:0]
	for _, id := range db.leafGenerationPendingFileIDs {
		if _, exists := db.leafGenerationPendingSet[id]; exists {
			keep = append(keep, id)
		}
	}
	db.leafGenerationPendingFileIDs = keep
}
