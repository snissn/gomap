package db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// rootPublicationRuntimeV1 is the live coordinator bridge. The coordinator
// owns queued logical candidates and their resource closures. This runtime
// owns only the independently cloned visible closure and publisher-time seals
// needed to turn an ordered visible prefix into one recovery-selectable root.
type rootPublicationRuntimeV1 struct {
	db      *DB
	idx     *indexGen
	lineage rootpublication.DurableRootLineageID

	mu sync.Mutex

	coordinator      *rootpublication.Coordinator
	visibleResources *rootpublication.StableResourceSet
	visibleMembers   map[uint64]*rootPublicationVisibleMemberV1
	debt             []*freelist.PreparedCOWCandidateV1
	seals            []*rootPublicationSealV1
	activeSeal       *rootPublicationSealV1
	poison           error
}

// rootPublicationVisibleMemberV1 contains only precomputed, non-fallible
// activation state. It is installed by the transaction Activate callback
// before the coordinator advances its visible frontier.
type rootPublicationVisibleMemberV1 struct {
	sequence         uint64
	next             page.MetaPageBody
	prepared         *freelist.PreparedCOWCandidateV1
	resources        *rootpublication.StableResourceSet
	install          *rootPublicationVisibleInstallV1
	activated        bool
	resourcesAdopted bool
}

// rootPublicationVisibleInstallV1 owns all state that could otherwise make
// visible activation fail after the allocator generation has crossed its
// visibility boundary. Preparation resolves the value-log set and leaf view;
// activate performs only bounded in-memory swaps and bookkeeping.
type rootPublicationVisibleInstallV1 struct {
	db   *DB
	idx  *indexGen
	next page.MetaPageBody

	post                        finalizeCommitPost
	valueLogSet                 *valuelog.Set
	leafManifest                *leafGenerationManifest
	installLeafManifest         bool
	leafGenerationView          *leafGenerationView
	commandWALPublish           bool
	skipConditionalRootConflict bool
	oldUserRootID               uint64
	newUserRootID               uint64
	recordVacuumMutation        func()
	conditionalMutation         conditionalCommitMutation
	activated                   bool
	postActivationCompleted     bool
}

// rootPublicationSealV1 freezes the exact publisher-time durable base,
// alternate slot, dependency union, allocator prefix and index authority. A
// pre-meta retry reuses this object byte-for-byte. If later visible work joins
// the retry, Prepare creates a successor seal and retains this seal as ordered
// allocator debt until the successor publishes.
type rootPublicationSealV1 struct {
	latestSequence uint64
	groupLength    int
	idx            *indexGen
	base           durableRootRuntimeV1
	next           page.MetaPageBody
	resources      *rootpublication.StableResourceSet
	manifest       *rootpublication.DependencyManifestV1
	prepared       *freelist.PreparedCOWCandidateV1
	prefix         []*freelist.PreparedCOWCandidateV1
	token          *rootpublication.StableResourceToken
	record         rootpublication.DurableRootRecordV1
	meta           page.DurableMetaV1
	target         uint64
	materialized   bool
	released       bool
}

// publicRootPublicationErrorV1 preserves coordinator diagnostics while making
// an ambiguous/post-meta publication failure recognizable through TreeDB's
// public recovery-required sentinel.
func publicRootPublicationErrorV1(err error) error {
	if err == nil || !errors.Is(err, rootpublication.ErrRecoveryRequired) || errors.Is(err, ErrRecoveryRequired) {
		return err
	}
	return errors.Join(err, ErrRecoveryRequired)
}

func (db *DB) acquireRootPublicationBuilderV1() (*rootpublication.BuilderToken, error) {
	_, builder, err := db.acquireRootPublicationBuilderForRuntimeV1()
	return builder, err
}

func (db *DB) acquireRootPublicationBuilderForRuntimeV1() (*rootPublicationRuntimeV1, *rootpublication.BuilderToken, error) {
	if db == nil || db.rootPublication == nil {
		return nil, nil, nil
	}
	runtime := db.rootPublication
	if runtime.coordinator == nil {
		return runtime, nil, nil
	}
	builder, err := runtime.coordinator.AcquireBuilder(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("acquire root-publication builder: %w", publicRootPublicationErrorV1(err))
	}
	return runtime, builder, nil
}

// acquireCommandWALPublicationBuilderV1 returns with writeMu held and the
// builder bound to the runtime that is still live under that lock.
func (db *DB) acquireCommandWALPublicationBuilderV1() (*rootpublication.BuilderToken, error) {
	for {
		runtime, builder, err := db.acquireRootPublicationBuilderForRuntimeV1()
		if err != nil {
			return nil, err
		}
		if hook := db.testCommandWALAfterBuilderAcquireHook; hook != nil {
			hook()
		}
		db.writeMu.Lock()
		if err := db.checkWriteAdmissionLocked(); err != nil {
			db.writeMu.Unlock()
			if builder != nil {
				builder.Release()
			}
			return nil, err
		}
		if db.rootPublication == runtime {
			return builder, nil
		}
		db.writeMu.Unlock()
		if builder != nil {
			builder.Release()
		}
	}
}

func (runtime *rootPublicationRuntimeV1) cloneVisibleResources() (*rootpublication.StableResourceSet, error) {
	resources, _, err := runtime.cloneVisibleResourcesWithWork()
	return resources, err
}

func (runtime *rootPublicationRuntimeV1) cloneVisibleResourcesWithWork() (*rootpublication.StableResourceSet, rootpublication.StableResourceClosureWork, error) {
	if runtime == nil {
		return nil, rootpublication.StableResourceClosureWork{}, errors.New("missing root-publication runtime")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.poison != nil {
		return nil, rootpublication.StableResourceClosureWork{}, runtime.poison
	}
	resources, work, err := rootpublication.CloneStableResourceSetForLogicalObligationsWithWork(
		runtime.visibleResources, rootpublication.StableLogicalObligationRequirements{},
	)
	if err != nil {
		return nil, work, fmt.Errorf("clone visible root resources: %w", err)
	}
	return resources, work, nil
}

func (seal *rootPublicationSealV1) release() {
	if seal == nil || seal.released {
		return
	}
	seal.released = true
	seal.resources.Release()
	seal.resources = nil
	if seal.token != nil {
		seal.token.Release()
		seal.token = nil
	}
}

func rootPublicationLineageIDV1(db *DB, idx *indexGen, durable durableRootRuntimeV1) rootpublication.DurableRootLineageID {
	var material [80]byte
	if db != nil {
		binary.LittleEndian.PutUint64(material[0:8], durable.record.CommitSeq)
		binary.LittleEndian.PutUint64(material[8:16], durable.record.Freelist.GenerationID)
		binary.LittleEndian.PutUint64(material[16:24], durable.record.Freelist.HighWater)
		copy(material[24:56], durable.meta.RootRecordDigest[:])
		copy(material[56:72], []byte(db.dir))
	}
	if idx != nil {
		binary.LittleEndian.PutUint64(material[72:80], idx.id)
	}
	digest := sha256.Sum256(material[:])
	var lineage rootpublication.DurableRootLineageID
	copy(lineage[:], digest[:len(lineage)])
	if lineage == (rootpublication.DurableRootLineageID{}) {
		lineage[0] = 1
	}
	return lineage
}

func rootPublicationFrontierV1(meta page.MetaPageBody) rootpublication.Frontier {
	return rootpublication.NewFrontier(
		meta.CommitSeq,
		meta.UserRootPageID,
		meta.SystemRootPageID,
		meta.AppliedCommandLSN,
		meta.MaxEntryRevision,
	)
}

func (db *DB) initializeRootPublicationRuntimeV1(idx *indexGen) error {
	if db == nil || idx == nil || db.durableRoot.record.CommitSeq == 0 {
		return errors.New("missing root-publication durable base")
	}
	runtime, err := newRootPublicationRuntimeV1(db, idx, db.durableRoot, db.meta)
	if err != nil {
		return err
	}
	db.rootPublication = runtime
	return nil
}

func newRootPublicationRuntimeV1(db *DB, idx *indexGen, durable durableRootRuntimeV1, visible page.MetaPageBody) (*rootPublicationRuntimeV1, error) {
	if db == nil || idx == nil || durable.record.CommitSeq == 0 {
		return nil, errors.New("missing root-publication durable base")
	}
	baseResources, err := rootpublication.CloneStableResourceSetExcludingKinds(
		durable.slotResources[durable.slot],
	)
	if err != nil {
		return nil, fmt.Errorf("clone initial visible root resources: %w", err)
	}
	runtime := &rootPublicationRuntimeV1{
		db: db, idx: idx, lineage: rootPublicationLineageIDV1(db, idx, durable),
		visibleResources: baseResources,
		visibleMembers:   make(map[uint64]*rootPublicationVisibleMemberV1),
	}
	oldest, err := oldestRecoverableSlotCommitV1(durable.slotCommit)
	if err != nil {
		baseResources.Release()
		return nil, err
	}
	coordinator, err := rootpublication.New(rootpublication.Options{
		Publisher:                  runtime,
		FixedPublishDelay:          db.rootPublicationFixedDelay,
		InitialDurableFrontier:     rootPublicationFrontierV1(visible),
		OldestRecoverableCommitSeq: oldest,
		DurableRootLineage:         runtime.lineage,
		DurableRootSequence:        durable.record.CommitSeq,
	})
	if err != nil {
		baseResources.Release()
		return nil, err
	}
	runtime.coordinator = coordinator
	return runtime, nil
}

func stopRootPublicationRuntimeV1(runtime *rootPublicationRuntimeV1) (*rootpublication.RecoveryResourceHandoff, error) {
	if runtime == nil || runtime.coordinator == nil {
		return nil, nil
	}
	stopErr := runtime.coordinator.Stop(context.Background())
	handoff, handoffErr := runtime.coordinator.TakeRecoveryHandoff()
	if stopErr != nil || handoffErr != nil {
		return handoff, errors.Join(publicRootPublicationErrorV1(stopErr), publicRootPublicationErrorV1(handoffErr))
	}
	return handoff, nil
}

func rootPublicationLogicalCandidateIDV1(lineage rootpublication.DurableRootLineageID, generationID uint64, next page.MetaPageBody) freelist.CandidateIDV1 {
	var material [80]byte
	copy(material[0:16], lineage[:])
	binary.LittleEndian.PutUint64(material[16:24], generationID)
	binary.LittleEndian.PutUint64(material[24:32], next.CommitSeq)
	binary.LittleEndian.PutUint64(material[32:40], next.UserRootPageID)
	binary.LittleEndian.PutUint64(material[40:48], next.SystemRootPageID)
	binary.LittleEndian.PutUint64(material[48:56], next.AppliedCommandLSN)
	binary.LittleEndian.PutUint64(material[56:64], next.MaxEntryRevision)
	binary.LittleEndian.PutUint64(material[64:72], next.LastCommitHeight)
	binary.LittleEndian.PutUint64(material[72:80], next.TotalPages)
	digest := sha256.Sum256(material[:])
	var candidate freelist.CandidateIDV1
	copy(candidate[:], digest[:len(candidate)])
	return candidate
}

func (db *DB) prepareRootPublicationVisibleInstallV1(
	idx *indexGen,
	next page.MetaPageBody,
	oldUserRootID uint64,
	newUserRootID uint64,
	syncWrite bool,
	post finalizeCommitPost,
	leafManifest *leafGenerationManifest,
	leafManifestRawFileIDs []uint32,
	opts finalizeCommitOptions,
) (*rootPublicationVisibleInstallV1, error) {
	install := &rootPublicationVisibleInstallV1{
		db: db, idx: idx, next: next, post: post,
		commandWALPublish:           opts.commandWALPublish,
		skipConditionalRootConflict: opts.skipConditionalRootConflict,
		oldUserRootID:               oldUserRootID,
		newUserRootID:               newUserRootID,
		recordVacuumMutation:        opts.recordVacuumMutation,
		conditionalMutation:         opts.conditionalMutation,
	}
	if db.valueLogManager != nil {
		install.valueLogSet = db.valueLogManager.CurrentSetNoRefresh()
	}
	abort := true
	defer func() {
		if abort {
			install.abort()
		}
	}()

	manifestBasis := db.leafGenerationManifest
	if leafManifest != nil {
		manifestBasis = leafManifest
		install.leafManifest = leafManifest
		install.installLeafManifest = true
		install.post.persistLeafGenerationManifest = !opts.leafManifestAlreadyPersistent
		install.post.persistLeafGenerationIndexesOnly = opts.leafManifestAlreadyPersistent
		install.post.persistLeafGenerationManifestView = leafManifest
		install.post.persistLeafGenerationRawFileIDs = append(install.post.persistLeafGenerationRawFileIDs[:0], leafManifestRawFileIDs...)
		install.leafGenerationView = db.leafGenerationViewForManifest(leafManifest)
	}
	if db.leafPageLog != nil {
		staged, err := db.stagedLeafGenerationManifestWithPendingResult(manifestBasis, 0, next.CommitSeq)
		if err != nil {
			return nil, err
		}
		if staged.changed {
			install.leafGenerationView = db.leafGenerationViewForManifest(staged.manifest)
			install.post.persistLeafGenerationManifest = true
			install.post.persistLeafGenerationManifestView = staged.manifest
			install.post.persistLeafGenerationStateView = install.leafGenerationView
			install.post.persistLeafGenerationRawFileIDs = append(install.post.persistLeafGenerationRawFileIDs, staged.rawFileIDs...)
			install.post.clearLeafGenerationPendingFileIDs = append(install.post.clearLeafGenerationPendingFileIDs[:0], staged.pendingFileIDs...)
		}
	}
	if install.leafGenerationView == nil {
		install.leafGenerationView = db.currentLeafGenerationView()
	}
	if db.leafPageLog != nil && len(install.post.clearLeafGenerationPendingFileIDs) == 0 {
		install.post.drainLeafGenerationPending = true
	}
	install.post.commitSeq = next.CommitSeq
	install.post.sync = syncWrite
	abort = false
	return install, nil
}

func (install *rootPublicationVisibleInstallV1) abort() {
	if install == nil || install.activated || install.valueLogSet == nil || install.db == nil || install.db.valueLogManager == nil {
		return
	}
	_ = install.db.valueLogManager.Release(install.valueLogSet)
	install.valueLogSet = nil
}

func (install *rootPublicationVisibleInstallV1) activate(activateAllocator func() error) error {
	if install == nil || install.db == nil || install.idx == nil || install.activated {
		return errors.New("invalid root-publication visible install")
	}
	if activateAllocator == nil {
		return errors.New("missing root-publication allocator activation")
	}
	db := install.db
	db.mu.Lock()
	if db.meta.CommitSeq+1 != install.next.CommitSeq {
		db.mu.Unlock()
		return fmt.Errorf("%w: visible=%d candidate=%d", errDurableRootCandidateStale, db.meta.CommitSeq, install.next.CommitSeq)
	}
	if db.testFailDurableRootVisibleInstall.Load() {
		db.mu.Unlock()
		return errTestDurableRootVisibleInstallFailpoint
	}
	// Allocator activation is the visibility point of no return. Hold db.mu
	// across the final sequence check and this transition; every operation below
	// it is a bounded, non-fallible in-memory install. An activation error leaves
	// both the allocator and DB root at their prior visible generation.
	if err := activateAllocator(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.meta = install.next
	db.advanceEntryRevisionFloor(page.EntryRevision(install.next.MaxEntryRevision))
	install.post.oldState = db.state.Load()
	if install.installLeafManifest {
		db.leafGenerationManifest = install.leafManifest
	}
	newState := &DBState{
		CommitSeq:         install.next.CommitSeq,
		RootPageID:        install.next.UserRootPageID,
		SystemRootPageID:  install.next.SystemRootPageID,
		AppliedCommandLSN: install.next.AppliedCommandLSN,
		MaxEntryRevision:  page.EntryRevision(install.next.MaxEntryRevision),
		ValueLogSet:       install.valueLogSet,
		LeafGenerations:   install.leafGenerationView,
	}
	if install.leafGenerationView != nil {
		db.leafGenerationStateVersion++
		newState.LeafGenerationStateVersion = db.leafGenerationStateVersion
	}
	db.state.Store(newState)
	install.conditionalMutation.record(db, install.next.CommitSeq)
	if install.commandWALPublish {
		previousApplied := uint64(0)
		if install.post.oldState != nil {
			previousApplied = install.post.oldState.AppliedCommandLSN
		}
		db.observeCommandWALCovered(previousApplied, install.next.AppliedCommandLSN)
	}
	db.publishSnapshotView(install.idx, newState, db.valueLogManager)
	db.mu.Unlock()
	install.valueLogSet = nil
	install.activated = true
	return nil
}

// completeOrderedPostActivation performs bookkeeping that must observe the
// newly visible root before another DB builder can pass durablePublishMu. It is
// deliberately outside the coordinator mutex: value-log deltas and vacuum
// batches are not bounded activation work, and reportError may invoke user
// code. The returned error is reported only after the caller releases every
// root-build lock.
func (install *rootPublicationVisibleInstallV1) completeOrderedPostActivation() error {
	if install == nil || install.db == nil || !install.activated || install.postActivationCompleted {
		return errors.New("invalid root-publication post-activation completion")
	}
	install.postActivationCompleted = true
	db := install.db
	var reportErr error
	if db.valueLogRefTracker != nil {
		if install.post.vlogRefDelta != nil {
			if err := db.valueLogRefTracker.applyDelta(install.post.commitSeq, install.post.vlogRefDelta); err != nil {
				db.valueLogRefTracker.invalidate()
				reportErr = err
			}
		} else {
			db.valueLogRefTracker.invalidate()
		}
		install.post.vlogRefTrackerAdvanced = true
	}
	install.post.kickPrune = db.pruner.Enabled()
	install.post.doPrune = !install.post.kickPrune
	if !install.skipConditionalRootConflict {
		db.conditionalRecordRootCommit(install.oldUserRootID, install.newUserRootID, install.post.commitSeq)
	}
	if install.recordVacuumMutation != nil {
		install.recordVacuumMutation()
	}
	return reportErr
}

func (runtime *rootPublicationRuntimeV1) prepareVisibleCandidate(
	next page.MetaPageBody,
	retired []uint64,
	resources *rootpublication.StableResourceSet,
	install *rootPublicationVisibleInstallV1,
	dependencyBytes uint64,
	indexBytes uint64,
	timing *CommandWALPublishTiming,
) (_ *rootpublication.PreparedRootCandidate, err error) {
	if runtime == nil || runtime.db == nil || runtime.idx == nil || install == nil {
		return nil, errors.New("missing visible root candidate input")
	}
	visibleCloneStart := time.Now()
	visibleResources, visibleWork, err := rootpublication.CloneStableResourceSetForLogicalObligationsWithWork(
		resources, rootpublication.StableLogicalObligationRequirements{},
	)
	if timing != nil {
		timing.FinalizeCandidateVisibleClone += time.Since(visibleCloneStart)
		timing.FinalizeCandidateResourceWork.Add(visibleWork)
	}
	if err != nil {
		return nil, fmt.Errorf("clone visible root resource closure: %w", err)
	}
	visibleResourcesOwned := true
	defer func() {
		if visibleResourcesOwned {
			visibleResources.Release()
		}
	}()

	capability, err := runtime.db.durableRootReuseCapabilityV1(runtime.db.durableRoot)
	if err != nil {
		return nil, err
	}
	liveGeneration := runtime.idx.allocator.COWGenerationV1()
	if liveGeneration == nil || liveGeneration.GenerationID() == ^uint64(0) {
		return nil, errors.New("missing live COW generation for visible root")
	}
	generationID := liveGeneration.GenerationID() + 1
	retirements := make([]freelist.COWRetirementV1, 0, 1)
	if len(retired) != 0 {
		retirements = append(retirements, freelist.COWRetirementV1{
			PageIDs: retired, LastReachableCommitSeq: next.CommitSeq - 1,
		})
	}
	cowPrepareStart := time.Now()
	prepared, err := runtime.idx.allocator.PrepareCOWCandidateRetiringV1(
		generationID,
		next.CommitSeq,
		rootPublicationLogicalCandidateIDV1(runtime.lineage, generationID, next),
		capability,
		retirements,
		0,
		freelist.NewCandidatePageSinkV1(),
	)
	if timing != nil {
		timing.FinalizeCandidateCOWPrepare += time.Since(cowPrepareStart)
	}
	if err != nil {
		return nil, fmt.Errorf("prepare visible root COW generation: %w", err)
	}
	preparedOwned := true
	defer func() {
		if preparedOwned {
			if abortErr := runtime.idx.allocator.AbortCOWCandidateV1(prepared); abortErr != nil {
				cause := errors.Join(err, fmt.Errorf("abort visible root COW candidate: %w", abortErr), ErrRecoveryRequired)
				runtime.db.publicationPoisoned.Store(true)
				if failErr := runtime.idx.allocator.FailCOWCandidateV1(prepared, cause); failErr != nil {
					cause = errors.Join(cause, fmt.Errorf("fail visible root COW candidate: %w", failErr))
				}
				err = cause
			}
		}
	}()
	if runtime.db.testFailDurableRootAfterCOWPrepare.Load() {
		return nil, errTestDurableRootAfterCOWPrepareFailpoint
	}
	generation := prepared.Candidate().Generation()
	if generation == nil {
		return nil, errors.New("visible root COW candidate has no generation")
	}
	if indexBytes == 0 {
		pageCount := uint64(prepared.Candidate().PageCount())
		if pageCount > ^uint64(0)/page.PageSize {
			indexBytes = ^uint64(0)
		} else {
			indexBytes = pageCount * page.PageSize
		}
	}
	next.TotalPages = generation.HighWater()
	next.FreelistHeadID = 0
	install.next = next
	install.post.commitSeq = next.CommitSeq
	member := &rootPublicationVisibleMemberV1{
		sequence: next.CommitSeq, next: next, prepared: prepared,
		resources: visibleResources, install: install,
	}

	transaction, err := rootpublication.NewDurableRootTransaction(rootpublication.DurableRootTransactionSpec{
		Lineage: runtime.lineage, Sequence: next.CommitSeq, PreparedCOW: prepared,
		Activate: func(input rootpublication.DurableRootCallbackInput) error {
			if input.PreparedCOW != member.prepared || input.Sequence != member.sequence {
				return rootpublication.ErrDurableRootOwnership
			}
			if err := member.install.activate(func() error {
				return runtime.idx.allocator.ActivateCOWCandidateV1(member.prepared)
			}); err != nil {
				return err
			}
			runtime.mu.Lock()
			previousResources := runtime.visibleResources
			runtime.visibleResources = member.resources
			member.resourcesAdopted = true
			runtime.visibleMembers[member.sequence] = member
			runtime.debt = append(runtime.debt, member.prepared)
			member.activated = true
			runtime.mu.Unlock()
			previousResources.Release()
			return nil
		},
		Consume: func(input rootpublication.DurableRootCallbackInput) error {
			if input.PreparedCOW != member.prepared || input.Sequence != member.sequence {
				return rootpublication.ErrDurableRootOwnership
			}
			runtime.mu.Lock()
			delete(runtime.visibleMembers, member.sequence)
			runtime.mu.Unlock()
			return nil
		},
		Abort: func(input rootpublication.DurableRootCallbackInput) error {
			if input.PreparedCOW != member.prepared || input.Sequence != member.sequence {
				return rootpublication.ErrDurableRootOwnership
			}
			member.install.abort()
			if !member.resourcesAdopted {
				member.resources.Release()
				member.resources = nil
			}
			return runtime.idx.allocator.AbortCOWCandidateV1(member.prepared)
		},
		Fail: func(input rootpublication.DurableRootCallbackInput, cause error) error {
			if input.PreparedCOW != member.prepared || input.Sequence != member.sequence {
				return rootpublication.ErrDurableRootOwnership
			}
			return runtime.idx.allocator.FailCOWCandidateV1(member.prepared, cause)
		},
	})
	if err != nil {
		return nil, err
	}
	preparedOwned = false
	visibleResourcesOwned = false
	candidate, err := rootpublication.NewPreparedRootCandidate(rootpublication.CandidateSpec{
		Frontier:        rootPublicationFrontierV1(next),
		FreelistHeadID:  0,
		TotalPages:      generation.HighWater(),
		DependencyBytes: dependencyBytes,
		IndexBytes:      indexBytes,
		ResourceSet:     resources,
		DurableRoot:     transaction,
	})
	if err != nil {
		_ = transaction.Abort()
		return nil, err
	}
	return candidate, nil
}

func rootPublicationDependencyBytesV1(resources *rootpublication.StableResourceSet) uint64 {
	var total uint64
	for _, stats := range resources.Stats(time.Now()) {
		if ^uint64(0)-total < stats.PendingBytes {
			return ^uint64(0)
		}
		total += stats.PendingBytes
	}
	return total
}

// finalizeQueuedRootPublicationV1 transfers one fully built logical root from
// caller serialization into the coordinator. It performs no stable I/O. The
// allocator, visible DB state, resource closure, and coordinator frontier cross
// visibility atomically inside EnqueueBuilt's activation callback; only then
// are rootReuseMu and durablePublishMu released.
func (db *DB) finalizeQueuedRootPublicationV1(
	runtime *rootPublicationRuntimeV1,
	builder *rootpublication.BuilderToken,
	idx *indexGen,
	next page.MetaPageBody,
	oldUserRootID uint64,
	newUserRootID uint64,
	retired []uint64,
	syncWrite bool,
	post finalizeCommitPost,
	vlogRefDelta *valueLogRefDelta,
	leafManifest *leafGenerationManifest,
	leafManifestRawFileIDs []uint32,
	opts finalizeCommitOptions,
	inlinePrepareGuard *finalizeCommitPrepareGuard,
	releaseDurablePublish func(),
) (finalizeCommitPost, error) {
	candidateBuildStart := time.Now()
	var candidateTiming CommandWALPublishTiming
	prePublishErr := func(err error) error { return wrapFinalizeCommitError(err, true) }
	if runtime == nil || runtime.coordinator == nil || builder == nil || idx == nil || releaseDurablePublish == nil {
		return post, prePublishErr(errors.New("incomplete queued root-publication handoff"))
	}
	visibleBaseCloneStart := time.Now()
	visibleBase, visibleBaseWork, err := runtime.cloneVisibleResourcesWithWork()
	candidateTiming.FinalizeCandidateVisibleBaseClone += time.Since(visibleBaseCloneStart)
	candidateTiming.FinalizeCandidateResourceWork.Add(visibleBaseWork)
	if err != nil {
		return post, prePublishErr(err)
	}
	defer visibleBase.Release()

	resources, err := db.captureDurableRootResourcesFromBaseV1(
		idx, next, vlogRefDelta, visibleBase, opts.durableResources,
		opts.durableResourceRequirements, opts.durableResourceMutation,
		opts.durableResourceAppendMutation, opts.durableResourceRequirementWork, opts.durableResourceRequirementsFallback,
		opts.valueLogPublicationLocked,
		&candidateTiming,
	)
	if err != nil {
		return post, prePublishErr(fmt.Errorf("capture queued root dependencies: %w", err))
	}
	resourcesOwned := true
	defer func() {
		if resourcesOwned {
			resources.Release()
		}
	}()

	db.rootReuseMu.Lock()
	rootReuseLocked := true
	buildLocksReleased := false
	releaseBuildLocks := func() {
		if buildLocksReleased {
			return
		}
		if rootReuseLocked {
			db.rootReuseMu.Unlock()
			rootReuseLocked = false
		}
		releaseDurablePublish()
		buildLocksReleased = true
	}
	defer releaseBuildLocks()

	post.vlogRefDelta = vlogRefDelta
	install, err := db.prepareRootPublicationVisibleInstallV1(
		idx, next, oldUserRootID, newUserRootID, syncWrite, post,
		leafManifest, leafManifestRawFileIDs, opts,
	)
	if err != nil {
		return post, prePublishErr(fmt.Errorf("prepare queued visible root: %w", err))
	}
	installOwned := true
	defer func() {
		if installOwned {
			install.abort()
		}
	}()

	dependencyBytes := rootPublicationDependencyBytesV1(resources)
	if forced := db.testRootPublicationDependencyBytes.Load(); forced != 0 {
		dependencyBytes = forced
	}
	candidate, err := runtime.prepareVisibleCandidate(
		next, retired, resources, install,
		dependencyBytes, 0, &candidateTiming,
	)
	if err != nil {
		return post, prePublishErr(err)
	}
	resourcesOwned = false
	installOwned = false
	candidateOwned := true
	defer func() {
		if candidateOwned {
			_ = candidate.Abandon()
		}
	}()

	if releaseRootSerialization := opts.releaseRootSerialization; releaseRootSerialization != nil {
		releaseRootSerialization()
	}
	if inlinePrepareGuard != nil {
		inlinePrepareGuard.Release()
	}
	if hook := db.testDurableRootCandidatePreparedHook; hook != nil {
		hook()
	}
	if opts.publishTiming != nil {
		candidateTiming.FinalizeCandidateBuild = time.Since(candidateBuildStart)
		accounted := candidateTiming.FinalizeCandidateVisibleBaseClone +
			candidateTiming.FinalizeCandidateInheritedFilter +
			candidateTiming.FinalizeCandidateFreshCapture +
			candidateTiming.FinalizeCandidateClosureAssemble +
			candidateTiming.FinalizeCandidateVisibleClone +
			candidateTiming.FinalizeCandidateCOWPrepare
		if candidateTiming.FinalizeCandidateBuild > accounted {
			candidateTiming.FinalizeCandidateOther = candidateTiming.FinalizeCandidateBuild - accounted
		}
		opts.publishTiming.Add(candidateTiming)
	}

	enqueueStart := time.Now()
	receipt, enqueueErr := runtime.coordinator.EnqueueBuilt(candidate, builder)
	if opts.publishTiming != nil {
		opts.publishTiming.FinalizeEnqueueActivation += time.Since(enqueueStart)
	}
	if enqueueErr != nil {
		enqueueErr = publicRootPublicationErrorV1(enqueueErr)
		if abandonErr := candidate.Abandon(); abandonErr != nil {
			enqueueErr = errors.Join(enqueueErr, abandonErr, ErrRecoveryRequired)
			db.publicationPoisoned.Store(true)
		}
		candidateOwned = false
		return post, prePublishErr(enqueueErr)
	}
	candidateOwned = false
	reportErr := install.completeOrderedPostActivation()
	post = install.post
	post.accepted = true
	// The candidate is coordinator-owned and all ordered bookkeeping is now
	// complete. Release DB build locks before any user error callback or
	// admission/durability wait can re-enter the coordinator.
	releaseBuildLocks()
	if reportErr != nil {
		db.reportError(reportErr)
	}
	admissionStart := time.Now()
	if err := runtime.coordinator.WaitForAdmission(context.Background(), receipt); err != nil {
		if opts.publishTiming != nil {
			opts.publishTiming.FinalizeAdmissionWait += time.Since(admissionStart)
		}
		post = db.finalizeAcceptedCommitPostWorkOnError(post)
		return post, wrapAcceptedFinalizeCommitError(publicRootPublicationErrorV1(err))
	}
	if opts.publishTiming != nil {
		opts.publishTiming.FinalizeAdmissionWait += time.Since(admissionStart)
	}
	if syncWrite && !db.commandWAL {
		durabilityStart := time.Now()
		if err := runtime.coordinator.WaitThrough(context.Background(), next.CommitSeq); err != nil {
			if opts.publishTiming != nil {
				opts.publishTiming.FinalizeDurabilityWait += time.Since(durabilityStart)
			}
			post = db.finalizeAcceptedCommitPostWorkOnError(post)
			return post, wrapAcceptedFinalizeCommitError(publicRootPublicationErrorV1(err))
		}
		if opts.publishTiming != nil {
			opts.publishTiming.FinalizeDurabilityWait += time.Since(durabilityStart)
		}
	}
	return post, nil
}

func rootPublicationSealCandidateIDV1(base durableRootRuntimeV1, next page.MetaPageBody, generationID uint64) freelist.CandidateIDV1 {
	var material [104]byte
	copy(material[0:16], []byte("root-seal-v1"))
	binary.LittleEndian.PutUint64(material[16:24], generationID)
	binary.LittleEndian.PutUint64(material[24:32], next.CommitSeq)
	binary.LittleEndian.PutUint64(material[32:40], next.UserRootPageID)
	binary.LittleEndian.PutUint64(material[40:48], next.SystemRootPageID)
	binary.LittleEndian.PutUint64(material[48:56], next.AppliedCommandLSN)
	binary.LittleEndian.PutUint64(material[56:64], next.MaxEntryRevision)
	binary.LittleEndian.PutUint64(material[64:72], base.slot)
	binary.LittleEndian.PutUint64(material[72:80], base.record.CommitSeq)
	copy(material[80:104], base.meta.RootRecordDigest[:24])
	digest := sha256.Sum256(material[:])
	var candidate freelist.CandidateIDV1
	copy(candidate[:], digest[:len(candidate)])
	return candidate
}

// Prepare is the optional coordinator preparation callback. The coordinator's
// preparation-only gate guarantees that no builder can activate while this
// method samples the exact visible prefix and activates its final seal. The
// gate opens as soon as this method returns; dependency/index/meta I/O happens
// later in Publish while new builders are admitted.
func (runtime *rootPublicationRuntimeV1) Prepare(ctx context.Context, candidate *rootpublication.PreparedRootCandidate) (err error) {
	if runtime == nil || runtime.db == nil || runtime.idx == nil || candidate == nil {
		return errors.New("missing root-publication prepare input")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	group := candidate.DurableRootGroup()
	latest := group.Latest()
	if latest == nil || group.Lineage() != runtime.lineage || latest.Sequence() != candidate.Frontier().CommitSeq() {
		return fmt.Errorf("%w: publisher group does not match live lineage", rootpublication.ErrDurableRootLineage)
	}

	db := runtime.db
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	db.rootReuseMu.Lock()
	defer db.rootReuseMu.Unlock()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.poison != nil {
		return runtime.poison
	}
	if seal := runtime.activeSeal; seal != nil && seal.latestSequence == latest.Sequence() && seal.groupLength == group.Len() {
		return nil
	}
	member := runtime.visibleMembers[latest.Sequence()]
	if member == nil || member.prepared != latest.PreparedCOW() || member.next.CommitSeq != latest.Sequence() {
		return fmt.Errorf("%w: visible member %d is not activated", rootpublication.ErrDurableRootLineage, latest.Sequence())
	}
	if len(runtime.debt) == 0 || runtime.debt[len(runtime.debt)-1] != member.prepared {
		return fmt.Errorf("%w: visible allocator debt is not the captured prefix", rootpublication.ErrDurableRootLineage)
	}
	// Seals from retryable attempts may be interleaved in allocator debt, but
	// every logical member captured by the coordinator must appear exactly once
	// and in group order. This prevents a malformed coalesced group from using a
	// later seal to publish an allocator prefix it does not own.
	debtCursor := 0
	for _, transaction := range group.Members() {
		if transaction == nil || transaction.Lineage() != runtime.lineage {
			return fmt.Errorf("%w: malformed root-publication group", rootpublication.ErrDurableRootLineage)
		}
		groupMember := runtime.visibleMembers[transaction.Sequence()]
		if groupMember == nil || groupMember.prepared != transaction.PreparedCOW() {
			return fmt.Errorf("%w: visible member %d is absent from runtime debt", rootpublication.ErrDurableRootLineage, transaction.Sequence())
		}
		found := -1
		for i := debtCursor; i < len(runtime.debt); i++ {
			if runtime.debt[i] == transaction.PreparedCOW() {
				found = i
				break
			}
		}
		if found < 0 {
			return fmt.Errorf("%w: allocator debt omits visible member %d", rootpublication.ErrDurableRootLineage, transaction.Sequence())
		}
		debtCursor = found + 1
	}

	base := db.durableRoot
	if base.pending != nil || len(base.ambiguous) != 0 || base.record.CommitSeq >= member.next.CommitSeq {
		return fmt.Errorf("%w: durable=%d visible=%d", errDurableRootCandidateStale, base.record.CommitSeq, member.next.CommitSeq)
	}
	// The durable slot names only the latest visible root. A coalesced candidate's
	// aggregate resource set also contains dependencies reachable exclusively
	// from superseded intermediate roots; retaining that union in the slot would
	// pin unreachable value-log and leaf-log files for an extra slot lifetime.
	// The latest activated member owns the exact closure for the root we seal.
	resources, err := rootpublication.CloneStableResourceSetExcludingKinds(member.resources)
	if err != nil {
		return fmt.Errorf("clone latest root resources: %w", err)
	}
	releaseResources := true
	defer func() {
		if releaseResources {
			resources.Release()
		}
	}()
	manifest, err := db.durableManifestFromResourcesV1WithStats(resources)
	if err != nil {
		return err
	}

	// Close drains the coordinator before index/resource teardown. Publication
	// already admitted before closing therefore retains valid teardown authority
	// and must be allowed to capture the exact stable index handle while Drain
	// waits outside maintenanceMu.
	stableSnapshot := db.acquireDurableCandidateStableIndexSnapshotV1(runtime.idx, true)
	if stableSnapshot == nil {
		return errors.New("capture root-publication stable index snapshot")
	}
	token, err := stableSnapshot.CaptureStableIndexFileResource()
	if err != nil {
		_ = stableSnapshot.Close()
		return fmt.Errorf("capture root-publication index resource: %w", err)
	}
	releaseToken := true
	defer func() {
		if releaseToken {
			token.Release()
		}
	}()

	target := uint64(MetaPage0ID)
	if base.slot == MetaPage0ID {
		target = MetaPage1ID
	}
	overwrittenPages, err := durableRootSlotAuxiliaryPagesV1(base.slotMeta[target], base.slotRecord[target])
	if err != nil {
		return err
	}
	retirements := make([]freelist.COWRetirementV1, 0, 2)
	if len(overwrittenPages) != 0 {
		retirements = append(retirements, freelist.COWRetirementV1{
			PageIDs: overwrittenPages, LastReachableCommitSeq: base.slotCommit[target],
		})
	}
	if previous := runtime.activeSeal; previous != nil {
		if auxiliary := previous.prepared.AuxiliaryPageIDs(); len(auxiliary) != 0 {
			retirements = append(retirements, freelist.COWRetirementV1{
				PageIDs: auxiliary, LastReachableCommitSeq: base.record.CommitSeq,
			})
		}
	}
	capability, err := db.durableRootReuseCapabilityV1(base)
	if err != nil {
		return err
	}
	liveGeneration := runtime.idx.allocator.COWGenerationV1()
	if liveGeneration == nil || liveGeneration.GenerationID() == ^uint64(0) {
		return errors.New("missing live COW generation for root-publication seal")
	}
	generationID := liveGeneration.GenerationID() + 1
	auxiliaryCount := int(manifest.PageCount()) + 1
	prepared, err := runtime.idx.allocator.PrepareCOWCandidateRetiringV1(
		generationID,
		member.next.CommitSeq,
		rootPublicationSealCandidateIDV1(base, member.next, generationID),
		capability,
		retirements,
		auxiliaryCount,
		freelist.NewCandidatePageSinkV1(),
	)
	if err != nil {
		return fmt.Errorf("prepare root-publication seal generation: %w", err)
	}
	preparedOwned := true
	defer func() {
		if !preparedOwned {
			return
		}
		if abortErr := runtime.idx.allocator.AbortCOWCandidateV1(prepared); abortErr != nil {
			cause := errors.Join(err, fmt.Errorf("abort root-publication seal: %w", abortErr), ErrRecoveryRequired)
			db.publicationPoisoned.Store(true)
			runtime.poison = cause
			_ = runtime.idx.allocator.FailCOWCandidateV1(prepared, cause)
			err = cause
		}
	}()

	generation := prepared.Candidate().Generation()
	auxiliary := prepared.AuxiliaryPageIDs()
	if generation == nil || len(auxiliary) != auxiliaryCount {
		return errors.New("incomplete root-publication seal generation")
	}
	next := member.next
	next.TotalPages = generation.HighWater()
	next.FreelistHeadID = 0
	manifestRef, err := manifest.Materialize(auxiliary[0], freelist.NewMemoryPageStoreV1())
	if err != nil {
		return err
	}
	recordPageID := auxiliary[len(auxiliary)-1]
	if base.record.DurableSeq == ^uint64(0) {
		return errors.New("durable root publication sequence overflow")
	}
	durableSeq := base.record.DurableSeq + 1
	if durableSeq > next.CommitSeq {
		return errors.New("durable root publication sequence exceeds commit frontier")
	}
	record := rootpublication.DurableRootRecordV1{
		CommitSeq: next.CommitSeq, DurableSeq: durableSeq,
		UserRootPageID: next.UserRootPageID, SystemRootPageID: next.SystemRootPageID,
		TotalPages: next.TotalPages, MaxEntryRevision: next.MaxEntryRevision,
		AppliedCommandLSN: next.AppliedCommandLSN, LastCommitHeight: next.LastCommitHeight,
		Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(),
		Manifest:             manifestRef,
		ParentRecordPageID:   base.meta.RootRecordPageID,
		ParentCommitSeq:      base.meta.CommitSeq,
		ParentRecordDigest:   base.meta.RootRecordDigest,
		MetaProjectionDigest: page.DurableMetaProjectionDigestV1(next.CommitSeq, durableSeq, recordPageID),
	}
	_, recordDigest, err := record.EncodePage(recordPageID)
	if err != nil {
		return err
	}
	meta, err := page.NewDurableMetaV1(next.CommitSeq, durableSeq, recordPageID, recordDigest)
	if err != nil {
		return err
	}
	if err := runtime.idx.allocator.ActivateCOWCandidateV1(prepared); err != nil {
		return fmt.Errorf("activate root-publication seal generation: %w", err)
	}
	preparedOwned = false
	prefix := append([]*freelist.PreparedCOWCandidateV1(nil), runtime.debt...)
	prefix = append(prefix, prepared)
	seal := &rootPublicationSealV1{
		latestSequence: member.next.CommitSeq, groupLength: group.Len(),
		idx: runtime.idx, base: base, next: next, resources: resources, manifest: manifest,
		prepared: prepared, prefix: prefix, token: token, record: record, meta: meta,
		target: target,
	}
	runtime.debt = append(runtime.debt, prepared)
	runtime.seals = append(runtime.seals, seal)
	runtime.activeSeal = seal
	releaseResources = false
	releaseToken = false
	return nil
}

func (runtime *rootPublicationRuntimeV1) materializeSeal(seal *rootPublicationSealV1) error {
	if seal == nil || seal.idx == nil || seal.prepared == nil {
		return errors.New("missing root-publication seal")
	}
	if seal.materialized {
		return nil
	}
	generation := seal.prepared.Candidate().Generation()
	if generation == nil {
		return errors.New("missing root-publication seal generation")
	}
	if seal.idx.pager.PageCount() < generation.HighWater() {
		if err := seal.idx.pager.Truncate(generation.HighWater()); err != nil {
			return fmt.Errorf("grow root-publication index: %w", err)
		}
	}
	if err := seal.idx.allocator.ValidateCOWPhysicalTailV1(generation.HighWater()); err != nil {
		return fmt.Errorf("validate root-publication physical tail: %w", err)
	}
	for _, prepared := range seal.prefix {
		if prepared == nil || prepared.Candidate() == nil {
			return errors.New("root-publication prefix contains no COW candidate")
		}
		if err := prepared.Candidate().WritePagesToV1(durablePagerSinkV1{pager: seal.idx.pager}); err != nil {
			return fmt.Errorf("write root-publication COW pages: %w", err)
		}
	}
	auxiliary := seal.prepared.AuxiliaryPageIDs()
	if len(auxiliary) != int(seal.manifest.PageCount())+1 {
		return errors.New("root-publication seal auxiliary inventory changed")
	}
	if _, err := seal.manifest.Materialize(auxiliary[0], durablePagerSinkV1{pager: seal.idx.pager}); err != nil {
		return err
	}
	recordPageID := auxiliary[len(auxiliary)-1]
	recordImage, recordDigest, err := seal.record.EncodePage(recordPageID)
	if err != nil {
		return err
	}
	if recordDigest != seal.meta.RootRecordDigest {
		return errors.New("root-publication record digest changed after preparation")
	}
	recordOffset := int64(recordPageID) * int64(page.PageSize)
	indexPath := filepath.Join(runtime.db.dir, indexFileName)
	if err := durabilitycut.EmitRange(durabilitycut.BeforePublicationSealWrite, durabilitycut.ResourceSeal, runtime.db.dir, indexPath, recordOffset, int64(page.PageSize)); err != nil {
		return err
	}
	if err := seal.idx.pager.Write(recordPageID, recordImage); err != nil {
		return fmt.Errorf("write root-publication record: %w", err)
	}
	if err := durabilitycut.EmitRange(durabilitycut.AfterPublicationSealWrite, durabilitycut.ResourceSeal, runtime.db.dir, indexPath, recordOffset, int64(page.PageSize)); err != nil {
		return err
	}
	seal.materialized = true
	return nil
}

func (runtime *rootPublicationRuntimeV1) preparedSealFor(candidate *rootpublication.PreparedRootCandidate) (*rootPublicationSealV1, error) {
	if runtime == nil || candidate == nil {
		return nil, errors.New("missing root-publication candidate")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.poison != nil {
		return nil, runtime.poison
	}
	seal := runtime.activeSeal
	if seal == nil || seal.latestSequence != candidate.Frontier().CommitSeq() || seal.groupLength != candidate.DurableRootGroup().Len() {
		return nil, errors.New("root-publication attempt has no exact prepared seal")
	}
	return seal, nil
}

// Publish performs only stable I/O and the post-I/O allocator/durable-state
// transition. No DB write, commit, root-build, or preparation lock is held
// across executeDurableRootStorageTransactionV1.
func (runtime *rootPublicationRuntimeV1) Publish(ctx context.Context, candidate *rootpublication.PreparedRootCandidate) rootpublication.PublishResult {
	if err := ctx.Err(); err != nil {
		return rootpublication.PublishResult{Outcome: rootpublication.PublishRetryableFailure, Err: err}
	}
	seal, err := runtime.preparedSealFor(candidate)
	if err != nil {
		return rootpublication.PublishResult{Outcome: rootpublication.PublishRetryableFailure, Err: err}
	}
	db := runtime.db
	metaPath := filepath.Join(db.dir, indexFileName)
	mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		resources:   seal.resources,
		materialize: func() error { return runtime.materializeSeal(seal) },
		syncIndex:   seal.token.SyncThrough,
		sink:        durablePagerSinkV1{pager: seal.idx.pager},
		target:      seal.target,
		meta:        seal.meta,
		syncMeta: func() error {
			return seal.token.WithPinnedFile(func(file *os.File) error {
				return seal.idx.pager.SyncPagesWithStableFile(file, []uint64{seal.target})
			})
		},
		dir: db.dir, indexPath: metaPath,
		beforeMetaWrite: func() error {
			if db.testFailWriteMeta.Load() {
				return errTestWriteMetaFailpoint
			}
			return ctx.Err()
		},
		beforeMetaSync: func() error {
			if db.testFailSyncMeta.Load() {
				return errTestSyncMetaFailpoint
			}
			return ctx.Err()
		},
	})
	if err != nil {
		if mutated {
			runtime.failAmbiguousSeal(seal, err)
			return rootpublication.PublishResult{Outcome: rootpublication.PublishAmbiguous, Err: err}
		}
		return rootpublication.PublishResult{Outcome: rootpublication.PublishRetryableFailure, Err: err}
	}
	oldest, err := runtime.commitPublishedSeal(seal)
	if err != nil {
		runtime.failAmbiguousSeal(seal, err)
		return rootpublication.PublishResult{Outcome: rootpublication.PublishAmbiguous, Err: err}
	}
	return rootpublication.PublishResult{
		Outcome: rootpublication.PublishSucceeded, DurableCommitSeq: seal.latestSequence,
		OldestRecoverableCommitSeq: oldest,
	}
}

func (runtime *rootPublicationRuntimeV1) commitPublishedSeal(seal *rootPublicationSealV1) (uint64, error) {
	db := runtime.db
	db.durablePublishMu.Lock()
	db.rootReuseMu.Lock()
	runtime.mu.Lock()
	if runtime.activeSeal != seal || runtime.poison != nil {
		runtime.mu.Unlock()
		db.rootReuseMu.Unlock()
		db.durablePublishMu.Unlock()
		return 0, errors.New("root-publication seal lost exact attempt ownership")
	}
	if len(seal.prefix) == 0 || len(runtime.debt) < len(seal.prefix) || seal.prefix[len(seal.prefix)-1] != seal.prepared {
		runtime.mu.Unlock()
		db.rootReuseMu.Unlock()
		db.durablePublishMu.Unlock()
		return 0, errors.New("published root seal has no exact allocator prefix")
	}
	for i, prepared := range seal.prefix {
		if runtime.debt[i] != prepared {
			runtime.mu.Unlock()
			db.rootReuseMu.Unlock()
			db.durablePublishMu.Unlock()
			return 0, fmt.Errorf("published root seal allocator prefix diverged at member %d", i)
		}
	}
	current := seal.base
	current.meta = seal.meta
	current.record = seal.record
	current.manifest = seal.manifest
	current.slot = seal.target
	current.slotCommit[seal.target] = seal.next.CommitSeq
	current.slotMeta[seal.target] = seal.meta
	current.slotRecord[seal.target] = seal.record
	previousResources := current.slotResources[seal.target]
	current.slotResources[seal.target] = seal.resources
	current.pending = nil
	nextCapability, err := db.durableRootReuseCapabilityV1(current)
	if err != nil {
		runtime.mu.Unlock()
		db.rootReuseMu.Unlock()
		db.durablePublishMu.Unlock()
		return 0, fmt.Errorf("derive post-publication reuse capability: %w", err)
	}
	oldest, err := oldestRecoverableSlotCommitV1(current.slotCommit)
	if err != nil {
		runtime.mu.Unlock()
		db.rootReuseMu.Unlock()
		db.durablePublishMu.Unlock()
		return 0, err
	}
	// This is the final fallible transition. Validate every runtime invariant
	// above before consuming the allocator prefix so no error can leave the
	// allocator published while db.durableRoot still names the older slot.
	if err := seal.idx.allocator.PublishActivatedCOWPrefixV1(seal.prefix, nextCapability); err != nil {
		runtime.mu.Unlock()
		db.rootReuseMu.Unlock()
		db.durablePublishMu.Unlock()
		return 0, fmt.Errorf("publish activated root COW prefix: %w", err)
	}
	db.durableRoot = current
	db.metaPageID = seal.target
	seal.resources = nil
	if seal.token != nil {
		seal.token.Release()
		seal.token = nil
	}
	seal.released = true
	clear(runtime.debt[:len(seal.prefix)])
	runtime.debt = append([]*freelist.PreparedCOWCandidateV1(nil), runtime.debt[len(seal.prefix):]...)
	var retiredSeals []*rootPublicationSealV1
	keptSeals := runtime.seals[:0]
	for _, candidateSeal := range runtime.seals {
		if candidateSeal == seal || candidateSeal.latestSequence <= seal.latestSequence {
			if candidateSeal != seal {
				retiredSeals = append(retiredSeals, candidateSeal)
			}
			continue
		}
		keptSeals = append(keptSeals, candidateSeal)
	}
	clear(runtime.seals[len(keptSeals):])
	runtime.seals = keptSeals
	runtime.activeSeal = nil
	runtime.mu.Unlock()
	db.rootReuseMu.Unlock()
	db.durablePublishMu.Unlock()

	previousResources.Release()
	for _, retired := range retiredSeals {
		retired.release()
	}
	return oldest, nil
}

func (runtime *rootPublicationRuntimeV1) failAmbiguousSeal(seal *rootPublicationSealV1, cause error) {
	if runtime == nil || seal == nil {
		return
	}
	runtime.mu.Lock()
	if runtime.poison == nil {
		runtime.poison = errors.Join(cause, ErrRecoveryRequired)
	}
	poison := runtime.poison
	runtime.mu.Unlock()
	runtime.db.publicationPoisoned.Store(true)
	_ = seal.idx.allocator.FailCOWCandidateV1(seal.prepared, poison)
}

func (runtime *rootPublicationRuntimeV1) release() {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	visibleResources := runtime.visibleResources
	runtime.visibleResources = nil
	seals := append([]*rootPublicationSealV1(nil), runtime.seals...)
	runtime.seals = nil
	runtime.activeSeal = nil
	runtime.mu.Unlock()
	visibleResources.Release()
	for _, seal := range seals {
		seal.release()
	}
}
