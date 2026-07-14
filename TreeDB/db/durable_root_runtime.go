package db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type durablePagerSinkV1 struct{ pager *pager.Pager }

var errDurableRootCandidateStale = errors.New("durable-root candidate base changed")

func (sink durablePagerSinkV1) WritePage(pageID uint64, image []byte) error {
	if sink.pager == nil {
		return errors.New("missing durable pager")
	}
	return sink.pager.Write(pageID, image)
}

type durableRootRuntimeV1 struct {
	meta          page.DurableMetaV1
	record        rootpublication.DurableRootRecordV1
	manifest      *rootpublication.DependencyManifestV1
	slot          uint64
	slotCommit    [2]uint64
	slotResources [2]*rootpublication.StableResourceSet
	slotMeta      [2]page.DurableMetaV1
	slotRecord    [2]rootpublication.DurableRootRecordV1
	pending       *durableRootPublishCandidateV1
	ambiguous     []*durableRootPublishCandidateV1
}

// durableRootPublishCandidateV1 owns every identity and allocator reservation
// needed to retry one exact pre-meta publication. It is immutable after
// preparation except for the phase markers, which only move forward while
// durablePublishMu is held.
type durableRootPublishCandidateV1 struct {
	idx        *indexGen
	base       durableRootRuntimeV1
	next       page.MetaPageBody
	resources  *rootpublication.StableResourceSet
	manifest   *rootpublication.DependencyManifestV1
	prepared   *freelist.PreparedCOWCandidateV1
	capability freelist.ReuseCapability
	token      *rootpublication.StableResourceToken
	record     rootpublication.DurableRootRecordV1
	meta       page.DurableMetaV1
	target     uint64

	materialized bool
	metaMutated  bool
	released     bool
}

func (candidate *durableRootPublishCandidateV1) release() {
	if candidate == nil || candidate.released {
		return
	}
	candidate.released = true
	candidate.resources.Release()
	if candidate.token != nil {
		candidate.token.Release()
	}
}

func durableRootCandidateIDV1(current durableRootRuntimeV1, next page.MetaPageBody) freelist.CandidateIDV1 {
	var material [88]byte
	binary.LittleEndian.PutUint64(material[0:8], current.record.Freelist.GenerationID+1)
	binary.LittleEndian.PutUint64(material[8:16], next.CommitSeq)
	binary.LittleEndian.PutUint64(material[16:24], next.UserRootPageID)
	binary.LittleEndian.PutUint64(material[24:32], next.SystemRootPageID)
	binary.LittleEndian.PutUint64(material[32:40], next.AppliedCommandLSN)
	binary.LittleEndian.PutUint64(material[40:48], next.MaxEntryRevision)
	copy(material[48:80], current.meta.RootRecordDigest[:])
	binary.LittleEndian.PutUint64(material[80:88], current.slot)
	digest := sha256.Sum256(material[:])
	var candidate freelist.CandidateIDV1
	copy(candidate[:], digest[:len(candidate)])
	return candidate
}

func oldestRecoverableSlotCommitV1(commits [2]uint64) (uint64, error) {
	oldest := uint64(0)
	for _, commit := range commits {
		if commit != 0 && (oldest == 0 || commit < oldest) {
			oldest = commit
		}
	}
	if oldest == 0 {
		return 0, errors.New("durable root has no recoverable slot")
	}
	return oldest, nil
}

func durableManifestFromResourcesV1(resources *rootpublication.StableResourceSet) (*rootpublication.DependencyManifestV1, error) {
	if resources == nil {
		return rootpublication.NewDependencyManifestV1(nil)
	}
	descriptors := resources.Descriptors()
	entries := make([]rootpublication.DependencyManifestEntryV1, len(descriptors))
	for i, descriptor := range descriptors {
		entries[i] = rootpublication.DependencyManifestEntryV1{
			Kind: descriptor.Kind(), LogicalLane: descriptor.LogicalLane(),
			ResourceID: descriptor.ResourceID(), DiagnosticPath: descriptor.DiagnosticPath(),
			Identity: descriptor.Identity(), Generation: descriptor.Generation(),
			Frontier: descriptor.Frontier(), Reachability: descriptor.ReachabilityFields(),
		}
	}
	return rootpublication.NewDependencyManifestV1(entries)
}

func durableRootSlotAuxiliaryPagesV1(meta page.DurableMetaV1, record rootpublication.DurableRootRecordV1) ([]uint64, error) {
	if meta.CommitSeq == 0 && record.CommitSeq == 0 {
		return nil, nil
	}
	if meta.CommitSeq == 0 || record.CommitSeq != meta.CommitSeq || meta.RootRecordPageID < 2 || record.Manifest.FirstPageID < 2 || record.Manifest.PageCount == 0 {
		return nil, errors.New("incomplete durable-root slot auxiliary inventory")
	}
	lastManifestPage := record.Manifest.FirstPageID + uint64(record.Manifest.PageCount) - 1
	if lastManifestPage < record.Manifest.FirstPageID || lastManifestPage >= record.TotalPages || meta.RootRecordPageID >= record.TotalPages {
		return nil, errors.New("durable-root slot auxiliary inventory outside durable extent")
	}
	pages := make([]uint64, 0, int(record.Manifest.PageCount)+1)
	for pageID := record.Manifest.FirstPageID; pageID <= lastManifestPage; pageID++ {
		pages = append(pages, pageID)
	}
	pages = append(pages, meta.RootRecordPageID)
	return pages, nil
}

func (db *DB) projectedValueLogReferencesV1(next page.MetaPageBody, delta *valueLogRefDelta) (map[uint32]struct{}, bool, error) {
	tracker := db.valueLogRefTracker
	if tracker == nil {
		return nil, false, nil
	}
	tracker.mu.RLock()
	if !tracker.valid || tracker.commitSeq != db.durableRoot.record.CommitSeq {
		tracker.mu.RUnlock()
		return nil, false, nil
	}
	counts := make(map[uint32]uint64, len(tracker.counts))
	for fileID, count := range tracker.counts {
		counts[fileID] = count
	}
	tracker.mu.RUnlock()

	if delta == nil {
		if next.UserRootPageID != db.durableRoot.record.UserRootPageID || next.SystemRootPageID != db.durableRoot.record.SystemRootPageID {
			return nil, false, nil
		}
	} else if err := delta.forEachChange(func(fileID uint32, change int64) error {
		current := counts[fileID]
		switch {
		case change > 0:
			if uint64(change) > ^uint64(0)-current {
				return fmt.Errorf("value-log dependency count overflow for file %d", fileID)
			}
			counts[fileID] = current + uint64(change)
		case change < 0:
			decrement := uint64(-change)
			if decrement > current {
				return fmt.Errorf("value-log dependency count underflow for file %d", fileID)
			}
			if decrement == current {
				delete(counts, fileID)
			} else {
				counts[fileID] = current - decrement
			}
		}
		return nil
	}); err != nil {
		return nil, false, err
	}

	references := make(map[uint32]struct{}, len(counts))
	for fileID, count := range counts {
		if count != 0 {
			references[fileID] = struct{}{}
		}
	}
	return references, true, nil
}

func (db *DB) scanCandidateValueLogReferencesV1(idx *indexGen, next page.MetaPageBody) (map[uint32]struct{}, error) {
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		// Command-WAL recovery publishes roots before Open installs the first
		// public snapshot view. Build the same bounded candidate projection from
		// the already-recovered index and the manager's registered file set.
		// This fallback is recovery-only: once visible state exists, a failed
		// AcquireSnapshot must continue to fail closed rather than bypass its
		// publication and shutdown guards.
		if db.state.Load() != nil || db.valueLogManager == nil {
			return nil, errors.New("capture candidate dependency snapshot")
		}
		set := db.valueLogManager.CurrentSetNoRefresh()
		candidateState := DBState{
			ValueLogSet:                set,
			LeafGenerations:            db.currentLeafGenerationView(),
			LeafGenerationStateVersion: db.leafGenerationStateVersion,
		}
		applyDurableRootCandidateStateV1(&candidateState, next)
		recoverySnapshot := &Snapshot{
			db:                db,
			idx:               idx,
			state:             &candidateState,
			vlogManager:       db.valueLogManager,
			reader:            newValueReader(set),
			registryShardHint: snapshotShardHintUnset,
		}
		result, scanErr := db.maintenanceReachabilityScan(context.Background(), recoverySnapshot, maintenanceReachabilityScanOptions{
			Collectors: maintenanceReachabilityValueLogRefCounts,
		})
		releaseErr := db.valueLogManager.Release(set)
		if scanErr != nil || releaseErr != nil {
			return nil, errors.Join(scanErr, releaseErr)
		}
		return result.valueLogReferencedSegments, nil
	}
	if snapshot.state == nil {
		_ = snapshot.Close()
		return nil, errors.New("capture candidate dependency snapshot")
	}
	if snapshot.idx != idx {
		_ = snapshot.Close()
		return nil, errors.New("candidate dependency index changed")
	}
	candidateState := *snapshot.state
	applyDurableRootCandidateStateV1(&candidateState, next)
	snapshot.state = &candidateState
	result, scanErr := db.maintenanceReachabilityScan(context.Background(), snapshot, maintenanceReachabilityScanOptions{
		Collectors: maintenanceReachabilityValueLogRefCounts,
	})
	closeErr := snapshot.Close()
	if scanErr != nil || closeErr != nil {
		return nil, errors.Join(scanErr, closeErr)
	}
	return result.valueLogReferencedSegments, nil
}

func applyDurableRootCandidateStateV1(state *DBState, next page.MetaPageBody) {
	state.CommitSeq = next.CommitSeq
	state.RootPageID = next.UserRootPageID
	state.SystemRootPageID = next.SystemRootPageID
	state.AppliedCommandLSN = next.AppliedCommandLSN
	state.MaxEntryRevision = page.EntryRevision(next.MaxEntryRevision)
}

func durableDiagnosticPathV1(root, path string) (string, error) {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}
	relative = filepath.Clean(relative)
	if relative == "." || filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("resource path %q is outside database root", path)
	}
	return filepath.ToSlash(relative), nil
}

// ensureDurableValueLogReferencesRegisteredV1 resolves only the projected
// pointer dependencies that are absent from the manager. Normal in-process
// writers register segments at creation time; this bounded fallback covers
// externally prepared pointer segments without turning root publication into a
// recursive directory scan. ValuePtr does not encode the value/leaf namespace,
// so the same file ID in both namespaces is an identity conflict.
func (db *DB) ensureDurableValueLogReferencesRegisteredV1(references map[uint32]struct{}) error {
	if db == nil || db.valueLogManager == nil || len(references) == 0 {
		return nil
	}
	fileIDs := make([]uint32, 0, len(references))
	for fileID := range references {
		fileIDs = append(fileIDs, fileID)
	}
	sort.Slice(fileIDs, func(i, j int) bool { return fileIDs[i] < fileIDs[j] })
	for _, fileID := range fileIDs {
		if db.valueLogManager.HasSegment(fileID) {
			continue
		}
		candidatePaths := []string{
			valuelog.SegmentPath(ValueLogDirPath(db.dir), fileID),
			valuelog.SegmentPath(LeafLogDirPath(db.dir), fileID),
		}
		found := make([]string, 0, len(candidatePaths))
		for _, path := range candidatePaths {
			info, err := os.Stat(path)
			switch {
			case err == nil && !info.IsDir():
				found = append(found, path)
			case err == nil:
				return fmt.Errorf("%w: value-log file %d path %q is not a file", rootpublication.ErrUnresolvedResource, fileID, path)
			case errors.Is(err, os.ErrNotExist):
				continue
			default:
				return fmt.Errorf("%w: inspect value-log file %d path %q: %v", rootpublication.ErrUnresolvedResource, fileID, path, err)
			}
		}
		switch len(found) {
		case 0:
			return fmt.Errorf("%w: value-log file %d is not registered and has no canonical segment", rootpublication.ErrUnresolvedResource, fileID)
		case 1:
			if err := db.valueLogManager.RegisterSegment(found[0], fileID); err != nil {
				return fmt.Errorf("register durable value-log file %d: %w", fileID, err)
			}
		default:
			return fmt.Errorf("%w: value-log file %d exists in both canonical namespaces", rootpublication.ErrResourceConflict, fileID)
		}
	}
	return nil
}

func (db *DB) captureDurableValueLogResourcesV1(idx *indexGen, next page.MetaPageBody, delta *valueLogRefDelta) (*rootpublication.StableResourceSet, error) {
	if db.valueLogManager == nil {
		return nil, nil
	}
	references, projected, err := db.projectedValueLogReferencesV1(next, delta)
	if err != nil {
		return nil, err
	}
	if !projected {
		references, err = db.scanCandidateValueLogReferencesV1(idx, next)
		if err != nil {
			return nil, err
		}
	}
	if len(references) == 0 {
		return nil, nil
	}
	if err := db.ensureDurableValueLogReferencesRegisteredV1(references); err != nil {
		return nil, err
	}

	set := db.valueLogManager.CurrentSetNoRefresh()
	defer func() { _ = db.valueLogManager.Release(set) }()
	fileIDs := make([]uint32, 0, len(references))
	for fileID := range references {
		fileIDs = append(fileIDs, fileID)
	}
	sort.Slice(fileIDs, func(i, j int) bool { return fileIDs[i] < fileIDs[j] })
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
	abandon := true
	defer func() {
		if abandon {
			builder.Abandon()
		}
	}()
	for _, fileID := range fileIDs {
		file := set.Files[fileID]
		if file == nil || file.IsZombie.Load() || file.File == nil {
			return nil, fmt.Errorf("%w: value-log file %d is not registered", rootpublication.ErrUnresolvedResource, fileID)
		}
		diagnosticPath, err := durableDiagnosticPathV1(db.dir, file.Path)
		if err != nil {
			return nil, fmt.Errorf("%w: value-log file %d: %v", rootpublication.ErrUnresolvedResource, fileID, err)
		}
		token, err := db.valueLogManager.StableResourceToken(fileID, valuelog.StableResourceRegistration{
			Kind: rootpublication.ResourceValueLog, LogicalLane: "db/value-log",
			Generation: uint64(fileID), DiagnosticPath: diagnosticPath,
			Reachability: rootpublication.ReachabilityValueLogPointer,
		})
		if err != nil {
			return nil, err
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			return nil, err
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	abandon = false
	return resources, nil
}

// prepareDurableRootCandidateV1 validates and freezes one exact publication
// candidate without mutating either meta slot. COW pages are encoded into the
// allocator-owned memory store here but are not copied into the index until
// after external dependencies cross their flush and sync frontiers.
func (db *DB) prepareDurableRootCandidateV1(idx *indexGen, next page.MetaPageBody, retired []uint64, vlogRefDelta *valueLogRefDelta) (*durableRootPublishCandidateV1, error) {
	current := db.durableRoot
	if current.meta.CommitSeq == 0 || current.record.CommitSeq != current.meta.CommitSeq || next.CommitSeq != current.meta.CommitSeq+1 {
		return nil, fmt.Errorf("%w: durable=%d candidate=%d", errDurableRootCandidateStale, current.meta.CommitSeq, next.CommitSeq)
	}
	if next.UserRootPageID < 2 || next.SystemRootPageID < 2 {
		return nil, errors.New("durable-root candidate has invalid roots")
	}
	oldest, err := oldestRecoverableSlotCommitV1(current.slotCommit)
	if err != nil {
		return nil, err
	}
	capability, err := freelist.NewReuseCapability(oldest, db.MinPinnedSnapshotCommitSeq(), 0)
	if err != nil {
		return nil, err
	}
	resources, err := db.captureDurableValueLogResourcesV1(idx, next, vlogRefDelta)
	if err != nil {
		return nil, fmt.Errorf("capture durable-root dependencies: %w", err)
	}
	releaseResources := true
	defer func() {
		if releaseResources {
			resources.Release()
		}
	}()
	manifest, err := durableManifestFromResourcesV1(resources)
	if err != nil {
		return nil, err
	}
	target := uint64(MetaPage0ID)
	if current.slot == MetaPage0ID {
		target = MetaPage1ID
	}
	overwrittenPages, err := durableRootSlotAuxiliaryPagesV1(current.slotMeta[target], current.slotRecord[target])
	if err != nil {
		return nil, err
	}
	if err := idx.allocator.RetireCOWV1(overwrittenPages, current.slotCommit[target]); err != nil {
		return nil, fmt.Errorf("retire overwritten durable-root slot pages: %w", err)
	}
	if err := idx.allocator.RetireCOWV1(retired, current.record.CommitSeq); err != nil {
		return nil, fmt.Errorf("retire COW pages: %w", err)
	}
	auxiliaryCount := int(manifest.PageCount()) + 1
	prepared, err := idx.allocator.PrepareCOWCandidateV1(
		current.record.Freelist.GenerationID+1,
		next.CommitSeq,
		durableRootCandidateIDV1(current, next),
		capability,
		auxiliaryCount,
		freelist.NewMemoryPageStoreV1(),
	)
	if err != nil {
		return nil, fmt.Errorf("prepare COW generation: %w", err)
	}
	generation := prepared.Candidate().Generation()
	auxiliary := prepared.AuxiliaryPageIDs()
	if generation == nil || len(auxiliary) != auxiliaryCount {
		return nil, errors.New("incomplete durable-root COW candidate")
	}

	next.TotalPages = generation.HighWater()
	next.FreelistHeadID = 0
	recordPageID := auxiliary[len(auxiliary)-1]
	manifestRef, err := manifest.Materialize(auxiliary[0], freelist.NewMemoryPageStoreV1())
	if err != nil {
		return nil, err
	}
	record := rootpublication.DurableRootRecordV1{
		CommitSeq: next.CommitSeq, DurableSeq: next.CommitSeq,
		UserRootPageID: next.UserRootPageID, SystemRootPageID: next.SystemRootPageID,
		TotalPages: next.TotalPages, MaxEntryRevision: next.MaxEntryRevision,
		AppliedCommandLSN: next.AppliedCommandLSN, LastCommitHeight: next.LastCommitHeight,
		Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(),
		Manifest:           manifestRef,
		ParentRecordPageID: current.meta.RootRecordPageID, ParentCommitSeq: current.meta.CommitSeq,
		ParentRecordDigest:   current.meta.RootRecordDigest,
		MetaProjectionDigest: page.DurableMetaProjectionDigestV1(next.CommitSeq, next.CommitSeq, recordPageID),
	}
	_, recordDigest, err := record.EncodePage(recordPageID)
	if err != nil {
		return nil, err
	}
	meta, err := page.NewDurableMetaV1(next.CommitSeq, next.CommitSeq, recordPageID, recordDigest)
	if err != nil {
		return nil, err
	}
	stableSnapshot := db.acquireDurableCandidateStableIndexSnapshotV1(idx)
	if stableSnapshot == nil {
		return nil, errors.New("capture stable index snapshot")
	}
	if stableSnapshot.idx != idx {
		_ = stableSnapshot.Close()
		return nil, errors.New("stable index generation changed during durable-root preparation")
	}
	token, err := stableSnapshot.CaptureStableIndexFileResource()
	if err != nil {
		_ = stableSnapshot.Close()
		return nil, fmt.Errorf("capture stable index resource: %w", err)
	}

	candidate := &durableRootPublishCandidateV1{
		idx: idx, base: current, next: next, resources: resources, manifest: manifest,
		prepared: prepared, capability: capability, token: token, record: record, meta: meta, target: target,
	}
	releaseResources = false
	return candidate, nil
}

// acquireDurableCandidateStableIndexSnapshotV1 uses the normal public stable
// snapshot once Open has installed visible state. Command-WAL replay reaches
// durable publication earlier, while the recovered index is already the DB's
// exclusive live generation but no public snapshot view exists yet. In that
// recovery-only window, synthesize the same maintenance lease around the exact
// index generation so the resource token retains it through publication.
func (db *DB) acquireDurableCandidateStableIndexSnapshotV1(idx *indexGen) *Snapshot {
	if snapshot := db.AcquireStableSnapshot(); snapshot != nil {
		return snapshot
	}
	if db == nil || idx == nil || db.state.Load() != nil || db.closing.Load() {
		return nil
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	if db.state.Load() != nil || db.closing.Load() || db.idx.Load() != idx {
		return nil
	}
	snapshot := db.snapPool.Get()
	snapshot.db = db
	snapshot.idx = idx
	snapshot.stableIndexCapture = true
	snapshot.closed.Store(false)
	snapshot.readState.Store(0)
	db.stableIndexCaptures.Add(1)
	return snapshot
}

func (db *DB) materializeDurableRootCandidateV1(candidate *durableRootPublishCandidateV1) error {
	if candidate == nil || candidate.idx == nil || candidate.prepared == nil {
		return errors.New("missing durable-root candidate")
	}
	if candidate.materialized {
		return nil
	}
	generation := candidate.prepared.Candidate().Generation()
	if generation == nil {
		return errors.New("missing durable-root COW generation")
	}
	if err := candidate.idx.pager.Truncate(generation.HighWater()); err != nil {
		return fmt.Errorf("extend durable index: %w", err)
	}
	for _, image := range candidate.prepared.Candidate().Pages() {
		if err := candidate.idx.pager.Write(image.PageID, image.Data); err != nil {
			return fmt.Errorf("write COW page %d: %w", image.PageID, err)
		}
	}
	auxiliary := candidate.prepared.AuxiliaryPageIDs()
	if _, err := candidate.manifest.Materialize(auxiliary[0], durablePagerSinkV1{pager: candidate.idx.pager}); err != nil {
		return err
	}
	recordPageID := auxiliary[len(auxiliary)-1]
	recordImage, recordDigest, err := candidate.record.EncodePage(recordPageID)
	if err != nil {
		return err
	}
	if recordDigest != candidate.meta.RootRecordDigest {
		return errors.New("durable-root record digest changed after preparation")
	}
	if err := candidate.idx.pager.Write(recordPageID, recordImage); err != nil {
		return fmt.Errorf("write durable root record: %w", err)
	}
	candidate.materialized = true
	return nil
}

func (db *DB) retainDurableRootRetryV1(candidate *durableRootPublishCandidateV1, err error) (page.MetaPageBody, error) {
	current := candidate.base
	current.pending = candidate
	db.durableRoot = current
	return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
}

func (db *DB) poisonDurableRootCandidateV1(candidate *durableRootPublishCandidateV1, err error) (page.MetaPageBody, error) {
	current := candidate.base
	current.pending = nil
	current.ambiguous = append(current.ambiguous, candidate)
	db.durableRoot = current
	db.publicationPoisoned.Store(true)
	return page.MetaPageBody{}, wrapFinalizeCommitError(errors.Join(err, ErrRecoveryRequired), false)
}

func (db *DB) executeDurableRootCandidateV1(candidate *durableRootPublishCandidateV1) (page.MetaPageBody, error) {
	if candidate == nil || candidate.released || candidate.metaMutated {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.New("durable-root candidate is not retryable"), true)
	}
	if err := candidate.resources.FlushThrough(); err != nil {
		return db.retainDurableRootRetryV1(candidate, fmt.Errorf("flush durable-root external dependencies: %w", err))
	}
	if err := candidate.resources.SyncThrough(); err != nil {
		return db.retainDurableRootRetryV1(candidate, fmt.Errorf("sync durable-root external dependencies: %w", err))
	}
	if err := db.materializeDurableRootCandidateV1(candidate); err != nil {
		return db.retainDurableRootRetryV1(candidate, err)
	}
	if err := candidate.token.SyncThrough(); err != nil {
		return db.retainDurableRootRetryV1(candidate, fmt.Errorf("sync durable-root index: %w", err))
	}

	metaPath := ""
	if durabilitycut.Enabled() {
		metaPath = filepath.Join(db.dir, indexFileName)
	}
	metaOffset := int64(candidate.target) * int64(page.PageSize)
	if err := durabilitycut.EmitRange(durabilitycut.BeforeMetaWrite, durabilitycut.ResourceMeta, db.dir, metaPath, metaOffset, int64(page.PageSize)); err != nil {
		return db.retainDurableRootRetryV1(candidate, err)
	}
	if db.testFailWriteMeta.Load() {
		return db.retainDurableRootRetryV1(candidate, errTestWriteMetaFailpoint)
	}

	// The outcome becomes ambiguous as soon as the target-meta mutation starts,
	// including a write call that returns an error after a partial page update.
	candidate.metaMutated = true
	if err := writeDurableMetaSlotV1(durablePagerSinkV1{pager: candidate.idx.pager}, candidate.target, candidate.meta); err != nil {
		return db.poisonDurableRootCandidateV1(candidate, err)
	}
	if err := durabilitycut.EmitRange(durabilitycut.AfterMetaWrite, durabilitycut.ResourceMeta, db.dir, metaPath, metaOffset, int64(page.PageSize)); err != nil {
		return db.poisonDurableRootCandidateV1(candidate, err)
	}
	if err := durabilitycut.EmitPath(durabilitycut.BeforeMetaSync, durabilitycut.ResourceMeta, db.dir, metaPath); err != nil {
		return db.poisonDurableRootCandidateV1(candidate, err)
	}
	if db.testFailSyncMeta.Load() {
		return db.poisonDurableRootCandidateV1(candidate, errTestSyncMetaFailpoint)
	}
	if err := candidate.token.WithPinnedFile(func(file *os.File) error {
		return candidate.idx.pager.SyncPagesWithStableFile(file, []uint64{candidate.target})
	}); err != nil {
		return db.poisonDurableRootCandidateV1(candidate, err)
	}
	if err := durabilitycut.EmitPath(durabilitycut.AfterMetaSync, durabilitycut.ResourceMeta, db.dir, metaPath); err != nil {
		return db.poisonDurableRootCandidateV1(candidate, err)
	}
	if err := candidate.idx.allocator.PublishCOWCandidateV1(candidate.prepared, candidate.capability); err != nil {
		return db.poisonDurableRootCandidateV1(candidate, fmt.Errorf("publish COW generation: %w", err))
	}

	current := candidate.base
	current.meta = candidate.meta
	current.record = candidate.record
	current.manifest = candidate.manifest
	current.slot = candidate.target
	current.slotCommit[candidate.target] = candidate.next.CommitSeq
	current.slotMeta[candidate.target] = candidate.meta
	current.slotRecord[candidate.target] = candidate.record
	previousResources := current.slotResources[candidate.target]
	current.slotResources[candidate.target] = candidate.resources
	current.pending = nil
	db.durableRoot = current
	candidate.resources = nil
	candidate.release()
	previousResources.Release()
	return candidate.next, nil
}

func sameDurableRootCandidateV1(candidate *durableRootPublishCandidateV1, idx *indexGen, next page.MetaPageBody) bool {
	if candidate == nil || candidate.idx != idx {
		return false
	}
	want := candidate.next
	return next.CommitSeq == want.CommitSeq && next.UserRootPageID == want.UserRootPageID &&
		next.SystemRootPageID == want.SystemRootPageID && next.AppliedCommandLSN == want.AppliedCommandLSN &&
		next.MaxEntryRevision == want.MaxEntryRevision && next.LastCommitHeight == want.LastCommitHeight
}

// publishDurableRootV1 is the sole V1 meta mutator. Pre-meta failures retain
// the exact immutable candidate for retry; post-meta failures retain it as an
// ambiguous recovery closure and poison the writable handle.
func (db *DB) publishDurableRootV1(idx *indexGen, next page.MetaPageBody, retired []uint64, vlogRefDelta *valueLogRefDelta) (page.MetaPageBody, error) {
	if db == nil || idx == nil || idx.pager == nil || idx.allocator == nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.New("missing durable-root index"), true)
	}
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	if pending := db.durableRoot.pending; pending != nil {
		if !sameDurableRootCandidateV1(pending, idx, next) {
			return page.MetaPageBody{}, wrapFinalizeCommitError(freelist.ErrCOWCandidatePrepared, true)
		}
		return db.executeDurableRootCandidateV1(pending)
	}
	candidate, err := db.prepareDurableRootCandidateV1(idx, next, retired, vlogRefDelta)
	if err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
	}
	return db.executeDurableRootCandidateV1(candidate)
}

func (db *DB) retryPendingDurableRootV1() (page.MetaPageBody, error) {
	if db == nil {
		return page.MetaPageBody{}, ErrClosed
	}
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	pending := db.durableRoot.pending
	if pending == nil {
		return page.MetaPageBody{}, errors.New("durable-root publication has no retry candidate")
	}
	return db.executeDurableRootCandidateV1(pending)
}

func (db *DB) initializeDurableRootV1(idx *indexGen) error {
	if db == nil || idx == nil || idx.pager == nil || idx.allocator == nil {
		return errors.New("missing durable-root index")
	}
	p := idx.pager
	if _, err := p.Alloc(2); err != nil {
		return err
	}
	rootIDs := make([]uint64, 2)
	for i := range rootIDs {
		pageID, err := p.Alloc(1)
		if err != nil {
			return err
		}
		image, err := p.GetForWrite(pageID)
		if err != nil {
			return err
		}
		builder := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			PackedValuePtr:        db.indexPackedValuePtr,
			InternalBaseDelta:     db.indexInternalBaseDelta,
		})
		builder.SetPageID(pageID)
		builder.Finish()
		rootIDs[i] = pageID
	}

	base, err := freelist.NewFreelistGenerationV1(1, p.PageCount(), nil, nil)
	if err != nil {
		return err
	}
	ledger := freelist.NewReservationLedger()
	if err := idx.allocator.EnableCOWV1(base, ledger); err != nil {
		return err
	}
	capability, err := freelist.NewReuseCapability(1, 1, 0)
	if err != nil {
		return err
	}
	manifest, err := rootpublication.NewDependencyManifestV1(nil)
	if err != nil {
		return err
	}
	store := freelist.NewMemoryPageStoreV1()
	var candidateID freelist.CandidateIDV1
	binary.LittleEndian.PutUint64(candidateID[:8], 1)
	prepared, err := idx.allocator.PrepareCOWCandidateV1(2, 1, candidateID, capability, 2, store)
	if err != nil {
		return err
	}
	auxiliary := prepared.AuxiliaryPageIDs()
	if len(auxiliary) != 2 {
		return errors.New("durable-root initializer did not reserve manifest and record pages")
	}
	generation := prepared.Candidate().Generation()
	if err := p.Truncate(generation.HighWater()); err != nil {
		return err
	}
	for _, image := range prepared.Candidate().Pages() {
		if err := p.Write(image.PageID, image.Data); err != nil {
			return fmt.Errorf("write initial COW freelist page %d: %w", image.PageID, err)
		}
	}
	sink := durablePagerSinkV1{pager: p}
	manifestRef, err := manifest.Materialize(auxiliary[0], sink)
	if err != nil {
		return err
	}
	record := rootpublication.DurableRootRecordV1{
		CommitSeq: 1, DurableSeq: 1,
		UserRootPageID: rootIDs[0], SystemRootPageID: rootIDs[1], TotalPages: generation.HighWater(),
		Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(),
		Manifest: manifestRef, MetaProjectionDigest: page.DurableMetaProjectionDigestV1(1, 1, auxiliary[1]),
	}
	recordImage, recordDigest, err := record.EncodePage(auxiliary[1])
	if err != nil {
		return err
	}
	if err := p.Write(auxiliary[1], recordImage); err != nil {
		return err
	}
	meta, err := page.NewDurableMetaV1(1, 1, auxiliary[1], recordDigest)
	if err != nil {
		return err
	}
	if err := writeDurableMetaSlotV1(sink, MetaPage0ID, meta); err != nil {
		return err
	}
	if err := p.Sync(); err != nil {
		return err
	}
	if err := idx.allocator.PublishCOWCandidateV1(prepared, capability); err != nil {
		return err
	}
	db.installDurableRootSelectionV1(durableRootSelectionV1{
		Slot: MetaPage0ID, Meta: meta, Record: record, Freelist: generation, Manifest: manifest,
		SlotCommits: [2]uint64{1, 0},
		SlotMetas:   [2]page.DurableMetaV1{meta, {}},
		SlotRecords: [2]rootpublication.DurableRootRecordV1{record, {}},
	})
	return nil
}

func writeDurableMetaSlotV1(sink freelist.AppendPageSink, slot uint64, meta page.DurableMetaV1) error {
	if sink == nil || slot > 1 {
		return page.ErrDurableMetaFormat
	}
	image := make([]byte, page.PageSize)
	if err := meta.Encode(image[page.PageHeaderSize:]); err != nil {
		return err
	}
	header := page.PageHeader{PageID: slot, Flags: uint16(page.PageTypeMeta)}
	header.Encode(image)
	page.UpdateChecksum(image)
	return sink.WritePage(slot, image)
}

func (db *DB) installDurableRootSelectionV1(selected durableRootSelectionV1) {
	db.durableRoot = durableRootRuntimeV1{
		meta: selected.Meta, record: selected.Record, manifest: selected.Manifest,
		slot: selected.Slot, slotCommit: selected.SlotCommits, slotResources: selected.SlotResources,
		slotMeta: selected.SlotMetas, slotRecord: selected.SlotRecords,
	}
	db.meta = page.MetaPageBody{
		CommitSeq: selected.Record.CommitSeq, UserRootPageID: selected.Record.UserRootPageID,
		SystemRootPageID: selected.Record.SystemRootPageID, TotalPages: selected.Record.TotalPages,
		LastCommitHeight: selected.Record.LastCommitHeight, AppliedCommandLSN: selected.Record.AppliedCommandLSN,
		MaxEntryRevision: selected.Record.MaxEntryRevision,
	}
	db.metaPageID = selected.Slot
}

func (db *DB) validateDurableDependencyManifestV1(manifest *rootpublication.DependencyManifestV1) (*rootpublication.StableResourceSet, error) {
	if manifest == nil {
		return nil, rootpublication.ErrDependencyManifestFormat
	}
	entries := manifest.Entries()
	if len(entries) == 0 {
		return nil, nil
	}
	if db == nil || db.valueLogManager == nil {
		return nil, fmt.Errorf("%w: value-log manager unavailable", rootpublication.ErrUnresolvedResource)
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	defer func() { _ = db.valueLogManager.Release(set) }()
	builder := rootpublication.NewStableResourceSetBuilder()
	abandon := true
	defer func() {
		if abandon {
			builder.Abandon()
		}
	}()
	for _, entry := range entries {
		if entry.Kind != rootpublication.ResourceValueLog {
			return nil, fmt.Errorf("%w: unsupported durable dependency kind %q", rootpublication.ErrUnresolvedResource, entry.Kind)
		}
		fileID64, err := strconv.ParseUint(entry.ResourceID, 10, 32)
		if err != nil || fileID64 == 0 || entry.Generation != fileID64 || entry.LogicalLane != "db/value-log" {
			return nil, fmt.Errorf("%w: invalid value-log resource identity %q/%d", rootpublication.ErrUnresolvedResource, entry.ResourceID, entry.Generation)
		}
		if len(entry.Reachability) != 1 || entry.Reachability[0] != rootpublication.ReachabilityValueLogPointer {
			return nil, fmt.Errorf("%w: value-log resource %q has invalid reachability", rootpublication.ErrUnresolvedResource, entry.ResourceID)
		}
		fileID := uint32(fileID64)
		file := set.Files[fileID]
		if file == nil || file.File == nil || file.IsZombie.Load() {
			return nil, fmt.Errorf("%w: value-log file %d is missing", rootpublication.ErrUnresolvedResource, fileID)
		}
		diagnosticPath, err := durableDiagnosticPathV1(db.dir, file.Path)
		if err != nil || diagnosticPath != entry.DiagnosticPath {
			return nil, fmt.Errorf("%w: value-log file %d path differs from manifest", rootpublication.ErrResourceConflict, fileID)
		}
		token, err := db.valueLogManager.StableResourceToken(fileID, valuelog.StableResourceRegistration{
			Kind: rootpublication.ResourceValueLog, LogicalLane: entry.LogicalLane,
			Generation: entry.Generation, DiagnosticPath: entry.DiagnosticPath,
			Reachability: rootpublication.ReachabilityValueLogPointer,
		})
		if err != nil {
			return nil, err
		}
		identity := token.Identity()
		frontier := token.Frontier()
		if identity.Generation != entry.Identity.Generation || !rootpublication.SamePhysicalIdentity(identity, entry.Identity) {
			token.Release()
			return nil, fmt.Errorf("%w: value-log file %d identity differs from manifest", rootpublication.ErrResourceConflict, fileID)
		}
		if frontier.Bytes < entry.Frontier.Bytes {
			token.Release()
			return nil, fmt.Errorf("%w: value-log file %d frontier %d is below required %d", rootpublication.ErrFrontierBeyondResource, fileID, frontier.Bytes, entry.Frontier.Bytes)
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			return nil, err
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	abandon = false
	return resources, nil
}

func (db *DB) releaseDurableRootResourcesV1() {
	if db == nil {
		return
	}
	db.durablePublishMu.Lock()
	resources := append([]*rootpublication.StableResourceSet(nil), db.durableRoot.slotResources[:]...)
	pending := db.durableRoot.pending
	ambiguous := append([]*durableRootPublishCandidateV1(nil), db.durableRoot.ambiguous...)
	db.durableRoot.slotResources = [2]*rootpublication.StableResourceSet{}
	db.durableRoot.pending = nil
	db.durableRoot.ambiguous = nil
	db.durablePublishMu.Unlock()
	for _, set := range resources {
		set.Release()
	}
	pending.release()
	for _, candidate := range ambiguous {
		candidate.release()
	}
}
