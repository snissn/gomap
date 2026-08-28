package db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type durablePagerSinkV1 struct{ pager *pager.Pager }

var _ freelist.CandidatePageWriterV1 = durablePagerSinkV1{}

var (
	errDurableRootCandidateStale = errors.New("durable-root candidate base changed")
)

const maxDurableRootPreMetaRetriesV1 = 64

func (sink durablePagerSinkV1) WritePage(pageID uint64, image []byte) error {
	if sink.pager == nil {
		return errors.New("missing durable pager")
	}
	return sink.pager.Write(pageID, image)
}

func (sink durablePagerSinkV1) WriteCandidatePageV1(pageID uint64, view freelist.CandidatePageViewV1) error {
	if sink.pager == nil {
		return errors.New("missing durable pager")
	}
	return freelist.WriteCandidatePageToPagerV1(sink.pager, pageID, view)
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
// preparation except for the retry reuse capability and phase markers. The
// capability is refreshed immediately before a retry while durablePublishMu
// and the root-reuse admission gate are held; phase markers only move forward.
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

func (db *DB) durableRootReuseCapabilityV1(current durableRootRuntimeV1) (freelist.ReuseCapability, error) {
	oldest, err := oldestRecoverableSlotCommitV1(current.slotCommit)
	if err != nil {
		return freelist.ReuseCapability{}, err
	}
	return freelist.NewReuseCapability(oldest, db.MinPinnedSnapshotCommitSeq(), 0)
}

func durableManifestFromResourcesV1(resources *rootpublication.StableResourceSet) (*rootpublication.DependencyManifestV1, error) {
	manifest, _, err := durableManifestFromResourcesV1WithWork(resources)
	return manifest, err
}

func durableManifestFromResourcesV1WithWork(resources *rootpublication.StableResourceSet) (*rootpublication.DependencyManifestV1, rootpublication.DependencyManifestBuildWorkV1, error) {
	if resources == nil {
		return rootpublication.NewDependencyManifestV1WithWork(nil)
	}
	return resources.DependencyManifestV1()
}

func (db *DB) durableManifestFromResourcesV1WithStats(resources *rootpublication.StableResourceSet) (*rootpublication.DependencyManifestV1, error) {
	started := time.Now()
	manifest, work, err := durableManifestFromResourcesV1WithWork(resources)
	db.durableRootManifestBuildCount.Add(1)
	db.durableRootManifestBuildNs.Add(uint64(time.Since(started)))
	db.durableRootManifestEntriesSeen.Add(work.EntriesVisited)
	db.durableRootManifestEntriesEncoded.Add(work.EntriesEncoded)
	db.durableRootManifestBytesEncoded.Add(work.BytesEncoded)
	return manifest, err
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
	// Logical counts alone cannot describe raw outer-leaf dependencies. The
	// bounded reuse planner composes them explicitly when it has certified
	// predecessor/current-segment evidence; every fallback must scan the full
	// candidate closure.
	if db.indexOuterLeavesInValueLog || delta != nil && delta.outerLeafDependencyReuse {
		return nil, false, nil
	}
	return db.projectedLogicalValueLogReferencesV1(next, delta)
}

func (db *DB) projectedLogicalValueLogReferencesV1(next page.MetaPageBody, delta *valueLogRefDelta) (map[uint32]struct{}, bool, error) {
	// The persisted tracker projects logical ValuePtr counts. Raw outer-leaf
	// LogRecordRef dependencies are composed separately by the bounded
	// predecessor/current-segment plan below.
	tracker := db.valueLogRefTracker
	if tracker == nil {
		return nil, false, nil
	}
	tracker.mu.RLock()
	// The reachability tracker follows the visible root, not the last stable
	// meta slot. Once root publication is activated several visible generations
	// may legitimately be queued behind one durable generation. The candidate's
	// predecessor sequence is the exact projection base and avoids sampling
	// db.meta under a second lock while the tracker is pinned.
	expectedBaseCommitSeq := next.CommitSeq - 1
	if !tracker.valid || tracker.commitSeq != expectedBaseCommitSeq {
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
				return fmt.Errorf("value-log dependency count underflow for file %d: have=%d decrement=%d", fileID, current, decrement)
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

func (db *DB) scanCandidateValueLogReferencesV1(idx *indexGen, next page.MetaPageBody, valueLogPublicationLocked bool) (map[uint32]struct{}, error) {
	var snapshot *Snapshot
	if valueLogPublicationLocked {
		snapshot = db.acquireSnapshotWithValueLogPublicationLockHeld()
	} else {
		snapshot = db.AcquireSnapshot()
	}
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
			vlogPinned:        true,
			reader:            newValueReader(set),
			registryShardHint: snapshotShardHintUnset,
		}
		references, scanErr := db.scanCandidateExternalReferencesV1(recoverySnapshot)
		closeErr := recoverySnapshot.Close()
		if scanErr != nil || closeErr != nil {
			return nil, errors.Join(scanErr, closeErr)
		}
		return references, nil
	}
	if snapshot.state == nil {
		_ = snapshot.Close()
		return nil, errors.New("capture candidate dependency snapshot")
	}
	if snapshot.idx != idx {
		_ = snapshot.Close()
		return nil, errors.New("candidate dependency index changed")
	}
	// Publication registers newly produced value/outer-leaf segments before
	// this scan, while the visible snapshot may still carry the pre-publication
	// value-log set. Rebind this private candidate view to a fresh manager set so
	// traversing a newly reachable outer leaf never consults the stale set.
	freshSet := db.valueLogManager.CurrentSetNoRefresh()
	if len(freshSet.Files) == 0 {
		_ = db.valueLogManager.Release(freshSet)
		freshSet = nil
	}
	oldSet := snapshot.state.ValueLogSet
	oldSetPinned := snapshot.vlogPinned
	candidateState := *snapshot.state
	candidateState.ValueLogSet = freshSet
	applyDurableRootCandidateStateV1(&candidateState, next)
	snapshot.state = &candidateState
	snapshot.vlogPinned = freshSet != nil
	snapshot.reader = newValueReader(freshSet)
	if oldSetPinned && oldSet != nil {
		if err := db.valueLogManager.Release(oldSet); err != nil {
			_ = snapshot.Close()
			return nil, fmt.Errorf("release stale candidate dependency set: %w", err)
		}
	}
	references, scanErr := db.scanCandidateExternalReferencesV1(snapshot)
	closeErr := snapshot.Close()
	if scanErr != nil || closeErr != nil {
		return nil, errors.Join(scanErr, closeErr)
	}
	return references, nil
}

// scanCandidateExternalReferencesV1 computes the exact segment closure for
// values stored in value-log frames and for outer leaves stored as raw leaf-log
// records. The shared maintenance scan projects values inside an outer leaf;
// the leaf-ref walk separately records the segment that owns the outer leaf
// itself.
func (db *DB) scanCandidateExternalReferencesV1(snapshot *Snapshot) (map[uint32]struct{}, error) {
	if db == nil || db.valueLogManager == nil || snapshot == nil || snapshot.state == nil || snapshot.idx == nil || snapshot.idx.pager == nil {
		return nil, errors.New("scan candidate external references: missing snapshot state")
	}
	if hook := db.testScanCandidateExternalReferencesHook; hook != nil {
		hook()
	}
	references := make(map[uint32]struct{})
	registerOuterLeaves := func(rootIDs []uint64) error {
		if err := leafrefscan.WalkRoots(context.Background(), rootIDs, snapshot.idx.pager.Get, nil, func(ptr page.LeafLogPtr) error {
			if ptr.FileID != 0 {
				// LeafLogPtr stores the unmarked segment ID; the manager and
				// dependency manifest use the ValuePtr-compatible marked ID.
				references[ptr.ValueLogFileID()] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}
		if err := db.requireDurableValueLogReferencesRegisteredV1(references); err != nil {
			return err
		}
		return db.rebindCandidateValueLogSetV1(snapshot)
	}
	registerValuePointers := func(rootIDs []uint64) error {
		result, err := db.maintenanceReachabilityScan(context.Background(), snapshot, maintenanceReachabilityScanOptions{
			Collectors:      maintenanceReachabilityValueLogRefCounts,
			ExplicitRootIDs: rootIDs,
		})
		if err != nil {
			return err
		}
		for fileID := range result.valueLogReferencedSegments {
			references[fileID] = struct{}{}
		}
		if err := db.requireDurableValueLogReferencesRegisteredV1(references); err != nil {
			return err
		}
		return db.rebindCandidateValueLogSetV1(snapshot)
	}
	// Raw outer-leaf pointers can be discovered without dereferencing their
	// segments. Register those exact canonical files first so an unreported but
	// valid producer segment remains readable during the full closure scan.
	primaryRootIDs := []uint64{snapshot.state.RootPageID, snapshot.state.SystemRootPageID}
	if err := registerOuterLeaves(primaryRootIDs); err != nil {
		return nil, err
	}
	// Pointer projection exposes the exact value-log IDs in the primary roots
	// without reading their records. This must precede descriptor decoding: a
	// collection root descriptor can itself be pointer-backed by a canonical
	// segment that an external producer has not registered with the manager.
	if err := registerValuePointers(primaryRootIDs); err != nil {
		return nil, err
	}
	roots, _, err := maintenanceReachabilityRoots(context.Background(), snapshot, nil, nil, false, false)
	if err != nil {
		return nil, err
	}
	// System descriptors may expose collection/vector roots that have their own
	// raw outer leaves. Register that second bounded frontier before projecting
	// value pointers inside any of those leaves.
	rootIDs := maintenanceRootIDs(roots)
	if err := registerOuterLeaves(rootIDs); err != nil {
		return nil, err
	}
	if err := registerValuePointers(rootIDs); err != nil {
		return nil, err
	}
	result, err := db.maintenanceReachabilityScan(context.Background(), snapshot, maintenanceReachabilityScanOptions{
		Collectors: maintenanceReachabilityValueLogRefCounts,
	})
	if err != nil {
		return nil, err
	}
	for fileID := range result.valueLogReferencedSegments {
		references[fileID] = struct{}{}
	}
	return references, nil
}

// rebindCandidateValueLogSetV1 refreshes only a private candidate snapshot
// from the manager's already-registered files. It never scans the filesystem;
// the caller must register every newly discovered canonical segment first.
func (db *DB) rebindCandidateValueLogSetV1(snapshot *Snapshot) error {
	if db == nil || db.valueLogManager == nil || snapshot == nil || snapshot.state == nil {
		return errors.New("rebind candidate value-log set: missing snapshot state")
	}
	fresh := db.valueLogManager.CurrentSetNoRefresh()
	old := snapshot.state.ValueLogSet
	oldPinned := snapshot.vlogPinned
	state := *snapshot.state
	state.ValueLogSet = fresh
	snapshot.state = &state
	snapshot.vlogPinned = true
	snapshot.reader = newValueReader(fresh)
	if oldPinned && old != nil {
		if err := db.valueLogManager.Release(old); err != nil {
			return fmt.Errorf("release stale candidate dependency set: %w", err)
		}
	}
	return nil
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

// requireDurableValueLogReferencesRegisteredV1 enforces producer ownership:
// every pointer made reachable by a candidate must already have been
// registered from the producer's exact open handle. Publication never stats a
// derived path to discover or authorize an otherwise unreported resource.
func (db *DB) requireDurableValueLogReferencesRegisteredV1(references map[uint32]struct{}) error {
	if db == nil || db.valueLogManager == nil || len(references) == 0 {
		return nil
	}
	fileIDs := make([]uint32, 0, len(references))
	for fileID := range references {
		fileIDs = append(fileIDs, fileID)
	}
	sort.Slice(fileIDs, func(i, j int) bool { return fileIDs[i] < fileIDs[j] })
	for _, fileID := range fileIDs {
		if !db.valueLogManager.HasSegment(fileID) {
			return fmt.Errorf("%w: value-log file %d was not registered by its producer", rootpublication.ErrUnresolvedResource, fileID)
		}
	}
	return nil
}

func (db *DB) captureDurableValueLogResourcesV1(idx *indexGen, next page.MetaPageBody, delta *valueLogRefDelta, exactPackedFileIDs map[uint32]struct{}, valueLogPublicationLocked bool) (*rootpublication.StableResourceSet, error) {
	if db.valueLogManager == nil {
		return nil, nil
	}
	references, projected, err := db.projectedValueLogReferencesV1(next, delta)
	if err != nil {
		return nil, err
	}
	if !projected {
		references, err = db.scanCandidateValueLogReferencesV1(idx, next, valueLogPublicationLocked)
		if err != nil {
			return nil, err
		}
	}
	// Packed outer-leaf files are captured by their promotion producer with a
	// stronger immutable digest and namespace token. Do not recapture the same
	// physical file as a generic raw outer-leaf segment: that would both weaken
	// its semantic classification and conflict with the producer token.
	for fileID := range exactPackedFileIDs {
		delete(references, fileID)
	}
	if len(references) == 0 {
		return nil, nil
	}
	if err := db.requireDurableValueLogReferencesRegisteredV1(references); err != nil {
		return nil, err
	}
	return db.captureRegisteredDurableValueLogResourcesV1(references)
}

// planOuterLeafBaseDependencyReuseV1 recognizes the common COW transition in
// which the preceding visible root's exact resource identities remain a
// complete bounded projection for the next candidate. Their handles are still
// recaptured so an append to an existing segment advances the stable frontier.
// Logical value-pointer additions are supplied by the apply delta. Raw
// outer-leaf identities remain safe as a predecessor dependency superset while
// producer-reported current segments are admitted. Ordinary DB-root
// destructive transitions keep requiresCandidateProjection set and therefore
// fall back to the exact scanner for leaf-generation GC. The ordered multi-root
// path may retain raw predecessor membership across segment rotation, while
// the exact logical ValuePtr tracker removes stale value-log membership.
// A replacement generation manifest does not invalidate this proof: the caller
// replaces that authoritative namespace token independently while this plan
// recaptures only the raw/value-log handles.
func (db *DB) planOuterLeafBaseDependencyReuseV1(base, additional *rootpublication.StableResourceSet, next page.MetaPageBody, delta *valueLogRefDelta) (map[uint32]struct{}, bool, error) {
	if db == nil || delta == nil || (!db.indexOuterLeavesInValueLog && !delta.outerLeafDependencyReuse) {
		return nil, false, nil
	}
	if delta.requiresCandidateProjection {
		return nil, false, nil
	}
	known := make(map[uint32]struct{})
	references := make(map[uint32]struct{})
	additionalReferences := make(map[uint32]struct{})
	for _, item := range []struct {
		resources  *rootpublication.StableResourceSet
		additional bool
	}{{resources: base}, {resources: additional, additional: true}} {
		resources := item.resources
		for _, descriptor := range resources.Descriptors() {
			switch descriptor.Kind() {
			case rootpublication.ResourceValueLog, rootpublication.ResourceOuterLeafLog:
				if descriptor.Generation() <= uint64(^uint32(0)) {
					fileID := uint32(descriptor.Generation())
					known[fileID] = struct{}{}
					if item.additional {
						additionalReferences[fileID] = struct{}{}
					} else {
						references[fileID] = struct{}{}
					}
				}
			}
		}
	}
	predecessorDependencyEmpty := len(known) == 0
	// Producer-reported positive identities include newly created/current raw
	// outer-leaf segments. Admit them before comparing the current segment set
	// with the predecessor closure; otherwise the first publication after a
	// rotation unnecessarily falls back to a full candidate scan.
	if err := delta.forEachPositive(func(fileID uint32, _ int64) error {
		known[fileID] = struct{}{}
		if _, ownedByAdditional := additionalReferences[fileID]; !ownedByAdditional {
			references[fileID] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, false, err
	}
	if predecessorDependencyEmpty && delta.allowEmptyDependencyReuse {
		created, err := leafPageLogCreatedSegments(db.leafPageLog)
		if err != nil {
			return nil, false, err
		}
		for _, segment := range created {
			if segment.FileID == 0 {
				continue
			}
			known[segment.FileID] = struct{}{}
			references[segment.FileID] = struct{}{}
		}
	}
	current, err := leafPageLogCurrentSegments(db.leafPageLog)
	if err != nil {
		return nil, false, err
	}
	if db.leafPageLog != nil && len(current) == 0 {
		// A producer that cannot report its current stable identity cannot prove
		// which raw leaf segment the new COW pages reference. Preserve the
		// fail-closed contract by projecting the candidate instead.
		return nil, false, nil
	}
	for _, segment := range current {
		if segment.FileID == 0 {
			continue
		}
		if _, ok := known[segment.FileID]; !ok {
			if predecessorDependencyEmpty && delta.allowEmptyDependencyReuse {
				// The empty predecessor has no value-log or raw-leaf
				// dependency to retire, so every producer-reported created and
				// current segment can be admitted without a candidate scan.
				references[segment.FileID] = struct{}{}
				known[segment.FileID] = struct{}{}
				continue
			}
			// The first publication into a new raw-leaf segment is the bounded
			// point where the old segment set is re-projected exactly.
			return nil, false, nil
		}
	}
	hasNegative := false
	if err := delta.forEachChange(func(fileID uint32, change int64) error {
		if change < 0 {
			hasNegative = true
		}
		if change > 0 {
			if _, ownedByAdditional := additionalReferences[fileID]; !ownedByAdditional {
				references[fileID] = struct{}{}
			}
		}
		return nil
	}); err != nil {
		return nil, false, err
	}
	if hasNegative {
		logicalReferences, projected, err := db.projectedLogicalValueLogReferencesV1(next, delta)
		if err != nil {
			return nil, false, err
		}
		if !projected {
			return nil, false, nil
		}
		for fileID := range references {
			lane, _ := valuelog.DecodeFileID(fileID)
			if lane != valuelog.ReservedLeafLogLaneID {
				delete(references, fileID)
			}
		}
		for fileID := range logicalReferences {
			lane, _ := valuelog.DecodeFileID(fileID)
			if lane == valuelog.ReservedLeafLogLaneID {
				continue
			}
			references[fileID] = struct{}{}
		}
	}
	for fileID := range additionalReferences {
		delete(references, fileID)
	}
	return references, true, nil
}

// captureDurableRootResourcesV1 builds the candidate root's complete external
// closure. Immutable resources already retained by the selected durable slot
// are cloned from their exact handles; value-log and raw outer-leaf resources
// are replaced by a fresh candidate dependency capture. additional is a
// producer-owned exact closure for resources made reachable by this publish
// and is consumed on both success and failure.
func (db *DB) captureDurableRootResourcesV1(idx *indexGen, next page.MetaPageBody, delta *valueLogRefDelta, additional *rootpublication.StableResourceSet, requirements rootpublication.StableLogicalObligationRequirements, mutation rootpublication.StableLogicalObligationMutation, appendMutation rootpublication.StableLogicalObligationMutation, requirementWork rootpublication.StableResourceClosureWork, requirementsFallback func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error), valueLogPublicationLocked bool, timing *CommandWALPublishTiming) (*rootpublication.StableResourceSet, error) {
	selected := db.durableRoot.slotResources[db.durableRoot.slot]
	return db.captureDurableRootResourcesFromBaseV1(idx, next, delta, selected, additional, requirements, mutation, appendMutation, requirementWork, requirementsFallback, valueLogPublicationLocked, timing)
}

// captureDurableRootResourcesFromBaseV1 is the common closure builder for
// synchronous durable publication and queued visible publication. Synchronous
// callers pass the currently selected durable-slot closure. The coordinator
// path passes its independently owned visible-root closure so a candidate
// built while an earlier group is syncing inherits every transitive resource
// that remains reachable from the immediately preceding visible root.
func (db *DB) captureDurableRootResourcesFromBaseV1(idx *indexGen, next page.MetaPageBody, delta *valueLogRefDelta, base *rootpublication.StableResourceSet, additional *rootpublication.StableResourceSet, requirements rootpublication.StableLogicalObligationRequirements, mutation rootpublication.StableLogicalObligationMutation, appendMutation rootpublication.StableLogicalObligationMutation, requirementWork rootpublication.StableResourceClosureWork, requirementsFallback func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error), valueLogPublicationLocked bool, timing *CommandWALPublishTiming) (*rootpublication.StableResourceSet, error) {
	if additional != nil {
		defer additional.Release()
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	abandon := true
	defer func() {
		if abandon {
			builder.Abandon()
		}
	}()
	merge := func(resources *rootpublication.StableResourceSet) error {
		if resources == nil {
			return nil
		}
		if err := builder.Merge(resources); err != nil {
			resources.Release()
			return err
		}
		return nil
	}

	exactPackedFileIDs := make(map[uint32]struct{})
	hasReplacementManifest := false
	for _, resources := range []*rootpublication.StableResourceSet{base, additional} {
		for _, descriptor := range resources.PhysicalDescriptors() {
			switch descriptor.Kind {
			case rootpublication.ResourceOuterLeafPack:
				if descriptor.Generation <= uint64(^uint32(0)) {
					exactPackedFileIDs[uint32(descriptor.Generation)] = struct{}{}
				}
			case rootpublication.ResourceOuterLeafManifest:
				if resources == additional {
					hasReplacementManifest = true
				}
			}
		}
	}
	freshOuterLeafReferences, reuseOuterLeafBase, err := db.planOuterLeafBaseDependencyReuseV1(base, additional, next, delta)
	if err != nil {
		return nil, fmt.Errorf("plan outer-leaf candidate dependencies: %w", err)
	}
	excludedInheritedKinds := []rootpublication.ResourceKind{
		rootpublication.ResourceValueLog,
	}
	// A non-nil delta without outer-leaf evidence only changed pager-backed
	// roots. Raw outer-leaf dependencies are therefore unchanged and must remain
	// inherited; the logical value-pointer projection cannot rediscover them.
	if delta == nil || delta.outerLeafDependencyReuse {
		excludedInheritedKinds = append(excludedInheritedKinds, rootpublication.ResourceOuterLeafLog)
	}
	if hasReplacementManifest {
		excludedInheritedKinds = append(excludedInheritedKinds, rootpublication.ResourceOuterLeafManifest)
	}
	if timing != nil {
		timing.FinalizeCandidateResourceWork.Add(requirementWork)
	}
	appendRequirementsCertified := false
	if requirementsFallback != nil {
		fallback := requirementsFallback
		materializeRequirements := func() error {
			if fallback == nil {
				return errors.New("durable-root exact requirements fallback invoked more than once")
			}
			exact, fallbackWork, fallbackErr := fallback()
			fallback = nil
			fallbackWork.FinalRequirementProofFallbacks++
			if timing != nil {
				timing.FinalizeCandidateResourceWork.Add(fallbackWork)
			}
			if fallbackErr != nil {
				return fmt.Errorf("materialize durable-root exact requirements fallback: %w", fallbackErr)
			}
			mergedRequirements, mergeErr := rootpublication.MergeStableLogicalObligationRequirements(requirements, exact)
			if mergeErr != nil {
				return fmt.Errorf("merge durable-root exact requirements fallback: %w", mergeErr)
			}
			mergedMutation, mergeErr := rootpublication.MergeStableLogicalObligationMutations(mutation, appendMutation)
			if mergeErr != nil {
				return fmt.Errorf("merge durable-root append mutation fallback: %w", mergeErr)
			}
			requirements = mergedRequirements
			mutation = mergedMutation
			return nil
		}
		mixedGenericRegistration := len(requirements.ScopedFields) != 0 || len(requirements.Obligations) != 0 || len(mutation.ScopedFields) != 0 || len(mutation.Added) != 0 || len(mutation.Removed) != 0
		if mixedGenericRegistration || len(appendMutation.ScopedFields) == 0 || len(appendMutation.Removed) != 0 {
			if err := materializeRequirements(); err != nil {
				return nil, err
			}
		} else {
			proofWork, certified, proofErr := rootpublication.CertifyStableLogicalObligationAppendMutation(
				base, additional, appendMutation, excludedInheritedKinds...,
			)
			if timing != nil {
				timing.FinalizeCandidateResourceWork.Add(proofWork)
			}
			if proofErr != nil {
				return nil, fmt.Errorf("certify durable-root append requirements: %w", proofErr)
			}
			if certified {
				mutation = appendMutation
				appendRequirementsCertified = true
				if timing != nil {
					timing.FinalizeCandidateResourceWork.FinalRequirementProofFastPath++
				}
			} else if err := materializeRequirements(); err != nil {
				return nil, err
			}
		}
	} else if len(appendMutation.ScopedFields) != 0 || len(appendMutation.Added) != 0 || len(appendMutation.Removed) != 0 {
		return nil, errors.New("durable-root append mutation missing exact requirements fallback")
	}
	var inherited *rootpublication.StableResourceSet
	inheritedStart := time.Now()
	var inheritedWork rootpublication.StableResourceClosureWork
	// Mutation handling is fail-closed: uncertified evidence uses the full
	// inherited filter, destructive mutations require full closure validation,
	// and append-only merge declines fall back to ordinary merge plus validation.
	// The no-addition fast path is counted below; successful append-only merges
	// with additions are counted by MergeAppendOnlyLogicalObligations.
	hasMutationEvidence := len(mutation.ScopedFields) != 0
	mutationCertified := false
	if hasMutationEvidence && !appendRequirementsCertified {
		if err := rootpublication.ValidateStableLogicalObligationMutationFinalRequirements(mutation, requirements); err != nil {
			return nil, fmt.Errorf("validate durable-root logical mutation evidence: %w", err)
		}
		mutationCertified, err = rootpublication.CertifyStableLogicalObligationMutationFinalRequirements(
			base, mutation, requirements, excludedInheritedKinds...,
		)
		if err != nil {
			return nil, fmt.Errorf("certify durable-root logical mutation completeness: %w", err)
		}
	}
	if appendRequirementsCertified {
		mutationCertified = true
	}
	appendOnlyMutation := mutationCertified && len(mutation.Removed) == 0
	if mutationCertified {
		inherited, inheritedWork, err = rootpublication.CloneStableResourceSetApplyingLogicalObligationMutation(base, mutation, excludedInheritedKinds...)
	} else {
		inherited, inheritedWork, err = rootpublication.CloneStableResourceSetForLogicalObligationsWithWork(base, requirements, excludedInheritedKinds...)
		if hasMutationEvidence && len(mutation.Removed) == 0 {
			inheritedWork.AppendOnlyFallbacks++
		}
	}
	if timing != nil {
		timing.FinalizeCandidateInheritedFilter += time.Since(inheritedStart)
		timing.FinalizeCandidateResourceWork.Add(inheritedWork)
	}
	if err != nil {
		return nil, fmt.Errorf("clone base durable-root resources: %w", err)
	}
	if err := merge(inherited); err != nil {
		return nil, fmt.Errorf("merge base durable-root resources: %w", err)
	}
	var fresh *rootpublication.StableResourceSet
	freshStart := time.Now()
	if reuseOuterLeafBase {
		for fileID := range exactPackedFileIDs {
			delete(freshOuterLeafReferences, fileID)
		}
		fresh, err = db.captureRegisteredDurableValueLogResourcesV1(freshOuterLeafReferences)
	} else {
		fresh, err = db.captureDurableValueLogResourcesV1(idx, next, delta, exactPackedFileIDs, valueLogPublicationLocked)
	}
	if err != nil {
		return nil, err
	}
	if timing != nil {
		timing.FinalizeCandidateFreshCapture += time.Since(freshStart)
	}
	if err := merge(fresh); err != nil {
		return nil, fmt.Errorf("merge candidate value-log resources: %w", err)
	}
	appendOnlyCertified := appendOnlyMutation && len(mutation.Added) == 0 && additional == nil
	closureStart := time.Now()
	if appendOnlyMutation && additional != nil {
		appendWork, appendErr := builder.MergeAppendOnlyLogicalObligations(additional, mutation)
		if timing != nil {
			timing.FinalizeCandidateResourceWork.Add(appendWork)
		}
		if appendErr == nil {
			appendOnlyCertified = true
		} else if appendRequirementsCertified {
			return nil, fmt.Errorf("merge certified producer durable-root resources: %w", appendErr)
		} else if err := merge(additional); err != nil {
			return nil, fmt.Errorf("merge producer durable-root resources after append-only decline (%v): %w", appendErr, err)
		} else if timing != nil {
			timing.FinalizeCandidateResourceWork.AppendOnlyFallbacks++
		}
	} else if err := merge(additional); err != nil {
		return nil, fmt.Errorf("merge producer durable-root resources: %w", err)
	}
	resources, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	if timing != nil {
		timing.FinalizeCandidateResourceWork.Add(builder.ClosureWorkSnapshot())
	}
	if !appendOnlyCertified {
		validationWork, validationErr := rootpublication.ValidateStableResourceSetLogicalObligationsWithWork(resources, requirements)
		if timing != nil {
			timing.FinalizeCandidateResourceWork.Add(validationWork)
			if len(mutation.Removed) != 0 {
				timing.FinalizeCandidateResourceWork.DestructiveFallbacks++
			}
		}
		if validationErr != nil {
			resources.Release()
			return nil, validationErr
		}
	} else if timing != nil && len(mutation.Added) == 0 {
		timing.FinalizeCandidateResourceWork.AppendOnlyFastPath++
	}
	if timing != nil {
		timing.FinalizeCandidateClosureAssemble += time.Since(closureStart)
		timing.FinalizeCandidateResourceWork.FreezeOperations++
	}
	abandon = false
	return resources, nil
}

func (db *DB) captureRegisteredDurableValueLogResourcesV1(references map[uint32]struct{}) (*rootpublication.StableResourceSet, error) {
	if db == nil || db.valueLogManager == nil || len(references) == 0 {
		return nil, nil
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	defer func() { _ = db.valueLogManager.Release(set) }()
	fileIDs := make([]uint32, 0, len(references))
	for fileID := range references {
		fileIDs = append(fileIDs, fileID)
	}
	sort.Slice(fileIDs, func(i, j int) bool { return fileIDs[i] < fileIDs[j] })
	required := make([]rootpublication.ReachabilityField, 0, 2)
	var requiresValueLog, requiresOuterLeafRaw bool
	for _, fileID := range fileIDs {
		lane, _ := valuelog.DecodeFileID(fileID)
		if lane == valuelog.ReservedLeafLogLaneID {
			requiresOuterLeafRaw = true
		} else {
			requiresValueLog = true
		}
	}
	if requiresValueLog {
		required = append(required, rootpublication.ReachabilityValueLogPointer)
	}
	if requiresOuterLeafRaw {
		required = append(required, rootpublication.ReachabilityOuterLeafRawPointer)
	}
	builder := rootpublication.NewStableResourceSetBuilder(required...)
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
		kind := rootpublication.ResourceValueLog
		logicalLane := "db/value-log"
		reachability := rootpublication.ReachabilityValueLogPointer
		lane, _ := valuelog.DecodeFileID(fileID)
		if lane == valuelog.ReservedLeafLogLaneID {
			kind = rootpublication.ResourceOuterLeafLog
			logicalLane = "db/outer-leaf-raw"
			reachability = rootpublication.ReachabilityOuterLeafRawPointer
		}
		token, err := db.valueLogManager.StableResourceToken(fileID, valuelog.StableResourceRegistration{
			Kind: kind, LogicalLane: logicalLane,
			Generation: uint64(fileID), DiagnosticPath: diagnosticPath,
			Reachability: reachability,
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

// captureRebuiltIndexDurableResourcesV1 scans a replacement index before its
// namespace is made live. The replacement pager supplies the exact new roots;
// the current manager set supplies the immutable external handles those roots
// reference. This is maintenance-only and deliberately does not publish a
// snapshot or mutate the live DB generation.
func (db *DB) captureRebuiltIndexDurableResourcesV1(p *pager.Pager, meta page.MetaPageBody) (*rootpublication.StableResourceSet, error) {
	var inherited *rootpublication.StableResourceSet
	if db != nil {
		inherited = db.durableRoot.slotResources[db.durableRoot.slot]
	}
	return db.captureRebuiltIndexDurableResourcesFromV1(p, meta, inherited)
}

func (db *DB) captureRebuiltIndexDurableResourcesFromV1(p *pager.Pager, meta page.MetaPageBody, source *rootpublication.StableResourceSet) (*rootpublication.StableResourceSet, error) {
	resources, _, err := db.captureRebuiltIndexDurableResourcesWithWorkV1(p, meta, source)
	return resources, err
}

// rebuiltDurableResourceWorkV1 describes one completed rebuilt-index resource
// capture.  It deliberately counts external segment identities, not outer
// leaves: the shared leaf-reference scanner exposes segment IDs but not a
// stable outer-leaf record identity.
type rebuiltDurableResourceWorkV1 struct {
	ExactCandidateScan            bool
	Projected                     bool
	ProjectionFallbackReason      string
	ReusedNonValueLogDescriptors  uint64
	UniqueScannedExternalSegments uint64
}

const (
	rebuiltDurableResourceFallbackMissingSource  = "missing-exact-source"
	rebuiltDurableResourceFallbackPolicy         = "unsupported-source-policy"
	rebuiltDurableResourceFallbackIdentity       = "unresolved-index-identity"
	rebuiltDurableResourceFallbackOuterLeafDelta = "replayed-outer-leaf-delta"
)

// projectRebuiltOlderRootDurableResourcesV1 retains the exact external
// closure of an unchanged recovery-selectable root. Index pages, durable meta,
// and dependency-manifest pages are rebuilt and synced by the adjacent
// replacement-index transaction, so an inherited index token is deliberately
// excluded here.
func projectRebuiltOlderRootDurableResourcesV1(source *rootpublication.StableResourceSet) (*rootpublication.StableResourceSet, bool, error) {
	if source == nil {
		return nil, true, nil
	}
	manifest, _, err := source.DependencyManifestV1()
	if err != nil {
		return nil, false, nil
	}
	for _, entry := range manifest.Entries() {
		for _, field := range entry.Reachability {
			policy, ok := rootpublication.StableResourcePolicyFor(field)
			if !ok || !policy.Registerable || policy.Kind != entry.Kind {
				return nil, false, nil
			}
		}
	}
	projected, err := rootpublication.CloneStableResourceSetExcludingKinds(source, rootpublication.ResourceIndex)
	if err != nil {
		return nil, false, err
	}
	return projected, true, nil
}

func rebuiltOlderRootIndexAuthorityV1(source *rootpublication.StableResourceSet, identity rootpublication.StableIdentity, generation uint64) bool {
	for _, descriptor := range source.Descriptors() {
		if descriptor.Kind() != rootpublication.ResourceIndex {
			continue
		}
		if _, ok := descriptor.Namespace(); !ok || descriptor.Generation() != generation || !rootpublication.SamePhysicalIdentity(descriptor.Identity(), identity) {
			return false
		}
	}
	return true
}

func (db *DB) captureRebuiltIndexDurableResourcesProjectedWithFallbackV1(
	source *rootpublication.StableResourceSet,
	sourceExact bool,
	projectionBlockedReason string,
	sourceIndex *indexGen,
	sourceIndexID uint64,
	sourceIndexIdentity rootpublication.StableIdentity,
	rebuiltPager *pager.Pager,
	meta page.MetaPageBody,
) (*rootpublication.StableResourceSet, rebuiltDurableResourceWorkV1, error) {
	var work rebuiltDurableResourceWorkV1
	if projectionBlockedReason != "" {
		work.ProjectionFallbackReason = projectionBlockedReason
	} else if sourceExact && sourceIndex != nil && sourceIndex.pager != nil {
		var oldIdentity, newIdentity rootpublication.StableIdentity
		oldIdentityErr := sourceIndex.pager.WithStableResourceFile(func(file *os.File) error {
			var identityErr error
			oldIdentity, identityErr = rootpublication.StableIdentityFromFile(file)
			return identityErr
		})
		newIdentityErr := rebuiltPager.WithStableResourceFile(func(file *os.File) error {
			var identityErr error
			newIdentity, identityErr = rootpublication.StableIdentityFromFile(file)
			return identityErr
		})
		switch {
		case oldIdentityErr != nil || newIdentityErr != nil:
			work.ProjectionFallbackReason = rebuiltDurableResourceFallbackIdentity
		case rootpublication.SamePhysicalIdentity(oldIdentity, newIdentity):
			return nil, work, fmt.Errorf("vacuum: rebuilt index aliases source index: %w", rootpublication.ErrResourceConflict)
		case sourceIndexID != sourceIndex.id || !rootpublication.SamePhysicalIdentity(sourceIndexIdentity, oldIdentity):
			work.ProjectionFallbackReason = rebuiltDurableResourceFallbackIdentity
		case !rebuiltOlderRootIndexAuthorityV1(source, oldIdentity, sourceIndex.id):
			work.ProjectionFallbackReason = rebuiltDurableResourceFallbackIdentity
		default:
			projected, supported, err := projectRebuiltOlderRootDurableResourcesV1(source)
			if err != nil {
				return nil, work, err
			}
			if supported {
				work.Projected = true
				return projected, work, nil
			}
			work.ProjectionFallbackReason = rebuiltDurableResourceFallbackPolicy
		}
	} else {
		work.ProjectionFallbackReason = rebuiltDurableResourceFallbackMissingSource
	}
	fallbackReason := work.ProjectionFallbackReason
	resources, exactWork, err := db.captureRebuiltIndexDurableResourcesWithWorkV1(rebuiltPager, meta, source)
	if fallbackReason == "" {
		fallbackReason = rebuiltDurableResourceFallbackPolicy
	}
	exactWork.ProjectionFallbackReason = fallbackReason
	return resources, exactWork, err
}

func (db *DB) captureRebuiltIndexDurableResourcesWithWorkV1(p *pager.Pager, meta page.MetaPageBody, source *rootpublication.StableResourceSet) (*rootpublication.StableResourceSet, rebuiltDurableResourceWorkV1, error) {
	var work rebuiltDurableResourceWorkV1
	if db == nil || db.valueLogManager == nil || p == nil || meta.UserRootPageID < 2 || meta.SystemRootPageID < 2 {
		return nil, work, fmt.Errorf("%w: rebuilt index dependency scanner unavailable", rootpublication.ErrUnresolvedResource)
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	state := DBState{
		CommitSeq: meta.CommitSeq, RootPageID: meta.UserRootPageID, SystemRootPageID: meta.SystemRootPageID,
		AppliedCommandLSN: meta.AppliedCommandLSN, MaxEntryRevision: page.EntryRevision(meta.MaxEntryRevision),
		ValueLogSet: set, LeafGenerations: db.currentLeafGenerationView(),
		LeafGenerationStateVersion: db.leafGenerationStateVersion,
	}
	snapshot := &Snapshot{
		db: db, idx: &indexGen{pager: p}, state: &state,
		vlogManager: db.valueLogManager, vlogPinned: true, reader: newValueReader(set),
		registryShardHint: snapshotShardHintUnset,
	}
	references, scanErr := db.scanCandidateExternalReferencesV1(snapshot)
	closeErr := snapshot.Close()
	if scanErr != nil || closeErr != nil {
		return nil, work, errors.Join(scanErr, closeErr)
	}
	work.ExactCandidateScan = true
	work.UniqueScannedExternalSegments = uint64(len(references))
	exactPackedFileIDs := make(map[uint32]struct{})
	for _, descriptor := range source.Descriptors() {
		if descriptor.Kind() == rootpublication.ResourceOuterLeafPack && descriptor.Generation() <= uint64(^uint32(0)) {
			exactPackedFileIDs[uint32(descriptor.Generation())] = struct{}{}
		}
	}
	for fileID := range exactPackedFileIDs {
		delete(references, fileID)
	}
	inherited, err := rootpublication.CloneStableResourceSetExcludingKinds(
		source,
		rootpublication.ResourceValueLog,
		rootpublication.ResourceOuterLeafLog,
	)
	if err != nil {
		return nil, work, fmt.Errorf("clone rebuilt-index durable resources: %w", err)
	}
	work.ReusedNonValueLogDescriptors = uint64(inherited.Len())
	fresh, err := db.captureRegisteredDurableValueLogResourcesV1(references)
	if err != nil {
		inherited.Release()
		return nil, work, err
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	defer builder.Abandon()
	closures := []struct {
		label     string
		resources *rootpublication.StableResourceSet
	}{
		{label: "inherited", resources: inherited},
		{label: "fresh", resources: fresh},
	}
	for _, closure := range closures {
		resources := closure.resources
		if resources == nil {
			continue
		}
		if err := builder.Merge(resources); err != nil {
			resources.Release()
			return nil, work, fmt.Errorf("merge %s rebuilt-index durable resources: %w", closure.label, err)
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		return nil, work, err
	}
	if resources.Len() == 0 {
		resources.Release()
		return nil, work, nil
	}
	return resources, work, nil
}

// prepareDurableRootCandidateV1 validates and freezes one exact publication
// candidate without mutating either meta slot. COW pages are encoded into the
// allocator-owned memory store here but are not copied into the index until
// after external dependencies cross their flush and sync frontiers.
func (db *DB) prepareDurableRootCandidateV1(idx *indexGen, next page.MetaPageBody, retired []uint64, resources *rootpublication.StableResourceSet, closeTeardownPinned bool) (candidate *durableRootPublishCandidateV1, err error) {
	current := db.durableRoot
	if current.meta.CommitSeq == 0 || current.record.CommitSeq != current.meta.CommitSeq || next.CommitSeq != current.meta.CommitSeq+1 {
		return nil, fmt.Errorf("%w: durable=%d candidate=%d", errDurableRootCandidateStale, current.meta.CommitSeq, next.CommitSeq)
	}
	if next.UserRootPageID < 2 || next.SystemRootPageID < 2 {
		return nil, errors.New("durable-root candidate has invalid roots")
	}
	capability, err := db.durableRootReuseCapabilityV1(current)
	if err != nil {
		return nil, err
	}
	releaseResources := true
	defer func() {
		if releaseResources {
			resources.Release()
		}
	}()
	manifest, err := db.durableManifestFromResourcesV1WithStats(resources)
	if err != nil {
		return nil, err
	}
	// Capture the exact index identity before freezing the COW allocator. This
	// acquisition is deliberately maintenance-lock-free: online vacuum and the
	// capture use an atomic exclusion handshake, so candidate preparation never
	// waits for maintenance while it owns allocator reservations.
	stableSnapshot := db.acquireDurableCandidateStableIndexSnapshotV1(idx, closeTeardownPinned)
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
	releaseToken := true
	defer func() {
		if releaseToken {
			token.Release()
		}
	}()
	target := uint64(MetaPage0ID)
	if current.slot == MetaPage0ID {
		target = MetaPage1ID
	}
	overwrittenPages, err := durableRootSlotAuxiliaryPagesV1(current.slotMeta[target], current.slotRecord[target])
	if err != nil {
		return nil, err
	}
	auxiliaryCount := int(manifest.PageCount()) + 1
	prepared, err := idx.allocator.PrepareCOWCandidateRetiringV1(
		current.record.Freelist.GenerationID+1,
		next.CommitSeq,
		durableRootCandidateIDV1(current, next),
		capability,
		[]freelist.COWRetirementV1{
			{PageIDs: overwrittenPages, LastReachableCommitSeq: current.slotCommit[target]},
			{PageIDs: retired, LastReachableCommitSeq: current.record.CommitSeq},
		},
		auxiliaryCount,
		freelist.NewCandidatePageSinkV1(),
	)
	if err != nil {
		return nil, fmt.Errorf("prepare COW generation: %w", err)
	}
	preparedOwned := true
	defer func() {
		if !preparedOwned {
			return
		}
		if abortErr := idx.allocator.AbortCOWCandidateV1(prepared); abortErr != nil {
			cause := errors.Join(err, fmt.Errorf("abort incomplete COW generation: %w", abortErr), ErrRecoveryRequired)
			db.publicationPoisoned.Store(true)
			if failErr := idx.allocator.FailCOWCandidateV1(prepared, cause); failErr != nil {
				cause = errors.Join(cause, fmt.Errorf("fail incomplete COW generation: %w", failErr))
			}
			err = cause
		}
	}()
	if db.testFailDurableRootAfterCOWPrepare.Load() {
		return nil, errTestDurableRootAfterCOWPrepareFailpoint
	}
	generation := prepared.Candidate().Generation()
	auxiliary := prepared.AuxiliaryPageIDs()
	if generation == nil || len(auxiliary) != auxiliaryCount {
		return nil, errors.New("incomplete durable-root COW candidate")
	}

	next.TotalPages = generation.HighWater()
	next.FreelistHeadID = 0
	recordPageID := auxiliary[len(auxiliary)-1]
	if current.record.DurableSeq == ^uint64(0) {
		return nil, errors.New("durable root publication sequence overflow")
	}
	durableSeq := current.record.DurableSeq + 1
	if durableSeq > next.CommitSeq {
		return nil, errors.New("durable root publication sequence exceeds commit frontier")
	}
	manifestRef, err := manifest.Materialize(auxiliary[0], freelist.NewMemoryPageStoreV1())
	if err != nil {
		return nil, err
	}
	record := rootpublication.DurableRootRecordV1{
		CommitSeq: next.CommitSeq, DurableSeq: durableSeq,
		UserRootPageID: next.UserRootPageID, SystemRootPageID: next.SystemRootPageID,
		TotalPages: next.TotalPages, MaxEntryRevision: next.MaxEntryRevision,
		AppliedCommandLSN: next.AppliedCommandLSN, LastCommitHeight: next.LastCommitHeight,
		Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(),
		Manifest:           manifestRef,
		ParentRecordPageID: current.meta.RootRecordPageID, ParentCommitSeq: current.meta.CommitSeq,
		ParentRecordDigest:   current.meta.RootRecordDigest,
		MetaProjectionDigest: page.DurableMetaProjectionDigestV1(next.CommitSeq, durableSeq, recordPageID),
	}
	_, recordDigest, err := record.EncodePage(recordPageID)
	if err != nil {
		return nil, err
	}
	meta, err := page.NewDurableMetaV1(next.CommitSeq, durableSeq, recordPageID, recordDigest)
	if err != nil {
		return nil, err
	}
	candidate = &durableRootPublishCandidateV1{
		idx: idx, base: current, next: next, resources: resources, manifest: manifest,
		prepared: prepared, capability: capability, token: token, record: record, meta: meta, target: target,
	}
	preparedOwned = false
	releaseResources = false
	releaseToken = false
	return candidate, nil
}

// acquireDurableCandidateStableIndexSnapshotV1 pins exactly idx without taking
// maintenanceMu. vacuumCutoverInProgress is raised while durablePublishMu is
// held and before vacuum observes the stable capture count. Increment-then-check
// therefore makes either the capture or the replacement back out, with no
// unsafe window between them. The broader vacuumInProgress flag deliberately
// does not block durable publication while online vacuum builds its replacement.
func (db *DB) acquireDurableCandidateStableIndexSnapshotV1(idx *indexGen, closeTeardownPinned bool) *Snapshot {
	if db == nil || idx == nil || (db.closing.Load() && !closeTeardownPinned) {
		return nil
	}
	// Durable publication already owns rootReuseMu exclusively while capturing
	// this maintenance-only index lease, so it must not enter AcquireSnapshot's
	// reader-admission path. The lease never reads a logical tree; the stable
	// capture counter and generation identity are the required vacuum exclusion.
	if db.idx.Load() != idx {
		return nil
	}
	snapshot := db.snapPool.Get()
	snapshot.db = db
	snapshot.idx = idx
	snapshot.closed.Store(false)
	snapshot.readState.Store(0)
	db.durableCandidateIndexCaptures.Add(1)
	snapshot.stableIndexCapture = true
	snapshot.stableIndexCaptureCounter = &db.durableCandidateIndexCaptures
	if db.vacuumCutoverInProgress.Load() || db.idx.Load() != idx || (db.closing.Load() && !closeTeardownPinned) {
		_ = snapshot.Close()
		return nil
	}
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
	if err := candidate.prepared.Candidate().WritePagesToV1(durablePagerSinkV1{pager: candidate.idx.pager}); err != nil {
		return fmt.Errorf("write durable-root COW pages: %w", err)
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
	recordOffset := int64(recordPageID) * int64(page.PageSize)
	indexPath := filepath.Join(db.dir, indexFileName)
	if err := durabilitycut.EmitRange(durabilitycut.BeforePublicationSealWrite, durabilitycut.ResourceSeal, db.dir, indexPath, recordOffset, int64(page.PageSize)); err != nil {
		return err
	}
	if err := candidate.idx.pager.Write(recordPageID, recordImage); err != nil {
		return fmt.Errorf("write durable root record: %w", err)
	}
	if err := durabilitycut.EmitRange(durabilitycut.AfterPublicationSealWrite, durabilitycut.ResourceSeal, db.dir, indexPath, recordOffset, int64(page.PageSize)); err != nil {
		return err
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

func failDurableRootAllocatorWaitersV1(candidate *durableRootPublishCandidateV1) error {
	if candidate == nil || candidate.idx == nil || candidate.idx.allocator == nil || candidate.prepared == nil {
		return nil
	}
	cause := fmt.Errorf("%w: prepared durable-root publication cannot progress on this handle", ErrRecoveryRequired)
	if err := candidate.idx.allocator.FailCOWCandidateV1(candidate.prepared, cause); err != nil {
		return fmt.Errorf("fail prepared COW allocator waiters: %w", err)
	}
	return nil
}

func (db *DB) poisonDurableRootCandidateV1(candidate *durableRootPublishCandidateV1, err error) (page.MetaPageBody, error) {
	current := candidate.base
	current.pending = nil
	current.ambiguous = append(current.ambiguous, candidate)
	db.durableRoot = current
	db.publicationPoisoned.Store(true)
	if wakeErr := failDurableRootAllocatorWaitersV1(candidate); wakeErr != nil {
		err = errors.Join(err, wakeErr)
	}
	return page.MetaPageBody{}, wrapFinalizeCommitError(errors.Join(err, ErrRecoveryRequired), false)
}

type durableRootStorageTransactionV1 struct {
	resources       *rootpublication.StableResourceSet
	materialize     func() error
	syncIndex       func() error
	sink            freelist.AppendPageSink
	target          uint64
	meta            page.DurableMetaV1
	syncMeta        func() error
	dir             string
	indexPath       string
	beforeMetaWrite func() error
	beforeMetaSync  func() error
}

type stableResourceDependencySyncPhaseV1 uint8

const (
	stableResourceDependencyObserveV1 stableResourceDependencySyncPhaseV1 = iota + 1
	stableResourceDependencyEmitBeforeV1
	stableResourceDependencySyncV1
	stableResourceDependencyEmitAfterV1
)

// executeDurableRootStorageTransactionV1 is the only V1 durable-meta mutation
// implementation. Live commits, initialization, and replacement-index
// maintenance all pass through the same dependency -> index -> meta ordering.
// metaMutated becomes true immediately before the one meta write, allowing a
// live caller to distinguish retryable pre-meta failures from ambiguous ones.
func executeDurableRootStorageTransactionV1(tx durableRootStorageTransactionV1) (metaMutated bool, err error) {
	if tx.sink == nil || tx.target > 1 || tx.syncIndex == nil || tx.syncMeta == nil {
		return false, errors.New("invalid durable-root storage transaction")
	}
	if tx.resources != nil {
		if err := tx.resources.FlushThrough(); err != nil {
			return false, fmt.Errorf("flush durable-root external dependencies: %w", err)
		}
		phase, err := syncStableResourceDependenciesV1(tx.resources, tx.dir, tx.resources.SyncThrough, nil)
		if err != nil {
			switch phase {
			case stableResourceDependencyObserveV1:
				return false, fmt.Errorf("observe durable-root external dependencies: %w", err)
			case stableResourceDependencySyncV1:
				return false, fmt.Errorf("sync durable-root external dependencies: %w", err)
			default:
				return false, err
			}
		}
	}
	if tx.materialize != nil {
		if err := tx.materialize(); err != nil {
			return false, err
		}
	}
	if err := tx.syncIndex(); err != nil {
		return false, fmt.Errorf("sync durable-root index: %w", err)
	}

	metaOffset := int64(tx.target) * int64(page.PageSize)
	if tx.dir != "" {
		if err := durabilitycut.EmitRange(durabilitycut.BeforeMetaWrite, durabilitycut.ResourceMeta, tx.dir, tx.indexPath, metaOffset, int64(page.PageSize)); err != nil {
			return false, err
		}
	}
	if tx.beforeMetaWrite != nil {
		if err := tx.beforeMetaWrite(); err != nil {
			return false, err
		}
	}

	metaMutated = true
	if err := writeDurableMetaSlotV1(tx.sink, tx.target, tx.meta); err != nil {
		return true, err
	}
	if tx.dir != "" {
		if err := durabilitycut.EmitRange(durabilitycut.AfterMetaWrite, durabilitycut.ResourceMeta, tx.dir, tx.indexPath, metaOffset, int64(page.PageSize)); err != nil {
			return true, err
		}
		if err := durabilitycut.EmitPath(durabilitycut.BeforeMetaSync, durabilitycut.ResourceMeta, tx.dir, tx.indexPath); err != nil {
			return true, err
		}
	}
	if tx.beforeMetaSync != nil {
		if err := tx.beforeMetaSync(); err != nil {
			return true, err
		}
	}
	if err := tx.syncMeta(); err != nil {
		return true, err
	}
	if tx.dir != "" {
		if err := durabilitycut.EmitPath(durabilitycut.AfterMetaSync, durabilitycut.ResourceMeta, tx.dir, tx.indexPath); err != nil {
			return true, err
		}
	}
	return true, nil
}

// stableResourceDependencyObservationPathsV1 exposes only the names attached to
// the exact handles already retained by the resource set. It never reopens a
// diagnostic path. The list is collected only while the deterministic cut
// observer is active, so the production-disabled path remains allocation-free.
func stableResourceDependencyObservationPathsV1(resources *rootpublication.StableResourceSet, root string) ([]string, error) {
	if resources == nil || root == "" || !durabilitycut.Enabled() {
		return nil, nil
	}
	paths := make([]string, 0, resources.Len())
	for _, token := range resources.Tokens() {
		if token == nil || token.Kind() == rootpublication.ResourceIndex {
			continue
		}
		var path string
		if err := token.WithPinnedFile(func(file *os.File) error {
			path = stableResourceDependencyObservationPathV1(file.Name())
			if path == "" {
				return errors.New("stable resource handle has no observation name")
			}
			if !filepath.IsAbs(path) {
				path = filepath.Join(root, path)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	unique := paths[:0]
	for _, path := range paths {
		if len(unique) == 0 || unique[len(unique)-1] != path {
			unique = append(unique, path)
		}
	}
	return unique, nil
}

// syncStableResourceDependenciesV1 keeps stable-resource cut observation and
// the corresponding sync operation in one sequence. onError lets callers
// retain retry debt before any error is returned.
func syncStableResourceDependenciesV1(
	resources *rootpublication.StableResourceSet,
	root string,
	sync func() error,
	onError func(),
) (stableResourceDependencySyncPhaseV1, error) {
	fail := func(phase stableResourceDependencySyncPhaseV1, err error) (stableResourceDependencySyncPhaseV1, error) {
		if onError != nil {
			onError()
		}
		return phase, err
	}
	dependencyPaths, err := stableResourceDependencyObservationPathsV1(resources, root)
	if err != nil {
		return fail(stableResourceDependencyObserveV1, err)
	}
	if len(dependencyPaths) != 0 {
		if err := durabilitycut.Emit(durabilitycut.Event{
			Point: durabilitycut.BeforeDependencyFileSync, Resource: durabilitycut.ResourceAuxiliary,
			Root: root, Paths: dependencyPaths,
		}); err != nil {
			return fail(stableResourceDependencyEmitBeforeV1, err)
		}
	}
	if err := sync(); err != nil {
		return fail(stableResourceDependencySyncV1, err)
	}
	if len(dependencyPaths) != 0 {
		if err := durabilitycut.Emit(durabilitycut.Event{
			Point: durabilitycut.AfterDependencyFileSync, Resource: durabilitycut.ResourceAuxiliary,
			Root: root, Paths: dependencyPaths,
		}); err != nil {
			return fail(stableResourceDependencyEmitAfterV1, err)
		}
	}
	return 0, nil
}

// stableResourceDependencyObservationPathV1 strips the private handle suffixes
// used by stable-resource pins. A resource may be cloned through several
// candidate closures, so normalize every trailing layer before exposing the
// underlying path to the deterministic power-loss observer.
func stableResourceDependencyObservationPathV1(path string) string {
	for {
		switch {
		case strings.HasSuffix(path, "#stable-sync-pin"):
			path = strings.TrimSuffix(path, "#stable-sync-pin")
		case strings.HasSuffix(path, "#stable-pin"):
			path = strings.TrimSuffix(path, "#stable-pin")
		default:
			return path
		}
	}
}

func (db *DB) executeDurableRootCandidateV1(candidate *durableRootPublishCandidateV1) (page.MetaPageBody, error) {
	if candidate == nil || candidate.released || candidate.metaMutated {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.New("durable-root candidate is not retryable"), true)
	}
	metaPath := filepath.Join(db.dir, indexFileName)
	mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		resources:   candidate.resources,
		materialize: func() error { return db.materializeDurableRootCandidateV1(candidate) },
		syncIndex:   candidate.token.SyncThrough,
		sink:        durablePagerSinkV1{pager: candidate.idx.pager},
		target:      candidate.target,
		meta:        candidate.meta,
		syncMeta: func() error {
			return candidate.token.WithPinnedFile(func(file *os.File) error {
				return candidate.idx.pager.SyncPagesWithStableFile(file, []uint64{candidate.target})
			})
		},
		dir:       db.dir,
		indexPath: metaPath,
		beforeMetaWrite: func() error {
			if db.testFailWriteMeta.Load() {
				return errTestWriteMetaFailpoint
			}
			return nil
		},
		beforeMetaSync: func() error {
			if db.testFailSyncMeta.Load() {
				return errTestSyncMetaFailpoint
			}
			return nil
		},
	})
	candidate.metaMutated = mutated
	if err != nil {
		if mutated {
			return db.poisonDurableRootCandidateV1(candidate, err)
		}
		return db.retainDurableRootRetryV1(candidate, err)
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

func (db *DB) executeDurableRootCandidateWithRetryV1(candidate *durableRootPublishCandidateV1) (page.MetaPageBody, error) {
	var lastErr error
	for attempt := 0; attempt < maxDurableRootPreMetaRetriesV1; attempt++ {
		next, err := db.executeDurableRootCandidateV1(candidate)
		if err == nil || candidate == nil || candidate.metaMutated || db.publicationPoisoned.Load() {
			return next, err
		}
		lastErr = err
		if attempt+1 < maxDurableRootPreMetaRetriesV1 {
			if attempt < 4 {
				runtime.Gosched()
			} else {
				shift := attempt - 4
				if shift > 6 {
					shift = 6
				}
				time.Sleep(time.Duration(1<<shift) * time.Microsecond)
			}
		}
	}
	// The prior durable slot remains authoritative, but this live allocator has
	// an immutable COW candidate prepared and cannot safely build another root.
	// Retain its exact ownership until Close and fail the handle closed instead
	// of allowing a later allocation to wait forever.
	db.publicationPoisoned.Store(true)
	if wakeErr := failDurableRootAllocatorWaitersV1(candidate); wakeErr != nil {
		lastErr = errors.Join(lastErr, wakeErr)
	}
	return page.MetaPageBody{}, wrapFinalizeCommitError(errors.Join(lastErr, ErrRecoveryRequired), true)
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

// prepareOrResumeDurableRootCandidateV1 runs with durablePublishMu held. A
// pre-meta failure retains the exact candidate, so only an identical logical
// publication may resume it; every competing root fails closed.
func (db *DB) prepareOrResumeDurableRootCandidateV1(idx *indexGen, next page.MetaPageBody, retired []uint64, resources *rootpublication.StableResourceSet, closeTeardownPinned bool) (*durableRootPublishCandidateV1, error) {
	if pending := db.durableRoot.pending; pending != nil {
		if resources != nil {
			resources.Release()
			return nil, errors.New("durable-root retry received replacement dependency resources")
		}
		if !sameDurableRootCandidateV1(pending, idx, next) {
			return nil, freelist.ErrCOWCandidatePrepared
		}
		capability, err := db.durableRootReuseCapabilityV1(pending.base)
		if err != nil {
			return nil, err
		}
		pending.capability = capability
		return pending, nil
	}
	return db.prepareDurableRootCandidateV1(idx, next, retired, resources, closeTeardownPinned)
}

var errDirectDurableRootPublisherDisabledV1 = errors.New("direct durable-root publisher disabled after coordinator activation")

// publishDurableRootV1 is retained only as a tripwire for stale internal call
// sites. Once the root-publication coordinator is active, every root must pass
// through its accepted-candidate handoff and stable publisher.
func (db *DB) publishDurableRootV1(idx *indexGen, next page.MetaPageBody, retired []uint64, vlogRefDelta *valueLogRefDelta) (page.MetaPageBody, error) {
	if db == nil || idx == nil || idx.pager == nil || idx.allocator == nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.New("missing durable-root index"), true)
	}
	if db.rootPublication != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(
			errors.Join(ErrRecoveryRequired, errDirectDurableRootPublisherDisabledV1), true,
		)
	}
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	var resources *rootpublication.StableResourceSet
	var err error
	if db.durableRoot.pending == nil {
		resources, err = db.captureDurableRootResourcesV1(idx, next, vlogRefDelta, nil, rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{}, rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{}, nil, false, nil)
		if err != nil {
			return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("capture durable-root dependencies: %w", err), true)
		}
	}
	db.rootReuseMu.Lock()
	defer db.rootReuseMu.Unlock()
	candidate, err := db.prepareOrResumeDurableRootCandidateV1(idx, next, retired, resources, false)
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
	var candidateID freelist.CandidateIDV1
	binary.LittleEndian.PutUint64(candidateID[:8], 1)
	prepared, err := idx.allocator.PrepareCOWCandidateV1(2, 1, candidateID, capability, 2, freelist.NewCandidatePageSinkV1())
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
	if err := prepared.Candidate().WritePagesToV1(durablePagerSinkV1{pager: p}); err != nil {
		return fmt.Errorf("write initial COW freelist pages: %w", err)
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
	if _, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		syncIndex: p.Sync,
		sink:      sink,
		target:    MetaPage0ID,
		meta:      meta,
		syncMeta: func() error {
			return p.SyncPages([]uint64{MetaPage0ID})
		},
		dir:       db.dir,
		indexPath: filepath.Join(db.dir, indexFileName),
	}); err != nil {
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

// writeRebuiltDurableRootV1 installs the first durable-root generation for an
// already materialized replacement index (offline vacuum/rewrite). The caller
// owns resources; this function only flushes/syncs their exact frontiers before
// the replacement index and its sole valid meta slot become durable.
func writeRebuiltDurableRootV1(dir, indexPath string, p *pager.Pager, meta page.MetaPageBody, resources *rootpublication.StableResourceSet) error {
	if p == nil || meta.CommitSeq == 0 || meta.UserRootPageID < 2 || meta.SystemRootPageID < 2 ||
		meta.UserRootPageID >= p.PageCount() || meta.SystemRootPageID >= p.PageCount() {
		return errors.New("invalid rebuilt durable-root input")
	}
	manifest, err := durableManifestFromResourcesV1(resources)
	if err != nil {
		return err
	}
	base, err := freelist.NewFreelistGenerationV1(1, p.PageCount(), nil, nil)
	if err != nil {
		return err
	}
	allocator := freelist.New(p, 0)
	ledger := freelist.NewReservationLedger()
	if err := allocator.EnableCOWV1(base, ledger); err != nil {
		return err
	}
	capability, err := freelist.NewReuseCapability(meta.CommitSeq, meta.CommitSeq, 0)
	if err != nil {
		return err
	}
	var candidateID freelist.CandidateIDV1
	binary.LittleEndian.PutUint64(candidateID[:8], meta.CommitSeq)
	binary.LittleEndian.PutUint64(candidateID[8:], meta.UserRootPageID^meta.SystemRootPageID)
	prepared, err := allocator.PrepareCOWCandidateV1(2, meta.CommitSeq, candidateID, capability, int(manifest.PageCount())+1, freelist.NewCandidatePageSinkV1())
	if err != nil {
		return err
	}
	generation := prepared.Candidate().Generation()
	auxiliary := prepared.AuxiliaryPageIDs()
	if generation == nil || len(auxiliary) != int(manifest.PageCount())+1 {
		return errors.New("incomplete rebuilt durable-root COW generation")
	}
	if err := p.Truncate(generation.HighWater()); err != nil {
		return err
	}
	if err := prepared.Candidate().WritePagesToV1(durablePagerSinkV1{pager: p}); err != nil {
		return fmt.Errorf("write rebuilt COW pages: %w", err)
	}
	sink := durablePagerSinkV1{pager: p}
	manifestRef, err := manifest.Materialize(auxiliary[0], sink)
	if err != nil {
		return err
	}
	recordPageID := auxiliary[len(auxiliary)-1]
	meta.TotalPages = generation.HighWater()
	meta.FreelistHeadID = 0
	record := rootpublication.DurableRootRecordV1{
		CommitSeq: meta.CommitSeq, DurableSeq: meta.CommitSeq,
		UserRootPageID: meta.UserRootPageID, SystemRootPageID: meta.SystemRootPageID,
		TotalPages: meta.TotalPages, MaxEntryRevision: meta.MaxEntryRevision,
		AppliedCommandLSN: meta.AppliedCommandLSN, LastCommitHeight: meta.LastCommitHeight,
		Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(),
		Manifest: manifestRef, MetaProjectionDigest: page.DurableMetaProjectionDigestV1(meta.CommitSeq, meta.CommitSeq, recordPageID),
	}
	recordImage, recordDigest, err := record.EncodePage(recordPageID)
	if err != nil {
		return err
	}
	if err := p.Write(recordPageID, recordImage); err != nil {
		return err
	}
	durableMeta, err := page.NewDurableMetaV1(meta.CommitSeq, meta.CommitSeq, recordPageID, recordDigest)
	if err != nil {
		return err
	}
	_, err = executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		resources: resources,
		syncIndex: p.Sync,
		sink:      sink,
		target:    MetaPage0ID,
		meta:      durableMeta,
		syncMeta: func() error {
			return p.SyncPages([]uint64{MetaPage0ID})
		},
		dir:       dir,
		indexPath: indexPath,
	})
	return err
}

type rebuiltDurableRootV1 struct {
	meta      page.MetaPageBody
	resources *rootpublication.StableResourceSet
	pages     []uint64
}

func writeRebuiltDurableRootsV1(dir, indexPath string, p *pager.Pager, roots []rebuiltDurableRootV1) error {
	if len(roots) != 2 || roots[0].meta.CommitSeq == 0 || roots[1].meta.CommitSeq <= roots[0].meta.CommitSeq {
		return errors.New("invalid rebuilt durable-root pair")
	}
	if err := writeRebuiltDurableRootV1(dir, indexPath, p, roots[0].meta, roots[0].resources); err != nil {
		return err
	}
	selected, err := selectDurableRootV1(p, p.PageCount(), nil)
	if err != nil {
		return err
	}
	return appendRebuiltDurableRootV1(dir, indexPath, p, selected, roots[1])
}

func appendRebuiltDurableRootV1(dir, indexPath string, p *pager.Pager, current durableRootSelectionV1, next rebuiltDurableRootV1) error {
	meta := next.meta
	if p == nil || current.Record.CommitSeq == 0 || meta.CommitSeq <= current.Record.CommitSeq ||
		meta.UserRootPageID < 2 || meta.SystemRootPageID < 2 ||
		meta.UserRootPageID >= p.PageCount() || meta.SystemRootPageID >= p.PageCount() {
		return errors.New("invalid rebuilt durable-root successor")
	}
	manifest, err := durableManifestFromResourcesV1(next.resources)
	if err != nil {
		return err
	}
	allocator := freelist.New(p, 0)
	if err := allocator.EnableCOWV1(current.Freelist, freelist.NewReservationLedger()); err != nil {
		return err
	}
	capability, err := freelist.NewReuseCapability(current.Record.CommitSeq, current.Record.CommitSeq, 0)
	if err != nil {
		return err
	}
	var candidateID freelist.CandidateIDV1
	binary.LittleEndian.PutUint64(candidateID[:8], meta.CommitSeq)
	binary.LittleEndian.PutUint64(candidateID[8:], meta.UserRootPageID^meta.SystemRootPageID^uint64(MetaPage1ID))
	prepared, err := allocator.PrepareCOWCandidateV1(
		current.Freelist.GenerationID()+1,
		meta.CommitSeq,
		candidateID,
		capability,
		int(manifest.PageCount())+1,
		freelist.NewMemoryPageStoreV1(),
	)
	if err != nil {
		return err
	}
	generation := prepared.Candidate().Generation()
	auxiliary := prepared.AuxiliaryPageIDs()
	if generation == nil || len(auxiliary) != int(manifest.PageCount())+1 {
		return errors.New("incomplete rebuilt durable-root successor COW generation")
	}
	if err := p.Truncate(generation.HighWater()); err != nil {
		return err
	}
	for _, image := range prepared.Candidate().Pages() {
		if err := p.Write(image.PageID, image.Data); err != nil {
			return fmt.Errorf("write rebuilt successor COW page %d: %w", image.PageID, err)
		}
	}
	sink := durablePagerSinkV1{pager: p}
	manifestRef, err := manifest.Materialize(auxiliary[0], sink)
	if err != nil {
		return err
	}
	recordPageID := auxiliary[len(auxiliary)-1]
	meta.TotalPages = generation.HighWater()
	meta.FreelistHeadID = 0
	durableSeq := current.Record.DurableSeq + 1
	if durableSeq > meta.CommitSeq {
		return errors.New("rebuilt durable-root successor sequence exceeds commit frontier")
	}
	record := rootpublication.DurableRootRecordV1{
		CommitSeq: meta.CommitSeq, DurableSeq: durableSeq,
		UserRootPageID: meta.UserRootPageID, SystemRootPageID: meta.SystemRootPageID,
		TotalPages: meta.TotalPages, MaxEntryRevision: meta.MaxEntryRevision,
		AppliedCommandLSN: meta.AppliedCommandLSN, LastCommitHeight: meta.LastCommitHeight,
		Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(),
		Manifest:           manifestRef,
		ParentRecordPageID: current.Meta.RootRecordPageID, ParentCommitSeq: current.Meta.CommitSeq,
		ParentRecordDigest:   current.Meta.RootRecordDigest,
		MetaProjectionDigest: page.DurableMetaProjectionDigestV1(meta.CommitSeq, durableSeq, recordPageID),
	}
	recordImage, recordDigest, err := record.EncodePage(recordPageID)
	if err != nil {
		return err
	}
	if err := p.Write(recordPageID, recordImage); err != nil {
		return err
	}
	durableMeta, err := page.NewDurableMetaV1(meta.CommitSeq, durableSeq, recordPageID, recordDigest)
	if err != nil {
		return err
	}
	_, err = executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		resources: next.resources,
		syncIndex: p.Sync,
		sink:      sink,
		target:    MetaPage1ID,
		meta:      durableMeta,
		syncMeta:  func() error { return p.SyncPages([]uint64{MetaPage1ID}) },
		dir:       dir,
		indexPath: indexPath,
	})
	return err
}

func (db *DB) installDurableRootSelectionV1(selected durableRootSelectionV1) {
	db.durableRoot = durableRootRuntimeFromSelectionV1(selected)
	db.meta = page.MetaPageBody{
		CommitSeq: selected.Record.CommitSeq, UserRootPageID: selected.Record.UserRootPageID,
		SystemRootPageID: selected.Record.SystemRootPageID, TotalPages: selected.Record.TotalPages,
		LastCommitHeight: selected.Record.LastCommitHeight, AppliedCommandLSN: selected.Record.AppliedCommandLSN,
		MaxEntryRevision: selected.Record.MaxEntryRevision,
	}
	db.metaPageID = selected.Slot
}

func durableRootRuntimeFromSelectionV1(selected durableRootSelectionV1) durableRootRuntimeV1 {
	return durableRootRuntimeV1{
		meta: selected.Meta, record: selected.Record, manifest: selected.Manifest,
		slot: selected.Slot, slotCommit: selected.SlotCommits, slotResources: selected.SlotResources,
		slotMeta: selected.SlotMetas, slotRecord: selected.SlotRecords,
	}
}

// registerDurableManifestValueLogSegmentsV1 resolves only exact value-log and
// outer-leaf segment identities named by the bounded durable inventory. The
// physical path is derived from the resource kind and encoded file ID;
// DiagnosticPath is checked against that derivation and is never used as
// discovery authority.
func (db *DB) registerDurableManifestValueLogSegmentsV1(entries []rootpublication.DependencyManifestEntryV1) error {
	if db == nil || db.valueLogManager == nil {
		return fmt.Errorf("%w: value-log manager unavailable", rootpublication.ErrUnresolvedResource)
	}
	for _, entry := range entries {
		var dir string
		switch entry.Kind {
		case rootpublication.ResourceValueLog:
			dir = ValueLogDirPath(db.dir)
		case rootpublication.ResourceOuterLeafLog, rootpublication.ResourceOuterLeafPack:
			dir = LeafLogDirPath(db.dir)
		default:
			continue
		}
		fileID64, err := strconv.ParseUint(entry.ResourceID, 10, 32)
		if err != nil || fileID64 == 0 || entry.Generation != fileID64 {
			return fmt.Errorf("%w: invalid %s resource identity %q/%d", rootpublication.ErrUnresolvedResource, entry.Kind, entry.ResourceID, entry.Generation)
		}
		fileID := uint32(fileID64)
		lane, _ := valuelog.DecodeFileID(fileID)
		outerLeaf := entry.Kind == rootpublication.ResourceOuterLeafLog || entry.Kind == rootpublication.ResourceOuterLeafPack
		if outerLeaf != (lane == valuelog.ReservedLeafLogLaneID) {
			return fmt.Errorf("%w: file %d namespace does not match durable kind %q", rootpublication.ErrResourceConflict, fileID, entry.Kind)
		}
		path := valuelog.SegmentPath(dir, fileID)
		diagnosticPath, err := durableDiagnosticPathV1(db.dir, path)
		if err != nil || diagnosticPath != entry.DiagnosticPath {
			return fmt.Errorf("%w: %s file %d path %q differs from canonical namespace %q", rootpublication.ErrResourceConflict, entry.Kind, fileID, entry.DiagnosticPath, diagnosticPath)
		}
		if err := db.valueLogManager.RegisterSegment(path, fileID); err != nil {
			return fmt.Errorf("register manifest %s file %d: %w", entry.Kind, fileID, err)
		}
	}
	return nil
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
	if err := db.registerDurableManifestValueLogSegmentsV1(entries); err != nil {
		return nil, err
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
		var reachability rootpublication.ReachabilityField
		var logicalLane string
		switch entry.Kind {
		case rootpublication.ResourceValueLog:
			reachability = rootpublication.ReachabilityValueLogPointer
			logicalLane = "db/value-log"
		case rootpublication.ResourceOuterLeafLog:
			reachability = rootpublication.ReachabilityOuterLeafRawPointer
			logicalLane = "db/outer-leaf-raw"
		default:
			if err := db.validateGenericDurableDependencyEntryV1(builder, entry); err != nil {
				return nil, err
			}
			continue
		}
		fileID64, err := strconv.ParseUint(entry.ResourceID, 10, 32)
		if err != nil || fileID64 == 0 || entry.Generation != fileID64 || entry.LogicalLane != logicalLane {
			return nil, fmt.Errorf("%w: invalid %s resource identity %q/%d", rootpublication.ErrUnresolvedResource, entry.Kind, entry.ResourceID, entry.Generation)
		}
		if len(entry.Reachability) != 1 || entry.Reachability[0] != reachability || len(entry.LogicalObligations) != 0 {
			return nil, fmt.Errorf("%w: %s resource %q has invalid reachability or logical obligations", rootpublication.ErrUnresolvedResource, entry.Kind, entry.ResourceID)
		}
		fileID := uint32(fileID64)
		lane, _ := valuelog.DecodeFileID(fileID)
		if (entry.Kind == rootpublication.ResourceOuterLeafLog) != (lane == valuelog.ReservedLeafLogLaneID) {
			return nil, fmt.Errorf("%w: file %d namespace does not match durable kind %q", rootpublication.ErrResourceConflict, fileID, entry.Kind)
		}
		file := set.Files[fileID]
		if file == nil || file.File == nil || file.IsZombie.Load() {
			return nil, fmt.Errorf("%w: %s file %d is missing", rootpublication.ErrUnresolvedResource, entry.Kind, fileID)
		}
		diagnosticPath, err := durableDiagnosticPathV1(db.dir, file.Path)
		if err != nil || diagnosticPath != entry.DiagnosticPath {
			return nil, fmt.Errorf("%w: %s file %d path differs from manifest", rootpublication.ErrResourceConflict, entry.Kind, fileID)
		}
		token, err := db.valueLogManager.StableResourceToken(fileID, valuelog.StableResourceRegistration{
			Kind: entry.Kind, LogicalLane: entry.LogicalLane,
			Generation: entry.Generation, DiagnosticPath: entry.DiagnosticPath,
			Reachability: reachability,
		})
		if err != nil {
			return nil, err
		}
		identity := token.Identity()
		frontier := token.Frontier()
		if identity.Generation != entry.Identity.Generation || !rootpublication.SamePhysicalIdentity(identity, entry.Identity) || token.Digest() != entry.Digest {
			token.Release()
			return nil, fmt.Errorf("%w: %s file %d identity or digest differs from manifest", rootpublication.ErrResourceConflict, entry.Kind, fileID)
		}
		if frontier.Bytes < entry.Frontier.Bytes || frontier.MaxLSN < entry.Frontier.MaxLSN {
			token.Release()
			return nil, fmt.Errorf("%w: %s file %d frontier is below required manifest frontier", rootpublication.ErrFrontierBeyondResource, entry.Kind, fileID)
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

func durableDependencyPathV1(root, relative string) (string, error) {
	if root == "" || relative == "" || filepath.IsAbs(relative) {
		return "", rootpublication.ErrUnresolvedResource
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.ToSlash(clean) != relative {
		return "", rootpublication.ErrUnresolvedResource
	}
	return filepath.Join(root, clean), nil
}

// durableDependencyPathForKindV1 resolves transitive dictionary/template
// dependencies against their sibling side-store roots in the public
// <root>/{maindb,dictdb,templatedb} layout. Other dependencies remain relative
// to the publishing backend directory. A non-empty sideRoot is used by
// snapshot restore while its extracted trees still have staging names.
func durableDependencyPathForKindV1(mainDir, sideRoot string, kind rootpublication.ResourceKind, relative string) (string, error) {
	root := mainDir
	sideName := ""
	switch kind {
	case rootpublication.ResourceDictionary:
		sideName = "dictdb"
	case rootpublication.ResourceTemplate:
		sideName = "templatedb"
	}
	if sideName != "" {
		if sideRoot == "" && filepath.Base(filepath.Clean(mainDir)) == "maindb" {
			sideRoot = filepath.Dir(filepath.Clean(mainDir))
		}
		if sideRoot != "" {
			root = filepath.Join(sideRoot, sideName)
		}
	}
	return durableDependencyPathV1(root, relative)
}

func durableColumnSegmentDigestV1(lane string, fileID uint32) [32]byte {
	namespace := []byte(lane)
	raw := make([]byte, 4+len(namespace)+4)
	binary.LittleEndian.PutUint32(raw[:4], uint32(len(namespace)))
	copy(raw[4:], namespace)
	binary.LittleEndian.PutUint32(raw[4+len(namespace):], fileID)
	return sha256.Sum256(raw)
}

func durableColumnLogicalObligationDigestV1(obligation rootpublication.StableLogicalObligation) [32]byte {
	appendString := func(raw []byte, value string) []byte {
		var size [4]byte
		binary.LittleEndian.PutUint32(size[:], uint32(len(value)))
		raw = append(raw, size[:]...)
		return append(raw, value...)
	}
	appendUint64 := func(raw []byte, value uint64) []byte {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], value)
		return append(raw, encoded[:]...)
	}
	appendUint32 := func(raw []byte, value uint32) []byte {
		var encoded [4]byte
		binary.LittleEndian.PutUint32(encoded[:], value)
		return append(raw, encoded[:]...)
	}
	raw := make([]byte, 0, len(obligation.Class)+len(obligation.Kind)+len(obligation.Namespace)+len(obligation.Reachability)+75)
	raw = appendString(raw, obligation.Class)
	raw = appendString(raw, obligation.Kind)
	raw = appendString(raw, obligation.Namespace)
	raw = appendUint64(raw, obligation.Generation)
	raw = appendUint64(raw, obligation.PartID)
	raw = appendUint32(raw, uint32(obligation.FileID))
	raw = appendUint64(raw, uint64(obligation.Offset))
	raw = appendUint64(raw, uint64(obligation.Length))
	raw = appendUint32(raw, obligation.Checksum)
	raw = appendString(raw, string(obligation.Reachability))
	return sha256.Sum256(raw)
}

func validateDurableDependencyContentV1(file *os.File, entry rootpublication.DependencyManifestEntryV1) error {
	if file == nil || entry.Frontier.Bytes == 0 || entry.Frontier.MaxLSN != 0 || len(entry.Frontier.RIDs()) != 0 {
		return fmt.Errorf("%w: invalid external frontier for %q", rootpublication.ErrUnresolvedResource, entry.DiagnosticPath)
	}
	if entry.Frontier.Bytes > math.MaxInt64 {
		return fmt.Errorf("%w: external frontier exceeds readable range for %q", rootpublication.ErrFrontierBeyondResource, entry.DiagnosticPath)
	}
	switch entry.Kind {
	case rootpublication.ResourceColumnAsset, rootpublication.ResourceTypedColumnAsset, rootpublication.ResourceVectorGraphPack:
		fileID, err := strconv.ParseUint(entry.ResourceID, 10, 32)
		if err != nil || fileID == 0 || entry.Generation != fileID || entry.Digest != durableColumnSegmentDigestV1(entry.LogicalLane, uint32(fileID)) {
			return fmt.Errorf("%w: invalid column resource identity or digest", rootpublication.ErrResourceConflict)
		}
		if len(entry.LogicalObligations) == 0 {
			return fmt.Errorf("%w: column resource has no logical obligations", rootpublication.ErrUnresolvedResource)
		}
		for _, obligation := range entry.LogicalObligations {
			if obligation.Class != "column-asset-ref-v1" || obligation.Namespace != entry.LogicalLane || obligation.FileID != fileID ||
				obligation.Offset < 0 || obligation.Length <= 0 || uint64(obligation.Offset) > entry.Frontier.Bytes ||
				uint64(obligation.Length) > entry.Frontier.Bytes-uint64(obligation.Offset) ||
				obligation.Digest != durableColumnLogicalObligationDigestV1(obligation) {
				return fmt.Errorf("%w: invalid column logical obligation", rootpublication.ErrResourceConflict)
			}
			hash := crc32.NewIEEE()
			if _, err := io.CopyN(hash, io.NewSectionReader(file, obligation.Offset, obligation.Length), obligation.Length); err != nil {
				return fmt.Errorf("read column logical obligation: %w", err)
			}
			if hash.Sum32() != obligation.Checksum {
				return fmt.Errorf("%w: column logical obligation checksum mismatch", rootpublication.ErrResourceConflict)
			}
		}
	case rootpublication.ResourceOuterLeafPack:
		fileID, err := strconv.ParseUint(entry.ResourceID, 10, 32)
		if err != nil || fileID == 0 || entry.Generation != fileID {
			return fmt.Errorf("%w: invalid packed outer-leaf identity", rootpublication.ErrResourceConflict)
		}
		digest, err := stablePackedSegmentDigest(file, uint32(fileID), entry.Frontier.Bytes)
		if err != nil || digest != entry.Digest {
			return fmt.Errorf("%w: packed outer-leaf digest mismatch", rootpublication.ErrResourceConflict)
		}
	case rootpublication.ResourceOuterLeafManifest:
		data, err := io.ReadAll(io.NewSectionReader(file, 0, int64(entry.Frontier.Bytes)))
		if err != nil {
			return fmt.Errorf("read outer-leaf manifest: %w", err)
		}
		if sha256.Sum256(data) != entry.Digest {
			return fmt.Errorf("%w: outer-leaf manifest digest mismatch", rootpublication.ErrResourceConflict)
		}
		manifest, err := decodeLeafGenerationManifest(data, entry.ResourceID)
		if err != nil || manifest.ManifestRevision != entry.Generation {
			return fmt.Errorf("%w: outer-leaf manifest revision differs from durable dependency", rootpublication.ErrResourceConflict)
		}
	case rootpublication.ResourceDictionary, rootpublication.ResourceTemplate:
		// Their producer-canonical logical digests bind definitions while the
		// exact index/value-log identities and frontiers below fence replacement.
		// Traversing nested stores here would violate bounded root recovery.
		if len(entry.LogicalObligations) == 0 {
			return fmt.Errorf("%w: transitive resource has no logical obligations", rootpublication.ErrUnresolvedResource)
		}
	default:
		return fmt.Errorf("%w: unsupported durable dependency kind %q", rootpublication.ErrUnresolvedResource, entry.Kind)
	}
	return nil
}

func (db *DB) validateGenericDurableDependencyEntryV1(builder *rootpublication.StableResourceSetBuilder, entry rootpublication.DependencyManifestEntryV1) error {
	if db == nil || builder == nil {
		return fmt.Errorf("%w: durable dependency validator unavailable", rootpublication.ErrUnresolvedResource)
	}
	if entry.Namespace == nil {
		return fmt.Errorf("%w: durable dependency %q has no namespace proof", rootpublication.ErrUnresolvedResource, entry.DiagnosticPath)
	}
	if entry.Generation == 0 || entry.Identity.Generation != entry.Generation {
		return fmt.Errorf("%w: durable dependency %q generation=%d identity_generation=%d", rootpublication.ErrUnresolvedResource, entry.DiagnosticPath, entry.Generation, entry.Identity.Generation)
	}
	if entry.LogicalLane == "" || entry.ResourceID == "" || len(entry.Reachability) == 0 {
		return fmt.Errorf("%w: durable dependency %q lane=%q resource_id=%q reachability=%d", rootpublication.ErrUnresolvedResource, entry.DiagnosticPath, entry.LogicalLane, entry.ResourceID, len(entry.Reachability))
	}
	path, err := durableDependencyPathForKindV1(db.dir, "", entry.Kind, entry.DiagnosticPath)
	if err != nil {
		return fmt.Errorf("%w: invalid durable dependency path %q", rootpublication.ErrUnresolvedResource, entry.DiagnosticPath)
	}
	name := filepath.Base(path)
	if entry.Namespace.NewName != name || entry.Namespace.ParentIdentity.Generation == 0 {
		return fmt.Errorf("%w: durable dependency namespace differs from path", rootpublication.ErrResourceConflict)
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open durable dependency parent: %w", err)
	}
	defer parent.Close()
	file, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open durable dependency child: %w", err)
	}
	defer file.Close()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil || !rootpublication.SamePhysicalIdentity(identity, entry.Identity) {
		return fmt.Errorf("%w: durable dependency identity differs from manifest", rootpublication.ErrResourceConflict)
	}
	info, err := file.Stat()
	if err != nil || info.Size() < 0 || uint64(info.Size()) < entry.Frontier.Bytes {
		return fmt.Errorf("%w: durable dependency frontier exceeds file", rootpublication.ErrFrontierBeyondResource)
	}
	if err := validateDurableDependencyContentV1(file, entry); err != nil {
		return err
	}
	registry := db.StableResourceIdentityPinRegistry()
	for _, reachability := range entry.Reachability {
		policy, ok := rootpublication.StableResourcePolicyFor(reachability)
		if !ok || !policy.Registerable || policy.Kind != entry.Kind {
			return fmt.Errorf("%w: invalid reachability %q for kind %q", rootpublication.ErrResourceConflict, reachability, entry.Kind)
		}
		obligations := make([]rootpublication.StableLogicalObligation, 0, len(entry.LogicalObligations))
		for _, obligation := range entry.LogicalObligations {
			if obligation.Reachability == reachability {
				obligations = append(obligations, obligation)
			}
		}
		namespace, err := rootpublication.NewRecoveredStableNamespaceToken(rootpublication.StableNamespaceSpec{
			Parent: parent, LinkedResource: file, ParentGeneration: entry.Namespace.ParentIdentity.Generation,
			Operation: entry.Namespace.Operation, OldName: entry.Namespace.OldName, NewName: entry.Namespace.NewName,
			DiagnosticPath: entry.Namespace.DiagnosticPath,
		}, entry.Namespace.ParentIdentity)
		if err != nil {
			return err
		}
		if err := registry.Observe(identity); err != nil {
			namespace.Release()
			return err
		}
		observed := true
		token, err := rootpublication.NewStableProducerResourceToken(rootpublication.StableResourceSpec{
			Kind: entry.Kind, LogicalLane: entry.LogicalLane, ResourceID: entry.ResourceID,
			Generation: entry.Generation, DiagnosticPath: entry.DiagnosticPath, File: file,
			Frontier: entry.Frontier, Digest: entry.Digest, Reachability: reachability,
			Namespace: namespace, LogicalObligations: obligations, ContentSynced: true, PinRegistry: registry,
			OnRelease: func() { _ = registry.Unobserve(identity) },
		}, policy.Classification)
		if err != nil {
			if observed {
				_ = registry.Unobserve(identity)
			}
			namespace.Release()
			return err
		}
		observed = false
		if err := builder.Add(token); err != nil {
			token.Release()
			namespace.Release()
			return err
		}
		namespace.Release()
	}
	return nil
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
