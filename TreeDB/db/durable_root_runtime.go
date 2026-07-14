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

func (sink durablePagerSinkV1) WritePage(pageID uint64, image []byte) error {
	if sink.pager == nil {
		return errors.New("missing durable pager")
	}
	return sink.pager.Write(pageID, image)
}

type durableRootRuntimeV1 struct {
	meta               page.DurableMetaV1
	record             rootpublication.DurableRootRecordV1
	manifest           *rootpublication.DependencyManifestV1
	slot               uint64
	slotCommit         [2]uint64
	slotResources      [2]*rootpublication.StableResourceSet
	ambiguousResources []*rootpublication.StableResourceSet
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
	if snapshot == nil || snapshot.state == nil {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		return nil, errors.New("capture candidate dependency snapshot")
	}
	if snapshot.idx != idx {
		_ = snapshot.Close()
		return nil, errors.New("candidate dependency index changed")
	}
	candidateState := *snapshot.state
	candidateState.CommitSeq = next.CommitSeq
	candidateState.RootPageID = next.UserRootPageID
	candidateState.SystemRootPageID = next.SystemRootPageID
	candidateState.AppliedCommandLSN = next.AppliedCommandLSN
	candidateState.MaxEntryRevision = page.EntryRevision(next.MaxEntryRevision)
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

// publishDurableRootV1 is the sole V1 meta mutator. It materializes the COW
// inventory and root record, persists the exact captured index identity, then
// writes the alternate meta slot once. In-memory authority advances only after
// that slot is durably synchronized.
func (db *DB) publishDurableRootV1(idx *indexGen, next page.MetaPageBody, retired []uint64, vlogRefDelta *valueLogRefDelta) (page.MetaPageBody, error) {
	if db == nil || idx == nil || idx.pager == nil || idx.allocator == nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.New("missing durable-root index"), true)
	}
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()

	current := db.durableRoot
	if current.meta.CommitSeq == 0 || current.record.CommitSeq != current.meta.CommitSeq || next.CommitSeq != current.meta.CommitSeq+1 {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.New("durable-root publication sequence mismatch"), true)
	}
	if next.UserRootPageID < 2 || next.SystemRootPageID < 2 {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.New("durable-root candidate has invalid roots"), true)
	}
	oldest, err := oldestRecoverableSlotCommitV1(current.slotCommit)
	if err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
	}
	capability, err := freelist.NewReuseCapability(oldest, db.MinPinnedSnapshotCommitSeq(), 0)
	if err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
	}
	if err := idx.allocator.RetireCOWV1(retired, current.record.CommitSeq); err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("retire COW pages: %w", err), true)
	}

	resources, err := db.captureDurableValueLogResourcesV1(idx, next, vlogRefDelta)
	if err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("capture durable-root dependencies: %w", err), true)
	}
	releaseResources := resources != nil
	defer func() {
		if releaseResources {
			resources.Release()
		}
	}()
	manifest, err := durableManifestFromResourcesV1(resources)
	if err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
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
		return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("prepare COW generation: %w", err), true)
	}
	generation := prepared.Candidate().Generation()
	auxiliary := prepared.AuxiliaryPageIDs()
	if generation == nil || len(auxiliary) != auxiliaryCount {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.New("incomplete durable-root COW candidate"), true)
	}
	if err := idx.pager.Truncate(generation.HighWater()); err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("extend durable index: %w", err), true)
	}
	for _, image := range prepared.Candidate().Pages() {
		if err := idx.pager.Write(image.PageID, image.Data); err != nil {
			return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("write COW page %d: %w", image.PageID, err), true)
		}
	}
	sink := durablePagerSinkV1{pager: idx.pager}
	manifestRef, err := manifest.Materialize(auxiliary[0], sink)
	if err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
	}
	recordPageID := auxiliary[len(auxiliary)-1]
	next.TotalPages = generation.HighWater()
	next.FreelistHeadID = 0
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
	recordImage, recordDigest, err := record.EncodePage(recordPageID)
	if err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
	}
	if err := idx.pager.Write(recordPageID, recordImage); err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("write durable root record: %w", err), true)
	}
	meta, err := page.NewDurableMetaV1(next.CommitSeq, next.CommitSeq, recordPageID, recordDigest)
	if err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
	}

	stableSnapshot := db.AcquireStableSnapshot()
	if stableSnapshot == nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.New("capture stable index snapshot"), true)
	}
	token, err := stableSnapshot.CaptureStableIndexFileResource()
	if err != nil {
		_ = stableSnapshot.Close()
		return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("capture stable index resource: %w", err), true)
	}
	defer token.Release()
	if resources != nil {
		if err := resources.SyncThrough(); err != nil {
			return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("sync durable-root external dependencies: %w", err), true)
		}
	}
	if err := token.SyncThrough(); err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(fmt.Errorf("sync durable-root dependencies: %w", err), true)
	}

	target := uint64(MetaPage0ID)
	if current.slot == MetaPage0ID {
		target = MetaPage1ID
	}
	metaPath := ""
	if durabilitycut.Enabled() {
		metaPath = filepath.Join(db.dir, indexFileName)
	}
	metaOffset := int64(target) * int64(page.PageSize)
	if err := durabilitycut.EmitRange(durabilitycut.BeforeMetaWrite, durabilitycut.ResourceMeta, db.dir, metaPath, metaOffset, int64(page.PageSize)); err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
	}
	if db.testFailWriteMeta.Load() {
		return page.MetaPageBody{}, wrapFinalizeCommitError(errTestWriteMetaFailpoint, true)
	}
	if err := writeDurableMetaSlotV1(sink, target, meta); err != nil {
		return page.MetaPageBody{}, wrapFinalizeCommitError(err, true)
	}
	postMetaError := func(err error) (page.MetaPageBody, error) {
		// Once the target meta page may have changed, either its old or new
		// generation can become authoritative after a later whole-file sync.
		// Retain the candidate closure in addition to both installed slot
		// closures until recovery resolves that ambiguity.
		if resources != nil {
			current.ambiguousResources = append(current.ambiguousResources, resources)
			db.durableRoot = current
			releaseResources = false
		}
		db.publicationPoisoned.Store(true)
		return page.MetaPageBody{}, wrapFinalizeCommitError(errors.Join(err, ErrRecoveryRequired), false)
	}
	if err := durabilitycut.EmitRange(durabilitycut.AfterMetaWrite, durabilitycut.ResourceMeta, db.dir, metaPath, metaOffset, int64(page.PageSize)); err != nil {
		return postMetaError(err)
	}
	if err := durabilitycut.EmitPath(durabilitycut.BeforeMetaSync, durabilitycut.ResourceMeta, db.dir, metaPath); err != nil {
		return postMetaError(err)
	}
	if db.testFailSyncMeta.Load() {
		return postMetaError(errTestSyncMetaFailpoint)
	}
	if err := token.WithPinnedFile(func(file *os.File) error {
		return idx.pager.SyncPagesWithStableFile(file, []uint64{target})
	}); err != nil {
		return postMetaError(err)
	}
	if err := durabilitycut.EmitPath(durabilitycut.AfterMetaSync, durabilitycut.ResourceMeta, db.dir, metaPath); err != nil {
		return postMetaError(err)
	}
	if err := idx.allocator.PublishCOWCandidateV1(prepared, capability); err != nil {
		return postMetaError(fmt.Errorf("publish COW generation: %w", err))
	}
	current.meta = meta
	current.record = record
	current.manifest = manifest
	current.slot = target
	current.slotCommit[target] = next.CommitSeq
	previousResources := current.slotResources[target]
	current.slotResources[target] = resources
	db.durableRoot = current
	releaseResources = false
	previousResources.Release()
	return next, nil
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
	resources = append(resources, db.durableRoot.ambiguousResources...)
	db.durableRoot.slotResources = [2]*rootpublication.StableResourceSet{}
	db.durableRoot.ambiguousResources = nil
	db.durablePublishMu.Unlock()
	for _, set := range resources {
		set.Release()
	}
}
