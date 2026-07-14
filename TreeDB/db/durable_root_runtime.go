package db

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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
	meta       page.DurableMetaV1
	record     rootpublication.DurableRootRecordV1
	manifest   *rootpublication.DependencyManifestV1
	slot       uint64
	slotCommit [2]uint64
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

// publishDurableRootV1 is the sole V1 meta mutator. It materializes the COW
// inventory and root record, persists the exact captured index identity, then
// writes the alternate meta slot once. In-memory authority advances only after
// that slot is durably synchronized.
func (db *DB) publishDurableRootV1(idx *indexGen, next page.MetaPageBody, retired []uint64) (page.MetaPageBody, error) {
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

	// External producers are added to this deterministic inventory below; the
	// index itself is represented by the enclosing root/meta/COW record.
	manifest, err := rootpublication.NewDependencyManifestV1(nil)
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
	db.durableRoot = current
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
		slot: selected.Slot, slotCommit: selected.SlotCommits,
	}
	db.meta = page.MetaPageBody{
		CommitSeq: selected.Record.CommitSeq, UserRootPageID: selected.Record.UserRootPageID,
		SystemRootPageID: selected.Record.SystemRootPageID, TotalPages: selected.Record.TotalPages,
		LastCommitHeight: selected.Record.LastCommitHeight, AppliedCommandLSN: selected.Record.AppliedCommandLSN,
		MaxEntryRevision: selected.Record.MaxEntryRevision,
	}
	db.metaPageID = selected.Slot
}

func (db *DB) validateDurableDependencyManifestV1(manifest *rootpublication.DependencyManifestV1) error {
	if manifest == nil {
		return rootpublication.ErrDependencyManifestFormat
	}
	// Stable external-resource reopening is added by the publisher alongside
	// manifest population. An empty manifest is the complete inventory for a
	// fresh index without reachable external dependencies.
	return nil
}
