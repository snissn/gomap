package db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const primaryIndexFileName = "index.db.primary"

type primaryStateRootV5 struct {
	arena      *primaryarena.Arena
	ref        primaryarena.Ref
	dataCommit uint64
}
type primaryDurableRuntimeV5 struct {
	projection     rootpublication.PrimaryProjectionV5
	slots          [2]rootpublication.PrimaryProjectionV5
	records        [2]primaryarena.Ref
	proofs         [2]primaryarena.Ref
	proofRecords   [2]rootpublication.DurablePrimaryRootRecordV5
	proofResources [2]*rootpublication.StableResourceSet
	dataFloor      [4]uint64
	current        *primaryStateRootV5
	capsuleImages  [2][]byte
	capsuleViews   [2]rootpublication.PrimaryCapsuleViewV6
}
type primaryPublicationSealV5 struct {
	prepared          *rootpublication.PreparedPrimaryRootV5
	projection        rootpublication.PrimaryProjectionV5
	manifestBanks     *primaryBankConstructionV5
	token             *rootpublication.StableResourceToken
	proof             primaryarena.Ref
	recordTransferred bool
	proofResources    *rootpublication.StableResourceSet
	promotion         *rootpublication.PrimaryPromotionV6
}

func (db *DB) primaryCurrentRootV5() *primaryStateRootV5 {
	if db.durableRoot.primary == nil {
		return nil
	}
	return db.durableRoot.primary.current
}

func (db *DB) attachPrimaryArenaV5(idx *indexGen, opts Options) error {
	path := filepath.Join(db.dir, primaryIndexFileName)
	_, e := os.Stat(path)
	existing := e == nil
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	if !existing && !opts.IndexPrimaryDirectory && opts.ResolvedProfile != ProfileNoWALFast {
		return nil
	}
	if db.readOnly && !existing {
		return errors.New("read-only primary open requires an existing arena")
	}
	if !existing && idx.pager.PageCount() >= 2 {
		return fmt.Errorf("%w: primary bank format requires rebuild", ErrLegacyFormatRebuildRequired)
	}
	var a *primaryarena.Arena
	if existing {
		a, e = primaryarena.OpenExisting(path, db.readOnly)
	} else {
		a, e = primaryarena.OpenCapsule(path)
	}
	if e != nil {
		return e
	}
	if e = a.SetOwnedMetadataDecoder(rootpublication.PrimaryBankMetadataEdgesV5, rootpublication.PrimaryBankMetadataEdgesOwnedV5); e != nil {
		a.Close()
		return e
	}
	if e = idx.pager.AttachPrimaryBankPager(a.Pager()); e != nil {
		a.Close()
		return e
	}
	idx.primary = a
	idx.primaryOwner, e = newPrimaryArenaOwnerV5(a)
	if e != nil {
		idx.primary = nil
		return errors.Join(e, a.Close())
	}
	idx.zipper.SetPrimaryArena(a)
	return nil
}

func (db *DB) recoverDurablePrimaryV5(idx *indexGen) error {
	defer idx.clearPrimaryRecoveryLeasesV5()
	if idx.primary.CapsuleFormatV6() {
		return db.recoverPrimaryCapsuleV6(idx)
	}
	a, p := idx.primary, idx.pager
	descriptorCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryStateRootV5{})))
	scratchCharge := retainedalloc.AllocationCharge(4*uint64(unsafe.Sizeof(primaryarena.RecoveredRoot{}))) + retainedalloc.AllocationCharge(4*uint64(unsafe.Sizeof(primaryarena.Ref{})))
	pendingCharge := descriptorCharge + scratchCharge
	if e := a.MetadataOwner().AddPending(pendingCharge); e != nil {
		return e
	}
	defer func() { a.MetadataOwner().RemovePending(pendingCharge) }()
	selected, e := selectDurablePrimaryRootWithMetadataV5(p, a.Pager(), p.PageCount(), a.Pager().PageCount(), a.UUID(), db.validateDurableDependencyManifestV1, db.primaryDependencyDirectoryValidatorV5(idx), a.MetadataOwner())
	if e != nil {
		return e
	}
	installed := false
	defer func() {
		if !installed {
			for _, r := range selected.SlotResources {
				r.Release()
			}
			for _, r := range selected.ParentResources {
				r.Release()
			}
		}
	}()
	for _, r := range selected.SlotRecords {
		if r.CommitSeq != 0 && (r.Directory.RootPageID != 0) != db.dependencyDirectoryRequiredFeature {
			return fmt.Errorf("%w: primary dependency directory feature mismatch", ErrLegacyFormatRebuildRequired)
		}
	}
	rootBacking := make([]primaryarena.RecoveredRoot, 0, 4)
	refBacking := make([]primaryarena.Ref, 0, 4)
	defer func() { clear(rootBacking[:cap(rootBacking)]); clear(refBacking[:cap(refBacking)]) }()
	extent, roots, e := recoveredPrimaryRootsIntoV5(selected, rootBacking)
	if e != nil {
		return e
	}
	refs, e := a.RecoverInto(extent, roots, refBacking)
	if e != nil {
		return e
	}
	refsOwned := true
	defer func() {
		if refsOwned {
			for _, ref := range refs {
				a.Drop(ref, nil)
			}
		}
	}()
	if e = idx.activatePrimaryRecoveryLeasesV5(); e != nil {
		return e
	}
	primary := &primaryDurableRuntimeV5{projection: selected.Primary, slots: selected.SlotPrimary, proofRecords: selected.ParentRecords, proofResources: selected.ParentResources}

	cursor := 0
	for i := range primary.records {
		if selected.SlotRecords[i].CommitSeq != 0 {
			primary.records[i] = refs[cursor]
			cursor++
			primary.dataFloor[i] = selected.SlotPrimary[i].DataCommitSeq
			if selected.ParentRecords[i].Record.CommitSeq != 0 {
				primary.proofs[i] = refs[cursor]
				cursor++
				primary.dataFloor[i+2] = selected.ParentRecords[i].Primary.DataCommitSeq
			}
		}
	}
	r, ok, e := a.BorrowDirectory(selected.Record.UserRootPageID, nil)
	if !ok || e != nil {
		return errors.Join(e, rootpublication.ErrResourceOwnership)
	}
	if ok, e = a.Acquire(r, nil); !ok || e != nil {
		return errors.Join(e, rootpublication.ErrResourceOwnership)
	}
	primary.current = &primaryStateRootV5{a, r, selected.Primary.DataCommitSeq}
	defer func() {
		if !installed {
			a.Drop(r, nil)
		}
	}()
	p.SetPageCount(selected.Record.TotalPages)
	if e = idx.allocator.EnableCOWV1(selected.Freelist, freelist.NewReservationLedger()); e != nil {
		return e
	}
	p.SetPageCount(selected.Record.TotalPages)
	db.installDurableRootSelectionV1(selected)
	db.durableRoot.primary = primary
	a.AdoptPendingRootMetadata(r, descriptorCharge)
	pendingCharge -= descriptorCharge
	refsOwned = false
	installed = true
	return nil
}

// The sink owns one admitted contiguous page span. Canonical encoding reuses
// separately admitted page scratch; child-first bank sealing follows only
// after all bytes are complete.
type primaryManifestImagesV5 struct {
	first  uint64
	images []byte
}

func (s *primaryManifestImagesV5) WritePage(id uint64, image []byte) error {
	if id < s.first || len(image) != page.PageSize {
		return rootpublication.ErrDependencyManifestFormat
	}
	offset := id - s.first
	if offset >= uint64(len(s.images)/page.PageSize) {
		return rootpublication.ErrDependencyManifestFormat
	}
	copy(s.images[int(offset)*page.PageSize:], image)
	return nil
}
func materializePrimaryManifestV5(idx *indexGen, m *rootpublication.DependencyManifestV1) (ref rootpublication.DependencyManifestRefV1, banks *primaryBankConstructionV5, err error) {
	if m == nil || m.PageCount() == 0 {
		return ref, nil, rootpublication.ErrDependencyManifestFormat
	}
	claims, err := newPrimaryBankConstructionV5(idx)
	if err != nil {
		return ref, nil, err
	}
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, claims.release())
		}
	}()
	count := int(m.PageCount())
	if e := claims.ensureCapacity(count); e != nil {
		return ref, nil, e
	}
	// Admit the complete simultaneously live sink object, page span and reusable
	// encoder scratch before the first bank claim or allocation.
	imageBytes := uint64(count) * page.PageSize
	if imageBytes > uint64(^uint(0)>>1) {
		return ref, nil, retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryManifestImagesV5{}))) +
		retainedalloc.AllocationCharge(imageBytes) + retainedalloc.AllocationCharge(page.PageSize)
	if e := claims.metadata.AddPending(charge); e != nil {
		return ref, nil, e
	}
	sink := &primaryManifestImagesV5{images: make([]byte, int(imageBytes))}
	scratch := make([]byte, page.PageSize)
	defer func() {
		clear(sink.images)
		sink.images = nil
		clear(scratch)
		scratch = nil
		claims.metadata.RemovePending(charge)
	}()
	if e := claims.claim(primaryarena.Manifest, count); e != nil {
		return ref, nil, e
	}
	sink.first = claims.refs[0].PageID
	ref, err = m.MaterializeWithScratchV1(sink.first, sink, scratch)
	if err != nil {
		return ref, nil, err
	}
	for i := len(claims.refs) - 1; i >= 0; i-- {
		image := sink.images[i*page.PageSize : (i+1)*page.PageSize]
		ready, e := idx.primary.SealMetadata(claims.refs[i], image, nil)
		if !ready || e != nil {
			return ref, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
		}
	}
	owned = false
	return ref, claims, nil
}

func (db *DB) initializeDurablePrimaryV5(idx *indexGen) (err error) {
	p, a := idx.pager, idx.primary
	if _, err = p.Alloc(2); err != nil {
		return err
	}
	var roots [2]uint64
	for i := range roots {
		id, e := p.Alloc(1)
		if e != nil {
			return e
		}
		image, e := p.GetForWrite(id)
		if e != nil {
			return e
		}
		b := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: db.leafPrefixCompression, LeafColumnar: db.indexColumnarLeaves, PackedValuePtr: db.indexPackedValuePtr, InternalBaseDelta: db.indexInternalBaseDelta})
		b.SetPageID(id)
		b.Finish()
		roots[i] = id
	}
	// Initialization has no invented DATA auxiliaries for bank-owned metadata.
	base, e := freelist.NewFreelistGenerationV1(1, p.PageCount(), nil, nil)
	if e != nil {
		return e
	}
	if e = idx.allocator.EnableCOWV1(base, freelist.NewReservationLedger()); e != nil {
		return e
	}
	cap, e := freelist.NewReuseCapability(1, 1, 0)
	if e != nil {
		return e
	}
	var cid freelist.CandidateIDV1
	binary.LittleEndian.PutUint64(cid[:8], 1)
	cow, e := idx.allocator.PrepareCOWCandidateV1(2, 1, cid, cap, 0, freelist.NewCandidatePageSinkV1())
	if e != nil {
		return e
	}
	generation := cow.Candidate().Generation()
	if e = p.Truncate(generation.HighWater()); e != nil {
		return e
	}
	if e = cow.Candidate().WritePagesToV1(durablePagerSinkV1{p}); e != nil {
		return e
	}
	root, e := idx.zipper.BuildPrimaryRoot(roots[0], 1)
	if e != nil {
		return e
	}
	bundle, ok, e := a.TakePublicationBundle(root, nil)
	if !ok || e != nil {
		return e
	}
	next := page.MetaPageBody{CommitSeq: 1, UserRootPageID: root, SystemRootPageID: roots[1], TotalPages: generation.HighWater()}
	prepared, ok, e := prepareSelectedPrimaryRoot(a, bundle, generation, cow, rootPublicationFrontierV1(next), p, nil)
	if !ok || e != nil {
		return e
	}
	var m *rootpublication.DependencyManifestV1
	var mr rootpublication.DependencyManifestRefV1
	var mrefs *primaryBankConstructionV5
	var directoryRef rootpublication.DependencyDirectoryRefV2
	var initialResources *rootpublication.StableResourceSet
	initialOwned := true
	defer func() {
		if initialOwned {
			initialResources.Release()
		}
	}()
	if db.dependencyDirectoryRequiredFeature {
		alloc, e := newPrimaryDependencyAllocatorV5(idx)
		if e != nil {
			return e
		}
		defer func() { err = errors.Join(err, alloc.release()) }()
		id, e := alloc.Alloc(0)
		if e != nil {
			return e
		}
		scratchCharge := retainedalloc.AllocationCharge(page.PageSize)
		if e := a.MetadataOwner().AddPending(scratchCharge); e != nil {
			return e
		}
		image := make([]byte, page.PageSize)
		defer func() { clear(image); a.MetadataOwner().RemovePending(scratchCharge) }()
		builder := node.NewBuilder(image, page.PageTypeLeaf)
		builder.SetPageID(id)
		if e = idx.pager.Write(id, builder.Finish().Data()); e != nil {
			return e
		}
		if e = alloc.seal(id); e != nil {
			return e
		}
		directoryRef.RootPageID = id
		directory, e := db.pinPrimaryDependencyV5(idx, directoryRef, a.Pager().PageCount())
		if e != nil {
			return e
		}
		defer directory.Release()
		emptyBuilder, e := rootpublication.NewStableResourceSetBuilderWithMetadata(a.MetadataOwner())
		if e != nil {
			return e
		}
		defer emptyBuilder.Abandon()
		empty, e := emptyBuilder.Freeze()
		if e != nil {
			return e
		}
		defer empty.Release()
		initialResources, e = rootpublication.BindDependencyDirectoryV2(empty, directory)
		if e != nil {
			return e
		}
	} else {
		emptyBuilder, e := rootpublication.NewStableResourceSetBuilderWithMetadata(a.MetadataOwner())
		if e != nil {
			return e
		}
		defer emptyBuilder.Abandon()
		empty, e := emptyBuilder.Freeze()
		if e != nil {
			return e
		}
		defer empty.Release()
		m, _, e = empty.DependencyManifestV1()
		if e != nil {
			return e
		}
		defer m.ReleaseOwnedMetadataV1()
		mr, mrefs, e = materializePrimaryManifestV5(idx, m)
		if e != nil {
			return e
		}
	}
	defer func() { err = errors.Join(err, mrefs.release()) }()
	record := rootpublication.DurableRootRecordV1{CommitSeq: 1, DurableSeq: 1, UserRootPageID: root, SystemRootPageID: roots[1], TotalPages: generation.HighWater(), Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(), Manifest: mr, Directory: directoryRef, MetaProjectionDigest: page.DurableMetaProjectionDigestV1(1, 1, bundle.Record.PageID)}
	projection := prepared.Projection()
	projection.ArenaHighWater = a.Pager().PageCount()
	if a.CapsuleFormatV6() {
		err = db.initializeCapsuleAuthorityV6(idx, bundle, generation, cow, cap, record, projection, m, initialResources)
		if err == nil {
			initialOwned = false
		}
		return err
	}
	initialMetadata := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryStateRootV5{})))
	if e = a.AdmitRootMetadata(bundle.Directory, initialMetadata); e != nil {
		return e
	}
	digest, e := sealPrimaryRecordV5(a, bundle.Record, rootpublication.DurablePrimaryRootRecordV5{Record: record, Primary: projection})
	if e != nil {
		return e
	}
	meta, e := page.NewDurableMetaV1(1, 1, bundle.Record.PageID, digest)
	if e != nil {
		return e
	}
	// Both independent files and the new arena namespace are stable before META.
	if e = a.Pager().Sync(); e != nil {
		return e
	}
	ns, e := idx.stablePagerNamespaceToken(db.dir, primaryIndexFileName, a.Pager())
	if e != nil {
		return e
	}
	defer ns.Release()
	if e = ns.Stabilize(); e != nil {
		return e
	}
	if _, e = executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{syncIndex: p.Sync, sink: durablePagerSinkV1{p}, target: MetaPage0ID, meta: meta, syncMeta: func() error { return p.SyncPages([]uint64{MetaPage0ID}) }, dir: db.dir, indexPath: filepath.Join(db.dir, indexFileName)}); e != nil {
		return e
	}
	if e = idx.allocator.PublishCOWCandidateV1(cow, cap); e != nil {
		return e
	}
	db.installDurableRootSelectionV1(durableRootSelectionV1{Slot: 0, Meta: meta, Record: record, Freelist: generation, Manifest: m, SlotCommits: [2]uint64{1, 0}, SlotMetas: [2]page.DurableMetaV1{meta, {}}, SlotRecords: [2]rootpublication.DurableRootRecordV1{record, {}}, SlotResources: [2]*rootpublication.StableResourceSet{initialResources, nil}})
	initialOwned = false
	db.durableRoot.primary = &primaryDurableRuntimeV5{projection: projection, slots: [2]rootpublication.PrimaryProjectionV5{projection, {}}, records: [2]primaryarena.Ref{bundle.Record, {}}, dataFloor: [4]uint64{generation.CommitSeq()}, current: &primaryStateRootV5{a, bundle.Directory, generation.CommitSeq()}}
	return nil
}

func (runtime *rootPublicationRuntimeV1) preparePrimarySealV5(ctx context.Context, candidate *rootpublication.PreparedRootCandidate, member *rootPublicationVisibleMemberV1) (err error) {
	db, a := runtime.db, runtime.idx.primary
	base := db.durableRoot
	group := candidate.DurableRootGroup()
	if base.primary == nil || member.primary == nil || base.record.CommitSeq >= member.sequence {
		return errDurableRootCandidateStale
	}
	for _, tx := range group.Members() {
		m := runtime.visibleMember(tx.Sequence())
		if m == nil || m.primary != tx.PreparedPrimary() {
			return rootpublication.ErrDurableRootOwnership
		}
	}
	resources, e := rootpublication.CloneStableResourceSetExcludingKinds(member.resources)
	if e != nil {
		return e
	}
	owned := true
	defer func() {
		if owned {
			resources.Release()
		}
	}()
	directory, e := resources.DependencyDirectoryV2()
	if e != nil {
		return e
	}
	var manifest *rootpublication.DependencyManifestV1
	defer func() {
		if owned {
			manifest.ReleaseOwnedMetadataV1()
		}
	}()
	var mr rootpublication.DependencyManifestRefV1
	var mrefs *primaryBankConstructionV5
	var directoryRef rootpublication.DependencyDirectoryRefV2
	if directory != nil {
		directoryRef = directory.Reference()
		if !primaryarena.IsPage(directoryRef.RootPageID) {
			return rootpublication.ErrResourceOwnership
		}
	} else {
		manifest, e = db.durableManifestFromResourcesV1WithStats(resources)
		if e != nil {
			return e
		}
		mr, mrefs, e = materializePrimaryManifestV5(runtime.idx, manifest)
		if e != nil {
			return e
		}
	}
	mOwned := true
	defer func() {
		if mOwned {
			err = errors.Join(err, mrefs.release())
		}
	}()
	snapshot := db.acquireDurableCandidateStableIndexSnapshotV1(runtime.idx, true)
	if snapshot == nil {
		return ErrClosed
	}
	token, e := snapshot.captureOwnedStableIndexFileResource()
	if e != nil {
		snapshot.Close()
		return e
	}
	tOwned := true
	defer func() {
		if tOwned {
			token.Release()
		}
	}()
	ps := db.acquireDurableCandidateStableIndexSnapshotV1(runtime.idx, true)
	if ps == nil {
		return ErrClosed
	}
	pt, e := ps.captureStablePrimaryIndexResource()
	if e != nil {
		ps.Close()
		return e
	}
	pOwned := true
	defer func() {
		if pOwned {
			pt.Release()
		}
	}()
	bundle, generation := member.primary.Bundle(), member.primary.Generation()
	target := uint64(1)
	if base.slot == 1 {
		target = 0
	}
	next := member.next
	projection := member.primary.Projection()
	projection.ArenaHighWater = a.Pager().PageCount()
	record := rootpublication.DurableRootRecordV1{CommitSeq: next.CommitSeq, DurableSeq: base.record.DurableSeq + 1, UserRootPageID: next.UserRootPageID, SystemRootPageID: next.SystemRootPageID, TotalPages: generation.HighWater(), AppliedCommandLSN: next.AppliedCommandLSN, LastCommitHeight: next.LastCommitHeight, MaxEntryRevision: next.MaxEntryRevision, Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(), Manifest: mr, Directory: directoryRef, ParentRecordPageID: base.meta.RootRecordPageID, ParentCommitSeq: base.meta.CommitSeq, ParentRecordDigest: base.meta.RootRecordDigest, MetaProjectionDigest: page.DurableMetaProjectionDigestV1(next.CommitSeq, base.record.DurableSeq+1, bundle.Record.PageID)}
	if a.CapsuleFormatV6() {
		e = runtime.prepareCapsuleSealV6(member, record, projection, resources, manifest, mrefs, token, pt, target, group.Len())
		if e == nil {
			owned, mOwned, tOwned, pOwned = false, false, false, false
		}
		return e
	}
	if e = growPrimaryRuntimeSlice(runtime, &runtime.seals, len(runtime.seals)+1); e != nil {
		return e
	}
	sealMetadata := primarySealMetadataV6(len(runtime.debt))
	if e = a.MetadataOwner().AddPending(sealMetadata); e != nil {
		return e
	}
	sealTransferred := false
	defer func() {
		if !sealTransferred {
			a.MetadataOwner().RemovePending(sealMetadata)
		}
	}()
	// The record claims are sealed once. A retry retains this exact physical image.
	digest, e := sealPrimaryRecordV5(a, bundle.Record, rootpublication.DurablePrimaryRootRecordV5{Record: record, Primary: projection})
	if e != nil {
		return e
	}
	proofResources, e := rootpublication.CloneStableResourceSetExcludingKinds(base.slotResources[base.slot])
	if e != nil {
		return e
	}
	prOwned := true
	defer func() {
		if prOwned {
			proofResources.Release()
		}
	}()
	proof := base.primary.records[base.slot]
	if ok, e := a.Acquire(proof, nil); !ok || e != nil {
		return e
	}
	meta, e := page.NewDurableMetaV1(next.CommitSeq, record.DurableSeq, bundle.Record.PageID, digest)
	if e != nil {
		a.Drop(proof, nil)
		return e
	}
	seal := &rootPublicationSealV1{metadataCharge: sealMetadata, latestSequence: member.sequence, groupLength: group.Len(), idx: runtime.idx, next: next, resources: resources, manifest: manifest, prefix: append([]*freelist.PreparedCOWCandidateV1(nil), runtime.debt...), token: token, record: record, meta: meta, target: target, base: rootPublicationSealBaseV1{slotCommit: base.slotCommit, slotResources: base.slotResources, slotMeta: base.slotMeta, slotRecord: base.slotRecord}, primary: &primaryPublicationSealV5{prepared: member.primary, projection: projection, manifestBanks: mrefs, token: pt, proof: proof, proofResources: proofResources}}
	runtime.seals = append(runtime.seals, seal)
	runtime.activeSeal = seal
	owned, mOwned, tOwned, pOwned, prOwned, sealTransferred = false, false, false, false, false, true
	return nil
}

func (runtime *rootPublicationRuntimeV1) materializePrimarySealV5(seal *rootPublicationSealV1) error {
	if seal.materialized {
		return nil
	}
	generation := seal.primary.prepared.Generation()
	p := seal.idx.pager
	if e := p.GrowTo(generation.HighWater()); e != nil {
		return e
	}
	if e := seal.idx.allocator.ValidateCOWPhysicalTailV1(generation.HighWater()); e != nil {
		return e
	}
	for _, cow := range seal.prefix {
		if e := cow.Candidate().WritePagesToV1(durablePagerSinkV1{p}); e != nil {
			return e
		}
	}
	if seal.primary.promotion != nil {
		if seal.primary.promotion.View.Slot() != seal.target || seal.primary.promotion.View.Current().Record != seal.record {
			return rootpublication.ErrDurableRootOwnership
		}
		seal.materialized = true
		return nil
	}
	class, digest, ok, e := seal.idx.primary.Identity(seal.primary.prepared.Bundle().Record, nil)
	if !ok || e != nil {
		return e
	}
	image, e := seal.idx.primary.Pager().Get(primaryarena.Local(seal.primary.prepared.Bundle().Record.PageID))
	if e != nil {
		return e
	}
	if class != primaryarena.Record || digest != sha256.Sum256(image) {
		return rootpublication.ErrDurableRootRecordDigest
	}
	if _, e = rootpublication.DecodeDurablePrimaryRootRecordV5(image, seal.primary.prepared.Bundle().Record.PageID, seal.meta.RootRecordDigest); e != nil {
		return e
	}
	seal.materialized = true
	return nil
}

func (runtime *rootPublicationRuntimeV1) commitPrimarySealV5(seal *rootPublicationSealV1) (oldest uint64, err error) {
	db, a := runtime.db, seal.idx.primary
	db.durablePublishMu.Lock()
	db.rootReuseMu.Lock()
	runtime.mu.Lock()
	locked := true
	unlock := func() {
		if locked {
			locked = false
			runtime.mu.Unlock()
			db.rootReuseMu.Unlock()
			db.durablePublishMu.Unlock()
		}
	}
	defer unlock()
	if runtime.activeSeal != seal || runtime.poison != nil || len(runtime.debt) < len(seal.prefix) {
		return 0, rootpublication.ErrDurableRootOwnership
	}
	for i, cow := range seal.prefix {
		if runtime.debt[i] != cow {
			return 0, rootpublication.ErrDurableRootOwnership
		}
	}
	member := runtime.visibleMember(seal.latestSequence)
	if member == nil || member.primary != seal.primary.prepared || member.primaryRecordTransferred {
		return 0, rootpublication.ErrDurableRootOwnership
	}
	base := db.durableRoot
	if base.primary == nil || base.record.CommitSeq >= seal.latestSequence {
		return 0, errDurableRootCandidateStale
	}
	runtimeRoot := member.primary.Bundle().Record
	if seal.primary.promotion != nil {
		runtimeRoot = seal.primary.promotion.Root
	}
	if e := a.AdmitRootMetadata(runtimeRoot, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDurableRuntimeV5{})))); e != nil {
		return 0, e
	}
	primary := *base.primary
	oldRecord, oldProof := primary.records[seal.target], primary.proofs[seal.target]
	oldProofResources := primary.proofResources[seal.target]
	primary.proofResources[seal.target] = seal.primary.proofResources
	primary.proofRecords[seal.target] = rootpublication.DurablePrimaryRootRecordV5{Record: base.record, Primary: base.primary.projection}
	if seal.primary.promotion != nil {
		view := seal.primary.promotion.View
		if view.Slot() != seal.target || view.Current().Record != seal.record {
			return 0, rootpublication.ErrDurableRootOwnership
		}
		primary.proofRecords[seal.target] = view.Parent()
	}
	if seal.primary.promotion != nil {
		primary.records[seal.target] = seal.primary.promotion.Root
		primary.capsuleImages[seal.target] = seal.primary.promotion.Image
		primary.capsuleViews[seal.target] = seal.primary.promotion.View
	} else {
		primary.records[seal.target] = member.primary.Bundle().Record
	}
	if seal.primary.promotion != nil {
		primary.proofs[seal.target] = seal.primary.promotion.Parent
	} else {
		primary.proofs[seal.target] = seal.primary.proof
	}
	primary.slots[seal.target] = seal.primary.projection
	primary.projection = seal.primary.projection
	primary.current = member.install.primaryRoot
	primary.dataFloor[seal.target] = seal.primary.projection.DataCommitSeq
	primary.dataFloor[seal.target+2] = base.primary.slots[base.slot].DataCommitSeq
	current := durableRootRuntimeV1{meta: seal.meta, record: seal.record, manifest: seal.manifest.DiagnosticProjectionV1(), slot: seal.target, slotCommit: base.slotCommit, slotResources: base.slotResources, slotMeta: base.slotMeta, slotRecord: base.slotRecord, primary: &primary}
	current.slotCommit[seal.target] = seal.latestSequence
	current.slotMeta[seal.target] = seal.meta
	current.slotRecord[seal.target] = seal.record
	previousResources := current.slotResources[seal.target]
	current.slotResources[seal.target] = seal.resources
	cap, e := db.durableRootReuseCapabilityV1(current)
	if e != nil {
		return 0, e
	}
	oldest, e = oldestRecoverableSlotCommitV1(current.slotCommit)
	if e != nil {
		return 0, e
	}
	// The unlocked retirement batch owns a real independent pointer array.
	// Admit full capacity before allocation; concurrent producers can then
	// replace the runtime array without prematurely refunding this backing.
	retiredCharge := retainedalloc.AllocationCharge(uint64(len(runtime.seals)) * uint64(unsafe.Sizeof((*rootPublicationSealV1)(nil))))
	if e = a.MetadataOwner().AddPending(retiredCharge); e != nil {
		return 0, e
	}
	retired := make([]*rootPublicationSealV1, 0, len(runtime.seals))
	defer func() {
		clear(retired)
		retired = nil
		a.MetadataOwner().RemovePending(retiredCharge)
	}()
	if len(seal.prefix) != 0 {
		if e = seal.idx.allocator.PublishActivatedCOWPrefixV1(seal.prefix, cap); e != nil {
			return 0, e
		}
	}
	db.durableRoot = current
	db.metaPageID = seal.target
	member.primaryRecordTransferred = true
	seal.primary.recordTransferred = true
	if seal.primary.promotion != nil {
		member.transaction.TransferPrimaryPromotionImageV6()
		seal.primary.promotion.Root = primaryarena.Ref{}
		seal.primary.promotion.Parent = primaryarena.Ref{}
	}
	seal.primary.proof = primaryarena.Ref{}
	seal.primary.proofResources = nil
	seal.resources = nil
	clear(runtime.debt[:len(seal.prefix)])
	remainingDebt := copy(runtime.debt, runtime.debt[len(seal.prefix):])
	clear(runtime.debt[remainingDebt:])
	runtime.debt = runtime.debt[:remainingDebt]
	kept := runtime.seals[:0]
	for _, s := range runtime.seals {
		if s.latestSequence <= seal.latestSequence {
			retired = append(retired, s)
		} else {
			kept = append(kept, s)
		}
	}
	clear(runtime.seals[len(kept):])
	runtime.seals = kept
	runtime.activeSeal = nil
	unlock()
	previousResources.Release()
	oldProofResources.Release()
	for _, r := range []primaryarena.Ref{oldRecord, oldProof} {
		if r != (primaryarena.Ref{}) {
			if _, e = a.Drop(r, nil); e != nil {
				return 0, e
			}
		}
	}
	for _, s := range retired {
		s.release()
	}
	return oldest, nil
}

func (seal *primaryPublicationSealV5) release() {
	if seal == nil {
		return
	}
	a := seal.prepared.Arena()
	if seal.promotion != nil {
		work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
		for _, r := range []primaryarena.Ref{seal.promotion.Root, seal.promotion.Parent} {
			if r != (primaryarena.Ref{}) {
				_, _ = a.Drop(r, work)
			}
		}
		seal.promotion.Root, seal.promotion.Parent = primaryarena.Ref{}, primaryarena.Ref{}
	}

	if seal.promotion != nil {
		seal.promotion.Image = nil
		seal.promotion.View = rootpublication.PrimaryCapsuleViewV6{}
	}
	seal.proofResources.Release()
	seal.proofResources = nil
	_ = seal.manifestBanks.release()
	seal.manifestBanks = nil
	if seal.proof != (primaryarena.Ref{}) {
		_, _ = a.Drop(seal.proof, nil)
		seal.proof = primaryarena.Ref{}
	}
	if seal.token != nil {
		seal.token.Release()
		seal.token = nil
	}
}

func (state *DBState) primaryRetentionCommitV5() uint64 {
	if state.primaryRoot != nil && state.primaryRoot.dataCommit < state.CommitSeq {
		return state.primaryRoot.dataCommit
	}
	return state.CommitSeq
}
