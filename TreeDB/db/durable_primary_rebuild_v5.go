package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

type rebuiltPrimaryRootV5 struct {
	root   rebuiltDurableRootV1
	source rootpublication.DurableRootRecordV1
}

type rebuiltPrimaryOwnedRecordV5 struct {
	bundle primaryarena.PublicationBundle
	record rootpublication.DurablePrimaryRootRecordV5
	meta   page.DurableMetaV1
}

// The two eligible roots and their direct proofs have a format maximum of four.
// This backing is private construction storage, not an owning publication list.
type primaryRebuildScratchV5 struct {
	sequences        [4]uint64
	inputs           [4]rebuiltPrimaryRootV5
	built            [4]rebuiltPrimaryOwnedRecordV5
	privateResources [2]*rootpublication.StableResourceSet
	acquired         [4]primaryarena.Ref
	count            int
}

func (s *primaryRebuildScratchV5) index(seq uint64) int {
	for i := 0; i < s.count; i++ {
		if s.sequences[i] == seq {
			return i
		}
	}
	return -1
}
func (s *primaryRebuildScratchV5) put(seq uint64, input rebuiltPrimaryRootV5) error {
	if i := s.index(seq); i >= 0 {
		s.inputs[i] = input
		return nil
	}
	if s.count == len(s.sequences) {
		return rootpublication.ErrResourceOwnership
	}
	i := s.count
	s.count++
	s.sequences[i] = seq
	s.inputs[i] = input
	return nil
}

// writeRebuiltPrimaryRootsV5 constructs independent immutable records in the
// retained V5 companion or private V6 companion, while replacement DATA is
// unpublished. V6 copies current/one-hop proof directories into fixed capsules. The one new
// genuine DATA generation owns every rebuilt slot/proof tree. Its sequence is
// the earliest retained record's commit; later bank-only records retain that
// exact generation. No old DATA generation or bank image is relabelled.
func (db *DB) writeRebuiltPrimaryRootsV5(ctx context.Context, idx *indexGen, roots *RecoverableRootSet, older rebuiltDurableRootV1, latest rebuiltDurableRootV1, indexPath string) (selected durableRootSelectionV1, runtime *primaryDurableRuntimeV5, err error) {
	return db.writeRebuiltPrimaryRootsWithProducerV5(ctx, idx, roots, older, latest, indexPath, nil)
}

// produce is synchronous construction work. It is never stored in a root or
// runtime: offline value-log rewrite must rewrite proof pointers with the same
// producer as the two slots, rather than copying old value-log dependencies.
func (db *DB) writeRebuiltPrimaryRootsWithProducerV5(ctx context.Context, idx *indexGen, roots *RecoverableRootSet, older rebuiltDurableRootV1, latest rebuiltDurableRootV1, indexPath string, produce func(RecoverableRoot) (rebuiltDurableRootV1, error)) (selected durableRootSelectionV1, runtime *primaryDurableRuntimeV5, err error) {
	if idx == nil || idx.primaryOwner == nil || roots == nil {
		return selected, nil, rootpublication.ErrResourceOwnership
	}
	a, p := idx.primary, idx.pager
	source := roots.durable
	scratchCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryRebuildScratchV5{})))
	if e := a.MetadataOwner().AddPending(scratchCharge); e != nil {
		return selected, nil, e
	}
	scratch := &primaryRebuildScratchV5{}
	defer func() { *scratch = primaryRebuildScratchV5{}; a.MetadataOwner().RemovePending(scratchCharge) }()
	if e := scratch.put(older.meta.CommitSeq, rebuiltPrimaryRootV5{root: older, source: source.slotRecord[source.slot^1]}); e != nil {
		return selected, nil, e
	}
	if e := scratch.put(latest.meta.CommitSeq, rebuiltPrimaryRootV5{root: latest, source: source.slotRecord[source.slot]}); e != nil {
		return selected, nil, e
	}
	privateResources := scratch.privateResources[:0]
	defer func() {
		for _, resources := range privateResources {
			resources.Release()
		}
	}()
	// Only the two explicitly retained one-hop proofs participate. Their own
	// parent fields are not followed or used for lookup or ownership.
	for proofSlot, proof := range source.primaryProof {
		record := proof.Record
		if record.CommitSeq == 0 {
			continue
		}
		if scratch.index(record.CommitSeq) >= 0 {
			continue
		}
		readRoot := record.UserRootPageID
		if source.primaryReadRoots[proofSlot+2] != 0 {
			readRoot = source.primaryReadRoots[proofSlot+2]
		}
		root := RecoverableRoot{CommitSeq: record.CommitSeq, UserRootPageID: readRoot, SystemRootPageID: record.SystemRootPageID, AppliedCommandLSN: record.AppliedCommandLSN, MaxEntryRevision: record.MaxEntryRevision, Durable: true}
		var rebuilt rebuiltDurableRootV1
		var e error
		if produce != nil {
			rebuilt, e = produce(root)
		} else {
			rebuilt, _, e = db.rebuildRecoverableRootWithPublicationLockV1(ctx, roots, root, p, idx.allocator, true)
		}
		if e != nil {
			return selected, nil, e
		}
		rebuilt.meta.LastCommitHeight = record.LastCommitHeight
		privateResources = append(privateResources, rebuilt.resources)
		if e = scratch.put(record.CommitSeq, rebuiltPrimaryRootV5{root: rebuilt, source: record}); e != nil {
			return selected, nil, e
		}
	}
	// Sort the fixed inputs together without an allocated map or sort closure.
	for i := 1; i < scratch.count; i++ {
		for j := i; j > 0 && scratch.sequences[j] < scratch.sequences[j-1]; j-- {
			scratch.sequences[j], scratch.sequences[j-1] = scratch.sequences[j-1], scratch.sequences[j]
			scratch.inputs[j], scratch.inputs[j-1] = scratch.inputs[j-1], scratch.inputs[j]
		}
	}
	sequences := scratch.sequences[:scratch.count]
	if len(sequences) < 2 || len(sequences) > 4 || sequences[0] == 0 {
		return selected, nil, rootpublication.ErrResourceOwnership
	}
	// All effective trees have been materialized before this real allocator cut.
	allocator := freelist.New(p, 0)
	base, e := freelist.NewFreelistGenerationV1(1, p.PageCount(), nil, nil)
	if e != nil {
		return selected, nil, e
	}
	if e = allocator.EnableCOWV1(base, freelist.NewReservationLedger()); e != nil {
		return selected, nil, e
	}
	capability, e := freelist.NewReuseCapability(sequences[0], sequences[0], 0)
	if e != nil {
		return selected, nil, e
	}
	var cid freelist.CandidateIDV1
	binary.LittleEndian.PutUint64(cid[:8], sequences[0])
	binary.LittleEndian.PutUint64(cid[8:], idx.id)
	cow, e := allocator.PrepareCOWCandidateV1(2, sequences[0], cid, capability, 0, freelist.NewCandidatePageSinkV1())
	if e != nil {
		return selected, nil, e
	}
	generation := cow.Candidate().Generation()
	if e = p.Truncate(generation.HighWater()); e != nil {
		return selected, nil, e
	}
	if e = cow.Candidate().WritePagesToV1(durablePagerSinkV1{p}); e != nil {
		return selected, nil, e
	}
	owned := true
	defer func() {
		if owned {
			for _, record := range scratch.built[:scratch.count] {
				var drop error
				if record.bundle.Record.PageID != 0 {
					_, drop = a.Drop(record.bundle.Record, nil)
				}
				err = errors.Join(err, drop)
				if record.bundle.Directory.PageID != 0 {
					_, drop = a.Drop(record.bundle.Directory, nil)
					err = errors.Join(err, drop)
				}
			}
		}
	}()
	for _, seq := range sequences {
		if e = ctx.Err(); e != nil {
			return selected, nil, e
		}
		input := scratch.inputs[scratch.index(seq)]
		meta := input.root.meta
		root, e := idx.zipper.BuildPrimaryRoot(meta.UserRootPageID, meta.CommitSeq)
		if e != nil {
			return selected, nil, e
		}
		bundle, ready, e := a.TakePublicationBundle(root, nil)
		if !ready || e != nil {
			return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
		}
		// Register both owners immediately so every subsequent failure unwinds only
		// this private construction, never a source slot or current root.
		scratch.built[scratch.index(seq)] = rebuiltPrimaryOwnedRecordV5{bundle: bundle}
		meta.UserRootPageID = root
		meta.TotalPages = generation.HighWater()
		prepared, ready, e := prepareSelectedPrimaryRoot(a, bundle, generation, nil, rootPublicationFrontierV1(meta), p, nil)
		if !ready || e != nil {
			return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
		}
		var manifest rootpublication.DependencyManifestRefV1
		var directory rootpublication.DependencyDirectoryRefV2
		if db.dependencyDirectoryRequiredFeature {
			alloc, e := newPrimaryDependencyAllocatorV5(idx)
			if e != nil {
				return selected, nil, e
			}
			directory, e = rebuildDependencyDirectoryWithRequirementV2(p, alloc, input.root.resources, true)
			if e == nil && directory.RootPageID == 0 {
				e = rootpublication.ErrDependencyManifestFormat
			}
			if e == nil {
				e = alloc.seal(directory.RootPageID)
			}
			// The record acquires its own edge below; keep all construction refs until
			// sealing is complete, including the empty canonical dependency tree.
			if e != nil {
				alloc.release()
				return selected, nil, e
			}
			defer func() { err = errors.Join(err, alloc.release()) }()
		} else {
			m, e := durableManifestFromResourcesV1(input.root.resources)
			if e != nil {
				return selected, nil, e
			}
			// Materialization borrows this private encoding; the resulting banks
			// own independent images, including on a later rebuild failure.
			defer m.ReleaseOwnedMetadataV1()
			var refs *primaryBankConstructionV5
			manifest, refs, e = materializePrimaryManifestV5(idx, m)
			if e != nil {
				return selected, nil, e
			}
			defer func() { err = errors.Join(err, refs.release()) }()
		}
		record := rootpublication.DurableRootRecordV1{CommitSeq: seq, DurableSeq: input.source.DurableSeq, UserRootPageID: root, SystemRootPageID: meta.SystemRootPageID, TotalPages: generation.HighWater(), AppliedCommandLSN: meta.AppliedCommandLSN, LastCommitHeight: meta.LastCommitHeight, MaxEntryRevision: meta.MaxEntryRevision, Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(), Manifest: manifest, Directory: directory, MetaProjectionDigest: page.DurableMetaProjectionDigestV1(seq, input.source.DurableSeq, bundle.Record.PageID)}
		// A slot binds its exact freshly rebuilt retained parent. A proof record
		// outside that slot-parent relation has no further owning/lookup ancestry.
		isSlot := seq == older.meta.CommitSeq || seq == latest.meta.CommitSeq
		if isSlot && !a.CapsuleFormatV6() && input.source.ParentRecordPageID != 0 {
			parentIndex := scratch.index(input.source.ParentCommitSeq)
			if parentIndex < 0 {
				return selected, nil, fmt.Errorf("rebuilt primary: missing retained parent %d", input.source.ParentCommitSeq)
			}
			parent := scratch.built[parentIndex]
			record.ParentRecordPageID = parent.bundle.Record.PageID
			record.ParentCommitSeq = parent.record.Record.CommitSeq
			record.ParentRecordDigest = parent.meta.RootRecordDigest
		}
		projection := prepared.Projection()
		projection.ArenaHighWater = a.Pager().PageCount()
		value := rootpublication.DurablePrimaryRootRecordV5{Record: record, Primary: projection}
		if a.CapsuleFormatV6() {
			// The copied logical root retains its own actual dependency edge.
			// Fixed images are installed only after all roots/dependencies sync.
			if ok, e := a.BindReadRootDependencyV6(bundle.Directory, primaryCapsuleDependencyEdgeV6(record), nil); !ok || e != nil {
				return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
			}
			scratch.built[scratch.index(seq)] = rebuiltPrimaryOwnedRecordV5{bundle: bundle, record: value}
		} else {
			digest, e := sealPrimaryRecordV5(a, bundle.Record, value)
			if e != nil {
				return selected, nil, e
			}
			durableMeta, e := page.NewDurableMetaV1(seq, record.DurableSeq, bundle.Record.PageID, digest)
			if e != nil {
				return selected, nil, e
			}
			scratch.built[scratch.index(seq)] = rebuiltPrimaryOwnedRecordV5{bundle: bundle, record: value, meta: durableMeta}
		}
		// Proof dependencies are also genuine recovery authorities and must be
		// durable even when neither selected slot uses that exact resource frontier.
		if resources := input.root.resources; resources != nil {
			if e = resources.FlushThrough(); e != nil {
				return selected, nil, e
			}
			if _, e = syncStableResourceDependenciesV1(resources, db.dir, resources.SyncThrough, nil); e != nil {
				return selected, nil, e
			}
		}
	}
	primaryName := primaryIndexFileName
	if a.CapsuleFormatV6() {
		primaryName = primaryNewFileName
	}
	namespace, e := idx.stablePagerNamespaceToken(db.dir, primaryName, a.Pager())
	if e != nil {
		return selected, nil, e
	}
	defer namespace.Release()
	if a.CapsuleFormatV6() {
		if e = p.Sync(); e != nil {
			return selected, nil, e
		}
		if e = a.Pager().Sync(); e != nil {
			return selected, nil, e
		}
		if e = namespace.Stabilize(); e != nil {
			return selected, nil, e
		}
		imageCharge := retainedalloc.AllocationCharge(rootpublication.PrimaryCapsuleSizeV6)
		descriptorCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryStateRootV5{})))
		// Both installation images overlap the selector copies. Selected copies
		// transfer to their independently retained roots only after complete install.
		pendingCharge := 4*imageCharge + descriptorCharge
		if e = a.MetadataOwner().AddPending(pendingCharge); e != nil {
			return selected, nil, e
		}
		var installImages, images [2][]byte
		defer func() {
			clear(installImages[:])
			if owned {
				clear(images[:])
			}
			a.MetadataOwner().RemovePending(pendingCharge)
		}()

		for _, slot := range []uint64{source.slot ^ 1, source.slot} {
			seq := older.meta.CommitSeq
			if slot == source.slot {
				seq = latest.meta.CommitSeq
			}
			value := scratch.built[scratch.index(seq)]
			dir, e := a.Get(value.bundle.Directory.PageID)
			if e != nil {
				return selected, nil, e
			}
			var parent *rootpublication.DurablePrimaryRootRecordV5
			var parentDir []byte
			if proof := source.primaryProof[slot]; proof.Record.CommitSeq != 0 {
				parentIndex := scratch.index(proof.Record.CommitSeq)
				if parentIndex < 0 {
					return selected, nil, rootpublication.ErrResourceOwnership
				}
				retained := &scratch.built[parentIndex]
				parent = &retained.record
				parentDir, e = a.Get(retained.bundle.Directory.PageID)
				if e != nil {
					return selected, nil, e
				}
			}
			r := value.record
			r.Record.MetaProjectionDigest = page.DurableMetaProjectionDigestV1(r.Record.CommitSeq, r.Record.DurableSeq, rootpublication.PrimaryCapsulePageV6(slot))
			if parent != nil {
				pv := *parent
				pv.Record.MetaProjectionDigest = page.DurableMetaProjectionDigestV1(pv.Record.CommitSeq, pv.Record.DurableSeq, rootpublication.PrimaryCapsulePageV6(slot))
				parent = &pv
			}
			image := make([]byte, rootpublication.PrimaryCapsuleSizeV6)
			if e = rootpublication.EncodePrimaryCapsuleV6(image, slot, r, dir, parent, parentDir); e != nil {
				return selected, nil, e
			}
			if _, ok, e := a.Pager().WritePrimaryCapsuleViewWithWork(2+slot*3, image, nil); !ok || e != nil {
				return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
			}
			installImages[slot] = image
		}
		selected, images, e = selectPrimaryCapsuleAuthorityV6(p, a.Pager(), p.PageCount(), a.Pager().PageCount(), a.UUID(), db.validateDurableDependencyManifestV1, db.primaryDependencyDirectoryValidatorV5(idx), a.MetadataOwner())
		if e != nil {
			return selected, nil, e
		}
		runtime = &primaryDurableRuntimeV5{projection: selected.Primary, slots: selected.SlotPrimary, proofRecords: selected.ParentRecords, proofResources: selected.ParentResources, capsuleImages: images}
		acquired := scratch.acquired[:0]
		defer func() {
			if owned {
				for _, r := range acquired {
					a.Drop(r, nil)
				}
				for _, r := range selected.SlotResources {
					r.Release()
				}
				for _, r := range selected.ParentResources {
					r.Release()
				}
			}
		}()
		for slot, image := range images {
			if len(image) == 0 {
				return selected, nil, rootpublication.ErrResourceOwnership
			}
			view, e := rootpublication.DecodePrimaryCapsuleV6(image, uint64(slot), a.UUID())
			if e != nil {
				return selected, nil, e
			}
			runtime.capsuleViews[slot] = view
			root := scratch.built[scratch.index(selected.SlotRecords[slot].CommitSeq)].bundle.Directory
			if ok, e := a.Acquire(root, nil); !ok || e != nil {
				return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
			}
			acquired = append(acquired, root)
			runtime.records[slot] = root
			runtime.dataFloor[slot] = generation.CommitSeq()
			if parent := selected.ParentRecords[slot]; parent.Record.CommitSeq != 0 {
				proof := scratch.built[scratch.index(parent.Record.CommitSeq)].bundle.Directory
				if ok, e := a.Acquire(proof, nil); !ok || e != nil {
					return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
				}
				acquired = append(acquired, proof)
				runtime.proofs[slot] = proof
				runtime.dataFloor[slot+2] = generation.CommitSeq()
			}
		}
		current := scratch.built[scratch.index(selected.Record.CommitSeq)].bundle.Directory
		runtime.current = &primaryStateRootV5{a, current, generation.CommitSeq()}
		for i, seq := range sequences {
			value := scratch.built[i]
			if seq != selected.Record.CommitSeq {
				if _, e = a.Drop(value.bundle.Directory, nil); e != nil {
					return selected, nil, e
				}
				value.bundle.Directory = primaryarena.Ref{}
				scratch.built[scratch.index(seq)] = value
			}
		}
		for i, image := range images {
			if image != nil {
				a.AdoptPendingRootMetadata(runtime.records[i], imageCharge)
				pendingCharge -= imageCharge
			}
		}
		a.AdoptPendingRootMetadata(current, descriptorCharge)
		pendingCharge -= descriptorCharge
		owned = false
		return selected, runtime, nil
	}
	for _, slot := range []uint64{source.slot ^ 1, source.slot} {
		seq := older.meta.CommitSeq
		if slot == source.slot {
			seq = latest.meta.CommitSeq
		}
		value := scratch.built[scratch.index(seq)]
		if e = a.Pager().SyncIndexData(); e != nil {
			return selected, nil, e
		}
		if e = namespace.Stabilize(); e != nil {
			return selected, nil, e
		}
		_, e = executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{syncIndex: p.SyncIndexData, sink: durablePagerSinkV1{p}, target: slot, meta: value.meta, syncMeta: func() error { return p.SyncPages([]uint64{slot}) }, dir: db.dir, indexPath: indexPath})
		if e != nil {
			return selected, nil, e
		}
	}
	descriptorCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryStateRootV5{})))
	if e = a.MetadataOwner().AddPending(descriptorCharge); e != nil {
		return selected, nil, e
	}
	descriptorOwned := true
	defer func() {
		if descriptorOwned {
			a.MetadataOwner().RemovePending(descriptorCharge)
		}
	}()
	selected, e = selectDurablePrimaryRootWithMetadataV5(p, a.Pager(), p.PageCount(), a.Pager().PageCount(), a.UUID(), db.validateDurableDependencyManifestV1, db.primaryDependencyDirectoryValidatorV5(idx), a.MetadataOwner())
	if e != nil {
		return selected, nil, e
	}
	runtime = &primaryDurableRuntimeV5{projection: selected.Primary, slots: selected.SlotPrimary, proofRecords: selected.ParentRecords, proofResources: selected.ParentResources}
	acquired := scratch.acquired[:0]
	defer func() {
		if owned {
			for _, ref := range acquired {
				_, drop := a.Drop(ref, nil)
				err = errors.Join(err, drop)
			}
			for _, resources := range selected.SlotResources {
				resources.Release()
			}
			for _, resources := range selected.ParentResources {
				resources.Release()
			}
		}
	}()
	for slot, record := range selected.SlotRecords {
		if record.CommitSeq == 0 {
			return selected, nil, rootpublication.ErrResourceOwnership
		}
		value := scratch.built[scratch.index(record.CommitSeq)]
		ready, e := a.Acquire(value.bundle.Record, nil)
		if !ready || e != nil {
			return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
		}
		acquired = append(acquired, value.bundle.Record)
		runtime.records[slot] = value.bundle.Record
		runtime.dataFloor[slot] = selected.SlotPrimary[slot].DataCommitSeq
		if parent := selected.ParentRecords[slot]; parent.Record.CommitSeq != 0 {
			proof := scratch.built[scratch.index(parent.Record.CommitSeq)].bundle.Record
			ready, e = a.Acquire(proof, nil)
			if !ready || e != nil {
				return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
			}
			acquired = append(acquired, proof)
			runtime.proofs[slot] = proof
			runtime.dataFloor[slot+2] = parent.Primary.DataCommitSeq
		}
	}
	current := scratch.built[scratch.index(selected.Record.CommitSeq)].bundle.Directory
	runtime.current = &primaryStateRootV5{a, current, generation.CommitSeq()}
	// Each slot/proof now owns exact independent record handles. The selected
	// current state takes one directory owner; other private directory handles go.
	for i, seq := range sequences {
		value := scratch.built[i]
		if _, e = a.Drop(value.bundle.Record, nil); e != nil {
			return selected, nil, e
		}
		value.bundle.Record = primaryarena.Ref{}
		scratch.built[scratch.index(seq)] = value
		if seq != selected.Record.CommitSeq {
			if _, e = a.Drop(value.bundle.Directory, nil); e != nil {
				return selected, nil, e
			}
			value.bundle.Directory = primaryarena.Ref{}
			scratch.built[scratch.index(seq)] = value
		}
	}
	a.AdoptPendingRootMetadata(current, descriptorCharge)
	descriptorOwned = false
	owned = false
	return selected, runtime, nil
}

func releasePrimaryRuntimeV5(a *primaryarena.Arena, runtime *primaryDurableRuntimeV5) {
	if runtime == nil {
		return
	}
	for _, resources := range runtime.proofResources {
		resources.Release()
	}
	for _, ref := range runtime.records {
		if ref.PageID != 0 {
			_, _ = a.Drop(ref, nil)
		}
	}
	for _, ref := range runtime.proofs {
		if ref.PageID != 0 {
			_, _ = a.Drop(ref, nil)
		}
	}
	if runtime.current != nil {
		_, _ = a.Drop(runtime.current.ref, nil)
	}
}
