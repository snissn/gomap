package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

type rebuiltPrimaryRootV5 struct {
	root   rebuiltDurableRootV1
	source rootpublication.DurableRootRecordV1
}

// writeRebuiltPrimaryRootsV5 constructs independent immutable records in the
// retained V5 companion or private V6 companion, while replacement DATA is
// unpublished. V6 copies current/one-hop proof directories into fixed capsules. The one new
// genuine DATA generation owns every rebuilt slot/proof tree. Its sequence is
// the earliest retained record's commit; later bank-only records retain that
// exact generation. No old DATA generation or bank image is relabelled.
func (db *DB) writeRebuiltPrimaryRootsV5(ctx context.Context, idx *indexGen, roots *RecoverableRootSet, older rebuiltDurableRootV1, latest rebuiltDurableRootV1, indexPath string) (selected durableRootSelectionV1, runtime *primaryDurableRuntimeV5, err error) {
	if idx == nil || idx.primaryOwner == nil || roots == nil {
		return selected, nil, rootpublication.ErrResourceOwnership
	}
	a, p := idx.primary, idx.pager
	source := roots.durable
	bySeq := map[uint64]rebuiltPrimaryRootV5{
		older.meta.CommitSeq:  {root: older, source: source.slotRecord[source.slot^1]},
		latest.meta.CommitSeq: {root: latest, source: source.slotRecord[source.slot]},
	}
	var privateResources []*rootpublication.StableResourceSet
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
		if _, ok := bySeq[record.CommitSeq]; ok {
			continue
		}
		readRoot := record.UserRootPageID
		if source.primaryReadRoots[proofSlot+2] != 0 {
			readRoot = source.primaryReadRoots[proofSlot+2]
		}
		root := RecoverableRoot{CommitSeq: record.CommitSeq, UserRootPageID: readRoot, SystemRootPageID: record.SystemRootPageID, AppliedCommandLSN: record.AppliedCommandLSN, MaxEntryRevision: record.MaxEntryRevision, Durable: true}
		rebuilt, _, e := db.rebuildRecoverableRootWithPublicationLockV1(ctx, roots, root, p, idx.allocator, true)
		if e != nil {
			return selected, nil, e
		}
		rebuilt.meta.LastCommitHeight = record.LastCommitHeight
		privateResources = append(privateResources, rebuilt.resources)
		bySeq[record.CommitSeq] = rebuiltPrimaryRootV5{root: rebuilt, source: record}
	}
	sequences := make([]uint64, 0, len(bySeq))
	for seq := range bySeq {
		sequences = append(sequences, seq)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
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
	type ownedRecord struct {
		bundle primaryarena.PublicationBundle
		record rootpublication.DurablePrimaryRootRecordV5
		meta   page.DurableMetaV1
	}
	built := make(map[uint64]ownedRecord, len(sequences))
	owned := true
	defer func() {
		if owned {
			for _, record := range built {
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
		input := bySeq[seq]
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
		built[seq] = ownedRecord{bundle: bundle}
		meta.UserRootPageID = root
		meta.TotalPages = generation.HighWater()
		prepared, ready, e := prepareSelectedPrimaryRoot(a, bundle, generation, nil, rootPublicationFrontierV1(meta), p, nil)
		if !ready || e != nil {
			return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
		}
		var manifest rootpublication.DependencyManifestRefV1
		var directory rootpublication.DependencyDirectoryRefV2
		if db.dependencyDirectoryRequiredFeature {
			alloc := &primaryDependencyAllocatorV5{idx: idx}
			directory, e = rebuildDependencyDirectoryV2(p, alloc, input.root.resources)
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
			defer alloc.release()
		} else {
			m, e := durableManifestFromResourcesV1(input.root.resources)
			if e != nil {
				return selected, nil, e
			}
			var refs []primaryarena.Ref
			manifest, refs, e = materializePrimaryManifestV5(a, m)
			if e != nil {
				return selected, nil, e
			}
			defer func() {
				for _, ref := range refs {
					_, drop := a.Drop(ref, nil)
					err = errors.Join(err, drop)
				}
			}()
		}
		record := rootpublication.DurableRootRecordV1{CommitSeq: seq, DurableSeq: input.source.DurableSeq, UserRootPageID: root, SystemRootPageID: meta.SystemRootPageID, TotalPages: generation.HighWater(), AppliedCommandLSN: meta.AppliedCommandLSN, LastCommitHeight: meta.LastCommitHeight, MaxEntryRevision: meta.MaxEntryRevision, Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(), Manifest: manifest, Directory: directory, MetaProjectionDigest: page.DurableMetaProjectionDigestV1(seq, input.source.DurableSeq, bundle.Record.PageID)}
		// A slot binds its exact freshly rebuilt retained parent. A proof record
		// outside that slot-parent relation has no further owning/lookup ancestry.
		isSlot := seq == older.meta.CommitSeq || seq == latest.meta.CommitSeq
		if isSlot && !a.CapsuleFormatV6() && input.source.ParentRecordPageID != 0 {
			parent, ok := built[input.source.ParentCommitSeq]
			if !ok {
				return selected, nil, fmt.Errorf("rebuilt primary: missing retained parent %d", input.source.ParentCommitSeq)
			}
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
			built[seq] = ownedRecord{bundle: bundle, record: value}
		} else {
			image, digest, e := value.EncodePage(bundle.Record.PageID)
			if e != nil {
				return selected, nil, e
			}
			ready, e = a.SealMetadata(bundle.Record, image, nil)
			if !ready || e != nil {
				return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
			}
			durableMeta, e := page.NewDurableMetaV1(seq, record.DurableSeq, bundle.Record.PageID, digest)
			if e != nil {
				return selected, nil, e
			}
			built[seq] = ownedRecord{bundle: bundle, record: value, meta: durableMeta}
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
		var images [2][]byte
		for _, slot := range []uint64{source.slot ^ 1, source.slot} {
			seq := older.meta.CommitSeq
			if slot == source.slot {
				seq = latest.meta.CommitSeq
			}
			value := built[seq]
			dir, e := a.Get(value.bundle.Directory.PageID)
			if e != nil {
				return selected, nil, e
			}
			var parent *rootpublication.DurablePrimaryRootRecordV5
			var parentDir []byte
			if proof := source.primaryProof[slot]; proof.Record.CommitSeq != 0 {
				retained, ok := built[proof.Record.CommitSeq]
				if !ok {
					return selected, nil, rootpublication.ErrResourceOwnership
				}
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
			images[slot] = image
		}
		selected, images, e = selectPrimaryCapsuleAuthorityV6(p, a.Pager(), p.PageCount(), a.Pager().PageCount(), a.UUID(), db.validateDurableDependencyManifestV1, db.primaryDependencyDirectoryValidatorV5(idx))
		if e != nil {
			return selected, nil, e
		}
		runtime = &primaryDurableRuntimeV5{projection: selected.Primary, slots: selected.SlotPrimary, proofRecords: selected.ParentRecords, proofResources: selected.ParentResources, capsuleImages: images}
		var acquired []primaryarena.Ref
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
			root := built[selected.SlotRecords[slot].CommitSeq].bundle.Directory
			if ok, e := a.Acquire(root, nil); !ok || e != nil {
				return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
			}
			acquired = append(acquired, root)
			runtime.records[slot] = root
			runtime.dataFloor[slot] = generation.CommitSeq()
			if parent := selected.ParentRecords[slot]; parent.Record.CommitSeq != 0 {
				proof := built[parent.Record.CommitSeq].bundle.Directory
				if ok, e := a.Acquire(proof, nil); !ok || e != nil {
					return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
				}
				acquired = append(acquired, proof)
				runtime.proofs[slot] = proof
				runtime.dataFloor[slot+2] = generation.CommitSeq()
			}
		}
		current := built[selected.Record.CommitSeq].bundle.Directory
		runtime.current = &primaryStateRootV5{a, current, generation.CommitSeq()}
		for seq, value := range built {
			if seq != selected.Record.CommitSeq {
				if _, e = a.Drop(value.bundle.Directory, nil); e != nil {
					return selected, nil, e
				}
				value.bundle.Directory = primaryarena.Ref{}
				built[seq] = value
			}
		}
		owned = false
		return selected, runtime, nil
	}
	for _, slot := range []uint64{source.slot ^ 1, source.slot} {
		seq := older.meta.CommitSeq
		if slot == source.slot {
			seq = latest.meta.CommitSeq
		}
		value := built[seq]
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
	selected, e = selectDurablePrimaryRootV5(p, a.Pager(), p.PageCount(), a.Pager().PageCount(), a.UUID(), db.validateDurableDependencyManifestV1, db.primaryDependencyDirectoryValidatorV5(idx))
	if e != nil {
		return selected, nil, e
	}
	runtime = &primaryDurableRuntimeV5{projection: selected.Primary, slots: selected.SlotPrimary, proofRecords: selected.ParentRecords, proofResources: selected.ParentResources}
	var acquired []primaryarena.Ref
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
		value := built[record.CommitSeq]
		ready, e := a.Acquire(value.bundle.Record, nil)
		if !ready || e != nil {
			return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
		}
		acquired = append(acquired, value.bundle.Record)
		runtime.records[slot] = value.bundle.Record
		runtime.dataFloor[slot] = selected.SlotPrimary[slot].DataCommitSeq
		if parent := selected.ParentRecords[slot]; parent.Record.CommitSeq != 0 {
			proof := built[parent.Record.CommitSeq].bundle.Record
			ready, e = a.Acquire(proof, nil)
			if !ready || e != nil {
				return selected, nil, errors.Join(e, rootpublication.ErrResourceOwnership)
			}
			acquired = append(acquired, proof)
			runtime.proofs[slot] = proof
			runtime.dataFloor[slot+2] = parent.Primary.DataCommitSeq
		}
	}
	current := built[selected.Record.CommitSeq].bundle.Directory
	runtime.current = &primaryStateRootV5{a, current, generation.CommitSeq()}
	// Each slot/proof now owns exact independent record handles. The selected
	// current state takes one directory owner; other private directory handles go.
	for seq, value := range built {
		if _, e = a.Drop(value.bundle.Record, nil); e != nil {
			return selected, nil, e
		}
		value.bundle.Record = primaryarena.Ref{}
		built[seq] = value
		if seq != selected.Record.CommitSeq {
			if _, e = a.Drop(value.bundle.Directory, nil); e != nil {
				return selected, nil, e
			}
			value.bundle.Directory = primaryarena.Ref{}
			built[seq] = value
		}
	}
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
