package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

func prepareSelectedPrimaryRoot(a *primaryarena.Arena, b primaryarena.PublicationBundle, g *freelist.FreelistGenerationV1, c *freelist.PreparedCOWCandidateV1, f rootpublication.Frontier, p freelist.PageSource, w *iterator.OrdinalScanWork) (*rootpublication.PreparedPrimaryRootV5, bool, error) {
	if a.CapsuleFormatV6() {
		return rootpublication.NewPreparedPrimaryReadRootV6(a, b, g, c, f, p, w)
	}
	return rootpublication.NewPreparedPrimaryRootV5(a, b, g, c, f, p, w)
}

func primaryCapsuleDependencyEdgeV6(r rootpublication.DurableRootRecordV1) primaryarena.MetadataEdge {
	if r.Directory.RootPageID != 0 {
		return primaryarena.MetadataEdge{PageID: r.Directory.RootPageID, Class: primaryarena.Dependency}
	}
	return primaryarena.MetadataEdge{PageID: r.Manifest.FirstPageID, Class: primaryarena.Manifest}
}
func (db *DB) initializeCapsuleAuthorityV6(idx *indexGen, b primaryarena.PublicationBundle, g *freelist.FreelistGenerationV1, cow *freelist.PreparedCOWCandidateV1, cap freelist.ReuseCapability, r rootpublication.DurableRootRecordV1, projection rootpublication.PrimaryProjectionV5, m *rootpublication.DependencyManifestV1, resources *rootpublication.StableResourceSet) error {
	a, p := idx.primary, idx.pager
	dir, e := a.Get(b.Directory.PageID)
	if e != nil {
		return e
	}
	charge := retainedalloc.AllocationCharge(rootpublication.PrimaryCapsuleSizeV6) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryStateRootV5{})))
	if e = a.AdmitRootMetadata(b.Directory, charge); e != nil {
		return e
	}
	image := make([]byte, rootpublication.PrimaryCapsuleSizeV6)
	r.MetaProjectionDigest = page.DurableMetaProjectionDigestV1(r.CommitSeq, r.DurableSeq, rootpublication.PrimaryCapsulePageV6(0))
	if e = rootpublication.EncodePrimaryCapsuleV6(image, 0, rootpublication.DurablePrimaryRootRecordV5{Record: r, Primary: projection}, dir, nil, nil); e != nil {
		return e
	}
	view, e := rootpublication.DecodePrimaryCapsuleV6(image, 0, a.UUID())
	if e != nil {
		return e
	}
	if ok, e := a.BindReadRootDependencyV6(b.Directory, primaryCapsuleDependencyEdgeV6(r), nil); !ok || e != nil {
		return e
	}
	if e = p.Sync(); e != nil {
		return e
	}
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
	if _, ok, e := a.Pager().WritePrimaryCapsuleViewWithWork(2, image, nil); !ok || e != nil {
		return e
	}
	if e = idx.allocator.PublishCOWCandidateV1(cow, cap); e != nil {
		return e
	}
	if ok, e := a.Acquire(b.Directory, nil); !ok || e != nil {
		return e
	}
	actual := view.Current()
	var digest [32]byte
	copy(digest[:], image[48:80])
	meta, e := page.NewDurableMetaV1(r.CommitSeq, r.DurableSeq, rootpublication.PrimaryCapsulePageV6(0), digest)
	if e != nil {
		return e
	}
	db.installDurableRootSelectionV1(durableRootSelectionV1{Slot: 0, Meta: meta, Record: actual.Record, Freelist: g, Manifest: m, SlotCommits: [2]uint64{r.CommitSeq, 0}, SlotMetas: [2]page.DurableMetaV1{meta, {}}, SlotRecords: [2]rootpublication.DurableRootRecordV1{actual.Record, {}}, SlotResources: [2]*rootpublication.StableResourceSet{resources, nil}})
	db.meta.UserRootPageID = b.Directory.PageID
	db.durableRoot.primary = &primaryDurableRuntimeV5{projection: actual.Primary, slots: [2]rootpublication.PrimaryProjectionV5{actual.Primary, {}}, records: [2]primaryarena.Ref{b.Directory, {}}, dataFloor: [4]uint64{g.CommitSeq()}, current: &primaryStateRootV5{a, b.Directory, g.CommitSeq()}, capsuleImages: [2][]byte{image, nil}, capsuleViews: [2]rootpublication.PrimaryCapsuleViewV6{view, {}}}
	return nil
}

// copyCapsuleReadImageV6 is a complete independent RAM root. The physical
// capsule image is copied before slot reuse; component/dependency custody is
// acquired from the already recovered immutable bank namespace.
func copyCapsuleReadImageV6(a *primaryarena.Arena, physical []byte, r rootpublication.DurableRootRecordV1) (primaryarena.Ref, error) {
	d, e := node.DecodePrimaryDirectory(physical)
	if e != nil {
		return primaryarena.Ref{}, e
	}
	root, ok, e := a.PrepareReadRootV6(nil)
	if !ok || e != nil {
		return primaryarena.Ref{}, e
	}
	owned := true
	defer func() {
		if owned {
			a.Drop(root, nil)
		}
	}()
	// These private decode/encode buffers are additional format metadata, not
	// pager cache. Admit their full simultaneous backing before construction.
	scratchCharge := retainedalloc.AllocationCharge(uint64(d.Count())*uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))) + retainedalloc.AllocationCharge(page.PageSize)
	if e = a.MetadataOwner().AddPending(scratchCharge); e != nil {
		return primaryarena.Ref{}, e
	}
	var cells [node.PrimaryDirectoryMaxEntries]primaryarena.Ref
	entries := make([]node.PrimaryDirectoryEntry, d.Count())
	image := make([]byte, page.PageSize)
	defer func() {
		clear(entries)
		clear(image)
		a.MetadataOwner().RemovePending(scratchCharge)
	}()
	for i := range entries {
		entries[i], e = d.Entry(i)
		if e != nil {
			return primaryarena.Ref{}, e
		}
		if entries[i].InlineAbsence() {
			continue
		}
		ref, ok, err := a.BorrowComponent(entries[i].Operand.Ref.Page, nil)
		if !ok || err != nil {
			return primaryarena.Ref{}, err
		}
		cells[entries[i].ClassSlot] = ref
	}
	var groups [1]*primaryarena.Group
	if len(entries) > 0 {
		groups[0], ok, e = a.NewGroup(cells, nil)
		if !ok || e != nil {
			return primaryarena.Ref{}, e
		}
		defer a.DropGroup(groups[0], nil)
	}
	base, seq := d.Base()
	if e = node.EncodePrimaryDirectory(image, root.PageID, seq, base, entries); e != nil {
		return primaryarena.Ref{}, e
	}
	if ok, e = a.SealDirectory(root, image, groups, nil); !ok || e != nil {
		return primaryarena.Ref{}, e
	}
	if ok, e = a.BindReadRootDependencyV6(root, primaryCapsuleDependencyEdgeV6(r), nil); !ok || e != nil {
		return primaryarena.Ref{}, e
	}
	owned = false
	return root, nil
}

func (runtime *rootPublicationRuntimeV1) prepareCapsuleSealV6(member *rootPublicationVisibleMemberV1, record rootpublication.DurableRootRecordV1, projection rootpublication.PrimaryProjectionV5, resources *rootpublication.StableResourceSet, manifest *rootpublication.DependencyManifestV1, mrefs *primaryBankConstructionV5, token, primaryToken *rootpublication.StableResourceToken, target uint64, groupLength int) error {
	base := runtime.db.durableRoot
	parentView := base.primary.capsuleViews[base.slot]
	if parentView.Current().Record != base.record || parentView.Slot() != base.slot {
		return rootpublication.ErrDurableRootOwnership
	}
	record.MetaProjectionDigest = page.DurableMetaProjectionDigestV1(record.CommitSeq, record.DurableSeq, rootpublication.PrimaryCapsulePageV6(target))
	// Clone fallible predecessor custody before attaching any fresh promotion
	// edges to the same transaction. A rejected clone must not strand a prepared
	// promotion whose dependency banks are still caller-owned.
	importedProof, e := rootpublication.ImportStableResourceSetMetadata(runtime.idx.primary.MetadataOwner(), base.slotResources[base.slot])
	if e != nil {
		return e
	}
	defer importedProof.Release()
	proofResources, e := rootpublication.CloneStableResourceSetExcludingKinds(importedProof)
	if e != nil {
		return e
	}
	transferred := false
	defer func() {
		if !transferred {
			proofResources.Release()
		}
	}()
	work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	proof := base.primary.records[base.slot]
	promotion, ready, e := member.transaction.PreparePrimaryPromotionV6(record, projection, &parentView, proof, work)
	if !ready || e != nil {
		return errors.Join(e, rootpublication.ErrDurableRootOwnership)
	}
	image, view := promotion.Image, promotion.View
	var digest [32]byte
	copy(digest[:], image[48:80])
	actual := view.Current()
	meta, e := page.NewDurableMetaV1(record.CommitSeq, record.DurableSeq, rootpublication.PrimaryCapsulePageV6(target), digest)
	if e != nil {
		return e
	}
	if e = growPrimaryRuntimeSlice(runtime, &runtime.seals, len(runtime.seals)+1); e != nil {
		return e
	}
	sealMetadata := primarySealMetadataV6(len(runtime.debt))
	if e = runtime.idx.primary.MetadataOwner().AddPending(sealMetadata); e != nil {
		return e
	}
	seal := &rootPublicationSealV1{metadataCharge: sealMetadata, latestSequence: member.sequence, groupLength: groupLength, idx: runtime.idx, next: member.next, resources: resources, manifest: manifest, prefix: append([]*freelist.PreparedCOWCandidateV1(nil), runtime.debt...), token: token, record: actual.Record, meta: meta, target: target, base: rootPublicationSealBaseV1{slotCommit: base.slotCommit, slotResources: base.slotResources, slotMeta: base.slotMeta, slotRecord: base.slotRecord}, primary: &primaryPublicationSealV5{prepared: member.primary, projection: actual.Primary, manifestBanks: mrefs, token: primaryToken, proofResources: proofResources, promotion: promotion}}
	runtime.seals = append(runtime.seals, seal)
	runtime.activeSeal = seal
	transferred = true
	return nil
}
