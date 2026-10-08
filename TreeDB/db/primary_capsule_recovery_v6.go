package db

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// Capsule eligibility comes exclusively from the two fixed complete images.
// DATA META pages and embedded parent images are never selector candidates.
func selectPrimaryCapsuleAuthorityV6(data, local freelist.PageSource, dataPages, arenaPages uint64, uuid [16]byte, mv durableManifestValidatorV1, dv durablePrimaryDirectoryValidatorV5, metadata *retainedalloc.Owner) (durableRootSelectionV1, [2][]byte, error) {
	var images [2][]byte
	var views [2]rootpublication.PrimaryCapsuleViewV6
	var reasons [2]error
	var candidateBacking [2]durableMetaCandidateV1
	candidates := candidateBacking[:0]
	for slot := uint64(0); slot < 2; slot++ {
		image := make([]byte, rootpublication.PrimaryCapsuleSizeV6)
		for n := uint64(0); n < 3; n++ {
			b, e := local.ReadPage(2 + slot*3 + n)
			if e != nil || len(b) != page.PageSize {
				reasons[slot] = errors.Join(e, errors.New("capsule physical span unavailable"))
				break
			}
			copy(image[n*page.PageSize:], b)
		}
		if reasons[slot] != nil {
			continue
		}
		v, e := rootpublication.DecodePrimaryCapsuleV6(image, slot, uuid)
		if e != nil {
			reasons[slot] = e
			continue
		}
		var digest [32]byte
		copy(digest[:], image[48:80])
		r := v.Current().Record
		meta, e := page.NewDurableMetaV1(r.CommitSeq, r.DurableSeq, rootpublication.PrimaryCapsulePageV6(slot), digest)
		if e != nil {
			reasons[slot] = e
			continue
		}
		if r.MetaProjectionDigest != meta.MetaProjectionDigest {
			reasons[slot] = errors.New("capsule meta projection mismatch")
			continue
		}
		images[slot], views[slot] = image, v
		candidates = append(candidates, durableMetaCandidateV1{slot: slot, meta: meta})
	}
	bank := primaryBankPageSource{local: local, extent: arenaPages}
	validateResources := func(v rootpublication.DurablePrimaryRootRecordV5) (*rootpublication.DependencyManifestV1, *rootpublication.StableResourceSet, error) {
		if v.Record.Directory.RootPageID != 0 {
			if dv == nil {
				return nil, nil, errors.New("capsule dependency validator unavailable")
			}
			s, e := dv(v)
			return nil, s, e
		}
		m, e := rootpublication.LoadDependencyManifestWithMetadataV1(bank, v.Record.Manifest, metadata)
		if e != nil {
			return nil, nil, e
		}
		defer m.ReleaseOwnedMetadataV1()
		if mv == nil {
			return m.DiagnosticProjectionV1(), nil, nil
		}
		s, e := mv(m)
		return m.DiagnosticProjectionV1(), s, e
	}
	selected, e := selectDurableRootCandidateSetV1(candidates, reasons, func(c durableMetaCandidateV1) (durableRootSelectionV1, error) {
		v := views[c.slot]
		g, e := v.ValidatePhysicalProjectionV6(data, local, dataPages, arenaPages)
		if e != nil {
			return durableRootSelectionV1{}, fmt.Errorf("capsule complete physical closure: %w", e)
		}
		current := v.Current()
		s := durableRootSelectionV1{Slot: c.slot, Meta: c.meta, Record: current.Record, Freelist: g, Primary: current.Primary}
		m, resources, e := validateResources(current)
		if e != nil {
			return s, e
		}
		s.Manifest, s.resources = m, resources
		owned := true
		defer func() {
			if owned {
				s.resources.Release()
				s.ParentResources[c.slot].Release()
			}
		}()
		if parent := v.Parent(); parent.Record.CommitSeq != 0 {
			_, s.ParentResources[c.slot], e = validateResources(parent)
			if e != nil {
				return durableRootSelectionV1{}, e
			}
			s.ParentRecords[c.slot] = parent
		}
		owned = false
		return s, nil
	})
	if e != nil {
		return selected, images, e
	}
	for i := range images {
		if selected.SlotCommits[i] == 0 {
			images[i] = nil
		}
	}
	return selected, images, nil
}

// Recovery registers actual banks only. Every current and embedded parent
// contributes an independent handle, then copied read-root custody takes over.
func recoveredCapsuleBanksV6(a *primaryarena.Arena, selected durableRootSelectionV1, images [2][]byte, roots []primaryarena.RecoveredRoot) (uint64, []primaryarena.RecoveredRoot, error) {
	extent := uint64(rootpublication.PrimaryCapsuleFirstBankV6)
	if len(roots) != 0 || cap(roots) < 4*(node.PrimaryDirectoryMaxEntries+1) {
		return 0, nil, rootpublication.ErrResourceOwnership
	}
	add := func(r rootpublication.DurablePrimaryRootRecordV5, dir []byte) error {
		if r.Primary.ArenaHighWater > extent {
			extent = r.Primary.ArenaHighWater
		}
		d, e := node.DecodePrimaryDirectory(dir)
		if e != nil {
			return e
		}
		for i := 0; i < d.Count(); i++ {
			entry, e := d.Entry(i)
			if e != nil {
				return e
			}
			if !entry.InlineAbsence() {
				roots = append(roots, primaryarena.RecoveredRoot{PageID: entry.Operand.Ref.Page, Digest: entry.Operand.Digest, Class: primaryarena.Component})
			}
		}
		edge := primaryCapsuleDependencyEdgeV6(r.Record)
		image, e := a.Get(edge.PageID)
		if e != nil {
			return e
		}
		roots = append(roots, primaryarena.RecoveredRoot{PageID: edge.PageID, Digest: sha256.Sum256(image), Class: edge.Class})
		return nil
	}
	for i := range images {
		if selected.SlotCommits[i] == 0 {
			continue
		}
		v, e := rootpublication.DecodePrimaryCapsuleV6(images[i], uint64(i), a.UUID())
		if e != nil {
			return 0, nil, e
		}
		if e = add(v.Current(), v.Directory()); e != nil {
			return 0, nil, e
		}
		if p := v.Parent(); p.Record.CommitSeq != 0 {
			if e = add(p, v.ParentDirectory()); e != nil {
				return 0, nil, e
			}
		}
	}
	return extent, roots, nil
}

func (db *DB) recoverPrimaryCapsuleV6(idx *indexGen) error {
	defer idx.clearPrimaryRecoveryLeasesV5()
	a, p := idx.primary, idx.pager
	// Reserve both physical image copies and the installation descriptors before
	// selection allocates. Successful eligible roots adopt their own capacities;
	// invalid slots and failed installation refund only this private reservation.
	imageCharge := retainedalloc.AllocationCharge(rootpublication.PrimaryCapsuleSizeV6)
	descriptorCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryStateRootV5{})))
	const rootCapacity = 4 * (node.PrimaryDirectoryMaxEntries + 1)
	rootScratchCharge := retainedalloc.AllocationCharge(rootCapacity*uint64(unsafe.Sizeof(primaryarena.RecoveredRoot{}))) + retainedalloc.AllocationCharge(rootCapacity*uint64(unsafe.Sizeof(primaryarena.Ref{})))
	pendingCharge := 2*imageCharge + descriptorCharge + rootScratchCharge
	if e := a.MetadataOwner().AddPending(pendingCharge); e != nil {
		return e
	}
	defer func() { a.MetadataOwner().RemovePending(pendingCharge) }()
	selected, images, e := selectPrimaryCapsuleAuthorityV6(p, a.Pager(), p.PageCount(), a.Pager().PageCount(), a.UUID(), db.validateDurableDependencyManifestV1, db.primaryDependencyDirectoryValidatorV5(idx), a.MetadataOwner())
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
			return fmt.Errorf("%w: capsule dependency feature mismatch", ErrLegacyFormatRebuildRequired)
		}
	}
	rootBacking := make([]primaryarena.RecoveredRoot, 0, rootCapacity)
	refBacking := make([]primaryarena.Ref, 0, rootCapacity)
	defer func() {
		clear(rootBacking[:cap(rootBacking)])
		clear(refBacking[:cap(refBacking)])
	}()
	extent, roots, e := recoveredCapsuleBanksV6(a, selected, images, rootBacking)
	if e != nil {
		return e
	}
	initial, e := a.RecoverInto(extent, roots, refBacking)
	if e != nil {
		return e
	}
	defer func() {
		for _, r := range initial {
			a.Drop(r, nil)
		}
	}()
	if e = idx.activatePrimaryRecoveryLeasesV5(); e != nil {
		return e
	}
	runtime := &primaryDurableRuntimeV5{projection: selected.Primary, slots: selected.SlotPrimary, proofRecords: selected.ParentRecords, proofResources: selected.ParentResources, capsuleImages: images}
	owned := true
	defer func() {
		if owned {
			for _, r := range runtime.records {
				if r.PageID != 0 {
					a.Drop(r, nil)
				}
			}
			for _, r := range runtime.proofs {
				if r.PageID != 0 {
					a.Drop(r, nil)
				}
			}
		}
	}()
	for i := range images {
		if selected.SlotCommits[i] == 0 {
			continue
		}
		v, e := rootpublication.DecodePrimaryCapsuleV6(images[i], uint64(i), a.UUID())
		if e != nil {
			return e
		}
		runtime.capsuleViews[i] = v
		runtime.records[i], e = copyCapsuleReadImageV6(a, v.Directory(), v.Current().Record)
		if e != nil {
			return e
		}
		runtime.dataFloor[i] = v.Current().Primary.DataCommitSeq
		if parent := v.Parent(); parent.Record.CommitSeq != 0 {
			runtime.proofs[i], e = copyCapsuleReadImageV6(a, v.ParentDirectory(), parent.Record)
			if e != nil {
				return e
			}
			runtime.dataFloor[i+2] = parent.Primary.DataCommitSeq
		}
	}
	current := runtime.records[selected.Slot]
	if ok, e := a.Acquire(current, nil); !ok || e != nil {
		return errors.Join(e, rootpublication.ErrResourceOwnership)
	}
	runtime.current = &primaryStateRootV5{a, current, selected.Primary.DataCommitSeq}
	p.SetPageCount(selected.Record.TotalPages)
	if e = idx.allocator.EnableCOWV1(selected.Freelist, freelist.NewReservationLedger()); e != nil {
		a.Drop(current, nil)
		return e
	}
	// All independently eligible DATA extents remain physically retained; the
	// selected immutable generation alone supplies allocator authority.
	dataExtent := selected.Record.TotalPages
	for _, r := range selected.SlotRecords {
		if r.TotalPages > dataExtent {
			dataExtent = r.TotalPages
		}
	}
	for _, r := range selected.ParentRecords {
		if r.Record.TotalPages > dataExtent {
			dataExtent = r.Record.TotalPages
		}
	}
	p.SetPageCount(dataExtent)
	db.installDurableRootSelectionV1(selected)
	db.meta.UserRootPageID = current.PageID
	db.durableRoot.primary = runtime
	for i, image := range images {
		if image != nil {
			a.AdoptPendingRootMetadata(runtime.records[i], imageCharge)
			pendingCharge -= imageCharge
		}
	}
	a.AdoptPendingRootMetadata(current, descriptorCharge)
	pendingCharge -= descriptorCharge
	installed = true
	owned = false
	return nil
}
