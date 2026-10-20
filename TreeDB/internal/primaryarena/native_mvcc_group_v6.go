package primaryarena

import (
	"crypto/sha256"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"unsafe"
)

// SealConstructedBaseAbsenceV6 composes one inline absence over the CURRENT
// directory in the original constructor bank. base/sequence are predicates
// from the caller's retained, selected DATA proof, not authority to select a
// record. The caller must prove that selection and hold the current-root cut.
// No remembered directory position or old root is used for negative lookup.
// A full directory is reported to the caller's phased materialization path.
func (a *Arena) SealConstructedBaseAbsenceV6(destination, current Ref, entry node.PrimaryDirectoryEntry, base node.PrimaryOperand, sequence uint64, scratch []byte, owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
	return a.sealConstructedCurrentAbsenceV6(destination, current, entry, base, sequence, 0, scratch, owner, w)
}

// SealConstructedCurrentAbsenceV6 composes the SAME selected immutable record
// over a freshly checked current directory. A matching current component must
// have the selected record's exact revision; absence or foreign replacement
// never grants deletion authority. A missing component keeps the fresh DATA
// base, whose historical selected record remains retained by the caller.
func (a *Arena) SealConstructedCurrentAbsenceV6(destination, current Ref, entry node.PrimaryDirectoryEntry, base node.PrimaryOperand, sequence uint64, selectedRevision page.EntryRevision, scratch []byte, owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
	if selectedRevision == 0 {
		return nil, false, ErrStale
	}
	return a.sealConstructedCurrentAbsenceV6(destination, current, entry, base, sequence, selectedRevision, scratch, owner, w)
}

func (a *Arena) sealConstructedCurrentAbsenceV6(destination, current Ref, entry node.PrimaryDirectoryEntry, base node.PrimaryOperand, sequence uint64, selectedRevision page.EntryRevision, scratch []byte, owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
	if a == nil || w == nil || owner == nil {
		return nil, false, ErrStale
	}
	// Bank/owner observations, whole current CRC/digest, canonical copy/CRC,
	// destination certificate digest and immutable backing equality are actual
	// fixed operands. Pager copy/registration reserve independently below.
	bytes := 4*uint64(unsafe.Sizeof(bank{})) + 8*page.PageSize + 2*uint64(unsafe.Sizeof(ReadRootCertificateV6{})) + 2*uint64(unsafe.Sizeof(Group{}))
	if !w.Reserve(5, bytes) {
		return nil, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	dst, src := a.valid(destination), a.valid(current)
	if !a.capsuleFormat || dst == nil || src == nil || dst.class != Directory || dst.sealed || dst.construction != owner || Local(destination.PageID) < pager.PrimaryReadRootLocalBaseV6 || src.class != Directory || !src.sealed || len(scratch) != page.PageSize || !entry.InlineAbsence() || entry.Revision == 0 {
		return nil, false, ErrStale
	}
	if len(src.image) != page.PageSize || !page.VerifyChecksumNonMutating(src.image) || sha256.Sum256(src.image) != src.digest {
		return nil, false, ErrFormat
	}
	actualBase, actualSequence := src.directory.Base()
	if actualBase != base || actualSequence != sequence {
		return nil, false, ErrStale
	}
	// Append uses a fresh stable class. Existing classes and physical group
	// edges keep their identities; an absence adds no component-bank edge.
	entry.ClassSlot = uint16(src.directory.Count())
	for _, g := range src.groups {
		if g != nil && (g.owner != a || g.refs == 0 || g.refs == ^uint64(0)) {
			return nil, false, ErrStale
		}
	}
	if !w.Reserve(groupCount, 2*uint64(unsafe.Sizeof(Group{}))) {
		return nil, false, nil
	}
	visit := func(observed node.PrimaryDirectoryEntry) bool {
		return w.Reserve(1, 2*uint64(len(observed.Key))+2*uint64(len(entry.Key))+2*uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{})))
	}
	var view node.PrimaryDirectoryView
	var ready bool
	var err error
	if selectedRevision != 0 {
		old, found, observed, lookupErr := src.directory.SearchPrimaryComponentWithVisit(entry.Key, visit)
		if !observed || lookupErr != nil {
			return nil, observed, lookupErr
		}
		if found {
			if old.Revision != selectedRevision || old.Kind != node.PrimaryPut {
				return nil, false, ErrStale
			}
			entry.ClassSlot = old.ClassSlot
			view, ready, err = src.directory.ReplacePrimaryComponentWithVisit(scratch, destination.PageID, entry, visit)
		} else {
			view, ready, err = src.directory.InsertPrimaryComponentWithVisit(scratch, destination.PageID, entry, visit)
		}
	} else {
		view, ready, err = src.directory.InsertPrimaryComponentWithVisit(scratch, destination.PageID, entry, visit)
	}
	if !ready || err != nil {
		return nil, ready, err
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ReadRootCertificateV6{})))
	if err = a.metadata.Add(charge); err != nil {
		return nil, false, err
	}
	physical, ready, err := a.writeBundleLocked(destination, scratch, nil, w)
	if !ready || err != nil {
		a.metadata.Remove(charge)
		return nil, ready, err
	}
	// All remaining effects are infallible and were admitted before output.
	for _, g := range src.groups {
		if g != nil {
			g.refs++
			a.counters.EdgeReads++
			a.counters.EdgeWrites++
		}
	}
	dst.groups, dst.image = src.groups, physical
	dst.directory = view.WithPhysicalImage(physical)
	dst.digest = sha256.Sum256(physical)
	dst.sealed, dst.privatePublication = true, false
	dst.metadataCharge += charge
	dst.readCertificate = &ReadRootCertificateV6{arena: a, ref: destination, directory: dst.directory, image: physical, digest: dst.digest}
	a.counters.Bytes += bytes
	return dst.readCertificate, true, nil
}

// DropReadRootConstructionToReleaseQueueV6 consumes the exact original
// constructor discovery alias and root edge together on its embedded ordinary
// queue. This synchronous ownership transition has no work-credit governor.
func (a *Arena) DropReadRootConstructionToReleaseQueueV6(ref Ref, owner ReadRootConstructionV6, queue *ReleaseQueue) error {
	bytes := 2*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(ReleaseQueue{}))
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(ref)
	if b == nil || b.refs == 0 || owner == nil || queue == nil || queue.owner != a || (b.construction != nil && b.construction != owner) {
		return ErrStale
	}
	b.refs--
	b.construction = nil
	if b.refs == 0 {
		a.enqueueOn(queue, Local(ref.PageID), b)
	}
	a.counters.RootDrops++
	a.counters.Bytes += bytes
	return nil
}
