package db

import (
	"bytes"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// nativeMVCCRootPreparationV6 is embedded in the original admitted producer.
// Fresh current components are retained by the original root while the exact
// materialized DATA replacement is sealed in that producer's SAME constructor.
// No DB/allocator lock, live transaction pointer or prepared slot crosses Step.
type nativeMVCCRootPreparationV6 struct {
	arena             *primaryarena.Arena
	source            primaryarena.Ref
	sourceCertificate *primaryarena.ReadRootCertificateV6
	sourceDirectory   node.PrimaryDirectoryView
	expectedSequence  uint64
	next              page.MetaPageBody
	entries           [node.PrimaryDirectoryMaxEntries]node.PrimaryDirectoryEntry
	cells             [node.PrimaryDirectoryMaxEntries]primaryarena.Ref
	count, index      int
	physical          bool
	group             *primaryarena.Group
	groupWork         primaryarena.GroupConstructionWorkV6
	image             [page.PageSize]byte
	encode            node.PrimaryDirectoryEncodeWorkV6
	seal              primaryarena.DirectorySealWorkV6
	queue             primaryarena.ReleaseQueue
	phase             uint8
}

// StepNativeMVCCBackendRootV6 grafts the completed selected-delete DATA output
// onto a freshly acquired current directory. Exact old components already
// folded into output disappear; newer components retain their original bytes.
// This is private preparation only. Final publication must revalidate the SAME
// Request, current base/root sequence, and original allocator capture.
func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCBackendRootV6(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.db == nil || c.snapshot == nil || c.transaction == nil || c.output == nil || !c.outputAdoptionReady || c.phase != 3 && c.phase != 4 || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	s := &c.rootPreparation
	output, _, _, ready := c.output.Result()
	if !ready {
		return false, ErrNativeMVCCSnapshotWork
	}
	if s.phase == 7 {
		return true, nil
	}
	if s.phase == 0 {
		ready, err := c.request.Validate(w)
		if !ready || err != nil {
			return false, err
		}
		if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(page.MetaPageBody{}))) {
			return false, nil
		}
		db := c.db
		if !db.writeMu.TryLock() {
			return false, nil
		}
		defer db.writeMu.Unlock()
		if !db.durablePublishMu.TryLock() {
			return false, nil
		}
		defer db.durablePublishMu.Unlock()
		if !db.mu.TryRLock() {
			return false, nil
		}
		defer db.mu.RUnlock()
		if db.closing.Load() || db.idx.Load() != c.snapshot.idx || db.meta.CommitSeq == ^uint64(0) {
			return false, ErrNativeMVCCSnapshotWork
		}
		current := db.primaryCurrentRootV5()
		if current == nil || current.arena != c.snapshot.primaryRoot.arena || current.certificate == nil {
			return false, ErrNativeMVCCSnapshotWork
		}
		cert, ready, err := current.arena.BorrowReadRootCertificateV6(current.ref, w)
		if !ready || err != nil {
			return false, err
		}
		if cert != current.certificate {
			return false, rootpublication.ErrDurableRootOwnership
		}
		base, sequence := cert.Directory().Base()
		if base != c.outputInput.Base || sequence != c.outputInput.Sequence {
			return false, errDurableRootCandidateStale
		}
		if ready, err := current.arena.Acquire(current.ref, w); !ready || err != nil {
			return false, err
		}
		s.arena, s.source, s.sourceCertificate, s.sourceDirectory = current.arena, current.ref, cert, cert.Directory()
		s.expectedSequence, s.next = db.meta.CommitSeq, db.meta
		s.next.CommitSeq++
		s.next.UserRootPageID = c.transaction.PrimaryConstructionReferenceV6().PageID
		s.arena.InitializeReleaseQueue(&s.queue)
		s.phase = 1
		// End the original current-root cut BEFORE any finite continuation return.
		return false, nil
	}
	for {
		switch s.phase {
		case 1:
			captured := c.snapshot.primaryRoot.certificate.Directory()
			for s.index < s.sourceDirectory.Count() {
				if !w.Reserve(1, 4*uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))+2*page.PageSize) {
					return false, nil
				}
				entry, err := s.sourceDirectory.Entry(s.index)
				if err != nil {
					return false, err
				}
				old, found, ready, err := captured.SearchPrimaryComponentWithVisit(entry.Key, func(observed node.PrimaryDirectoryEntry) bool {
					return w.Reserve(1, 2*uint64(len(observed.Key)+len(entry.Key))+2*uint64(unsafe.Sizeof(entry)))
				})
				if !ready || err != nil {
					return false, err
				}
				folded := found && bytes.Equal(old.Key, entry.Key) && old.Operand == entry.Operand && old.Revision == entry.Revision && old.Kind == entry.Kind
				if !folded {
					entry.ClassSlot = uint16(s.count)
					if !entry.InlineAbsence() {
						ref, ready, err := s.arena.BorrowComponent(entry.Operand.Ref.Page, w)
						if !ready || err != nil {
							return false, err
						}
						s.cells[s.count] = ref
						s.physical = true
					}
					s.entries[s.count] = entry
					s.count++
				}
				s.index++
			}
			s.phase = 2
		case 2:
			if s.physical {
				group, ready, err := s.groupWork.Step(s.arena, s.cells, w)
				if group != nil {
					s.group = group
				}
				if !ready || err != nil {
					return false, err
				}
			}
			s.phase = 3
		case 3:
			ready, err := s.encode.Step(s.image[:], s.next.UserRootPageID, c.outputSequence, output, s.entries[:s.count], w)
			if !ready || err != nil {
				return false, err
			}
			s.phase = 4
		case 4:
			ready, err := c.transaction.SealPrimaryConstructionWithWorkV6(s.image[:], [1]*primaryarena.Group{s.group}, &s.seal, w)
			if !ready || err != nil {
				return false, err
			}
			c.phase = 4
			s.phase = 5
		case 5, 6:
			ready, err := s.closeStep(w)
			if !ready || err != nil {
				return false, err
			}
		case 7:
			return true, nil
		default:
			return false, ErrNativeMVCCSnapshotWork
		}
	}
}

// closeStep drops only this original preparation's temporary physical edges.
// The SAME constructor independently owns any successfully sealed output root.
func (s *nativeMVCCRootPreparationV6) closeStep(w *iterator.OrdinalScanWork) (bool, error) {
	if s.phase == 0 || s.phase == 7 {
		return true, nil
	}
	if s.phase != 6 {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
			return false, nil
		}
		// Detach all borrowed certificate/key/component/control aliases BEFORE
		// the original source/group edges can reach physical release and refund.
		s.sourceCertificate = nil
		s.sourceDirectory = node.PrimaryDirectoryView{}
		clear(s.entries[:])
		clear(s.cells[:])
		clear(s.image[:])
		s.encode = node.PrimaryDirectoryEncodeWorkV6{}
		s.seal = primaryarena.DirectorySealWorkV6{}
		s.groupWork = primaryarena.GroupConstructionWorkV6{}
		s.phase = 6
	}
	if s.group != nil {
		if ready, err := s.arena.DropGroupToReleaseQueue(s.group, &s.queue, w); !ready || err != nil {
			return false, err
		}
		s.group = nil
	}
	if s.source != (primaryarena.Ref{}) {
		if ready, err := s.arena.DropToReleaseQueue(s.source, &s.queue, w); !ready || err != nil {
			return false, err
		}
		s.source = primaryarena.Ref{}
	}
	for {
		ready, progress, err := s.arena.ReleaseQueueStep(&s.queue, w)
		if !ready || err != nil {
			return false, err
		}
		if !progress {
			break
		}
	}
	s.phase = 7
	return true, nil
}
