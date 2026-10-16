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
			ready, err := s.closeScratchStep(w)
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

// closeScratchStep ends construction scratch/group custody, preserving the
// SAME acquired current-root edge and its canonical certificate for final-basis
// validation. Phase 7 is prepared, not terminally closed.
func (s *nativeMVCCRootPreparationV6) closeScratchStep(w *iterator.OrdinalScanWork) (bool, error) {
	if s.phase == 0 || s.phase == 7 {
		return true, nil
	}
	if s.phase != 6 {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
			return false, nil
		}
		// Detach scratch/group aliases before the temporary group edge can
		// release. The source certificate still borrows our live source Ref.
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
	for {
		ready, progress, err := s.arena.ReleaseNativeQueueStepV6(&s.queue, w)
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

// closeStep consumes this preparation's original source edge once. Both private
// cancellation and the post-receipt published terminal use the SAME queue; no
// ordinary release driver can steal its descendants or accounting completion.
func (s *nativeMVCCRootPreparationV6) closeStep(w *iterator.OrdinalScanWork) (bool, error) {
	if s.phase == 0 || s.phase == 10 {
		return true, nil
	}
	if s.phase < 7 {
		if ready, err := s.closeScratchStep(w); !ready || err != nil {
			return false, err
		}
	}
	if s.phase == 7 {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
			return false, nil
		}
		// End every borrowed source-image alias before its Ref may reach zero.
		s.sourceCertificate = nil
		s.sourceDirectory = node.PrimaryDirectoryView{}
		s.phase = 8
	}
	if s.phase == 8 {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(s.source))+2*uint64(unsafe.Sizeof(s.phase))) {
			return false, nil
		}
		if s.source != (primaryarena.Ref{}) {
			if ready, err := s.arena.DropToReleaseQueue(s.source, &s.queue, w); !ready || err != nil {
				return false, err
			}
			s.source = primaryarena.Ref{}
		}
		s.phase = 9
	}
	if s.phase != 9 {
		return false, ErrNativeMVCCSnapshotWork
	}
	for {
		ready, progress, err := s.arena.ReleaseNativeQueueStepV6(&s.queue, w)
		if !ready || err != nil {
			return false, err
		}
		if !progress {
			break
		}
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
		return false, nil
	}
	// Queue/refund is genuinely empty. End the arena/queue aliases before the
	// original producer's containing admission can be withdrawn.
	*s = nativeMVCCRootPreparationV6{phase: 10}
	return true, nil
}

// The caller admits this fixed comparison before inspecting either operand.
// This tests identity only; it neither borrows nor creates a certificate/edge.
func (s *nativeMVCCRootPreparationV6) matchesCurrentRoot(root *primaryStateRootV5) bool {
	return s.phase == 7 && s.arena != nil && s.source != (primaryarena.Ref{}) && s.sourceCertificate != nil &&
		root != nil && root.arena == s.arena && root.ref == s.source && root.certificate == s.sourceCertificate
}

func nativeRootSourceWitnessWorkV6() (uint64, uint64) {
	return 2, 2*uint64(unsafe.Sizeof(primaryStateRootV5{})) + 2*uint64(unsafe.Sizeof(primaryarena.Ref{})) +
		2*uint64(unsafe.Sizeof((*primaryarena.Arena)(nil))) + 2*uint64(unsafe.Sizeof((*primaryarena.ReadRootCertificateV6)(nil))) +
		uint64(unsafe.Sizeof(nativeMVCCRootPreparationV6{}.phase))
}
