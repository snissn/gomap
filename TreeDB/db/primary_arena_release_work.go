package db

import (
	"errors"
	"os"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The original owner admits these controls with its containing allocation.
// Physical progress is shared by ordinary and native releases.
type primaryArenaCloseWork struct {
	mu                            sync.Mutex
	native, started, done         bool
	phase                         uint8
	arenaErr, proofErr, parentErr error
	namespace                     rootpublication.NamespaceProofReleaseWork
}

// The disposing original parent admits this external receipt. It survives
// removal of the exact owner's last alias and containing metadata charge.
type primaryArenaReleaseState struct {
	owner               *primaryArenaOwnerV5
	phase               uint8
	bound, last, native bool
	prior               error
	refundOwner         *retainedalloc.Owner
	refundCharge        uint64
}
type primaryArenaReleaseWork struct {
	mu sync.Mutex
	primaryArenaReleaseState
	proof                     rootpublication.NamespaceProofReleaseWork
	parentRefund, ownerRefund retainedalloc.RemovalWork
}

func (s *primaryArenaReleaseWork) Bind(owner *primaryArenaOwnerV5, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return bindPrimaryArenaRelease(&s.primaryArenaReleaseState, owner, true, w)
}
func bindPrimaryArenaRelease(s *primaryArenaReleaseState, owner *primaryArenaOwnerV5, native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if s.bound || owner == nil {
		return false, ErrSnapshotCapacity
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*owner))+2*uint64(unsafe.Sizeof(primaryArenaReleaseWork{}))+64) {
		return false, nil
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.refs == 0 {
		return false, ErrNativeMVCCSnapshotWork
	}
	if native {
		if owner.arena == nil || !owner.arena.Pager().SupportsBoundedPhysicalClose() {
			return false, ErrNativeMVCCSnapshotWork
		}
		owner.namespaceMu.Lock()
		ready, err := owner.namespaceProof.SupportsNativeReleaseV6(w)
		owner.namespaceMu.Unlock()
		if !ready || err != nil {
			return false, err
		}
	}
	s.bound, s.native, s.prior = true, native, owner.cleanupErr
	owner.refs--
	s.last = owner.refs == 0
	if !s.last {
		s.phase = 6
		return true, nil
	}
	s.owner = owner
	owner.closeWork.native = native
	owner.closeWork.started = true
	return true, nil
}
func (s *primaryArenaReleaseWork) ContinueWork(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if w == nil {
		return false, false, ErrNativeMVCCSnapshotWork
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return continuePrimaryArenaRelease(&s.primaryArenaReleaseState, &s.proof, &s.parentRefund, &s.ownerRefund, w)
}
func continuePrimaryArenaRelease(s *primaryArenaReleaseState, proof *rootpublication.NamespaceProofReleaseWork, parentRefund, ownerRefund *retainedalloc.RemovalWork, w *iterator.OrdinalScanWork) (bool, bool, error) {
	if !s.bound {
		return false, false, ErrSnapshotCapacity
	}
	if s.phase == 6 {
		return true, false, s.prior
	}
	if s.phase == 5 {
		if !s.native {
			metadata, charge := s.refundOwner, s.refundCharge
			s.refundOwner, s.refundCharge = nil, 0
			metadata.RemovePending(charge)
			s.phase = 6
			return true, false, nil
		}
		ready, err := ownerRefund.ContinueWork(w)
		if ready {
			s.phase = 6
		}
		return ready, !ready, err
	}
	owner := s.owner
	if owner == nil {
		return false, true, ErrSnapshotCapacity
	}
	state := &owner.closeWork
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.done {
		return false, true, &owner.closeFailure
	}
	for {
		switch s.phase {
		case 0:
			var ready bool
			var err error
			if s.native {
				ready, err = owner.arena.CloseWork(w)
			} else {
				err = owner.arena.Close()
				ready = err == nil
			}
			if err != nil {
				state.arenaErr = err
				s.phase = 1
				continue
			}
			if !ready {
				return false, true, nil
			}
			s.phase = 1
		case 1:
			owner.namespaceMu.Lock()
			if owner.namespaceProof != nil {
				if !s.native {
					eligible, eligibilityErr := owner.namespaceProof.SupportsNativeReleaseV6(w)
					if !eligible || eligibilityErr != nil {
						// Manual/foreign legacy proofs retain their ordinary supported route.
						// A native caller was rejected before accepting the original owner edge.
						err := owner.namespaceProof.ReleaseWithError()
						if err == nil {
							owner.namespaceProof = nil
						} else {
							state.proofErr = err
						}
						owner.namespaceMu.Unlock()
						s.phase = 3
						continue
					}
				}
				receipt := proof
				if !s.native {
					receipt = &state.namespace
				}
				ready, err := receipt.Bind(owner.namespaceProof, w)
				if !ready || err != nil {
					owner.namespaceMu.Unlock()
					return false, true, err
				}
				owner.namespaceProof = nil
				owner.namespaceMu.Unlock()
				s.phase = 2
			} else {
				owner.namespaceMu.Unlock()
				s.phase = 3
			}
		case 2:
			receipt := proof
			if !s.native {
				receipt = &state.namespace
			}
			ready, err := receipt.ContinueWork(w)
			if err != nil {
				state.proofErr = err
				s.phase = 3
				continue
			}
			if !ready {
				return false, true, nil
			}
			s.phase = 3
		case 3:
			owner.namespaceMu.Lock()
			if owner.namespaceParent != nil {
				if s.native && owner.namespaceParentCharge != 0 {
					ready, err := parentRefund.BindPendingWork(owner.arena.MetadataOwner(), owner.namespaceParentCharge, w)
					if !ready || err != nil {
						owner.namespaceMu.Unlock()
						return false, true, err
					}
				}
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*owner))+2*uint64(unsafe.Sizeof(*owner.namespaceParent))+64) {
					owner.namespaceMu.Unlock()
					return false, true, nil
				}
				err := owner.namespaceParent.Close()
				if err == nil || errors.Is(err, os.ErrClosed) {
					owner.namespaceParent = nil
				} else {
					state.parentErr = err
				}
			}
			hasRefund := owner.namespaceParentCharge != 0 && state.parentErr == nil
			owner.namespaceMu.Unlock()
			if hasRefund {
				if s.native {
					ready, err := parentRefund.ContinueWork(w)
					if !ready || err != nil {
						return false, true, err
					}
				} else {
					owner.arena.MetadataOwner().RemovePending(owner.namespaceParentCharge)
				}
				owner.namespaceParentCharge = 0
			}
			s.phase = 4
		case 4:
			if state.arenaErr != nil || state.proofErr != nil || state.parentErr != nil || s.prior != nil {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*owner))+64) {
					return false, true, nil
				}
				owner.namespaceFailure.causes = [3]error{state.proofErr, state.parentErr}
				var namespaceErr error
				if state.proofErr != nil || state.parentErr != nil {
					namespaceErr = &owner.namespaceFailure
				}
				owner.closeFailure.causes = [3]error{state.arenaErr, namespaceErr, s.prior}
				owner.arena.MetadataOwner().CleanupFailed()
				state.done = true
				return false, true, &owner.closeFailure
			}
			if s.native {
				ready, err := ownerRefund.BindPendingWork(owner.arena.MetadataOwner(), owner.metadataCharge, w)
				if !ready || err != nil {
					return false, true, err
				}
			} else {
				s.refundOwner, s.refundCharge = owner.arena.MetadataOwner(), owner.metadataCharge
			}
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*owner))+64) {
				return false, true, nil
			}
			owner.arena = nil
			owner.metadataCharge = 0
			state.done = true
			s.owner = nil
			s.phase = 5
			// Unlock the disposed header before the external receipt removes its charge.
			return false, true, nil
		default:
			return false, true, ErrSnapshotCapacity
		}
	}
}
