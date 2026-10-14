package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"unsafe"
)

// CloseNativeMVCCSnapshotStepV6 consumes the SAME captured Snapshot. No ordinary
// finalizer may drain its native-owned callbacks. Admission refusal leaves the
// exact next physical position in the original containing allocation.
func (s *Snapshot) CloseNativeMVCCSnapshotStepV6(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil {
		return true, nil
	}
	state := &s.nativeMVCC
	if !state.workOwned || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	if state.finalizePhase == 12 {
		return true, s.cleanupErr
	}
	if state.finalizePhase == 0 {
		if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*s))+64) {
			return false, nil
		}
		s.iteratorMu.Lock()
		// Native capture has no ordinary bound iterator/root-tree allocation or
		// callback. Such a callback needs its original work-bearing continuation;
		// refuse before accepting physical cleanup ownership until it is supplied.
		if len(s.iterators) != 0 || len(s.rootTrees) != 0 || s.stablePagerCompletion != nil || s.foregroundReadEnd != nil {
			s.iteratorMu.Unlock()
			return false, ErrNativeMVCCSnapshotWork
		}
		s.closed.Store(true)
		s.readState.Or(snapshotReadClosedBit)
		if s.readState.Load() != snapshotReadClosedBit {
			s.iteratorMu.Unlock()
			return false, nil
		}
		s.finalized.Store(true)
		state.finalizePhase = 1
		s.iteratorMu.Unlock()
	}
	for {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*state))+64) {
			return false, nil
		}
		switch state.finalizePhase {
		case 1:
			if s.idx != nil && s.idx.registry != nil {
				ready, err := s.idx.registry.CancelNativeMVCCRegistration(&state.registration, w)
				if !ready || err != nil {
					return false, err
				}
			}
			s.registryID = 0
			state.finalizePhase = 2
		case 2:
			if s.primaryRoot != nil {
				ready, err := s.primaryRoot.arena.DropToReleaseQueue(s.primaryRoot.ref, &s.primaryRelease, w)
				if !ready || err != nil {
					return false, err
				}
				state.finalizePhase = 3
			} else {
				state.finalizePhase = 4
			}
		case 3:
			ready, progress, err := s.primaryRoot.arena.ReleaseQueueStep(&s.primaryRelease, w)
			if !ready || err != nil {
				return false, err
			}
			if progress {
				continue
			}
			s.primaryRoot = nil
			s.primaryRelease = primaryarena.ReleaseQueue{}
			state.finalizePhase = 4
		case 4:
			if s.vlogPinned && s.vlogManager != nil && s.state != nil && s.state.ValueLogSet != nil {
				ready, err := s.vlogManager.ReleaseNativeMVCCSetStep(s.state.ValueLogSet, w)
				if !ready || err != nil {
					return false, err
				}
			}
			s.vlogPinned = false
			state.finalizePhase = 5
		case 5:
			if s.leafGenerationPinSet != nil {
				ready, err := s.leafGenerationPinSet.releasePinWork(&s.db.leafGenerationPins, &state.leafRelease, w)
				if !ready || err != nil {
					return false, err
				}
			}
			s.leafGenerationPinSet = nil
			s.leafGenerationIDs = nil
			s.leafGenerationPinnedIDs = nil
			state.finalizePhase = 6
		case 6:
			if s.primaryOwner != nil && !state.primaryReleaseBound {
				ready, err := state.primaryReleaseWork.Bind(s.primaryOwner, w)
				if !ready || err != nil {
					return false, err
				}
				s.primaryOwner = nil
				state.primaryReleaseBound = true
			}
			if state.primaryReleaseBound {
				ready, _, err := state.primaryReleaseWork.ContinueWork(w)
				if !ready || err != nil {
					return false, err
				}
			}
			state.finalizePhase = 7
		case 7:
			// Until the shared generation's actual physical Close continuation is
			// bound, retirement refuses before discovering/removing its generation.
			// Current captures release only their real containing borrower edge.
			if s.idx != nil {
				s.idx.wrapperMu.Lock()
				terminal := s.idx.physicalCompleted
				s.idx.wrapperMu.Unlock()
				if terminal || s.db == nil || s.db.idx.Load() != s.idx {
					return false, ErrNativeMVCCSnapshotWork
				}
			}
			if !w.Reserve(2, 4*uint64(unsafe.Sizeof(*s))+64) {
				return false, nil
			}
			if state.epochLive {
				s.finishNativeMVCCCaptureEpochV6()
			}
			if s.stableIndexCapture && s.stableIndexCaptureCounter != nil && !s.stableIndexCaptureTransferred {
				s.stableIndexCaptureCounter.Add(-1)
			}
			s.stableIndexCapture = false
			s.stableIndexCaptureCounter = nil
			state.view = nil
			s.tree.Reset(nil, nil, 0)
			s.treePager = nil
			s.treeRoot = 0
			s.reader = valueReader{}
			s.state = nil
			if s.indexWrapperBorrowed && s.idx != nil {
				// The original live generation retains its physical and constructor
				// metadata. Its containing edge decrement performs no physical disposal.
				s.idx.wrapperMu.Lock()
				if s.idx.physicalCompleted {
					s.idx.wrapperMu.Unlock()
					return false, ErrNativeMVCCSnapshotWork
				}
				if s.idx.wrapperBorrows == 0 {
					s.idx.wrapperMu.Unlock()
					return false, ErrSnapshotCapacity
				}
				s.idx.wrapperBorrows--
				s.idx.wrapperMu.Unlock()
				s.indexWrapperBorrowed = false
			}
			s.idx = nil
			state.finalizePhase = 8
		case 8:
			if s.vlogWrapperBorrowed {
				ready, err := state.managerRelease.BindSnapshotWrapper(s.vlogManager, w)
				if !ready || err != nil {
					return false, err
				}
				s.vlogManager = nil
				s.vlogWrapperBorrowed = false
				state.finalizePhase = 9
			} else {
				s.vlogManager = nil
				state.finalizePhase = 10
			}
		case 9:
			ready, err := state.managerRelease.ReleaseWork(w)
			if !ready || err != nil {
				return false, err
			}
			state.finalizePhase = 10
		case 10:
			if s.primaryMetadata != nil && !state.enrollmentBound {
				ready, err := state.enrollmentRelease.Bind(s.primaryMetadata, w)
				if !ready || err != nil {
					return false, err
				}
				// The exact original pair now lives only in this preborn external receipt.
				s.primaryMetadata = nil
				state.enrollmentBound = true
			}
			if state.enrollmentBound {
				ready, err := state.enrollmentRelease.ContinueWork(w)
				if !ready || err != nil {
					return false, err
				}
			}
			state.finalizePhase = 11
		case 11:
			if s.cleanupCompletion != nil {
				return false, ErrNativeMVCCSnapshotWork
			}
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))+64) {
				return false, nil
			}
			s.db = nil
			s.iteratorMu.Lock()
			s.cleanupFinished = true
			s.cleanupErr = nil
			state.finalizePhase = 12
			s.iteratorMu.Unlock()
			return true, nil
		default:
			return false, ErrNativeMVCCSnapshotWork
		}
	}
}

// This direct continuation of the original owner refuses the unbound last
// physical action before consuming its edge. The original owner mutex makes
// the decision and effect atomic with ordinary releases; no scalar wrapper
// can race a nonlast observation into a synchronous last Arena.Close.
func (owner *primaryArenaOwnerV5) releaseNativeMVCCNonlast(w *iterator.OrdinalScanWork) (bool, error) {
	if owner == nil || w == nil {
		return false, ErrSnapshotCapacity
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*owner))+64) {
		return false, nil
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.refs <= 1 {
		return false, ErrNativeMVCCSnapshotWork
	}
	if owner.cleanupErr != nil {
		return false, owner.cleanupErr
	}
	owner.refs--
	return true, nil
}
