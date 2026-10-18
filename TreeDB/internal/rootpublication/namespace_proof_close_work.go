package rootpublication

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// The proof's original constructor admits this state as part of its containing
// allocation. It prevents an ordinary release from driving native-owned work.
type namespaceProofCloseWork struct {
	phase        uint8
	native, done bool
}

// NamespaceProofReleaseWork belongs to the disposing original owner. Binding
// transfers its exact proof alias before any containing-storage refund. The
// caller clears its own proof alias after Bind and retains this admitted
// receipt until ContinueWork reports actual completion.
type namespaceProofReleaseState struct {
	proof        *StableNamespaceCreationProof
	phase        uint8
	removalBound bool
	owner        *retainedalloc.Owner
	charge       uint64
}
type NamespaceProofReleaseWork struct {
	mu sync.Mutex
	namespaceProofReleaseState
	removal retainedalloc.RemovalWork
}

func (s *NamespaceProofReleaseWork) Bind(proof *StableNamespaceCreationProof, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || proof == nil || w == nil {
		return false, ErrResourceOwnership
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proof != nil || s.phase != 0 {
		return false, ErrResourceOwnership
	}
	if !namespaceProofReleaseReserve(w, 1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*proof))) {
		return false, nil
	}
	proof.mu.Lock()
	defer proof.mu.Unlock()
	if proof.metadata == nil || proof.closeWork.native || proof.closeWork.done || proof.released.Load() {
		return false, ErrResourceOwnership
	}
	proof.closeWork.native = true
	s.proof = proof
	return true, nil
}
func (s *NamespaceProofReleaseWork) ContinueWork(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	return s.continueWork(true, w)
}
func (s *NamespaceProofReleaseWork) continueWork(native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil {
		return false, ErrResourceOwnership
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.namespaceProofReleaseState.continueWork(native, w, &s.removal)
}
func (s *namespaceProofReleaseState) continueWork(native bool, w *iterator.OrdinalScanWork, removal *retainedalloc.RemovalWork) (bool, error) {

	if s.phase == 2 {
		return true, nil
	}
	if s.phase == 1 {
		ready, err := removal.ContinueWork(w)
		if ready {
			s.phase = 2
		}
		return ready, err
	}
	proof := s.proof
	if proof == nil {
		return false, ErrResourceOwnership
	}
	proof.mu.Lock()
	defer proof.mu.Unlock()
	state := &proof.closeWork
	if !native && state.native && !state.done {
		return false, retainedalloc.ErrWorkRequired
	}
	if state.done {
		if proof.cleanupErr != nil {
			return false, proof.cleanupErr
		}
		s.proof = nil
		s.phase = 2
		return true, nil
	}
	if state.phase == 0 {
		if !namespaceProofReleaseReserve(w, 1, 2*uint64(unsafe.Sizeof(*proof))+64) {
			return false, nil
		}
		proof.released.Store(true)
		state.native = native
		state.phase = 1
	}
	if state.phase == 1 {
		if proof.persistence != nil && proof.persistence != proof.parent {
			if !namespaceProofReleaseReserve(w, 1, 2*uint64(unsafe.Sizeof(*proof))+2*uint64(unsafe.Sizeof(*proof.persistence))+64) {
				return false, nil
			}
			err := proof.persistence.Close()
			if err == nil {
				proof.persistence = nil
			} else {
				proof.addCloseErrorLocked(err)
			}
		}
		state.phase = 2
	}
	if state.phase == 2 {
		if proof.parent != nil {
			if !namespaceProofReleaseReserve(w, 1, 2*uint64(unsafe.Sizeof(*proof))+2*uint64(unsafe.Sizeof(*proof.parent))+64) {
				return false, nil
			}
			parent := proof.parent
			err := parent.Close()
			if err == nil {
				proof.parent = nil
				if proof.persistence == parent {
					proof.persistence = nil
				}
			} else {
				proof.addCloseErrorLocked(err)
			}
		}
		state.phase = 3
	}
	if proof.cleanupErr != nil {
		if !namespaceProofReleaseReserve(w, 1, 2*uint64(unsafe.Sizeof(*proof))+64) {
			return false, nil
		}
		if proof.metadata != nil {
			proof.metadata.CleanupFailed()
		}
		state.done = true
		// Failed exact handles, original producer admission and proof stay in this
		// original receipt/owner. Repeated release never retries either OS Close.
		return false, proof.cleanupErr
	}
	if proof.metadata != nil {
		if removal != nil {
			ready, err := removal.BindPendingWork(proof.metadata, proof.metadataCharge, w)
			if !ready || err != nil {
				return false, err
			}
			s.removalBound = true
		} else {
			s.owner, s.charge = proof.metadata, proof.metadataCharge
		}
	}
	if !namespaceProofReleaseReserve(w, 1, 2*uint64(unsafe.Sizeof(*proof))+64) {
		return false, nil
	}
	proof.name = ""
	proof.adapter = nil
	proof.metadata = nil
	proof.metadataCharge = 0
	state.done = true
	// No further write through the disposed proof occurs after alias detachment.
	s.proof = nil
	if s.removalBound {
		s.phase = 1
	} else {
		s.phase = 2
	}
	return s.phase == 2, nil
}
func (proof *StableNamespaceCreationProof) addCloseErrorLocked(err error) {
	if proof.metadata != nil {
		proof.cleanupFailure.add(err)
		proof.cleanupErr = &proof.cleanupFailure
	} else {
		proof.cleanupErr = errors.Join(proof.cleanupErr, err)
	}
}

// The ordinary nil driver executes this same close sequence synchronously.
func namespaceProofReleaseReserve(w *iterator.OrdinalScanWork, records, bytes uint64) bool {
	return w == nil || w.Reserve(records, bytes)
}
