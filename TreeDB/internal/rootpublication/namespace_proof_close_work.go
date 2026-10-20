package rootpublication

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// The proof retains its original physical completion and sticky failure state.
type namespaceProofCloseWork struct {
	phase uint8
	done  bool
}

// The local receipt detaches proof aliases before the original containing refund.
type namespaceProofReleaseState struct {
	proof  *StableNamespaceCreationProof
	phase  uint8
	owner  *retainedalloc.Owner
	charge uint64
}

func (s *namespaceProofReleaseState) continueRelease() (bool, error) {

	if s.phase == 2 {
		return true, nil
	}
	proof := s.proof
	if proof == nil {
		return false, ErrResourceOwnership
	}
	proof.mu.Lock()
	defer proof.mu.Unlock()
	state := &proof.closeWork
	if state.done {
		if proof.cleanupErr != nil {
			return false, proof.cleanupErr
		}
		s.proof = nil
		s.phase = 2
		return true, nil
	}
	if state.phase == 0 {
		proof.released.Store(true)
		state.phase = 1
	}
	if state.phase == 1 {
		if proof.persistence != nil && proof.persistence != proof.parent {
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
		if proof.metadata != nil {
			proof.metadata.CleanupFailed()
		}
		state.done = true
		// Failed exact handles, original producer admission and proof stay in this
		// original receipt/owner. Repeated release never retries either OS Close.
		return false, proof.cleanupErr
	}
	if proof.metadata != nil {
		s.owner, s.charge = proof.metadata, proof.metadataCharge
	}
	proof.name = ""
	proof.adapter = nil
	proof.metadata = nil
	proof.metadataCharge = 0
	state.done = true
	// No further write through the disposed proof occurs after alias detachment.
	s.proof = nil
	s.phase = 2
	return true, nil
}
func (proof *StableNamespaceCreationProof) addCloseErrorLocked(err error) {
	if proof.metadata != nil {
		proof.cleanupFailure.add(err)
		proof.cleanupErr = &proof.cleanupFailure
	} else {
		proof.cleanupErr = errors.Join(proof.cleanupErr, err)
	}
}
