package mvcc

import (
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

// prepareProtectedFloor is the semantic producer's maintenance preparation,
// independent of ordinary CommitAt acknowledgement. Its synchronous Checkpoint
// is ordinary storage work, not a bounded native adapter or a scalar reserve.
// The eventual bounded caller must account/drain this actual cut before issuing
// authority; a WAL-only durable ACK or older Summary never substitutes for it.
func (s *Store) prepareProtectedFloor() (*mvccadmission.Protection, error) {
	if s == nil || s.db == nil || s.admission == nil {
		return nil, storageError("prepare protected floor", treedb.ErrClosed)
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadDiscardFloorLocked(); err != nil {
		return nil, err
	}
	floor := s.discardFloor
	if floor == 0 {
		return nil, ErrZeroTimestamp
	}
	if err := s.persistDiscardFloorLocked(floor, CommitDurable); err != nil {
		s.invalidateDiscardFloorLocked()
		return nil, err
	}
	// This characterization is explicitly ordinary synchronous maintenance.
	// Its unlimited work does not qualify any public bounded call.
	work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	drain, ready, err := s.admission.CaptureFloorDrain(floor, work)
	if !ready || err != nil {
		return nil, err
	}
	checkpoint, ok := s.db.(interface{ Checkpoint() error })
	if !ok {
		return nil, storageError("protected pre-floor cut", treedb.ErrClosed)
	}
	if err := checkpoint.Checkpoint(); err != nil {
		return nil, storageError("protected pre-floor cut", err)
	}
	protection, _, err := s.admission.ConfirmDrainedFloor(drain, work)
	return protection, err
}
