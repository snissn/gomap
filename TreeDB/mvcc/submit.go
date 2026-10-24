package mvcc

import (
	"fmt"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

type logicalMVCCSubmitter interface {
	logicalMVCCWriter
	SubmitLogicalMVCC([]mvccadmission.LogicalMutation, mvccadmission.LogicalInput, bool, *mvccadmission.CompletionLease) (*treedb.CommitFuture, error)
}

// SubmitCommitGroupAt preserves CommitGroupAt's aggregate validation and atomic
// publication. Native COW durable submission returns after canonical WAL order
// and atomic installation are established, before final durability. The database
// owns the future's progress, private bytes and Store floor exclusion through
// completion, even if Wait is never called. Caller operands may be reused after
// Submit returns. Wait errors can be commit ambiguous; do not replay them.
//
// Relaxed or unsupported profiles use CommitGroupAt synchronously and return an
// already-completed future on success. Accepted native work is bounded; capacity
// waits precede database lifecycle admission. DB.Close drains accepted futures.
func (s *Store) SubmitCommitGroupAt(groups []CommitGroup, mode CommitMode) (*treedb.CommitFuture, error) {
	if mode != CommitRelaxed && mode != CommitDurable {
		return nil, ErrInvalidCommitMode
	}
	if s == nil || s.db == nil {
		return nil, storageError("create batch", treedb.ErrClosed)
	}
	total := 0
	for i := range groups {
		if groups[i].Timestamp == 0 {
			return nil, fmt.Errorf("%w at group index %d", ErrZeroTimestamp, i)
		}
		total += len(groups[i].Mutations)
	}
	if total == 0 {
		return &treedb.CommitFuture{}, nil
	}
	writer, ok := s.db.(logicalMVCCSubmitter)
	if mode != CommitDurable || s.admission == nil || !ok || !writer.SupportsLogicalMVCC() {
		if err := s.CommitGroupAt(groups, mode); err != nil {
			return nil, err
		}
		return &treedb.CommitFuture{}, nil
	}
	mutations, err := logicalGroupMutations(groups, total)
	if err != nil {
		return nil, err
	}
	if err := s.lockDiscardFloorRead(); err != nil {
		return nil, err
	}
	operation := "commit batch"
	if total == 1 {
		operation = "commit point"
	}
	lease := mvccadmission.NewCompletionLease(&s.mu, ErrStorage, operation)
	floor := s.discardFloor
	for i := range groups {
		if len(groups[i].Mutations) != 0 && groups[i].Timestamp <= floor {
			lease.Release()
			return nil, fmt.Errorf("%w: group index %d timestamp %d is not above floor %d", ErrVersionBelowDiscardFloor, i, groups[i].Timestamp, floor)
		}
	}
	future, err := writer.SubmitLogicalMVCC(mutations, s.admission.LogicalInput(mutations, floor), true, lease)
	if err != nil {
		lease.Release()
		return nil, storageError(operation, err)
	}
	return future, nil
}
