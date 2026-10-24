package mvcc

import (
	"errors"
	"fmt"
	"sync"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

type logicalMVCCSubmitter interface {
	logicalMVCCWriter
	SubmitLogicalMVCC([]mvccadmission.LogicalMutation, mvccadmission.LogicalInput, bool, *treedb.LogicalMVCCCompletion) error
}

// CommitCompletion is caller-owned custody for one submitted atomic group. It
// contains no detached future or background worker. A completion owner must
// drive Complete independently of application Wait; until then it retains the
// original discard-floor exclusion and engine lifecycle/batch. Do not copy it.
// Reuse is allowed after Complete returns, with no old users of the record.
type CommitCompletion struct {
	mu     sync.Mutex
	engine treedb.LogicalMVCCCompletion
	store  *Store
	err    error
}

// Complete drives physical progress and original ordered ACK publication, then
// releases custody once. A zero or already completed record is safe to call.
func (c *CommitCompletion) Complete() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store != nil {
		err := c.engine.Complete()
		c.store.mu.RUnlock()
		c.store = nil
		if err != nil {
			c.err = storageError("complete batch", err)
		}
	}
	return c.err
}

// SubmitCommitGroupAt preserves CommitGroupAt's whole-command validation and
// publication. It returns after append/registration and cut installation; only
// the existing acknowledgement is transferred to completion. Storage errors
// remain ambiguous and must never be retried as a size-bound subdivision.
// Unsupported profiles/adapters retain their synchronous CommitGroupAt route.
func (s *Store) SubmitCommitGroupAt(groups []CommitGroup, mode CommitMode, completion *CommitCompletion) (err error) {
	if completion == nil {
		return errors.New("mvcc: nil completion record")
	}
	completion.mu.Lock()
	defer completion.mu.Unlock()
	if completion.store != nil {
		return errors.New("mvcc: completion still owns a command")
	}
	completion.err = nil
	defer func() {
		if err != nil {
			completion.err = err
		}
	}()
	if mode != CommitRelaxed && mode != CommitDurable {
		return ErrInvalidCommitMode
	}
	if s == nil || s.db == nil {
		return storageError("create batch", treedb.ErrClosed)
	}
	total := 0
	for i := range groups {
		if groups[i].Timestamp == 0 {
			return fmt.Errorf("%w at group index %d", ErrZeroTimestamp, i)
		}
		if len(groups[i].Mutations) > int(^uint(0)>>1)-total {
			return storageError("submit batch", errors.New("mutation count overflow"))
		}
		total += len(groups[i].Mutations)
	}
	if total == 0 {
		return nil
	}
	if s.admission != nil {
		if writer, ok := s.db.(logicalMVCCSubmitter); ok && writer.SupportsLogicalMVCC() {
			return s.commitLogicalGroupsWithCompletion(writer, groups, total, mode, completion)
		}
	}
	return s.CommitGroupAt(groups, mode)
}
