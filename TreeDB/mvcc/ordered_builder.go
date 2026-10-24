package mvcc

import (
	"errors"
	"fmt"
	"sync"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

type logicalMVCCBuilderOwner interface {
	BeginLogicalMVCC(*treedb.LogicalMVCCBuilder) error
	SupportsLogicalMVCCStaging() bool
}

type commitIdentity struct {
	key       string
	timestamp uint64
}

// CommitBuilder owns a single unassigned atomic cut. AppendGroup validates and
// copies operands incrementally; Submit performs root-dependent preparation once.
// Begin retains discard-floor/lifecycle exclusion until Submit or Abort. No
// accepted prefix or ticket exists before Submit. Never copy an active builder.
type CommitBuilder struct {
	mu     sync.Mutex
	store  *Store
	engine treedb.LogicalMVCCBuilder
	seen   map[commitIdentity]struct{}
	err    error
}

// SupportsCommitGroupStaging reports exact native incremental route support.
// A false result requires the unchanged synchronous writer, even when physical
// work shaping is applicable. It does not grant admission or lifecycle custody.
func (s *Store) SupportsCommitGroupStaging() bool {
	if s == nil || s.db == nil || s.admission == nil {
		return false
	}
	owner, ok := s.db.(logicalMVCCBuilderOwner)
	return ok && owner.SupportsLogicalMVCCStaging()
}

// BeginCommitGroup starts caller-owned staging. Unsupported adapters keep the
// established synchronous CommitGroupAt route. The same floor lock is transferred
// at Submit, without recursively taking RLock behind a waiting floor writer.
func (s *Store) BeginCommitGroup(b *CommitBuilder) error {
	if b == nil {
		return errors.New("mvcc: nil builder")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.store != nil {
		return errors.New("mvcc: builder still owns a command")
	}
	if s == nil || s.db == nil {
		return storageError("begin batch", treedb.ErrClosed)
	}
	owner, ok := s.db.(logicalMVCCBuilderOwner)
	if !ok || s.admission == nil {
		return errors.New("mvcc: owned staging unsupported")
	}
	if err := s.lockDiscardFloorRead(); err != nil {
		return err
	}
	if err := owner.BeginLogicalMVCC(&b.engine); err != nil {
		s.mu.RUnlock()
		return storageError("begin batch", err)
	}
	b.store, b.seen, b.err = s, nil, nil
	return nil
}

// AppendGroup fully validates the new group before copying it. Validation
// failures retain an unchanged builder and are not ErrStorage. Storage/staging
// failures poison this unassigned builder; callers must Abort, never resubmit it.
func (b *CommitBuilder) AppendGroup(group CommitGroup) error {
	if b == nil {
		return storageError("stage batch", treedb.ErrClosed)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.store == nil {
		return storageError("stage batch", treedb.ErrClosed)
	}
	if b.err != nil {
		return b.err
	}
	if group.Timestamp == 0 {
		return ErrZeroTimestamp
	}
	mutations := make([]mvccadmission.LogicalMutation, len(group.Mutations))
	added := make(map[commitIdentity]struct{}, len(group.Mutations))
	for i, m := range group.Mutations {
		if _, err := mvcckey.EncodedLen(m.Key); err != nil {
			return fmt.Errorf("%w at key index %d: %w", ErrInvalidKey, i, err)
		}
		id := commitIdentity{string(m.Key), group.Timestamp}
		if _, exists := b.seen[id]; exists {
			return fmt.Errorf("%w at key index %d", ErrDuplicateKey, i)
		}
		if _, exists := added[id]; exists {
			return fmt.Errorf("%w at key index %d", ErrDuplicateKey, i)
		}
		added[id] = struct{}{}
		mutations[i] = mvccadmission.LogicalMutation{Key: m.Key, Value: m.Value, Timestamp: group.Timestamp, Delete: m.Delete}
	}
	if len(group.Mutations) != 0 && group.Timestamp <= b.store.discardFloor {
		return fmt.Errorf("%w: timestamp %d is not above floor %d", ErrVersionBelowDiscardFloor, group.Timestamp, b.store.discardFloor)
	}
	if err := b.engine.Append(mutations, b.store.admission.LogicalInput(mutations, b.store.discardFloor)); err != nil {
		b.err = storageError("stage batch", err)
		return b.err
	}
	if b.seen == nil {
		b.seen = added
	} else {
		for id := range added {
			b.seen[id] = struct{}{}
		}
	}
	return nil
}

// Submit seals the whole group and transfers original custody. An error leaves
// no caller-owned pending completion and no builder custody; assigned storage
// errors are never size signals and must not be retried.
func (b *CommitBuilder) Submit(mode CommitMode, c *CommitCompletion) error {
	if b == nil {
		return storageError("submit batch", treedb.ErrClosed)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.store == nil {
		return storageError("submit batch", treedb.ErrClosed)
	}
	if mode != CommitRelaxed && mode != CommitDurable {
		return errors.Join(ErrInvalidCommitMode, b.abortLocked())
	}
	if c == nil {
		return errors.Join(errors.New("mvcc: nil completion record"), b.abortLocked())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store != nil {
		return errors.Join(errors.New("mvcc: completion still owns a command"), b.abortLocked())
	}
	c.err = nil
	if b.err == nil && len(b.seen) == 0 {
		err := b.engine.Abort()
		b.store.mu.RUnlock()
		b.store, b.seen = nil, nil
		if err != nil {
			c.err = storageError("close empty batch", err)
		}
		return c.err
	}
	err := b.err
	if err == nil {
		if engineErr := b.engine.Submit(mode == CommitDurable, &c.engine); engineErr != nil {
			err = storageError("submit batch", engineErr)
		}
	}
	if err != nil {
		err = errors.Join(err, b.engine.Abort())
		b.store.mu.RUnlock()
		c.err = err
	} else {
		c.store = b.store
	}
	b.store, b.seen, b.err = nil, nil, nil
	return err
}

// Abort drops only an unassigned cut and releases its owners exactly once.
func (b *CommitBuilder) Abort() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.abortLocked()
}

func (b *CommitBuilder) abortLocked() error {
	if b.store == nil {
		return nil
	}
	err := b.engine.Abort()
	b.store.mu.RUnlock()
	b.store, b.seen, b.err = nil, nil, nil
	if err != nil {
		return storageError("abort batch", err)
	}
	return nil
}
