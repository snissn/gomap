package mvcc

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

type logicalMVCCBuilderOwner interface {
	BeginLogicalMVCC(*treedb.LogicalMVCCBuilder) error
	SupportsLogicalMVCCStaging() bool
}

// Scratch reuse is a retention policy, not an admission limit. A large cut may
// grow these structures, but cannot pin its largest allocation in an idle builder.
const commitBuilderScratchRows = (64 << 10) / int(unsafe.Sizeof(mvccadmission.LogicalMutation{}))

type commitIdentity struct {
	key       string
	timestamp uint64
}

// CommitBuilder owns a single unassigned atomic cut. AppendGroup validates and
// copies operands incrementally; Submit performs root-dependent preparation once.
// Begin retains discard-floor/lifecycle exclusion until Submit or Abort. No
// accepted prefix or ticket exists before Submit. Never copy an active builder.
type CommitBuilder struct {
	mu        sync.Mutex
	store     *Store
	engine    treedb.LogicalMVCCBuilder
	seen      map[commitIdentity]struct{}
	seenPeak  int
	mutations []mvccadmission.LogicalMutation
	err       error
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
	b.store, b.err = s, nil
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
	n := len(group.Mutations)
	if cap(b.mutations) < n {
		capacity := max(n, 2*cap(b.mutations))
		b.mutations = make([]mvccadmission.LogicalMutation, n, capacity)
	}
	mutations := b.mutations[:n]
	if b.seen == nil && n != 0 {
		b.seen = make(map[commitIdentity]struct{}, n)
		b.seenPeak = n
	}
	inserted, staged := 0, false
	defer func() {
		// These identities were absent before this call. Roll back only its
		// additions, preserving prior groups even if this group duplicates one.
		if !staged {
			for _, m := range mutations[:inserted] {
				delete(b.seen, commitIdentity{string(m.Key), m.Timestamp})
			}
		}
		// LogicalInput/Append borrow this vector only through native staging;
		// the batch retains its own copied keys and values, never these slices.
		clear(mutations)
		b.mutations = mutations[:0]
		if cap(b.mutations) > commitBuilderScratchRows {
			b.mutations = nil
		}
	}()
	for i, m := range group.Mutations {
		if _, err := mvcckey.EncodedLen(m.Key); err != nil {
			return fmt.Errorf("%w at key index %d: %w", ErrInvalidKey, i, err)
		}
		id := commitIdentity{string(m.Key), group.Timestamp}
		if _, exists := b.seen[id]; exists {
			return fmt.Errorf("%w at key index %d", ErrDuplicateKey, i)
		}
		b.seen[id] = struct{}{}
		b.seenPeak = max(b.seenPeak, len(b.seen))
		mutations[i] = mvccadmission.LogicalMutation{Key: m.Key, Value: m.Value, Timestamp: group.Timestamp, Delete: m.Delete}
		inserted++
	}
	if len(group.Mutations) != 0 && group.Timestamp <= b.store.discardFloor {
		return fmt.Errorf("%w: timestamp %d is not above floor %d", ErrVersionBelowDiscardFloor, group.Timestamp, b.store.discardFloor)
	}
	if err := b.engine.Append(mutations, b.store.admission.LogicalInput(mutations, b.store.discardFloor)); err != nil {
		b.err = storageError("stage batch", err)
		return b.err
	}
	staged = true
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
		b.store = nil
		b.resetScratchLocked()
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
	b.store, b.err = nil, nil
	b.resetScratchLocked()
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
	b.store, b.err = nil, nil
	b.resetScratchLocked()
	if err != nil {
		return storageError("abort batch", err)
	}
	return nil
}

// resetScratchLocked releases identity strings at the cut boundary. The peak
// includes rolled-back groups: an oversized invalid input must not leave a large
// empty map retained by an otherwise small reusable builder.
func (b *CommitBuilder) resetScratchLocked() {
	if b.seenPeak > commitBuilderScratchRows {
		b.seen = nil
	} else {
		clear(b.seen)
	}
	b.seenPeak = 0
}
