package collections

import (
	"fmt"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type collectionCommandWALCoordinator struct {
	mu    sync.Mutex
	cond  *sync.Cond
	owner *collectionWriteDomain
}

var collectionCommandWALCoordinators sync.Map

func collectionCommandWALCoordinatorForDB(db *backenddb.DB) *collectionCommandWALCoordinator {
	if db == nil {
		return nil
	}
	coord := newCollectionCommandWALCoordinator()
	var actual any
	var loaded bool
	if _, ok := db.RegisterCloseHookIfOpenAfter(func() bool {
		actual, loaded = collectionCommandWALCoordinators.LoadOrStore(db, coord)
		return !loaded
	}, func() error {
		collectionCommandWALCoordinators.Delete(db)
		return nil
	}); !ok {
		return nil
	}
	if loaded {
		return actual.(*collectionCommandWALCoordinator)
	}
	return coord
}

func newCollectionCommandWALCoordinator() *collectionCommandWALCoordinator {
	coord := &collectionCommandWALCoordinator{}
	coord.cond = sync.NewCond(&coord.mu)
	return coord
}

func (coord *collectionCommandWALCoordinator) condLocked() *sync.Cond {
	if coord == nil {
		return nil
	}
	if coord.cond == nil {
		coord.cond = sync.NewCond(&coord.mu)
	}
	return coord.cond
}

func (domain *collectionWriteDomain) commandWALCoordinatorForDomain(db *backenddb.DB) *collectionCommandWALCoordinator {
	if domain == nil {
		return nil
	}
	if coord := domain.commandWALCoordinator.Load(); coord != nil {
		return coord
	}
	coord := collectionCommandWALCoordinatorForDB(db)
	if coord == nil {
		return nil
	}
	if domain.commandWALCoordinator.CompareAndSwap(nil, coord) {
		return coord
	}
	return domain.commandWALCoordinator.Load()
}

func collectionCommandWALDomainPendingLocked(domain *collectionWriteDomain) bool {
	if domain == nil {
		return false
	}
	return domain.pendingCommandWALFirst != 0 && domain.pendingCommandWALLast != 0
}

func collectionCommandWALDomainStageReserved(domain *collectionWriteDomain) bool {
	return domain != nil && domain.commandWALStageReservations.Load() > 0
}

func (domain *collectionWriteDomain) reserveCommandWALCoordinatorOwnerLocked() {
	if domain == nil {
		return
	}
	coord := domain.commandWALCoordinator.Load()
	if coord == nil {
		return
	}
	coord.mu.Lock()
	coord.owner = domain
	if cond := coord.condLocked(); cond != nil {
		cond.Broadcast()
	}
	coord.mu.Unlock()
}

func (domain *collectionWriteDomain) clearCommandWALCoordinatorOwnerIfNoPendingLocked() {
	if domain == nil || collectionCommandWALDomainPendingLocked(domain) {
		return
	}
	coord := domain.commandWALCoordinator.Load()
	if coord == nil {
		return
	}
	coord.mu.Lock()
	if collectionCommandWALDomainStageReserved(domain) {
		coord.mu.Unlock()
		return
	}
	if coord.owner == domain {
		coord.owner = nil
		if cond := coord.condLocked(); cond != nil {
			cond.Broadcast()
		}
	}
	coord.mu.Unlock()
}

func (domain *collectionWriteDomain) clearCommandWALCoordinatorOwnerIfNoPending() {
	if domain == nil {
		return
	}
	domain.mu.Lock()
	defer domain.mu.Unlock()
	domain.clearCommandWALCoordinatorOwnerIfNoPendingLocked()
}

func (c *Collection) lockCommandWALStageCoordinator() (func(), error) {
	return c.lockCommandWALStageCoordinatorWithRawPublishState(false)
}

func (c *Collection) lockCommandWALStageCoordinatorWithHeldRawPublishLock() (func(), error) {
	return c.lockCommandWALStageCoordinatorWithRawPublishState(true)
}

func (c *Collection) lockCommandWALStageCoordinatorWithRawPublishState(rawPublishLocked bool) (func(), error) {
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() || c.writeDomain == nil {
		return func() {}, nil
	}
	domain := c.writeDomain
	coord := domain.commandWALCoordinatorForDomain(c.db)
	if coord == nil {
		return func() {}, nil
	}
	for {
		coord.mu.Lock()
		owner := coord.owner
		if owner == nil || owner == domain {
			domain.commandWALStageReservations.Add(1)
			coord.owner = domain
			if cond := coord.condLocked(); cond != nil {
				cond.Broadcast()
			}
			coord.mu.Unlock()
			var unlockOnce sync.Once
			return func() {
				unlockOnce.Do(func() {
					domain.finishCommandWALStageReservation()
					domain.clearCommandWALCoordinatorOwnerIfNoPending()
				})
			}, nil
		}
		if collectionCommandWALDomainStageReserved(owner) {
			coord.waitForCommandWALStageReservationLocked(owner)
			coord.mu.Unlock()
			continue
		}
		coord.mu.Unlock()
		var err error
		if rawPublishLocked {
			err = flushCollectionWriteDomainWithHeldCommandWALRawPublishLock(c.db, owner)
		} else {
			err = flushCollectionWriteDomain(c.db, owner)
		}
		if err != nil {
			return nil, err
		}
	}
}

func (c *Collection) drainCommandWALStageCoordinatorBeforeMutation() error {
	return c.drainCommandWALStageCoordinatorBeforeMutationWithRawPublishState(false)
}

func (c *Collection) drainCommandWALStageCoordinatorBeforeMutationWithHeldRawPublishLock() error {
	return c.drainCommandWALStageCoordinatorBeforeMutationWithRawPublishState(true)
}

func (c *Collection) drainCommandWALStageCoordinatorBeforeMutationWithRawPublishState(rawPublishLocked bool) error {
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() || c.writeDomain == nil {
		return nil
	}
	domain := c.writeDomain
	coord := domain.commandWALCoordinatorForDomain(c.db)
	if coord == nil {
		return nil
	}
	for {
		coord.mu.Lock()
		owner := coord.owner
		if owner == nil || owner == domain {
			coord.mu.Unlock()
			return nil
		}
		if collectionCommandWALDomainStageReserved(owner) {
			coord.waitForCommandWALStageReservationLocked(owner)
			coord.mu.Unlock()
			continue
		}
		coord.mu.Unlock()
		var err error
		if rawPublishLocked {
			err = flushCollectionWriteDomainWithHeldCommandWALRawPublishLock(c.db, owner)
		} else {
			err = flushCollectionWriteDomain(c.db, owner)
		}
		if err != nil {
			return err
		}
	}
}

func (domain *collectionWriteDomain) finishCommandWALStageReservation() {
	if domain == nil {
		return
	}
	coord := domain.commandWALCoordinator.Load()
	if coord == nil {
		return
	}
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if next := domain.commandWALStageReservations.Add(-1); next < 0 {
		domain.commandWALStageReservations.Store(0)
		panic("collections: command WAL stage reservation underflow")
	}
	if cond := coord.condLocked(); cond != nil {
		cond.Broadcast()
	}
}

func (coord *collectionCommandWALCoordinator) waitForCommandWALStageReservationLocked(owner *collectionWriteDomain) {
	if coord == nil || owner == nil {
		return
	}
	cond := coord.condLocked()
	if cond == nil {
		return
	}
	for coord.owner == owner && collectionCommandWALDomainStageReserved(owner) {
		cond.Wait()
	}
}

func (c *Collection) lockCommandWALPublishCoordinator() (func(), error) {
	return c.lockCommandWALPublishCoordinatorWithRawPublishState(false)
}

func (c *Collection) lockCommandWALPublishCoordinatorWithHeldRawPublishLock() (func(), error) {
	return c.lockCommandWALPublishCoordinatorWithRawPublishState(true)
}

func (c *Collection) lockCommandWALPublishCoordinatorWithRawPublishState(rawPublishLocked bool) (func(), error) {
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() || c.writeDomain == nil {
		return func() {}, nil
	}
	domain := c.writeDomain
	coord := domain.commandWALCoordinatorForDomain(c.db)
	if coord == nil {
		return func() {}, nil
	}
	for {
		coord.mu.Lock()
		owner := coord.owner
		if owner == nil {
			return coord.mu.Unlock, nil
		}
		if collectionCommandWALDomainStageReserved(owner) {
			coord.waitForCommandWALStageReservationLocked(owner)
			coord.mu.Unlock()
			continue
		}
		coord.mu.Unlock()
		if owner == domain {
			var err error
			if rawPublishLocked {
				err = c.flushBufferedWritesWithRawPublishState(true)
			} else {
				err = c.flushBufferedWrites()
			}
			if err != nil {
				return nil, err
			}
		} else if rawPublishLocked {
			if err := flushCollectionWriteDomainWithHeldCommandWALRawPublishLock(c.db, owner); err != nil {
				return nil, err
			}
		} else if err := flushCollectionWriteDomain(c.db, owner); err != nil {
			return nil, err
		}
	}
}

func (c *Collection) lockCommandWALFlushPublishCoordinator(domain *collectionWriteDomain) (func(), error) {
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() || domain == nil {
		return func() {}, nil
	}
	coord := domain.commandWALCoordinatorForDomain(c.db)
	if coord == nil {
		return func() {}, nil
	}
	for {
		coord.mu.Lock()
		owner := coord.owner
		if owner == nil || owner == domain {
			return coord.mu.Unlock, nil
		}
		if collectionCommandWALDomainStageReserved(owner) {
			coord.waitForCommandWALStageReservationLocked(owner)
			coord.mu.Unlock()
			continue
		}
		coord.mu.Unlock()
		if err := flushCollectionWriteDomain(c.db, owner); err != nil {
			return nil, err
		}
	}
}

func (c *Collection) publishCommandWALNoop(intent *backenddb.CommandWALIntent, sync bool) error {
	if c == nil || c.db == nil {
		return errCollectionDBNil
	}
	return c.withCommandWALPublishCoordinatorForIntent(intent, func() error {
		return c.db.PublishStagedCommandWALNoop(intent, sync)
	})
}

func (m *CollectionManager) lockCommandWALPublishCoordinator() (func(), error) {
	return m.lockCommandWALPublishCoordinatorWithRawPublishState(false)
}

func (m *CollectionManager) lockCommandWALPublishCoordinatorWithHeldRawPublishLock() (func(), error) {
	return m.lockCommandWALPublishCoordinatorWithRawPublishState(true)
}

func (m *CollectionManager) lockCommandWALPublishCoordinatorWithRawPublishState(rawPublishLocked bool) (func(), error) {
	if m == nil || m.db == nil || !m.db.CommandWALEnabled() {
		return func() {}, nil
	}
	coord := m.commandWALCoordinator
	if coord == nil {
		coord = collectionCommandWALCoordinatorForDB(m.db)
	}
	if coord == nil {
		return func() {}, nil
	}
	for {
		coord.mu.Lock()
		owner := coord.owner
		if owner == nil {
			return coord.mu.Unlock, nil
		}
		if collectionCommandWALDomainStageReserved(owner) {
			coord.waitForCommandWALStageReservationLocked(owner)
			coord.mu.Unlock()
			continue
		}
		coord.mu.Unlock()
		var err error
		if rawPublishLocked {
			err = flushCollectionWriteDomainWithHeldCommandWALRawPublishLock(m.db, owner)
		} else {
			err = flushCollectionWriteDomain(m.db, owner)
		}
		if err != nil {
			return nil, err
		}
	}
}

func (m *CollectionManager) publishCommandWALNoop(intent *backenddb.CommandWALIntent, sync bool) error {
	if m == nil || m.db == nil {
		return errCollectionDBNil
	}
	return m.withCommandWALPublishCoordinatorForIntent(intent, func() error {
		return m.db.PublishStagedCommandWALNoop(intent, sync)
	})
}

func (m *CollectionManager) flushPendingCommandWALBeforeRawPublish() error {
	if m == nil || m.db == nil || !m.db.CommandWALEnabled() {
		return nil
	}
	unlock, err := m.lockCommandWALPublishCoordinatorWithHeldRawPublishLock()
	if err != nil {
		return err
	}
	unlock()
	return nil
}

func (m *CollectionManager) withCommandWALPublishCoordinator(fn func() error) error {
	if fn == nil {
		return nil
	}
	var unlockRaw func()
	lockHeldRaw := false
	if m != nil && m.db != nil && m.db.CommandWALEnabled() {
		unlockRaw = m.db.LockCommandWALPublish()
		defer unlockRaw()
		lockHeldRaw = true
	}
	var unlock func()
	var err error
	if lockHeldRaw {
		unlock, err = m.lockCommandWALPublishCoordinatorWithHeldRawPublishLock()
	} else {
		unlock, err = m.lockCommandWALPublishCoordinator()
	}
	if err != nil {
		return err
	}
	defer unlock()
	return fn()
}

func (m *CollectionManager) withCommandWALPublishCoordinatorForIntent(intent *backenddb.CommandWALIntent, fn func() error) error {
	if fn == nil {
		return nil
	}
	if intent == nil || !intent.StagedForPublish() {
		return m.withCommandWALPublishCoordinator(fn)
	}
	unlock, err := m.lockCommandWALPublishCoordinatorWithHeldRawPublishLock()
	if err != nil {
		return err
	}
	defer unlock()
	return fn()
}

func (c *Collection) withCommandWALPublishCoordinator(fn func() error) error {
	if fn == nil {
		return nil
	}
	lockHeldRaw := c != nil && c.commandWALRawPublishLocked
	var unlockRaw func()
	if !lockHeldRaw && c != nil && c.db != nil && c.db.CommandWALEnabled() {
		unlockRaw = c.db.LockCommandWALPublish()
		defer unlockRaw()
		lockHeldRaw = true
	}
	var unlock func()
	var err error
	if lockHeldRaw {
		unlock, err = c.lockCommandWALPublishCoordinatorWithHeldRawPublishLock()
	} else {
		unlock, err = c.lockCommandWALPublishCoordinator()
	}
	if err != nil {
		return err
	}
	defer unlock()
	return fn()
}

func (c *Collection) withCommandWALPublishCoordinatorForIntent(intent *backenddb.CommandWALIntent, fn func() error) error {
	if fn == nil {
		return nil
	}
	if intent == nil || !intent.StagedForPublish() {
		return c.withCommandWALPublishCoordinator(fn)
	}
	unlock, err := c.lockCommandWALPublishCoordinatorWithHeldRawPublishLock()
	if err != nil {
		return err
	}
	defer unlock()
	return fn()
}

func (domain *collectionWriteDomain) recordPendingCommandWALLSNLocked(db *backenddb.DB, lsn uint64) error {
	if domain == nil || lsn == 0 {
		return nil
	}
	if domain.pendingCommandWALFirst == 0 {
		if db == nil {
			return backenddb.ErrClosed
		}
		state, ok := db.StateToken()
		if !ok {
			return backenddb.ErrClosed
		}
		if lsn < state.AppliedCommandLSN {
			return fmt.Errorf("%w: pending collection command WAL lsn %d is behind applied %d", backenddb.ErrCommandWALAppliedLSNNonContig, lsn, state.AppliedCommandLSN)
		}
		if lsn == state.AppliedCommandLSN {
			return nil
		}
		if lsn != state.AppliedCommandLSN+1 {
			return fmt.Errorf("%w: pending collection command WAL starts at %d after applied %d", backenddb.ErrCommandWALAppliedLSNNonContig, lsn, state.AppliedCommandLSN)
		}
		domain.pendingCommandWALFirst = lsn
		domain.pendingCommandWALLast = lsn
		domain.reserveCommandWALCoordinatorOwnerLocked()
		return nil
	}
	if lsn != domain.pendingCommandWALLast+1 {
		return fmt.Errorf("%w: pending collection command WAL range [%d,%d] cannot cover interleaved lsn %d", backenddb.ErrCommandWALAppliedLSNNonContig, domain.pendingCommandWALFirst, domain.pendingCommandWALLast, lsn)
	}
	domain.pendingCommandWALLast = lsn
	domain.reserveCommandWALCoordinatorOwnerLocked()
	return nil
}

// recordPendingIndexedCommandWALLSNLocked assigns a buffered indexed write to
// the mutable interval that will move intact into its flush unit.
func (domain *collectionWriteDomain) recordPendingIndexedCommandWALLSNLocked(db *backenddb.DB, lsn uint64) error {
	if err := domain.recordPendingCommandWALLSNLocked(db, lsn); err != nil || lsn == 0 {
		return err
	}
	if domain.indexedMutableCommandWALFirst == 0 {
		domain.indexedMutableCommandWALFirst = lsn
		domain.indexedMutableCommandWALLast = lsn
	} else {
		if lsn != domain.indexedMutableCommandWALLast+1 {
			return fmt.Errorf("%w: indexed mutable command WAL range [%d,%d] cannot cover lsn %d", backenddb.ErrCommandWALAppliedLSNNonContig, domain.indexedMutableCommandWALFirst, domain.indexedMutableCommandWALLast, lsn)
		}
		domain.indexedMutableCommandWALLast = lsn
	}
	return domain.validateIndexedFlushUnitCommandWALOwnershipLocked(db)
}

func (domain *collectionWriteDomain) pendingCommandWALCoverageIntentLocked(db *backenddb.DB) (*backenddb.CommandWALIntent, uint64, error) {
	if domain == nil {
		return nil, 0, nil
	}
	return domain.pendingCommandWALCoverageIntentThroughLocked(db, domain.pendingCommandWALLast)
}

// pendingCommandWALCoverageIntentThroughLocked covers an owned prefix only;
// later queued or mutable frames remain pending.
func (domain *collectionWriteDomain) pendingCommandWALCoverageIntentThroughLocked(db *backenddb.DB, through uint64) (*backenddb.CommandWALIntent, uint64, error) {
	if domain == nil || db == nil || !db.CommandWALEnabled() {
		return nil, 0, nil
	}
	first, last := domain.pendingCommandWALFirst, domain.pendingCommandWALLast
	if first == 0 || last == 0 {
		return nil, 0, nil
	}
	state, ok := db.StateToken()
	if !ok {
		return nil, 0, backenddb.ErrClosed
	}
	current := state.AppliedCommandLSN
	if last <= current {
		domain.clearPendingCommandWALThroughLocked(current)
		return nil, 0, nil
	}
	if first > current+1 {
		return nil, 0, fmt.Errorf("%w: pending collection command WAL starts at %d after applied %d", backenddb.ErrCommandWALAppliedLSNNonContig, first, current)
	}
	if through < first || through > last {
		return nil, 0, fmt.Errorf("%w: pending collection command WAL range [%d,%d] cannot cover through %d", backenddb.ErrCommandWALAppliedLSNNonContig, first, last, through)
	}
	applied := through
	intent, err := backenddb.NewCommandWALCoverageIntent(applied, backenddb.CommandWALLSNRange{First: current + 1, Last: applied})
	if err != nil {
		return nil, 0, err
	}
	return intent, applied, nil
}

func (domain *collectionWriteDomain) validateIndexedFlushUnitCommandWALOwnershipLocked(db *backenddb.DB) error {
	if domain == nil {
		return nil
	}
	var current uint64
	if db != nil {
		state, ok := db.StateToken()
		if !ok {
			return backenddb.ErrClosed
		}
		current = state.AppliedCommandLSN
	}
	units := make([]indexedFlushUnit, 0, len(domain.indexedPublishingUnits)+len(domain.indexedFlushUnits)+1)
	units = append(units, domain.indexedPublishingUnits...)
	units = append(units, domain.indexedFlushUnits...)
	units = append(units, indexedFlushUnit{commandWALFirst: domain.indexedMutableCommandWALFirst, commandWALLast: domain.indexedMutableCommandWALLast})
	var first, last uint64
	for _, unit := range units {
		unitFirst, unitLast := unit.commandWALFirst, unit.commandWALLast
		if unitFirst == 0 && unitLast == 0 {
			continue
		}
		if unitFirst == 0 || unitLast < unitFirst || unitFirst != current+1 {
			return fmt.Errorf("%w: indexed flush command WAL interval [%d,%d] after applied %d", backenddb.ErrCommandWALAppliedLSNNonContig, unitFirst, unitLast, current)
		}
		if first == 0 {
			first = unitFirst
		}
		last = unitLast
		current = unitLast
	}
	if first == 0 {
		return nil
	}
	if domain.pendingCommandWALFirst != first || domain.pendingCommandWALLast != last {
		return fmt.Errorf("%w: indexed flush command WAL intervals [%d,%d] do not own pending range [%d,%d]", backenddb.ErrCommandWALAppliedLSNNonContig, first, last, domain.pendingCommandWALFirst, domain.pendingCommandWALLast)
	}
	return nil
}

func (domain *collectionWriteDomain) clearPendingCommandWALThroughLocked(lsn uint64) {
	if domain == nil || lsn == 0 {
		return
	}
	first, last := domain.pendingCommandWALFirst, domain.pendingCommandWALLast
	if first == 0 || last == 0 || lsn < first {
		return
	}
	if lsn >= last {
		domain.pendingCommandWALFirst = 0
		domain.pendingCommandWALLast = 0
		domain.clearCommandWALCoordinatorOwnerIfNoPendingLocked()
		return
	}
	domain.pendingCommandWALFirst = lsn + 1
}
