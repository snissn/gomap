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
		if err := flushCollectionWriteDomain(c.db, owner); err != nil {
			return nil, err
		}
	}
}

func (c *Collection) drainCommandWALStageCoordinatorBeforeMutation() error {
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
		if err := flushCollectionWriteDomain(c.db, owner); err != nil {
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
			if err := c.flushBufferedWrites(); err != nil {
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
	unlock, err := c.lockCommandWALPublishCoordinator()
	if err != nil {
		return err
	}
	defer unlock()
	return c.db.PublishCommandWALNoop(intent, sync)
}

func (m *CollectionManager) lockCommandWALPublishCoordinator() (func(), error) {
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
		if err := flushCollectionWriteDomain(m.db, owner); err != nil {
			return nil, err
		}
	}
}

func (m *CollectionManager) publishCommandWALNoop(intent *backenddb.CommandWALIntent, sync bool) error {
	if m == nil || m.db == nil {
		return errCollectionDBNil
	}
	unlock, err := m.lockCommandWALPublishCoordinator()
	if err != nil {
		return err
	}
	defer unlock()
	return m.db.PublishCommandWALNoop(intent, sync)
}

func (m *CollectionManager) flushPendingCommandWALBeforeRawPublish() error {
	if m == nil || m.db == nil || !m.db.CommandWALEnabled() {
		return nil
	}
	unlock, err := m.lockCommandWALPublishCoordinator()
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
	unlock, err := m.lockCommandWALPublishCoordinator()
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
	unlock, err := c.lockCommandWALPublishCoordinator()
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
		state := db.State()
		if state == nil {
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

func (domain *collectionWriteDomain) pendingCommandWALCoverageIntentLocked(db *backenddb.DB) (*backenddb.CommandWALIntent, uint64, error) {
	if domain == nil || db == nil || !db.CommandWALEnabled() {
		return nil, 0, nil
	}
	first, last := domain.pendingCommandWALFirst, domain.pendingCommandWALLast
	if first == 0 || last == 0 {
		return nil, 0, nil
	}
	state := db.State()
	if state == nil {
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
	applied := last
	intent, err := backenddb.NewCommandWALCoverageIntent(applied, backenddb.CommandWALLSNRange{First: current + 1, Last: applied})
	if err != nil {
		return nil, 0, err
	}
	return intent, applied, nil
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
