package collections

import (
	"fmt"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type collectionCommandWALCoordinator struct {
	mu    sync.Mutex
	owner *collectionWriteDomain
}

var collectionCommandWALCoordinators sync.Map

func collectionCommandWALCoordinatorForDB(db *backenddb.DB) *collectionCommandWALCoordinator {
	if db == nil {
		return nil
	}
	if existing, ok := collectionCommandWALCoordinators.Load(db); ok {
		return existing.(*collectionCommandWALCoordinator)
	}
	coord := &collectionCommandWALCoordinator{}
	actual, loaded := collectionCommandWALCoordinators.LoadOrStore(db, coord)
	if !loaded {
		db.RegisterCloseHook(func() error {
			collectionCommandWALCoordinators.Delete(db)
			return nil
		})
		return coord
	}
	return actual.(*collectionCommandWALCoordinator)
}

func (domain *collectionWriteDomain) commandWALCoordinatorForDomain(db *backenddb.DB) *collectionCommandWALCoordinator {
	if domain == nil {
		return nil
	}
	if domain.commandWALCoordinator != nil {
		return domain.commandWALCoordinator
	}
	return collectionCommandWALCoordinatorForDB(db)
}

func collectionCommandWALDomainHasPending(domain *collectionWriteDomain) bool {
	if domain == nil {
		return false
	}
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	return domain.pendingCommandWALFirst != 0 && domain.pendingCommandWALLast != 0
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
			return coord.mu.Unlock, nil
		}
		if !collectionCommandWALDomainHasPending(owner) {
			coord.owner = nil
			coord.mu.Unlock()
			continue
		}
		coord.mu.Unlock()
		if err := flushCollectionWriteDomain(c.db, owner); err != nil {
			return nil, err
		}
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
		if !collectionCommandWALDomainHasPending(owner) {
			coord.owner = nil
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
		if !collectionCommandWALDomainHasPending(owner) {
			coord.owner = nil
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
		if !collectionCommandWALDomainHasPending(owner) {
			coord.owner = nil
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
		if lsn != state.AppliedCommandLSN+1 {
			return fmt.Errorf("%w: pending collection command WAL starts at %d after applied %d", backenddb.ErrCommandWALAppliedLSNNonContig, lsn, state.AppliedCommandLSN)
		}
		domain.pendingCommandWALFirst = lsn
		domain.pendingCommandWALLast = lsn
		if domain.commandWALCoordinator != nil {
			domain.commandWALCoordinator.owner = domain
		}
		return nil
	}
	if lsn != domain.pendingCommandWALLast+1 {
		return fmt.Errorf("%w: pending collection command WAL range [%d,%d] cannot cover interleaved lsn %d", backenddb.ErrCommandWALAppliedLSNNonContig, domain.pendingCommandWALFirst, domain.pendingCommandWALLast, lsn)
	}
	domain.pendingCommandWALLast = lsn
	if domain.commandWALCoordinator != nil {
		domain.commandWALCoordinator.owner = domain
	}
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
		return
	}
	domain.pendingCommandWALFirst = lsn + 1
}
