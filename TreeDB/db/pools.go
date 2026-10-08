package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"math"
	"sync"
	"time"
)

// Iterator is the internal interface for iteration.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	KeyCopy(dst []byte) []byte
	ValueCopy(dst []byte) []byte
	Close() error
	Error() error
	// Reset resets the iterator for reuse.
	Reset(start, end []byte)
}

// SnapshotPool centralizes Snapshot allocation and cleanup.
//
// Exported Snapshot handles are deliberately not reused. A caller can retain a
// pointer after Close, and reactivating that same address would let the stale
// alias operate on an unrelated snapshot. No generation stored in the reused
// object can distinguish those aliases.
type SnapshotPool struct{}

// Private point holders are fresh constructor-owned handles. Removing the
// process-wide pool adds actual Snapshot/capsule births; point performance is
// an explicit landing gate, never assumed from this source batch.
type oneShotRead struct{ snapshot *Snapshot }

func (db *DB) acquireOneShotReadOrErr() (*oneShotRead, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if err := db.publicationPoisonedError(); err != nil {
		return nil, err
	}
	db.valueLogPublicationMu.RLock()
	s, ok := db.captureSnapshotOwnedHolderRoleV1(nil, residentcredit.PrivatePointReadV1)
	db.valueLogPublicationMu.RUnlock()
	if !ok {
		return nil, ErrClosed
	}
	return &oneShotRead{snapshot: s}, nil
}
func (r *oneShotRead) close() (err error) {
	if r == nil || r.snapshot == nil {
		return nil
	}
	s := r.snapshot
	database := s.db
	c := s.originalCleanup
	if c == nil {
		return ErrClosed
	}
	if err = c.RetainOriginalCleanupV1(); err != nil {
		return err
	}
	defer func() {
		if !c.ObserveOriginalCleanupV1().Complete() {
			database.attachFailedSnapshotCleanupV1(c)
		}
		r.snapshot = nil
		r = nil
		s = nil
		database = nil
		c.ReleaseOriginalCleanupV1()
		c = nil
	}()
	err = s.Close()
	return
}

// Transfer the only point wrapper alias before final private backing release.
func closeOneShotReadV1(holder **oneShotRead) error {
	if holder == nil || *holder == nil {
		return nil
	}
	r := *holder
	*holder = nil
	holder = nil
	return r.close()
}

func NewSnapshotPool() *SnapshotPool {
	return &SnapshotPool{}
}

func (p *SnapshotPool) Get() *Snapshot {
	s := &Snapshot{registryShardHint: snapshotShardHintUnset}
	s.iteratorMu.Lock()
	s.generation.Add(1)
	s.closed.Store(true)
	s.iteratorMu.Unlock()
	return s
}

func (p *SnapshotPool) Put(s *Snapshot) {
	if s == nil {
		return
	}
	// A closed exported handle must retain neither embedded reader binding nor
	// Pager. This scrub precedes the actual intrinsic creator release.
	s.tree.Reset(nil, nil, 0)
	s.treePager = nil
	s.treeRoot = 0
	for i := range s.rootTrees {
		s.rootTrees[i].tree.Reset(nil, nil, 0)
		s.rootTrees[i].root = 0
	}
	s.rootTrees = nil
	creator := s.pagerCreator
	s.pagerCreator = nil
	s.db = nil
	s.idx = nil
	s.state = nil
	s.vlogManager = nil
	s.vlogPinned = false
	s.leafGenerationIDs = nil
	s.leafGenerationPinnedIDs = nil
	if cap(s.leafGenerationRefs) > 0 {
		clear(s.leafGenerationRefs[:cap(s.leafGenerationRefs)])
	}
	s.leafGenerationRefs = s.leafGenerationRefs[:0]
	s.leafGenerationPinSet = nil
	s.reader = valueReader{}
	s.registryID = 0
	s.iteratorMu.Lock()
	s.ownedPointRegistration = nil
	s.ownedPointCreatorCredit = nil
	s.ownedPointTerminalRetentions = nil
	s.ownedPointTerminalExecuting = false
	s.ownedPointTerminalStarted = false
	clear(s.iterators)
	s.readState.Store(snapshotReadClosedBit)
	s.closed.Store(true)
	// finalization is published only by the original capsule after control scrub.
	if s.originalCleanup == nil {
		s.finalized.Store(true)
	}
	s.iteratorMu.Unlock()
	if creator != nil {
		creator.ReleaseStableMetadata()
	}
	// Do not make the exported handle reusable. See SnapshotPool's contract.
}

type ghostIndex struct {
	gen       *indexGen
	retiredAt time.Time
}

type indexGhostManager struct {
	mu       sync.Mutex
	ghosts   []ghostIndex
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
	closeMu  sync.Mutex // serial terminal/scavenge; operations never need it
	stopped  bool
}

func (m *indexGhostManager) start() {
	m.stopCh = make(chan struct{})
	m.doneCh = make(chan struct{})
	go m.loop()
}

func (m *indexGhostManager) loop() {
	defer close(m.doneCh)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.scavenge(5 * time.Second)
		}
	}
}

func (m *indexGhostManager) add(gen *indexGen) {
	m.mu.Lock()
	stopped := m.stopped
	defer func() {
		m.mu.Unlock()
		if stopped {
			_ = m.closeAll()
		}
	}()
	for _, g := range m.ghosts {
		if g.gen == gen {
			return
		}
	}
	m.ghosts = append(m.ghosts, ghostIndex{
		gen:       gen,
		retiredAt: time.Now(),
	})
}

// closeEligible keeps each actual ghost until physical cleanup succeeds.
// No Manager lock is held over a Pager operation join or close IO.
func (m *indexGhostManager) closeEligible(maxAge time.Duration, all bool) error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	m.mu.Lock()
	now := time.Now()
	var selected []*indexGen
	for _, g := range m.ghosts {
		if !all && (now.Sub(g.retiredAt) <= maxAge || g.gen.registry != nil && g.gen.registry.MinPinnedSeq() != math.MaxUint64) {
			continue
		}
		selected = append(selected, g.gen)
	}
	m.mu.Unlock()
	var errs []error
	for _, gen := range selected {
		var err error
		if all {
			err = gen.closeForShutdownV1()
		} else {
			err = gen.close()
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// Physical Close is not the terminal certificate for a live allocator
		// writer/constructor. Ordinary scavenge must keep the SAME retry holder too.
		if !gen.constructorTerminalCompleteV1() {
			continue
		}
		m.mu.Lock()
		for i := range m.ghosts {
			if m.ghosts[i].gen == gen {
				copy(m.ghosts[i:], m.ghosts[i+1:])
				m.ghosts[len(m.ghosts)-1] = ghostIndex{}
				m.ghosts = m.ghosts[:len(m.ghosts)-1]
				break
			}
		}
		m.mu.Unlock()
	}
	return errors.Join(errs...)
}
func (m *indexGhostManager) scavenge(maxAge time.Duration) { _ = m.closeEligible(maxAge, false) }
func (m *indexGhostManager) stop() error {
	if m.stopCh != nil {
		m.stopOnce.Do(func() { close(m.stopCh) })
	}
	if m.doneCh != nil {
		<-m.doneCh
	}
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()
	return m.closeAll()
}
func (m *indexGhostManager) closeAll() error { return m.closeEligible(0, true) }

// finishClosedConstructorsV1 is called only after complete runtime shutdown
// cleanup. Failed/externally retained writers remain actual ghosts, not scalar
// debt, and their original creator remains held.
func (m *indexGhostManager) finishClosedConstructorsV1() {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	m.mu.Lock()
	gens := make([]*indexGen, 0, len(m.ghosts))
	for _, entry := range m.ghosts {
		gens = append(gens, entry.gen)
	}
	m.mu.Unlock()
	for _, gen := range gens {
		if !gen.handlesClosed.Load() {
			continue
		}
		gen.detachAllocatorWriterV1(true)
		gen.closeMu.Lock()
		gen.releaseConstructorCreatorLockedV1()
		gen.closeMu.Unlock()
		if !gen.constructorTerminalCompleteV1() {
			continue
		}
		m.mu.Lock()
		for i := range m.ghosts {
			if m.ghosts[i].gen == gen {
				copy(m.ghosts[i:], m.ghosts[i+1:])
				m.ghosts[len(m.ghosts)-1] = ghostIndex{}
				m.ghosts = m.ghosts[:len(m.ghosts)-1]
				break
			}
		}
		m.mu.Unlock()
	}
}
