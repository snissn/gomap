package db

import (
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

// oneShotRead is never exposed to callers, callbacks, or iterators. Its owner
// performs exactly one synchronous owned-value read before returning it to the
// pool. Exported Snapshot handles must never enter this pool.
type oneShotRead struct {
	snapshot Snapshot
}

var oneShotReadPool = sync.Pool{New: func() any {
	return &oneShotRead{snapshot: Snapshot{registryShardHint: snapshotShardHintUnset}}
}}

func (db *DB) acquireOneShotReadOrErr() (*oneShotRead, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if err := db.publicationPoisonedError(); err != nil {
		return nil, err
	}
	r := oneShotReadPool.Get().(*oneShotRead)
	db.valueLogPublicationMu.RLock()
	ok := db.captureSnapshotWithValueLogPublicationLockHeld(&r.snapshot)
	db.valueLogPublicationMu.RUnlock()
	if !ok {
		oneShotReadPool.Put(r)
		return nil, ErrClosed
	}
	return r, nil
}

func (r *oneShotRead) close() error {
	// Close shares all registry, value-log, and leaf-generation release logic.
	// The read has returned, so there are no outstanding readers or iterators.
	err := r.snapshot.Close()
	if err != nil {
		// The exact Snapshot may now be linked to its original cleanup owner.
		// Reusing it would overwrite that owner's surviving physical debt.
		return err
	}
	// Do not keep an index pager or its reader interfaces alive in the pool.
	r.snapshot.tree.Reset(nil, nil, 0)
	r.snapshot.treePager = nil
	r.snapshot.treeRoot = 0
	oneShotReadPool.Put(r)
	return err
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
	// Exported handles are never reused: scrub all tree/pager aliases before
	// publishing completion of their original cleanup owner.
	s.tree.Reset(nil, nil, 0)
	s.treePager = nil
	s.treeRoot = 0
	for i := range s.rootTrees {
		s.rootTrees[i].tree.Reset(nil, nil, 0)
		s.rootTrees[i].root = 0
	}
	clear(s.rootTrees)
	s.rootTrees = nil
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
	clear(s.iterators)
	s.readState.Store(snapshotReadClosedBit)
	s.closed.Store(true)
	s.finalized.Store(false)
	s.iteratorMu.Unlock()
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
	defer m.mu.Unlock()
	m.ghosts = append(m.ghosts, ghostIndex{
		gen:       gen,
		retiredAt: time.Now(),
	})
}

func (m *indexGhostManager) scavenge(maxAge time.Duration) {
	m.mu.Lock()
	if len(m.ghosts) == 0 {
		m.mu.Unlock()
		return
	}

	now := time.Now()
	var keep []ghostIndex
	var toClose []*indexGen

	for _, g := range m.ghosts {
		if now.Sub(g.retiredAt) > maxAge {
			// Keep retired generations alive while any snapshot reader is still
			// registered on that generation.
			if g.gen != nil && g.gen.registry != nil && g.gen.registry.MinPinnedSeq() != math.MaxUint64 {
				keep = append(keep, g)
				continue
			}
			toClose = append(toClose, g.gen)
		} else {
			keep = append(keep, g)
		}
	}
	m.ghosts = keep
	m.mu.Unlock()

	for _, gen := range toClose {
		_ = gen.close()
	}
}

func (m *indexGhostManager) stop() {
	stopCh := m.stopCh
	doneCh := m.doneCh
	if stopCh != nil {
		m.stopOnce.Do(func() { close(stopCh) })
	}
	if doneCh != nil {
		<-doneCh
	}
	m.closeAll()
}

func (m *indexGhostManager) closeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.ghosts {
		_ = g.gen.close()
	}
	m.ghosts = nil
}
