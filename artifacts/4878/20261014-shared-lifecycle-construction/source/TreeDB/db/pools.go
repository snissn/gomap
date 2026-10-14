package db

import (
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
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
	// This private capture exposes no Snapshot/iterator and its one synchronous
	// owned-value read has returned. Close therefore completed the finalizer,
	// including SnapshotPool.Put's scrub before stablePagerCompletion. Repeat
	// neither that scrub nor failed-cleanup reuse here.
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
	s.primaryRebindRoot = primaryStateRootV5{}
	s.primaryRebindErr = nil
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
	retirementMu   sync.Mutex
	stopped        bool
	db             *DB
	metadata       *retainedalloc.Owner
	metadataCharge uint64
	ghostCharge    uint64
	mu             sync.Mutex
	ghosts         []ghostIndex
	stopCh         chan struct{}
	doneCh         chan struct{}
	stopOnce       sync.Once
}

func newIndexGhostManager(metadata *retainedalloc.Owner) (*indexGhostManager, error) {
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(indexGhostManager{})))
	if err := metadata.AddPending(charge); err != nil {
		return nil, err
	}
	return &indexGhostManager{metadata: metadata, metadataCharge: charge}, nil
}

func (m *indexGhostManager) closeOriginalGeneration(gen *indexGen) (*retainedalloc.Owner, uint64) {
	if gen == nil {
		return nil, 0
	}
	metadata, charge, err := gen.closeWithCompletionV6()
	if err != nil {
		m.db.retainOriginalCleanupError(gen.originalCloseFailure(err))
		m.db.quiesceFailedRetiredGeneration(gen)
	}
	gen = nil
	return metadata, charge
}

func (m *indexGhostManager) start(db *DB) {
	m.db = db
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

func (m *indexGhostManager) add(gen *indexGen) (*retainedalloc.Owner, uint64, error) {
	m.retirementMu.Lock()
	defer m.retirementMu.Unlock()
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return gen.closeWithCompletionV6()
	}
	defer m.mu.Unlock()
	if len(m.ghosts) == cap(m.ghosts) {
		capacity := cap(m.ghosts) * 2
		if capacity < 4 {
			capacity = 4
		}
		if capacity < cap(m.ghosts) || uint64(capacity) > ^uint64(0)/uint64(unsafe.Sizeof(ghostIndex{})) {
			return nil, 0, retainedalloc.ErrCapacity
		}
		charge := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(ghostIndex{})))
		if err := m.metadata.AddPending(charge); err != nil {
			return nil, 0, err
		}
		old, oldCharge := m.ghosts, m.ghostCharge
		next := make([]ghostIndex, len(old), capacity)
		copy(next, old)
		m.ghosts, m.ghostCharge = next, charge
		clear(old[:cap(old)])
		old = nil
		m.metadata.RemovePending(oldCharge)
	}
	m.ghosts = append(m.ghosts, ghostIndex{gen: gen, retiredAt: time.Now()})
	return nil, 0, nil
}

func (m *indexGhostManager) scavenge(maxAge time.Duration) {
	// The original queue backing doubles as the retirement batch. Serialize
	// growth until its tail is consumed, so no copied list or premature refund
	// appears and one partition remains linear in the number of generations.
	m.retirementMu.Lock()
	defer m.retirementMu.Unlock()
	m.mu.Lock()
	original := m.ghosts
	now := time.Now()
	kept, boundary := 0, len(original)
	for kept < boundary {
		ghost := original[kept]
		if now.Sub(ghost.retiredAt) <= maxAge || (ghost.gen.registry != nil && ghost.gen.registry.MinPinnedSeq() != math.MaxUint64) {
			kept++
			continue
		}
		boundary--
		original[kept], original[boundary] = original[boundary], original[kept]
	}
	m.ghosts = original[:boundary]
	m.mu.Unlock()
	for i := boundary; i < len(original); i++ {
		metadata, charge := m.closeOriginalGeneration(original[i].gen)
		original[i] = ghostIndex{}
		if charge != 0 {
			metadata.RemovePending(charge)
		}
	}
}

func (m *indexGhostManager) stopWithCompletionV6() (*retainedalloc.Owner, uint64) {
	stopCh := m.stopCh
	doneCh := m.doneCh
	if stopCh != nil {
		m.stopOnce.Do(func() { close(stopCh) })
	}
	if doneCh != nil {
		<-doneCh
	}
	return m.closeAllWithCompletionV6()
}

func (m *indexGhostManager) closeAllWithCompletionV6() (*retainedalloc.Owner, uint64) {
	m.retirementMu.Lock()
	m.mu.Lock()
	m.stopped = true
	original := m.ghosts
	m.ghosts = nil
	m.mu.Unlock()
	for i := range original {
		metadata, charge := m.closeOriginalGeneration(original[i].gen)
		original[i] = ghostIndex{}
		if charge != 0 {
			metadata.RemovePending(charge)
		}
	}
	clear(original[:cap(original)])
	original = nil
	m.mu.Lock()
	owner, charge := m.metadata, m.ghostCharge+m.metadataCharge
	m.metadata, m.ghostCharge, m.metadataCharge = nil, 0, 0
	m.stopCh, m.doneCh = nil, nil
	m.db = nil
	m.mu.Unlock()
	m.retirementMu.Unlock()
	return owner, charge
}
