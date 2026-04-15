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

// SnapshotPool manages a pool of Snapshot objects to reduce allocation overhead.
type SnapshotPool struct {
	pool sync.Pool
}

func NewSnapshotPool() *SnapshotPool {
	return &SnapshotPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Snapshot{registryShardHint: snapshotShardHintUnset}
			},
		},
	}
}

func (p *SnapshotPool) Get() *Snapshot {
	return p.pool.Get().(*Snapshot)
}

func (p *SnapshotPool) Put(s *Snapshot) {
	if s == nil {
		return
	}
	s.db = nil
	s.idx = nil
	s.state = nil
	s.vlogManager = nil
	s.vlogPinned = false
	s.leafGenerationIDs = s.leafGenerationIDs[:0]
	s.reader = valueReader{}
	s.registryID = 0
	s.closed.Store(false)
	// treePager/treeRoot are intentionally preserved as a pooled cache key for
	// the next AcquireSnapshot() on this same object. registryShardHint is also
	// preserved so the same pooled Snapshot keeps a stable fast registry shard.
	// The reader backing address is stable per pooled Snapshot object, so Reset
	// can be skipped safely when pager+root are unchanged.
	p.pool.Put(s)
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
