package db

import (
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
				return &Snapshot{}
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
	s.tree = nil
	s.registryID = 0
	p.pool.Put(s)
}

type ghostIndex struct {
	gen       *indexGen
	retiredAt time.Time
}

type indexGhostManager struct {
	mu     sync.Mutex
	ghosts []ghostIndex
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
	defer m.mu.Unlock()

	now := time.Now()
	var keep []ghostIndex
	for _, g := range m.ghosts {
		if now.Sub(g.retiredAt) > maxAge {
			_ = g.gen.close() // Close the underlying pager/mmap
		} else {
			keep = append(keep, g)
		}
	}
	m.ghosts = keep
}

func (m *indexGhostManager) closeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.ghosts {
		_ = g.gen.close()
	}
	m.ghosts = nil
}
