package caching

import (
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type memtableReclaimer struct {
	readers atomic.Int64
	mu      sync.Mutex
	pending []memtable.Table
	ch      chan struct{}
}

func (r *memtableReclaimer) start(stop <-chan struct{}) {
	r.ch = make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-r.ch:
				r.tryRecycle()
			case <-stop:
				return
			}
		}
	}()
}

func (r *memtableReclaimer) readEnter() {
	r.readers.Add(1)
}

func (r *memtableReclaimer) readExit() {
	if r.readers.Add(-1) == 0 {
		r.signal()
	}
}

func (r *memtableReclaimer) deferRecycle(mem memtable.Table) {
	if mem == nil {
		return
	}
	r.mu.Lock()
	r.pending = append(r.pending, mem)
	r.mu.Unlock()
	r.signal()
}

func (r *memtableReclaimer) signal() {
	if r.ch == nil {
		return
	}
	select {
	case r.ch <- struct{}{}:
	default:
	}
}

func (r *memtableReclaimer) tryRecycle() {
	if r.readers.Load() != 0 {
		return
	}
	r.mu.Lock()
	pending := r.pending
	r.pending = nil
	r.mu.Unlock()
	for _, mem := range pending {
		recycleMemtable(mem)
	}
}
