package caching

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// cowBackendBasis is shared by successor cuts. A handoff captures one new
// backend snapshot before publication; readers never independently capture it.
type cowBackendBasis struct {
	refs     atomic.Int64
	snapshot *backenddb.Snapshot
}

func newCOWBackendBasis(s *backenddb.Snapshot) *cowBackendBasis {
	b := &cowBackendBasis{snapshot: s}
	b.refs.Store(1)
	return b
}
func (b *cowBackendBasis) retain() { b.refs.Add(1) }
func (b *cowBackendBasis) release() {
	if b.refs.Add(-1) == 0 && b.snapshot != nil {
		_ = b.snapshot.Close()
	}
}

// All members are immutable after installation. refs is controlled by cutMu;
// dropping the last reference transfers bounded retirement to the caller.
type cowReadCut struct {
	refs    int
	shards  []cowTable
	frozen  []cowTable // oldest to newest; every member precedes basis
	basis   *cowBackendBasis
	sources []memtable.Table
	domains []rootDomainSnapshot
}

func (c *cowReadCut) drain() {
	for i := range c.shards {
		r := c.shards[i].root.Release()
		r.Drain()
	}
	for i := range c.frozen {
		r := c.frozen[i].root.Release()
		r.Drain()
	}
	c.basis.release()
}

// cowCache serializes private preparation separately from the brief read latch.
// writeMu admission and the existing command barrier remain outside this owner.
type cowCache struct {
	writerMu   sync.Mutex
	readMu     sync.RWMutex
	readClosed atomic.Bool
	cutMu      sync.Mutex
	cut        *cowReadCut
	writers    []*memtable.COWWriter
	budget     *memtable.COWBudget
	closed     bool
}

func newCOWCache(basis *backenddb.Snapshot, shards int, limits memtable.COWLimits) (*cowCache, error) {
	if basis == nil || shards < 1 {
		return nil, fmt.Errorf("COW cache requires a backend basis and fixed shards")
	}
	budget, err := memtable.NewCOWBudget(limits)
	if err != nil {
		return nil, err
	}
	c := &cowCache{budget: budget, writers: make([]*memtable.COWWriter, shards)}
	cut := &cowReadCut{refs: 1, shards: make([]cowTable, shards), basis: newCOWBackendBasis(basis)}
	// Charge cache, vectors, basis and cut headers before allocating the first
	// owned root. The caller transfers basis ownership only on success.
	extra := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowCache{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof((*memtable.COWWriter)(nil)))) +
		cowCutCharge(shards, 0) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBackendBasis{})))
	for i := range c.writers {
		w, e := memtable.NewCOWWriter(budget)
		if e != nil {
			err = e
			break
		}
		c.writers[i] = w
		opts := memtable.COWPrepareOptions{}
		if i == 0 {
			opts.ExtraBytes = extra
		}
		p, e := w.Prepare(nil, opts)
		if e != nil {
			err = e
			break
		}
		cut.shards[i].root = p.Publish()
		cut.shards[i].shard = i
	}
	if err != nil {
		for i := range cut.shards {
			if cut.shards[i].root != nil {
				r := cut.shards[i].root.Release()
				r.Drain()
			}
		}
		for _, w := range c.writers {
			if w != nil {
				r := w.Close()
				r.Drain()
			}
		}
		budget.Close()
		return nil, err
	}
	cut.buildDomains()
	c.cut = cut
	return c, nil
}
func cowCutCharge(shards, frozen int) uint64 {
	return memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadCut{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(cowTable{}))) +
		memtable.COWAllocationCharge(uint64(frozen)*uint64(unsafe.Sizeof(cowTable{}))) +
		2*memtable.COWAllocationCharge(uint64(shards+frozen)*uint64(unsafe.Sizeof(memtable.Table(nil)))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(rootDomainSnapshot{})))
}

// buildDomains prepares existing lookup/merge adapters before publication.
// Fixed vectors plus the bounded frontier are the only capture-visible metadata.
func (c *cowReadCut) buildDomains() {
	c.sources = make([]memtable.Table, 0, len(c.frozen)+len(c.shards))
	for i := range c.frozen {
		c.sources = append(c.sources, &c.frozen[i])
	}
	for i := range c.shards {
		c.sources = append(c.sources, &c.shards[i])
	}
	c.domains = make([]rootDomainSnapshot, len(c.shards))
	pointSources := make([]memtable.Table, 0, len(c.sources))
	for shard := range c.shards {
		start := len(pointSources)
		for i := range c.frozen {
			if c.frozen[i].shard == shard {
				pointSources = append(pointSources, &c.frozen[i])
			}
		}
		pointSources = append(pointSources, &c.shards[shard])
		c.domains[shard] = rootDomainSnapshot{immutables: pointSources[start:len(pointSources):len(pointSources)]}
	}
}

func (db *DB) acquireCOWSnapshot() *Snapshot {
	if !db.cow.beginRead() {
		return nil
	}
	defer db.cow.endRead()
	cut := db.cow.retainCut()
	if cut == nil {
		return nil
	}
	// Admission and allocation are outside cutMu. The temporary cut reference
	// prevents a concurrent publication from retiring the chosen anchor.
	pin, err := cut.shards[0].root.Acquire(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(Snapshot{}))))
	if err != nil {
		db.cow.releaseCut(cut)
		if db.notifyError != nil {
			db.notifyError(err)
		}
		return nil
	}
	snap := getSnapshot()
	snap.db = db
	snap.cowCache = db.cow
	snap.cowCut = cut
	snap.cowPin = pin
	snap.backend = cut.basis.snapshot
	if state, ok := snap.backend.StateToken(); ok {
		snap.backendRootID = state.RootPageID
	}
	snap.backendFallback = backendSnapshotLookup{db: db, snapshot: snap.backend, rootID: snap.backendRootID}
	snap.rootPointShards = cut.domains
	snap.rootIterator = rootDomainSnapshot{immutables: cut.sources}
	return activateSnapshot(snap)
}

func (c *cowCache) retainCut() *cowReadCut {
	c.cutMu.Lock()
	defer c.cutMu.Unlock()
	if c.closed || c.cut == nil {
		return nil
	}
	c.cut.refs++
	return c.cut
}
func (c *cowCache) releaseCut(cut *cowReadCut) {
	c.cutMu.Lock()
	cut.refs--
	last := cut.refs == 0
	c.cutMu.Unlock()
	if last {
		cut.drain()
	}
}

// cowPreparedCut is built entirely before the frame. Unchanged roots and basis
// are retained while changed roots remain private until publish. There is no
// tree copy, allocation or callback in the installation latch.
type cowPreparedCut struct {
	cache    *cowCache
	next     *cowReadCut
	prepared []*memtable.COWPrepared
	consumed bool
	retired  []memtable.COWRetirement
}

func (c *cowCache) prepare(groups [][]memtable.COWMutation, opts []memtable.COWPrepareOptions) (*cowPreparedCut, error) {
	// The caller owns writerMu continuously through prepare/cancel/publication.
	old := c.cut
	if len(groups) != len(c.writers) || len(opts) != len(groups) {
		return nil, fmt.Errorf("COW fixed shard vector mismatch")
	}
	first := -1
	for i := range groups {
		if len(groups[i]) > 0 {
			first = i
			break
		}
	}
	if first < 0 {
		return nil, fmt.Errorf("empty COW publication")
	}
	// Reserve every cut/preparation/retirement pointer before allocating wrappers.
	opts[first].ExtraBytes += cowCutCharge(len(groups), len(old.frozen)) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPreparedCut{}))) +
		memtable.COWAllocationCharge(uint64(len(groups))*uint64(unsafe.Sizeof((*memtable.COWPrepared)(nil)))) +
		memtable.COWAllocationCharge(uint64(len(groups))*uint64(unsafe.Sizeof(memtable.COWRetirement{})))
	firstPrepared, err := c.writers[first].Prepare(groups[first], opts[first])
	if err != nil {
		return nil, err
	}
	p := &cowPreparedCut{cache: c, prepared: make([]*memtable.COWPrepared, len(groups)), retired: make([]memtable.COWRetirement, len(groups))}
	p.prepared[first] = firstPrepared
	for i := range groups {
		if i == first || len(groups[i]) == 0 {
			continue
		}
		staged, err := c.writers[i].Prepare(groups[i], opts[i])
		if err != nil {
			p.cancel()
			return p, err
		}
		p.prepared[i] = staged
	}
	next := &cowReadCut{refs: 1, shards: make([]cowTable, len(groups)), frozen: make([]cowTable, len(old.frozen)), basis: old.basis}
	next.basis.retain()
	copy(next.frozen, old.frozen)
	for i := range next.frozen {
		if !next.frozen[i].root.Retain() {
			panic("lost owned COW frozen root")
		}
	}
	for i := range groups {
		next.shards[i] = old.shards[i]
		if p.prepared[i] != nil {
			next.shards[i].root = p.prepared[i].Root()
		} else if !next.shards[i].root.Retain() {
			panic("lost owned COW root")
		}
	}
	next.buildDomains()
	p.next = next
	return p, nil
}

// cancel drops pending writer state; drain must follow after writer/admission
// release. Resource callbacks and backend closure never run under those locks.
func (p *cowPreparedCut) cancel() {
	if p.consumed {
		return
	}
	p.consumed = true
	for i, s := range p.prepared {
		if s != nil {
			p.retired[i] = s.Cancel()
		}
	}
}
func (p *cowPreparedCut) drainCancelled() {
	if p.next != nil {
		for i := range p.next.shards {
			if p.prepared[i] == nil {
				r := p.next.shards[i].root.Release()
				r.Drain()
			}
		}
		for i := range p.next.frozen {
			r := p.next.frozen[i].root.Release()
			r.Drain()
		}
		p.next.basis.release()
	}
	for i := range p.retired {
		p.retired[i].Drain()
	}
}
func (p *cowPreparedCut) publish() *cowReadCut {
	if p.consumed {
		panic("consumed COW cut")
	}
	for _, s := range p.prepared {
		if s != nil {
			s.Publish()
		}
	}
	c := p.cache
	c.cutMu.Lock()
	old := c.cut
	c.cut = p.next
	old.refs--
	last := old.refs == 0
	c.cutMu.Unlock()
	p.consumed = true
	if last {
		return old
	}
	return nil
}
func (c *cowCache) beginRead() bool {
	c.readMu.RLock()
	if c.readClosed.Load() {
		c.readMu.RUnlock()
		return false
	}
	return true
}
func (c *cowCache) endRead() { c.readMu.RUnlock() }
func (c *cowCache) close() {
	c.readMu.Lock()
	if c.readClosed.Swap(true) {
		c.readMu.Unlock()
		return
	}
	c.readMu.Unlock()
	retired := make([]memtable.COWRetirement, len(c.writers))
	c.writerMu.Lock()
	c.cutMu.Lock()
	c.closed = true
	old := c.cut
	c.cut = nil
	old.refs--
	last := old.refs == 0
	c.cutMu.Unlock()
	for i, w := range c.writers {
		retired[i] = w.Close()
	}
	c.budget.Close()
	c.writerMu.Unlock()
	for i := range retired {
		retired[i].Drain()
	}
	if last {
		old.drain()
	}
}
