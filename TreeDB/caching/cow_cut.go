package caching

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// cowBackendBasis is shared by successor cuts. A handoff captures one new
// backend snapshot before publication; readers never independently capture it.
type cowBackendBasis struct {
	refs     atomic.Int64
	snapshot *backenddb.Snapshot
	lease    *memtable.COWExternalLease
}

func newCOWBackendBasis(s *backenddb.Snapshot, lease *memtable.COWExternalLease) *cowBackendBasis {
	b := &cowBackendBasis{snapshot: s, lease: lease}
	b.refs.Store(1)
	return b
}
func (b *cowBackendBasis) retain() { b.refs.Add(1) }
func (b *cowBackendBasis) release() {
	if b.refs.Add(-1) == 0 && b.snapshot != nil {
		_ = b.snapshot.Close()
		b.lease.Close()
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
	lease   *memtable.COWExternalLease
	cache   *cowCache
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
	c.lease.Close()
	if c.cache.activeCuts.Add(-1) == 0 && c.cache.readClosed.Load() {
		c.cache.lease.Close()
	}
}

// cowCache serializes private preparation separately from the brief read latch.
// writeMu admission and the existing command barrier remain outside this owner.
type cowCache struct {
	writerMu     sync.Mutex
	readMu       sync.RWMutex
	readClosed   atomic.Bool
	cutMu        sync.Mutex
	cut          *cowReadCut
	writers      []*memtable.COWWriter
	budget       *memtable.COWBudget
	closed       bool
	lease        *memtable.COWExternalLease
	activeCuts   atomic.Int64
	closeRetired []memtable.COWRetirement
}

// Backend wrapper capture uses a separate lease: a basis can outlive every
// generation that existed when it was captured. Cache and cut allocations also
// have independent lifetimes rather than attaching them to an arbitrary shard.
func newCOWCache(provider backendSnapshotProvider, shards int, limits memtable.COWLimits) (*cowCache, error) {
	if provider == nil || shards < 1 {
		return nil, fmt.Errorf("COW cache requires backend and fixed shards")
	}
	budget, err := memtable.NewCOWBudget(limits)
	if err != nil {
		return nil, err
	}
	// The temporary admission closure and captured lease pointer are admitted
	// with the cache wrapper before constructing the callback.
	cacheBytes := memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(uintptr(0)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof((*memtable.COWExternalLease)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowCache{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof((*memtable.COWWriter)(nil)))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(memtable.COWRetirement{})))
	cacheLease, err := budget.AcquireExternal(cacheBytes)
	if err != nil {
		budget.Close()
		return nil, err
	}
	bounded, ok := provider.(interface {
		AcquireSnapshotWithAllocationAdmission(func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error)
	})
	if !ok {
		cacheLease.Close()
		budget.Close()
		return nil, ErrCOWUnsupported
	}
	cutLease, err := budget.AcquireExternal(cowCutCharge(shards, 0))
	if err != nil {
		cacheLease.Close()
		budget.Close()
		return nil, err
	}
	var basisLease *memtable.COWExternalLease
	basis, err := bounded.AcquireSnapshotWithAllocationAdmission(func(sizes backenddb.SnapshotAllocationSizes) error {
		if sizes.ValueLog.MapHint > limits.MaxResources || sizes.ValueLog.FileCount > limits.MaxResources {
			return memtable.ErrCOWCapacity
		}
		bytes := cowBasisCharge(sizes)
		var e error
		basisLease, e = budget.AcquireExternal(bytes)
		return e
	})
	if err != nil {
		cutLease.Close()
		basisLease.Close()
		cacheLease.Close()
		budget.Close()
		if err == backenddb.ErrSnapshotCapacity {
			err = memtable.ErrCOWCapacity
		}
		return nil, err
	}
	c := &cowCache{budget: budget, writers: make([]*memtable.COWWriter, shards), lease: cacheLease, closeRetired: make([]memtable.COWRetirement, shards)}
	cut := &cowReadCut{refs: 1, shards: make([]cowTable, shards), basis: newCOWBackendBasis(basis, basisLease), lease: cutLease, cache: c}
	for i := range c.writers {
		w, e := memtable.NewCOWWriter(budget)
		if e != nil {
			err = e
			break
		}
		c.writers[i] = w
		p, e := w.Prepare(nil, memtable.COWPrepareOptions{})
		if e != nil {
			err = e
			break
		}
		cut.shards[i] = cowTable{root: p.Publish(), shard: i}
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
		cut.basis.release()
		cutLease.Close()
		cacheLease.Close()
		budget.Close()
		return nil, err
	}
	cut.buildDomains()
	c.activeCuts.Store(1)
	c.cut = cut
	return c, nil
}
func cowBasisCharge(sizes backenddb.SnapshotAllocationSizes) uint64 {
	return memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBackendBasis{}))) +
		memtable.COWAllocationCharge(sizes.Wrapper) + memtable.COWAllocationCharge(sizes.PinSet) +
		memtable.COWAllocationCharge(sizes.Refs) + memtable.COWAllocationCharge(sizes.IDs) +
		uint64(sizes.PinRefCount)*memtable.COWAllocationCharge(sizes.PinRef) +
		memtable.COWAllocationCharge(sizes.ValueLog.Wrapper) + memtable.COWAllocationCharge(sizes.ValueLog.MapEnvelope) +
		uint64(sizes.ValueLog.FileCount)*memtable.COWAllocationCharge(sizes.ValueLog.FileWrapper) +
		memtable.COWAllocationCharge(sizes.ValueLog.PathEnvelope)
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
	snap, err := db.acquireCOWSnapshotWithError()
	if err != nil && db.notifyError != nil {
		db.notifyError(err)
	}
	return snap
}

func (db *DB) acquireCOWSnapshotWithError() (*Snapshot, error) {
	if !db.cow.beginRead() {
		return nil, backenddb.ErrClosed
	}
	defer db.cow.endRead()
	cut := db.cow.retainCut()
	if cut == nil {
		return nil, backenddb.ErrClosed
	}
	// Admission and allocation are outside cutMu. The temporary cut reference
	// prevents a concurrent publication from retiring the chosen anchor.
	pin, err := cut.shards[0].root.Acquire(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(Snapshot{}))))
	if err != nil {
		db.cow.releaseCut(cut)
		return nil, err
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
	return activateSnapshot(snap), nil
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
	lease    *memtable.COWExternalLease
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
	dedupBytes := uint64(0)
	for _, group := range groups {
		if len(group) > 0 {
			dedupBytes += memtable.COWAllocationCharge(uint64(len(group))*128 + 4096)
		}
	}
	// The deduplication map borrows staged owned key strings; it never copies
	// payloads or traverses unchanged history. Conservative per-entry map
	// capacity and header allowance are admitted before allocation.
	bytes := dedupBytes + cowCutCharge(len(groups), len(old.frozen)) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPreparedCut{}))) +
		memtable.COWAllocationCharge(uint64(len(groups))*uint64(unsafe.Sizeof((*memtable.COWPrepared)(nil)))) +
		memtable.COWAllocationCharge(uint64(len(groups))*uint64(unsafe.Sizeof(memtable.COWRetirement{})))
	lease, err := c.budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	p := &cowPreparedCut{cache: c, prepared: make([]*memtable.COWPrepared, len(groups)), retired: make([]memtable.COWRetirement, len(groups)), lease: lease}
	for i := range groups {
		if len(groups[i]) == 0 {
			continue
		}
		staged, err := c.writers[i].Prepare(groups[i], opts[i])
		if err != nil {
			p.cancel()
			return p, err
		}
		p.prepared[i] = staged
	}
	next := &cowReadCut{refs: 1, shards: make([]cowTable, len(groups)), frozen: make([]cowTable, len(old.frozen)), basis: old.basis, lease: lease, cache: c}
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
			next.shards[i].size = cowChangedSize(old.shards[i], next.shards[i].root, groups[i])
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
	p.lease.Close()
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
	c.activeCuts.Add(1)
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
	retired := c.closeRetired
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

// cowChangedSize updates logical flush accounting from changed keys only.
// Duplicate operations use the final staged record once, preserving last-write
// wins without a capture-time census or cumulative history ledger.
func cowChangedSize(old cowTable, next *memtable.COWRoot, group []memtable.COWMutation) int64 {
	size := old.size
	seen := make(map[string]struct{}, len(group))
	for _, mutation := range group {
		record, found := next.Get(mutation.Key)
		if !found {
			panic("staged COW point disappeared")
		}
		if _, duplicate := seen[record.Key]; duplicate {
			continue
		}
		seen[record.Key] = struct{}{}
		if previous, found := old.root.Get(mutation.Key); found {
			size -= cowRecordSize(previous)
		}
		size += cowRecordSize(record)
	}
	return size
}
func cowRecordSize(record memtable.COWRecord) int64 {
	size := len(record.Key)
	if record.Flags&node.FlagPointer != 0 {
		size += page.ValuePtrSize + len(record.Value)
	} else if record.Flags&node.FlagTombstone == 0 {
		size += len(record.Value)
	}
	return int64(size)
}
