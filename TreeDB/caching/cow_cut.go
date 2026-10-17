package caching

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// cowBackendBasis is shared by successor cuts. A handoff captures one new
// backend snapshot before publication; readers never independently capture it.
type cowBackendBasis struct {
	logicalSeq      uint64
	refs            atomic.Int64
	snapshot        *backenddb.Snapshot
	lease           *memtable.COWExternalLease
	pins            []*rootpublication.IdentityPin
	metadata        *retainedalloc.Enrollment
	completionMu    sync.Mutex
	completed       bool
	cleanupErr      error
	primaryMetadata bool
	originalCustody *rootpublication.OriginalCleanupCustody
}

func (b *cowBackendBasis) retain() { b.refs.Add(1) }
func (b *cowBackendBasis) release() error {
	if b == nil || b.refs.Add(-1) != 0 {
		return nil
	}
	b.completionMu.Lock()
	snapshot := b.snapshot
	b.completionMu.Unlock()
	var err error
	if snapshot != nil {
		err = snapshot.Close()
	} else {
		b.SnapshotCleanupCompleted(nil)
	}
	b.completionMu.Lock()
	defer b.completionMu.Unlock()
	if b.cleanupErr != nil {
		return b.cleanupErr
	}
	return err
}

// All members are immutable after installation. refs is controlled by cutMu;
// dropping the last reference transfers bounded retirement to the caller.
type cowReadCut struct {
	visibleCommandLSN     uint64
	logicalSeq            uint64
	frozenLogicalSeq      uint64 // immutable coverage of frozen sources, excluding newer mutable effects
	frozenCommandLSN      uint64
	frozenSemanticRootSet any
	semanticRootSet       any // immutable semantic/catalog projection installed with this same raw cut
	refs                  int
	shards                []cowTable
	frozen                []cowTable // oldest to newest; every member precedes basis
	basis                 *cowBackendBasis
	sources               []memtable.Table
	domains               []rootDomainSnapshot
	lease                 *memtable.COWExternalLease
	cache                 *cowCache
}

func (c *cowReadCut) drain() error {
	for i := range c.shards {
		r := c.shards[i].root.Release()
		r.Drain()
	}
	for i := range c.frozen {
		r := c.frozen[i].root.Release()
		r.Drain()
	}
	err := c.basis.release()
	c.lease.Close()
	if c.cache.activeCuts.Add(-1) == 0 && c.cache.readClosed.Load() {
		c.cache.lease.Close()
	}
	return err
}

// cowCache serializes private preparation separately from the brief read latch.
// writeMu admission and the existing command barrier remain outside this owner.
type cowCache struct {
	journalBarrierActive                                                                           atomic.Bool
	journalBarrierEffects, journalBarrierBytes, journalBarrierReplacement, journalBarrierConflicts atomic.Uint64
	commitWaitNs, commitHoldNs, validationNs, tooOld                                               atomic.Uint64
	journalCount, journalBytes, journalFloor, journalPoints, journalRanges                         atomic.Uint64
	txnOwner                                                                                       *transactionowner.Owner
	journal                                                                                        *txnJournal
	writerMu                                                                                       sync.Mutex
	readMu                                                                                         sync.RWMutex
	readClosed                                                                                     atomic.Bool
	refreshRequired                                                                                atomic.Bool // maintenance changed the basis before refresh admission
	cutMu                                                                                          sync.Mutex
	cut                                                                                            *cowReadCut
	writers                                                                                        []*memtable.COWWriter
	budget                                                                                         *memtable.COWBudget
	maintenance                                                                                    *memtable.COWBudget
	prefix                                                                                         *rootpublication.Coordinator
	prefixLease                                                                                    *memtable.COWExternalLease
	closed                                                                                         bool
	lease                                                                                          *memtable.COWExternalLease
	activeCuts                                                                                     atomic.Int64
	captureCalls                                                                                   atomic.Uint64
	prepareCalls                                                                                   atomic.Uint64
	publications                                                                                   atomic.Uint64
	rollovers                                                                                      atomic.Uint64
	handoffs                                                                                       atomic.Uint64
	closeRetired                                                                                   []memtable.COWRetirement
	generationBase                                                                                 uint64           // exact identical empty-generation startup history charge
	handoff                                                                                        *cowFlushHandoff // serialized by the existing flush coordinator
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
	// One finite constructor/receipt/successor tranche is admitted before any
	// producer is exposed. Its capacity remains charged once in the parent.
	maintenanceBytes := min(limits.MaxInFlightBytes, limits.MaxTotalBytes, limits.MaxRetiredBytes) / 2
	maintenance, err := budget.ReserveMaintenance(maintenanceBytes)
	if err != nil {
		budget.Close()
		return nil, err
	}
	// Keep the established callback envelope and captured-cell admission ceiling.
	// The callback now captures the budget, scalar limit and SAME admitted basis
	// cell; completion does not discount the existing startup allocation charge.
	maxResources := limits.MaxResources
	cacheBytes := memtable.COWAllocationCharge(6*uint64(unsafe.Sizeof(uintptr(0)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof((*memtable.COWExternalLease)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof((*retainedalloc.Enrollment)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(int(0)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowCache{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof((*memtable.COWWriter)(nil)))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(memtable.COWRetirement{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(memShard{})))
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
	var admitted *cowBackendBasis
	snapshot, err := bounded.AcquireSnapshotWithAllocationAdmission(func(sizes backenddb.SnapshotAllocationSizes) error {
		if sizes.ValueLog.MapHint > maxResources || sizes.ValueLog.FileCount > maxResources {
			return memtable.ErrCOWCapacity
		}
		var e error
		admitted, e = admitCOWBackendBasis(maintenance, sizes)
		return e
	})
	if err == nil {
		err = admitted.adoptSnapshot(snapshot)
	}
	if err != nil {
		cleanupErr := admitted.release()
		cutLease.Close()
		cacheLease.Close()
		budget.Close()
		if err == backenddb.ErrSnapshotCapacity {
			err = memtable.ErrCOWCapacity
		}
		return nil, joinCOWCleanupError(err, cleanupErr)
	}
	c := &cowCache{budget: budget, maintenance: maintenance, writers: make([]*memtable.COWWriter, shards), lease: cacheLease, closeRetired: make([]memtable.COWRetirement, shards)}
	cut := &cowReadCut{refs: 1, shards: make([]cowTable, shards), basis: admitted, lease: cutLease, cache: c}
	for i := range c.writers {
		beforeHistory := budget.Stats().HistoryBytes
		w, e := memtable.NewCOWWriter(budget)
		if e != nil {
			err = e
			break
		}
		c.writers[i] = w
		root, resources, e := prepareCOWEmptyRoot(w, budget)
		if e != nil {
			err = e
			break
		}
		history := budget.Stats().HistoryBytes - beforeHistory
		if c.generationBase == 0 {
			c.generationBase = history
		} else if history != c.generationBase {
			panic("COW empty generation charge changed during startup")
		}
		cut.shards[i] = cowTable{root: root, shard: i, resources: resources, history: history}
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
		err = joinCOWCleanupError(err, cut.basis.release())
		cutLease.Close()
		cacheLease.Close()
		budget.Close()
		return nil, err
	}
	if err = snapshot.ActivatePrimaryMetadataProducer(); err != nil {
		for _, table := range cut.shards {
			r := table.root.Release()
			r.Drain()
		}
		for _, w := range c.writers {
			r := w.Close()
			r.Drain()
		}
		err = joinCOWCleanupError(err, cut.basis.release())
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
	return memtable.COWAllocationCharge(uint64(sizes.ValueLog.FileCount)*uint64(unsafe.Sizeof((*rootpublication.IdentityPin)(nil)))) + uint64(sizes.ValueLog.FileCount)*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.IdentityPin{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBackendBasis{}))) +
		cowSnapshotRetentionCharge(sizes)
}

func cowSnapshotRetentionCharge(sizes backenddb.SnapshotAllocationSizes) uint64 {
	return memtable.COWAllocationCharge(sizes.Wrapper) + memtable.COWAllocationCharge(sizes.PrimaryRoot) + memtable.COWAllocationCharge(sizes.PinSet) +
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
		memtable.COWAllocationCharge(uint64(shards+frozen)*uint64(unsafe.Sizeof(memtable.Table(nil)))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(rootDomainSnapshot{})))
}

// buildDomains prepares existing lookup/merge adapters before publication.
// Fixed vectors plus the bounded frontier are the only capture-visible metadata.
func (c *cowReadCut) buildDomains() {
	if cap(c.sources) < len(c.frozen)+len(c.shards) {
		c.sources = make([]memtable.Table, 0, len(c.frozen)+len(c.shards))
	} else {
		c.sources = c.sources[:0]
	}
	if cap(c.domains) < len(c.shards) {
		c.domains = make([]rootDomainSnapshot, len(c.shards))
	} else {
		c.domains = c.domains[:len(c.shards)]
	}
	// A key belongs to one fixed shard. Merge precedence between shards is
	// irrelevant, so point and iteration views share this preadmitted vector.
	// Within each shard frozen history remains oldest to newest, then mutable.
	for shard := range c.shards {
		start := len(c.sources)
		for i := range c.frozen {
			if c.frozen[i].shard == shard {
				c.sources = append(c.sources, &c.frozen[i])
			}
		}
		c.sources = append(c.sources, &c.shards[shard])
		c.domains[shard] = rootDomainSnapshot{immutables: c.sources[start:len(c.sources):len(c.sources)]}
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
	db.cow.captureCalls.Add(1)
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
		return nil, joinCOWCleanupError(err, db.cow.releaseCut(cut))
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
func (c *cowCache) releaseCut(cut *cowReadCut) error {
	if cut == nil {
		return nil
	}
	c.cutMu.Lock()
	cut.refs--
	last := cut.refs == 0
	c.cutMu.Unlock()
	if last {
		return cut.drain()
	}
	return nil
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
	c.prepareCalls.Add(1)
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
		if len(group) > 1 {
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
	next := &cowReadCut{visibleCommandLSN: old.visibleCommandLSN, logicalSeq: old.logicalSeq, frozenLogicalSeq: old.frozenLogicalSeq, frozenCommandLSN: old.frozenCommandLSN, frozenSemanticRootSet: old.frozenSemanticRootSet, semanticRootSet: old.semanticRootSet, refs: 1, shards: make([]cowTable, len(groups)), frozen: make([]cowTable, len(old.frozen)), basis: old.basis, lease: lease, cache: c}
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
			charge := p.prepared[i].Charge().History()
			if charge > ^uint64(0)-next.shards[i].history {
				panic("COW accepted history accounting overflow")
			}
			next.shards[i].history += charge
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
func (p *cowPreparedCut) drainCancelled() error {
	var cleanupErr error
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
		cleanupErr = p.next.basis.release()
	}
	for i := range p.retired {
		p.retired[i].Drain()
	}
	p.lease.Close()
	return cleanupErr
}
func (p *cowPreparedCut) publish() *cowReadCut {
	old, err := p.publishAccepted(rootpublication.AcceptedTicket{})
	if err != nil {
		panic(err)
	}
	return old
}

func (p *cowPreparedCut) publishAccepted(ticket rootpublication.AcceptedTicket) (*cowReadCut, error) {
	if p.consumed {
		panic("consumed COW cut")
	}
	var install rootpublication.AcceptedInstall
	if ticket.Valid() {
		var err error
		install, err = ticket.BeginInstall(rootpublication.AcceptedDescriptor{Sequence: p.next.logicalSeq})
		if err != nil {
			return nil, err
		}
		defer install.Abort()
	}
	for _, s := range p.prepared {
		if s != nil {
			s.Publish()
		}
	}
	c := p.cache
	c.publications.Add(1)
	c.cutMu.Lock()
	old := c.cut
	c.cut = p.next
	if ticket.Valid() {
		install.Commit()
	}
	c.activeCuts.Add(1)
	old.refs--
	last := old.refs == 0
	c.cutMu.Unlock()
	p.consumed = true
	if last {
		return old, nil
	}
	return nil, nil
}
func (c *cowCache) beginRead() bool {
	c.readMu.RLock()
	if c.readClosed.Load() {
		c.readMu.RUnlock()
		return false
	}
	return true
}
func (c *cowCache) endRead()     { c.readMu.RUnlock() }
func (c *cowCache) close() error { return c.closeWithBudget(true) }

// The DB shutdown path keeps the producer budget open through its final backend
// checkpoint and physical disposal. Standalone cache close keeps its existing
// terminal semantics.
func (c *cowCache) closeWithBudget(closeBudget bool) error {
	c.readMu.Lock()
	if c.readClosed.Swap(true) {
		c.readMu.Unlock()
		return nil
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
	// FIFO/index cleanup and admission release are finite but stay outside
	// the publication latch; writer exclusion still prevents mutation.
	if c.journal != nil {
		c.journal.Close()
		c.updateTxnStats()
	}
	for i, w := range c.writers {
		retired[i] = w.Close()
	}
	if closeBudget {
		c.budget.Close()
	}
	c.writerMu.Unlock()
	for i := range retired {
		retired[i].Drain()
	}
	var err error
	if h := c.handoff; h != nil {
		c.handoff = nil
		err = c.releaseCut(h.cut)
		err = joinCOWCleanupError(err, h.basis.release())
		if h.next != nil {
			h.next.lease.Close()
		}
		h.guard.Release()
		h.lease.Close()
	}
	if last {
		err = joinCOWCleanupError(err, old.drain())
	}
	if c.prefix != nil {
		err = joinCOWCleanupError(err, c.prefix.CloseAccepted())
		c.prefixLease.Close()
	}
	return err
}

// cowChangedSize updates logical flush accounting from changed keys only.
// Duplicate operations use the final staged record once, preserving last-write
// wins without a capture-time census or cumulative history ledger.
func cowChangedSize(old cowTable, next *memtable.COWRoot, group []memtable.COWMutation) int64 {
	size := old.size
	if len(group) == 1 {
		key := group[0].Key
		record, found := next.Get(key)
		if !found {
			panic("staged COW point disappeared")
		}
		if previous, found := old.root.Get(key); found {
			size -= cowRecordSize(previous)
		}
		return size + cowRecordSize(record)
	}
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

// admitCOWSnapshotRetention is shared by startup, handoff and dictionary readers.
// One producer owner/budget binding accounts the arena once, while each capture
// owns an enrollment. New producer growth must be admitted by every binding.
func admitCOWSnapshotRetention(budget *memtable.COWBudget, bytes uint64, s backenddb.SnapshotAllocationSizes) (*memtable.COWExternalLease, *retainedalloc.Enrollment, error) {
	var metadata *retainedalloc.Enrollment
	var err error
	if s.PrimaryMetadata != nil {
		metadata, err = retainedalloc.EnrollPair(s.PrimaryMetadata, s.RegistryMetadata, budget)
		if err != nil {
			return nil, nil, err
		}
	}
	lease, err := budget.AcquireExternal(bytes)
	if err != nil {
		metadata.Close()
		return nil, nil, err
	}
	return lease, metadata, nil
}
