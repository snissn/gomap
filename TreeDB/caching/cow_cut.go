package caching

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// cowBackendBasis is shared by successor cuts. A handoff captures one new
// backend snapshot before publication; readers never independently capture it.
type cowBackendBasis struct {
	readMu          sync.Mutex // serializes physical workspace reuse, never publication gates
	reader          *cowReadWorkspace
	_               [64 - unsafe.Sizeof(sync.Mutex{}) - unsafe.Sizeof(uintptr(0))]byte
	readSlots       [7]cowBasisReadSlot // bounded, opportunistic 64B-padded parallel traversal owners
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

type cowBasisReadSlot struct {
	mu     sync.Mutex
	reader *cowReadWorkspace
	_      [64 - unsafe.Sizeof(sync.Mutex{}) - unsafe.Sizeof(uintptr(0))]byte
}

func (b *cowBackendBasis) tryLockReadLane(seed uint32) (*sync.Mutex, **cowReadWorkspace, bool) {
	if b.readMu.TryLock() {
		return &b.readMu, &b.reader, true
	}
	n := uint32(len(b.readSlots))
	start := seed % n
	for i := uint32(0); i < n; i++ {
		slot := &b.readSlots[(start+i)%n]
		if slot.mu.TryLock() {
			return &slot.mu, &slot.reader, true
		}
	}
	return nil, nil, false
}

func (b *cowBackendBasis) lockReadLane(seed uint32) (*sync.Mutex, **cowReadWorkspace) {
	if mu, reader, ok := b.tryLockReadLane(seed); ok {
		return mu, reader
	}
	idx := seed % uint32(len(b.readSlots)+1)
	if idx == uint32(len(b.readSlots)) {
		b.readMu.Lock()
		return &b.readMu, &b.reader
	}
	slot := &b.readSlots[idx]
	slot.mu.Lock()
	return &slot.mu, &slot.reader
}

// physicalWorkspaceLocked borrows only the immutable backend snapshot. Each
// caller sets its original inspection limits under readMu. This workspace owns
// traversal and, for successor reads, bounded Snappy/LZ4 leaf decoding. Generic
// dictionary-free decoding belongs to the original cowCache.
func (b *cowBackendBasis) physicalWorkspaceLocked(budget *memtable.COWBudget, limits valuelog.COWReadLimits) (*cowReadWorkspace, error) {
	return b.workspaceLocked(&b.reader, budget, limits)
}

// The original basis retains authority; each slot owns only admitted scratch.
// Its own mutex protects both lazy admission and the caller's inspection limits.
func (b *cowBackendBasis) workspaceLocked(reader **cowReadWorkspace, budget *memtable.COWBudget, limits valuelog.COWReadLimits) (*cowReadWorkspace, error) {
	if *reader == nil {
		w, err := admitCOWReadWorkspace(budget, limits)
		if err != nil {
			return nil, err
		}
		w.basis = b.snapshot
		*reader = w
	}
	(*reader).limits = limits
	return *reader, nil
}

func (b *cowBackendBasis) retain() { b.refs.Add(1) }
func (b *cowBackendBasis) release() error {
	if b == nil || b.refs.Add(-1) != 0 {
		return nil
	}
	b.readMu.Lock()
	reader := b.reader
	b.reader = nil
	b.readMu.Unlock()
	if reader != nil {
		reader.close()
		reader = nil
	}
	for i := range b.readSlots {
		slot := &b.readSlots[i]
		slot.mu.Lock()
		reader, slot.reader = slot.reader, nil
		slot.mu.Unlock()
		if reader != nil {
			reader.close()
			reader = nil
		}
	}
	b.completionMu.Lock()
	snapshot, priorErr := b.snapshot, b.cleanupErr
	var refund *memtable.COWExternalLease
	if b.completed && b.cleanupErr == nil {
		refund, b.lease = b.lease, nil
	}
	b.completionMu.Unlock()
	// Close may synchronously complete and refund this SAME basis. Capture the
	// exact prior refusal first and never access b after the original callback.
	if snapshot != nil {
		return joinCOWCleanupError(priorErr, snapshot.Close())
	}
	if refund != nil {
		refund.Close()
	} else {
		b.SnapshotCleanupCompleted(nil)
	}
	return priorErr
}

const cowReadCutInlineShards = 4

// cowFrozenSet holds an immutable slice of frozen cowTable sources shared by
// reference across successor cuts until rollover or flush replaces the frozen
// frontier. It owns its own embedded maintenance lease so ordinary write cuts
// neither copy frozen slices nor re-retain frozen roots.
type cowFrozenSet struct {
	refs         atomic.Int64
	tables       []cowTable
	leaseStorage memtable.COWExternalLease
}

func cowFrozenSetCharge(capacity int) uint64 {
	if capacity <= 0 {
		return 0
	}
	return memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowFrozenSet{}))) +
		memtable.COWAllocationCharge(uint64(capacity)*uint64(unsafe.Sizeof(cowTable{})))
}

func admitCOWFrozenSet(budget *memtable.COWBudget, length, capacity int) (*cowFrozenSet, error) {
	if capacity <= 0 {
		return nil, nil
	}
	lease, err := budget.AcquireExternalValue(cowFrozenSetCharge(capacity))
	if err != nil {
		return nil, err
	}
	f := &cowFrozenSet{
		tables:       make([]cowTable, length, capacity),
		leaseStorage: lease,
	}
	f.refs.Store(1)
	return f, nil
}

func (f *cowFrozenSet) retain() {
	if f != nil {
		f.refs.Add(1)
	}
}

func (f *cowFrozenSet) release() {
	if f == nil || f.refs.Add(-1) != 0 {
		return
	}
	tables := f.tables
	f.tables = nil
	drainCOWTables(tables)
	lease := f.leaseStorage
	f.leaseStorage = memtable.COWExternalLease{}
	lease.Close()
}

// cowCutDomains holds lazily built snapshot point/iterator projections shared
// across concurrent snapshots pinned to the same compact write cut.
type cowCutDomains struct {
	refs    atomic.Int64
	sources []memtable.Table
	domains []rootDomainSnapshot
}

func (d *cowCutDomains) tryRetain() bool {
	for {
		cur := d.refs.Load()
		if cur <= 0 {
			return false
		}
		if d.refs.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func cowCutDomainsCharge(shards, frozen int) (uint64, bool) {
	if shards < 0 || frozen < 0 {
		return 0, false
	}
	totalSources := uint64(shards) + uint64(frozen)
	if totalSources < uint64(shards) {
		return 0, false
	}
	tableSize := uint64(unsafe.Sizeof(memtable.Table(nil)))
	if tableSize != 0 && totalSources > math.MaxUint64/tableSize {
		return 0, false
	}
	domainSize := uint64(unsafe.Sizeof(rootDomainSnapshot{}))
	if domainSize != 0 && uint64(shards) > math.MaxUint64/domainSize {
		return 0, false
	}
	c1 := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowCutDomains{})))
	c2 := memtable.COWAllocationCharge(totalSources * tableSize)
	c3 := memtable.COWAllocationCharge(uint64(shards) * domainSize)
	if c2 > math.MaxUint64-c1 {
		return 0, false
	}
	sum := c1 + c2
	if c3 > math.MaxUint64-sum {
		return 0, false
	}
	return sum + c3, true
}

// Authority members are immutable after installation; only the separately
// synchronized derived catalog and lazy snapshot-domain slots may initialize
// after installation. refs is managed atomically; dropping the last reference
// transfers bounded retirement to the caller.
type cowReadCut struct {
	visibleCommandLSN     uint64
	logicalSeq            uint64
	frozenLogicalSeq      uint64 // immutable coverage of frozen sources, excluding newer mutable effects
	frozenCommandLSN      uint64
	frozenSemanticRootSet backenddb.NativeSemanticState
	semanticRootSet       backenddb.NativeSemanticState // immutable semantic/catalog projection installed with this same raw cut
	catalogProjection     cowNativeCatalogProjection    // derived once from this exact cut; never semantic publication
	refs                  atomic.Int64
	shards                []cowTable
	frozen                []cowTable // oldest to newest; every member precedes basis
	frozenSet             *cowFrozenSet
	basis                 *cowBackendBasis
	sources               []memtable.Table
	domains               []rootDomainSnapshot
	domainsMu             sync.Mutex
	domainsCache          atomic.Pointer[cowCutDomains]
	lease                 *memtable.COWExternalLease
	leaseStorage          memtable.COWExternalLease
	cache                 *cowCache
	inlineShards          [cowReadCutInlineShards]cowTable
}

func (c *cowReadCut) initShards(shards int) {
	if shards <= cowReadCutInlineShards {
		c.shards = c.inlineShards[:shards:shards]
		return
	}
	c.shards = make([]cowTable, shards)
}

func (c *cowReadCut) lookupShardEntryWithRevision(shard int, key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	if val, ptr, flags, rev, found := c.shards[shard].GetEntryWithRevision(key); found {
		return val, ptr, flags, rev, true
	}
	for i := len(c.frozen) - 1; i >= 0; i-- {
		if c.frozen[i].shard == shard {
			if val, ptr, flags, rev, found := c.frozen[i].GetEntryWithRevision(key); found {
				return val, ptr, flags, rev, true
			}
		}
	}
	return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
}

func (c *cowReadCut) retainOrBuildDomains() *cowCutDomains {
	if d := c.domainsCache.Load(); d != nil && d.tryRetain() {
		return d
	}
	c.domainsMu.Lock()
	if d := c.domainsCache.Load(); d != nil && d.tryRetain() {
		c.domainsMu.Unlock()
		return d
	}
	sources := make([]memtable.Table, 0, len(c.frozen)+len(c.shards))
	domains := make([]rootDomainSnapshot, len(c.shards))
	for shard := range c.shards {
		start := len(sources)
		for i := range c.frozen {
			if c.frozen[i].shard == shard {
				sources = append(sources, &c.frozen[i])
			}
		}
		sources = append(sources, &c.shards[shard])
		domains[shard] = rootDomainSnapshot{immutables: sources[start:len(sources):len(sources)]}
	}
	d := &cowCutDomains{sources: sources, domains: domains}
	d.refs.Store(1)
	c.domainsCache.Store(d)
	c.domainsMu.Unlock()
	return d
}

func (c *cowReadCut) releaseDomains(d *cowCutDomains) {
	if d == nil || d.refs.Add(-1) != 0 {
		return
	}
	c.domainsCache.CompareAndSwap(d, nil)
	clear(d.sources)
	clear(d.domains)
	d.sources, d.domains = nil, nil
}

func (c *cowReadCut) drain() error {
	shards, frozen, frozenSet := c.shards, c.frozen, c.frozenSet
	c.shards, c.frozen, c.frozenSet = nil, nil, nil
	clear(c.sources)
	clear(c.domains)
	c.sources, c.domains = nil, nil
	c.domainsCache.Store(nil)
	drainCOWTables(shards)
	if frozenSet != nil {
		frozenSet.release()
	} else {
		drainCOWTables(frozen)
	}
	clear(c.inlineShards[:])
	basis := c.basis
	c.basis = nil
	err := basis.release()
	lease := c.lease
	var embeddedLease memtable.COWExternalLease
	if lease == &c.leaseStorage {
		embeddedLease = c.leaseStorage
		c.leaseStorage = memtable.COWExternalLease{}
		lease = &embeddedLease
	}
	c.lease = nil
	cache, semantic, frozenSemantic, projection := c.detachSemanticState()
	c.visibleCommandLSN = 0
	c.logicalSeq = 0
	c.frozenLogicalSeq = 0
	c.frozenCommandLSN = 0
	lease.Close()
	if cache != nil {
		cache.recycleCut(c)
		cache.finishSemanticStateRelease(semantic, frozenSemantic, projection)
	}
	return err
}

// Every table is only a borrowed projection of its independently owned root.
// Clear both root and resolver aliases before last-generation callbacks/refund.
func drainCOWTables(tables []cowTable) {
	for i := range tables {
		root := tables[i].root
		tables[i] = cowTable{}
		if root != nil {
			retired := root.Release()
			root = nil
			retired.Drain()
		}
	}
}

const (
	cowReadGateShards = 16
	cowReadGateMask   = cowReadGateShards - 1
)

type cowReadGateShard struct {
	mu  sync.RWMutex
	pad [64 - unsafe.Sizeof(sync.RWMutex{})]byte
}

type cowReadGate struct {
	shards [cowReadGateShards]cowReadGateShard
}

func (g *cowReadGate) rLockStripe(stripe uint32) {
	g.shards[stripe&cowReadGateMask].mu.RLock()
}

func (g *cowReadGate) rUnlockStripe(stripe uint32) {
	g.shards[stripe&cowReadGateMask].mu.RUnlock()
}

func (g *cowReadGate) tryRLockStripe(stripe uint32) bool {
	return g.shards[stripe&cowReadGateMask].mu.TryRLock()
}

func (g *cowReadGate) lockAll() {
	for i := range g.shards {
		g.shards[i].mu.Lock()
	}
}

func (g *cowReadGate) unlockAll() {
	for i := range g.shards {
		g.shards[i].mu.Unlock()
	}
}

func (g *cowReadGate) tryLockAll() bool {
	for i := range g.shards {
		if !g.shards[i].mu.TryLock() {
			for j := i - 1; j >= 0; j-- {
				g.shards[j].mu.Unlock()
			}
			return false
		}
	}
	return true
}

func cowReadStripe(key []byte) uint32 {
	n := len(key)
	if n == 0 {
		return 0
	}
	if n == 8 {
		x := binary.LittleEndian.Uint64(key)
		x ^= x >> 33
		x *= 0xff51afd7ed558ccd
		x ^= x >> 33
		return uint32(x) & cowReadGateMask
	}
	var h uint32 = 2166136261
	for _, b := range key {
		h ^= uint32(b)
		h *= 16777619
	}
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	return h & cowReadGateMask
}

// cowCache serializes private preparation separately from the brief read latch.
// writeMu admission and the existing command barrier remain outside this owner.
type cowCache struct {
	ordered bool
	// Lock order is scope/caller -> basis.readMu -> physicalMu -> budget.mu.
	// Pure decoder scratch never acquires a basis, scope or publication lock.
	physicalMu                                                                                     sync.Mutex
	physicalReader                                                                                 *cowReadWorkspace
	journalBarrierActive                                                                           atomic.Bool
	journalBarrierEffects, journalBarrierBytes, journalBarrierReplacement, journalBarrierConflicts atomic.Uint64
	commitWaitNs, commitHoldNs, validationNs, tooOld                                               atomic.Uint64
	journalCount, journalBytes, journalFloor, journalPoints, journalRanges                         atomic.Uint64
	txnOwner                                                                                       *transactionowner.Owner
	journal                                                                                        *txnJournal
	writerMu                                                                                       sync.Mutex
	readGate                                                                                       cowReadGate
	readClosed                                                                                     atomic.Bool
	refreshRequired                                                                                atomic.Bool // maintenance changed the basis before refresh admission
	cutMu                                                                                          sync.Mutex
	cut                                                                                            *cowReadCut
	liveCut                                                                                        atomic.Pointer[cowReadCut]
	recycledCut                                                                                    atomic.Pointer[cowReadCut]
	writers                                                                                        []*memtable.COWWriter
	successors                                                                                     []cowRawSuccessor
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
	inlinePointPublications                                                                        atomic.Uint64
	rollovers                                                                                      atomic.Uint64
	handoffs                                                                                       atomic.Uint64
	closeRetired                                                                                   []memtable.COWRetirement
	generationBase                                                                                 uint64           // exact identical empty-generation startup history charge
	handoff                                                                                        *cowFlushHandoff // serialized by the existing flush coordinator
	semanticMu                                                                                     sync.Mutex
	semanticCond                                                                                   sync.Cond
	semanticEnqueued, semanticCompleted                                                            uint64
	nativePressureMu                                                                               sync.Mutex // only before a native producer captures its opening
	nativePressureState                                                                            cowNativePressureState
	nativeProjectedRefusals                                                                        atomic.Uint64
	nativeProjectedReliefNs                                                                        atomic.Uint64
	reservationDrainFloor                                                                          atomic.Uint64
	nativePressureRemembered                                                                       bool
	semanticHead, semanticTail, semanticFailed                                                     *rootpublication.SemanticCleanup
	semanticRunning, semanticStopped, closeCleanupDone                                             bool
	flushWake                                                                                      chan struct{}
	pointStaging                                                                                   *cowPointStaging
}

// Backend wrapper capture uses a separate lease: a basis can outlive every
// generation that existed when it was captured. Cache and cut allocations also
// have independent lifetimes rather than attaching them to an arbitrary shard.
func (c *cowCache) newWriter(maintenance bool) (*memtable.COWWriter, error) {
	if maintenance {
		if c.ordered {
			return memtable.NewCOWOrderedWriterWithMaintenanceConstruction(c.budget, c.maintenance)
		}
		return memtable.NewCOWWriterWithMaintenanceConstruction(c.budget, c.maintenance)
	}
	if c.ordered {
		return memtable.NewCOWOrderedWriter(c.budget)
	}
	return memtable.NewCOWWriter(c.budget)
}

// SupportsLogicalMVCC identifies the concrete, opt-in logical-history route.
func (db *DB) SupportsLogicalMVCC() bool { return db != nil && db.cow != nil && db.cow.ordered }

func newCOWCache(provider backendSnapshotProvider, shards int, limits memtable.COWLimits) (*cowCache, error) {
	return newCOWCacheMode(provider, shards, limits, false)
}

func newCOWCacheMode(provider backendSnapshotProvider, shards int, limits memtable.COWLimits, ordered bool) (*cowCache, error) {
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
	cacheBytes += memtable.COWAllocationCharge(uint64(shards) * uint64(unsafe.Sizeof(cowRawSuccessor{})))
	cacheBytes += cowPointStagingCharge(shards)
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
	c := &cowCache{ordered: ordered, budget: budget, maintenance: maintenance, writers: make([]*memtable.COWWriter, shards), successors: make([]cowRawSuccessor, shards), lease: cacheLease, closeRetired: make([]memtable.COWRetirement, shards), pointStaging: newCOWPointStaging(shards)}
	c.semanticCond.L = &c.semanticMu
	cut := &cowReadCut{basis: admitted, lease: cutLease, cache: c}
	cut.refs.Store(1)
	cut.initShards(shards)
	for i := range c.writers {
		beforeHistory := budget.Stats().HistoryBytes
		w, e := c.newWriter(false)
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
	c.liveCut.Store(cut)
	return c, nil
}
func cowBasisCharge(sizes backenddb.SnapshotAllocationSizes) uint64 {
	// Pre-admit fixed caller/unwind cells with this containing capture owner.
	return memtable.COWAllocationCharge(6*uint64(unsafe.Sizeof(uintptr(0)))) + memtable.COWAllocationCharge(uint64(sizes.ValueLog.FileCount)*uint64(unsafe.Sizeof((*rootpublication.IdentityPin)(nil)))) + uint64(sizes.ValueLog.FileCount)*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.IdentityPin{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBackendBasis{}))) +
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
func cowWriteCutCharge(shards int) uint64 {
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadCut{})))
	if shards > cowReadCutInlineShards {
		bytes += memtable.COWAllocationCharge(uint64(shards) * uint64(unsafe.Sizeof(cowTable{})))
	}
	return bytes
}

func cowCutCharge(shards, frozen int) uint64 {
	return cowWriteCutCharge(shards) +
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
	stripe := uint32(db.cow.captureCalls.Add(1))
	if !db.cow.beginReadStripe(stripe) {
		return nil, backenddb.ErrClosed
	}
	defer db.cow.endReadStripe(stripe)
	cut := db.cow.retainCut()
	if cut == nil {
		return nil, backenddb.ErrClosed
	}
	return db.snapshotFromCOWCut(cut)
}

// snapshotFromCOWCut consumes the caller's retained cut, including on admission
// failure. The caller already holds the cache read gate; no recursive admission
// is allowed while a concurrent Close may be waiting for that gate.
func (db *DB) snapshotFromCOWCut(cut *cowReadCut) (*Snapshot, error) {
	// Admission and allocation are outside cutMu. The temporary cut reference
	// prevents a concurrent publication from retiring the chosen anchor.
	extra := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(Snapshot{})))
	lazyDomains := cut.domains == nil
	if lazyDomains {
		domBytes, ok := cowCutDomainsCharge(len(cut.shards), len(cut.frozen))
		if !ok || domBytes > math.MaxUint64-extra {
			return nil, joinCOWCleanupError(memtable.ErrCOWCapacity, db.cow.releaseCut(cut))
		}
		extra += domBytes
	}
	pin, err := cut.shards[0].root.Acquire(extra)
	if err != nil {
		return nil, joinCOWCleanupError(err, db.cow.releaseCut(cut))
	}
	snap := newActiveCOWSnapshot()
	snap.db = db
	snap.cowCache = db.cow
	snap.cowCut = cut
	snap.cowPin = pin
	snap.backend = cut.basis.snapshot
	if state, ok := snap.backend.StateToken(); ok {
		snap.backendRootID = state.RootPageID
	}
	snap.backendFallback = backendSnapshotLookup{db: db, snapshot: snap.backend, rootID: snap.backendRootID}
	if lazyDomains {
		d := cut.retainOrBuildDomains()
		snap.cowDomains = d
		snap.rootPointShards = d.domains
		snap.rootIterator = rootDomainSnapshot{immutables: d.sources}
	} else {
		snap.rootPointShards = cut.domains
		snap.rootIterator = rootDomainSnapshot{immutables: cut.sources}
	}
	return snap, nil
}

func (c *cowCache) retainCut() *cowReadCut {
	for {
		cut := c.liveCut.Load()
		if cut == nil {
			return c.retainCutSlow()
		}
		for {
			cur := cut.refs.Load()
			if cur > 0 {
				if cut.refs.CompareAndSwap(cur, cur+1) {
					return cut
				}
				continue
			}
			if cur == 0 {
				break // retired; reload liveCut
			}
			// cur < 0: ActivateNativeSemanticOwner holds cutMu
			return c.retainCutSlow()
		}
	}
}

func (c *cowCache) retainCutSlow() *cowReadCut {
	c.cutMu.Lock()
	defer c.cutMu.Unlock()
	if c.closed || c.cut == nil {
		return nil
	}
	c.cut.refs.Add(1)
	return c.cut
}

func (c *cowCache) releaseCut(cut *cowReadCut) error {
	if cut == nil {
		return nil
	}
	if cut.refs.Add(-1) == 0 {
		return cut.drain()
	}
	return nil
}

// cowPreparedCut is built entirely before the frame. Unchanged roots and basis
// are retained while changed roots remain private until publish. There is no
// tree copy, allocation or callback in the installation latch.
type cowPreparedCut struct {
	cache          *cowCache
	next           *cowReadCut
	opening        *cowReadCut
	native         *cowNativeOwner
	prepared       []*memtable.COWPrepared
	successors     []cowRawSuccessor // private first-dirty provisions until whole-cut install
	consumed       bool
	retired        []memtable.COWRetirement
	lease          *memtable.COWExternalLease
	inlinePrepared [cowReadCutInlineShards]*memtable.COWPrepared
	inlineRetired  [cowReadCutInlineShards]memtable.COWRetirement
}

func (c *cowCache) prepare(groups [][]memtable.COWMutation, opts []memtable.COWPrepareOptions) (*cowPreparedCut, error) {
	return c.prepareNative(groups, opts, nil)
}

// cowPreparedCutCharge matches the original publication cut and private vectors.
func cowPreparedCutCharge(shards, frozen int, needsSuccessors bool) uint64 {
	bytes := cowWriteCutCharge(shards) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPreparedCut{})))
	if shards > cowReadCutInlineShards {
		bytes += memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof((*memtable.COWPrepared)(nil)))) +
			memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(memtable.COWRetirement{})))
	}
	if needsSuccessors {
		bytes += memtable.COWAllocationCharge(uint64(shards) * uint64(unsafe.Sizeof(cowRawSuccessor{})))
	}
	return bytes
}

func (c *cowCache) prepareNative(groups [][]memtable.COWMutation, opts []memtable.COWPrepareOptions, native *cowNativeOwner) (*cowPreparedCut, error) {
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
	if first < 0 && native == nil {
		return nil, fmt.Errorf("empty COW publication")
	}
	needsSuccessors := false
	for i, group := range groups {
		if len(group) != 0 && c.successors[i].writer == nil {
			needsSuccessors = true
		}
	}
	bytes := cowPreparedCutCharge(len(groups), len(old.frozen), needsSuccessors)
	lease, err := c.budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	p := &cowPreparedCut{cache: c, lease: lease}
	if len(groups) <= cowReadCutInlineShards {
		p.prepared = p.inlinePrepared[:len(groups):len(groups)]
		p.retired = p.inlineRetired[:len(groups):len(groups)]
	} else {
		p.prepared = make([]*memtable.COWPrepared, len(groups))
		p.retired = make([]memtable.COWRetirement, len(groups))
	}
	if needsSuccessors {
		p.successors = make([]cowRawSuccessor, len(groups))
	}
	old.refs.Add(1)
	p.opening = old
	for i := range groups {
		if len(groups[i]) == 0 {
			continue
		}
		if c.successors[i].writer == nil {
			if err := c.writers[i].ReserveRawRolloverSource(); err != nil {
				p.cancel()
				return p, err
			}
			p.successors[i].sourceReserved = true
			var err error
			p.successors[i].writer, err = c.newWriter(true)
			if err == nil {
				p.successors[i].root, p.successors[i].resources, err = prepareCOWReservedEmptyRoot(p.successors[i].writer, c.maintenance)
			}
			if err != nil {
				p.cancel()
				return p, err
			}
		}
		staged, err := c.writers[i].Prepare(groups[i], opts[i])
		if err != nil {
			p.cancel()
			return p, err
		}
		p.prepared[i] = staged
		if delta, valid := staged.LogicalSizeDelta(); !valid {
			p.cancel()
			return p, memtable.ErrCOWCapacity
		} else if _, valid := cowApplySizeDelta(old.shards[i].size, delta); !valid {
			p.cancel()
			return p, memtable.ErrCOWCapacity
		}
	}
	next := c.takeRecycledCut()
	if next == nil {
		next = &cowReadCut{}
	}
	next.visibleCommandLSN = old.visibleCommandLSN
	next.logicalSeq = old.logicalSeq
	next.frozenLogicalSeq = old.frozenLogicalSeq
	next.frozenCommandLSN = old.frozenCommandLSN
	next.frozenSemanticRootSet = old.frozenSemanticRootSet
	next.semanticRootSet = old.semanticRootSet
	next.basis = old.basis
	next.lease = lease
	next.cache = c
	next.initShards(len(groups))
	next.basis.retain()
	retainCOWSemanticState(next)
	if old.frozenSet != nil {
		next.frozenSet = old.frozenSet
		next.frozenSet.retain()
		next.frozen = old.frozen
	} else if len(old.frozen) > 0 {
		next.frozen = make([]cowTable, len(old.frozen))
		copy(next.frozen, old.frozen)
		for i := range next.frozen {
			if !next.frozen[i].root.RetainOwned() {
				panic("lost owned COW frozen root")
			}
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
			// Preparation checked this scalar result before constructing the cut.
			// Duplicates already telescope through each private replacement.
			delta, _ := p.prepared[i].LogicalSizeDelta()
			next.shards[i].size += delta
		} else if !next.shards[i].root.RetainOwned() {
			panic("lost owned COW root")
		}
	}
	if native != nil {
		// Opening cut still retains the old edge, so this cannot run cleanup.
		if next.semanticRootSet != nil {
			if record := next.semanticRootSet.Release(); record != nil {
				panic("lost opening semantic edge")
			}
		}
		next.semanticRootSet = native.plan.State
		native.plan.State = nil
		p.native = native
	}
	p.next = next
	return p, nil
}

func cowApplySizeDelta(size, delta int64) (int64, bool) {
	if size < 0 || delta < -size || delta > math.MaxInt64-size {
		return 0, false
	}
	return size + delta, true
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
			// Cancel can immediately refund staging storage. The private table
			// borrows that exact root; it owns no independent root reference.
			if p.next != nil {
				p.next.shards[i] = cowTable{}
			}
			p.prepared[i] = nil
			p.retired[i] = s.Cancel()
		}
		if p.successors != nil && p.successors[i].sourceReserved {
			p.cache.writers[i].CancelRawRolloverSource()
			p.successors[i].sourceReserved = false
		}
	}
}
func (p *cowPreparedCut) drainCancelled() error {
	refund, err := p.drainCancelledWithCompletion()
	p = nil
	refund.Close()
	return err
}

// The original containing lease also prices p and private Next. Return it only
// after their aliases have ended, so a containing caller can detach its cut
// field before the final independent accounting action. A failed basis already
// retains its SAME original object and charge through its preborn registry
// custody; this completed private wrapper is not a second failure owner.
func (p *cowPreparedCut) drainCancelledWithCompletion() (*memtable.COWExternalLease, error) {
	var cleanupErr error
	next, c := p.next, p.cache
	p.next, p.cache, p.native = nil, nil, nil
	if next != nil {
		shards, frozen, frozenSet := next.shards, next.frozen, next.frozenSet
		next.shards, next.frozen, next.frozenSet = nil, nil, nil
		clear(next.sources)
		clear(next.domains)
		next.sources, next.domains = nil, nil
		next.domainsCache.Store(nil)
		drainCOWTables(shards)
		if frozenSet != nil {
			frozenSet.release()
		} else {
			drainCOWTables(frozen)
		}
		clear(next.inlineShards[:])
		c.releasePrivateSemanticState(next)
		basis := next.basis
		next.basis = nil
		next.cache, next.lease = nil, nil
		cleanupErr = basis.release()
		basis = nil // original failed basis, if any, remains in its own custody
	}
	next = nil
	p.prepared = nil
	clear(p.inlinePrepared[:])
	for i := range p.successors {
		p.successors[i].drain()
	}
	p.successors = nil
	for i := range p.retired {
		p.retired[i].Drain()
	}
	p.retired = nil
	clear(p.inlineRetired[:])
	opening := p.opening
	p.opening = nil
	cleanupErr = joinCOWCleanupError(cleanupErr, c.releaseCut(opening))
	opening, c = nil, nil
	refund := p.lease
	p.lease = nil
	return refund, cleanupErr
}

func (p *cowPreparedCut) publish() *cowReadCut {
	old, _, err := p.publishAccepted(rootpublication.AcceptedTicket{})
	if err != nil {
		panic(err)
	}
	return old
}

func (p *cowPreparedCut) publishAccepted(ticket rootpublication.AcceptedTicket) (*cowReadCut, bool, error) {
	if p.consumed {
		panic("consumed COW cut")
	}
	var install rootpublication.AcceptedInstall
	returnCustody := false
	if ticket.Valid() {
		var err error
		descriptor := rootpublication.AcceptedDescriptor{Sequence: p.next.logicalSeq}
		if p.native != nil {
			plan := p.native.plan
			descriptor.Projections = plan.Projections
			descriptor.Custody = p.native
			descriptor.SealTarget = p.native.sealTarget
			descriptor.AdmittedBytes = plan.AdmittedBytes
			descriptor.CleanupBytes = plan.CleanupBytes
			returnCustody = plan.ReturnCustodyOnInstall
		}
		install, err = ticket.BeginInstall(descriptor)
		if err != nil {
			return nil, false, err
		}
		defer install.Abort()
	}
	for _, s := range p.prepared {
		if s != nil {
			s.Publish()
		}
	}
	c := p.cache
	for i := range p.successors {
		if p.successors[i].writer != nil {
			if c.successors[i].writer != nil {
				panic("duplicate raw rollover successor")
			}
			c.successors[i] = p.successors[i]
			p.successors[i] = cowRawSuccessor{}
		}
	}
	c.publications.Add(1)
	c.cutMu.Lock()
	old := c.cut
	if p.opening != old {
		c.cutMu.Unlock()
		panic("COW opening cut changed")
	}
	c.cut = p.next
	if p.native != nil {
		p.native.plan = nil
		p.native = nil
	}
	if ticket.Valid() {
		if returnCustody {
			returnCustody = install.CommitReturningRootCustody()
		} else {
			install.Commit()
		}
	}
	c.activeCuts.Add(1)
	p.next.refs.Store(1)
	c.liveCut.Store(p.next)
	p.opening = nil
	last := old.refs.Add(-2) == 0
	c.cutMu.Unlock()
	p.consumed = true
	if last {
		return old, returnCustody, nil
	}
	return nil, returnCustody, nil
}
func (c *cowCache) beginRead() bool {
	return c.beginReadStripe(0)
}
func (c *cowCache) endRead() {
	c.endReadStripe(0)
}
func (c *cowCache) beginReadStripe(stripe uint32) bool {
	c.readGate.rLockStripe(stripe)
	if c.readClosed.Load() {
		c.readGate.rUnlockStripe(stripe)
		return false
	}
	return true
}
func (c *cowCache) endReadStripe(stripe uint32) {
	c.readGate.rUnlockStripe(stripe)
}
func (c *cowCache) takeRecycledCut() *cowReadCut {
	if c == nil {
		return nil
	}
	return c.recycledCut.Swap(nil)
}

func (c *cowCache) recycleCut(cut *cowReadCut) {
	if c == nil || cut == nil || c.readClosed.Load() {
		return
	}
	c.recycledCut.CompareAndSwap(nil, cut)
}

func (c *cowCache) clearRecycledCut() {
	if c != nil {
		c.recycledCut.Swap(nil)
	}
}

func (c *cowCache) close() error { return c.closeWithBudget(true) }

// The DB shutdown path keeps the producer budget open through its final backend
// checkpoint and physical disposal. Standalone cache close keeps its existing
// terminal semantics.
func (c *cowCache) closeWithBudget(closeBudget bool) error {
	c.readGate.lockAll()
	already := c.readClosed.Swap(true)
	c.readGate.unlockAll()
	if already {
		return nil
	}
	retired := c.closeRetired
	c.writerMu.Lock()
	c.cutMu.Lock()
	c.closed = true
	old := c.cut
	c.cut = nil
	c.liveCut.Store(nil)
	last := old.refs.Add(-1) == 0
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
		c.successors[i].drain()
	}
	var err error
	if h := c.handoff; h != nil {
		c.handoff = nil
		var refund *memtable.COWExternalLease
		refund, err = h.release(c)
		h = nil
		refund.Close()
	}
	if last {
		err = joinCOWCleanupError(err, old.drain())
	}
	if c.prefix != nil {
		prefix := c.prefix
		completion, closeErr := prefix.CloseAcceptedWithCompletion()
		err = joinCOWCleanupError(err, closeErr)
		// Physical/checkpoint users are withdrawn under the caller's flushMu.
		// Retained-cut/cleanup-only readers use the existing semantic latch.
		var prefixRefund *memtable.COWExternalLease
		c.semanticMu.Lock()
		if !prefix.AcceptedCleanupFailed() {
			c.prefix = nil
			prefixRefund, c.prefixLease = c.prefixLease, nil
		}
		c.semanticMu.Unlock()
		prefix = nil
		prefixRefund.Close()
		completion.Complete()
	}
	var refund *memtable.COWExternalLease
	c.semanticMu.Lock()
	c.closeCleanupDone = true
	if c.flushWake == nil && c.activeCuts.Load() == 0 && c.semanticHead == nil && c.semanticFailed == nil && (c.prefix == nil || !c.prefix.AcceptedCleanupFailed()) {
		refund, c.lease = c.lease, nil
	}
	c.semanticMu.Unlock()
	if refund != nil {
		c.closePhysicalReader()
		c.clearRecycledCut()
		if c.pointStaging != nil {
			c.pointStaging.clearAll()
			c.pointStaging = nil
		}
	}
	refund.Close()
	return err
}

// cowChangedSize updates logical flush accounting from changed keys only.
// Prepare applies these canonical mutations in order without changing their
// key/value sizes or flags. Use the final mutation directly instead of looking
// its record up again in the private postimage. The old root remains the sole
// source of the previous record's size.
func cowChangedSize(old cowTable, group []memtable.COWMutation) int64 {
	size := old.size
	if len(group) == 1 {
		mutation := group[0]
		if previous, found := old.root.Get(mutation.Key); found {
			size -= cowRecordSize(previous)
		}
		return size + cowMutationSize(mutation)
	}
	seen := make(map[string]struct{}, len(group))
	for i := len(group) - 1; i >= 0; i-- {
		mutation := group[i]
		// The staged operands remain immutable and owned throughout Prepare.
		// This local deduplication map borrows their keys only until return.
		key := conditionalBytesString(mutation.Key)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		if previous, found := old.root.Get(mutation.Key); found {
			size -= cowRecordSize(previous)
		}
		size += cowMutationSize(mutation)
	}
	return size
}

func cowMutationSize(mutation memtable.COWMutation) int64 {
	if mutation.Remove {
		return 0
	}
	return cowPointSize(len(mutation.Key), len(mutation.Value), mutation.Flags)
}

func cowRecordSize(record memtable.COWRecord) int64 {
	return cowPointSize(len(record.Key), len(record.Value), record.Flags)
}

func cowPointSize(keyBytes, valueBytes int, flags byte) int64 {
	size := keyBytes
	if flags&node.FlagPointer != 0 {
		size += page.ValuePtrSize + valueBytes
	} else if flags&node.FlagTombstone == 0 {
		size += valueBytes
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
