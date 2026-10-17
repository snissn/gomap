package caching

import (
	"fmt"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// cowRollover reserves the entire successor before freezing any source. The
// owner serializes it with writes; Drain runs after owner/admission release.
// Empty shards continue their existing generation rather than consuming a
// frozen-source slot or adding a useless frontier entry.
type cowRollover struct {
	cache     *cowCache
	next      *cowReadCut
	writers   []*memtable.COWWriter
	roots     []*memtable.COWRoot
	resources []*cowGenerationResources
	retired   []memtable.COWRetirement
	scratch   *memtable.COWExternalLease
	cutLease  *memtable.COWExternalLease
	old       *cowReadCut
	opening   *cowReadCut                   // native private cleanup queues before this cut leaves
	semantic  backenddb.NativeSemanticState // owned until successor adoption
	installed bool
}

func (c *cowCache) prepareRollover() (*cowRollover, error) {
	old := c.cut
	count := 0
	for _, table := range old.shards {
		if table.Len() > 0 {
			count++
		}
	}
	if count == 0 && old.logicalSeq == old.frozenLogicalSeq {
		return nil, nil
	}
	limits := c.budget.Limits()
	if c.budget.Stats().Sources > limits.MaxSources-count {
		return nil, memtable.ErrCOWCapacity
	}
	n := len(c.writers)
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowRollover{}))) +
		2*memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof((*memtable.COWWriter)(nil)))) +
		memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof((*cowGenerationResources)(nil)))) +
		memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof(memtable.COWRetirement{})))
	lease, err := c.maintenance.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	p := &cowRollover{cache: c, scratch: lease, writers: make([]*memtable.COWWriter, n), roots: make([]*memtable.COWRoot, n), resources: make([]*cowGenerationResources, n), retired: make([]memtable.COWRetirement, n)}
	p.cutLease, err = c.maintenance.AcquireExternal(cowCutCharge(n, len(old.frozen)+count))
	if err != nil {
		return p, err
	}
	for i, table := range old.shards {
		if table.Len() == 0 {
			continue
		}
		p.writers[i], err = memtable.NewCOWWriterWithMaintenanceConstruction(c.budget, c.maintenance)
		if err != nil {
			return p, err
		}
		root, resources, e := prepareCOWReservedEmptyRoot(p.writers[i], c.maintenance)
		if e != nil {
			return p, e
		}
		p.roots[i], p.resources[i] = root, resources
	}
	if old.semanticRootSet != nil {
		p.opening = c.retainCut()
		if p.opening == nil {
			return p, backenddb.ErrClosed
		}
		p.semantic, err = old.semanticRootSet.PrepareRollover()
		if err != nil {
			return p, err
		}
		if p.semantic == nil {
			return p, backenddb.ErrNativeSemanticUnsupported
		}
	}
	// All admitted history is precharged against potential retirement. Source
	// count is the only additional Freeze admission and is serialized here.
	for i, w := range p.writers {
		if w != nil {
			if err := c.writers[i].Freeze(); err != nil {
				return p, fmt.Errorf("COW rollover invariant: %w", err)
			}
		}
	}
	next := &cowReadCut{visibleCommandLSN: old.visibleCommandLSN, logicalSeq: old.logicalSeq, frozenLogicalSeq: old.logicalSeq, frozenCommandLSN: old.visibleCommandLSN, frozenSemanticRootSet: old.semanticRootSet, semanticRootSet: p.semantic, refs: 1, cache: c, lease: p.cutLease, basis: old.basis, shards: make([]cowTable, n), frozen: make([]cowTable, len(old.frozen), len(old.frozen)+count)}
	next.basis.retain()
	p.semantic = nil // next adopts the returned active successor exactly once
	if next.frozenSemanticRootSet != nil {
		next.frozenSemanticRootSet.Retain()
	}
	copy(next.frozen, old.frozen)
	for i := range next.frozen {
		if !next.frozen[i].root.Retain() {
			panic("lost COW frozen rollover root")
		}
	}
	for i, table := range old.shards {
		next.shards[i] = table
		if p.writers[i] == nil {
			if !table.root.Retain() {
				panic("lost empty COW rollover root")
			}
			continue
		}
		if !table.root.Retain() {
			panic("lost COW rollover source")
		}
		next.frozen = append(next.frozen, table)
		next.shards[i] = cowTable{shard: i, root: p.roots[i], resources: p.resources[i], history: c.generationBase}
	}
	next.buildDomains()
	p.next = next
	return p, nil
}

func (p *cowRollover) install() {
	c := p.cache
	c.rollovers.Add(1)
	for i, w := range p.writers {
		if w != nil {
			p.retired[i] = c.writers[i].Close()
			c.writers[i] = w
		}
	}
	c.cutMu.Lock()
	old := c.cut
	c.cut = p.next
	c.activeCuts.Add(1)
	old.refs--
	if old.refs == 0 {
		p.old = old
	}
	c.cutMu.Unlock()
	p.installed = true
}

func (p *cowRollover) drain() error {
	if p == nil {
		return nil
	}
	var cleanupErr error
	cache, opening, next, old := p.cache, p.opening, p.next, p.old
	p.cache, p.opening, p.next, p.old = nil, nil, nil, nil
	p.resources = nil // roots own these; end aliases before their final callbacks
	if p.installed {
		// The installed cut owns these edges; the preparation only borrowed them.
		next = nil
		p.cutLease = nil
		for i := range p.retired {
			p.retired[i].Drain()
		}
		if old != nil {
			cleanupErr = old.drain()
			old = nil
		}
	} else {
		if next != nil {
			// A completely prepared but unpublished successor owns all its raw
			// and semantic edges. Do not release the new roots a second time below.
			clear(p.roots)
			for i := range next.shards {
				r := next.shards[i].root.Release()
				next.shards[i] = cowTable{}
				r.Drain()
			}
			for i := range next.frozen {
				r := next.frozen[i].root.Release()
				next.frozen[i] = cowTable{}
				r.Drain()
			}
			basis := next.basis
			next.basis = nil
			cleanupErr = basis.release()
			cache.releasePrivateSemanticState(next)
			next.lease, next.cache = nil, nil
			next.shards, next.frozen, next.sources, next.domains = nil, nil, nil, nil
			next = nil
		}
		// No newly created writer was visible. All release traversal and
		// actual resource callbacks run after owner/admission release.
		for i, root := range p.roots {
			if root != nil {
				p.roots[i] = nil
				r := root.Release()
				root = nil
				r.Drain()
			}
		}
		for i, w := range p.writers {
			if w != nil {
				p.writers[i] = nil
				r := w.Close()
				w = nil
				r.Drain()
			}
		}
		if p.semantic != nil {
			record := p.semantic.Release()
			p.semantic = nil
			cache.semanticMu.Lock()
			cache.enqueueSemanticLocked(record)
			cache.semanticMu.Unlock()
			cache.wakeSemanticCleanup()
		}
		lease := p.cutLease
		p.cutLease = nil
		lease.Close()
	}
	// End all preparation aliases before either containing-owner refund. The
	// retained opening cut survives every private semantic cleanup enqueue.
	scratch := p.scratch
	p.scratch = nil
	p.writers, p.roots, p.resources, p.retired = nil, nil, nil, nil
	if opening != nil {
		cleanupErr = joinCOWCleanupError(cleanupErr, cache.releaseCut(opening))
		opening = nil
	}
	cache, p = nil, nil
	scratch.Close()
	return cleanupErr
}
