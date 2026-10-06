package caching

import (
	"fmt"
	"unsafe"

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
	if count == 0 {
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
	lease, err := c.budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	p := &cowRollover{cache: c, scratch: lease, writers: make([]*memtable.COWWriter, n), roots: make([]*memtable.COWRoot, n), resources: make([]*cowGenerationResources, n), retired: make([]memtable.COWRetirement, n)}
	p.cutLease, err = c.budget.AcquireExternal(cowCutCharge(n, len(old.frozen)+count))
	if err != nil {
		return p, err
	}
	for i, table := range old.shards {
		if table.Len() == 0 {
			continue
		}
		p.writers[i], err = memtable.NewCOWWriter(c.budget)
		if err != nil {
			return p, err
		}
		root, resources, e := prepareCOWEmptyRoot(p.writers[i], c.budget)
		if e != nil {
			return p, e
		}
		p.roots[i], p.resources[i] = root, resources
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
	next := &cowReadCut{refs: 1, cache: c, lease: p.cutLease, basis: old.basis, shards: make([]cowTable, n), frozen: make([]cowTable, len(old.frozen), len(old.frozen)+count)}
	next.basis.retain()
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
		next.shards[i] = cowTable{shard: i, root: p.roots[i], resources: p.resources[i]}
	}
	next.buildDomains()
	p.next = next
	return p, nil
}

func (p *cowRollover) install() {
	c := p.cache
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

func (p *cowRollover) drain() {
	if p == nil {
		return
	}
	if p.installed {
		for i := range p.retired {
			p.retired[i].Drain()
		}
		if p.old != nil {
			p.old.drain()
		}
	} else {
		// Preparation failed before successor installation. No newly created writer
		// was visible and no source callback may run until this unlocked drain.
		for i, root := range p.roots {
			if root != nil {
				r := root.Release()
				r.Drain()
				p.roots[i] = nil
			}
		}
		for _, w := range p.writers {
			if w != nil {
				r := w.Close()
				r.Drain()
			}
		}
		if p.cutLease != nil {
			p.cutLease.Close()
		}
	}
	p.scratch.Close()
}
