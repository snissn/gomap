package caching

import (
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeRolloverWorkV6 prepares the SAME rollover privately. The initial cut
// owns the immutable frozen prefix; successor point writes may replace active
// shard roots while preparation runs. No writer is frozen before the complete
// successor metadata and every replacement writer exist.
type nativeRolloverWorkV6 struct {
	plan                                      cowRollover
	initial                                   NativeMVCCCaptureV6
	displaced                                 NativeMVCCCaptureV6
	phase                                     uint8
	index, count, domain, source, domainStart int
	prepared                                  *memtable.COWPrepared
	captured                                  *cowReadCut
	cancelling                                bool
	retirement                                memtable.COWRetirement
	points                                    []memtable.Table
}

func (s *nativeRolloverWorkV6) Step(db *DB, w *iterator.OrdinalScanWork) (bool, error) {
	if db == nil || db.cow == nil {
		return false, ErrCOWUnsupported
	}
	c := db.cow
	if !c.readMu.TryRLock() {
		return false, nil
	}
	defer c.readMu.RUnlock()
	if db.closing.Load() || c.readClosed.Load() {
		return false, backenddb.ErrClosed
	}
	p := &s.plan
	if s.cancelling {
		return false, ErrCOWUnsupported
	}
	if s.phase == 0 {
		db.writeMu.Lock()
		c.writerMu.Lock()
		old := c.cut
		n := len(c.writers)
		if n > nativeCOWSourcesV6 || old == nil {
			c.writerMu.Unlock()
			db.writeMu.Unlock()
			return false, ErrCOWUnsupported
		}
		// Each length test is funded before examining the current fixed frontier.
		if !w.Reserve(uint64(n)+1, 2*uint64(n)*uint64(unsafe.Sizeof(cowTable{}))) {
			c.writerMu.Unlock()
			db.writeMu.Unlock()
			return false, nil
		}
		count := 0
		for _, table := range old.shards {
			if table.Len() > 0 {
				count++
			}
		}
		if count == 0 {
			if !w.Reserve(1, 128) {
				c.writerMu.Unlock()
				db.writeMu.Unlock()
				return false, nil
			}
			c.cutMu.Lock()
			old.refs++
			c.cutMu.Unlock()
			s.captured = old
			s.phase = 7
			c.writerMu.Unlock()
			db.writeMu.Unlock()
			return true, nil
		}
		f := len(old.frozen) + count
		bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowRollover{}))) +
			2*memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof((*memtable.COWWriter)(nil)))) +
			memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof((*cowGenerationResources)(nil)))) +
			memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof(memtable.COWRetirement{})))
		// Original rollover scratch + successor cut vectors and their two leases.
		if !w.Reserve(12, 2*(bytes+cowCutCharge(n, f))) {
			c.writerMu.Unlock()
			db.writeMu.Unlock()
			return false, nil
		}
		scratch, e := c.budget.AcquireExternal(bytes)
		if e != nil {
			c.writerMu.Unlock()
			db.writeMu.Unlock()
			return false, e
		}
		lease, e := c.budget.AcquireExternal(cowCutCharge(n, f))
		if e != nil {
			scratch.Close()
			c.writerMu.Unlock()
			db.writeMu.Unlock()
			return false, e
		}
		*p = cowRollover{cache: c, scratch: scratch, cutLease: lease, writers: make([]*memtable.COWWriter, n), roots: make([]*memtable.COWRoot, n), resources: make([]*cowGenerationResources, n), retired: make([]memtable.COWRetirement, n)}
		next := &cowReadCut{refs: 1, cache: c, lease: lease, shards: make([]cowTable, n), frozen: make([]cowTable, f), sources: make([]memtable.Table, 0, n+f), domains: make([]rootDomainSnapshot, n)}
		s.points = make([]memtable.Table, 0, n+f)
		// Only these initially nonempty writers will be replaced. A different
		// formerly-empty shard may acquire newer writes and stays active.
		for i, table := range old.shards {
			if table.Len() > 0 {
				next.shards[i].history = 1
			}
		}
		c.cutMu.Lock()
		old.refs++
		c.cutMu.Unlock()
		s.initial.composite.cut, s.initial.composite.cache = old, c
		s.initial.Snapshot = old.basis.snapshot
		p.next = next
		s.count, s.phase = count, 1
		c.writerMu.Unlock()
		db.writeMu.Unlock()
	}
	old := s.initial.composite.cut
	next := p.next
	for {
		switch s.phase {
		case 1:
			for s.index < len(p.writers) {
				i := s.index
				if next.shards[i].history == 0 {
					s.index++
					continue
				}
				if p.writers[i] == nil {
					writer, e := memtable.NewCOWWriterWithWorkV6(c.budget, w)
					if errors.Is(e, cowbtree.ErrWork) {
						return false, nil
					}
					if e != nil {
						return false, e
					}
					p.writers[i] = writer
				}
				if p.resources[i] == nil {
					r, e := newCOWGenerationResourcesWithWorkV6(c.budget, w)
					if errors.Is(e, cowbtree.ErrWork) {
						return false, nil
					}
					if e != nil {
						return false, e
					}
					p.resources[i] = r
				}
				if p.roots[i] == nil {
					// Attach one exact resolver and transfer the prepared empty root. Reserve
					// both scalar transitions before the original empty Prepare can succeed.
					if !w.Reserve(2, 512) {
						return false, nil
					}
					prepared, e := p.writers[i].PrepareEmptyWithWorkV6(w)
					if errors.Is(e, cowbtree.ErrWork) {
						return false, nil
					}
					if e != nil {
						return false, e
					}
					s.prepared = prepared
					ids := [1]memtable.COWResourceID{{Kind: cowResourceGenerationOwner, ID: 1}}
					if e = prepared.AttachCheckedResourcesV6(ids[:], p.resources[i]); e != nil {
						return false, e
					}
					p.roots[i] = prepared.Publish()
					s.prepared = nil
				}
				s.index++
			}
			s.index, s.phase = 0, 2
		case 2:
			if next.basis == nil {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(cowBackendBasis{}))) {
					return false, nil
				}
				next.basis = old.basis
				next.basis.retain()
			}
			for s.index < len(old.frozen) {
				if !w.Reserve(2, 2*uint64(unsafe.Sizeof(cowTable{}))+2*uint64(unsafe.Sizeof(memtable.COWRoot{}))) {
					return false, nil
				}
				next.frozen[s.index] = old.frozen[s.index]
				if !next.frozen[s.index].root.Retain() {
					return false, memtable.ErrCOWClosed
				}
				s.index++
			}
			s.index, s.source, s.phase = 0, len(old.frozen), 20
		case 20:
			for s.index < len(p.writers) {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(cowTable{}))) {
					return false, nil
				}
				if p.writers[s.index] != nil {
					next.frozen[s.source].shard = s.index
					s.source++
				}
				s.index++
			}
			s.index, s.source, s.phase = 0, 0, 3
		case 3:
			for s.index < len(next.frozen)+len(next.shards) {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(memtable.Table(nil)))) {
					return false, nil
				}
				i := s.index
				if i < len(next.frozen) {
					next.sources = append(next.sources, &next.frozen[i])
				} else {
					next.sources = append(next.sources, &next.shards[i-len(next.frozen)])
				}
				s.index++
			}
			s.index, s.phase = 0, 4
		case 4:
			for s.domain < len(next.shards) {
				// source retains progress independently of the current active shard roots.
				for s.source < len(next.frozen) {
					if !w.Reserve(1, 2*uint64(unsafe.Sizeof(cowTable{}))+2*uint64(unsafe.Sizeof(memtable.Table(nil)))) {
						return false, nil
					}
					i := s.source
					s.source++
					if next.frozen[i].shard == s.domain {
						s.points = append(s.points, &next.frozen[i])
					}
				}
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(rootDomainSnapshot{}))) {
					return false, nil
				}
				s.points = append(s.points, &next.shards[s.domain])
				next.domains[s.domain] = rootDomainSnapshot{immutables: s.points[s.domainStart:len(s.points):len(s.points)]}
				s.domain++
				s.source = 0
				s.domainStart = len(s.points)
			}
			s.phase = 5
		case 5:
			db.writeMu.Lock()
			c.writerMu.Lock()
			current := c.cut
			if current == nil || current.basis != old.basis || len(current.frozen) != len(old.frozen) {
				c.writerMu.Unlock()
				db.writeMu.Unlock()
				return false, backenddb.ErrRootPublicationBasisMismatch
			}
			// Exact immutable-prefix comparison, current shard transfer/retains, each
			// Freeze/Close/replacement and the original publication swap are all funded
			// BEFORE the first live writer is frozen. Metadata backing was built above.
			records := uint64(len(old.frozen) + 3*len(p.writers) + 3*s.count + 6)
			bytes := 2*uint64(len(p.writers)+len(old.frozen))*uint64(unsafe.Sizeof(cowTable{})) + uint64(s.count)*4*uint64(unsafe.Sizeof(memtable.COWWriter{})) + 1024
			if !w.Reserve(records, bytes) {
				c.writerMu.Unlock()
				db.writeMu.Unlock()
				return false, nil
			}
			for i := range old.frozen {
				if current.frozen[i].root != old.frozen[i].root {
					c.writerMu.Unlock()
					db.writeMu.Unlock()
					return false, backenddb.ErrRootPublicationBasisMismatch
				}
			}
			if c.budget.Stats().Sources > c.budget.Limits().MaxSources-s.count {
				c.writerMu.Unlock()
				db.writeMu.Unlock()
				return false, memtable.ErrCOWCapacity
			}
			j := len(old.frozen)
			for i, table := range current.shards {
				next.shards[i] = table
				if p.writers[i] == nil {
					if !table.root.Retain() {
						panic("lost current COW root during admitted rollover")
					}
					continue
				}
				if e := c.writers[i].Freeze(); e != nil {
					c.writerMu.Unlock()
					db.writeMu.Unlock()
					return false, e
				}
				if !table.root.Retain() {
					panic("lost frozen COW root during admitted rollover")
				}
				next.frozen[j] = table
				j++
				next.shards[i] = cowTable{shard: i, root: p.roots[i], resources: p.resources[i], history: c.generationBase}
			}
			c.cutMu.Lock()
			current.refs++
			c.cutMu.Unlock()
			s.displaced.composite.cut, s.displaced.composite.cache = current, c
			s.displaced.Snapshot = current.basis.snapshot
			// Keep the exact installed cut for the eventual handoff, even if later
			// ordinary point writes replace the cache's current cut during retirement.
			next.refs++
			s.captured = next
			p.install()
			s.phase = 6
			c.writerMu.Unlock()
			db.writeMu.Unlock()
		case 6:
			for s.index < len(p.retired) {
				ready, e := p.retired[s.index].DrainWithWork(w)
				if !ready || e != nil {
					return false, e
				}
				s.index++
			}
			ready, e := s.initial.closeNativeCOWCaptureV6(w)
			if !ready || e != nil {
				return false, e
			}
			ready, e = s.displaced.closeNativeCOWCaptureV6(w)
			if !ready || e != nil {
				return false, e
			}
			// Installed aliases now belong to the live cut/writers. Detach every
			// scratch alias before returning its original containing lease.
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*p))) || !p.scratch.AdmitRetentionClose(w) {
				return false, nil
			}
			lease := p.scratch
			p.writers, p.roots, p.resources, p.retired = nil, nil, nil, nil
			p.next, p.old, p.scratch, p.cutLease = nil, nil, nil, nil
			s.points = nil
			lease.Close()
			s.phase = 7
			return true, nil
		case 7:
			return true, nil
		}
	}
}

// TakeCut transfers the original captured cut edge to the SAME handoff. No
// current recapture can silently widen the prefix after intervening writes.
func (s *nativeRolloverWorkV6) TakeCut() *cowReadCut {
	if s.phase != 7 {
		return nil
	}
	cut := s.captured
	s.captured = nil
	return cut
}

// CloseStep cancels private preparation, or joins an already-installed rollover
// without removing any prefix. Refusal keeps the SAME plan and physical receipt.
func (s *nativeRolloverWorkV6) CloseStep(w *iterator.OrdinalScanWork) (bool, error) {
	if !s.cancelling && !s.plan.installed {
		s.index, s.source = 0, 0
	}
	s.cancelling = true
	p := &s.plan
	if p.installed {
		for s.index < len(p.retired) {
			ready, e := p.retired[s.index].DrainWithWork(w)
			if !ready || e != nil {
				return false, e
			}
			s.index++
		}
	} else {
		if s.prepared != nil {
			if !w.Reserve(2, 2*uint64(unsafe.Sizeof(memtable.COWPrepared{}))) {
				return false, nil
			}
			s.retirement = s.prepared.Cancel()
			s.prepared = nil
		}
		ready, e := s.retirement.DrainWithWork(w)
		if !ready || e != nil {
			return false, e
		}
		for s.index < len(p.writers) {
			i := s.index
			if p.roots[i] != nil {
				if !w.Reserve(3, 1024) {
					return false, nil
				}
				root := p.roots[i]
				p.roots[i] = nil
				// Attached resolver custody is owned by this original generation. Drop
				// the preparation alias before the last checked owner callback can refund.
				p.resources[i] = nil
				s.retirement = root.Release()
				root = nil
			}
			ready, e := s.retirement.DrainWithWork(w)
			if !ready || e != nil {
				return false, e
			}
			if p.writers[i] != nil {
				if !w.Reserve(3, 1024) {
					return false, nil
				}
				writer := p.writers[i]
				p.writers[i] = nil
				p.retired[i] = writer.Close()
				writer = nil
			}
			ready, e = p.retired[i].DrainWithWork(w)
			if !ready || e != nil {
				return false, e
			}
			// A resolver unattached because Prepare/Attach refused is still original
			// private producer custody; attached resolvers have already completed here.
			if p.resources[i] != nil {
				consumed, done, completion, e := p.resources[i].ReleaseCOWResourceWorkV6(w)
				if e != nil {
					return false, e
				}
				if !done || !consumed {
					return false, nil
				}
				p.resources[i] = nil
				if completion != nil {
					completion.Close()
				}
			}
			s.index++
		}
		if p.next != nil {
			for s.source < len(p.next.frozen) {
				table := &p.next.frozen[s.source]
				if table.root != nil {
					if !w.Reserve(3, 1024) {
						return false, nil
					}
					root := table.root
					*table = cowTable{}
					s.retirement = root.Release()
					root = nil
				}
				ready, e := s.retirement.DrainWithWork(w)
				if !ready || e != nil {
					return false, e
				}
				s.source++
			}
			if p.next.basis != nil {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(cowBackendBasis{}))) {
					return false, nil
				}
				// initial owns the same basis until after this nonlast release.
				basis := p.next.basis
				n := basis.refs.Load()
				if n <= 1 {
					return false, memtable.ErrCOWResourceWork
				}
				if !basis.refs.CompareAndSwap(n, n-1) {
					return false, nil
				}
				p.next.basis = nil
			}
		}
	}
	if s.initial.composite.cut != nil {
		ready, e := s.initial.closeNativeCOWCaptureV6(w)
		if !ready || e != nil {
			return false, e
		}
	}
	if s.displaced.composite.cut != nil {
		ready, e := s.displaced.closeNativeCOWCaptureV6(w)
		if !ready || e != nil {
			return false, e
		}
	}
	if s.captured != nil {
		s.displaced = NativeMVCCCaptureV6{}
		s.displaced.composite.cut, s.displaced.composite.cache = s.captured, s.captured.cache
		s.displaced.Snapshot = s.captured.basis.snapshot
		s.captured = nil
		ready, e := s.displaced.closeNativeCOWCaptureV6(w)
		if !ready || e != nil {
			return false, e
		}
	}
	if p.scratch != nil {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*p))) || !p.scratch.AdmitRetentionClose(w) {
			return false, nil
		}
		if !p.installed && p.cutLease != nil && !p.cutLease.AdmitRetentionClose(w) {
			return false, nil
		}
		scratch, cutLease := p.scratch, p.cutLease
		installed := p.installed
		*p = cowRollover{}
		s.points = nil
		if !installed && cutLease != nil {
			cutLease.Close()
		}
		scratch.Close()
	}
	return true, nil
}
