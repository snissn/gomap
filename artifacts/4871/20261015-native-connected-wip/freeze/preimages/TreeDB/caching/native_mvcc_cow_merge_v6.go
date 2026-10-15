package caching

import (
	"bytes"
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
	"unsafe"
)

// The original request admits this fixed source vector as part of its containing
// capture before allocation. It retains the SAME C1/C3 cut, never a second
// backend capture. Source order is newest first, with the retained basis last.
const nativeCOWSourcesV6 = 32

type nativeCOWSourceV6 struct {
	cursor         *memtable.COWTraversal
	record         node.LeafEntry
	valid, advance bool
}
type nativeCOWMergeV6 struct {
	cut                                    *cowReadCut
	cache                                  *cowCache
	sources                                [nativeCOWSourcesV6]nativeCOWSourceV6
	count, opened, index, winner, compare  int
	base                                   *tree.ChargedProjection
	key                                    []byte
	record                                 node.LeafEntry
	operation                              tree.ProjectionOperation
	phase                                  uint8
	pending, valid, baseValid, baseAdvance bool
	baseRecord                             node.LeafEntry
	retirement                             memtable.COWRetirement
	closeIndex                             int
	lookupID                               uint32
	lookupFile                             *os.File
	lookupSource, lookupEntry              int
	closeFailure                           error
	cutReleased                            bool
	cutClosing                             bool
	cutCloseIndex                          int
	cutBasisReleased                       bool
}

func (db *DB) stepNativeCOWCaptureV6(c *NativeMVCCCaptureV6, w *iterator.OrdinalScanWork) (*backenddb.Snapshot, bool, error) {
	m := &c.composite
	if m.cut == nil {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(cowReadCut{}))) {
			return nil, false, nil
		}
		cache := db.cow
		if cache == nil || !cache.readMu.TryRLock() {
			return nil, false, nil
		}
		if cache.readClosed.Load() {
			cache.readMu.RUnlock()
			return nil, false, backenddb.ErrClosed
		}
		if !cache.cutMu.TryLock() {
			cache.readMu.RUnlock()
			return nil, false, nil
		}
		cut := cache.cut
		if cache.closed || cut == nil || cut.basis == nil || cut.basis.snapshot == nil {
			cache.cutMu.Unlock()
			cache.readMu.RUnlock()
			return nil, false, backenddb.ErrClosed
		}
		count := len(cut.shards) + len(cut.frozen)
		if count > len(m.sources) || cut.refs == int(^uint(0)>>1) {
			cache.cutMu.Unlock()
			cache.readMu.RUnlock()
			return nil, false, memtable.ErrCOWCapacity
		}
		cut.refs++
		m.cut, m.cache, m.count = cut, cache, count
		c.cache, c.Snapshot = db, cut.basis.snapshot
		cache.cutMu.Unlock()
		cache.readMu.RUnlock()
	}
	for m.opened < m.count {
		i := m.opened
		var root *memtable.COWRoot
		if i < len(m.cut.shards) {
			root = m.cut.shards[len(m.cut.shards)-1-i].root
		} else {
			root = m.cut.frozen[len(m.cut.frozen)-1-(i-len(m.cut.shards))].root
		}
		cursor, err := root.NewTraversal(w, 0)
		if errors.Is(err, cowbtree.ErrWork) {
			return c.Snapshot, false, nil
		}
		if err != nil {
			return c.Snapshot, false, err
		}
		m.sources[i].cursor = cursor
		m.opened++
	}
	c.phase = 3
	return c.Snapshot, true, nil
}

func (c *NativeMVCCCaptureV6) IsCompositeCOWV6() bool { return c != nil && c.composite.cut != nil }

// BeginNativeProjectionV6 preserves the existing ordered overlay precedence.
// The seek key is a borrowed request-owned immutable buffer until Step finishes.
func (c *NativeMVCCCaptureV6) BeginNativeProjectionV6(base *tree.ChargedProjection, op tree.ProjectionOperation, key []byte, w *iterator.OrdinalScanWork) error {
	if !c.IsCompositeCOWV6() {
		return base.Begin(op, key, w)
	}
	m := &c.composite
	if m.pending {
		return tree.ErrProjectionOwner
	}
	if op != tree.ProjectionSeekGE && op != tree.ProjectionNext {
		return tree.ErrProjectionOwner
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*m))) {
		return cowbtree.ErrWork
	}
	m.base, m.operation, m.key, m.index, m.phase, m.pending = base, op, key, 0, 0, true
	if op == tree.ProjectionSeekGE {
		m.phase = 8
	} else {
		m.phase = 8
	}

	m.valid = false
	return nil
}

func (c *NativeMVCCCaptureV6) StepNativeProjectionV6(base *tree.ChargedProjection, w *iterator.OrdinalScanWork) (bool, error) {
	if !c.IsCompositeCOWV6() {
		return base.Step(w)
	}
	m := &c.composite
	if !m.pending {
		return true, nil
	}
	for {
		switch m.phase {
		case 8:
			for m.index <= m.count {
				i := m.index
				r, valid := m.baseRecord, m.baseValid
				if i < m.count {
					r, valid = m.sources[i].record, m.sources[i].valid
				}
				if !w.Reserve(1, uint64(len(r.Key))+uint64(len(m.record.Key))+32) {
					return false, nil
				}
				advance := m.operation == tree.ProjectionSeekGE || (valid && bytes.Equal(r.Key, m.record.Key))
				if i < m.count {
					m.sources[i].advance = advance
				} else {
					m.baseAdvance = advance
				}
				m.index++
			}
			m.index, m.phase = 0, 0

		case 0:
			if m.index == m.count {
				m.phase = 3
				continue
			}
			s := &m.sources[m.index]
			if !s.advance {
				m.index++
				continue
			}
			op := memtable.COWNext
			if m.operation == tree.ProjectionSeekGE {
				op = memtable.COWSeekGE
			}
			// The request buffer is immutable through this operation, so the zero-copy
			// conversion has the same borrowed-key lifetime as the original traversal.
			key := ""
			if len(m.key) != 0 {
				key = unsafe.String(unsafe.SliceData(m.key), len(m.key))
			}
			if err := s.cursor.Begin(op, key, w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return false, nil
				}
				return false, err
			}
			m.phase = 1
		case 1:
			ready, err := m.sources[m.index].cursor.Step(w)
			if !ready || err != nil {
				return false, err
			}
			m.phase = 2
		case 2:
			r, valid, err := m.sources[m.index].cursor.Record(w)
			if errors.Is(err, cowbtree.ErrWork) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			s := &m.sources[m.index]
			s.record = node.LeafEntry{Key: cowBytes(r.Key), Value: cowBytes(r.Value), ValuePtr: r.Ptr, Flags: r.Flags, Revision: r.Revision}
			s.valid, s.advance = valid, false
			m.index++
			m.phase = 0
		case 3:
			if !m.baseAdvance {
				m.phase = 6
				m.compare = 0
				m.winner = -1
				continue
			}
			if err := base.Begin(m.operation, m.key, w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return false, nil
				}
				return false, err
			}
			m.phase = 4
		case 4:
			ready, err := base.Step(w)
			if !ready || err != nil {
				return false, err
			}
			m.phase = 5
		case 5:
			r, valid, err := base.Record(w)
			if errors.Is(err, cowbtree.ErrWork) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			m.baseRecord, m.baseValid, m.baseAdvance = r, valid, false
			m.phase = 6
			m.compare = 0
			m.winner = -1
		case 6:
			for m.compare <= m.count {
				i := m.compare
				r, valid := m.baseRecord, m.baseValid
				if i < m.count {
					r, valid = m.sources[i].record, m.sources[i].valid
				}
				if !w.Reserve(1, uint64(len(r.Key))+uint64(len(m.record.Key))+uint64(unsafe.Sizeof(r))) {
					return false, nil
				}
				if valid && (m.winner < 0 || bytes.Compare(r.Key, m.record.Key) < 0) {
					m.record, m.winner = r, i
				}
				m.compare++
			}
			if m.winner < 0 {
				m.valid = false
				m.pending = false
				m.key = nil
				return true, nil
			}
			if m.record.Flags&node.FlagTombstone != 0 {
				// Advance ALL equal sources. A cached tombstone hides the older basis;
				// omitting only its top source would resurrect the disk record.
				m.operation, m.index, m.phase = tree.ProjectionNext, 0, 8
				continue
			}
			m.valid, m.pending, m.key = true, false, nil
			return true, nil
		}
	}
}
func (c *NativeMVCCCaptureV6) NativeProjectionRecordV6(base *tree.ChargedProjection, w *iterator.OrdinalScanWork) (node.LeafEntry, bool, error) {
	if !c.IsCompositeCOWV6() {
		return base.Record(w)
	}
	m := &c.composite
	if m.pending {
		return node.LeafEntry{}, false, tree.ErrProjectionOwner
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(m.record))) {
		return node.LeafEntry{}, false, cowbtree.ErrWork
	}
	return m.record, m.valid, nil
}
func (c *NativeMVCCCaptureV6) NativeProjectionUsesBackendV6() bool {
	return !c.IsCompositeCOWV6() || c.composite.winner == c.composite.count
}

// NativePinnedValueLogFileV6 searches only SAME-cut original resolver entries.
// Every inspected fixed entry is admitted before reading it; no manager refresh
// or filesystem fallback can silently recapture a later physical incarnation.
func (c *NativeMVCCCaptureV6) NativePinnedValueLogFileV6(id uint32, w *iterator.OrdinalScanWork) (*os.File, bool, error) {
	if !c.IsCompositeCOWV6() {
		if !w.Reserve(1, 128) {
			return nil, false, nil
		}
		f, _, err := c.Snapshot.PinnedValueLogFile(id)
		return f, true, err
	}
	m := &c.composite
	if m.lookupID != id {
		m.lookupID, m.lookupSource, m.lookupEntry = id, 0, 0
		m.lookupFile = nil
	}
	if m.lookupFile != nil {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(m.lookupFile))) {
			return nil, false, nil
		}
		return m.lookupFile, true, nil
	}
	for m.lookupSource < m.count {
		i := m.lookupSource
		var resources *cowGenerationResources
		if i < len(m.cut.shards) {
			resources = m.cut.shards[len(m.cut.shards)-1-i].resources
		} else {
			resources = m.cut.frozen[len(m.cut.frozen)-1-(i-len(m.cut.shards))].resources
		}
		if resources != nil {
			resources.mu.RLock()
			for m.lookupEntry < len(resources.entries) {
				if !w.Reserve(1, uint64(unsafe.Sizeof(cowLiveResource{}))) {
					resources.mu.RUnlock()
					return nil, false, nil
				}
				r := resources.entries[m.lookupEntry]
				m.lookupEntry++
				if r != nil && r.id.Kind == cowResourceValueLogFile && r.id.ID == uint64(id) && r.set != nil {
					f := r.set.Files[id]
					if f != nil {
						m.lookupFile = f.File
						resources.mu.RUnlock()
						return m.lookupFile, true, nil
					}
				}
			}
			resources.mu.RUnlock()
		}
		m.lookupSource++
		m.lookupEntry = 0
	}
	if !w.Reserve(1, 128) {
		return nil, false, nil
	}
	f, _, err := c.Snapshot.PinnedValueLogFile(id)
	if err == nil {
		m.lookupFile = f
	}
	return f, true, err
}

// closeNativeCOWCaptureV6 consumes traversal borrows before its original cut
// reference. A shared basis is never independently finalized or re-enrolled.
func (c *NativeMVCCCaptureV6) closeNativeCOWCaptureV6(w *iterator.OrdinalScanWork) (bool, error) {
	m := &c.composite
	for m.closeIndex < m.opened {
		s := &m.sources[m.closeIndex]
		if s.cursor != nil {
			ready, retirement, err := s.cursor.CloseStep(w)
			if err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return false, nil
				}
				return false, err
			}
			if !ready {
				return false, nil
			}
			m.retirement = retirement
			s.cursor = nil
			s.record = node.LeafEntry{}
		}
		ready, err := m.retirement.DrainWithWork(w)
		if !ready || err != nil {
			return false, err
		}
		m.closeIndex++
	}
	if !m.cutReleased {
		cache := m.cache
		if !m.cutClosing {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*m))+2*uint64(unsafe.Sizeof(cowReadCut{}))) {
				return false, nil
			}
			cache.cutMu.Lock()
			if m.cut.refs > 1 {
				m.cut.refs--
				m.cut = nil
			} else if m.cut.refs == 1 {
				// SAME original cut is no longer discoverable. This capture alone
				// serializes its original root/retirement progress and containing charge.
				m.cutClosing = true
			} else {
				cache.cutMu.Unlock()
				return false, memtable.ErrCOWResourceWork
			}
			cache.cutMu.Unlock()
		}
		if m.cutClosing {
			cut := m.cut
			for m.cutCloseIndex < len(cut.shards)+len(cut.frozen) {
				var table *cowTable
				if m.cutCloseIndex < len(cut.shards) {
					table = &cut.shards[m.cutCloseIndex]
				} else {
					table = &cut.frozen[m.cutCloseIndex-len(cut.shards)]
				}
				if table.root != nil {
					// COWRoot.Release only drops original root/generation ref edges;
					// actual callbacks remain in SAME COWRetirement and are metered below.
					if !w.Reserve(3, 2*uint64(unsafe.Sizeof(memtable.COWRoot{}))+2*uint64(unsafe.Sizeof(memtable.COWBudget{}))+512) {
						return false, nil
					}
					root := table.root
					*table = cowTable{}
					m.retirement = root.Release()
					root = nil
				}
				ready, err := m.retirement.DrainWithWork(w)
				if !ready || err != nil {
					return false, err
				}
				m.cutCloseIndex++
			}
			if !m.cutBasisReleased {
				// The current successor cut owns the SAME backend basis for this
				// eligible route. CAS retries are real charged attempts. Last-basis
				// physical cleanup is a distinct unsupported class, before effects.
				for {
					if !w.Reserve(1, 2*uint64(unsafe.Sizeof(cowBackendBasis{}))) {
						return false, nil
					}
					n := cut.basis.refs.Load()
					if n <= 1 {
						return false, memtable.ErrCOWResourceWork
					}
					if cut.basis.refs.CompareAndSwap(n, n-1) {
						cut.basis = nil
						m.cutBasisReleased = true
						break
					}
				}
			}
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*cut))+2*uint64(unsafe.Sizeof(*m))) {
				return false, nil
			}
			lease := cut.lease
			if !lease.AdmitRetentionClose(w) {
				return false, nil
			}
			cut.shards, cut.frozen, cut.sources, cut.domains = nil, nil, nil, nil
			cut.cache, cut.lease = nil, nil
			cut.refs = 0
			m.cut = nil
			cut = nil
			lease.Close()
			lease = nil
			// Cache containing admission stays until every original cut returns.
			if cache.activeCuts.Add(-1) == 0 && cache.readClosed.Load() {
				cache.lease.Close()
			}
		}
		m.cache = nil
		m.cutReleased = true
		c.Snapshot = nil
		m.base = nil
		m.key = nil
		m.record = node.LeafEntry{}
		m.baseRecord = node.LeafEntry{}
		m.lookupFile = nil
	}

	return true, nil
}

// NativeMVCCSelectedRecordV6 validates a stationary winner in this SAME retained
// composite cut. No backend leaf claim is synthesized for an overlay record.
// The original request owns this capture and prevents traversal advancement
// until the bound current-delete transaction has finished or been cancelled.
func (c *NativeMVCCCaptureV6) NativeMVCCSelectedRecordV6(snapshot *backenddb.Snapshot, w *iterator.OrdinalScanWork) (node.LeafEntry, bool, error) {
	if c == nil || w == nil {
		return node.LeafEntry{}, false, backenddb.ErrNativeMVCCSnapshotWork
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(node.LeafEntry{}))+128) {
		return node.LeafEntry{}, false, nil
	}
	m := &c.composite
	if !c.IsCompositeCOWV6() || c.Snapshot != snapshot || m.cut.basis == nil || m.cut.basis.snapshot != snapshot || m.pending || !m.valid || m.cutReleased || m.winner < 0 || m.winner > m.count {
		return node.LeafEntry{}, false, tree.ErrProjectionOwner
	}
	return m.record, true, nil
}
