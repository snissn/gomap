package caching

import (
	"errors"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// nativeFrozenDeltaWorkV6 prepares the exact captured prefix using the SAME
// ordered traversal/merge engine. Entries borrow immutable COW payloads from
// capture; the one admitted metadata vector is frozen before zipper frames use it.
// Winning tombstones remain physical deletes, unlike a public value iterator.
type nativeFrozenDeltaWorkV6 struct {
	capture                  NativeMVCCCaptureV6
	entries                  []batch.Entry
	lease                    *memtable.COWExternalLease
	phase                    uint8
	index, limit, clearIndex int
	eof                      bool
}

func (s *nativeFrozenDeltaWorkV6) Step(db *DB, cut *cowReadCut, w *iterator.OrdinalScanWork) (bool, error) {
	if db == nil || cut == nil || cut.cache != db.cow {
		return false, ErrCOWUnsupported
	}
	for {
		switch s.phase {
		case 0:
			for s.index < len(cut.frozen) {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(cowTable{}))) {
					return false, nil
				}
				n := cut.frozen[s.index].Len()
				if n < 0 || n > int(^uint(0)>>1)-s.limit {
					return false, memtable.ErrCOWCapacity
				}
				s.limit += n
				s.index++
			}
			size := memtable.COWAllocationCharge(uint64(s.limit) * uint64(unsafe.Sizeof(batch.Entry{})))
			if !w.Reserve(2, 2*size) {
				return false, nil
			}
			lease, e := db.cow.budget.AcquireExternal(size)
			if e != nil {
				return false, e
			}
			s.lease = lease
			s.entries = make([]batch.Entry, 0, s.limit)
			s.phase = 1
		case 1:
			ready, e := db.stepNativeFrozenInputV6(&s.capture, cut, w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 2
		case 2:
			op := tree.ProjectionNext
			if len(s.entries) == 0 {
				op = tree.ProjectionSeekGE
			}
			e := s.capture.BeginNativeProjectionV6(nil, op, nil, w)
			if errors.Is(e, cowbtree.ErrWork) {
				return false, nil
			}
			if e != nil {
				return false, e
			}
			s.phase = 3
		case 3:
			ready, e := s.capture.StepNativeProjectionV6(nil, w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 4
		case 4:
			record, valid, e := s.capture.NativeProjectionRecordV6(nil, w)
			if errors.Is(e, cowbtree.ErrWork) {
				return false, nil
			}
			if e != nil {
				return false, e
			}
			if !valid {
				s.phase = 5
				s.eof = true
				continue
			}
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(batch.Entry{}))) {
				return false, nil
			}
			if len(s.entries) == cap(s.entries) {
				return false, memtable.ErrCOWCapacity
			}
			entry := batch.Entry{Type: batch.OpPut, Key: record.Key, Value: record.Value, ValuePtr: record.ValuePtr, IsPtr: record.Flags&node.FlagPointer != 0, Revision: record.Revision}
			if record.Flags&node.FlagTombstone != 0 {
				entry.Type = batch.OpDelete
				entry.Value = nil
			}
			s.entries = append(s.entries, entry)
			s.phase = 2
		case 5:
			return true, nil
		default:
			return false, ErrCOWUnsupported
		}
	}
}

// CloseStep first ends borrowed vector lifetimes, then closes the original
// traversal/cut edge. Its containing lease is returned after all aliases clear.
func (s *nativeFrozenDeltaWorkV6) CloseStep(w *iterator.OrdinalScanWork) (bool, *memtable.COWExternalLease, error) {
	for s.clearIndex < len(s.entries) {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(batch.Entry{}))) {
			return false, nil, nil
		}
		s.entries[s.clearIndex] = batch.Entry{}
		s.clearIndex++
	}
	if s.capture.composite.cut != nil {
		ready, e := s.capture.closeNativeCOWCaptureV6(w)
		if !ready || e != nil {
			return false, nil, e
		}
	}
	if s.lease != nil && !s.lease.AdmitRetentionClose(w) {
		return false, nil, nil
	}
	lease := s.lease
	*s = nativeFrozenDeltaWorkV6{}
	return true, lease, nil
}
