package zipper

import (
	"bytes"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
)

// leafMergeEvent is one original merge decision. Entry and Removed borrow the
// immutable input page/operations (or the caller's admitted decoder scratch).
// The consumer must finish using them before advancing the same cursor.
// A delete/tombstone can consume an input without emitting an output entry.
type leafMergeEvent struct {
	entry, removed                 node.LeafEntry
	emit, removes, fromBatch, done bool
}

// leafMergeCursor is the shared selection engine for ordinary mergeLeaf and its
// retained materialized-DATA driver. Only the driver owns output pages, original
// allocator custody and publication. This cursor creates no logical authority.
// Nil work follows the original direct layout decoder/comparison fast path;
// bounded work retains the original ProjectionDecoder and exact comparison.
type leafMergeCursor struct {
	old                 *node.Node
	ops                 []batch.Entry
	ranges              []batch.DeleteRange
	scratch             []byte
	oldIndex            uint16
	opIndex, rangeIndex int
	decoded             node.LeafEntry
	decoder             node.ProjectionDecoder
	comparison          node.ProjectionComparison
	haveOld             bool
	phase, choice       uint8
}

func mergeReserve(w *iterator.OrdinalScanWork, records, size uint64) (bool, error) {
	if w == nil {
		return true, nil
	}
	if records > w.RecordLimit || size > w.ByteLimit {
		return false, &iterator.OrdinalUnitTooLarge{Records: records, Bytes: size}
	}
	return w.Reserve(records, size), nil
}

func (s *leafMergeCursor) next(w *iterator.OrdinalScanWork) (leafMergeEvent, bool, error) {
	for {
		switch s.phase {
		case 0:
			if ok, e := mergeReserve(w, 1, 128); !ok || e != nil {
				return leafMergeEvent{}, false, e
			}
			if s.oldIndex >= s.old.Count() && s.opIndex >= len(s.ops) {
				return leafMergeEvent{done: true}, true, nil
			}
			if s.oldIndex >= s.old.Count() {
				s.choice = 1
				s.phase = 4
				continue
			}
			if !s.haveOld {
				if w == nil {
					k, v, p, f, r, e := s.old.GetLeafEntryViewWithRevision(s.oldIndex)
					if e != nil {
						return leafMergeEvent{}, false, e
					}
					s.decoded = node.LeafEntry{Key: k, Value: v, ValuePtr: p, Flags: f, Revision: r}
					s.haveOld = true
				} else {
					ready, e := s.decoder.Begin(*s.old, s.oldIndex, s.scratch, w)
					if !ready || e != nil {
						return leafMergeEvent{}, false, e
					}
					s.phase = 1
					continue
				}
			}
			s.phase = 2
		case 1:
			ready, e := s.decoder.Step(w)
			if !ready || e != nil {
				return leafMergeEvent{}, false, e
			}
			s.decoded, _ = s.decoder.Result()
			s.haveOld = true
			s.phase = 2
		case 2:
			if s.opIndex >= len(s.ops) {
				s.choice = 0
				s.phase = 4
				continue
			}
			if w == nil {
				s.selectComparison(bytes.Compare(s.decoded.Key, s.ops[s.opIndex].Key))
				continue
			}
			if !s.comparison.Begin(s.decoded.Key, s.ops[s.opIndex].Key, w) {
				return leafMergeEvent{}, false, nil
			}
			s.phase = 3
		case 3:
			cmp, ready := s.comparison.Step(w)
			if !ready {
				return leafMergeEvent{}, false, nil
			}
			s.selectComparison(cmp)
		case 4:
			// Reserve the real event/indices update BEFORE consuming either operand.
			if ok, e := mergeReserve(w, 1, 2*uint64(unsafe.Sizeof(node.LeafEntry{}))+64); !ok || e != nil {
				return leafMergeEvent{}, false, e
			}
			if s.choice != 0 {
				op := s.ops[s.opIndex]
				out := leafMergeEvent{fromBatch: true}
				if s.choice == 2 {
					out.removed = s.decoded
					out.removes = true
					s.oldIndex++
					s.haveOld = false
				}
				s.opIndex++
				if op.Type != batch.OpDelete {
					out.emit = true
					out.entry = node.LeafEntry{Key: op.Key, Value: op.Value, ValuePtr: op.ValuePtr, Revision: op.Revision, Flags: node.FlagInline}
					if op.IsPtr {
						out.entry.Flags = node.FlagPointer
						out.entry.Value = nil
					}
				}
				s.phase = 0
				return out, true, nil
			}
			if s.decoded.Flags&node.FlagTombstone != 0 {
				s.oldIndex++
				s.haveOld = false
				s.phase = 0
				return leafMergeEvent{}, true, nil
			}
			s.rangeIndex = 0
			s.phase = 5
		case 5:
			for s.rangeIndex < len(s.ranges) {
				r := s.ranges[s.rangeIndex]
				// One actual interval test; all compared bytes are funded before access.
				if ok, e := mergeReserve(w, 1, uint64(2*len(s.decoded.Key)+len(r.Start)+len(r.End))+64); !ok || e != nil {
					return leafMergeEvent{}, false, e
				}
				s.rangeIndex++
				if batch.DeleteRangeContainsKey(r, s.decoded.Key) {
					out := leafMergeEvent{removed: s.decoded, removes: true}
					s.oldIndex++
					s.haveOld = false
					s.phase = 0
					return out, true, nil
				}
			}
			if ok, e := mergeReserve(w, 1, uint64(unsafe.Sizeof(node.LeafEntry{}))+32); !ok || e != nil {
				return leafMergeEvent{}, false, e
			}
			out := leafMergeEvent{entry: s.decoded, emit: true}
			s.oldIndex++
			s.haveOld = false
			s.phase = 0
			return out, true, nil
		}
	}
}
func (s *leafMergeCursor) selectComparison(cmp int) {
	s.choice = 0
	if cmp > 0 {
		s.choice = 1
	} else if cmp == 0 {
		s.choice = 2
	}
	s.phase = 4
}

// Kept separate so the ordinary writer uses the same complete event semantics
// without introducing bounded decoder state or allocations in its fast path.
func newLeafMergeCursor(old *node.Node, ops []batch.Entry, ranges []batch.DeleteRange, scratch []byte) leafMergeCursor {
	return leafMergeCursor{old: old, ops: ops, ranges: ranges, scratch: scratch}
}
