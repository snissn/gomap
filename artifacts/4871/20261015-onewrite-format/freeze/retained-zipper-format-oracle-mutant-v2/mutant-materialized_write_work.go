package zipper

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var ErrMaterializedWriteContinuation = errors.New("zipper: retained materialized split or external-page continuation required")

// MaterializedWriteWork retains the ordinary zipper's physical merge decisions,
// original node encoders and original allocator/Pager operations. It borrows a
// caller-owned immutable DATA basis and sorted point delta. It owns NO root or
// publication authority. Installed pages stay in the original live allocator;
// no prepared candidate, BuildGroup or writer lock survives Step.
// Split outputs retain the original first-right separators and exact ChildRefs.
// Outer-leaf persistence is currently explicit unbounded-diagnostic work;
// production append and installed-page cancellation still require continuation.
type MaterializedWriteWork struct {
	z                                  *Zipper
	owner                              *retainedalloc.Owner
	head, current, root                *materializedWriteFrame
	reader                             MaterializedFrameReaderV6
	unboundedLeafPersistenceDiagnostic bool
	charge                             uint64
	refs                               PointerRefCounts
	removed                            uint64
	result                             node.PrimaryOperand
	phase                              uint8
}

// MaterializedFrameReaderV6 borrows the original retained basis phased
// page/vlog reader. Destination images belong to admitted output frames.
type MaterializedFrameReaderV6 interface {
	BeginProjectionFrame(page.ChildRef, []byte, *iterator.OrdinalScanWork) (bool, error)
	StepProjectionFrame(*iterator.OrdinalScanWork) ([]byte, bool, error)
}

func (s *MaterializedWriteWork) BindFrameReaderV6(reader MaterializedFrameReaderV6) {
	s.reader = reader
}

// EnableUnboundedLeafPersistenceDiagnosticV6 is only for the existing public
// diagnostic driver. The original append and all codec/registration work stay
// inside its measured call; this grants NO bounded work or production credit.
func (s *MaterializedWriteWork) EnableUnboundedLeafPersistenceDiagnosticV6() {
	s.unboundedLeafPersistenceDiagnostic = true
}

type materializedWriteFrame struct {
	parent, next                        *materializedWriteFrame
	active, segmentNext, childResult    *materializedWriteFrame
	splitPending, written, splitSegment bool
	old                                 node.Node
	oldID                               uint64
	oldRef                              page.ChildRef
	input                               [page.PageSize]byte
	ops                                 []batch.Entry
	output                              [page.PageSize]byte
	scratch, key, nextKey               [page.PageSize]byte
	encoder                             node.Builder
	selectLeaf                          leafMergeCursor
	event                               leafMergeEvent
	haveEvent                           bool
	decoder                             node.ProjectionDecoder
	comparison                          node.ProjectionComparison
	child, nextChild                    page.ChildRef
	childIndex                          uint16
	keySize, nextSize, startOp, opIndex int
	append                              freelist.AppendWork
	result                              page.ChildRef
	phase, decodePhase                  uint8
}

func NewMaterializedWriteWork(z *Zipper, owner *retainedalloc.Owner, ref page.ChildRef, ops []batch.Entry, w *iterator.OrdinalScanWork) (*MaterializedWriteWork, bool, error) {
	if z == nil || owner == nil || !ref.IsPage() {
		return nil, false, fmt.Errorf("%w: root ref=%+v", ErrMaterializedWriteContinuation, ref)
	}
	n := uint64(unsafe.Sizeof(MaterializedWriteWork{}))
	if ok, e := mergeReserve(w, 1, 2*n); !ok || e != nil {
		return nil, false, e
	}
	charge := retainedalloc.AllocationCharge(n)
	if e := owner.AddPending(charge); e != nil {
		return nil, false, e
	}
	s := &MaterializedWriteWork{z: z, owner: owner, charge: charge}
	// Retain this admitted containing object on frame refusal so the caller can
	// retry creation without losing a charged receipt.
	f, ready, e := s.newFrame(nil, ref, ops, w)
	if e != nil || !ready {
		m, c := s.owner, s.charge
		s = nil
		m.RemovePending(c)
		return nil, false, e
	}
	s.root, s.current = f, f
	return s, true, nil
}
func (s *MaterializedWriteWork) newFrame(parent *materializedWriteFrame, ref page.ChildRef, ops []batch.Entry, w *iterator.OrdinalScanWork) (*materializedWriteFrame, bool, error) {
	if (!ref.IsPage() || ref.Page < 2) && !ref.IsLeafLog() {
		return nil, false, fmt.Errorf("%w: child ref=%+v", ErrMaterializedWriteContinuation, ref)
	}
	n := uint64(unsafe.Sizeof(materializedWriteFrame{}))
	if ok, e := mergeReserve(w, 1, 2*n); !ok || e != nil {
		return nil, false, e
	}
	charge := retainedalloc.AllocationCharge(n)
	if e := s.owner.AddPending(charge); e != nil {
		return nil, false, e
	}
	f := &materializedWriteFrame{parent: parent, next: s.head, oldID: ref.Page, oldRef: ref, ops: ops}
	s.head = f
	s.charge += charge
	return f, true, nil
}
func (s *MaterializedWriteWork) Step(a *freelist.Allocator, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || s.owner == nil {
		return false, ErrMaterializedWriteContinuation
	}
	if s.phase == 1 {
		return true, nil
	}
	for s.current != nil {
		f := s.current
		switch f.phase {
		case 0:
			if f.oldRef.IsLeafLog() {
				if s.reader == nil {
					return false, ErrMaterializedWriteContinuation
				}
				ready, e := s.reader.BeginProjectionFrame(f.oldRef, f.input[:], w)
				if !ready || e != nil {
					return false, e
				}
				f.phase = 12
				continue
			}
			image, ready, e := s.z.pager.GetWithWork(f.oldID, w)
			if !ready || e != nil {
				return false, e
			}
			if ok, e := mergeReserve(w, 1, 2*page.PageSize); !ok || e != nil {
				return false, e
			}
			copy(f.input[:], image)
			f.phase = 13
		case 12:
			image, ready, e := s.reader.StepProjectionFrame(w)
			if !ready || e != nil {
				return false, e
			}
			if len(image) != page.PageSize || &image[0] != &f.input[0] {
				return false, ErrMaterializedWriteContinuation
			}
			f.phase = 13
		case 13:
			if ok, e := mergeReserve(w, 1, page.PageSize+256); !ok || e != nil {
				return false, e
			}
			if !page.VerifyChecksumNonMutating(f.input[:]) {
				return false, node.ErrPrimaryDirectory
			}
			f.old = node.NewNodeView(f.input[:])
			switch f.old.Type() {
			case page.PageTypeLeaf:
				// Preserve the original configured/adaptive format and enclosing revision
				// decision. Diagnostic codec selection/finalization is unbounded.
				revisions := f.old.LeafEntryRevisionsEnabled() || batchHasEntryRevisions(f.ops)
				f.encoder.ResetWithOptions(f.output[:], page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: revisions, PackedValuePtr: s.z.indexPackedValuePtr})
				f.selectLeaf = newLeafMergeCursor(&f.old, f.ops, nil, f.scratch[:])
				f.active = f
				f.phase = 1
			case page.PageTypeInternal:
				f.encoder.ResetWithOptions(f.output[:], page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: s.z.indexInternalBaseDelta})
				f.phase = 2
			default:
				return false, fmt.Errorf("%w: page %d type %d", ErrMaterializedWriteContinuation, f.oldID, f.old.Type())
			}
		case 1:
			if !f.haveEvent {
				event, ready, e := f.selectLeaf.next(w)
				if !ready || e != nil {
					return false, e
				}
				f.event, f.haveEvent = event, true
			}
			if f.event.done {
				f.haveEvent = false
				f.phase = 8
				continue
			}
			if f.event.removes {
				if ok, e := mergeReserve(w, 1, 256); !ok || e != nil {
					return false, e
				}
				old := f.event.removed
				if old.Flags&node.FlagPointer != 0 && old.ValuePtr.FileID != 0 {
					id := old.ValuePtr.FileID
					if s.refs.Count(id) == 0 && int(s.refs.inlineN) == len(s.refs.inline) {
						return false, ErrMaterializedWriteContinuation
					}
					s.refs.add(id, 1)
				}
				s.removed++
				f.event.removes = false
			}
			if f.event.emit {
				r := f.event.entry
				var ready bool
				var e error
				if s.unboundedLeafPersistenceDiagnostic {
					// SAME ordinary sizing, soft split and configured encoder. Its
					// columnar scratch/copy work is intentionally not metered here.
					entrySize, prefixLen, suffixLen := leafEntrySizeWithRevision(&f.active.encoder, r.Key, r.Value, r.Flags, r.Revision)
					if s.z.leafSoftFull(&f.active.encoder, entrySize) {
						e = node.ErrNodeFull
					} else {
						e = f.active.encoder.AddLeafEntryWithPrefixRevision(r.Key, r.Value, r.Flags, r.ValuePtr, r.Revision, entrySize, prefixLen, suffixLen)
					}
					ready = e == nil
				} else {
					ready, e = f.active.encoder.AddLeafEntryWithRevisionWithWork(r.Key, r.Value, r.Flags, r.ValuePtr, r.Revision, w)
				}
				if e == node.ErrNodeFull {
					if f.active.encoder.Count() == 0 {
						return false, e
					}
					f.splitPending = true
					f.phase = 8
					continue
				}
				if !ready || e != nil {
					return false, e
				}
			}
			f.event = leafMergeEvent{}
			f.haveEvent = false
		case 2:
			if f.childIndex >= f.old.Count() {
				f.phase = 8
				continue
			}
			// Decode current/next separator with the SAME phased layout decoder. The
			// child's low key must remain alive while a recursive child is constructed.
			ready, e := f.decoder.Begin(f.old, f.childIndex, f.scratch[:], w)
			if !ready || e != nil {
				return false, e
			}
			f.phase = 3
		case 3:
			ready, e := f.decoder.Step(w)
			if !ready || e != nil {
				return false, e
			}
			r, ref := f.decoder.Result()
			if len(r.Key) > len(f.key) {
				return false, ErrMaterializedWriteContinuation
			}
			if ok, e := mergeReserve(w, 1, uint64(len(r.Key))+64); !ok || e != nil {
				return false, e
			}
			f.keySize = copy(f.key[:], r.Key)
			f.child = ref
			f.startOp = f.opIndex
			if f.childIndex+1 == f.old.Count() {
				f.opIndex = len(f.ops)
				f.phase = 6
				continue
			}
			f.phase = 4
		case 4:
			ready, e := f.decoder.Begin(f.old, f.childIndex+1, f.scratch[:], w)
			if !ready || e != nil {
				return false, e
			}
			f.phase = 5
		case 5:
			if f.decodePhase == 0 {
				ready, e := f.decoder.Step(w)
				if !ready || e != nil {
					return false, e
				}
				r, ref := f.decoder.Result()
				if len(r.Key) > len(f.nextKey) {
					return false, ErrMaterializedWriteContinuation
				}
				if ok, e := mergeReserve(w, 1, uint64(len(r.Key))+64); !ok || e != nil {
					return false, e
				}
				f.nextSize = copy(f.nextKey[:], r.Key)
				f.nextChild = ref
				f.decodePhase = 1
			}
			for f.opIndex < len(f.ops) {
				if f.decodePhase == 1 {
					if !f.comparison.Begin(f.ops[f.opIndex].Key, f.nextKey[:f.nextSize], w) {
						return false, nil
					}
					f.decodePhase = 2
				}
				cmp, ready := f.comparison.Step(w)
				if !ready {
					return false, nil
				}
				f.decodePhase = 1
				if cmp >= 0 {
					break
				}
				f.opIndex++
			}
			f.decodePhase = 0
			f.phase = 6
		case 6:
			if f.startOp == f.opIndex {
				f.result = f.child
				f.childResult = nil
				f.phase = 7
				continue
			}
			child, ready, e := s.newFrame(f, f.child, f.ops[f.startOp:f.opIndex], w)
			if !ready || e != nil {
				return false, e
			}
			f.phase = 10
			s.current = child
		case 7:
			key := f.key[:f.keySize]
			result := f.result
			segment := f.childResult
			if segment != nil {
				result = segment.result
			}
			if segment != nil && segment.splitSegment {
				key = segment.key[:segment.keySize]
			} else {
				if f.startOp < f.opIndex && (len(key) == 0 || bytes.Compare(f.ops[f.startOp].Key, key) < 0) {
					key = f.ops[f.startOp].Key
				}
				if f.childIndex == 0 {
					key = nil
				}
			}
			ready, e := f.encoder.AddInternalChildRefWithWork(key, result, w)
			if !ready || e != nil {
				return false, e
			}
			if segment != nil && segment.segmentNext != nil {
				f.childResult = segment.segmentNext
				continue
			}
			f.result = page.ChildRef{}
			f.childResult = nil
			f.childIndex++
			f.phase = 2
		case 8:
			target := f
			if f.old.Type() == page.PageTypeLeaf {
				target = f.active
			}
			if f.old.Type() == page.PageTypeLeaf && s.z.outerLeavesInValueLog {
				if !s.unboundedLeafPersistenceDiagnostic {
					return false, ErrMaterializedWriteContinuation
				}
				target.encoder.SetPageID(0)
				ready, e := target.encoder.FinishNoNodeWithWork(nil)
				if !ready || e != nil {
					return false, e
				}
				var metrics adaptive.Metrics
				ref, e := s.z.persistLeafPageData(target.output[:], &metrics)
				if e != nil {
					return false, e
				}
				target.result = ref
				f.phase = 9
				continue
			}
			id, ready, e := target.append.Step(a, w)
			if !ready || e != nil {
				return false, e
			}
			if ok, e := mergeReserve(w, 1, 8); !ok || e != nil {
				return false, e
			}
			target.encoder.SetPageID(id)
			if f.old.Type() == page.PageTypeLeaf {
				codecWork := w
				if s.unboundedLeafPersistenceDiagnostic {
					codecWork = nil
				}
				ready, e = target.encoder.FinishNoNodeWithWork(codecWork)
			} else {
				ready, e = target.encoder.FinishInternalWithWork(w)
			}
			if !ready || e != nil {
				return false, e
			}
			target.result = page.PageChildRef(id)
			f.phase = 9
		case 9:
			target := f
			if f.old.Type() == page.PageTypeLeaf {
				target = f.active
			}
			if target.result.IsPage() {
				ready, e := s.z.pager.WriteWithWork(target.result.Page, target.output[:], w)
				if !ready || e != nil {
					return false, e
				}
			}
			target.written = true
			if f.splitPending {
				f.phase = 14
				continue
			}
			if ok, e := mergeReserve(w, 1, page.PageSize+128); !ok || e != nil {
				return false, e
			}
			if f.parent == nil {
				if f.segmentNext != nil {
					return false, ErrMaterializedWriteContinuation
				}
				s.result = node.PrimaryOperand{Ref: f.result, Digest: sha256.Sum256(f.output[:])}
				s.current = nil
				s.phase = 1
				return true, nil
			}
			parent := f.parent
			parent.result = f.result
			parent.childResult = f
			parent.phase = 7
			s.current = parent
			f.phase = 11
		case 14:
			// The original mergeLeaf persists a full target before creating its
			// right sibling. Keep that same separator and pending event alive.
			n := uint64(unsafe.Sizeof(materializedWriteFrame{}))
			if ok, e := mergeReserve(w, 1, 2*n+uint64(len(f.event.entry.Key))); !ok || e != nil {
				return false, e
			}
			charge := retainedalloc.AllocationCharge(n)
			if e := s.owner.AddPending(charge); e != nil {
				return false, e
			}
			next := &materializedWriteFrame{parent: f.parent, next: s.head, splitSegment: true}
			s.head = next
			s.charge += charge
			next.keySize = copy(next.key[:], f.event.entry.Key)
			start := f.selectLeaf.opIndex
			if f.event.fromBatch && start > 0 {
				start--
			}
			next.encoder.ResetWithOptions(next.output[:], page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: f.active.encoder.LeafEntryRevisionsEnabled(), PackedValuePtr: s.z.indexPackedValuePtr})
			f.active.segmentNext = next
			f.active = next
			f.splitPending = false
			f.phase = 1
		default:
			return false, ErrMaterializedWriteContinuation
		}
	}
	return s.phase == 1, nil
}
func (s *MaterializedWriteWork) Result() (node.PrimaryOperand, *PointerRefCounts, uint64, bool) {
	if s == nil || s.phase != 1 {
		return node.PrimaryOperand{}, nil, 0, false
	}
	return s.result, &s.refs, s.removed, true
}

// VisitPages borrows original installed/retired IDs after complete construction.
// The accepting original BuildGroup records them before this receipt is detached.
func (s *MaterializedWriteWork) VisitPages(visit func(allocated, retired uint64)) {
	for f := s.head; f != nil; f = f.next {
		visit(f.result.Page, f.oldID)
	}
}

// Complete is called ONLY after the original accepting owner has taken every
// installed page. The caller clears its receipt alias before consuming charge.
func (s *MaterializedWriteWork) Complete() (*retainedalloc.Owner, uint64) {
	m, c := s.owner, s.charge
	for f := s.head; f != nil; {
		next := f.next
		*f = materializedWriteFrame{}
		f = next
	}
	*s = MaterializedWriteWork{}
	return m, c
}
func (s *MaterializedWriteWork) Cancel(w *iterator.OrdinalScanWork) (bool, error) {
	for s.head != nil {
		f := s.head
		ready, e := f.append.Cancel(w)
		if !ready || e != nil {
			return false, e
		}
		if ok, e := mergeReserve(w, 1, uint64(unsafe.Sizeof(*f))); !ok || e != nil {
			return false, e
		}
		s.head = f.next
		*f = materializedWriteFrame{}
	}
	return true, nil
}

// MaterializedInputV6 is fixed containing metadata for the original PRIMARY
// components moved into DATA. It is part of the admitted native constructor.
// Prefix collection/normalization remains explicitly synchronous diagnostic work.
type MaterializedInputV6 struct {
	Base         node.PrimaryOperand
	Sequence     uint64
	Removed      uint64
	Refs         PointerRefCounts
	Retired      [node.PrimaryDirectoryMaxEntries]uint64
	RetiredCount int
}

func (z *Zipper) PrepareMaterializedInputV6(root uint64, prefix *batch.Batch) (result *batch.Batch, state MaterializedInputV6, err error) {
	image, err := z.pager.Get(root)
	if err != nil {
		return nil, state, err
	}
	state.Base = node.PrimaryOperand{Ref: page.PageChildRef(root), Digest: sha256.Sum256(image)}
	result = batch.New(nil, int(^uint(0)>>1))
	defer func() {
		if err != nil {
			result.Close()
			result = nil
		}
	}()
	points, ranges := prefix.ApplyPlan()
	if len(ranges) != 0 {
		return result, state, ErrMaterializedWriteContinuation
	}
	n := node.NewNodeView(image)
	if n.Type() == page.PageTypePrimaryDirectory {
		d, e := node.DecodePrimaryDirectory(image)
		if e != nil {
			return result, state, e
		}
		state.Base, state.Sequence = d.Base()
		scratch := z.acquireApplyScratch()
		defer z.releaseApplyScratch(scratch)
		var metrics adaptive.Metrics
		if _, e = z.loadPrimaryOperand(state.Base, nil, &metrics, scratch); e != nil {
			return result, state, e
		}
		for i := 0; i < d.Count(); i++ {
			entry, e := d.Entry(i)
			if e != nil {
				return result, state, e
			}
			op := batch.Entry{Type: batch.OpDelete, Key: bytes.Clone(entry.Key), Revision: entry.Revision}
			if !entry.InlineAbsence() {
				leaf, e := z.loadPrimaryOperand(entry.Operand, &entry, &metrics, scratch)
				if e != nil {
					return result, state, e
				}
				k, v, ptr, flags, rev, e := leaf.GetLeafEntryViewWithRevision(0)
				if e != nil {
					return result, state, e
				}
				op = batch.Entry{Type: batch.OpPut, Key: bytes.Clone(k), Value: bytes.Clone(v), ValuePtr: ptr, IsPtr: flags&node.FlagPointer != 0, Revision: rev}
				if flags&node.FlagTombstone != 0 {
					op.Type = batch.OpDelete
					op.Value = nil
				}
				// The original two-pass materializer removes the component if a later
				// prefix op wins. Preserve that physical reference delta in the fused pass.
				for j := range points {
					if bytes.Equal(points[j].Key, k) {
						if flags&node.FlagPointer != 0 && ptr.FileID != 0 {
							if state.Refs.Count(ptr.FileID) == 0 && int(state.Refs.inlineN) == len(state.Refs.inline) {
								return result, state, ErrMaterializedWriteContinuation
							}
							state.Refs.add(ptr.FileID, 1)
						}
						state.Removed++
						break
					}
				}
				if entry.Operand.Ref.IsPage() && !primaryarena.IsPage(entry.Operand.Ref.Page) {
					state.Retired[state.RetiredCount] = entry.Operand.Ref.Page
					state.RetiredCount++
				}
			}
			if e = result.SetOps([]batch.Entry{op}); e != nil {
				return result, state, e
			}
		}
	}
	if err = result.SetOps(points); err != nil {
		return result, state, err
	}
	result.ApplyPlan()
	return result, state, nil
}
func (s *MaterializedWriteWork) BindMaterializedInputAccountingV6(input *MaterializedInputV6) {
	s.refs = input.Refs
	s.removed = input.Removed
}
