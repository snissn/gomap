package tree

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// ProjectionFrameReader is an actual phased read on the SAME captured backend.
// Begin must reserve before capturing/probing ref or dst. Step must price every
// file/node/record visit, integrity check, decode and copy before effect, retain
// partial progress and fill the borrowed 4KiB dst for fixed pages. A larger
// external leaf may instead return a producer-owned immutable image retained by
// the SAME root edge until consumer close; no eager large allocation/copy is
// permitted. Internal/page operands must use dst so parent frames remain stable.
// Cancel must phase its
// own cleanup. A synchronous readLeaf func is deliberately not this interface.
// Implementing these methods is not a certificate of their accounting: the
// integrated owner must demonstrate the concrete producer work. No opaque
// callback adapter, provider registry or second backend capture is installed.
type ProjectionFrameReader interface {
	BeginProjectionFrame(page.ChildRef, []byte, *iterator.OrdinalScanWork) (bool, error)
	StepProjectionFrame(*iterator.OrdinalScanWork) ([]byte, bool, error)
	CancelProjectionFrame(*iterator.OrdinalScanWork) (bool, error)
}

type ProjectionOperation = cowbtree.Operation

const (
	ProjectionSeekGE = cowbtree.SeekGE
	ProjectionSeekLE = cowbtree.SeekLE
	ProjectionFirst  = cowbtree.First
	ProjectionLast   = cowbtree.Last
	ProjectionNext   = cowbtree.Next
	ProjectionPrev   = cowbtree.Prev
)

var (
	ErrProjectionReader  = errors.New("tree: phased exact-backend projection reader required")
	ErrProjectionOwner   = errors.New("tree: exact retained projection root/certificate required")
	ErrProjectionPending = errors.New("tree: projection operation pending")
	ErrProjectionClosed  = errors.New("tree: projection closed")
	ErrProjectionDepth   = errors.New("tree: projection traversal depth exceeded")
)

// ChargedProjection borrows the caller's ALREADY retained exact Tree/root edge
// and optional SAME PRIMARY certificate. It acquires no unpriced arena/pager
// owner and discovers no transaction. Keep that edge, bounds, seek key and frame
// reader alive/immutable until CloseStep completes. Then the original owner may
// reclaim through its existing lifecycle; CloseStep is not physical Finish.
// The entire fixed storage must be admitted by the existing enclosing owner
// before calling the constructor, just like OwnedPointerProjectionIterator.
type ChargedProjection struct {
	mu                                                                                   sync.Mutex
	tree                                                                                 Tree
	certificate                                                                          *primaryarena.ReadRootCertificateV6
	reader                                                                               ProjectionFrameReader
	base                                                                                 projectionBase
	pages                                                                                [maxTraversalDepth][page.PageSize]byte
	deltaPage                                                                            [page.PageSize]byte
	turnKey                                                                              [node.ProjectionKeyCapacity]byte
	turnSource, skipKey                                                                  []byte
	turnCopied                                                                           int
	skipCurrent                                                                          bool
	baseKey, deltaKey                                                                    [node.ProjectionKeyCapacity]byte
	deltaDecoder                                                                         node.ProjectionDecoder
	comparison                                                                           node.ProjectionComparison
	directory                                                                            node.PrimaryDirectoryView
	entry                                                                                node.PrimaryDirectoryEntry
	record                                                                               node.LeafEntry
	start, end, key                                                                      []byte
	op                                                                                   ProjectionOperation
	phase                                                                                uint8
	position, lo, hi, pivot, closeIndex                                                  int
	primary, reverse, equal, selectedDelta, valid, pending, closing, closed, frameActive bool
	positioned                                                                           bool
	advanceBase, advanceDelta                                                            bool
	frameNode                                                                            node.Node
	err                                                                                  error
}

// ChargedProjectionAllocationSize is the single fixed allocation's raw size.
// ChargedProjectionRetainedCharge uses the existing allocator policy; the
// enclosing root owner must admit that capacity before construction.
func ChargedProjectionAllocationSize() uint64 { return uint64(unsafe.Sizeof(ChargedProjection{})) }
func ChargedProjectionRetainedCharge() uint64 {
	return retainedalloc.AllocationCharge(ChargedProjectionAllocationSize())
}

// NewChargedProjection is UNPOSITIONED. It does no DecodeDirectory, Seek,
// pager access, frame callback, bound comparison or root scan. Unsupported
// callbacks refuse before invocation. No production activation occurs here.
func (t *Tree) NewChargedProjection(cert *primaryarena.ReadRootCertificateV6, start, end []byte, reader any, w *iterator.OrdinalScanWork) (*ChargedProjection, error) {
	r, ok := reader.(ProjectionFrameReader)
	if !ok || r == nil {
		return nil, ErrProjectionReader
	}
	if t == nil {
		return nil, ErrProjectionOwner
	}
	if w == nil || !w.Reserve(1, ChargedProjectionRetainedCharge()+uint64(unsafe.Sizeof(Tree{}))) {
		return nil, cowbtree.ErrWork
	}
	if cert != nil && (cert.Arena() == nil || cert.Reference().PageID != t.rootPageID || len(cert.Image()) != page.PageSize) {
		return nil, ErrProjectionOwner
	}
	c := &ChargedProjection{tree: *t, certificate: cert, reader: r, start: start, end: end, primary: cert != nil}
	c.base.reader = r
	c.base.pages = &c.pages
	c.base.keyScratch = c.baseKey[:]
	c.base.source = &c.tree
	if cert == nil {
		c.base.root = page.PageChildRef(t.rootPageID)
	}
	return c, nil
}

// Begin borrows key for Seek and rejects replacement of a paused request.
func (c *ChargedProjection) Begin(op ProjectionOperation, key []byte, w *iterator.OrdinalScanWork) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.closing {
		return ErrProjectionClosed
	}
	if c.pending {
		return ErrProjectionPending
	}
	if op > ProjectionPrev {
		return cowbtree.ErrOperation
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))+64) {
		return cowbtree.ErrWork
	}
	previousReverse := c.reverse
	newReverse := op == ProjectionSeekLE || op == ProjectionLast || op == ProjectionPrev
	turn := c.positioned && c.valid && (op == ProjectionNext || op == ProjectionPrev) && previousReverse != newReverse
	if !c.positioned || (!c.valid && (op == ProjectionNext || op == ProjectionPrev) && previousReverse != newReverse) {
		if op == ProjectionNext {
			op = ProjectionFirst
		}
		if op == ProjectionPrev {
			op = ProjectionLast
		}
	}
	if op == ProjectionSeekGE && key == nil {
		op = ProjectionFirst
	}
	if op == ProjectionSeekLE && key == nil {
		op = ProjectionLast
	}
	if op == ProjectionFirst && c.start != nil {
		op, key = ProjectionSeekGE, c.start
	}
	if op == ProjectionLast && c.end != nil {
		op, key = ProjectionSeekLE, c.end
	}
	c.op = op
	c.key = key
	c.pending = true
	c.valid = false
	c.err = nil
	c.phase = 0
	c.reverse = op == ProjectionSeekLE || op == ProjectionLast || op == ProjectionPrev
	if turn {
		c.turnSource = c.record.Key
		c.turnCopied = 0
		c.skipCurrent = true
		c.phase = 40
	}
	return nil
}

// Phases keep base seek, directory seek, equal-key merge, component validation
// and bounds independent. Finishing one request never loads its successor.
func (c *ChargedProjection) Step(w *iterator.OrdinalScanWork) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.closing {
		return false, ErrProjectionClosed
	}
	if c.err != nil {
		return false, c.err
	}
	if !c.pending {
		return true, nil
	}
	if w == nil || (w.RecordLimit == 0 && w.ByteLimit == 0) {
		return false, nil
	}
	done, err := c.step(w)
	if err != nil {
		c.err = err
	}
	return done, err
}
func (c *ChargedProjection) step(w *iterator.OrdinalScanWork) (bool, error) {
	for {
		switch c.phase {
		case 0: // fresh integrity on precisely the constructor-owned image
			if c.primary && c.directory.Count() == 0 && c.base.root == (page.ChildRef{}) {
				if !w.Reserve(1, page.PageSize+64) {
					return false, nil
				}
				if !page.VerifyChecksumNonMutating(c.certificate.Image()) {
					return false, node.ErrPrimaryDirectory
				}
				c.phase = 1
			} else {
				c.phase = 3
			}
		case 1:
			if !w.Reserve(1, page.PageSize+64) {
				return false, nil
			}
			if sha256.Sum256(c.certificate.Image()) != c.certificate.Digest() {
				return false, node.ErrPrimaryDirectory
			}
			c.phase = 2
		case 2:
			if !w.Reserve(1, uint64(unsafe.Sizeof(node.PrimaryOperand{}))+uint64(unsafe.Sizeof(node.PrimaryDirectoryView{}))) {
				return false, nil
			}
			c.directory = c.certificate.Directory()
			op, _ := c.directory.Base()
			c.base.root = op.Ref
			c.base.digest = op.Digest
			c.phase = 3
		case 3:
			if !w.Reserve(1, 64) {
				return false, nil
			}
			c.advanceBase = c.op == ProjectionNext || c.op == ProjectionPrev
			c.advanceDelta = c.advanceBase && c.selectedDelta
			if c.advanceBase && c.selectedDelta && !c.equal {
				c.advanceBase = false
			}
			c.phase = 4
		case 4:
			if c.advanceBase || (c.op != ProjectionNext && c.op != ProjectionPrev) {
				ready, err := c.base.begin(c.op, c.key, w)
				if err != nil || !ready {
					return false, err
				}
				c.phase = 5
			} else {
				c.phase = 6
			}
		case 5:
			done, err := c.base.step(w)
			if err != nil || !done {
				return false, err
			}
			c.phase = 6
		case 6:
			if !c.primary {
				c.phase = 12
				continue
			}
			if c.op == ProjectionNext || c.op == ProjectionPrev {
				if c.advanceDelta {
					if !w.Reserve(1, 8) {
						return false, nil
					}
					if c.reverse {
						c.position--
					} else {
						c.position++
					}
				}
				c.phase = 11
			} else if c.op == ProjectionFirst || c.op == ProjectionLast {
				if !w.Reserve(1, 8) {
					return false, nil
				}
				c.position = 0
				if c.reverse {
					c.position = c.directory.Count() - 1
				}
				c.phase = 11
			} else {
				if !w.Reserve(1, 24) {
					return false, nil
				}
				c.lo = 0
				c.hi = c.directory.Count()
				c.phase = 7
			}
		case 7:
			if c.lo == c.hi {
				if !w.Reserve(1, 8) {
					return false, nil
				}
				c.position = c.lo
				if c.reverse {
					c.position--
				}
				c.phase = 11
				continue
			}
			if !w.Reserve(1, uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))+64) {
				return false, nil
			}
			c.pivot = c.lo + (c.hi-c.lo)/2
			var err error
			c.entry, err = c.directory.Entry(c.pivot)
			if err != nil {
				return false, err
			}
			c.phase = 8
		case 8:
			if !c.comparison.Begin(c.entry.Key, c.key, w) {
				return false, nil
			}
			c.phase = 9
		case 9:
			cmp, done := c.comparison.Step(w)
			if !done {
				return false, nil
			}
			if !w.Reserve(1, 16) {
				return false, nil
			}
			if cmp < 0 || (c.reverse && cmp == 0) {
				c.lo = c.pivot + 1
			} else {
				c.hi = c.pivot
			}
			c.phase = 7
		case 11:
			if c.position >= 0 && c.position < c.directory.Count() {
				if !w.Reserve(1, uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))+64) {
					return false, nil
				}
				var err error
				c.entry, err = c.directory.Entry(c.position)
				if err != nil {
					return false, err
				}
			}
			c.phase = 12
		case 12:
			if !w.Reserve(1, 32) {
				return false, nil
			}
			hasDelta := c.primary && c.position >= 0 && c.position < c.directory.Count()
			c.equal = false
			c.selectedDelta = hasDelta
			if !hasDelta && !c.base.valid {
				c.phase = 30
				continue
			}
			if hasDelta && c.base.valid {
				c.phase = 13
			} else if hasDelta {
				c.phase = 16
			} else {
				c.phase = 24
			}
		case 13:
			if !c.comparison.Begin(c.entry.Key, c.base.record.Key, w) {
				return false, nil
			}
			c.phase = 14
		case 14:
			cmp, done := c.comparison.Step(w)
			if !done {
				return false, nil
			}
			if !w.Reserve(1, 16) {
				return false, nil
			}
			c.equal = cmp == 0
			c.selectedDelta = cmp <= 0
			if c.reverse {
				c.selectedDelta = cmp >= 0
			}
			if c.selectedDelta {
				c.phase = 16
			} else {
				c.phase = 24
			}
		case 16:
			if c.entry.InlineAbsence() {
				if !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))) {
					return false, nil
				}
				c.record = node.LeafEntry{Key: c.entry.Key, Flags: node.FlagTombstone, Revision: c.entry.Revision}
				c.phase = 25
			} else {
				c.phase = 17
			}
		case 17:
			if !w.Reserve(1, uint64(unsafe.Sizeof(page.ChildRef{}))+32) {
				return false, nil
			}
			if c.entry.Operand.Ref.Kind != page.ChildRefPage || c.entry.Operand.Ref.Page < 2 || c.entry.Operand.Ref.Log != (page.LogRecordRef{}) {
				return false, node.ErrPrimaryDirectory
			}
			c.phase = 33
		case 33:
			ready, err := c.reader.BeginProjectionFrame(c.entry.Operand.Ref, c.deltaPage[:], w)
			if err != nil || !ready {
				return false, err
			}
			c.frameActive = true
			c.phase = 18
		case 18:
			image, done, err := c.reader.StepProjectionFrame(w)
			if err != nil || !done {
				return false, err
			}
			if len(image) != page.PageSize || &image[0] != &c.deltaPage[0] {
				return false, ErrProjectionReader
			}
			c.frameActive = false
			c.phase = 19
		case 19:
			if !w.Reserve(1, page.PageSize+uint64(unsafe.Sizeof(node.Node{}))) {
				return false, nil
			}
			if !page.VerifyChecksumNonMutating(c.deltaPage[:]) {
				return false, node.ErrPrimaryDirectory
			}
			c.frameNode = node.NewNodeView(c.deltaPage[:])
			if c.frameNode.Type() != page.PageTypeLeaf || c.frameNode.Count() != 1 || (c.entry.Operand.Ref.Kind == page.ChildRefPage && page.DecodeHeader(c.deltaPage[:]).PageID != c.entry.Operand.Ref.Page) {
				return false, node.ErrPrimaryDirectory
			}
			c.phase = 20
		case 20:
			if !w.Reserve(1, page.PageSize+32) {
				return false, nil
			}
			if sha256.Sum256(c.deltaPage[:]) != c.entry.Operand.Digest {
				return false, node.ErrPrimaryDirectory
			}
			c.phase = 21
		case 21:
			ready, err := c.deltaDecoder.Begin(c.frameNode, 0, c.deltaKey[:], w)
			if err != nil || !ready {
				return false, err
			}
			c.phase = 22
		case 22:
			done, err := c.deltaDecoder.Step(w)
			if err != nil || !done {
				return false, err
			}
			if !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))) {
				return false, nil
			}
			c.record, _ = c.deltaDecoder.Result()
			if c.record.Revision != c.entry.Revision || (c.entry.Kind == node.PrimaryAbsence) != (c.record.Flags&node.FlagTombstone != 0) {
				return false, node.ErrPrimaryDirectory
			}
			c.phase = 23
		case 23:
			if !c.comparison.Begin(c.record.Key, c.entry.Key, w) {
				return false, nil
			}
			c.phase = 31
		case 31:
			cmp, done := c.comparison.Step(w)
			if !done {
				return false, nil
			}
			if cmp != 0 {
				return false, node.ErrPrimaryDirectory
			}
			c.phase = 25
		case 24:
			if !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))) {
				return false, nil
			}
			c.record = c.base.record
			c.phase = 25
		case 25:
			if c.skipCurrent {
				if !c.comparison.Begin(c.record.Key, c.skipKey, w) {
					return false, nil
				}
				c.phase = 41
				continue
			}
			if c.start != nil {
				if !c.comparison.Begin(c.record.Key, c.start, w) {
					return false, nil
				}
				c.phase = 26
			} else {
				c.phase = 27
			}
		case 26:
			cmp, done := c.comparison.Step(w)
			if !done {
				return false, nil
			}
			if cmp < 0 {
				if c.reverse {
					c.phase = 30
				} else {
					c.phase = 32
				}
				continue
			}
			c.phase = 27
		case 27:
			if c.end != nil {
				if !c.comparison.Begin(c.record.Key, c.end, w) {
					return false, nil
				}
				c.phase = 28
			} else {
				c.phase = 29
			}
		case 28:
			cmp, done := c.comparison.Step(w)
			if !done {
				return false, nil
			}
			if cmp >= 0 {
				if !c.reverse {
					c.phase = 30
				} else {
					c.phase = 32
				}
				continue
			}
			c.phase = 29
		case 29:
			if !w.Reserve(1, 32) {
				return false, nil
			}
			c.valid = true
			c.pending = false
			c.positioned = true
			c.key = nil
			return true, nil
		case 30:
			if !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))+32) {
				return false, nil
			}
			c.record = node.LeafEntry{}
			c.valid = false
			c.pending = false
			c.positioned = true
			c.key = nil
			return true, nil
		case 40: // Preserve current-key custody before a direction-changing seek.
			if len(c.turnSource) <= len(c.turnKey) && c.turnCopied < len(c.turnSource) {
				if w.Bytes > w.ByteLimit {
					return false, nil
				}
				amount := int(min(uint64(len(c.turnSource)-c.turnCopied), (w.ByteLimit-w.Bytes)/2))
				if amount == 0 || !w.Reserve(1, uint64(2*amount)) {
					return false, nil
				}
				copy(c.turnKey[c.turnCopied:c.turnCopied+amount], c.turnSource[c.turnCopied:c.turnCopied+amount])
				c.turnCopied += amount
				continue
			}
			if !w.Reserve(1, 96) {
				return false, nil
			}
			c.skipKey = c.turnSource
			if len(c.turnSource) <= len(c.turnKey) {
				c.skipKey = c.turnKey[:len(c.turnSource)]
			}
			// Reconstructed keys use the full existing uint16 prefix/suffix bound.
			// Larger plain keys remain producer-owned immutable image borrows.
			c.turnSource = nil
			c.key = c.skipKey
			c.op = ProjectionSeekGE
			if c.reverse {
				c.op = ProjectionSeekLE
			}
			c.phase = 0
		case 41:
			cmp, done := c.comparison.Step(w)
			if !done {
				return false, nil
			}
			if !w.Reserve(1, 64) {
				return false, nil
			}
			c.skipCurrent = false
			c.skipKey = nil
			if cmp == 0 {
				c.phase = 32
			} else {
				c.phase = 25
			}

		case 32: // a physical record outside the bound still counts; move sources
			if !w.Reserve(1, 32) {
				return false, nil
			}
			c.op = ProjectionNext
			if c.reverse {
				c.op = ProjectionPrev
			}
			c.phase = 3
		}
	}
}
func (c *ChargedProjection) Record(w *iterator.OrdinalScanWork) (node.LeafEntry, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.closing {
		return node.LeafEntry{}, false, ErrProjectionClosed
	}
	if c.err != nil {
		return node.LeafEntry{}, false, c.err
	}
	if c.pending || !c.valid {
		return node.LeafEntry{}, false, nil
	}
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(node.LeafEntry{}))) {
		return node.LeafEntry{}, false, cowbtree.ErrWork
	}
	return c.record, true, nil
}

// CloseStep cancels the phased backend operation, drops decoder/stack references
// through charged phases, then ends this consumer's borrow. It invokes no arena
// Drop, resource callback, file unlink or reclamation/refund. The original root
// owner keeps custody and may finish its separately charged cleanup afterward.
func (c *ChargedProjection) CloseStep(w *iterator.OrdinalScanWork) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return true, nil
	}
	if w == nil {
		return false, nil
	}
	if !c.closing {
		if !w.Reserve(1, 32) {
			return false, nil
		}
		c.closing = true
	}
	for c.closeIndex < maxTraversalDepth {
		if !w.Reserve(1, uint64(unsafe.Sizeof(CursorItem{}))) {
			return false, nil
		}
		c.base.stack[c.closeIndex] = CursorItem{}
		c.closeIndex++
	}
	for c.closeIndex < maxTraversalDepth+4 {
		var done bool
		switch c.closeIndex - maxTraversalDepth {
		case 0:
			done = c.base.decoder.Cancel(w)
		case 1:
			done = c.deltaDecoder.Cancel(w)
		case 2:
			done = c.base.comparison.Cancel(w)
		case 3:
			done = c.comparison.Cancel(w)
		}
		if !done {
			return false, nil
		}
		c.closeIndex++
	}

	if c.closeIndex == maxTraversalDepth+4 {
		if !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))*2+uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))+uint64(unsafe.Sizeof(node.PrimaryDirectoryView{}))+uint64(unsafe.Sizeof(node.Node{}))+256) {
			return false, nil
		}
		c.directory = node.PrimaryDirectoryView{}
		c.entry = node.PrimaryDirectoryEntry{}
		c.record = node.LeafEntry{}
		c.frameNode = node.Node{}
		c.start, c.end, c.key = nil, nil, nil
		c.turnSource, c.skipKey = nil, nil
		c.base.image, c.base.key = nil, nil
		c.base.record = node.LeafEntry{}
		c.closeIndex++
	}
	if c.closeIndex == maxTraversalDepth+5 {
		// A completed reader can retain aliases too. Cancel exactly once after
		// consumer views are gone, while the original certificate owner lives.
		done, err := c.reader.CancelProjectionFrame(w)
		if err != nil || !done {
			return false, err
		}
		c.closeIndex++
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(Tree{}))+96) {
		return false, nil
	}
	c.tree = Tree{}
	c.certificate = nil
	c.reader, c.base.reader = nil, nil
	c.base.source = nil
	c.frameActive, c.base.frameActive = false, false

	c.base.closed = true
	c.closed = true
	c.pending = false
	return true, nil
}

type projectionBase struct {
	image     []byte
	crcOffset int
	crcSum    uint32

	source                                          *Tree
	reader                                          ProjectionFrameReader
	pages                                           *[maxTraversalDepth][page.PageSize]byte
	stack                                           [maxTraversalDepth]CursorItem
	keyScratch                                      []byte
	decoder                                         node.ProjectionDecoder
	comparison                                      node.ProjectionComparison
	root, ref                                       page.ChildRef
	digest                                          [32]byte
	key                                             []byte
	record                                          node.LeafEntry
	op                                              ProjectionOperation
	phase                                           uint8
	depth, clearAt, lo, hi, pivot, index, direction int
	valid, pending, closed, frameActive             bool
}

func (b *projectionBase) begin(op ProjectionOperation, key []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if b.pending {
		return false, ErrProjectionPending
	}
	if !w.Reserve(1, 64) {
		return false, nil
	}
	b.op = op
	b.key = key
	b.pending = true
	b.direction = 1
	if op == ProjectionSeekLE || op == ProjectionLast || op == ProjectionPrev {
		b.direction = -1
	}
	if op == ProjectionNext || op == ProjectionPrev {
		b.phase = 20
	} else {
		b.clearAt = 0
		b.phase = 0
	}
	return true, nil
}
func (b *projectionBase) step(w *iterator.OrdinalScanWork) (bool, error) {
	for {
		switch b.phase {
		case 0:
			if b.clearAt < b.depth {
				if !w.Reserve(1, uint64(unsafe.Sizeof(CursorItem{}))) {
					return false, nil
				}
				b.stack[b.clearAt] = CursorItem{}
				b.clearAt++
				continue
			}
			if !w.Reserve(1, 64) {
				return false, nil
			}
			b.depth = 0
			b.ref = b.root
			b.valid = false
			b.phase = 1
		case 1:
			if !w.Reserve(1, uint64(unsafe.Sizeof(page.ChildRef{}))+32) {
				return false, nil
			}
			if b.depth >= maxTraversalDepth {
				return false, ErrProjectionDepth
			}
			if (b.ref.Kind != page.ChildRefPage && b.ref.Kind != page.ChildRefLeafLog) ||
				(b.ref.Kind == page.ChildRefPage && (b.ref.Log != (page.LogRecordRef{}) || b.ref.Page < 2 || (b.source.pageLimit != 0 && b.ref.Page >= b.source.pageLimit) || (b.source.pageNamespace != 0 && b.ref.Page < b.source.pageNamespace+2))) ||
				(b.ref.Kind == page.ChildRefLeafLog && b.ref.Page != 0) {
				return false, ErrProjectionOwner
			}
			b.phase = 31
		case 31:
			ready, err := b.reader.BeginProjectionFrame(b.ref, b.pages[b.depth][:], w)
			if err != nil || !ready {
				return false, err
			}
			b.frameActive = true
			b.phase = 2
		case 2:
			image, done, err := b.reader.StepProjectionFrame(w)
			if err != nil || !done {
				return false, err
			}
			if len(image) < page.PageSize || (b.ref.Kind == page.ChildRefPage && (len(image) != page.PageSize || &image[0] != &b.pages[b.depth][0])) {
				return false, ErrProjectionReader
			}
			b.image = image
			b.crcOffset = 0
			b.crcSum = 0
			b.frameActive = false
			b.phase = 3
		case 3:
			if b.crcOffset < len(b.image) {
				if w.Bytes > w.ByteLimit {
					return false, nil
				}
				amount := int(min(uint64(len(b.image)-b.crcOffset), w.ByteLimit-w.Bytes))
				if amount == 0 || !w.Reserve(1, uint64(amount)) {
					return false, nil
				}
				end := b.crcOffset + amount
				// The checksum's four bytes are logical zeroes, never a source mutation.
				at := b.crcOffset
				if at < 8 {
					edge := min(end, 8)
					b.crcSum = crc.Update(b.crcSum, b.image[at:edge])
					at = edge
				}
				if at < 12 && at < end {
					edge := min(end, 12)
					var zero [4]byte
					b.crcSum = crc.Update(b.crcSum, zero[:edge-at])
					at = edge
				}
				if at < end {
					b.crcSum = crc.Update(b.crcSum, b.image[at:end])
				}
				b.crcOffset = end
				continue
			}
			if !w.Reserve(1, 16+uint64(unsafe.Sizeof(CursorItem{}))) {
				return false, nil
			}
			data := b.image
			if binary.LittleEndian.Uint32(data[8:12]) != b.crcSum {
				return false, node.ErrCorruptedNode
			}
			if b.ref.Kind == page.ChildRefPage && page.DecodeHeader(data).PageID != b.ref.Page {
				return false, ErrProjectionOwner
			}
			n := node.NewNodeView(data)
			if n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal {
				return false, page.ErrInvalidPageType
			}
			if b.ref.Kind == page.ChildRefLeafLog && n.Type() != page.PageTypeLeaf {
				return false, page.ErrInvalidPageType
			}
			if n.Type() == page.PageTypeInternal && len(data) != page.PageSize {
				return false, ErrProjectionReader
			}
			b.stack[b.depth] = CursorItem{PageID: b.ref.Page, Ref: b.ref, Node: n}
			b.depth++
			b.image = nil
			b.phase = 4
		case 4:
			if b.depth == 1 && b.digest != ([32]byte{}) {
				if !w.Reserve(1, page.PageSize+32) {
					return false, nil
				}
				if sha256.Sum256(b.pages[0][:]) != b.digest {
					return false, node.ErrPrimaryDirectory
				}
			}
			b.phase = 5
		case 5:
			top := &b.stack[b.depth-1]
			if !w.Reserve(1, 32) {
				return false, nil
			}
			count := int(top.Node.Count())
			if count == 0 {
				if top.Node.Type() != page.PageTypeLeaf {
					return false, node.ErrCorruptedNode
				}
				b.phase = 21
				continue
			}
			if b.op == ProjectionFirst || b.op == ProjectionLast || b.key == nil || b.op == ProjectionNext || b.op == ProjectionPrev {
				b.index = 0
				if b.direction < 0 {
					b.index = count - 1
				}
				b.phase = 12
			} else {
				b.lo = 0
				b.hi = count
				b.phase = 6
			}
		case 6:
			if b.lo == b.hi {
				if !w.Reserve(1, 16) {
					return false, nil
				}
				b.index = b.lo
				top := &b.stack[b.depth-1]
				if top.Node.Type() == page.PageTypeInternal || b.direction < 0 {
					b.index--
				}
				if top.Node.Type() == page.PageTypeInternal && b.index < 0 {
					b.index = 0
				}
				b.phase = 12
				continue
			}
			if !w.Reserve(1, 8) {
				return false, nil
			}
			b.pivot = b.lo + (b.hi-b.lo)/2
			b.phase = 7
		case 7:
			ready, err := b.decoder.Begin(b.stack[b.depth-1].Node, uint16(b.pivot), b.keyScratch, w)
			if err != nil || !ready {
				return false, err
			}
			b.phase = 8
		case 8:
			done, err := b.decoder.Step(w)
			if err != nil || !done {
				return false, err
			}
			b.phase = 9
		case 9:
			e, _ := b.decoder.Result()
			if !b.comparison.Begin(e.Key, b.key, w) {
				return false, nil
			}
			b.phase = 10
		case 10:
			cmp, done := b.comparison.Step(w)
			if !done {
				return false, nil
			}
			if !w.Reserve(1, 16) {
				return false, nil
			}
			upper := b.stack[b.depth-1].Node.Type() == page.PageTypeInternal || b.direction < 0
			if cmp < 0 || (upper && cmp == 0) {
				b.lo = b.pivot + 1
			} else {
				b.hi = b.pivot
			}
			b.phase = 6
		case 12:
			top := &b.stack[b.depth-1]
			if !w.Reserve(1, 8) {
				return false, nil
			}
			top.Index = b.index
			if top.Node.Type() == page.PageTypeInternal {
				b.phase = 13
			} else if b.index < 0 || b.index >= int(top.Node.Count()) {
				b.phase = 21
			} else {
				b.phase = 15
			}
		case 13:
			if !w.Reserve(1, page.PageSize+uint64(unsafe.Sizeof(page.ChildRef{}))) {
				return false, nil
			}
			var err error
			top := &b.stack[b.depth-1]
			b.ref, err = top.Node.GetInternalChildRef(uint16(top.Index))
			if err != nil {
				return false, err
			}
			b.phase = 1
		case 15:
			top := &b.stack[b.depth-1]
			ready, err := b.decoder.Begin(top.Node, uint16(top.Index), b.keyScratch, w)
			if err != nil || !ready {
				return false, err
			}
			b.phase = 16
		case 16:
			done, err := b.decoder.Step(w)
			if err != nil || !done {
				return false, err
			}
			b.phase = 17
		case 17:
			if !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))) {
				return false, nil
			}
			b.record, _ = b.decoder.Result()
			b.valid = true
			b.pending = false
			b.key = nil
			return true, nil
		case 20:
			if !b.valid {
				b.phase = 30
				continue
			}
			if !w.Reserve(1, 8) {
				return false, nil
			}
			b.index = b.stack[b.depth-1].Index + b.direction
			b.valid = false
			b.phase = 12
		case 21:
			if b.depth == 0 {
				b.phase = 30
				continue
			}
			if !w.Reserve(1, uint64(unsafe.Sizeof(CursorItem{}))) {
				return false, nil
			}
			b.depth--
			b.stack[b.depth] = CursorItem{}
			if b.depth == 0 {
				b.phase = 30
			} else {
				b.phase = 22
			}
		case 22:
			if !w.Reserve(1, 16) {
				return false, nil
			}
			top := &b.stack[b.depth-1]
			b.index = top.Index + b.direction
			if b.index < 0 || b.index >= int(top.Node.Count()) {
				b.phase = 21
			} else {
				top.Index = b.index
				b.phase = 13
			}
		case 30:
			if !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))+32) {
				return false, nil
			}
			b.valid = false
			b.pending = false
			b.record = node.LeafEntry{}
			b.key = nil
			return true, nil
		}
	}
}
