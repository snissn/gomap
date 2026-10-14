package node

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// ProjectionKeyCapacity covers the existing encoded prefix/suffix geometry.
// Prefix-v1/v2 and producer columnar-prefix-v2 encode each constituent in a
// uint16; internal base-delta does likewise. This is not a new key limit.
// Plain keys remain borrowed; no complete key is allocated/copied eagerly.
const ProjectionKeyCapacity = 2 * int(^uint16(0))

// ProjectionDecoder phases physical entry metadata and prefix reconstruction on
// one immutable page. It uses the SAME Node layout parsers as ordinary reads;
// it never invokes SearchLeaf or the synchronous prefix-chain decoder. Scratch
// and the page must stay owned until completion/cancellation. No payload read,
// allocation, root lookup, publication or independent work counter occurs here.
type ProjectionDecoder struct {
	n              Node
	scratch        []byte
	target, at     uint16
	prefix, copied int
	suffix, prior  []byte
	leaf           LeafEntry
	child          page.ChildRef
	phase          uint8
	active         bool
}

func (d *ProjectionDecoder) Begin(n Node, index uint16, scratch []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if d.active {
		return false, ErrProjectionPending
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*d))+uint64(unsafe.Sizeof(n))) {
		return false, nil
	}
	if len(n.data) < page.PageSize || index >= n.Count() || len(scratch) < page.PageSize {
		return false, ErrCorruptedNode
	}
	*d = ProjectionDecoder{n: n, scratch: scratch, target: index, at: index, active: true}
	if n.Type() == page.PageTypeLeaf && n.leafPrefixCompressed() {
		d.at = index - index%leafPrefixRestartInterval
	}
	return true, nil
}

// Each header parser below has fixed field reads and no entry walk. The full
// page reservation conservatively covers those field/header operands; key-byte
// copying is priced separately. Each prefix predecessor is a separate record.
func (d *ProjectionDecoder) parse() error {
	n := &d.n
	if n.Type() == page.PageTypeInternal {
		if !n.internalBaseDelta() {
			k, r, e := n.GetInternalEntryRefView(d.at)
			d.suffix = k
			d.child = r
			return e
		}
		m, e := n.internalBaseDeltaMeta()
		if e != nil {
			return e
		}
		off, e := n.getOffset(d.at)
		if e != nil {
			return e
		}
		ptr := int(off)
		width, head := n.internalBaseDeltaEntryWidths()
		if ptr < NodeHeaderSize || ptr+head > m.footerStart {
			return ErrCorruptedNode
		}
		size := int(getUint16(n.data[ptr : ptr+2]))
		end := ptr + head + size
		if end > m.footerStart {
			return ErrCorruptedNode
		}
		id, e := n.internalBaseDeltaChildIDAtPtr(m, ptr, width, head)
		if e != nil {
			return e
		}
		d.prior = m.prefix
		d.prefix = len(m.prefix)
		d.suffix = n.data[ptr+head : end]
		d.child = page.PageChildRef(id)
		return nil
	}
	if n.Type() != page.PageTypeLeaf {
		return ErrInvalidType
	}
	var e error
	if n.leafColumnar() && n.leafPrefixCompressed() && n.leafPrefixV2() {
		d.prefix, d.suffix, d.leaf.Flags, e = n.leafColumnarPrefixV2KeyMetaAt(d.at)
	} else if n.leafColumnar() && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		d.suffix, e = n.leafColumnarKeyViewAtIndex(int(d.at))
	} else {
		off, err := n.getOffset(d.at)
		if err != nil {
			return err
		}
		l, err := n.leafEntryLayoutAt(int(off))
		if err != nil {
			return err
		}
		start := int(off) + l.keyOff
		end := start + l.suffixLen
		if start < NodeHeaderSize || end < start || end > len(n.data) {
			return ErrCorruptedNode
		}
		d.prefix = l.prefixLen
		d.suffix = n.data[start:end]
		d.leaf.Flags = l.flags
	}
	if e != nil {
		return e
	}
	if n.leafPrefixCompressed() && d.at%leafPrefixRestartInterval == 0 && d.prefix != 0 {
		return ErrCorruptedNode
	}
	// Values are borrowed metadata. The existing value parser performs fixed
	// offset/pointer/revision reads only and never reconstructs a prefix chain.
	if d.at == d.target {
		d.leaf.Value, d.leaf.ValuePtr, d.leaf.Flags, e = n.GetLeafValueView(d.at)
		if e != nil {
			return e
		}
		if n.leafColumnar() && n.leafPrefixCompressed() && n.leafPrefixV2() {
			d.leaf.Revision, e = n.leafColumnarPrefixV2RevisionAt(d.at)
		} else if n.leafColumnar() && n.leafColumnarV2() && !n.leafPrefixCompressed() {
			d.leaf.Revision, e = n.leafColumnarV2RevisionAt(d.at)
		} else {
			off, err := n.getOffset(d.at)
			if err != nil {
				return err
			}
			l, err := n.leafEntryLayoutAt(int(off))
			if err != nil {
				return err
			}
			d.leaf.Revision = l.revision
		}
	}
	return e
}

func (d *ProjectionDecoder) Step(w *iterator.OrdinalScanWork) (bool, error) {
	if !d.active {
		return true, nil
	}
	if w == nil {
		return false, nil
	}
	for {
		switch d.phase {
		case 0:
			if !w.Reserve(1, page.PageSize+uint64(unsafe.Sizeof(LeafEntry{}))) {
				return false, nil
			}
			d.prefix = 0
			d.copied = 0
			d.suffix = nil
			if err := d.parse(); err != nil {
				return false, err
			}
			if d.prefix > len(d.prior) {
				return false, ErrCorruptedNode
			}
			if d.prefix != 0 && d.prefix+len(d.suffix) > len(d.scratch) {
				return false, ErrProjectionGeometry
			}
			d.phase = 1
		case 1:
			if d.prefix == 0 {
				d.leaf.Key = d.suffix
				d.phase = 2
				continue
			}
			total := d.prefix + len(d.suffix)
			if d.copied < total {
				if w.Bytes > w.ByteLimit {
					return false, nil
				}
				amount := min(total-d.copied, int(min((w.ByteLimit-w.Bytes)/2, uint64(page.PageSize))))
				if amount == 0 || !w.Reserve(1, uint64(2*amount)) {
					return false, nil
				}
				// Prefix may already occupy this backing; still price the copy. The
				// suffix starts at prefixLen so it cannot overwrite a needed prefix.
				end := d.copied + amount
				if d.copied < d.prefix {
					edge := min(end, d.prefix)
					copy(d.scratch[d.copied:edge], d.prior[d.copied:edge])
				}
				if end > d.prefix {
					start := max(d.copied, d.prefix)
					copy(d.scratch[start:end], d.suffix[start-d.prefix:end-d.prefix])
				}
				d.copied = end
				if d.copied < total {
					continue
				}
			}
			d.leaf.Key = d.scratch[:total]
			d.phase = 2
		case 2:
			if !w.Reserve(1, uint64(unsafe.Sizeof(LeafEntry{}))+uint64(unsafe.Sizeof(page.ChildRef{}))) {
				return false, nil
			}
			if d.at == d.target {
				d.active = false
				return true, nil
			}
			d.prior = d.leaf.Key
			d.at++
			d.phase = 0
		}
	}
}

// Result is immutable borrowed metadata until the decoder's next Begin/Cancel.
// The enclosing adapter must charge its own record export before calling it.
func (d *ProjectionDecoder) Result() (LeafEntry, page.ChildRef) {
	if d.active {
		return LeafEntry{}, page.ChildRef{}
	}
	return d.leaf, d.child
}
func (d *ProjectionDecoder) Cancel(w *iterator.OrdinalScanWork) bool {
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*d))) {
		return false
	}
	*d = ProjectionDecoder{}
	return true
}
