package zipper

import (
	"bytes"
	"crypto/sha256"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// MaterializedInputWorkV6 phases the original PRIMARY-to-DATA operands and
// ordered prefix delta. It borrows an immutable root and input vectors. Only
// component payloads are copied, since their one decode page is reused.
// No allocator candidate, publisher or independent authority exists here.
type MaterializedInputWorkV6 struct {
	owner                                                      *retainedalloc.Owner
	charge                                                     uint64
	reader                                                     MaterializedFrameReaderV6
	certificate                                                *primaryarena.ReadRootCertificateV6
	root                                                       uint64
	prefix                                                     []batch.Entry
	deletes                                                    []node.LeafEntry
	entries                                                    []batch.Entry
	components                                                 [node.PrimaryDirectoryMaxEntries]batch.Entry
	directory                                                  node.PrimaryDirectoryView
	entry                                                      node.PrimaryDirectoryEntry
	image, operand, key                                        [page.PageSize]byte
	decoder                                                    node.ProjectionDecoder
	state                                                      MaterializedInputV6
	phase                                                      uint8
	index, component, prefixAt, deleteAt, clearAt, outputClear int
	pending                                                    node.LeafEntry
	closed                                                     bool
	primary                                                    bool
}

func NewMaterializedInputWorkV6(owner *retainedalloc.Owner, reader MaterializedFrameReaderV6, root uint64, certificate *primaryarena.ReadRootCertificateV6, prefix []batch.Entry, deletes []node.LeafEntry, w *iterator.OrdinalScanWork) (*MaterializedInputWorkV6, bool, error) {
	if owner == nil || reader == nil || root < 2 || (certificate != nil && certificate.Reference().PageID != root) {
		return nil, false, ErrMaterializedWriteContinuation
	}
	n := uint64(unsafe.Sizeof(MaterializedInputWorkV6{}))
	if !w.Reserve(1, 2*n) {
		return nil, false, nil
	}
	charge := retainedalloc.AllocationCharge(n)
	if e := owner.AddPending(charge); e != nil {
		return nil, false, e
	}
	return &MaterializedInputWorkV6{owner: owner, charge: charge, reader: reader, root: root, certificate: certificate, prefix: prefix, deletes: deletes}, true, nil
}
func (s *MaterializedInputWorkV6) Step(w *iterator.OrdinalScanWork) (bool, error) {
	for {
		switch s.phase {
		case 0:
			ready, e := s.reader.BeginProjectionFrame(page.PageChildRef(s.root), s.image[:], w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 1
		case 1:
			_, ready, e := s.reader.StepProjectionFrame(w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 2
		case 2:
			n := node.NewNodeView(s.image[:])
			// The original retained Snapshot owns the producer's canonical
			// directory certificate. Validate the complete immutable image
			// against it; do not reparse all classes as an indivisible unit.
			if !w.Reserve(3, 7*page.PageSize) {
				return false, nil
			}
			if !page.VerifyChecksumNonMutating(s.image[:]) {
				return false, node.ErrPrimaryDirectory
			}
			digest := sha256.Sum256(s.image[:])
			s.state.Base = node.PrimaryOperand{Ref: page.PageChildRef(s.root), Digest: digest}
			if n.Type() == page.PageTypePrimaryDirectory {
				cert := s.certificate
				if cert == nil || cert.Reference().PageID != s.root || digest != cert.Digest() || !bytes.Equal(s.image[:], cert.Image()) {
					return false, node.ErrPrimaryDirectory
				}
				s.directory = cert.Directory().WithPhysicalImage(s.image[:])
				s.primary = true
				s.state.Base, s.state.Sequence = s.directory.Base()
			} else if n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal {
				return false, node.ErrPrimaryDirectory
			}
			s.phase = 3
		case 3:
			capacity := len(s.prefix) + len(s.deletes) + s.directory.Count()
			n := uint64(capacity) * uint64(unsafe.Sizeof(batch.Entry{}))
			if !w.Reserve(1, 2*n) {
				return false, nil
			}
			charge := retainedalloc.AllocationCharge(n)
			if e := s.owner.AddPending(charge); e != nil {
				return false, e
			}
			s.charge += charge
			s.entries = make([]batch.Entry, 0, capacity)
			if !s.primary {
				s.phase = 10
				continue
			}
			s.phase = 4
		case 4:
			ready, e := s.reader.BeginProjectionFrame(s.state.Base.Ref, s.operand[:], w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 5
		case 5:
			_, ready, e := s.reader.StepProjectionFrame(w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 6
		case 6:
			if !w.Reserve(2, 3*page.PageSize) {
				return false, nil
			}
			n := node.NewNodeView(s.operand[:])
			if !node.VerifyPrimaryOperand(s.state.Base, s.operand[:]) || (n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal) {
				return false, node.ErrPrimaryDirectory
			}
			s.phase = 7
		case 7:
			if s.index == s.directory.Count() {
				s.phase = 10
				continue
			}
			if !w.Reserve(1, page.PageSize+2*uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))) {
				return false, nil
			}
			entry, e := s.directory.Entry(s.index)
			if e != nil {
				return false, e
			}
			s.entry = entry
			if entry.InlineAbsence() {
				s.components[s.index] = batch.Entry{Type: batch.OpDelete, Key: entry.Key, Revision: entry.Revision}
				s.index++
				continue
			}
			s.phase = 8
		case 8:
			ready, e := s.reader.BeginProjectionFrame(s.entry.Operand.Ref, s.operand[:], w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 9
		case 9:
			_, ready, e := s.reader.StepProjectionFrame(w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 20
		case 20:
			if !w.Reserve(2, 3*page.PageSize) {
				return false, nil
			}
			n := node.NewNodeView(s.operand[:])
			if !node.VerifyPrimaryOperand(s.entry.Operand, s.operand[:]) || n.Type() != page.PageTypeLeaf || n.Count() != 1 ||
				(s.entry.Operand.Ref.IsPage() && page.DecodeHeader(s.operand[:]).PageID != s.entry.Operand.Ref.Page) {
				return false, node.ErrPrimaryDirectory
			}
			s.phase = 21
		case 21:
			ready, e := s.decoder.Begin(*node.NewNode(s.operand[:]), 0, s.key[:], w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 22
		case 22:
			ready, e := s.decoder.Step(w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 23
		case 23:
			if !w.Reserve(2, 2*page.PageSize) {
				return false, nil
			}
			record, _ := s.decoder.Result()
			if !bytes.Equal(record.Key, s.entry.Key) || record.Revision != s.entry.Revision ||
				(s.entry.Kind == node.PrimaryAbsence) != (record.Flags&node.FlagTombstone != 0) {
				return false, node.ErrPrimaryDirectory
			}
			s.pending = record
			s.phase = 24
		case 24:
			r := s.pending
			size := len(r.Key) + len(r.Value)
			if !w.Reserve(2, 3*uint64(size)+2*uint64(unsafe.Sizeof(batch.Entry{}))) {
				return false, nil
			}
			charge := retainedalloc.AllocationCharge(uint64(size))
			if e := s.owner.AddPending(charge); e != nil {
				return false, e
			}
			s.charge += charge
			payload := make([]byte, size)
			copy(payload, r.Key)
			copy(payload[len(r.Key):], r.Value)
			op := batch.Entry{Type: batch.OpPut, Key: payload[:len(r.Key):len(r.Key)], Value: payload[len(r.Key):], ValuePtr: r.ValuePtr, IsPtr: r.Flags&node.FlagPointer != 0, Revision: r.Revision}
			if r.Flags&node.FlagTombstone != 0 {
				op.Type = batch.OpDelete
				op.Value = nil
			}
			s.components[s.index] = op
			if s.entry.Operand.Ref.IsPage() && !primaryarena.IsPage(s.entry.Operand.Ref.Page) {
				s.state.Retired[s.state.RetiredCount] = s.entry.Operand.Ref.Page
				s.state.RetiredCount++
			}
			s.pending = node.LeafEntry{}
			s.index++
			s.phase = 7
		case 10:
			// Three sorted streams: original components, canonical prefix winners,
			// and selected deletes. Later operands win equal keys.
			ci, pi, di := s.component, s.prefixAt, s.deleteAt
			if ci == s.directory.Count() && pi == len(s.prefix) && di == len(s.deletes) {
				s.phase = 11
				continue
			}
			var component, prefix, del batch.Entry
			if ci < s.directory.Count() {
				component = s.components[ci]
			}
			if pi < len(s.prefix) {
				prefix = s.prefix[pi]
			}
			if di < len(s.deletes) {
				r := s.deletes[di]
				del = batch.Entry{Type: batch.OpDelete, Key: r.Key, Revision: r.Revision}
			}
			n := uint64(len(component.Key) + len(prefix.Key) + len(del.Key))
			if !w.Reserve(3, 2*n+4*uint64(unsafe.Sizeof(batch.Entry{}))) {
				return false, nil
			}
			key := component.Key
			if ci == s.directory.Count() || (pi < len(s.prefix) && bytes.Compare(prefix.Key, key) < 0) {
				key = prefix.Key
			}
			if (ci == s.directory.Count() && pi == len(s.prefix)) || (di < len(s.deletes) && bytes.Compare(del.Key, key) < 0) {
				key = del.Key
			}
			takeComponent := ci < s.directory.Count() && bytes.Equal(component.Key, key)
			takePrefix := pi < len(s.prefix) && bytes.Equal(prefix.Key, key)
			takeDelete := di < len(s.deletes) && bytes.Equal(del.Key, key)
			op := component
			if takePrefix {
				op = prefix
			}
			if takeDelete {
				op = del
			}
			if takeComponent {
				if takePrefix || takeDelete {
					if component.IsPtr && component.ValuePtr.FileID != 0 {
						id := component.ValuePtr.FileID
						if s.state.Refs.Count(id) == 0 && int(s.state.Refs.inlineN) == len(s.state.Refs.inline) {
							return false, ErrMaterializedWriteContinuation
						}
						s.state.Refs.add(id, 1)
					}
					// Original materialization counts removal only for real
					// component entries, not inline absence descriptors.
					if !s.mustInlineAbsence(ci) {
						s.state.Removed++
					}
				}
				s.component++
			}
			if takePrefix {
				s.prefixAt++
			}
			if takeDelete {
				s.deleteAt++
			}
			s.entries = append(s.entries, op)
		case 11:
			return true, nil
		default:
			return false, ErrMaterializedWriteContinuation
		}
	}
}
func (s *MaterializedInputWorkV6) mustInlineAbsence(i int) bool {
	// Canonical view already validated above; this field read has no traversal.
	e, _ := s.directory.Entry(i)
	return e.InlineAbsence()
}
func (s *MaterializedInputWorkV6) Result() ([]batch.Entry, MaterializedInputV6, bool) {
	return s.entries, s.state, s.phase == 11
}

// CloseStep ends borrowed input/output lifetimes before returning the original
// containing charge. The caller detaches this producer before consuming it.
func (s *MaterializedInputWorkV6) CloseStep(w *iterator.OrdinalScanWork) (bool, *retainedalloc.Owner, uint64, error) {
	if !s.decoder.Cancel(w) {
		return false, nil, 0, nil
	}
	for s.clearAt < len(s.components) {
		e := &s.components[s.clearAt]
		if !w.Reserve(1, uint64(len(e.Key)+len(e.Value))+2*uint64(unsafe.Sizeof(*e))) {
			return false, nil, 0, nil
		}
		// Payload aliases may also occur in entries; clear both vectors before
		// refund. Bytes need not be zeroed because no descriptor remains.
		*e = batch.Entry{}
		s.clearAt++
	}
	for s.outputClear < len(s.entries) {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(batch.Entry{}))) {
			return false, nil, 0, nil
		}
		s.entries[s.outputClear] = batch.Entry{}
		s.outputClear++
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
		return false, nil, 0, nil
	}
	owner, charge := s.owner, s.charge
	*s = MaterializedInputWorkV6{closed: true}
	return true, owner, charge, nil
}
