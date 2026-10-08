package rootpublication

import (
	"bytes"
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"math"
	"unsafe"
)

// Strings in this reader are borrowed from a retained immutable iterator record.
// They may only be used synchronously; a retaining constructor must admit/copy.
// The generic public decoder continues returning independently owned strings.
type dependencyBorrowReader struct{ manifestReaderV1 }

func (r *dependencyBorrowReader) str() (string, bool) {
	n, ok := r.u32()
	if !ok || uint64(n) > uint64(r.remaining()) {
		return "", false
	}
	b, _ := r.bytes(int(n))
	return unsafe.String(unsafe.SliceData(b), len(b)), true
}
func decodeBorrowedPhysicalKey(key []byte) (stableLogicalResourceKey, error) {
	var k stableLogicalResourceKey
	if len(key) == 0 || key[0] != dependencyPhysicalKeyV2 {
		return k, ErrDependencyManifestFormat
	}
	r := dependencyBorrowReader{manifestReaderV1{data: key, offset: 1}}
	var a2, a3, a4 bool
	kind, a := r.str()
	k.kind = ResourceKind(kind)
	k.lane, a2 = r.str()
	k.resourceID, a3 = r.str()
	k.generation, a4 = r.u64()
	if !a || !a2 || !a3 || !a4 || r.remaining() != 0 || k.kind == "" || k.resourceID == "" || k.generation == 0 {
		return stableLogicalResourceKey{}, ErrDependencyManifestFormat
	}
	return k, nil
}
func decodeBorrowedLogicalV2(key, value []byte) ([]byte, StableLogicalObligation, error) {
	var o StableLogicalObligation
	if len(key) == 0 || key[0] != dependencyLogicalKeyV2 || len(value) < 40 {
		return nil, o, ErrDependencyManifestFormat
	}
	n := uint64(binary.LittleEndian.Uint32(value))
	if n == 0 || n != uint64(len(value)-40) {
		return nil, o, ErrDependencyManifestFormat
	}
	owner := value[4 : 4+int(n)]
	if _, err := decodeBorrowedPhysicalKey(owner); err != nil {
		return nil, o, err
	}
	r := dependencyBorrowReader{manifestReaderV1{data: key, offset: 1}}
	var a, b, c, d, e, f, g, h, i bool
	var off, length uint64
	var field string
	o.Class, a = r.str()
	o.Kind, b = r.str()
	o.Namespace, c = r.str()
	o.Generation, d = r.u64()
	o.PartID, e = r.u64()
	o.FileID, f = r.u64()
	off, g = r.u64()
	length, h = r.u64()
	field, i = r.str()
	if !a || !b || !c || !d || !e || !f || !g || !h || !i || r.remaining() != 0 || off > math.MaxInt64 || length > math.MaxInt64 {
		return nil, StableLogicalObligation{}, ErrDependencyManifestFormat
	}
	o.Offset, o.Length = int64(off), int64(length)
	o.Reachability = ReachabilityField(field)
	o.Checksum = binary.LittleEndian.Uint32(value[4+int(n):])
	copy(o.Digest[:], value[8+int(n):])
	if err := validateStableLogicalObligation(o, o.Reachability); err != nil {
		return nil, StableLogicalObligation{}, ErrDependencyManifestFormat
	}
	return owner, o, nil
}

// Inline directory records cannot exceed one page. This exact fixed backing
// includes canonical-codec arrays, namespace and ordering scratch. It is admitted
// before allocation, in addition to the actual fixed iterator allocation.
type dependencyBorrowScratch struct {
	previous   [page.PageSize]byte
	fields     [page.PageSize / 4]ReachabilityField
	rids       [page.PageSize / 8]uint64
	membership exactRIDMembership
	namespace  DependencyManifestNamespaceV1
}

func (s *dependencyBorrowScratch) physical(key, value []byte) (DependencyManifestEntryV1, error) {
	var e DependencyManifestEntryV1
	bad := func() (DependencyManifestEntryV1, error) {
		return DependencyManifestEntryV1{}, ErrDependencyManifestFormat
	}
	k, err := decodeBorrowedPhysicalKey(key)
	if err != nil {
		return bad()
	}
	r := dependencyBorrowReader{manifestReaderV1{data: value}}
	var a2, a3, a4, a5, a6, a8, a9, a11, a12 bool
	kind, a := r.str()
	e.Kind = ResourceKind(kind)
	e.LogicalLane, a2 = r.str()
	e.ResourceID, a3 = r.str()
	e.DiagnosticPath, a4 = r.str()
	e.Identity.Platform, a5 = r.str()
	e.Identity.VolumeID, a6 = r.u64()
	object, a7 := r.bytes(16)
	e.Identity.Generation, a8 = r.u64()
	e.Generation, a9 = r.u64()
	digest, a10 := r.bytes(32)
	e.Frontier.Bytes, a11 = r.u64()
	e.Frontier.MaxLSN, a12 = r.u64()
	if !a || !a2 || !a3 || !a4 || !a5 || !a6 || !a7 || !a8 || !a9 || !a10 || !a11 || !a12 {
		return bad()
	}
	copy(e.Identity.ObjectID[:], object)
	copy(e.Digest[:], digest)
	if e.Kind != k.kind || e.LogicalLane != k.lane || e.ResourceID != k.resourceID || e.Generation != k.generation || !e.Identity.valid() || e.Identity.Generation != e.Generation || validateDiagnosticPath(e.DiagnosticPath) != nil {
		return bad()
	}
	count, ok := r.u32()
	if !ok || uint64(count) > uint64(len(s.rids)) || uint64(count) > uint64(r.remaining()/8) {
		return bad()
	}
	for j := 0; j < int(count); j++ {
		v, ok := r.u64()
		if !ok || (j > 0 && v <= s.rids[j-1]) {
			return bad()
		}
		s.rids[j] = v
	}
	if count > 0 {
		s.membership.values = s.rids[:count]
		b, lsn := e.Frontier.Bytes, e.Frontier.MaxLSN
		e.Frontier = exactRIDSummary(s.membership.values)
		e.Frontier.Bytes, e.Frontier.MaxLSN = b, lsn
		e.Frontier.exactRIDs = &s.membership
	}
	if validateDurableFrontier(e.Frontier) != nil {
		return bad()
	}
	count, ok = r.u32()
	if !ok || count == 0 || uint64(count) > uint64(len(s.fields)) || uint64(count) > uint64(r.remaining()/4) {
		return bad()
	}
	for j := 0; j < int(count); j++ {
		f, ok := r.str()
		if !ok || f == "" || (j > 0 && f <= string(s.fields[j-1])) {
			return bad()
		}
		s.fields[j] = ReachabilityField(f)
	}
	e.Reachability = s.fields[:count]
	logical, ok := r.u32()
	if !ok || logical != 0 {
		return bad()
	}
	present, ok := r.u32()
	if !ok || present > 1 {
		return bad()
	}
	if present == 1 {
		n := &s.namespace
		*n = DependencyManifestNamespaceV1{}
		var a, b, c, d, f, g, h, i bool
		var obj []byte
		var op uint32
		n.ParentIdentity.Platform, a = r.str()
		n.ParentIdentity.VolumeID, b = r.u64()
		obj, c = r.bytes(16)
		n.ParentIdentity.Generation, d = r.u64()
		op, f = r.u32()
		n.OldName, g = r.str()
		n.NewName, h = r.str()
		n.DiagnosticPath, i = r.str()
		if !a || !b || !c || !d || !f || !g || !h || !i || op > math.MaxUint8 {
			return bad()
		}
		copy(n.ParentIdentity.ObjectID[:], obj)
		n.Operation = NamespaceOperation(op)
		if !n.ParentIdentity.valid() || n.ParentIdentity.Generation == 0 || (n.Operation != NamespaceCreate && n.Operation != NamespaceRename) || !stableChildBaseName(n.NewName) || (n.Operation == NamespaceRename && !stableChildBaseName(n.OldName)) || validateDiagnosticPath(n.DiagnosticPath) != nil {
			return bad()
		}
		e.Namespace = n
	}
	if r.remaining() != 0 {
		return bad()
	}
	return e, nil
}

// No pooled iterator, growable maps, key copies, decoded strings or hidden RID
// constructors are reached. Exact canonical physical and logical records and
// all counts are checked, including records unrelated to the selected entry.
func (d *DependencyDirectoryV2) walkBorrowed(owner *retainedalloc.Owner, visit func([]byte, StableLogicalObligation) error) error {
	return d.walkBorrowedRecords(owner, visit, nil)
}

// ValidateWithMetadataV2 checks every canonical record with admitted fixed
// scratch. check is a synchronous cancellation guard, not a retained callback.
func (d *DependencyDirectoryV2) ValidateWithMetadataV2(owner *retainedalloc.Owner, check func() error) error {
	return d.walkBorrowedRecords(owner, nil, check)
}

func (d *DependencyDirectoryV2) walkBorrowedRecords(owner *retainedalloc.Owner, visit func([]byte, StableLogicalObligation) error, check func() error) error {
	if owner == nil || d == nil {
		return ErrResourceOwnership
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(dependencyBorrowScratch{}))) + 2*retainedalloc.AllocationCharge(tree.OwnedPointerProjectionAllocationSize())
	if err := owner.AddPending(charge); err != nil {
		return err
	}
	defer owner.RemovePending(charge)
	if err := d.Retain(); err != nil {
		return err
	}
	defer d.Release()
	s := new(dependencyBorrowScratch)
	defer func() { clear(s.fields[:]); s.membership.values = nil; s.namespace = DependencyManifestNamespaceV1{} }()
	it := d.tree.OwnedPointerProjectionIterator(nil, nil, nil)
	defer it.Close()
	previous := s.previous[:0]
	var physical, logical uint64
	for ; it.Valid(); it.Next() {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		key := it.UnsafeKey()
		value, _, flags := it.UnsafeEntry()
		if len(key) == 0 || len(key) > len(s.previous) || flags&(node.FlagPointer|node.FlagTombstone) != 0 || (len(previous) > 0 && bytes.Compare(previous, key) >= 0) {
			return ErrDependencyManifestFormat
		}
		switch key[0] {
		case dependencyPhysicalKeyV2:
			if physical >= d.ref.PhysicalCount {
				return ErrDependencyManifestFormat
			}
			if _, err := s.physical(key, value); err != nil {
				return err
			}
			physical++
		case dependencyLogicalKeyV2:
			if logical >= d.ref.LogicalCount {
				return ErrDependencyManifestFormat
			}
			ownerKey, o, err := decodeBorrowedLogicalV2(key, value)
			if err != nil {
				return err
			}
			// Exact lookup uses its own already admitted iterator. It avoids GetAppend's
			// allocating returned value and preserves a validated immutable tree owner.
			target := d.tree.OwnedPointerProjectionIterator(ownerKey, nil, nil)
			if !target.Valid() || !bytes.Equal(target.UnsafeKey(), ownerKey) {
				err = target.Error()
				target.Close()
				if err != nil {
					return err
				}
				return ErrDependencyManifestFormat
			}
			record, _, f := target.UnsafeEntry()
			if f&(node.FlagPointer|node.FlagTombstone) != 0 {
				target.Close()
				return ErrDependencyManifestFormat
			}
			entry, err := s.physical(ownerKey, record)
			if err == nil {
				err = dependencyLogicalOwnerV2(entry, o)
			}
			target.Close()
			if err != nil {
				return err
			}
			if visit != nil {
				if err := visit(ownerKey, o); err != nil {
					return err
				}
			}
			logical++
		default:
			return ErrDependencyManifestFormat
		}
		previous = s.previous[:len(key)]
		copy(previous, key)
	}
	if err := it.Error(); err != nil {
		return err
	}
	if physical != d.ref.PhysicalCount || logical != d.ref.LogicalCount {
		return ErrDependencyManifestFormat
	}
	return nil
}

// WithReboundDependencyPhysicalV2 borrows one immutable record only through the
// callbacks. Fixed admitted parser/output backing cannot escape this scope.
// The shared canonical encoder replaces only exact-width physical identities.
func WithReboundDependencyPhysicalV2(owner *retainedalloc.Owner, key, value []byte, rebind func(DependencyManifestEntryV1) (StableIdentity, StableIdentity, error), visit func([]byte) error) error {
	if owner == nil || rebind == nil || visit == nil || len(value) > page.PageSize {
		return ErrResourceOwnership
	}
	type scratch struct {
		parser  dependencyBorrowScratch
		encoded [page.PageSize]byte
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(scratch{})))
	if err := owner.AddPending(charge); err != nil {
		return err
	}
	s := new(scratch)
	defer func() { *s = scratch{}; owner.RemovePending(charge) }()
	entry, err := s.parser.physical(key, value)
	if err != nil {
		return err
	}
	child, parent, err := rebind(entry)
	if err != nil {
		return err
	}
	if !child.valid() || child.Generation != entry.Generation || child.Platform != entry.Identity.Platform {
		return ErrResourceConflict
	}
	child.Platform = entry.Identity.Platform
	entry.Identity = child
	if entry.Namespace != nil {
		if !parent.valid() || parent.Generation != entry.Namespace.ParentIdentity.Generation || parent.Platform != entry.Namespace.ParentIdentity.Platform {
			return ErrResourceConflict
		}
		parent.Platform = entry.Namespace.ParentIdentity.Platform
		entry.Namespace.ParentIdentity = parent
	} else if parent != (StableIdentity{}) {
		return ErrResourceConflict
	}
	if !canonicalDecodedManifestEntryV1(entry) {
		return ErrDependencyManifestFormat
	}
	// Shape and string lengths are invariant, so the canonical encoder cannot
	// grow beyond the pre-admitted exact one-page destination.
	var rids []uint64
	if entry.Frontier.exactRIDs != nil {
		rids = entry.Frontier.exactRIDs.values
	}
	if dependencyManifestEntrySizeV1(entry, rids) != len(value) {
		return ErrResourceConflict
	}
	encoded := appendDependencyManifestEntryV1(s.encoded[:0], entry, rids)
	if len(encoded) != len(value) || cap(encoded) != cap(s.encoded) {
		return ErrResourceConflict
	}
	return visit(encoded)
}
