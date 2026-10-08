package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
	"slices"
	"unsafe"
)

// The containing immutable manifest owns every decoded allocation. Growth
// reserves actual independently rounded backing before allocation, including
// temporary parser output that overlaps the final entry backing.
func growResourceAllocationV1(a *resourceAllocation, bytes uint64) error {
	if bytes > math.MaxUint64-a.charge {
		return retainedalloc.ErrCapacity
	}
	if err := a.owner.AddPending(bytes); err != nil {
		return err
	}
	a.charge += bytes
	return nil
}

// LoadDependencyManifestWithMetadataV1 shares physical and wire validation with
// the legacy loader. The result is publisher-scoped; Entries cannot export its
// admitted strings. WithEntriesV1 and ReleaseOwnedMetadataV1 own exact aliases.
func LoadDependencyManifestWithMetadataV1(source freelist.PageSource, ref DependencyManifestRefV1, owner *retainedalloc.Owner) (*DependencyManifestV1, error) {
	if owner == nil {
		return LoadDependencyManifestV1(source, ref)
	}
	if ref.ByteLength < 16 || ref.ByteLength > maxDependencyManifestBytesV1 || uint64(ref.EntryCount) > (ref.ByteLength-16)/4 {
		return nil, ErrDependencyManifestFormat
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(DependencyManifestV1{}))) +
		retainedalloc.AllocationCharge(uint64(ref.EntryCount)*uint64(unsafe.Sizeof(dependencyManifestEncodedEntryV1{}))) +
		retainedalloc.AllocationCharge(uint64(ref.EntryCount)*uint64(unsafe.Sizeof((*dependencyManifestEncodedEntryV1)(nil))))
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	var payload []byte
	var parsed []DependencyManifestEntryV1
	var backing []dependencyManifestEncodedEntryV1
	var pointers []*dependencyManifestEncodedEntryV1
	done := false
	defer func() {
		clear(parsed)
		if !done {
			clear(pointers)
			clear(backing)
			clear(payload)
			pointers, backing, payload = nil, nil, nil
			allocation.drop()
			allocation.refund()
		}
	}()
	payload, err = loadDependencyManifestPayloadV1(source, ref, allocation)
	if err != nil {
		return nil, err
	}
	parsed, err = decodeDependencyManifestPayloadAllocatedV1(payload, ref.EntryCount, allocation)
	if err != nil {
		return nil, err
	}
	backing = make([]dependencyManifestEncodedEntryV1, len(parsed))
	pointers = make([]*dependencyManifestEncodedEntryV1, len(parsed))
	offset := 16
	var prior []byte
	for i, entry := range parsed {
		length := int(binary.LittleEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		raw := payload[offset : offset+length]
		offset += length
		if (i > 0 && bytes.Compare(prior, raw) >= 0) || !canonicalDecodedManifestEntryV1(entry) {
			return nil, ErrDependencyManifestFormat
		}
		backing[i] = dependencyManifestEncodedEntryV1{entry: entry, encoded: raw}
		pointers[i] = &backing[i]
		prior = raw
	}
	manifest := &DependencyManifestV1{allocation: allocation, selected: true, loadedEntries: backing, entries: pointers, payload: payload, byteLength: len(payload), digest: ref.Digest}
	// The parser output array ends here; entry strings/arrays have transferred to
	// the immutable backing. Refund only the disposed array, not those fields.
	parsedCharge := retainedalloc.AllocationCharge(uint64(cap(parsed)) * uint64(unsafe.Sizeof(DependencyManifestEntryV1{})))
	clear(parsed)
	parsed = nil
	allocation.charge -= parsedCharge
	owner.RemovePending(parsedCharge)
	done = true
	return manifest, nil
}

func canonicalDecodedManifestEntryV1(e DependencyManifestEntryV1) bool {
	if e.Kind == "" || e.ResourceID == "" || e.Generation == 0 || !e.Identity.valid() || e.Identity.Generation != e.Generation || validateDiagnosticPath(e.DiagnosticPath) != nil || validateDurableFrontier(e.Frontier) != nil || len(e.Reachability) == 0 {
		return false
	}
	for i, f := range e.Reachability {
		if f == "" || (i > 0 && f <= e.Reachability[i-1]) {
			return false
		}
	}
	for i, v := range e.LogicalObligations {
		found := false
		for _, f := range e.Reachability {
			if f == v.Reachability {
				found = true
				break
			}
		}
		if !found || validateStableLogicalObligation(v, v.Reachability) != nil || (i > 0 && (!stableLogicalObligationLess(e.LogicalObligations[i-1], v) || stableLogicalObligationKey(e.LogicalObligations[i-1]) == stableLogicalObligationKey(v))) {
			return false
		}
	}
	if n := e.Namespace; n != nil {
		if !n.ParentIdentity.valid() || n.ParentIdentity.Generation == 0 || (n.Operation != NamespaceCreate && n.Operation != NamespaceRename) || !stableChildBaseName(n.NewName) || (n.Operation == NamespaceRename && !stableChildBaseName(n.OldName)) || validateDiagnosticPath(n.DiagnosticPath) != nil {
			return false
		}
	}
	return true
}

// MetadataOwnerV1 is the constructor-owned metadata authority. It is not a
// borrowed entry lifetime: resolver output must admit and copy its own fields.
func (manifest *DependencyManifestV1) MetadataOwnerV1() *retainedalloc.Owner {
	if manifest == nil {
		return nil
	}
	manifest.metadataMu.Lock()
	defer manifest.metadataMu.Unlock()
	if manifest.released || manifest.allocation == nil {
		return nil
	}
	return manifest.allocation.owner
}

// RebindPhysicalIdentitiesV1 edits only a exclusively-owned loaded manifest in
// an unpublished snapshot copy. The callback borrows read-only fields and
// returns identities; it cannot introduce unadmitted string/array storage.
// Both replacement encoding spans overlap the original and are admitted in
// full before allocation. Physical and canonical validation remain mandatory.
func (manifest *DependencyManifestV1) RebindPhysicalIdentitiesV1(visit func(DependencyManifestEntryV1) (StableIdentity, StableIdentity, error)) error {
	if manifest == nil || visit == nil {
		return ErrResourceOwnership
	}
	manifest.metadataMu.Lock()
	defer manifest.metadataMu.Unlock()
	a := manifest.allocation
	if manifest.released || a == nil || !manifest.selected || a.refs.Load() != 1 || len(manifest.loadedEntries) != len(manifest.entries) {
		return ErrResourceOwnership
	}
	length := len(manifest.payload)
	one := retainedalloc.AllocationCharge(uint64(length))
	if one > math.MaxUint64/2 {
		return retainedalloc.ErrCapacity
	}
	if err := growResourceAllocationV1(a, 2*one); err != nil {
		return err
	}
	scratch := make([]byte, 0, length)
	output := make([]byte, 16, length)
	installed := false
	defer func() {
		clear(scratch)
		scratch = nil
		if !installed {
			clear(output)
			output = nil
			// Failed construction has no remaining raw encoding aliases.
			for _, value := range manifest.entries {
				value.encoded = nil
			}
			a.charge -= 2 * one
			a.owner.RemovePending(2 * one)
		} else {
			a.charge -= one
			a.owner.RemovePending(one)
		}
	}()
	for _, encoded := range manifest.entries {
		value := encoded.entry
		identity, parent, err := visit(value)
		if err != nil {
			return err
		}
		if identity.Platform != value.Identity.Platform || identity.Generation != value.Generation || !identity.valid() {
			return ErrDependencyManifestFormat
		}
		identity.Platform = value.Identity.Platform
		encoded.entry.Identity = identity
		if value.Namespace != nil {
			if parent.Platform != value.Namespace.ParentIdentity.Platform || !parent.valid() || parent.Generation == 0 {
				return ErrDependencyManifestFormat
			}
			parent.Platform = value.Namespace.ParentIdentity.Platform
			encoded.entry.Namespace.ParentIdentity = parent
		}
		if !canonicalDecodedManifestEntryV1(encoded.entry) {
			return ErrDependencyManifestFormat
		}
		var rids []uint64
		if encoded.entry.Frontier.exactRIDs != nil {
			rids = encoded.entry.Frontier.exactRIDs.values
		}
		if dependencyManifestEntrySizeV1(encoded.entry, rids) != len(encoded.encoded) {
			return ErrDependencyManifestFormat
		}
	}
	for _, value := range manifest.entries {
		begin := len(scratch)
		var rids []uint64
		if value.entry.Frontier.exactRIDs != nil {
			rids = value.entry.Frontier.exactRIDs.values
		}
		scratch = appendDependencyManifestEntryV1(scratch, value.entry, rids)
		if len(scratch) > length {
			return ErrDependencyManifestFormat
		}
		value.encoded = scratch[begin:]
	}
	slices.SortFunc(manifest.entries, func(a, b *dependencyManifestEncodedEntryV1) int { return bytes.Compare(a.encoded, b.encoded) })
	copy(output[:8], dependencyManifestBodyMagicV1[:])
	binary.LittleEndian.PutUint16(output[8:10], 1)
	binary.LittleEndian.PutUint16(output[10:12], 16)
	binary.LittleEndian.PutUint32(output[12:16], uint32(len(manifest.entries)))
	for i, value := range manifest.entries {
		if i > 0 && bytes.Equal(manifest.entries[i-1].encoded, value.encoded) {
			return ErrDependencyManifestFormat
		}
		output = appendU32V1(output, uint32(len(value.encoded)))
		output = append(output, value.encoded...)
	}
	if len(output) != length {
		return ErrDependencyManifestFormat
	}
	offset := 16
	for _, value := range manifest.entries {
		size := int(binary.LittleEndian.Uint32(output[offset : offset+4]))
		offset += 4
		value.encoded = output[offset : offset+size]
		offset += size
	}
	old := manifest.payload
	manifest.payload = output
	manifest.digest = sha256.Sum256(output)
	oldCharge := retainedalloc.AllocationCharge(uint64(cap(old)))
	clear(old)
	old = nil
	a.charge -= oldCharge
	a.owner.RemovePending(oldCharge)
	installed = true
	return nil
}
