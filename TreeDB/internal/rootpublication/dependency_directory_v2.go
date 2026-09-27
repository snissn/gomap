package rootpublication

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// DependencyDirectoryRefV2 identifies a third COW B-tree in the durable root's
// index generation. Counts are checked by a streaming recovery walk. Page
// integrity and the pinned root are local authority, not a Merkle commitment.
type DependencyDirectoryRefV2 struct {
	RootPageID    uint64
	PhysicalCount uint64
	LogicalCount  uint64
}

const (
	dependencyPhysicalKeyV2 = byte(1)
	dependencyLogicalKeyV2  = byte(2)
)

// DependencyPhysicalKeyV2 excludes host file identity so snapshot rebind can
// replace physical identity values without renaming logical directory records.
func DependencyPhysicalKeyV2(entry DependencyManifestEntryV1) []byte {
	key := make([]byte, 1, 1+3*4+8+len(entry.Kind)+len(entry.LogicalLane)+len(entry.ResourceID))
	key[0] = dependencyPhysicalKeyV2
	key = appendStringV1(key, string(entry.Kind))
	key = appendStringV1(key, entry.LogicalLane)
	key = appendStringV1(key, entry.ResourceID)
	return appendU64V1(key, entry.Generation)
}

// DependencyLogicalKeyV2 encodes the existing exact logical identity. Neither
// the physical owner nor checksums are part of this key: changing either must
// conflict with the existing record rather than create a second identity.
func DependencyLogicalKeyV2(obligation StableLogicalObligation) []byte {
	identity := stableLogicalObligationKey(obligation)
	key := make([]byte, 1, 1+4*4+5*8+len(identity.class)+len(identity.kind)+len(identity.namespace)+len(identity.reachability))
	key[0] = dependencyLogicalKeyV2
	key = appendStringV1(key, identity.class)
	key = appendStringV1(key, identity.kind)
	key = appendStringV1(key, identity.namespace)
	key = appendU64V1(key, identity.generation)
	key = appendU64V1(key, identity.partID)
	key = appendU64V1(key, identity.fileID)
	key = appendU64V1(key, uint64(identity.offset))
	key = appendU64V1(key, uint64(identity.length))
	return appendStringV1(key, string(identity.reachability))
}

// EncodeDependencyPhysicalV2 uses the established canonical descriptor codec,
// with logical obligations stored independently. Callers must explicitly split
// them; silently dropping obligations at this boundary would lose authority.
func EncodeDependencyPhysicalV2(entry DependencyManifestEntryV1) ([]byte, error) {
	if len(entry.LogicalObligations) != 0 {
		return nil, fmt.Errorf("%w: physical directory record contains logical obligations", ErrDependencyManifestFormat)
	}
	normalized, err := normalizeDependencyManifestEntryV1(entry)
	if err != nil {
		return nil, err
	}
	return encodeDependencyManifestEntryV1(normalized), nil
}

func DecodeDependencyPhysicalV2(key, value []byte) (DependencyManifestEntryV1, error) {
	reader := manifestReaderV1{data: value}
	entry, ok := reader.entry()
	if !ok || reader.remaining() != 0 || !bytes.Equal(key, DependencyPhysicalKeyV2(entry)) {
		return DependencyManifestEntryV1{}, ErrDependencyManifestFormat
	}
	canonical, err := EncodeDependencyPhysicalV2(entry)
	if err != nil || !bytes.Equal(canonical, value) {
		return DependencyManifestEntryV1{}, ErrDependencyManifestFormat
	}
	return entry, nil
}

// EncodeDependencyLogicalV2 binds exact integrity metadata to its independently
// admitted physical owner. The key reconstructs every other obligation field.
func EncodeDependencyLogicalV2(owner []byte, obligation StableLogicalObligation) ([]byte, error) {
	if !validDependencyPhysicalKeyV2(owner) || uint64(len(owner)) > uint64(^uint32(0)) {
		return nil, ErrDependencyManifestFormat
	}
	if err := validateStableLogicalObligation(obligation, obligation.Reachability); err != nil {
		return nil, err
	}
	value := make([]byte, 0, 4+len(owner)+4+32)
	value = appendU32V1(value, uint32(len(owner)))
	value = append(value, owner...)
	value = appendU32V1(value, obligation.Checksum)
	return append(value, obligation.Digest[:]...), nil
}

func validDependencyPhysicalKeyV2(key []byte) bool {
	if len(key) == 0 || key[0] != dependencyPhysicalKeyV2 {
		return false
	}
	reader := manifestReaderV1{data: key, offset: 1}
	for i := 0; i < 3; i++ {
		value, ok := reader.str()
		if !ok || (i != 1 && value == "") {
			return false
		}
	}
	generation, ok := reader.u64()
	return ok && generation != 0 && reader.remaining() == 0
}

func DecodeDependencyLogicalV2(key, value []byte) ([]byte, StableLogicalObligation, error) {
	var obligation StableLogicalObligation
	bad := func() ([]byte, StableLogicalObligation, error) {
		return nil, StableLogicalObligation{}, ErrDependencyManifestFormat
	}
	if len(key) == 0 || key[0] != dependencyLogicalKeyV2 || len(value) < 4+1+4+32 {
		return bad()
	}
	ownerBytes := uint64(binary.LittleEndian.Uint32(value[:4]))
	if ownerBytes == 0 || ownerBytes != uint64(len(value)-40) || value[4] != dependencyPhysicalKeyV2 {
		return bad()
	}
	reader := manifestReaderV1{data: key, offset: 1}
	var ok bool
	if obligation.Class, ok = reader.str(); !ok {
		return bad()
	}
	if obligation.Kind, ok = reader.str(); !ok {
		return bad()
	}
	if obligation.Namespace, ok = reader.str(); !ok {
		return bad()
	}
	if obligation.Generation, ok = reader.u64(); !ok {
		return bad()
	}
	if obligation.PartID, ok = reader.u64(); !ok {
		return bad()
	}
	if obligation.FileID, ok = reader.u64(); !ok {
		return bad()
	}
	offset, ok := reader.u64()
	if !ok || offset > uint64(^uint64(0)>>1) {
		return bad()
	}
	length, ok := reader.u64()
	if !ok || length > uint64(^uint64(0)>>1) {
		return bad()
	}
	field, ok := reader.str()
	if !ok || reader.remaining() != 0 {
		return bad()
	}
	obligation.Offset, obligation.Length = int64(offset), int64(length)
	obligation.Reachability = ReachabilityField(field)
	owner := value[4 : 4+int(ownerBytes)]
	obligation.Checksum = binary.LittleEndian.Uint32(value[4+int(ownerBytes):])
	copy(obligation.Digest[:], value[8+int(ownerBytes):])
	canonical, err := EncodeDependencyLogicalV2(owner, obligation)
	if err != nil || !bytes.Equal(key, DependencyLogicalKeyV2(obligation)) || !bytes.Equal(value, canonical) {
		return bad()
	}
	return owner, obligation, nil
}
