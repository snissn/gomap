package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// The original bounded leaf parser permits 2MiB stored/255-page raw frames.
// Walk and Lookup can nest, so each owns a complete independent slot. These
// slots are included in the original materialization receipt before assets.
func nativeSourceRequirementCapacityV2() uint64 {
	return nativeSourceRequirementCapacityForStateV2(columnVectorIndexStateMaxDecodedBytes)
}

func nativeSourceRequirementStateCapacityV2(decoded uint64) uint64 {
	if decoded == 0 {
		return 0
	}
	return memtable.COWAllocationCharge(columnVectorIndexStateMaxInlineRecordBytes) + 3*memtable.COWAllocationCharge(decoded) + memtable.COWAllocationCharge((decoded/120)*uint64(unsafe.Sizeof(columnVectorIndexStateAssetSnapshot{}))) + memtable.COWAllocationCharge((decoded/120)*128)
}

func nativeSourceRequirementCapacityForStateV2(decoded uint64) uint64 {
	leaf := memtable.COWAllocationCharge(2<<20) + memtable.COWAllocationCharge(valuelog.MaxFrameK*page.PageSize)
	sizes, _ := valuelog.COWBlockDecoderRetentionSizes(valuelog.BlockCodecLZ4, valuelog.MaxFrameK*page.PageSize, 2<<20)
	leaf += memtable.COWAllocationCharge(sizes.OwnerWrapper)
	// The same maintained sixteen-copy record envelope, but only ONE bounded
	// inline standard asset record per slot rather than a historical array.
	record := 16 * (memtable.COWAllocationCharge(uint64(unsafe.Sizeof(columnManifestRecord{}))) + 2*memtable.COWAllocationCharge(3000) + preparedSemanticStreamMapEntryReserveBytes)
	// Original requirement normalization/merge keeps finite scopes and source
	// headers, including maps and simultaneous old/new normalized copies.
	scopes := uint64(len(stableColumnDurableRequirementFields)) * (memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.StableLogicalObligationNamespaceScope{}))) + preparedSemanticStreamMapEntryReserveBytes)
	headers := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.StableLogicalObligationRequirements{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof((*nativeSourceRequirementStreamV2)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof([2]uintptr{})))
	// TVIS has a bounded compressed inline image and at most decoded/120
	// asset headers; names/IDs and validation keys copy bytes from that image.
	// Duplicate-map buckets and growth have a separate 128-byte allowance per
	// decoded asset; concatenated validation keys are in the image copies.
	state := nativeSourceRequirementStateCapacityV2(decoded)
	return memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeSourceRequirementStreamV2{}))) + 2*(leaf+record+state) + 16*(scopes+headers)
}

type nativeSourceRequirementSlotV2 struct {
	scratch nativeDomainReadScratch
	pivot   [page.PageSize + 1]byte
}

type nativeSourceRequirementStreamV2 struct {
	snapshot    *backenddb.Snapshot // borrowed through original group finalization
	reader      backenddb.SnapshotRootReader
	hasBase     bool
	generation  uint64
	namespace   string
	indexes     []VectorIndexDefinition // borrowed immutable captured catalog
	suffix      []columnManifestRecord  // bounded fresh original records, newest wins
	walk, point nativeSourceRequirementSlotV2
}

func (s *nativeSourceRequirementStreamV2) readLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return readNativeSnapshotLeafWithAdmission(s.snapshot, 2<<20, func(n uint64) (func(), error) {
		// The containing original receipt already owns both independent maximum
		// input/raw/codec slots. Inspection precedes allocation, never new admission.
		sizes, _ := valuelog.COWBlockDecoderRetentionSizes(valuelog.BlockCodecLZ4, valuelog.MaxFrameK*page.PageSize, 2<<20)
		maximum := memtable.COWAllocationCharge(2<<20) + memtable.COWAllocationCharge(valuelog.MaxFrameK*page.PageSize) + memtable.COWAllocationCharge(sizes.OwnerWrapper)
		if n > maximum {
			return nil, memtable.ErrCOWCapacity
		}
		return func() {}, nil
	}, ptr, dst)
}

func (s *nativeSourceRequirementStreamV2) decode(record columnManifestRecord, work *rootpublication.StableResourceClosureWork, emit func(rootpublication.StableLogicalObligation) error) error {
	limit := 3000
	if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) || bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) {
		known := false
		for _, def := range s.indexes {
			if bytes.Equal(record.key, columnVectorGraphManifestRecordKey(def.Name)) || bytes.Equal(record.key, columnVectorIndexStateRecordKey(def.Name)) {
				known = true
				break
			}
		}
		if !known {
			return backenddb.ErrNativeSemanticUnsupported
		}
		limit = columnVectorIndexStateMaxInlineRecordBytes
	}
	if len(record.key) > page.PageSize || len(record.value) > limit {
		return errors.New("collections: source requirement record exceeds inline bound")
	}
	return walkStableColumnManifestRequirementRecord(record, s.generation, s.namespace, work, emit)
}

func (s *nativeSourceRequirementStreamV2) latest(key []byte) (columnManifestRecord, bool) {
	for i := len(s.suffix) - 1; i >= 0; i-- {
		if bytes.Equal(s.suffix[i].key, key) {
			return s.suffix[i], true
		}
	}
	return columnManifestRecord{}, false
}

func (s *nativeSourceRequirementStreamV2) scan(slot *nativeSourceRequirementSlotV2, start, end []byte, visit func(columnManifestRecord) error) error {
	if !s.hasBase {
		return nil
	}
	if len(start) > page.PageSize {
		return memtable.ErrCOWCapacity
	}
	n := copy(slot.pivot[:], start)
	for {
		entry, ok, err := s.reader.SeekGEEntryWithFixedScratch(slot.pivot[:n], end, slot.scratch.key[:], slot.scratch.leaf[:], &slot.scratch.successor, s.readLeaf)
		if err != nil || !ok {
			return err
		}
		if len(entry.Key) > page.PageSize {
			return memtable.ErrCOWCapacity
		}
		n = copy(slot.pivot[:], entry.Key)
		slot.pivot[n] = 0
		n++
		if entry.Flags&node.FlagTombstone != 0 {
			continue
		}
		if entry.Flags&node.FlagPointer != 0 {
			return errors.New("collections: source requirement record is not inline")
		}
		if !columnManifestRecordKeyKnownForScan(entry.Key) {
			return errors.New("collections: unknown source requirement record")
		}
		if _, replaced := s.latest(entry.Key); replaced {
			continue
		}
		if err = visit(columnManifestRecord{key: entry.Key, value: entry.Value}); err != nil {
			return err
		}
	}
}

func (s *nativeSourceRequirementStreamV2) Walk(emit func(rootpublication.StableLogicalObligation) error) (work rootpublication.StableResourceClosureWork, err error) {
	if emit == nil {
		return work, backenddb.ErrNativeSemanticUnsupported
	}
	visit := func(record columnManifestRecord) error { return s.decode(record, &work, emit) }
	if err = s.scan(&s.walk, columnManifestPartRecordPrefixBytes, []byte(columnSourceDirectoryNodePrefixV2), visit); err != nil {
		return
	}
	for i, r := range s.suffix {
		latest := true
		for j := i + 1; j < len(s.suffix); j++ {
			if bytes.Equal(r.key, s.suffix[j].key) {
				latest = false
				break
			}
		}
		if latest {
			if err = visit(r); err != nil {
				return
			}
		}
	}
	return
}

func sameSourceRequirementIdentityV2(a, b rootpublication.StableLogicalObligation) bool {
	a.Checksum, b.Checksum = 0, 0
	a.Digest, b.Digest = [32]byte{}, [32]byte{}
	return a == b
}
func (s *nativeSourceRequirementStreamV2) Lookup(query rootpublication.StableLogicalObligation) (value rootpublication.StableLogicalObligation, found bool, err error) {
	var prefix []byte
	switch ColumnAssetKind(query.Kind) {
	case ColumnAssetKindTCS1PartImage, ColumnAssetKindTCS1TypedColumnPart, ColumnAssetKindTCS1HNSWSearchPack:
		prefix = columnManifestPartRecordPrefixBytes
	case ColumnAssetKindTCS1AggregateMetadata:
		prefix = columnManifestAggregateMetadataRecordPrefixBytes
	case ColumnAssetKindTCS1DictionaryCodes:
		prefix = columnManifestDictionaryCodesRecordPrefixBytes
	case ColumnAssetKindTCS1Int64Values:
		prefix = columnManifestInt64ValuesRecordPrefixBytes
	default:
		// Graph/state refs are addressed by captured declared index keys below.
	}
	var key [page.PageSize]byte
	n := copy(key[:], prefix)
	binary.BigEndian.PutUint64(key[n:], query.Generation)
	binary.BigEndian.PutUint64(key[n+8:], query.PartID)
	n += 16
	start := key[:n]
	var end [page.PageSize]byte
	copy(end[:], start)
	m := n
	for m > 0 && end[m-1] == 255 {
		m--
	}
	if m == 0 {
		return value, false, backenddb.ErrNativeSemanticUnsupported
	}
	end[m-1]++
	visit := func(record columnManifestRecord) error {
		if prefix == nil || !bytes.HasPrefix(record.key, start) {
			return nil
		}
		return s.decode(record, nil, func(candidate rootpublication.StableLogicalObligation) error {
			if !sameSourceRequirementIdentityV2(candidate, query) {
				return nil
			}
			if found && value != candidate {
				return rootpublication.ErrResourceConflict
			}
			value, found = query, true
			value.Checksum, value.Digest = candidate.Checksum, candidate.Digest
			return nil
		})
	}
	if prefix != nil {
		if err = s.scan(&s.point, start, end[:m], visit); err != nil {
			return
		}
		for i, r := range s.suffix {
			latest := true
			for j := i + 1; j < len(s.suffix); j++ {
				if bytes.Equal(r.key, s.suffix[j].key) {
					latest = false
					break
				}
			}
			if latest {
				if err = visit(r); err != nil {
					return
				}
			}
		}
	}
	graphVisit := func(record columnManifestRecord) error {
		return s.decode(record, nil, func(candidate rootpublication.StableLogicalObligation) error {
			if !sameSourceRequirementIdentityV2(candidate, query) {
				return nil
			}
			if found && (value.Checksum != candidate.Checksum || value.Digest != candidate.Digest) {
				return rootpublication.ErrResourceConflict
			}
			value, found = query, true
			value.Checksum, value.Digest = candidate.Checksum, candidate.Digest
			return nil
		})
	}
	for _, def := range s.indexes {
		for _, key := range [][]byte{columnVectorGraphManifestRecordKey(def.Name), columnVectorIndexStateRecordKey(def.Name)} {
			if r, ok := s.latest(key); ok {
				if err = graphVisit(r); err != nil {
					return
				}
				continue
			}
			if !s.hasBase {
				continue
			}
			entry, e := s.reader.GetEntryExactWithFixedScratch(key, s.point.scratch.key[:], s.point.scratch.leaf[:], s.readLeaf)
			if errors.Is(e, tree.ErrKeyNotFound) {
				continue
			}
			if e != nil {
				err = e
				return
			}
			if entry.Flags&node.FlagTombstone != 0 {
				continue
			}
			if entry.Flags&node.FlagPointer != 0 {
				err = backenddb.ErrNativeSemanticUnsupported
				return
			}
			if err = graphVisit(columnManifestRecord{key: entry.Key, value: entry.Value}); err != nil {
				return
			}
		}
	}

	return
}

// Source appends do not create TVIS records. Only the largest captured state
// can be decoded in each independent Walk/Lookup slot. Inspect its original
// codec envelope before admission, without decoding it or copying its assets.
// A graph producer separately admits BOTH maximum replacement-state slots.
func nativeSourceRequirementBasisCapacityV2(snapshot *backenddb.Snapshot, root uint64) (uint64, error) {
	if root == 0 {
		return nativeSourceRequirementCapacityForStateV2(0), nil
	}
	if snapshot == nil {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	it, err := snapshot.IteratorAtRoot(root, columnVectorIndexStateRecordPrefixBytes, prefixEnd(columnVectorIndexStateRecordPrefixBytes))
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var decoded uint64
	for it.Valid() {
		if !it.IsDeleted() {
			raw, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 || len(raw) > columnVectorIndexStateMaxInlineRecordBytes || len(it.UnsafeKey()) > page.PageSize {
				return 0, backenddb.ErrNativeSemanticUnsupported
			}
			n, _, err := columnVectorIndexStateRecordDecodedLength(raw)
			if err != nil {
				return 0, err
			}
			decoded = max(decoded, uint64(n))
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	return nativeSourceRequirementCapacityForStateV2(decoded), nil
}

func nativeTypedBasisCapacity(meta CollectionMeta, snapshot *backenddb.Snapshot, root uint64) (uint64, error) {
	if nativeSourceImportMetaV2(meta) {
		return nativeSourceRequirementBasisCapacityV2(snapshot, root)
	}
	return nativeTypedManifestCapacity(snapshot, root)
}

// nativeSourceManifestRequirementsV2 borrows only the exact captured physical
// root and the already-admitted fresh delta suffix. Every namespace declaration
// describes the final manifest, never a union with an obsolete complete list.
func nativeSourceManifestRequirementsV2(snapshot *backenddb.Snapshot, root, generation uint64, namespace string, suffix []columnManifestRecord, indexes ...VectorIndexDefinition) (rootpublication.StableLogicalObligationRequirements, error) {
	stream := &nativeSourceRequirementStreamV2{snapshot: snapshot, generation: generation, namespace: namespace, suffix: suffix, indexes: indexes}
	if root != 0 {
		var err error
		stream.reader, err = snapshot.NativeReaderAtRoot(root)
		if err != nil {
			return rootpublication.StableLogicalObligationRequirements{}, err
		}
		stream.hasBase = true
	}
	requirements := rootpublication.StableLogicalObligationRequirements{Streams: []rootpublication.StableLogicalObligationRequirementStream{stream}}
	for _, field := range stableColumnDurableRequirementFields {
		requirements.ScopedNamespaces = append(requirements.ScopedNamespaces, rootpublication.StableLogicalObligationNamespaceScope{Field: field, Namespace: namespace})
	}
	return requirements, nil
}
