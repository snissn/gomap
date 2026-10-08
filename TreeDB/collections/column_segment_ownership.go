package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const (
	columnManifestSegmentOwnershipRecordPrefix = "\x06column-manifest/v1/segment-ownership/"
	columnManifestSegmentOwnershipMagic        = uint32(0x54434d4f) // TCMO
)

var columnManifestSegmentOwnershipRecordPrefixBytes = []byte(columnManifestSegmentOwnershipRecordPrefix)

type columnManifestSegmentOwnership struct {
	Ref      ColumnAssetRef
	Frontier uint64
	Graph    bool // The witness belongs to a graph/state record.
}

func columnManifestSegmentOwnershipRecordKey(fileID uint32) []byte {
	key := make([]byte, len(columnManifestSegmentOwnershipRecordPrefix)+4)
	copy(key, columnManifestSegmentOwnershipRecordPrefix)
	binary.BigEndian.PutUint32(key[len(columnManifestSegmentOwnershipRecordPrefix):], fileID)
	return key
}

func encodeColumnManifestSegmentOwnership(record columnManifestSegmentOwnership) ([]byte, error) {
	return encodeColumnManifestSegmentOwnershipWithMetadataAccount(record, nil)
}
func encodeColumnManifestSegmentOwnershipWithMetadataAccount(record columnManifestSegmentOwnership, account rootpublication.StableMetadataAccount) ([]byte, error) {
	if err := validateColumnAssetRefForPlan(record.Ref); err != nil || columnAssetSegmentFileIDIsDirectView(record.Ref.FileID) || record.Frontier == 0 ||
		record.Ref.Offset < 0 || record.Ref.Length <= 0 || record.Ref.Offset > math.MaxInt64-record.Ref.Length || uint64(record.Ref.Offset+record.Ref.Length) != record.Frontier {
		return nil, errors.New("collections: invalid column segment ownership record")
	}
	if err := validateColumnManifestSegmentOwnershipClass(record); err != nil {
		return nil, err
	}
	count := columnManifestEncodingSink{}
	writeColumnManifestSegmentOwnership(&count, record)
	raw, err := count.allocate(account)
	if err != nil {
		return nil, err
	}
	sink := columnManifestEncodingSink{raw: raw}
	writeColumnManifestSegmentOwnership(&sink, record)
	return sink.result()
}
func writeColumnManifestSegmentOwnership(s *columnManifestEncodingSink, record columnManifestSegmentOwnership) {
	s.u32(columnManifestSegmentOwnershipMagic)
	s.u16(1)
	s.text(string(record.Ref.Kind))
	s.text(record.Ref.Namespace)
	s.u64(record.Ref.Generation)
	s.u64(record.Ref.PartID)
	s.u32(record.Ref.FileID)
	s.u64(uint64(record.Ref.Offset))
	s.u64(uint64(record.Ref.Length))
	s.u32(record.Ref.Checksum)
	s.u64(record.Frontier)
	graph := uint16(0)
	if record.Graph {
		graph = 1
	}
	s.u16(graph)
}
func validateColumnManifestSegmentOwnershipClass(marker columnManifestSegmentOwnership) error {
	_, _, classification, err := stableColumnAssetResourceClassification(marker.Ref.Kind)
	if err != nil {
		return err
	}
	if classification != "authoritative" {
		return rootpublication.ErrResourceExcluded
	}
	return nil
}

func decodeColumnManifestSegmentOwnership(key, value []byte) (columnManifestSegmentOwnership, error) {
	return decodeColumnManifestSegmentOwnershipWithMetadataAccount(key, value, nil)
}
func decodeColumnManifestSegmentOwnershipWithMetadataAccount(key, value []byte, account rootpublication.StableMetadataAccount) (columnManifestSegmentOwnership, error) {
	if len(key) != len(columnManifestSegmentOwnershipRecordPrefix)+4 || !bytes.HasPrefix(key, columnManifestSegmentOwnershipRecordPrefixBytes) {
		return columnManifestSegmentOwnership{}, errors.New("collections: invalid column segment ownership key")
	}
	cur := manifestCursor{raw: value, account: account}
	if magic := cur.u32(); magic != columnManifestSegmentOwnershipMagic {
		return columnManifestSegmentOwnership{}, errors.New("collections: invalid column segment ownership magic")
	}
	if version := cur.u16(); version != 1 {
		return columnManifestSegmentOwnership{}, errors.New("collections: invalid column segment ownership version")
	}
	kind := ColumnAssetKind(cur.string())
	namespace := cur.string()
	generation := cur.u64()
	partID := cur.u64()
	fileID := cur.u32()
	offset := cur.u64()
	length := cur.u64()
	checksum := cur.u32()
	frontier := cur.u64()
	graph := cur.u16()
	if cur.err != nil {
		return columnManifestSegmentOwnership{}, cur.err
	}
	if cur.pos != len(value) {
		return columnManifestSegmentOwnership{}, errors.New("collections: trailing bytes in column segment ownership record")
	}
	if offset > uint64(^uint64(0)>>1) || length > uint64(^uint64(0)>>1) {
		return columnManifestSegmentOwnership{}, errors.New("collections: column segment ownership bounds overflow")
	}
	ref := ColumnAssetRef{Kind: kind, Namespace: namespace, Generation: generation, PartID: partID, FileID: fileID, Offset: int64(offset), Length: int64(length), Checksum: checksum}
	if err := validateColumnAssetRefForPlan(ref); err != nil || graph > 1 || columnAssetSegmentFileIDIsDirectView(fileID) || frontier == 0 || ref.Offset > math.MaxInt64-ref.Length || uint64(ref.Offset+ref.Length) != frontier || binary.BigEndian.Uint32(key[len(columnManifestSegmentOwnershipRecordPrefix):]) != fileID {
		return columnManifestSegmentOwnership{}, fmt.Errorf("collections: invalid column segment ownership record file=%d", fileID)
	}
	return columnManifestSegmentOwnership{Ref: ref, Frontier: frontier, Graph: graph == 1}, nil
}

func normalizeColumnManifestSegmentOwnership(records []columnManifestRecord, producerOwned map[uint32]struct{}, generation uint64, namespace string) ([]columnManifestRecord, error) {
	return normalizeColumnManifestSegmentOwnershipWithMetadataAccount(records, producerOwned, generation, namespace, nil)
}
func columnManifestFileIDLess(a, b uint32) bool { return a < b }
func normalizeColumnManifestSegmentOwnershipWithMetadataAccount(records []columnManifestRecord, producerOwned map[uint32]struct{}, generation uint64, namespace string, account rootpublication.StableMetadataAccount) ([]columnManifestRecord, error) {
	// One exact eligible/frontier table replaces the two Swiss maps. Its nodes
	// are the existing registry AVL allocation family, prepaid before insertion.
	table := rootpublication.NewStableMetadataTable[uint32, columnManifestSegmentOwnership](columnManifestFileIDLess)
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestSegmentOwnershipRecordPrefixBytes) {
			continue
		}
		marker, err := decodeColumnManifestSegmentOwnershipForScanWithMetadataAccount(record.key, record.value, namespace, generation, account)
		if err != nil {
			return nil, err
		}
		if marker.Ref.Namespace != namespace {
			return nil, errors.New("collections: column segment ownership namespace mismatch")
		}
		if err = table.Set(marker.Ref.FileID, columnManifestSegmentOwnership{}, account); err != nil {
			return nil, err
		}
	}
	for fileID := range producerOwned {
		if fileID == 0 || columnAssetSegmentFileIDIsDirectView(fileID) {
			return nil, errors.New("collections: invalid producer-owned column segment")
		}
		if err := table.Set(fileID, columnManifestSegmentOwnership{}, account); err != nil {
			return nil, err
		}
	}
	if table.Len() == 0 {
		return records, nil
	}
	filtered := records[:0]
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestSegmentOwnershipRecordPrefixBytes) {
			filtered = append(filtered, record)
		}
	}
	err := visitColumnManifestPhysicalRefsForOwnershipWithMetadataAccount(filtered, generation, namespace, account, func(ref ColumnAssetRef, graph bool) error {
		old, ok := table.Lookup(ref.FileID)
		if !ok {
			return nil
		}
		if ref.Offset < 0 || ref.Length <= 0 || ref.Offset > math.MaxInt64-ref.Length {
			return errors.New("collections: invalid ownership logical frontier")
		}
		end := uint64(ref.Offset + ref.Length)
		if end > old.Frontier {
			return table.Set(ref.FileID, columnManifestSegmentOwnership{Ref: ref, Frontier: end, Graph: graph}, account)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	count := 0
	table.Visit(func(_ uint32, m columnManifestSegmentOwnership) bool {
		if m.Frontier != 0 {
			count++
		}
		return true
	})
	if count > math.MaxInt-len(filtered) {
		return nil, ErrPreparedInsertResourceLimit
	}
	capacity := len(filtered) + count
	if capacity > cap(filtered) {
		if err := reserveColumnManifestArray(account, capacity, uint64(unsafe.Sizeof(columnManifestRecord{}))); err != nil {
			return nil, err
		}
		replacement := make([]columnManifestRecord, len(filtered), capacity)
		copy(replacement, filtered)
		filtered = replacement
	}
	table.Visit(func(fileID uint32, marker columnManifestSegmentOwnership) bool {
		if marker.Frontier == 0 {
			return true
		}
		if err = reserveColumnManifestBacking(account, uint64(len(columnManifestSegmentOwnershipRecordPrefix)+4), false); err != nil {
			return false
		}
		var value []byte
		value, err = encodeColumnManifestSegmentOwnershipWithMetadataAccount(marker, account)
		if err != nil {
			return false
		}
		filtered = append(filtered, columnManifestRecord{key: columnManifestSegmentOwnershipRecordKey(fileID), value: value})
		return true
	})
	if err != nil {
		return nil, err
	}
	return filtered, nil
}

func visitColumnManifestPhysicalRefsForOwnership(records []columnManifestRecord, generation uint64, namespace string, visit func(ColumnAssetRef, bool) error) error {
	return visitColumnManifestPhysicalRefsForOwnershipWithMetadataAccount(records, generation, namespace, nil, visit)
}
func visitColumnManifestPhysicalRefsForOwnershipWithMetadataAccount(records []columnManifestRecord, generation uint64, namespace string, account rootpublication.StableMetadataAccount, visit func(ColumnAssetRef, bool) error) error {
	if account != nil {
		for _, r := range records {
			if bytes.HasPrefix(r.key, columnManifestVectorGraphRecordPrefixBytes) || bytes.HasPrefix(r.key, columnVectorIndexStateRecordPrefixBytes) {
				return ErrPreparedInsertResourceLimit
			}
		}
	}
	appendRef := func(ref ColumnAssetRef, graph bool) error {
		if ref.Namespace != namespace || ref.Generation == 0 || ref.Generation > generation {
			return fmt.Errorf("collections: ownership ref namespace/generation %+v outside candidate manifest", ref)
		}
		if err := validateColumnAssetRefForPlan(ref); err != nil {
			return err
		}
		return visit(ref, graph)
	}
	for _, record := range records {
		switch {
		case bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes):
			part, err := decodeColumnManifestPartRecordWithMetadataAccount(record.value, account)
			if err != nil {
				return err
			}
			if err := appendRef(part.AssetRef, false); err != nil {
				return err
			}
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			asset, err := decodeColumnManifestAggregateMetadataRecordWithMetadataAccount(record.key, record.value, account)
			if err != nil {
				return err
			}
			if err := appendRef(asset.AssetRef, false); err != nil {
				return err
			}
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			asset, err := decodeColumnManifestDictionaryCodesRecordWithMetadataAccount(record.key, record.value, account)
			if err != nil {
				return err
			}
			if err := appendRef(asset.AssetRef, false); err != nil {
				return err
			}
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			asset, err := decodeColumnManifestInt64ValuesRecordWithMetadataAccount(record.key, record.value, account)
			if err != nil {
				return err
			}
			if err := appendRef(asset.AssetRef, false); err != nil {
				return err
			}
		case bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes):
			graph, err := decodeColumnVectorGraphManifestRecord(record.value)
			if err != nil {
				return err
			}
			graphRefs, err := columnVectorGraphManifestAssetRefsForScan(graph, generation, namespace)
			if err != nil {
				return err
			}
			for _, ref := range graphRefs {
				if err := appendRef(ref, true); err != nil {
					return err
				}
			}
		case bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes):
			state, err := decodeColumnVectorIndexStateRecord(record.value)
			if err != nil {
				return err
			}
			stateRefs, err := columnVectorIndexStateManifestAssetRefsForScan(state, generation, namespace)
			if err != nil {
				return err
			}
			for _, ref := range stateRefs {
				if err := appendRef(ref, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (marker columnManifestSegmentOwnership) selector() (rootpublication.StableResourceSelector, error) {
	kind, field, classification, err := stableColumnAssetResourceClassification(marker.Ref.Kind)
	if err != nil {
		return rootpublication.StableResourceSelector{}, err
	}
	if classification != "authoritative" {
		return rootpublication.StableResourceSelector{}, rootpublication.ErrResourceExcluded
	}
	if marker.Graph {
		kind, field = rootpublication.ResourceVectorGraphPack, rootpublication.ReachabilityVectorGraphPack
	}
	return rootpublication.StableResourceSelector{Kind: kind, LogicalLane: marker.Ref.Namespace, ResourceID: fmt.Sprint(marker.Ref.FileID), PhysicalGeneration: uint64(marker.Ref.FileID), Obligation: stableColumnLogicalObligation(marker.Ref, field)}, nil
}

func decodeColumnManifestSegmentOwnershipForScan(key, value []byte, namespace string, generation uint64) (columnManifestSegmentOwnership, error) {
	return decodeColumnManifestSegmentOwnershipForScanWithMetadataAccount(key, value, namespace, generation, nil)
}
func decodeColumnManifestSegmentOwnershipForScanWithMetadataAccount(key, value []byte, namespace string, generation uint64, account rootpublication.StableMetadataAccount) (columnManifestSegmentOwnership, error) {
	marker, err := decodeColumnManifestSegmentOwnershipWithMetadataAccount(key, value, account)
	if err != nil {
		return columnManifestSegmentOwnership{}, err
	}
	if (namespace != "" && marker.Ref.Namespace != namespace) || marker.Ref.Generation > generation {
		return columnManifestSegmentOwnership{}, errors.New("collections: segment ownership outside manifest namespace/generation")
	}
	if err := validateColumnManifestSegmentOwnershipClass(marker); err != nil {
		return columnManifestSegmentOwnership{}, err
	}
	return marker, nil
}

// Bind ownership only to this snapshot's exact recovery-selectable root. The
// marker witnesses a real live ref; it never creates a logical obligation.
func (c *Collection) bindColumnSegmentOwnership(ctx context.Context, view columnPhysicalScanSnapshotView, input *columnAssetReachabilityInput) (func(), error) {
	markers := make(map[uint32]columnManifestSegmentOwnership, len(view.SegmentOwnership))
	for _, marker := range view.SegmentOwnership {
		if _, duplicate := markers[marker.Ref.FileID]; duplicate {
			return nil, errors.New("collections: duplicate segment ownership")
		}
		markers[marker.Ref.FileID] = marker
	}
	maxEnd := make(map[uint32]uint64, len(markers))
	witnesses := make(map[uint32]bool, len(markers))
	visit := func(ref ColumnAssetRef, graph bool) {
		marker, ok := markers[ref.FileID]
		if !ok {
			return
		}
		if ref.Offset >= 0 && ref.Length > 0 && ref.Offset <= math.MaxInt64-ref.Length {
			maxEnd[ref.FileID] = max(maxEnd[ref.FileID], uint64(ref.Offset+ref.Length))
		}
		if ref == marker.Ref && graph == marker.Graph {
			witnesses[ref.FileID] = true
		}
	}
	for _, ref := range view.AssetRefs {
		visit(ref.Ref, false)
	}
	for _, ref := range view.TypedColumnPartRefs {
		visit(ref.Ref, false)
	}
	for _, ref := range view.AggregateMetadata {
		visit(ref.AssetRef, false)
	}
	for _, ref := range view.DictionaryCodes {
		visit(ref.AssetRef, false)
	}
	for _, ref := range view.Int64Values {
		visit(ref.AssetRef, false)
	}
	for _, ref := range view.GraphAssetRefs {
		visit(ref, true)
	}
	for id, marker := range markers {
		if !witnesses[id] || maxEnd[id] != marker.Frontier {
			return nil, errors.New("collections: segment ownership has no maximal live witness")
		}
	}
	token, ok := view.snapshot.StateToken()
	if !ok {
		return nil, backenddb.ErrRecoverableRootSetStale
	}
	roots, err := c.db.CaptureRecoverableRootSetForInspection(ctx)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			roots.Release()
		}
	}()
	root := backenddb.RecoverableRoot{CommitSeq: token.CommitSeq, UserRootPageID: token.RootPageID, SystemRootPageID: token.SystemRootPageID, AppliedCommandLSN: token.AppliedCommandLSN, MaxEntryRevision: uint64(token.MaxEntryRevision)}
	input.ownedSegments = make(map[uint32]rootpublication.StableResourcePhysicalDescriptor, len(markers))
	for id, marker := range markers {
		selector, err := marker.selector()
		if err != nil {
			return nil, err
		}
		resources, err := roots.CloneStableResourceForRoot(root, selector)
		if err != nil {
			return nil, err
		}
		descriptors := resources.PhysicalDescriptors()
		resources.Release() // The captured root retains the physical pin through planning.
		if len(descriptors) != 1 || descriptors[0].Frontier().Bytes < marker.Frontier {
			return nil, errors.New("collections: segment ownership resource frontier mismatch")
		}
		input.ownedSegments[id] = descriptors[0]
	}
	success = true
	return roots.Release, nil
}
