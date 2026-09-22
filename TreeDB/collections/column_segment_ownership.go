package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"math"

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
	if err := validateColumnAssetRefForPlan(record.Ref); err != nil || columnAssetSegmentFileIDIsDirectView(record.Ref.FileID) || record.Frontier == 0 ||
		record.Ref.Offset < 0 || record.Ref.Length <= 0 || record.Ref.Offset > math.MaxInt64-record.Ref.Length || uint64(record.Ref.Offset+record.Ref.Length) != record.Frontier {
		return nil, errors.New("collections: invalid column segment ownership record")
	}
	if _, err := record.selector(); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestSegmentOwnershipMagic)
	writeManifestUint16(&b, 1)
	writeManifestString(&b, string(record.Ref.Kind))
	writeManifestString(&b, record.Ref.Namespace)
	writeManifestUint64(&b, record.Ref.Generation)
	writeManifestUint64(&b, record.Ref.PartID)
	writeManifestUint32(&b, record.Ref.FileID)
	writeManifestUint64(&b, uint64(record.Ref.Offset))
	writeManifestUint64(&b, uint64(record.Ref.Length))
	writeManifestUint32(&b, record.Ref.Checksum)
	writeManifestUint64(&b, record.Frontier)
	graph := uint16(0)
	if record.Graph {
		graph = 1
	}
	writeManifestUint16(&b, graph)
	return b.Bytes(), nil
}

func decodeColumnManifestSegmentOwnership(key, value []byte) (columnManifestSegmentOwnership, error) {
	if len(key) != len(columnManifestSegmentOwnershipRecordPrefix)+4 || !bytes.HasPrefix(key, columnManifestSegmentOwnershipRecordPrefixBytes) {
		return columnManifestSegmentOwnership{}, errors.New("collections: invalid column segment ownership key")
	}
	cur := manifestCursor{raw: value}
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
	eligible := make(map[uint32]struct{})
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestSegmentOwnershipRecordPrefixBytes) {
			continue
		}
		marker, err := decodeColumnManifestSegmentOwnershipForScan(record.key, record.value, namespace, generation)
		if err != nil {
			return nil, err
		}
		if marker.Ref.Namespace != namespace {
			return nil, errors.New("collections: column segment ownership namespace mismatch")
		}
		eligible[marker.Ref.FileID] = struct{}{}
	}
	for fileID := range producerOwned {
		if fileID == 0 || columnAssetSegmentFileIDIsDirectView(fileID) {
			return nil, errors.New("collections: invalid producer-owned column segment")
		}
		eligible[fileID] = struct{}{}
	}
	if len(eligible) == 0 {
		return records, nil
	}
	filtered := records[:0]
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestSegmentOwnershipRecordPrefixBytes) {
			filtered = append(filtered, record)
		}
	}
	frontiers := make(map[uint32]columnManifestSegmentOwnership, len(eligible))
	err := visitColumnManifestPhysicalRefsForOwnership(filtered, generation, namespace, func(ref ColumnAssetRef, graph bool) error {
		if _, ok := eligible[ref.FileID]; !ok {
			return nil
		}
		if ref.Offset < 0 || ref.Length <= 0 || ref.Offset > math.MaxInt64-ref.Length {
			return errors.New("collections: invalid ownership logical frontier")
		}
		end := uint64(ref.Offset + ref.Length)
		if end > frontiers[ref.FileID].Frontier {
			frontiers[ref.FileID] = columnManifestSegmentOwnership{Ref: ref, Frontier: end, Graph: graph}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for fileID, marker := range frontiers {
		value, err := encodeColumnManifestSegmentOwnership(marker)
		if err != nil {
			return nil, err
		}
		filtered = append(filtered, columnManifestRecord{key: columnManifestSegmentOwnershipRecordKey(fileID), value: value})
	}
	return filtered, nil
}

func visitColumnManifestPhysicalRefsForOwnership(records []columnManifestRecord, generation uint64, namespace string, visit func(ColumnAssetRef, bool) error) error {
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
			part, err := decodeColumnManifestPartRecord(record.value)
			if err != nil {
				return err
			}
			if err := appendRef(part.AssetRef, false); err != nil {
				return err
			}
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			asset, err := decodeColumnManifestAggregateMetadataRecord(record.key, record.value)
			if err != nil {
				return err
			}
			if err := appendRef(asset.AssetRef, false); err != nil {
				return err
			}
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			asset, err := decodeColumnManifestDictionaryCodesRecord(record.key, record.value)
			if err != nil {
				return err
			}
			if err := appendRef(asset.AssetRef, false); err != nil {
				return err
			}
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			asset, err := decodeColumnManifestInt64ValuesRecord(record.key, record.value)
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
	marker, err := decodeColumnManifestSegmentOwnership(key, value)
	if err != nil {
		return columnManifestSegmentOwnership{}, err
	}
	if (namespace != "" && marker.Ref.Namespace != namespace) || marker.Ref.Generation > generation {
		return columnManifestSegmentOwnership{}, errors.New("collections: segment ownership outside manifest namespace/generation")
	}
	if _, err := marker.selector(); err != nil {
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
	input.ownedSegments = make(map[uint32]rootpublication.StableResourceDescriptor, len(markers))
	for id, marker := range markers {
		selector, err := marker.selector()
		if err != nil {
			return nil, err
		}
		resources, err := roots.CloneStableResourceForRoot(root, selector)
		if err != nil {
			return nil, err
		}
		descriptors := resources.Descriptors()
		resources.Release() // The captured root retains the physical pin through planning.
		if len(descriptors) != 1 || descriptors[0].Frontier().Bytes < marker.Frontier {
			return nil, errors.New("collections: segment ownership resource frontier mismatch")
		}
		input.ownedSegments[id] = descriptors[0]
	}
	success = true
	return roots.Release, nil
}
