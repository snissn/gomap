package collections

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
)

// Reuse the parser's validated records, not a second manifest read. The lease
// includes every typed field: document materialization may start after GC has
// retired this snapshot's roots, long after graph preparation finished.
func (c *Collection) acquireTypedGraphOwnerPin(records []columnManifestRecord, generation uint64, namespace string, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*ColumnAssetLifecyclePinSet, error) {
	refs, err := typedGraphOwnerRefs(records, generation, namespace, graph, state)
	if err != nil {
		return nil, err
	}
	return c.acquireColumnAssetLifecyclePinSetOwned(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "vector_index_searcher", Refs: refs})
}

// The reader already validated this manifest and decoded its selected graph and
// TVIS. Ordinary refs still need extraction: the ForScan manifest is header-only.
// Preserve the whole manifest closure, including document fields/sidecars for
// lazy materialization and other indexes. Only other graphs need decoding. No
// durable-publication normalization or duplicate selected TVIS decode is needed.
func typedGraphOwnerRefs(records []columnManifestRecord, generation uint64, namespace string, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) ([]ColumnAssetRef, error) {
	graphRefs, err := columnVectorGraphManifestAssetRefsForScan(graph, generation, namespace)
	if err != nil {
		return nil, err
	}
	stateRefs, err := columnVectorIndexStateManifestAssetRefsForScan(state, generation, namespace)
	if err != nil {
		return nil, err
	}
	refs := make([]ColumnAssetRef, 0, len(records)+len(graphRefs)+len(stateRefs))
	for _, record := range records {
		var ref ColumnAssetRef
		switch {
		case bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes):
			ref, _, _, _, _, _, err = decodeColumnManifestPartFieldsForScan(record.value, namespace)
			if err == nil {
				var keyGeneration, keyPart uint64
				keyGeneration, keyPart, err = decodeColumnManifestPartRecordKey(record.key)
				if err == nil && (keyGeneration != ref.Generation || keyPart != ref.PartID) {
					err = fmt.Errorf("collections: graph owner part key does not match asset ref")
				}
			}
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			var sidecar columnManifestAggregateMetadataSnapshot
			sidecar, err = decodeColumnManifestAggregateMetadataRecord(record.key, record.value)
			ref = sidecar.AssetRef
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			var sidecar columnManifestDictionaryCodesSnapshot
			sidecar, err = decodeColumnManifestDictionaryCodesRecord(record.key, record.value)
			ref = sidecar.AssetRef
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			var sidecar columnManifestInt64ValuesSnapshot
			sidecar, err = decodeColumnManifestInt64ValuesRecord(record.key, record.value)
			ref = sidecar.AssetRef
		case bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes):
			if columnManifestBytesEqualString(record.key[len(columnManifestVectorGraphRecordPrefixBytes):], graph.IndexName) {
				continue
			}
			other, decodeErr := decodeColumnVectorGraphManifestRecord(record.value)
			if decodeErr != nil {
				return nil, decodeErr
			}
			otherRefs, refsErr := columnVectorGraphManifestAssetRefsForScan(other, generation, namespace)
			if refsErr != nil {
				return nil, refsErr
			}
			refs = append(refs, otherRefs...)
			continue
		case bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes):
			if columnManifestBytesEqualString(record.key[len(columnVectorIndexStateRecordPrefixBytes):], state.IndexName) {
				continue
			}
			other, decodeErr := decodeColumnVectorIndexStateRecord(record.value)
			if decodeErr != nil {
				return nil, decodeErr
			}
			otherRefs, refsErr := columnVectorIndexStateManifestAssetRefsForScan(other, generation, namespace)
			if refsErr != nil {
				return nil, refsErr
			}
			refs = append(refs, otherRefs...)
			continue
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
		if ref.Namespace != namespace || ref.Generation == 0 || ref.Generation > generation {
			return nil, fmt.Errorf("collections: graph owner ref outside captured manifest: %+v", ref)
		}
		refs = append(refs, ref)
	}
	refs = append(refs, graphRefs...)
	refs = append(refs, stateRefs...)
	slices.SortFunc(refs, compareColumnAssetRefs)
	return slices.Compact(refs), nil
}

var typedGraphOwnerAfterSnapshotHook struct {
	sync.RWMutex
	fn func(*Collection)
}

func runTypedGraphOwnerAfterSnapshotHook(c *Collection) {
	typedGraphOwnerAfterSnapshotHook.RLock()
	fn := typedGraphOwnerAfterSnapshotHook.fn
	typedGraphOwnerAfterSnapshotHook.RUnlock()
	if fn != nil {
		fn(c)
	}
}
