package collections

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// This domain owns actual system-tree effects, not a raw user-key carrier or a
// separately published catalog tree. Its physical root stays zero: the existing
// complete-cut build group owns the exact system basis and resulting root.
const nativeCatalogDomainName = "system-catalog"

func nativeCatalogHasPending(source *nativeDomainState) bool {
	return source != nil && (source.pending != nil || source.captured != nil)
}

// prepareNativeCatalogUpdate copies original system mutations into the same
// bounded native source machinery as row domains. Identity-free producer
// preparation calls this before canonical assignment. Pointers are not catalog
// encodings: durable external resources have their own original closure.
func prepareNativeCatalogUpdate(base *nativeDomainState, entries []memtable.COWMutation, allocator nativeDomainAllocator) (*nativeDomainState, error) {
	if len(entries) == 0 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	for _, row := range entries {
		if row.Flags&node.FlagPointer != 0 || row.Remove {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
	}
	return prepareNativeDomainUpdate(base, nativeCatalogDomainName, 0, 1, 0, entries, allocator)
}

// prepareNativeEngineCatalogUpdate is an identity-free private catalog-only
// successor. The original Work owns its partial before a later admission can
// fail. DDL producers also update the immutable schema directories in their
// original plan; this helper alone does not claim a complete DDL operation.
func prepareNativeEngineCatalogUpdate(base *nativeEngineVersion, entries []memtable.COWMutation, nextIncarnation uint64) (*nativeEngineVersion, error) {
	if base == nil || nextIncarnation < base.nextIncarnation || nextIncarnation == 0 || len(entries) == 0 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	next, err := newNativeEnginePreparation(base, 0, 0, base.allocator)
	if err != nil {
		return nil, err
	}
	next.nextIncarnation = nextIncarnation
	next.catalog, err = prepareNativeCatalogUpdate(base.catalog, entries, base.allocator)
	if err != nil {
		return next, err
	}
	retainNativeEngineDirectories(next, base)
	return next, nil
}

// A private catalog-only successor shares the existing immutable directories.
// These are actual owned node/value edges, with no opening Engine backpointer.
func retainNativeEngineDirectories(next, base *nativeEngineVersion) {
	base.names.Retain()
	next.names = base.names
	base.collections.Retain()
	next.collections = base.collections
	base.pending.Retain()
	next.pending = base.pending
}

// nativeCatalogProjectionMeta borrows only the exact captured directory. Older
// catalog images retain logical command history, but their physical manifest
// fields can precede a completed build. Tombstones and all other effects keep
// their original representation and ordering.
func nativeCatalogProjectionMeta(names *cowbtree.OwnedMap[string, *nativeCollectionState], row memtable.COWRecord) *CollectionMeta {
	if row.Flags&(node.FlagTombstone|node.FlagPointer) != 0 || !strings.HasPrefix(row.Key, systemCollectionMetaPrefix) {
		return nil
	}
	collection, ok := names.Get(row.Key[len(systemCollectionMetaPrefix):])
	if !ok || collection.schema.meta.Options.ColumnStore == nil {
		return nil
	}
	return collection.schema.meta
}

// nativeCatalogProjectionCapacity counts the same immutable source rows later
// visited by materialize. Every output may remain borrowed by the original
// 256-entry batch until its synchronous flush; charge all output buffers, not
// just the largest row. Decode scratch is sequential and uses the original
// metadata-import bound, while encoding uses the maintained JSON-size census.
func nativeCatalogProjectionCapacity(captured *nativeEngineVersion) (uint64, error) {
	if !nativeCatalogHasPending(captured.catalog) {
		return 0, nil
	}
	var outputs, decodePeak uint64
	var failure error
	visit := func(row memtable.COWRecord) bool {
		meta := nativeCatalogProjectionMeta(captured.names, row)
		if meta == nil {
			return true
		}
		if uint64(len(row.Value)) > (^uint64(0)-uint64(unsafe.Sizeof(CollectionMeta{})))/16 {
			failure = memtable.ErrCOWCapacity
			return false
		}
		decode := memtable.COWAllocationCharge(uint64(len(row.Value))*16 + uint64(unsafe.Sizeof(CollectionMeta{})))
		if decode > decodePeak {
			decodePeak = decode
		}
		encoded, err := nativeIndexJSONBound(reflect.ValueOf(collectionMetaDisk{Version: collectionMetaVersion, Name: meta.Name, Options: meta.Options, Indexes: meta.Indexes, VectorIndexes: meta.VectorIndexes, TextIndexes: meta.TextIndexes}), 0)
		if err != nil || encoded > ^uint64(0)/2 {
			failure = memtable.ErrCOWCapacity
			if err != nil {
				failure = err
			}
			return false
		}
		// The encoder's growable buffer and returned image coexist. Reuse the
		// original metadata-encoding allowance for these actual allocations.
		for _, size := range [2]uint64{2 * encoded, encoded} {
			if failure = nativeTypedCheckedAdd(&outputs, memtable.COWAllocationCharge(size)); failure != nil {
				return false
			}
		}
		return true
	}
	for source := captured.catalog.captured; source != nil && failure == nil; source = source.older {
		source.source.root.ScanOwned(visit)
	}
	if failure == nil && captured.catalog.pending != nil {
		captured.catalog.pending.ScanOwned(visit)
	}
	if failure != nil {
		return 0, failure
	}
	if err := nativeTypedCheckedAdd(&outputs, decodePeak); err != nil {
		return 0, err
	}
	return outputs, nil
}

// nativeCatalogProjectedValue validates every replaced source image before
// encoding the same captured logical schema with its already accepted physical
// outputs. No current-state lookup or captured-engine edge escapes this call.
func nativeCatalogProjectedValue(meta *CollectionMeta, row memtable.COWRecord) ([]byte, error) {
	before, err := decodeCollectionMeta(nativeBorrowedStringBytes(row.Value))
	if err != nil {
		return nil, fmt.Errorf("collections: captured catalog metadata %s: %w", meta.Name, err)
	}
	if before.Name != meta.Name || !nativeTypedMeta(before) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if !nativeIndexLogicalMetaEqual(*meta, before) {
		compatible := before
		compatible.Indexes = meta.Indexes
		compatible = normalizeCollectionIndexedBufferOptions(compatible)
		if !nativeIndexSubsetRemoval(before.Indexes, meta.Indexes) || !nativeIndexLogicalMetaEqual(*meta, compatible) {
			return nil, backenddb.ErrRootPublicationBasisMismatch
		}
	}
	return encodeNormalizedCollectionMeta(*meta)
}
