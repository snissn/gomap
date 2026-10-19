package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
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
