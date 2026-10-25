package collections

import (
	"encoding/binary"
	"errors"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// importNativeJSONCatalog is a private complete import of the exact physical
// catalog. It publishes nothing and retains no snapshot/resource inventory.
// Every descriptor is memory-only; physical readers retain the enclosing cut's
// original Snapshot. Unsupported metadata, roots and overlays refuse before ACK; row values are
// resolved lazily by exact captured reads, never scanned during import.
func importNativeJSONCatalog(capture backenddb.NativeSemanticCapture) (base *nativeEngineVersion, resultErr error) {
	token, ok := capture.Snapshot().StateToken()
	if !ok {
		return nil, backenddb.ErrClosed
	}
	base, err := newNativeEmptyEngine(capture.NativeAllocator(), capture.NativeMaintenanceAllocator())
	if err != nil {
		return base, err
	}
	if token.SystemRootPageID == 0 {
		return base, nil
	}
	// Two fixed merge workspaces and one successor key own all borrowed scan
	// aliases; the original snapshot remains held by capture throughout.
	lease, err := capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(NativeJSONRead{}))) + 2*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainReadScratch{}))) + memtable.COWAllocationCharge(page.PageSize+1))
	if err != nil {
		return base, err
	}
	var scanScratch nativeDomainReadScratch
	probe := NativeJSONRead{capture: capture, scratch: &scanScratch}
	var scratch nativeDomainReadScratch
	var cursor [page.PageSize + 1]byte
	defer func() {
		probe = NativeJSONRead{}
		scratch = nativeDomainReadScratch{}
		scanScratch = nativeDomainReadScratch{}
		cursor = [page.PageSize + 1]byte{}
		lease.Close()
	}()
	reader, err := capture.Snapshot().NativeReaderAtRoot(token.SystemRootPageID)
	if err != nil {
		return base, err
	}
	get := func(key string) ([]byte, error) {
		entry, err := reader.GetEntryExactWithOwnedScratch([]byte(key), scratch.key[:], scratch.leaf[:], &scratch.point, probe.readLeaf)
		if err != nil {
			return nil, err
		}
		if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		return entry.Value, nil
	}
	raw, err := get(nativeCatalogNextIncarnationKey)
	if errors.Is(err, tree.ErrKeyNotFound) { // An empty physical catalog needs no counter.
		raw = nil
	} else if err != nil {
		return base, err
	} else {
		base.nextIncarnation, err = nativeCatalogIdentityValue(raw, true)
		if err != nil {
			return base, err
		}
	}
	scan := func(prefix string, visit func(node.LeafEntry) error) error {
		start := []byte(prefix)
		end := prefixEnd(start)
		for {
			probe.scratch.point.Invalidate()
			entry, found, err := reader.SeekGEEntryWithFixedScratch(start, end, probe.scratch.key[:], probe.scratch.leaf[:], &probe.scratch.successor, probe.readLeaf)
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
			if len(entry.Key) > page.PageSize {
				return memtable.ErrCOWCapacity
			}
			n := copy(cursor[:], entry.Key)
			cursor[n] = 0
			if entry.Flags&node.FlagTombstone == 0 {
				if entry.Flags&node.FlagPointer != 0 {
					return backenddb.ErrNativeSemanticUnsupported
				}
				if err := visit(entry); err != nil {
					return err
				}
			}
			start = cursor[:n+1]
		}
	}
	err = scan(systemCollectionMetaPrefix, func(entry node.LeafEntry) error {
		// Charge input-sized decode scratch before parsing scalar-index schema.
		// Unsupported column/vector/compound metadata still refuses below.
		raw := entry.Value
		if uint64(len(raw)) > (^uint64(0)-uint64(unsafe.Sizeof(CollectionMeta{})))/16 {
			return memtable.ErrCOWCapacity
		}
		transient, err := capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(uint64(len(raw))*16 + uint64(unsafe.Sizeof(CollectionMeta{}))))
		if err != nil {
			return err
		}
		var meta CollectionMeta
		defer func() { meta = CollectionMeta{}; raw = nil; transient.Close() }()
		meta, err = decodeCollectionMeta(raw)
		if err != nil {
			return err
		}
		meta, err = nativeJSONMeta(meta)
		if err != nil {
			return err
		}
		if string(entry.Key) != systemCollectionMetaPrefix+meta.Name {
			return backenddb.ErrNativeSemanticUnsupported
		}
		identity, err := get(nativeCatalogIncarnationPrefix + meta.Name)
		if err != nil {
			return err
		}
		id, err := nativeCatalogIdentityValue(identity, false)
		if err != nil || id >= base.nextIncarnation {
			return errNativeCatalogIdentity
		}
		if _, exists := base.collections.Get(id); exists {
			return errNativeCatalogIdentity
		}
		next, err := importNativeJSONCollection(base, meta, id, get)
		if err != nil {
			if next != nil {
				record := next.Release()
				next = nil
				err = errors.Join(err, finishPrivateNative(record))
			}
			return err
		}
		previous := base
		base = next
		record := previous.Release()
		previous = nil
		next = nil
		return finishPrivateNative(record)
	})
	if err != nil {
		return base, err
	}
	// Every live physical collection root must belong to one admitted descriptor;
	// overlays/unknown live domains refuse rather than installing a partial catalog.
	err = scan(systemCollectionRootOverlayPrefix, func(node.LeafEntry) error { return backenddb.ErrNativeSemanticUnsupported })
	if err != nil {
		return base, err
	}
	err = scan(systemCollectionRootPrefix, func(entry node.LeafEntry) error {
		qualified := strings.TrimPrefix(string(entry.Key), systemCollectionRootPrefix)
		name, domain, found := strings.Cut(qualified, "/")
		if !found || name == "" || domain == "" {
			return backenddb.ErrNativeSemanticUnsupported
		}
		root, err := decodeRootID(entry.Value)
		if err != nil {
			return err
		}
		// Maintained schema removal clears descriptors with encodeRootID(0).
		// Text Drop clears both versions, while recreation declares only one.
		// Such inactive descriptors carry no physical root to import.
		if root == 0 {
			return nil
		}
		c, ok := base.names.Get(name)
		if !ok || nativeJSONDomainNumber(*c.schema.meta, domain) == 0 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		d, ok := c.domains.Get(domain)
		if !ok {
			return backenddb.ErrNativeSemanticUnsupported
		}
		if root != d.baseRoot {
			return backenddb.ErrNativeSemanticUnsupported
		}
		return nil
	})
	return base, err
}

func importNativeJSONCollection(base *nativeEngineVersion, meta CollectionMeta, id uint64, get func(string) ([]byte, error)) (next *nativeEngineVersion, err error) {
	if base.inventory != nil || base.build != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	} // private import only
	generation := uint64(0)
	rawGeneration, generationErr := get(systemCollectionDocumentGenerationKey(meta.Name))
	if generationErr == nil {
		generation, generationErr = decodeRootID(rawGeneration)
	}
	if generationErr != nil && !errors.Is(generationErr, tree.ErrKeyNotFound) {
		return nil, generationErr
	}
	roots := collectionRootNames(meta)
	next, err = newNativeEnginePreparation(base, 1, len(roots), base.allocator)
	if err != nil {
		return nil, err
	}
	p := next.preparation
	defer p.sealPrivateBuilders(next)
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return base.allocator.AdmitNativeBytes(n) }
	p.schema, err = newNativeSchemaState(&meta, admit)
	if err != nil {
		return next, err
	}
	p.domainBuilder, err = cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, len(roots), nativeDomainLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.pendingDomainBuilder, err = cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, 0, nativeDomainLifetime{}, admit)
	if err != nil {
		return next, err
	}
	for _, qualified := range roots {
		root := uint64(0)
		raw, readErr := get(systemCollectionRootKey(qualified))
		if readErr == nil {
			root, readErr = decodeRootID(raw)
		}
		if readErr != nil && !errors.Is(readErr, tree.ErrKeyNotFound) {
			return next, readErr
		}
		policy, policyErr := collectionRootStoragePolicy(meta, qualified)
		if policyErr != nil {
			return next, policyErr
		}
		name := strings.TrimPrefix(qualified, meta.Name+"/")
		interpretation, sourceGeneration := uint64(1), uint64(0)
		if index, ok := strings.CutPrefix(name, "vector-index/"); ok {
			version, e := get(nativeVectorVersionKey(meta.Name, index))
			if e == nil {
				if len(version) != 16 {
					return next, backenddb.ErrNativeSemanticUnsupported
				}
				interpretation = binary.BigEndian.Uint64(version[:8])
				sourceGeneration = binary.BigEndian.Uint64(version[8:])
				if interpretation == 0 {
					return next, backenddb.ErrNativeSemanticUnsupported
				}
			} else if !errors.Is(e, tree.ErrKeyNotFound) {
				return next, e
			} else if root != 0 {
				interpretation = root
			}
		}

		domain, domainErr := newNativeDomainState(name, root, interpretation, policy, nil, admit)
		if domain != nil {
			domain.sourceGeneration = sourceGeneration
			p.ownedDomains = append(p.ownedDomains, domain)
		}
		if domainErr != nil {
			return next, domainErr
		}
		if err = p.domainBuilder.Set(name, domain); err != nil {
			return next, err
		}
	}
	p.sealPrivateBuilders(next)
	c, err := newNativeCollectionState(id, generation, p.schema, p.domains, p.pendingDomains, admit)
	if err != nil {
		return next, err
	}
	p.schema, p.domains, p.pendingDomains = nil, nil, nil
	p.ownedCollections = append(p.ownedCollections, c)
	p.names, err = cowbtree.NewOwnedMapBuilder(base.names, 1, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.collections, err = cowbtree.NewOwnedMapBuilder(base.collections, 1, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.pending, err = cowbtree.NewOwnedMapBuilder(base.pending, 0, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	if err = p.names.Set(meta.Name, c); err != nil {
		return next, err
	}
	err = p.collections.Set(id, c)
	return next, err
}
