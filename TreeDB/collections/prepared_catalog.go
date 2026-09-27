package collections

import (
	"bytes"
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

// catalogForPreparedInsertSnapshot keeps a stale prepared token able to rebind
// after a sibling publication without taking the general catalog loader's
// unbounded root-overlay and typed-graph paths.
func (c *Collection) catalogForPreparedInsertSnapshot(snap *backenddb.Snapshot, schema CollectionMeta) (*collectionCatalog, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	snap.MarkForegroundRead()
	if cached := c.cachedCatalogForSnapshot(snap, true); cached != nil {
		if err := checkPreparedCatalogShape(cached); err != nil {
			return nil, err
		}
		return cached, nil
	}
	systemRoot, commitSeq := snapshotSystemRoot(snap), snapshotCommitSeq(snap)
	cached := cachedWriteDomainCatalogForState(c.writeDomain, systemRoot, commitSeq)
	if cached != nil && cached.pager == snap.Pager() {
		if err := checkPreparedCatalogShape(cached); err != nil {
			return nil, err
		}
		c.rememberCatalog(snap, cached)
		return cached, nil
	}
	catalog, err := loadPreparedInsertCatalog(snap, schema)
	if err != nil {
		return nil, err
	}
	c.rememberCatalog(snap, catalog)
	return catalog, nil
}

func checkPreparedCatalogShape(catalog *collectionCatalog) error {
	if catalog == nil || len(catalog.rootOverlays) != 0 || catalog.typedGraphBase != nil {
		return fmt.Errorf("%w: prepared collection has root overlays or typed graph aliases", ErrPreparedInsertResourceLimit)
	}
	return nil
}

// loadPreparedInsertCatalog accepts the bounded no-index column shape captured
// by Prepare. Point values are checked before cloning or JSON decoding. A root
// overlay or typed-graph alias rejects this prepared commit before WAL.
func loadPreparedInsertCatalog(snap *backenddb.Snapshot, schema CollectionMeta) (*collectionCatalog, error) {
	raw, ok, err := getPreparedSystemValue(snap, systemCollectionMetaKey(schema.Name), preparedInsertMaxSystemMetaJSONBytes)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errCollectionNotFound
	}
	meta, err := decodeCollectionMeta(raw)
	if err != nil {
		return nil, err
	}
	if !sameCollectionMeta(preparedInsertSchemaMeta(meta), schema) {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", schema.Name)
	}
	rootNames := collectionRootNames(meta)
	if len(rootNames) > 8 {
		return nil, fmt.Errorf("%w: prepared collection has too many roots", ErrPreparedInsertResourceLimit)
	}
	roots := make(map[string]uint64, len(rootNames))
	for _, rootName := range rootNames {
		rootValue, found, err := getPreparedSystemValue(snap, systemCollectionRootKey(rootName), 8)
		if err != nil {
			return nil, err
		}
		if found {
			rootID, err := decodeRootID(rootValue)
			if err != nil {
				return nil, err
			}
			roots[rootName] = rootID
		}
		if _, found, err := getPreparedSystemValue(snap, systemCollectionRootOverlayKey(rootName), 0); err != nil {
			return nil, err
		} else if found {
			return nil, fmt.Errorf("%w: prepared collection root overlay %q", ErrPreparedInsertResourceLimit, rootName)
		}
	}
	if _, found, err := getPreparedSystemValue(snap, typedGraphBaseControlPrefix+meta.Name, 0); err != nil {
		return nil, err
	} else if found {
		return nil, fmt.Errorf("%w: prepared collection has a typed graph alias", ErrPreparedInsertResourceLimit)
	}
	catalog := newCollectionCatalog(meta, roots)
	catalog.pager = snap.Pager()
	if err := validateColumnStoreCatalogRoot(snap, catalog); err != nil {
		return nil, err
	}
	return catalog, nil
}

func getPreparedSystemValue(snap *backenddb.Snapshot, key string, maxBytes int) ([]byte, bool, error) {
	if snap == nil {
		return nil, false, backenddb.ErrClosed
	}
	state, ok := snap.StateToken()
	if !ok || state.SystemRootPageID == 0 {
		return nil, false, nil
	}
	entry, err := snap.GetEntryAtRoot(state.SystemRootPageID, []byte(key))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if len(entry.Value) > maxBytes {
		return nil, false, fmt.Errorf("%w: prepared system value %q exceeds %d bytes", ErrPreparedInsertResourceLimit, key, maxBytes)
	}
	return bytes.Clone(entry.Value), true, nil
}
