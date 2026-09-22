package collections

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// collectionQueryReadyGenerationOpenKey derives M3's query-independent key
// from the existing collection publication/cache identity. It deliberately
// does not inspect a query request or choose an authoritative root.
func collectionQueryReadyGenerationOpenKey(identity ColumnStoreCacheIdentity) (typedcolumn.QueryReadyGenerationOpenKey, bool) {
	if identity.Collection == "" || identity.SchemaHash == 0 || identity.ManifestGeneration == 0 || identity.ManifestChecksum == 0 || identity.ManifestRoot == 0 || identity.ManifestRootName == "" {
		return typedcolumn.QueryReadyGenerationOpenKey{}, false
	}
	var schemaInput [8]byte
	binary.LittleEndian.PutUint64(schemaInput[:], identity.SchemaHash)
	schemaDigest := sha256.New()
	_, _ = schemaDigest.Write([]byte("treedb-query-ready-column-schema-v1\x00"))
	_, _ = schemaDigest.Write(schemaInput[:])
	var schemaHash [sha256.Size]byte
	copy(schemaHash[:], schemaDigest.Sum(nil))

	manifestDigest := sha256.New()
	_, _ = manifestDigest.Write([]byte("treedb-query-ready-column-manifest-v1\x00"))
	writeQueryReadyIdentityString(manifestDigest, identity.Collection)
	writeQueryReadyIdentityUint64(manifestDigest, identity.SchemaHash)
	writeQueryReadyIdentityUint64(manifestDigest, identity.CatalogSystemRoot)
	writeQueryReadyIdentityUint64(manifestDigest, identity.CatalogCommitSeq)
	writeQueryReadyIdentityUint64(manifestDigest, identity.ManifestGeneration)
	writeQueryReadyIdentityUint64(manifestDigest, identity.ManifestChecksum)
	writeQueryReadyIdentityUint64(manifestDigest, identity.RecoveryAuthoritativeGeneration)
	writeQueryReadyIdentityUint64(manifestDigest, identity.RecoveryAuthoritativeChecksum)
	writeQueryReadyIdentityUint64(manifestDigest, identity.RecoveryAuthoritativeAppliedCommandLSN)
	writeQueryReadyIdentityUint64(manifestDigest, identity.ManifestRoot)
	writeQueryReadyIdentityString(manifestDigest, identity.ManifestRootName)
	var manifestHash [sha256.Size]byte
	copy(manifestHash[:], manifestDigest.Sum(nil))
	return typedcolumn.QueryReadyGenerationOpenKey{
		Identity:     typedcolumn.QueryReadyBaseIdentity{Generation: identity.ManifestGeneration, SchemaHash: schemaHash},
		ManifestHash: manifestHash,
	}, true
}

type queryReadyIdentityHash interface {
	Write([]byte) (int, error)
}

func writeQueryReadyIdentityUint64(hash queryReadyIdentityHash, value uint64) {
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], value)
	_, _ = hash.Write(raw[:])
}

func writeQueryReadyIdentityString(hash queryReadyIdentityHash, value string) {
	writeQueryReadyIdentityUint64(hash, uint64(len(value)))
	_, _ = hash.Write([]byte(value))
}

type collectionQueryReadyGenerationCacheSnapshot struct {
	Present       bool
	Identity      ColumnStoreCacheIdentity
	Open          typedcolumn.QueryReadyGenerationOpenStats
	CacheHits     uint64
	CacheBuilds   uint64
	Invalidations uint64
	// ActiveLeases is scoped to the current entry. Retired entries are detached
	// and remain protected by their leases until final release.
	ActiveLeases int
}

type collectionQueryReadyGenerationCacheEntry struct {
	identity ColumnStoreCacheIdentity
	files    typedcolumn.QueryReadyGenerationOpenFiles
	cache    *typedcolumn.QueryReadyGenerationOpenCache
	refs     int
	stale    bool
}

// collectionQueryReadyGenerationFileSelectionEqual compares the immutable
// physical snapshot selected by publication. Bound is deliberately excluded:
// it is a caller-local admission policy that the underlying open cache checks
// independently and must not change the mapped generation identity.
func collectionQueryReadyGenerationFileSelectionEqual(left, right typedcolumn.QueryReadyGenerationOpenFiles) bool {
	return left.Key == right.Key &&
		left.Base == right.Base &&
		left.SnapshotGeneration == right.SnapshotGeneration &&
		slices.Equal(left.Deltas, right.Deltas)
}

func cloneCollectionQueryReadyGenerationOpenFiles(files typedcolumn.QueryReadyGenerationOpenFiles) typedcolumn.QueryReadyGenerationOpenFiles {
	cloned := files
	cloned.Deltas = slices.Clone(files.Deltas)
	return cloned
}

// collectionQueryReadyGenerationLease pins one mapped prepared generation
// across collection identity invalidation. Close releases the pin; stale
// mappings close only after the final lease is released.
type collectionQueryReadyGenerationLease struct {
	collection *Collection
	entry      *collectionQueryReadyGenerationCacheEntry
	prepared   *typedcolumn.QueryReadyPreparedGeneration
	closeOnce  sync.Once
	closeErr   error
}

// Prepared returns the immutable generation pinned by this lease. The pointer
// must not be retained or used after Close.
func (l *collectionQueryReadyGenerationLease) Prepared() *typedcolumn.QueryReadyPreparedGeneration {
	if l == nil {
		return nil
	}
	return l.prepared
}

func (l *collectionQueryReadyGenerationLease) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		if l.collection != nil && l.entry != nil {
			l.closeErr = l.collection.releaseCollectionQueryReadyGenerationLease(l.entry)
		}
		l.prepared = nil
	})
	return l.closeErr
}

// openCollectionQueryReadyGeneration is the collection-scoped M3 integration
// seam for M4. The authoritative ColumnStoreCacheIdentity is read first; the
// caller-supplied files must already be selected by the existing publication
// inventory and carry the exact derived key.
func (c *Collection) openCollectionQueryReadyGeneration(files typedcolumn.QueryReadyGenerationOpenFiles) (*collectionQueryReadyGenerationLease, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	identity, ok := c.ColumnStoreCacheIdentity()
	if !ok {
		return nil, &typedcolumn.QueryReadyGenerationOpenError{State: typedcolumn.QueryReadyOpenAbsentRebuildable, Err: errors.New("collections: query-ready generation has no column-store identity")}
	}
	return c.openCollectionQueryReadyGenerationForIdentity(identity, files)
}

func (c *Collection) openCollectionQueryReadyGenerationForIdentity(identity ColumnStoreCacheIdentity, files typedcolumn.QueryReadyGenerationOpenFiles) (*collectionQueryReadyGenerationLease, error) {
	key, ok := collectionQueryReadyGenerationOpenKey(identity)
	if !ok {
		return nil, &typedcolumn.QueryReadyGenerationOpenError{State: typedcolumn.QueryReadyOpenAbsentRebuildable, Err: errors.New("collections: incomplete query-ready column-store identity")}
	}
	if files.Key != key {
		return nil, &typedcolumn.QueryReadyGenerationOpenError{State: typedcolumn.QueryReadyOpenUnsupportedOrStale, Err: errors.New("collections: query-ready file set does not match current column-store identity")}
	}

	var closeOld *typedcolumn.QueryReadyGenerationOpenCache
	c.queryReadyGenerationMu.Lock()
	entry := c.queryReadyGenerationEntry
	if entry != nil && entry.identity == identity && collectionQueryReadyGenerationFileSelectionEqual(entry.files, files) {
		c.queryReadyGenerationHits++
	} else {
		if entry != nil {
			entry.stale = true
			if entry.refs == 0 {
				closeOld = entry.cache
			}
			c.queryReadyGenerationInvalidations++
		}
		entry = &collectionQueryReadyGenerationCacheEntry{identity: identity, files: cloneCollectionQueryReadyGenerationOpenFiles(files), cache: typedcolumn.NewQueryReadyGenerationOpenCache(key)}
		c.queryReadyGenerationEntry = entry
		c.queryReadyGenerationBuilds++
	}
	entry.refs++
	c.queryReadyGenerationMu.Unlock()
	if closeOld != nil {
		_ = closeOld.Close()
	}

	prepared, err := entry.cache.Open(files)
	if err != nil {
		return nil, errors.Join(err, c.releaseCollectionQueryReadyGenerationLease(entry))
	}
	if c.manager != nil && !c.manager.registerCollectionHandleIfOpen(c) {
		c.queryReadyGenerationMu.Lock()
		if c.queryReadyGenerationEntry == entry {
			c.queryReadyGenerationEntry = nil
			entry.stale = true
			c.queryReadyGenerationInvalidations++
		}
		c.queryReadyGenerationMu.Unlock()
		return nil, errors.Join(backenddb.ErrClosed, c.releaseCollectionQueryReadyGenerationLease(entry))
	}
	return &collectionQueryReadyGenerationLease{collection: c, entry: entry, prepared: prepared}, nil
}

func (c *Collection) releaseCollectionQueryReadyGenerationLease(entry *collectionQueryReadyGenerationCacheEntry) error {
	if c == nil || entry == nil {
		return nil
	}
	var closeCache *typedcolumn.QueryReadyGenerationOpenCache
	c.queryReadyGenerationMu.Lock()
	if entry.refs > 0 {
		entry.refs--
	}
	if entry.refs == 0 && entry.stale {
		closeCache = entry.cache
	}
	c.queryReadyGenerationMu.Unlock()
	if closeCache != nil {
		err := closeCache.Close()
		if c.manager != nil && !c.hasDirtyNativeVectorIndex() && !c.hasCollectionVectorIndexPreparedSearchCacheEntries() && !c.hasCollectionTypedColumnOneShotCacheEntries() && !c.hasCollectionQueryReadyGenerationCache() {
			c.manager.unregisterCollectionHandle(c)
		}
		return err
	}
	return nil
}

func (c *Collection) collectionQueryReadyGenerationCacheSnapshot() collectionQueryReadyGenerationCacheSnapshot {
	if c == nil {
		return collectionQueryReadyGenerationCacheSnapshot{}
	}
	c.queryReadyGenerationMu.Lock()
	defer c.queryReadyGenerationMu.Unlock()
	snapshot := collectionQueryReadyGenerationCacheSnapshot{
		Present:   c.queryReadyGenerationEntry != nil,
		CacheHits: c.queryReadyGenerationHits, CacheBuilds: c.queryReadyGenerationBuilds,
		Invalidations: c.queryReadyGenerationInvalidations,
	}
	if c.queryReadyGenerationEntry != nil {
		snapshot.Identity = c.queryReadyGenerationEntry.identity
		snapshot.Open = c.queryReadyGenerationEntry.cache.Stats()
		snapshot.ActiveLeases = c.queryReadyGenerationEntry.refs
	}
	return snapshot
}

func (c *Collection) hasCollectionQueryReadyGenerationCache() bool {
	if c == nil {
		return false
	}
	c.queryReadyGenerationMu.Lock()
	defer c.queryReadyGenerationMu.Unlock()
	return c.queryReadyGenerationEntry != nil
}

func (c *Collection) closeCollectionQueryReadyGenerationCache() error {
	if c == nil {
		return nil
	}
	c.queryReadyGenerationMu.Lock()
	entry := c.queryReadyGenerationEntry
	c.queryReadyGenerationEntry = nil
	var cache *typedcolumn.QueryReadyGenerationOpenCache
	if entry != nil {
		entry.stale = true
		if entry.refs == 0 {
			cache = entry.cache
		}
		c.queryReadyGenerationInvalidations++
	}
	c.queryReadyGenerationMu.Unlock()
	var err error
	if cache != nil {
		err = cache.Close()
	}
	if c.manager != nil && (entry == nil || cache != nil) && !c.hasDirtyNativeVectorIndex() && !c.hasCollectionVectorIndexPreparedSearchCacheEntries() && !c.hasCollectionTypedColumnOneShotCacheEntries() && !c.hasCollectionQueryReadyGenerationCache() {
		c.manager.unregisterCollectionHandle(c)
	}
	return err
}

// retireCollectionQueryReadyGenerationCache closes the exact M3 cache that
// may map a prepared QRBG before its asset-manager tail is reclaimed. It
// refuses to detach a cache with live leases so cleanup cannot truncate under
// an active mmap.
func (c *Collection) retireCollectionQueryReadyGenerationCache(identity ColumnStoreCacheIdentity, files typedcolumn.QueryReadyGenerationOpenFiles) error {
	if c == nil {
		return nil
	}
	c.queryReadyGenerationMu.Lock()
	entry := c.queryReadyGenerationEntry
	if entry == nil || entry.identity != identity || !collectionQueryReadyGenerationFileSelectionEqual(entry.files, files) {
		c.queryReadyGenerationMu.Unlock()
		return nil
	}
	if entry.refs != 0 {
		c.queryReadyGenerationMu.Unlock()
		return ErrQueryReadyColumnGenerationBusy
	}
	c.queryReadyGenerationEntry = nil
	entry.stale = true
	c.queryReadyGenerationInvalidations++
	c.queryReadyGenerationMu.Unlock()
	err := entry.cache.Close()
	if c.manager != nil && !c.hasDirtyNativeVectorIndex() && !c.hasCollectionVectorIndexPreparedSearchCacheEntries() && !c.hasCollectionTypedColumnOneShotCacheEntries() && !c.hasCollectionQueryReadyGenerationCache() {
		c.manager.unregisterCollectionHandle(c)
	}
	return err
}

func (m *CollectionManager) closeCollectionQueryReadyGenerationCaches() error {
	if m == nil {
		return nil
	}
	m.collectionsMu.RLock()
	collections := make([]*Collection, 0, len(m.collections))
	for collection := range m.collections {
		if collection != nil {
			collections = append(collections, collection)
		}
	}
	m.collectionsMu.RUnlock()
	var closeErr error
	for _, collection := range collections {
		closeErr = errors.Join(closeErr, collection.closeCollectionQueryReadyGenerationCache())
	}
	return closeErr
}
