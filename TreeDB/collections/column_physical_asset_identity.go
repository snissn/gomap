package collections

import (
	"errors"
	"os"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This reservation is call-local under the same namespace allocator stripe
// already used by ordinary freshAppender. It predicts no independent durable
// authority: no counter is consumed, namespace created, descriptor opened or
// registry Observe performed by preparation. The actual O_EXCL constructor is
// still authoritative. External collisions refuse this prepared operation;
// they cannot change its captured key or retry under another physical identity.
type columnPhysicalAssetIdentityReservation struct {
	namespace     columnAssetManagerNamespace
	namespaceName string
	lock          *sync.Mutex
	cache         *columnAssetSegmentAllocationCache
	fileID        uint32
	consumed      bool
	shared        bool
	incarnation   uint64
	start         int64
}

// Warm installed namespace geometry is required until the cold namespace/
// directory-scan constructor family has its complete pre-birth certificate.
// The caller prepays its own control/key plan before invoking this operation.
// It must release the reservation on every pre-WAL refusal and terminal path.
func prepareColumnPhysicalAssetIdentity(rootDir string, cfg ColumnStoreConfig, r *columnPhysicalAssetIdentityReservation) error {
	if r == nil || r.lock != nil || r.consumed || cfg.AssetManager == nil ||
		cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped || !cfg.AssetManager.IsolatedNamespace {
		return ErrPreparedInsertResourceLimit
	}
	pathCache := &columnAssetManagerNamespacePathCaches[columnAssetNamespacePathCacheIndex(rootDir, cfg.AssetManager.Namespace)]
	pathCache.Lock()
	if !pathCache.valid || pathCache.rootDir != rootDir || pathCache.namespace != cfg.AssetManager.Namespace {
		pathCache.Unlock()
		return ErrPreparedInsertResourceLimit
	}
	namespace := pathCache.value
	pathCache.Unlock()
	// Namespace paths are immutable string values. Keeping these exact values
	// live prevents cache eviction/address reuse from changing the identity.
	index := columnAssetSegmentAllocationLockIndex(namespace.SegmentDir)
	lock := &columnAssetSegmentAllocationLocks[index]
	cache := &columnAssetSegmentAllocationCaches[index]
	lock.Lock()
	if !cache.valid || cache.segmentDir != namespace.SegmentDir || cache.nextFileID == 0 ||
		cache.nextFileID >= columnAssetDirectViewSegmentFileIDBase {
		lock.Unlock()
		return ErrPreparedInsertResourceLimit
	}
	*r = columnPhysicalAssetIdentityReservation{namespace: namespace, namespaceName: cfg.AssetManager.Namespace, lock: lock, cache: cache, fileID: cache.nextFileID}
	return nil
}

func (r *columnPhysicalAssetIdentityReservation) release() {
	if r == nil || r.lock == nil {
		return
	}
	lock := r.lock
	*r = columnPhysicalAssetIdentityReservation{}
	lock.Unlock()
}

func (s *columnPhysicalAssetAppendSession) freshAppenderAtReservedIdentity(r *columnPhysicalAssetIdentityReservation) (*columnPhysicalAssetSegmentAppender, error) {
	if s == nil || s.active != nil || s.stableRegistry == nil || r == nil || r.lock == nil ||
		r.consumed || r.fileID == 0 || r.cache == nil || !r.cache.valid ||
		r.cache.segmentDir != r.namespace.SegmentDir || r.cache.nextFileID != r.fileID ||
		s.rootDir != r.namespace.ManagerRootDir {
		return nil, ErrPreparedInsertResourceLimit
	}
	if s.cfg.AssetManager == nil || s.cfg.AssetManager.Namespace != r.namespaceName {
		return nil, ErrPreparedInsertResourceLimit
	}
	pathCache := &columnAssetManagerNamespacePathCaches[columnAssetNamespacePathCacheIndex(s.rootDir, s.cfg.AssetManager.Namespace)]
	pathCache.Lock()
	matches := pathCache.valid && pathCache.rootDir == s.rootDir &&
		pathCache.namespace == s.cfg.AssetManager.Namespace && pathCache.value.SegmentDir == r.namespace.SegmentDir
	pathCache.Unlock()
	if !matches {
		return nil, ErrPreparedInsertResourceLimit
	}
	if err := s.candidateAdmission.charge(0, 1); err != nil {
		return nil, err
	}
	r.consumed = true // one actual attempt, never rebirth on a different key
	appender, err := newColumnPhysicalAssetSegmentAppenderWithStableResources(s.rootDir, s.cfg, r.fileID, s.stableRegistry, s.stableRecoveryRetainer)
	if err != nil {
		if errors.Is(err, ErrRecoveryRequired) {
			advanceColumnAssetSegmentFileIDCache(r.namespace.SegmentDir, r.cache, r.fileID)
		}
		// O_EXCL collision owns nothing and burns no identity. It is a refusal,
		// rather than an invitation to rebuild this prepared key after WAL.
		if errors.Is(err, os.ErrExist) {
			return nil, rootpublication.ErrResourceOwnership
		}
		return nil, err
	}
	if appender.fileID != r.fileID {
		panic("collections: exact physical constructor changed prepared identity")
	}
	advanceColumnAssetSegmentFileIDCache(r.namespace.SegmentDir, r.cache, r.fileID)
	appender.candidateAdmission = s.candidateAdmission
	appender.stableRecoveryRetainer = s.stableRecoveryRetainer
	s.active, s.activeFile = appender, appender.fileID
	return appender, nil
}

// prepareSharedColumnPhysicalAssetIdentity binds the actual installed M12A
// producer under the SAME append/delete stripe. It burns no ID and performs no
// constructor, append, Observe or install. Operational DB is only call-time.
// The reservation itself retains immutable path values + scalar incarnation;
// no DB, owner, FD, namespace token or callback is stored on it.
func prepareSharedColumnPhysicalAssetIdentity(db *backenddb.DB, rootDir string, cfg ColumnStoreConfig, r *columnPhysicalAssetIdentityReservation, account rootpublication.StableMetadataAccount) error {
	if db == nil || r == nil || r.lock != nil || r.consumed || cfg.AssetManager == nil ||
		cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped || !cfg.AssetManager.IsolatedNamespace ||
		columnStoreHasTypedColumnPartOwners(cfg) {
		return ErrPreparedInsertResourceLimit
	}
	cache := &columnAssetManagerNamespacePathCaches[columnAssetNamespacePathCacheIndex(rootDir, cfg.AssetManager.Namespace)]
	cache.Lock()
	if !cache.valid || cache.rootDir != rootDir || cache.namespace != cfg.AssetManager.Namespace {
		cache.Unlock()
		return ErrPreparedInsertResourceLimit
	}
	namespace := cache.value
	cache.Unlock()
	// This is one real canonical path allocation; admit its actual class before
	// concatenation. Existing namespace path values are immutable borrowed backing,
	// whose retained loan remains part of the complete packet census.
	const sharedName = "segment-000001.tca"
	if account != nil {
		class, err := rootpublication.StableBackingClassBytes(uint64(len(namespace.SegmentDir)+1+len(sharedName)), false)
		if err != nil {
			return err
		}
		if err := account.ReserveStableMetadata(class); err != nil {
			return err
		}
	}
	path := namespace.SegmentDir + string(os.PathSeparator) + sharedName
	index := rootpublication.SegmentWriteStripeIndex(path)
	lock := rootpublication.SegmentWriteStripe(index)
	lock.Lock()
	incarnation, err := db.ColumnSegmentProducerIdentityV1(cfg.AssetManager.Namespace, columnAssetM12ASegmentFileID, index)
	if err != nil {
		lock.Unlock()
		return err
	}
	start, err := db.ColumnSegmentProducerSizeV1(cfg.AssetManager.Namespace, columnAssetM12ASegmentFileID, incarnation, account)
	if err != nil || start > uint64(columnPhysicalAssetSegmentTargetBytes) {
		lock.Unlock()
		if err == nil {
			err = ErrPreparedInsertResourceLimit
		}
		return err
	}
	*r = columnPhysicalAssetIdentityReservation{namespace: namespace, namespaceName: cfg.AssetManager.Namespace,
		lock: lock, fileID: columnAssetM12ASegmentFileID, shared: true, incarnation: incarnation, start: int64(start)}
	return nil
}

// sharedAppenderAtReservedIdentity uses the same installed owner and SAME held
// stripe as preparation. Its numeric frontier must still match before writes.
// No FD/parent/counter is born, and release never unlocks the caller's stripe.
func (s *columnPhysicalAssetAppendSession) sharedAppenderAtReservedIdentity(r *columnPhysicalAssetIdentityReservation) (*columnPhysicalAssetSegmentAppender, error) {
	if s == nil || s.active != nil || s.producerDB == nil || r == nil || !r.shared || r.lock == nil ||
		r.consumed || r.fileID != columnAssetM12ASegmentFileID || r.incarnation == 0 ||
		s.cfg.AssetManager == nil || s.cfg.AssetManager.Namespace != r.namespaceName || s.rootDir != r.namespace.ManagerRootDir {
		return nil, ErrPreparedInsertResourceLimit
	}
	start, err := s.producerDB.ColumnSegmentProducerSizeV1(r.namespaceName, r.fileID, r.incarnation, nil)
	if err != nil {
		return nil, err
	}
	if start != uint64(r.start) {
		return nil, rootpublication.ErrResourceConflict
	}
	path, err := columnAssetSegmentPath(s.rootDir, ColumnAssetRef{Namespace: r.namespaceName, FileID: r.fileID})
	if err != nil {
		return nil, err
	}
	if columnAssetSegmentWriteLock(path) != r.lock {
		return nil, rootpublication.ErrResourceOwnership
	}
	r.consumed = true // one actual append attempt; no allocation/durable ID counter
	a := &columnPhysicalAssetSegmentAppender{cfg: s.cfg, namespace: r.namespace, fileID: r.fileID,
		assetPath: path, offset: r.start, appendStart: r.start, lock: r.lock, unlockLock: false,
		stableRegistry: s.stableRegistry, producerDB: s.producerDB, producerIncarnation: r.incarnation,
		producerReused: true, producerInstalled: true, candidateAdmission: s.candidateAdmission}
	s.active, s.activeFile = a, a.fileID
	return a, nil
}
