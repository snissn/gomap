package collections

import (
	"context"
	"sync"
	"sync/atomic"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type chunkLifecycleLock struct {
	token chan struct{}
	refs  int
}
type collectionSchemaCoordinator struct {
	mutationMu              sync.Mutex
	schemaMu                sync.RWMutex
	nativeVectorAdmissionMu sync.RWMutex
	nativeVectorBaseline    atomic.Pointer[uint64]
	hasNativeVectorIndexes  atomic.Bool
	legacyVectorSidecarMu   sync.Mutex
	domainsMu               sync.Mutex
	domains                 map[*collectionWriteDomain]struct{}
	chunkLifecycleMu        sync.Mutex
	chunkLifecycles         map[string]*chunkLifecycleLock
	chunkMutationOnce       sync.Once
	chunkMutationToken      chan struct{}
}

type collectionDBSchemaCoordinators struct {
	mu          sync.Mutex
	collections map[string]*collectionSchemaCoordinator
}

var collectionSchemaCoordinators sync.Map

var collectionSchemaMutationFlushHook struct {
	mu sync.Mutex
	fn func()
}

var collectionSchemaMutationBeforeLockHook struct {
	mu sync.Mutex
	fn func()
}

var legacyVectorSnapshotPostEpochRenameHook struct {
	mu sync.Mutex
	fn func()
}

var legacyVectorSidecarBeforeLockHook struct {
	mu sync.Mutex
	fn func()
}

func setLegacyVectorSnapshotPostEpochRenameHookForTest(fn func()) func() {
	legacyVectorSnapshotPostEpochRenameHook.mu.Lock()
	prev := legacyVectorSnapshotPostEpochRenameHook.fn
	legacyVectorSnapshotPostEpochRenameHook.fn = fn
	legacyVectorSnapshotPostEpochRenameHook.mu.Unlock()
	return func() {
		legacyVectorSnapshotPostEpochRenameHook.mu.Lock()
		legacyVectorSnapshotPostEpochRenameHook.fn = prev
		legacyVectorSnapshotPostEpochRenameHook.mu.Unlock()
	}
}

func runLegacyVectorSnapshotPostEpochRenameHookForTest() {
	legacyVectorSnapshotPostEpochRenameHook.mu.Lock()
	fn := legacyVectorSnapshotPostEpochRenameHook.fn
	legacyVectorSnapshotPostEpochRenameHook.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func setLegacyVectorSidecarBeforeLockHookForTest(fn func()) func() {
	legacyVectorSidecarBeforeLockHook.mu.Lock()
	prev := legacyVectorSidecarBeforeLockHook.fn
	legacyVectorSidecarBeforeLockHook.fn = fn
	legacyVectorSidecarBeforeLockHook.mu.Unlock()
	return func() {
		legacyVectorSidecarBeforeLockHook.mu.Lock()
		legacyVectorSidecarBeforeLockHook.fn = prev
		legacyVectorSidecarBeforeLockHook.mu.Unlock()
	}
}

func runLegacyVectorSidecarBeforeLockHookForTest() {
	legacyVectorSidecarBeforeLockHook.mu.Lock()
	fn := legacyVectorSidecarBeforeLockHook.fn
	legacyVectorSidecarBeforeLockHook.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// setCollectionSchemaMutationFlushHookForTest temporarily replaces the flush
// hook and returns a closure that restores the previous hook.
func setCollectionSchemaMutationFlushHookForTest(fn func()) func() {
	collectionSchemaMutationFlushHook.mu.Lock()
	prev := collectionSchemaMutationFlushHook.fn
	collectionSchemaMutationFlushHook.fn = fn
	collectionSchemaMutationFlushHook.mu.Unlock()
	return func() {
		collectionSchemaMutationFlushHook.mu.Lock()
		collectionSchemaMutationFlushHook.fn = prev
		collectionSchemaMutationFlushHook.mu.Unlock()
	}
}

func runCollectionSchemaMutationFlushHookForTest() {
	collectionSchemaMutationFlushHook.mu.Lock()
	fn := collectionSchemaMutationFlushHook.fn
	collectionSchemaMutationFlushHook.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// setCollectionSchemaMutationBeforeLockHookForTest temporarily replaces the
// pre-lock hook and returns a closure that restores the previous hook.
func setCollectionSchemaMutationBeforeLockHookForTest(fn func()) func() {
	collectionSchemaMutationBeforeLockHook.mu.Lock()
	prev := collectionSchemaMutationBeforeLockHook.fn
	collectionSchemaMutationBeforeLockHook.fn = fn
	collectionSchemaMutationBeforeLockHook.mu.Unlock()
	return func() {
		collectionSchemaMutationBeforeLockHook.mu.Lock()
		collectionSchemaMutationBeforeLockHook.fn = prev
		collectionSchemaMutationBeforeLockHook.mu.Unlock()
	}
}

func runCollectionSchemaMutationBeforeLockHookForTest() {
	collectionSchemaMutationBeforeLockHook.mu.Lock()
	fn := collectionSchemaMutationBeforeLockHook.fn
	collectionSchemaMutationBeforeLockHook.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func collectionSchemaCoordinatorForDBCollection(db *backenddb.DB, collection string) *collectionSchemaCoordinator {
	if db == nil || collection == "" {
		return nil
	}
	dbCoord := collectionDBSchemaCoordinatorForDB(db)
	if dbCoord == nil {
		return nil
	}
	dbCoord.mu.Lock()
	defer dbCoord.mu.Unlock()
	if dbCoord.collections == nil {
		dbCoord.collections = make(map[string]*collectionSchemaCoordinator)
	}
	coord := dbCoord.collections[collection]
	if coord == nil {
		coord = &collectionSchemaCoordinator{}
		dbCoord.collections[collection] = coord
	}
	return coord
}

func collectionDBSchemaCoordinatorForDB(db *backenddb.DB) *collectionDBSchemaCoordinators {
	if db == nil {
		return nil
	}
	coord := &collectionDBSchemaCoordinators{}
	var actual any
	var loaded bool
	if _, ok := db.RegisterCloseHookIfOpenAfter(func() bool {
		actual, loaded = collectionSchemaCoordinators.LoadOrStore(db, coord)
		return !loaded
	}, func() error {
		collectionSchemaCoordinators.Delete(db)
		return nil
	}); !ok {
		return nil
	}
	if loaded {
		return actual.(*collectionDBSchemaCoordinators)
	}
	return coord
}

func (coord *collectionSchemaCoordinator) registerDomain(domain *collectionWriteDomain) {
	if coord == nil || domain == nil {
		return
	}
	coord.domainsMu.Lock()
	defer coord.domainsMu.Unlock()
	if coord.domains == nil {
		coord.domains = make(map[*collectionWriteDomain]struct{})
	}
	coord.domains[domain] = struct{}{}
}

func (coord *collectionSchemaCoordinator) snapshotDomains() []*collectionWriteDomain {
	if coord == nil {
		return nil
	}
	coord.domainsMu.Lock()
	defer coord.domainsMu.Unlock()
	out := make([]*collectionWriteDomain, 0, len(coord.domains))
	for domain := range coord.domains {
		if domain != nil {
			out = append(out, domain)
		}
	}
	return out
}

func (coord *collectionSchemaCoordinator) lockChunkLifecycle(ctx context.Context, parentID string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	coord.chunkLifecycleMu.Lock()
	if coord.chunkLifecycles == nil {
		coord.chunkLifecycles = make(map[string]*chunkLifecycleLock)
	}
	entry := coord.chunkLifecycles[parentID]
	if entry == nil {
		entry = &chunkLifecycleLock{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		coord.chunkLifecycles[parentID] = entry
	}
	entry.refs++
	coord.chunkLifecycleMu.Unlock()

	select {
	case <-entry.token:
		var once sync.Once
		return func() {
			once.Do(func() {
				entry.token <- struct{}{}
				coord.releaseChunkLifecycleRef(parentID, entry)
			})
		}, nil
	case <-ctx.Done():
		coord.releaseChunkLifecycleRef(parentID, entry)
		return nil, ctx.Err()
	}
}

func (coord *collectionSchemaCoordinator) releaseChunkLifecycleRef(parentID string, entry *chunkLifecycleLock) {
	coord.chunkLifecycleMu.Lock()
	defer coord.chunkLifecycleMu.Unlock()
	entry.refs--
	if entry.refs == 0 {
		delete(coord.chunkLifecycles, parentID)
	}
}

func (c *Collection) collectionSchemaCoordinator() *collectionSchemaCoordinator {
	if c == nil {
		return nil
	}
	if c.writeDomain != nil && c.writeDomain.schemaCoordinator != nil {
		return c.writeDomain.schemaCoordinator
	}
	name := c.name
	if name == "" {
		name = c.meta.Name
	}
	return collectionSchemaCoordinatorForDBCollection(c.db, name)
}

func (c *Collection) lockCollectionSchemaRead() func() {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return func() {}
	}
	coord.schemaMu.RLock()
	return coord.schemaMu.RUnlock
}

func (c *Collection) lockCollectionSchemaWrite() func() {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return func() {}
	}
	coord.schemaMu.Lock()
	return coord.schemaMu.Unlock
}

func (c *Collection) lockLegacyVectorSidecar() func() {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return func() {}
	}
	runLegacyVectorSidecarBeforeLockHookForTest()
	coord.legacyVectorSidecarMu.Lock()
	return coord.legacyVectorSidecarMu.Unlock
}

func (c *Collection) flushCollectionWriteDomainsForSchemaMutation() error {
	if c == nil || c.db == nil {
		return nil
	}
	runCollectionSchemaMutationFlushHookForTest()
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return c.flushBufferedWrites()
	}
	for _, domain := range coord.snapshotDomains() {
		if domain == nil {
			continue
		}
		if err := flushCollectionWriteDomain(c.db, domain); err != nil {
			return err
		}
	}
	return nil
}
