package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	collectionMetaVersion        = 1
	maxCollectionMutationRetries = 64

	// DefaultIndexedWriteMemtableMaxDocuments bounds the native indexed
	// collection write-domain before it auto-flushes to persistent roots.
	DefaultIndexedWriteMemtableMaxDocuments = 64000
	// DefaultIndexedWriteMemtableDirectBatchDocuments keeps large, already
	// well-amortized InsertBatch calls on the immediate publish path. Smaller
	// batches use the indexed write-domain memtable path by default.
	DefaultIndexedWriteMemtableDirectBatchDocuments = DefaultIndexedWriteMemtableMaxDocuments / 4
)

var (
	ErrCollectionNotFound  = errors.New("collections: collection not found")
	ErrDocumentExists      = errors.New("collections: document already exists")
	ErrDuplicateDocumentID = errors.New("collections: duplicate document id in batch")
	ErrIndexNotFound       = errors.New("collections: index not found")
	ErrUniqueIndexConflict = errors.New("collections: unique index conflict")
	ErrConcurrentMutation  = errors.New("collections: concurrent mutation")

	errCollectionManagerNil = errors.New("collections: collection manager is nil")
	errCollectionNil        = errors.New("collections: collection is nil")
	errCollectionDBNil      = errors.New("collections: db is nil")
	errCollectionNotFound   = ErrCollectionNotFound
)

// UpdateBatchItem describes one document update in a batch. DocumentID must be
// non-empty and unique within the batch. Update receives the current stored
// document bytes and returns the replacement document bytes in the same format
// expected by Update. If Update returns changed=true, replacement must be a
// complete valid stored document for the collection format; returning
// replacement=nil, changed=false is the supported no-op form.
type UpdateBatchItem struct {
	DocumentID []byte
	Update     func(current []byte) (replacement []byte, changed bool, err error)
}

// UpdateBatchResult reports the outcome for one UpdateBatch item.
type UpdateBatchResult struct {
	Matched  bool
	Modified bool
}

func IsDuplicateKeyError(err error) bool {
	return errors.Is(err, ErrDocumentExists) ||
		errors.Is(err, ErrDuplicateDocumentID) ||
		errors.Is(err, ErrUniqueIndexConflict)
}

const (
	systemCollectionMetaPrefix = "collections/meta/"
	systemCollectionRootPrefix = "collections/root/"
)

func backendRootStoragePolicy(policy RootStoragePolicy) (backenddb.OrderedRootStoragePolicy, error) {
	switch policy {
	case RootStorageDefault:
		return backenddb.OrderedRootStorageDefault, nil
	case RootStorageFast:
		return backenddb.OrderedRootStoragePagerLeaves, nil
	case RootStorageCompressed:
		return backenddb.OrderedRootStorageValueLogLeaves, nil
	default:
		return backenddb.OrderedRootStorageDefault, fmt.Errorf("collections: unsupported root storage policy %q", policy)
	}
}

func collectionPlannerOptions(meta CollectionMeta) (collectionOptions, error) {
	documentFormat, err := normalizeDocumentFormat(meta.Options.DocumentFormat)
	if err != nil {
		return collectionOptions{}, err
	}
	dataPolicy, err := backendRootStoragePolicy(meta.Options.DataRootStoragePolicy)
	if err != nil {
		return collectionOptions{}, err
	}
	indexStatePolicy, err := backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy)
	if err != nil {
		return collectionOptions{}, err
	}
	return collectionOptions{
		allowArrayValuesInIndex: meta.Options.AllowArrayValuesInIndex,
		documentFormat:          documentFormat,
		dataStoragePolicy:       dataPolicy,
		indexStateStoragePolicy: indexStatePolicy,
	}, nil
}

func persistIndexStateForOptions(opts collectionOptions) bool {
	return persistIndexStateForDocumentFormat(opts.documentFormat)
}

func persistIndexStateForDocumentFormat(format DocumentFormat) bool {
	switch normalizedDocumentFormat(format) {
	case DocumentFormatTemplateV1, DocumentFormatBSON:
		return false
	default:
		return true
	}
}

type CollectionManager struct {
	db              *backenddb.DB
	closeUnregister func()
	domainMu        sync.RWMutex
	domains         map[string]*collectionWriteDomain
}

type Collection struct {
	db                *backenddb.DB
	writeDomain       *collectionWriteDomain
	meta              CollectionMeta
	catalogMu         sync.RWMutex
	catalogCommitSeq  uint64
	catalogSystemRoot uint64
	catalog           *collectionCatalog
	insertStatsMu     sync.RWMutex
	lastInsertStats   CollectionInsertStats
}

// StoredDocumentJSONMaterializer reuses any resources needed to materialize
// stored collection documents as JSON.
type StoredDocumentJSONMaterializer struct {
	documentFormat   DocumentFormat
	templateResolver templateV1Resolver
	closeFn          func() error
}

// Close releases resources held by the materializer.
func (m *StoredDocumentJSONMaterializer) Close() error {
	if m == nil || m.closeFn == nil {
		return nil
	}
	closeFn := m.closeFn
	m.closeFn = nil
	return closeFn()
}

// DocumentFormat returns the collection storage format this materializer was
// created for.
func (m *StoredDocumentJSONMaterializer) DocumentFormat() DocumentFormat {
	if m == nil {
		return DocumentFormatDefault
	}
	return m.documentFormat
}

// StoredDocumentJSON materializes one stored collection document as JSON bytes.
func (m *StoredDocumentJSONMaterializer) StoredDocumentJSON(document []byte) ([]byte, error) {
	if m == nil {
		return nil, errCollectionNil
	}
	switch m.documentFormat {
	case DocumentFormatJSON:
		return bytes.Clone(document), nil
	case DocumentFormatBSON:
		raw := bson.Raw(document)
		if err := raw.Validate(); err != nil {
			return nil, fmt.Errorf("collections: BSON stored document: %w", err)
		}
		return bson.MarshalExtJSON(raw, true, false)
	case DocumentFormatTemplateV1:
		return templateV1StoredDocumentJSON(document, m.templateResolver)
	default:
		return nil, fmt.Errorf("collections: unsupported document format %q", m.documentFormat)
	}
}

// CollectionInsertStats captures phase timings and counters from the most
// recent successful InsertBatch call on a Collection handle.
type CollectionInsertStats struct {
	Documents                    int
	Indexes                      int
	Runs                         int
	BufferedIndexedBatches       int
	BufferedIndexedBypassBatches int
	PrepareDocuments             time.Duration
	IndexStateExtraction         time.Duration
	// DuplicateDocumentPreflight includes duplicate-ID detection and
	// existing-document conflict checks.
	DuplicateDocumentPreflight time.Duration
	UniqueIndexPreflight       time.Duration
	TemplateRunBuild           time.Duration
	PrimaryRunBuild            time.Duration
	IndexStateRunBuild         time.Duration
	SecondaryRunBuild          time.Duration
	Publish                    time.Duration
	SecondaryEntries           int
	SecondaryKeyBytes          int
	SecondarySortedRuns        int
	SecondaryUnsortedRuns      int
	SecondaryRuns              []CollectionSecondaryRunStats
}

// CollectionSecondaryRunStats captures per-secondary-index run construction
// counters from an InsertBatch call.
type CollectionSecondaryRunStats struct {
	IndexName     string
	Entries       int
	KeyBytes      int
	AlreadySorted bool
	Build         time.Duration
}

// DocumentRecord is one primary collection record returned by ScanDocuments.
// ID and Document are cloned byte slices owned by the caller.
type DocumentRecord struct {
	ID       []byte
	Document []byte
}

type RootStoragePolicy string

const (
	RootStorageDefault    RootStoragePolicy = ""
	RootStorageFast       RootStoragePolicy = "fast"
	RootStorageCompressed RootStoragePolicy = "compressed"
)

type DocumentFormat string

const (
	DocumentFormatDefault    DocumentFormat = ""
	DocumentFormatJSON       DocumentFormat = "json"
	DocumentFormatBSON       DocumentFormat = "bson"
	DocumentFormatTemplateV1 DocumentFormat = "template-v1"
)

type CollectionOptions struct {
	AllowArrayValuesInIndex bool              `json:"allow_array_values_in_index,omitempty"`
	DocumentFormat          DocumentFormat    `json:"document_format,omitempty"`
	DataRootStoragePolicy   RootStoragePolicy `json:"data_root_storage_policy,omitempty"`
	IndexStateStoragePolicy RootStoragePolicy `json:"index_state_storage_policy,omitempty"`
	// DisableIndexedWriteMemtables opts an indexed collection out of the native
	// write-domain memtable path. It is intended for debugging and baseline
	// comparisons; indexed collections use memtables by default.
	DisableIndexedWriteMemtables bool `json:"disable_indexed_write_memtables,omitempty"`
	// BufferedIndexedWrites is normalized metadata describing whether indexed
	// InsertBatch root deltas are staged in the collection write domain before
	// Flush/Close or auto-flush. Staged writes are visible to primary and
	// secondary reads on the same manager, but durability remains at the flush
	// boundary, matching the existing no-index buffered path.
	BufferedIndexedWrites bool `json:"buffered_indexed_writes,omitempty"`
	// BufferedIndexedWriteMaxDocuments flushes indexed write buffers once this
	// many staged documents are pending. Zero uses the native default for indexed
	// memtables unless DisableIndexedWriteMemtables is set or the schema has no
	// indexes.
	BufferedIndexedWriteMaxDocuments int `json:"buffered_indexed_write_max_documents,omitempty"`
	// BufferedIndexedWriteMaxBytes flushes indexed write buffers once the staged
	// root-run payload estimate reaches this many bytes. Zero leaves flushing to
	// explicit Flush/Close calls.
	BufferedIndexedWriteMaxBytes int64 `json:"buffered_indexed_write_max_bytes,omitempty"`
}

type IndexDefinition struct {
	Name          string            `json:"name"`
	Field         string            `json:"field"`
	Unique        bool              `json:"unique,omitempty"`
	MultiKey      bool              `json:"multi_key,omitempty"`
	StoragePolicy RootStoragePolicy `json:"storage_policy,omitempty"`
}

type CollectionMeta struct {
	Name    string            `json:"name"`
	Options CollectionOptions `json:"options,omitempty"`
	Indexes []IndexDefinition `json:"indexes,omitempty"`
}

type collectionMetaDisk struct {
	Version int               `json:"version"`
	Name    string            `json:"name"`
	Options CollectionOptions `json:"options,omitempty"`
	Indexes []IndexDefinition `json:"indexes,omitempty"`
}

type collectionCatalog struct {
	// collectionCatalog is immutable once cached or published. Root updates must
	// create a replacement catalog via cloneCatalogWithRootUpdates.
	meta  CollectionMeta
	roots map[string]uint64
}

type createIndexBackfillPlan struct {
	rootNames   []string
	baseRootIDs map[string]uint64
	tables      []memtable.Table
	policies    []backenddb.OrderedRootStoragePolicy
}

type noIndexBatchEntry struct {
	id       []byte
	document []byte
}

type bufferedIndexedCheckpoint struct {
	count           int
	bufferedBytes   int64
	rootRuns        map[string][]memtable.Table
	rootPolicies    map[string]backenddb.OrderedRootStoragePolicy
	rootBaseIDs     map[string]uint64
	uniqueValueRuns map[string][]memtable.Table
}

type bufferedUniqueValueIndex struct {
	values     map[uint64][]byte
	collisions map[uint64][][]byte
	arenas     [][]byte
}

type collectionWriteDomain struct {
	// mutationMu serializes root descriptor publishes for handles opened
	// through the same manager so optimistic retries do not starve under
	// sustained collection write contention.
	mutationMu       sync.Mutex
	mu               sync.RWMutex
	loaded           bool
	meta             CollectionMeta
	catalog          *collectionCatalog
	baseCommitSeq    uint64
	baseSystemRoot   uint64
	primaryRoot      uint64
	storagePolicy    backenddb.OrderedRootStoragePolicy
	table            memtable.Table
	rootRuns         map[string][]memtable.Table
	rootPolicies     map[string]backenddb.OrderedRootStoragePolicy
	rootBaseIDs      map[string]uint64
	primaryIDIndex   *bufferedUniqueValueIndex
	uniqueValueRuns  map[string][]memtable.Table
	uniqueValueIndex map[string]*bufferedUniqueValueIndex
	count            int
	bufferedBytes    int64
}

func NewCollectionManager(database *backenddb.DB) *CollectionManager {
	manager := &CollectionManager{db: database}
	if database != nil {
		manager.closeUnregister = database.RegisterCloseHook(manager.FlushAll)
	}
	return manager
}

// LastInsertStats returns phase timings and counters from the most recent
// successful InsertBatch call on this Collection handle.
func (c *Collection) LastInsertStats() CollectionInsertStats {
	if c == nil {
		return CollectionInsertStats{}
	}
	c.insertStatsMu.RLock()
	defer c.insertStatsMu.RUnlock()
	return cloneCollectionInsertStats(c.lastInsertStats)
}

func (c *Collection) setLastInsertStats(stats CollectionInsertStats) {
	if c == nil {
		return
	}
	c.insertStatsMu.Lock()
	c.lastInsertStats = cloneCollectionInsertStats(stats)
	c.insertStatsMu.Unlock()
}

func cloneCollectionInsertStats(stats CollectionInsertStats) CollectionInsertStats {
	if len(stats.SecondaryRuns) > 0 {
		stats.SecondaryRuns = append([]CollectionSecondaryRunStats(nil), stats.SecondaryRuns...)
	}
	return stats
}

func (m *CollectionManager) writeDomainForCollection(name string) *collectionWriteDomain {
	if m == nil {
		return nil
	}
	m.domainMu.Lock()
	defer m.domainMu.Unlock()
	if m.domains == nil {
		m.domains = make(map[string]*collectionWriteDomain)
	}
	if domain := m.domains[name]; domain != nil {
		return domain
	}
	domain := &collectionWriteDomain{}
	m.domains[name] = domain
	return domain
}

func (m *CollectionManager) existingWriteDomainForCollection(name string) *collectionWriteDomain {
	if m == nil {
		return nil
	}
	m.domainMu.RLock()
	defer m.domainMu.RUnlock()
	if m.domains == nil {
		return nil
	}
	return m.domains[name]
}

// FlushAll publishes buffered writes for every collection opened through this
// manager. The backend DB also calls this as a close hook while write APIs are
// still available.
func (m *CollectionManager) FlushAll() error {
	if m == nil || m.db == nil {
		return nil
	}
	m.domainMu.RLock()
	domains := make([]*collectionWriteDomain, 0, len(m.domains))
	for _, domain := range m.domains {
		if domain != nil {
			domains = append(domains, domain)
		}
	}
	m.domainMu.RUnlock()

	var errs []error
	for _, domain := range domains {
		if err := flushCollectionWriteDomain(m.db, domain); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func flushCollectionWriteDomain(db *backenddb.DB, domain *collectionWriteDomain) error {
	if db == nil || domain == nil {
		return nil
	}
	collection := &Collection{db: db, writeDomain: domain}
	domain.mutationMu.Lock()
	defer domain.mutationMu.Unlock()
	domain.mu.Lock()
	defer domain.mu.Unlock()
	return collection.flushBufferedWritesLocked(domain)
}

func (c *Collection) lockMutation() func() {
	if c == nil || c.writeDomain == nil {
		return func() {}
	}
	c.writeDomain.mutationMu.Lock()
	return c.writeDomain.mutationMu.Unlock
}

func (m *CollectionManager) CreateCollection(meta *CollectionMeta) (*CollectionMeta, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, errCollectionDBNil
	}
	if meta == nil {
		return nil, errors.New("collections: nil collection metadata")
	}
	normalized, err := normalizeCollectionMeta(*meta)
	if err != nil {
		return nil, err
	}

	snap := m.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	existing, err := loadCollectionCatalog(snap, normalized.Name)
	_ = snap.Close()
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if !sameCollectionMeta(existing.meta, normalized) {
			return nil, fmt.Errorf("collections: existing schema for %q is incompatible", normalized.Name)
		}
		return existing.meta.copy(), nil
	}

	encoded, err := encodeCollectionMeta(normalized)
	if err != nil {
		return nil, err
	}
	_, _, err = m.db.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		current := m.db.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		existing, err := loadCollectionCatalog(current, normalized.Name)
		if err != nil {
			return nil, err
		}
		if existing != nil && !sameCollectionMeta(existing.meta, normalized) {
			return nil, fmt.Errorf("collections: existing schema for %q is incompatible", normalized.Name)
		}
		iter, err := buildSystemTargetIterator(current, map[string][]byte{
			systemCollectionMetaKey(normalized.Name): encoded,
		})
		if err != nil {
			return nil, err
		}
		return iter, nil
	})
	if err != nil {
		return nil, err
	}
	return normalized.copy(), nil
}

func (m *CollectionManager) OpenCollection(name string) (*Collection, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, errCollectionDBNil
	}
	if m.db.IsClosing() {
		return nil, backenddb.ErrClosed
	}
	if err := ValidateCollectionName(name); err != nil {
		return nil, err
	}
	if collection, ok := m.openCollectionFromWriteDomainCache(name); ok {
		if m.db.IsClosing() {
			return nil, backenddb.ErrClosed
		}
		return collection, nil
	}
	snap := m.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	collection := &Collection{
		db:          m.db,
		writeDomain: m.writeDomainForCollection(catalog.meta.Name),
		meta:        *catalog.meta.copy(),
	}
	collection.rememberCatalog(snap, catalog)
	collection.noteWriteDomainCatalog(snapshotSystemRoot(snap), catalog)
	return collection, nil
}

func (m *CollectionManager) openCollectionFromWriteDomainCache(name string) (*Collection, bool) {
	if m == nil || m.db == nil {
		return nil, false
	}
	state := m.db.State()
	if state == nil || state.SystemRootPageID == 0 {
		return nil, false
	}
	domain := m.existingWriteDomainForCollection(name)
	if domain == nil {
		return nil, false
	}
	catalog := cachedWriteDomainCatalogForState(domain, state.SystemRootPageID, state.CommitSeq)
	if catalog == nil {
		return nil, false
	}
	currentState := m.db.State()
	if currentState == nil ||
		currentState.SystemRootPageID != state.SystemRootPageID ||
		currentState.CommitSeq != state.CommitSeq {
		return nil, false
	}
	collection := &Collection{
		db:          m.db,
		writeDomain: domain,
		meta:        *catalog.meta.copy(),
	}
	collection.rememberCatalogAtSystemRoot(state.SystemRootPageID, catalog)
	return collection, true
}

func (m *CollectionManager) ListCollections() ([]CollectionMeta, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, errCollectionDBNil
	}
	snap := m.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	if snap.State() == nil || snap.State().SystemRootPageID == 0 {
		return nil, nil
	}
	prefix := []byte(systemCollectionMetaPrefix)
	it, err := snap.IteratorAtRoot(snap.State().SystemRootPageID, prefix, prefixEnd(prefix))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	var out []CollectionMeta
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			meta, err := decodeCollectionMeta(it.ValueCopy(nil))
			if err != nil {
				return nil, err
			}
			out = append(out, meta)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (c *Collection) Name() string {
	if c == nil {
		return ""
	}
	return c.meta.Name
}

func (c *Collection) Meta() CollectionMeta {
	if c == nil {
		return CollectionMeta{}
	}
	return *c.meta.copy()
}

func (c *Collection) CreateIndex(def IndexDefinition) (*CollectionMeta, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation()
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}

	baseMeta := catalog.meta
	c.meta = baseMeta
	baseOptions, err := collectionPlannerOptions(baseMeta)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	baseOptions = collectionOptionsWithTemplateV1Resolver(baseOptions, snap, catalog)
	newMeta, normalizedDef, err := addIndexToCollectionMeta(baseMeta, def)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	newRuntime, err := singleIndexRuntime(normalizedDef)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	existingRuntimes, err := (insertBatchPlanner{
		collection: baseMeta.Name,
		indexes:    plannerIndexes(baseMeta.Indexes),
	}).indexRuntimes()
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	plan, err := buildCreateIndexBackfillPlan(snap, catalog, newRuntime, existingRuntimes, baseOptions)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(plan.rootNames))
	iterators := make([]iterator.UnsafeIterator, 0, len(plan.rootNames))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionTables(plan.tables)
	}()
	for i, rootName := range plan.rootNames {
		iter := plan.tables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      plan.baseRootIDs[rootName],
			Iter:          iter,
			StoragePolicy: plan.policies[i],
		})
	}

	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaAndRootDescriptorSystemIterator(baseMeta, newMeta, plan.rootNames, plan.baseRootIDs, rootIDs)
	})
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(plan.rootNames) {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	c.meta = newMeta
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, plan.rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return newMeta.copy(), nil
}

func (c *Collection) DropIndex(name string) (*CollectionMeta, error) {
	if err := ValidateIndexName(name); err != nil {
		return nil, err
	}
	return c.dropIndexes(map[string]struct{}{name: {}}, false)
}

func (c *Collection) DropIndexes(names []string) (*CollectionMeta, error) {
	if len(names) == 0 {
		return nil, ErrIndexNotFound
	}
	nameSet := make(map[string]struct{}, len(names))
	for _, name := range names {
		if err := ValidateIndexName(name); err != nil {
			return nil, err
		}
		nameSet[name] = struct{}{}
	}
	return c.dropIndexes(nameSet, false)
}

func (c *Collection) DropAllIndexes() (*CollectionMeta, error) {
	return c.dropIndexes(nil, true)
}

func (c *Collection) dropIndexes(names map[string]struct{}, all bool) (*CollectionMeta, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation()
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}
	baseMeta := catalog.meta
	c.meta = baseMeta
	baseSystemRoot := snapshotSystemRoot(snap)
	_ = snap.Close()

	nextIndexes := make([]IndexDefinition, 0, len(baseMeta.Indexes))
	clearedRootNames := make([]string, 0, len(baseMeta.Indexes)+1)
	dropped := 0
	for _, idx := range baseMeta.Indexes {
		if all {
			dropped++
			clearedRootNames = append(clearedRootNames, collectionSecondaryRootName(baseMeta.Name, idx.Name))
			continue
		}
		if _, ok := names[idx.Name]; ok {
			dropped++
			clearedRootNames = append(clearedRootNames, collectionSecondaryRootName(baseMeta.Name, idx.Name))
			continue
		}
		nextIndexes = append(nextIndexes, idx)
	}
	if !all && dropped != len(names) {
		return nil, ErrIndexNotFound
	}
	if dropped == 0 {
		c.meta = baseMeta
		c.rememberCatalogAtSystemRoot(baseSystemRoot, catalog)
		return baseMeta.copy(), nil
	}

	newMeta, err := normalizeCollectionMeta(CollectionMeta{
		Name:    baseMeta.Name,
		Options: baseMeta.Options,
		Indexes: nextIndexes,
	})
	if err != nil {
		return nil, err
	}
	if len(newMeta.Indexes) == 0 && persistIndexStateForDocumentFormat(baseMeta.Options.DocumentFormat) {
		clearedRootNames = append(clearedRootNames, collectionIndexStateRootName(baseMeta.Name))
	}
	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		return nil, err
	}
	newSystemRoot, _, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaOnlySystemDeltaIterator(baseMeta, encodedMeta, clearedRootNames)
	})
	if err != nil {
		return nil, err
	}
	c.meta = newMeta
	clearedRootIDs := make([]uint64, len(clearedRootNames))
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, clearedRootNames, clearedRootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return newMeta.copy(), nil
}

func (c *Collection) Insert(id, document []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if len(c.meta.Indexes) == 0 {
		return c.insertOneNoIndexBuffered(id, document)
	}
	ids, err := c.InsertBatch([][]byte{id}, [][]byte{document})
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, errors.New("collections: insert returned no document id")
	}
	return ids[0], nil
}

// Flush publishes buffered collection-local writes to the backend roots. Single
// no-index inserts use this boundary to match TreeDB's cached write path while
// still giving callers an explicit durability/visibility point.
func (c *Collection) Flush() error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionDBNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation()
	return c.flushBufferedWrites()
}

func (c *Collection) insertOneNoIndexBuffered(id, document []byte) ([]byte, error) {
	if len(id) == 0 {
		return nil, errors.New("collections: document id cannot be empty")
	}
	domain := c.writeDomain
	if domain == nil {
		return c.insertOneNoIndex(id, document)
	}

	domain.mu.Lock()
	catalog, plannerOptions, indexed, err := c.ensureWriteDomainLocked(domain)
	if err != nil {
		domain.mu.Unlock()
		return nil, err
	}
	if indexed || plannerOptions.documentFormat != DocumentFormatJSON {
		domain.mu.Unlock()
		return c.insertOneViaBatch(id, document)
	}
	if catalog == nil {
		domain.mu.Unlock()
		return nil, errCollectionNotFound
	}
	c.meta = catalog.meta
	if domain.table == nil {
		domain.table = newCollectionRunTable(0)
	}
	if _, _, flags, found := domain.table.GetEntry(id); found && flags&node.FlagTombstone == 0 {
		domain.mu.Unlock()
		return nil, ErrDocumentExists
	}
	if domain.primaryRoot != 0 {
		exists, err := c.persistedDocumentExists(domain.primaryRoot, id)
		if err != nil {
			domain.mu.Unlock()
			return nil, err
		}
		if exists {
			domain.mu.Unlock()
			return nil, ErrDocumentExists
		}
	}
	domain.storagePolicy = plannerOptions.dataStoragePolicy
	domain.table.SetEntry(id, document, page.ValuePtr{}, node.FlagInline)
	domain.count++
	resultID := bytes.Clone(id)
	domain.mu.Unlock()
	return resultID, nil
}

func (c *Collection) ensureWriteDomainLocked(domain *collectionWriteDomain) (*collectionCatalog, collectionOptions, bool, error) {
	if domain == nil {
		return nil, collectionOptions{}, false, errors.New("collections: missing write domain")
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	if domain.loaded && domain.count > 0 {
		catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		options, err := collectionPlannerOptions(catalog.meta)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		return catalog, options, len(catalog.meta.Indexes) > 0, nil
	}
	if domain.loaded && domain.count == 0 && domain.baseSystemRoot == currentSystemRoot && domain.baseCommitSeq == currentCommitSeq {
		options, err := collectionPlannerOptions(domain.meta)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		return domain.catalog, options, len(domain.meta.Indexes) > 0, nil
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, collectionOptions{}, false, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return nil, collectionOptions{}, false, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, collectionOptions{}, false, errCollectionNotFound
	}
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	_ = snap.Close()

	options, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		return nil, collectionOptions{}, false, err
	}
	rootName := collectionPrimaryRootName(catalog.meta.Name)
	domain.loaded = true
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.baseCommitSeq = baseCommitSeq
	domain.baseSystemRoot = baseSystemRoot
	domain.primaryRoot = catalog.rootID(rootName)
	domain.storagePolicy = options.dataStoragePolicy
	return catalog, options, len(catalog.meta.Indexes) > 0, nil
}

func (c *Collection) revalidateBufferedWriteDomainLocked(domain *collectionWriteDomain, currentCommitSeq, currentSystemRoot uint64) (*collectionCatalog, error) {
	if c == nil || c.db == nil {
		return nil, errCollectionDBNil
	}
	if domain == nil {
		return nil, errors.New("collections: missing write domain")
	}
	if domain.catalog == nil {
		return nil, errCollectionNotFound
	}
	if domain.baseSystemRoot == currentSystemRoot && domain.baseCommitSeq == currentCommitSeq {
		return domain.catalog, nil
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, domain.meta.Name)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	_ = snap.Close()
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	if !sameCollectionMeta(catalog.meta, domain.meta) {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", domain.meta.Name)
	}

	primaryRootName := collectionPrimaryRootName(domain.meta.Name)
	if len(domain.rootBaseIDs) > 0 {
		for rootName, baseRootID := range domain.rootBaseIDs {
			if rootID := catalog.rootID(rootName); rootID != baseRootID {
				return nil, fmt.Errorf("collections: concurrent root modification detected for %q", domain.meta.Name)
			}
		}
	} else {
		if rootID := catalog.rootID(primaryRootName); rootID != domain.primaryRoot {
			return nil, fmt.Errorf("collections: concurrent root modification detected for %q", domain.meta.Name)
		}
	}
	options, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		return nil, err
	}
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.baseCommitSeq = baseCommitSeq
	domain.baseSystemRoot = baseSystemRoot
	domain.primaryRoot = catalog.rootID(primaryRootName)
	domain.storagePolicy = options.dataStoragePolicy
	c.meta = catalog.meta
	c.rememberCatalogAtSystemRoot(baseSystemRoot, catalog)
	return catalog, nil
}

func (c *Collection) persistedDocumentExists(rootID uint64, id []byte) (bool, error) {
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	if _, err := snap.GetEntryAtRoot(rootID, id); err == nil {
		return true, nil
	} else if !errors.Is(err, tree.ErrKeyNotFound) {
		return false, err
	}
	return false, nil
}

func (c *Collection) flushBufferedNoIndex() error {
	domain := c.writeDomain
	if domain == nil {
		return nil
	}
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if len(domain.rootRuns) > 0 {
		return nil
	}
	return c.flushBufferedNoIndexLocked(domain)
}

func (c *Collection) flushBufferedWrites() error {
	domain := c.writeDomain
	if domain == nil {
		return nil
	}
	domain.mu.Lock()
	defer domain.mu.Unlock()
	return c.flushBufferedWritesLocked(domain)
}

func (c *Collection) flushBufferedWritesLocked(domain *collectionWriteDomain) error {
	if domain == nil || domain.count == 0 {
		return nil
	}
	if len(domain.rootRuns) > 0 {
		return c.flushBufferedIndexedLocked(domain)
	}
	return c.flushBufferedNoIndexLocked(domain)
}

func (c *Collection) flushBufferedNoIndexLocked(domain *collectionWriteDomain) error {
	if domain == nil || domain.count == 0 || domain.table == nil {
		return nil
	}
	if domain.catalog == nil {
		return errCollectionNotFound
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
	if err != nil {
		return err
	}
	meta := catalog.meta
	if len(meta.Indexes) > 0 {
		return errors.New("collections: buffered no-index writes cannot be flushed into indexed schema")
	}
	c.meta = meta
	rootName := collectionPrimaryRootName(meta.Name)
	baseRoot := domain.primaryRoot
	pin := c.db.AcquireSnapshot()
	if pin == nil {
		return backenddb.ErrClosed
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = pin.Close() }()
	pinnedCatalog, err := loadCollectionCatalog(pin, meta.Name)
	if err != nil {
		return err
	}
	if pinnedCatalog == nil {
		return errCollectionNotFound
	}
	if !sameCollectionMeta(pinnedCatalog.meta, meta) {
		return fmt.Errorf("collections: concurrent schema modification detected for %q", meta.Name)
	}
	if got := pinnedCatalog.rootID(rootName); got != baseRoot {
		return fmt.Errorf("collections: concurrent root modification detected for %q", meta.Name)
	}
	baseSystemRoot := snapshotSystemRoot(pin)
	baseCommitSeq := snapshotCommitSeq(pin)
	baseRootIDs := map[string]uint64{rootName: baseRoot}
	table := domain.table
	iter := table.NewIterator(nil, nil)

	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: domain.storagePolicy,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, []string{rootName}, baseRootIDs, rootIDs)
	})
	_ = iter.Close()
	if err != nil {
		return err
	}
	if len(rootIDs) != 1 {
		return errors.New("collections: ordered root publish returned unexpected root count")
	}
	nextCatalog := cloneCatalogWithRootUpdates(domain.catalog, meta, []string{rootName}, rootIDs)
	domain.loaded = true
	domain.meta = meta
	domain.catalog = nextCatalog
	domain.baseCommitSeq = c.commitSeqForSystemRoot(newSystemRoot)
	domain.baseSystemRoot = newSystemRoot
	domain.primaryRoot = rootIDs[0]
	domain.table = newCollectionRunTable(0)
	domain.count = 0
	c.meta = meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	resetCollectionRunTable(table)
	return nil
}

func (c *Collection) shouldBufferIndexedInserts(meta CollectionMeta) bool {
	return c != nil && c.writeDomain != nil && meta.Options.BufferedIndexedWrites && len(meta.Indexes) > 0
}

func (c *Collection) shouldBufferIndexedInsertBatch(meta CollectionMeta, documentCount int) bool {
	if !c.shouldBufferIndexedInserts(meta) {
		return false
	}
	if documentCount >= DefaultIndexedWriteMemtableDirectBatchDocuments &&
		meta.Options.BufferedIndexedWriteMaxDocuments == DefaultIndexedWriteMemtableMaxDocuments {
		return false
	}
	return true
}

func (c *Collection) bufferIndexedInsertPlanLocked(catalog *collectionCatalog, baseCommitSeq, baseSystemRoot uint64, plan *insertBatchPlan) (time.Duration, error) {
	domain := c.writeDomain
	if domain == nil {
		return 0, errors.New("collections: missing write domain")
	}
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if catalog == nil {
		return 0, errCollectionNotFound
	}
	if len(catalog.meta.Indexes) == 0 {
		return 0, errors.New("collections: indexed write buffer requires an indexed schema")
	}
	if domain.count > 0 {
		currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
		currentCatalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
		if err != nil {
			return 0, err
		}
		if !sameCollectionMeta(currentCatalog.meta, catalog.meta) {
			return 0, fmt.Errorf("collections: concurrent schema modification detected for %q", catalog.meta.Name)
		}
		for rootName, baseRoot := range domain.rootBaseIDs {
			if got := currentCatalog.rootID(rootName); got != baseRoot {
				return 0, fmt.Errorf("%w: concurrent root modification detected for %q", ErrConcurrentMutation, rootName)
			}
		}
		catalog = currentCatalog
	} else {
		c.initializeWriteDomainFromCatalogLocked(domain, catalog, baseCommitSeq, baseSystemRoot)
	}

	if err := c.rejectBufferedIndexedInsertConflictsLocked(domain, catalog.meta, plan); err != nil {
		return 0, err
	}
	if domain.rootPolicies == nil {
		domain.rootPolicies = make(map[string]backenddb.OrderedRootStoragePolicy, len(plan.runs))
	}
	if domain.rootBaseIDs == nil {
		domain.rootBaseIDs = make(map[string]uint64, len(plan.runs))
	}
	if domain.rootRuns == nil {
		domain.rootRuns = make(map[string][]memtable.Table, len(plan.runs))
	}
	if domain.uniqueValueRuns == nil {
		domain.uniqueValueRuns = make(map[string][]memtable.Table)
	}
	if domain.uniqueValueIndex == nil {
		domain.uniqueValueIndex = make(map[string]*bufferedUniqueValueIndex)
	}
	autoFlushEnabled := bufferedIndexedAutoFlushEnabled(catalog.meta.Options)
	var checkpoint bufferedIndexedCheckpoint
	if autoFlushEnabled {
		checkpoint = checkpointBufferedIndexedDomain(domain)
	}
	uniqueIndexes := uniqueCollectionIndexNames(catalog.meta)
	var stagedBytes int64
	for _, run := range plan.runs {
		var uniqueValueTable memtable.Table
		var uniquePrefixes [][]byte
		if _, ok := uniqueIndexes[run.indexName]; ok && run.kind == collectionRootSecondary {
			var err error
			uniqueValueTable, uniquePrefixes, err = bufferedUniqueIndexValueRun(run.table)
			if err != nil {
				if autoFlushEnabled {
					rollbackBufferedIndexedDomain(domain, checkpoint)
				}
				return 0, err
			}
		}
		if len(domain.rootRuns[run.name]) == 0 {
			domain.rootBaseIDs[run.name] = catalog.rootID(run.name)
		}
		domain.rootPolicies[run.name] = run.storagePolicy
		domain.rootRuns[run.name] = append(domain.rootRuns[run.name], run.table)
		stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, run.table.Size())
		if run.kind == collectionRootPrimary {
			if domain.primaryIDIndex == nil {
				domain.primaryIDIndex = newBufferedUniqueValueIndex(max(1, run.table.Len()))
			}
			if err := addBufferedPrimaryIDs(domain.primaryIDIndex, run.table); err != nil {
				if autoFlushEnabled {
					rollbackBufferedIndexedDomain(domain, checkpoint)
				}
				return 0, err
			}
		}
		if uniqueValueTable != nil {
			domain.uniqueValueRuns[run.indexName] = append(domain.uniqueValueRuns[run.indexName], uniqueValueTable)
			index := domain.uniqueValueIndex[run.indexName]
			if index == nil {
				index = newBufferedUniqueValueIndex(max(1, len(uniquePrefixes)))
				domain.uniqueValueIndex[run.indexName] = index
			}
			index.addAll(uniquePrefixes)
			stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, uniqueValueTable.Size())
		}
	}
	domain.loaded = true
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.primaryRoot = catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	domain.count += len(plan.resultIDs)
	domain.bufferedBytes = saturatingAddNonNegativeInt64(domain.bufferedBytes, stagedBytes)
	c.meta = catalog.meta
	if shouldFlushBufferedIndexedWrites(domain, catalog.meta.Options) {
		flushStart := time.Now()
		if err := c.flushBufferedIndexedLocked(domain); err != nil {
			rollbackBufferedIndexedDomain(domain, checkpoint)
			return 0, err
		}
		return time.Since(flushStart), nil
	}
	return 0, nil
}

func (c *Collection) initializeWriteDomainFromCatalogLocked(domain *collectionWriteDomain, catalog *collectionCatalog, baseCommitSeq, baseSystemRoot uint64) {
	domain.loaded = true
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.baseCommitSeq = baseCommitSeq
	domain.baseSystemRoot = baseSystemRoot
	domain.primaryRoot = catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	domain.rootRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.primaryIDIndex = nil
	domain.uniqueValueRuns = nil
	domain.uniqueValueIndex = nil
	domain.bufferedBytes = 0
}

func shouldFlushBufferedIndexedWrites(domain *collectionWriteDomain, opts CollectionOptions) bool {
	if domain == nil || domain.count == 0 {
		return false
	}
	if opts.BufferedIndexedWriteMaxDocuments > 0 && domain.count >= opts.BufferedIndexedWriteMaxDocuments {
		return true
	}
	if opts.BufferedIndexedWriteMaxBytes > 0 && domain.bufferedBytes >= opts.BufferedIndexedWriteMaxBytes {
		return true
	}
	return false
}

func bufferedIndexedAutoFlushEnabled(opts CollectionOptions) bool {
	return opts.BufferedIndexedWriteMaxDocuments > 0 || opts.BufferedIndexedWriteMaxBytes > 0
}

func saturatingAddNonNegativeInt64(total, n int64) int64 {
	if n <= 0 {
		return total
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if total > maxInt64-n {
		return maxInt64
	}
	return total + n
}

func checkpointBufferedIndexedDomain(domain *collectionWriteDomain) bufferedIndexedCheckpoint {
	if domain == nil {
		return bufferedIndexedCheckpoint{}
	}
	return bufferedIndexedCheckpoint{
		count:           domain.count,
		bufferedBytes:   domain.bufferedBytes,
		rootRuns:        cloneTableRunMap(domain.rootRuns),
		rootPolicies:    cloneRootPolicyMap(domain.rootPolicies),
		rootBaseIDs:     cloneUint64Map(domain.rootBaseIDs),
		uniqueValueRuns: cloneTableRunMap(domain.uniqueValueRuns),
	}
}

func rollbackBufferedIndexedDomain(domain *collectionWriteDomain, checkpoint bufferedIndexedCheckpoint) {
	if domain == nil {
		return
	}
	resetTableRunsAddedAfterCheckpoint(domain.rootRuns, checkpoint.rootRuns)
	resetTableRunsAddedAfterCheckpoint(domain.uniqueValueRuns, checkpoint.uniqueValueRuns)
	domain.count = checkpoint.count
	domain.bufferedBytes = checkpoint.bufferedBytes
	domain.rootRuns = checkpoint.rootRuns
	domain.rootPolicies = checkpoint.rootPolicies
	domain.rootBaseIDs = checkpoint.rootBaseIDs
	domain.primaryIDIndex = rebuildBufferedPrimaryIDIndex(domain.meta.Name, checkpoint.rootRuns)
	domain.uniqueValueRuns = checkpoint.uniqueValueRuns
	domain.uniqueValueIndex = rebuildBufferedUniqueValueIndexes(checkpoint.uniqueValueRuns)
}

func resetTableRunsAddedAfterCheckpoint(current, checkpoint map[string][]memtable.Table) {
	for name, runs := range current {
		keep := 0
		if checkpointRuns, ok := checkpoint[name]; ok {
			keep = len(checkpointRuns)
		}
		for _, table := range runs[keep:] {
			resetCollectionRunTable(table)
		}
	}
}

func cloneTableRunMap(in map[string][]memtable.Table) map[string][]memtable.Table {
	if in == nil {
		return nil
	}
	out := make(map[string][]memtable.Table, len(in))
	for name, runs := range in {
		out[name] = runs
	}
	return out
}

func cloneRootPolicyMap(in map[string]backenddb.OrderedRootStoragePolicy) map[string]backenddb.OrderedRootStoragePolicy {
	if in == nil {
		return nil
	}
	out := make(map[string]backenddb.OrderedRootStoragePolicy, len(in))
	for name, policy := range in {
		out[name] = policy
	}
	return out
}

func cloneUint64Map(in map[string]uint64) map[string]uint64 {
	if in == nil {
		return nil
	}
	out := make(map[string]uint64, len(in))
	for name, value := range in {
		out[name] = value
	}
	return out
}

func newBufferedUniqueValueIndex(capacity int) *bufferedUniqueValueIndex {
	if capacity < 0 {
		capacity = 0
	}
	return &bufferedUniqueValueIndex{values: make(map[uint64][]byte, capacity)}
}

func (idx *bufferedUniqueValueIndex) len() int {
	if idx == nil {
		return 0
	}
	total := len(idx.values)
	for _, collisions := range idx.collisions {
		total += len(collisions)
	}
	return total
}

func (idx *bufferedUniqueValueIndex) addAll(prefixes [][]byte) {
	if idx == nil {
		return
	}
	if idx.values == nil {
		idx.values = make(map[uint64][]byte, len(prefixes))
	}
	for _, prefix := range prefixes {
		idx.add(prefix)
	}
}

func (idx *bufferedUniqueValueIndex) add(prefix []byte) {
	if idx == nil {
		return
	}
	hash := xxhash.Sum64(prefix)
	if existing, ok := idx.values[hash]; ok {
		if bytes.Equal(existing, prefix) {
			return
		}
		if idx.collisions == nil {
			idx.collisions = make(map[uint64][][]byte)
		}
		idx.collisions[hash] = append(idx.collisions[hash], prefix)
		return
	}
	idx.values[hash] = prefix
}

func (idx *bufferedUniqueValueIndex) contains(prefix []byte) bool {
	if idx == nil || len(idx.values) == 0 {
		return false
	}
	hash := xxhash.Sum64(prefix)
	if bytes.Equal(idx.values[hash], prefix) {
		return true
	}
	for _, candidate := range idx.collisions[hash] {
		if bytes.Equal(candidate, prefix) {
			return true
		}
	}
	return false
}

func rebuildBufferedUniqueValueIndexes(runs map[string][]memtable.Table) map[string]*bufferedUniqueValueIndex {
	if len(runs) == 0 {
		return nil
	}
	out := make(map[string]*bufferedUniqueValueIndex, len(runs))
	for indexName, tables := range runs {
		index := newBufferedUniqueValueIndex(0)
		for _, table := range tables {
			it := table.NewIterator(nil, nil)
			for it.Valid() {
				index.add(bytes.Clone(it.UnsafeKey()))
				it.Next()
			}
			_ = it.Close()
		}
		if index.len() > 0 {
			out[indexName] = index
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c *Collection) rejectBufferedIndexedInsertConflictsLocked(domain *collectionWriteDomain, meta CollectionMeta, plan *insertBatchPlan) error {
	if domain == nil || domain.count == 0 || len(domain.rootRuns) == 0 {
		return nil
	}
	primaryName := collectionPrimaryRootName(meta.Name)
	if pendingPrimary := domain.rootRuns[primaryName]; len(pendingPrimary) > 0 {
		for _, run := range plan.runs {
			if run.name != primaryName {
				continue
			}
			if err := rejectBufferedPrimaryConflicts(domain.primaryIDIndex, pendingPrimary, run.table); err != nil {
				return err
			}
			break
		}
	}
	uniqueIndexes := uniqueCollectionIndexNames(meta)
	for _, run := range plan.runs {
		if run.kind != collectionRootSecondary {
			continue
		}
		if _, ok := uniqueIndexes[run.indexName]; !ok {
			continue
		}
		pending := domain.uniqueValueIndex[run.indexName]
		if pending == nil || pending.len() == 0 {
			continue
		}
		if err := rejectBufferedUniqueIndexConflicts(run.indexName, pending, run.table); err != nil {
			return err
		}
	}
	return nil
}

func rejectBufferedPrimaryConflicts(pendingIndex *bufferedUniqueValueIndex, pendingPrimary []memtable.Table, batchPrimary memtable.Table) error {
	it := batchPrimary.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if pendingIndex != nil && pendingIndex.contains(it.UnsafeKey()) {
			return ErrDocumentExists
		}
		if pendingIndex == nil {
			if _, _, flags, found := getBufferedRunEntry(pendingPrimary, it.UnsafeKey()); found && flags&node.FlagTombstone == 0 {
				return ErrDocumentExists
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return nil
}

func addBufferedPrimaryIDs(index *bufferedUniqueValueIndex, batchPrimary memtable.Table) error {
	if index == nil || batchPrimary == nil {
		return nil
	}
	arena := make([]byte, 0, bufferedPrimaryIDArenaCap(batchPrimary.Len()))
	it := batchPrimary.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		key := it.UnsafeKey()
		start := len(arena)
		arena = append(arena, key...)
		index.add(arena[start:len(arena)])
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	if len(arena) > 0 {
		index.arenas = append(index.arenas, arena)
	}
	return nil
}

func bufferedPrimaryIDArenaCap(entries int) int {
	if entries <= 0 {
		return 0
	}
	const bytesPerKeyEstimate = 16
	maxInt := int(^uint(0) >> 1)
	if entries > maxInt/bytesPerKeyEstimate {
		return 0
	}
	return entries * bytesPerKeyEstimate
}

func rebuildBufferedPrimaryIDIndex(collectionName string, runs map[string][]memtable.Table) *bufferedUniqueValueIndex {
	if collectionName == "" || len(runs) == 0 {
		return nil
	}
	tables := runs[collectionPrimaryRootName(collectionName)]
	if len(tables) == 0 {
		return nil
	}
	index := newBufferedUniqueValueIndex(0)
	for _, table := range tables {
		if err := addBufferedPrimaryIDs(index, table); err != nil {
			return nil
		}
	}
	if index.len() == 0 {
		return nil
	}
	return index
}

func uniqueCollectionIndexNames(meta CollectionMeta) map[string]struct{} {
	uniqueIndexes := make(map[string]struct{}, len(meta.Indexes))
	for _, idx := range meta.Indexes {
		if idx.Unique {
			uniqueIndexes[idx.Name] = struct{}{}
		}
	}
	return uniqueIndexes
}

func rejectBufferedUniqueIndexConflicts(indexName string, pendingIndex *bufferedUniqueValueIndex, batchIndex memtable.Table) error {
	it := batchIndex.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		prefix, err := indexEntryValuePrefix(it.UnsafeKey())
		if err != nil {
			return err
		}
		if pendingIndex.contains(prefix) {
			return fmt.Errorf("%w %q", ErrUniqueIndexConflict, indexName)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return nil
}

func bufferedUniqueIndexValueRun(batchIndex memtable.Table) (memtable.Table, [][]byte, error) {
	table := newCollectionRunTable(max(0, batchIndex.Len()))
	maxInt := int(^uint(0) >> 1)
	arenaCap := batchIndex.Size()
	if arenaCap < 0 || arenaCap > int64(maxInt) {
		arenaCap = 0
	}
	arena := make([]byte, 0, int(arenaCap))
	prefixes := make([][]byte, 0, max(0, batchIndex.Len()))
	it := batchIndex.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		prefix, err := indexEntryValuePrefix(it.UnsafeKey())
		if err != nil {
			resetCollectionRunTable(table)
			return nil, nil, err
		}
		start := len(arena)
		arena = append(arena, prefix...)
		owned := arena[start:len(arena)]
		setCollectionRunValue(table, owned, nil)
		prefixes = append(prefixes, owned)
		it.Next()
	}
	if err := it.Error(); err != nil {
		resetCollectionRunTable(table)
		return nil, nil, err
	}
	table.Freeze()
	return table, prefixes, nil
}

func indexEntryValuePrefix(key []byte) ([]byte, error) {
	if len(key) < 2 {
		return nil, errors.New("collections: malformed index entry key")
	}
	n := int(binary.BigEndian.Uint16(key[:2]))
	if len(key) < 2+n {
		return nil, errors.New("collections: malformed index entry key")
	}
	return key[:2+n], nil
}

func getBufferedRunEntry(runs []memtable.Table, key []byte) ([]byte, page.ValuePtr, byte, bool) {
	for i := len(runs) - 1; i >= 0; i-- {
		if runs[i] == nil {
			continue
		}
		if value, ptr, flags, found := runs[i].GetEntry(key); found {
			return value, ptr, flags, true
		}
	}
	return nil, page.ValuePtr{}, 0, false
}

type bufferedRootRunHeapItem struct {
	idx      int
	priority int
	key      []byte
}

type bufferedRootRunHeap []bufferedRootRunHeapItem

func (h bufferedRootRunHeap) Len() int { return len(h) }

func (h bufferedRootRunHeap) Less(i, j int) bool {
	if cmp := bytes.Compare(h[i].key, h[j].key); cmp != 0 {
		return cmp < 0
	}
	return h[i].priority < h[j].priority
}

func (h bufferedRootRunHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *bufferedRootRunHeap) push(item bufferedRootRunHeapItem) {
	*h = append(*h, item)
	h.up(len(*h) - 1)
}

func (h *bufferedRootRunHeap) pop() bufferedRootRunHeapItem {
	old := *h
	n := len(old)
	if n == 0 {
		return bufferedRootRunHeapItem{}
	}
	old.Swap(0, n-1)
	h.down(0, n-1)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (h bufferedRootRunHeap) peek() *bufferedRootRunHeapItem {
	if len(h) == 0 {
		return nil
	}
	return &h[0]
}

func (h *bufferedRootRunHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *bufferedRootRunHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

type bufferedRootRunsIterator struct {
	iters    []iterator.UnsafeIterator
	heap     bufferedRootRunHeap
	cur      bufferedRootRunHeapItem
	hasCur   bool
	valid    bool
	start    []byte
	end      []byte
	closed   bool
	firstErr error
}

func newBufferedRootRunsIterator(runs []memtable.Table, start, end []byte) iterator.UnsafeIterator {
	if len(runs) == 1 {
		return runs[0].NewIterator(start, end)
	}
	it := &bufferedRootRunsIterator{
		iters: make([]iterator.UnsafeIterator, 0, len(runs)),
		start: start,
		end:   end,
	}
	for i, run := range runs {
		if run == nil {
			continue
		}
		runIter := run.NewIterator(start, end)
		idx := len(it.iters)
		it.iters = append(it.iters, runIter)
		if runIter.Valid() {
			it.heap.push(bufferedRootRunHeapItem{
				idx:      idx,
				priority: len(runs) - 1 - i,
				key:      runIter.UnsafeKey(),
			})
		}
	}
	it.advance()
	return it
}

func (it *bufferedRootRunsIterator) Valid() bool {
	return it != nil && it.valid
}

func (it *bufferedRootRunsIterator) Next() {
	if it == nil || !it.valid {
		return
	}
	if it.hasCur {
		it.advanceItem(it.cur)
		it.hasCur = false
	}
	it.advance()
}

func (it *bufferedRootRunsIterator) Seek(key []byte) {
	if it == nil || it.closed {
		return
	}
	if it.start != nil && bytes.Compare(key, it.start) < 0 {
		key = it.start
	}
	it.heap = it.heap[:0]
	it.valid = false
	it.hasCur = false
	for idx, source := range it.iters {
		source.Seek(key)
		if source.Valid() {
			it.heap.push(bufferedRootRunHeapItem{
				idx:      idx,
				priority: len(it.iters) - 1 - idx,
				key:      source.UnsafeKey(),
			})
		}
	}
	it.advance()
}

func (it *bufferedRootRunsIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.iters[it.cur.idx].UnsafeKey()
}

func (it *bufferedRootRunsIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.iters[it.cur.idx].UnsafeValue()
}

func (it *bufferedRootRunsIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline
	}
	return it.iters[it.cur.idx].UnsafeEntry()
}

func (it *bufferedRootRunsIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *bufferedRootRunsIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *bufferedRootRunsIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst[:0], it.UnsafeKey()...)
}

func (it *bufferedRootRunsIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst[:0], it.UnsafeValue()...)
}

func (it *bufferedRootRunsIterator) IsDeleted() bool {
	return it.Valid() && it.iters[it.cur.idx].IsDeleted()
}

func (it *bufferedRootRunsIterator) Error() error {
	if it == nil {
		return nil
	}
	if it.firstErr != nil {
		return it.firstErr
	}
	for _, source := range it.iters {
		if err := source.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (it *bufferedRootRunsIterator) Close() error {
	if it == nil || it.closed {
		return nil
	}
	it.closed = true
	var firstErr error
	for _, source := range it.iters {
		if err := source.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	it.valid = false
	it.hasCur = false
	it.heap = nil
	if firstErr != nil {
		it.firstErr = firstErr
	}
	return firstErr
}

func (it *bufferedRootRunsIterator) Domain() (start, end []byte) {
	if it == nil {
		return nil, nil
	}
	return it.start, it.end
}

func (it *bufferedRootRunsIterator) advance() {
	it.valid = false
	it.hasCur = false
	for it.heap.Len() > 0 {
		top := it.heap.pop()
		key := top.key
		if it.end != nil && bytes.Compare(key, it.end) >= 0 {
			return
		}
		for it.heap.Len() > 0 {
			next := it.heap.peek()
			if next == nil || !bytes.Equal(next.key, key) {
				break
			}
			shadowed := it.heap.pop()
			it.advanceItem(shadowed)
		}
		if it.iters[top.idx].IsDeleted() {
			it.advanceItem(top)
			continue
		}
		it.cur = top
		it.hasCur = true
		it.valid = true
		return
	}
}

func (it *bufferedRootRunsIterator) advanceItem(item bufferedRootRunHeapItem) {
	source := it.iters[item.idx]
	source.Next()
	if source.Valid() {
		item.key = source.UnsafeKey()
		it.heap.push(item)
	} else if err := source.Error(); err != nil && it.firstErr == nil {
		it.firstErr = err
	}
}

func (c *Collection) flushBufferedIndexedLocked(domain *collectionWriteDomain) error {
	if domain == nil || domain.count == 0 || len(domain.rootRuns) == 0 {
		return nil
	}
	if domain.catalog == nil {
		return errCollectionNotFound
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
	if err != nil {
		return err
	}
	meta := catalog.meta
	c.meta = meta
	pin := c.db.AcquireSnapshot()
	if pin == nil {
		return backenddb.ErrClosed
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = pin.Close() }()
	pinnedCatalog, err := loadCollectionCatalog(pin, meta.Name)
	if err != nil {
		return err
	}
	if pinnedCatalog == nil {
		return errCollectionNotFound
	}
	if !sameCollectionMeta(pinnedCatalog.meta, meta) {
		return fmt.Errorf("collections: concurrent schema modification detected for %q", meta.Name)
	}
	for rootName, baseRoot := range domain.rootBaseIDs {
		if got := pinnedCatalog.rootID(rootName); got != baseRoot {
			return fmt.Errorf("collections: concurrent root modification detected for %q", rootName)
		}
	}

	rootNames := orderedBufferedRootNames(meta, domain.rootRuns)
	if len(rootNames) == 0 {
		domain.count = 0
		domain.bufferedBytes = 0
		return nil
	}
	baseSystemRoot := snapshotSystemRoot(pin)
	baseCommitSeq := snapshotCommitSeq(pin)
	baseRootIDs := make(map[string]uint64, len(rootNames))
	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(rootNames))
	iterators := make([]iterator.UnsafeIterator, 0, len(rootNames))
	for _, rootName := range rootNames {
		iter := newBufferedRootRunsIterator(domain.rootRuns[rootName], nil, nil)
		iterators = append(iterators, iter)
		baseRoot := domain.rootBaseIDs[rootName]
		baseRootIDs[rootName] = baseRoot
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      baseRoot,
			Iter:          iter,
			StoragePolicy: domain.rootPolicies[rootName],
		})
	}
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	})
	for _, it := range iterators {
		_ = it.Close()
	}
	if err != nil {
		return err
	}
	if len(rootIDs) != len(rootNames) {
		return errors.New("collections: ordered root publish returned unexpected root count")
	}
	nextCatalog := cloneCatalogWithRootUpdates(domain.catalog, meta, rootNames, rootIDs)
	oldRuns := domain.rootRuns
	domain.loaded = true
	domain.meta = meta
	domain.catalog = nextCatalog
	domain.baseCommitSeq = c.commitSeqForSystemRoot(newSystemRoot)
	domain.baseSystemRoot = newSystemRoot
	domain.primaryRoot = nextCatalog.rootID(collectionPrimaryRootName(meta.Name))
	domain.rootRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.primaryIDIndex = nil
	oldUniqueValueRuns := domain.uniqueValueRuns
	domain.uniqueValueRuns = nil
	domain.uniqueValueIndex = nil
	domain.count = 0
	domain.bufferedBytes = 0
	c.meta = meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	for _, runs := range oldRuns {
		resetCollectionTables(runs)
	}
	for _, runs := range oldUniqueValueRuns {
		resetCollectionTables(runs)
	}
	return nil
}

func orderedBufferedRootNames(meta CollectionMeta, runs map[string][]memtable.Table) []string {
	if len(runs) == 0 {
		return nil
	}
	out := make([]string, 0, len(runs))
	seen := make(map[string]struct{}, len(runs))
	for _, rootName := range collectionRootNames(meta) {
		if len(runs[rootName]) > 0 {
			out = append(out, rootName)
			seen[rootName] = struct{}{}
		}
	}
	extra := make([]string, 0)
	for rootName, rootRuns := range runs {
		if _, ok := seen[rootName]; !ok {
			if len(rootRuns) > 0 {
				extra = append(extra, rootName)
			}
		}
	}
	sort.Strings(extra)
	out = append(out, extra...)
	return out
}

func (c *Collection) insertOneNoIndex(id, document []byte) ([]byte, error) {
	if len(id) == 0 {
		return nil, errors.New("collections: document id cannot be empty")
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}
	c.meta = catalog.meta
	if len(c.meta.Indexes) > 0 {
		_ = snap.Close()
		return c.insertOneViaBatch(id, document)
	}
	plannerOptions, err := collectionPlannerOptions(c.meta)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if plannerOptions.documentFormat != DocumentFormatJSON {
		_ = snap.Close()
		return c.insertOneViaBatch(id, document)
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	rootName := collectionPrimaryRootName(c.meta.Name)
	baseRoot := catalog.rootID(rootName)
	if baseRoot != 0 {
		if _, err := snap.GetEntryAtRoot(baseRoot, id); err == nil {
			_ = snap.Close()
			return nil, ErrDocumentExists
		} else if !errors.Is(err, tree.ErrKeyNotFound) {
			_ = snap.Close()
			return nil, err
		}
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	resultID := bytes.Clone(id)
	iter := &systemTargetIterator{entries: []systemTargetEntry{{
		key:   resultID,
		value: bytes.Clone(document),
	}}}
	defer func() { _ = iter.Close() }()

	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: plannerOptions.dataStoragePolicy,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, []string{rootName}, map[string]uint64{rootName: baseRoot}, rootIDs)
	})
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != 1 {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	c.rememberCatalogAtSystemRoot(newSystemRoot, cloneCatalogWithRootUpdates(catalog, c.meta, []string{rootName}, rootIDs))
	return resultID, nil
}

func (c *Collection) insertOneViaBatch(id, document []byte) ([]byte, error) {
	ids, err := c.InsertBatch([][]byte{id}, [][]byte{document})
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, errors.New("collections: insert returned no document id")
	}
	return ids[0], nil
}

func (c *Collection) InsertBatch(ids, documents [][]byte) ([][]byte, error) {
	return c.insertBatch(ids, documents, false)
}

// InsertBatchValidatedBSON inserts native BSON documents that the caller has
// already validated. It is intended for wire-protocol gateways that validate
// BSON while parsing the request and need to avoid a duplicate full-document
// validation pass on the insert hot path.
func (c *Collection) InsertBatchValidatedBSON(ids, documents [][]byte) ([][]byte, error) {
	return c.insertBatch(ids, documents, true)
}

func (c *Collection) insertBatch(ids, documents [][]byte, trustedValidBSON bool) ([][]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation()
	if len(documents) == 0 {
		c.setLastInsertStats(CollectionInsertStats{
			Documents: 0,
			Indexes:   len(c.meta.Indexes),
		})
		return nil, nil
	}
	if err := c.flushBufferedNoIndex(); err != nil {
		return nil, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}
	c.meta = catalog.meta
	plannerOptions, err := collectionPlannerOptions(c.meta)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	indexedMemtablesEnabled := c.shouldBufferIndexedInserts(c.meta)
	bufferIndexedInserts := c.shouldBufferIndexedInsertBatch(c.meta, len(documents))
	if indexedMemtablesEnabled && !bufferIndexedInserts {
		_ = snap.Close()
		if err := c.flushBufferedWrites(); err != nil {
			return nil, err
		}
		snap = c.db.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		catalog, err = c.catalogForSnapshot(snap)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		if catalog == nil {
			_ = snap.Close()
			return nil, errCollectionNotFound
		}
		c.meta = catalog.meta
		plannerOptions, err = collectionPlannerOptions(c.meta)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
		indexedMemtablesEnabled = c.shouldBufferIndexedInserts(c.meta)
		bufferIndexedInserts = c.shouldBufferIndexedInsertBatch(c.meta, len(documents))
	}
	if bufferIndexedInserts {
		plannerOptions = collectionOptionsWithBufferedTemplateV1Resolver(plannerOptions, c.writeDomain, c.meta.Name)
	}
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	if len(c.meta.Indexes) == 0 && plannerOptions.documentFormat == DocumentFormatJSON {
		return c.insertBatchNoIndex(catalog, snap, baseCommitSeq, baseSystemRoot, plannerOptions, ids, documents)
	}

	planner := insertBatchPlanner{
		collection:     c.meta.Name,
		primaryRoot:    collectionPrimaryRootName(c.meta.Name),
		templateRoot:   collectionTemplateRootName(c.meta.Name),
		indexStateRoot: collectionIndexStateRootName(c.meta.Name),
		indexes:        plannerIndexes(c.meta.Indexes),
		options:        plannerOptions,
	}
	if bufferIndexedInserts {
		planner.buildPrimaryVal = clonePrimaryDocument
		planner.cloneTemplateRunValues = true
	}
	plan, err := planner.planInsertBatchWithPreflight(ids, documents, insertBatchPreflight{
		snapshot:           snap,
		primaryRootID:      catalog.rootID(collectionPrimaryRootName(c.meta.Name)),
		uniqueIndexRootIDs: uniqueIndexRootIDs(catalog),
	})
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if bufferIndexedInserts {
		plan.stats.BufferedIndexedBatches = 1
	} else if indexedMemtablesEnabled {
		plan.stats.BufferedIndexedBypassBatches = 1
	}
	if len(plan.runs) == 0 {
		_ = snap.Close()
		c.setLastInsertStats(plan.stats.CollectionInsertStats)
		return plan.resultIDs, nil
	}

	if bufferIndexedInserts {
		resultIDs, err := cloneBatchDocumentIDs(plan.resultIDs)
		if err != nil {
			_ = snap.Close()
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
		bufferFlushElapsed, err := c.bufferIndexedInsertPlanLocked(catalog, baseCommitSeq, baseSystemRoot, plan)
		_ = snap.Close()
		if err != nil {
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
		plan.stats.Publish += bufferFlushElapsed
		c.setLastInsertStats(plan.stats.CollectionInsertStats)
		return resultIDs, nil
	}

	baseRootIDs := make(map[string]uint64, len(plan.runs))
	for _, run := range plan.runs {
		baseRootIDs[run.name] = catalog.rootID(run.name)
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(plan.runs))
	iterators := make([]iterator.UnsafeIterator, 0, len(plan.runs))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionRunTables(plan.runs)
	}()
	for _, run := range plan.runs {
		iter := run.table.NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      baseRootIDs[run.name],
			Iter:          iter,
			StoragePolicy: run.storagePolicy,
		})
	}

	rootNames := make([]string, len(plan.runs))
	for i, run := range plan.runs {
		rootNames[i] = run.name
	}
	publishStart := time.Now()
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	})
	plan.stats.Publish = time.Since(publishStart)
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(plan.runs) {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	c.setLastInsertStats(plan.stats.CollectionInsertStats)
	return plan.resultIDs, nil
}

func collectionOptionsWithTrustedBSONDocuments(opts collectionOptions, trusted bool) (collectionOptions, error) {
	if !trusted {
		return opts, nil
	}
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatBSON {
		return collectionOptions{}, errors.New("collections: trusted BSON insert requires BSON document format")
	}
	opts.trustedBSONDocuments = true
	return opts, nil
}

func (c *Collection) insertBatchNoIndex(
	catalog *collectionCatalog,
	snap *backenddb.Snapshot,
	baseCommitSeq uint64,
	baseSystemRoot uint64,
	plannerOptions collectionOptions,
	ids, documents [][]byte,
) ([][]byte, error) {
	if len(ids) != len(documents) {
		_ = snap.Close()
		return nil, fmt.Errorf("collections: caller-provided batch ids length mismatch")
	}

	stats := CollectionInsertStats{
		Documents: len(documents),
		Indexes:   len(c.meta.Indexes),
	}
	resultIDs, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	entries := make([]noIndexBatchEntry, len(documents))
	for i := range documents {
		id := resultIDs[i]
		entries[i] = noIndexBatchEntry{
			id:       id,
			document: documents[i],
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].id, entries[j].id) < 0
	})
	phaseStart := time.Now()
	for i := 1; i < len(entries); i++ {
		if bytes.Equal(entries[i-1].id, entries[i].id) {
			_ = snap.Close()
			return nil, ErrDuplicateDocumentID
		}
	}

	rootName := collectionPrimaryRootName(c.meta.Name)
	baseRoot := catalog.rootID(rootName)
	if baseRoot != 0 {
		keys := make([][]byte, len(entries))
		for i := range entries {
			keys[i] = entries[i].id
		}
		exists, err := snap.HasAnySortedAtRoot(baseRoot, keys)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		if exists {
			_ = snap.Close()
			return nil, ErrDocumentExists
		}
	}
	stats.DuplicateDocumentPreflight = time.Since(phaseStart)
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	phaseStart = time.Now()
	table := newCollectionRunTable(len(entries))
	for i := range entries {
		setCollectionRunValue(table, entries[i].id, entries[i].document)
	}
	table.Freeze()
	stats.PrimaryRunBuild = time.Since(phaseStart)
	iter := table.NewIterator(nil, nil)
	defer func() {
		_ = iter.Close()
		resetCollectionRunTable(table)
	}()

	baseRootIDs := map[string]uint64{rootName: baseRoot}
	publishStart := time.Now()
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: plannerOptions.dataStoragePolicy,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, []string{rootName}, baseRootIDs, rootIDs)
	})
	stats.Publish = time.Since(publishStart)
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != 1 {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	stats.Runs = 1
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, []string{rootName}, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	c.setLastInsertStats(stats)
	return resultIDs, nil
}

func (c *Collection) Delete(documentID []byte) error {
	_, err := c.DeleteDocument(documentID)
	return err
}

// DeleteDocument removes a document and reports whether this call deleted an
// existing primary document.
func (c *Collection) DeleteDocument(documentID []byte) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	if len(documentID) == 0 {
		return false, errors.New("collections: document id cannot be empty")
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation()
	if err := c.flushBufferedWrites(); err != nil {
		return false, err
	}

	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		deleted, err := c.deleteDocumentOnce(documentID)
		if errors.Is(err, ErrConcurrentMutation) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return deleted, err
	}
	return false, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) deleteDocumentOnce(documentID []byte) (bool, error) {
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return false, err
	}
	if catalog == nil {
		_ = snap.Close()
		return false, errCollectionNotFound
	}
	c.meta = catalog.meta
	plannerOptions, err := collectionPlannerOptions(c.meta)
	if err != nil {
		_ = snap.Close()
		return false, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	primaryRoot := catalog.rootID(collectionPrimaryRootName(c.meta.Name))
	if primaryRoot == 0 {
		_ = snap.Close()
		return false, nil
	}
	entry, err := snap.GetEntryAtRoot(primaryRoot, documentID)
	if errors.Is(err, tree.ErrKeyNotFound) {
		_ = snap.Close()
		return false, nil
	}
	if err != nil {
		_ = snap.Close()
		return false, err
	}

	runtimes, err := (insertBatchPlanner{
		collection: c.meta.Name,
		indexes:    plannerIndexes(c.meta.Indexes),
	}).indexRuntimes()
	if err != nil {
		_ = snap.Close()
		return false, err
	}
	var state documentIndexState
	if len(runtimes) > 0 {
		state, err = loadDeleteIndexState(snap, catalog, documentID, entry.Value, runtimes, plannerOptions)
		if err != nil {
			_ = snap.Close()
			return false, err
		}
	}

	rootNames := []string{collectionPrimaryRootName(c.meta.Name)}
	baseRootIDs := map[string]uint64{
		rootNames[0]: primaryRoot,
	}
	policies := []backenddb.OrderedRootStoragePolicy{plannerOptions.dataStoragePolicy}
	deltaTables := make([]memtable.Table, 0, 2+len(runtimes))
	deltaTables = append(deltaTables, buildDeleteRootDeltaTable([][]byte{documentID}))

	if len(runtimes) > 0 {
		if persistIndexStateForOptions(plannerOptions) {
			stateRootName := collectionIndexStateRootName(c.meta.Name)
			stateRootID := catalog.rootID(stateRootName)
			if stateRootID != 0 {
				rootNames = append(rootNames, stateRootName)
				baseRootIDs[stateRootName] = stateRootID
				policies = append(policies, plannerOptions.indexStateStoragePolicy)
				deltaTables = append(deltaTables, buildDeleteRootDeltaTable([][]byte{documentID}))
			}
		}
		for _, runtime := range runtimes {
			deleteKeys, err := secondaryDeleteKeysForDocument(runtime, state, documentID)
			if err != nil {
				_ = snap.Close()
				return false, err
			}
			rootName := collectionSecondaryRootName(c.meta.Name, runtime.def.name)
			rootID := catalog.rootID(rootName)
			if rootID == 0 || len(deleteKeys) == 0 {
				continue
			}
			rootNames = append(rootNames, rootName)
			baseRootIDs[rootName] = rootID
			policies = append(policies, runtime.def.storagePolicy)
			deltaTables = append(deltaTables, buildDeleteRootDeltaTable(deleteKeys))
		}
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(rootNames))
	iterators := make([]iterator.UnsafeIterator, 0, len(rootNames))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionTables(deltaTables)
	}()
	for i, rootName := range rootNames {
		iter := deltaTables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      baseRootIDs[rootName],
			Iter:          iter,
			StoragePolicy: policies[i],
		})
	}
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	})
	if err != nil {
		return false, err
	}
	if len(rootIDs) != len(rootNames) {
		return false, errors.New("collections: ordered root publish returned unexpected root count")
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return true, nil
}

func (c *Collection) Replace(documentID, document []byte) (bool, error) {
	matched, _, err := c.Update(documentID, func(current []byte) ([]byte, bool, error) {
		if bytes.Equal(current, document) {
			return current, false, nil
		}
		return document, true, nil
	})
	return matched, err
}

// Update applies update to the latest document value and retries if another
// collection write changes the root before this update publishes.
func (c *Collection) Update(documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) (bool, bool, error) {
	if c == nil {
		return false, false, errCollectionNil
	}
	if c.db == nil {
		return false, false, errCollectionDBNil
	}
	if len(documentID) == 0 {
		return false, false, errors.New("collections: document id cannot be empty")
	}
	if update == nil {
		return false, false, errors.New("collections: update function is nil")
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation()
	if err := c.flushBufferedWrites(); err != nil {
		return false, false, err
	}

	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		matched, modified, err := c.updateDocumentOnce(documentID, update)
		if errors.Is(err, ErrConcurrentMutation) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return matched, modified, err
	}
	return false, false, collectionMutationRetryExhausted(lastErr)
}

// UpdateBatch applies a unique set of document updates under one collection
// mutation. Missing documents report Matched=false. Duplicate document IDs are
// rejected so callers that require same-document ordering can fall back to
// sequential Update calls.
func (c *Collection) UpdateBatch(items []UpdateBatchItem) ([]UpdateBatchResult, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errors.New("collections: db is nil")
	}
	if len(items) == 0 {
		return nil, nil
	}
	if err := validateUpdateBatchItems(items); err != nil {
		return nil, err
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation()
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		results, err := c.updateBatchOnce(items)
		if errors.Is(err, ErrConcurrentMutation) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return results, err
	}
	return nil, collectionMutationRetryExhausted(lastErr)
}

func validateUpdateBatchItems(items []UpdateBatchItem) error {
	seen := make(map[string]struct{}, len(items))
	for i, item := range items {
		if len(item.DocumentID) == 0 {
			return fmt.Errorf("collections: document id cannot be empty at index %d", i)
		}
		if item.Update == nil {
			return fmt.Errorf("collections: update function is nil at index %d", i)
		}
		key := string(item.DocumentID)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%w at index %d", ErrDuplicateDocumentID, i)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateBSONReplacementPreservesID(current, replacement []byte, opts collectionOptions) error {
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatBSON {
		return nil
	}
	currentRaw := bson.Raw(current)
	if err := currentRaw.Validate(); err != nil {
		return fmt.Errorf("collections: current BSON document: %w", err)
	}
	replacementRaw := bson.Raw(replacement)
	if err := replacementRaw.Validate(); err != nil {
		return fmt.Errorf("collections: replacement BSON document: %w", err)
	}
	currentID := currentRaw.Lookup("_id")
	replacementID := replacementRaw.Lookup("_id")
	if currentID.IsZero() && replacementID.IsZero() {
		return nil
	}
	if currentID.IsZero() || replacementID.IsZero() || !currentID.Equal(replacementID) {
		return errors.New("collections: update replacement cannot modify _id")
	}
	return nil
}

func waitBeforeCollectionMutationRetry(attempt int) {
	if attempt < 4 {
		runtime.Gosched()
		return
	}
	shift := attempt - 4
	if shift > 6 {
		shift = 6
	}
	time.Sleep(time.Duration(1<<shift) * time.Microsecond)
}

func collectionMutationRetryExhausted(err error) error {
	if err == nil {
		err = ErrConcurrentMutation
	}
	return fmt.Errorf("%w: retry budget exceeded after %d attempts: %v", ErrConcurrentMutation, maxCollectionMutationRetries, err)
}

func (c *Collection) updateDocumentOnce(documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) (bool, bool, error) {
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, false, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	if catalog == nil {
		_ = snap.Close()
		return false, false, errCollectionNotFound
	}
	c.meta = catalog.meta
	plannerOptions, err := collectionPlannerOptions(c.meta)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseUserRoot := snapshotUserRoot(snap)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	primaryRoot := catalog.rootID(collectionPrimaryRootName(c.meta.Name))
	if primaryRoot == 0 {
		_ = snap.Close()
		return false, false, nil
	}
	entry, err := snap.GetEntryAtRoot(primaryRoot, documentID)
	if errors.Is(err, tree.ErrKeyNotFound) {
		_ = snap.Close()
		return false, false, nil
	}
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}

	document, changed, err := update(bytes.Clone(entry.Value))
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	if !changed {
		_ = snap.Close()
		return true, false, nil
	}
	preparedDocuments, templateRecords, templateResolver, err := prepareInsertDocuments([][]byte{document}, plannerOptions)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	if len(preparedDocuments) != 1 {
		_ = snap.Close()
		return false, false, errors.New("collections: update prepared unexpected document count")
	}
	document = preparedDocuments[0]
	if templateResolver != nil {
		plannerOptions.templateResolver = templateResolver
	}

	runtimes, err := (insertBatchPlanner{
		collection: c.meta.Name,
		indexes:    plannerIndexes(c.meta.Indexes),
	}).indexRuntimes()
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	var oldState documentIndexState
	var newState documentIndexState
	indexStateChanged := false
	if len(runtimes) > 0 {
		oldState, err = loadDeleteIndexState(snap, catalog, documentID, entry.Value, runtimes, plannerOptions)
		if err != nil {
			_ = snap.Close()
			return false, false, err
		}
		newState, err = indexStateForDocument(document, runtimes, plannerOptions)
		if err != nil {
			_ = snap.Close()
			return false, false, err
		}
		indexStateChanged = !documentIndexStatesEqual(oldState, newState)
		if indexStateChanged {
			if err := rejectReplaceUniqueConflicts(snap, catalog, runtimes, newState, documentID, nil); err != nil {
				_ = snap.Close()
				return false, false, err
			}
		}
	}

	rootNames := make([]string, 0, 2+len(runtimes))
	baseRootIDs := make(map[string]uint64, 2+len(runtimes))
	policies := make([]backenddb.OrderedRootStoragePolicy, 0, 2+len(runtimes))
	deltaTables := make([]memtable.Table, 0, 2+len(runtimes))

	if len(templateRecords) > 0 {
		templatePlan := &insertBatchPlan{}
		if err := (insertBatchPlanner{
			collection:             c.meta.Name,
			templateRoot:           collectionTemplateRootName(c.meta.Name),
			options:                plannerOptions,
			cloneTemplateRunValues: true,
		}).emitTemplateRun(templatePlan, templateRecords); err != nil {
			_ = snap.Close()
			return false, false, err
		}
		for _, run := range templatePlan.runs {
			rootNames = append(rootNames, run.name)
			baseRootIDs[run.name] = catalog.rootID(run.name)
			policies = append(policies, run.storagePolicy)
			deltaTables = append(deltaTables, run.table)
		}
	}

	primaryRootName := collectionPrimaryRootName(c.meta.Name)
	rootNames = append(rootNames, primaryRootName)
	baseRootIDs[primaryRootName] = primaryRoot
	policies = append(policies, plannerOptions.dataStoragePolicy)
	primaryTable := newCollectionRunTable(1)
	setCollectionRunValue(primaryTable, bytes.Clone(documentID), document)
	primaryTable.Freeze()
	deltaTables = append(deltaTables, primaryTable)

	if indexStateChanged {
		if persistIndexStateForOptions(plannerOptions) {
			stateRootName := collectionIndexStateRootName(c.meta.Name)
			stateRootID := catalog.rootID(stateRootName)
			rootNames = append(rootNames, stateRootName)
			baseRootIDs[stateRootName] = stateRootID
			policies = append(policies, plannerOptions.indexStateStoragePolicy)
			stateTable := newCollectionRunTable(1)
			rawState, err := encodeDocumentIndexState(newState)
			if err != nil {
				_ = snap.Close()
				resetCollectionTables(append(deltaTables, stateTable))
				return false, false, err
			}
			if len(newState) == 0 {
				stateTable.DeleteSteal(bytes.Clone(documentID))
			} else {
				stateTable.SetSteal(bytes.Clone(documentID), rawState)
			}
			stateTable.Freeze()
			deltaTables = append(deltaTables, stateTable)
		}

		for _, runtime := range runtimes {
			rootName := collectionSecondaryRootName(c.meta.Name, runtime.def.name)
			rootID := catalog.rootID(rootName)
			table := newCollectionRunTable(0)
			deleteKeys, err := secondaryDeleteKeysForDocument(runtime, oldState, documentID)
			if err != nil {
				_ = snap.Close()
				resetCollectionTables(append(deltaTables, table))
				return false, false, err
			}
			for _, key := range deleteKeys {
				table.DeleteSteal(bytes.Clone(key))
			}
			for _, encoded := range newState[runtime.def.name] {
				key, err := indexEntryKey(encoded, documentID)
				if err != nil {
					_ = snap.Close()
					resetCollectionTables(append(deltaTables, table))
					return false, false, err
				}
				table.SetSteal(key, nil)
			}
			if table.Len() == 0 {
				resetCollectionRunTable(table)
				continue
			}
			table.Freeze()
			rootNames = append(rootNames, rootName)
			baseRootIDs[rootName] = rootID
			policies = append(policies, runtime.def.storagePolicy)
			deltaTables = append(deltaTables, table)
		}
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(rootNames))
	iterators := make([]iterator.UnsafeIterator, 0, len(rootNames))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionTables(deltaTables)
	}()
	for i, rootName := range rootNames {
		iter := deltaTables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      baseRootIDs[rootName],
			Iter:          iter,
			StoragePolicy: policies[i],
		})
	}
	preflight := func() error {
		return c.validateMutationRootDescriptors(baseUserRoot, baseSystemRoot, baseCommitSeq)
	}
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	})
	if err != nil {
		return false, false, err
	}
	if len(rootIDs) != len(rootNames) {
		return false, false, errors.New("collections: ordered root publish returned unexpected root count")
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return true, true, nil
}

type preparedBatchUpdate struct {
	itemIndex         int
	documentID        []byte
	document          []byte
	oldState          documentIndexState
	newState          documentIndexState
	indexStateChanged bool
}

func (c *Collection) updateBatchOnce(items []UpdateBatchItem) ([]UpdateBatchResult, error) {
	results := make([]UpdateBatchResult, len(items))
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}
	c.meta = catalog.meta
	plannerOptions, err := collectionPlannerOptions(c.meta)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseUserRoot := snapshotUserRoot(snap)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	primaryRoot := catalog.rootID(collectionPrimaryRootName(c.meta.Name))
	if primaryRoot == 0 {
		_ = snap.Close()
		return results, nil
	}
	runtimes, err := (insertBatchPlanner{
		collection: c.meta.Name,
		indexes:    plannerIndexes(c.meta.Indexes),
	}).indexRuntimes()
	if err != nil {
		_ = snap.Close()
		return nil, err
	}

	changed := make([]preparedBatchUpdate, 0, len(items))
	changedDocuments := make([][]byte, 0, len(items))
	for i, item := range items {
		entry, err := snap.GetEntryAtRoot(primaryRoot, item.DocumentID)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		results[i].Matched = true
		document, changedOne, err := item.Update(bytes.Clone(entry.Value))
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		if !changedOne {
			continue
		}
		if err := validateBSONReplacementPreservesID(entry.Value, document, plannerOptions); err != nil {
			_ = snap.Close()
			return nil, err
		}
		prepared := preparedBatchUpdate{
			itemIndex:  i,
			documentID: bytes.Clone(item.DocumentID),
		}
		if len(runtimes) > 0 {
			prepared.oldState, err = loadDeleteIndexState(snap, catalog, item.DocumentID, entry.Value, runtimes, plannerOptions)
			if err != nil {
				_ = snap.Close()
				return nil, err
			}
		}
		changed = append(changed, prepared)
		changedDocuments = append(changedDocuments, bytes.Clone(document))
	}
	if len(changed) == 0 {
		_ = snap.Close()
		return results, nil
	}

	preparedDocuments, templateRecords, templateResolver, err := prepareInsertDocuments(changedDocuments, plannerOptions)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if len(preparedDocuments) != len(changed) {
		_ = snap.Close()
		return nil, errors.New("collections: update batch prepared unexpected document count")
	}
	if templateResolver != nil {
		plannerOptions.templateResolver = templateResolver
	}
	for i := range changed {
		changed[i].document = preparedDocuments[i]
		if len(runtimes) > 0 {
			changed[i].newState, err = indexStateForDocument(changed[i].document, runtimes, plannerOptions)
			if err != nil {
				_ = snap.Close()
				return nil, err
			}
			changed[i].indexStateChanged = !documentIndexStatesEqual(changed[i].oldState, changed[i].newState)
		}
	}
	batchReplacements := batchUniqueReplacementOwners(runtimes, changed)
	for i := range changed {
		if changed[i].indexStateChanged {
			if err := rejectReplaceUniqueConflicts(snap, catalog, runtimes, changed[i].newState, changed[i].documentID, batchReplacements); err != nil {
				_ = snap.Close()
				return nil, err
			}
		}
	}
	if err := rejectBatchUniqueConflicts(runtimes, changed); err != nil {
		_ = snap.Close()
		return nil, err
	}

	rootNames := make([]string, 0, 2+len(runtimes))
	baseRootIDs := make(map[string]uint64, 2+len(runtimes))
	policies := make([]backenddb.OrderedRootStoragePolicy, 0, 2+len(runtimes))
	deltaTables := make([]memtable.Table, 0, 2+len(runtimes))
	var stateTable memtable.Table
	secondaryTables := make(map[string]memtable.Table, len(runtimes))
	var iterators []iterator.UnsafeIterator
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionTables(deltaTables)
		resetCollectionRunTable(stateTable)
		for _, table := range secondaryTables {
			resetCollectionRunTable(table)
		}
	}()

	if len(templateRecords) > 0 {
		templatePlan := &insertBatchPlan{}
		if err := (insertBatchPlanner{
			collection:             c.meta.Name,
			templateRoot:           collectionTemplateRootName(c.meta.Name),
			options:                plannerOptions,
			cloneTemplateRunValues: true,
		}).emitTemplateRun(templatePlan, templateRecords); err != nil {
			_ = snap.Close()
			return nil, err
		}
		for _, run := range templatePlan.runs {
			rootNames = append(rootNames, run.name)
			baseRootIDs[run.name] = catalog.rootID(run.name)
			policies = append(policies, run.storagePolicy)
			deltaTables = append(deltaTables, run.table)
		}
	}

	primaryRootName := collectionPrimaryRootName(c.meta.Name)
	rootNames = append(rootNames, primaryRootName)
	baseRootIDs[primaryRootName] = primaryRoot
	policies = append(policies, plannerOptions.dataStoragePolicy)
	primaryTable := newCollectionRunTable(len(changed))
	for _, item := range changed {
		setCollectionRunValue(primaryTable, bytes.Clone(item.documentID), item.document)
		results[item.itemIndex].Modified = true
	}
	primaryTable.Freeze()
	deltaTables = append(deltaTables, primaryTable)

	if len(runtimes) > 0 {
		if persistIndexStateForOptions(plannerOptions) {
			stateTable = newCollectionRunTable(len(changed))
		}
		for _, item := range changed {
			if !item.indexStateChanged {
				continue
			}
			if stateTable != nil {
				rawState, err := encodeDocumentIndexState(item.newState)
				if err != nil {
					_ = snap.Close()
					return nil, err
				}
				if len(item.newState) == 0 {
					stateTable.DeleteSteal(bytes.Clone(item.documentID))
				} else {
					stateTable.SetSteal(bytes.Clone(item.documentID), rawState)
				}
			}
			for _, runtime := range runtimes {
				rootName := collectionSecondaryRootName(c.meta.Name, runtime.def.name)
				table := secondaryTables[rootName]
				if table == nil {
					table = newCollectionRunTable(0)
					secondaryTables[rootName] = table
				}
				deleteKeys, err := secondaryDeleteKeysForDocument(runtime, item.oldState, item.documentID)
				if err != nil {
					_ = snap.Close()
					return nil, err
				}
				for _, key := range deleteKeys {
					table.DeleteSteal(bytes.Clone(key))
				}
				for _, encoded := range item.newState[runtime.def.name] {
					key, err := indexEntryKey(encoded, item.documentID)
					if err != nil {
						_ = snap.Close()
						return nil, err
					}
					table.SetSteal(key, nil)
				}
			}
		}
		if stateTable != nil && stateTable.Len() > 0 {
			stateTable.Freeze()
			stateRootName := collectionIndexStateRootName(c.meta.Name)
			rootNames = append(rootNames, stateRootName)
			baseRootIDs[stateRootName] = catalog.rootID(stateRootName)
			policies = append(policies, plannerOptions.indexStateStoragePolicy)
			deltaTables = append(deltaTables, stateTable)
			stateTable = nil
		}
		for _, runtime := range runtimes {
			rootName := collectionSecondaryRootName(c.meta.Name, runtime.def.name)
			table := secondaryTables[rootName]
			if table == nil || table.Len() == 0 {
				continue
			}
			table.Freeze()
			rootNames = append(rootNames, rootName)
			baseRootIDs[rootName] = catalog.rootID(rootName)
			policies = append(policies, runtime.def.storagePolicy)
			deltaTables = append(deltaTables, table)
			delete(secondaryTables, rootName)
		}
	}

	defer func() { _ = snap.Close() }()
	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(rootNames))
	iterators = make([]iterator.UnsafeIterator, 0, len(rootNames))
	for i, rootName := range rootNames {
		iter := deltaTables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      baseRootIDs[rootName],
			Iter:          iter,
			StoragePolicy: policies[i],
		})
	}
	preflight := func() error {
		return c.validateMutationRootDescriptors(baseUserRoot, baseSystemRoot, baseCommitSeq)
	}
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	})
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(rootNames) {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return results, nil
}

func rejectBatchUniqueConflicts(runtimes []indexRuntime, updates []preparedBatchUpdate) error {
	for _, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		seen := make(map[string][]byte)
		for _, update := range updates {
			for _, encoded := range update.newState[runtime.def.name] {
				key := string(encoded)
				if previous, ok := seen[key]; ok && !bytes.Equal(previous, update.documentID) {
					return fmt.Errorf("%w %q", ErrUniqueIndexConflict, runtime.def.name)
				}
				seen[key] = update.documentID
			}
		}
	}
	return nil
}

type batchUniqueReplacementSet map[string]map[string]map[string]struct{}

func batchUniqueReplacementOwners(runtimes []indexRuntime, updates []preparedBatchUpdate) batchUniqueReplacementSet {
	if len(runtimes) == 0 || len(updates) == 0 {
		return nil
	}
	out := make(batchUniqueReplacementSet)
	for _, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		indexName := runtime.def.name
		for _, update := range updates {
			oldValues := update.oldState[indexName]
			if len(oldValues) == 0 {
				continue
			}
			newValues := update.newState[indexName]
			for _, oldValue := range oldValues {
				if documentIndexStateContainsValue(newValues, oldValue) {
					continue
				}
				byValue := out[indexName]
				if byValue == nil {
					byValue = make(map[string]map[string]struct{})
					out[indexName] = byValue
				}
				owners := byValue[string(oldValue)]
				if owners == nil {
					owners = make(map[string]struct{})
					byValue[string(oldValue)] = owners
				}
				owners[string(update.documentID)] = struct{}{}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func documentIndexStateContainsValue(values [][]byte, target []byte) bool {
	for _, value := range values {
		if bytes.Equal(value, target) {
			return true
		}
	}
	return false
}

func (s batchUniqueReplacementSet) allows(indexName string, encoded, documentID []byte) bool {
	if len(s) == 0 {
		return false
	}
	byValue := s[indexName]
	if len(byValue) == 0 {
		return false
	}
	owners := byValue[string(encoded)]
	if len(owners) == 0 {
		return false
	}
	_, ok := owners[string(documentID)]
	return ok
}

func (c *Collection) catalogForSnapshot(snap *backenddb.Snapshot) (*collectionCatalog, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	systemRoot := snapshotSystemRoot(snap)
	commitSeq := snapshotCommitSeq(snap)

	c.catalogMu.RLock()
	if cached := c.catalog; cached != nil && c.catalogSystemRoot == systemRoot && c.catalogCommitSeq == commitSeq {
		c.catalogMu.RUnlock()
		return cached, nil
	}
	c.catalogMu.RUnlock()

	if cached := cachedWriteDomainCatalogForState(c.writeDomain, systemRoot, commitSeq); cached != nil {
		c.rememberCatalog(snap, cached)
		return cached, nil
	}

	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return nil, err
	}
	c.rememberCatalog(snap, catalog)
	return catalog, nil
}

func cachedWriteDomainCatalogForState(domain *collectionWriteDomain, systemRoot, commitSeq uint64) *collectionCatalog {
	if domain == nil || systemRoot == 0 {
		return nil
	}
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	if !domain.loaded || domain.catalog == nil || domain.baseSystemRoot != systemRoot || domain.baseCommitSeq != commitSeq {
		return nil
	}
	return domain.catalog.copy()
}

func snapshotSystemRoot(snap *backenddb.Snapshot) uint64 {
	if snap == nil {
		return 0
	}
	if state := snap.State(); state != nil {
		return state.SystemRootPageID
	}
	return 0
}

func snapshotUserRoot(snap *backenddb.Snapshot) uint64 {
	if snap == nil {
		return 0
	}
	if state := snap.State(); state != nil {
		return state.RootPageID
	}
	return 0
}

func snapshotCommitSeq(snap *backenddb.Snapshot) uint64 {
	if snap == nil {
		return 0
	}
	if state := snap.State(); state != nil {
		return state.CommitSeq
	}
	return 0
}

func dbCommitSeqAndSystemRoot(db *backenddb.DB) (uint64, uint64) {
	if db == nil {
		return 0, 0
	}
	if state := db.State(); state != nil {
		return state.CommitSeq, state.SystemRootPageID
	}
	return 0, 0
}

func (c *Collection) commitSeqForSystemRoot(systemRoot uint64) uint64 {
	if c == nil {
		return 0
	}
	commitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	if currentSystemRoot != systemRoot {
		return 0
	}
	return commitSeq
}

func (c *Collection) rememberCatalog(snap *backenddb.Snapshot, catalog *collectionCatalog) {
	if c == nil || snap == nil || catalog == nil {
		return
	}
	commitSeq := snapshotCommitSeq(snap)
	systemRoot := snapshotSystemRoot(snap)
	c.catalogMu.Lock()
	c.catalogCommitSeq = commitSeq
	c.catalogSystemRoot = systemRoot
	c.catalog = catalog.copy()
	c.catalogMu.Unlock()
}

func (c *Collection) rememberCatalogAtSystemRoot(systemRoot uint64, catalog *collectionCatalog) {
	if c == nil || catalog == nil {
		return
	}
	commitSeq := c.commitSeqForSystemRoot(systemRoot)
	c.catalogMu.Lock()
	c.catalogCommitSeq = commitSeq
	c.catalogSystemRoot = systemRoot
	c.catalog = catalog.copy()
	c.catalogMu.Unlock()
}

func (c *Collection) noteWriteDomainCatalog(systemRoot uint64, catalog *collectionCatalog) {
	if c == nil || catalog == nil || c.writeDomain == nil {
		return
	}
	domain := c.writeDomain
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if domain.count != 0 {
		return
	}
	options, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		domain.loaded = false
		return
	}
	domain.loaded = true
	domain.meta = *catalog.meta.copy()
	domain.catalog = catalog.copy()
	domain.baseCommitSeq = c.commitSeqForSystemRoot(systemRoot)
	domain.baseSystemRoot = systemRoot
	domain.primaryRoot = catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	domain.storagePolicy = options.dataStoragePolicy
	domain.rootRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.primaryIDIndex = nil
	domain.uniqueValueRuns = nil
	domain.uniqueValueIndex = nil
	if domain.table == nil {
		domain.table = newCollectionRunTable(0)
	}
}

func cloneCatalogWithRootUpdates(base *collectionCatalog, meta CollectionMeta, rootNames []string, rootIDs []uint64) *collectionCatalog {
	roots := make(map[string]uint64)
	if base != nil {
		for name, rootID := range base.roots {
			roots[name] = rootID
		}
	}
	for i, name := range rootNames {
		if i < len(rootIDs) {
			roots[name] = rootIDs[i]
		}
	}
	return (&collectionCatalog{meta: meta, roots: roots}).copy()
}

func buildDeleteRootDeltaTable(deleteKeys [][]byte) memtable.Table {
	table := newCollectionRunTable(len(deleteKeys))
	for _, key := range deleteKeys {
		table.DeleteSteal(bytes.Clone(key))
	}
	table.Freeze()
	return table
}

func buildCreateIndexBackfillPlan(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	newRuntime indexRuntime,
	existingRuntimes []indexRuntime,
	opts collectionOptions,
) (*createIndexBackfillPlan, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	primaryRootName := collectionPrimaryRootName(catalog.meta.Name)
	primaryRootID := catalog.rootID(primaryRootName)
	plan := &createIndexBackfillPlan{
		baseRootIDs: make(map[string]uint64, 2),
	}
	plan.baseRootIDs[primaryRootName] = primaryRootID
	if primaryRootID == 0 {
		return plan, nil
	}

	it, err := snap.IteratorAtRoot(primaryRootID, nil, nil)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return plan, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	persistIndexState := persistIndexStateForOptions(opts)
	var indexStateTable memtable.Table
	if persistIndexState {
		indexStateTable = newCollectionRunTable(0)
	}
	secondaryTable := newCollectionRunTable(0)
	uniqueProbes := make([]uniqueProbeCandidate, 0)
	stateRootName := collectionIndexStateRootName(catalog.meta.Name)
	stateRootID := catalog.rootID(stateRootName)
	secondaryRootName := collectionSecondaryRootName(catalog.meta.Name, newRuntime.def.name)
	documentCount := 0
	secondaryCount := 0
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		documentID := bytes.Clone(it.UnsafeKey())
		document := it.ValueCopy(nil)
		if err := it.Error(); err != nil {
			return nil, err
		}

		newState, err := indexStateForDocument(document, []indexRuntime{newRuntime}, opts)
		if err != nil {
			return nil, err
		}
		values := newState[newRuntime.def.name]
		if persistIndexState {
			existingState, err := loadBackfillIndexState(snap, stateRootID, documentID, document, existingRuntimes, opts)
			if err != nil {
				return nil, err
			}
			merged := cloneDocumentIndexState(existingState)
			if len(values) > 0 {
				merged[newRuntime.def.name] = values
			} else {
				delete(merged, newRuntime.def.name)
			}
			rawState, err := encodeNormalizedDocumentIndexState(merged)
			if err != nil {
				return nil, err
			}
			indexStateTable.SetSteal(bytes.Clone(documentID), rawState)
			documentCount++
		}

		for _, encoded := range values {
			key, err := indexEntryKey(encoded, documentID)
			if err != nil {
				return nil, err
			}
			secondaryTable.SetSteal(key, nil)
			secondaryCount++
			if !newRuntime.def.unique {
				continue
			}
			uniqueProbes = append(uniqueProbes, uniqueProbeCandidate{
				indexName:    newRuntime.def.name,
				encodedValue: encoded,
				documentID:   bytes.Clone(documentID),
			})
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if _, err := buildUniqueProbeRuns(uniqueProbes); err != nil {
		return nil, err
	}
	if documentCount > 0 {
		indexStateTable.Freeze()
		plan.rootNames = append(plan.rootNames, stateRootName)
		plan.baseRootIDs[stateRootName] = stateRootID
		plan.tables = append(plan.tables, indexStateTable)
		plan.policies = append(plan.policies, opts.indexStateStoragePolicy)
	}
	if secondaryCount > 0 {
		secondaryTable.Freeze()
		plan.rootNames = append(plan.rootNames, secondaryRootName)
		plan.baseRootIDs[secondaryRootName] = catalog.rootID(secondaryRootName)
		plan.tables = append(plan.tables, secondaryTable)
		plan.policies = append(plan.policies, newRuntime.def.storagePolicy)
	}
	return plan, nil
}

func loadBackfillIndexState(snap *backenddb.Snapshot, stateRootID uint64, documentID, document []byte, existingRuntimes []indexRuntime, opts collectionOptions) (documentIndexState, error) {
	if persistIndexStateForOptions(opts) && stateRootID != 0 {
		entry, err := snap.GetEntryAtRoot(stateRootID, documentID)
		if err == nil {
			return decodeDocumentIndexState(entry.Value)
		}
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return nil, err
		}
	}
	return indexStateForDocument(document, existingRuntimes, opts)
}

func cloneDocumentIndexState(state documentIndexState) documentIndexState {
	out := make(documentIndexState, len(state))
	for name, values := range state {
		out[name] = normalizeEncodedIndexValues(values)
	}
	return out
}

func documentIndexStatesEqual(left, right documentIndexState) bool {
	if len(left) != len(right) {
		return false
	}
	for name, leftValues := range left {
		rightValues, ok := right[name]
		if !ok || len(leftValues) != len(rightValues) {
			return false
		}
		for i := range leftValues {
			if !bytes.Equal(leftValues[i], rightValues[i]) {
				return false
			}
		}
	}
	return true
}

func (c *Collection) buildRootDescriptorSystemIterator(rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	catalog, err := loadCollectionCatalog(current, c.meta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	for _, rootName := range rootNames {
		if got, want := catalog.rootID(rootName), baseRootIDs[rootName]; got != want {
			return nil, fmt.Errorf("collections: concurrent root modification detected for %q", rootName)
		}
	}
	updates := make(map[string][]byte, len(rootNames))
	for i, rootName := range rootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemTargetIterator(current, updates)
}

func (c *Collection) buildRootDescriptorSystemDeltaIterator(expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	if err := c.validateRootDescriptorSystemDelta(expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs); err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, len(rootNames))
	for i, rootName := range rootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemDeltaIterator(updates)
}

func (c *Collection) validateRootDescriptorSystemDelta(expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64) error {
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(nil)
	if c != nil {
		currentCommitSeq, currentSystemRoot = dbCommitSeqAndSystemRoot(c.db)
	}
	if currentSystemRoot != expectedSystemRoot || currentCommitSeq != expectedCommitSeq {
		current := c.db.AcquireSnapshot()
		if current == nil {
			return backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		catalog, err := loadCollectionCatalog(current, c.meta.Name)
		if err != nil {
			return err
		}
		if catalog == nil {
			return errCollectionNotFound
		}
		if !sameCollectionMeta(catalog.meta, c.meta) {
			return fmt.Errorf("collections: concurrent schema modification detected for %q", c.meta.Name)
		}
		for _, rootName := range rootNames {
			if got, want := catalog.rootID(rootName), baseRootIDs[rootName]; got != want {
				return fmt.Errorf("%w: concurrent root modification detected for %q", ErrConcurrentMutation, rootName)
			}
		}
	}
	return nil
}

func (c *Collection) validateMutationRootDescriptors(expectedUserRoot, expectedSystemRoot, expectedCommitSeq uint64) error {
	if c == nil || c.db == nil {
		return backenddb.ErrClosed
	}
	state := c.db.State()
	if state == nil {
		return backenddb.ErrClosed
	}
	if state.CommitSeq == expectedCommitSeq {
		return nil
	}
	// Raw TreeDB writes advance CommitSeq and move only the user root. Those do
	// not invalidate collection roots, so Update can still publish safely.
	if state.SystemRootPageID == expectedSystemRoot {
		if state.RootPageID != expectedUserRoot {
			return nil
		}
		return fmt.Errorf("%w: ambiguous same-root commit detected", ErrConcurrentMutation)
	}
	return fmt.Errorf("%w: concurrent collection root modification detected", ErrConcurrentMutation)
}

func (c *Collection) buildSchemaAndRootDescriptorSystemIterator(
	baseMeta CollectionMeta,
	newMeta CollectionMeta,
	rootNames []string,
	baseRootIDs map[string]uint64,
	rootIDs []uint64,
) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	catalog, err := loadCollectionCatalog(current, baseMeta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	if !sameCollectionMeta(catalog.meta, baseMeta) {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", baseMeta.Name)
	}
	primaryRootName := collectionPrimaryRootName(baseMeta.Name)
	if baseRootID, ok := baseRootIDs[primaryRootName]; ok {
		if got := catalog.rootID(primaryRootName); got != baseRootID {
			return nil, fmt.Errorf("collections: concurrent root modification detected for %q", primaryRootName)
		}
	}
	for _, rootName := range rootNames {
		if got, want := catalog.rootID(rootName), baseRootIDs[rootName]; got != want {
			return nil, fmt.Errorf("collections: concurrent root modification detected for %q", rootName)
		}
	}

	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, 1+len(rootNames))
	updates[systemCollectionMetaKey(baseMeta.Name)] = encodedMeta
	for i, rootName := range rootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemTargetIterator(current, updates)
}

func (c *Collection) buildSchemaOnlySystemDeltaIterator(baseMeta CollectionMeta, encodedMeta []byte, clearedRootNames []string) (iterator.UnsafeIterator, error) {
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	catalog, err := loadCollectionCatalog(current, baseMeta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	if !sameCollectionMeta(catalog.meta, baseMeta) {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", baseMeta.Name)
	}
	updates := map[string][]byte{
		systemCollectionMetaKey(baseMeta.Name): encodedMeta,
	}
	for _, rootName := range clearedRootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(0)
	}
	return buildSystemDeltaIterator(updates)
}

func loadDeleteIndexState(snap *backenddb.Snapshot, catalog *collectionCatalog, documentID, document []byte, runtimes []indexRuntime, opts collectionOptions) (documentIndexState, error) {
	if catalog == nil || len(runtimes) == 0 {
		return nil, nil
	}
	stateRoot := catalog.rootID(collectionIndexStateRootName(catalog.meta.Name))
	if persistIndexStateForOptions(opts) && stateRoot != 0 {
		entry, err := snap.GetEntryAtRoot(stateRoot, documentID)
		if err == nil {
			return decodeDocumentIndexState(entry.Value)
		}
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return nil, err
		}
	}
	return indexStateForDocument(document, runtimes, opts)
}

func secondaryDeleteKeysForDocument(runtime indexRuntime, state documentIndexState, documentID []byte) ([][]byte, error) {
	values := state[runtime.def.name]
	if len(values) == 0 {
		return nil, nil
	}
	out := make([][]byte, 0, len(values))
	for _, encoded := range values {
		key, err := indexEntryKey(encoded, documentID)
		if err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, nil
}

func rejectReplaceUniqueConflicts(snap *backenddb.Snapshot, catalog *collectionCatalog, runtimes []indexRuntime, state documentIndexState, documentID []byte, batchReplacements batchUniqueReplacementSet) error {
	if snap == nil || catalog == nil {
		return nil
	}
	for _, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		rootID := catalog.rootID(collectionSecondaryRootName(catalog.meta.Name, runtime.def.name))
		if rootID == 0 {
			continue
		}
		for _, encoded := range state[runtime.def.name] {
			_, prefix, err := appendIndexValuePrefixSlice(make([]byte, 0, 2+len(encoded)), encoded)
			if err != nil {
				return err
			}
			it, err := snap.IteratorAtRoot(rootID, prefix, prefixEnd(prefix))
			if errors.Is(err, tree.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			conflict := false
			for it.Valid() {
				key := it.UnsafeKey()
				if !bytes.HasPrefix(key, prefix) {
					break
				}
				ownerID := key[len(prefix):]
				if !it.IsDeleted() && !bytes.Equal(ownerID, documentID) && !batchReplacements.allows(runtime.def.name, encoded, ownerID) {
					conflict = true
					break
				}
				it.Next()
			}
			iterErr := it.Error()
			_ = it.Close()
			if iterErr != nil {
				return iterErr
			}
			if conflict {
				return fmt.Errorf("%w %q", ErrUniqueIndexConflict, runtime.def.name)
			}
		}
	}
	return nil
}

// Get returns an owned copy of the document for documentID.
//
// Missing documents return (nil, nil), matching the existing collection API.
// Present-but-empty documents return a non-nil empty slice.
func (c *Collection) Get(documentID []byte) ([]byte, error) {
	out, found, err := c.GetInto(documentID, nil)
	if err != nil || !found {
		return nil, err
	}
	if len(out) == 0 {
		return []byte{}, nil
	}
	if cap(out) == len(out) {
		return out, nil
	}
	owned := make([]byte, len(out))
	copy(owned, out)
	return owned, nil
}

// NewStoredDocumentJSONMaterializer prepares a reusable materializer for stored
// collection documents. Callers that materialize multiple template-v1 documents
// should reuse one materializer so the backend snapshot and template resolver
// are shared across the request.
func (c *Collection) NewStoredDocumentJSONMaterializer() (*StoredDocumentJSONMaterializer, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	documentFormat, err := normalizeDocumentFormat(c.meta.Options.DocumentFormat)
	if err != nil {
		return nil, err
	}
	switch documentFormat {
	case DocumentFormatJSON, DocumentFormatBSON:
		return &StoredDocumentJSONMaterializer{documentFormat: documentFormat}, nil
	case DocumentFormatTemplateV1:
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		closeOnErr := true
		defer func() {
			if closeOnErr {
				_ = snap.Close()
			}
		}()
		catalog, err := c.catalogForSnapshot(snap)
		if err != nil {
			return nil, err
		}
		if catalog == nil {
			return nil, errCollectionNotFound
		}
		plannerOptions, err := collectionPlannerOptions(c.meta)
		if err != nil {
			return nil, err
		}
		plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
		plannerOptions = collectionOptionsWithBufferedTemplateV1Resolver(plannerOptions, c.writeDomain, c.meta.Name)
		closeOnErr = false
		return &StoredDocumentJSONMaterializer{
			documentFormat:   documentFormat,
			templateResolver: plannerOptions.templateResolver,
			closeFn:          snap.Close,
		}, nil
	default:
		return nil, fmt.Errorf("collections: unsupported document format %q", c.meta.Options.DocumentFormat)
	}
}

// StoredDocumentJSON materializes one stored collection document as JSON bytes.
// JSON-format collections return an owned copy of document. BSON-format
// collections return canonical Extended JSON. Template-v1 collections resolve
// the document's template from the collection template root and any buffered
// template runs.
func (c *Collection) StoredDocumentJSON(document []byte) ([]byte, error) {
	materializer, err := c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return nil, err
	}
	defer func() { _ = materializer.Close() }()
	return materializer.StoredDocumentJSON(document)
}

// GetInto appends the document for documentID into dst[:0].
//
// The returned slice is owned by the caller. Missing documents return
// (dst[:0], false, nil).
func (c *Collection) GetInto(documentID []byte, dst []byte) ([]byte, bool, error) {
	if c == nil {
		return dst[:0], false, errCollectionNil
	}
	if c.db == nil {
		return dst[:0], false, errCollectionDBNil
	}
	if len(documentID) == 0 {
		return dst[:0], false, errors.New("collections: document id cannot be empty")
	}
	if value, buffered, found := c.getBufferedDocumentInto(documentID, dst); buffered {
		return value, found, nil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return dst[:0], false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return dst[:0], false, err
	}
	if catalog == nil {
		return dst[:0], false, errCollectionNotFound
	}
	primaryRoot := catalog.rootID(collectionPrimaryRootName(c.meta.Name))
	if primaryRoot == 0 {
		return dst[:0], false, nil
	}
	out, err := snap.GetAppendAtRoot(primaryRoot, documentID, dst[:0])
	if errors.Is(err, tree.ErrKeyNotFound) {
		return dst[:0], false, nil
	}
	if err != nil {
		return dst[:0], false, err
	}
	return out, true, nil
}

func (c *Collection) getBufferedDocumentInto(documentID []byte, dst []byte) ([]byte, bool, bool) {
	if c == nil || c.writeDomain == nil {
		return nil, false, false
	}
	domain := c.writeDomain
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	if domain.count == 0 {
		return nil, false, false
	}
	table := domain.table
	if len(domain.rootRuns) > 0 {
		name := collectionPrimaryRootName(c.meta.Name)
		if domain.meta.Name != "" {
			name = collectionPrimaryRootName(domain.meta.Name)
		}
		if value, _, flags, found := getBufferedRunEntry(domain.rootRuns[name], documentID); found {
			if flags&node.FlagTombstone != 0 {
				return dst[:0], true, false
			}
			return append(dst[:0], value...), true, true
		}
	}
	if table == nil {
		return nil, false, false
	}
	value, _, flags, found := table.GetEntry(documentID)
	if !found {
		return nil, false, false
	}
	if flags&node.FlagTombstone != 0 {
		return dst[:0], true, false
	}
	return append(dst[:0], value...), true, true
}

func (c *Collection) FindByIndex(indexName, value string) ([][]byte, error) {
	return c.FindByIndexValue(indexName, value)
}

// FindByIndexValue returns document IDs whose named secondary index equals
// value. Supported scalar value types are string, bool, int32, int64, float64,
// json.Number, and nil. If indexName does not exist, it returns nil, nil.
func (c *Collection) FindByIndexValue(indexName string, value any) ([][]byte, error) {
	out, _, err := c.findByIndexValue(indexName, value, 0)
	return out, err
}

// FindByIndexValueLimit is like FindByIndexValue but stops after maxResults
// document IDs and reports whether additional matches were present. If
// indexName does not exist, it returns nil, false, nil.
func (c *Collection) FindByIndexValueLimit(indexName string, value any, maxResults int) ([][]byte, bool, error) {
	if maxResults <= 0 {
		return nil, false, errors.New("collections: max index results must be positive")
	}
	return c.findByIndexValue(indexName, value, maxResults)
}

func (c *Collection) findByIndexValue(indexName string, value any, maxResults int) ([][]byte, bool, error) {
	if c == nil {
		return nil, false, errCollectionNil
	}
	if err := ValidateIndexName(indexName); err != nil {
		return nil, false, err
	}
	if err := c.flushBufferedNoIndex(); err != nil {
		return nil, false, err
	}
	domain := c.writeDomain
	domainLocked := false
	if domain != nil {
		domain.mu.RLock()
		domainLocked = true
		defer func() {
			if domainLocked {
				domain.mu.RUnlock()
			}
		}()
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return nil, false, err
	}
	if catalog == nil {
		return nil, false, errCollectionNotFound
	}
	idx, ok := findIndex(catalog.meta.Indexes, indexName)
	if !ok {
		return nil, false, nil
	}
	var arena []byte
	arena, encoded, err := appendIndexScalar(arena, value)
	if err != nil {
		return nil, false, err
	}
	arena, prefix, err := appendIndexValuePrefixSlice(arena, encoded)
	if err != nil {
		return nil, false, err
	}
	collectLimit := collectionIndexReadCollectLimit(maxResults)
	bufferedIDs, bufferedTruncated, err := c.bufferedIndexIDsLocked(domain, catalog.meta.Name, indexName, prefix, collectLimit)
	if err != nil {
		return nil, false, err
	}
	if domainLocked {
		domain.mu.RUnlock()
		domainLocked = false
	}
	// Buffered indexed writes currently stage inserts only. Primary conflict
	// checks reject IDs that already exist in pending or persisted roots, so
	// buffered secondary IDs cannot duplicate persisted secondary IDs. If update
	// or delete staging is added here, this merge must account for pending
	// tombstones before returning persisted rows.
	rootID := catalog.rootID(collectionSecondaryRootName(catalog.meta.Name, idx.Name))
	if rootID == 0 {
		out, truncated := mergeCollectionIndexIDResults(bufferedIDs, nil, maxResults, bufferedTruncated, false)
		return out, truncated, nil
	}
	it, err := snap.IteratorAtRoot(rootID, prefix, prefixEnd(prefix))
	if errors.Is(err, tree.ErrKeyNotFound) {
		out, truncated := mergeCollectionIndexIDResults(bufferedIDs, nil, maxResults, bufferedTruncated, false)
		return out, truncated, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = it.Close() }()
	persistedIDs, persistedTruncated, err := collectCollectionIndexIDsFromIterator(it, prefix, collectLimit)
	if err != nil {
		return nil, false, err
	}
	out, truncated := mergeCollectionIndexIDResults(bufferedIDs, persistedIDs, maxResults, bufferedTruncated, persistedTruncated)
	return out, truncated, nil
}

func collectionIndexReadCollectLimit(maxResults int) int {
	if maxResults <= 0 {
		return 0
	}
	if maxResults == int(^uint(0)>>1) {
		return 0
	}
	return maxResults + 1
}

func (c *Collection) bufferedIndexIDsLocked(domain *collectionWriteDomain, collectionName, indexName string, prefix []byte, limit int) ([][]byte, bool, error) {
	if c == nil || domain == nil {
		return nil, false, nil
	}
	if domain.count == 0 || len(domain.rootRuns) == 0 {
		return nil, false, nil
	}
	if domain.meta.Name != "" {
		collectionName = domain.meta.Name
	}
	runs := domain.rootRuns[collectionSecondaryRootName(collectionName, indexName)]
	if len(runs) == 0 {
		return nil, false, nil
	}
	it := newBufferedRootRunsIterator(runs, prefix, prefixEnd(prefix))
	defer func() { _ = it.Close() }()
	return collectCollectionIndexIDsFromIterator(it, prefix, limit)
}

func collectCollectionIndexIDsFromIterator(it iterator.UnsafeIterator, prefix []byte, limit int) ([][]byte, bool, error) {
	if it == nil {
		return nil, false, nil
	}
	out := make([][]byte, 0, 1)
	truncated := false
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			if limit > 0 && len(out) >= limit {
				truncated = true
				break
			}
			out = append(out, bytes.Clone(key[len(prefix):]))
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, false, err
	}
	return out, truncated, nil
}

func mergeCollectionIndexIDResults(bufferedIDs, persistedIDs [][]byte, maxResults int, bufferedTruncated, persistedTruncated bool) ([][]byte, bool) {
	total := len(bufferedIDs) + len(persistedIDs)
	limit := collectionIndexReadCollectLimit(maxResults)
	if limit > 0 && total > limit {
		total = limit
	}
	out := make([][]byte, 0, total)
	truncated := bufferedTruncated || persistedTruncated
	i, j := 0, 0
	appendID := func(id []byte) {
		if len(out) > 0 && bytes.Equal(out[len(out)-1], id) {
			return
		}
		out = append(out, id)
	}
	for (i < len(bufferedIDs) || j < len(persistedIDs)) && (limit == 0 || len(out) < limit) {
		switch {
		case i >= len(bufferedIDs):
			appendID(persistedIDs[j])
			j++
		case j >= len(persistedIDs):
			appendID(bufferedIDs[i])
			i++
		default:
			cmp := bytes.Compare(bufferedIDs[i], persistedIDs[j])
			if cmp < 0 {
				appendID(bufferedIDs[i])
				i++
			} else if cmp > 0 {
				appendID(persistedIDs[j])
				j++
			} else {
				appendID(bufferedIDs[i])
				i++
				j++
			}
		}
	}
	if maxResults > 0 && len(out) > maxResults {
		out = out[:maxResults]
		truncated = true
	}
	return out, truncated
}

// ScanDocuments flushes buffered writes before acquiring a snapshot, then scans
// the collection primary root up to maxDocuments. The returned boolean is true
// when additional documents were present beyond the limit.
func (c *Collection) ScanDocuments(maxDocuments int) ([]DocumentRecord, bool, error) {
	out := make([]DocumentRecord, 0)
	truncated, err := c.ScanDocumentsFunc(maxDocuments, func(record DocumentRecord) (bool, error) {
		out = append(out, record)
		return true, nil
	})
	if err != nil {
		return nil, false, err
	}
	return out, truncated, nil
}

// ScanDocumentsFunc flushes buffered writes before acquiring a snapshot, then
// calls fn for primary collection records until maxDocuments is reached, the
// collection is exhausted, or fn returns false. The returned boolean is true
// only when additional documents were present beyond the maxDocuments limit.
func (c *Collection) ScanDocumentsFunc(maxDocuments int, fn func(DocumentRecord) (bool, error)) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	if maxDocuments <= 0 {
		return false, errors.New("collections: max documents must be positive")
	}
	if fn == nil {
		return false, errors.New("collections: scan callback is nil")
	}
	if err := c.flushBufferedWrites(); err != nil {
		return false, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return false, err
	}
	if catalog == nil {
		return false, errCollectionNotFound
	}
	rootID := catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	if rootID == 0 {
		return false, nil
	}
	it, err := snap.IteratorAtRoot(rootID, nil, nil)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = it.Close() }()
	truncated := false
	scanned := 0
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		if scanned >= maxDocuments {
			truncated = true
			break
		}
		record := DocumentRecord{
			ID:       bytes.Clone(it.UnsafeKey()),
			Document: it.ValueCopy(nil),
		}
		scanned++
		next, err := fn(record)
		if err != nil {
			return false, err
		}
		if !next {
			return false, nil
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	return truncated, nil
}

func loadCollectionCatalog(snap *backenddb.Snapshot, name string) (*collectionCatalog, error) {
	raw, ok, err := getSystemValue(snap, systemCollectionMetaKey(name))
	if err != nil || !ok {
		return nil, err
	}
	meta, err := decodeCollectionMeta(raw)
	if err != nil {
		return nil, err
	}
	roots := make(map[string]uint64)
	for _, rootName := range collectionRootNames(meta) {
		rawRoot, ok, err := getSystemValue(snap, systemCollectionRootKey(rootName))
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		rootID, err := decodeRootID(rawRoot)
		if err != nil {
			return nil, fmt.Errorf("collections: root %q: %w", rootName, err)
		}
		roots[rootName] = rootID
	}
	return (&collectionCatalog{meta: meta, roots: roots}).copy(), nil
}

func (c *collectionCatalog) rootID(rootName string) uint64 {
	if c == nil || c.roots == nil {
		return 0
	}
	return c.roots[rootName]
}

func (c *collectionCatalog) copy() *collectionCatalog {
	if c == nil {
		return nil
	}
	roots := make(map[string]uint64, len(c.roots))
	for name, rootID := range c.roots {
		roots[name] = rootID
	}
	return &collectionCatalog{
		meta:  *c.meta.copy(),
		roots: roots,
	}
}

func getSystemValue(snap *backenddb.Snapshot, key string) ([]byte, bool, error) {
	if snap == nil || snap.State() == nil || snap.State().SystemRootPageID == 0 {
		return nil, false, nil
	}
	entry, err := snap.GetEntryAtRoot(snap.State().SystemRootPageID, []byte(key))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return bytes.Clone(entry.Value), true, nil
}

type systemTargetEntry struct {
	key   []byte
	value []byte
}

type systemTargetIterator struct {
	entries []systemTargetEntry
	idx     int
}

func (it *systemTargetIterator) Valid() bool {
	return it != nil && it.idx >= 0 && it.idx < len(it.entries)
}

func (it *systemTargetIterator) Next() {
	if it != nil && it.idx < len(it.entries) {
		it.idx++
	}
}

func (it *systemTargetIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	it.idx = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].key, key) >= 0
	})
}

func (it *systemTargetIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].key
}

func (it *systemTargetIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].value
}

func (it *systemTargetIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline
	}
	return it.entries[it.idx].value, page.ValuePtr{}, node.FlagInline
}

func (it *systemTargetIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *systemTargetIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *systemTargetIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].key...)
}

func (it *systemTargetIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].value...)
}

func (it *systemTargetIterator) IsDeleted() bool {
	return false
}

func (it *systemTargetIterator) Error() error {
	return nil
}

func (it *systemTargetIterator) Close() error {
	return nil
}

func (it *systemTargetIterator) Domain() (start, end []byte) {
	return nil, nil
}

func (it *systemTargetIterator) StableUnsafeIteratorSlices() bool {
	return true
}

func (it *systemTargetIterator) Len() int {
	if it == nil {
		return 0
	}
	return len(it.entries)
}

func buildSystemTargetIterator(snap *backenddb.Snapshot, updates map[string][]byte) (iterator.UnsafeIterator, error) {
	updateEntries := make([]systemTargetEntry, 0, len(updates))
	for key, value := range updates {
		updateEntries = append(updateEntries, systemTargetEntry{
			key:   []byte(key),
			value: bytes.Clone(value),
		})
	}
	sort.Slice(updateEntries, func(i, j int) bool {
		return bytes.Compare(updateEntries[i].key, updateEntries[j].key) < 0
	})

	entries := make([]systemTargetEntry, 0, len(updateEntries))
	updateIdx := 0
	if snap != nil && snap.State() != nil && snap.State().SystemRootPageID != 0 {
		it, err := snap.IteratorAtRoot(snap.State().SystemRootPageID, nil, nil)
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return nil, err
		}
		if err == nil {
			defer func() { _ = it.Close() }()
			for it.Valid() {
				if it.IsDeleted() {
					it.Next()
					continue
				}
				currKey := it.UnsafeKey()
				for updateIdx < len(updateEntries) && bytes.Compare(updateEntries[updateIdx].key, currKey) < 0 {
					entries = append(entries, updateEntries[updateIdx])
					updateIdx++
				}
				if updateIdx < len(updateEntries) && bytes.Equal(updateEntries[updateIdx].key, currKey) {
					entries = append(entries, updateEntries[updateIdx])
					updateIdx++
				} else {
					entries = append(entries, systemTargetEntry{
						key:   bytes.Clone(currKey),
						value: it.ValueCopy(nil),
					})
				}
				it.Next()
			}
			iterErr := it.Error()
			if iterErr != nil {
				return nil, iterErr
			}
		}
	}
	for updateIdx < len(updateEntries) {
		entries = append(entries, updateEntries[updateIdx])
		updateIdx++
	}
	return &systemTargetIterator{entries: entries}, nil
}

func buildSystemDeltaIterator(updates map[string][]byte) (iterator.UnsafeIterator, error) {
	entries := make([]systemTargetEntry, 0, len(updates))
	for key, value := range updates {
		entries = append(entries, systemTargetEntry{
			key:   []byte(key),
			value: bytes.Clone(value),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return &systemTargetIterator{entries: entries}, nil
}

func encodeCollectionMeta(meta CollectionMeta) ([]byte, error) {
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		return nil, err
	}
	return json.Marshal(collectionMetaDisk{
		Version: collectionMetaVersion,
		Name:    normalized.Name,
		Options: normalized.Options,
		Indexes: normalized.Indexes,
	})
}

func decodeCollectionMeta(raw []byte) (CollectionMeta, error) {
	var disk collectionMetaDisk
	if err := json.Unmarshal(raw, &disk); err != nil {
		return CollectionMeta{}, err
	}
	if disk.Version != collectionMetaVersion {
		return CollectionMeta{}, fmt.Errorf("collections: unsupported collection metadata version %d", disk.Version)
	}
	return normalizeCollectionMeta(CollectionMeta{
		Name:    disk.Name,
		Options: disk.Options,
		Indexes: disk.Indexes,
	})
}

func normalizeCollectionMeta(meta CollectionMeta) (CollectionMeta, error) {
	if err := ValidateCollectionName(meta.Name); err != nil {
		return CollectionMeta{}, err
	}
	if _, err := backendRootStoragePolicy(meta.Options.DataRootStoragePolicy); err != nil {
		return CollectionMeta{}, err
	}
	if _, err := backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy); err != nil {
		return CollectionMeta{}, err
	}
	if meta.Options.BufferedIndexedWriteMaxDocuments < 0 {
		return CollectionMeta{}, errors.New("collections: buffered indexed write max documents cannot be negative")
	}
	if meta.Options.BufferedIndexedWriteMaxBytes < 0 {
		return CollectionMeta{}, errors.New("collections: buffered indexed write max bytes cannot be negative")
	}
	documentFormat, err := normalizeDocumentFormat(meta.Options.DocumentFormat)
	if err != nil {
		return CollectionMeta{}, err
	}
	if documentFormat == DocumentFormatJSON {
		meta.Options.DocumentFormat = DocumentFormatDefault
	} else {
		meta.Options.DocumentFormat = documentFormat
	}
	indexes := append([]IndexDefinition(nil), meta.Indexes...)
	sort.SliceStable(indexes, func(i, j int) bool {
		return indexes[i].Name < indexes[j].Name
	})
	seen := make(map[string]struct{}, len(indexes))
	for i := range indexes {
		if err := ValidateIndexName(indexes[i].Name); err != nil {
			return CollectionMeta{}, fmt.Errorf("collections: invalid index name %q: %w", indexes[i].Name, err)
		}
		if err := ValidateIndexPath(indexes[i].Field); err != nil {
			return CollectionMeta{}, fmt.Errorf("collections: invalid index %q field: %w", indexes[i].Name, err)
		}
		if _, err := backendRootStoragePolicy(indexes[i].StoragePolicy); err != nil {
			return CollectionMeta{}, err
		}
		if _, ok := seen[indexes[i].Name]; ok {
			return CollectionMeta{}, fmt.Errorf("collections: duplicate index %q", indexes[i].Name)
		}
		seen[indexes[i].Name] = struct{}{}
	}
	meta.Indexes = indexes
	if len(meta.Indexes) == 0 || meta.Options.DisableIndexedWriteMemtables {
		meta.Options.BufferedIndexedWrites = false
		meta.Options.BufferedIndexedWriteMaxDocuments = 0
		meta.Options.BufferedIndexedWriteMaxBytes = 0
	} else {
		meta.Options.BufferedIndexedWrites = true
		if meta.Options.BufferedIndexedWriteMaxDocuments == 0 && meta.Options.BufferedIndexedWriteMaxBytes == 0 {
			meta.Options.BufferedIndexedWriteMaxDocuments = DefaultIndexedWriteMemtableMaxDocuments
		}
	}
	return meta, nil
}

func (m CollectionMeta) copy() *CollectionMeta {
	return &CollectionMeta{
		Name:    m.Name,
		Options: m.Options,
		Indexes: append([]IndexDefinition(nil), m.Indexes...),
	}
}

func sameCollectionMeta(a, b CollectionMeta) bool {
	na, err := normalizeCollectionMeta(a)
	if err != nil {
		return false
	}
	nb, err := normalizeCollectionMeta(b)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(na, nb)
}

func addIndexToCollectionMeta(meta CollectionMeta, def IndexDefinition) (CollectionMeta, IndexDefinition, error) {
	if _, ok := findIndex(meta.Indexes, def.Name); ok {
		return CollectionMeta{}, IndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	candidate := CollectionMeta{
		Name:    meta.Name,
		Options: meta.Options,
		Indexes: append(append([]IndexDefinition(nil), meta.Indexes...), def),
	}
	normalized, err := normalizeCollectionMeta(candidate)
	if err != nil {
		return CollectionMeta{}, IndexDefinition{}, err
	}
	normalizedDef, ok := findIndex(normalized.Indexes, def.Name)
	if !ok {
		return CollectionMeta{}, IndexDefinition{}, fmt.Errorf("collections: normalized index %q not found", def.Name)
	}
	return normalized, normalizedDef, nil
}

func singleIndexRuntime(def IndexDefinition) (indexRuntime, error) {
	runtimes, err := (insertBatchPlanner{
		indexes: plannerIndexes([]IndexDefinition{def}),
	}).indexRuntimes()
	if err != nil {
		return indexRuntime{}, err
	}
	if len(runtimes) != 1 {
		return indexRuntime{}, errors.New("collections: expected one index runtime")
	}
	return runtimes[0], nil
}

func plannerIndexes(indexes []IndexDefinition) []indexDefinition {
	out := make([]indexDefinition, len(indexes))
	for i, idx := range indexes {
		policy, _ := backendRootStoragePolicy(idx.StoragePolicy)
		out[i] = indexDefinition{
			name:          idx.Name,
			field:         idx.Field,
			unique:        idx.Unique,
			multiKey:      idx.MultiKey,
			storagePolicy: policy,
		}
	}
	return out
}

func uniqueIndexRootIDs(catalog *collectionCatalog) map[string]uint64 {
	if catalog == nil {
		return nil
	}
	out := make(map[string]uint64)
	for _, idx := range catalog.meta.Indexes {
		if !idx.Unique {
			continue
		}
		rootID := catalog.rootID(collectionSecondaryRootName(catalog.meta.Name, idx.Name))
		if rootID != 0 {
			out[idx.Name] = rootID
		}
	}
	return out
}

func findIndex(indexes []IndexDefinition, name string) (IndexDefinition, bool) {
	for _, idx := range indexes {
		if idx.Name == name {
			return idx, true
		}
	}
	return IndexDefinition{}, false
}

func collectionRootNames(meta CollectionMeta) []string {
	out := []string{collectionPrimaryRootName(meta.Name)}
	if normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatTemplateV1 {
		out = append(out, collectionTemplateRootName(meta.Name))
	}
	if len(meta.Indexes) > 0 && persistIndexStateForDocumentFormat(meta.Options.DocumentFormat) {
		out = append(out, collectionIndexStateRootName(meta.Name))
	}
	for _, idx := range meta.Indexes {
		out = append(out, collectionSecondaryRootName(meta.Name, idx.Name))
	}
	return out
}

func collectionPrimaryRootName(collection string) string {
	return collection + "/primary"
}

func collectionTemplateRootName(collection string) string {
	return collection + "/templates"
}

func collectionIndexStateRootName(collection string) string {
	return collection + "/index-state"
}

func collectionSecondaryRootName(collection, indexName string) string {
	return collection + "/index/" + indexName
}

func systemCollectionMetaKey(collection string) string {
	return systemCollectionMetaPrefix + collection
}

func systemCollectionRootKey(rootName string) string {
	return systemCollectionRootPrefix + rootName
}

func encodeRootID(rootID uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, rootID)
	return out
}

func decodeRootID(raw []byte) (uint64, error) {
	if len(raw) != 8 {
		return 0, errors.New("malformed root id")
	}
	return binary.BigEndian.Uint64(raw), nil
}

func ValidateCollectionName(name string) error {
	if len(name) == 0 {
		return errors.New("collection name cannot be empty")
	}
	if len(name) > 128 {
		return errors.New("collection name too long")
	}
	if strings.ContainsAny(name, "\x00/:") {
		return errors.New("collection name contains reserved punctuation")
	}
	if strings.TrimSpace(name) != name {
		return errors.New("collection name has leading or trailing spaces")
	}
	if !utf8.ValidString(name) {
		return errors.New("collection name invalid utf-8")
	}
	return nil
}

func ValidateIndexName(name string) error {
	if len(name) == 0 {
		return errors.New("index name cannot be empty")
	}
	if len(name) > 128 {
		return errors.New("index name too long")
	}
	if strings.ContainsAny(name, "\x00/:") {
		return errors.New("index name contains reserved punctuation")
	}
	if strings.TrimSpace(name) != name {
		return errors.New("index name has leading or trailing spaces")
	}
	if !utf8.ValidString(name) {
		return errors.New("index name invalid utf-8")
	}
	return nil
}

func ValidateIndexPath(path string) error {
	if len(path) == 0 {
		return errors.New("path cannot be empty")
	}
	if strings.Contains(path, "\x00") {
		return errors.New("path cannot contain NUL")
	}
	if strings.HasPrefix(path, ".") || strings.HasSuffix(path, ".") || strings.Contains(path, "..") {
		return errors.New("path cannot contain empty segments")
	}
	return nil
}
