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

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	collectionMetaVersion        = 1
	maxCollectionMutationRetries = 64
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
	errCollectionNotFound   = ErrCollectionNotFound
)

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

type CollectionManager struct {
	db              *backenddb.DB
	closeUnregister func()
	domainMu        sync.Mutex
	domains         map[string]*collectionWriteDomain
}

type Collection struct {
	db                *backenddb.DB
	writeDomain       *collectionWriteDomain
	meta              CollectionMeta
	catalogMu         sync.RWMutex
	catalogSystemRoot uint64
	catalogCommitSeq  uint64
	catalog           *collectionCatalog
	insertStatsMu     sync.RWMutex
	lastInsertStats   CollectionInsertStats
}

// CollectionInsertStats captures phase timings and counters from the most
// recent successful InsertBatch call on a Collection handle.
type CollectionInsertStats struct {
	Documents            int
	Indexes              int
	Runs                 int
	PrepareDocuments     time.Duration
	IndexStateExtraction time.Duration
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
	DocumentFormatTemplateV1 DocumentFormat = "template-v1"
)

type CollectionOptions struct {
	AllowArrayValuesInIndex bool              `json:"allow_array_values_in_index,omitempty"`
	DocumentFormat          DocumentFormat    `json:"document_format,omitempty"`
	DataRootStoragePolicy   RootStoragePolicy `json:"data_root_storage_policy,omitempty"`
	IndexStateStoragePolicy RootStoragePolicy `json:"index_state_storage_policy,omitempty"`
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

type collectionWriteDomain struct {
	mu             sync.RWMutex
	loaded         bool
	meta           CollectionMeta
	catalog        *collectionCatalog
	baseSystemRoot uint64
	primaryRoot    uint64
	storagePolicy  backenddb.OrderedRootStoragePolicy
	table          memtable.Table
	count          int
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

// FlushAll publishes buffered writes for every collection opened through this
// manager. The backend DB also calls this as a close hook while write APIs are
// still available.
func (m *CollectionManager) FlushAll() error {
	if m == nil || m.db == nil {
		return nil
	}
	m.domainMu.Lock()
	domains := make([]*collectionWriteDomain, 0, len(m.domains))
	for _, domain := range m.domains {
		if domain != nil {
			domains = append(domains, domain)
		}
	}
	m.domainMu.Unlock()

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
	domain.mu.Lock()
	defer domain.mu.Unlock()
	return collection.flushBufferedNoIndexLocked(domain)
}

func (m *CollectionManager) CreateCollection(meta *CollectionMeta) (*CollectionMeta, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, errors.New("collections: db is nil")
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
		return nil, errors.New("collections: db is nil")
	}
	if err := ValidateCollectionName(name); err != nil {
		return nil, err
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
		meta:        catalog.meta,
	}
	collection.rememberCatalog(snap, catalog)
	return collection, nil
}

func (m *CollectionManager) ListCollections() ([]CollectionMeta, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, errors.New("collections: db is nil")
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
		return nil, errors.New("collections: db is nil")
	}
	if err := c.flushBufferedNoIndex(); err != nil {
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
	_ = snap.Close()
	if err != nil {
		return nil, err
	}

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
		return nil, errors.New("collections: db is nil")
	}
	if err := c.flushBufferedNoIndex(); err != nil {
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
	if len(newMeta.Indexes) == 0 {
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
		return nil, errors.New("collections: db is nil")
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
		return errors.New("collections: db is nil")
	}
	return c.flushBufferedNoIndex()
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
	currentSystemRoot := uint64(0)
	if state := c.db.State(); state != nil {
		currentSystemRoot = state.SystemRootPageID
	}
	if domain.loaded && domain.count > 0 {
		catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentSystemRoot)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		options, err := collectionPlannerOptions(catalog.meta)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		return catalog, options, len(catalog.meta.Indexes) > 0, nil
	}
	if domain.loaded && domain.count == 0 && domain.baseSystemRoot == currentSystemRoot {
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
	_ = snap.Close()

	options, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		return nil, collectionOptions{}, false, err
	}
	rootName := collectionPrimaryRootName(catalog.meta.Name)
	domain.loaded = true
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.baseSystemRoot = baseSystemRoot
	domain.primaryRoot = catalog.rootID(rootName)
	domain.storagePolicy = options.dataStoragePolicy
	return catalog, options, len(catalog.meta.Indexes) > 0, nil
}

func (c *Collection) revalidateBufferedWriteDomainLocked(domain *collectionWriteDomain, currentSystemRoot uint64) (*collectionCatalog, error) {
	if c == nil || c.db == nil {
		return nil, errors.New("collections: db is nil")
	}
	if domain == nil {
		return nil, errors.New("collections: missing write domain")
	}
	if domain.catalog == nil {
		return nil, errCollectionNotFound
	}
	if domain.baseSystemRoot == currentSystemRoot {
		return domain.catalog, nil
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, domain.meta.Name)
	baseSystemRoot := snapshotSystemRoot(snap)
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

	rootName := collectionPrimaryRootName(domain.meta.Name)
	if rootID := catalog.rootID(rootName); rootID != domain.primaryRoot {
		return nil, fmt.Errorf("collections: concurrent root modification detected for %q", domain.meta.Name)
	}
	options, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		return nil, err
	}
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.baseSystemRoot = baseSystemRoot
	domain.primaryRoot = catalog.rootID(rootName)
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
	return c.flushBufferedNoIndexLocked(domain)
}

func (c *Collection) flushBufferedNoIndexLocked(domain *collectionWriteDomain) error {
	if domain == nil || domain.count == 0 || domain.table == nil {
		return nil
	}
	if domain.catalog == nil {
		return errCollectionNotFound
	}
	currentSystemRoot := uint64(0)
	if state := c.db.State(); state != nil {
		currentSystemRoot = state.SystemRootPageID
	}
	catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentSystemRoot)
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
	baseSystemRoot := domain.baseSystemRoot
	baseRootIDs := map[string]uint64{rootName: baseRoot}
	table := domain.table
	iter := table.NewIterator(nil, nil)

	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: domain.storagePolicy,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseSystemRoot, []string{rootName}, baseRootIDs, rootIDs)
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
	domain.baseSystemRoot = newSystemRoot
	domain.primaryRoot = rootIDs[0]
	domain.table = newCollectionRunTable(0)
	domain.count = 0
	c.meta = meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	resetCollectionRunTable(table)
	return nil
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
	_ = snap.Close()

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
		return c.buildRootDescriptorSystemDeltaIterator(baseSystemRoot, []string{rootName}, map[string]uint64{rootName: baseRoot}, rootIDs)
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
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errors.New("collections: db is nil")
	}
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
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseSystemRoot := snapshotSystemRoot(snap)

	if len(c.meta.Indexes) == 0 && plannerOptions.documentFormat == DocumentFormatJSON {
		return c.insertBatchNoIndex(catalog, snap, baseSystemRoot, plannerOptions, ids, documents)
	}

	planner := insertBatchPlanner{
		collection:     c.meta.Name,
		primaryRoot:    collectionPrimaryRootName(c.meta.Name),
		templateRoot:   collectionTemplateRootName(c.meta.Name),
		indexStateRoot: collectionIndexStateRootName(c.meta.Name),
		indexes:        plannerIndexes(c.meta.Indexes),
		options:        plannerOptions,
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
	if len(plan.runs) == 0 {
		_ = snap.Close()
		c.setLastInsertStats(plan.stats.CollectionInsertStats)
		return plan.resultIDs, nil
	}

	baseRootIDs := make(map[string]uint64, len(plan.runs))
	for _, run := range plan.runs {
		baseRootIDs[run.name] = catalog.rootID(run.name)
	}
	_ = snap.Close()

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
		return c.buildRootDescriptorSystemDeltaIterator(baseSystemRoot, rootNames, baseRootIDs, rootIDs)
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

func (c *Collection) insertBatchNoIndex(
	catalog *collectionCatalog,
	snap *backenddb.Snapshot,
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
	_ = snap.Close()

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
		return c.buildRootDescriptorSystemDeltaIterator(baseSystemRoot, []string{rootName}, baseRootIDs, rootIDs)
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
		return false, errors.New("collections: db is nil")
	}
	if len(documentID) == 0 {
		return false, errors.New("collections: document id cannot be empty")
	}
	if err := c.flushBufferedNoIndex(); err != nil {
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
		stateRootName := collectionIndexStateRootName(c.meta.Name)
		stateRootID := catalog.rootID(stateRootName)
		if stateRootID != 0 {
			rootNames = append(rootNames, stateRootName)
			baseRootIDs[stateRootName] = stateRootID
			policies = append(policies, plannerOptions.indexStateStoragePolicy)
			deltaTables = append(deltaTables, buildDeleteRootDeltaTable([][]byte{documentID}))
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
		return c.buildRootDescriptorSystemDeltaIterator(baseSystemRoot, rootNames, baseRootIDs, rootIDs)
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
		return false, false, errors.New("collections: db is nil")
	}
	if len(documentID) == 0 {
		return false, false, errors.New("collections: document id cannot be empty")
	}
	if update == nil {
		return false, false, errors.New("collections: update function is nil")
	}
	if err := c.flushBufferedNoIndex(); err != nil {
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
		if err := rejectReplaceUniqueConflicts(snap, catalog, runtimes, newState, documentID); err != nil {
			_ = snap.Close()
			return false, false, err
		}
	}

	rootNames := []string{collectionPrimaryRootName(c.meta.Name)}
	baseRootIDs := map[string]uint64{
		rootNames[0]: primaryRoot,
	}
	policies := []backenddb.OrderedRootStoragePolicy{plannerOptions.dataStoragePolicy}
	deltaTables := make([]memtable.Table, 0, 2+len(runtimes))
	primaryTable := newCollectionRunTable(1)
	setCollectionRunValue(primaryTable, bytes.Clone(documentID), document)
	primaryTable.Freeze()
	deltaTables = append(deltaTables, primaryTable)

	if len(runtimes) > 0 {
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
		return c.buildRootDescriptorSystemDeltaIterator(baseSystemRoot, rootNames, baseRootIDs, rootIDs)
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

	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return nil, err
	}
	c.rememberCatalog(snap, catalog)
	return catalog, nil
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

func (c *Collection) rememberCatalog(snap *backenddb.Snapshot, catalog *collectionCatalog) {
	if c == nil || snap == nil || catalog == nil {
		return
	}
	systemRoot := snapshotSystemRoot(snap)
	commitSeq := snapshotCommitSeq(snap)
	c.catalogMu.Lock()
	c.catalogSystemRoot = systemRoot
	c.catalogCommitSeq = commitSeq
	c.catalog = catalog
	c.catalogMu.Unlock()
}

func (c *Collection) rememberCatalogAtSystemRoot(systemRoot uint64, catalog *collectionCatalog) {
	if c == nil || catalog == nil {
		return
	}
	c.catalogMu.Lock()
	c.catalogSystemRoot = systemRoot
	c.catalogCommitSeq = c.commitSeqForSystemRoot(systemRoot)
	c.catalog = catalog
	c.catalogMu.Unlock()
}

func (c *Collection) commitSeqForSystemRoot(systemRoot uint64) uint64 {
	if c == nil || c.db == nil {
		return 0
	}
	state := c.db.State()
	if state == nil || state.SystemRootPageID != systemRoot {
		return 0
	}
	return state.CommitSeq
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
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.baseSystemRoot = systemRoot
	domain.primaryRoot = catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	domain.storagePolicy = options.dataStoragePolicy
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
	return &collectionCatalog{
		meta:  meta,
		roots: roots,
	}
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

	indexStateTable := newCollectionRunTable(0)
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
		existingState, err := loadBackfillIndexState(snap, stateRootID, documentID, document, existingRuntimes, opts)
		if err != nil {
			return nil, err
		}
		merged := cloneDocumentIndexState(existingState)
		values := newState[newRuntime.def.name]
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
	if stateRootID != 0 {
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

func (c *Collection) buildRootDescriptorSystemDeltaIterator(expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	if err := c.validateRootDescriptorSystemDelta(expectedSystemRoot, rootNames, baseRootIDs); err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, len(rootNames))
	for i, rootName := range rootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemDeltaIterator(updates)
}

func (c *Collection) validateRootDescriptorSystemDelta(expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64) error {
	currentSystemRoot := uint64(0)
	if c != nil && c.db != nil {
		if state := c.db.State(); state != nil {
			currentSystemRoot = state.SystemRootPageID
		}
	}
	if currentSystemRoot != expectedSystemRoot {
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
	if stateRoot != 0 {
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

func rejectReplaceUniqueConflicts(snap *backenddb.Snapshot, catalog *collectionCatalog, runtimes []indexRuntime, state documentIndexState, documentID []byte) error {
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
				if !it.IsDeleted() && !bytes.Equal(key[len(prefix):], documentID) {
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

// GetInto appends the document for documentID into dst[:0].
//
// The returned slice is owned by the caller. Missing documents return
// (dst[:0], false, nil).
func (c *Collection) GetInto(documentID []byte, dst []byte) ([]byte, bool, error) {
	if c == nil {
		return dst[:0], false, errCollectionNil
	}
	if c.db == nil {
		return dst[:0], false, errors.New("collections: db is nil")
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
	if domain.count == 0 || domain.table == nil {
		return nil, false, false
	}
	value, _, flags, found := domain.table.GetEntry(documentID)
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
	rootID := catalog.rootID(collectionSecondaryRootName(catalog.meta.Name, idx.Name))
	if rootID == 0 {
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
	it, err := snap.IteratorAtRoot(rootID, prefix, prefixEnd(prefix))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = it.Close() }()
	out := make([][]byte, 0, 1)
	truncated := false
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			if maxResults > 0 && len(out) >= maxResults {
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

// ScanDocuments flushes buffered no-index writes before acquiring a snapshot,
// then scans the collection primary root up to maxDocuments. The returned
// boolean is true when additional documents were present beyond the limit.
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

// ScanDocumentsFunc flushes buffered no-index writes before acquiring a
// snapshot, then calls fn for primary collection records until maxDocuments is
// reached, the collection is exhausted, or fn returns false. The returned
// boolean is true only when additional documents were present beyond the
// maxDocuments limit.
func (c *Collection) ScanDocumentsFunc(maxDocuments int, fn func(DocumentRecord) (bool, error)) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errors.New("collections: db is nil")
	}
	if maxDocuments <= 0 {
		return false, errors.New("collections: max documents must be positive")
	}
	if fn == nil {
		return false, errors.New("collections: scan callback is nil")
	}
	if err := c.flushBufferedNoIndex(); err != nil {
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
	return &collectionCatalog{meta: meta, roots: roots}, nil
}

func (c *collectionCatalog) rootID(rootName string) uint64 {
	if c == nil || c.roots == nil {
		return 0
	}
	return c.roots[rootName]
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
	if len(meta.Indexes) > 0 {
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
