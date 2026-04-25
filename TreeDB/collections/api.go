package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

const collectionMetaVersion = 1

var (
	errCollectionManagerNil = errors.New("collections: collection manager is nil")
	errCollectionNil        = errors.New("collections: collection is nil")
	errCollectionNotFound   = errors.New("collections: collection not found")
)

const (
	systemCollectionMetaPrefix = "collections/meta/"
	systemCollectionRootPrefix = "collections/root/"
)

type CollectionManager struct {
	db *backenddb.DB
}

type Collection struct {
	db   *backenddb.DB
	meta CollectionMeta
}

type CollectionOptions struct {
	AllowArrayValuesInIndex bool `json:"allow_array_values_in_index,omitempty"`
}

type IndexDefinition struct {
	Name     string `json:"name"`
	Field    string `json:"field"`
	Unique   bool   `json:"unique,omitempty"`
	MultiKey bool   `json:"multi_key,omitempty"`
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

func NewCollectionManager(database *backenddb.DB) *CollectionManager {
	return &CollectionManager{db: database}
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
		table, err := buildSystemTargetTable(current, map[string][]byte{
			systemCollectionMetaKey(normalized.Name): encoded,
		})
		if err != nil {
			return nil, err
		}
		return table.NewIterator(nil, nil), nil
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
	return &Collection{
		db:   m.db,
		meta: catalog.meta,
	}, nil
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

func (c *Collection) Insert(id, document []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
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

func (c *Collection) InsertBatch(ids, documents [][]byte) ([][]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errors.New("collections: db is nil")
	}
	if len(documents) == 0 {
		return nil, nil
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
	c.meta = catalog.meta

	planner := insertBatchPlanner{
		collection:     c.meta.Name,
		primaryRoot:    collectionPrimaryRootName(c.meta.Name),
		indexStateRoot: collectionIndexStateRootName(c.meta.Name),
		indexes:        plannerIndexes(c.meta.Indexes),
		options: collectionOptions{
			allowArrayValuesInIndex: c.meta.Options.AllowArrayValuesInIndex,
		},
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
		return cloneIDBatch(plan.resultIDs), nil
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
	}()
	for _, run := range plan.runs {
		iter := run.table.NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot: baseRootIDs[run.name],
			Iter:     iter,
		})
	}

	rootNames := make([]string, len(plan.runs))
	for i, run := range plan.runs {
		rootNames[i] = run.name
	}
	_, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemIterator(rootNames, baseRootIDs, rootIDs)
	})
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(plan.runs) {
		return nil, errors.New("collections: ordered root publish returned unexpected root count")
	}
	return cloneIDBatch(plan.resultIDs), nil
}

func (c *Collection) Delete(documentID []byte) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errors.New("collections: db is nil")
	}
	if len(documentID) == 0 {
		return errors.New("collections: document id cannot be empty")
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		_ = snap.Close()
		return err
	}
	if catalog == nil {
		_ = snap.Close()
		return errCollectionNotFound
	}
	c.meta = catalog.meta

	primaryRoot := catalog.rootID(collectionPrimaryRootName(c.meta.Name))
	if primaryRoot == 0 {
		_ = snap.Close()
		return nil
	}
	entry, err := snap.GetEntryAtRoot(primaryRoot, documentID)
	if errors.Is(err, tree.ErrKeyNotFound) {
		_ = snap.Close()
		return nil
	}
	if err != nil {
		_ = snap.Close()
		return err
	}

	runtimes, err := (insertBatchPlanner{
		collection: c.meta.Name,
		indexes:    plannerIndexes(c.meta.Indexes),
	}).indexRuntimes()
	if err != nil {
		_ = snap.Close()
		return err
	}
	var state documentIndexState
	if len(runtimes) > 0 {
		state, err = loadDeleteIndexState(snap, catalog, documentID, entry.Value, runtimes, collectionOptions{
			allowArrayValuesInIndex: c.meta.Options.AllowArrayValuesInIndex,
		})
		if err != nil {
			_ = snap.Close()
			return err
		}
	}

	rootNames := []string{collectionPrimaryRootName(c.meta.Name)}
	baseRootIDs := map[string]uint64{
		rootNames[0]: primaryRoot,
	}
	deltaTables := make([]memtable.Table, 0, 2+len(runtimes))
	deltaTables = append(deltaTables, buildDeleteRootDeltaTable([][]byte{documentID}))

	if len(runtimes) > 0 {
		stateRootName := collectionIndexStateRootName(c.meta.Name)
		stateRootID := catalog.rootID(stateRootName)
		if stateRootID != 0 {
			rootNames = append(rootNames, stateRootName)
			baseRootIDs[stateRootName] = stateRootID
			deltaTables = append(deltaTables, buildDeleteRootDeltaTable([][]byte{documentID}))
		}
		for _, runtime := range runtimes {
			deleteKeys, err := secondaryDeleteKeysForDocument(runtime, state, documentID)
			if err != nil {
				_ = snap.Close()
				return err
			}
			rootName := collectionSecondaryRootName(c.meta.Name, runtime.def.name)
			rootID := catalog.rootID(rootName)
			if rootID == 0 || len(deleteKeys) == 0 {
				continue
			}
			rootNames = append(rootNames, rootName)
			baseRootIDs[rootName] = rootID
			deltaTables = append(deltaTables, buildDeleteRootDeltaTable(deleteKeys))
		}
	}
	_ = snap.Close()

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(rootNames))
	iterators := make([]iterator.UnsafeIterator, 0, len(rootNames))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
	}()
	for i, rootName := range rootNames {
		iter := deltaTables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot: baseRootIDs[rootName],
			Iter:     iter,
		})
	}
	_, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemIterator(rootNames, baseRootIDs, rootIDs)
	})
	if err != nil {
		return err
	}
	if len(rootIDs) != len(rootNames) {
		return errors.New("collections: ordered root publish returned unexpected root count")
	}
	return nil
}

func buildDeleteRootDeltaTable(deleteKeys [][]byte) memtable.Table {
	table := newCollectionRunTable(len(deleteKeys))
	for _, key := range deleteKeys {
		table.DeleteSteal(bytes.Clone(key))
	}
	table.Freeze()
	return table
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
	table, err := buildSystemTargetTable(current, updates)
	if err != nil {
		return nil, err
	}
	return table.NewIterator(nil, nil), nil
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

func (c *Collection) Get(documentID []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if len(documentID) == 0 {
		return nil, errors.New("collections: document id cannot be empty")
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	primaryRoot := catalog.rootID(collectionPrimaryRootName(c.meta.Name))
	if primaryRoot == 0 {
		return nil, nil
	}
	entry, err := snap.GetEntryAtRoot(primaryRoot, documentID)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return bytes.Clone(entry.Value), nil
}

func (c *Collection) FindByIndex(indexName, value string) ([][]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if err := ValidateIndexName(indexName); err != nil {
		return nil, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	idx, ok := findIndex(catalog.meta.Indexes, indexName)
	if !ok {
		return nil, nil
	}
	rootID := catalog.rootID(collectionSecondaryRootName(catalog.meta.Name, idx.Name))
	if rootID == 0 {
		return nil, nil
	}
	encoded, err := encodeIndexScalar(value)
	if err != nil {
		return nil, err
	}
	prefix, err := indexValuePrefix(encoded)
	if err != nil {
		return nil, err
	}
	it, err := snap.IteratorAtRoot(rootID, prefix, prefixEnd(prefix))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	out := make([][]byte, 0, 1)
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			out = append(out, bytes.Clone(key[len(prefix):]))
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return out, nil
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

func buildSystemTargetTable(snap *backenddb.Snapshot, updates map[string][]byte) (memtable.Table, error) {
	table := memtable.NewHashSorted()
	if snap != nil && snap.State() != nil && snap.State().SystemRootPageID != 0 {
		it, err := snap.IteratorAtRoot(snap.State().SystemRootPageID, nil, nil)
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return nil, err
		}
		if err == nil {
			for it.Valid() {
				if !it.IsDeleted() {
					table.Set(bytes.Clone(it.UnsafeKey()), bytes.Clone(it.UnsafeValue()))
				}
				it.Next()
			}
			iterErr := it.Error()
			_ = it.Close()
			if iterErr != nil {
				return nil, iterErr
			}
		}
	}
	for key, value := range updates {
		table.Set([]byte(key), bytes.Clone(value))
	}
	table.Freeze()
	return table, nil
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

func plannerIndexes(indexes []IndexDefinition) []indexDefinition {
	out := make([]indexDefinition, len(indexes))
	for i, idx := range indexes {
		out[i] = indexDefinition{
			name:     idx.Name,
			field:    idx.Field,
			unique:   idx.Unique,
			multiKey: idx.MultiKey,
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

func cloneIDBatch(ids [][]byte) [][]byte {
	if len(ids) == 0 {
		return nil
	}
	out := make([][]byte, len(ids))
	for i := range ids {
		out[i] = bytes.Clone(ids[i])
	}
	return out
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
