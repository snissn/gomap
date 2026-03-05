package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

const (
	autoIDBytesLen = 8
)

var (
	errCollectionManagerNil = errors.New("collections: collection manager is nil")
	errCollectionNil        = errors.New("collections: collection is nil")
	errCollectionNotFound   = errors.New("collections: collection not found")
)

type autoIDState struct {
	lastID      uint64
	initialized bool
}

type CollectionManager struct {
	db      collectionDB
	seqMu   sync.Mutex
	autoSeq map[string]autoIDState
	epoch   atomic.Uint64
}

type Collection struct {
	db        collectionDB
	mgr       *CollectionManager
	meta      CollectionMeta
	metaEpoch uint64
}

type collectionDB interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	GetSystem(key []byte) ([]byte, error)
	SetSystem(key, value []byte) error
	NewBatch() batch.Interface
	NewSystemBatch() batch.Interface
	Iterator(start, end []byte) (systemIterator, error)
	SystemIterator(start, end []byte) (systemIterator, error)
}

type systemIterator = iterator.UnsafeIterator

func NewCollectionManager(database collectionDB) *CollectionManager {
	return &CollectionManager{
		db:      database,
		autoSeq: make(map[string]autoIDState),
	}
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
	if err := meta.normalizeAndValidate(); err != nil {
		return nil, err
	}

	key, err := SystemCollectionMetaKey(meta.Name)
	if err != nil {
		return nil, err
	}
	existingRaw, err := m.db.GetSystem(key)
	if err != nil {
		return nil, err
	}
	if len(existingRaw) > 0 {
		var existing CollectionMeta
		if err := existing.Decode(existingRaw); err != nil {
			return nil, err
		}
		if !sameCollectionDefinition(meta, &existing) {
			return nil, fmt.Errorf("collections: existing schema for %q is incompatible", meta.Name)
		}
		return existing.copy(), nil
	}

	encoded, err := meta.Encode()
	if err != nil {
		return nil, err
	}
	sysBatch := m.db.NewSystemBatch()
	if sysBatch == nil {
		return nil, fmt.Errorf("collections: failed to create system batch")
	}
	defer func() { _ = sysBatch.Close() }()
	if err := sysBatch.Set(key, encoded); err != nil {
		return nil, err
	}
	primaryRootDesc, err := newPrimaryCollectionRootDescriptor(meta)
	if err != nil {
		return nil, err
	}
	if err := setCollectionRootDescriptorOnBatch(sysBatch, primaryRootDesc); err != nil {
		return nil, err
	}
	for i := range meta.Indexes {
		indexKey, err := SystemIndexKey(meta.Name, meta.Indexes[i].Name)
		if err != nil {
			return nil, err
		}
		encodedDef := make([]byte, 0, 128)
		if err := encodeIndexDefinition(&encodedDef, &meta.Indexes[i]); err != nil {
			return nil, err
		}
		if err := sysBatch.Set(indexKey, encodedDef); err != nil {
			return nil, err
		}
		rootDesc, err := newSecondaryCollectionRootDescriptor(meta.Name, &meta.Indexes[i])
		if err != nil {
			return nil, err
		}
		if err := setCollectionRootDescriptorOnBatch(sysBatch, rootDesc); err != nil {
			return nil, err
		}
	}
	if err := sysBatch.Write(); err != nil {
		return nil, err
	}
	m.epoch.Add(1)
	result := meta.copy()
	result.SetDefaults()
	return result, nil
}

func (m *CollectionManager) OpenCollection(name string) (*Collection, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	meta, err := m.getCollection(name)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, errCollectionNotFound
	}
	return &Collection{
		db:        m.db,
		mgr:       m,
		meta:      *meta,
		metaEpoch: m.epoch.Load(),
	}, nil
}

func (m *CollectionManager) ListCollections() ([]CollectionMeta, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}

	iterator, err := m.db.SystemIterator(SystemCollectionMetaPrefix(), nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = iterator.Close() }()

	var out []CollectionMeta
	prefix := SystemCollectionMetaPrefix()
	for iterator.Valid() {
		key := iterator.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if iterator.IsDeleted() {
			iterator.Next()
			continue
		}
		value := iterator.UnsafeValue()
		var meta CollectionMeta
		if err := meta.Decode(value); err != nil {
			return nil, err
		}
		out = append(out, meta)
		iterator.Next()
	}
	if err := iterator.Error(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (m *CollectionManager) DropCollection(name string) error {
	if m == nil {
		return errCollectionManagerNil
	}
	if m.db == nil {
		return errors.New("collections: db is nil")
	}
	if err := ValidateCollectionName(name); err != nil {
		return err
	}

	metaKey, err := SystemCollectionMetaKey(name)
	if err != nil {
		return err
	}
	existing, err := m.db.GetSystem(metaKey)
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		m.clearSequenceState(name)
		return nil
	}
	var existingMeta CollectionMeta
	if err := existingMeta.Decode(existing); err != nil {
		return err
	}

	keys := make([][]byte, 0, 8)
	keys = append(keys, metaKey)
	if rootKey, err := SystemCollectionRootKey(existingMeta.PrimaryRoot); err == nil {
		keys = append(keys, rootKey)
	}
	for _, idx := range existingMeta.Indexes {
		if rootKey, err := SystemCollectionRootKey(idx.RootName); err == nil {
			keys = append(keys, rootKey)
		}
	}

	if seqKey, err := SystemCollectionIDSequenceKey(name); err == nil {
		keys = append(keys, seqKey)
	}

	if idxPrefix, err := SystemIndexPrefix(name); err == nil {
		it, err := m.db.SystemIterator(idxPrefix, nil)
		if err != nil {
			return err
		}
		for it.Valid() {
			key := it.UnsafeKey()
			if !bytes.HasPrefix(key, idxPrefix) {
				break
			}
			if !it.IsDeleted() {
				keys = append(keys, append([]byte{}, key...))
			}
			it.Next()
		}
		err = it.Error()
		_ = it.Close()
		if err != nil {
			return err
		}
	}

	if dataPrefix, err := SystemCollectionPrefix(name); err == nil {
		it, err := m.db.Iterator(dataPrefix, nil)
		if err == nil {
			for it.Valid() {
				key := it.UnsafeKey()
				if !bytes.HasPrefix(key, dataPrefix) {
					break
				}
				if !it.IsDeleted() {
					keys = append(keys, append([]byte{}, key...))
				}
				it.Next()
			}
			err = it.Error()
			_ = it.Close()
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	if indexDataPrefix, err := CollectionIndexDataPrefix(name); err == nil {
		it, err := m.db.Iterator(indexDataPrefix, nil)
		if err != nil {
			return err
		}
		for it.Valid() {
			key := it.UnsafeKey()
			if !bytes.HasPrefix(key, indexDataPrefix) {
				break
			}
			if !it.IsDeleted() {
				keys = append(keys, append([]byte{}, key...))
			}
			it.Next()
		}
		err = it.Error()
		_ = it.Close()
		if err != nil {
			return err
		}
	}

	b := m.db.NewSystemBatch()
	if b == nil {
		return fmt.Errorf("collections: failed to create system batch")
	}
	defer func() {
		if err := b.Close(); err != nil {
			// best effort
		}
	}()
	for _, key := range keys {
		if err := b.Delete(key); err != nil {
			return err
		}
	}
	if err := b.Write(); err != nil {
		return err
	}
	m.epoch.Add(1)
	m.clearSequenceState(name)
	return nil
}

func (m *CollectionManager) getCollection(name string) (*CollectionMeta, error) {
	if err := ValidateCollectionName(name); err != nil {
		return nil, err
	}
	key, err := SystemCollectionMetaKey(name)
	if err != nil {
		return nil, err
	}
	raw, err := m.db.GetSystem(key)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var meta CollectionMeta
	if err := meta.Decode(raw); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (m *CollectionManager) allocateAutoID(name string) ([]byte, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if err := ValidateCollectionName(name); err != nil {
		return nil, err
	}
	seqKey, err := SystemCollectionIDSequenceKey(name)
	if err != nil {
		return nil, err
	}

	m.seqMu.Lock()
	defer m.seqMu.Unlock()

	state, ok := m.autoSeq[name]
	if !ok {
		state = autoIDState{}
	}
	if !state.initialized {
		encoded, err := m.db.GetSystem(seqKey)
		if err != nil {
			return nil, err
		}
		var last uint64
		if len(encoded) > 0 {
			if len(encoded) != autoIDBytesLen {
				return nil, errors.New("collections: corrupted auto-id sequence")
			}
			last = binary.BigEndian.Uint64(encoded)
		}
		state.lastID = last
		state.initialized = true
	}
	nextID := state.lastID + 1
	encodedNext := make([]byte, autoIDBytesLen)
	binary.BigEndian.PutUint64(encodedNext, nextID)
	if err := m.db.SetSystem(seqKey, encodedNext); err != nil {
		return nil, err
	}
	state.lastID = nextID
	m.autoSeq[name] = state
	out := make([]byte, autoIDBytesLen)
	copy(out, encodedNext)
	return out, nil
}

func (m *CollectionManager) clearSequenceState(name string) {
	m.seqMu.Lock()
	defer m.seqMu.Unlock()
	delete(m.autoSeq, name)
}

func sameCollectionDefinition(a, b *CollectionMeta) bool {
	if a == nil || b == nil {
		return false
	}
	if a.Name != b.Name {
		return false
	}
	if a.Options != b.Options {
		return false
	}
	if len(a.Indexes) != len(b.Indexes) {
		return false
	}
	for i := range a.Indexes {
		if a.Indexes[i] != b.Indexes[i] {
			return false
		}
	}
	return true
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
	return c.meta
}

func (c *Collection) AllocateID(id []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	switch c.meta.Options.IDMode {
	case idModeCallerProvided:
		if len(id) == 0 {
			return nil, fmt.Errorf("collections: caller-provided id cannot be empty")
		}
		out := make([]byte, len(id))
		copy(out, id)
		return out, nil
	case idModeAuto:
		if c.mgr == nil {
			return nil, errCollectionManagerNil
		}
		return c.mgr.allocateAutoID(c.meta.Name)
	default:
		return nil, fmt.Errorf("collections: unsupported id mode %d", c.meta.Options.IDMode)
	}
}

func (c *Collection) Insert(id, document []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if err := c.refreshMeta(); err != nil {
		return nil, err
	}
	persistedID, err := c.AllocateID(id)
	if err != nil {
		return nil, err
	}
	key, err := c.documentKey(persistedID)
	if err != nil {
		return nil, err
	}
	if c.db == nil {
		return nil, errCollectionManagerNil
	}
	existing, err := c.db.Get(key)
	if err != nil {
		return nil, err
	}
	removals, additions, err := c.indexMutationForUpsert(persistedID, existing, document)
	if err != nil {
		return nil, err
	}
	if len(removals) == 0 && len(additions) == 0 {
		if err := c.db.Set(key, document); err != nil {
			return nil, err
		}
		return persistedID, nil
	}
	b := c.db.NewBatch()
	if b == nil {
		return nil, fmt.Errorf("collections: failed to create batch")
	}
	defer func() { _ = b.Close() }()
	if err := setDocumentOnBatch(b, key, document); err != nil {
		return nil, err
	}
	for _, idxKey := range removals {
		if err := b.Delete(idxKey); err != nil {
			return nil, err
		}
	}
	for _, idxKey := range additions {
		if err := b.Set(idxKey, nil); err != nil {
			return nil, err
		}
	}
	if err := b.Write(); err != nil {
		return nil, err
	}
	return persistedID, nil
}

func (c *Collection) Get(documentID []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if err := c.refreshMeta(); err != nil {
		return nil, err
	}
	key, err := c.documentKey(documentID)
	if err != nil {
		return nil, err
	}
	if c.db == nil {
		return nil, errCollectionManagerNil
	}
	return c.db.Get(key)
}

func (c *Collection) Delete(documentID []byte) error {
	if c == nil {
		return errCollectionNil
	}
	if err := c.refreshMeta(); err != nil {
		return err
	}
	if err := requireDocumentID(documentID); err != nil {
		return err
	}
	key, err := c.documentKey(documentID)
	if err != nil {
		return err
	}
	if c.db == nil {
		return errCollectionManagerNil
	}
	existing, err := c.db.Get(key)
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		return c.db.Delete(key)
	}
	entries, err := c.indexEntriesForDocument(documentID, existing)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return c.db.Delete(key)
	}
	b := c.db.NewBatch()
	if b == nil {
		return fmt.Errorf("collections: failed to create batch")
	}
	defer func() { _ = b.Close() }()
	if err := b.Delete(key); err != nil {
		return err
	}
	for _, idxKey := range entries {
		if err := b.Delete(idxKey); err != nil {
			return err
		}
	}
	return b.Write()
}

func (c *Collection) Upsert(documentID, document []byte) ([]byte, error) {
	return c.Insert(documentID, document)
}

func (c *Collection) documentKey(documentID []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionManagerNil
	}
	if err := requireDocumentID(documentID); err != nil {
		return nil, err
	}
	prefix, err := CollectionDataPrefix(c.meta.Name)
	if err != nil {
		return nil, err
	}
	key := make([]byte, 0, len(prefix)+len(documentID))
	key = append(key, prefix...)
	key = append(key, documentID...)
	return key, nil
}

func requireDocumentID(documentID []byte) error {
	if len(documentID) == 0 {
		return errors.New("collections: document id cannot be empty")
	}
	return nil
}

func (m *CollectionManager) CreateIndex(collection string, def IndexDefinition) (*IndexDefinition, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if err := ValidateCollectionName(collection); err != nil {
		return nil, err
	}
	if err := ValidateIndexName(def.Name); err != nil {
		return nil, err
	}
	if err := ValidateIndexPath(def.Field); err != nil {
		return nil, err
	}
	if def.RootName == "" {
		rootName, err := CollectionIndexRootName(collection, def.Name)
		if err != nil {
			return nil, err
		}
		def.RootName = rootName
	}
	if err := ValidateRootName(def.RootName); err != nil {
		return nil, err
	}
	meta, err := m.getCollection(collection)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, errCollectionNotFound
	}
	for _, existing := range meta.Indexes {
		if existing.Name == def.Name {
			if existing == def {
				cp := existing
				return &cp, nil
			}
			return nil, fmt.Errorf("collections: index %q already exists", def.Name)
		}
	}
	meta.Indexes = append(meta.Indexes, def)
	encodedMeta, err := meta.Encode()
	if err != nil {
		return nil, err
	}
	sysBatch := m.db.NewSystemBatch()
	if sysBatch == nil {
		return nil, fmt.Errorf("collections: failed to create system batch")
	}
	defer func() { _ = sysBatch.Close() }()
	metaKey, err := SystemCollectionMetaKey(collection)
	if err != nil {
		return nil, err
	}
	if err := sysBatch.Set(metaKey, encodedMeta); err != nil {
		return nil, err
	}
	indexKey, err := SystemIndexKey(collection, def.Name)
	if err != nil {
		return nil, err
	}
	encodedDef := make([]byte, 0, 128)
	if err := encodeIndexDefinition(&encodedDef, &def); err != nil {
		return nil, err
	}
	if err := sysBatch.Set(indexKey, encodedDef); err != nil {
		return nil, err
	}
	rootDesc, err := newSecondaryCollectionRootDescriptor(collection, &def)
	if err != nil {
		return nil, err
	}
	if err := setCollectionRootDescriptorOnBatch(sysBatch, rootDesc); err != nil {
		return nil, err
	}
	if err := sysBatch.Write(); err != nil {
		return nil, err
	}
	m.epoch.Add(1)
	if err := m.backfillIndex(collection, def); err != nil {
		return nil, err
	}
	cp := def
	return &cp, nil
}

func (m *CollectionManager) DropIndex(collection, indexName string) error {
	if m == nil {
		return errCollectionManagerNil
	}
	if err := ValidateCollectionName(collection); err != nil {
		return err
	}
	if err := ValidateIndexName(indexName); err != nil {
		return err
	}
	meta, err := m.getCollection(collection)
	if err != nil {
		return err
	}
	if meta == nil {
		return errCollectionNotFound
	}
	next := make([]IndexDefinition, 0, len(meta.Indexes))
	found := false
	var removed IndexDefinition
	for _, idx := range meta.Indexes {
		if idx.Name == indexName {
			found = true
			removed = idx
			continue
		}
		next = append(next, idx)
	}
	if !found {
		return nil
	}
	meta.Indexes = next
	encodedMeta, err := meta.Encode()
	if err != nil {
		return err
	}
	sysBatch := m.db.NewSystemBatch()
	if sysBatch == nil {
		return fmt.Errorf("collections: failed to create system batch")
	}
	defer func() { _ = sysBatch.Close() }()
	metaKey, err := SystemCollectionMetaKey(collection)
	if err != nil {
		return err
	}
	if err := sysBatch.Set(metaKey, encodedMeta); err != nil {
		return err
	}
	indexKey, err := SystemIndexKey(collection, indexName)
	if err != nil {
		return err
	}
	if err := sysBatch.Delete(indexKey); err != nil {
		return err
	}
	if rootKey, err := SystemCollectionRootKey(removed.RootName); err == nil {
		if err := sysBatch.Delete(rootKey); err != nil {
			return err
		}
	}
	if err := sysBatch.Write(); err != nil {
		return err
	}
	m.epoch.Add(1)
	prefix, err := CollectionIndexPrefix(collection, indexName)
	if err != nil {
		return err
	}
	it, err := m.db.Iterator(prefix, nil)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	b := m.db.NewBatch()
	if b == nil {
		return fmt.Errorf("collections: failed to create batch")
	}
	defer func() { _ = b.Close() }()
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			if err := b.Delete(append([]byte{}, key...)); err != nil {
				return err
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return b.Write()
}

func (c *Collection) FindByIndex(indexName, value string) ([][]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if err := c.refreshMeta(); err != nil {
		return nil, err
	}
	if err := ValidateIndexName(indexName); err != nil {
		return nil, err
	}
	idxDef, ok := c.indexByName(indexName)
	if !ok {
		return nil, nil
	}
	encodedValue, err := encodeIndexScalar(value)
	if err != nil {
		return nil, err
	}
	prefix, err := indexValuePrefix(c.meta.Name, idxDef.Name, encodedValue)
	if err != nil {
		return nil, err
	}
	it, err := c.db.Iterator(prefix, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	out := make([][]byte, 0, 8)
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			docID, err := parseIndexEntryDocID(c.meta.Name, idxDef.Name, key)
			if err != nil {
				return nil, err
			}
			out = append(out, docID)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i], out[j]) < 0 })
	return out, nil
}

func (m *CollectionManager) backfillIndex(collection string, def IndexDefinition) error {
	dataPrefix, err := CollectionDataPrefix(collection)
	if err != nil {
		return err
	}
	it, err := m.db.Iterator(dataPrefix, nil)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	b := m.db.NewBatch()
	if b == nil {
		return fmt.Errorf("collections: failed to create batch")
	}
	defer func() { _ = b.Close() }()
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, dataPrefix) {
			break
		}
		if !it.IsDeleted() {
			docID := append([]byte{}, key[len(dataPrefix):]...)
			indexKeys, err := indexKeysForSingleDefinition(collection, docID, it.UnsafeValue(), def, DefaultCollectionOptions())
			if err != nil {
				return err
			}
			for _, idxKey := range indexKeys {
				if err := b.Set(idxKey, nil); err != nil {
					return err
				}
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return b.Write()
}

func (c *Collection) indexMutationForUpsert(documentID, oldDoc, newDoc []byte) ([][]byte, [][]byte, error) {
	oldEntries, err := c.indexEntriesForDocument(documentID, oldDoc)
	if err != nil {
		return nil, nil, err
	}
	newEntries, err := c.indexEntriesForDocument(documentID, newDoc)
	if err != nil {
		return nil, nil, err
	}
	oldSet := make(map[string][]byte, len(oldEntries))
	for _, key := range oldEntries {
		oldSet[string(key)] = key
	}
	newSet := make(map[string][]byte, len(newEntries))
	for _, key := range newEntries {
		newSet[string(key)] = key
	}
	removals := make([][]byte, 0, len(oldEntries))
	for k, key := range oldSet {
		if _, keep := newSet[k]; !keep {
			removals = append(removals, key)
		}
	}
	additions := make([][]byte, 0, len(newEntries))
	for k, key := range newSet {
		if _, exists := oldSet[k]; !exists {
			additions = append(additions, key)
		}
	}
	if err := c.ensureUniqueConflicts(documentID, additions); err != nil {
		return nil, nil, err
	}
	return removals, additions, nil
}

func (c *Collection) ensureUniqueConflicts(documentID []byte, additions [][]byte) error {
	if len(additions) == 0 {
		return nil
	}
	for _, key := range additions {
		indexName, valuePrefix, err := parseIndexEntryForConflict(c.meta.Name, key)
		if err != nil {
			return err
		}
		idx, ok := c.indexByName(indexName)
		if !ok || !idx.Unique {
			continue
		}
		it, err := c.db.Iterator(valuePrefix, nil)
		if err != nil {
			return err
		}
		for it.Valid() {
			existing := it.UnsafeKey()
			if !bytes.HasPrefix(existing, valuePrefix) {
				break
			}
			if !it.IsDeleted() {
				existingID, err := parseIndexEntryDocID(c.meta.Name, idx.Name, existing)
				if err != nil {
					_ = it.Close()
					return err
				}
				if !bytes.Equal(existingID, documentID) {
					_ = it.Close()
					return fmt.Errorf("collections: unique index %q conflict", idx.Name)
				}
			}
			it.Next()
		}
		err = it.Error()
		_ = it.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Collection) indexEntriesForDocument(documentID, document []byte) ([][]byte, error) {
	if len(document) == 0 || len(c.meta.Indexes) == 0 {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(document, &decoded); err != nil {
		return nil, fmt.Errorf("collections: index extraction requires JSON document: %w", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("collections: index extraction requires JSON object document")
	}
	out := make([][]byte, 0, len(c.meta.Indexes))
	seen := make(map[string]struct{}, len(c.meta.Indexes))
	for _, idx := range c.meta.Indexes {
		keys, err := indexKeysForSingleDefinition(c.meta.Name, documentID, obj, idx, c.meta.Options)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			s := string(key)
			if _, exists := seen[s]; exists {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, key)
		}
	}
	return out, nil
}

func indexKeysForSingleDefinition(collection string, documentID []byte, document any, idx IndexDefinition, opts CollectionOptions) ([][]byte, error) {
	value, found := extractIndexPathValue(document, idx.Field)
	if !found || value == nil {
		return nil, nil
	}
	values, err := normalizeIndexValues(value, idx.MultiKey, opts.AllowArrayValuesInIndex)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(values))
	for _, v := range values {
		encoded, err := encodeIndexScalar(v)
		if err != nil {
			return nil, err
		}
		key, err := buildIndexEntryKey(collection, idx.Name, encoded, documentID)
		if err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, nil
}

func normalizeIndexValues(value any, multiKey, allowArray bool) ([]any, error) {
	if arr, ok := value.([]any); ok {
		if !multiKey && !allowArray {
			return nil, fmt.Errorf("collections: array value not allowed for index")
		}
		if len(arr) == 0 {
			return nil, nil
		}
		out := make([]any, 0, len(arr))
		for _, elem := range arr {
			out = append(out, elem)
		}
		return out, nil
	}
	return []any{value}, nil
}

func extractIndexPathValue(document any, path string) (any, bool) {
	if path == "" {
		return nil, false
	}
	current := document
	for _, segment := range strings.Split(path, ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := obj[segment]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func encodeIndexScalar(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return append([]byte("s:"), []byte(v)...), nil
	case bool:
		if v {
			return []byte("b:1"), nil
		}
		return []byte("b:0"), nil
	case float64:
		return []byte("n:" + strconv.FormatFloat(v, 'g', -1, 64)), nil
	case nil:
		return []byte("z:"), nil
	default:
		return nil, fmt.Errorf("collections: unsupported indexed value type %T", value)
	}
}

func buildIndexEntryKey(collection, indexName string, encodedValue, documentID []byte) ([]byte, error) {
	prefix, err := CollectionIndexPrefix(collection, indexName)
	if err != nil {
		return nil, err
	}
	if len(encodedValue) > 65535 {
		return nil, fmt.Errorf("collections: index key too large")
	}
	key := make([]byte, 0, len(prefix)+2+len(encodedValue)+len(documentID))
	key = append(key, prefix...)
	key = binary.BigEndian.AppendUint16(key, uint16(len(encodedValue)))
	key = append(key, encodedValue...)
	key = append(key, documentID...)
	return key, nil
}

func indexValuePrefix(collection, indexName string, encodedValue []byte) ([]byte, error) {
	prefix, err := CollectionIndexPrefix(collection, indexName)
	if err != nil {
		return nil, err
	}
	if len(encodedValue) > 65535 {
		return nil, fmt.Errorf("collections: index key too large")
	}
	out := make([]byte, 0, len(prefix)+2+len(encodedValue))
	out = append(out, prefix...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(encodedValue)))
	out = append(out, encodedValue...)
	return out, nil
}

func parseIndexEntryDocID(collection, indexName string, key []byte) ([]byte, error) {
	prefix, err := CollectionIndexPrefix(collection, indexName)
	if err != nil {
		return nil, err
	}
	if len(key) < len(prefix)+2 {
		return nil, fmt.Errorf("collections: malformed index entry key")
	}
	if !bytes.HasPrefix(key, prefix) {
		return nil, fmt.Errorf("collections: invalid index entry prefix")
	}
	cursor := len(prefix)
	valueLen := int(binary.BigEndian.Uint16(key[cursor : cursor+2]))
	cursor += 2
	if len(key) < cursor+valueLen {
		return nil, fmt.Errorf("collections: malformed index entry value length")
	}
	cursor += valueLen
	if len(key) < cursor {
		return nil, fmt.Errorf("collections: malformed index entry")
	}
	out := make([]byte, len(key[cursor:]))
	copy(out, key[cursor:])
	return out, nil
}

func parseIndexEntryForConflict(collection string, key []byte) (string, []byte, error) {
	basePrefix, err := CollectionIndexDataPrefix(collection)
	if err != nil {
		return "", nil, err
	}
	if !bytes.HasPrefix(key, basePrefix) {
		return "", nil, fmt.Errorf("collections: malformed index key")
	}
	remainder := key[len(basePrefix):]
	sep := bytes.IndexByte(remainder, ':')
	if sep <= 0 {
		return "", nil, fmt.Errorf("collections: malformed index key")
	}
	indexPart := remainder[:sep]
	indexNameRaw, err := base64DecodeString(string(indexPart))
	if err != nil {
		return "", nil, err
	}
	indexName := string(indexNameRaw)
	prefix, err := CollectionIndexPrefix(collection, indexName)
	if err != nil {
		return "", nil, err
	}
	if len(key) < len(prefix)+2 {
		return "", nil, fmt.Errorf("collections: malformed index key")
	}
	valueLen := int(binary.BigEndian.Uint16(key[len(prefix) : len(prefix)+2]))
	cut := len(prefix) + 2 + valueLen
	if len(key) < cut {
		return "", nil, fmt.Errorf("collections: malformed index key")
	}
	return indexName, append([]byte{}, key[:cut]...), nil
}

func (c *Collection) indexByName(indexName string) (IndexDefinition, bool) {
	for _, idx := range c.meta.Indexes {
		if idx.Name == indexName {
			return idx, true
		}
	}
	return IndexDefinition{}, false
}

func (c *Collection) refreshMeta() error {
	if c == nil {
		return errCollectionNil
	}
	if c.mgr == nil {
		return nil
	}
	currentEpoch := c.mgr.epoch.Load()
	if c.metaEpoch == currentEpoch {
		return nil
	}
	meta, err := c.mgr.getCollection(c.meta.Name)
	if err != nil {
		return err
	}
	if meta == nil {
		return errCollectionNotFound
	}
	c.meta = *meta
	c.metaEpoch = currentEpoch
	return nil
}

func setDocumentOnBatch(b batch.Interface, key, value []byte) error {
	if auto, ok := b.(interface {
		SetAuto(key, value []byte) error
	}); ok {
		return auto.SetAuto(key, value)
	}
	return b.Set(key, value)
}
