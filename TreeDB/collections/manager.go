package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/rootfmt"
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
	db           collectionDB
	mgr          *CollectionManager
	meta         CollectionMeta
	metaEpoch    uint64
	primary      *collectionRootRuntime
	state        *collectionRootRuntime
	indexes      map[string]*collectionIndexRuntime
	cacheVersion uint64
}

type collectionRootRuntime struct {
	desc    CollectionRootDescriptor
	rootKey []byte
}

type collectionIndexRuntime struct {
	def     IndexDefinition
	desc    CollectionRootDescriptor
	rootKey []byte
	prefix  []byte
	path    []string
}

type documentScratchHandle struct {
	buf []byte
}

const (
	documentScratchInitCap = 4 << 10
	documentScratchMaxCap  = 256 << 10
)

var documentScratchPool = sync.Pool{
	New: func() any {
		return &documentScratchHandle{buf: make([]byte, 0, documentScratchInitCap)}
	},
}

type collectionDB interface {
	Get(key []byte) ([]byte, error)
	HasAtRoot(rootID uint64, key []byte) (bool, error)
	HasPrefixAtRoot(rootID uint64, prefix []byte) (bool, error)
	GetAtRoot(rootID uint64, key []byte) ([]byte, error)
	GetAtRootAppend(rootID uint64, key, dst []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	GetSystem(key []byte) ([]byte, error)
	SetSystem(key, value []byte) error
	NewBatch() batch.Interface
	NewSystemBatch() batch.Interface
	Iterator(start, end []byte) (systemIterator, error)
	IteratorAtRoot(rootID uint64, start, end []byte) (systemIterator, error)
	SystemIterator(start, end []byte) (systemIterator, error)
	MutateRootsWithFormatOps(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootOps [][]batch.Entry, buildSystemOps func([]uint64) ([]batch.Entry, error)) ([]uint64, error)
	MutateRootsWithFormats(sync bool, rootIDs []uint64, formats []*rootfmt.Format, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error)
	MutateRootWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error)
	MutateRootAndUserWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error)
	MutateRootsWithFuncs(sync bool, rootIDs []uint64, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error)
	MutateRoot(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error)
	MutateRootAndUser(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error)
	SystemRootVersion() uint64
}

type systemIterator = iterator.UnsafeIterator

func getDocumentScratch() *documentScratchHandle {
	handle := documentScratchPool.Get().(*documentScratchHandle)
	if cap(handle.buf) == 0 {
		handle.buf = make([]byte, 0, documentScratchInitCap)
	}
	return handle
}

func putDocumentScratch(handle *documentScratchHandle, buf []byte) {
	if handle == nil {
		return
	}
	if cap(buf) > documentScratchMaxCap {
		handle.buf = nil
		return
	}
	handle.buf = buf[:0]
	documentScratchPool.Put(handle)
}

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
	indexStateRootDesc, err := newIndexStateCollectionRootDescriptor(meta.Name)
	if err != nil {
		return nil, err
	}
	if err := setCollectionRootDescriptorOnBatch(sysBatch, indexStateRootDesc); err != nil {
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
	if stateRootName, err := CollectionIndexStateRootName(existingMeta.Name); err == nil {
		if rootKey, err := SystemCollectionRootKey(stateRootName); err == nil {
			keys = append(keys, rootKey)
		}
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

func (m *CollectionManager) allocateAutoIDs(name string, count int) ([][]byte, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if count == 0 {
		return nil, nil
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

	out := make([][]byte, count)
	nextID := state.lastID
	for i := 0; i < count; i++ {
		nextID++
		encoded := make([]byte, autoIDBytesLen)
		binary.BigEndian.PutUint64(encoded, nextID)
		out[i] = encoded
	}
	if err := m.db.SetSystem(seqKey, out[len(out)-1]); err != nil {
		return nil, err
	}
	state.lastID = nextID
	m.autoSeq[name] = state
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
	rootDesc, rootKey, err := c.primaryRootDescriptor()
	if err != nil {
		return nil, err
	}
	writePrimary := func(root batch.Interface) error {
		return setDocumentOnBatch(root, key, document)
	}
	if len(c.meta.Indexes) == 0 {
		if err := c.mutateDocumentAndIndexes(rootDesc, rootKey, writePrimary, persistedID, nil, false, nil, nil); err != nil {
			return nil, err
		}
		return persistedID, nil
	}
	exists, err := c.db.HasAtRoot(rootDesc.RootPageID, key)
	if err != nil {
		return nil, err
	}
	var (
		oldStateRaw []byte
		stateStored bool
	)
	if exists {
		oldStateRaw, err = c.loadIndexState(persistedID)
		if err != nil {
			return nil, err
		}
		stateStored = len(oldStateRaw) > 0
		if len(oldStateRaw) == 0 {
			existingScratch := getDocumentScratch()
			existing, getErr := c.db.GetAtRootAppend(rootDesc.RootPageID, key, existingScratch.buf[:0])
			if getErr != nil {
				putDocumentScratch(existingScratch, existingScratch.buf)
				return nil, getErr
			}
			existingState, stateErr := c.indexStateForDocument(existing)
			putDocumentScratch(existingScratch, existing)
			if stateErr != nil {
				return nil, stateErr
			}
			oldStateRaw, err = encodeDocumentIndexState(existingState)
			if err != nil {
				return nil, err
			}
		}
	}
	newStateRaw, removals, additions, err := c.indexMutationForUpsert(persistedID, oldStateRaw, document)
	if err != nil {
		return nil, err
	}
	if exists && stateStored && bytes.Equal(oldStateRaw, newStateRaw) {
		newStateRaw = nil
	}
	if err := c.mutateDocumentAndIndexes(rootDesc, rootKey, writePrimary, persistedID, newStateRaw, false, removals, additions); err != nil {
		return nil, err
	}
	return persistedID, nil
}

func (c *Collection) InsertBatch(ids, documents [][]byte) ([][]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if err := c.refreshMeta(); err != nil {
		return nil, err
	}
	if len(documents) == 0 {
		return nil, nil
	}
	if c.db == nil {
		return nil, errCollectionManagerNil
	}

	resolvedIDs, err := c.resolveBatchInsertIDs(ids, len(documents))
	if err != nil {
		return nil, err
	}
	rootDesc, rootKey, err := c.primaryRootDescriptor()
	if err != nil {
		return nil, err
	}

	seenIDs := make(map[string]struct{}, len(resolvedIDs))
	primaryOps := make([]batch.Entry, 0, len(documents))
	for i := range documents {
		documentID := resolvedIDs[i]
		if _, exists := seenIDs[string(documentID)]; exists {
			return nil, fmt.Errorf("collections: duplicate document id in batch")
		}
		seenIDs[string(documentID)] = struct{}{}
		key, err := c.documentKey(documentID)
		if err != nil {
			return nil, err
		}
		exists, err := c.db.HasAtRoot(rootDesc.RootPageID, key)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("collections: document already exists")
		}
		primaryOps = append(primaryOps, batch.Entry{
			Type:  batch.OpPut,
			Key:   key,
			Value: documents[i],
		})
	}

	rootIDs := make([]uint64, 0, len(c.meta.Indexes)+2)
	rootFormats := make([]*rootfmt.Format, 0, len(c.meta.Indexes)+2)
	rootOps := make([][]batch.Entry, 0, len(c.meta.Indexes)+2)
	rootUpdates := make([]collectionRootUpdate, 0, len(c.meta.Indexes)+2)
	rootIDs = append(rootIDs, rootDesc.RootPageID)
	rootFormats = append(rootFormats, &rootDesc.Format)
	rootOps = append(rootOps, primaryOps)
	rootUpdates = append(rootUpdates, collectionRootUpdate{desc: rootDesc, rootKey: rootKey})

	if len(c.meta.Indexes) > 0 {
		stateDesc, stateRootKey, err := c.indexStateRootDescriptor()
		if err != nil {
			return nil, err
		}
		stateOps := make([]batch.Entry, 0, len(documents))
		indexOpsByName := make(map[string][]batch.Entry, len(c.meta.Indexes))
		seenUniquePrefixes := make(map[string]string, len(documents)*len(c.meta.Indexes))

		for i := range documents {
			documentID := resolvedIDs[i]
			state, err := c.indexStateForDocument(documents[i])
			if err != nil {
				return nil, err
			}
			stateRaw, err := encodeDocumentIndexState(state)
			if err != nil {
				return nil, err
			}
			stateOps = append(stateOps, batch.Entry{
				Type:  batch.OpPut,
				Key:   documentID,
				Value: stateRaw,
			})

			for _, idx := range c.meta.Indexes {
				values := state[idx.Name]
				if len(values) == 0 {
					continue
				}
				runtime, err := c.indexRuntime(idx)
				if err != nil {
					return nil, err
				}
				ops := indexOpsByName[idx.Name]
				for _, encoded := range values {
					key, err := buildIndexEntryKeyWithPrefix(runtime.prefix, encoded, documentID)
					if err != nil {
						return nil, err
					}
					if idx.Unique {
						if len(key) < len(documentID) || !bytes.HasSuffix(key, documentID) {
							return nil, fmt.Errorf("collections: malformed index key")
						}
						valuePrefix := key[:len(key)-len(documentID)]
						batchKey := string(valuePrefix)
						if existingID, ok := seenUniquePrefixes[batchKey]; ok && existingID != string(documentID) {
							return nil, fmt.Errorf("collections: unique index %q conflict", idx.Name)
						}
						seenUniquePrefixes[batchKey] = string(documentID)
						hasPrefix, err := c.db.HasPrefixAtRoot(runtime.desc.RootPageID, valuePrefix)
						if err != nil {
							return nil, err
						}
						if hasPrefix {
							return nil, fmt.Errorf("collections: unique index %q conflict", idx.Name)
						}
					}
					ops = append(ops, batch.Entry{Type: batch.OpPut, Key: key})
				}
				if len(ops) > 0 {
					indexOpsByName[idx.Name] = ops
				}
			}
		}

		rootIDs = append(rootIDs, stateDesc.RootPageID)
		rootFormats = append(rootFormats, &stateDesc.Format)
		rootOps = append(rootOps, stateOps)
		rootUpdates = append(rootUpdates, collectionRootUpdate{desc: stateDesc, rootKey: stateRootKey})
		for _, idx := range c.meta.Indexes {
			ops := indexOpsByName[idx.Name]
			if len(ops) == 0 {
				continue
			}
			runtime, err := c.indexRuntime(idx)
			if err != nil {
				return nil, err
			}
			rootIDs = append(rootIDs, runtime.desc.RootPageID)
			rootFormats = append(rootFormats, &runtime.desc.Format)
			rootOps = append(rootOps, ops)
			rootUpdates = append(rootUpdates, collectionRootUpdate{
				desc:    &runtime.desc,
				rootKey: runtime.rootKey,
			})
		}
	}

	if err := c.publishRootOps(rootIDs, rootFormats, rootOps, rootUpdates); err != nil {
		return nil, err
	}
	return resolvedIDs, nil
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
	rootDesc, _, err := c.primaryRootDescriptor()
	if err != nil {
		return nil, err
	}
	return c.db.GetAtRoot(rootDesc.RootPageID, key)
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
	rootDesc, rootKey, err := c.primaryRootDescriptor()
	if err != nil {
		return err
	}
	if len(c.meta.Indexes) == 0 {
		writePrimaryDelete := func(root batch.Interface) error {
			return deleteDocumentOnBatch(root, key)
		}
		return c.mutateDocumentAndIndexes(rootDesc, rootKey, writePrimaryDelete, documentID, nil, false, nil, nil)
	}
	oldStateRaw, err := c.loadIndexState(documentID)
	if err != nil {
		return err
	}
	stateStored := len(oldStateRaw) > 0
	if len(oldStateRaw) == 0 {
		existingScratch := getDocumentScratch()
		existing, getErr := c.db.GetAtRootAppend(rootDesc.RootPageID, key, existingScratch.buf[:0])
		if getErr != nil {
			putDocumentScratch(existingScratch, existingScratch.buf)
			return getErr
		}
		if len(existing) == 0 {
			putDocumentScratch(existingScratch, existing)
			return nil
		}
		existingState, stateErr := c.indexStateForDocument(existing)
		putDocumentScratch(existingScratch, existing)
		if stateErr != nil {
			return stateErr
		}
		oldStateRaw, err = encodeDocumentIndexState(existingState)
		if err != nil {
			return err
		}
	}
	oldState, err := decodeDocumentIndexState(oldStateRaw)
	if err != nil {
		return err
	}
	entries, err := c.indexEntriesForState(documentID, oldState)
	if err != nil {
		return err
	}
	writePrimaryDelete := func(root batch.Interface) error {
		return deleteDocumentOnBatch(root, key)
	}
	return c.mutateDocumentAndIndexes(rootDesc, rootKey, writePrimaryDelete, documentID, nil, stateStored, entries, nil)
}

func (c *Collection) Upsert(documentID, document []byte) ([]byte, error) {
	return c.Insert(documentID, document)
}

func (c *Collection) resolveBatchInsertIDs(ids [][]byte, count int) ([][]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if count == 0 {
		return nil, nil
	}
	switch c.meta.Options.IDMode {
	case idModeCallerProvided:
		if len(ids) != count {
			return nil, fmt.Errorf("collections: caller-provided batch ids length mismatch")
		}
		out := make([][]byte, count)
		for i := 0; i < count; i++ {
			if err := requireDocumentID(ids[i]); err != nil {
				return nil, err
			}
			out[i] = append([]byte(nil), ids[i]...)
		}
		return out, nil
	case idModeAuto:
		if c.mgr == nil {
			return nil, errCollectionManagerNil
		}
		return c.mgr.allocateAutoIDs(c.meta.Name, count)
	default:
		return nil, fmt.Errorf("collections: unsupported id mode %d", c.meta.Options.IDMode)
	}
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
	return documentID, nil
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
	runtime, err := c.indexRuntime(idxDef)
	if err != nil {
		return nil, err
	}
	prefix, err := indexValuePrefixForString(runtime.prefix, value)
	if err != nil {
		return nil, err
	}
	it, err := c.db.IteratorAtRoot(runtime.desc.RootPageID, prefix, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	resultCap := 1
	arenaCap := 32
	if !idxDef.Unique {
		resultCap = 64
		arenaCap = 1024
	}
	out := make([][]byte, 0, resultCap)
	arena := make([]byte, 0, arenaCap)
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			start := len(arena)
			arena = append(arena, key[len(prefix):]...)
			out = append(out, arena[start:len(arena)])
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

func (m *CollectionManager) backfillIndex(collection string, def IndexDefinition) error {
	meta, err := m.getCollection(collection)
	if err != nil {
		return err
	}
	if meta == nil {
		return errCollectionNotFound
	}
	primaryRootDesc, err := m.rootDescriptor(meta.PrimaryRoot)
	if err != nil {
		return err
	}
	it, err := m.db.IteratorAtRoot(primaryRootDesc.RootPageID, nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	col := &Collection{
		db:        m.db,
		mgr:       m,
		meta:      *meta,
		metaEpoch: m.epoch.Load(),
	}
	indexRootDesc, indexRootKey, err := col.secondaryRootDescriptor(def)
	if err != nil {
		return err
	}
	stateRootDesc, stateRootKey, err := col.indexStateRootDescriptor()
	if err != nil {
		return err
	}
	type stateBackfillEntry struct {
		documentID []byte
		stateRaw   []byte
	}
	var (
		indexKeys    [][]byte
		stateEntries []stateBackfillEntry
	)
	for it.Valid() {
		key := it.UnsafeKey()
		if !it.IsDeleted() {
			docID := append([]byte{}, key...)
			state, err := col.indexStateForDocument(it.UnsafeValue())
			if err != nil {
				return err
			}
			stateRaw, err := encodeDocumentIndexState(state)
			if err != nil {
				return err
			}
			stateEntries = append(stateEntries, stateBackfillEntry{
				documentID: docID,
				stateRaw:   stateRaw,
			})
			keysForDoc, err := col.indexEntriesForState(docID, documentIndexState{def.Name: state[def.Name]})
			if err != nil {
				return err
			}
			indexKeys = append(indexKeys, keysForDoc...)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	if len(indexKeys) == 0 && len(stateEntries) == 0 {
		return nil
	}
	rootIDs := make([]uint64, 0, 2)
	rootFormats := make([]*rootfmt.Format, 0, 2)
	rootOps := make([][]batch.Entry, 0, 2)
	rootUpdates := make([]collectionRootUpdate, 0, 2)
	if len(indexKeys) > 0 {
		rootIDs = append(rootIDs, indexRootDesc.RootPageID)
		rootFormats = append(rootFormats, &indexRootDesc.Format)
		ops := make([]batch.Entry, 0, len(indexKeys))
		for _, indexKey := range indexKeys {
			ops = append(ops, batch.Entry{Type: batch.OpPut, Key: indexKey, Value: nil})
		}
		rootOps = append(rootOps, ops)
		rootUpdates = append(rootUpdates, collectionRootUpdate{desc: indexRootDesc, rootKey: indexRootKey})
	}
	if len(stateEntries) > 0 {
		rootIDs = append(rootIDs, stateRootDesc.RootPageID)
		rootFormats = append(rootFormats, &stateRootDesc.Format)
		ops := make([]batch.Entry, 0, len(stateEntries))
		for _, entry := range stateEntries {
			ops = append(ops, batch.Entry{Type: batch.OpPut, Key: entry.documentID, Value: entry.stateRaw})
		}
		rootOps = append(rootOps, ops)
		rootUpdates = append(rootUpdates, collectionRootUpdate{desc: stateRootDesc, rootKey: stateRootKey})
	}
	var publishedRootIDs []uint64
	_, err = m.db.MutateRootsWithFormatOps(false, rootIDs, rootFormats, rootOps, func(newRootIDs []uint64) ([]batch.Entry, error) {
		publishedRootIDs = append(publishedRootIDs[:0], newRootIDs...)
		return buildRootDescriptorEntries(rootUpdates, newRootIDs)
	})
	if err == nil {
		for i := range rootUpdates {
			rootUpdates[i].desc.RootPageID = publishedRootIDs[i]
		}
	}
	return err
}

func (c *Collection) indexMutationForUpsert(documentID []byte, oldStateRaw, newDoc []byte) ([]byte, [][]byte, [][]byte, error) {
	oldState, err := decodeDocumentIndexState(oldStateRaw)
	if err != nil {
		return nil, nil, nil, err
	}
	newState, err := c.indexStateForDocument(newDoc)
	if err != nil {
		return nil, nil, nil, err
	}
	newStateRaw, err := encodeDocumentIndexState(newState)
	if err != nil {
		return nil, nil, nil, err
	}
	oldEntries, err := c.indexEntriesForState(documentID, oldState)
	if err != nil {
		return nil, nil, nil, err
	}
	newEntries, err := c.indexEntriesForState(documentID, newState)
	if err != nil {
		return nil, nil, nil, err
	}
	removals, additions := diffIndexEntries(oldEntries, newEntries)
	if err := c.ensureUniqueConflicts(documentID, additions); err != nil {
		return nil, nil, nil, err
	}
	return newStateRaw, removals, additions, nil
}

func (c *Collection) ensureUniqueConflicts(documentID []byte, additions [][]byte) error {
	if len(additions) == 0 {
		return nil
	}
	for _, key := range additions {
		runtime, err := c.indexRuntimeForEntry(key)
		if err != nil {
			return err
		}
		if !runtime.def.Unique {
			continue
		}
		if len(key) < len(documentID) || !bytes.HasSuffix(key, documentID) {
			return fmt.Errorf("collections: malformed index key")
		}
		valuePrefix := key[:len(key)-len(documentID)]
		hasPrefix, err := c.db.HasPrefixAtRoot(runtime.desc.RootPageID, valuePrefix)
		if err != nil {
			return err
		}
		if !hasPrefix {
			continue
		}
		exact, err := c.db.HasAtRoot(runtime.desc.RootPageID, key)
		if err != nil {
			return err
		}
		if !exact {
			return fmt.Errorf("collections: unique index %q conflict", runtime.def.Name)
		}
	}
	return nil
}

func (c *Collection) indexEntriesForDocument(documentID, document []byte) ([][]byte, error) {
	state, err := c.indexStateForDocument(document)
	if err != nil {
		return nil, err
	}
	return c.indexEntriesForState(documentID, state)
}

func diffIndexEntries(oldEntries, newEntries [][]byte) ([][]byte, [][]byte) {
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
	return removals, additions
}

func (c *Collection) loadIndexState(documentID []byte) ([]byte, error) {
	stateDesc, _, err := c.indexStateRootDescriptor()
	if err != nil {
		return nil, err
	}
	return c.db.GetAtRoot(stateDesc.RootPageID, documentID)
}

func indexKeysForSingleDefinition(documentID []byte, document any, idx IndexDefinition, opts CollectionOptions, prefix []byte, path []string) ([][]byte, error) {
	values, err := encodedIndexValuesForDefinition(document, idx, opts, path)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(values))
	for _, encoded := range values {
		key, err := buildIndexEntryKeyWithPrefix(prefix, encoded, documentID)
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
		return arr, nil
	}
	return []any{value}, nil
}

func splitIndexPath(path string) []string {
	if path == "" {
		return nil
	}
	if !strings.Contains(path, ".") {
		return []string{path}
	}
	return strings.Split(path, ".")
}

func extractIndexPathValue(document any, path []string) (any, bool) {
	if len(path) == 0 {
		return nil, false
	}
	current := document
	for _, segment := range path {
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
	return buildIndexEntryKeyWithPrefix(prefix, encodedValue, documentID)
}

func buildIndexEntryKeyWithPrefix(prefix, encodedValue, documentID []byte) ([]byte, error) {
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
	return indexValuePrefixWithPrefix(prefix, encodedValue)
}

func indexValuePrefixWithPrefix(prefix, encodedValue []byte) ([]byte, error) {
	if len(encodedValue) > 65535 {
		return nil, fmt.Errorf("collections: index key too large")
	}
	out := make([]byte, 0, len(prefix)+2+len(encodedValue))
	out = append(out, prefix...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(encodedValue)))
	out = append(out, encodedValue...)
	return out, nil
}

func indexValuePrefixForString(prefix []byte, value string) ([]byte, error) {
	if len(value) > 65533 {
		return nil, fmt.Errorf("collections: index key too large")
	}
	out := make([]byte, 0, len(prefix)+4+len(value))
	out = append(out, prefix...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(value)+2))
	out = append(out, 's', ':')
	out = append(out, value...)
	return out, nil
}

func parseIndexEntryDocID(collection, indexName string, key []byte) ([]byte, error) {
	prefix, err := CollectionIndexPrefix(collection, indexName)
	if err != nil {
		return nil, err
	}
	return parseIndexEntryDocIDWithPrefix(prefix, key)
}

func parseIndexEntryDocIDWithPrefix(prefix, key []byte) ([]byte, error) {
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
	indexName, err := parseIndexEntryIndexName(collection, key)
	if err != nil {
		return "", nil, err
	}
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

func parseIndexEntryIndexName(collection string, key []byte) (string, error) {
	basePrefix, err := CollectionIndexDataPrefix(collection)
	if err != nil {
		return "", err
	}
	if !bytes.HasPrefix(key, basePrefix) {
		return "", fmt.Errorf("collections: malformed index key")
	}
	remainder := key[len(basePrefix):]
	sep := bytes.IndexByte(remainder, ':')
	if sep <= 0 {
		return "", fmt.Errorf("collections: malformed index key")
	}
	indexNameRaw, err := base64DecodeString(string(remainder[:sep]))
	if err != nil {
		return "", err
	}
	return string(indexNameRaw), nil
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
	c.resetRuntimeCache()
	c.cacheVersion = c.db.SystemRootVersion()
	return nil
}

func (c *Collection) resetRuntimeCache() {
	if c == nil {
		return
	}
	c.primary = nil
	c.state = nil
	c.indexes = nil
}

func (c *Collection) ensureRuntimeCacheFresh() {
	if c == nil || c.db == nil {
		return
	}
	currentVersion := c.db.SystemRootVersion()
	if c.cacheVersion != 0 && c.cacheVersion != currentVersion {
		c.resetRuntimeCache()
	}
	c.cacheVersion = currentVersion
}

type collectionOpBatch struct {
	entries []batch.Entry
	closed  bool
}

func newCollectionOpBatch(backing []batch.Entry) collectionOpBatch {
	return collectionOpBatch{entries: backing[:0]}
}

func (b *collectionOpBatch) Set(key, value []byte) error {
	if b.closed {
		return fmt.Errorf("collections: op batch closed")
	}
	if value == nil {
		return fmt.Errorf("collections: nil value")
	}
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: key, Value: value})
	return nil
}

func (b *collectionOpBatch) Delete(key []byte) error {
	if b.closed {
		return fmt.Errorf("collections: op batch closed")
	}
	b.entries = append(b.entries, batch.Entry{Type: batch.OpDelete, Key: key})
	return nil
}

func (b *collectionOpBatch) SetOps(ops []batch.Entry) error {
	if b.closed {
		return fmt.Errorf("collections: op batch closed")
	}
	b.entries = append(b.entries, ops...)
	return nil
}

func (b *collectionOpBatch) Write() error     { return nil }
func (b *collectionOpBatch) WriteSync() error { return nil }
func (b *collectionOpBatch) Close() error {
	b.closed = true
	return nil
}
func (b *collectionOpBatch) Replay(fn func(batch.Entry) error) error {
	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}
func (b *collectionOpBatch) GetByteSize() (int, error) {
	size := 0
	for _, entry := range b.entries {
		size += len(entry.Key) + len(entry.Value)
	}
	return size, nil
}

func setDocumentOnBatch(b batch.Interface, key, value []byte) error {
	if autoView, ok := b.(interface {
		SetAutoView(key, value []byte) error
	}); ok {
		return autoView.SetAutoView(key, value)
	}
	if auto, ok := b.(interface {
		SetAuto(key, value []byte) error
	}); ok {
		return auto.SetAuto(key, value)
	}
	return b.Set(key, value)
}

func setBytesOnBatch(b batch.Interface, key, value []byte) error {
	if setView, ok := b.(interface {
		SetView(key, value []byte) error
	}); ok {
		return setView.SetView(key, value)
	}
	return b.Set(key, value)
}

func setOwnedBytesOnBatch(b batch.Interface, key, value []byte) error {
	if owned, ok := b.(interface {
		SetOwnedBytes(key, value []byte) error
	}); ok {
		return owned.SetOwnedBytes(key, value)
	}
	return setBytesOnBatch(b, key, value)
}

func deleteDocumentOnBatch(b batch.Interface, key []byte) error {
	if deleteView, ok := b.(interface {
		DeleteView(key []byte) error
	}); ok {
		return deleteView.DeleteView(key)
	}
	return b.Delete(key)
}

func setIndexEntryOnBatch(b batch.Interface, key []byte) error {
	if setView, ok := b.(interface {
		SetView(key, value []byte) error
	}); ok {
		return setView.SetView(key, nil)
	}
	return b.Set(key, nil)
}

func setOwnedIndexEntryOnBatch(b batch.Interface, key []byte) error {
	if owned, ok := b.(interface {
		SetOwnedKey(key []byte) error
	}); ok {
		return owned.SetOwnedKey(key)
	}
	return setIndexEntryOnBatch(b, key)
}

func deleteOwnedIndexEntryOnBatch(b batch.Interface, key []byte) error {
	if owned, ok := b.(interface {
		DeleteOwnedKey(key []byte) error
	}); ok {
		return owned.DeleteOwnedKey(key)
	}
	return deleteDocumentOnBatch(b, key)
}

func (m *CollectionManager) rootDescriptor(rootName string) (*CollectionRootDescriptor, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	return loadRootDescriptor(m.db, rootName)
}

func (c *Collection) secondaryRootDescriptor(idx IndexDefinition) (*CollectionRootDescriptor, []byte, error) {
	if c == nil {
		return nil, nil, errCollectionNil
	}
	c.ensureRuntimeCacheFresh()
	runtime, err := c.indexRuntime(idx)
	if err != nil {
		return nil, nil, err
	}
	return &runtime.desc, runtime.rootKey, nil
}

func (c *Collection) indexStateRootDescriptor() (*CollectionRootDescriptor, []byte, error) {
	if c == nil {
		return nil, nil, errCollectionNil
	}
	c.ensureRuntimeCacheFresh()
	if c.state != nil {
		return &c.state.desc, c.state.rootKey, nil
	}
	rootName, err := CollectionIndexStateRootName(c.meta.Name)
	if err != nil {
		return nil, nil, err
	}
	rootKey, err := SystemCollectionRootKey(rootName)
	if err != nil {
		return nil, nil, err
	}
	desc, err := loadRootDescriptorByKey(c.db, rootKey, rootName)
	if err != nil {
		return nil, nil, err
	}
	c.state = &collectionRootRuntime{
		desc:    *desc,
		rootKey: rootKey,
	}
	return &c.state.desc, c.state.rootKey, nil
}

func (c *Collection) primaryRootDescriptor() (*CollectionRootDescriptor, []byte, error) {
	if c == nil {
		return nil, nil, errCollectionNil
	}
	c.ensureRuntimeCacheFresh()
	if c.primary != nil {
		return &c.primary.desc, c.primary.rootKey, nil
	}
	rootKey, err := SystemCollectionRootKey(c.meta.PrimaryRoot)
	if err != nil {
		return nil, nil, err
	}
	desc, err := loadRootDescriptorByKey(c.db, rootKey, c.meta.PrimaryRoot)
	if err != nil {
		return nil, nil, err
	}
	c.primary = &collectionRootRuntime{
		desc:    *desc,
		rootKey: rootKey,
	}
	return &c.primary.desc, c.primary.rootKey, nil
}

func loadRootDescriptor(database collectionDB, rootName string) (*CollectionRootDescriptor, error) {
	if database == nil {
		return nil, errCollectionManagerNil
	}
	rootKey, err := SystemCollectionRootKey(rootName)
	if err != nil {
		return nil, err
	}
	return loadRootDescriptorByKey(database, rootKey, rootName)
}

func loadRootDescriptorByKey(database collectionDB, rootKey []byte, rootName string) (*CollectionRootDescriptor, error) {
	if database == nil {
		return nil, errCollectionManagerNil
	}
	raw, err := database.GetSystem(rootKey)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("collections: root descriptor %q not found", rootName)
	}
	var desc CollectionRootDescriptor
	if err := desc.Decode(raw); err != nil {
		return nil, err
	}
	return &desc, nil
}

func (c *Collection) indexRuntime(idx IndexDefinition) (*collectionIndexRuntime, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	c.ensureRuntimeCacheFresh()
	if c.indexes == nil {
		c.indexes = make(map[string]*collectionIndexRuntime, len(c.meta.Indexes))
	}
	if runtime := c.indexes[idx.Name]; runtime != nil {
		return runtime, nil
	}
	rootKey, err := SystemCollectionRootKey(idx.RootName)
	if err != nil {
		return nil, err
	}
	desc, err := loadRootDescriptorByKey(c.db, rootKey, idx.RootName)
	if err != nil {
		return nil, err
	}
	prefix, err := CollectionIndexPrefix(c.meta.Name, idx.Name)
	if err != nil {
		return nil, err
	}
	runtime := &collectionIndexRuntime{
		def:     idx,
		desc:    *desc,
		rootKey: rootKey,
		prefix:  prefix,
		path:    splitIndexPath(idx.Field),
	}
	c.indexes[idx.Name] = runtime
	return runtime, nil
}

func writeRootDescriptorUpdate(sys batch.Interface, rootKey []byte, desc *CollectionRootDescriptor, newRootID uint64) error {
	if sys == nil {
		return fmt.Errorf("collections: nil system batch")
	}
	if desc == nil {
		return fmt.Errorf("collections: nil root descriptor")
	}
	encoded := desc.encodeWithRootPageIDAssumeValid(newRootID)
	return setOwnedBytesOnBatch(sys, rootKey, encoded)
}

type collectionRootUpdate struct {
	desc    *CollectionRootDescriptor
	rootKey []byte
}

type collectionIndexRootMutation struct {
	desc      *CollectionRootDescriptor
	rootKey   []byte
	removals  [][]byte
	additions [][]byte
}

func buildRootDescriptorEntries(rootUpdates []collectionRootUpdate, newRootIDs []uint64) ([]batch.Entry, error) {
	if len(rootUpdates) != len(newRootIDs) {
		return nil, fmt.Errorf("collections: root publish mismatch: got %d ids for %d updates", len(newRootIDs), len(rootUpdates))
	}
	ops := make([]batch.Entry, 0, len(rootUpdates))
	for i := range rootUpdates {
		ops = append(ops, batch.Entry{
			Type:  batch.OpPut,
			Key:   rootUpdates[i].rootKey,
			Value: rootUpdates[i].desc.encodeWithRootPageIDAssumeValid(newRootIDs[i]),
		})
	}
	return ops, nil
}

func (c *Collection) mutateDocumentAndIndexes(primaryDesc *CollectionRootDescriptor, primaryRootKey []byte, mutatePrimary func(batch.Interface) error, documentID, stateValue []byte, deleteState bool, removals, additions [][]byte) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionManagerNil
	}
	indexMutations, err := c.indexRootMutations(removals, additions)
	if err != nil {
		return err
	}
	hasStateMutation := deleteState || len(stateValue) > 0
	var primaryOps []batch.Entry
	if len(indexMutations) == 0 && !hasStateMutation {
		var primaryScratch [1]batch.Entry
		b := newCollectionOpBatch(primaryScratch[:0])
		if err := mutatePrimary(&b); err != nil {
			return err
		}
		primaryOps = b.entries
	} else {
		var primaryScratch [1]batch.Entry
		b := newCollectionOpBatch(primaryScratch[:0])
		if err := mutatePrimary(&b); err != nil {
			return err
		}
		primaryOps = b.entries
	}

	rootIDs := make([]uint64, 0, len(indexMutations)+2)
	rootFormats := make([]*rootfmt.Format, 0, len(indexMutations)+2)
	rootOps := make([][]batch.Entry, 0, len(indexMutations)+2)
	rootUpdates := make([]collectionRootUpdate, 0, len(indexMutations)+2)
	rootIDs = append(rootIDs, primaryDesc.RootPageID)
	rootFormats = append(rootFormats, &primaryDesc.Format)
	rootOps = append(rootOps, primaryOps)
	rootUpdates = append(rootUpdates, collectionRootUpdate{desc: primaryDesc, rootKey: primaryRootKey})
	if hasStateMutation {
		stateDesc, stateRootKey, err := c.indexStateRootDescriptor()
		if err != nil {
			return err
		}
		rootIDs = append(rootIDs, stateDesc.RootPageID)
		rootFormats = append(rootFormats, &stateDesc.Format)
		if deleteState {
			rootOps = append(rootOps, []batch.Entry{{Type: batch.OpDelete, Key: documentID}})
		} else {
			rootOps = append(rootOps, []batch.Entry{{Type: batch.OpPut, Key: documentID, Value: stateValue}})
		}
		rootUpdates = append(rootUpdates, collectionRootUpdate{
			desc:    stateDesc,
			rootKey: stateRootKey,
		})
	}

	for i := range indexMutations {
		indexMutation := indexMutations[i]
		rootIDs = append(rootIDs, indexMutation.desc.RootPageID)
		rootFormats = append(rootFormats, &indexMutation.desc.Format)
		ops := make([]batch.Entry, 0, len(indexMutation.removals)+len(indexMutation.additions))
		for _, indexKey := range indexMutation.removals {
			ops = append(ops, batch.Entry{Type: batch.OpDelete, Key: indexKey})
		}
		for _, indexKey := range indexMutation.additions {
			ops = append(ops, batch.Entry{Type: batch.OpPut, Key: indexKey, Value: nil})
		}
		rootOps = append(rootOps, ops)
		rootUpdates = append(rootUpdates, collectionRootUpdate{
			desc:    indexMutation.desc,
			rootKey: indexMutation.rootKey,
		})
	}

	return c.publishRootOps(rootIDs, rootFormats, rootOps, rootUpdates)
}

func (c *Collection) publishRootOps(rootIDs []uint64, rootFormats []*rootfmt.Format, rootOps [][]batch.Entry, rootUpdates []collectionRootUpdate) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionManagerNil
	}
	var publishedRootIDs []uint64
	_, err := c.db.MutateRootsWithFormatOps(false, rootIDs, rootFormats, rootOps, func(newRootIDs []uint64) ([]batch.Entry, error) {
		publishedRootIDs = append(publishedRootIDs[:0], newRootIDs...)
		return buildRootDescriptorEntries(rootUpdates, newRootIDs)
	})
	if err != nil {
		return err
	}
	for i := range rootUpdates {
		rootUpdates[i].desc.RootPageID = publishedRootIDs[i]
	}
	c.cacheVersion = c.db.SystemRootVersion()
	return nil
}

func (c *Collection) indexRootMutations(removals, additions [][]byte) ([]collectionIndexRootMutation, error) {
	if len(removals) == 0 && len(additions) == 0 {
		return nil, nil
	}
	byName := make(map[string]*collectionIndexRootMutation, len(c.meta.Indexes))
	appendEntries := func(entries [][]byte, deleteOp bool) error {
		for _, entry := range entries {
			runtime, err := c.indexRuntimeForEntry(entry)
			if err != nil {
				return err
			}
			mutation := byName[runtime.def.Name]
			if mutation == nil {
				mutation = &collectionIndexRootMutation{
					desc:    &runtime.desc,
					rootKey: runtime.rootKey,
				}
				byName[runtime.def.Name] = mutation
			}
			if deleteOp {
				mutation.removals = append(mutation.removals, entry)
			} else {
				mutation.additions = append(mutation.additions, entry)
			}
		}
		return nil
	}
	if err := appendEntries(removals, true); err != nil {
		return nil, err
	}
	if err := appendEntries(additions, false); err != nil {
		return nil, err
	}
	out := make([]collectionIndexRootMutation, 0, len(byName))
	for _, idx := range c.meta.Indexes {
		mutation := byName[idx.Name]
		if mutation == nil {
			continue
		}
		out = append(out, *mutation)
	}
	return out, nil
}

func (c *Collection) indexRuntimeForEntry(entry []byte) (*collectionIndexRuntime, error) {
	for _, idx := range c.meta.Indexes {
		runtime, err := c.indexRuntime(idx)
		if err != nil {
			return nil, err
		}
		if bytes.HasPrefix(entry, runtime.prefix) {
			return runtime, nil
		}
	}
	return nil, fmt.Errorf("collections: unknown index root for entry")
}
