package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

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
}

type Collection struct {
	db   collectionDB
	mgr  *CollectionManager
	meta CollectionMeta
}

type collectionDB interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	GetSystem(key []byte) ([]byte, error)
	SetSystem(key, value []byte) error
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
	if err := m.db.SetSystem(key, encoded); err != nil {
		return nil, err
	}
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
		db:   m.db,
		mgr:  m,
		meta: *meta,
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

	keys := make([][]byte, 0, 8)
	keys = append(keys, metaKey)

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
	if err := c.db.Set(key, document); err != nil {
		return nil, err
	}
	return persistedID, nil
}

func (c *Collection) Get(documentID []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
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
	return c.db.Delete(key)
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
