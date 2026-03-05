package collections

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestInsert_BatchWriteFailure_NoPartialPrimary(t *testing.T) {
	db := newAtomicMockDB()
	mgr := NewCollectionManager(db)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	db.failUserBatchWrite = true
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"a@example.com"}`)); err == nil {
		t.Fatalf("expected insert failure")
	}

	if db.directSetCalled {
		t.Fatalf("expected no direct Set() for primary write")
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Fatalf("expected no persisted primary value after failed write")
	}
}

type atomicMockDB struct {
	userStore          map[string][]byte
	systemStore        map[string][]byte
	failUserBatchWrite bool
	failSystemBatch    bool
	directSetCalled    bool
}

func newAtomicMockDB() *atomicMockDB {
	return &atomicMockDB{
		userStore:   make(map[string][]byte),
		systemStore: make(map[string][]byte),
	}
}

func (d *atomicMockDB) Get(key []byte) ([]byte, error) {
	v := d.userStore[string(key)]
	if v == nil {
		return nil, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (d *atomicMockDB) Set(key, value []byte) error {
	d.directSetCalled = true
	cp := append([]byte{}, value...)
	d.userStore[string(key)] = cp
	return nil
}

func (d *atomicMockDB) Delete(key []byte) error {
	delete(d.userStore, string(key))
	return nil
}

func (d *atomicMockDB) GetSystem(key []byte) ([]byte, error) {
	v := d.systemStore[string(key)]
	if v == nil {
		return nil, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (d *atomicMockDB) SetSystem(key, value []byte) error {
	cp := append([]byte{}, value...)
	d.systemStore[string(key)] = cp
	return nil
}

func (d *atomicMockDB) NewBatch() batch.Interface {
	return &atomicMockBatch{db: d}
}

func (d *atomicMockDB) NewSystemBatch() batch.Interface {
	return &atomicMockBatch{db: d, system: true}
}

func (d *atomicMockDB) Iterator(start, end []byte) (systemIterator, error) {
	return newAtomicMapIterator(d.userStore, start), nil
}

func (d *atomicMockDB) SystemIterator(start, end []byte) (systemIterator, error) {
	return newAtomicMapIterator(d.systemStore, start), nil
}

type atomicMockBatch struct {
	db     *atomicMockDB
	system bool
	ops    []batch.Entry
}

func (b *atomicMockBatch) Set(key, value []byte) error {
	k := append([]byte{}, key...)
	v := append([]byte{}, value...)
	b.ops = append(b.ops, batch.Entry{Type: batch.OpPut, Key: k, Value: v})
	return nil
}

func (b *atomicMockBatch) Delete(key []byte) error {
	k := append([]byte{}, key...)
	b.ops = append(b.ops, batch.Entry{Type: batch.OpDelete, Key: k})
	return nil
}

func (b *atomicMockBatch) SetOps(ops []batch.Entry) error {
	b.ops = append(b.ops, ops...)
	return nil
}

func (b *atomicMockBatch) Write() error {
	if b.system && b.db.failSystemBatch {
		return errors.New("system batch write failed")
	}
	if !b.system && b.db.failUserBatchWrite {
		return errors.New("user batch write failed")
	}
	target := b.db.userStore
	if b.system {
		target = b.db.systemStore
	}
	for _, op := range b.ops {
		if op.Type == batch.OpDelete {
			delete(target, string(op.Key))
			continue
		}
		target[string(op.Key)] = append([]byte{}, op.Value...)
	}
	return nil
}

func (b *atomicMockBatch) WriteSync() error { return b.Write() }
func (b *atomicMockBatch) Close() error     { return nil }

func (b *atomicMockBatch) Replay(fn func(batch.Entry) error) error {
	for _, entry := range b.ops {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *atomicMockBatch) GetByteSize() (int, error) { return len(b.ops), nil }

type atomicMapIterator struct {
	keys [][]byte
	db   map[string][]byte
	idx  int
}

func newAtomicMapIterator(source map[string][]byte, prefix []byte) *atomicMapIterator {
	keys := make([][]byte, 0, len(source))
	for key := range source {
		keyBytes := []byte(key)
		if len(prefix) > 0 && !bytes.HasPrefix(keyBytes, prefix) {
			continue
		}
		keys = append(keys, append([]byte{}, keyBytes...))
	}
	return &atomicMapIterator{keys: keys, db: source}
}

func (it *atomicMapIterator) Valid() bool { return it.idx >= 0 && it.idx < len(it.keys) }
func (it *atomicMapIterator) Next()       { it.idx++ }
func (it *atomicMapIterator) Seek(key []byte) {
	for i := range it.keys {
		if bytes.Compare(it.keys[i], key) >= 0 {
			it.idx = i
			return
		}
	}
	it.idx = len(it.keys)
}
func (it *atomicMapIterator) Key() []byte { return append([]byte{}, it.UnsafeKey()...) }
func (it *atomicMapIterator) Value() []byte {
	return append([]byte{}, it.UnsafeValue()...)
}
func (it *atomicMapIterator) KeyCopy(dst []byte) []byte {
	key := it.UnsafeKey()
	if key == nil {
		return nil
	}
	return append(dst[:0], key...)
}
func (it *atomicMapIterator) ValueCopy(dst []byte) []byte {
	value := it.UnsafeValue()
	if value == nil {
		return nil
	}
	return append(dst[:0], value...)
}
func (it *atomicMapIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.keys[it.idx]
}
func (it *atomicMapIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.db[string(it.keys[it.idx])]
}
func (it *atomicMapIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return nil, page.ValuePtr{}, 0
}
func (it *atomicMapIterator) IsDeleted() bool          { return false }
func (it *atomicMapIterator) Error() error             { return nil }
func (it *atomicMapIterator) Close() error             { return nil }
func (it *atomicMapIterator) Domain() ([]byte, []byte) { return nil, nil }

var _ iterator.UnsafeIterator = (*atomicMapIterator)(nil)
