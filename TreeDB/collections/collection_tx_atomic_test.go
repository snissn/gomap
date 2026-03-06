package collections

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/rootfmt"
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
	rootStores         map[uint64]map[string][]byte
	nextRootID         uint64
	systemVersion      uint64
	failUserBatchWrite bool
	failSystemBatch    bool
	directSetCalled    bool
}

func newAtomicMockDB() *atomicMockDB {
	return &atomicMockDB{
		userStore:   make(map[string][]byte),
		systemStore: make(map[string][]byte),
		rootStores:  make(map[uint64]map[string][]byte),
		nextRootID:  1,
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
	d.systemVersion++
	return nil
}

func (d *atomicMockDB) SystemRootVersion() uint64 {
	return d.systemVersion
}

func (d *atomicMockDB) GetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	if rootID == 0 {
		return nil, nil
	}
	store := d.rootStores[rootID]
	if store == nil {
		return nil, nil
	}
	value := store[string(key)]
	if value == nil {
		return nil, nil
	}
	return append([]byte{}, value...), nil
}

func (d *atomicMockDB) GetAtRootAppend(rootID uint64, key, dst []byte) ([]byte, error) {
	if rootID == 0 {
		return dst, nil
	}
	store := d.rootStores[rootID]
	if store == nil {
		return dst, nil
	}
	value := store[string(key)]
	if value == nil {
		return dst, nil
	}
	return append(dst[:0], value...), nil
}

func (d *atomicMockDB) HasAtRoot(rootID uint64, key []byte) (bool, error) {
	if rootID == 0 {
		return false, nil
	}
	store := d.rootStores[rootID]
	if store == nil {
		return false, nil
	}
	_, ok := store[string(key)]
	return ok, nil
}

func (d *atomicMockDB) HasPrefixAtRoot(rootID uint64, prefix []byte) (bool, error) {
	if rootID == 0 {
		return false, nil
	}
	store := d.rootStores[rootID]
	if store == nil {
		return false, nil
	}
	for key := range store {
		if bytes.HasPrefix([]byte(key), prefix) {
			return true, nil
		}
	}
	return false, nil
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

func (d *atomicMockDB) IteratorAtRoot(rootID uint64, start, end []byte) (systemIterator, error) {
	if rootID == 0 {
		return newAtomicMapIterator(nil, start), nil
	}
	return newAtomicMapIterator(d.rootStores[rootID], start), nil
}

func (d *atomicMockDB) SystemIterator(start, end []byte) (systemIterator, error) {
	return newAtomicMapIterator(d.systemStore, start), nil
}

func (d *atomicMockDB) MutateRoot(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	return d.mutateRootInternal(rootID, mutateRoot, nil, updateSystem)
}

func (d *atomicMockDB) MutateRootWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	return d.mutateRootInternal(rootID, mutateRoot, nil, updateSystem)
}

func (d *atomicMockDB) MutateRootAndUser(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	return d.mutateRootInternal(rootID, mutateRoot, mutateUser, updateSystem)
}

func (d *atomicMockDB) MutateRootAndUserWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	return d.mutateRootInternal(rootID, mutateRoot, mutateUser, updateSystem)
}

func (d *atomicMockDB) MutateRootsWithFuncs(sync bool, rootIDs []uint64, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error) {
	rootBatches := make([]*atomicMockBatch, len(rootIDs))
	newRootIDs := make([]uint64, len(rootIDs))
	for i := range rootIDs {
		rootBatches[i] = &atomicMockBatch{db: d}
		if mutateRoots[i] != nil {
			if err := mutateRoots[i](rootBatches[i]); err != nil {
				return nil, err
			}
		}
		newRootIDs[i] = rootIDs[i]
		if newRootIDs[i] == 0 && len(rootBatches[i].ops) > 0 {
			newRootIDs[i] = d.nextRootID
			d.nextRootID++
		}
	}

	systemBatch := &atomicMockBatch{db: d, system: true}
	if updateSystem != nil {
		if err := updateSystem(systemBatch, newRootIDs); err != nil {
			return nil, err
		}
	}
	if d.failUserBatchWrite {
		for i := 1; i < len(rootBatches); i++ {
			if len(rootBatches[i].ops) > 0 {
				return nil, errors.New("secondary root batch write failed")
			}
		}
	}
	if d.failSystemBatch && len(systemBatch.ops) > 0 {
		return nil, errors.New("system batch write failed")
	}

	nextRoots := make(map[uint64]map[string][]byte, len(d.rootStores))
	for rootID, store := range d.rootStores {
		nextRoots[rootID] = cloneAtomicStore(store)
	}
	for i := range rootIDs {
		targetRootID := newRootIDs[i]
		if targetRootID == 0 {
			continue
		}
		nextStore := cloneAtomicStore(d.rootStores[rootIDs[i]])
		applyAtomicOps(nextStore, rootBatches[i].ops)
		nextRoots[targetRootID] = nextStore
	}
	nextSystemStore := cloneAtomicStore(d.systemStore)
	applyAtomicOps(nextSystemStore, systemBatch.ops)

	d.rootStores = nextRoots
	d.systemStore = nextSystemStore
	if len(systemBatch.ops) > 0 {
		d.systemVersion++
	}
	return newRootIDs, nil
}

func (d *atomicMockDB) MutateRootsWithFormats(sync bool, rootIDs []uint64, formats []*rootfmt.Format, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error) {
	return d.MutateRootsWithFuncs(sync, rootIDs, mutateRoots, updateSystem)
}

func (d *atomicMockDB) MutateRootsWithFormatOps(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootOps [][]batch.Entry, buildSystemOps func([]uint64) ([]batch.Entry, error)) ([]uint64, error) {
	mutators := make([]func(batch.Interface) error, len(rootOps))
	for i := range rootOps {
		ops := rootOps[i]
		mutators[i] = func(target batch.Interface) error {
			if len(ops) == 0 {
				return nil
			}
			return target.SetOps(ops)
		}
	}
	return d.MutateRootsWithFormats(sync, rootIDs, formats, mutators, func(sys batch.Interface, newRootIDs []uint64) error {
		if buildSystemOps == nil {
			return nil
		}
		ops, err := buildSystemOps(newRootIDs)
		if err != nil {
			return err
		}
		if len(ops) == 0 {
			return nil
		}
		return sys.SetOps(ops)
	})
}

func (d *atomicMockDB) mutateRootInternal(rootID uint64, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	rootBatch := &atomicMockBatch{db: d}
	if mutateRoot != nil {
		if err := mutateRoot(rootBatch); err != nil {
			return 0, err
		}
	}
	userBatch := &atomicMockBatch{db: d}
	if mutateUser != nil {
		if err := mutateUser(userBatch); err != nil {
			return 0, err
		}
	}
	newRootID := rootID
	if newRootID == 0 && len(rootBatch.ops) > 0 {
		newRootID = d.nextRootID
		d.nextRootID++
	}
	systemBatch := &atomicMockBatch{db: d, system: true}
	if updateSystem != nil {
		if err := updateSystem(systemBatch, newRootID); err != nil {
			return 0, err
		}
	}
	if d.failUserBatchWrite && len(userBatch.ops) > 0 {
		return 0, errors.New("user batch write failed")
	}
	if d.failSystemBatch && len(systemBatch.ops) > 0 {
		return 0, errors.New("system batch write failed")
	}

	nextRootStore := cloneAtomicStore(d.rootStores[rootID])
	applyAtomicOps(nextRootStore, rootBatch.ops)
	nextUserStore := cloneAtomicStore(d.userStore)
	applyAtomicOps(nextUserStore, userBatch.ops)
	nextSystemStore := cloneAtomicStore(d.systemStore)
	applyAtomicOps(nextSystemStore, systemBatch.ops)

	if newRootID != 0 {
		d.rootStores[newRootID] = nextRootStore
	}
	d.userStore = nextUserStore
	d.systemStore = nextSystemStore
	if len(systemBatch.ops) > 0 {
		d.systemVersion++
	}
	return newRootID, nil
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
	if b.system && len(b.ops) > 0 {
		b.db.systemVersion++
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

func cloneAtomicStore(source map[string][]byte) map[string][]byte {
	if len(source) == 0 {
		return make(map[string][]byte)
	}
	out := make(map[string][]byte, len(source))
	for key, value := range source {
		out[key] = append([]byte{}, value...)
	}
	return out
}

func applyAtomicOps(target map[string][]byte, ops []batch.Entry) {
	for _, op := range ops {
		if op.Type == batch.OpDelete {
			delete(target, string(op.Key))
			continue
		}
		target[string(op.Key)] = append([]byte{}, op.Value...)
	}
}

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
