package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// MockBackend implements BackendDB
type MockBackend struct {
	mu         sync.RWMutex
	data       map[string][]byte
	writeCalls int
	writeSyncs int
	writeErr   error
	setOpsErr  error
	setErr     error
	deleteErr  error
}

func NewMockBackend() *MockBackend {
	return &MockBackend{data: make(map[string][]byte)}
}

func (m *MockBackend) SetWriteErr(err error) {
	m.mu.Lock()
	m.writeErr = err
	m.mu.Unlock()
}

func (m *MockBackend) getWriteErr() error {
	m.mu.RLock()
	err := m.writeErr
	m.mu.RUnlock()
	return err
}

func (m *MockBackend) getSetErr() error {
	m.mu.RLock()
	err := m.setErr
	m.mu.RUnlock()
	return err
}

func (m *MockBackend) getSetOpsErr() error {
	m.mu.RLock()
	err := m.setOpsErr
	m.mu.RUnlock()
	return err
}

func (m *MockBackend) getDeleteErr() error {
	m.mu.RLock()
	err := m.deleteErr
	m.mu.RUnlock()
	return err
}

func setMutable(db *DB, key, value []byte) {
	shard := db.shardForKey(key)
	shard.mu.Lock()
	shard.mem.Set(key, value)
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	shard.mu.Unlock()
	db.mutableBytes.Add(delta)
}

func deleteMutable(db *DB, key []byte) {
	shard := db.shardForKey(key)
	shard.mu.Lock()
	shard.mem.Delete(key)
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	shard.mu.Unlock()
	db.mutableBytes.Add(delta)
}

func (m *MockBackend) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	// Mimic safe copy for Get
	ret := make([]byte, len(val))
	copy(ret, val)
	return ret, nil
}

func (m *MockBackend) GetUnsafe(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	return val, nil
}

func (m *MockBackend) GetAppend(key, dst []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	if !ok {
		return dst, fmt.Errorf("mock: key not found") // Use error to match contract, though tests might not check type strictly
	}
	return append(dst, val...), nil
}

func (m *MockBackend) Has(key []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[string(key)]
	return ok, nil
}

func (m *MockBackend) Set(key, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = val
}

func (m *MockBackend) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	m.mu.RLock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	m.mu.RUnlock()
	sort.Strings(keys)
	it := &MockIterator{backend: m, keys: keys, idx: -1}
	it.Seek(start)
	return it, nil
}

type MockIterator struct {
	backend *MockBackend
	keys    []string
	idx     int
}

func (it *MockIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *MockIterator) Next() {
	it.idx++
}

func (it *MockIterator) Seek(key []byte) {
	it.idx = sort.SearchStrings(it.keys, string(key))
	// If not found, sort.Search returns insertion point.
	// If exact match or greater, that's what we want.
	if it.idx == len(it.keys) {
		// eof
	}
}

func (it *MockIterator) UnsafeKey() []byte {
	return []byte(it.keys[it.idx])
}

func (it *MockIterator) UnsafeValue() []byte {
	it.backend.mu.RLock()
	defer it.backend.mu.RUnlock()
	return it.backend.data[it.keys[it.idx]]
}

func (it *MockIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.UnsafeValue(), page.ValuePtr{}, 0
}

func (it *MockIterator) IsDeleted() bool           { return false }
func (it *MockIterator) Error() error              { return nil }
func (it *MockIterator) Close() error              { return nil }
func (it *MockIterator) Domain() ([]byte, []byte)  { return nil, nil }
func (it *MockIterator) Key() []byte               { return it.UnsafeKey() }
func (it *MockIterator) Value() []byte             { return it.UnsafeValue() }
func (it *MockIterator) KeyCopy(dst []byte) []byte { return append(dst[:0], it.UnsafeKey()...) }
func (it *MockIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}

func (m *MockBackend) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	m.mu.RLock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	m.mu.RUnlock()
	sort.Strings(keys)

	it := &MockReverseIterator{backend: m, keys: keys, idx: len(keys) - 1}
	it.Seek(end) // end is exclusive; start at first >= end, then step back via Next() if needed.
	return it, nil
}

func (m *MockBackend) Print() error             { return nil }
func (m *MockBackend) Stats() map[string]string { return nil }

// NewBatch returns a struct that satisfies BatchInterface
func (m *MockBackend) NewBatch() batch.Interface {
	return &MockBatch{mb: m}
}

type MockBatch struct {
	mb *MockBackend
}

func (b *MockBatch) Set(key, value []byte) error {
	if err := b.mb.getSetErr(); err != nil {
		return err
	}
	b.mb.Set(key, value)
	return nil
}
func (b *MockBatch) Delete(key []byte) error {
	if err := b.mb.getDeleteErr(); err != nil {
		return err
	}
	b.mb.mu.Lock()
	delete(b.mb.data, string(key))
	b.mb.mu.Unlock()
	return nil
}
func (b *MockBatch) SetOps(ops []batch.Entry) error {
	if err := b.mb.getSetOpsErr(); err != nil {
		return err
	}
	b.mb.mu.Lock()
	defer b.mb.mu.Unlock()
	for _, op := range ops {
		if op.Type == batch.OpDelete {
			delete(b.mb.data, string(op.Key))
		} else {
			b.mb.data[string(op.Key)] = op.Value
		}
	}
	return nil
}

func (b *MockBatch) Replay(fn func(batch.Entry) error) error {
	return nil
}

func (b *MockBatch) Write() error {
	if err := b.mb.getWriteErr(); err != nil {
		return err
	}
	b.mb.mu.Lock()
	b.mb.writeCalls++
	b.mb.mu.Unlock()
	return nil
}

func (b *MockBatch) WriteSync() error {
	if err := b.mb.getWriteErr(); err != nil {
		return err
	}
	b.mb.mu.Lock()
	b.mb.writeCalls++
	b.mb.writeSyncs++
	b.mb.mu.Unlock()
	return nil
}

func (b *MockBatch) Close() error              { return nil }
func (b *MockBatch) GetByteSize() (int, error) { return 0, nil }

func (m *MockBackend) Close() error { return nil }

func TestCachingDB_WriteAndFlush(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Threshold 1 byte to trigger flush
	db, err := Open(dir, backend, Options{FlushThreshold: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write 10 keys (should fit in memtable or trigger flush)
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		v := []byte(fmt.Sprintf("v%d", i))
		if err := db.SetSync(k, v); err != nil {
			t.Fatalf("SetSync: %v", err)
		}
	}

	// Verify visibility (Get)
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		val, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(val) != fmt.Sprintf("v%d", i) {
			t.Errorf("Get %s: got %q", k, val)
		}
	}

	// Close to flush everything
	db.Close()

	// Verify backend received data
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%d", i)
		if string(backend.data[k]) != fmt.Sprintf("v%d", i) {
			t.Errorf("Backend missing %s", k)
		}
	}
}

func TestCachingDB_FlushSyncsWhenWALDisabled(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Drain(); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	backend.mu.RLock()
	writeSyncs := backend.writeSyncs
	writeCalls := backend.writeCalls
	backend.mu.RUnlock()

	if writeCalls == 0 {
		t.Fatalf("expected backend writes")
	}
	if writeSyncs != 0 {
		t.Fatalf("expected WAL-off flush to use non-sync backend writes; got %d syncs", writeSyncs)
	}
}

func TestCachingDB_FlushAllCombinesMemtables(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 60, FlushBuildConcurrency: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	db.mu.Lock()
	setMutable(db, []byte("k"), []byte("v1"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k"), []byte("v2"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k2"), []byte("v3"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	backend.mu.RUnlock()
	if writeCalls != 3 {
		t.Fatalf("expected 3 backend batch commits (sequential flush), got %d", writeCalls)
	}

	got, err := db.backend.Get([]byte("k"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("backend.Get(k): got %q want %q", got, "v2")
	}

	got, err = db.backend.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("backend.Get(k2): got %q want %q", got, "v3")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_FlushAllCombinesMemtablesParallel(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:        1 << 60,
		FlushBuildConcurrency: 4,
		FlushBuildMinEntries:  1,
		FlushBuildMinUnits:    2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	db.mu.Lock()
	setMutable(db, []byte("k"), []byte("v1"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k"), []byte("v2"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k2"), []byte("v3"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	backend.mu.RUnlock()
	if writeCalls != 1 {
		t.Fatalf("expected 1 backend batch commit (combined flush), got %d", writeCalls)
	}

	got, err := db.backend.Get([]byte("k"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("backend.Get(k): got %q want %q", got, "v2")
	}

	got, err = db.backend.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("backend.Get(k2): got %q want %q", got, "v3")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_DeleteRange_DisableWAL_CoversInMemoryDeletesBackend(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.Set([]byte("a"), []byte("va"))
	backend.Set([]byte("b"), []byte("vb"))

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Set([]byte("c"), []byte("vc")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set([]byte("d"), []byte("vd")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.DeleteRange(nil, nil); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")} {
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if val != nil {
			t.Fatalf("expected %q to be deleted, got %q", key, val)
		}
	}

	if got := len(backend.data); got != 0 {
		t.Fatalf("expected backend to be empty, got %d keys", got)
	}
	if backend.writeCalls == 0 {
		t.Fatalf("expected backend batch write to be used")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_DeleteRange_DisableWAL_PartialRangeUsesTombstones(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set([]byte("z"), []byte("vz")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.DeleteRange([]byte("a"), []byte("m")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	val, err := db.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != nil {
		t.Fatalf("expected %q to be deleted, got %q", "a", val)
	}

	val, err = db.Get([]byte("z"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "vz" {
		t.Fatalf("expected %q, got %q", "vz", val)
	}

	if backend.writeCalls != 0 {
		t.Fatalf("expected no backend writes, got %d", backend.writeCalls)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_FlushAllParallelBuildPreservesNewestWins(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 60, FlushBuildConcurrency: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const keys = 1000

	db.mu.Lock()
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		setMutable(db, k, []byte("v1"))
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		setMutable(db, k, []byte("v2"))
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		if i%2 == 0 {
			deleteMutable(db, k)
		} else {
			setMutable(db, k, []byte("v3"))
		}
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	// Even keys should be deleted, odd keys should be v3 (newest memtable).
	got0, err := db.backend.Get([]byte("k0000"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if got0 != nil {
		t.Fatalf("expected k0000 deleted, got %q", got0)
	}
	got1, err := db.backend.Get([]byte("k0001"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got1) != "v3" {
		t.Fatalf("expected k0001=v3, got %q", got1)
	}
	got999, err := db.backend.Get([]byte("k0999"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got999) != "v3" {
		t.Fatalf("expected k0999=v3, got %q", got999)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

type MockReverseIterator struct {
	backend *MockBackend
	keys    []string
	idx     int
}

func (it *MockReverseIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *MockReverseIterator) Next() {
	it.idx--
}

func (it *MockReverseIterator) Seek(key []byte) {
	if len(it.keys) == 0 {
		it.idx = -1
		return
	}
	if key == nil {
		it.idx = len(it.keys) - 1
		return
	}

	// Find first key >= target.
	pos := sort.SearchStrings(it.keys, string(key))
	if pos >= len(it.keys) {
		it.idx = len(it.keys) - 1
		return
	}
	it.idx = pos
}

func (it *MockReverseIterator) UnsafeKey() []byte {
	return []byte(it.keys[it.idx])
}

func (it *MockReverseIterator) UnsafeValue() []byte {
	it.backend.mu.RLock()
	defer it.backend.mu.RUnlock()
	return it.backend.data[it.keys[it.idx]]
}

func (it *MockReverseIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.UnsafeValue(), page.ValuePtr{}, 0
}

func (it *MockReverseIterator) IsDeleted() bool           { return false }
func (it *MockReverseIterator) Error() error              { return nil }
func (it *MockReverseIterator) Close() error              { return nil }
func (it *MockReverseIterator) Domain() ([]byte, []byte)  { return nil, nil }
func (it *MockReverseIterator) Key() []byte               { return it.UnsafeKey() }
func (it *MockReverseIterator) Value() []byte             { return it.UnsafeValue() }
func (it *MockReverseIterator) KeyCopy(dst []byte) []byte { return append(dst[:0], it.UnsafeKey()...) }
func (it *MockReverseIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}

func TestCachingDB_IteratorIncludesBackendAfterStreamingBatch(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Large threshold so nothing flushes from memtable; we want the batch fast-path.
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	for i := 0; i < 64; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		v := []byte("v")
		if err := b.Set(k, v); err != nil {
			t.Fatalf("Batch.Set: %v", err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Batch.Write: %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	got := 0
	for it.Valid() {
		_ = it.Key()
		_ = it.Value()
		it.Next()
		got++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("Iterator.Error: %v", err)
	}
	if got != 64 {
		t.Fatalf("expected %d keys, got %d", 64, got)
	}
}

func TestCachingDB_NotifyErrorOnFlushFailure(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.SetWriteErr(errors.New("write failed"))

	errCh := make(chan error, 1)
	db, err := Open(dir, backend, Options{
		FlushThreshold: 1,
		NotifyError: func(err error) {
			select {
			case errCh <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	db.flushAll(false)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected non-nil error callback")
		}
	default:
		t.Fatalf("expected NotifyError to be called")
	}

	backend.SetWriteErr(nil)
	if err := db.Close(); err == nil {
		t.Fatalf("expected Close to return background error")
	}
}

func TestCachingDB_IteratorDoesNotBlockOnWriteMu(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	done := make(chan error, 1)
	go func() {
		it, err := db.Iterator(nil, nil)
		if err != nil {
			done <- err
			return
		}
		done <- it.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Iterator: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("iterator creation blocked behind writeMu")
	}
}

func TestCachingDB_SetDoesNotBlockOnWriteMuRLock(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.writeMu.RLock()
	defer db.writeMu.RUnlock()

	done := make(chan error, 1)
	go func() {
		done <- db.Set([]byte("k2"), []byte("v2"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Set blocked behind writeMu RLock")
	}
}

func TestCachingDB_FlushPersistsValueLogPointer(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	key := []byte("k1")
	val := bytes.Repeat([]byte("v"), page.DefaultInlineThreshold+64)
	if err := cache.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	cache.flushAll(true)

	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	entry, err := snap.GetEntry(key)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		_ = snap.Close()
		t.Fatalf("expected backend to persist value-log pointer, got flags=%#x file_id=%#x", entry.Flags, entry.ValuePtr.FileID)
	}
	_ = snap.Close()

	got, err := backend.Get(key)
	if err != nil {
		t.Fatalf("backend Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("backend Get mismatch")
	}
}

func TestCachingDB_CloseDeferredValueLogDoesNotFail(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		FlushThreshold:           1 << 60,
		ValueLogPointerThreshold: 1,
		DisableWAL:               true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}

	key := []byte("k1")
	valSize := 1 << 20
	val := bytes.Repeat([]byte("v"), valSize)
	for i := 0; i < 4; i++ {
		if err := cache.Set(key, val); err != nil {
			_ = cache.Close()
			t.Fatalf("Set: %v", err)
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCachingDB_ValueLogHardCapDisablesPointers(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                  true,
		FlushThreshold:               1 << 30,
		ValueLogPointerThreshold:     1,
		MaxValueLogRetainedBytesHard: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	val := bytes.Repeat([]byte("v"), page.DefaultInlineThreshold+64)
	if err := cache.Set([]byte("k1"), val); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}

	// Ensure buffered value-log writes are visible in retention stats.
	if err := cache.flushValueLog(); err != nil {
		t.Fatalf("flushValueLog: %v", err)
	}
	_, bytes1 := cache.valueLogRetainedStats()
	if bytes1 <= 0 {
		t.Fatalf("expected retained value-log bytes after first large value, got %d", bytes1)
	}

	if err := cache.Set([]byte("k2"), val); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	if err := cache.flushValueLog(); err != nil {
		t.Fatalf("flushValueLog: %v", err)
	}
	_, bytes2 := cache.valueLogRetainedStats()

	// Hard cap should disable *new* value-log pointers once retained bytes exceed
	// the cap; retained bytes should stop growing after the cap trips.
	if bytes2 != bytes1 {
		t.Fatalf("expected retained value-log bytes to stop growing after hard cap (before=%d after=%d)", bytes1, bytes2)
	}
}

func TestCachingDB_PrunesRetainedValueLog(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	key := []byte("k1")
	large := bytes.Repeat([]byte("v"), page.DefaultInlineThreshold+64)

	// Flush without a durability boundary so WAL/value-log segments remain and
	// show up as retained.
	if err := cache.Set(key, large); err != nil {
		t.Fatalf("Set(large): %v", err)
	}
	cache.flushAll(false)
	stats := cache.Stats()
	segments, err := strconv.Atoi(stats["treedb.cache.vlog_retained_segments"])
	if err != nil {
		t.Fatalf("parse retained segments: %v", err)
	}
	if segments == 0 {
		t.Fatalf("expected retained value-log segments after non-sync flush")
	}

	// Delete the key and checkpoint. The value-log segments that only contain
	// now-unreferenced payloads should become reclaimable and be pruned.
	if err := cache.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats = cache.Stats()
	segments, err = strconv.Atoi(stats["treedb.cache.vlog_retained_segments"])
	if err != nil {
		t.Fatalf("parse retained segments: %v", err)
	}
	if segments != 0 {
		t.Fatalf("expected retained segments to be pruned after checkpoint, got %d", segments)
	}
}
