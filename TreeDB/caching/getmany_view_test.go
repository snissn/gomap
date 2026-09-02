package caching

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/tree"
)

type getManyViewBackendStub struct {
	values           map[string][]byte
	getManyCalls     int
	getManyViewCalls int
	getCalls         int
}

func (b *getManyViewBackendStub) Get(key []byte) ([]byte, error) {
	b.getCalls++
	if val, ok := b.values[string(key)]; ok {
		return val, nil
	}
	return nil, nil
}

func (b *getManyViewBackendStub) GetUnsafe(key []byte) ([]byte, error) { return b.Get(key) }

func (b *getManyViewBackendStub) GetAppend(key, dst []byte) ([]byte, error) {
	if val, ok := b.values[string(key)]; ok {
		return append(dst, val...), nil
	}
	return dst, tree.ErrKeyNotFound
}

func (b *getManyViewBackendStub) GetMany(keys [][]byte) ([][]byte, error) {
	b.getManyCalls++
	return nil, errors.New("backend GetMany should not be used by cached GetManyView")
}

func (b *getManyViewBackendStub) GetManyView(keys [][]byte, fn tree.GetManyViewFunc) error {
	b.getManyViewCalls++
	for i, key := range keys {
		val, found := b.values[string(key)]
		if err := fn(i, key, val, found); err != nil {
			return err
		}
	}
	return nil
}

func (b *getManyViewBackendStub) Has(key []byte) (bool, error) {
	_, ok := b.values[string(key)]
	return ok, nil
}

func (b *getManyViewBackendStub) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return nil, backenddb.ErrClosed
}

func (b *getManyViewBackendStub) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return nil, backenddb.ErrClosed
}

func (b *getManyViewBackendStub) NewBatch() batch.Interface { return nil }
func (b *getManyViewBackendStub) Close() error              { return nil }
func (b *getManyViewBackendStub) Print() error              { return nil }
func (b *getManyViewBackendStub) Stats() map[string]string  { return nil }

func collectGetManyViewResults(t *testing.T, keys [][]byte, call func(tree.GetManyViewFunc) error) (seen []int, found []bool, values [][]byte) {
	t.Helper()
	seen = make([]int, len(keys))
	found = make([]bool, len(keys))
	values = make([][]byte, len(keys))
	err := call(func(index int, key []byte, value []byte, ok bool) error {
		if index < 0 || index >= len(keys) {
			return fmt.Errorf("callback index %d outside %d keys", index, len(keys))
		}
		if !bytes.Equal(key, keys[index]) {
			return fmt.Errorf("callback key[%d]=%q want %q", index, key, keys[index])
		}
		seen[index]++
		found[index] = ok
		if ok {
			values[index] = append([]byte(nil), value...)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("GetManyView: %v", err)
	}
	return seen, found, values
}

func assertGetManyViewResults(t *testing.T, keys [][]byte, seen []int, found []bool, values [][]byte) {
	t.Helper()
	want := map[int][]byte{
		0: []byte("value-a"),
		1: []byte("value-b"),
		2: []byte("value-a"),
		4: []byte{},
	}
	for i := range keys {
		if seen[i] != 1 {
			t.Fatalf("callback count for index %d = %d, want 1", i, seen[i])
		}
		wantVal, wantFound := want[i]
		if found[i] != wantFound {
			t.Fatalf("found[%d]=%v want %v", i, found[i], wantFound)
		}
		if wantFound && !bytes.Equal(values[i], wantVal) {
			t.Fatalf("value[%d]=%q want %q", i, values[i], wantVal)
		}
		if !wantFound && values[i] != nil {
			t.Fatalf("missing value[%d]=%q want nil", i, values[i])
		}
	}
}

func TestGetManyViewBypassUsesBackendView(t *testing.T) {
	backend := &getManyViewBackendStub{values: map[string][]byte{
		"a":     []byte("value-a"),
		"b":     []byte("value-b"),
		"empty": []byte{},
	}}
	db := &DB{backend: backend}
	view := &memtableView{}
	view.refs.Store(1)
	db.memtables.Store(view)

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("a"), []byte("missing"), []byte("empty")}
	seen, found, values := collectGetManyViewResults(t, keys, func(fn tree.GetManyViewFunc) error {
		return db.GetManyView(keys, fn)
	})
	assertGetManyViewResults(t, keys, seen, found, values)
	if backend.getManyViewCalls != 1 {
		t.Fatalf("backend GetManyView calls=%d want 1", backend.getManyViewCalls)
	}
	if backend.getManyCalls != 0 || backend.getCalls != 0 {
		t.Fatalf("backend safe-copy reads used: GetMany=%d Get=%d", backend.getManyCalls, backend.getCalls)
	}
}

func TestGetManyViewBackendFallbackUsesBackendView(t *testing.T) {
	backend := &getManyViewBackendStub{values: map[string][]byte{
		"a":     []byte("value-a"),
		"b":     []byte("value-b"),
		"empty": []byte{},
	}}
	db := &DB{backend: backend}
	view := &memtableView{rootPointShards: []rootDomainSnapshot{{}}}

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("a"), []byte("missing"), []byte("empty")}
	seen, found, values := collectGetManyViewResults(t, keys, func(fn tree.GetManyViewFunc) error {
		return db.getManyViewFromPublishedRootPointShards(view, keys, fn)
	})
	assertGetManyViewResults(t, keys, seen, found, values)
	if backend.getManyViewCalls != 1 {
		t.Fatalf("backend GetManyView calls=%d want 1", backend.getManyViewCalls)
	}
	if backend.getManyCalls != 0 || backend.getCalls != 0 {
		t.Fatalf("backend safe-copy reads used: GetMany=%d Get=%d", backend.getManyCalls, backend.getCalls)
	}
}
