package btreeonhashdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/snissn/gomap/HashDB"
)

type mockKV struct {
	data map[string][]byte
}

func newMockKV() *mockKV {
	return &mockKV{data: make(map[string][]byte)}
}

func (m *mockKV) Get(key []byte) ([]byte, error) {
	v, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	cpy := make([]byte, len(v))
	copy(cpy, v)
	return cpy, nil
}

func (m *mockKV) Put(key, value []byte) error {
	cpy := make([]byte, len(value))
	copy(cpy, value)
	m.data[string(key)] = cpy
	return nil
}

func (m *mockKV) Delete(key []byte) error {
	delete(m.data, string(key))
	return nil
}

func TestBasicCRUD(t *testing.T) {
	tree, err := OpenTree(newMockKV(), "basic", nil)
	if err != nil {
		t.Fatalf("OpenTree: %v", err)
	}

	k := []byte("foo")
	v1 := []byte("bar")
	v2 := []byte("baz")

	if err := tree.Put(k, v1); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := tree.Get(k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, v1) {
		t.Fatalf("expected %s, got %s", v1, got)
	}

	if err := tree.Put(k, v2); err != nil {
		t.Fatalf("Put update: %v", err)
	}
	got, _ = tree.Get(k)
	if !bytes.Equal(got, v2) {
		t.Fatalf("expected %s after update, got %s", v2, got)
	}

	if err := tree.Delete(k); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, _ = tree.Get(k)
	if got != nil {
		t.Fatalf("expected nil after delete, got %s", got)
	}
}

func TestSplitAndOrdering(t *testing.T) {
	tree, err := OpenTree(newMockKV(), "split", nil)
	if err != nil {
		t.Fatalf("OpenTree: %v", err)
	}

	n := MaxKeys*4 + 37
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("val-%04d", i))
		if err := tree.Put(k, v); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	iter, err := tree.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	defer iter.Close()

	count := 0
	var last []byte
	for iter.Valid() {
		k := iter.Key()
		if last != nil && bytes.Compare(k, last) <= 0 {
			t.Fatalf("order violation: %s <= %s", k, last)
		}
		count++
		last = append([]byte{}, k...)
		iter.Next()
	}
	if iter.Error() != nil {
		t.Fatalf("iterator error: %v", iter.Error())
	}
	if count != n {
		t.Fatalf("expected %d keys, got %d", n, count)
	}
}

func TestRangeQueries(t *testing.T) {
	tree, err := OpenTree(newMockKV(), "range", nil)
	if err != nil {
		t.Fatalf("OpenTree: %v", err)
	}

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		if err := tree.Put([]byte(k), []byte(k)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	checkRange := func(start, end []byte, expected []string) {
		iter, err := tree.Range(start, end)
		if err != nil {
			t.Fatalf("Range %s-%s: %v", start, end, err)
		}
		defer iter.Close()

		var got []string
		for iter.Valid() {
			got = append(got, string(iter.Key()))
			iter.Next()
		}
		if iter.Error() != nil {
			t.Fatalf("iter error: %v", iter.Error())
		}
		if len(got) != len(expected) {
			t.Fatalf("expected %v, got %v", expected, got)
		}
		for i := range expected {
			if got[i] != expected[i] {
				t.Fatalf("expected %v, got %v", expected, got)
			}
		}
	}

	checkRange([]byte("b"), []byte("d"), []string{"b", "c"})
	checkRange(nil, []byte("c"), []string{"a", "b"})
	checkRange([]byte("d"), nil, []string{"d", "e"})
	checkRange([]byte("c"), []byte("c"), []string{})
}

func TestReverseRange(t *testing.T) {
	tree, err := OpenTree(newMockKV(), "rev_range", nil)
	if err != nil {
		t.Fatalf("OpenTree: %v", err)
	}

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		if err := tree.Put([]byte(k), []byte(k)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	iter, err := tree.ReverseRange(nil, nil)
	if err != nil {
		t.Fatalf("ReverseRange: %v", err)
	}
	defer iter.Close()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	if iter.Error() != nil {
		t.Fatalf("iter error: %v", iter.Error())
	}
	expectedAll := []string{"e", "d", "c", "b", "a"}
	if fmt.Sprint(got) != fmt.Sprint(expectedAll) {
		t.Fatalf("expected %v got %v", expectedAll, got)
	}

	iter, err = tree.ReverseRange([]byte("b"), []byte("e"))
	if err != nil {
		t.Fatalf("ReverseRange b-e: %v", err)
	}
	defer iter.Close()
	got = got[:0]
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	expected := []string{"d", "c", "b"}
	if fmt.Sprint(got) != fmt.Sprint(expected) {
		t.Fatalf("expected %v got %v", expected, got)
	}
}

func TestPersistenceWithHashDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "btree-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ref := make(map[string]string)

	store := &hashdb.HashDB{}
	if err := store.NewWithShards(dir, 4); err != nil {
		t.Fatalf("init hashdb: %v", err)
	}

	tree, err := NewTreeOnHashDB(store, "persist", nil)
	if err != nil {
		t.Fatalf("OpenTree: %v", err)
	}

	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("p-%04d", i)
		v := fmt.Sprintf("val-%04d", i)
		ref[k] = v
		if err := tree.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// Ensure all cached writes are flushed to disk before reopening.
	if err := store.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	store2 := &hashdb.HashDB{}
	if err := store2.NewWithShards(dir, 4); err != nil {
		t.Fatalf("reopen hashdb: %v", err)
	}
	defer store2.Close()

	tree2, err := NewTreeOnHashDB(store2, "persist", nil)
	if err != nil {
		t.Fatalf("reopen tree: %v", err)
	}

	for k, v := range ref {
		got, err := tree2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if string(got) != v {
			t.Fatalf("value mismatch for %s: want %s got %s", k, v, got)
		}
	}

	iter, err := tree2.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	defer iter.Close()

	var sortedKeys []string
	for k := range ref {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	idx := 0
	for iter.Valid() {
		if idx >= len(sortedKeys) {
			t.Fatalf("iterator returned too many keys")
		}
		if string(iter.Key()) != sortedKeys[idx] {
			t.Fatalf("order mismatch at %d: want %s got %s", idx, sortedKeys[idx], iter.Key())
		}
		idx++
		iter.Next()
	}
	if iter.Error() != nil {
		t.Fatalf("iterator error: %v", iter.Error())
	}
	if idx != len(sortedKeys) {
		t.Fatalf("iterator returned %d keys, expected %d", idx, len(sortedKeys))
	}
}

func TestRandomizedOperations(t *testing.T) {
	tree, err := OpenTree(newMockKV(), "rand", nil)
	if err != nil {
		t.Fatalf("OpenTree: %v", err)
	}

	rng := rand.New(rand.NewSource(42))
	ref := make(map[string]string)

	keySpace := 200
	ops := 2000

	for i := 0; i < ops; i++ {
		k := fmt.Sprintf("%04d", rng.Intn(keySpace))
		if rng.Intn(3) == 0 {
			// delete
			if err := tree.Delete([]byte(k)); err != nil {
				t.Fatalf("Delete %s: %v", k, err)
			}
			delete(ref, k)
			continue
		}

		v := fmt.Sprintf("val-%d", rng.Int())
		if err := tree.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		ref[k] = v
	}

	for i := 0; i < keySpace; i++ {
		k := fmt.Sprintf("%04d", i)
		got, err := tree.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		v, ok := ref[k]
		if !ok {
			if got != nil {
				t.Fatalf("expected %s to be absent, got %s", k, got)
			}
			continue
		}
		if string(got) != v {
			t.Fatalf("value mismatch for %s: want %s got %s", k, v, got)
		}
	}

	iter, err := tree.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	defer iter.Close()

	var sortedKeys []string
	for k := range ref {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	idx := 0
	for iter.Valid() {
		if idx >= len(sortedKeys) {
			t.Fatalf("iterator yielded extra key %s", iter.Key())
		}
		if string(iter.Key()) != sortedKeys[idx] {
			t.Fatalf("iter mismatch at %d: want %s got %s", idx, sortedKeys[idx], iter.Key())
		}
		idx++
		iter.Next()
	}
	if iter.Error() != nil {
		t.Fatalf("iterator error: %v", iter.Error())
	}
	if idx != len(sortedKeys) {
		t.Fatalf("iterator returned %d keys, expected %d", idx, len(sortedKeys))
	}
}

// TestRandomizedOperationsWithHashDB exercises the tree on top of HashDB with
// mixed put/delete operations, similar to the cosmos-db
// dbbench mixed phase. This helps catch integration issues that do not
// appear with the in-memory mockKV.
func TestRandomizedOperationsWithHashDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "btree-rand-hashdb-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store := &hashdb.HashDB{}
	if err := store.NewWithShards(dir, 8); err != nil {
		t.Fatalf("init hashdb: %v", err)
	}

	tree, err := NewTreeOnHashDB(store, "rand-hashdb", nil)
	if err != nil {
		t.Fatalf("OpenTree: %v", err)
	}

	rng := rand.New(rand.NewSource(99))
	ref := make(map[string]string)

	keySpace := 500
	ops := 5000

	for i := 0; i < ops; i++ {
		k := fmt.Sprintf("%04d", rng.Intn(keySpace))
		if rng.Intn(3) == 0 {
			// delete
			if err := tree.Delete([]byte(k)); err != nil {
				t.Fatalf("Delete %s: %v", k, err)
			}
			delete(ref, k)
			continue
		}

		v := fmt.Sprintf("val-%d", rng.Int())
		if err := tree.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		ref[k] = v
	}

	// Verify point lookups against reference map.
	for i := 0; i < keySpace; i++ {
		k := fmt.Sprintf("%04d", i)
		got, err := tree.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		v, ok := ref[k]
		if !ok {
			if got != nil {
				t.Fatalf("expected %s to be absent, got %s", k, got)
			}
			continue
		}
		if string(got) != v {
			t.Fatalf("value mismatch for %s: want %s got %s", k, v, got)
		}
	}

	// Verify full scan order and membership.
	iter, err := tree.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	defer iter.Close()

	var sortedKeys []string
	for k := range ref {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	idx := 0
	for iter.Valid() {
		if idx >= len(sortedKeys) {
			t.Fatalf("iterator yielded extra key %s", iter.Key())
		}
		if string(iter.Key()) != sortedKeys[idx] {
			t.Fatalf("iter mismatch at %d: want %s got %s", idx, sortedKeys[idx], iter.Key())
		}
		idx++
		iter.Next()
	}
	if iter.Error() != nil {
		t.Fatalf("iterator error: %v", iter.Error())
	}
	if idx != len(sortedKeys) {
		t.Fatalf("iterator returned %d keys, expected %d", idx, len(sortedKeys))
	}
}
