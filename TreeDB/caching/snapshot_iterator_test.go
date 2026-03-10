package caching

import (
	"reflect"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestSnapshotIterator_QueueValueOverridesPublishedAndTombstoneHidesPublished(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	if err := backend.SetSync([]byte("a"), []byte("backend_a")); err != nil {
		t.Fatalf("backend set a: %v", err)
	}
	if err := backend.SetSync([]byte("b"), []byte("backend_b")); err != nil {
		t.Fatalf("backend set b: %v", err)
	}
	if err := backend.SetSync([]byte("c"), []byte("backend_c")); err != nil {
		t.Fatalf("backend set c: %v", err)
	}

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open caching db: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("b"), []byte("queue_b")); err != nil {
		t.Fatalf("set queued b: %v", err)
	}
	if err := db.Delete([]byte("c")); err != nil {
		t.Fatalf("delete queued c: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	it, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("snapshot iterator: %v", err)
	}
	defer it.Close()

	if err := db.Set([]byte("d"), []byte("post_open_queue")); err != nil {
		t.Fatalf("post-open queued set: %v", err)
	}
	if err := backend.SetSync([]byte("e"), []byte("post_open_backend")); err != nil {
		t.Fatalf("post-open backend set: %v", err)
	}

	var gotKeys []string
	values := make(map[string]string)
	for it.Valid() {
		k := string(it.Key())
		gotKeys = append(gotKeys, k)
		values[k] = string(it.Value())
		it.Next()
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	wantKeys := []string{"a", "b"}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("keys: got=%v want=%v", gotKeys, wantKeys)
	}
	if values["a"] != "backend_a" {
		t.Fatalf("a value: got=%q want=%q", values["a"], "backend_a")
	}
	if values["b"] != "queue_b" {
		t.Fatalf("b value: got=%q want=%q", values["b"], "queue_b")
	}
	if _, ok := values["c"]; ok {
		t.Fatal("unexpected tombstoned key c")
	}
	if _, ok := values["d"]; ok {
		t.Fatal("unexpected post-open queued key d")
	}
	if _, ok := values["e"]; ok {
		t.Fatal("unexpected post-open backend key e")
	}
}

func TestSnapshotReverseIterator_QueueValueOverridesPublishedAndTombstoneHidesPublished(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	if err := backend.SetSync([]byte("a"), []byte("backend_a")); err != nil {
		t.Fatalf("backend set a: %v", err)
	}
	if err := backend.SetSync([]byte("b"), []byte("backend_b")); err != nil {
		t.Fatalf("backend set b: %v", err)
	}
	if err := backend.SetSync([]byte("c"), []byte("backend_c")); err != nil {
		t.Fatalf("backend set c: %v", err)
	}

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open caching db: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("b"), []byte("queue_b")); err != nil {
		t.Fatalf("set queued b: %v", err)
	}
	if err := db.Delete([]byte("c")); err != nil {
		t.Fatalf("delete queued c: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	it, err := snap.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("snapshot reverse iterator: %v", err)
	}
	defer it.Close()

	if err := db.Set([]byte("d"), []byte("post_open_queue")); err != nil {
		t.Fatalf("post-open queued set: %v", err)
	}
	if err := backend.SetSync([]byte("e"), []byte("post_open_backend")); err != nil {
		t.Fatalf("post-open backend set: %v", err)
	}

	var gotKeys []string
	values := make(map[string]string)
	for it.Valid() {
		k := string(it.Key())
		gotKeys = append(gotKeys, k)
		values[k] = string(it.Value())
		it.Next()
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	wantKeys := []string{"b", "a"}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("keys: got=%v want=%v", gotKeys, wantKeys)
	}
	if values["a"] != "backend_a" {
		t.Fatalf("a value: got=%q want=%q", values["a"], "backend_a")
	}
	if values["b"] != "queue_b" {
		t.Fatalf("b value: got=%q want=%q", values["b"], "queue_b")
	}
	if _, ok := values["c"]; ok {
		t.Fatal("unexpected tombstoned key c")
	}
	if _, ok := values["d"]; ok {
		t.Fatal("unexpected post-open queued key d")
	}
	if _, ok := values["e"]; ok {
		t.Fatal("unexpected post-open backend key e")
	}
}
