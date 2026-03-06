package collections_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestCollectionBenchmarkEngineDefaultIsCached(t *testing.T) {
	t.Setenv("TREEDB_COLLECTION_BENCH_ENGINE", "")

	engine, err := collectionBenchmarkEngine()
	if err != nil {
		t.Fatalf("default benchmark engine: %v", err)
	}
	if engine.name != "cached" {
		t.Fatalf("default benchmark engine = %q, want cached", engine.name)
	}
}

func TestCollectionBenchmarkEngineAllowsBackendOverride(t *testing.T) {
	t.Setenv("TREEDB_COLLECTION_BENCH_ENGINE", "backend_direct")

	engine, err := collectionBenchmarkEngine()
	if err != nil {
		t.Fatalf("override benchmark engine: %v", err)
	}
	if engine.name != "backend_direct" {
		t.Fatalf("override benchmark engine = %q, want backend_direct", engine.name)
	}
}

func TestCachedBenchmarkDefaultCheckpointedLookup(t *testing.T) {
	t.Setenv("TREEDB_COLLECTION_BENCH_ENGINE", "")

	manager, checkpoint, cleanup := openCollectionBenchmarkManager(t)
	defer cleanup()

	meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := manager.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	const count = 1024
	emails := make([]string, count)
	for i := 0; i < count; i++ {
		emails[i] = fmt.Sprintf("user-%d@example.com", i)
		if _, err := col.Insert([]byte(fmt.Sprintf("u-%d", i)), []byte(fmt.Sprintf(`{"email":"%s"}`, emails[i]))); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}
	checkpoint()

	for i := 0; i < 1024; i++ {
		ids, err := col.FindByIndex("email_idx", emails[i%count])
		if err != nil {
			t.Fatalf("find after checkpoint (iteration %d): %v", i, err)
		}
		want := []byte(fmt.Sprintf("u-%d", i%count))
		if len(ids) != 1 || !bytes.Equal(ids[0], want) {
			t.Fatalf("ids after checkpoint (iteration %d) = %#v, want %q", i, ids, want)
		}
	}
}
