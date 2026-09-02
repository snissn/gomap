package collections

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkCollectionVectorIndexMetadataCreateDrop(b *testing.B) {
	d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		const name = "embedding"
		if _, err := col.CreateVectorIndex(VectorIndexDefinition{Name: name, Field: "embedding", Dimensions: 64}); err != nil {
			b.Fatalf("create vector index: %v", err)
		}
		if _, err := col.DropVectorIndex(name); err != nil {
			b.Fatalf("drop vector index: %v", err)
		}
	}
}

func BenchmarkCollectionOpenVectorIndexMetadata(b *testing.B) {
	dir := b.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Dimensions: 64,
		}},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	if err := d.Close(); err != nil {
		b.Fatalf("close db: %v", err)
	}
	d = nil

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			b.Fatalf("reopen db: %v", err)
		}
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			_ = d.Close()
			b.Fatalf("open collection: %v", err)
		}
		if len(col.Meta().VectorIndexes) != 1 {
			_ = d.Close()
			b.Fatalf("vector indexes=%d want 1", len(col.Meta().VectorIndexes))
		}
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
}
