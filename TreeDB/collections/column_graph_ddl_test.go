package collections

import (
	"errors"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVectorIndexDDLOwnerMixedIdempotentAndExpired(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	manager := NewCollectionManager(d)
	vector := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Strategy: VectorIndexStrategyColumnGraph}
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{vector}}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	var escaped *CollectionIndexDDL
	err = collection.WithVectorIndexDDL([]VectorIndexDefinition{vector}, func(owner *CollectionIndexDDL) error {
		escaped = owner
		if _, err := owner.CreateIndex(IndexDefinition{Name: "tenant", Field: "tenant", ValueType: IndexValueString}); err != nil {
			return err
		}
		before := owner.Meta().VectorIndexes[0]
		meta, err := owner.CreateVectorIndex(vector)
		if err != nil {
			return err
		}
		if !vectorIndexDefinitionValuesEqual(before, meta.VectorIndexes[0]) {
			t.Fatal("idempotent DDL changed stored vector identity")
		}
		if len(owner.Meta().Indexes) != 1 {
			t.Fatal("owner lost accepted scalar schema")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := escaped.CreateIndex(IndexDefinition{Name: "escaped", Field: "escaped", ValueType: IndexValueString}); err == nil {
		t.Fatal("escaped token published scalar index")
	}
	if _, err := escaped.CreateVectorIndex(vector); err == nil {
		t.Fatal("escaped token accepted vector DDL")
	}
	if escaped.Meta().Name != "" {
		t.Fatal("escaped owner retained schema access")
	}
	if len(collection.Meta().Indexes) != 1 {
		t.Fatal("escaped token changed collection schema")
	}
}

func TestVectorIndexDDLOwnerHoldsSchemaThroughPublications(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	manager := NewCollectionManager(d)
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	done := make(chan error, 1)
	err = collection.WithVectorIndexDDL(nil, func(owner *CollectionIndexDDL) error {
		go func() {
			close(entered)
			_, err := collection.CreateIndex(IndexDefinition{Name: "other", Field: "other", ValueType: IndexValueString})
			done <- err
		}()
		<-entered
		select {
		case err := <-done:
			t.Fatalf("concurrent DDL escaped schema authority: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		_, err := owner.CreateIndex(IndexDefinition{Name: "mine", Field: "mine", ValueType: IndexValueString})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(collection.Meta().Indexes) != 2 {
		t.Fatalf("schema after serialized publication: %+v", collection.Meta())
	}
}

func TestVectorIndexDDLOwnerRefusesUnsupportedBeforeCallback(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	manager := NewCollectionManager(d)
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = collection.WithVectorIndexDDL([]VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2}}, func(*CollectionIndexDDL) error { called = true; return nil })
	if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || called {
		t.Fatalf("unsupported vector reached callback: called=%v err=%v", called, err)
	}
}
