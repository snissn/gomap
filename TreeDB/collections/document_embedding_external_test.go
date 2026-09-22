package collections_test

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/collections/embedding"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestEmbedForIngestUnknownVectorIndexPublicError(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	_, err = col.EmbedForIngest(context.Background(), "missing", [][]byte{[]byte("text")},
		embedding.Config{Provider: embedding.ProviderHashing, Dimensions: 16})
	if !errors.Is(err, collections.ErrIndexNotFound) {
		t.Fatalf("EmbedForIngest error=%v want collections.ErrIndexNotFound", err)
	}
}
