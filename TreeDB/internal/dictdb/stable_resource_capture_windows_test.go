//go:build windows

package dictdb

import (
	"context"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureDictionaryResourcesFailsClosedWithoutDurableNamespacePrimitive(t *testing.T) {
	store, err := Open(t.TempDir(), backenddb.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open dictionary store: %v", err)
	}
	defer store.Close()
	dictID, err := store.PutDictBytes(context.Background(), []byte("windows dictionary authority"))
	if err != nil {
		t.Fatalf("put dictionary: %v", err)
	}
	resources, err := store.CaptureDictionaryResources(context.Background(), dictID)
	if resources != nil {
		resources.Release()
		t.Fatal("unsupported Windows namespace capture returned resources")
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("capture error=%v want namespace persistence unsupported", err)
	}
}
