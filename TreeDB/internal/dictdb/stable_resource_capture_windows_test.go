//go:build windows

package dictdb

import (
	"context"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCaptureDictionaryResourcesUsesCreateOnlyWindowsEvidence(t *testing.T) {
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
	if err != nil {
		t.Fatalf("capture dictionary resources: %v", err)
	}
	if resources == nil {
		t.Fatal("capture dictionary resources returned nil authority")
	}
	defer resources.Release()
	if err := resources.SyncThrough(); err != nil {
		t.Fatalf("sync dictionary authority: %v", err)
	}
}
