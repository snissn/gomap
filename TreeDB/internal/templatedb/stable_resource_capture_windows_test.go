//go:build windows

package templatedb

import (
	"context"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureTemplateResourcesFailsClosedWithoutDurableNamespacePrimitive(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	store := New(stableTestKV{db: backend}, Config{})
	templateID, err := store.PutTemplateDef(context.Background(), []byte("windows template authority"), nil)
	if err != nil {
		t.Fatalf("put template: %v", err)
	}
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if resources != nil {
		resources.Release()
		t.Fatal("unsupported Windows namespace capture returned resources")
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("capture error=%v want namespace persistence unsupported", err)
	}
}
