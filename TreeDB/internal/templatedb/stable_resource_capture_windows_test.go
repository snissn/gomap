//go:build windows

package templatedb

import (
	"context"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCaptureTemplateResourcesUsesCreateOnlyWindowsEvidence(t *testing.T) {
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
	if err != nil {
		t.Fatalf("capture template resources: %v", err)
	}
	if resources == nil {
		t.Fatal("capture template resources returned nil authority")
	}
	defer resources.Release()
	if err := resources.SyncThrough(); err != nil {
		t.Fatalf("sync template authority: %v", err)
	}
}
