//go:build !windows

package treedb

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/templatedb"
)

func TestTemplateKVStableCaptureSeesSynchronousPublication(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.cached == nil {
		t.Fatal("test requires cached public adapter")
	}
	store := templatedb.New(templateKV{db: database}, templatedb.Config{})
	templateID, err := store.PutTemplateDef(context.Background(), []byte("cached-template-definition"), nil)
	if err != nil {
		t.Fatalf("put template: %v", err)
	}
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture just-published template: %v", err)
	}
	resources.Release()
}
