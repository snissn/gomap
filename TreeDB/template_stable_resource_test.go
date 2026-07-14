//go:build !windows

package treedb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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

func TestTemplateKVStableCaptureSerializesConcurrentCloseThroughTokenConstruction(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	store := templatedb.New(templateKV{db: database}, templatedb.Config{})
	templateID, err := store.PutTemplateDef(context.Background(), []byte("concurrent-close-template"), nil)
	if err != nil {
		_ = database.Close()
		t.Fatalf("put template: %v", err)
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce, releaseOnce sync.Once
	testBeforeStableTemplateSnapshotAcquire = func() {
		reachedOnce.Do(func() { close(reached) })
		<-release
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		testBeforeStableTemplateSnapshotAcquire = nil
		_ = database.Close()
	})

	type captureResult struct {
		resources *rootpublication.StableResourceSet
		err       error
	}
	captureDone := make(chan captureResult, 1)
	go func() {
		resources, captureErr := store.CaptureTemplateResources(context.Background(), templateID)
		captureDone <- captureResult{resources: resources, err: captureErr}
	}()
	<-reached

	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- database.Close()
	}()
	<-closeStarted
	select {
	case closeErr := <-closeDone:
		t.Fatalf("close crossed an in-flight stable template capture: %v", closeErr)
	case <-time.After(50 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })
	result := <-captureDone
	if result.err != nil {
		t.Fatalf("capture during concurrent close: %v", result.err)
	}
	if result.resources == nil {
		t.Fatal("capture during concurrent close returned nil resources")
	}
	result.resources.Release()
	if closeErr := <-closeDone; closeErr != nil {
		t.Fatalf("close after stable capture: %v", closeErr)
	}
}
