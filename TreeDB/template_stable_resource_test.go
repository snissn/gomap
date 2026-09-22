//go:build !windows

package treedb

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/page"
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

func TestTemplateKVStableCaptureResourcesOutliveDatabaseClose(t *testing.T) {
	opts := Options{Dir: t.TempDir()}
	opts.ValueLog.ForcePointers = true
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = database.Close()
		}
	})
	registry := database.backend.StableResourceIdentityPinRegistry()
	store := templatedb.New(templateKV{db: database}, templatedb.Config{})
	definition := make([]byte, 4096)
	if _, err := rand.New(rand.NewSource(2)).Read(definition); err != nil {
		t.Fatalf("prepare template: %v", err)
	}
	templateID, err := store.PutTemplateDef(context.Background(), definition, nil)
	if err != nil {
		t.Fatalf("put template: %v", err)
	}
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture template: %v", err)
	}
	if len(resources.Tokens()) < 2 {
		resources.Release()
		t.Fatalf("captured stable resources=%d want at least index and value-log identities", len(resources.Tokens()))
	}
	// The capture also owns the parent namespace identity used to prove the
	// exact path binding. Namespace pins are intentionally not exposed as
	// readable resource tokens.
	wantPins := registry.ActivePins()
	if wantPins < uint64(len(resources.Tokens())) {
		resources.Release()
		t.Fatalf("captured stable-resource identity pins=%d want at least token count %d", wantPins, len(resources.Tokens()))
	}

	if err := database.Close(); err != nil {
		resources.Release()
		t.Fatalf("close with live stable resources: %v", err)
	}
	closed = true
	// Closing the database releases its namespace-owner pin. The captured
	// readable identities remain pinned by the caller-owned resource set.
	wantRetainedPins := uint64(len(resources.Tokens()))
	if got := registry.ActivePins(); got != wantRetainedPins {
		resources.Release()
		t.Fatalf("identity pins after database close=%d want %d retained resource tokens", got, wantRetainedPins)
	}
	for _, token := range resources.Tokens() {
		buf := make([]byte, 1)
		if n, err := token.ReadAt(buf, 0); err != nil || n != len(buf) {
			resources.Release()
			t.Fatalf("read retained %q after database close: n=%d err=%v", token.DiagnosticPath(), n, err)
		}
	}
	resources.Release()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("identity pins after resource release=%d want 0", got)
	}
}

func TestTemplateKVStableResourceReleaseRacesDatabaseClose(t *testing.T) {
	for _, forcePointers := range []bool{false, true} {
		name := "inline"
		if forcePointers {
			name = "pointer"
		}
		t.Run(name, func(t *testing.T) {
			opts := Options{Dir: t.TempDir()}
			opts.ValueLog.ForcePointers = forcePointers
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()

			definition := []byte("inline-release-close-race")
			if forcePointers {
				definition = make([]byte, 4096)
				if _, err := rand.New(rand.NewSource(3)).Read(definition); err != nil {
					t.Fatalf("prepare template: %v", err)
				}
			}
			store := templatedb.New(templateKV{db: database}, templatedb.Config{})
			templateID, err := store.PutTemplateDef(context.Background(), definition, nil)
			if err != nil {
				t.Fatalf("put template: %v", err)
			}
			resources, err := store.CaptureTemplateResources(context.Background(), templateID)
			if err != nil {
				t.Fatalf("capture template: %v", err)
			}
			registry := database.backend.StableResourceIdentityPinRegistry()

			start := make(chan struct{})
			closeDone := make(chan error, 1)
			releaseDone := make(chan struct{})
			go func() {
				<-start
				closeDone <- database.Close()
			}()
			go func() {
				<-start
				resources.Release()
				close(releaseDone)
			}()
			close(start)
			if closeErr := <-closeDone; closeErr != nil {
				t.Fatalf("close racing stable resource release: %v", closeErr)
			}
			<-releaseDone
			if got := registry.ActivePins(); got != 0 {
				t.Fatalf("identity pins after concurrent close/release=%d want 0", got)
			}
			resources.Release()
		})
	}
}

func TestTemplateKVStableCaptureResolvesOmittedGroupedRecordLength(t *testing.T) {
	opts := Options{Dir: t.TempDir()}
	opts.ValueLog.ForcePointers = true
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	store := templatedb.New(templateKV{db: database}, templatedb.Config{})
	// Grouped pointers have only 23 bits for their record-length hint. A valid
	// larger frame therefore carries a zero hint and must derive its exact
	// capture frontier from the pinned value-log record header.
	definition := make([]byte, int(page.ValuePtrGroupedMaxRecordLen)+1)
	if _, err := rand.New(rand.NewSource(1)).Read(definition); err != nil {
		t.Fatalf("prepare large template: %v", err)
	}
	templateID, err := store.PutTemplateDef(context.Background(), definition, nil)
	if err != nil {
		t.Fatalf("put large template: %v", err)
	}

	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture large template with omitted record-length hint: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 2 {
		t.Fatalf("closure len=%d want index+value-log", got)
	}
	for _, token := range resources.Tokens() {
		if token.DiagnosticPath() == "index.db" {
			continue
		}
		if got := token.Frontier().Bytes; got <= uint64(page.ValuePtrGroupedMaxRecordLen) {
			t.Fatalf("value-log frontier=%d want header-derived frontier beyond grouped hint limit", got)
		}
	}
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
