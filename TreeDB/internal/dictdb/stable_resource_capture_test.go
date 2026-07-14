//go:build !windows

package dictdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCaptureDictionaryResourcesReturnsExactTransitiveClosureForReusedPointerID(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	payload := bytes.Repeat([]byte("stable-dictionary-payload|"), 64)
	dictID, err := store.PutDictBytes(ctx, payload)
	if err != nil {
		t.Fatalf("put dictionary: %v", err)
	}
	reusedID, err := store.PutDictBytes(ctx, payload)
	if err != nil {
		t.Fatalf("reuse dictionary: %v", err)
	}
	if reusedID != dictID {
		t.Fatalf("reused id=%d want %d", reusedID, dictID)
	}

	resources, err := store.CaptureDictionaryResources(ctx, reusedID)
	if err != nil {
		t.Fatalf("capture reused dictionary: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 2 {
		t.Fatalf("resource closure len=%d want index+value-log", got)
	}
	if got := store.backend.StableResourceIdentityPinRegistry().ActivePins(); got != 1 {
		t.Fatalf("active value-log identity pins=%d want 1", got)
	}

	wantDigest := sha256.Sum256(payload)
	paths := make(map[string]bool, 2)
	for _, token := range resources.Tokens() {
		if token.Kind() != rootpublication.ResourceDictionary {
			t.Fatalf("resource kind=%q want dictionary", token.Kind())
		}
		if token.Reachability() != rootpublication.ReachabilityDictionaryGeneration {
			t.Fatalf("resource reachability=%q want dictionary generation", token.Reachability())
		}
		paths[token.DiagnosticPath()] = true
		if token.DiagnosticPath() == "index.db" && token.Namespace() == nil {
			t.Fatal("index token missing stable namespace evidence")
		}
		obligations := token.LogicalObligations()
		if len(obligations) != 1 {
			t.Fatalf("resource %q obligations=%d want 1", token.DiagnosticPath(), len(obligations))
		}
		obligation := obligations[0]
		if obligation.Generation != dictID || obligation.FileID != dictID ||
			obligation.Length != int64(len(payload)) || obligation.Digest != wantDigest {
			t.Fatalf("resource %q obligation=%+v want dict=%d len=%d digest=%x", token.DiagnosticPath(), obligation, dictID, len(payload), wantDigest)
		}
	}
	if !paths["index.db"] || !paths["value_vlog/value-l0-000001.log"] {
		t.Fatalf("resource paths=%v want exact index and value-log segment", paths)
	}
	if err := resources.SyncThrough(); err != nil {
		t.Fatalf("sync closure: %v", err)
	}
	resources.Release()
	if got := store.backend.StableResourceIdentityPinRegistry().ActivePins(); got != 0 {
		t.Fatalf("active value-log identity pins after release=%d want 0", got)
	}
}

func TestCaptureDictionaryResourcesReopenedPointerUsesExactRecordFrontier(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	payload := bytes.Repeat([]byte("reopened-dictionary|"), 128)
	dictID, err := store.PutDictBytes(context.Background(), payload)
	if err != nil {
		t.Fatalf("put dictionary: %v", err)
	}
	snapshot := store.backend.AcquireSnapshot()
	entry, err := snapshot.GetEntryExact(bytesKey(dictID))
	if err != nil {
		t.Fatalf("get dictionary entry: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if entry.Flags&1 == 0 {
		t.Fatal("dictionary unexpectedly stored inline")
	}
	wantFrontier := entry.ValuePtr.Offset + uint64(page.ValuePtrRecordLength(entry.ValuePtr))
	if err := store.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	store, err = Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()
	resources, err := store.CaptureDictionaryResources(context.Background(), dictID)
	if err != nil {
		t.Fatalf("capture reopened dictionary: %v", err)
	}
	defer resources.Release()
	for _, token := range resources.Tokens() {
		if token.DiagnosticPath() == "value_vlog/value-l0-000001.log" && token.Frontier().Bytes != wantFrontier {
			t.Fatalf("value-log frontier=%d want exact record end %d", token.Frontier().Bytes, wantFrontier)
		}
	}
}

func TestCaptureDictionaryResourcesFailureDoesNotLeakVacuumFence(t *testing.T) {
	store, err := Open(t.TempDir(), db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if resources, err := store.CaptureDictionaryResources(context.Background(), 42); err == nil || resources != nil {
		if resources != nil {
			resources.Release()
		}
		t.Fatalf("missing dictionary capture resources=%v err=%v", resources, err)
	}
	if got := store.backend.StableResourceIdentityPinRegistry().ActivePins(); got != 0 {
		t.Fatalf("pins after failed capture=%d want 0", got)
	}
}

func TestCaptureDictionaryResourcesBlocksOnlineIndexVacuumUntilRelease(t *testing.T) {
	store, err := Open(t.TempDir(), db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	dictID, err := store.PutDictBytes(context.Background(), []byte("stable-inline-dictionary"))
	if err != nil {
		t.Fatalf("put dictionary: %v", err)
	}
	resources, err := store.CaptureDictionaryResources(context.Background(), dictID)
	if err != nil {
		t.Fatalf("capture dictionary: %v", err)
	}
	if err := store.backend.VacuumIndexOnline(context.Background()); !errors.Is(err, rootpublication.ErrResourcePinned) {
		resources.Release()
		t.Fatalf("vacuum with live capture error=%v want ErrResourcePinned", err)
	}
	resources.Release()
	if err := store.backend.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("vacuum after release: %v", err)
	}
}

func TestCaptureDictionaryResourcesInlineDefinitionNeedsOnlyPinnedIndexGeneration(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	dictID, err := store.PutDictBytes(ctx, []byte("inline"))
	if err != nil {
		t.Fatalf("put inline dictionary: %v", err)
	}
	resources, err := store.CaptureDictionaryResources(ctx, dictID)
	if err != nil {
		t.Fatalf("capture inline dictionary: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 1 {
		t.Fatalf("inline closure len=%d want index only", got)
	}
	tokens := resources.Tokens()
	if len(tokens) != 1 || tokens[0].DiagnosticPath() != "index.db" {
		t.Fatalf("inline closure tokens=%v want index.db", tokens)
	}
}
