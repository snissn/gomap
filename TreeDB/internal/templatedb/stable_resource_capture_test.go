//go:build !windows

package templatedb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/template"
)

func TestCaptureTemplateResourcesInlineDefinitionNeedsExactPinnedIndex(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	store := New(stableTestKV{db: backend}, Config{})
	definition := []byte("inline-template-definition")
	templateID, err := store.PutTemplateDef(context.Background(), definition, nil)
	if err != nil {
		t.Fatalf("put template: %v", err)
	}

	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture template: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 1 {
		t.Fatalf("closure len=%d want index only", got)
	}
	tokens := resources.Tokens()
	if len(tokens) != 1 || tokens[0].DiagnosticPath() != "index.db" || tokens[0].Namespace() == nil {
		t.Fatalf("inline closure tokens=%v want pinned index namespace", tokens)
	}
	assertTemplateObligation(t, tokens[0], templateID, definition)
}

func TestCaptureTemplateResourcesPointerDefinitionReturnsExactTransitiveClosure(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{
		Dir:       t.TempDir(),
		ChunkSize: 64 * 1024,
		ValueLog:  backenddb.ValueLogOptions{ForcePointers: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	kv := stableTestKV{db: backend}
	store := New(kv, Config{})
	definition := bytes.Repeat([]byte("pointer-template-definition|"), 64)
	templateID := template.TemplateID(definition, 0)
	if err := seedPointerTemplate(backend, templateID, definition); err != nil {
		t.Fatalf("seed pointer template: %v", err)
	}
	snapshot := backend.AcquireSnapshot()
	entry, err := snapshot.GetEntryExact(templateKey(templateID))
	if err != nil {
		t.Fatalf("get pointer entry: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	wantFrontier := entry.ValuePtr.Offset + uint64(page.ValuePtrRecordLength(entry.ValuePtr))

	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture template: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 2 {
		t.Fatalf("closure len=%d want index+value-log", got)
	}
	if got := backend.StableResourceIdentityPinRegistry().ActivePins(); got != 1 {
		t.Fatalf("value-log identity pins=%d want 1", got)
	}
	paths := make(map[string]bool, 2)
	for _, token := range resources.Tokens() {
		paths[token.DiagnosticPath()] = true
		assertTemplateObligation(t, token, templateID, definition)
		if token.DiagnosticPath() != "index.db" && token.Frontier().Bytes != wantFrontier {
			t.Fatalf("value-log frontier=%d want exact record end %d", token.Frontier().Bytes, wantFrontier)
		}
	}
	if !paths["index.db"] || !paths["value_vlog/value-l0-000001.log"] {
		t.Fatalf("closure paths=%v want exact index and value-log segment", paths)
	}
	resources.Release()
	if got := backend.StableResourceIdentityPinRegistry().ActivePins(); got != 0 {
		t.Fatalf("value-log identity pins after release=%d want 0", got)
	}
}

func seedPointerTemplate(backend *backenddb.DB, templateID uint64, definition []byte) error {
	valueLogDir := backenddb.ValueLogDirPath(backend.Dir())
	if err := os.MkdirAll(valueLogDir, 0700); err != nil {
		return err
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		return err
	}
	path := filepath.Join(valueLogDir, fmt.Sprintf("value-l0-%06d.log", 1))
	writer, err := valuelog.NewWriterWithStableResourcePinRegistry(path, fileID, backend.StableResourceIdentityPinRegistry())
	if err != nil {
		return err
	}
	defer writer.Close()
	pointer, err := writer.Append(0, nil, templateID, definition)
	if err != nil {
		return err
	}
	if err := writer.Sync(); err != nil {
		return err
	}
	batch := backend.NewBatch().(*backenddb.Batch)
	defer batch.Close()
	if err := batch.SetPointer(templateKey(templateID), pointer); err != nil {
		return err
	}
	if err := batch.WriteSync(); err != nil {
		return err
	}
	return backend.RefreshValueLogSet()
}

func TestCaptureTemplateResourcesAcceptsConfiguredSaltedID(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	kv := stableTestKV{db: backend}
	definition := []byte("salted-template-definition")
	templateID := template.TemplateID(definition, 1)
	if err := kv.SetSync(templateKey(templateID), definition); err != nil {
		t.Fatalf("seed salted template: %v", err)
	}
	store := New(kv, Config{MaxIDAttempts: 2})
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture salted template: %v", err)
	}
	resources.Release()
}

func TestCaptureTemplateResourcesRejectsMissingAndMismatchedDefinitionsWithoutPins(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	kv := stableTestKV{db: backend}
	store := New(kv, Config{MaxIDAttempts: 2})
	if resources, captureErr := store.CaptureTemplateResources(context.Background(), 42); captureErr == nil || resources != nil {
		if resources != nil {
			resources.Release()
		}
		t.Fatalf("missing capture resources=%v err=%v", resources, captureErr)
	}
	wrongID := template.TemplateID([]byte("expected"), 0)
	if err := kv.SetSync(templateKey(wrongID), []byte("different")); err != nil {
		t.Fatalf("seed mismatch: %v", err)
	}
	if resources, captureErr := store.CaptureTemplateResources(context.Background(), wrongID); !errors.Is(captureErr, rootpublication.ErrResourceConflict) || resources != nil {
		if resources != nil {
			resources.Release()
		}
		t.Fatalf("mismatch capture resources=%v err=%v want resource conflict", resources, captureErr)
	}
	if got := backend.StableResourceIdentityPinRegistry().ActivePins(); got != 0 {
		t.Fatalf("pins after failed captures=%d want 0", got)
	}
}

func TestCaptureTemplateResourcesBlocksVacuumUntilRelease(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	store := New(stableTestKV{db: backend}, Config{})
	templateID, err := store.PutTemplateDef(context.Background(), []byte("vacuum-fenced-template"), nil)
	if err != nil {
		t.Fatalf("put template: %v", err)
	}
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture template: %v", err)
	}
	if err := backend.VacuumIndexOnline(context.Background()); !errors.Is(err, rootpublication.ErrResourcePinned) {
		resources.Release()
		t.Fatalf("vacuum while captured error=%v want ErrResourcePinned", err)
	}
	resources.Release()
	if err := backend.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("vacuum after release: %v", err)
	}
}

func TestCaptureTemplateResourcesFailsBeforeStoreMutationWithoutPhysicalProvider(t *testing.T) {
	kv := newMemKV()
	store := New(kv, Config{})
	definition := []byte("no-physical-provider")
	templateID, err := store.PutTemplateDef(context.Background(), definition, nil)
	if err != nil {
		t.Fatalf("put template: %v", err)
	}
	before := len(kv.data)
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if resources != nil {
		resources.Release()
		t.Fatal("capture without physical provider returned resources")
	}
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("capture error=%v want unresolved resource", err)
	}
	if got := len(kv.data); got != before {
		t.Fatalf("capture mutated store entries=%d want %d", got, before)
	}
}

func assertTemplateObligation(t *testing.T, token *rootpublication.StableResourceToken, templateID uint64, definition []byte) {
	t.Helper()
	if token.Kind() != rootpublication.ResourceTemplate || token.Reachability() != rootpublication.ReachabilityTemplateGeneration {
		t.Fatalf("token kind=%q reachability=%q want template generation", token.Kind(), token.Reachability())
	}
	obligations := token.LogicalObligations()
	if len(obligations) != 1 {
		t.Fatalf("token %q obligations=%d want 1", token.DiagnosticPath(), len(obligations))
	}
	wantDigest := sha256.Sum256(definition)
	obligation := obligations[0]
	if obligation.Class != "template-generation" || obligation.Kind != "template" || obligation.Namespace != "templatedb" ||
		obligation.Generation != templateID || obligation.FileID != templateID || obligation.Offset != 0 ||
		obligation.Length != int64(len(definition)) || obligation.Digest != wantDigest ||
		obligation.Reachability != rootpublication.ReachabilityTemplateGeneration {
		t.Fatalf("token %q obligation=%+v", token.DiagnosticPath(), obligation)
	}
}
