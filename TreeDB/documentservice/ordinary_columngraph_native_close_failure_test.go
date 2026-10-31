package documentservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// A successful ordinary graph lifecycle cannot conceal an error from the
// original owner's final command-WAL checkpoint. This process control injects
// after the actual META write, where publication is recovery-required, and
// preserves the first Close result before ending the failed owner process.
func TestOrdinaryColumnGraphNativeCloseRetainsCheckpointFailure(t *testing.T) {
	const helper = "GOMAP_NATIVE_ORDINARY_CLOSE_CHECKPOINT_FAILURE_DIR"
	const marker = "ORDINARY CLOSE ORIGINAL CHECKPOINT FAILURE RETAINED"
	const name = "close_docs"
	ctx := context.Background()
	documents := make([]Document, 8)
	for i := range documents {
		documents[i] = Document{ID: fmt.Sprintf("d%03d", i), Content: fmt.Sprintf("content %d", i), Embedding: []float32{1, float32(i) / 8}, Meta: map[string]any{"tenant": fmt.Sprintf("t%d", i%2)}}
	}
	options := func(dir string) treedb.Options {
		opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
		opts.MemtableMode = "cow_btree"
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1
		return opts
	}
	checkContents := func(svc *Service) {
		t.Helper()
		out, err := svc.FilterDocuments(ctx, name, FilterDocumentsRequest{ReturnEmbedding: true})
		if err != nil || out.Truncated || out.MatchedCount != len(documents) || !reflect.DeepEqual(out.Documents, documents) {
			t.Fatalf("ordinary full contents: got=%+v err=%v want=%+v", out, err, documents)
		}
	}
	rawKey, rawValue := []byte("final-checkpoint-marker"), []byte("acknowledged before original Close")
	if dir := os.Getenv(helper); dir != "" {
		injected := errors.New("ordinary final checkpoint after META write")
		var notified atomic.Bool
		opts := options(dir)
		opts.NotifyError = func(err error) {
			if errors.Is(err, injected) {
				notified.Store(true)
			}
		}
		owner, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager := collections.NewCollectionManager(backend)
		svc := New(manager)
		info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
		if err != nil || info.TypedInput || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
			t.Fatalf("ordinary declaration=%+v err=%v", info, err)
		}
		if out, err := svc.UpsertDocuments(ctx, name, UpsertDocumentsRequest{Documents: documents, DeferVectorIndexRebuild: true}); err != nil || out.Upserted != len(documents) {
			t.Fatalf("ordinary upsert=%+v err=%v", out, err)
		}
		if out, err := svc.OptimizeIndex(ctx, name, OptimizeIndexRequest{}); err != nil || !out.Status.Loaded || out.Status.RebuildNeeded {
			t.Fatalf("ordinary Optimize=%+v err=%v", out, err)
		}
		checkContents(svc)
		if err := svc.Close(); err != nil {
			t.Fatalf("original service Close: %v", err)
		}
		if err := manager.Close(); err != nil {
			t.Fatalf("original manager Close: %v", err)
		}
		// Manager Close may already drain its collection prefix. A final raw
		// command on the same public owner makes its remaining checkpoint real.
		if err := owner.Set(rawKey, rawValue); err != nil {
			t.Fatalf("acknowledged final marker: %v", err)
		}
		root := filepath.Join(dir, "maindb")
		path := filepath.Join(root, "index.db")
		var writes atomic.Uint32
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Point == durabilitycut.AfterMetaWrite && event.Resource == durabilitycut.ResourceMeta && event.Root == root && event.Path == path {
				writes.Add(1)
				return injected
			}
			return nil
		})
		firstClose := owner.Close()
		restore()
		if writes.Load() != 1 || !errors.Is(firstClose, injected) || !errors.Is(firstClose, backenddb.ErrRecoveryRequired) || !strings.Contains(firstClose.Error(), "final command WAL checkpoint during close") {
			t.Fatalf("original owner Close: writes=%d err=%v", writes.Load(), firstClose)
		}
		if !notified.Load() || !errors.Is(backend.CheckCommandWALPublishReady(), backenddb.ErrRecoveryRequired) {
			t.Fatal("failed original Close lost notification or recovery-required state")
		}
		fmt.Println(marker)
		os.Exit(0) // No retry, second Close, or later flush replaces firstClose.
	}

	dir := t.TempDir()
	childCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestOrdinaryColumnGraphNativeCloseRetainsCheckpointFailure$")
	cmd.Env = append(os.Environ(), helper+"="+dir)
	output, err := cmd.CombinedOutput()
	if err != nil || strings.Count(string(output), marker) != 1 || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("original Close failure child: %v\n%s", err, output)
	}
	owner, err := treedb.Open(options(dir))
	if err != nil {
		t.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		if closeErr := owner.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	svc := New(manager)
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Error(err)
		}
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	checkContents(svc)
	if got, err := owner.Get(rawKey); err != nil || !bytes.Equal(got, rawValue) {
		t.Fatalf("recovered acknowledged marker=%q err=%v", got, err)
	}
	if err := backend.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("fresh recovery owner remains fenced: %v", err)
	}
}
