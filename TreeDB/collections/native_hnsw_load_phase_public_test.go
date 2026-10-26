package collections_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Unlike the older 2D persistence lifecycle, this is the actual command
// discriminator's 4096/128/270B/8D/16-tenant corpus. A new process issues mixed
// Hybrid FIRST: Keyword/Dense must not warm the registered runtime beforehand.
// The unchanged command smoke remains the HTTP/listener ownership proof.
func TestNativeHNSWColdFirstMixedHybrid4096(t *testing.T) {
	const childDirectory = "GOMAP_HNSW_COLD_FIRST_HYBRID_DIRECTORY"
	directory := os.Getenv(childDirectory)
	serving := directory != ""
	if !serving {
		directory = t.TempDir()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, directory)
	opts.MemtableMode = "cow_btree"
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := db.NativeSemanticBackend()
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	svc := documentservice.NewWithDeferredVectorBuildMaintenance(manager, db.DeferredVectorBuildMaintenance())
	var budget interface{ Stats() memtable.COWStats }
	closed := false
	closeOwners := func() {
		if closed {
			return
		}
		closed = true
		if err := svc.Close(); err != nil {
			t.Error(err)
		}
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		if budget != nil {
			if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
				t.Errorf("full Close: %+v", s)
			}
		}
	}
	t.Cleanup(closeOwners)
	// Observe the original budget without pinning an extra cut across the load.
	cut, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	budget = cut.NativeAllocator().(interface{ Stats() memtable.COWStats })
	if err = cut.Close(); err != nil {
		t.Fatal(err)
	}
	if serving {
		// No Create, Optimize, Get, Keyword or Dense in this process before the
		// mixed request: scalar reconstruction must share the original capture.
		response, err := svc.SearchHybrid(ctx, "docs", documentservice.HybridSearchRequest{
			Query: "policy", QueryEmbedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 10,
			TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 64,
			Filter: &documentservice.Filter{Field: "meta.tenant", Operator: "==", Value: "t00"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.TextIndex != "content" || response.VectorIndex != "embedding" || response.Stats.TextCandidatesReturned == 0 || response.Stats.VectorCandidatesReturned == 0 || response.Stats.FullDocumentScanFallbacks != 0 || response.Stats.FailClosed != 0 {
			t.Fatalf("mixed source/route: text=%q vector=%q stats=%+v", response.TextIndex, response.VectorIndex, response.Stats)
		}
		if len(response.Documents) != 10 {
			t.Fatalf("documents=%d", len(response.Documents))
		}
		seen := make(map[string]bool)
		for _, doc := range response.Documents {
			var ordinal int
			_, parseErr := fmt.Sscanf(doc.ID, "d%06d", &ordinal)
			if parseErr != nil || ordinal < 0 || ordinal >= 4096 || doc.ID != fmt.Sprintf("d%06d", ordinal) || ordinal%16 != 0 || seen[doc.ID] || doc.Meta["tenant"] != "t00" || len(doc.Embedding) != 0 || !strings.Contains(doc.Content, "policy") {
				t.Fatalf("invalid filtered/projected result: %+v", doc)
			}
			seen[doc.ID] = true
		}
		closeOwners()
		return
	}
	info, err := svc.CreateIndex(ctx, documentservice.CreateIndexRequest{
		Name: "docs", Dimension: 8,
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime, M: 16, EfConstruction: 128, EfSearch: 64},
		ScalarFields:       []documentservice.ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: documentservice.ScalarFieldString}, {Field: "meta.rank", ValueType: documentservice.ScalarFieldInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.VectorStrategy != collections.VectorIndexStrategyNativeRuntime {
		t.Fatalf("strategy=%s", info.VectorStrategy)
	}
	for first := 0; first < 4096; first += 128 {
		docs := make([]documentservice.Document, 128)
		for j := range docs {
			i := first + j
			vector := make([]float32, 8)
			vector[0] = 1
			for d := 1; d < 8; d++ {
				vector[d] = float32((i+d)%17) / 17
			}
			docs[j] = documentservice.Document{ID: fmt.Sprintf("d%06d", i), Content: "refund policy " + strings.Repeat("payload ", 32), Embedding: vector, Meta: map[string]any{"tenant": fmt.Sprintf("t%02d", i%16), "rank": i}}
		}
		response, err := svc.UpsertDocuments(ctx, "docs", documentservice.UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true})
		if err != nil {
			t.Fatalf("batch[%d:%d): %v", first, first+128, err)
		}
		if response.Upserted != 128 {
			t.Fatalf("prefix=%d upserted=%d", first, response.Upserted)
		}
	}
	if _, err = svc.OptimizeIndex(ctx, "docs", documentservice.OptimizeIndexRequest{}); err != nil {
		t.Fatal(err)
	}
	closeOwners()
	if t.Failed() {
		return
	}
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeHNSWColdFirstMixedHybrid4096$", "-test.count=1", "-test.v")
	child.Env = append(os.Environ(), childDirectory+"="+directory)
	output, err := child.CombinedOutput()
	t.Logf("fresh-process first-Hybrid:\n%s", output)
	if err != nil {
		t.Fatalf("fresh-process first-Hybrid: %v", err)
	}
}
