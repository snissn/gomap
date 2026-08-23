package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections/embedding"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func openEmbeddingTestCollection(t *testing.T, dims int) (*Collection, *backenddb.DB) {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: dims,
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col, d
}

func documentCount(t *testing.T, col *Collection) int {
	t.Helper()
	n := 0
	if _, err := col.ScanDocumentIDsFunc(chunkingScanMaxDocuments, func([]byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatalf("ScanDocumentIDsFunc: %v", err)
	}
	return n
}

// TestEmbedForIngestUnknownProviderFailsClosed proves the ingest seam rejects
// an unregistered provider with the typed error and lands no write.
func TestEmbedForIngestUnknownProviderFailsClosed(t *testing.T) {
	col, _ := openEmbeddingTestCollection(t, 16)
	before := documentCount(t, col)
	_, err := col.EmbedForIngest(context.Background(), "embedding",
		[][]byte{[]byte("hello world")}, embedding.Config{Provider: "no-such-provider", Dimensions: 16})
	if !errors.Is(err, embedding.ErrUnknownProvider) {
		t.Fatalf("EmbedForIngest=%v want ErrUnknownProvider", err)
	}
	if after := documentCount(t, col); after != before {
		t.Fatalf("document count %d -> %d on failed embed: write landed", before, after)
	}
}

// TestEmbedForIngestDimensionMismatchFailsClosed proves the dimension check
// against the declared vector index definition fails closed before any write
// and before any embedding work.
func TestEmbedForIngestDimensionMismatchFailsClosed(t *testing.T) {
	col, _ := openEmbeddingTestCollection(t, 8)
	before := documentCount(t, col)
	_, err := col.EmbedForIngest(context.Background(), "embedding",
		[][]byte{[]byte("hello world")}, embedding.Config{Provider: embedding.ProviderHashing, Dimensions: 4})
	if !errors.Is(err, embedding.ErrDimensionMismatch) {
		t.Fatalf("EmbedForIngest=%v want ErrDimensionMismatch", err)
	}
	if !strings.Contains(err.Error(), "8") || !strings.Contains(err.Error(), "4") {
		t.Fatalf("err=%v want both declared and embedder dimensions in message", err)
	}
	if after := documentCount(t, col); after != before {
		t.Fatalf("document count %d -> %d on failed embed: write landed", before, after)
	}
}

// TestEmbedForIngestUnknownVectorIndexFailsClosed proves a bad index name is
// rejected with a typed collections error.
func TestEmbedForIngestUnknownVectorIndexFailsClosed(t *testing.T) {
	col, _ := openEmbeddingTestCollection(t, 16)
	_, err := col.EmbedForIngest(context.Background(), "not-an-index",
		[][]byte{[]byte("hello world")}, embedding.Config{Provider: embedding.ProviderHashing, Dimensions: 16})
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("EmbedForIngest=%v want ErrIndexNotFound", err)
	}
}

// TestEmbedForIngestHappyPath proves the seam returns index-aligned vectors
// matching the reference embedder's direct output.
func TestEmbedForIngestHappyPath(t *testing.T) {
	col, _ := openEmbeddingTestCollection(t, 16)
	texts := [][]byte{[]byte("alpha beta"), []byte("gamma delta epsilon")}
	got, err := col.EmbedForIngest(context.Background(), "embedding", texts,
		embedding.Config{Provider: embedding.ProviderHashing, Dimensions: 16})
	if err != nil {
		t.Fatalf("EmbedForIngest: %v", err)
	}
	emb, err := embedding.DefaultRegistry().Create(embedding.Config{Provider: embedding.ProviderHashing, Dimensions: 16})
	if err != nil {
		t.Fatalf("Create reference embedder: %v", err)
	}
	want, err := emb.EmbedBatch(context.Background(), texts)
	if err != nil {
		t.Fatalf("EmbedBatch: %v", err)
	}
	if len(got) != len(texts) {
		t.Fatalf("got %d vectors want %d", len(got), len(texts))
	}
	for i := range got {
		if len(got[i]) != 16 {
			t.Fatalf("vector %d width %d want 16", i, len(got[i]))
		}
		for j := range got[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("vector %d elem %d: %v want %v", i, j, got[i][j], want[i][j])
			}
		}
	}
}

func TestEmbedForIngestRejectsMalformedProviderOutput(t *testing.T) {
	const dims = 16
	texts := [][]byte{[]byte("alpha"), []byte("beta")}
	valid := func(n, width int) [][]float32 {
		out := make([][]float32, n)
		for i := range out {
			out[i] = make([]float32, width)
			if width > 0 {
				out[i][0] = 1
			}
		}
		return out
	}
	tests := []struct {
		name             string
		vectors          [][]float32
		wantDimensionErr bool
	}{
		{name: "wrong count", vectors: valid(1, dims)},
		{name: "wrong width", vectors: valid(len(texts), dims-1), wantDimensionErr: true},
		{name: "non-finite", vectors: func() [][]float32 {
			out := valid(len(texts), dims)
			out[1][3] = float32(math.Inf(1))
			return out
		}()},
		{name: "zero cosine vector", vectors: make([][]float32, len(texts))},
	}
	for i := range tests {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			for j := range tc.vectors {
				if tc.vectors[j] == nil {
					tc.vectors[j] = make([]float32, dims)
				}
			}
			provider := fmt.Sprintf("embed-output-test-%d", i)
			if err := embedding.DefaultRegistry().Register(provider, func(embedding.Config) (embedding.Embedder, error) {
				return staticOutputEmbedder{dims: dims, vectors: tc.vectors}, nil
			}); err != nil {
				t.Fatalf("Register: %v", err)
			}
			col, _ := openEmbeddingTestCollection(t, dims)
			before := documentCount(t, col)
			vectors, err := col.EmbedForIngest(context.Background(), "embedding", texts,
				embedding.Config{Provider: provider, Dimensions: dims})
			if !errors.Is(err, embedding.ErrInvalidOutput) {
				t.Fatalf("EmbedForIngest=(%v, %v) want ErrInvalidOutput", vectors, err)
			}
			if tc.wantDimensionErr && !errors.Is(err, embedding.ErrDimensionMismatch) {
				t.Fatalf("EmbedForIngest error %v want ErrDimensionMismatch", err)
			}
			if vectors != nil {
				t.Fatalf("EmbedForIngest returned malformed vectors: %v", vectors)
			}
			if after := documentCount(t, col); after != before {
				t.Fatalf("document count %d -> %d on malformed output", before, after)
			}
		})
	}
}

// TestValidateEmbedderForVectorIndex proves the standalone dimension gate.
func TestValidateEmbedderForVectorIndex(t *testing.T) {
	col, _ := openEmbeddingTestCollection(t, 16)
	if err := col.ValidateEmbedderForVectorIndex("embedding", stubDimsEmbedder{dims: 16}); err != nil {
		t.Fatalf("ValidateEmbedderForVectorIndex(match): %v", err)
	}
	err := col.ValidateEmbedderForVectorIndex("embedding", stubDimsEmbedder{dims: 32})
	if !errors.Is(err, embedding.ErrDimensionMismatch) {
		t.Fatalf("ValidateEmbedderForVectorIndex(mismatch)=%v want ErrDimensionMismatch", err)
	}
	if err := col.ValidateEmbedderForVectorIndex("missing", stubDimsEmbedder{dims: 16}); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("ValidateEmbedderForVectorIndex(missing index)=%v want ErrIndexNotFound", err)
	}
	if err := col.ValidateEmbedderForVectorIndex("embedding", nil); err == nil {
		t.Fatal("nil embedder accepted")
	}
}

type stubDimsEmbedder struct{ dims int }

func (s stubDimsEmbedder) Dimensions() int { return s.dims }
func (s stubDimsEmbedder) EmbedBatch(ctx context.Context, texts [][]byte) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = make([]float32, s.dims)
	}
	return out, nil
}

type staticOutputEmbedder struct {
	dims    int
	vectors [][]float32
}

func (s staticOutputEmbedder) Dimensions() int { return s.dims }
func (s staticOutputEmbedder) EmbedBatch(context.Context, [][]byte) ([][]float32, error) {
	return s.vectors, nil
}
