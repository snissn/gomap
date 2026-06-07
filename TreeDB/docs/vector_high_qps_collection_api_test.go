package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocs_VectorHighQPSCollectionAPIGuide2406(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	guidePath := filepath.Join(treeRoot, "docs", "guides", "vector-search-high-qps-collection-api.md")
	data, err := os.ReadFile(guidePath)
	if err != nil {
		t.Fatalf("read high-qps vector guide: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"## Choosing a vector search API",
		"`Collection.SearchVectorIndexWithBuffer`",
		"caller-owned `VectorIndexSearchBuffer`",
		"steady state targets `0 B/op`, `0 allocs/op`",
		"`Collection.SearchVectorIndex` with `IncludeDocuments=false`",
		"response-owned results/IDs",
		"`Collection.SearchVectorIndex` with `IncludeDocuments=true`",
		"explicit materialization path",
		"`OpenVectorIndexSearcher` + `SearchWithBuffer`",
		"Open/warm one searcher and one buffer per worker",
		"## Production high-QPS serving recipe",
		"Warm the collection-owned prepared search state outside the timed loop",
		"response.Results aliases buffer",
		"## Runnable exact buffered demo",
		"go run ./cmd/treedb_vector_highqps_demo",
		"reuses a caller-owned buffer",
		"The demo is exact-only",
		"not a benchmark replacement",
		"## Do not overclaim",
		"Apple M3",
		"`darwin/arm64`",
		"`32e143240dbffb24172e0ec91c5565ea7c84328a`",
		"With-document search is a different materialization path",
		"Filters, projections, debug-only stats, and quantized modes are outside",
		"Collection-level buffered quantized serving for supported score planes",
		"Quantized collection buffered search is supported as a separate route state",
		"vector-search-closeout-2483.md",
		"#2494 crossover synthesis",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("%s missing #2406 high-QPS API guidance %q", guidePath, want)
		}
	}

	for _, forbidden := range []string{
		"TreeDB beats USearch",
		"TreeDB is faster than USearch",
		"Collection.SearchVectorIndex` is the zero-allocation target",
		"Collection-level buffered quantized search support is unavailable/planned",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("%s contains overclaim %q", guidePath, forbidden)
		}
	}
}

func TestDocs_GuidesReadmeLinksHighQPSCollectionAPI2406(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	readmePath := filepath.Join(treeRoot, "docs", "guides", "README.md")
	data, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("read guides README: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"High-QPS collection vector-search guide",
		"vector-search-high-qps-collection-api.md",
		"buffered no-document serving",
		"explicit materialization",
		"reusable searchers",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("%s missing high-QPS guide link wording %q", readmePath, want)
		}
	}
}
