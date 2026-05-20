package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type demoOutput struct {
	Dir     string                       `json:"dir"`
	Status  demoStatus                   `json:"status"`
	Trace   collections.VectorIndexTrace `json:"trace"`
	Results []demoResult                 `json:"results"`
}

type demoStatus struct {
	RootName                      string `json:"root_name"`
	RootID                        uint64 `json:"root_id"`
	Strategy                      string `json:"strategy"`
	ColumnGraphLoaded             bool   `json:"column_graph_loaded"`
	PhysicalColumnAssetsSupported bool   `json:"physical_column_assets_supported"`
	RebuildNeeded                 bool   `json:"rebuild_needed"`
	ExactFallbackReason           string `json:"exact_fallback_reason,omitempty"`
	ColumnGraphUnavailableReason  string `json:"column_graph_unavailable_reason,omitempty"`
}

type demoResult struct {
	DocumentID string          `json:"document_id"`
	Distance   float32         `json:"distance"`
	Document   json.RawMessage `json:"document"`
}

func main() {
	dir := flag.String("dir", "", "TreeDB directory to create; defaults to a temporary directory")
	keepDir := flag.Bool("keep-dir", false, "keep the temporary directory after the demo")
	jsonOut := flag.Bool("json", false, "emit JSON instead of text")
	flag.Parse()

	dbDir := *dir
	tempDir := false
	if dbDir == "" {
		var err error
		dbDir, err = os.MkdirTemp("", "treedb-column-graph-demo-*")
		if err != nil {
			log.Fatalf("create temp dir: %v", err)
		}
		tempDir = true
	}
	if tempDir && !*keepDir {
		defer func() { _ = os.RemoveAll(dbDir) }()
	}
	if err := runDemo(dbDir, *jsonOut); err != nil {
		log.Fatal(err)
	}
}

func runDemo(dir string, jsonOut bool) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create db dir: %w", err)
	}
	if entries, err := os.ReadDir(dir); err != nil {
		return fmt.Errorf("read db dir: %w", err)
	} else if len(entries) != 0 {
		return fmt.Errorf("demo dir must be empty: %s", dir)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{
		RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1},
	}); err != nil {
		return fmt.Errorf("enable command WAL format: %w", err)
	}

	db, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true})
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "docs",
		Options: collections.CollectionOptions{
			ColumnStore: columnGraphDemoColumnStore(),
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     collections.VectorMetricCosine,
			Dimensions: 3,
			M:          2,
			EfSearch:   16,
			Strategy:   collections.VectorIndexStrategyColumnGraph,
		}},
	}); err != nil {
		_ = db.Close()
		return fmt.Errorf("create collection: %w", err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("open collection: %w", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("doc-a"), []byte("doc-b"), []byte("doc-c")},
		[][]byte{
			[]byte(`{"title":"north","embedding":[1,0,0],"embedding_inv_norm":1,"embedding_neighbors":[1,2]}`),
			[]byte(`{"title":"east","embedding":[0,1,0],"embedding_inv_norm":1,"embedding_neighbors":[0,2]}`),
			[]byte(`{"title":"up","embedding":[0,0,1],"embedding_inv_norm":1,"embedding_neighbors":[0,1]}`),
		},
	); err != nil {
		_ = db.Close()
		return fmt.Errorf("insert docs: %w", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		return fmt.Errorf("checkpoint: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close seed db: %w", err)
	}

	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true})
	if err != nil {
		return fmt.Errorf("reopen db: %w", err)
	}
	defer func() { _ = reopen.Close() }()
	reopened, err := collections.NewCollectionManager(reopen).OpenCollection("docs")
	if err != nil {
		return fmt.Errorf("open reopened collection: %w", err)
	}
	status, err := reopened.VectorIndexStatus("embedding")
	if err != nil {
		return fmt.Errorf("vector status: %w", err)
	}
	results, trace, err := reopened.SearchVectorIndex("embedding", []float32{1, 0, 0}, collections.VectorIndexSearchOptions{
		TopK:                 2,
		EfSearch:             16,
		DisableExactFallback: true,
	})
	if err != nil {
		return fmt.Errorf("column graph search: %w", err)
	}

	out := demoOutput{Dir: filepath.Clean(dir), Status: demoStatusFromVectorStatus(status), Trace: trace, Results: demoResults(results)}
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	fmt.Printf("TreeDB column_graph demo dir: %s\n", out.Dir)
	fmt.Printf("status: loaded=%t physical_assets=%t rebuild_needed=%t fallback=%q root=%d\n",
		out.Status.ColumnGraphLoaded,
		out.Status.PhysicalColumnAssetsSupported,
		out.Status.RebuildNeeded,
		out.Status.ExactFallbackReason,
		out.Status.RootID,
	)
	fmt.Printf("trace: strategy=%s candidates=%d returned=%d fallback=%q\n",
		trace.Strategy,
		trace.CandidatesExamined,
		trace.ReturnedCount,
		trace.ExactFallbackReason,
	)
	for i, result := range results {
		fmt.Printf("%d. id=%s distance=%.6f doc=%s\n", i+1, result.DocumentID, result.Distance, result.Document)
	}
	return nil
}

func demoStatusFromVectorStatus(status collections.VectorIndexStatus) demoStatus {
	return demoStatus{
		RootName:                      status.RootName,
		RootID:                        status.RootID,
		Strategy:                      status.Strategy.String(),
		ColumnGraphLoaded:             status.ColumnGraphLoaded,
		PhysicalColumnAssetsSupported: status.PhysicalColumnAssetsSupported,
		RebuildNeeded:                 status.RebuildNeeded,
		ExactFallbackReason:           status.ExactFallbackReason,
		ColumnGraphUnavailableReason:  status.ColumnGraphUnavailableReason,
	}
}

func demoResults(results []collections.VectorSearchResult) []demoResult {
	out := make([]demoResult, len(results))
	for i, result := range results {
		out[i] = demoResult{
			DocumentID: string(result.DocumentID),
			Distance:   result.Distance,
			Document:   append(json.RawMessage(nil), result.Document...),
		}
	}
	return out
}

func columnGraphDemoColumnStore() *collections.ColumnStoreConfig {
	return &collections.ColumnStoreConfig{
		Enabled: true,
		Columns: []collections.ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 3},
			{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: collections.ColumnStoreValueFloat32},
			{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: collections.ColumnStoreValueAdjacencyList},
		},
		RetainedPayload: collections.ColumnRetainedPayloadFull,
		AssetManager: &collections.ColumnAssetManagerConfig{
			Kind:              collections.ColumnAssetManagerValueLogShaped,
			IsolatedNamespace: true,
			Namespace:         "column_graph_demo",
		},
	}
}
