package colgranule

import (
	"container/heap"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestColumnVectorGraphDeep1BBuildableGranuleScout(t *testing.T) {
	if os.Getenv("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_SCOUT") != "1" {
		t.Skip("set COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_SCOUT=1 to run the opt-in Deep1B buildable-granule scout")
	}
	queryIndexes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_QUERIES", []int{0})
	baseRows := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_BASE_ROWS", 100_000)
	granuleRows := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_ROWS", 8192)
	builder := columnVectorGraphDeep1BEnvString("COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER", "row_id_contiguous")
	selection := columnVectorGraphDeep1BEnvString("COLUMN_VECTOR_DEEP1B_BUILDABLE_SELECTION", "centroid_blocks")
	kmeansIters := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS", 8)
	graphDegree := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_GRAPH_DEGREE", 16)
	graphEntryClusters := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_GRAPH_ENTRY_CLUSTERS", 4)
	topGranulesList := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_TOP_GRANULES", []int{1, 4})
	ranks := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_PCA_RANKS", []int{32, 48, 64, 80, columnVectorGraphDeep1BDims})
	pqBytes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_BYTES", nil)
	opqBytes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_OPQ_BYTES", nil)
	residualPQBytes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_RESIDUAL_PQ_BYTES", nil)
	localResidualPQBytes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_LOCAL_RESIDUAL_PQ_BYTES", nil)
	localOPQBytes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_LOCAL_OPQ_BYTES", nil)
	pqTrainRows := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_TRAIN_ROWS", 8192)
	pqTrainIters := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_ITERS", 4)
	opqIters := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_OPQ_ITERS", 3)
	scanIters := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_SCAN_ITERS", 8)
	if baseRows <= 0 {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_BASE_ROWS=%d must be positive", baseRows)
	}
	if granuleRows <= 0 {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_ROWS=%d must be positive", granuleRows)
	}
	if graphDegree <= 0 {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRAPH_DEGREE=%d must be positive", graphDegree)
	}
	if graphEntryClusters <= 0 {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRAPH_ENTRY_CLUSTERS=%d must be positive", graphEntryClusters)
	}
	if selection != "centroid_blocks" && selection != "graph_visited_blocks" && selection != "graph_visited_rows" {
		t.Fatalf("unknown COLUMN_VECTOR_DEEP1B_BUILDABLE_SELECTION=%q; supported: centroid_blocks, graph_visited_blocks, graph_visited_rows", selection)
	}
	if columnVectorGraphDeep1BIsGraphVisitedSelection(selection) && !columnVectorGraphDeep1BSupportsGraphVisitedSelection(builder) {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_SELECTION=%s currently requires COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=ivf_graph_sorted_blocks, treedb_graph_sorted_part_granules, or ivf_exact_graph_sorted_blocks", selection)
	}
	if selection == "graph_visited_rows" && (len(localResidualPQBytes) > 0 || len(localOPQBytes) > 0) {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_SELECTION=graph_visited_rows does not support local residual-PQ/local OPQ budgets; those codebooks are valid only for sealed storage granules")
	}
	codebookEnabled := len(pqBytes) > 0 || len(opqBytes) > 0 || len(residualPQBytes) > 0
	if codebookEnabled {
		if pqTrainRows < 256 {
			t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_TRAIN_ROWS=%d must be at least 256 for 8-bit PQ codebooks", pqTrainRows)
		}
		if pqTrainIters <= 0 {
			t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_ITERS=%d must be positive", pqTrainIters)
		}
		if len(opqBytes) > 0 && opqIters <= 0 {
			t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_OPQ_ITERS=%d must be positive", opqIters)
		}
	}
	if len(localResidualPQBytes) > 0 && pqTrainIters <= 0 {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_ITERS=%d must be positive for local residual PQ", pqTrainIters)
	}
	if len(localOPQBytes) > 0 {
		if pqTrainIters <= 0 {
			t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_ITERS=%d must be positive for local OPQ", pqTrainIters)
		}
		if opqIters <= 0 {
			t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_OPQ_ITERS=%d must be positive for local OPQ", opqIters)
		}
	}
	outDir := strings.TrimSpace(os.Getenv("COLUMN_VECTOR_DEEP1B_BUILDABLE_OUT"))
	if outDir == "" {
		outDir = filepath.Join(os.TempDir(), "gomap_deep1b_buildable_granule_scout_"+time.Now().Format("20060102_150405"))
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("create buildable granule scout output dir: %v", err)
	}

	totalRows := baseRows
	if codebookEnabled {
		totalRows += pqTrainRows
	}
	data := columnVectorGraphDeep1BEnsureData(t, totalRows)
	baseFile, err := os.Open(data.basePath)
	if err != nil {
		t.Fatalf("open Deep1B base: %v", err)
	}
	defer baseFile.Close()
	var raw []byte
	totalRows = min(totalRows, data.baseHeader.Rows)
	if codebookEnabled && totalRows <= pqTrainRows {
		t.Fatalf("Deep1B base rows=%d are insufficient for pqTrainRows=%d plus eval rows", totalRows, pqTrainRows)
	}
	_, allVectors, err := columnVectorGraphDeep1BReadFbinVectorsAt(baseFile, data.baseHeader, 0, totalRows, raw, nil)
	if err != nil {
		t.Fatalf("read Deep1B base prefix rows=%d: %v", totalRows, err)
	}
	var trainVectors []float32
	evalOffset := 0
	if codebookEnabled {
		trainVectors = allVectors[:pqTrainRows*columnVectorGraphDeep1BDims]
		evalOffset = pqTrainRows
	}
	availableEvalRows := totalRows - evalOffset
	baseRows = min(baseRows, availableEvalRows)
	vectors := allVectors[evalOffset*columnVectorGraphDeep1BDims : (evalOffset+baseRows)*columnVectorGraphDeep1BDims]
	invNorms := columnVectorGraphDeep1BInvNorms(vectors, columnVectorGraphDeep1BDims)
	granuleBuildStart := time.Now()
	granules, builderNotes := columnVectorGraphDeep1BBuildableGranules(t, builder, vectors, invNorms, baseRows, columnVectorGraphDeep1BDims, granuleRows, kmeansIters, graphDegree)
	granuleBuildNanos := time.Since(granuleBuildStart).Nanoseconds()
	if len(granules) == 0 {
		t.Fatalf("no granules for builder=%s baseRows=%d granuleRows=%d", builder, baseRows, granuleRows)
	}
	var graphRouting columnVectorGraphDeep1BGraphBlockRoutingIndex
	var routingBuildNanos int64
	if columnVectorGraphDeep1BIsGraphVisitedSelection(selection) {
		routingBuildStart := time.Now()
		graphRouting = columnVectorGraphDeep1BBuildGraphBlockRoutingIndex(t, builder, vectors, invNorms, granules, baseRows, columnVectorGraphDeep1BDims, granuleRows, kmeansIters, graphDegree)
		routingBuildNanos = time.Since(routingBuildStart).Nanoseconds()
		if selection == "graph_visited_blocks" {
			builderNotes += "; selection=graph_visited_blocks uses a query-time greedy expansion over the same query-independent row graph, then expands visited rows to their sealed graph-sorted storage blocks"
		} else {
			builderNotes += "; selection=graph_visited_rows uses a query-time greedy expansion over the same query-independent row graph and evaluates only the visited rows; local per-granule codecs are intentionally skipped because the candidate set is dynamic rather than a sealed storage block"
		}
	}
	reportGraphDegree := 0
	if columnVectorGraphDeep1BBuilderUsesGraph(builder) {
		reportGraphDegree = graphDegree
	}
	pqModels := columnVectorGraphDeep1BFitBuildablePQModels(t, trainVectors, pqBytes, pqTrainRows, baseRows, columnVectorGraphDeep1BDims, pqTrainIters)
	residualPQModels := columnVectorGraphDeep1BFitBuildableResidualPQModels(t, trainVectors, residualPQBytes, pqTrainRows, baseRows, columnVectorGraphDeep1BDims, pqTrainIters)
	opqModels := columnVectorGraphDeep1BFitBuildableOPQModels(t, trainVectors, opqBytes, pqTrainRows, baseRows, columnVectorGraphDeep1BDims, pqTrainIters, opqIters)
	localResidualPQModels := columnVectorGraphDeep1BFitBuildableLocalResidualPQModels(t, vectors, invNorms, granules, localResidualPQBytes, baseRows, columnVectorGraphDeep1BDims, pqTrainIters)
	localOPQModels := columnVectorGraphDeep1BFitBuildableLocalOPQModels(t, vectors, invNorms, granules, localOPQBytes, baseRows, columnVectorGraphDeep1BDims, pqTrainIters, opqIters)
	codebookModels := make([]columnVectorGraphDeep1BPQModel, 0, len(pqModels)+len(residualPQModels)+len(opqModels))
	codebookModels = append(codebookModels, pqModels...)
	codebookModels = append(codebookModels, residualPQModels...)
	codebookModels = append(codebookModels, opqModels...)

	report := columnVectorGraphDeep1BBuildableGranuleScoutReport{
		GeneratedAt:             time.Now().UTC().Format(time.RFC3339),
		OutputDir:               outDir,
		BasePath:                data.basePath,
		QueryPath:               data.queryPath,
		BaseRows:                baseRows,
		EvalRowOffset:           evalOffset,
		Dims:                    columnVectorGraphDeep1BDims,
		Builder:                 builder,
		Selection:               selection,
		GranuleRows:             granuleRows,
		GranuleCount:            len(granules),
		GranuleBuildNanos:       granuleBuildNanos,
		RoutingBuildNanos:       routingBuildNanos,
		KMeansIters:             kmeansIters,
		GraphDegree:             reportGraphDegree,
		GraphEntryClusters:      graphEntryClusters,
		PQBytes:                 append([]int(nil), pqBytes...),
		ResidualPQBytes:         append([]int(nil), residualPQBytes...),
		OPQBytes:                append([]int(nil), opqBytes...),
		LocalResidualPQBytes:    append([]int(nil), localResidualPQBytes...),
		LocalOPQBytes:           append([]int(nil), localOPQBytes...),
		PQTrainRows:             len(trainVectors) / columnVectorGraphDeep1BDims,
		PQTrainIters:            pqTrainIters,
		OPQIterations:           opqIters,
		PQTraining:              columnVectorGraphDeep1BPQTrainingReports(codebookModels),
		LocalResidualPQTraining: localResidualPQModels.training,
		LocalOPQTraining:        localOPQModels.training,
		RequestedQueries:        append([]int(nil), queryIndexes...),
		TopGranules:             append([]int(nil), topGranulesList...),
		Ranks:                   append([]int(nil), ranks...),
		ScanIters:               scanIters,
		Notes:                   builderNotes,
	}
	for _, queryIndex := range queryIndexes {
		if queryIndex < 0 || queryIndex >= data.queryHeader.Rows {
			t.Fatalf("query index=%d outside query rows=%d", queryIndex, data.queryHeader.Rows)
		}
		query := columnVectorGraphDeep1BReadQuery(t, data.queryPath, data.queryHeader, queryIndex)
		queryInvNorm := float32(columnVectorGraphDeep1BInvNorm(query))
		globalScores := make([]float32, baseRows)
		columnVectorGraphDeep1BScorePrefixInto(query, queryInvNorm, vectors, invNorms, columnVectorGraphDeep1BDims, columnVectorGraphDeep1BDims, globalScores)
		globalTopRows := make([]int, min(100, baseRows))
		globalTopScores := make([]float32, len(globalTopRows))
		columnVectorGraphDeep1BTopKFromScores(globalScores, len(globalTopRows), globalTopRows, globalTopScores)
		granuleOrder := columnVectorGraphDeep1BRankGranulesByCentroid(query, queryInvNorm, granules, columnVectorGraphDeep1BDims)
		for _, topGranules := range topGranulesList {
			if topGranules <= 0 {
				t.Fatalf("topGranules=%d must be positive", topGranules)
			}
			selected := columnVectorGraphDeep1BSelectGranules(granules, granuleOrder, topGranules)
			if selection == "graph_visited_blocks" {
				selected = columnVectorGraphDeep1BSelectGraphVisitedBlocks(query, queryInvNorm, vectors, invNorms, granules, graphRouting, topGranules, graphEntryClusters, columnVectorGraphDeep1BDims)
				selected = columnVectorGraphDeep1BSelectedWithSelectionBuilder(selected, selection)
			} else if selection == "graph_visited_rows" {
				selected = columnVectorGraphDeep1BSelectGraphVisitedRows(query, queryInvNorm, vectors, invNorms, builder, graphRouting, topGranules*granuleRows, graphEntryClusters, columnVectorGraphDeep1BDims)
				selected = columnVectorGraphDeep1BSelectedWithSelectionBuilder(selected, selection)
			}
			queryReport := columnVectorGraphDeep1BAnalyzeBuildableGranuleSelection(t, vectors, invNorms, query, queryInvNorm, globalScores, globalTopRows, selected, queryIndex, topGranules, ranks, codebookModels, localResidualPQModels, localOPQModels, scanIters)
			report.Queries = append(report.Queries, queryReport)
		}
	}
	if err := columnVectorGraphDeep1BWriteJSON(filepath.Join(outDir, "results.json"), report); err != nil {
		t.Fatalf("write buildable granule scout JSON: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "report.md"), []byte(columnVectorGraphDeep1BRenderBuildableGranuleScoutMarkdown(report)), 0o644); err != nil {
		t.Fatalf("write buildable granule scout Markdown: %v", err)
	}
	t.Logf("Deep1B buildable granule scout report: %s", filepath.Join(outDir, "report.md"))
}

type columnVectorGraphDeep1BBuildableGranuleScoutReport struct {
	GeneratedAt             string                                               `json:"generated_at"`
	OutputDir               string                                               `json:"output_dir"`
	BasePath                string                                               `json:"base_path"`
	QueryPath               string                                               `json:"query_path"`
	BaseRows                int                                                  `json:"base_rows"`
	EvalRowOffset           int                                                  `json:"eval_row_offset,omitempty"`
	Dims                    int                                                  `json:"dims"`
	Builder                 string                                               `json:"builder"`
	Selection               string                                               `json:"selection"`
	GranuleRows             int                                                  `json:"granule_rows"`
	GranuleCount            int                                                  `json:"granule_count"`
	GranuleBuildNanos       int64                                                `json:"granule_build_nanos,omitempty"`
	RoutingBuildNanos       int64                                                `json:"routing_build_nanos,omitempty"`
	KMeansIters             int                                                  `json:"kmeans_iters,omitempty"`
	GraphDegree             int                                                  `json:"graph_degree,omitempty"`
	GraphEntryClusters      int                                                  `json:"graph_entry_clusters,omitempty"`
	PQBytes                 []int                                                `json:"pq_bytes,omitempty"`
	ResidualPQBytes         []int                                                `json:"residual_pq_bytes,omitempty"`
	OPQBytes                []int                                                `json:"opq_bytes,omitempty"`
	LocalResidualPQBytes    []int                                                `json:"local_residual_pq_bytes,omitempty"`
	LocalOPQBytes           []int                                                `json:"local_opq_bytes,omitempty"`
	PQTrainRows             int                                                  `json:"pq_train_rows,omitempty"`
	PQTrainIters            int                                                  `json:"pq_train_iters,omitempty"`
	OPQIterations           int                                                  `json:"opq_iterations,omitempty"`
	PQTraining              []columnVectorGraphDeep1BPQTrainingReport            `json:"pq_training,omitempty"`
	LocalResidualPQTraining []columnVectorGraphDeep1BPQTrainingReport            `json:"local_residual_pq_training,omitempty"`
	LocalOPQTraining        []columnVectorGraphDeep1BPQTrainingReport            `json:"local_opq_training,omitempty"`
	RequestedQueries        []int                                                `json:"requested_queries"`
	TopGranules             []int                                                `json:"top_granules"`
	Ranks                   []int                                                `json:"ranks"`
	ScanIters               int                                                  `json:"scan_iters"`
	Queries                 []columnVectorGraphDeep1BBuildableGranuleQueryReport `json:"queries"`
	Notes                   string                                               `json:"notes,omitempty"`
}

type columnVectorGraphDeep1BBuildableGranuleQueryReport struct {
	QueryIndex               int                                              `json:"query_index"`
	Builder                  string                                           `json:"builder"`
	TopGranules              int                                              `json:"top_granules"`
	CandidateRows            int                                              `json:"candidate_rows"`
	RoutingTop10InCandidates int                                              `json:"routing_top10_in_candidates"`
	RoutingTop20InCandidates int                                              `json:"routing_top20_in_candidates"`
	RoutingTop50InCandidates int                                              `json:"routing_top50_in_candidates"`
	RoutingRecallAt10        float64                                          `json:"routing_recall_at_10"`
	RoutingRecallAt20        float64                                          `json:"routing_recall_at_20"`
	RoutingRecallAt50        float64                                          `json:"routing_recall_at_50"`
	SelectedGranules         []columnVectorGraphDeep1BSelectedGranuleReport   `json:"selected_granules"`
	CandidateExactMargins    map[string]float64                               `json:"candidate_exact_margins"`
	Methods                  []columnVectorGraphDeep1BGroundtruthMethodReport `json:"methods"`
	Notes                    string                                           `json:"notes,omitempty"`
}

type columnVectorGraphDeep1BSelectedGranuleReport struct {
	Ordinal            int     `json:"ordinal"`
	FirstRow           int     `json:"first_row"`
	RowIDMin           int     `json:"row_id_min"`
	RowIDMax           int     `json:"row_id_max"`
	Rows               int     `json:"rows"`
	CentroidCosine     float64 `json:"centroid_cosine_to_query"`
	GlobalTop10Overlap int     `json:"global_top10_overlap"`
	GlobalTop20Overlap int     `json:"global_top20_overlap"`
}

type columnVectorGraphDeep1BBuildableGranule struct {
	Ordinal       int
	Builder       string
	FirstRow      int
	Rows          int
	RowIDs        []int
	Centroid      []float32
	CentroidInv   float32
	CentroidScore float64
}

type columnVectorGraphDeep1BLocalResidualPQModels struct {
	budgets   []int
	byGranule map[int]map[int]columnVectorGraphDeep1BPQModel
	training  []columnVectorGraphDeep1BPQTrainingReport
}

func columnVectorGraphDeep1BBuildableGranules(tb testing.TB, builder string, vectors []float32, invNorms []float32, rows int, dims int, granuleRows int, kmeansIters int, graphDegree int) ([]columnVectorGraphDeep1BBuildableGranule, string) {
	tb.Helper()
	switch builder {
	case "row_id_contiguous":
		return columnVectorGraphDeep1BBuildRowIDContiguousGranules(vectors, rows, dims, granuleRows), "production/buildable granule scout; row-id-contiguous blocks are buildable storage units, but they are not expected to have nearest-neighbor locality"
	case "ivf_kmeans":
		if kmeansIters <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=%d must be positive for ivf_kmeans", kmeansIters)
		}
		return columnVectorGraphDeep1BBuildIVFKMeansGranules(vectors, invNorms, rows, dims, granuleRows, kmeansIters), "production/buildable granule scout; ivf_kmeans trains deterministic cosine k-means centroids on the base prefix and assigns rows to buildable IVF-style granules"
	case "ivf_kmeans_sorted_blocks":
		if kmeansIters <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=%d must be positive for ivf_kmeans_sorted_blocks", kmeansIters)
		}
		return columnVectorGraphDeep1BBuildIVFKMeansSortedBlockGranules(vectors, invNorms, rows, dims, granuleRows, kmeansIters), "production/buildable granule scout; ivf_kmeans_sorted_blocks trains deterministic cosine k-means centroids, sorts rows by assigned centroid locality, and chunks that storage order into fixed-size TreeDB-style blocks"
	case "ivf_graph_neighborhood_blocks":
		if kmeansIters <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=%d must be positive for ivf_graph_neighborhood_blocks", kmeansIters)
		}
		if graphDegree <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRAPH_DEGREE=%d must be positive for ivf_graph_neighborhood_blocks", graphDegree)
		}
		return columnVectorGraphDeep1BBuildIVFGraphNeighborhoodBlockGranules(vectors, invNorms, rows, dims, granuleRows, kmeansIters, graphDegree), "production/buildable graph-neighborhood scout; ivf_graph_neighborhood_blocks trains deterministic cosine k-means centroids, builds a query-independent local nearest-neighbor graph inside IVF-sorted windows, and chunks graph BFS neighborhoods into fixed-size TreeDB-style blocks"
	case "ivf_graph_sorted_blocks":
		if kmeansIters <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=%d must be positive for ivf_graph_sorted_blocks", kmeansIters)
		}
		if graphDegree <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRAPH_DEGREE=%d must be positive for ivf_graph_sorted_blocks", graphDegree)
		}
		return columnVectorGraphDeep1BBuildIVFGraphSortedBlockGranules(vectors, invNorms, rows, dims, granuleRows, kmeansIters, graphDegree), "production/buildable graph-sorted row-adjacent scout; ivf_graph_sorted_blocks trains deterministic cosine k-means centroids, builds a query-independent local nearest-neighbor graph inside IVF-sorted windows, materializes a deterministic graph traversal order, and chunks adjacent rows in that graph order into fixed-size TreeDB-style blocks"
	case "treedb_graph_sorted_part_granules":
		if kmeansIters <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=%d must be positive for treedb_graph_sorted_part_granules", kmeansIters)
		}
		if graphDegree <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRAPH_DEGREE=%d must be positive for treedb_graph_sorted_part_granules", graphDegree)
		}
		return columnVectorGraphDeep1BBuildTreeDBGraphSortedPartGranules(tb, vectors, invNorms, rows, dims, granuleRows, kmeansIters, graphDegree), "production/buildable TreeDB part-granule scout; treedb_graph_sorted_part_granules trains deterministic cosine k-means centroids, builds a query-independent local nearest-neighbor graph inside IVF-sorted windows, materializes that graph traversal order through BuildColumnPart, and derives candidate blocks from ColumnPart.Descriptor.Granules"
	case "ivf_exact_graph_neighborhood_blocks":
		if kmeansIters <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=%d must be positive for ivf_exact_graph_neighborhood_blocks", kmeansIters)
		}
		if graphDegree <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRAPH_DEGREE=%d must be positive for ivf_exact_graph_neighborhood_blocks", graphDegree)
		}
		return columnVectorGraphDeep1BBuildIVFExactGraphNeighborhoodBlockGranules(vectors, invNorms, rows, dims, granuleRows, kmeansIters, graphDegree), "production/buildable exact-graph-neighborhood scout; ivf_exact_graph_neighborhood_blocks trains deterministic cosine k-means centroids, builds exact in-cluster kNN adjacency for the eval slice, and chunks graph BFS neighborhoods into fixed-size TreeDB-style blocks"
	case "ivf_exact_graph_sorted_blocks":
		if kmeansIters <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=%d must be positive for ivf_exact_graph_sorted_blocks", kmeansIters)
		}
		if graphDegree <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRAPH_DEGREE=%d must be positive for ivf_exact_graph_sorted_blocks", graphDegree)
		}
		return columnVectorGraphDeep1BBuildIVFExactGraphSortedBlockGranules(vectors, invNorms, rows, dims, granuleRows, kmeansIters, graphDegree), "production/buildable exact-graph-sorted row-adjacent scout; ivf_exact_graph_sorted_blocks trains deterministic cosine k-means centroids, builds exact in-cluster kNN adjacency for the eval slice, materializes a deterministic graph traversal order, and chunks adjacent rows in that graph order into fixed-size TreeDB-style blocks"
	default:
		tb.Fatalf("unknown COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=%q; supported: row_id_contiguous, ivf_kmeans, ivf_kmeans_sorted_blocks, ivf_graph_neighborhood_blocks, ivf_graph_sorted_blocks, treedb_graph_sorted_part_granules, ivf_exact_graph_neighborhood_blocks, ivf_exact_graph_sorted_blocks", builder)
		return nil, ""
	}
}

func columnVectorGraphDeep1BSupportsGraphVisitedSelection(builder string) bool {
	return builder == "ivf_graph_sorted_blocks" || builder == "treedb_graph_sorted_part_granules" || builder == "ivf_exact_graph_sorted_blocks"
}

func columnVectorGraphDeep1BIsGraphVisitedSelection(selection string) bool {
	return selection == "graph_visited_blocks" || selection == "graph_visited_rows"
}

func columnVectorGraphDeep1BBuilderUsesGraph(builder string) bool {
	switch builder {
	case "ivf_graph_neighborhood_blocks", "ivf_graph_sorted_blocks", "treedb_graph_sorted_part_granules", "ivf_exact_graph_neighborhood_blocks", "ivf_exact_graph_sorted_blocks":
		return true
	default:
		return false
	}
}

func columnVectorGraphDeep1BBuilderUsesKMeans(builder string) bool {
	switch builder {
	case "ivf_kmeans", "ivf_kmeans_sorted_blocks", "ivf_graph_neighborhood_blocks", "ivf_graph_sorted_blocks", "treedb_graph_sorted_part_granules", "ivf_exact_graph_neighborhood_blocks", "ivf_exact_graph_sorted_blocks":
		return true
	default:
		return false
	}
}

func columnVectorGraphDeep1BFitBuildableLocalResidualPQModels(tb testing.TB, vectors []float32, invNorms []float32, granules []columnVectorGraphDeep1BBuildableGranule, rowCodeBytes []int, amortizeRows int, dims int, iterations int) columnVectorGraphDeep1BLocalResidualPQModels {
	tb.Helper()
	budgets := columnVectorGraphDeep1BFilterPQBudgetsForDims(tb, "local residual PQ", rowCodeBytes, dims)
	if len(budgets) == 0 {
		return columnVectorGraphDeep1BLocalResidualPQModels{}
	}
	models := columnVectorGraphDeep1BLocalResidualPQModels{
		budgets:   budgets,
		byGranule: make(map[int]map[int]columnVectorGraphDeep1BPQModel, len(granules)),
		training:  make([]columnVectorGraphDeep1BPQTrainingReport, 0, len(budgets)),
	}
	for _, budget := range budgets {
		var trainRows int
		var trainNanos int64
		var metadataBytes int
		for _, granule := range granules {
			if granule.Rows < columnVectorGraphDeep1BPQCodebookSize {
				tb.Fatalf("local residual PQ granule=%d rows=%d must be at least codebook size=%d", granule.Ordinal, granule.Rows, columnVectorGraphDeep1BPQCodebookSize)
			}
			gVectors, _ := columnVectorGraphDeep1BGatherRows(vectors, invNorms, granule.RowIDs, dims)
			model := columnVectorGraphDeep1BFitResidualPQModel(tb, gVectors, granule.Rows, dims, budget, iterations, granule.Rows)
			model.method = "local_residual_pq"
			model.family = "local_residual_product_quantization"
			model.amortizeRows = granule.Rows
			if models.byGranule[granule.Ordinal] == nil {
				models.byGranule[granule.Ordinal] = make(map[int]columnVectorGraphDeep1BPQModel, len(budgets))
			}
			models.byGranule[granule.Ordinal][budget] = model
			trainRows += granule.Rows
			trainNanos += model.trainNanos
			metadataBytes += model.codebookMetadataBytes
		}
		models.training = append(models.training, columnVectorGraphDeep1BPQTrainingReport{
			Method:                       "local_residual_pq",
			RowCodeBytes:                 budget,
			Subquantizers:                budget,
			CodebookSize:                 columnVectorGraphDeep1BPQCodebookSize,
			TrainRows:                    trainRows,
			TrainIterations:              iterations,
			TrainNanos:                   trainNanos,
			CodebookMetadataBytes:        metadataBytes,
			CodebookMetadataBytesPerEval: float64(metadataBytes) / float64(max(1, amortizeRows)),
			Notes:                        "per-buildable-granule f16 residual centroid plus 8-bit residual PQ codebooks trained on the rows in each sealed granule; LOPQ-lite without OPQ rotation, not an official top100 oracle fit",
		})
	}
	return models
}

func columnVectorGraphDeep1BFitBuildableLocalOPQModels(tb testing.TB, vectors []float32, invNorms []float32, granules []columnVectorGraphDeep1BBuildableGranule, rowCodeBytes []int, amortizeRows int, dims int, pqIterations int, opqIterations int) columnVectorGraphDeep1BLocalResidualPQModels {
	tb.Helper()
	budgets := columnVectorGraphDeep1BFilterPQBudgetsForDims(tb, "local OPQ", rowCodeBytes, dims)
	if len(budgets) == 0 {
		return columnVectorGraphDeep1BLocalResidualPQModels{}
	}
	models := columnVectorGraphDeep1BLocalResidualPQModels{
		budgets:   budgets,
		byGranule: make(map[int]map[int]columnVectorGraphDeep1BPQModel, len(granules)),
		training:  make([]columnVectorGraphDeep1BPQTrainingReport, 0, len(budgets)),
	}
	for _, budget := range budgets {
		var trainRows int
		var trainNanos int64
		var metadataBytes int
		for _, granule := range granules {
			if granule.Rows < columnVectorGraphDeep1BPQCodebookSize {
				tb.Fatalf("local OPQ granule=%d rows=%d must be at least codebook size=%d", granule.Ordinal, granule.Rows, columnVectorGraphDeep1BPQCodebookSize)
			}
			gVectors, _ := columnVectorGraphDeep1BGatherRows(vectors, invNorms, granule.RowIDs, dims)
			model := columnVectorGraphDeep1BFitResidualOPQModel(tb, gVectors, granule.Rows, dims, budget, pqIterations, opqIterations, granule.Rows)
			model.method = "local_residual_opq"
			model.family = "local_residual_optimized_product_quantization"
			model.amortizeRows = granule.Rows
			if models.byGranule[granule.Ordinal] == nil {
				models.byGranule[granule.Ordinal] = make(map[int]columnVectorGraphDeep1BPQModel, len(budgets))
			}
			models.byGranule[granule.Ordinal][budget] = model
			trainRows += granule.Rows
			trainNanos += model.trainNanos
			metadataBytes += model.codebookMetadataBytes
		}
		models.training = append(models.training, columnVectorGraphDeep1BPQTrainingReport{
			Method:                       "local_residual_opq",
			RowCodeBytes:                 budget,
			Subquantizers:                budget,
			CodebookSize:                 columnVectorGraphDeep1BPQCodebookSize,
			TrainRows:                    trainRows,
			TrainIterations:              pqIterations,
			OPQIterations:                opqIterations,
			TrainNanos:                   trainNanos,
			CodebookMetadataBytes:        metadataBytes,
			CodebookMetadataBytesPerEval: float64(metadataBytes) / float64(max(1, amortizeRows)),
			Notes:                        "per-buildable-granule f16 residual centroid, f16 OPQ rotation, and 8-bit residual OPQ/PQ codebooks trained on the rows in each sealed granule; production/buildable LOPQ-style lane, not an official top100 oracle fit",
		})
	}
	return models
}

func columnVectorGraphDeep1BEnvString(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func columnVectorGraphDeep1BBuildRowIDContiguousGranules(vectors []float32, rows int, dims int, granuleRows int) []columnVectorGraphDeep1BBuildableGranule {
	granules := make([]columnVectorGraphDeep1BBuildableGranule, 0, (rows+granuleRows-1)/granuleRows)
	for first := 0; first < rows; first += granuleRows {
		count := min(granuleRows, rows-first)
		rowIDs := make([]int, count)
		for row := range rowIDs {
			rowIDs[row] = first + row
		}
		centroid := columnVectorGraphDeep1BCentroidForRowIDs(vectors, rowIDs, dims)
		granules = append(granules, columnVectorGraphDeep1BBuildableGranule{
			Builder:     "row_id_contiguous",
			Ordinal:     len(granules),
			FirstRow:    first,
			Rows:        count,
			RowIDs:      rowIDs,
			Centroid:    centroid,
			CentroidInv: float32(columnVectorGraphDeep1BInvNorm(centroid)),
		})
	}
	return granules
}

func columnVectorGraphDeep1BBuildIVFKMeansGranules(vectors []float32, invNorms []float32, rows int, dims int, targetRows int, iterations int) []columnVectorGraphDeep1BBuildableGranule {
	_, assignments, clusterCount := columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors, invNorms, rows, dims, targetRows, iterations)
	rowIDsByCluster := make([][]int, clusterCount)
	for row, cluster := range assignments {
		rowIDsByCluster[cluster] = append(rowIDsByCluster[cluster], row)
	}
	granules := make([]columnVectorGraphDeep1BBuildableGranule, 0, clusterCount)
	for cluster, rowIDs := range rowIDsByCluster {
		if len(rowIDs) == 0 {
			continue
		}
		centroid := columnVectorGraphDeep1BCentroidForRowIDs(vectors, rowIDs, dims)
		sort.Ints(rowIDs)
		granules = append(granules, columnVectorGraphDeep1BBuildableGranule{
			Builder:     "ivf_kmeans",
			Ordinal:     cluster,
			FirstRow:    rowIDs[0],
			Rows:        len(rowIDs),
			RowIDs:      rowIDs,
			Centroid:    centroid,
			CentroidInv: float32(columnVectorGraphDeep1BInvNorm(centroid)),
		})
	}
	return granules
}

func columnVectorGraphDeep1BBuildIVFKMeansSortedBlockGranules(vectors []float32, invNorms []float32, rows int, dims int, targetRows int, iterations int) []columnVectorGraphDeep1BBuildableGranule {
	centroids, assignments, _ := columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors, invNorms, rows, dims, targetRows, iterations)
	centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
	order := columnVectorGraphDeep1BIVFSortedRows(vectors, invNorms, centroids, centroidInvNorms, assignments, rows, dims)
	granules := make([]columnVectorGraphDeep1BBuildableGranule, 0, (rows+targetRows-1)/targetRows)
	for first := 0; first < rows; first += targetRows {
		count := min(targetRows, rows-first)
		rowIDs := make([]int, count)
		for i := 0; i < count; i++ {
			rowIDs[i] = order[first+i].row
		}
		centroid := columnVectorGraphDeep1BCentroidForRowIDs(vectors, rowIDs, dims)
		granules = append(granules, columnVectorGraphDeep1BBuildableGranule{
			Builder:     "ivf_kmeans_sorted_blocks",
			Ordinal:     len(granules),
			FirstRow:    rowIDs[0],
			Rows:        count,
			RowIDs:      rowIDs,
			Centroid:    centroid,
			CentroidInv: float32(columnVectorGraphDeep1BInvNorm(centroid)),
		})
	}
	return granules
}

type columnVectorGraphDeep1BIVFSortedRow struct {
	row     int
	cluster int
	score   float32
}

func columnVectorGraphDeep1BIVFSortedRows(vectors []float32, invNorms []float32, centroids []float32, centroidInvNorms []float32, assignments []int, rows int, dims int) []columnVectorGraphDeep1BIVFSortedRow {
	order := make([]columnVectorGraphDeep1BIVFSortedRow, rows)
	for row := 0; row < rows; row++ {
		cluster := assignments[row]
		vector := vectors[row*dims : (row+1)*dims]
		centroid := centroids[cluster*dims : (cluster+1)*dims]
		var dot float32
		for j := 0; j < dims; j++ {
			dot += vector[j] * centroid[j]
		}
		order[row] = columnVectorGraphDeep1BIVFSortedRow{
			row:     row,
			cluster: cluster,
			score:   dot * invNorms[row] * centroidInvNorms[cluster],
		}
	}
	sort.Slice(order, func(i, j int) bool {
		left := order[i]
		right := order[j]
		if left.cluster != right.cluster {
			return left.cluster < right.cluster
		}
		if left.score != right.score {
			return left.score > right.score
		}
		return left.row < right.row
	})
	return order
}

func columnVectorGraphDeep1BBuildIVFGraphNeighborhoodBlockGranules(vectors []float32, invNorms []float32, rows int, dims int, targetRows int, iterations int, graphDegree int) []columnVectorGraphDeep1BBuildableGranule {
	centroids, assignments, _ := columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors, invNorms, rows, dims, targetRows, iterations)
	centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
	order := columnVectorGraphDeep1BIVFSortedRows(vectors, invNorms, centroids, centroidInvNorms, assignments, rows, dims)
	adjacency := columnVectorGraphDeep1BBuildIVFWindowGraph(vectors, invNorms, order, rows, dims, graphDegree)
	assigned := make([]bool, rows)
	assignedCount := 0
	nextSeed := 0
	granules := make([]columnVectorGraphDeep1BBuildableGranule, 0, (rows+targetRows-1)/targetRows)
	for assignedCount < rows {
		rowIDs := make([]int, 0, min(targetRows, rows-assignedCount))
		queue := make([]int, 0, targetRows+graphDegree)
		queueHead := 0
		for len(rowIDs) < targetRows && assignedCount < rows {
			if queueHead >= len(queue) {
				queue = queue[:0]
				queueHead = 0
				for nextSeed < len(order) && assigned[order[nextSeed].row] {
					nextSeed++
				}
				if nextSeed >= len(order) {
					break
				}
				queue = append(queue, order[nextSeed].row)
				nextSeed++
			}
			row := queue[queueHead]
			queueHead++
			if assigned[row] {
				continue
			}
			assigned[row] = true
			assignedCount++
			rowIDs = append(rowIDs, row)
			for _, neighbor := range adjacency[row] {
				if !assigned[neighbor] {
					queue = append(queue, neighbor)
				}
			}
		}
		if len(rowIDs) == 0 {
			break
		}
		centroid := columnVectorGraphDeep1BCentroidForRowIDs(vectors, rowIDs, dims)
		granules = append(granules, columnVectorGraphDeep1BBuildableGranule{
			Builder:     "ivf_graph_neighborhood_blocks",
			Ordinal:     len(granules),
			FirstRow:    rowIDs[0],
			Rows:        len(rowIDs),
			RowIDs:      rowIDs,
			Centroid:    centroid,
			CentroidInv: float32(columnVectorGraphDeep1BInvNorm(centroid)),
		})
	}
	return granules
}

func columnVectorGraphDeep1BBuildIVFGraphSortedBlockGranules(vectors []float32, invNorms []float32, rows int, dims int, targetRows int, iterations int, graphDegree int) []columnVectorGraphDeep1BBuildableGranule {
	centroids, assignments, _ := columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors, invNorms, rows, dims, targetRows, iterations)
	centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
	order := columnVectorGraphDeep1BIVFSortedRows(vectors, invNorms, centroids, centroidInvNorms, assignments, rows, dims)
	adjacency := columnVectorGraphDeep1BBuildIVFWindowGraph(vectors, invNorms, order, rows, dims, graphDegree)
	graphOrder := columnVectorGraphDeep1BIVFGraphTraversalOrder(order, adjacency, rows)
	granules := make([]columnVectorGraphDeep1BBuildableGranule, 0, (rows+targetRows-1)/targetRows)
	for first := 0; first < len(graphOrder); first += targetRows {
		count := min(targetRows, len(graphOrder)-first)
		rowIDs := make([]int, count)
		copy(rowIDs, graphOrder[first:first+count])
		centroid := columnVectorGraphDeep1BCentroidForRowIDs(vectors, rowIDs, dims)
		granules = append(granules, columnVectorGraphDeep1BBuildableGranule{
			Builder:     "ivf_graph_sorted_blocks",
			Ordinal:     len(granules),
			FirstRow:    rowIDs[0],
			Rows:        count,
			RowIDs:      rowIDs,
			Centroid:    centroid,
			CentroidInv: float32(columnVectorGraphDeep1BInvNorm(centroid)),
		})
	}
	return granules
}

func columnVectorGraphDeep1BBuildTreeDBGraphSortedPartGranules(tb testing.TB, vectors []float32, invNorms []float32, rows int, dims int, targetRows int, iterations int, graphDegree int) []columnVectorGraphDeep1BBuildableGranule {
	tb.Helper()
	centroids, assignments, _ := columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors, invNorms, rows, dims, targetRows, iterations)
	centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
	order := columnVectorGraphDeep1BIVFSortedRows(vectors, invNorms, centroids, centroidInvNorms, assignments, rows, dims)
	adjacency := columnVectorGraphDeep1BBuildIVFWindowGraph(vectors, invNorms, order, rows, dims, graphDegree)
	graphOrder := columnVectorGraphDeep1BIVFGraphTraversalOrder(order, adjacency, rows)
	return columnVectorGraphDeep1BBuildTreeDBPartGranulesFromOrder(tb, vectors, invNorms, graphOrder, "treedb_graph_sorted_part_granules", dims, targetRows)
}

func columnVectorGraphDeep1BBuildTreeDBPartGranulesFromOrder(tb testing.TB, vectors []float32, invNorms []float32, rowOrder []int, builder string, dims int, rowsPerGranule int) []columnVectorGraphDeep1BBuildableGranule {
	tb.Helper()
	rows := len(rowOrder)
	ids := make([]int64, rows)
	storageVectors := make([]float32, rows*dims)
	storageInvNorms := make([]float32, rows)
	for storageRow, sourceRow := range rowOrder {
		if sourceRow < 0 || sourceRow >= len(invNorms) {
			tb.Fatalf("%s row order contains source row %d outside [0,%d)", builder, sourceRow, len(invNorms))
		}
		ids[storageRow] = int64(storageRow)
		copy(storageVectors[storageRow*dims:(storageRow+1)*dims], vectors[sourceRow*dims:(sourceRow+1)*dims])
		storageInvNorms[storageRow] = invNorms[sourceRow]
	}
	part, err := BuildColumnPart(1, columnVectorGraphDeep1BTreeDBPartOptions(dims, rowsPerGranule), ColumnBatch{
		Rows:    rows,
		Columns: map[string][]int64{"id": ids},
		Float32Vectors: map[string]Float32VectorColumn{
			"embedding":          {Dims: dims, Values: storageVectors},
			"embedding_inv_norm": {Dims: 1, Values: storageInvNorms},
		},
	})
	if err != nil {
		tb.Fatalf("BuildColumnPart(%s): %v", builder, err)
	}
	granules := make([]columnVectorGraphDeep1BBuildableGranule, 0, len(part.Descriptor.Granules))
	for _, descriptor := range part.Descriptor.Granules {
		if descriptor.FirstRow < 0 || descriptor.RowCount <= 0 || descriptor.FirstRow+descriptor.RowCount > len(rowOrder) {
			tb.Fatalf("%s descriptor granule ordinal=%d has invalid first_row=%d row_count=%d for rows=%d", builder, descriptor.Ordinal, descriptor.FirstRow, descriptor.RowCount, len(rowOrder))
		}
		rowIDs := append([]int(nil), rowOrder[descriptor.FirstRow:descriptor.FirstRow+descriptor.RowCount]...)
		centroid := columnVectorGraphDeep1BCentroidForRowIDs(vectors, rowIDs, dims)
		granules = append(granules, columnVectorGraphDeep1BBuildableGranule{
			Builder:     builder,
			Ordinal:     descriptor.Ordinal,
			FirstRow:    descriptor.FirstRow,
			Rows:        descriptor.RowCount,
			RowIDs:      rowIDs,
			Centroid:    centroid,
			CentroidInv: float32(columnVectorGraphDeep1BInvNorm(centroid)),
		})
	}
	return granules
}

func columnVectorGraphDeep1BTreeDBPartOptions(dims int, rowsPerGranule int) ColumnStoreOptions {
	return ColumnStoreOptions{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CodecBlockRows: rowsPerGranule},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, VectorDims: dims, Compression: CompressionNone, CodecBlockRows: rowsPerGranule},
			{Name: "embedding_inv_norm", Type: ColumnTypeFloat32Vector, VectorDims: 1, Compression: CompressionNone, CodecBlockRows: rowsPerGranule},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy: ColumnPartPolicy{
			RowsPerGranule:        rowsPerGranule,
			DefaultCodecBlockRows: rowsPerGranule,
		},
	}
}

func columnVectorGraphDeep1BBuildIVFExactGraphNeighborhoodBlockGranules(vectors []float32, invNorms []float32, rows int, dims int, targetRows int, iterations int, graphDegree int) []columnVectorGraphDeep1BBuildableGranule {
	centroids, assignments, clusterCount := columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors, invNorms, rows, dims, targetRows, iterations)
	centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
	order := columnVectorGraphDeep1BIVFSortedRows(vectors, invNorms, centroids, centroidInvNorms, assignments, rows, dims)
	adjacency := columnVectorGraphDeep1BBuildIVFExactClusterGraph(vectors, invNorms, assignments, clusterCount, rows, dims, graphDegree)
	assigned := make([]bool, rows)
	assignedCount := 0
	nextSeed := 0
	granules := make([]columnVectorGraphDeep1BBuildableGranule, 0, (rows+targetRows-1)/targetRows)
	for assignedCount < rows {
		rowIDs := make([]int, 0, min(targetRows, rows-assignedCount))
		queue := make([]int, 0, targetRows+graphDegree)
		queueHead := 0
		for len(rowIDs) < targetRows && assignedCount < rows {
			if queueHead >= len(queue) {
				queue = queue[:0]
				queueHead = 0
				for nextSeed < len(order) && assigned[order[nextSeed].row] {
					nextSeed++
				}
				if nextSeed >= len(order) {
					break
				}
				queue = append(queue, order[nextSeed].row)
				nextSeed++
			}
			row := queue[queueHead]
			queueHead++
			if assigned[row] {
				continue
			}
			assigned[row] = true
			assignedCount++
			rowIDs = append(rowIDs, row)
			for _, neighbor := range adjacency[row] {
				if !assigned[neighbor] {
					queue = append(queue, neighbor)
				}
			}
		}
		if len(rowIDs) == 0 {
			break
		}
		centroid := columnVectorGraphDeep1BCentroidForRowIDs(vectors, rowIDs, dims)
		granules = append(granules, columnVectorGraphDeep1BBuildableGranule{
			Builder:     "ivf_exact_graph_neighborhood_blocks",
			Ordinal:     len(granules),
			FirstRow:    rowIDs[0],
			Rows:        len(rowIDs),
			RowIDs:      rowIDs,
			Centroid:    centroid,
			CentroidInv: float32(columnVectorGraphDeep1BInvNorm(centroid)),
		})
	}
	return granules
}

func columnVectorGraphDeep1BBuildIVFExactGraphSortedBlockGranules(vectors []float32, invNorms []float32, rows int, dims int, targetRows int, iterations int, graphDegree int) []columnVectorGraphDeep1BBuildableGranule {
	centroids, assignments, clusterCount := columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors, invNorms, rows, dims, targetRows, iterations)
	centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
	order := columnVectorGraphDeep1BIVFSortedRows(vectors, invNorms, centroids, centroidInvNorms, assignments, rows, dims)
	adjacency := columnVectorGraphDeep1BBuildIVFExactClusterGraph(vectors, invNorms, assignments, clusterCount, rows, dims, graphDegree)
	graphOrder := columnVectorGraphDeep1BIVFGraphTraversalOrder(order, adjacency, rows)
	granules := make([]columnVectorGraphDeep1BBuildableGranule, 0, (rows+targetRows-1)/targetRows)
	for first := 0; first < len(graphOrder); first += targetRows {
		count := min(targetRows, len(graphOrder)-first)
		rowIDs := make([]int, count)
		copy(rowIDs, graphOrder[first:first+count])
		centroid := columnVectorGraphDeep1BCentroidForRowIDs(vectors, rowIDs, dims)
		granules = append(granules, columnVectorGraphDeep1BBuildableGranule{
			Builder:     "ivf_exact_graph_sorted_blocks",
			Ordinal:     len(granules),
			FirstRow:    rowIDs[0],
			Rows:        count,
			RowIDs:      rowIDs,
			Centroid:    centroid,
			CentroidInv: float32(columnVectorGraphDeep1BInvNorm(centroid)),
		})
	}
	return granules
}

func columnVectorGraphDeep1BIVFGraphTraversalOrder(order []columnVectorGraphDeep1BIVFSortedRow, adjacency [][]int, rows int) []int {
	visited := make([]bool, rows)
	queued := make([]bool, rows)
	graphOrder := make([]int, 0, rows)
	queue := make([]int, 0, rows)
	for _, seed := range order {
		seedRow := seed.row
		if visited[seedRow] {
			continue
		}
		queue = queue[:0]
		queue = append(queue, seedRow)
		queued[seedRow] = true
		for head := 0; head < len(queue); head++ {
			row := queue[head]
			if visited[row] {
				continue
			}
			visited[row] = true
			graphOrder = append(graphOrder, row)
			for _, neighbor := range adjacency[row] {
				if neighbor < 0 || neighbor >= rows || visited[neighbor] || queued[neighbor] {
					continue
				}
				queue = append(queue, neighbor)
				queued[neighbor] = true
			}
		}
	}
	return graphOrder
}

func columnVectorGraphDeep1BBuildIVFWindowGraph(vectors []float32, invNorms []float32, order []columnVectorGraphDeep1BIVFSortedRow, rows int, dims int, graphDegree int) [][]int {
	adjacency := make([][]int, rows)
	window := max(graphDegree*4, 32)
	for first := 0; first < len(order); {
		cluster := order[first].cluster
		last := first + 1
		for last < len(order) && order[last].cluster == cluster {
			last++
		}
		for pos := first; pos < last; pos++ {
			row := order[pos].row
			start := max(first, pos-window)
			end := min(last, pos+window+1)
			neighbors := make([]columnVectorGraphDeep1BIVFWindowNeighbor, 0, end-start-1)
			left := vectors[row*dims : (row+1)*dims]
			for otherPos := start; otherPos < end; otherPos++ {
				other := order[otherPos].row
				if other == row {
					continue
				}
				right := vectors[other*dims : (other+1)*dims]
				var dot float32
				for j := 0; j < dims; j++ {
					dot += left[j] * right[j]
				}
				neighbors = append(neighbors, columnVectorGraphDeep1BIVFWindowNeighbor{
					row:   other,
					score: dot * invNorms[row] * invNorms[other],
				})
			}
			sort.Slice(neighbors, func(i, j int) bool {
				if neighbors[i].score == neighbors[j].score {
					return neighbors[i].row < neighbors[j].row
				}
				return neighbors[i].score > neighbors[j].score
			})
			limit := min(graphDegree, len(neighbors))
			adjacency[row] = make([]int, limit)
			for i := 0; i < limit; i++ {
				adjacency[row][i] = neighbors[i].row
			}
		}
		first = last
	}
	return adjacency
}

func columnVectorGraphDeep1BBuildIVFExactClusterGraph(vectors []float32, invNorms []float32, assignments []int, clusterCount int, rows int, dims int, graphDegree int) [][]int {
	rowIDsByCluster := make([][]int, clusterCount)
	for row, cluster := range assignments {
		if cluster < 0 || cluster >= clusterCount {
			continue
		}
		rowIDsByCluster[cluster] = append(rowIDsByCluster[cluster], row)
	}
	adjacency := make([][]int, rows)
	for _, clusterRows := range rowIDsByCluster {
		for _, row := range clusterRows {
			if len(clusterRows) <= 1 {
				continue
			}
			left := vectors[row*dims : (row+1)*dims]
			neighbors := make([]columnVectorGraphDeep1BIVFWindowNeighbor, 0, len(clusterRows)-1)
			for _, other := range clusterRows {
				if other == row {
					continue
				}
				right := vectors[other*dims : (other+1)*dims]
				var dot float32
				for j := 0; j < dims; j++ {
					dot += left[j] * right[j]
				}
				neighbors = append(neighbors, columnVectorGraphDeep1BIVFWindowNeighbor{
					row:   other,
					score: dot * invNorms[row] * invNorms[other],
				})
			}
			sort.Slice(neighbors, func(i, j int) bool {
				if neighbors[i].score == neighbors[j].score {
					return neighbors[i].row < neighbors[j].row
				}
				return neighbors[i].score > neighbors[j].score
			})
			limit := min(graphDegree, len(neighbors))
			adjacency[row] = make([]int, limit)
			for i := 0; i < limit; i++ {
				adjacency[row][i] = neighbors[i].row
			}
		}
	}
	return adjacency
}

type columnVectorGraphDeep1BIVFWindowNeighbor struct {
	row   int
	score float32
}

func columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors []float32, invNorms []float32, rows int, dims int, targetRows int, iterations int) ([]float32, []int, int) {
	clusterCount := max(1, (rows+targetRows-1)/targetRows)
	centroids := columnVectorGraphDeep1BInitKMeansCentroids(vectors, invNorms, rows, dims, clusterCount)
	assignments := make([]int, rows)
	counts := make([]int, clusterCount)
	for iter := 0; iter < iterations; iter++ {
		centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
		for i := range counts {
			counts[i] = 0
		}
		for row := 0; row < rows; row++ {
			cluster := columnVectorGraphDeep1BNearestCentroid(vectors[row*dims:(row+1)*dims], invNorms[row], centroids, centroidInvNorms, dims)
			assignments[row] = cluster
			counts[cluster]++
		}
		next := make([]float32, len(centroids))
		for row, cluster := range assignments {
			base := row * dims
			dst := cluster * dims
			for j := 0; j < dims; j++ {
				next[dst+j] += vectors[base+j]
			}
		}
		for cluster := 0; cluster < clusterCount; cluster++ {
			if counts[cluster] == 0 {
				row := (cluster * rows) / clusterCount
				copy(next[cluster*dims:(cluster+1)*dims], vectors[row*dims:(row+1)*dims])
				continue
			}
			invCount := float32(1 / float64(counts[cluster]))
			for j := 0; j < dims; j++ {
				next[cluster*dims+j] *= invCount
			}
		}
		centroids = next
	}
	centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
	for row := 0; row < rows; row++ {
		assignments[row] = columnVectorGraphDeep1BNearestCentroid(vectors[row*dims:(row+1)*dims], invNorms[row], centroids, centroidInvNorms, dims)
	}
	return centroids, assignments, clusterCount
}

func columnVectorGraphDeep1BInitKMeansCentroids(vectors []float32, invNorms []float32, rows int, dims int, clusterCount int) []float32 {
	centroids := make([]float32, clusterCount*dims)
	first := 0
	copy(centroids[:dims], vectors[first*dims:(first+1)*dims])
	chosen := []int{first}
	for cluster := 1; cluster < clusterCount; cluster++ {
		bestRow := 0
		bestNearest := float32(-math.MaxFloat32)
		for row := 0; row < rows; row++ {
			rowVector := vectors[row*dims : (row+1)*dims]
			nearest := float32(math.MaxFloat32)
			for _, chosenRow := range chosen {
				var dot float32
				chosenVector := vectors[chosenRow*dims : (chosenRow+1)*dims]
				for j := 0; j < dims; j++ {
					dot += rowVector[j] * chosenVector[j]
				}
				distance := 1 - dot*invNorms[row]*invNorms[chosenRow]
				if distance < nearest {
					nearest = distance
				}
			}
			if nearest > bestNearest {
				bestNearest = nearest
				bestRow = row
			}
		}
		copy(centroids[cluster*dims:(cluster+1)*dims], vectors[bestRow*dims:(bestRow+1)*dims])
		chosen = append(chosen, bestRow)
	}
	return centroids
}

func columnVectorGraphDeep1BCentroidInvNorms(centroids []float32, dims int) []float32 {
	centroidInvNorms := make([]float32, len(centroids)/dims)
	for cluster := range centroidInvNorms {
		centroid := centroids[cluster*dims : (cluster+1)*dims]
		centroidInvNorms[cluster] = float32(columnVectorGraphDeep1BInvNorm(centroid))
	}
	return centroidInvNorms
}

func columnVectorGraphDeep1BNearestCentroid(vector []float32, vectorInvNorm float32, centroids []float32, centroidInvNorms []float32, dims int) int {
	bestCluster := 0
	bestScore := float32(-math.MaxFloat32)
	for cluster := 0; cluster < len(centroids)/dims; cluster++ {
		centroid := centroids[cluster*dims : (cluster+1)*dims]
		var dot float32
		for j := 0; j < dims; j++ {
			dot += vector[j] * centroid[j]
		}
		score := dot * vectorInvNorm * centroidInvNorms[cluster]
		if score > bestScore {
			bestScore = score
			bestCluster = cluster
		}
	}
	return bestCluster
}

func columnVectorGraphDeep1BCentroidForRowIDs(vectors []float32, rowIDs []int, dims int) []float32 {
	centroid := make([]float32, dims)
	if len(rowIDs) == 0 {
		return centroid
	}
	for _, rowID := range rowIDs {
		base := rowID * dims
		for j := 0; j < dims; j++ {
			centroid[j] += vectors[base+j]
		}
	}
	invRows := float32(1 / float64(len(rowIDs)))
	for j := range centroid {
		centroid[j] *= invRows
	}
	return centroid
}

func columnVectorGraphDeep1BRankGranulesByCentroid(query []float32, queryInvNorm float32, granules []columnVectorGraphDeep1BBuildableGranule, dims int) []int {
	order := make([]int, len(granules))
	for i := range granules {
		order[i] = i
		var dot float32
		for j := 0; j < dims; j++ {
			dot += query[j] * granules[i].Centroid[j]
		}
		granules[i].CentroidScore = float64(dot * queryInvNorm * granules[i].CentroidInv)
	}
	sort.Slice(order, func(i, j int) bool {
		left := granules[order[i]]
		right := granules[order[j]]
		if left.CentroidScore == right.CentroidScore {
			return left.Ordinal < right.Ordinal
		}
		return left.CentroidScore > right.CentroidScore
	})
	return order
}

func columnVectorGraphDeep1BSelectGranules(granules []columnVectorGraphDeep1BBuildableGranule, order []int, topGranules int) []columnVectorGraphDeep1BBuildableGranule {
	topGranules = min(topGranules, len(order))
	selected := make([]columnVectorGraphDeep1BBuildableGranule, 0, topGranules)
	for _, ordinal := range order[:topGranules] {
		selected = append(selected, granules[ordinal])
	}
	return selected
}

type columnVectorGraphDeep1BGraphBlockRoutingIndex struct {
	centroids        []float32
	centroidInvNorms []float32
	clusterEntryRows []int
	adjacency        [][]int
	rowToGranule     []int
}

func columnVectorGraphDeep1BBuildGraphBlockRoutingIndex(tb testing.TB, builder string, vectors []float32, invNorms []float32, granules []columnVectorGraphDeep1BBuildableGranule, rows int, dims int, targetRows int, iterations int, graphDegree int) columnVectorGraphDeep1BGraphBlockRoutingIndex {
	tb.Helper()
	switch builder {
	case "ivf_graph_sorted_blocks":
		return columnVectorGraphDeep1BBuildIVFGraphBlockRoutingIndex(tb, vectors, invNorms, granules, rows, dims, targetRows, iterations, graphDegree)
	case "treedb_graph_sorted_part_granules":
		return columnVectorGraphDeep1BBuildIVFGraphBlockRoutingIndex(tb, vectors, invNorms, granules, rows, dims, targetRows, iterations, graphDegree)
	case "ivf_exact_graph_sorted_blocks":
		return columnVectorGraphDeep1BBuildIVFExactGraphBlockRoutingIndex(tb, vectors, invNorms, granules, rows, dims, targetRows, iterations, graphDegree)
	default:
		tb.Fatalf("builder=%s does not support graph_visited_blocks selection", builder)
		return columnVectorGraphDeep1BGraphBlockRoutingIndex{}
	}
}

func columnVectorGraphDeep1BBuildIVFGraphBlockRoutingIndex(tb testing.TB, vectors []float32, invNorms []float32, granules []columnVectorGraphDeep1BBuildableGranule, rows int, dims int, targetRows int, iterations int, graphDegree int) columnVectorGraphDeep1BGraphBlockRoutingIndex {
	tb.Helper()
	centroids, assignments, _ := columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors, invNorms, rows, dims, targetRows, iterations)
	centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
	order := columnVectorGraphDeep1BIVFSortedRows(vectors, invNorms, centroids, centroidInvNorms, assignments, rows, dims)
	adjacency := columnVectorGraphDeep1BBuildIVFWindowGraph(vectors, invNorms, order, rows, dims, graphDegree)
	return columnVectorGraphDeep1BNewGraphBlockRoutingIndex(tb, centroids, centroidInvNorms, order, adjacency, granules, rows)
}

func columnVectorGraphDeep1BBuildIVFExactGraphBlockRoutingIndex(tb testing.TB, vectors []float32, invNorms []float32, granules []columnVectorGraphDeep1BBuildableGranule, rows int, dims int, targetRows int, iterations int, graphDegree int) columnVectorGraphDeep1BGraphBlockRoutingIndex {
	tb.Helper()
	centroids, assignments, clusterCount := columnVectorGraphDeep1BFitIVFKMeansAssignments(vectors, invNorms, rows, dims, targetRows, iterations)
	centroidInvNorms := columnVectorGraphDeep1BCentroidInvNorms(centroids, dims)
	order := columnVectorGraphDeep1BIVFSortedRows(vectors, invNorms, centroids, centroidInvNorms, assignments, rows, dims)
	adjacency := columnVectorGraphDeep1BBuildIVFExactClusterGraph(vectors, invNorms, assignments, clusterCount, rows, dims, graphDegree)
	return columnVectorGraphDeep1BNewGraphBlockRoutingIndex(tb, centroids, centroidInvNorms, order, adjacency, granules, rows)
}

func columnVectorGraphDeep1BNewGraphBlockRoutingIndex(tb testing.TB, centroids []float32, centroidInvNorms []float32, order []columnVectorGraphDeep1BIVFSortedRow, adjacency [][]int, granules []columnVectorGraphDeep1BBuildableGranule, rows int) columnVectorGraphDeep1BGraphBlockRoutingIndex {
	tb.Helper()
	clusterCount := len(centroidInvNorms)
	clusterEntryRows := make([]int, clusterCount)
	for i := range clusterEntryRows {
		clusterEntryRows[i] = -1
	}
	for _, row := range order {
		if clusterEntryRows[row.cluster] < 0 {
			clusterEntryRows[row.cluster] = row.row
		}
	}
	rowToGranule := make([]int, rows)
	for i := range rowToGranule {
		rowToGranule[i] = -1
	}
	for granuleIndex, granule := range granules {
		for _, row := range granule.RowIDs {
			if row < 0 || row >= rows {
				tb.Fatalf("granule=%d row=%d outside rows=%d", granule.Ordinal, row, rows)
			}
			rowToGranule[row] = granuleIndex
		}
	}
	for row, granuleIndex := range rowToGranule {
		if granuleIndex < 0 {
			tb.Fatalf("row=%d is not assigned to a graph-routed storage block", row)
		}
	}
	return columnVectorGraphDeep1BGraphBlockRoutingIndex{
		centroids:        centroids,
		centroidInvNorms: centroidInvNorms,
		clusterEntryRows: clusterEntryRows,
		adjacency:        adjacency,
		rowToGranule:     rowToGranule,
	}
}

func columnVectorGraphDeep1BSelectGraphVisitedBlocks(query []float32, queryInvNorm float32, vectors []float32, invNorms []float32, granules []columnVectorGraphDeep1BBuildableGranule, routing columnVectorGraphDeep1BGraphBlockRoutingIndex, topGranules int, entryClusters int, dims int) []columnVectorGraphDeep1BBuildableGranule {
	topGranules = min(topGranules, len(granules))
	if topGranules <= 0 {
		return nil
	}
	centroidOrder := columnVectorGraphDeep1BRankRawCentroidsByQuery(query, queryInvNorm, routing.centroids, routing.centroidInvNorms, dims)
	entryClusters = min(entryClusters, len(centroidOrder))
	expanded := make([]bool, len(routing.adjacency))
	queued := make([]bool, len(routing.adjacency))
	candidates := make([]columnVectorGraphDeep1BGraphVisitCandidate, 0, entryClusters*max(1, len(routing.adjacency[0])+1))
	for _, cluster := range centroidOrder[:entryClusters] {
		if cluster < 0 || cluster >= len(routing.clusterEntryRows) {
			continue
		}
		row := routing.clusterEntryRows[cluster]
		if row < 0 || row >= len(routing.adjacency) || queued[row] {
			continue
		}
		queued[row] = true
		candidates = append(candidates, columnVectorGraphDeep1BGraphVisitCandidate{
			row:   row,
			score: columnVectorGraphDeep1BScoreRow(query, queryInvNorm, vectors, invNorms, row, dims),
		})
	}
	selectedOrdinals := make([]int, 0, topGranules)
	selectedSet := make(map[int]struct{}, topGranules)
	for len(selectedOrdinals) < topGranules {
		best := -1
		for i, candidate := range candidates {
			if expanded[candidate.row] {
				continue
			}
			if best < 0 || candidate.score > candidates[best].score || (candidate.score == candidates[best].score && candidate.row < candidates[best].row) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		row := candidates[best].row
		expanded[row] = true
		if granuleIndex := routing.rowToGranule[row]; granuleIndex >= 0 {
			if _, ok := selectedSet[granuleIndex]; !ok {
				selectedSet[granuleIndex] = struct{}{}
				selectedOrdinals = append(selectedOrdinals, granuleIndex)
			}
		}
		for _, neighbor := range routing.adjacency[row] {
			if neighbor < 0 || neighbor >= len(routing.adjacency) || expanded[neighbor] || queued[neighbor] {
				continue
			}
			queued[neighbor] = true
			candidates = append(candidates, columnVectorGraphDeep1BGraphVisitCandidate{
				row:   neighbor,
				score: columnVectorGraphDeep1BScoreRow(query, queryInvNorm, vectors, invNorms, neighbor, dims),
			})
		}
	}
	if len(selectedOrdinals) < topGranules {
		blockOrder := columnVectorGraphDeep1BRankGranulesByCentroid(query, queryInvNorm, granules, dims)
		for _, granuleIndex := range blockOrder {
			if _, ok := selectedSet[granuleIndex]; ok {
				continue
			}
			selectedSet[granuleIndex] = struct{}{}
			selectedOrdinals = append(selectedOrdinals, granuleIndex)
			if len(selectedOrdinals) >= topGranules {
				break
			}
		}
	}
	selected := make([]columnVectorGraphDeep1BBuildableGranule, 0, len(selectedOrdinals))
	for _, granuleIndex := range selectedOrdinals {
		selected = append(selected, granules[granuleIndex])
	}
	return selected
}

func columnVectorGraphDeep1BSelectGraphVisitedRows(query []float32, queryInvNorm float32, vectors []float32, invNorms []float32, builder string, routing columnVectorGraphDeep1BGraphBlockRoutingIndex, targetRows int, entryClusters int, dims int) []columnVectorGraphDeep1BBuildableGranule {
	targetRows = min(targetRows, len(routing.adjacency))
	if targetRows <= 0 {
		return nil
	}
	centroidOrder := columnVectorGraphDeep1BRankRawCentroidsByQuery(query, queryInvNorm, routing.centroids, routing.centroidInvNorms, dims)
	entryClusters = min(entryClusters, len(centroidOrder))
	expanded := make([]bool, len(routing.adjacency))
	queued := make([]bool, len(routing.adjacency))
	candidates := make(columnVectorGraphDeep1BGraphVisitHeap, 0, entryClusters*max(1, len(routing.adjacency[0])+1))
	for _, cluster := range centroidOrder[:entryClusters] {
		if cluster < 0 || cluster >= len(routing.clusterEntryRows) {
			continue
		}
		row := routing.clusterEntryRows[cluster]
		if row < 0 || row >= len(routing.adjacency) || queued[row] {
			continue
		}
		queued[row] = true
		heap.Push(&candidates, columnVectorGraphDeep1BGraphVisitCandidate{
			row:   row,
			score: columnVectorGraphDeep1BScoreRow(query, queryInvNorm, vectors, invNorms, row, dims),
		})
	}
	rowIDs := make([]int, 0, targetRows)
	for len(rowIDs) < targetRows && candidates.Len() > 0 {
		candidate := heap.Pop(&candidates).(columnVectorGraphDeep1BGraphVisitCandidate)
		row := candidate.row
		if row < 0 || row >= len(routing.adjacency) || expanded[row] {
			continue
		}
		expanded[row] = true
		rowIDs = append(rowIDs, row)
		for _, neighbor := range routing.adjacency[row] {
			if neighbor < 0 || neighbor >= len(routing.adjacency) || expanded[neighbor] || queued[neighbor] {
				continue
			}
			queued[neighbor] = true
			heap.Push(&candidates, columnVectorGraphDeep1BGraphVisitCandidate{
				row:   neighbor,
				score: columnVectorGraphDeep1BScoreRow(query, queryInvNorm, vectors, invNorms, neighbor, dims),
			})
		}
	}
	if len(rowIDs) == 0 {
		return nil
	}
	firstRow, _ := columnVectorGraphDeep1BRowIDRange(rowIDs)
	centroid := columnVectorGraphDeep1BCentroidForRowIDs(vectors, rowIDs, dims)
	centroidInv := float32(columnVectorGraphDeep1BInvNorm(centroid))
	var dot float32
	for j := 0; j < dims; j++ {
		dot += query[j] * centroid[j]
	}
	return []columnVectorGraphDeep1BBuildableGranule{{
		Ordinal:       0,
		Builder:       builder,
		FirstRow:      firstRow,
		Rows:          len(rowIDs),
		RowIDs:        rowIDs,
		Centroid:      centroid,
		CentroidInv:   centroidInv,
		CentroidScore: float64(dot * queryInvNorm * centroidInv),
	}}
}

type columnVectorGraphDeep1BGraphVisitCandidate struct {
	row   int
	score float32
}

type columnVectorGraphDeep1BGraphVisitHeap []columnVectorGraphDeep1BGraphVisitCandidate

func (h columnVectorGraphDeep1BGraphVisitHeap) Len() int { return len(h) }

func (h columnVectorGraphDeep1BGraphVisitHeap) Less(i, j int) bool {
	if h[i].score == h[j].score {
		return h[i].row < h[j].row
	}
	return h[i].score > h[j].score
}

func (h columnVectorGraphDeep1BGraphVisitHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *columnVectorGraphDeep1BGraphVisitHeap) Push(x any) {
	*h = append(*h, x.(columnVectorGraphDeep1BGraphVisitCandidate))
}

func (h *columnVectorGraphDeep1BGraphVisitHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func columnVectorGraphDeep1BRankRawCentroidsByQuery(query []float32, queryInvNorm float32, centroids []float32, centroidInvNorms []float32, dims int) []int {
	clusterCount := len(centroidInvNorms)
	order := make([]int, clusterCount)
	scores := make([]float32, clusterCount)
	for cluster := 0; cluster < clusterCount; cluster++ {
		order[cluster] = cluster
		centroid := centroids[cluster*dims : (cluster+1)*dims]
		var dot float32
		for j := 0; j < dims; j++ {
			dot += query[j] * centroid[j]
		}
		scores[cluster] = dot * queryInvNorm * centroidInvNorms[cluster]
	}
	sort.Slice(order, func(i, j int) bool {
		left := order[i]
		right := order[j]
		if scores[left] == scores[right] {
			return left < right
		}
		return scores[left] > scores[right]
	})
	return order
}

func columnVectorGraphDeep1BScoreRow(query []float32, queryInvNorm float32, vectors []float32, invNorms []float32, row int, dims int) float32 {
	vector := vectors[row*dims : (row+1)*dims]
	var dot float32
	for j := 0; j < dims; j++ {
		dot += query[j] * vector[j]
	}
	return dot * queryInvNorm * invNorms[row]
}

func columnVectorGraphDeep1BSelectedWithSelectionBuilder(selected []columnVectorGraphDeep1BBuildableGranule, selection string) []columnVectorGraphDeep1BBuildableGranule {
	if selection == "centroid_blocks" {
		return selected
	}
	out := append([]columnVectorGraphDeep1BBuildableGranule(nil), selected...)
	for i := range out {
		out[i].Builder = out[i].Builder + "_" + selection
	}
	return out
}

func columnVectorGraphDeep1BAnalyzeBuildableGranuleSelection(tb testing.TB, vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, globalScores []float32, globalTopRows []int, selected []columnVectorGraphDeep1BBuildableGranule, queryIndex int, topGranules int, ranks []int, pqModels []columnVectorGraphDeep1BPQModel, localResidualPQModels columnVectorGraphDeep1BLocalResidualPQModels, localOPQModels columnVectorGraphDeep1BLocalResidualPQModels, scanIters int) columnVectorGraphDeep1BBuildableGranuleQueryReport {
	tb.Helper()
	dims := columnVectorGraphDeep1BDims
	candidateRows := 0
	for _, granule := range selected {
		candidateRows += granule.Rows
	}
	candidateExact := make([]float32, 0, candidateRows)
	candidateSet := make(map[int]struct{}, candidateRows)
	selectedReports := make([]columnVectorGraphDeep1BSelectedGranuleReport, 0, len(selected))
	for _, granule := range selected {
		rowIDMin, rowIDMax := columnVectorGraphDeep1BRowIDRange(granule.RowIDs)
		report := columnVectorGraphDeep1BSelectedGranuleReport{
			Ordinal:        granule.Ordinal,
			FirstRow:       granule.FirstRow,
			RowIDMin:       rowIDMin,
			RowIDMax:       rowIDMax,
			Rows:           granule.Rows,
			CentroidCosine: granule.CentroidScore,
		}
		for _, row := range granule.RowIDs {
			candidateSet[row] = struct{}{}
			candidateExact = append(candidateExact, globalScores[row])
		}
		report.GlobalTop10Overlap = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 10, columnVectorGraphDeep1BRowIDSet(granule.RowIDs))
		report.GlobalTop20Overlap = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 20, columnVectorGraphDeep1BRowIDSet(granule.RowIDs))
		selectedReports = append(selectedReports, report)
	}
	builder := "unknown"
	if len(selected) > 0 {
		builder = selected[0].Builder
	}
	dynamicVisitedRows := strings.Contains(builder, "graph_visited_rows")
	notes := "buildable granule scout; codec recalls are conditional on the routed candidate union, while routing recalls measure how many global exact winners reached that union"
	if dynamicVisitedRows {
		notes = "dynamic graph visited-row scout; global scalar/PQ codec recalls are conditional on the graph-visited row union. Local PCA and local per-granule codebooks are intentionally skipped because this selection is not a sealed storage granule and would otherwise become a query-time/oracle fit."
	}
	q := columnVectorGraphDeep1BBuildableGranuleQueryReport{
		QueryIndex:            queryIndex,
		Builder:               builder,
		TopGranules:           topGranules,
		CandidateRows:         candidateRows,
		SelectedGranules:      selectedReports,
		CandidateExactMargins: columnVectorGraphDeep1BScoreMarginMetrics(candidateExact),
		Notes:                 notes,
	}
	q.RoutingTop10InCandidates = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 10, candidateSet)
	q.RoutingTop20InCandidates = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 20, candidateSet)
	q.RoutingTop50InCandidates = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 50, candidateSet)
	q.RoutingRecallAt10 = float64(q.RoutingTop10InCandidates) / float64(min(10, len(globalTopRows)))
	q.RoutingRecallAt20 = float64(q.RoutingTop20InCandidates) / float64(min(20, len(globalTopRows)))
	q.RoutingRecallAt50 = float64(q.RoutingTop50InCandidates) / float64(min(50, len(globalTopRows)))
	rowIDs := columnVectorGraphDeep1BSelectedRowIDs(selected)
	exactFP32RerankNanosPerVector := columnVectorGraphDeep1BMeasureBuildableExactFP32Rerank(vectors, invNorms, query, queryInvNorm, rowIDs, scanIters)
	fullInt8Scores, fullInt8RerankNanosPerVector, fullInt8RerankBytesPerVector := columnVectorGraphDeep1BScoreBuildableFullInt8Rerank(vectors, invNorms, query, queryInvNorm, rowIDs, scanIters)
	q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildableScalarMethod(vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, 8, "per_dim", "reconstructed", scanIters))
	q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildableScalarMethod(vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, 4, "per_dim", "reconstructed", scanIters))
	for _, model := range pqModels {
		q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildablePQMethod(vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, model, scanIters))
	}
	if !dynamicVisitedRows {
		for _, budget := range localResidualPQModels.budgets {
			q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildableLocalCodebookMethod(tb, vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, localResidualPQModels, budget, scanIters))
		}
		for _, budget := range localOPQModels.budgets {
			q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildableLocalCodebookMethod(tb, vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, localOPQModels, budget, scanIters))
		}
		minSelectedRows := candidateRows
		for _, granule := range selected {
			minSelectedRows = min(minSelectedRows, granule.Rows)
		}
		for _, rank := range columnVectorGraphDeep1BFilterRanksForRows(tb, ranks, minSelectedRows, dims) {
			q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildablePCAMethod(tb, vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, rank, scanIters))
		}
	}
	for i := range q.Methods {
		columnVectorGraphDeep1BSetCascadeFP32RerankEstimate(&q.Methods[i], candidateRows, dims, exactFP32RerankNanosPerVector)
		columnVectorGraphDeep1BSetCascadeInt8RerankEstimate(&q.Methods[i], candidateRows, fullInt8RerankNanosPerVector, fullInt8RerankBytesPerVector)
		columnVectorGraphDeep1BSetFullInt8RerankRecall(&q.Methods[i], candidateExact, q.Methods[i].ApproxScores, fullInt8Scores)
	}
	return q
}

func columnVectorGraphDeep1BMeasureBuildableExactFP32Rerank(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, rowIDs []int, scanIters int) float64 {
	rows := min(100, len(rowIDs))
	if rows <= 0 {
		return 0
	}
	dims := columnVectorGraphDeep1BDims
	return columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
		for i := 0; i < rows; i++ {
			rowID := rowIDs[i]
			base := rowID * dims
			var dot float32
			for j := 0; j < dims; j++ {
				dot += query[j] * vectors[base+j]
			}
			dst[i] = dot * queryInvNorm * invNorms[rowID]
		}
	})
}

func columnVectorGraphDeep1BScoreBuildableFullInt8Rerank(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, rowIDs []int, scanIters int) ([]float32, float64, float64) {
	rows := len(rowIDs)
	if rows <= 0 {
		return nil, 0, 0
	}
	dims := columnVectorGraphDeep1BDims
	gVectors, gInvNorms := columnVectorGraphDeep1BGatherRows(vectors, invNorms, rowIDs, dims)
	encoding := columnVectorGraphDeep1BEncodeGroundtruthScalar(gVectors, gInvNorms, rows, dims, 8, "per_dim", "reconstructed")
	scores := make([]float32, rows)
	columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, query, queryInvNorm, rows, dims, scores)
	nanosPerVector := columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
		columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, query, queryInvNorm, rows, dims, dst)
	})
	return scores, nanosPerVector, encoding.rowCodeBytesPerVector + encoding.metadataBytesPerVector
}

func columnVectorGraphDeep1BEvaluateBuildableScalarMethod(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, selected []columnVectorGraphDeep1BBuildableGranule, builder string, bits int, policy string, normMode string, scanIters int) columnVectorGraphDeep1BGroundtruthMethodReport {
	dims := columnVectorGraphDeep1BDims
	approxScores := make([]float32, 0, len(exactScores))
	var totalRows int
	var rowCodeBytes float64
	var metadataBytes float64
	var buildNanos int64
	var scanNanos float64
	var meanRelL2 float64
	var maxRelL2 float64
	for _, granule := range selected {
		gVectors, gInvNorms := columnVectorGraphDeep1BGatherRows(vectors, invNorms, granule.RowIDs, dims)
		start := time.Now()
		encoding := columnVectorGraphDeep1BEncodeGroundtruthScalar(gVectors, gInvNorms, granule.Rows, dims, bits, policy, normMode)
		buildNanos += time.Since(start).Nanoseconds()
		gApprox := make([]float32, granule.Rows)
		columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, query, queryInvNorm, granule.Rows, dims, gApprox)
		approxScores = append(approxScores, gApprox...)
		scanNanos += columnVectorGraphDeep1BMeasureGroundtruthScan(granule.Rows, scanIters, func(dst []float32) {
			columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, query, queryInvNorm, granule.Rows, dims, dst)
		}) * float64(granule.Rows)
		rowCodeBytes += encoding.rowCodeBytesPerVector * float64(granule.Rows)
		metadataBytes += encoding.metadataBytesPerVector * float64(granule.Rows)
		meanRelL2 += encoding.meanRelativeL2 * float64(granule.Rows)
		maxRelL2 = math.Max(maxRelL2, encoding.maxRelativeL2)
		totalRows += granule.Rows
	}
	if totalRows == 0 {
		totalRows = 1
	}
	method := columnVectorGraphDeep1BNewCompressionMethodReport(
		"buildable_granule_scout",
		"metadata_amortized_over_selected_buildable_granules",
		"scalar_quantization",
		fmt.Sprintf("buildable_%s_scalar_u%d_affine_%s_%s", builder, bits, policy, normMode),
		rowCodeBytes/float64(totalRows),
		metadataBytes/float64(totalRows),
		buildNanos,
		columnVectorGraphDeep1BBuildableScalarMethodNotes(builder),
	)
	method.ScanNanosPerVector = scanNanos / float64(totalRows)
	method.MeanRelativeL2 = meanRelL2 / float64(totalRows)
	method.MaxRelativeL2 = maxRelL2
	columnVectorGraphDeep1BSetEstimatedCandidateBytesRead(&method, totalRows)
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approxScores, margins)
	columnVectorGraphDeep1BSetApproxTopKSelectionTimings(&method, approxScores, scanIters)
	return method
}

func columnVectorGraphDeep1BBuildableScalarMethodNotes(builder string) string {
	if strings.Contains(builder, "graph_visited_rows") {
		return fmt.Sprintf("dynamic graph visited-row scout over %s; scalar metadata is amortized over the dynamic candidate row set, and codec recall is conditional on graph routing", builder)
	}
	return fmt.Sprintf("production/buildable scout over %s granules; codec recall is conditional on centroid-routed candidate union", builder)
}

func columnVectorGraphDeep1BEvaluateBuildablePCAMethod(tb testing.TB, vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, selected []columnVectorGraphDeep1BBuildableGranule, builder string, rank int, scanIters int) columnVectorGraphDeep1BGroundtruthMethodReport {
	tb.Helper()
	dims := columnVectorGraphDeep1BDims
	approxScores := make([]float32, 0, len(exactScores))
	var totalRows int
	var metadataBytes float64
	var buildNanos int64
	var scanNanos float64
	var meanRelL2 float64
	var maxRelL2 float64
	for _, granule := range selected {
		validRanks := columnVectorGraphDeep1BFilterRanksForRows(tb, []int{rank}, granule.Rows, dims)
		if validRanks[0] != rank {
			tb.Fatalf("rank=%d is not valid for selected granule rows=%d", rank, granule.Rows)
		}
		gVectors, gInvNorms := columnVectorGraphDeep1BGatherRows(vectors, invNorms, granule.RowIDs, dims)
		buildStart := time.Now()
		model := columnVectorGraphDeep1BFitLocalPCAModel(tb, gVectors, granule.Rows, dims, []int{rank})
		encoding := columnVectorGraphDeep1BEncodeLocalPCARank(gVectors, gInvNorms, model, rank, query, queryInvNorm, granule.Rows, dims)
		buildNanos += time.Since(buildStart).Nanoseconds()
		approxScores = append(approxScores, encoding.approxScores...)
		scorer := columnVectorGraphDeep1BPrepareLocalPCAScorer(model, encoding, query, granule.Rows, dims)
		scanNanos += columnVectorGraphDeep1BMeasureGroundtruthScan(granule.Rows, scanIters, func(dst []float32) {
			scorer.scoreInto(encoding, queryInvNorm, granule.Rows, dst)
		}) * float64(granule.Rows)
		metadataBytes += float64(model.centroidMetaBytes + rank*dims*2 + rank*2 + granule.Rows*2)
		meanRelL2 += encoding.meanRelativeL2 * float64(granule.Rows)
		maxRelL2 = math.Max(maxRelL2, encoding.maxRelativeL2)
		totalRows += granule.Rows
	}
	if totalRows == 0 {
		totalRows = 1
	}
	method := columnVectorGraphDeep1BNewCompressionMethodReport(
		"buildable_granule_scout",
		"metadata_amortized_over_selected_buildable_granules",
		"local_pca",
		fmt.Sprintf("buildable_%s_local_pca_i8_rank%d", builder, rank),
		float64(rank),
		metadataBytes/float64(totalRows),
		buildNanos,
		fmt.Sprintf("production/buildable scout over %s granules; local PCA metadata is amortized over each selected granule, and codec recall is conditional on centroid-routed candidate union", builder),
	)
	method.ScanNanosPerVector = scanNanos / float64(totalRows)
	method.MeanRelativeL2 = meanRelL2 / float64(totalRows)
	method.MaxRelativeL2 = maxRelL2
	columnVectorGraphDeep1BSetEstimatedCandidateBytesRead(&method, totalRows)
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approxScores, margins)
	columnVectorGraphDeep1BSetApproxTopKSelectionTimings(&method, approxScores, scanIters)
	return method
}

func columnVectorGraphDeep1BEvaluateBuildableLocalCodebookMethod(tb testing.TB, vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, selected []columnVectorGraphDeep1BBuildableGranule, builder string, models columnVectorGraphDeep1BLocalResidualPQModels, rowCodeBytes int, scanIters int) columnVectorGraphDeep1BGroundtruthMethodReport {
	tb.Helper()
	dims := columnVectorGraphDeep1BDims
	approxScores := make([]float32, 0, len(exactScores))
	var totalRows int
	var rowCodeBytesTotal float64
	var metadataBytes float64
	var buildNanos int64
	var scanNanos float64
	var meanRelL2 float64
	var maxRelL2 float64
	methodName := ""
	family := ""
	for _, granule := range selected {
		gVectors, gInvNorms := columnVectorGraphDeep1BGatherRows(vectors, invNorms, granule.RowIDs, dims)
		localRows := columnVectorGraphDeep1BSequentialRowIDs(granule.Rows)
		granuleModels := models.byGranule[granule.Ordinal]
		model, ok := granuleModels[rowCodeBytes]
		if !ok {
			tb.Fatalf("missing local codebook model for granule=%d row-code budget=%d", granule.Ordinal, rowCodeBytes)
		}
		if methodName == "" {
			methodName = model.method
			family = model.family
		}
		buildStart := time.Now()
		encoding := columnVectorGraphDeep1BEncodePQRows(gVectors, gInvNorms, localRows, model, dims)
		buildNanos += time.Since(buildStart).Nanoseconds()
		scorer := columnVectorGraphDeep1BPreparePQScorer(model, query)
		scorer.scoreInto(encoding, queryInvNorm, granule.Rows, encoding.approxScores)
		approxScores = append(approxScores, encoding.approxScores...)
		scanNanos += columnVectorGraphDeep1BMeasureGroundtruthScan(granule.Rows, scanIters, func(dst []float32) {
			scorer.scoreInto(encoding, queryInvNorm, granule.Rows, dst)
		}) * float64(granule.Rows)
		rowCodeBytesTotal += float64(rowCodeBytes * granule.Rows)
		metadataBytes += float64(model.codebookMetadataBytes + granule.Rows*2)
		meanRelL2 += encoding.meanRelativeL2 * float64(granule.Rows)
		maxRelL2 = math.Max(maxRelL2, encoding.maxRelativeL2)
		totalRows += granule.Rows
	}
	if methodName == "" {
		methodName = "local_residual_pq"
		family = "local_residual_product_quantization"
	}
	if totalRows == 0 {
		totalRows = 1
	}
	method := columnVectorGraphDeep1BNewCompressionMethodReport(
		"buildable_granule_scout",
		columnVectorGraphDeep1BLocalCodebookStorageNotes(methodName),
		family,
		fmt.Sprintf("buildable_%s_%s_%dB_x8", builder, methodName, rowCodeBytes),
		rowCodeBytesTotal/float64(totalRows),
		metadataBytes/float64(totalRows),
		buildNanos,
		columnVectorGraphDeep1BLocalCodebookMethodNotes(builder, methodName),
	)
	method.ScanNanosPerVector = scanNanos / float64(totalRows)
	method.MeanRelativeL2 = meanRelL2 / float64(totalRows)
	method.MaxRelativeL2 = maxRelL2
	columnVectorGraphDeep1BSetEstimatedCandidateBytesRead(&method, totalRows)
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approxScores, margins)
	columnVectorGraphDeep1BSetApproxTopKSelectionTimings(&method, approxScores, scanIters)
	return method
}

func columnVectorGraphDeep1BLocalCodebookStorageNotes(method string) string {
	if method == "local_residual_opq" {
		return "local_f16_centroid_opq_rotation_and_residual_opq_codebooks_amortized_over_selected_buildable_granules_plus_f16_inv_norm_per_row"
	}
	return "local_f16_centroid_and_residual_pq_codebooks_amortized_over_selected_buildable_granules_plus_f16_inv_norm_per_row"
}

func columnVectorGraphDeep1BLocalCodebookMethodNotes(builder string, method string) string {
	if method == "local_residual_opq" {
		return fmt.Sprintf("production/buildable scout over %s granules; local residual OPQ rotations and PQ codebooks were prefitted per buildable granule on the rows they encode; this is a LOPQ-style lane, and codec recall is conditional on centroid-routed candidate union", builder)
	}
	return fmt.Sprintf("production/buildable scout over %s granules; local residual PQ codebooks were prefitted per buildable granule on the rows they encode; this is LOPQ-lite without OPQ rotation, and codec recall is conditional on centroid-routed candidate union", builder)
}

func columnVectorGraphDeep1BSequentialRowIDs(rows int) []int {
	rowIDs := make([]int, rows)
	for row := range rowIDs {
		rowIDs[row] = row
	}
	return rowIDs
}

func columnVectorGraphDeep1BFilterPQBudgetsForDims(tb testing.TB, label string, budgets []int, dims int) []int {
	tb.Helper()
	if len(budgets) == 0 {
		return nil
	}
	seen := make(map[int]bool, len(budgets))
	out := make([]int, 0, len(budgets))
	for _, budget := range budgets {
		if budget <= 0 {
			tb.Fatalf("%s row-code budget=%d must be positive", label, budget)
		}
		if budget > dims {
			tb.Fatalf("%s row-code budget=%d exceeds dims=%d; this scout uses one 8-bit subcode per subquantizer", label, budget, dims)
		}
		if !seen[budget] {
			seen[budget] = true
			out = append(out, budget)
		}
	}
	sort.Ints(out)
	return out
}

func columnVectorGraphDeep1BGatherRows(vectors []float32, invNorms []float32, rowIDs []int, dims int) ([]float32, []float32) {
	gathered := make([]float32, len(rowIDs)*dims)
	gatheredInvNorms := make([]float32, len(rowIDs))
	for i, rowID := range rowIDs {
		copy(gathered[i*dims:(i+1)*dims], vectors[rowID*dims:(rowID+1)*dims])
		gatheredInvNorms[i] = invNorms[rowID]
	}
	return gathered, gatheredInvNorms
}

func columnVectorGraphDeep1BCountRowsInSet(rows []int, topK int, set map[int]struct{}) int {
	topK = min(topK, len(rows))
	var count int
	for _, row := range rows[:topK] {
		if _, ok := set[row]; ok {
			count++
		}
	}
	return count
}

func columnVectorGraphDeep1BRowIDSet(rowIDs []int) map[int]struct{} {
	set := make(map[int]struct{}, len(rowIDs))
	for _, rowID := range rowIDs {
		set[rowID] = struct{}{}
	}
	return set
}

func columnVectorGraphDeep1BRowIDRange(rowIDs []int) (int, int) {
	if len(rowIDs) == 0 {
		return 0, 0
	}
	minRow := rowIDs[0]
	maxRow := rowIDs[0]
	for _, rowID := range rowIDs[1:] {
		if rowID < minRow {
			minRow = rowID
		}
		if rowID > maxRow {
			maxRow = rowID
		}
	}
	return minRow, maxRow
}

func columnVectorGraphDeep1BRenderBuildableGranuleScoutMarkdown(report columnVectorGraphDeep1BBuildableGranuleScoutReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Deep1B Buildable Granule Scout\n\n")
	fmt.Fprintf(&b, "Generated: `%s`\n\n", report.GeneratedAt)
	fmt.Fprintf(&b, "- Regime: `buildable_granule_scout`\n")
	fmt.Fprintf(&b, "- Builder: `%s`\n", report.Builder)
	fmt.Fprintf(&b, "- Base path: `%s`\n", report.BasePath)
	fmt.Fprintf(&b, "- Eval rows: `%d`\n", report.BaseRows)
	if report.EvalRowOffset > 0 {
		fmt.Fprintf(&b, "- Eval row offset: `%d` (rows before this offset are held out for training)\n", report.EvalRowOffset)
	}
	fmt.Fprintf(&b, "- Dims: `%d`\n", report.Dims)
	fmt.Fprintf(&b, "- Granule rows: `%d`\n", report.GranuleRows)
	fmt.Fprintf(&b, "- Granules: `%d`\n", report.GranuleCount)
	fmt.Fprintf(&b, "- Granule build ms: `%.3f`\n", float64(report.GranuleBuildNanos)/1e6)
	fmt.Fprintf(&b, "- Selection: `%s`\n", report.Selection)
	if report.RoutingBuildNanos > 0 {
		fmt.Fprintf(&b, "- Routing graph build ms: `%.3f`\n", float64(report.RoutingBuildNanos)/1e6)
	}
	if columnVectorGraphDeep1BBuilderUsesKMeans(report.Builder) {
		fmt.Fprintf(&b, "- K-means iterations: `%d`\n", report.KMeansIters)
	}
	if columnVectorGraphDeep1BBuilderUsesGraph(report.Builder) {
		fmt.Fprintf(&b, "- Graph degree: `%d`\n", report.GraphDegree)
	}
	if columnVectorGraphDeep1BIsGraphVisitedSelection(report.Selection) {
		fmt.Fprintf(&b, "- Graph entry clusters: `%d`\n", report.GraphEntryClusters)
	}
	if len(report.PQTraining) > 0 {
		fmt.Fprintf(&b, "- PQ train rows: `%d`\n", report.PQTrainRows)
		fmt.Fprintf(&b, "- PQ k-means iterations: `%d`\n", report.PQTrainIters)
		if len(report.PQBytes) > 0 {
			fmt.Fprintf(&b, "- PQ row-code byte budgets: `%v`\n", report.PQBytes)
		}
		if len(report.ResidualPQBytes) > 0 {
			fmt.Fprintf(&b, "- Residual PQ row-code byte budgets: `%v`\n", report.ResidualPQBytes)
		}
		if len(report.OPQBytes) > 0 {
			fmt.Fprintf(&b, "- OPQ row-code byte budgets: `%v`\n", report.OPQBytes)
			fmt.Fprintf(&b, "- OPQ outer iterations: `%d`\n", report.OPQIterations)
		}
	}
	if len(report.LocalResidualPQBytes) > 0 {
		fmt.Fprintf(&b, "- Local residual PQ row-code byte budgets: `%v`\n", report.LocalResidualPQBytes)
		fmt.Fprintf(&b, "- Local residual PQ k-means iterations: `%d`\n", report.PQTrainIters)
	}
	if len(report.LocalOPQBytes) > 0 {
		fmt.Fprintf(&b, "- Local OPQ row-code byte budgets: `%v`\n", report.LocalOPQBytes)
		fmt.Fprintf(&b, "- Local OPQ PQ k-means iterations: `%d`\n", report.PQTrainIters)
		fmt.Fprintf(&b, "- Local OPQ outer iterations: `%d`\n", report.OPQIterations)
	}
	fmt.Fprintf(&b, "- Scan iterations: `%d`\n\n", report.ScanIters)
	fmt.Fprintf(&b, "%s\n\n", columnVectorGraphDeep1BBuildableBuilderMarkdown(report))
	if report.Selection == "graph_visited_blocks" {
		graphKind := "IVF-window"
		if report.Builder == "ivf_exact_graph_sorted_blocks" {
			graphKind = "exact in-cluster kNN"
		}
		fmt.Fprintf(&b, "Selection mode `graph_visited_blocks` is a query-time graph-routing scout: it starts from static IVF centroid entry rows, greedily expands the query-independent %s row graph using exact query-to-row scores, then reads the sealed graph-sorted storage blocks containing the visited rows. This is closer to an actual graph visited-set route than centroid-ranked block selection, but it is still not a full production HNSW/TreeDB graph implementation.\n\n", graphKind)
	} else if report.Selection == "graph_visited_rows" {
		graphKind := "IVF-window"
		if report.Builder == "ivf_exact_graph_sorted_blocks" {
			graphKind = "exact in-cluster kNN"
		}
		fmt.Fprintf(&b, "Selection mode `graph_visited_rows` is a dynamic graph-routing scout: it starts from static IVF centroid entry rows, greedily expands the query-independent %s row graph using exact query-to-row scores, and evaluates only the visited rows. This tests actual visited-set quality before storage-block expansion. It is not a sealed TreeDB granule proof; local per-granule PCA and local codebook lanes are intentionally skipped for this mode because fitting them on the dynamic visited set would be a query-time/oracle fit.\n\n", graphKind)
	}
	if strings.Contains(report.Builder, "exact_graph") {
		fmt.Fprintf(&b, "The exact in-cluster graph builders are intentionally stronger research proxies: they build exact kNN adjacency inside each IVF cluster, so build cost is expected to scale roughly with the sum of squared cluster sizes times dimension. Treat their build timing as scout evidence, not as a proposed production graph-construction path.\n\n")
	}
	if len(report.PQTraining) > 0 {
		fmt.Fprintf(&b, "## PQ/OPQ/Residual-PQ Training\n\n")
		fmt.Fprintf(&b, "These are global 8-bit PQ-family codebooks trained on held-out base-prefix rows and evaluated on a disjoint eval slice. They are production/buildable codebook lanes, not official top100 oracle fits. Codebook metadata is counted as f16 centroids, f16 residual centroids for residual PQ, and f16 rotation metadata for OPQ, amortized over all eval rows; per-row f16 inverse norms are counted in method metadata.\n\n")
		fmt.Fprintf(&b, "| Method | Row-code B/vector | Subquantizers | Codebook size | Train rows | PQ train iters | OPQ outer iters | Train ms | Codebook metadata B | Codebook metadata B/eval-vector |\n")
		fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		for _, training := range report.PQTraining {
			fmt.Fprintf(&b, "| `%s` | %d | %d | %d | %d | %d | %d | %.3f | %d | %.3f |\n",
				training.Method,
				training.RowCodeBytes,
				training.Subquantizers,
				training.CodebookSize,
				training.TrainRows,
				training.TrainIterations,
				training.OPQIterations,
				float64(training.TrainNanos)/1e6,
				training.CodebookMetadataBytes,
				training.CodebookMetadataBytesPerEval,
			)
		}
		fmt.Fprintf(&b, "\n")
	}
	if len(report.LocalResidualPQTraining) > 0 {
		fmt.Fprintf(&b, "## Local Residual-PQ Training\n\n")
		fmt.Fprintf(&b, "These are per-buildable-granule residual PQ codebooks trained once for each sealed granule, then reused by every query that routes to that granule. This is a production/buildable LOPQ-lite lane, not an official top100 oracle fit and not full OPQ/LOPQ. Metadata is counted as one f16 residual centroid plus f16 residual-PQ codebooks per granule, amortized over eval rows in this training table and over selected candidate rows in method rows; per-row f16 inverse norms are counted in method metadata.\n\n")
		fmt.Fprintf(&b, "| Method | Row-code B/vector | Subquantizers | Codebook size | Granule train rows | PQ train iters | Train ms | Codebook metadata B | Codebook metadata B/eval-vector |\n")
		fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		for _, training := range report.LocalResidualPQTraining {
			fmt.Fprintf(&b, "| `%s` | %d | %d | %d | %d | %d | %.3f | %d | %.3f |\n",
				training.Method,
				training.RowCodeBytes,
				training.Subquantizers,
				training.CodebookSize,
				training.TrainRows,
				training.TrainIterations,
				float64(training.TrainNanos)/1e6,
				training.CodebookMetadataBytes,
				training.CodebookMetadataBytesPerEval,
			)
		}
		fmt.Fprintf(&b, "\n")
	}
	if len(report.LocalOPQTraining) > 0 {
		fmt.Fprintf(&b, "## Local OPQ/LOPQ-Style Training\n\n")
		fmt.Fprintf(&b, "These are per-buildable-granule residual OPQ rotations plus PQ codebooks trained once for each sealed granule, then reused by every query that routes to that granule. This is a production/buildable local-codebook lane and is closer to LOPQ than local residual-PQ, but it is still a scout implementation rather than a production codec. Metadata is counted as one f16 residual centroid, one f16 OPQ rotation, and f16 residual-OPQ/PQ codebooks per granule, amortized over eval rows in this training table and over selected candidate rows in method rows; per-row f16 inverse norms are counted in method metadata.\n\n")
		fmt.Fprintf(&b, "| Method | Row-code B/vector | Subquantizers | Codebook size | Granule train rows | PQ train iters | OPQ outer iters | Train ms | Codebook metadata B | Codebook metadata B/eval-vector |\n")
		fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		for _, training := range report.LocalOPQTraining {
			fmt.Fprintf(&b, "| `%s` | %d | %d | %d | %d | %d | %d | %.3f | %d | %.3f |\n",
				training.Method,
				training.RowCodeBytes,
				training.Subquantizers,
				training.CodebookSize,
				training.TrainRows,
				training.TrainIterations,
				training.OPQIterations,
				float64(training.TrainNanos)/1e6,
				training.CodebookMetadataBytes,
				training.CodebookMetadataBytesPerEval,
			)
		}
		fmt.Fprintf(&b, "\n")
	}
	columnVectorGraphDeep1BRenderBuildableRoutingAggregateMarkdown(&b, report)
	fmt.Fprintf(&b, "## Routing\n\n")
	fmt.Fprintf(&b, "| Query | Builder | Top granules | Candidate rows | Global top10 routed | Global top20 routed | Global top50 routed |\n")
	fmt.Fprintf(&b, "| ---: | --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		fmt.Fprintf(&b, "| %d | `%s` | %d | %d | %d/10 | %d/20 | %d/50 |\n",
			q.QueryIndex,
			q.Builder,
			q.TopGranules,
			q.CandidateRows,
			q.RoutingTop10InCandidates,
			q.RoutingTop20InCandidates,
			q.RoutingTop50InCandidates,
		)
	}
	fmt.Fprintf(&b, "\n## Conditional Codec Results\n\n")
	fmt.Fprintf(&b, "These codec metrics are conditional on the selected buildable granules. A method can look good here while the routing row above still fails to bring global winners into the candidate set.\n\n")
	fmt.Fprintf(&b, "| Query | Top granules | Method | Row-code B/vector | Metadata B/vector | Build ms | Compressed top10 | Top10 in approx@20 | Top10 in approx@50 | Top10 in approx@100 | Top20 in approx@50 | Top20 in approx@100 | Exact rerank@50 recall@10 | Exact rerank@100 recall@10 | Full-int8 rerank@50 recall@10 | Full-int8 rerank@100 recall@10 | Mean score err | Err/gap10 | Err/gap20 | Err/gap50 | Scan ns/vector | TopK@50 us | FP32 cascade@50 us | FP32 cascade@100 us | Int8 cascade@50 us | Int8 cascade@100 us | FP32 cascade@50 KiB | Int8 cascade@50 KiB |\n")
	fmt.Fprintf(&b, "| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		for _, method := range q.Methods {
			fmt.Fprintf(&b, "| %d | %d | `%s` | %.2f | %.2f | %.3f | %d/10 | %s | %s | %s | %s | %s | %.2f | %.2f | %.2f | %.2f | %.5f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.1f | %.1f |\n",
				q.QueryIndex,
				q.TopGranules,
				method.Name,
				method.RowCodeBytesPerVector,
				method.MetadataBytesPerVector,
				float64(method.BuildNanos)/1e6,
				method.Top10Overlap,
				columnVectorGraphDeep1BFormatOverlap(method.Top10InApproxTop20, method.Top10RecallAt20, 10),
				columnVectorGraphDeep1BFormatOverlap(method.Top10InApproxTop50, method.Top10RecallAt50, 10),
				columnVectorGraphDeep1BFormatOverlap(method.Top10InApproxTop100, method.Top10RecallAt100, 10),
				columnVectorGraphDeep1BFormatOverlap(method.Top20InApproxTop50, method.Top20RecallAt50, 20),
				columnVectorGraphDeep1BFormatOverlap(method.Top20InApproxTop100, method.Top20RecallAt100, 20),
				method.ExactRerankRecallAt10FromTop50,
				method.ExactRerankRecallAt10FromTop100,
				method.FullInt8RerankRecallAt10FromTop50,
				method.FullInt8RerankRecallAt10FromTop100,
				method.MeanScoreError,
				method.MeanErrorOverGap10,
				method.MeanErrorOverGap20,
				method.MeanErrorOverGap50,
				method.ScanNanosPerVector,
				method.ApproxTop50SelectionNanosPerQuery/1e3,
				method.MeasuredCascadeFP32Top50NanosPerQuery/1e3,
				method.MeasuredCascadeFP32Top100NanosPerQuery/1e3,
				method.MeasuredCascadeInt8Top50NanosPerQuery/1e3,
				method.MeasuredCascadeInt8Top100NanosPerQuery/1e3,
				method.CascadeFP32Top50BytesPerQuery/1024,
				method.CascadeInt8Top50BytesPerQuery/1024,
			)
		}
	}
	columnVectorGraphDeep1BRenderBuildableAggregateMarkdown(&b, report)
	fmt.Fprintf(&b, "\n## Selected Granules\n\n")
	fmt.Fprintf(&b, "| Query | Top granules | Granule | First row | Row-id min | Row-id max | Rows | Centroid cos(query) | Global top10 in granule | Global top20 in granule |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		for _, granule := range q.SelectedGranules {
			fmt.Fprintf(&b, "| %d | %d | %d | %d | %d | %d | %d | %.5f | %d/10 | %d/20 |\n",
				q.QueryIndex,
				q.TopGranules,
				granule.Ordinal,
				granule.FirstRow,
				granule.RowIDMin,
				granule.RowIDMax,
				granule.Rows,
				granule.CentroidCosine,
				granule.GlobalTop10Overlap,
				granule.GlobalTop20Overlap,
			)
		}
	}
	return b.String()
}

func columnVectorGraphDeep1BRenderBuildableRoutingAggregateMarkdown(b *strings.Builder, report columnVectorGraphDeep1BBuildableGranuleScoutReport) {
	type routingAggregate struct {
		topGranules   int
		count         int
		candidateRows float64
		top10         []int
		top20         []int
		top50         []int
	}
	byTopGranules := make(map[int]*routingAggregate)
	var keys []int
	for _, q := range report.Queries {
		agg := byTopGranules[q.TopGranules]
		if agg == nil {
			agg = &routingAggregate{topGranules: q.TopGranules}
			byTopGranules[q.TopGranules] = agg
			keys = append(keys, q.TopGranules)
		}
		agg.count++
		agg.candidateRows += float64(q.CandidateRows)
		agg.top10 = append(agg.top10, q.RoutingTop10InCandidates)
		agg.top20 = append(agg.top20, q.RoutingTop20InCandidates)
		agg.top50 = append(agg.top50, q.RoutingTop50InCandidates)
	}
	sort.Ints(keys)
	if len(keys) == 0 {
		return
	}
	fmt.Fprintf(b, "## Aggregate Routing Gates\n\n")
	selectionUnit := "selected buildable blocks"
	if report.Selection == "graph_visited_rows" {
		selectionUnit = "dynamic graph-visited row set"
	}
	fmt.Fprintf(b, "These routing gates are codec-independent. If routing does not bring exact winners into the %s, no conditional codec or exact rerank can recover them. For overlap metrics, p90 is the 90th-percentile success count and worst is the lower-tail query.\n\n", selectionUnit)
	fmt.Fprintf(b, "| Top granules | Queries | Avg candidate rows | p50 top10 routed | p90 top10 routed | worst top10 routed | p50 top20 routed | p90 top20 routed | worst top20 routed | p50 top50 routed | p90 top50 routed | worst top50 routed |\n")
	fmt.Fprintf(b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, key := range keys {
		agg := byTopGranules[key]
		sort.Ints(agg.top10)
		sort.Ints(agg.top20)
		sort.Ints(agg.top50)
		count := float64(max(1, agg.count))
		fmt.Fprintf(b, "| %d | %d | %.1f | %d/10 | %d/10 | %d/10 | %d/20 | %d/20 | %d/20 | %d/50 | %d/50 | %d/50 |\n",
			agg.topGranules,
			agg.count,
			agg.candidateRows/count,
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top20, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top20, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top20, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top50, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top50, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top50, 0),
		)
	}
	fmt.Fprintf(b, "\n")
}

func columnVectorGraphDeep1BBuildableBuilderMarkdown(report columnVectorGraphDeep1BBuildableGranuleScoutReport) string {
	switch report.Builder {
	case "row_id_contiguous":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 || len(report.LocalOPQBytes) > 0 {
			return "This is a **production/buildable granule** scout using `row_id_contiguous` blocks. They are real storage units TreeDB can build without oracle labels, but they are intentionally a weak locality control. The result separates routing/locality failure from codec failure. Global PQ/OPQ/residual-PQ rows use a held-out train/eval split when enabled; local residual PQ rows use prefitted per-granule residual codebooks as a LOPQ-lite lane; local OPQ rows add a per-granule residual OPQ rotation and are the first LOPQ-style scout lane."
		}
		return "This is a **production/buildable granule** scout using `row_id_contiguous` blocks. They are real storage units TreeDB can build without oracle labels, but they are intentionally a weak locality control. The result separates routing/locality failure from codec failure; it should not be read as evidence that row-id order is a good ANN granule builder. PQ/OPQ/residual-PQ/LOPQ are still pending because they require real train/eval splits and trained codebooks."
	case "ivf_kmeans":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 || len(report.LocalOPQBytes) > 0 {
			return "This is a **production/buildable granule** scout using deterministic cosine `ivf_kmeans` clusters trained on the eval slice. It is a buildable locality probe, unlike the official top100 oracle clouds. Global PQ/OPQ/residual-PQ rows use held-out global codebooks when enabled; local residual PQ uses separate prefitted residual codebooks per buildable granule as a LOPQ-lite lane; local OPQ adds a per-granule residual OPQ rotation and is the first LOPQ-style scout lane, with metadata amortized over those granule rows."
		}
		return "This is a **production/buildable granule** scout using deterministic cosine `ivf_kmeans` clusters trained on the base prefix. It is a buildable locality probe, unlike the official top100 oracle clouds, but it is still not a PQ/OPQ/residual-PQ tournament: codebook methods still require separate train/eval discipline and metadata accounting."
	case "ivf_kmeans_sorted_blocks":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 || len(report.LocalOPQBytes) > 0 {
			return "This is a **production/buildable granule** scout using deterministic cosine `ivf_kmeans_sorted_blocks`: rows are assigned to k-means centroids, sorted by assigned centroid locality, and then chunked into fixed-size storage blocks. This is a buildable locality-sorted TreeDB-granule proxy, unlike the official top100 oracle clouds. It is not a graph-neighborhood proof, but it tests whether locality-ordered row-adjacent blocks can support the same compressed-code tournament. Global PQ/OPQ/residual-PQ rows use held-out global codebooks when enabled; local residual PQ and local OPQ train per sealed block with metadata amortized over those block rows."
		}
		return "This is a **production/buildable granule** scout using deterministic cosine `ivf_kmeans_sorted_blocks`: rows are assigned to k-means centroids, sorted by assigned centroid locality, and chunked into fixed-size storage blocks. This is a buildable locality-sorted TreeDB-granule proxy, not an official top100 oracle cloud and not a graph-neighborhood proof."
	case "ivf_graph_neighborhood_blocks":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 || len(report.LocalOPQBytes) > 0 {
			return "This is a **production/buildable granule** scout using deterministic cosine `ivf_graph_neighborhood_blocks`: rows are assigned to k-means centroids, a query-independent local nearest-neighbor graph is built inside IVF-sorted windows, and fixed-size storage blocks are formed by graph BFS. This is a buildable graph-neighborhood proxy, unlike the official top100 oracle clouds. It is still not a full production HNSW/TreeDB graph-visited-set result. Global PQ/OPQ/residual-PQ rows use held-out global codebooks when enabled; local residual PQ and local OPQ train per sealed graph-neighborhood block with metadata amortized over those block rows."
		}
		return "This is a **production/buildable granule** scout using deterministic cosine `ivf_graph_neighborhood_blocks`: rows are assigned to k-means centroids, a query-independent local nearest-neighbor graph is built inside IVF-sorted windows, and fixed-size storage blocks are formed by graph BFS. This is a buildable graph-neighborhood proxy, not an official top100 oracle cloud and not a full production HNSW/TreeDB graph-visited-set result."
	case "ivf_graph_sorted_blocks":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 || len(report.LocalOPQBytes) > 0 {
			return "This is a **production/buildable granule** scout using deterministic cosine `ivf_graph_sorted_blocks`: rows are assigned to k-means centroids, a query-independent local nearest-neighbor graph is built inside IVF-sorted windows, a deterministic graph traversal order is materialized, and adjacent rows in that graph order are chunked into fixed-size storage blocks. This is a buildable graph-sorted row-adjacent TreeDB-granule proxy, unlike the official top100 oracle clouds. It is still not a full production HNSW/TreeDB graph-visited-set result. Global PQ/OPQ/residual-PQ rows use held-out global codebooks when enabled; local residual PQ and local OPQ train per sealed graph-sorted block with metadata amortized over those block rows."
		}
		return "This is a **production/buildable granule** scout using deterministic cosine `ivf_graph_sorted_blocks`: rows are assigned to k-means centroids, a query-independent local nearest-neighbor graph is built inside IVF-sorted windows, a deterministic graph traversal order is materialized, and adjacent rows in that graph order are chunked into fixed-size storage blocks. This is a buildable graph-sorted row-adjacent TreeDB-granule proxy, not an official top100 oracle cloud and not a full production HNSW/TreeDB graph-visited-set result."
	case "treedb_graph_sorted_part_granules":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 || len(report.LocalOPQBytes) > 0 {
			return "This is a **production/buildable TreeDB part-granule** scout using deterministic cosine `treedb_graph_sorted_part_granules`: rows are assigned to k-means centroids, a query-independent local nearest-neighbor graph is built inside IVF-sorted windows, the deterministic graph traversal order is materialized through `BuildColumnPart`, and candidate blocks are derived from `ColumnPart.Descriptor.Granules`. This is closer to actual TreeDB storage granules than hand-chunked block proxies, but it is still not a full production HNSW/TreeDB graph-visited-set result. The part uses uncompressed float32 vector columns for descriptor materialization; compressed-code rows are the tournament payloads being evaluated. Global PQ/OPQ/residual-PQ rows use held-out global codebooks when enabled; local residual PQ and local OPQ train per actual descriptor granule with metadata amortized over those granule rows."
		}
		return "This is a **production/buildable TreeDB part-granule** scout using deterministic cosine `treedb_graph_sorted_part_granules`: rows are assigned to k-means centroids, a query-independent local nearest-neighbor graph is built inside IVF-sorted windows, the deterministic graph traversal order is materialized through `BuildColumnPart`, and candidate blocks are derived from `ColumnPart.Descriptor.Granules`. This is closer to actual TreeDB storage granules than hand-chunked block proxies, not an official top100 oracle cloud and not a full production HNSW/TreeDB graph-visited-set result."
	case "ivf_exact_graph_neighborhood_blocks":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 || len(report.LocalOPQBytes) > 0 {
			return "This is a **production/buildable granule** scout using deterministic cosine `ivf_exact_graph_neighborhood_blocks`: rows are assigned to k-means centroids, an exact in-cluster kNN graph is built over the eval slice, and fixed-size storage blocks are formed by graph BFS. This is a stronger buildable graph-neighborhood proxy than the IVF-window graph, unlike the official top100 oracle clouds. It is still not a full production HNSW/TreeDB graph build. Global PQ/OPQ/residual-PQ rows use held-out global codebooks when enabled; local residual PQ and local OPQ train per sealed exact-graph-neighborhood block with metadata amortized over those block rows."
		}
		return "This is a **production/buildable granule** scout using deterministic cosine `ivf_exact_graph_neighborhood_blocks`: rows are assigned to k-means centroids, an exact in-cluster kNN graph is built over the eval slice, and fixed-size storage blocks are formed by graph BFS. This is a stronger buildable graph-neighborhood proxy than the IVF-window graph, not an official top100 oracle cloud and not a full production HNSW/TreeDB graph build."
	case "ivf_exact_graph_sorted_blocks":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 || len(report.LocalOPQBytes) > 0 {
			return "This is a **production/buildable granule** scout using deterministic cosine `ivf_exact_graph_sorted_blocks`: rows are assigned to k-means centroids, an exact in-cluster kNN graph is built over the eval slice, a deterministic graph traversal order is materialized, and adjacent rows in that graph order are chunked into fixed-size storage blocks. This is a stronger buildable graph-sorted TreeDB-granule proxy than the IVF-window graph, unlike the official top100 oracle clouds. It is still not a full production HNSW/TreeDB graph-visited-set result. Global PQ/OPQ/residual-PQ rows use held-out global codebooks when enabled; local residual PQ and local OPQ train per sealed exact-graph-sorted block with metadata amortized over those block rows."
		}
		return "This is a **production/buildable granule** scout using deterministic cosine `ivf_exact_graph_sorted_blocks`: rows are assigned to k-means centroids, an exact in-cluster kNN graph is built over the eval slice, a deterministic graph traversal order is materialized, and adjacent rows in that graph order are chunked into fixed-size storage blocks. This is a stronger buildable graph-sorted TreeDB-granule proxy than the IVF-window graph, not an official top100 oracle cloud and not a full production HNSW/TreeDB graph-visited-set result."
	default:
		return fmt.Sprintf("This is a **production/buildable granule** scout using `%s`. Codec metrics are conditional on the routed candidate union, and trained-codebook methods still require separate train/eval discipline before production claims.", report.Builder)
	}
}

func columnVectorGraphDeep1BRenderBuildableAggregateMarkdown(b *strings.Builder, report columnVectorGraphDeep1BBuildableGranuleScoutReport) {
	type aggregate struct {
		name            string
		count           int
		rowBytes        float64
		metaBytes       float64
		buildNanos      float64
		scanNanos       float64
		rowRead         float64
		totalRead       float64
		scoreError      float64
		gap10           float64
		gap20           float64
		gap50           float64
		top10           []int
		top10At20       []int
		top10At50       []int
		top10At100      []int
		top20At50       []int
		top20At100      []int
		rerank20        []float64
		rerank50        []float64
		rerank100       []float64
		int8Rerank20    []float64
		int8Rerank50    []float64
		int8Rerank100   []float64
		candidates      []int
		scanQuery       []float64
		exactFP32       float64
		fullInt8        float64
		cascade20       []float64
		cascade50       []float64
		cascade100      []float64
		select50        []float64
		select100       []float64
		measured50      []float64
		measured100     []float64
		int8Measured50  []float64
		int8Measured100 []float64
		bytes20         float64
		bytes50         float64
		bytes100        float64
		int8Bytes50     float64
		int8Bytes100    float64
	}
	byName := make(map[string]*aggregate)
	var names []string
	for _, q := range report.Queries {
		for _, method := range q.Methods {
			key := fmt.Sprintf("top%d/%s", q.TopGranules, method.Name)
			agg := byName[key]
			if agg == nil {
				agg = &aggregate{name: key}
				byName[key] = agg
				names = append(names, key)
			}
			agg.count++
			agg.rowBytes += method.RowCodeBytesPerVector
			agg.metaBytes += method.MetadataBytesPerVector
			agg.buildNanos += float64(method.BuildNanos)
			agg.scanNanos += method.ScanNanosPerVector
			agg.rowRead += method.EstimatedRowCodeBytesPerQuery
			agg.totalRead += method.EstimatedTotalBytesPerQuery
			agg.scoreError += method.MeanScoreError
			agg.gap10 += method.MeanErrorOverGap10
			agg.gap20 += method.MeanErrorOverGap20
			agg.gap50 += method.MeanErrorOverGap50
			agg.top10 = append(agg.top10, method.Top10Overlap)
			agg.top10At20 = append(agg.top10At20, method.Top10InApproxTop20)
			agg.top10At50 = append(agg.top10At50, method.Top10InApproxTop50)
			agg.top10At100 = append(agg.top10At100, method.Top10InApproxTop100)
			agg.top20At50 = append(agg.top20At50, method.Top20InApproxTop50)
			agg.top20At100 = append(agg.top20At100, method.Top20InApproxTop100)
			agg.rerank20 = append(agg.rerank20, method.ExactRerankRecallAt10FromTop20)
			agg.rerank50 = append(agg.rerank50, method.ExactRerankRecallAt10FromTop50)
			agg.rerank100 = append(agg.rerank100, method.ExactRerankRecallAt10FromTop100)
			agg.int8Rerank20 = append(agg.int8Rerank20, method.FullInt8RerankRecallAt10FromTop20)
			agg.int8Rerank50 = append(agg.int8Rerank50, method.FullInt8RerankRecallAt10FromTop50)
			agg.int8Rerank100 = append(agg.int8Rerank100, method.FullInt8RerankRecallAt10FromTop100)
			agg.candidates = append(agg.candidates, method.CandidateRows)
			agg.scanQuery = append(agg.scanQuery, method.CompressedScanNanosPerQuery)
			agg.exactFP32 += method.ExactFP32RerankNanosPerVector
			agg.fullInt8 += method.FullInt8RerankNanosPerVector
			agg.cascade20 = append(agg.cascade20, method.CascadeFP32Top20NanosPerQuery)
			agg.cascade50 = append(agg.cascade50, method.CascadeFP32Top50NanosPerQuery)
			agg.cascade100 = append(agg.cascade100, method.CascadeFP32Top100NanosPerQuery)
			agg.select50 = append(agg.select50, method.ApproxTop50SelectionNanosPerQuery)
			agg.select100 = append(agg.select100, method.ApproxTop100SelectionNanosPerQuery)
			agg.measured50 = append(agg.measured50, method.MeasuredCascadeFP32Top50NanosPerQuery)
			agg.measured100 = append(agg.measured100, method.MeasuredCascadeFP32Top100NanosPerQuery)
			agg.int8Measured50 = append(agg.int8Measured50, method.MeasuredCascadeInt8Top50NanosPerQuery)
			agg.int8Measured100 = append(agg.int8Measured100, method.MeasuredCascadeInt8Top100NanosPerQuery)
			agg.bytes20 += method.CascadeFP32Top20BytesPerQuery
			agg.bytes50 += method.CascadeFP32Top50BytesPerQuery
			agg.bytes100 += method.CascadeFP32Top100BytesPerQuery
			agg.int8Bytes50 += method.CascadeInt8Top50BytesPerQuery
			agg.int8Bytes100 += method.CascadeInt8Top100BytesPerQuery
		}
	}
	sort.Slice(names, func(i, j int) bool {
		left := byName[names[i]]
		right := byName[names[j]]
		leftBytes := left.rowBytes / float64(max(1, left.count))
		rightBytes := right.rowBytes / float64(max(1, right.count))
		if leftBytes != rightBytes {
			return leftBytes < rightBytes
		}
		return left.name < right.name
	})
	fmt.Fprintf(b, "\n## Aggregate Conditional Codec Gates\n\n")
	fmt.Fprintf(b, "For overlap and rerank-recall metrics, p90 is the 90th-percentile success value and worst is the lower-tail query. These codec metrics remain conditional on the routed buildable block union.\n\n")
	fmt.Fprintf(b, "| Method | Queries | Row-code B/vector | Metadata B/vector | Avg row-code KiB/query | Avg total KiB/query | Avg build ms | p50 compressed top10 | p90 compressed top10 | worst compressed top10 | p50 top10@20 | p90 top10@20 | worst top10@20 | p50 top10@50 | p90 top10@50 | worst top10@50 | p50 top10@100 | p90 top10@100 | worst top10@100 | p50 top20@50 | p90 top20@50 | worst top20@50 | p50 top20@100 | p90 top20@100 | worst top20@100 | p50 exact rerank@20 recall@10 | p90 exact rerank@20 recall@10 | worst exact rerank@20 recall@10 | p50 exact rerank@50 recall@10 | p90 exact rerank@50 recall@10 | worst exact rerank@50 recall@10 | p50 exact rerank@100 recall@10 | p90 exact rerank@100 recall@10 | worst exact rerank@100 recall@10 | p50 full-int8 rerank@50 recall@10 | p90 full-int8 rerank@50 recall@10 | worst full-int8 rerank@50 recall@10 | p50 full-int8 rerank@100 recall@10 | p90 full-int8 rerank@100 recall@10 | worst full-int8 rerank@100 recall@10 | Avg score err | Avg err/gap10 | Avg err/gap20 | Avg err/gap50 | Avg scan ns/vector |\n")
	fmt.Fprintf(b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, name := range names {
		agg := byName[name]
		sort.Ints(agg.top10)
		sort.Ints(agg.top10At20)
		sort.Ints(agg.top10At50)
		sort.Ints(agg.top10At100)
		sort.Ints(agg.top20At50)
		sort.Ints(agg.top20At100)
		sort.Float64s(agg.rerank20)
		sort.Float64s(agg.rerank50)
		sort.Float64s(agg.rerank100)
		sort.Float64s(agg.int8Rerank20)
		sort.Float64s(agg.int8Rerank50)
		sort.Float64s(agg.int8Rerank100)
		sort.Ints(agg.candidates)
		sort.Float64s(agg.scanQuery)
		sort.Float64s(agg.cascade20)
		sort.Float64s(agg.cascade50)
		sort.Float64s(agg.cascade100)
		sort.Float64s(agg.select50)
		sort.Float64s(agg.select100)
		sort.Float64s(agg.measured50)
		sort.Float64s(agg.measured100)
		sort.Float64s(agg.int8Measured50)
		sort.Float64s(agg.int8Measured100)
		count := float64(max(1, agg.count))
		fmt.Fprintf(b, "| `%s` | %d | %.2f | %.2f | %.1f | %.1f | %.3f | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/20 | %d/20 | %d/20 | %d/20 | %d/20 | %d/20 | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.5f | %.2f | %.2f | %.2f | %.2f |\n",
			agg.name,
			agg.count,
			agg.rowBytes/count,
			agg.metaBytes/count,
			agg.rowRead/count/1024,
			agg.totalRead/count/1024,
			agg.buildNanos/count/1e6,
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top10At20, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10At20, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top10At20, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top10At50, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10At50, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top10At50, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top10At100, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10At100, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top10At100, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top20At50, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top20At50, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top20At50, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top20At100, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top20At100, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top20At100, 0),
			columnVectorGraphDeep1BFloatQuantile(agg.rerank20, 0.50),
			columnVectorGraphDeep1BFloatQuantile(agg.rerank20, 0.90),
			columnVectorGraphDeep1BFloatQuantile(agg.rerank20, 0),
			columnVectorGraphDeep1BFloatQuantile(agg.rerank50, 0.50),
			columnVectorGraphDeep1BFloatQuantile(agg.rerank50, 0.90),
			columnVectorGraphDeep1BFloatQuantile(agg.rerank50, 0),
			columnVectorGraphDeep1BFloatQuantile(agg.rerank100, 0.50),
			columnVectorGraphDeep1BFloatQuantile(agg.rerank100, 0.90),
			columnVectorGraphDeep1BFloatQuantile(agg.rerank100, 0),
			columnVectorGraphDeep1BFloatQuantile(agg.int8Rerank50, 0.50),
			columnVectorGraphDeep1BFloatQuantile(agg.int8Rerank50, 0.90),
			columnVectorGraphDeep1BFloatQuantile(agg.int8Rerank50, 0),
			columnVectorGraphDeep1BFloatQuantile(agg.int8Rerank100, 0.50),
			columnVectorGraphDeep1BFloatQuantile(agg.int8Rerank100, 0.90),
			columnVectorGraphDeep1BFloatQuantile(agg.int8Rerank100, 0),
			agg.scoreError/count,
			agg.gap10/count,
			agg.gap20/count,
			agg.gap50/count,
			agg.scanNanos/count,
		)
	}
	fmt.Fprintf(b, "\n## Aggregate Cascade Cost Estimates\n\n")
	fmt.Fprintf(b, "These are staged cascade measurements, not full end-to-end query latency. Compressed scan time is measured with each method's score kernel, approximate topK selection is measured over materialized approximate scores, fp32 rerank time is measured with a resident row-id dot scorer, and full-int8 rerank time is measured with a resident full-dim SQ8 scorer. The measured cascade columns add those stages; they still exclude I/O, decompression, cache effects, and fused executor effects. Bytes are selected compressed-code bytes plus either fp32 vector+invNorm bytes or full-int8 row-code+metadata bytes for the reranked shortlist.\n\n")
	fmt.Fprintf(b, "| Method | Queries | p50 candidate rows | p95 candidate rows | Avg selected-code KiB/query | Avg exact-fp32 rerank ns/vector | Avg full-int8 rerank ns/vector | p50 scan us | p95 scan us | p50 topK@50 us | p95 topK@50 us | p50 fp32 cascade@50 us | p95 fp32 cascade@50 us | p50 int8 cascade@50 us | p95 int8 cascade@50 us | Avg fp32 cascade@50 KiB | Avg int8 cascade@50 KiB | p50 topK@100 us | p95 topK@100 us | p50 fp32 cascade@100 us | p95 fp32 cascade@100 us | p50 int8 cascade@100 us | p95 int8 cascade@100 us | Avg fp32 cascade@100 KiB | Avg int8 cascade@100 KiB |\n")
	fmt.Fprintf(b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, name := range names {
		agg := byName[name]
		count := float64(max(1, agg.count))
		fmt.Fprintf(b, "| `%s` | %d | %d | %d | %.1f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.1f | %.1f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.1f | %.1f |\n",
			agg.name,
			agg.count,
			columnVectorGraphDeep1BIntQuantile(agg.candidates, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.candidates, 0.95),
			agg.totalRead/count/1024,
			agg.exactFP32/count,
			agg.fullInt8/count,
			columnVectorGraphDeep1BFloatQuantile(agg.scanQuery, 0.50)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.scanQuery, 0.95)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.select50, 0.50)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.select50, 0.95)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.measured50, 0.50)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.measured50, 0.95)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.int8Measured50, 0.50)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.int8Measured50, 0.95)/1e3,
			agg.bytes50/count/1024,
			agg.int8Bytes50/count/1024,
			columnVectorGraphDeep1BFloatQuantile(agg.select100, 0.50)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.select100, 0.95)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.measured100, 0.50)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.measured100, 0.95)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.int8Measured100, 0.50)/1e3,
			columnVectorGraphDeep1BFloatQuantile(agg.int8Measured100, 0.95)/1e3,
			agg.bytes100/count/1024,
			agg.int8Bytes100/count/1024,
		)
	}
}
