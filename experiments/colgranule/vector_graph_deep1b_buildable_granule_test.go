package colgranule

import (
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
	kmeansIters := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS", 8)
	topGranulesList := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_TOP_GRANULES", []int{1, 4})
	ranks := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_PCA_RANKS", []int{32, 48, 64, 80, columnVectorGraphDeep1BDims})
	pqBytes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_BYTES", nil)
	opqBytes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_OPQ_BYTES", nil)
	residualPQBytes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_RESIDUAL_PQ_BYTES", nil)
	localResidualPQBytes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_LOCAL_RESIDUAL_PQ_BYTES", nil)
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
	granules, builderNotes := columnVectorGraphDeep1BBuildableGranules(t, builder, vectors, invNorms, baseRows, columnVectorGraphDeep1BDims, granuleRows, kmeansIters)
	if len(granules) == 0 {
		t.Fatalf("no granules for builder=%s baseRows=%d granuleRows=%d", builder, baseRows, granuleRows)
	}
	pqModels := columnVectorGraphDeep1BFitBuildablePQModels(t, trainVectors, pqBytes, pqTrainRows, baseRows, columnVectorGraphDeep1BDims, pqTrainIters)
	residualPQModels := columnVectorGraphDeep1BFitBuildableResidualPQModels(t, trainVectors, residualPQBytes, pqTrainRows, baseRows, columnVectorGraphDeep1BDims, pqTrainIters)
	opqModels := columnVectorGraphDeep1BFitBuildableOPQModels(t, trainVectors, opqBytes, pqTrainRows, baseRows, columnVectorGraphDeep1BDims, pqTrainIters, opqIters)
	localResidualPQModels := columnVectorGraphDeep1BFitBuildableLocalResidualPQModels(t, vectors, invNorms, granules, localResidualPQBytes, baseRows, columnVectorGraphDeep1BDims, pqTrainIters)
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
		GranuleRows:             granuleRows,
		GranuleCount:            len(granules),
		KMeansIters:             kmeansIters,
		PQBytes:                 append([]int(nil), pqBytes...),
		ResidualPQBytes:         append([]int(nil), residualPQBytes...),
		OPQBytes:                append([]int(nil), opqBytes...),
		LocalResidualPQBytes:    append([]int(nil), localResidualPQBytes...),
		PQTrainRows:             len(trainVectors) / columnVectorGraphDeep1BDims,
		PQTrainIters:            pqTrainIters,
		OPQIterations:           opqIters,
		PQTraining:              columnVectorGraphDeep1BPQTrainingReports(codebookModels),
		LocalResidualPQTraining: localResidualPQModels.training,
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
			queryReport := columnVectorGraphDeep1BAnalyzeBuildableGranuleSelection(t, vectors, invNorms, query, queryInvNorm, globalScores, globalTopRows, selected, queryIndex, topGranules, ranks, codebookModels, localResidualPQModels, scanIters)
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
	GranuleRows             int                                                  `json:"granule_rows"`
	GranuleCount            int                                                  `json:"granule_count"`
	KMeansIters             int                                                  `json:"kmeans_iters,omitempty"`
	PQBytes                 []int                                                `json:"pq_bytes,omitempty"`
	ResidualPQBytes         []int                                                `json:"residual_pq_bytes,omitempty"`
	OPQBytes                []int                                                `json:"opq_bytes,omitempty"`
	LocalResidualPQBytes    []int                                                `json:"local_residual_pq_bytes,omitempty"`
	PQTrainRows             int                                                  `json:"pq_train_rows,omitempty"`
	PQTrainIters            int                                                  `json:"pq_train_iters,omitempty"`
	OPQIterations           int                                                  `json:"opq_iterations,omitempty"`
	PQTraining              []columnVectorGraphDeep1BPQTrainingReport            `json:"pq_training,omitempty"`
	LocalResidualPQTraining []columnVectorGraphDeep1BPQTrainingReport            `json:"local_residual_pq_training,omitempty"`
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

func columnVectorGraphDeep1BBuildableGranules(tb testing.TB, builder string, vectors []float32, invNorms []float32, rows int, dims int, granuleRows int, kmeansIters int) ([]columnVectorGraphDeep1BBuildableGranule, string) {
	tb.Helper()
	switch builder {
	case "row_id_contiguous":
		return columnVectorGraphDeep1BBuildRowIDContiguousGranules(vectors, rows, dims, granuleRows), "production/buildable granule scout; row-id-contiguous blocks are buildable storage units, but they are not expected to have nearest-neighbor locality"
	case "ivf_kmeans":
		if kmeansIters <= 0 {
			tb.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=%d must be positive for ivf_kmeans", kmeansIters)
		}
		return columnVectorGraphDeep1BBuildIVFKMeansGranules(vectors, invNorms, rows, dims, granuleRows, kmeansIters), "production/buildable granule scout; ivf_kmeans trains deterministic cosine k-means centroids on the base prefix and assigns rows to buildable IVF-style granules"
	default:
		tb.Fatalf("unknown COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=%q; supported: row_id_contiguous, ivf_kmeans", builder)
		return nil, ""
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

func columnVectorGraphDeep1BAnalyzeBuildableGranuleSelection(tb testing.TB, vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, globalScores []float32, globalTopRows []int, selected []columnVectorGraphDeep1BBuildableGranule, queryIndex int, topGranules int, ranks []int, pqModels []columnVectorGraphDeep1BPQModel, localResidualPQModels columnVectorGraphDeep1BLocalResidualPQModels, scanIters int) columnVectorGraphDeep1BBuildableGranuleQueryReport {
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
	q := columnVectorGraphDeep1BBuildableGranuleQueryReport{
		QueryIndex:            queryIndex,
		Builder:               builder,
		TopGranules:           topGranules,
		CandidateRows:         candidateRows,
		SelectedGranules:      selectedReports,
		CandidateExactMargins: columnVectorGraphDeep1BScoreMarginMetrics(candidateExact),
		Notes:                 "buildable granule scout; codec recalls are conditional on the routed candidate union, while routing recalls measure how many global exact winners reached that union",
	}
	q.RoutingTop10InCandidates = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 10, candidateSet)
	q.RoutingTop20InCandidates = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 20, candidateSet)
	q.RoutingTop50InCandidates = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 50, candidateSet)
	q.RoutingRecallAt10 = float64(q.RoutingTop10InCandidates) / float64(min(10, len(globalTopRows)))
	q.RoutingRecallAt20 = float64(q.RoutingTop20InCandidates) / float64(min(20, len(globalTopRows)))
	q.RoutingRecallAt50 = float64(q.RoutingTop50InCandidates) / float64(min(50, len(globalTopRows)))
	q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildableScalarMethod(vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, 8, "per_dim", "reconstructed", scanIters))
	q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildableScalarMethod(vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, 4, "per_dim", "reconstructed", scanIters))
	for _, model := range pqModels {
		q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildablePQMethod(vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, model, scanIters))
	}
	for _, budget := range localResidualPQModels.budgets {
		q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildableLocalResidualPQMethod(tb, vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, localResidualPQModels, budget, scanIters))
	}
	minSelectedRows := candidateRows
	for _, granule := range selected {
		minSelectedRows = min(minSelectedRows, granule.Rows)
	}
	for _, rank := range columnVectorGraphDeep1BFilterRanksForRows(tb, ranks, minSelectedRows, dims) {
		q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildablePCAMethod(tb, vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, builder, rank, scanIters))
	}
	return q
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
		fmt.Sprintf("production/buildable scout over %s granules; codec recall is conditional on centroid-routed candidate union", builder),
	)
	method.ScanNanosPerVector = scanNanos / float64(totalRows)
	method.MeanRelativeL2 = meanRelL2 / float64(totalRows)
	method.MaxRelativeL2 = maxRelL2
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approxScores, margins)
	return method
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
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approxScores, margins)
	return method
}

func columnVectorGraphDeep1BEvaluateBuildableLocalResidualPQMethod(tb testing.TB, vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, selected []columnVectorGraphDeep1BBuildableGranule, builder string, models columnVectorGraphDeep1BLocalResidualPQModels, rowCodeBytes int, scanIters int) columnVectorGraphDeep1BGroundtruthMethodReport {
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
	for _, granule := range selected {
		gVectors, gInvNorms := columnVectorGraphDeep1BGatherRows(vectors, invNorms, granule.RowIDs, dims)
		localRows := columnVectorGraphDeep1BSequentialRowIDs(granule.Rows)
		granuleModels := models.byGranule[granule.Ordinal]
		model, ok := granuleModels[rowCodeBytes]
		if !ok {
			tb.Fatalf("missing local residual PQ model for granule=%d row-code budget=%d", granule.Ordinal, rowCodeBytes)
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
	if totalRows == 0 {
		totalRows = 1
	}
	method := columnVectorGraphDeep1BNewCompressionMethodReport(
		"buildable_granule_scout",
		"local_f16_centroid_and_residual_pq_codebooks_amortized_over_selected_buildable_granules_plus_f16_inv_norm_per_row",
		"local_residual_product_quantization",
		fmt.Sprintf("buildable_%s_local_residual_pq_%dB_x8", builder, rowCodeBytes),
		rowCodeBytesTotal/float64(totalRows),
		metadataBytes/float64(totalRows),
		buildNanos,
		fmt.Sprintf("production/buildable scout over %s granules; local residual PQ codebooks were prefitted per buildable granule on the rows they encode; this is LOPQ-lite without OPQ rotation, and codec recall is conditional on centroid-routed candidate union", builder),
	)
	method.ScanNanosPerVector = scanNanos / float64(totalRows)
	method.MeanRelativeL2 = meanRelL2 / float64(totalRows)
	method.MaxRelativeL2 = maxRelL2
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approxScores, margins)
	return method
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
	if report.Builder == "ivf_kmeans" {
		fmt.Fprintf(&b, "- K-means iterations: `%d`\n", report.KMeansIters)
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
	fmt.Fprintf(&b, "- Scan iterations: `%d`\n\n", report.ScanIters)
	fmt.Fprintf(&b, "%s\n\n", columnVectorGraphDeep1BBuildableBuilderMarkdown(report))
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
	fmt.Fprintf(&b, "| Query | Top granules | Method | Row-code B/vector | Metadata B/vector | Build ms | Compressed top10 | Top10 in approx@20 | Top10 in approx@50 | Top10 in approx@100 | Top20 in approx@50 | Top20 in approx@100 | Rerank@50 recall@10 | Rerank@100 recall@10 | Mean score err | Err/gap10 | Err/gap20 | Err/gap50 | Scan ns/vector |\n")
	fmt.Fprintf(&b, "| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		for _, method := range q.Methods {
			fmt.Fprintf(&b, "| %d | %d | `%s` | %.2f | %.2f | %.3f | %d/10 | %s | %s | %s | %s | %s | %.2f | %.2f | %.5f | %.2f | %.2f | %.2f | %.2f |\n",
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
				method.MeanScoreError,
				method.MeanErrorOverGap10,
				method.MeanErrorOverGap20,
				method.MeanErrorOverGap50,
				method.ScanNanosPerVector,
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

func columnVectorGraphDeep1BBuildableBuilderMarkdown(report columnVectorGraphDeep1BBuildableGranuleScoutReport) string {
	switch report.Builder {
	case "row_id_contiguous":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 {
			return "This is a **production/buildable granule** scout using `row_id_contiguous` blocks. They are real storage units TreeDB can build without oracle labels, but they are intentionally a weak locality control. The result separates routing/locality failure from codec failure. Global PQ/OPQ/residual-PQ rows use a held-out train/eval split when enabled; local residual PQ rows use prefitted per-granule residual codebooks as a LOPQ-lite lane."
		}
		return "This is a **production/buildable granule** scout using `row_id_contiguous` blocks. They are real storage units TreeDB can build without oracle labels, but they are intentionally a weak locality control. The result separates routing/locality failure from codec failure; it should not be read as evidence that row-id order is a good ANN granule builder. PQ/OPQ/residual-PQ/LOPQ are still pending because they require real train/eval splits and trained codebooks."
	case "ivf_kmeans":
		if len(report.PQTraining) > 0 || len(report.LocalResidualPQBytes) > 0 {
			return "This is a **production/buildable granule** scout using deterministic cosine `ivf_kmeans` clusters trained on the eval slice. It is a buildable locality probe, unlike the official top100 oracle clouds. Global PQ/OPQ/residual-PQ rows use held-out global codebooks when enabled; local residual PQ uses separate prefitted residual codebooks per buildable granule as a LOPQ-lite lane, with metadata amortized over those granule rows."
		}
		return "This is a **production/buildable granule** scout using deterministic cosine `ivf_kmeans` clusters trained on the base prefix. It is a buildable locality probe, unlike the official top100 oracle clouds, but it is still not a PQ/OPQ/residual-PQ tournament: codebook methods still require separate train/eval discipline and metadata accounting."
	default:
		return fmt.Sprintf("This is a **production/buildable granule** scout using `%s`. Codec metrics are conditional on the routed candidate union, and trained-codebook methods still require separate train/eval discipline before production claims.", report.Builder)
	}
}

func columnVectorGraphDeep1BRenderBuildableAggregateMarkdown(b *strings.Builder, report columnVectorGraphDeep1BBuildableGranuleScoutReport) {
	type aggregate struct {
		name       string
		count      int
		rowBytes   float64
		metaBytes  float64
		buildNanos float64
		scanNanos  float64
		scoreError float64
		gap10      float64
		gap20      float64
		gap50      float64
		top10      []int
		top10At20  []int
		top10At50  []int
		top10At100 []int
		top20At50  []int
		top20At100 []int
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
	fmt.Fprintf(b, "| Method | Queries | Row-code B/vector | Metadata B/vector | Avg build ms | p50 compressed top10 | worst compressed top10 | p50 top10@20 | worst top10@20 | p50 top10@50 | worst top10@50 | p50 top10@100 | worst top10@100 | p50 top20@50 | worst top20@50 | p50 top20@100 | worst top20@100 | Avg score err | Avg err/gap10 | Avg err/gap20 | Avg err/gap50 | Avg scan ns/vector |\n")
	fmt.Fprintf(b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, name := range names {
		agg := byName[name]
		sort.Ints(agg.top10)
		sort.Ints(agg.top10At20)
		sort.Ints(agg.top10At50)
		sort.Ints(agg.top10At100)
		sort.Ints(agg.top20At50)
		sort.Ints(agg.top20At100)
		count := float64(max(1, agg.count))
		fmt.Fprintf(b, "| `%s` | %d | %.2f | %.2f | %.3f | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/20 | %d/20 | %d/20 | %d/20 | %.5f | %.2f | %.2f | %.2f | %.2f |\n",
			agg.name,
			agg.count,
			agg.rowBytes/count,
			agg.metaBytes/count,
			agg.buildNanos/count/1e6,
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top10At20, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10At20, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top10At50, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10At50, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top10At100, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10At100, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top20At50, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top20At50, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top20At100, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top20At100, 0),
			agg.scoreError/count,
			agg.gap10/count,
			agg.gap20/count,
			agg.gap50/count,
			agg.scanNanos/count,
		)
	}
}
