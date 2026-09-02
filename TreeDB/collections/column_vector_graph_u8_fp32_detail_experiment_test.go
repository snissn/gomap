package collections

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	columnGraphU8FP32DetailExperimentEnv        = "TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_EXPERIMENT"
	columnGraphU8FP32DetailExperimentOutEnv     = "TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_OUT"
	columnGraphU8FP32DetailExperimentKEnv       = "TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_KS"
	columnGraphU8FP32DetailExperimentDatasetEnv = "TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_DATASET"
)

var defaultColumnGraphU8FP32DetailExperimentKs = []int{0, 4, 8, 16, 32, 64, 128}

type columnGraphU8FP32DetailExperimentReport struct {
	GeneratedAt      string                                             `json:"generated_at"`
	OutputDir        string                                             `json:"output_dir"`
	Shape            columnGraphU8FP32DetailExperimentShapeReport       `json:"shape"`
	DetailKs         []int                                              `json:"detail_ks"`
	Selectors        []string                                           `json:"selectors"`
	Notes            []string                                           `json:"notes"`
	Queries          []columnGraphU8FP32DetailExperimentQueryReport     `json:"queries"`
	AggregateMethods []columnGraphU8FP32DetailExperimentAggregateReport `json:"aggregate_methods"`
}

type columnGraphU8FP32DetailExperimentShapeReport struct {
	Rows         int    `json:"rows"`
	Dims         int    `json:"dims"`
	TopK         int    `json:"top_k"`
	QueryOrdinal int    `json:"query_ordinal"`
	QueryCount   int    `json:"query_count"`
	Dataset      string `json:"dataset"`
}

type columnGraphU8FP32DetailExperimentQueryReport struct {
	QueryOrdinal int                                             `json:"query_ordinal"`
	Methods      []columnGraphU8FP32DetailExperimentMethodReport `json:"methods"`
}

type columnGraphU8FP32DetailExperimentMethodReport struct {
	MethodName              string  `json:"method_name"`
	Selector                string  `json:"selector"`
	DetailK                 int     `json:"detail_k"`
	BaseBytesPerVector      float64 `json:"base_bytes_per_vector"`
	DetailBytesPerVector    float64 `json:"detail_bytes_per_vector"`
	TotalBytesPerVector     float64 `json:"total_bytes_per_vector"`
	CompressedTop10Overlap  int     `json:"compressed_top10_overlap,omitempty"`
	CompressedTop10Recall   float64 `json:"compressed_top10_recall,omitempty"`
	CompressedTop20Overlap  int     `json:"compressed_top20_overlap,omitempty"`
	CompressedTop20Recall   float64 `json:"compressed_top20_recall,omitempty"`
	Top10InApproxTop20      int     `json:"top10_in_approx_top20,omitempty"`
	Top10RecallAt20         float64 `json:"top10_recall_at_20,omitempty"`
	Top10InApproxTop50      int     `json:"top10_in_approx_top50,omitempty"`
	Top10RecallAt50         float64 `json:"top10_recall_at_50,omitempty"`
	Top10InApproxTop100     int     `json:"top10_in_approx_top100,omitempty"`
	Top10RecallAt100        float64 `json:"top10_recall_at_100,omitempty"`
	Top20InApproxTop50      int     `json:"top20_in_approx_top50,omitempty"`
	Top20RecallAt50         float64 `json:"top20_recall_at_50,omitempty"`
	Top20InApproxTop100     int     `json:"top20_in_approx_top100,omitempty"`
	Top20RecallAt100        float64 `json:"top20_recall_at_100,omitempty"`
	MeanScoreError          float64 `json:"mean_score_error"`
	MaxScoreError           float64 `json:"max_score_error"`
	MeanScoreErrorOverGap10 float64 `json:"mean_score_error_over_gap10,omitempty"`
}

type columnGraphU8FP32DetailExperimentAggregateReport struct {
	MethodName              string  `json:"method_name"`
	Selector                string  `json:"selector"`
	DetailK                 int     `json:"detail_k"`
	BaseBytesPerVector      float64 `json:"base_bytes_per_vector"`
	DetailBytesPerVector    float64 `json:"detail_bytes_per_vector"`
	TotalBytesPerVector     float64 `json:"total_bytes_per_vector"`
	Queries                 int     `json:"queries"`
	AvgCompressedTop10      float64 `json:"avg_compressed_top10"`
	WorstCompressedTop10    int     `json:"worst_compressed_top10"`
	AvgTop10At20            float64 `json:"avg_top10_at20"`
	WorstTop10At20          int     `json:"worst_top10_at20"`
	AvgTop10At50            float64 `json:"avg_top10_at50"`
	WorstTop10At50          int     `json:"worst_top10_at50"`
	AvgTop20At50            float64 `json:"avg_top20_at50"`
	WorstTop20At50          int     `json:"worst_top20_at50"`
	MeanScoreError          float64 `json:"mean_score_error"`
	MaxScoreError           float64 `json:"max_score_error"`
	MeanScoreErrorOverGap10 float64 `json:"mean_score_error_over_gap10"`
}

type columnGraphU8FP32DetailCandidate struct {
	dim   int
	score float64
}

// TestColumnGraphU8FP32DetailQueryExperiment is an opt-in query-time viability
// probe for a possible scalar_u8 + selected fp32 detail tier. It intentionally
// does not build a durable codec, does not use a production scorer, and does not
// claim hot-path speed. The goal is to answer whether selected fp32 dimensions
// can materially improve scalar_u8 ranking/shortlist quality on the same large
// synthetic shapes used by the scalar_u8 production benchmark gate.
func TestColumnGraphU8FP32DetailQueryExperiment(t *testing.T) {
	if os.Getenv(columnGraphU8FP32DetailExperimentEnv) != "1" {
		t.Skipf("set %s=1 to run the scalar_u8 + fp32 detail query-time experiment", columnGraphU8FP32DetailExperimentEnv)
	}
	shape := columnGraphQuantizedProductionBenchShapeFromEnv2591(t)
	if shape.rows <= 0 || shape.dims <= 0 {
		t.Fatalf("invalid shape rows=%d dims=%d", shape.rows, shape.dims)
	}
	if shape.dims > int(^uint16(0))+1 {
		t.Fatalf("dims=%d exceeds uint16 detail dim_id storage model", shape.dims)
	}
	if shape.queryCount <= 0 {
		shape.queryCount = 1
	}
	if shape.queryCount > shape.rows {
		shape.queryCount = shape.rows
	}
	if shape.queryOrdinal < 0 || shape.queryOrdinal >= shape.rows {
		shape.queryOrdinal = shape.rows / 3
	}
	detailKs, err := columnGraphU8FP32DetailExperimentKsFromEnv(shape.dims)
	if err != nil {
		t.Fatalf("detail ks: %v", err)
	}
	maxK := 0
	for _, k := range detailKs {
		if k > maxK {
			maxK = k
		}
	}

	outDir := strings.TrimSpace(os.Getenv(columnGraphU8FP32DetailExperimentOutEnv))
	if outDir == "" {
		outDir = filepath.Join(os.TempDir(), "treedb_u8_fp32_detail_experiment_"+time.Now().Format("20060102_150405"))
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	dataset, vectors, invNorms := columnGraphU8FP32DetailExperimentVectors(t, shape.rows, shape.dims)
	codes := columnGraphU8FP32DetailScalarU8Codes(vectors, invNorms, shape.rows, shape.dims)
	rowResidualDims := columnGraphU8FP32DetailRowResidualDims(vectors, invNorms, codes, shape.rows, shape.dims, maxK)

	report := columnGraphU8FP32DetailExperimentReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		OutputDir:   outDir,
		Shape: columnGraphU8FP32DetailExperimentShapeReport{
			Rows:         shape.rows,
			Dims:         shape.dims,
			TopK:         shape.topK,
			QueryOrdinal: shape.queryOrdinal,
			QueryCount:   shape.queryCount,
			Dataset:      dataset,
		},
		DetailKs:  append([]int(nil), detailKs...),
		Selectors: []string{"u8_base", "row_residual_topk", "query_score_error_topk"},
		Notes: []string{
			"query-time viability experiment only; no durable codec, no mmap asset, no production scorer, and no hot-path speed claim",
			"base score uses TreeDB scalar_u8 normalized cosine coding; detail dimensions replace scalar_u8 per-dim contributions with normalized fp32 contributions",
			"row_residual_topk is query-independent and production-plausible; query_score_error_topk is a query-time ceiling selector",
		},
	}

	queryOrdinals := columnGraphU8FP32DetailQueryOrdinals(shape.rows, shape.queryOrdinal, shape.queryCount)
	for _, queryOrdinal := range queryOrdinals {
		queryReport := columnGraphU8FP32DetailExperimentQueryReport{QueryOrdinal: queryOrdinal}
		queryReport.Methods = columnGraphU8FP32DetailEvaluateQuery(vectors, invNorms, codes, rowResidualDims, shape.rows, shape.dims, shape.topK, queryOrdinal, detailKs, maxK)
		report.Queries = append(report.Queries, queryReport)
	}
	report.AggregateMethods = columnGraphU8FP32DetailAggregateReports(report.Queries)

	jsonPath := filepath.Join(outDir, "results.json")
	jsonBytes, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal report JSON: %v", err)
	}
	if err := os.WriteFile(jsonPath, jsonBytes, 0o644); err != nil {
		t.Fatalf("write report JSON: %v", err)
	}
	mdPath := filepath.Join(outDir, "report.md")
	if err := os.WriteFile(mdPath, []byte(columnGraphU8FP32DetailRenderMarkdown(report)), 0o644); err != nil {
		t.Fatalf("write report Markdown: %v", err)
	}
	t.Logf("scalar_u8 + fp32 detail query experiment report: %s", mdPath)
}

func columnGraphU8FP32DetailExperimentKsFromEnv(dims int) ([]int, error) {
	raw := strings.TrimSpace(os.Getenv(columnGraphU8FP32DetailExperimentKEnv))
	if raw == "" {
		return columnGraphU8FP32DetailNormalizeKs(defaultColumnGraphU8FP32DetailExperimentKs, dims)
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' })
	ks := make([]int, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return nil, fmt.Errorf("%s=%q contains non-negative integer token %q", columnGraphU8FP32DetailExperimentKEnv, raw, part)
		}
		ks = append(ks, value)
	}
	return columnGraphU8FP32DetailNormalizeKs(ks, dims)
}

func columnGraphU8FP32DetailNormalizeKs(input []int, dims int) ([]int, error) {
	if dims <= 0 {
		return nil, fmt.Errorf("dims=%d", dims)
	}
	seen := make(map[int]struct{}, len(input)+1)
	out := make([]int, 0, len(input)+1)
	for _, k := range input {
		if k < 0 {
			return nil, fmt.Errorf("detail k=%d must be non-negative", k)
		}
		if k > dims {
			k = dims
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	if _, ok := seen[0]; !ok {
		out = append(out, 0)
	}
	sort.Ints(out)
	return out, nil
}

func columnGraphU8FP32DetailExperimentVectors(tb testing.TB, rows, dims int) (string, []float32, []float32) {
	tb.Helper()
	dataset := strings.TrimSpace(os.Getenv(columnGraphU8FP32DetailExperimentDatasetEnv))
	if dataset == "" {
		dataset = "clustered"
	}
	switch dataset {
	case "clustered":
		vectors := columnGraphU8FP32DetailClusteredVectors(rows, dims)
		return dataset, vectors, columnGraphU8FP32DetailInvNorms(vectors, rows, dims)
	case "rebuild_synthetic":
		inputRows := columnGraphRebuildSyntheticRowsV2A(rows, dims)
		vectors := make([]float32, len(inputRows)*dims)
		for row, input := range inputRows {
			if len(input.vector) != dims {
				tb.Fatalf("row %d dims=%d want %d", row, len(input.vector), dims)
			}
			copy(vectors[row*dims:(row+1)*dims], input.vector)
		}
		return dataset, vectors, columnGraphU8FP32DetailInvNorms(vectors, rows, dims)
	default:
		tb.Fatalf("unsupported %s=%q (want clustered or rebuild_synthetic)", columnGraphU8FP32DetailExperimentDatasetEnv, dataset)
		return "", nil, nil
	}
}

func columnGraphU8FP32DetailClusteredVectors(rows, dims int) []float32 {
	clusters := rows / 128
	if clusters < 8 {
		clusters = 8
	}
	if clusters > 256 {
		clusters = 256
	}
	if clusters > rows {
		clusters = rows
	}
	centroids := make([]float32, clusters*dims)
	for cluster := 0; cluster < clusters; cluster++ {
		base := cluster * dims
		for j := 0; j < dims; j++ {
			centroids[base+j] = float32(columnGraphU8FP32DetailHashUnit(uint64(cluster+1), uint64(j+1), 0x9e3779b97f4a7c15))
		}
		inv := columnGraphU8FP32DetailInvNorm(centroids[base : base+dims])
		for j := 0; j < dims; j++ {
			centroids[base+j] *= float32(inv)
		}
	}
	vectors := make([]float32, rows*dims)
	const centroidWeight = 1.0
	const noiseWeight = 0.30
	for row := 0; row < rows; row++ {
		cluster := (row * 131) % clusters
		centroidBase := cluster * dims
		rowBase := row * dims
		for j := 0; j < dims; j++ {
			noise := columnGraphU8FP32DetailHashUnit(uint64(row+1), uint64(j+1), 0xd1b54a32d192ed03)
			vectors[rowBase+j] = centroidWeight*centroids[centroidBase+j] + noiseWeight*float32(noise)/float32(math.Sqrt(float64(dims)))
		}
	}
	return vectors
}

func columnGraphU8FP32DetailHashUnit(a, b, seed uint64) float64 {
	value := seed + a*0x9e3779b97f4a7c15 + b*0xbf58476d1ce4e5b9
	value = columnGraphU8FP32DetailSplitMix64(value)
	unit := float64(value>>11) * (1.0 / (1 << 53))
	return unit*2 - 1
}

func columnGraphU8FP32DetailSplitMix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func columnGraphU8FP32DetailInvNorms(vectors []float32, rows, dims int) []float32 {
	invNorms := make([]float32, rows)
	for row := 0; row < rows; row++ {
		invNorms[row] = float32(columnGraphU8FP32DetailInvNorm(vectors[row*dims : (row+1)*dims]))
	}
	return invNorms
}

func columnGraphU8FP32DetailInvNorm(vector []float32) float64 {
	var norm2 float64
	for _, value := range vector {
		norm2 += float64(value) * float64(value)
	}
	if norm2 <= 0 || math.IsNaN(norm2) || math.IsInf(norm2, 0) {
		return 0
	}
	return 1 / math.Sqrt(norm2)
}

func columnGraphU8FP32DetailScalarU8Codes(vectors []float32, invNorms []float32, rows, dims int) []byte {
	codes := make([]byte, rows*dims)
	for row := 0; row < rows; row++ {
		base := row * dims
		invNorm := invNorms[row]
		for j := 0; j < dims; j++ {
			codes[base+j] = columnVectorGraphScalarU8Code(vectors[base+j] * invNorm)
		}
	}
	return codes
}

func columnGraphU8FP32DetailRowResidualDims(vectors []float32, invNorms []float32, codes []byte, rows, dims, maxK int) []uint16 {
	if maxK <= 0 {
		return nil
	}
	out := make([]uint16, rows*maxK)
	candidates := make([]columnGraphU8FP32DetailCandidate, dims)
	for row := 0; row < rows; row++ {
		base := row * dims
		invNorm := float64(invNorms[row])
		for j := 0; j < dims; j++ {
			xNorm := float64(vectors[base+j]) * invNorm
			xU8 := columnGraphU8FP32DetailCenteredCodeAsUnit(codes[base+j])
			candidates[j] = columnGraphU8FP32DetailCandidate{dim: j, score: math.Abs(xNorm - xU8)}
		}
		columnGraphU8FP32DetailSortCandidates(candidates)
		for k := 0; k < maxK; k++ {
			out[row*maxK+k] = uint16(candidates[k].dim)
		}
	}
	return out
}

func columnGraphU8FP32DetailEvaluateQuery(vectors []float32, invNorms []float32, codes []byte, rowResidualDims []uint16, rows, dims, topK, queryOrdinal int, detailKs []int, maxK int) []columnGraphU8FP32DetailExperimentMethodReport {
	qBase := queryOrdinal * dims
	queryInvNorm := float64(invNorms[queryOrdinal])
	qNorm := make([]float64, dims)
	qCentered := make([]int16, dims)
	for j := 0; j < dims; j++ {
		qNorm[j] = float64(vectors[qBase+j]) * queryInvNorm
		qCentered[j] = columnGraphU8FP32DetailCenteredCode(columnVectorGraphScalarU8Code(float32(qNorm[j])))
	}
	exactScores := make([]float64, rows)
	u8Scores := make([]float64, rows)
	for row := 0; row < rows; row++ {
		base := row * dims
		invNorm := float64(invNorms[row])
		var exact float64
		var dot int64
		for j := 0; j < dims; j++ {
			exact += qNorm[j] * float64(vectors[base+j]) * invNorm
			dot += int64(qCentered[j]) * int64(columnGraphU8FP32DetailCenteredCode(codes[base+j]))
		}
		exactScores[row] = exact
		u8Scores[row] = float64(dot) / columnVectorGraphScalarU8CodeScale
	}
	exactOrder := columnGraphU8FP32DetailOrderScores(exactScores)
	out := make([]columnGraphU8FP32DetailExperimentMethodReport, 0, 1+2*len(detailKs))
	out = append(out, columnGraphU8FP32DetailBuildMethodReport("u8_base", "u8_base", 0, float64(dims), 0, exactScores, exactOrder, u8Scores, topK))

	if maxK > 0 {
		rowScores := append([]float64(nil), u8Scores...)
		prevK := 0
		for _, k := range detailKs {
			if k == 0 {
				continue
			}
			columnGraphU8FP32DetailApplyDims(rowScores, vectors, invNorms, codes, qNorm, qCentered, rowResidualDims, rows, dims, maxK, prevK, k)
			out = append(out, columnGraphU8FP32DetailBuildMethodReport(fmt.Sprintf("u8_fp32_detail_row_residual_k%d", k), "row_residual_topk", k, float64(dims), float64(6*k), exactScores, exactOrder, rowScores, topK))
			prevK = k
		}

		queryDetailDims := columnGraphU8FP32DetailQueryScoreErrorDims(vectors, invNorms, codes, qNorm, qCentered, rows, dims, maxK)
		queryScores := append([]float64(nil), u8Scores...)
		prevK = 0
		for _, k := range detailKs {
			if k == 0 {
				continue
			}
			columnGraphU8FP32DetailApplyDims(queryScores, vectors, invNorms, codes, qNorm, qCentered, queryDetailDims, rows, dims, maxK, prevK, k)
			out = append(out, columnGraphU8FP32DetailBuildMethodReport(fmt.Sprintf("u8_fp32_detail_query_error_k%d", k), "query_score_error_topk", k, float64(dims), float64(6*k), exactScores, exactOrder, queryScores, topK))
			prevK = k
		}
	}
	return out
}

func columnGraphU8FP32DetailQueryScoreErrorDims(vectors []float32, invNorms []float32, codes []byte, qNorm []float64, qCentered []int16, rows, dims, maxK int) []uint16 {
	out := make([]uint16, rows*maxK)
	candidates := make([]columnGraphU8FP32DetailCandidate, dims)
	for row := 0; row < rows; row++ {
		for j := 0; j < dims; j++ {
			correction := columnGraphU8FP32DetailCorrection(vectors, invNorms, codes, qNorm, qCentered, row, j, dims)
			candidates[j] = columnGraphU8FP32DetailCandidate{dim: j, score: math.Abs(correction)}
		}
		columnGraphU8FP32DetailSortCandidates(candidates)
		for k := 0; k < maxK; k++ {
			out[row*maxK+k] = uint16(candidates[k].dim)
		}
	}
	return out
}

func columnGraphU8FP32DetailSortCandidates(candidates []columnGraphU8FP32DetailCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].dim < candidates[j].dim
	})
}

func columnGraphU8FP32DetailApplyDims(scores []float64, vectors []float32, invNorms []float32, codes []byte, qNorm []float64, qCentered []int16, dimsByRow []uint16, rows, dims, maxK, startK, endK int) {
	for row := 0; row < rows; row++ {
		dimBase := row * maxK
		for k := startK; k < endK; k++ {
			dim := int(dimsByRow[dimBase+k])
			scores[row] += columnGraphU8FP32DetailCorrection(vectors, invNorms, codes, qNorm, qCentered, row, dim, dims)
		}
	}
}

func columnGraphU8FP32DetailCorrection(vectors []float32, invNorms []float32, codes []byte, qNorm []float64, qCentered []int16, row, dim, dims int) float64 {
	idx := row*dims + dim
	exactContribution := qNorm[dim] * float64(vectors[idx]) * float64(invNorms[row])
	u8Contribution := float64(qCentered[dim]) * float64(columnGraphU8FP32DetailCenteredCode(codes[idx])) / columnVectorGraphScalarU8CodeScale
	return exactContribution - u8Contribution
}

func columnGraphU8FP32DetailCenteredCode(code byte) int16 {
	return int16(int(code)*2 - 255)
}

func columnGraphU8FP32DetailCenteredCodeAsUnit(code byte) float64 {
	return float64(columnGraphU8FP32DetailCenteredCode(code)) / 255.0
}

func columnGraphU8FP32DetailBuildMethodReport(methodName, selector string, detailK int, baseBytes, detailBytes float64, exactScores []float64, exactOrder []int, approxScores []float64, topK int) columnGraphU8FP32DetailExperimentMethodReport {
	approxOrder := columnGraphU8FP32DetailOrderScores(approxScores)
	meanErr, maxErr := columnGraphU8FP32DetailScoreError(exactScores, approxScores)
	gap10 := columnGraphU8FP32DetailBoundaryGap(exactScores, exactOrder, 10)
	report := columnGraphU8FP32DetailExperimentMethodReport{
		MethodName:              methodName,
		Selector:                selector,
		DetailK:                 detailK,
		BaseBytesPerVector:      baseBytes,
		DetailBytesPerVector:    detailBytes,
		TotalBytesPerVector:     baseBytes + detailBytes,
		MeanScoreError:          meanErr,
		MaxScoreError:           maxErr,
		MeanScoreErrorOverGap10: columnGraphU8FP32DetailSafeRatio(meanErr, gap10),
	}
	top10 := min(10, len(exactScores))
	if top10 > 0 {
		report.CompressedTop10Overlap = columnGraphU8FP32DetailCandidateOverlap(exactOrder, approxOrder, top10, top10)
		report.CompressedTop10Recall = float64(report.CompressedTop10Overlap) / float64(top10)
	}
	top20 := min(20, len(exactScores))
	if top20 > 0 {
		report.CompressedTop20Overlap = columnGraphU8FP32DetailCandidateOverlap(exactOrder, approxOrder, top20, top20)
		report.CompressedTop20Recall = float64(report.CompressedTop20Overlap) / float64(top20)
	}
	if len(exactScores) >= 20 && top10 > 0 {
		report.Top10InApproxTop20 = columnGraphU8FP32DetailCandidateOverlap(exactOrder, approxOrder, top10, 20)
		report.Top10RecallAt20 = float64(report.Top10InApproxTop20) / float64(top10)
	}
	if len(exactScores) >= 50 && top10 > 0 {
		report.Top10InApproxTop50 = columnGraphU8FP32DetailCandidateOverlap(exactOrder, approxOrder, top10, 50)
		report.Top10RecallAt50 = float64(report.Top10InApproxTop50) / float64(top10)
		report.Top20InApproxTop50 = columnGraphU8FP32DetailCandidateOverlap(exactOrder, approxOrder, top20, 50)
		report.Top20RecallAt50 = float64(report.Top20InApproxTop50) / float64(top20)
	}
	if len(exactScores) >= 100 && top10 > 0 {
		report.Top10InApproxTop100 = columnGraphU8FP32DetailCandidateOverlap(exactOrder, approxOrder, top10, 100)
		report.Top10RecallAt100 = float64(report.Top10InApproxTop100) / float64(top10)
		report.Top20InApproxTop100 = columnGraphU8FP32DetailCandidateOverlap(exactOrder, approxOrder, top20, 100)
		report.Top20RecallAt100 = float64(report.Top20InApproxTop100) / float64(top20)
	}
	if topK > 0 && topK != 10 && topK <= len(exactScores) {
		// Keep topK live in this query-time probe so future shape changes do not
		// accidentally make the parsed benchmark option irrelevant. The standard
		// report columns remain top10/top20 to match scalar_u8 vector gates.
		_ = columnGraphU8FP32DetailCandidateOverlap(exactOrder, approxOrder, topK, topK)
	}
	return report
}

func columnGraphU8FP32DetailOrderScores(scores []float64) []int {
	order := make([]int, len(scores))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		left, right := order[i], order[j]
		if scores[left] != scores[right] {
			return scores[left] > scores[right]
		}
		return left < right
	})
	return order
}

func columnGraphU8FP32DetailCandidateOverlap(exactOrder []int, approxOrder []int, targetK, candidateK int) int {
	if targetK <= 0 || candidateK <= 0 {
		return 0
	}
	targetK = min(targetK, len(exactOrder))
	candidateK = min(candidateK, len(approxOrder))
	exactSet := make(map[int]struct{}, targetK)
	for _, row := range exactOrder[:targetK] {
		exactSet[row] = struct{}{}
	}
	overlap := 0
	for _, row := range approxOrder[:candidateK] {
		if _, ok := exactSet[row]; ok {
			overlap++
		}
	}
	return overlap
}

func columnGraphU8FP32DetailScoreError(exactScores []float64, approxScores []float64) (float64, float64) {
	if len(exactScores) == 0 {
		return 0, 0
	}
	var sum float64
	var maxErr float64
	for i, exact := range exactScores {
		err := math.Abs(exact - approxScores[i])
		sum += err
		if err > maxErr {
			maxErr = err
		}
	}
	return sum / float64(len(exactScores)), maxErr
}

func columnGraphU8FP32DetailBoundaryGap(scores []float64, order []int, rank int) float64 {
	if rank <= 0 || len(order) <= rank {
		return 0
	}
	gap := scores[order[rank-1]] - scores[order[rank]]
	if gap < 0 {
		return 0
	}
	return gap
}

func columnGraphU8FP32DetailSafeRatio(numerator, denominator float64) float64 {
	if denominator <= 0 || math.IsNaN(denominator) || math.IsInf(denominator, 0) {
		return 0
	}
	return numerator / denominator
}

func columnGraphU8FP32DetailQueryOrdinals(rows, start, count int) []int {
	if rows <= 0 || count <= 0 {
		return nil
	}
	if start < 0 || start >= rows {
		start = rows / 3
	}
	if count > rows {
		count = rows
	}
	stride := rows / count
	if stride <= 0 {
		stride = 1
	}
	out := make([]int, 0, count)
	seen := make(map[int]struct{}, count)
	for len(out) < count {
		candidate := (start + len(out)*stride) % rows
		if _, ok := seen[candidate]; ok {
			candidate = len(seen) % rows
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	return out
}

func columnGraphU8FP32DetailAggregateReports(queries []columnGraphU8FP32DetailExperimentQueryReport) []columnGraphU8FP32DetailExperimentAggregateReport {
	type accum struct {
		report             columnGraphU8FP32DetailExperimentAggregateReport
		compressedTop10Sum int
		top10At20Sum       int
		top10At50Sum       int
		top20At50Sum       int
		meanScoreErrorSum  float64
		errorOverGap10Sum  float64
	}
	byName := make(map[string]*accum)
	order := make([]string, 0)
	for _, query := range queries {
		for _, method := range query.Methods {
			a := byName[method.MethodName]
			if a == nil {
				a = &accum{report: columnGraphU8FP32DetailExperimentAggregateReport{
					MethodName:           method.MethodName,
					Selector:             method.Selector,
					DetailK:              method.DetailK,
					BaseBytesPerVector:   method.BaseBytesPerVector,
					DetailBytesPerVector: method.DetailBytesPerVector,
					TotalBytesPerVector:  method.TotalBytesPerVector,
					WorstCompressedTop10: math.MaxInt,
					WorstTop10At20:       math.MaxInt,
					WorstTop10At50:       math.MaxInt,
					WorstTop20At50:       math.MaxInt,
				}}
				byName[method.MethodName] = a
				order = append(order, method.MethodName)
			}
			a.report.Queries++
			a.compressedTop10Sum += method.CompressedTop10Overlap
			a.top10At20Sum += method.Top10InApproxTop20
			a.top10At50Sum += method.Top10InApproxTop50
			a.top20At50Sum += method.Top20InApproxTop50
			a.meanScoreErrorSum += method.MeanScoreError
			a.errorOverGap10Sum += method.MeanScoreErrorOverGap10
			if method.MaxScoreError > a.report.MaxScoreError {
				a.report.MaxScoreError = method.MaxScoreError
			}
			if method.CompressedTop10Overlap < a.report.WorstCompressedTop10 {
				a.report.WorstCompressedTop10 = method.CompressedTop10Overlap
			}
			if method.Top10InApproxTop20 < a.report.WorstTop10At20 {
				a.report.WorstTop10At20 = method.Top10InApproxTop20
			}
			if method.Top10InApproxTop50 < a.report.WorstTop10At50 {
				a.report.WorstTop10At50 = method.Top10InApproxTop50
			}
			if method.Top20InApproxTop50 < a.report.WorstTop20At50 {
				a.report.WorstTop20At50 = method.Top20InApproxTop50
			}
		}
	}
	out := make([]columnGraphU8FP32DetailExperimentAggregateReport, 0, len(order))
	for _, name := range order {
		a := byName[name]
		if a.report.Queries > 0 {
			denom := float64(a.report.Queries)
			a.report.AvgCompressedTop10 = float64(a.compressedTop10Sum) / denom
			a.report.AvgTop10At20 = float64(a.top10At20Sum) / denom
			a.report.AvgTop10At50 = float64(a.top10At50Sum) / denom
			a.report.AvgTop20At50 = float64(a.top20At50Sum) / denom
			a.report.MeanScoreError = a.meanScoreErrorSum / denom
			a.report.MeanScoreErrorOverGap10 = a.errorOverGap10Sum / denom
		}
		out = append(out, a.report)
	}
	return out
}

func columnGraphU8FP32DetailRenderMarkdown(report columnGraphU8FP32DetailExperimentReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# scalar_u8 + fp32 detail query-time experiment\n\n")
	fmt.Fprintf(&b, "Generated: `%s`\n\n", report.GeneratedAt)
	fmt.Fprintf(&b, "This is an opt-in viability experiment, not a production codec or scorer. It uses the large TreeDB vector benchmark shape, computes scalar_u8 scores, and then applies selected fp32 per-dimension replacement corrections at query time. Runtime is intentionally not interpreted as production speed.\n\n")
	fmt.Fprintf(&b, "| Rows | Dims | Dataset | TopK | Query ordinal | Query count | Detail K values |\n")
	fmt.Fprintf(&b, "| ---: | ---: | --- | ---: | ---: | ---: | --- |\n")
	fmt.Fprintf(&b, "| %d | %d | `%s` | %d | %d | %d | `%s` |\n\n", report.Shape.Rows, report.Shape.Dims, report.Shape.Dataset, report.Shape.TopK, report.Shape.QueryOrdinal, report.Shape.QueryCount, columnGraphU8FP32DetailFormatInts(report.DetailKs))
	fmt.Fprintf(&b, "Storage model: base scalar_u8 costs `dims` bytes/vector; fp32 detail costs `6*K` bytes/vector (`uint16 dim_id + fp32 value` per selected dimension).\n\n")
	fmt.Fprintf(&b, "## Aggregate methods\n\n")
	fmt.Fprintf(&b, "| Method | Selector | K | B/vector | Avg top10@20 | Worst top10@20 | Avg top10@50 | Worst top10@50 | Avg top20@50 | Worst top20@50 | Avg compressed top10 | Worst compressed top10 | Mean score err | Max score err | Mean err/gap10 |\n")
	fmt.Fprintf(&b, "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, row := range report.AggregateMethods {
		fmt.Fprintf(&b, "| `%s` | `%s` | %d | %.1f | %.2f | %d | %.2f | %d | %.2f | %d | %.2f | %d | %.6f | %.6f | %.2f |\n",
			row.MethodName,
			row.Selector,
			row.DetailK,
			row.TotalBytesPerVector,
			row.AvgTop10At20,
			row.WorstTop10At20,
			row.AvgTop10At50,
			row.WorstTop10At50,
			row.AvgTop20At50,
			row.WorstTop20At50,
			row.AvgCompressedTop10,
			row.WorstCompressedTop10,
			row.MeanScoreError,
			row.MaxScoreError,
			row.MeanScoreErrorOverGap10,
		)
	}
	fmt.Fprintf(&b, "\n## Notes\n\n")
	for _, note := range report.Notes {
		fmt.Fprintf(&b, "- %s\n", note)
	}
	fmt.Fprintf(&b, "\n## Reproduction\n\n")
	fmt.Fprintf(&b, "```sh\n")
	fmt.Fprintf(&b, "%s=1 %s=10k_x_768 %s=clustered GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections -run '^TestColumnGraphU8FP32DetailQueryExperiment$' -count=1\n", columnGraphU8FP32DetailExperimentEnv, columnGraphScalarU8QuantizedBenchShapeEnv1926, columnGraphU8FP32DetailExperimentDatasetEnv)
	fmt.Fprintf(&b, "```\n")
	return b.String()
}

func columnGraphU8FP32DetailFormatInts(values []int) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.Itoa(value)
	}
	return strings.Join(parts, ",")
}
