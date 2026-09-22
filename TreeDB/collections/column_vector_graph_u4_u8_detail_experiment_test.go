package collections

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	columnGraphU4U8DetailExperimentEnv    = "TREEDB_COLUMN_GRAPH_U4_U8_DETAIL_EXPERIMENT"
	columnGraphU4U8DetailExperimentOutEnv = "TREEDB_COLUMN_GRAPH_U4_U8_DETAIL_OUT"
	columnGraphU4U8DetailExperimentKEnv   = "TREEDB_COLUMN_GRAPH_U4_U8_DETAIL_KS"
)

var defaultColumnGraphU4U8DetailExperimentKs = []int{0, 4, 8, 16, 32, 64, 128}

// TestColumnGraphU4U8DetailQueryExperiment is an opt-in query-time viability
// probe for a possible packed scalar_u4 base plus selected scalar_u8 detail tier.
// It intentionally uses unpacked experiment arrays and does not claim production
// speed. The goal is to determine whether the accuracy/shortlist-quality lift is
// large enough to justify a durable u4/u8 codec after the u8/fp32 detail probe.
func TestColumnGraphU4U8DetailQueryExperiment(t *testing.T) {
	if os.Getenv(columnGraphU4U8DetailExperimentEnv) != "1" {
		t.Skipf("set %s=1 to run the scalar_u4 + scalar_u8 detail query-time experiment", columnGraphU4U8DetailExperimentEnv)
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
	detailKs, err := columnGraphU4U8DetailExperimentKsFromEnv(shape.dims)
	if err != nil {
		t.Fatalf("detail ks: %v", err)
	}
	maxK := 0
	for _, k := range detailKs {
		if k > maxK {
			maxK = k
		}
	}

	outDir := strings.TrimSpace(os.Getenv(columnGraphU4U8DetailExperimentOutEnv))
	if outDir == "" {
		outDir = filepath.Join(os.TempDir(), "treedb_u4_u8_detail_experiment_"+time.Now().Format("20060102_150405"))
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	dataset, vectors, invNorms := columnGraphU8FP32DetailExperimentVectors(t, shape.rows, shape.dims)
	u4Codes := columnGraphU4U8DetailScalarU4Codes(vectors, invNorms, shape.rows, shape.dims)
	u8Codes := columnGraphU8FP32DetailScalarU8Codes(vectors, invNorms, shape.rows, shape.dims)
	rowDeltaDims := columnGraphU4U8DetailRowDeltaDims(u4Codes, u8Codes, shape.rows, shape.dims, maxK)

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
		Selectors: []string{"u4z_base", "row_u4_u8_delta_topk", "query_score_error_topk"},
		Notes: []string{
			"query-time viability experiment only; no durable codec, no packed asset, no production scorer, and no hot-path speed claim",
			"base score uses zero-preserving signed scalar_u4 normalized cosine coding (15 active levels plus one reserved code); detail dimensions replace scalar_u4 per-dim contributions with scalar_u8 contributions",
			"row_u4_u8_delta_topk is query-independent and production-plausible; query_score_error_topk is a query-time ceiling selector",
		},
	}

	queryOrdinals := columnGraphU8FP32DetailQueryOrdinals(shape.rows, shape.queryOrdinal, shape.queryCount)
	for _, queryOrdinal := range queryOrdinals {
		queryReport := columnGraphU8FP32DetailExperimentQueryReport{QueryOrdinal: queryOrdinal}
		queryReport.Methods = columnGraphU4U8DetailEvaluateQuery(vectors, invNorms, u4Codes, u8Codes, rowDeltaDims, shape.rows, shape.dims, shape.topK, queryOrdinal, detailKs, maxK)
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
	if err := os.WriteFile(mdPath, []byte(columnGraphU4U8DetailRenderMarkdown(report)), 0o644); err != nil {
		t.Fatalf("write report Markdown: %v", err)
	}
	t.Logf("scalar_u4 + scalar_u8 detail query experiment report: %s", mdPath)
}

func columnGraphU4U8DetailExperimentKsFromEnv(dims int) ([]int, error) {
	raw := strings.TrimSpace(os.Getenv(columnGraphU4U8DetailExperimentKEnv))
	if raw == "" {
		return columnGraphU8FP32DetailNormalizeKs(defaultColumnGraphU4U8DetailExperimentKs, dims)
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' })
	ks := make([]int, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return nil, fmt.Errorf("%s=%q contains non-negative integer token %q", columnGraphU4U8DetailExperimentKEnv, raw, part)
		}
		ks = append(ks, value)
	}
	return columnGraphU8FP32DetailNormalizeKs(ks, dims)
}

func columnGraphU4U8DetailEvaluateQuery(vectors []float32, invNorms []float32, u4Codes []byte, u8Codes []byte, rowDeltaDims []uint16, rows, dims, topK, queryOrdinal int, detailKs []int, maxK int) []columnGraphU8FP32DetailExperimentMethodReport {
	qBase := queryOrdinal * dims
	queryInvNorm := float64(invNorms[queryOrdinal])
	qNorm := make([]float64, dims)
	q4Centered := make([]int16, dims)
	q8Centered := make([]int16, dims)
	for j := 0; j < dims; j++ {
		qNorm[j] = float64(vectors[qBase+j]) * queryInvNorm
		q4Centered[j] = columnGraphU4U8DetailCenteredU4Code(columnGraphU4U8DetailScalarU4Code(qNorm[j]))
		q8Centered[j] = columnGraphU8FP32DetailCenteredCode(columnVectorGraphScalarU8Code(float32(qNorm[j])))
	}
	exactScores := make([]float64, rows)
	u4Scores := make([]float64, rows)
	for row := 0; row < rows; row++ {
		base := row * dims
		invNorm := float64(invNorms[row])
		var exact float64
		var dot int64
		for j := 0; j < dims; j++ {
			exact += qNorm[j] * float64(vectors[base+j]) * invNorm
			dot += int64(q4Centered[j]) * int64(columnGraphU4U8DetailCenteredU4Code(u4Codes[base+j]))
		}
		exactScores[row] = exact
		u4Scores[row] = float64(dot) / columnGraphU4U8DetailCodeScale
	}
	exactOrder := columnGraphU8FP32DetailOrderScores(exactScores)
	baseBytes := float64(dims) / 2
	out := make([]columnGraphU8FP32DetailExperimentMethodReport, 0, 1+2*len(detailKs))
	out = append(out, columnGraphU8FP32DetailBuildMethodReport("u4z_base", "u4z_base", 0, baseBytes, 0, exactScores, exactOrder, u4Scores, topK))

	if maxK > 0 {
		rowScores := append([]float64(nil), u4Scores...)
		prevK := 0
		for _, k := range detailKs {
			if k == 0 {
				continue
			}
			columnGraphU4U8DetailApplyDims(rowScores, u4Codes, u8Codes, q4Centered, q8Centered, rowDeltaDims, rows, dims, maxK, prevK, k)
			out = append(out, columnGraphU8FP32DetailBuildMethodReport(fmt.Sprintf("u4z_u8_detail_row_delta_k%d", k), "row_u4_u8_delta_topk", k, baseBytes, float64(3*k), exactScores, exactOrder, rowScores, topK))
			prevK = k
		}

		queryDetailDims := columnGraphU4U8DetailQueryScoreErrorDims(u4Codes, u8Codes, q4Centered, q8Centered, rows, dims, maxK)
		queryScores := append([]float64(nil), u4Scores...)
		prevK = 0
		for _, k := range detailKs {
			if k == 0 {
				continue
			}
			columnGraphU4U8DetailApplyDims(queryScores, u4Codes, u8Codes, q4Centered, q8Centered, queryDetailDims, rows, dims, maxK, prevK, k)
			out = append(out, columnGraphU8FP32DetailBuildMethodReport(fmt.Sprintf("u4z_u8_detail_query_error_k%d", k), "query_score_error_topk", k, baseBytes, float64(3*k), exactScores, exactOrder, queryScores, topK))
			prevK = k
		}
	}
	return out
}

func columnGraphU4U8DetailScalarU4Codes(vectors []float32, invNorms []float32, rows, dims int) []byte {
	codes := make([]byte, rows*dims)
	for row := 0; row < rows; row++ {
		base := row * dims
		invNorm := float64(invNorms[row])
		for j := 0; j < dims; j++ {
			codes[base+j] = columnGraphU4U8DetailScalarU4Code(float64(vectors[base+j]) * invNorm)
		}
	}
	return codes
}

func columnGraphU4U8DetailScalarU4Code(value float64) byte {
	if math.IsNaN(value) {
		return 7
	}
	signed := int(math.Round(value * 7.0))
	if signed < -7 {
		signed = -7
	}
	if signed > 7 {
		signed = 7
	}
	return byte(signed + 7)
}

const columnGraphU4U8DetailCodeScale = 7.0 * 7.0

func columnGraphU4U8DetailCenteredU4Code(code byte) int16 {
	if code > 14 {
		code = 14
	}
	return int16(int(code) - 7)
}

func columnGraphU4U8DetailCenteredU4CodeAsUnit(code byte) float64 {
	return float64(columnGraphU4U8DetailCenteredU4Code(code)) / 7.0
}

func columnGraphU4U8DetailRowDeltaDims(u4Codes []byte, u8Codes []byte, rows, dims, maxK int) []uint16 {
	if maxK <= 0 {
		return nil
	}
	out := make([]uint16, rows*maxK)
	candidates := make([]columnGraphU8FP32DetailCandidate, dims)
	for row := 0; row < rows; row++ {
		base := row * dims
		for j := 0; j < dims; j++ {
			u4 := columnGraphU4U8DetailCenteredU4CodeAsUnit(u4Codes[base+j])
			u8 := columnGraphU8FP32DetailCenteredCodeAsUnit(u8Codes[base+j])
			candidates[j] = columnGraphU8FP32DetailCandidate{dim: j, score: math.Abs(u8 - u4)}
		}
		columnGraphU8FP32DetailSortCandidates(candidates)
		for k := 0; k < maxK; k++ {
			out[row*maxK+k] = uint16(candidates[k].dim)
		}
	}
	return out
}

func columnGraphU4U8DetailQueryScoreErrorDims(u4Codes []byte, u8Codes []byte, q4Centered []int16, q8Centered []int16, rows, dims, maxK int) []uint16 {
	out := make([]uint16, rows*maxK)
	candidates := make([]columnGraphU8FP32DetailCandidate, dims)
	for row := 0; row < rows; row++ {
		base := row * dims
		for j := 0; j < dims; j++ {
			correction := columnGraphU4U8DetailCorrection(u4Codes, u8Codes, q4Centered, q8Centered, base+j, j)
			candidates[j] = columnGraphU8FP32DetailCandidate{dim: j, score: math.Abs(correction)}
		}
		columnGraphU8FP32DetailSortCandidates(candidates)
		for k := 0; k < maxK; k++ {
			out[row*maxK+k] = uint16(candidates[k].dim)
		}
	}
	return out
}

func columnGraphU4U8DetailApplyDims(scores []float64, u4Codes []byte, u8Codes []byte, q4Centered []int16, q8Centered []int16, dimsByRow []uint16, rows, dims, maxK, startK, endK int) {
	for row := 0; row < rows; row++ {
		dimBase := row * maxK
		rowBase := row * dims
		for k := startK; k < endK; k++ {
			dim := int(dimsByRow[dimBase+k])
			scores[row] += columnGraphU4U8DetailCorrection(u4Codes, u8Codes, q4Centered, q8Centered, rowBase+dim, dim)
		}
	}
}

func columnGraphU4U8DetailCorrection(u4Codes []byte, u8Codes []byte, q4Centered []int16, q8Centered []int16, codeIndex, dim int) float64 {
	u8Contribution := float64(q8Centered[dim]) * float64(columnGraphU8FP32DetailCenteredCode(u8Codes[codeIndex])) / columnVectorGraphScalarU8CodeScale
	u4Contribution := float64(q4Centered[dim]) * float64(columnGraphU4U8DetailCenteredU4Code(u4Codes[codeIndex])) / columnGraphU4U8DetailCodeScale
	return u8Contribution - u4Contribution
}

func columnGraphU4U8DetailRenderMarkdown(report columnGraphU8FP32DetailExperimentReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# scalar_u4 + scalar_u8 detail query-time experiment\n\n")
	fmt.Fprintf(&b, "Generated: `%s`\n\n", report.GeneratedAt)
	fmt.Fprintf(&b, "This is an opt-in viability experiment, not a production codec or scorer. It uses the large TreeDB vector benchmark shape, computes scalar_u4 scores, and then applies selected scalar_u8 per-dimension replacement corrections at query time. Runtime is intentionally not interpreted as production speed.\n\n")
	fmt.Fprintf(&b, "| Rows | Dims | Dataset | TopK | Query ordinal | Query count | Detail K values |\n")
	fmt.Fprintf(&b, "| ---: | ---: | --- | ---: | ---: | ---: | --- |\n")
	fmt.Fprintf(&b, "| %d | %d | `%s` | %d | %d | %d | `%s` |\n\n", report.Shape.Rows, report.Shape.Dims, report.Shape.Dataset, report.Shape.TopK, report.Shape.QueryOrdinal, report.Shape.QueryCount, columnGraphU8FP32DetailFormatInts(report.DetailKs))
	fmt.Fprintf(&b, "Storage model: packed scalar_u4 costs `dims/2` bytes/vector; scalar_u8 detail costs `3*K` bytes/vector (`uint16 dim_id + uint8 value` per selected dimension).\n\n")
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
	fmt.Fprintf(&b, "%s=1 %s=10k_x_768 %s=clustered GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections -run '^TestColumnGraphU4U8DetailQueryExperiment$' -count=1\n", columnGraphU4U8DetailExperimentEnv, columnGraphScalarU8QuantizedBenchShapeEnv1926, columnGraphU8FP32DetailExperimentDatasetEnv)
	fmt.Fprintf(&b, "```\n")
	return b.String()
}
