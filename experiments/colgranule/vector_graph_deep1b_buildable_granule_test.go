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
	topGranulesList := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_TOP_GRANULES", []int{1, 4})
	ranks := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_PCA_RANKS", []int{32, 48, 64, 80, columnVectorGraphDeep1BDims})
	scanIters := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_BUILDABLE_SCAN_ITERS", 8)
	if baseRows <= 0 {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_BASE_ROWS=%d must be positive", baseRows)
	}
	if granuleRows <= 0 {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_ROWS=%d must be positive", granuleRows)
	}
	outDir := strings.TrimSpace(os.Getenv("COLUMN_VECTOR_DEEP1B_BUILDABLE_OUT"))
	if outDir == "" {
		outDir = filepath.Join(os.TempDir(), "gomap_deep1b_buildable_granule_scout_"+time.Now().Format("20060102_150405"))
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("create buildable granule scout output dir: %v", err)
	}

	data := columnVectorGraphDeep1BEnsureData(t, baseRows)
	baseRows = min(baseRows, data.baseHeader.Rows)
	baseFile, err := os.Open(data.basePath)
	if err != nil {
		t.Fatalf("open Deep1B base: %v", err)
	}
	defer baseFile.Close()
	var raw []byte
	_, vectors, err := columnVectorGraphDeep1BReadFbinVectorsAt(baseFile, data.baseHeader, 0, baseRows, raw, nil)
	if err != nil {
		t.Fatalf("read Deep1B base prefix rows=%d: %v", baseRows, err)
	}
	invNorms := columnVectorGraphDeep1BInvNorms(vectors, columnVectorGraphDeep1BDims)
	granules := columnVectorGraphDeep1BBuildRowIDContiguousGranules(vectors, baseRows, columnVectorGraphDeep1BDims, granuleRows)
	if len(granules) == 0 {
		t.Fatalf("no granules for baseRows=%d granuleRows=%d", baseRows, granuleRows)
	}

	report := columnVectorGraphDeep1BBuildableGranuleScoutReport{
		GeneratedAt:      time.Now().UTC().Format(time.RFC3339),
		OutputDir:        outDir,
		BasePath:         data.basePath,
		QueryPath:        data.queryPath,
		BaseRows:         baseRows,
		Dims:             columnVectorGraphDeep1BDims,
		Builder:          "row_id_contiguous",
		GranuleRows:      granuleRows,
		GranuleCount:     len(granules),
		RequestedQueries: append([]int(nil), queryIndexes...),
		TopGranules:      append([]int(nil), topGranulesList...),
		Ranks:            append([]int(nil), ranks...),
		ScanIters:        scanIters,
		Notes:            "first production/buildable granule scout; row-id-contiguous blocks are buildable storage units, but they are not expected to have nearest-neighbor locality",
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
			queryReport := columnVectorGraphDeep1BAnalyzeBuildableGranuleSelection(t, vectors, invNorms, query, queryInvNorm, globalScores, globalTopRows, selected, queryIndex, topGranules, ranks, scanIters)
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
	GeneratedAt      string                                               `json:"generated_at"`
	OutputDir        string                                               `json:"output_dir"`
	BasePath         string                                               `json:"base_path"`
	QueryPath        string                                               `json:"query_path"`
	BaseRows         int                                                  `json:"base_rows"`
	Dims             int                                                  `json:"dims"`
	Builder          string                                               `json:"builder"`
	GranuleRows      int                                                  `json:"granule_rows"`
	GranuleCount     int                                                  `json:"granule_count"`
	RequestedQueries []int                                                `json:"requested_queries"`
	TopGranules      []int                                                `json:"top_granules"`
	Ranks            []int                                                `json:"ranks"`
	ScanIters        int                                                  `json:"scan_iters"`
	Queries          []columnVectorGraphDeep1BBuildableGranuleQueryReport `json:"queries"`
	Notes            string                                               `json:"notes,omitempty"`
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
	Rows               int     `json:"rows"`
	CentroidCosine     float64 `json:"centroid_cosine_to_query"`
	GlobalTop10Overlap int     `json:"global_top10_overlap"`
	GlobalTop20Overlap int     `json:"global_top20_overlap"`
}

type columnVectorGraphDeep1BBuildableGranule struct {
	Ordinal       int
	FirstRow      int
	Rows          int
	Centroid      []float32
	CentroidInv   float32
	CentroidScore float64
}

func columnVectorGraphDeep1BBuildRowIDContiguousGranules(vectors []float32, rows int, dims int, granuleRows int) []columnVectorGraphDeep1BBuildableGranule {
	granules := make([]columnVectorGraphDeep1BBuildableGranule, 0, (rows+granuleRows-1)/granuleRows)
	for first := 0; first < rows; first += granuleRows {
		count := min(granuleRows, rows-first)
		centroid := make([]float32, dims)
		for row := 0; row < count; row++ {
			base := (first + row) * dims
			for j := 0; j < dims; j++ {
				centroid[j] += vectors[base+j]
			}
		}
		invRows := float32(1 / float64(count))
		for j := range centroid {
			centroid[j] *= invRows
		}
		granules = append(granules, columnVectorGraphDeep1BBuildableGranule{
			Ordinal:     len(granules),
			FirstRow:    first,
			Rows:        count,
			Centroid:    centroid,
			CentroidInv: float32(columnVectorGraphDeep1BInvNorm(centroid)),
		})
	}
	return granules
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

func columnVectorGraphDeep1BAnalyzeBuildableGranuleSelection(tb testing.TB, vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, globalScores []float32, globalTopRows []int, selected []columnVectorGraphDeep1BBuildableGranule, queryIndex int, topGranules int, ranks []int, scanIters int) columnVectorGraphDeep1BBuildableGranuleQueryReport {
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
		report := columnVectorGraphDeep1BSelectedGranuleReport{
			Ordinal:        granule.Ordinal,
			FirstRow:       granule.FirstRow,
			Rows:           granule.Rows,
			CentroidCosine: granule.CentroidScore,
		}
		for row := granule.FirstRow; row < granule.FirstRow+granule.Rows; row++ {
			candidateSet[row] = struct{}{}
			candidateExact = append(candidateExact, globalScores[row])
		}
		report.GlobalTop10Overlap = columnVectorGraphDeep1BCountTopRowsInRange(globalTopRows, 10, granule.FirstRow, granule.FirstRow+granule.Rows)
		report.GlobalTop20Overlap = columnVectorGraphDeep1BCountTopRowsInRange(globalTopRows, 20, granule.FirstRow, granule.FirstRow+granule.Rows)
		selectedReports = append(selectedReports, report)
	}
	q := columnVectorGraphDeep1BBuildableGranuleQueryReport{
		QueryIndex:            queryIndex,
		Builder:               "row_id_contiguous",
		TopGranules:           topGranules,
		CandidateRows:         candidateRows,
		SelectedGranules:      selectedReports,
		CandidateExactMargins: columnVectorGraphDeep1BScoreMarginMetrics(candidateExact),
		Notes:                 "buildable row-id-contiguous granule scout; codec recalls are conditional on the routed candidate union, while routing recalls measure how many global exact winners reached that union",
	}
	q.RoutingTop10InCandidates = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 10, candidateSet)
	q.RoutingTop20InCandidates = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 20, candidateSet)
	q.RoutingTop50InCandidates = columnVectorGraphDeep1BCountRowsInSet(globalTopRows, 50, candidateSet)
	q.RoutingRecallAt10 = float64(q.RoutingTop10InCandidates) / float64(min(10, len(globalTopRows)))
	q.RoutingRecallAt20 = float64(q.RoutingTop20InCandidates) / float64(min(20, len(globalTopRows)))
	q.RoutingRecallAt50 = float64(q.RoutingTop50InCandidates) / float64(min(50, len(globalTopRows)))
	q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildableScalarMethod(vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, 8, "per_dim", "reconstructed", scanIters))
	q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildableScalarMethod(vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, 4, "per_dim", "reconstructed", scanIters))
	minSelectedRows := candidateRows
	for _, granule := range selected {
		minSelectedRows = min(minSelectedRows, granule.Rows)
	}
	for _, rank := range columnVectorGraphDeep1BFilterRanksForRows(tb, ranks, minSelectedRows, dims) {
		q.Methods = append(q.Methods, columnVectorGraphDeep1BEvaluateBuildablePCAMethod(tb, vectors, invNorms, query, queryInvNorm, candidateExact, q.CandidateExactMargins, selected, rank, scanIters))
	}
	return q
}

func columnVectorGraphDeep1BEvaluateBuildableScalarMethod(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, selected []columnVectorGraphDeep1BBuildableGranule, bits int, policy string, normMode string, scanIters int) columnVectorGraphDeep1BGroundtruthMethodReport {
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
		gVectors := columnVectorGraphDeep1BSliceRows(vectors, granule.FirstRow, granule.Rows, dims)
		gInvNorms := invNorms[granule.FirstRow : granule.FirstRow+granule.Rows]
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
		fmt.Sprintf("buildable_rowid_scalar_u%d_affine_%s_%s", bits, policy, normMode),
		rowCodeBytes/float64(totalRows),
		metadataBytes/float64(totalRows),
		buildNanos,
		"production/buildable scout over row-id-contiguous granules; codec recall is conditional on centroid-routed candidate union",
	)
	method.ScanNanosPerVector = scanNanos / float64(totalRows)
	method.MeanRelativeL2 = meanRelL2 / float64(totalRows)
	method.MaxRelativeL2 = maxRelL2
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approxScores, margins)
	return method
}

func columnVectorGraphDeep1BEvaluateBuildablePCAMethod(tb testing.TB, vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, selected []columnVectorGraphDeep1BBuildableGranule, rank int, scanIters int) columnVectorGraphDeep1BGroundtruthMethodReport {
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
		gVectors := columnVectorGraphDeep1BSliceRows(vectors, granule.FirstRow, granule.Rows, dims)
		gInvNorms := invNorms[granule.FirstRow : granule.FirstRow+granule.Rows]
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
		fmt.Sprintf("buildable_rowid_local_pca_i8_rank%d", rank),
		float64(rank),
		metadataBytes/float64(totalRows),
		buildNanos,
		"production/buildable scout over row-id-contiguous granules; local PCA metadata is amortized over each selected granule, and codec recall is conditional on centroid-routed candidate union",
	)
	method.ScanNanosPerVector = scanNanos / float64(totalRows)
	method.MeanRelativeL2 = meanRelL2 / float64(totalRows)
	method.MaxRelativeL2 = maxRelL2
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approxScores, margins)
	return method
}

func columnVectorGraphDeep1BSliceRows(vectors []float32, firstRow int, rows int, dims int) []float32 {
	start := firstRow * dims
	end := start + rows*dims
	return vectors[start:end]
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

func columnVectorGraphDeep1BCountTopRowsInRange(rows []int, topK int, start int, end int) int {
	topK = min(topK, len(rows))
	var count int
	for _, row := range rows[:topK] {
		if row >= start && row < end {
			count++
		}
	}
	return count
}

func columnVectorGraphDeep1BRenderBuildableGranuleScoutMarkdown(report columnVectorGraphDeep1BBuildableGranuleScoutReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Deep1B Buildable Granule Scout\n\n")
	fmt.Fprintf(&b, "Generated: `%s`\n\n", report.GeneratedAt)
	fmt.Fprintf(&b, "- Regime: `buildable_granule_scout`\n")
	fmt.Fprintf(&b, "- Builder: `%s`\n", report.Builder)
	fmt.Fprintf(&b, "- Base path: `%s`\n", report.BasePath)
	fmt.Fprintf(&b, "- Base rows: `%d`\n", report.BaseRows)
	fmt.Fprintf(&b, "- Dims: `%d`\n", report.Dims)
	fmt.Fprintf(&b, "- Granule rows: `%d`\n", report.GranuleRows)
	fmt.Fprintf(&b, "- Granules: `%d`\n", report.GranuleCount)
	fmt.Fprintf(&b, "- Scan iterations: `%d`\n\n", report.ScanIters)
	fmt.Fprintf(&b, "This is the first **production/buildable granule** scout. It deliberately uses row-id-contiguous blocks because they are real storage units TreeDB can build without oracle labels. The result should not be read as a good locality builder; it is a control that separates routing/locality failure from codec failure. PQ/OPQ/residual-PQ/LOPQ are still pending because they require real train/eval splits and trained codebooks.\n\n")
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
	fmt.Fprintf(&b, "| Query | Top granules | Method | Row-code B/vector | Metadata B/vector | Build ms | Compressed top10 | Top10 in approx@20 | Top10 in approx@50 | Top20 in approx@50 | Rerank@50 recall@10 | Mean score err | Err/gap10 | Scan ns/vector |\n")
	fmt.Fprintf(&b, "| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		for _, method := range q.Methods {
			fmt.Fprintf(&b, "| %d | %d | `%s` | %.2f | %.2f | %.3f | %d/10 | %s | %s | %s | %.2f | %.5f | %.2f | %.2f |\n",
				q.QueryIndex,
				q.TopGranules,
				method.Name,
				method.RowCodeBytesPerVector,
				method.MetadataBytesPerVector,
				float64(method.BuildNanos)/1e6,
				method.Top10Overlap,
				columnVectorGraphDeep1BFormatOverlap(method.Top10InApproxTop20, method.Top10RecallAt20, 10),
				columnVectorGraphDeep1BFormatOverlap(method.Top10InApproxTop50, method.Top10RecallAt50, 10),
				columnVectorGraphDeep1BFormatOverlap(method.Top20InApproxTop50, method.Top20RecallAt50, 20),
				method.ExactRerankRecallAt10FromTop50,
				method.MeanScoreError,
				method.MeanErrorOverGap10,
				method.ScanNanosPerVector,
			)
		}
	}
	columnVectorGraphDeep1BRenderBuildableAggregateMarkdown(&b, report)
	fmt.Fprintf(&b, "\n## Selected Granules\n\n")
	fmt.Fprintf(&b, "| Query | Top granules | Granule | First row | Rows | Centroid cos(query) | Global top10 in granule | Global top20 in granule |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		for _, granule := range q.SelectedGranules {
			fmt.Fprintf(&b, "| %d | %d | %d | %d | %d | %.5f | %d/10 | %d/20 |\n",
				q.QueryIndex,
				q.TopGranules,
				granule.Ordinal,
				granule.FirstRow,
				granule.Rows,
				granule.CentroidCosine,
				granule.GlobalTop10Overlap,
				granule.GlobalTop20Overlap,
			)
		}
	}
	return b.String()
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
		top10      []int
		top10At20  []int
		top10At50  []int
		top20At50  []int
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
			agg.top10 = append(agg.top10, method.Top10Overlap)
			agg.top10At20 = append(agg.top10At20, method.Top10InApproxTop20)
			agg.top10At50 = append(agg.top10At50, method.Top10InApproxTop50)
			agg.top20At50 = append(agg.top20At50, method.Top20InApproxTop50)
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
	fmt.Fprintf(b, "| Method | Queries | Row-code B/vector | Metadata B/vector | Avg build ms | p50 compressed top10 | worst compressed top10 | p50 top10@20 | worst top10@20 | p50 top10@50 | worst top10@50 | p50 top20@50 | worst top20@50 | Avg score err | Avg err/gap10 | Avg scan ns/vector |\n")
	fmt.Fprintf(b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, name := range names {
		agg := byName[name]
		sort.Ints(agg.top10)
		sort.Ints(agg.top10At20)
		sort.Ints(agg.top10At50)
		sort.Ints(agg.top20At50)
		count := float64(max(1, agg.count))
		fmt.Fprintf(b, "| `%s` | %d | %.2f | %.2f | %.3f | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/20 | %d/20 | %.5f | %.2f | %.2f |\n",
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
			columnVectorGraphDeep1BIntQuantile(agg.top20At50, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top20At50, 0),
			agg.scoreError/count,
			agg.gap10/count,
			agg.scanNanos/count,
		)
	}
}
