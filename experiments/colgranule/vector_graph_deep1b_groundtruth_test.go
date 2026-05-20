package colgranule

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gonum.org/v1/gonum/mat"
)

func TestColumnVectorGraphDeep1BGroundtruthLocality(t *testing.T) {
	if os.Getenv("COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_LOCALITY") != "1" {
		t.Skip("set COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_LOCALITY=1 to run the opt-in Deep1B groundtruth locality probe")
	}
	queryIndexes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_QUERIES", []int{0})
	ranks := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_PCA_RANKS", []int{8, 16, 32, 64, columnVectorGraphDeep1BDims})
	localBaseRows := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_BASE_ROWS", 1_000_000)
	fetchBase1B := os.Getenv("COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_BASE1B") == "1"
	fetchConcurrency := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_CONCURRENCY", 16)
	outDir := strings.TrimSpace(os.Getenv("COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_OUT"))
	if outDir == "" {
		outDir = filepath.Join(os.TempDir(), "gomap_deep1b_groundtruth_locality_"+time.Now().Format("20060102_150405"))
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("create groundtruth locality output dir: %v", err)
	}

	queryPath, queryHeader := columnVectorGraphDeep1BEnsureQueryData(t)
	truthPath, truthHeader := columnVectorGraphDeep1BEnsureGroundtruth(t)
	basePath, baseHeader, baseFile := columnVectorGraphDeep1BOpenOptionalBase(t, localBaseRows, !fetchBase1B)
	if baseFile != nil {
		defer baseFile.Close()
	}

	cache := columnVectorGraphDeep1BRemoteRowCache{
		client: &http.Client{Timeout: columnVectorGraphDeep1BDownloadTimeout(t)},
		rows:   make(map[int][]float32),
	}
	report := columnVectorGraphDeep1BGroundtruthLocalityReport{
		GeneratedAt:      time.Now().UTC().Format(time.RFC3339),
		OutputDir:        outDir,
		BasePath:         basePath,
		BaseRows:         baseHeader.Rows,
		QueryPath:        queryPath,
		GroundtruthPath:  truthPath,
		GroundtruthRows:  truthHeader.Rows,
		GroundtruthDims:  truthHeader.Dims,
		Dims:             columnVectorGraphDeep1BDims,
		FetchBase1B:      fetchBase1B,
		FetchConcurrency: fetchConcurrency,
		RequestedQueries: append([]int(nil), queryIndexes...),
		Ranks:            append([]int(nil), ranks...),
	}
	for _, queryIndex := range queryIndexes {
		if queryIndex < 0 || queryIndex >= queryHeader.Rows {
			t.Fatalf("query index=%d outside query rows=%d", queryIndex, queryHeader.Rows)
		}
		query := columnVectorGraphDeep1BReadQuery(t, queryPath, queryHeader, queryIndex)
		truthRows := columnVectorGraphDeep1BReadGroundtruthRow(t, truthPath, truthHeader, queryIndex)
		result := columnVectorGraphDeep1BAnalyzeGroundtruthQuery(t, baseFile, baseHeader, query, queryIndex, truthRows, ranks, fetchBase1B, fetchConcurrency, &cache)
		report.Queries = append(report.Queries, result)
	}
	if err := columnVectorGraphDeep1BWriteJSON(filepath.Join(outDir, "results.json"), report); err != nil {
		t.Fatalf("write groundtruth locality JSON: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "report.md"), []byte(columnVectorGraphDeep1BRenderGroundtruthLocalityMarkdown(report)), 0o644); err != nil {
		t.Fatalf("write groundtruth locality Markdown: %v", err)
	}
	t.Logf("Deep1B groundtruth locality report: %s", filepath.Join(outDir, "report.md"))
}

type columnVectorGraphDeep1BIbinHeader struct {
	Rows int
	Dims int
	Size int64
}

type columnVectorGraphDeep1BGroundtruthLocalityReport struct {
	GeneratedAt      string                                          `json:"generated_at"`
	OutputDir        string                                          `json:"output_dir"`
	BasePath         string                                          `json:"base_path"`
	BaseRows         int                                             `json:"base_rows"`
	QueryPath        string                                          `json:"query_path"`
	GroundtruthPath  string                                          `json:"groundtruth_path"`
	GroundtruthRows  int                                             `json:"groundtruth_rows"`
	GroundtruthDims  int                                             `json:"groundtruth_dims"`
	Dims             int                                             `json:"dims"`
	FetchBase1B      bool                                            `json:"fetch_base_1b"`
	FetchConcurrency int                                             `json:"fetch_concurrency"`
	RequestedQueries []int                                           `json:"requested_queries"`
	Ranks            []int                                           `json:"ranks"`
	Queries          []columnVectorGraphDeep1BGroundtruthQueryReport `json:"queries"`
}

type columnVectorGraphDeep1BGroundtruthQueryReport struct {
	QueryIndex                 int                                                    `json:"query_index"`
	GroundtruthCount           int                                                    `json:"groundtruth_count"`
	LoadedCount                int                                                    `json:"loaded_count"`
	LocalBaseCount             int                                                    `json:"local_base_count"`
	RemoteBase1BCount          int                                                    `json:"remote_base_1b_count"`
	MissingCount               int                                                    `json:"missing_count"`
	FirstGroundtruthRows       []int                                                  `json:"first_groundtruth_rows"`
	FirstLoadedRows            []int                                                  `json:"first_loaded_rows"`
	GroundtruthTop10Agreement  int                                                    `json:"groundtruth_top10_agreement"`
	CentroidNorm               float64                                                `json:"centroid_norm"`
	CentroidCosineToQuery      float64                                                `json:"centroid_cosine_to_query"`
	AveragePairwiseCosine      float64                                                `json:"average_pairwise_cosine"`
	ScoreQuantiles             map[string]float64                                     `json:"score_quantiles"`
	ScoreMargins               map[string]float64                                     `json:"score_margins"`
	MinRankRecallAt10Full      int                                                    `json:"min_rank_recall_at_10_full,omitempty"`
	MinRankRecallAt10GE90      int                                                    `json:"min_rank_recall_at_10_ge_90,omitempty"`
	MinRankTop10InApprox20     int                                                    `json:"min_rank_top10_in_approx20,omitempty"`
	MinRankTop10InApprox50     int                                                    `json:"min_rank_top10_in_approx50,omitempty"`
	MinRankTop20InApprox50GE95 int                                                    `json:"min_rank_top20_in_approx50_ge_95,omitempty"`
	PCA                        []columnVectorGraphDeep1BGroundtruthPCAQueryRankReport `json:"pca"`
	Methods                    []columnVectorGraphDeep1BGroundtruthMethodReport       `json:"methods"`
	Notes                      string                                                 `json:"notes,omitempty"`
}

type columnVectorGraphDeep1BGroundtruthPCAQueryRankReport struct {
	Rank                int     `json:"rank"`
	VarianceCaptured    float64 `json:"variance_captured"`
	Top10Overlap        int     `json:"top10_overlap"`
	RecallAt10          float64 `json:"recall_at_10"`
	Top20Overlap        int     `json:"top20_overlap,omitempty"`
	RecallAt20          float64 `json:"recall_at_20,omitempty"`
	Top50Overlap        int     `json:"top50_overlap,omitempty"`
	RecallAt50          float64 `json:"recall_at_50,omitempty"`
	Top10InApproxTop20  int     `json:"top10_in_approx_top20,omitempty"`
	Top10RecallAt20     float64 `json:"top10_recall_at_20,omitempty"`
	Top10InApproxTop50  int     `json:"top10_in_approx_top50,omitempty"`
	Top10RecallAt50     float64 `json:"top10_recall_at_50,omitempty"`
	Top10InApproxTop100 int     `json:"top10_in_approx_top100,omitempty"`
	Top10RecallAt100    float64 `json:"top10_recall_at_100,omitempty"`
	Top20InApproxTop50  int     `json:"top20_in_approx_top50,omitempty"`
	Top20RecallAt50     float64 `json:"top20_recall_at_50,omitempty"`
	Top20InApproxTop100 int     `json:"top20_in_approx_top100,omitempty"`
	Top20RecallAt100    float64 `json:"top20_recall_at_100,omitempty"`
	MeanScoreError      float64 `json:"mean_score_error"`
	MaxScoreError       float64 `json:"max_score_error"`
	MeanRelativeL2      float64 `json:"mean_relative_l2"`
	MaxRelativeL2       float64 `json:"max_relative_l2"`
}

type columnVectorGraphDeep1BGroundtruthMethodReport struct {
	Regime                          string  `json:"regime"`
	Family                          string  `json:"family"`
	Name                            string  `json:"name"`
	RowCodeBytesPerVector           float64 `json:"row_code_bytes_per_vector"`
	MetadataBytesPerVector          float64 `json:"metadata_bytes_per_vector"`
	TotalBytesPerVector             float64 `json:"total_bytes_per_vector"`
	MetadataStatus                  string  `json:"metadata_status"`
	BuildNanos                      int64   `json:"build_nanos"`
	ScanNanosPerVector              float64 `json:"scan_nanos_per_vector"`
	Top10Overlap                    int     `json:"top10_overlap"`
	RecallAt10                      float64 `json:"recall_at_10"`
	Top20Overlap                    int     `json:"top20_overlap,omitempty"`
	RecallAt20                      float64 `json:"recall_at_20,omitempty"`
	Top50Overlap                    int     `json:"top50_overlap,omitempty"`
	RecallAt50                      float64 `json:"recall_at_50,omitempty"`
	Top10InApproxTop20              int     `json:"top10_in_approx_top20,omitempty"`
	Top10RecallAt20                 float64 `json:"top10_recall_at_20,omitempty"`
	Top10InApproxTop50              int     `json:"top10_in_approx_top50,omitempty"`
	Top10RecallAt50                 float64 `json:"top10_recall_at_50,omitempty"`
	Top10InApproxTop100             int     `json:"top10_in_approx_top100,omitempty"`
	Top10RecallAt100                float64 `json:"top10_recall_at_100,omitempty"`
	Top20InApproxTop50              int     `json:"top20_in_approx_top50,omitempty"`
	Top20RecallAt50                 float64 `json:"top20_recall_at_50,omitempty"`
	Top20InApproxTop100             int     `json:"top20_in_approx_top100,omitempty"`
	Top20RecallAt100                float64 `json:"top20_recall_at_100,omitempty"`
	ExactRerankRecallAt10FromTop20  float64 `json:"exact_rerank_recall_at_10_from_top20,omitempty"`
	ExactRerankRecallAt10FromTop50  float64 `json:"exact_rerank_recall_at_10_from_top50,omitempty"`
	ExactRerankRecallAt10FromTop100 float64 `json:"exact_rerank_recall_at_10_from_top100,omitempty"`
	MeanScoreError                  float64 `json:"mean_score_error"`
	MaxScoreError                   float64 `json:"max_score_error"`
	MeanErrorOverGap10              float64 `json:"mean_error_over_gap_10_11,omitempty"`
	MaxErrorOverGap10               float64 `json:"max_error_over_gap_10_11,omitempty"`
	MeanErrorOverGap20              float64 `json:"mean_error_over_gap_20_21,omitempty"`
	MaxErrorOverGap20               float64 `json:"max_error_over_gap_20_21,omitempty"`
	MeanErrorOverGap50              float64 `json:"mean_error_over_gap_50_51,omitempty"`
	MaxErrorOverGap50               float64 `json:"max_error_over_gap_50_51,omitempty"`
	MeanRelativeL2                  float64 `json:"mean_relative_l2,omitempty"`
	MaxRelativeL2                   float64 `json:"max_relative_l2,omitempty"`
	Notes                           string  `json:"notes,omitempty"`
}

type columnVectorGraphDeep1BRemoteRowCache struct {
	mu     sync.Mutex
	client *http.Client
	rows   map[int][]float32
}

type columnVectorGraphDeep1BLoadedGroundtruthVector struct {
	rowID  int
	vector []float32
	source string
	ok     bool
}

func columnVectorGraphDeep1BAnalyzeGroundtruthQuery(tb testing.TB, baseFile *os.File, baseHeader columnVectorGraphDeep1BFbinHeader, query []float32, queryIndex int, truthRows []int, ranks []int, fetchBase1B bool, fetchConcurrency int, cache *columnVectorGraphDeep1BRemoteRowCache) columnVectorGraphDeep1BGroundtruthQueryReport {
	tb.Helper()
	loadedRows := make([]int, 0, len(truthRows))
	vectors := make([]float32, 0, len(truthRows)*columnVectorGraphDeep1BDims)
	var localCount int
	var remoteCount int
	var missingCount int
	for _, loaded := range columnVectorGraphDeep1BLoadGroundtruthVectors(tb, baseFile, baseHeader, truthRows, fetchBase1B, fetchConcurrency, cache) {
		if !loaded.ok {
			missingCount++
			continue
		}
		if loaded.source == "local" {
			localCount++
		} else if loaded.source == "remote_base_1b" {
			remoteCount++
		}
		loadedRows = append(loadedRows, loaded.rowID)
		vectors = append(vectors, loaded.vector...)
	}
	report := columnVectorGraphDeep1BGroundtruthQueryReport{
		QueryIndex:                 queryIndex,
		GroundtruthCount:           len(truthRows),
		LoadedCount:                len(loadedRows),
		LocalBaseCount:             localCount,
		RemoteBase1BCount:          remoteCount,
		MissingCount:               missingCount,
		FirstGroundtruthRows:       append([]int(nil), truthRows[:min(10, len(truthRows))]...),
		FirstLoadedRows:            append([]int(nil), loadedRows[:min(10, len(loadedRows))]...),
		MinRankRecallAt10Full:      -1,
		MinRankRecallAt10GE90:      -1,
		MinRankTop10InApprox20:     -1,
		MinRankTop10InApprox50:     -1,
		MinRankTop20InApprox50GE95: -1,
	}
	if len(loadedRows) < 2 {
		report.Notes = "fewer than two groundtruth rows are available from the local base file; enable COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_BASE1B=1 or provide base.1B.fbin-compatible rows"
		return report
	}
	rows := len(loadedRows)
	dims := columnVectorGraphDeep1BDims
	invNorms := columnVectorGraphDeep1BInvNorms(vectors, dims)
	queryInvNorm := float32(columnVectorGraphDeep1BInvNorm(query))
	exactScores := make([]float32, rows)
	columnVectorGraphDeep1BScorePrefixInto(query, queryInvNorm, vectors, invNorms, dims, dims, exactScores)
	exactTop10 := make([]int, min(10, rows))
	exactTop10Scores := make([]float32, len(exactTop10))
	columnVectorGraphDeep1BTopKFromScores(exactScores, len(exactTop10), exactTop10, exactTop10Scores)
	report.GroundtruthTop10Agreement = columnVectorGraphDeep1BGroundtruthAgreement(loadedRows, exactTop10, len(exactTop10))
	report.CentroidNorm, report.CentroidCosineToQuery, report.AveragePairwiseCosine = columnVectorGraphDeep1BLocalityMetrics(query, vectors, dims)
	report.ScoreQuantiles = columnVectorGraphDeep1BQuantiles(exactScores, []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1})
	report.ScoreMargins = columnVectorGraphDeep1BScoreMarginMetrics(exactScores)
	scanIters := columnVectorGraphDeep1BEnvInt(tb, "COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_SCAN_ITERS", 512)

	validRanks := columnVectorGraphDeep1BFilterRanksForRows(tb, ranks, rows, dims)
	pcaBuildStart := time.Now()
	model := columnVectorGraphDeep1BFitLocalPCAModel(tb, vectors, rows, dims, validRanks)
	pcaBuildNanos := time.Since(pcaBuildStart).Nanoseconds()
	for _, rank := range validRanks {
		encodeStart := time.Now()
		encoding := columnVectorGraphDeep1BEncodeLocalPCARank(vectors, invNorms, model, rank, query, queryInvNorm, rows, dims)
		encodeNanos := time.Since(encodeStart).Nanoseconds()
		maxScoreError, meanScoreError := columnVectorGraphDeep1BScoreErrorMetrics(exactScores, encoding.approxScores)
		row := columnVectorGraphDeep1BGroundtruthPCAQueryRankReport{
			Rank:             rank,
			VarianceCaptured: model.varianceByRank[rank],
			MeanScoreError:   meanScoreError,
			MaxScoreError:    maxScoreError,
			MeanRelativeL2:   encoding.meanRelativeL2,
			MaxRelativeL2:    encoding.maxRelativeL2,
		}
		row.Top10Overlap, row.RecallAt10 = columnVectorGraphDeep1BApproxRecall(exactScores, encoding.approxScores, 10)
		if rows >= 20 {
			row.Top20Overlap, row.RecallAt20 = columnVectorGraphDeep1BApproxRecall(exactScores, encoding.approxScores, 20)
			row.Top10InApproxTop20, row.Top10RecallAt20 = columnVectorGraphDeep1BCandidateRecall(exactScores, encoding.approxScores, 10, 20)
		}
		if rows >= 50 {
			row.Top50Overlap, row.RecallAt50 = columnVectorGraphDeep1BApproxRecall(exactScores, encoding.approxScores, 50)
			row.Top10InApproxTop50, row.Top10RecallAt50 = columnVectorGraphDeep1BCandidateRecall(exactScores, encoding.approxScores, 10, 50)
			row.Top20InApproxTop50, row.Top20RecallAt50 = columnVectorGraphDeep1BCandidateRecall(exactScores, encoding.approxScores, 20, 50)
		}
		if rows >= 100 {
			row.Top10InApproxTop100, row.Top10RecallAt100 = columnVectorGraphDeep1BCandidateRecall(exactScores, encoding.approxScores, 10, 100)
			row.Top20InApproxTop100, row.Top20RecallAt100 = columnVectorGraphDeep1BCandidateRecall(exactScores, encoding.approxScores, 20, 100)
		}
		if row.RecallAt10 == 1 && report.MinRankRecallAt10Full < 0 {
			report.MinRankRecallAt10Full = rank
		}
		if row.RecallAt10 >= 0.9 && report.MinRankRecallAt10GE90 < 0 {
			report.MinRankRecallAt10GE90 = rank
		}
		if row.Top10InApproxTop20 == 10 && report.MinRankTop10InApprox20 < 0 {
			report.MinRankTop10InApprox20 = rank
		}
		if row.Top10InApproxTop50 == 10 && report.MinRankTop10InApprox50 < 0 {
			report.MinRankTop10InApprox50 = rank
		}
		if row.Top20InApproxTop50 >= 19 && report.MinRankTop20InApprox50GE95 < 0 {
			report.MinRankTop20InApprox50GE95 = rank
		}
		report.PCA = append(report.PCA, row)

		method := columnVectorGraphDeep1BNewGroundtruthMethodReport(
			"local_pca",
			fmt.Sprintf("local_pca_i8_rank%d", rank),
			float64(rank),
			float64(model.centroidMetaBytes+rank*dims*2+rank*2+rows*2)/float64(rows),
			pcaBuildNanos+encodeNanos,
			"official top100 local-neighborhood upper-bound probe; fp16 centroid+basis+scales+invNorms metadata is not amortized like a production granule",
		)
		pcaScorer := columnVectorGraphDeep1BPrepareLocalPCAScorer(model, encoding, query, rows, dims)
		method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
			pcaScorer.scoreInto(encoding, queryInvNorm, rows, dst)
		})
		method.MeanRelativeL2 = encoding.meanRelativeL2
		method.MaxRelativeL2 = encoding.maxRelativeL2
		columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, encoding.approxScores, report.ScoreMargins)
		report.Methods = append(report.Methods, method)
	}
	report.Methods = append(report.Methods, columnVectorGraphDeep1BEvaluateGroundtruthPCABasisVariants(tb, vectors, invNorms, query, queryInvNorm, exactScores, report.ScoreMargins, rows, dims, validRanks, scanIters)...)
	report.Methods = append(report.Methods, columnVectorGraphDeep1BEvaluateGroundtruthScalarMethods(vectors, invNorms, query, queryInvNorm, exactScores, report.ScoreMargins, rows, dims, scanIters)...)
	report.Methods = append(report.Methods, columnVectorGraphDeep1BEvaluateGroundtruthQueryAxisOracle(vectors, invNorms, query, queryInvNorm, exactScores, report.ScoreMargins, rows, dims, scanIters)...)
	report.Methods = append(report.Methods, columnVectorGraphDeep1BEvaluateGroundtruthRandomRotationMethods(vectors, invNorms, query, queryInvNorm, exactScores, report.ScoreMargins, rows, dims, scanIters, int64(queryIndex))...)
	return report
}

func columnVectorGraphDeep1BLoadGroundtruthVectors(tb testing.TB, baseFile *os.File, baseHeader columnVectorGraphDeep1BFbinHeader, truthRows []int, fetchBase1B bool, fetchConcurrency int, cache *columnVectorGraphDeep1BRemoteRowCache) []columnVectorGraphDeep1BLoadedGroundtruthVector {
	tb.Helper()
	loaded := make([]columnVectorGraphDeep1BLoadedGroundtruthVector, len(truthRows))
	remoteIndexes := make([]int, 0, len(truthRows))
	for i, rowID := range truthRows {
		loaded[i].rowID = rowID
		if baseFile != nil && rowID >= 0 && rowID < baseHeader.Rows {
			_, vector, err := columnVectorGraphDeep1BReadFbinVectorsAt(baseFile, baseHeader, rowID, 1, nil, nil)
			if err != nil {
				tb.Fatalf("read local Deep1B row %d: %v", rowID, err)
			}
			loaded[i].vector = append([]float32(nil), vector...)
			loaded[i].source = "local"
			loaded[i].ok = true
			continue
		}
		if !fetchBase1B || rowID < 0 {
			continue
		}
		if cached, ok := cache.get(rowID); ok {
			loaded[i].vector = cached
			loaded[i].source = "remote_base_1b"
			loaded[i].ok = true
			continue
		}
		remoteIndexes = append(remoteIndexes, i)
	}
	if len(remoteIndexes) == 0 {
		return loaded
	}
	if fetchConcurrency <= 0 {
		fetchConcurrency = 1
	}
	fetchConcurrency = min(fetchConcurrency, len(remoteIndexes))
	type fetchResult struct {
		index  int
		rowID  int
		vector []float32
		err    error
	}
	jobs := make(chan int)
	results := make(chan fetchResult, len(remoteIndexes))
	for worker := 0; worker < fetchConcurrency; worker++ {
		go func() {
			for index := range jobs {
				rowID := truthRows[index]
				vector, err := columnVectorGraphDeep1BFetchBase1BRowErr(cache.client, rowID, columnVectorGraphDeep1BDims)
				results <- fetchResult{index: index, rowID: rowID, vector: vector, err: err}
			}
		}()
	}
	for _, index := range remoteIndexes {
		jobs <- index
	}
	close(jobs)
	for range remoteIndexes {
		result := <-results
		if result.err != nil {
			tb.Fatalf("fetch Deep1B base.1B row %d: %v", result.rowID, result.err)
		}
		cache.put(result.rowID, result.vector)
		loaded[result.index].vector = append([]float32(nil), result.vector...)
		loaded[result.index].source = "remote_base_1b"
		loaded[result.index].ok = true
	}
	return loaded
}

func columnVectorGraphDeep1BLoadGroundtruthVector(tb testing.TB, baseFile *os.File, baseHeader columnVectorGraphDeep1BFbinHeader, rowID int, fetchBase1B bool, cache *columnVectorGraphDeep1BRemoteRowCache) ([]float32, string, bool) {
	tb.Helper()
	if baseFile != nil && rowID >= 0 && rowID < baseHeader.Rows {
		_, vector, err := columnVectorGraphDeep1BReadFbinVectorsAt(baseFile, baseHeader, rowID, 1, nil, nil)
		if err != nil {
			tb.Fatalf("read local Deep1B row %d: %v", rowID, err)
		}
		return append([]float32(nil), vector...), "local", true
	}
	if !fetchBase1B {
		return nil, "", false
	}
	if rowID < 0 {
		return nil, "", false
	}
	if cached, ok := cache.get(rowID); ok {
		return cached, "remote_base_1b", true
	}
	vector := columnVectorGraphDeep1BFetchBase1BRow(tb, cache.client, rowID, columnVectorGraphDeep1BDims)
	cache.put(rowID, vector)
	return vector, "remote_base_1b", true
}

func (cache *columnVectorGraphDeep1BRemoteRowCache) get(rowID int) ([]float32, bool) {
	if cache == nil {
		return nil, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	vector, ok := cache.rows[rowID]
	if !ok {
		return nil, false
	}
	return append([]float32(nil), vector...), true
}

func (cache *columnVectorGraphDeep1BRemoteRowCache) put(rowID int, vector []float32) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.rows == nil {
		cache.rows = make(map[int][]float32)
	}
	cache.rows[rowID] = append([]float32(nil), vector...)
}

func columnVectorGraphDeep1BFetchBase1BRow(tb testing.TB, client *http.Client, rowID int, dims int) []float32 {
	tb.Helper()
	vector, err := columnVectorGraphDeep1BFetchBase1BRowErr(client, rowID, dims)
	if err != nil {
		tb.Fatalf("fetch Deep1B base.1B row %d: %v", rowID, err)
	}
	return vector
}

func columnVectorGraphDeep1BFetchBase1BRowErr(client *http.Client, rowID int, dims int) ([]float32, error) {
	rowBytes := dims * 4
	start := int64(8 + rowID*rowBytes)
	end := start + int64(rowBytes) - 1
	req, err := http.NewRequest(http.MethodGet, columnVectorGraphDeep1BBase1BURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("status=%s want 206 Partial Content", resp.Status)
	}
	raw := make([]byte, rowBytes)
	if _, err := io.ReadFull(resp.Body, raw); err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	vector := make([]float32, dims)
	for i := range vector {
		vector[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
	}
	return vector, nil
}

func columnVectorGraphDeep1BEnsureGroundtruth(tb testing.TB) (string, columnVectorGraphDeep1BIbinHeader) {
	tb.Helper()
	dir := columnVectorGraphDeep1BDataDir(tb)
	path := filepath.Join(dir, "groundtruth.public.10K.ibin")
	if header, err := columnVectorGraphDeep1BValidateIbin(path, 1, 100); err == nil {
		return path, header
	}
	if os.Getenv("COLUMN_VECTOR_DEEP1B_DOWNLOAD") != "1" {
		tb.Skipf("missing Deep1B groundtruth file %s; set COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 to download it into %s", filepath.Base(path), filepath.Dir(path))
	}
	if err := columnVectorGraphDeep1BDownloadFile(tb, path, columnVectorGraphDeep1BTruthURL); err != nil {
		tb.Fatalf("download Deep1B groundtruth: %v", err)
	}
	header, err := columnVectorGraphDeep1BValidateIbin(path, 1, 100)
	if err != nil {
		tb.Fatalf("validate Deep1B groundtruth: %v", err)
	}
	return path, header
}

func columnVectorGraphDeep1BEnsureQueryData(tb testing.TB) (string, columnVectorGraphDeep1BFbinHeader) {
	tb.Helper()
	dir := columnVectorGraphDeep1BDataDir(tb)
	path := filepath.Join(dir, "query.public.10K.fbin")
	header := columnVectorGraphDeep1BEnsureFbin(tb, path, columnVectorGraphDeep1BQueryURL, 1, columnVectorGraphDeep1BDims, 0)
	return path, header
}

func columnVectorGraphDeep1BOpenOptionalBase(tb testing.TB, rows int, require bool) (string, columnVectorGraphDeep1BFbinHeader, *os.File) {
	tb.Helper()
	if rows <= 0 {
		tb.Fatalf("Deep1B optional base rows=%d must be positive", rows)
	}
	dir := columnVectorGraphDeep1BDataDir(tb)
	candidates := []string{filepath.Join(dir, "base.10M.fbin")}
	if rows <= 1_000_000 {
		candidates = append(candidates, filepath.Join(dir, "base.10M.first1M.fbin"))
	}
	for _, path := range candidates {
		header, err := columnVectorGraphDeep1BValidateFbin(path, rows, columnVectorGraphDeep1BDims)
		if err != nil {
			continue
		}
		file, err := os.Open(path)
		if err != nil {
			tb.Fatalf("open optional Deep1B base %s: %v", path, err)
		}
		return path, header, file
	}
	if !require {
		return "", columnVectorGraphDeep1BFbinHeader{}, nil
	}
	data := columnVectorGraphDeep1BEnsureData(tb, rows)
	file, err := os.Open(data.basePath)
	if err != nil {
		tb.Fatalf("open required Deep1B base %s: %v", data.basePath, err)
	}
	return data.basePath, data.baseHeader, file
}

func columnVectorGraphDeep1BDownloadFile(tb testing.TB, path string, url string) error {
	tb.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: columnVectorGraphDeep1BDownloadTimeout(tb)}
	tb.Logf("downloading Deep1B %s to %s", url, path)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download status=%s want 200 OK", resp.Status)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, resp.Body)
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	return os.Rename(tmp, path)
}

func columnVectorGraphDeep1BValidateIbin(path string, minRows int, wantDims int) (columnVectorGraphDeep1BIbinHeader, error) {
	file, err := os.Open(path)
	if err != nil {
		return columnVectorGraphDeep1BIbinHeader{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return columnVectorGraphDeep1BIbinHeader{}, err
	}
	var header [8]byte
	if _, err := file.ReadAt(header[:], 0); err != nil {
		return columnVectorGraphDeep1BIbinHeader{}, err
	}
	rows := int(binary.LittleEndian.Uint32(header[0:4]))
	dims := int(binary.LittleEndian.Uint32(header[4:8]))
	if rows < minRows {
		return columnVectorGraphDeep1BIbinHeader{}, fmt.Errorf("Deep1B ibin rows=%d below requested %d", rows, minRows)
	}
	if dims != wantDims {
		return columnVectorGraphDeep1BIbinHeader{}, fmt.Errorf("Deep1B ibin dims=%d want %d", dims, wantDims)
	}
	needed, err := checkedMulInt(rows, dims, "Deep1B ibin values")
	if err != nil {
		return columnVectorGraphDeep1BIbinHeader{}, err
	}
	neededBytes, err := checkedMulInt(needed, 4, "Deep1B ibin payload bytes")
	if err != nil {
		return columnVectorGraphDeep1BIbinHeader{}, err
	}
	if info.Size() < int64(8+neededBytes) {
		return columnVectorGraphDeep1BIbinHeader{}, fmt.Errorf("Deep1B ibin size=%d below expected bytes=%d", info.Size(), 8+neededBytes)
	}
	return columnVectorGraphDeep1BIbinHeader{Rows: rows, Dims: dims, Size: info.Size()}, nil
}

func columnVectorGraphDeep1BReadGroundtruthRow(tb testing.TB, path string, header columnVectorGraphDeep1BIbinHeader, queryIndex int) []int {
	tb.Helper()
	if queryIndex < 0 || queryIndex >= header.Rows {
		tb.Fatalf("Deep1B groundtruth query index=%d outside rows=%d", queryIndex, header.Rows)
	}
	file, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open Deep1B groundtruth: %v", err)
	}
	defer file.Close()
	raw := make([]byte, header.Dims*4)
	offset := int64(8 + queryIndex*header.Dims*4)
	if _, err := file.ReadAt(raw, offset); err != nil {
		tb.Fatalf("read Deep1B groundtruth query %d: %v", queryIndex, err)
	}
	rows := make([]int, header.Dims)
	for i := range rows {
		rows[i] = int(int32(binary.LittleEndian.Uint32(raw[i*4:])))
	}
	return rows
}

func columnVectorGraphDeep1BLocalityMetrics(query []float32, vectors []float32, dims int) (float64, float64, float64) {
	rows := len(vectors) / dims
	queryInvNorm := columnVectorGraphDeep1BInvNorm(query)
	unitSum := make([]float64, dims)
	for row := 0; row < rows; row++ {
		base := row * dims
		var normSquared float64
		for j := 0; j < dims; j++ {
			value := float64(vectors[base+j])
			normSquared += value * value
		}
		if normSquared == 0 {
			continue
		}
		invNorm := 1 / math.Sqrt(normSquared)
		for j := 0; j < dims; j++ {
			unitSum[j] += float64(vectors[base+j]) * invNorm
		}
	}
	var sumNormSquared float64
	var queryDot float64
	for j := 0; j < dims; j++ {
		sumNormSquared += unitSum[j] * unitSum[j]
		queryDot += unitSum[j] * float64(query[j]) * queryInvNorm
	}
	sumNorm := math.Sqrt(sumNormSquared)
	centroidNorm := sumNorm / float64(rows)
	var centroidCosineToQuery float64
	if sumNorm > 0 {
		centroidCosineToQuery = queryDot / sumNorm
	}
	var averagePairwise float64
	if rows > 1 {
		averagePairwise = (sumNormSquared - float64(rows)) / float64(rows*(rows-1))
	}
	return centroidNorm, centroidCosineToQuery, averagePairwise
}

func columnVectorGraphDeep1BQuantiles(values []float32, quantiles []float64) map[string]float64 {
	sorted := make([]float64, len(values))
	for i, value := range values {
		sorted[i] = float64(value)
	}
	sort.Float64s(sorted)
	out := make(map[string]float64, len(quantiles))
	if len(sorted) == 0 {
		return out
	}
	for _, q := range quantiles {
		if q <= 0 {
			out["0"] = sorted[0]
			continue
		}
		if q >= 1 {
			out["1"] = sorted[len(sorted)-1]
			continue
		}
		pos := q * float64(len(sorted)-1)
		lo := int(math.Floor(pos))
		hi := int(math.Ceil(pos))
		value := sorted[lo]
		if hi != lo {
			value += (sorted[hi] - sorted[lo]) * (pos - float64(lo))
		}
		out[fmt.Sprintf("%.2f", q)] = value
	}
	return out
}

func columnVectorGraphDeep1BScoreMarginMetrics(scores []float32) map[string]float64 {
	topK := min(100, len(scores))
	out := make(map[string]float64)
	if topK < 2 {
		return out
	}
	topRows := make([]int, topK)
	topScores := make([]float32, topK)
	columnVectorGraphDeep1BTopKFromScores(scores, topK, topRows, topScores)
	cutoffs := []int{1, 5, 10, 20, 50}
	for _, cutoff := range cutoffs {
		if cutoff < topK {
			out[fmt.Sprintf("gap_%d_%d", cutoff, cutoff+1)] = float64(topScores[cutoff-1] - topScores[cutoff])
		}
	}
	gaps := make([]float32, topK-1)
	for i := 0; i < topK-1; i++ {
		gaps[i] = topScores[i] - topScores[i+1]
	}
	quantiles := columnVectorGraphDeep1BQuantiles(gaps, []float64{0.5, 0.9, 1})
	out["adjacent_gap_p50"] = quantiles["0.50"]
	out["adjacent_gap_p90"] = quantiles["0.90"]
	out["adjacent_gap_max"] = quantiles["1"]
	return out
}

func columnVectorGraphDeep1BFilterRanksForRows(tb testing.TB, ranks []int, rows int, dims int) []int {
	tb.Helper()
	limit := min(rows, dims)
	var out []int
	seen := make(map[int]bool, len(ranks))
	for _, rank := range ranks {
		if rank <= 0 {
			tb.Fatalf("local PCA rank %d must be positive", rank)
		}
		if rank > limit {
			continue
		}
		if !seen[rank] {
			seen[rank] = true
			out = append(out, rank)
		}
	}
	if len(out) == 0 {
		out = append(out, limit)
	}
	sort.Ints(out)
	return out
}

func columnVectorGraphDeep1BApproxRecall(exact []float32, approximate []float32, topK int) (int, float64) {
	if topK > len(exact) {
		topK = len(exact)
	}
	exactRows := make([]int, topK)
	exactScores := make([]float32, topK)
	approxRows := make([]int, topK)
	approxScores := make([]float32, topK)
	columnVectorGraphDeep1BTopKFromScores(exact, topK, exactRows, exactScores)
	columnVectorGraphDeep1BTopKFromScores(approximate, topK, approxRows, approxScores)
	overlap := columnVectorGraphDeep1BTopKOverlap(exactRows, approxRows)
	return overlap, float64(overlap) / float64(topK)
}

func columnVectorGraphDeep1BCandidateRecall(exact []float32, approximate []float32, targetK int, candidateK int) (int, float64) {
	targetK = min(targetK, len(exact))
	candidateK = min(candidateK, len(exact))
	if targetK <= 0 || candidateK <= 0 {
		return 0, 0
	}
	exactRows := make([]int, targetK)
	exactScores := make([]float32, targetK)
	candidateRows := make([]int, candidateK)
	candidateScores := make([]float32, candidateK)
	columnVectorGraphDeep1BTopKFromScores(exact, targetK, exactRows, exactScores)
	columnVectorGraphDeep1BTopKFromScores(approximate, candidateK, candidateRows, candidateScores)
	overlap := columnVectorGraphDeep1BTopKOverlap(exactRows, candidateRows)
	return overlap, float64(overlap) / float64(targetK)
}

func columnVectorGraphDeep1BNewGroundtruthMethodReport(family string, name string, rowCodeBytesPerVector float64, metadataBytesPerVector float64, buildNanos int64, notes string) columnVectorGraphDeep1BGroundtruthMethodReport {
	return columnVectorGraphDeep1BNewCompressionMethodReport(
		"official_top100_local_neighborhood_upper_bound_probe",
		"top100_oracle_metadata_not_production_amortized",
		family,
		name,
		rowCodeBytesPerVector,
		metadataBytesPerVector,
		buildNanos,
		notes,
	)
}

func columnVectorGraphDeep1BNewCompressionMethodReport(regime string, metadataStatus string, family string, name string, rowCodeBytesPerVector float64, metadataBytesPerVector float64, buildNanos int64, notes string) columnVectorGraphDeep1BGroundtruthMethodReport {
	return columnVectorGraphDeep1BGroundtruthMethodReport{
		Regime:                 regime,
		Family:                 family,
		Name:                   name,
		RowCodeBytesPerVector:  rowCodeBytesPerVector,
		MetadataBytesPerVector: metadataBytesPerVector,
		TotalBytesPerVector:    rowCodeBytesPerVector + metadataBytesPerVector,
		MetadataStatus:         metadataStatus,
		BuildNanos:             buildNanos,
		Notes:                  notes,
	}
}

func columnVectorGraphDeep1BFillGroundtruthMethodMetrics(method *columnVectorGraphDeep1BGroundtruthMethodReport, exact []float32, approximate []float32, margins map[string]float64) {
	method.Top10Overlap, method.RecallAt10 = columnVectorGraphDeep1BApproxRecall(exact, approximate, 10)
	if len(exact) >= 20 {
		method.Top20Overlap, method.RecallAt20 = columnVectorGraphDeep1BApproxRecall(exact, approximate, 20)
		method.Top10InApproxTop20, method.Top10RecallAt20 = columnVectorGraphDeep1BCandidateRecall(exact, approximate, 10, 20)
		method.ExactRerankRecallAt10FromTop20 = method.Top10RecallAt20
	}
	if len(exact) >= 50 {
		method.Top50Overlap, method.RecallAt50 = columnVectorGraphDeep1BApproxRecall(exact, approximate, 50)
		method.Top10InApproxTop50, method.Top10RecallAt50 = columnVectorGraphDeep1BCandidateRecall(exact, approximate, 10, 50)
		method.Top20InApproxTop50, method.Top20RecallAt50 = columnVectorGraphDeep1BCandidateRecall(exact, approximate, 20, 50)
		method.ExactRerankRecallAt10FromTop50 = method.Top10RecallAt50
	}
	if len(exact) >= 100 {
		method.Top10InApproxTop100, method.Top10RecallAt100 = columnVectorGraphDeep1BCandidateRecall(exact, approximate, 10, 100)
		method.Top20InApproxTop100, method.Top20RecallAt100 = columnVectorGraphDeep1BCandidateRecall(exact, approximate, 20, 100)
		method.ExactRerankRecallAt10FromTop100 = method.Top10RecallAt100
	}
	method.MaxScoreError, method.MeanScoreError = columnVectorGraphDeep1BScoreErrorMetrics(exact, approximate)
	method.MeanErrorOverGap10 = columnVectorGraphDeep1BRatioOrZero(method.MeanScoreError, margins["gap_10_11"])
	method.MaxErrorOverGap10 = columnVectorGraphDeep1BRatioOrZero(method.MaxScoreError, margins["gap_10_11"])
	method.MeanErrorOverGap20 = columnVectorGraphDeep1BRatioOrZero(method.MeanScoreError, margins["gap_20_21"])
	method.MaxErrorOverGap20 = columnVectorGraphDeep1BRatioOrZero(method.MaxScoreError, margins["gap_20_21"])
	method.MeanErrorOverGap50 = columnVectorGraphDeep1BRatioOrZero(method.MeanScoreError, margins["gap_50_51"])
	method.MaxErrorOverGap50 = columnVectorGraphDeep1BRatioOrZero(method.MaxScoreError, margins["gap_50_51"])
}

func columnVectorGraphDeep1BRatioOrZero(numerator float64, denominator float64) float64 {
	if denominator <= 0 {
		return 0
	}
	return numerator / denominator
}

func columnVectorGraphDeep1BMeasureGroundtruthScan(rows int, iters int, score func(dst []float32)) float64 {
	if rows <= 0 || iters <= 0 {
		return 0
	}
	dst := make([]float32, rows)
	start := time.Now()
	for i := 0; i < iters; i++ {
		score(dst)
	}
	elapsed := time.Since(start)
	return float64(elapsed.Nanoseconds()) / float64(rows*iters)
}

type columnVectorGraphDeep1BLocalPCAScorer struct {
	base            float32
	queryProjection []float32
}

func columnVectorGraphDeep1BPrepareLocalPCAScorer(model columnVectorGraphDeep1BLocalPCAModel, encoding columnVectorGraphDeep1BLocalPCAEncoding, query []float32, rows int, dims int) columnVectorGraphDeep1BLocalPCAScorer {
	_ = rows
	rank := encoding.rank
	queryProjection := make([]float32, rank)
	var base float32
	for j := 0; j < dims; j++ {
		base += query[j] * model.centroid[j]
	}
	for k := 0; k < rank; k++ {
		basisBase := k * dims
		var projection float32
		for j := 0; j < dims; j++ {
			projection += query[j] * model.basis[basisBase+j]
		}
		queryProjection[k] = projection
	}
	return columnVectorGraphDeep1BLocalPCAScorer{base: base, queryProjection: queryProjection}
}

func columnVectorGraphDeep1BScoreLocalPCAEncodingInto(model columnVectorGraphDeep1BLocalPCAModel, encoding columnVectorGraphDeep1BLocalPCAEncoding, query []float32, queryInvNorm float32, rows int, dims int, scores []float32) {
	scorer := columnVectorGraphDeep1BPrepareLocalPCAScorer(model, encoding, query, rows, dims)
	scorer.scoreInto(encoding, queryInvNorm, rows, scores)
}

func (scorer columnVectorGraphDeep1BLocalPCAScorer) scoreInto(encoding columnVectorGraphDeep1BLocalPCAEncoding, queryInvNorm float32, rows int, scores []float32) {
	if len(scores) < rows {
		panic(fmt.Sprintf("local PCA score dst len=%d want at least %d", len(scores), rows))
	}
	rank := encoding.rank
	for row := 0; row < rows; row++ {
		dot := scorer.base
		for k := 0; k < rank; k++ {
			dot += scorer.queryProjection[k] * float32(int8(encoding.codes[k*rows+row])) * encoding.scales[k]
		}
		scores[row] = dot * queryInvNorm * encoding.invNorms[row]
	}
}

func columnVectorGraphDeep1BEvaluateGroundtruthPCABasisVariants(tb testing.TB, vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, ranks []int, scanIters int) []columnVectorGraphDeep1BGroundtruthMethodReport {
	tb.Helper()
	type modelCase struct {
		family     string
		name       string
		model      columnVectorGraphDeep1BLocalPCAModel
		buildNanos int64
		notes      string
	}
	fitCase := func(family string, name string, notes string, fit func() columnVectorGraphDeep1BLocalPCAModel) modelCase {
		start := time.Now()
		model := fit()
		return modelCase{
			family:     family,
			name:       name,
			model:      model,
			buildNanos: time.Since(start).Nanoseconds(),
			notes:      notes,
		}
	}
	cases := []modelCase{
		fitCase(
			"boundary_weighted_pca",
			"boundary_weighted_pca_top20_hardneg",
			"official top100 oracle probe; row weights favor exact top10/top20 and hard negatives before fitting PCA, so this tests whether recall-aware weighting beats variance PCA",
			func() columnVectorGraphDeep1BLocalPCAModel {
				return columnVectorGraphDeep1BFitBoundaryWeightedPCAModel(tb, vectors, exactScores, rows, dims, ranks)
			},
		),
		fitCase(
			"pairwise_difference_pca",
			"pairwise_diff_pca_top10_vs_11_100",
			"official top100 oracle probe; basis comes from top10-minus-hard-negative difference vectors, so this tests boundary-separating directions rather than point-cloud variance",
			func() columnVectorGraphDeep1BLocalPCAModel {
				return columnVectorGraphDeep1BFitPairwiseDifferencePCAModel(tb, vectors, exactScores, rows, dims, ranks, 10, 100)
			},
		),
	}
	out := make([]columnVectorGraphDeep1BGroundtruthMethodReport, 0, len(cases)*len(ranks))
	for _, c := range cases {
		for _, rank := range ranks {
			start := time.Now()
			encoding := columnVectorGraphDeep1BEncodeLocalPCARank(vectors, invNorms, c.model, rank, query, queryInvNorm, rows, dims)
			buildNanos := time.Since(start).Nanoseconds()
			method := columnVectorGraphDeep1BNewGroundtruthMethodReport(
				c.family,
				fmt.Sprintf("%s_i8_rank%d", c.name, rank),
				float64(rank),
				float64(c.model.centroidMetaBytes+rank*dims*2+rank*2+rows*2)/float64(rows),
				c.buildNanos+buildNanos,
				c.notes+"; fp16 centroid+basis+scales+invNorms metadata is not production-amortized",
			)
			scorer := columnVectorGraphDeep1BPrepareLocalPCAScorer(c.model, encoding, query, rows, dims)
			method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
				scorer.scoreInto(encoding, queryInvNorm, rows, dst)
			})
			method.MeanRelativeL2 = encoding.meanRelativeL2
			method.MaxRelativeL2 = encoding.maxRelativeL2
			columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, encoding.approxScores, margins)
			out = append(out, method)
		}
	}
	return out
}

func columnVectorGraphDeep1BFitBoundaryWeightedPCAModel(tb testing.TB, vectors []float32, exactScores []float32, rows int, dims int, ranks []int) columnVectorGraphDeep1BLocalPCAModel {
	tb.Helper()
	order := columnVectorGraphDeep1BScoreOrderDesc(exactScores)
	weights := make([]float64, rows)
	for i := range weights {
		weights[i] = 1
	}
	for rank, row := range order {
		switch {
		case rank < 10:
			weights[row] = 8
		case rank < 20:
			weights[row] = 6
		case rank < 50:
			weights[row] = 3
		default:
			weights[row] = 1
		}
	}
	centroid := columnVectorGraphDeep1BWeightedCentroidF16(vectors, weights, rows, dims)
	residuals := make([]float64, rows*dims)
	for row := 0; row < rows; row++ {
		scale := math.Sqrt(weights[row])
		srcBase := row * dims
		for j := 0; j < dims; j++ {
			residuals[srcBase+j] = float64(vectors[srcBase+j]-centroid[j]) * scale
		}
	}
	return columnVectorGraphDeep1BFitLocalPCAModelFromResidualMatrix(tb, centroid, residuals, rows, dims, ranks, "boundary-weighted PCA")
}

func columnVectorGraphDeep1BFitPairwiseDifferencePCAModel(tb testing.TB, vectors []float32, exactScores []float32, rows int, dims int, ranks []int, positiveK int, negativeEnd int) columnVectorGraphDeep1BLocalPCAModel {
	tb.Helper()
	order := columnVectorGraphDeep1BScoreOrderDesc(exactScores)
	if positiveK > len(order) {
		positiveK = len(order)
	}
	if negativeEnd > len(order) {
		negativeEnd = len(order)
	}
	if positiveK <= 0 || negativeEnd <= positiveK {
		tb.Fatalf("invalid pairwise-difference PCA positiveK=%d negativeEnd=%d rows=%d", positiveK, negativeEnd, rows)
	}
	diffRows := positiveK * (negativeEnd - positiveK)
	differences := make([]float64, diffRows*dims)
	outRow := 0
	for _, positiveRow := range order[:positiveK] {
		positiveBase := positiveRow * dims
		for _, negativeRow := range order[positiveK:negativeEnd] {
			negativeBase := negativeRow * dims
			dstBase := outRow * dims
			for j := 0; j < dims; j++ {
				differences[dstBase+j] = float64(vectors[positiveBase+j] - vectors[negativeBase+j])
			}
			outRow++
		}
	}
	centroid := columnVectorGraphDeep1BWeightedCentroidF16(vectors, nil, rows, dims)
	return columnVectorGraphDeep1BFitLocalPCAModelFromResidualMatrix(tb, centroid, differences, diffRows, dims, ranks, "pairwise-difference PCA")
}

func columnVectorGraphDeep1BFitLocalPCAModelFromResidualMatrix(tb testing.TB, centroid []float32, residuals []float64, matrixRows int, dims int, ranks []int, label string) columnVectorGraphDeep1BLocalPCAModel {
	tb.Helper()
	ranks = columnVectorGraphDeep1BValidateLocalPCARanks(tb, ranks, dims)
	maxRank := ranks[len(ranks)-1]
	if len(residuals) != matrixRows*dims {
		tb.Fatalf("%s residual matrix len=%d want=%d", label, len(residuals), matrixRows*dims)
	}
	var svd mat.SVD
	if ok := svd.Factorize(mat.NewDense(matrixRows, dims, residuals), mat.SVDThinV); !ok {
		tb.Fatalf("%s SVD failed for %d x %d matrix", label, matrixRows, dims)
	}
	singularValues := svd.Values(nil)
	var v mat.Dense
	svd.VTo(&v)
	basis := make([]float32, maxRank*dims)
	for k := 0; k < maxRank; k++ {
		for j := 0; j < dims; j++ {
			value := float32(v.At(j, k))
			basis[k*dims+j] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(value))
		}
	}
	var totalVariance float64
	for _, singular := range singularValues {
		totalVariance += singular * singular
	}
	varianceByRank := make(map[int]float64, len(ranks))
	var captured float64
	nextRankIndex := 0
	for k, singular := range singularValues {
		captured += singular * singular
		for nextRankIndex < len(ranks) && k+1 == ranks[nextRankIndex] {
			if totalVariance == 0 {
				varianceByRank[ranks[nextRankIndex]] = 1
			} else {
				varianceByRank[ranks[nextRankIndex]] = captured / totalVariance
			}
			nextRankIndex++
		}
	}
	for nextRankIndex < len(ranks) {
		if totalVariance == 0 {
			varianceByRank[ranks[nextRankIndex]] = 1
		} else {
			varianceByRank[ranks[nextRankIndex]] = captured / totalVariance
		}
		nextRankIndex++
	}
	return columnVectorGraphDeep1BLocalPCAModel{
		maxRank:           maxRank,
		centroid:          centroid,
		centroidF16Bytes:  columnVectorGraphDeep1BFloat32ToFloat16Bytes(centroid),
		basis:             basis,
		basisF16Bytes:     columnVectorGraphDeep1BFloat32ToFloat16Bytes(basis),
		varianceByRank:    varianceByRank,
		centroidMetaBytes: dims * 2,
	}
}

func columnVectorGraphDeep1BWeightedCentroidF16(vectors []float32, weights []float64, rows int, dims int) []float32 {
	centroid := make([]float32, dims)
	var weightSum float64
	if weights == nil {
		weightSum = float64(rows)
		for row := 0; row < rows; row++ {
			base := row * dims
			for j := 0; j < dims; j++ {
				centroid[j] += vectors[base+j]
			}
		}
	} else {
		for row := 0; row < rows; row++ {
			weight := weights[row]
			weightSum += weight
			base := row * dims
			for j := 0; j < dims; j++ {
				centroid[j] += float32(weight * float64(vectors[base+j]))
			}
		}
	}
	if weightSum == 0 {
		weightSum = 1
	}
	invWeightSum := float32(1 / weightSum)
	for j := range centroid {
		centroid[j] *= invWeightSum
		centroid[j] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(centroid[j]))
	}
	return centroid
}

func columnVectorGraphDeep1BScoreOrderDesc(scores []float32) []int {
	order := make([]int, len(scores))
	for i := range order {
		order[i] = i
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

type columnVectorGraphDeep1BGroundtruthScalarEncoding struct {
	family                 string
	name                   string
	rowCodeBytesPerVector  float64
	metadataBytesPerVector float64
	codes                  []uint8
	bits                   int
	dims                   int
	sign                   bool
	policy                 string
	mins                   []float32
	scales                 []float32
	scoreInvNorms          []float32
	meanRelativeL2         float64
	maxRelativeL2          float64
	notes                  string
}

func columnVectorGraphDeep1BEvaluateGroundtruthScalarMethods(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int) []columnVectorGraphDeep1BGroundtruthMethodReport {
	type scalarCase struct {
		bits     int
		policy   string
		normMode string
	}
	cases := []scalarCase{
		{bits: 8, policy: "per_dim", normMode: "reconstructed"},
		{bits: 8, policy: "per_vector", normMode: "reconstructed"},
		{bits: 8, policy: "global", normMode: "reconstructed"},
		{bits: 8, policy: "per_dim", normMode: "f16_explicit_norm"},
		{bits: 4, policy: "per_dim", normMode: "reconstructed"},
		{bits: 4, policy: "per_vector", normMode: "reconstructed"},
		{bits: 4, policy: "global", normMode: "reconstructed"},
		{bits: 4, policy: "per_dim", normMode: "f16_explicit_norm"},
		{bits: 2, policy: "per_dim", normMode: "reconstructed"},
		{bits: 2, policy: "per_vector", normMode: "reconstructed"},
		{bits: 2, policy: "global", normMode: "reconstructed"},
		{bits: 2, policy: "per_dim", normMode: "f16_explicit_norm"},
	}
	out := make([]columnVectorGraphDeep1BGroundtruthMethodReport, 0, len(cases)+4)
	for _, c := range cases {
		start := time.Now()
		encoding := columnVectorGraphDeep1BEncodeGroundtruthScalar(vectors, invNorms, rows, dims, c.bits, c.policy, c.normMode)
		buildNanos := time.Since(start).Nanoseconds()
		approx := make([]float32, rows)
		columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, query, queryInvNorm, rows, dims, approx)
		method := columnVectorGraphDeep1BNewGroundtruthMethodReport(encoding.family, encoding.name, encoding.rowCodeBytesPerVector, encoding.metadataBytesPerVector, buildNanos, encoding.notes)
		method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
			columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, query, queryInvNorm, rows, dims, dst)
		})
		method.MeanRelativeL2 = encoding.meanRelativeL2
		method.MaxRelativeL2 = encoding.maxRelativeL2
		columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
		out = append(out, method)
	}
	for _, policy := range []string{"per_dim", "per_vector", "global", "per_dim_f16_norm"} {
		normMode := "reconstructed"
		scalePolicy := policy
		if policy == "per_dim_f16_norm" {
			scalePolicy = "per_dim"
			normMode = "f16_explicit_norm"
		}
		start := time.Now()
		encoding := columnVectorGraphDeep1BEncodeGroundtruthSign(vectors, invNorms, rows, dims, scalePolicy, normMode)
		buildNanos := time.Since(start).Nanoseconds()
		approx := make([]float32, rows)
		columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, query, queryInvNorm, rows, dims, approx)
		method := columnVectorGraphDeep1BNewGroundtruthMethodReport(encoding.family, encoding.name, encoding.rowCodeBytesPerVector, encoding.metadataBytesPerVector, buildNanos, encoding.notes)
		method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
			columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, query, queryInvNorm, rows, dims, dst)
		})
		method.MeanRelativeL2 = encoding.meanRelativeL2
		method.MaxRelativeL2 = encoding.maxRelativeL2
		columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
		out = append(out, method)
	}
	return out
}

func columnVectorGraphDeep1BEvaluateGroundtruthQueryAxisOracle(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int) []columnVectorGraphDeep1BGroundtruthMethodReport {
	start := time.Now()
	queryUnit := make([]float32, dims)
	for j := 0; j < dims; j++ {
		queryUnit[j] = query[j] * queryInvNorm
	}
	projections := make([]float32, rows)
	for row := 0; row < rows; row++ {
		base := row * dims
		var projection float32
		for j := 0; j < dims; j++ {
			projection += queryUnit[j] * vectors[base+j]
		}
		projections[row] = projection
	}
	invNormsF16 := make([]float32, rows)
	for row := 0; row < rows; row++ {
		invNormsF16[row] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(invNorms[row]))
	}
	exactProjectionScores := make([]float32, rows)
	for row := 0; row < rows; row++ {
		exactProjectionScores[row] = projections[row] * invNorms[row]
	}
	exactProjection := columnVectorGraphDeep1BNewGroundtruthMethodReport(
		"query_axis_score_oracle",
		"query_axis_oracle_f32_projection_f32_norm",
		4,
		4,
		time.Since(start).Nanoseconds(),
		"official top100 oracle only; stores the query-axis projection and exact row norm, so it is a non-deployable upper bound for query-specific score-aware 1D projection",
	)
	exactProjection.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
		for row := 0; row < rows; row++ {
			dst[row] = projections[row] * invNorms[row]
		}
	})
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&exactProjection, exactScores, exactProjectionScores, margins)

	quantStart := time.Now()
	var maxAbs float32
	for _, projection := range projections {
		absProjection := float32(math.Abs(float64(projection)))
		if absProjection > maxAbs {
			maxAbs = absProjection
		}
	}
	scale := columnVectorGraphDeep1BNonZeroScale(maxAbs / 127)
	scale = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(scale))
	codes := make([]byte, rows)
	for row, projection := range projections {
		code := int(math.Round(float64(projection / scale)))
		if code < -127 {
			code = -127
		} else if code > 127 {
			code = 127
		}
		codes[row] = byte(int8(code))
	}
	quantizedScores := make([]float32, rows)
	scoreQuantized := func(dst []float32) {
		for row := 0; row < rows; row++ {
			dst[row] = float32(int8(codes[row])) * scale * invNormsF16[row]
		}
	}
	scoreQuantized(quantizedScores)
	quantizedProjection := columnVectorGraphDeep1BNewGroundtruthMethodReport(
		"query_axis_score_oracle",
		"query_axis_oracle_i8_projection_f16_norm",
		1,
		2+2/float64(rows),
		time.Since(quantStart).Nanoseconds(),
		"official top100 oracle only; query-specific 1D projection with int8 scalar code and f16 norm estimates the upper bound for ranking-aware local projection",
	)
	quantizedProjection.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, scoreQuantized)
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&quantizedProjection, exactScores, quantizedScores, margins)

	return []columnVectorGraphDeep1BGroundtruthMethodReport{exactProjection, quantizedProjection}
}

func columnVectorGraphDeep1BEvaluateGroundtruthRandomRotationMethods(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int, seed int64) []columnVectorGraphDeep1BGroundtruthMethodReport {
	rotationStart := time.Now()
	rotation := columnVectorGraphDeep1BRandomOrthogonalMatrix(dims, 0x5eed_1b+seed)
	rotatedVectors := columnVectorGraphDeep1BApplyRotation(vectors, rows, dims, rotation)
	rotatedQuery := columnVectorGraphDeep1BApplyRotation(query, 1, dims, rotation)
	rotationBuildNanos := time.Since(rotationStart).Nanoseconds()
	type scalarCase struct {
		bits int
		sign bool
	}
	cases := []scalarCase{
		{bits: 8},
		{bits: 4},
		{bits: 2},
		{bits: 1, sign: true},
	}
	out := make([]columnVectorGraphDeep1BGroundtruthMethodReport, 0, len(cases))
	for _, c := range cases {
		start := time.Now()
		var encoding columnVectorGraphDeep1BGroundtruthScalarEncoding
		if c.sign {
			encoding = columnVectorGraphDeep1BEncodeGroundtruthSign(rotatedVectors, invNorms, rows, dims, "per_dim", "reconstructed")
			encoding.family = "random_rotation_binary_sign_quantization"
			encoding.name = "random_rotation_" + encoding.name
		} else {
			encoding = columnVectorGraphDeep1BEncodeGroundtruthScalar(rotatedVectors, invNorms, rows, dims, c.bits, "per_dim", "reconstructed")
			encoding.family = "random_rotation_scalar_quantization"
			encoding.name = "random_rotation_" + encoding.name
		}
		encoding.metadataBytesPerVector += 8 / float64(rows)
		encoding.notes = "official top100 local-neighborhood upper-bound probe; fixed deterministic global random orthogonal rotation plus per-dim scalar/sign quantization; only seed metadata is charged here, and production rotation/storage accounting is not established by this probe"
		buildNanos := rotationBuildNanos + time.Since(start).Nanoseconds()
		approx := make([]float32, rows)
		columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, rotatedQuery, queryInvNorm, rows, dims, approx)
		method := columnVectorGraphDeep1BNewGroundtruthMethodReport(encoding.family, encoding.name, encoding.rowCodeBytesPerVector, encoding.metadataBytesPerVector, buildNanos, encoding.notes)
		method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
			columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding, rotatedQuery, queryInvNorm, rows, dims, dst)
		})
		method.MeanRelativeL2 = encoding.meanRelativeL2
		method.MaxRelativeL2 = encoding.maxRelativeL2
		columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
		out = append(out, method)
	}
	return out
}

func columnVectorGraphDeep1BRandomOrthogonalMatrix(dims int, seed int64) []float32 {
	rng := rand.New(rand.NewSource(seed))
	basis := make([]float32, dims*dims)
	work := make([]float64, dims)
	for row := 0; row < dims; row++ {
		for attempt := 0; attempt < 8; attempt++ {
			for j := 0; j < dims; j++ {
				work[j] = rng.NormFloat64()
			}
			for prev := 0; prev < row; prev++ {
				var dot float64
				for j := 0; j < dims; j++ {
					dot += work[j] * float64(basis[prev*dims+j])
				}
				for j := 0; j < dims; j++ {
					work[j] -= dot * float64(basis[prev*dims+j])
				}
			}
			var normSquared float64
			for j := 0; j < dims; j++ {
				normSquared += work[j] * work[j]
			}
			if normSquared > 1e-18 {
				invNorm := 1 / math.Sqrt(normSquared)
				for j := 0; j < dims; j++ {
					basis[row*dims+j] = float32(work[j] * invNorm)
				}
				break
			}
		}
	}
	return basis
}

func columnVectorGraphDeep1BApplyRotation(vectors []float32, rows int, dims int, rotation []float32) []float32 {
	out := make([]float32, rows*dims)
	for row := 0; row < rows; row++ {
		srcBase := row * dims
		dstBase := row * dims
		for k := 0; k < dims; k++ {
			rotBase := k * dims
			var value float32
			for j := 0; j < dims; j++ {
				value += rotation[rotBase+j] * vectors[srcBase+j]
			}
			out[dstBase+k] = value
		}
	}
	return out
}

func columnVectorGraphDeep1BEncodeGroundtruthScalar(vectors []float32, invNorms []float32, rows int, dims int, bits int, policy string, normMode string) columnVectorGraphDeep1BGroundtruthScalarEncoding {
	levels := 1 << bits
	if bits <= 0 || bits > 8 {
		panic(fmt.Sprintf("scalar bits=%d must be in 1..8", bits))
	}
	encoding := columnVectorGraphDeep1BGroundtruthScalarEncoding{
		family:                "scalar_quantization",
		name:                  fmt.Sprintf("scalar_u%d_affine_%s_%s", bits, policy, normMode),
		rowCodeBytesPerVector: float64(dims*bits) / 8,
		bits:                  bits,
		dims:                  dims,
		policy:                policy,
		codes:                 make([]uint8, rows*dims),
		notes:                 "official top100 local-neighborhood upper-bound probe; theoretical packed row-code bytes, scorer uses unpacked Go codes; metadata is top100-local and not production-amortized",
	}
	switch policy {
	case "per_dim":
		encoding.mins = make([]float32, dims)
		encoding.scales = make([]float32, dims)
		encoding.metadataBytesPerVector = float64(dims*8) / float64(rows)
		for j := 0; j < dims; j++ {
			minValue := vectors[j]
			maxValue := vectors[j]
			for row := 1; row < rows; row++ {
				value := vectors[row*dims+j]
				if value < minValue {
					minValue = value
				}
				if value > maxValue {
					maxValue = value
				}
			}
			encoding.mins[j], encoding.scales[j] = columnVectorGraphDeep1BQuantRange(minValue, maxValue, levels)
		}
	case "per_vector":
		encoding.mins = make([]float32, rows)
		encoding.scales = make([]float32, rows)
		encoding.metadataBytesPerVector = 8
		for row := 0; row < rows; row++ {
			base := row * dims
			minValue := vectors[base]
			maxValue := vectors[base]
			for j := 1; j < dims; j++ {
				value := vectors[base+j]
				if value < minValue {
					minValue = value
				}
				if value > maxValue {
					maxValue = value
				}
			}
			encoding.mins[row], encoding.scales[row] = columnVectorGraphDeep1BQuantRange(minValue, maxValue, levels)
		}
	case "global":
		encoding.mins = make([]float32, 1)
		encoding.scales = make([]float32, 1)
		encoding.metadataBytesPerVector = 8 / float64(rows)
		minValue := vectors[0]
		maxValue := vectors[0]
		for _, value := range vectors[1:] {
			if value < minValue {
				minValue = value
			}
			if value > maxValue {
				maxValue = value
			}
		}
		encoding.mins[0], encoding.scales[0] = columnVectorGraphDeep1BQuantRange(minValue, maxValue, levels)
	default:
		panic(fmt.Sprintf("unknown scalar policy %q", policy))
	}
	for row := 0; row < rows; row++ {
		for j := 0; j < dims; j++ {
			minValue, scale := encoding.scalarParams(row, j)
			code := int(math.Round(float64((vectors[row*dims+j] - minValue) / scale)))
			if code < 0 {
				code = 0
			} else if code >= levels {
				code = levels - 1
			}
			encoding.codes[row*dims+j] = uint8(code)
		}
	}
	columnVectorGraphDeep1BFinishGroundtruthScalarEncoding(&encoding, vectors, invNorms, rows, dims, normMode)
	return encoding
}

func columnVectorGraphDeep1BEncodeGroundtruthSign(vectors []float32, invNorms []float32, rows int, dims int, policy string, normMode string) columnVectorGraphDeep1BGroundtruthScalarEncoding {
	encoding := columnVectorGraphDeep1BGroundtruthScalarEncoding{
		family:                "binary_sign_quantization",
		name:                  fmt.Sprintf("sign_scale_%s_%s", policy, normMode),
		rowCodeBytesPerVector: float64(dims) / 8,
		bits:                  1,
		dims:                  dims,
		sign:                  true,
		policy:                policy,
		codes:                 make([]uint8, rows*dims),
		notes:                 "official top100 local-neighborhood upper-bound probe; sign bits use least-squares mean-absolute scale and theoretical packed row-code bytes",
	}
	switch policy {
	case "per_dim":
		encoding.scales = make([]float32, dims)
		encoding.metadataBytesPerVector = float64(dims*4) / float64(rows)
		for j := 0; j < dims; j++ {
			var sumAbs float64
			for row := 0; row < rows; row++ {
				sumAbs += math.Abs(float64(vectors[row*dims+j]))
			}
			encoding.scales[j] = columnVectorGraphDeep1BNonZeroScale(float32(sumAbs / float64(rows)))
		}
	case "per_vector":
		encoding.scales = make([]float32, rows)
		encoding.metadataBytesPerVector = 4
		for row := 0; row < rows; row++ {
			base := row * dims
			var sumAbs float64
			for j := 0; j < dims; j++ {
				sumAbs += math.Abs(float64(vectors[base+j]))
			}
			encoding.scales[row] = columnVectorGraphDeep1BNonZeroScale(float32(sumAbs / float64(dims)))
		}
	case "global":
		encoding.scales = make([]float32, 1)
		encoding.metadataBytesPerVector = 4 / float64(rows)
		var sumAbs float64
		for _, value := range vectors {
			sumAbs += math.Abs(float64(value))
		}
		encoding.scales[0] = columnVectorGraphDeep1BNonZeroScale(float32(sumAbs / float64(len(vectors))))
	default:
		panic(fmt.Sprintf("unknown sign scale policy %q", policy))
	}
	for i, value := range vectors {
		if value >= 0 {
			encoding.codes[i] = 1
		}
	}
	columnVectorGraphDeep1BFinishGroundtruthScalarEncoding(&encoding, vectors, invNorms, rows, dims, normMode)
	return encoding
}

func columnVectorGraphDeep1BFinishGroundtruthScalarEncoding(encoding *columnVectorGraphDeep1BGroundtruthScalarEncoding, vectors []float32, invNorms []float32, rows int, dims int, normMode string) {
	reconstructed := make([]float32, rows*dims)
	for row := 0; row < rows; row++ {
		for j := 0; j < dims; j++ {
			reconstructed[row*dims+j] = encoding.reconstruct(row, j)
		}
	}
	reconstructedInvNorms := columnVectorGraphDeep1BInvNorms(reconstructed, dims)
	encoding.scoreInvNorms = reconstructedInvNorms
	if normMode == "f16_explicit_norm" {
		encoding.scoreInvNorms = make([]float32, rows)
		for row := 0; row < rows; row++ {
			encoding.scoreInvNorms[row] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(invNorms[row]))
		}
		encoding.metadataBytesPerVector += 2
	}
	var relSum float64
	var maxRel float64
	for row := 0; row < rows; row++ {
		base := row * dims
		var errSquared float64
		var normSquared float64
		for j := 0; j < dims; j++ {
			diff := float64(vectors[base+j] - reconstructed[base+j])
			errSquared += diff * diff
			value := float64(vectors[base+j])
			normSquared += value * value
		}
		var rel float64
		if normSquared > 0 {
			rel = math.Sqrt(errSquared / normSquared)
		}
		relSum += rel
		if rel > maxRel {
			maxRel = rel
		}
	}
	encoding.meanRelativeL2 = relSum / float64(rows)
	encoding.maxRelativeL2 = maxRel
}

func (encoding columnVectorGraphDeep1BGroundtruthScalarEncoding) scalarParams(row int, dim int) (float32, float32) {
	switch encoding.policy {
	case "per_dim":
		return encoding.mins[dim], encoding.scales[dim]
	case "per_vector":
		return encoding.mins[row], encoding.scales[row]
	case "global":
		return encoding.mins[0], encoding.scales[0]
	default:
		panic(fmt.Sprintf("unknown scalar policy %q", encoding.policy))
	}
}

func (encoding columnVectorGraphDeep1BGroundtruthScalarEncoding) scaleParam(row int, dim int) float32 {
	switch encoding.policy {
	case "per_dim":
		return encoding.scales[dim]
	case "per_vector":
		return encoding.scales[row]
	case "global":
		return encoding.scales[0]
	default:
		panic(fmt.Sprintf("unknown sign scale policy %q", encoding.policy))
	}
}

func (encoding columnVectorGraphDeep1BGroundtruthScalarEncoding) reconstruct(row int, dim int) float32 {
	code := encoding.codes[row*encoding.dims+dim]
	if encoding.sign {
		scale := encoding.scaleParam(row, dim)
		if code == 0 {
			return -scale
		}
		return scale
	}
	minValue, scale := encoding.scalarParams(row, dim)
	return minValue + float32(code)*scale
}

func columnVectorGraphDeep1BScoreGroundtruthScalarInto(encoding columnVectorGraphDeep1BGroundtruthScalarEncoding, query []float32, queryInvNorm float32, rows int, dims int, scores []float32) {
	if len(scores) < rows {
		panic(fmt.Sprintf("scalar score dst len=%d want at least %d", len(scores), rows))
	}
	for row := 0; row < rows; row++ {
		var dot float32
		for j := 0; j < dims; j++ {
			dot += query[j] * encoding.reconstruct(row, j)
		}
		scores[row] = dot * queryInvNorm * encoding.scoreInvNorms[row]
	}
}

func columnVectorGraphDeep1BQuantRange(minValue float32, maxValue float32, levels int) (float32, float32) {
	if levels <= 1 {
		return minValue, 1
	}
	scale := (maxValue - minValue) / float32(levels-1)
	if scale == 0 {
		scale = 1
	}
	return minValue, scale
}

func columnVectorGraphDeep1BNonZeroScale(scale float32) float32 {
	if scale == 0 || math.IsNaN(float64(scale)) || math.IsInf(float64(scale), 0) {
		return 1
	}
	return scale
}

func columnVectorGraphDeep1BGroundtruthAgreement(loadedRows []int, exactTopRows []int, topK int) int {
	if topK > len(loadedRows) {
		topK = len(loadedRows)
	}
	want := make(map[int]struct{}, topK)
	for i := 0; i < topK; i++ {
		want[loadedRows[i]] = struct{}{}
	}
	var overlap int
	for _, localRow := range exactTopRows {
		if _, ok := want[loadedRows[localRow]]; ok {
			overlap++
		}
	}
	return overlap
}

func columnVectorGraphDeep1BRenderGroundtruthLocalityMarkdown(report columnVectorGraphDeep1BGroundtruthLocalityReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Deep1B Groundtruth Locality Probe\n\n")
	fmt.Fprintf(&b, "Generated: `%s`\n\n", report.GeneratedAt)
	fmt.Fprintf(&b, "- Base path: `%s`\n", report.BasePath)
	fmt.Fprintf(&b, "- Base rows locally available: `%d`\n", report.BaseRows)
	fmt.Fprintf(&b, "- Groundtruth path: `%s`\n", report.GroundtruthPath)
	fmt.Fprintf(&b, "- Groundtruth shape: `%d x %d`\n", report.GroundtruthRows, report.GroundtruthDims)
	fmt.Fprintf(&b, "- Remote base.1B range fetch: `%t`\n\n", report.FetchBase1B)
	fmt.Fprintf(&b, "## Query Locality\n\n")
	fmt.Fprintf(&b, "| Query | Loaded top100 | Local | Remote | Missing | Centroid norm | Centroid cos(query) | Avg pairwise cos | Score p50 | Score p90 | GT top10 agreement | Min K recall@10=1 | Min K recall@10>=0.9 |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		fmt.Fprintf(&b, "| %d | %d/%d | %d | %d | %d | %.4f | %.4f | %.4f | %.4f | %.4f | %d/10 | %s | %s |\n",
			q.QueryIndex,
			q.LoadedCount,
			q.GroundtruthCount,
			q.LocalBaseCount,
			q.RemoteBase1BCount,
			q.MissingCount,
			q.CentroidNorm,
			q.CentroidCosineToQuery,
			q.AveragePairwiseCosine,
			q.ScoreQuantiles["0.50"],
			q.ScoreQuantiles["0.90"],
			q.GroundtruthTop10Agreement,
			columnVectorGraphDeep1BFormatRank(q.MinRankRecallAt10Full),
			columnVectorGraphDeep1BFormatRank(q.MinRankRecallAt10GE90),
		)
	}
	fmt.Fprintf(&b, "\n## PCA Recall Curve\n\n")
	fmt.Fprintf(&b, "| Query | Rank | PCA variance | Recall@10 | Recall@20 | Recall@50 | Top10 in approx@20 | Top10 in approx@50 | Top10 in approx@100 | Mean rel L2 | Mean score error | Max score error |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		for _, pca := range q.PCA {
			fmt.Fprintf(&b, "| %d | %d | %.2f%% | %d/10 | %s | %s | %s | %s | %s | %.4f | %.5f | %.5f |\n",
				q.QueryIndex,
				pca.Rank,
				pca.VarianceCaptured*100,
				pca.Top10Overlap,
				columnVectorGraphDeep1BFormatOverlap(pca.Top20Overlap, pca.RecallAt20, 20),
				columnVectorGraphDeep1BFormatOverlap(pca.Top50Overlap, pca.RecallAt50, 50),
				columnVectorGraphDeep1BFormatOverlap(pca.Top10InApproxTop20, pca.Top10RecallAt20, 10),
				columnVectorGraphDeep1BFormatOverlap(pca.Top10InApproxTop50, pca.Top10RecallAt50, 10),
				columnVectorGraphDeep1BFormatOverlap(pca.Top10InApproxTop100, pca.Top10RecallAt100, 10),
				pca.MeanRelativeL2,
				pca.MeanScoreError,
				pca.MaxScoreError,
			)
		}
	}
	fmt.Fprintf(&b, "\n## Official Top100 Oracle Method Tournament\n\n")
	fmt.Fprintf(&b, "These rows are **official top100 local-neighborhood upper-bound probes**. They are valid only for methods that need the query plus its 100 official nearest-neighbor vectors. They do not prove TreeDB can build equivalent production granules, and metadata bytes here are top100-local accounting rather than production amortization.\n\n")
	fmt.Fprintf(&b, "| Query | Method | Family | Row-code B/vector | Metadata B/vector | Total B/vector | Build ms | Compressed top10 | Top10 in approx@20 | Top10 in approx@50 | Top20 in approx@50 | Rerank@20 recall@10 | Rerank@50 recall@10 | Rerank@100 recall@10 | Mean score err | Err/gap10 | Err/gap20 | Err/gap50 | Scan ns/vector |\n")
	fmt.Fprintf(&b, "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		for _, method := range q.Methods {
			fmt.Fprintf(&b, "| %d | `%s` | `%s` | %.2f | %.2f | %.2f | %.3f | %d/10 | %s | %s | %s | %.2f | %.2f | %.2f | %.5f | %.2f | %.2f | %.2f | %.2f |\n",
				q.QueryIndex,
				method.Name,
				method.Family,
				method.RowCodeBytesPerVector,
				method.MetadataBytesPerVector,
				method.TotalBytesPerVector,
				float64(method.BuildNanos)/1e6,
				method.Top10Overlap,
				columnVectorGraphDeep1BFormatOverlap(method.Top10InApproxTop20, method.Top10RecallAt20, 10),
				columnVectorGraphDeep1BFormatOverlap(method.Top10InApproxTop50, method.Top10RecallAt50, 10),
				columnVectorGraphDeep1BFormatOverlap(method.Top20InApproxTop50, method.Top20RecallAt50, 20),
				method.ExactRerankRecallAt10FromTop20,
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
	columnVectorGraphDeep1BRenderGroundtruthAggregateMarkdown(&b, report)
	columnVectorGraphDeep1BRenderGroundtruthMethodAggregateMarkdown(&b, report)
	fmt.Fprintf(&b, "\n## Exact Score Margins\n\n")
	fmt.Fprintf(&b, "| Query | Gap 1/2 | Gap 5/6 | Gap 10/11 | Gap 20/21 | Gap 50/51 | Adjacent p50 | Adjacent p90 | Adjacent max |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		fmt.Fprintf(&b, "| %d | %.6f | %.6f | %.6f | %.6f | %.6f | %.6f | %.6f | %.6f |\n",
			q.QueryIndex,
			q.ScoreMargins["gap_1_2"],
			q.ScoreMargins["gap_5_6"],
			q.ScoreMargins["gap_10_11"],
			q.ScoreMargins["gap_20_21"],
			q.ScoreMargins["gap_50_51"],
			q.ScoreMargins["adjacent_gap_p50"],
			q.ScoreMargins["adjacent_gap_p90"],
			q.ScoreMargins["adjacent_gap_max"],
		)
	}
	return b.String()
}

type columnVectorGraphDeep1BGroundtruthRankAggregate struct {
	count          int
	variance       float64
	top10          float64
	top10At20      float64
	top10At50      float64
	top20At50      float64
	scoreError     float64
	worstTop10     int
	worstTop10At20 int
	worstTop10At50 int
	worstTop20At50 int
}

func columnVectorGraphDeep1BRenderGroundtruthAggregateMarkdown(b *strings.Builder, report columnVectorGraphDeep1BGroundtruthLocalityReport) {
	byRank := make(map[int]*columnVectorGraphDeep1BGroundtruthRankAggregate)
	var ranks []int
	for _, q := range report.Queries {
		for _, pca := range q.PCA {
			agg := byRank[pca.Rank]
			if agg == nil {
				agg = &columnVectorGraphDeep1BGroundtruthRankAggregate{
					worstTop10:     1_000_000,
					worstTop10At20: 1_000_000,
					worstTop10At50: 1_000_000,
					worstTop20At50: 1_000_000,
				}
				byRank[pca.Rank] = agg
				ranks = append(ranks, pca.Rank)
			}
			agg.count++
			agg.variance += pca.VarianceCaptured
			agg.top10 += float64(pca.Top10Overlap)
			agg.top10At20 += float64(pca.Top10InApproxTop20)
			agg.top10At50 += float64(pca.Top10InApproxTop50)
			agg.top20At50 += float64(pca.Top20InApproxTop50)
			agg.scoreError += pca.MeanScoreError
			agg.worstTop10 = min(agg.worstTop10, pca.Top10Overlap)
			agg.worstTop10At20 = min(agg.worstTop10At20, pca.Top10InApproxTop20)
			agg.worstTop10At50 = min(agg.worstTop10At50, pca.Top10InApproxTop50)
			agg.worstTop20At50 = min(agg.worstTop20At50, pca.Top20InApproxTop50)
		}
	}
	sort.Ints(ranks)
	fmt.Fprintf(b, "\n## Aggregate PCA Candidate Gates\n\n")
	fmt.Fprintf(b, "| Rank | Approx code B/vector | Queries | Avg PCA variance | Avg final top10 | Worst final top10 | Avg top10 in approx@20 | Worst top10 in approx@20 | Avg top10 in approx@50 | Worst top10 in approx@50 | Avg top20 in approx@50 | Worst top20 in approx@50 | Avg score error |\n")
	fmt.Fprintf(b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, rank := range ranks {
		agg := byRank[rank]
		if agg.count == 0 {
			continue
		}
		count := float64(agg.count)
		fmt.Fprintf(b, "| %d | %d | %d | %.2f%% | %.2f/10 | %d/10 | %.2f/10 | %d/10 | %.2f/10 | %d/10 | %.2f/20 | %d/20 | %.5f |\n",
			rank,
			rank,
			agg.count,
			100*agg.variance/count,
			agg.top10/count,
			agg.worstTop10,
			agg.top10At20/count,
			agg.worstTop10At20,
			agg.top10At50/count,
			agg.worstTop10At50,
			agg.top20At50/count,
			agg.worstTop20At50,
			agg.scoreError/count,
		)
	}

	type gate struct {
		name   string
		values []int
		failed int
	}
	gates := []gate{
		{name: "final top10 recall >= 9/10"},
		{name: "final top10 recall = 10/10"},
		{name: "exact top10 in approx@20 = 10/10"},
		{name: "exact top10 in approx@50 = 10/10"},
		{name: "exact top20 in approx@50 >= 19/20"},
	}
	appendGateRank := func(gate *gate, rank int) {
		if rank <= 0 {
			gate.failed++
			return
		}
		gate.values = append(gate.values, rank)
	}
	for _, q := range report.Queries {
		appendGateRank(&gates[0], q.MinRankRecallAt10GE90)
		appendGateRank(&gates[1], q.MinRankRecallAt10Full)
		appendGateRank(&gates[2], q.MinRankTop10InApprox20)
		appendGateRank(&gates[3], q.MinRankTop10InApprox50)
		appendGateRank(&gates[4], q.MinRankTop20InApprox50GE95)
	}
	fmt.Fprintf(b, "\n## Minimum Rank Quality Gates\n\n")
	fmt.Fprintf(b, "| Gate | Passed queries | Failed queries | p50 K | p90 K | Worst K |\n")
	fmt.Fprintf(b, "| --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, gate := range gates {
		sort.Ints(gate.values)
		fmt.Fprintf(b, "| %s | %d | %d | %s | %s | %s |\n",
			gate.name,
			len(gate.values),
			gate.failed,
			columnVectorGraphDeep1BFormatRank(columnVectorGraphDeep1BIntQuantile(gate.values, 0.50)),
			columnVectorGraphDeep1BFormatRank(columnVectorGraphDeep1BIntQuantile(gate.values, 0.90)),
			columnVectorGraphDeep1BFormatRank(columnVectorGraphDeep1BIntQuantile(gate.values, 1.00)),
		)
	}
}

func columnVectorGraphDeep1BRenderGroundtruthMethodAggregateMarkdown(b *strings.Builder, report columnVectorGraphDeep1BGroundtruthLocalityReport) {
	type aggregate struct {
		family     string
		name       string
		count      int
		rowBytes   float64
		metaBytes  float64
		totalBytes float64
		buildNanos float64
		scanNanos  float64
		scoreError float64
		top10      []int
		top10At20  []int
		top10At50  []int
		top20At50  []int
		meanGap10  float64
		meanGap20  float64
		meanGap50  float64
	}
	byName := make(map[string]*aggregate)
	var names []string
	for _, q := range report.Queries {
		for _, method := range q.Methods {
			agg := byName[method.Name]
			if agg == nil {
				agg = &aggregate{family: method.Family, name: method.Name}
				byName[method.Name] = agg
				names = append(names, method.Name)
			}
			agg.count++
			agg.rowBytes += method.RowCodeBytesPerVector
			agg.metaBytes += method.MetadataBytesPerVector
			agg.totalBytes += method.TotalBytesPerVector
			agg.buildNanos += float64(method.BuildNanos)
			agg.scanNanos += method.ScanNanosPerVector
			agg.scoreError += method.MeanScoreError
			agg.meanGap10 += method.MeanErrorOverGap10
			agg.meanGap20 += method.MeanErrorOverGap20
			agg.meanGap50 += method.MeanErrorOverGap50
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
		if left.family != right.family {
			return left.family < right.family
		}
		return left.name < right.name
	})
	fmt.Fprintf(b, "\n## Aggregate Top100 Oracle Method Candidate Gates\n\n")
	fmt.Fprintf(b, "| Method | Family | Row-code B/vector | Metadata B/vector | Queries | Avg build ms | p50 compressed top10 | worst compressed top10 | p50 top10@20 | p90 top10@20 | worst top10@20 | p50 top10@50 | p90 top10@50 | worst top10@50 | p50 top20@50 | worst top20@50 | Avg score err | Avg err/gap10 | Avg err/gap20 | Avg err/gap50 | Avg scan ns/vector |\n")
	fmt.Fprintf(b, "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, name := range names {
		agg := byName[name]
		if agg.count == 0 {
			continue
		}
		sort.Ints(agg.top10)
		sort.Ints(agg.top10At20)
		sort.Ints(agg.top10At50)
		sort.Ints(agg.top20At50)
		count := float64(agg.count)
		fmt.Fprintf(b, "| `%s` | `%s` | %.2f | %.2f | %d | %.3f | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/10 | %d/20 | %d/20 | %.5f | %.2f | %.2f | %.2f | %.2f |\n",
			agg.name,
			agg.family,
			agg.rowBytes/count,
			agg.metaBytes/count,
			agg.count,
			agg.buildNanos/count/1e6,
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top10At20, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10At20, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top10At20, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top10At50, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top10At50, 0.90),
			columnVectorGraphDeep1BIntQuantile(agg.top10At50, 0),
			columnVectorGraphDeep1BIntQuantile(agg.top20At50, 0.50),
			columnVectorGraphDeep1BIntQuantile(agg.top20At50, 0),
			agg.scoreError/count,
			agg.meanGap10/count,
			agg.meanGap20/count,
			agg.meanGap50/count,
			agg.scanNanos/count,
		)
	}
}

func columnVectorGraphDeep1BIntQuantile(values []int, quantile float64) int {
	if len(values) == 0 {
		return -1
	}
	if quantile <= 0 {
		return values[0]
	}
	if quantile >= 1 {
		return values[len(values)-1]
	}
	index := int(math.Ceil(quantile*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func columnVectorGraphDeep1BFormatRank(rank int) string {
	if rank <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%d", rank)
}

func columnVectorGraphDeep1BFormatOverlap(overlap int, recall float64, topK int) string {
	_ = recall
	if topK <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%d/%d", overlap, topK)
}
