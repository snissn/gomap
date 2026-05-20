package colgranule

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestColumnVectorGraphDeep1BGroundtruthLocality(t *testing.T) {
	if os.Getenv("COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_LOCALITY") != "1" {
		t.Skip("set COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_LOCALITY=1 to run the opt-in Deep1B groundtruth locality probe")
	}
	queryIndexes := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_QUERIES", []int{0})
	ranks := columnVectorGraphDeep1BEnvIntList(t, "COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_PCA_RANKS", []int{8, 16, 32, 64, columnVectorGraphDeep1BDims})
	localBaseRows := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_BASE_ROWS", 1_000_000)
	fetchBase1B := os.Getenv("COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_BASE1B") == "1"
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
		RequestedQueries: append([]int(nil), queryIndexes...),
		Ranks:            append([]int(nil), ranks...),
	}
	for _, queryIndex := range queryIndexes {
		if queryIndex < 0 || queryIndex >= queryHeader.Rows {
			t.Fatalf("query index=%d outside query rows=%d", queryIndex, queryHeader.Rows)
		}
		query := columnVectorGraphDeep1BReadQuery(t, queryPath, queryHeader, queryIndex)
		truthRows := columnVectorGraphDeep1BReadGroundtruthRow(t, truthPath, truthHeader, queryIndex)
		result := columnVectorGraphDeep1BAnalyzeGroundtruthQuery(t, baseFile, baseHeader, query, queryIndex, truthRows, ranks, fetchBase1B, &cache)
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
	RequestedQueries []int                                           `json:"requested_queries"`
	Ranks            []int                                           `json:"ranks"`
	Queries          []columnVectorGraphDeep1BGroundtruthQueryReport `json:"queries"`
}

type columnVectorGraphDeep1BGroundtruthQueryReport struct {
	QueryIndex                int                                                    `json:"query_index"`
	GroundtruthCount          int                                                    `json:"groundtruth_count"`
	LoadedCount               int                                                    `json:"loaded_count"`
	LocalBaseCount            int                                                    `json:"local_base_count"`
	RemoteBase1BCount         int                                                    `json:"remote_base_1b_count"`
	MissingCount              int                                                    `json:"missing_count"`
	FirstGroundtruthRows      []int                                                  `json:"first_groundtruth_rows"`
	FirstLoadedRows           []int                                                  `json:"first_loaded_rows"`
	GroundtruthTop10Agreement int                                                    `json:"groundtruth_top10_agreement"`
	CentroidNorm              float64                                                `json:"centroid_norm"`
	CentroidCosineToQuery     float64                                                `json:"centroid_cosine_to_query"`
	AveragePairwiseCosine     float64                                                `json:"average_pairwise_cosine"`
	ScoreQuantiles            map[string]float64                                     `json:"score_quantiles"`
	MinRankRecallAt10Full     int                                                    `json:"min_rank_recall_at_10_full,omitempty"`
	MinRankRecallAt10GE90     int                                                    `json:"min_rank_recall_at_10_ge_90,omitempty"`
	PCA                       []columnVectorGraphDeep1BGroundtruthPCAQueryRankReport `json:"pca"`
	Notes                     string                                                 `json:"notes,omitempty"`
}

type columnVectorGraphDeep1BGroundtruthPCAQueryRankReport struct {
	Rank             int     `json:"rank"`
	VarianceCaptured float64 `json:"variance_captured"`
	Top10Overlap     int     `json:"top10_overlap"`
	RecallAt10       float64 `json:"recall_at_10"`
	Top20Overlap     int     `json:"top20_overlap,omitempty"`
	RecallAt20       float64 `json:"recall_at_20,omitempty"`
	Top50Overlap     int     `json:"top50_overlap,omitempty"`
	RecallAt50       float64 `json:"recall_at_50,omitempty"`
	MeanScoreError   float64 `json:"mean_score_error"`
	MaxScoreError    float64 `json:"max_score_error"`
	MeanRelativeL2   float64 `json:"mean_relative_l2"`
	MaxRelativeL2    float64 `json:"max_relative_l2"`
}

type columnVectorGraphDeep1BRemoteRowCache struct {
	client *http.Client
	rows   map[int][]float32
}

func columnVectorGraphDeep1BAnalyzeGroundtruthQuery(tb testing.TB, baseFile *os.File, baseHeader columnVectorGraphDeep1BFbinHeader, query []float32, queryIndex int, truthRows []int, ranks []int, fetchBase1B bool, cache *columnVectorGraphDeep1BRemoteRowCache) columnVectorGraphDeep1BGroundtruthQueryReport {
	tb.Helper()
	loadedRows := make([]int, 0, len(truthRows))
	vectors := make([]float32, 0, len(truthRows)*columnVectorGraphDeep1BDims)
	var localCount int
	var remoteCount int
	var missingCount int
	for _, rowID := range truthRows {
		vector, source, ok := columnVectorGraphDeep1BLoadGroundtruthVector(tb, baseFile, baseHeader, rowID, fetchBase1B, cache)
		if !ok {
			missingCount++
			continue
		}
		if source == "local" {
			localCount++
		} else if source == "remote_base_1b" {
			remoteCount++
		}
		loadedRows = append(loadedRows, rowID)
		vectors = append(vectors, vector...)
	}
	report := columnVectorGraphDeep1BGroundtruthQueryReport{
		QueryIndex:            queryIndex,
		GroundtruthCount:      len(truthRows),
		LoadedCount:           len(loadedRows),
		LocalBaseCount:        localCount,
		RemoteBase1BCount:     remoteCount,
		MissingCount:          missingCount,
		FirstGroundtruthRows:  append([]int(nil), truthRows[:min(10, len(truthRows))]...),
		FirstLoadedRows:       append([]int(nil), loadedRows[:min(10, len(loadedRows))]...),
		MinRankRecallAt10Full: -1,
		MinRankRecallAt10GE90: -1,
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

	validRanks := columnVectorGraphDeep1BFilterRanksForRows(tb, ranks, rows, dims)
	model := columnVectorGraphDeep1BFitLocalPCAModel(tb, vectors, rows, dims, validRanks)
	for _, rank := range validRanks {
		encoding := columnVectorGraphDeep1BEncodeLocalPCARank(vectors, invNorms, model, rank, query, queryInvNorm, rows, dims)
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
		}
		if rows >= 50 {
			row.Top50Overlap, row.RecallAt50 = columnVectorGraphDeep1BApproxRecall(exactScores, encoding.approxScores, 50)
		}
		if row.RecallAt10 == 1 && report.MinRankRecallAt10Full < 0 {
			report.MinRankRecallAt10Full = rank
		}
		if row.RecallAt10 >= 0.9 && report.MinRankRecallAt10GE90 < 0 {
			report.MinRankRecallAt10GE90 = rank
		}
		report.PCA = append(report.PCA, row)
	}
	return report
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
	if cached, ok := cache.rows[rowID]; ok {
		return append([]float32(nil), cached...), "remote_base_1b", true
	}
	vector := columnVectorGraphDeep1BFetchBase1BRow(tb, cache.client, rowID, columnVectorGraphDeep1BDims)
	cache.rows[rowID] = append([]float32(nil), vector...)
	return vector, "remote_base_1b", true
}

func columnVectorGraphDeep1BFetchBase1BRow(tb testing.TB, client *http.Client, rowID int, dims int) []float32 {
	tb.Helper()
	rowBytes := dims * 4
	start := int64(8 + rowID*rowBytes)
	end := start + int64(rowBytes) - 1
	req, err := http.NewRequest(http.MethodGet, columnVectorGraphDeep1BBase1BURL, nil)
	if err != nil {
		tb.Fatalf("build Deep1B base.1B row request: %v", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := client.Do(req)
	if err != nil {
		tb.Fatalf("fetch Deep1B base.1B row %d: %v", rowID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		tb.Fatalf("fetch Deep1B base.1B row %d status=%s want 206 Partial Content", rowID, resp.Status)
	}
	raw := make([]byte, rowBytes)
	if _, err := io.ReadFull(resp.Body, raw); err != nil {
		tb.Fatalf("read Deep1B base.1B row %d: %v", rowID, err)
	}
	vector := make([]float32, dims)
	for i := range vector {
		vector[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
	}
	return vector
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
	fmt.Fprintf(&b, "| Query | Rank | PCA variance | Recall@10 | Recall@20 | Recall@50 | Mean rel L2 | Mean score error | Max score error |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, q := range report.Queries {
		for _, pca := range q.PCA {
			fmt.Fprintf(&b, "| %d | %d | %.2f%% | %d/10 | %s | %s | %.4f | %.5f | %.5f |\n",
				q.QueryIndex,
				pca.Rank,
				pca.VarianceCaptured*100,
				pca.Top10Overlap,
				columnVectorGraphDeep1BFormatOverlap(pca.Top20Overlap, pca.RecallAt20, 20),
				columnVectorGraphDeep1BFormatOverlap(pca.Top50Overlap, pca.RecallAt50, 50),
				pca.MeanRelativeL2,
				pca.MeanScoreError,
				pca.MaxScoreError,
			)
		}
	}
	return b.String()
}

func columnVectorGraphDeep1BFormatRank(rank int) string {
	if rank <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%d", rank)
}

func columnVectorGraphDeep1BFormatOverlap(overlap int, recall float64, topK int) string {
	if recall == 0 && overlap == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%d/%d", overlap, topK)
}
