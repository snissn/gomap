package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

const (
	qdrantComparisonArtifactSchema      = "treedb-rag-qdrant-comparison/v1"
	applicationComparisonSchema         = "treedb-rag-system-comparison/v1"
	qdrantSparseWeightSignificantDigits = 14
	qdrantClientLockSHA256              = "4c66f563c863801ab692132c5089075dc398959771784756ee9d14f7a353e595"
)

var qdrantSparseTokenPattern = regexp.MustCompile(`[a-z0-9]+`)
var qdrantServerIdentityPattern = regexp.MustCompile(`^pid:([1-9][0-9]*):.+\|reopened_pid:([1-9][0-9]*)$`)

func isSHA256(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == sha256.Size
}

type qdrantPythonFloat float64

func (value qdrantPythonFloat) MarshalJSON() ([]byte, error) {
	number := float64(value)
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return nil, fmt.Errorf("non-finite sparse vector value")
	}
	text := strconv.FormatFloat(number, 'g', -1, 64)
	if !strings.ContainsAny(text, ".e") {
		text += ".0"
	}
	return []byte(text), nil
}

type qdrantComparisonIndexProof struct {
	IndexedVectorsCount int            `json:"indexed_vectors_count"`
	FilterCardinalities map[string]int `json:"filter_cardinalities"`
}

type qdrantComparisonServer struct {
	Version            string                     `json:"version"`
	Deployment         string                     `json:"deployment"`
	BinarySHA256       string                     `json:"binary_sha256"`
	ReleaseAssetSHA256 string                     `json:"release_asset_sha256"`
	Identity           string                     `json:"identity"`
	LocalMode          bool                       `json:"local_mode"`
	Config             map[string]any             `json:"config"`
	IndexProof         qdrantComparisonIndexProof `json:"index_proof"`
}

type qdrantComparisonProcessSample struct {
	PID               int     `json:"pid"`
	RSSBytes          int64   `json:"rss_bytes"`
	CPUSeconds        float64 `json:"cpu_seconds"`
	CapturedUnixNanos int64   `json:"captured_unix_nanos"`
}

type qdrantComparisonResources struct {
	HostPIDMetrics        string                          `json:"host_pid_metrics"`
	ProcessSamples        []qdrantComparisonProcessSample `json:"process_samples"`
	PeakObservedRSSBytes  int64                           `json:"peak_observed_rss_bytes"`
	CPUSeconds            float64                         `json:"cpu_seconds"`
	DurableBytes          int64                           `json:"durable_bytes"`
	DurableBytesSemantics string                          `json:"durable_bytes_semantics"`
}

type qdrantComparisonBuild struct {
	Seconds float64 `json:"seconds"`
	Points  int     `json:"points"`
}

type qdrantComparisonReopen struct {
	Attempted                bool     `json:"attempted"`
	Succeeded                bool     `json:"succeeded"`
	OptimizerUpdateTriggered bool     `json:"optimizer_update_triggered"`
	Version                  string   `json:"version"`
	Status                   string   `json:"status"`
	PointCount               int      `json:"point_count"`
	IndexedVectorsCount      int      `json:"indexed_vectors_count"`
	PayloadIndexes           []string `json:"payload_indexes"`
	Seconds                  float64  `json:"seconds"`
}

type qdrantComparisonRouteProof struct {
	API              string   `json:"api"`
	NamedVectors     []string `json:"named_vectors"`
	Fusion           string   `json:"fusion,omitempty"`
	Fallbacks        int      `json:"fallbacks"`
	ExhaustiveSearch bool     `json:"exhaustive_search"`
	BoundedFetch     bool     `json:"bounded_fetch"`
}

type qdrantComparisonSummary struct {
	QPS          float64 `json:"qps"`
	LatencyMSP50 float64 `json:"latency_ms_p50"`
	LatencyMSP95 float64 `json:"latency_ms_p95"`
	LatencyMSP99 float64 `json:"latency_ms_p99"`
}
type qdrantComparisonSample struct {
	Repetition   int      `json:"repetition"`
	Ordinal      int      `json:"ordinal"`
	QueryID      string   `json:"query_id"`
	SearchMS     float64  `json:"search_ms"`
	FetchMS      float64  `json:"fetch_ms"`
	TotalMS      float64  `json:"total_ms"`
	ResultIDs    []string `json:"result_ids"`
	FetchedCount int      `json:"fetched_count"`
	FetchedBytes int      `json:"fetched_bytes"`
}

type qdrantComparisonRepetition struct {
	Repetition  int     `json:"repetition"`
	Order       string  `json:"order"`
	Samples     int     `json:"samples"`
	WallSeconds float64 `json:"wall_seconds"`
	QPS         float64 `json:"qps"`
}

type qdrantComparisonCell struct {
	Route                 string                       `json:"route"`
	Filter                string                       `json:"filter"`
	Equivalence           string                       `json:"equivalence"`
	TimingSemantics       string                       `json:"timing_semantics"`
	Warmups               int                          `json:"warmups"`
	Repetitions           int                          `json:"repetitions"`
	Samples               []qdrantComparisonSample     `json:"samples"`
	RepetitionPerformance []qdrantComparisonRepetition `json:"repetition_performance"`
	Summary               qdrantComparisonSummary      `json:"summary"`
	Quality               qualityMetrics               `json:"quality"`
	Leakage               int                          `json:"leakage"`
	Errors                int                          `json:"errors"`
	Timeouts              int                          `json:"timeouts"`
	FetchMaxCount         int                          `json:"fetch_max_count"`
	RouteProof            qdrantComparisonRouteProof   `json:"route_proof"`
}

type qdrantComparisonArtifact struct {
	Schema                 string                    `json:"schema"`
	Backend                string                    `json:"backend"`
	HarnessRevision        string                    `json:"harness_revision"`
	ClientVersion          string                    `json:"client_version"`
	ClientLockSHA256       string                    `json:"client_lock_sha256"`
	PythonVersion          string                    `json:"python_version"`
	PythonPlatform         string                    `json:"python_platform"`
	PythonImplementation   string                    `json:"python_implementation"`
	PythonExecutableSHA256 string                    `json:"python_executable_sha256"`
	ManifestSHA256         string                    `json:"manifest_sha256"`
	FixtureSHA256          string                    `json:"fixture_sha256"`
	SemanticVectorSHA256   string                    `json:"semantic_vector_sha256"`
	SparseVectorSHA256     string                    `json:"sparse_vector_sha256"`
	ConfigSHA256           string                    `json:"config_sha256"`
	SourceCount            int                       `json:"source_count"`
	ChunkCount             int                       `json:"chunk_count"`
	QueryCount             int                       `json:"query_count"`
	Server                 qdrantComparisonServer    `json:"server"`
	Resources              qdrantComparisonResources `json:"resources"`
	Build                  qdrantComparisonBuild     `json:"build"`
	QuerySeconds           float64                   `json:"query_seconds"`
	Reopen                 qdrantComparisonReopen    `json:"reopen"`
	Cells                  []qdrantComparisonCell    `json:"cells"`
	Failures               []string                  `json:"failures"`
}

type applicationComparisonRow struct {
	Backend      string         `json:"backend"`
	Route        string         `json:"route"`
	Filter       string         `json:"filter"`
	Equivalence  string         `json:"equivalence"`
	Samples      int            `json:"samples"`
	Repetitions  int            `json:"repetitions"`
	QPS          float64        `json:"qps"`
	LatencyMSP50 float64        `json:"latency_ms_p50"`
	LatencyMSP95 float64        `json:"latency_ms_p95"`
	LatencyMSP99 float64        `json:"latency_ms_p99"`
	Quality      qualityMetrics `json:"quality"`
}

type applicationComparisonReport struct {
	Schema                       string                     `json:"schema"`
	State                        string                     `json:"state"`
	HarnessRevision              string                     `json:"harness_revision"`
	TreeDBBinarySHA256           string                     `json:"treedb_binary_sha256"`
	TreeDBProcessResources       comparisonProcessResources `json:"treedb_process_resources"`
	TreeDBStorageBytes           int64                      `json:"treedb_storage_bytes"`
	QdrantResources              qdrantComparisonResources  `json:"qdrant_resources"`
	QdrantClientVersion          string                     `json:"qdrant_client_version"`
	QdrantClientLockSHA256       string                     `json:"qdrant_client_lock_sha256"`
	QdrantPythonVersion          string                     `json:"qdrant_python_version"`
	QdrantPythonPlatform         string                     `json:"qdrant_python_platform"`
	QdrantPythonImplementation   string                     `json:"qdrant_python_implementation"`
	QdrantPythonExecutableSHA256 string                     `json:"qdrant_python_executable_sha256"`
	QdrantServerVersion          string                     `json:"qdrant_server_version"`
	QdrantServerBinarySHA256     string                     `json:"qdrant_server_binary_sha256"`
	QdrantReleaseAssetSHA256     string                     `json:"qdrant_release_asset_sha256"`
	ManifestSHA256               string                     `json:"manifest_sha256"`
	FixtureSHA256                string                     `json:"fixture_sha256"`
	SemanticVectorSHA256         string                     `json:"semantic_vector_sha256"`
	ConfigSHA256                 string                     `json:"config_sha256"`
	Rows                         []applicationComparisonRow `json:"rows"`
	Dispositions                 []string                   `json:"dispositions"`
}

func readJSONFile(path string, value any) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, value); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	return raw, nil
}

func validateComparisonManifest(raw []byte, manifest *applicationComparisonManifest) (string, error) {
	canonical, err := applicationComparisonManifestBytes()
	if err != nil {
		return "", err
	}
	if string(raw) != string(canonical) {
		return "", fmt.Errorf("comparison manifest is not the canonical deterministic export")
	}
	if manifest.Schema != applicationComparisonManifestSchema || manifest.ProductBaseSHA != applicationComparisonProductBase ||
		manifest.FixtureSHA256 != applicationFixtureExpectedDigest || manifest.SemanticVectorSHA256 != semanticVectorsExpectedDigest ||
		manifest.ConfigSHA256 != applicationComparisonConfigDigest(manifest.Config) {
		return "", fmt.Errorf("comparison manifest bindings mismatch")
	}
	if len(manifest.Sources) != 18 || len(manifest.Chunks) != 54 || len(manifest.Queries) != 3 || len(manifest.Filters) != 4 {
		return "", fmt.Errorf("comparison manifest cardinality mismatch")
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func qdrantSparseVectorCanonicalBytes(manifest applicationComparisonManifest) ([]byte, error) {
	documentTokens := make([][]string, len(manifest.Chunks))
	vocabularySet := map[string]bool{}
	documentFrequency := map[string]int{}
	for i, chunk := range manifest.Chunks {
		tokens := qdrantSparseTokenPattern.FindAllString(strings.ToLower(chunk.Content), -1)
		documentTokens[i] = tokens
		seen := map[string]bool{}
		for _, token := range tokens {
			vocabularySet[token] = true
			if !seen[token] {
				documentFrequency[token]++
				seen[token] = true
			}
		}
	}
	queryTokens := make(map[string][]string, len(manifest.Queries))
	for _, query := range manifest.Queries {
		tokens := qdrantSparseTokenPattern.FindAllString(strings.ToLower(query.Text), -1)
		queryTokens[query.ID] = tokens
		for _, token := range tokens {
			vocabularySet[token] = true
		}
	}
	vocabulary := make([]string, 0, len(vocabularySet))
	for token := range vocabularySet {
		vocabulary = append(vocabulary, token)
	}
	sort.Strings(vocabulary)
	tokenIndexes := make(map[string]int, len(vocabulary))
	for index, token := range vocabulary {
		tokenIndexes[token] = index
	}

	serial := struct {
		Documents  map[string][][]any `json:"documents"`
		Queries    map[string][][]any `json:"queries"`
		Vocabulary []string           `json:"vocabulary"`
	}{
		Documents:  make(map[string][][]any, len(manifest.Chunks)),
		Queries:    make(map[string][][]any, len(manifest.Queries)),
		Vocabulary: vocabulary,
	}
	totalTokens := 0
	for _, tokens := range documentTokens {
		totalTokens += len(tokens)
	}
	documentCount := float64(len(documentTokens))
	averageLength := float64(totalTokens) / documentCount
	for i, chunk := range manifest.Chunks {
		counts := map[string]int{}
		for _, token := range documentTokens[i] {
			counts[token]++
		}
		values := make([][]any, 0, len(counts))
		for _, token := range vocabulary {
			termFrequency := counts[token]
			if termFrequency == 0 {
				continue
			}
			documentFrequencyValue := float64(documentFrequency[token])
			inverseDocumentFrequency := math.Log(1 + (documentCount-documentFrequencyValue+.5)/(documentFrequencyValue+.5))
			tf := float64(termFrequency)
			length := float64(len(documentTokens[i]))
			weight := inverseDocumentFrequency * tf * (manifest.Config.SparseBM25K1 + 1) /
				(tf + manifest.Config.SparseBM25K1*(1-manifest.Config.SparseBM25B+manifest.Config.SparseBM25B*length/averageLength))
			weight, err := strconv.ParseFloat(strconv.FormatFloat(weight, 'g', qdrantSparseWeightSignificantDigits, 64), 64)
			if err != nil {
				return nil, err
			}
			values = append(values, []any{tokenIndexes[token], qdrantPythonFloat(weight)})
		}
		serial.Documents[chunk.ID] = values
	}
	for _, query := range manifest.Queries {
		counts := map[string]int{}
		for _, token := range queryTokens[query.ID] {
			counts[token]++
		}
		values := make([][]any, 0, len(counts))
		for _, token := range vocabulary {
			if counts[token] != 0 {
				values = append(values, []any{tokenIndexes[token], qdrantPythonFloat(counts[token])})
			}
		}
		serial.Queries[query.ID] = values
	}
	raw, err := json.Marshal(serial)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func expectedQdrantSparseVectorSHA256(manifest applicationComparisonManifest) (string, error) {
	raw, err := qdrantSparseVectorCanonicalBytes(manifest)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func currentExecutableSHA256() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	raw, err := os.ReadFile(executable)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func validateTreeDBComparisonArtifact(artifact *treeDBComparisonArtifact, manifest applicationComparisonManifest, manifestSHA string) ([]applicationComparisonRow, error) {
	if artifact.Schema != treeDBComparisonArtifactSchema || artifact.Authority != "BOUNDED_COMPARISON_EVIDENCE" ||
		artifact.ManifestSHA256 != manifestSHA || artifact.ProductBaseSHA != manifest.ProductBaseSHA ||
		artifact.FixtureSHA256 != manifest.FixtureSHA256 || artifact.SemanticVectorSHA256 != manifest.SemanticVectorSHA256 ||
		artifact.ConfigSHA256 != manifest.ConfigSHA256 || applicationComparisonConfigDigest(artifact.Config) != manifest.ConfigSHA256 ||
		artifact.SourceCount != len(manifest.Sources) || artifact.ChunkCount != len(manifest.Chunks) || artifact.QueryCount != len(manifest.Queries) {
		return nil, fmt.Errorf("TreeDB artifact authority/manifest/hash/config/cardinality binding mismatch")
	}
	executableSHA, err := currentExecutableSHA256()
	if err != nil {
		return nil, fmt.Errorf("hash comparator executable: %w", err)
	}
	if !isFullRevision(artifact.HarnessRevision) || artifact.BinarySHA256 != executableSHA {
		return nil, fmt.Errorf("TreeDB artifact lacks exact clean harness/binary identity")
	}
	if len(artifact.Failures) != 0 || artifact.StorageBytes <= 0 || artifact.BuildReopenSeconds <= 0 ||
		artifact.BuildReopenSeconds > float64(manifest.Config.PhaseTimeoutSeconds) || artifact.QuerySeconds <= 0 ||
		artifact.QuerySeconds > float64(manifest.Config.PhaseTimeoutSeconds) {
		return nil, fmt.Errorf("TreeDB artifact failed or lacks bounded build/query/storage evidence")
	}
	resources := artifact.ProcessResources
	if !resources.Available || !resources.Before.Available || !resources.After.Available ||
		resources.Before.CapturedUnixNanos <= 0 || resources.After.CapturedUnixNanos <= resources.Before.CapturedUnixNanos ||
		resources.Before.CPUSeconds < 0 || resources.After.CPUSeconds < resources.Before.CPUSeconds ||
		resources.Before.PeakRSSBytes <= 0 || resources.After.PeakRSSBytes < resources.Before.PeakRSSBytes ||
		resources.CPUSeconds <= 0 || !comparisonFloatMatches(resources.CPUSeconds, resources.After.CPUSeconds-resources.Before.CPUSeconds) ||
		resources.PeakRSSBytes != resources.After.PeakRSSBytes ||
		resources.CPUSemantics != "getrusage(RUSAGE_SELF) user+system CPU; cumulative before/after snapshots, aggregate is after-before" ||
		resources.RSSSemantics != "getrusage(RUSAGE_SELF) process high-water RSS; before/after snapshots, aggregate is after high-water; Darwin bytes, Linux KiB normalized to bytes" ||
		resources.Scope != "fresh comparison process; build, lifecycle reopen, and all 12 query cells" {
		return nil, fmt.Errorf("TreeDB artifact lacks independently verifiable process CPU/RSS snapshots, aggregates, or exact semantics")
	}
	if err := validateLifecycleEvidence("semantic_minilm", artifact.Lifecycle); err != nil {
		return nil, err
	}
	routeNames := map[string]string{"text_only": "lexical", "vector_only": "dense", "hybrid": "hybrid"}
	seen := map[string]bool{}
	rows := make([]applicationComparisonRow, 0, 12)
	for _, row := range artifact.Rows {
		route, ok := routeNames[row.Cell.Route]
		if !ok || !containsString(applicationFilterOrder, row.Cell.Filter) || row.Cell.Projection != "fetch_topk" ||
			row.Cell.Collapse != "disabled" || row.Cell.Surface != "direct_collection" ||
			row.Cell.Embedding != "semantic_minilm" || row.Cell.Clients != 1 {
			return nil, fmt.Errorf("TreeDB artifact contains non-comparison cell %+v", row.Cell)
		}
		wantVectorRoute := "none"
		if route == "dense" || route == "hybrid" {
			wantVectorRoute = "declared_column_graph_exact"
		}
		if row.Cell.VectorRoute != wantVectorRoute {
			return nil, fmt.Errorf("TreeDB artifact cell %s/%s vector route=%q want %q", route, row.Cell.Filter, row.Cell.VectorRoute, wantVectorRoute)
		}
		wantWorkDigest := applicationCellWorkDigest(row.Cell, manifest.Config.TopK, manifest.Config.CandidateLimit,
			manifest.Config.TreeDBEfSearch, manifest.Config.TreeDBEfConstruction, manifest.Config.TreeDBM)
		if row.Comparison.WorkDigest != wantWorkDigest {
			return nil, fmt.Errorf("TreeDB artifact cell %s/%s work digest mismatch", route, row.Cell.Filter)
		}
		key := route + "\x00" + row.Cell.Filter
		if seen[key] {
			return nil, fmt.Errorf("TreeDB artifact has duplicate comparison cell %s/%s", route, row.Cell.Filter)
		}
		seen[key] = true
		for _, counter := range ragCounterKeys {
			if _, ok := row.Counters[counter.Key]; !ok {
				return nil, fmt.Errorf("TreeDB artifact cell %s/%s lacks required counter %q", route, row.Cell.Filter, counter.Key)
			}
		}
		for _, key := range []string{"cross_tenant_results", "cross_workspace_results", "cross_range_results"} {
			if _, ok := row.Counters[key]; !ok {
				return nil, fmt.Errorf("TreeDB artifact cell %s/%s lacks required counter %q", route, row.Cell.Filter, key)
			}
		}
		if row.Status != "supported" || row.Errors != 0 || row.Counters["full_document_scan_fallbacks"] != 0 ||
			row.Counters["cross_tenant_results"] != 0 || row.Counters["cross_workspace_results"] != 0 ||
			row.Counters["cross_range_results"] != 0 || row.Counters["documents_fetched"] > float64(manifest.Config.TopK) ||
			len(row.Repetitions) != manifest.Config.Repetitions ||
			len(row.Samples) != manifest.Config.SamplesPerCell*manifest.Config.Repetitions {
			return nil, fmt.Errorf("TreeDB artifact cell %s/%s is partial, leaking, unbounded, failed, or fell back", route, row.Cell.Filter)
		}
		if route == "lexical" && (row.Counters["vector_candidates_examined"] != 0 || row.Counters["vector_candidates_returned"] != 0) {
			return nil, fmt.Errorf("TreeDB lexical cell %s used vector candidates", row.Cell.Filter)
		}
		if (route == "dense" || route == "hybrid") &&
			(row.Counters["vector_candidates_examined"] <= 0 || row.Counters["vector_candidates_returned"] <= 0) {
			return nil, fmt.Errorf("TreeDB %s cell %s lacks exact vector route counters", route, row.Cell.Filter)
		}
		if route == "hybrid" && (row.Counters["text_candidates_returned"] <= 0 || row.Counters["candidates_fused"] <= 0) {
			return nil, fmt.Errorf("TreeDB hybrid cell %s lacks text/fusion route counters", row.Cell.Filter)
		}
		latencies, quality, err := recomputeTreeDBCellEvidence(row, manifest)
		if err != nil {
			return nil, fmt.Errorf("TreeDB artifact cell %s/%s sample evidence: %w", route, row.Cell.Filter, err)
		}
		p50, _ := percentile(latencies, 50)
		p95, _ := percentile(latencies, 95)
		p99, _ := percentile(latencies, 99)
		if !comparisonFloatMatches(row.LatencyMSMean, mean(latencies)) ||
			!comparisonFloatMatches(row.LatencyMSP50, p50) ||
			!comparisonFloatMatches(row.LatencyMSP95, p95) ||
			!comparisonFloatMatches(row.LatencyMSP99, p99) {
			return nil, fmt.Errorf("TreeDB artifact cell %s/%s latency summary does not match raw samples", route, row.Cell.Filter)
		}
		if !qdrantQualityMatches(row.Quality, quality) {
			return nil, fmt.Errorf("TreeDB artifact cell %s/%s quality does not match raw rankings", route, row.Cell.Filter)
		}
		qpsMean := 0.0
		for rep, performance := range row.Repetitions {
			retainedWall := 0.0
			for _, sample := range row.Samples {
				if sample.Repetition == rep {
					retainedWall += sample.Millis / 1000
				}
			}
			wantOrder := "forward"
			if rep%2 == 1 {
				wantOrder = "reverse"
			}
			expectedQPS := float64(manifest.Config.SamplesPerCell) / performance.WallSeconds
			if performance.Repetition != rep || performance.Order != wantOrder ||
				performance.Samples != manifest.Config.SamplesPerCell || performance.WallSeconds <= 0 ||
				performance.WallSeconds+1e-9 < retainedWall ||
				!comparisonFloatMatches(performance.QPS, expectedQPS) {
				return nil, fmt.Errorf("TreeDB artifact cell %s/%s has invalid repetition wall evidence", route, row.Cell.Filter)
			}
			qpsMean += performance.QPS
		}
		qpsMean /= float64(manifest.Config.Repetitions)
		if !comparisonFloatMatches(row.QPSMean, qpsMean) {
			return nil, fmt.Errorf("TreeDB artifact cell %s/%s QPS is not mean-per-repetition", route, row.Cell.Filter)
		}
		equivalence := "directional"
		rows = append(rows, applicationComparisonRow{Backend: "treedb", Route: route, Filter: row.Cell.Filter,
			Equivalence: equivalence, Samples: len(row.Samples), Repetitions: len(row.Repetitions), QPS: row.QPSMean,
			LatencyMSP50: row.LatencyMSP50, LatencyMSP95: row.LatencyMSP95, LatencyMSP99: row.LatencyMSP99, Quality: row.Quality})
	}
	if len(rows) != 12 {
		return nil, fmt.Errorf("TreeDB artifact comparison cells=%d want 12", len(rows))
	}
	return rows, nil
}

func comparisonFloatMatches(got, want float64) bool {
	return math.Abs(got-want) <= 1e-9*math.Max(1, math.Abs(want))
}

func qdrantServerIdentityPIDs(identity string) ([2]int, error) {
	matches := qdrantServerIdentityPattern.FindStringSubmatch(identity)
	if len(matches) != 3 {
		return [2]int{}, fmt.Errorf("invalid Qdrant server identity")
	}
	initial, initialErr := strconv.Atoi(matches[1])
	reopened, reopenedErr := strconv.Atoi(matches[2])
	if initialErr != nil || reopenedErr != nil || initial == reopened {
		return [2]int{}, fmt.Errorf("invalid Qdrant server identity PIDs")
	}
	return [2]int{initial, reopened}, nil
}

func qdrantQualityMatches(got, want qualityMetrics) bool {
	gotFloats := [...]float64{
		got.PrecisionAt5, got.PrecisionAt10, got.NDCGAt5, got.NDCGAt10, got.MRRAt10, got.HitRateAt10,
		got.ChunkRecallAt5, got.ChunkRecallAt10, got.ParentRecallAt5, got.ParentRecallAt10,
		got.RelevantChunksMean, got.RelevantParentsMean, got.MaxAchievableChunkRecallAt5,
		got.MaxAchievableChunkRecallAt10, got.MaxAchievableParentRecallAt5, got.MaxAchievableParentRecallAt10,
	}
	wantFloats := [...]float64{
		want.PrecisionAt5, want.PrecisionAt10, want.NDCGAt5, want.NDCGAt10, want.MRRAt10, want.HitRateAt10,
		want.ChunkRecallAt5, want.ChunkRecallAt10, want.ParentRecallAt5, want.ParentRecallAt10,
		want.RelevantChunksMean, want.RelevantParentsMean, want.MaxAchievableChunkRecallAt5,
		want.MaxAchievableChunkRecallAt10, want.MaxAchievableParentRecallAt5, want.MaxAchievableParentRecallAt10,
	}
	for i := range gotFloats {
		if !comparisonFloatMatches(gotFloats[i], wantFloats[i]) {
			return false
		}
	}
	return got.MaxPerParentResults == want.MaxPerParentResults &&
		got.CollapseRejections == want.CollapseRejections &&
		got.CollapseExhaustions == want.CollapseExhaustions &&
		got.TextAttributedResults == want.TextAttributedResults &&
		got.VectorAttributedResults == want.VectorAttributedResults &&
		got.TextVectorOverlapResults == want.TextVectorOverlapResults &&
		got.AttributionMode == want.AttributionMode
}

func recomputeQdrantCellEvidence(cell qdrantComparisonCell, manifest applicationComparisonManifest) ([]float64, qualityMetrics, error) {
	chunks := make(map[string]applicationComparisonChunk, len(manifest.Chunks))
	for _, chunk := range manifest.Chunks {
		chunks[chunk.ID] = chunk
	}
	var filter applicationComparisonFilter
	filterFound := false
	for _, candidate := range manifest.Filters {
		if candidate.ID == cell.Filter {
			filter, filterFound = candidate, true
			break
		}
	}
	if !filterFound {
		return nil, qualityMetrics{}, fmt.Errorf("unknown filter %q", cell.Filter)
	}

	latencies := make([]float64, 0, len(cell.Samples))
	queryCoverage := make(map[string]bool, len(manifest.Queries))
	fetchMax := 0
	for sampleIndex, sample := range cell.Samples {
		wantRepetition := sampleIndex / manifest.Config.SamplesPerCell
		wantOrdinal := sampleIndex % manifest.Config.SamplesPerCell
		wantQueryIndex := wantOrdinal % len(manifest.Queries)
		if wantRepetition%2 == 1 {
			wantQueryIndex = len(manifest.Queries) - 1 - wantQueryIndex
		}
		wantQuery := manifest.Queries[wantQueryIndex].ID
		if sample.Repetition != wantRepetition || sample.Ordinal != wantOrdinal || sample.QueryID != wantQuery ||
			sample.SearchMS <= 0 || sample.FetchMS <= 0 ||
			sample.TotalMS+1e-9 < sample.SearchMS+sample.FetchMS ||
			sample.FetchedCount != len(sample.ResultIDs) || sample.FetchedCount == 0 ||
			sample.FetchedCount > manifest.Config.TopK || sample.FetchedBytes <= 0 {
			return nil, qualityMetrics{}, fmt.Errorf("invalid sample order/query/timing/fetch at index %d", sampleIndex)
		}
		rankingIDs := map[string]bool{}
		for _, resultID := range sample.ResultIDs {
			chunk, ok := chunks[resultID]
			if !ok || rankingIDs[resultID] ||
				(filter.Tenant != "" && chunk.Tenant != filter.Tenant) ||
				(filter.Workspace != "" && chunk.Workspace != filter.Workspace) ||
				(filter.UpdatedYearGTE != 0 && chunk.UpdatedYear < filter.UpdatedYearGTE) {
				return nil, qualityMetrics{}, fmt.Errorf("invalid or duplicate result %q at sample %d", resultID, sampleIndex)
			}
			rankingIDs[resultID] = true
		}
		fetchMax = max(fetchMax, sample.FetchedCount)
		latencies = append(latencies, sample.TotalMS)
		queryCoverage[sample.QueryID] = true
	}
	if cell.FetchMaxCount != fetchMax || len(queryCoverage) != len(manifest.Queries) {
		return nil, qualityMetrics{}, fmt.Errorf("fetch/query coverage mismatch")
	}

	var quality qualityMetrics
	for _, sample := range cell.Samples {
		var query applicationComparisonQuery
		for _, candidate := range manifest.Queries {
			if candidate.ID == sample.QueryID {
				query = candidate
				break
			}
		}
		var judgment applicationComparisonCase
		judgmentFound := false
		for _, candidate := range query.Cases {
			if candidate.Filter == cell.Filter {
				judgment, judgmentFound = candidate, true
				break
			}
		}
		if !judgmentFound {
			return nil, qualityMetrics{}, fmt.Errorf("query %s lacks filter judgment %s", query.ID, cell.Filter)
		}
		ranked := append([]string(nil), sample.ResultIDs...)
		parents := make([]string, 0, len(ranked))
		perParent := map[string]int{}
		for _, id := range ranked {
			parent := chunks[id].ParentID
			parents = append(parents, parent)
			perParent[parent]++
			quality.MaxPerParentResults = max(quality.MaxPerParentResults, perParent[parent])
		}
		for len(ranked) < manifest.Config.TopK {
			ranked = append(ranked, "")
		}
		chunkRelevant := stringSet(judgment.RelevantChunks)
		parentRelevant := stringSet(judgment.RelevantParents)
		p5, _ := precisionAtK(ranked, chunkRelevant, 5)
		p10, _ := precisionAtK(ranked, chunkRelevant, 10)
		r5, _ := recallAtK(ranked, chunkRelevant, 5)
		r10, _ := recallAtK(ranked, chunkRelevant, 10)
		mrr, _ := mrrAtK(ranked, chunkRelevant, 10)
		quality.PrecisionAt5 += p5
		quality.PrecisionAt10 += p10
		quality.NDCGAt5 += ndcgAtK(ranked, chunkRelevant, 5)
		quality.NDCGAt10 += ndcgAtK(ranked, chunkRelevant, 10)
		quality.MRRAt10 += mrr
		if mrr > 0 {
			quality.HitRateAt10++
		}
		quality.ChunkRecallAt5 += r5
		quality.ChunkRecallAt10 += r10
		quality.ParentRecallAt5 += parentRecallAtK(parents, parentRelevant, 5)
		quality.ParentRecallAt10 += parentRecallAtK(parents, parentRelevant, 10)
		quality.RelevantChunksMean += float64(len(chunkRelevant))
		quality.RelevantParentsMean += float64(len(parentRelevant))
		quality.MaxAchievableChunkRecallAt5 += maxAchievableRecall(len(chunkRelevant), 5)
		quality.MaxAchievableChunkRecallAt10 += maxAchievableRecall(len(chunkRelevant), 10)
		quality.MaxAchievableParentRecallAt5 += maxAchievableRecall(len(parentRelevant), 5)
		quality.MaxAchievableParentRecallAt10 += maxAchievableRecall(len(parentRelevant), 10)
	}
	sampleCount := float64(len(cell.Samples))
	quality.PrecisionAt5 /= sampleCount
	quality.PrecisionAt10 /= sampleCount
	quality.NDCGAt5 /= sampleCount
	quality.NDCGAt10 /= sampleCount
	quality.MRRAt10 /= sampleCount
	quality.HitRateAt10 /= sampleCount
	quality.ChunkRecallAt5 /= sampleCount
	quality.ChunkRecallAt10 /= sampleCount
	quality.ParentRecallAt5 /= sampleCount
	quality.ParentRecallAt10 /= sampleCount
	quality.RelevantChunksMean /= sampleCount
	quality.RelevantParentsMean /= sampleCount
	quality.MaxAchievableChunkRecallAt5 /= sampleCount
	quality.MaxAchievableChunkRecallAt10 /= sampleCount
	quality.MaxAchievableParentRecallAt5 /= sampleCount
	quality.MaxAchievableParentRecallAt10 /= sampleCount
	quality.AttributionMode = "qdrant_native_route"
	return latencies, quality, nil
}

func recomputeTreeDBCellEvidence(row applicationRow, manifest applicationComparisonManifest) ([]float64, qualityMetrics, error) {
	chunks := make(map[string]applicationComparisonChunk, len(manifest.Chunks))
	for _, chunk := range manifest.Chunks {
		chunks[chunk.ID] = chunk
	}
	var filter applicationComparisonFilter
	filterFound := false
	for _, candidate := range manifest.Filters {
		if candidate.ID == row.Cell.Filter {
			filter, filterFound = candidate, true
			break
		}
	}
	if !filterFound {
		return nil, qualityMetrics{}, fmt.Errorf("unknown filter %q", row.Cell.Filter)
	}

	latencies := make([]float64, 0, len(row.Samples))
	queryCoverage := make(map[string]bool, len(manifest.Queries))
	for sampleIndex, sample := range row.Samples {
		wantRepetition := sampleIndex / manifest.Config.SamplesPerCell
		wantOrdinal := sampleIndex % manifest.Config.SamplesPerCell
		queryIndex := wantOrdinal % len(manifest.Queries)
		if wantRepetition%2 == 1 {
			queryIndex = len(manifest.Queries) - 1 - queryIndex
		}
		if sample.Repetition != wantRepetition || sample.Ordinal != wantOrdinal ||
			sample.QueryID != manifest.Queries[queryIndex].ID || sample.Millis <= 0 ||
			sample.Error != "" || len(sample.ResultIDs) == 0 || len(sample.ResultIDs) > manifest.Config.TopK ||
			sample.DocumentsFetched != float64(len(sample.ResultIDs)) ||
			len(sample.ResultSources) != len(sample.ResultIDs) {
			return nil, qualityMetrics{}, fmt.Errorf("invalid sample order/query/timing/ranking at index %d", sampleIndex)
		}
		rankingIDs := map[string]bool{}
		for _, resultID := range sample.ResultIDs {
			chunk, ok := chunks[resultID]
			source, sourceOK := sample.ResultSources[resultID]
			wrongAttribution := (row.Cell.Route == "text_only" && source != [2]bool{true, false}) ||
				(row.Cell.Route == "vector_only" && source != [2]bool{false, true})
			if !ok || rankingIDs[resultID] || !sourceOK || (!source[0] && !source[1]) || wrongAttribution ||
				(filter.Tenant != "" && chunk.Tenant != filter.Tenant) ||
				(filter.Workspace != "" && chunk.Workspace != filter.Workspace) ||
				(filter.UpdatedYearGTE != 0 && chunk.UpdatedYear < filter.UpdatedYearGTE) {
				return nil, qualityMetrics{}, fmt.Errorf("invalid or duplicate result %q at sample %d", resultID, sampleIndex)
			}
			rankingIDs[resultID] = true
		}
		latencies = append(latencies, sample.Millis)
		queryCoverage[sample.QueryID] = true
	}
	if len(queryCoverage) != len(manifest.Queries) {
		return nil, qualityMetrics{}, fmt.Errorf("query coverage mismatch")
	}

	var quality qualityMetrics
	for _, sample := range row.Samples {
		var query applicationComparisonQuery
		for _, candidate := range manifest.Queries {
			if candidate.ID == sample.QueryID {
				query = candidate
				break
			}
		}
		var judgment applicationComparisonCase
		judgmentFound := false
		for _, candidate := range query.Cases {
			if candidate.Filter == row.Cell.Filter {
				judgment, judgmentFound = candidate, true
				break
			}
		}
		if !judgmentFound {
			return nil, qualityMetrics{}, fmt.Errorf("query %s lacks filter judgment %s", query.ID, row.Cell.Filter)
		}
		ranked := append([]string(nil), sample.ResultIDs...)
		parents := make([]string, 0, len(ranked))
		perParent := map[string]int{}
		for _, id := range ranked {
			parent := chunks[id].ParentID
			parents = append(parents, parent)
			perParent[parent]++
			quality.MaxPerParentResults = max(quality.MaxPerParentResults, perParent[parent])
			source := sample.ResultSources[id]
			if source[0] {
				quality.TextAttributedResults++
			}
			if source[1] {
				quality.VectorAttributedResults++
			}
			if source[0] && source[1] {
				quality.TextVectorOverlapResults++
			}
		}
		for len(ranked) < manifest.Config.TopK {
			ranked = append(ranked, "")
		}
		chunkRelevant := stringSet(judgment.RelevantChunks)
		parentRelevant := stringSet(judgment.RelevantParents)
		p5, _ := precisionAtK(ranked, chunkRelevant, 5)
		p10, _ := precisionAtK(ranked, chunkRelevant, 10)
		r5, _ := recallAtK(ranked, chunkRelevant, 5)
		r10, _ := recallAtK(ranked, chunkRelevant, 10)
		mrr, _ := mrrAtK(ranked, chunkRelevant, 10)
		quality.PrecisionAt5 += p5
		quality.PrecisionAt10 += p10
		quality.NDCGAt5 += ndcgAtK(ranked, chunkRelevant, 5)
		quality.NDCGAt10 += ndcgAtK(ranked, chunkRelevant, 10)
		quality.MRRAt10 += mrr
		if mrr > 0 {
			quality.HitRateAt10++
		}
		quality.ChunkRecallAt5 += r5
		quality.ChunkRecallAt10 += r10
		quality.ParentRecallAt5 += parentRecallAtK(parents, parentRelevant, 5)
		quality.ParentRecallAt10 += parentRecallAtK(parents, parentRelevant, 10)
		quality.RelevantChunksMean += float64(len(chunkRelevant))
		quality.RelevantParentsMean += float64(len(parentRelevant))
		quality.MaxAchievableChunkRecallAt5 += maxAchievableRecall(len(chunkRelevant), 5)
		quality.MaxAchievableChunkRecallAt10 += maxAchievableRecall(len(chunkRelevant), 10)
		quality.MaxAchievableParentRecallAt5 += maxAchievableRecall(len(parentRelevant), 5)
		quality.MaxAchievableParentRecallAt10 += maxAchievableRecall(len(parentRelevant), 10)
	}
	sampleCount := float64(len(row.Samples))
	quality.PrecisionAt5 /= sampleCount
	quality.PrecisionAt10 /= sampleCount
	quality.NDCGAt5 /= sampleCount
	quality.NDCGAt10 /= sampleCount
	quality.MRRAt10 /= sampleCount
	quality.HitRateAt10 /= sampleCount
	quality.ChunkRecallAt5 /= sampleCount
	quality.ChunkRecallAt10 /= sampleCount
	quality.ParentRecallAt5 /= sampleCount
	quality.ParentRecallAt10 /= sampleCount
	quality.RelevantChunksMean /= sampleCount
	quality.RelevantParentsMean /= sampleCount
	quality.MaxAchievableChunkRecallAt5 /= sampleCount
	quality.MaxAchievableChunkRecallAt10 /= sampleCount
	quality.MaxAchievableParentRecallAt5 /= sampleCount
	quality.MaxAchievableParentRecallAt10 /= sampleCount
	quality.AttributionMode = "untimed_projection_query_sources"
	return latencies, quality, nil
}

func validateQdrantComparisonArtifact(artifact *qdrantComparisonArtifact, manifest applicationComparisonManifest, manifestSHA, harnessRevision string) error {
	if artifact.Schema != qdrantComparisonArtifactSchema || artifact.Backend != "qdrant_server" ||
		artifact.HarnessRevision != harnessRevision || !isFullRevision(artifact.HarnessRevision) ||
		artifact.ClientVersion != "1.19.0" || artifact.ClientLockSHA256 != qdrantClientLockSHA256 ||
		!strings.HasPrefix(artifact.PythonVersion, "3.13.") || artifact.PythonImplementation != "CPython" ||
		!strings.Contains(artifact.PythonPlatform, "macOS") || !strings.Contains(artifact.PythonPlatform, "arm64") ||
		!isSHA256(artifact.PythonExecutableSHA256) || artifact.ManifestSHA256 != manifestSHA ||
		artifact.FixtureSHA256 != manifest.FixtureSHA256 || artifact.SemanticVectorSHA256 != manifest.SemanticVectorSHA256 ||
		artifact.ConfigSHA256 != manifest.ConfigSHA256 || artifact.SourceCount != len(manifest.Sources) ||
		artifact.ChunkCount != len(manifest.Chunks) || artifact.QueryCount != len(manifest.Queries) {
		return fmt.Errorf("Qdrant artifact runtime/manifest/hash/cardinality binding mismatch")
	}
	expectedSparseVectorSHA256, err := expectedQdrantSparseVectorSHA256(manifest)
	if err != nil || artifact.SparseVectorSHA256 != expectedSparseVectorSHA256 {
		return fmt.Errorf("Qdrant artifact sparse vector digest mismatch")
	}
	exact, exactOK := artifact.Server.Config["exact"].(bool)
	fullScanThresholdKB, fullScanOK := artifact.Server.Config["full_scan_threshold"].(float64)
	hnswM, hnswMOK := artifact.Server.Config["hnsw_m"].(float64)
	indexingThreshold, indexingThresholdOK := artifact.Server.Config["indexing_threshold"].(float64)
	denseSize, denseSizeOK := artifact.Server.Config["dense_size"].(float64)
	sparseOnDisk, sparseOnDiskOK := artifact.Server.Config["sparse_on_disk"].(bool)
	hnswEFConstruct, hnswEFConstructOK := artifact.Server.Config["hnsw_ef_construct"].(float64)
	maxOptimizationThreads, maxOptimizationThreadsOK := artifact.Server.Config["max_optimization_threads"].(float64)
	queryHNSWEF, queryHNSWEFOK := artifact.Server.Config["query_hnsw_ef"].(float64)
	if artifact.Server.Version != manifest.Config.QdrantServerVersion || artifact.Server.LocalMode ||
		artifact.Server.Deployment != "standalone" ||
		artifact.Server.ReleaseAssetSHA256 != manifest.Config.QdrantReleaseAssetSHA256 ||
		artifact.Server.BinarySHA256 != manifest.Config.QdrantBinarySHA256 ||
		artifact.Server.Config["dense"] != manifest.Config.DenseVectorName ||
		artifact.Server.Config["sparse"] != manifest.Config.SparseVectorName ||
		artifact.Server.Config["dense_distance"] != "cosine" ||
		artifact.Server.Config["full_scan_threshold_unit"] != "KiB" ||
		!denseSizeOK || denseSize != 384 || !sparseOnDiskOK || sparseOnDisk ||
		!exactOK || exact || !fullScanOK || fullScanThresholdKB != 10 ||
		!hnswMOK || hnswM != 16 || !hnswEFConstructOK || hnswEFConstruct != 100 ||
		!indexingThresholdOK || indexingThreshold != 1 ||
		!maxOptimizationThreadsOK || maxOptimizationThreads != 1 ||
		!queryHNSWEFOK || queryHNSWEF != float64(manifest.Config.QdrantHNSWEF) ||
		artifact.Server.IndexProof.IndexedVectorsCount < 2*len(manifest.Chunks) {
		return fmt.Errorf("Qdrant artifact lacks pinned standalone server/index configuration")
	}
	identityPIDs, err := qdrantServerIdentityPIDs(artifact.Server.Identity)
	if err != nil {
		return fmt.Errorf("Qdrant artifact lacks pinned standalone process identity: %w", err)
	}
	if len(artifact.Server.IndexProof.FilterCardinalities) != len(manifest.Filters) {
		return fmt.Errorf("Qdrant artifact lacks exact filter-cardinality proof")
	}
	for _, filter := range manifest.Filters {
		cardinality := 0
		for _, chunk := range manifest.Chunks {
			if (filter.Tenant == "" || chunk.Tenant == filter.Tenant) &&
				(filter.Workspace == "" || chunk.Workspace == filter.Workspace) &&
				(filter.UpdatedYearGTE == 0 || chunk.UpdatedYear >= filter.UpdatedYearGTE) {
				cardinality++
			}
		}
		if artifact.Server.IndexProof.FilterCardinalities[filter.ID] != cardinality ||
			cardinality*len(manifest.Chunks[0].DenseVector)*4 <= int(fullScanThresholdKB)*1024 {
			return fmt.Errorf("Qdrant filter %s cardinality/footprint proof mismatch", filter.ID)
		}
	}
	if len(artifact.Failures) != 0 || artifact.Build.Points != len(manifest.Chunks) ||
		artifact.Build.Seconds <= 0 || artifact.Build.Seconds > float64(manifest.Config.PhaseTimeoutSeconds) ||
		artifact.QuerySeconds <= 0 || artifact.QuerySeconds > float64(manifest.Config.PhaseTimeoutSeconds) ||
		!artifact.Reopen.Attempted || !artifact.Reopen.Succeeded || !artifact.Reopen.OptimizerUpdateTriggered ||
		artifact.Reopen.Version != manifest.Config.QdrantServerVersion || artifact.Reopen.Status != "green" ||
		artifact.Reopen.PointCount != len(manifest.Chunks) ||
		artifact.Reopen.IndexedVectorsCount != artifact.Server.IndexProof.IndexedVectorsCount ||
		artifact.Reopen.IndexedVectorsCount < 2*len(manifest.Chunks) ||
		!equalStrings(artifact.Reopen.PayloadIndexes, []string{"tenant", "updated_year", "workspace"}) ||
		artifact.Reopen.Seconds <= 0 || artifact.Reopen.Seconds > float64(manifest.Config.PhaseTimeoutSeconds) {
		return fmt.Errorf("Qdrant artifact failed or lacks successful bounded build/query/durable reopen")
	}
	if artifact.Resources.DurableBytes <= 0 ||
		artifact.Resources.DurableBytesSemantics != "live_before_server_shutdown" ||
		artifact.Resources.HostPIDMetrics != "observed_process_samples_across_pre_and_post_restart_segments" ||
		len(artifact.Resources.ProcessSamples) < 4 || artifact.Resources.PeakObservedRSSBytes <= 0 ||
		artifact.Resources.CPUSeconds <= 0 {
		return fmt.Errorf("Qdrant artifact lacks authoritative standalone process/storage evidence")
	}
	processCounts := map[int]int{}
	processCPUDeltas := map[int]float64{}
	retiredProcesses := map[int]bool{}
	var lastPID int
	var lastCPU float64
	var lastCapture int64
	var peakRSS int64
	processPIDOrder := make([]int, 0, 2)
	cpuTotal := 0.0
	for sampleIndex, sample := range artifact.Resources.ProcessSamples {
		if sample.PID <= 0 || sample.RSSBytes <= 0 || sample.CPUSeconds < 0 ||
			sample.CapturedUnixNanos <= lastCapture {
			return fmt.Errorf("Qdrant artifact contains invalid or unordered process sample %d", sampleIndex)
		}
		if sample.PID == lastPID {
			if sample.CPUSeconds < lastCPU {
				return fmt.Errorf("Qdrant artifact process %d CPU regressed", sample.PID)
			}
			delta := sample.CPUSeconds - lastCPU
			processCPUDeltas[sample.PID] += delta
			cpuTotal += delta
		} else {
			if retiredProcesses[sample.PID] {
				return fmt.Errorf("Qdrant artifact process %d lifecycle is non-contiguous", sample.PID)
			}
			if lastPID != 0 {
				retiredProcesses[lastPID] = true
				cpuTotal += sample.CPUSeconds
				processCPUDeltas[sample.PID] += sample.CPUSeconds
			}
			lastPID = sample.PID
			processPIDOrder = append(processPIDOrder, sample.PID)
		}
		processCounts[sample.PID]++
		lastCPU = sample.CPUSeconds
		lastCapture = sample.CapturedUnixNanos
		peakRSS = max(peakRSS, sample.RSSBytes)
	}
	if len(processCounts) != 2 || len(processPIDOrder) != 2 ||
		processPIDOrder[0] != identityPIDs[0] || processPIDOrder[1] != identityPIDs[1] ||
		peakRSS != artifact.Resources.PeakObservedRSSBytes ||
		!comparisonFloatMatches(artifact.Resources.CPUSeconds, cpuTotal) {
		return fmt.Errorf("Qdrant artifact process aggregate/restart evidence mismatch")
	}
	for pid, count := range processCounts {
		if count < 2 || processCPUDeltas[pid] <= 0 {
			return fmt.Errorf("Qdrant artifact process %d lacks positive monotonic CPU samples", pid)
		}
	}
	seen := map[string]bool{}
	for _, cell := range artifact.Cells {
		key := cell.Route + "\x00" + cell.Filter
		if seen[key] {
			return fmt.Errorf("Qdrant artifact duplicate cell %s/%s", cell.Route, cell.Filter)
		}
		seen[key] = true
		if !containsString([]string{"lexical", "dense", "hybrid"}, cell.Route) ||
			!containsString(applicationFilterOrder, cell.Filter) ||
			cell.TimingSemantics != "total_ms spans query_points, point-ID extraction, bounded retrieve, and payload ordering/validation; benchmark quality/byte bookkeeping is excluded; search_ms and fetch_ms are nested subtimers" ||
			cell.Warmups != manifest.Config.WarmupsPerCell || cell.Repetitions != manifest.Config.Repetitions ||
			len(cell.Samples) != manifest.Config.SamplesPerCell*manifest.Config.Repetitions ||
			len(cell.RepetitionPerformance) != manifest.Config.Repetitions ||
			cell.Errors != 0 || cell.Timeouts != 0 || cell.Leakage != 0 ||
			cell.FetchMaxCount > manifest.Config.TopK || cell.RouteProof.Fallbacks != 0 ||
			cell.RouteProof.ExhaustiveSearch || !cell.RouteProof.BoundedFetch ||
			cell.RouteProof.API != "qdrant.query_points" {
			return fmt.Errorf("Qdrant artifact cell %s/%s is partial, leaking, unbounded, failed, exhaustive, or fell back", cell.Route, cell.Filter)
		}
		latencies, quality, err := recomputeQdrantCellEvidence(cell, manifest)
		if err != nil {
			return fmt.Errorf("Qdrant artifact cell %s/%s sample evidence: %w", cell.Route, cell.Filter, err)
		}
		p50, _ := percentile(latencies, 50)
		p95, _ := percentile(latencies, 95)
		p99, _ := percentile(latencies, 99)
		if !comparisonFloatMatches(cell.Summary.LatencyMSP50, p50) ||
			!comparisonFloatMatches(cell.Summary.LatencyMSP95, p95) ||
			!comparisonFloatMatches(cell.Summary.LatencyMSP99, p99) {
			return fmt.Errorf("Qdrant artifact cell %s/%s latency summary does not match raw samples", cell.Route, cell.Filter)
		}
		if !qdrantQualityMatches(cell.Quality, quality) {
			return fmt.Errorf("Qdrant artifact cell %s/%s quality does not match raw rankings", cell.Route, cell.Filter)
		}
		qpsMean := 0.0
		for rep, performance := range cell.RepetitionPerformance {
			retainedWall := 0.0
			for _, sample := range cell.Samples {
				if sample.Repetition == rep {
					retainedWall += sample.TotalMS / 1000
				}
			}
			expectedQPS := float64(manifest.Config.SamplesPerCell) / performance.WallSeconds
			expectedOrder := "forward"
			if rep%2 == 1 {
				expectedOrder = "reverse"
			}
			if performance.Repetition != rep || performance.Order != expectedOrder ||
				performance.Samples != manifest.Config.SamplesPerCell ||
				performance.WallSeconds <= 0 || performance.WallSeconds+1e-9 < retainedWall ||
				!comparisonFloatMatches(performance.QPS, expectedQPS) {
				return fmt.Errorf("Qdrant artifact cell %s/%s has invalid repetition wall evidence", cell.Route, cell.Filter)
			}
			qpsMean += performance.QPS
		}
		qpsMean /= float64(manifest.Config.Repetitions)
		if !comparisonFloatMatches(cell.Summary.QPS, qpsMean) {
			return fmt.Errorf("Qdrant artifact cell %s/%s QPS is not mean-per-repetition", cell.Route, cell.Filter)
		}
		wantVectors := []string{manifest.Config.SparseVectorName}
		if cell.Route == "dense" {
			wantVectors = []string{manifest.Config.DenseVectorName}
		} else if cell.Route == "hybrid" {
			wantVectors = []string{manifest.Config.SparseVectorName, manifest.Config.DenseVectorName}
		}
		if !equalStrings(cell.RouteProof.NamedVectors, wantVectors) {
			return fmt.Errorf("Qdrant %s cell %s named vectors=%q want %q", cell.Route, cell.Filter, cell.RouteProof.NamedVectors, wantVectors)
		}
		if cell.Route == "hybrid" && cell.RouteProof.Fusion != "rrf" {
			return fmt.Errorf("Qdrant hybrid cell %s lacks Query API RRF proof", cell.Filter)
		}
		if cell.Equivalence != "directional" {
			return fmt.Errorf("Qdrant %s cell must be labeled directional", cell.Route)
		}
	}
	if len(seen) != 12 {
		return fmt.Errorf("Qdrant artifact comparison cells=%d want 12", len(seen))
	}
	return nil
}

func validateComparisonPaths(paths ...string) error {
	seen := make(map[string]string, len(paths))
	for _, path := range paths {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		canonical, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			parent, parentErr := filepath.EvalSymlinks(filepath.Dir(absolute))
			if parentErr != nil {
				return fmt.Errorf("resolve comparison path %s: %w", path, err)
			}
			canonical = filepath.Join(parent, filepath.Base(absolute))
		}
		key := canonical
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if prior, exists := seen[key]; exists {
			return fmt.Errorf("comparison paths alias: %s and %s", prior, path)
		}
		seen[key] = path
	}
	return nil
}
func compareApplicationEvidence(manifestPath, treePath, qdrantPath, treeStoragePath, qdrantStoragePath, outputPath, markdownPath string) error {

	var manifest applicationComparisonManifest
	if err := validateComparisonPaths(manifestPath, treePath, qdrantPath, treeStoragePath, qdrantStoragePath, outputPath, markdownPath); err != nil {
		return err
	}
	rawManifest, err := readJSONFile(manifestPath, &manifest)
	if err != nil {
		return err
	}
	manifestSHA, err := validateComparisonManifest(rawManifest, &manifest)
	if err != nil {
		return err
	}
	var tree treeDBComparisonArtifact
	if _, err := readJSONFile(treePath, &tree); err != nil {
		return err
	}
	provenanceConfig := defaultApplicationConfig()
	provenanceConfig.FinalEvidence = true
	provenanceConfig.HarnessRevision = tree.HarnessRevision
	settings, buildInfoOK := runtimeBuildInfo()
	if _, err := resolveApplicationHarnessRevision(provenanceConfig, settings, buildInfoOK); err != nil {
		return fmt.Errorf("comparison executable provenance: %w", err)
	}
	treeRows, err := validateTreeDBComparisonArtifact(&tree, manifest, manifestSHA)
	if err != nil {
		return err
	}
	var qdrant qdrantComparisonArtifact
	if _, err := readJSONFile(qdrantPath, &qdrant); err != nil {
		return err
	}
	if err := validateQdrantComparisonArtifact(&qdrant, manifest, manifestSHA, tree.HarnessRevision); err != nil {
		return err
	}
	treeStorageBytes, err := dirSize(treeStoragePath)
	if err != nil {
		return fmt.Errorf("measure TreeDB storage: %w", err)
	}
	qdrantStorageBytes, err := dirSize(qdrantStoragePath)
	if err != nil {
		return fmt.Errorf("measure Qdrant storage: %w", err)
	}
	if treeStorageBytes != tree.StorageBytes {
		return fmt.Errorf("TreeDB storage total does not match live storage root")
	}
	qdrant.Resources.DurableBytes = qdrantStorageBytes
	qdrant.Resources.DurableBytesSemantics = "quiesced_after_server_shutdown"
	report := applicationComparisonReport{Schema: applicationComparisonSchema, State: "validated",
		HarnessRevision: tree.HarnessRevision, TreeDBBinarySHA256: tree.BinarySHA256,
		TreeDBProcessResources: tree.ProcessResources, TreeDBStorageBytes: tree.StorageBytes, QdrantResources: qdrant.Resources,
		QdrantClientVersion: qdrant.ClientVersion, QdrantServerVersion: qdrant.Server.Version,
		QdrantClientLockSHA256: qdrant.ClientLockSHA256,
		QdrantPythonVersion:    qdrant.PythonVersion, QdrantPythonPlatform: qdrant.PythonPlatform,
		QdrantPythonImplementation:   qdrant.PythonImplementation,
		QdrantPythonExecutableSHA256: qdrant.PythonExecutableSHA256,
		QdrantServerBinarySHA256:     qdrant.Server.BinarySHA256,
		QdrantReleaseAssetSHA256:     qdrant.Server.ReleaseAssetSHA256,
		ManifestSHA256:               manifestSHA, FixtureSHA256: manifest.FixtureSHA256,
		SemanticVectorSHA256: manifest.SemanticVectorSHA256, ConfigSHA256: manifest.ConfigSHA256,
		Rows: treeRows, Dispositions: []string{
			"All TreeDB-versus-Qdrant latency rows are directional: lexical scoring differs; TreeDB dense/hybrid uses declared_column_graph_exact, while Qdrant HNSW is indexed and exact=false is requested but server planner selection is opaque.",
			"Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.",
			"The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.",
			"CPU and RSS figures are scoped diagnostics, not cross-backend comparisons: TreeDB includes the in-process Go driver while Qdrant covers only the standalone server PID.",
		}}
	for _, cell := range qdrant.Cells {
		report.Rows = append(report.Rows, applicationComparisonRow{Backend: "qdrant", Route: cell.Route, Filter: cell.Filter,
			Equivalence: cell.Equivalence, Samples: len(cell.Samples), Repetitions: cell.Repetitions, QPS: cell.Summary.QPS,
			LatencyMSP50: cell.Summary.LatencyMSP50, LatencyMSP95: cell.Summary.LatencyMSP95,
			LatencyMSP99: cell.Summary.LatencyMSP99, Quality: cell.Quality})
	}
	sort.Slice(report.Rows, func(i, j int) bool {
		if report.Rows[i].Backend != report.Rows[j].Backend {
			return report.Rows[i].Backend < report.Rows[j].Backend
		}
		if report.Rows[i].Route != report.Rows[j].Route {
			return report.Rows[i].Route < report.Rows[j].Route
		}
		return report.Rows[i].Filter < report.Rows[j].Filter
	})
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(outputPath, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	var md strings.Builder
	fmt.Fprintf(&md, "# TreeDB / Qdrant bounded RAG comparison\n\nState: **validated**  \nManifest SHA-256: `%s`  \nHarness revision: `%s`  \nTreeDB binary SHA-256: `%s`  \nTreeDB process CPU / peak RSS: `%.6fs` / `%d bytes`  \nTreeDB resource semantics: `%s`; `%s`; `%s`  \nQdrant client/server: `%s` / `%s`  \nQdrant client lock SHA-256: `%s`  \nQdrant Python: `%s` / `%s` / `%s`  \nQdrant Python executable SHA-256: `%s`  \nQdrant server binary SHA-256: `%s`  \nQdrant release asset SHA-256: `%s`  \nQdrant process CPU / observed peak RSS / durable bytes: `%.6fs` / `%d bytes` / `%d bytes`  \nQdrant durable-byte semantics: `%s`\n\n", manifestSHA, report.HarnessRevision, report.TreeDBBinarySHA256, report.TreeDBProcessResources.CPUSeconds, report.TreeDBProcessResources.PeakRSSBytes, report.TreeDBProcessResources.CPUSemantics, report.TreeDBProcessResources.RSSSemantics, report.TreeDBProcessResources.Scope, report.QdrantClientVersion, report.QdrantServerVersion, report.QdrantClientLockSHA256, report.QdrantPythonVersion, report.QdrantPythonImplementation, report.QdrantPythonPlatform, report.QdrantPythonExecutableSHA256, report.QdrantServerBinarySHA256, report.QdrantReleaseAssetSHA256, report.QdrantResources.CPUSeconds, report.QdrantResources.PeakObservedRSSBytes, report.QdrantResources.DurableBytes, report.QdrantResources.DurableBytesSemantics)
	fmt.Fprintf(&md, "TreeDB durable bytes: `%d bytes`\n\n", report.TreeDBStorageBytes)
	md.WriteString("| Backend | Route | Filter | Semantics | Samples | Reps | QPS | p50 ms | p95 ms | p99 ms | P@10 | nDCG@10 | MRR@10 | Parent R@10 |\n|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, row := range report.Rows {
		fmt.Fprintf(&md, "| %s | %s | %s | %s | %d | %d | %.2f | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f |\n",
			row.Backend, row.Route, row.Filter, row.Equivalence, row.Samples, row.Repetitions, row.QPS,
			row.LatencyMSP50, row.LatencyMSP95, row.LatencyMSP99, row.Quality.PrecisionAt10,
			row.Quality.NDCGAt10, row.Quality.MRRAt10, row.Quality.ParentRecallAt10)
	}
	md.WriteString("\n## Dispositions\n\n")
	for _, disposition := range report.Dispositions {
		fmt.Fprintf(&md, "- %s\n", disposition)
	}
	return os.WriteFile(markdownPath, []byte(md.String()), 0o644)
}
