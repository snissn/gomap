package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

const (
	qdrantComparisonArtifactSchema = "treedb-rag-qdrant-comparison/v1"
	applicationComparisonSchema    = "treedb-rag-system-comparison/v1"
)

type qdrantComparisonIndexProof struct {
	IndexedVectorsCount int            `json:"indexed_vectors_count"`
	FilterCardinalities map[string]int `json:"filter_cardinalities"`
}

type qdrantComparisonServer struct {
	Version      string                     `json:"version"`
	Deployment   string                     `json:"deployment"`
	BinarySHA256 string                     `json:"binary_sha256"`
	Identity     string                     `json:"identity"`
	LocalMode    bool                       `json:"local_mode"`
	Config       map[string]any             `json:"config"`
	IndexProof   qdrantComparisonIndexProof `json:"index_proof"`
}

type qdrantComparisonProcessSample struct {
	PID               int     `json:"pid"`
	RSSBytes          int64   `json:"rss_bytes"`
	CPUSeconds        float64 `json:"cpu_seconds"`
	CapturedUnixNanos int64   `json:"captured_unix_nanos"`
}

type qdrantComparisonResources struct {
	HostPIDMetrics       string                          `json:"host_pid_metrics"`
	ProcessSamples       []qdrantComparisonProcessSample `json:"process_samples"`
	PeakObservedRSSBytes int64                           `json:"peak_observed_rss_bytes"`
	CPUSeconds           float64                         `json:"cpu_seconds"`
	DurableBytes         int64                           `json:"durable_bytes"`
}

type qdrantComparisonBuild struct {
	Seconds float64 `json:"seconds"`
	Points  int     `json:"points"`
}

type qdrantComparisonReopen struct {
	Attempted  bool    `json:"attempted"`
	Succeeded  bool    `json:"succeeded"`
	Version    string  `json:"version"`
	PointCount int     `json:"point_count"`
	Seconds    float64 `json:"seconds"`
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
	Schema               string                    `json:"schema"`
	Backend              string                    `json:"backend"`
	HarnessRevision      string                    `json:"harness_revision"`
	ClientVersion        string                    `json:"client_version"`
	ManifestSHA256       string                    `json:"manifest_sha256"`
	FixtureSHA256        string                    `json:"fixture_sha256"`
	SemanticVectorSHA256 string                    `json:"semantic_vector_sha256"`
	ConfigSHA256         string                    `json:"config_sha256"`
	SourceCount          int                       `json:"source_count"`
	ChunkCount           int                       `json:"chunk_count"`
	QueryCount           int                       `json:"query_count"`
	Server               qdrantComparisonServer    `json:"server"`
	Resources            qdrantComparisonResources `json:"resources"`
	Build                qdrantComparisonBuild     `json:"build"`
	QuerySeconds         float64                   `json:"query_seconds"`
	Reopen               qdrantComparisonReopen    `json:"reopen"`
	Cells                []qdrantComparisonCell    `json:"cells"`
	Failures             []string                  `json:"failures"`
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
	Schema                   string                     `json:"schema"`
	State                    string                     `json:"state"`
	HarnessRevision          string                     `json:"harness_revision"`
	TreeDBBinarySHA256       string                     `json:"treedb_binary_sha256"`
	TreeDBProcessResources   comparisonProcessResources `json:"treedb_process_resources"`
	QdrantResources          qdrantComparisonResources  `json:"qdrant_resources"`
	QdrantClientVersion      string                     `json:"qdrant_client_version"`
	QdrantServerVersion      string                     `json:"qdrant_server_version"`
	QdrantServerBinarySHA256 string                     `json:"qdrant_server_binary_sha256"`
	QdrantReleaseAssetSHA256 string                     `json:"qdrant_release_asset_sha256"`
	ManifestSHA256           string                     `json:"manifest_sha256"`
	FixtureSHA256            string                     `json:"fixture_sha256"`
	SemanticVectorSHA256     string                     `json:"semantic_vector_sha256"`
	ConfigSHA256             string                     `json:"config_sha256"`
	Rows                     []applicationComparisonRow `json:"rows"`
	Dispositions             []string                   `json:"dispositions"`
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

func validateTreeDBComparisonArtifact(artifact *treeDBComparisonArtifact, manifest applicationComparisonManifest, manifestSHA string) ([]applicationComparisonRow, error) {
	if artifact.Schema != treeDBComparisonArtifactSchema || artifact.Authority != "BOUNDED_COMPARISON_EVIDENCE" ||
		artifact.ManifestSHA256 != manifestSHA || artifact.ProductBaseSHA != manifest.ProductBaseSHA ||
		artifact.FixtureSHA256 != manifest.FixtureSHA256 || artifact.SemanticVectorSHA256 != manifest.SemanticVectorSHA256 ||
		artifact.ConfigSHA256 != manifest.ConfigSHA256 || applicationComparisonConfigDigest(artifact.Config) != manifest.ConfigSHA256 ||
		artifact.SourceCount != len(manifest.Sources) || artifact.ChunkCount != len(manifest.Chunks) || artifact.QueryCount != len(manifest.Queries) {
		return nil, fmt.Errorf("TreeDB artifact authority/manifest/hash/config/cardinality binding mismatch")
	}
	if !isFullRevision(artifact.HarnessRevision) || !validSHA256(artifact.BinarySHA256) {
		return nil, fmt.Errorf("TreeDB artifact lacks full clean harness/binary identity")
	}
	if len(artifact.Failures) != 0 || artifact.StorageBytes <= 0 || artifact.BuildReopenSeconds <= 0 ||
		artifact.BuildReopenSeconds > float64(manifest.Config.PhaseTimeoutSeconds) || artifact.QuerySeconds <= 0 ||
		artifact.QuerySeconds > float64(manifest.Config.PhaseTimeoutSeconds) {
		return nil, fmt.Errorf("TreeDB artifact failed or lacks bounded build/query/storage evidence")
	}
	if !artifact.ProcessResources.Available || artifact.ProcessResources.CPUSeconds <= 0 ||
		artifact.ProcessResources.PeakRSSBytes <= 0 ||
		artifact.ProcessResources.CPUSemantics != "getrusage(RUSAGE_SELF) user+system CPU delta" ||
		artifact.ProcessResources.RSSSemantics != "getrusage(RUSAGE_SELF) process high-water RSS; Darwin bytes, Linux KiB normalized to bytes" ||
		artifact.ProcessResources.Scope != "fresh comparison process; build, lifecycle reopen, and all 12 query cells" {
		return nil, fmt.Errorf("TreeDB artifact lacks process CPU/RSS evidence or exact semantics")
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
		key := route + "\x00" + row.Cell.Filter
		if seen[key] {
			return nil, fmt.Errorf("TreeDB artifact has duplicate comparison cell %s/%s", route, row.Cell.Filter)
		}
		seen[key] = true
		if row.Status != "supported" || row.Errors != 0 || row.Counters["full_document_scan_fallbacks"] != 0 ||
			row.Counters["cross_tenant_results"] != 0 || row.Counters["cross_workspace_results"] != 0 ||
			row.Counters["documents_fetched"] > float64(manifest.Config.TopK) ||
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

func qdrantNearestRank(values []float64, quantile float64) float64 {
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	index := max(0, int(math.Ceil(quantile*float64(len(ordered))))-1)
	return ordered[index]
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
	lastRankings := make(map[string][]string, len(manifest.Queries))
	fetchMax := 0
	for sampleIndex, sample := range cell.Samples {
		wantRepetition := sampleIndex / manifest.Config.SamplesPerCell
		wantOrdinal := sampleIndex % manifest.Config.SamplesPerCell
		wantQuery := manifest.Queries[wantOrdinal%len(manifest.Queries)].ID
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
		lastRankings[sample.QueryID] = append([]string(nil), sample.ResultIDs...)
	}
	if cell.FetchMaxCount != fetchMax || len(lastRankings) != len(manifest.Queries) {
		return nil, qualityMetrics{}, fmt.Errorf("fetch/query coverage mismatch")
	}

	var quality qualityMetrics
	for _, query := range manifest.Queries {
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
		ranked := append([]string(nil), lastRankings[query.ID]...)
		parents := make([]string, 0, len(ranked))
		for _, id := range ranked {
			parents = append(parents, chunks[id].ParentID)
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
	}
	queryCount := float64(len(manifest.Queries))
	quality.PrecisionAt5 /= queryCount
	quality.PrecisionAt10 /= queryCount
	quality.NDCGAt5 /= queryCount
	quality.NDCGAt10 /= queryCount
	quality.MRRAt10 /= queryCount
	quality.HitRateAt10 /= queryCount
	quality.ChunkRecallAt5 /= queryCount
	quality.ChunkRecallAt10 /= queryCount
	quality.ParentRecallAt5 /= queryCount
	quality.ParentRecallAt10 /= queryCount
	quality.AttributionMode = "qdrant_native_route"
	return latencies, quality, nil
}

func validateQdrantComparisonArtifact(artifact *qdrantComparisonArtifact, manifest applicationComparisonManifest, manifestSHA, harnessRevision string) error {
	if artifact.Schema != qdrantComparisonArtifactSchema || artifact.Backend != "qdrant_server" ||
		artifact.HarnessRevision != harnessRevision || !isFullRevision(artifact.HarnessRevision) ||
		artifact.ClientVersion != "1.19.0" || artifact.ManifestSHA256 != manifestSHA ||
		artifact.FixtureSHA256 != manifest.FixtureSHA256 || artifact.SemanticVectorSHA256 != manifest.SemanticVectorSHA256 ||
		artifact.ConfigSHA256 != manifest.ConfigSHA256 || artifact.SourceCount != len(manifest.Sources) ||
		artifact.ChunkCount != len(manifest.Chunks) || artifact.QueryCount != len(manifest.Queries) {
		return fmt.Errorf("Qdrant artifact runtime/manifest/hash/cardinality binding mismatch")
	}
	exact, exactOK := artifact.Server.Config["exact"].(bool)
	fullScanThresholdKB, fullScanOK := artifact.Server.Config["full_scan_threshold"].(float64)
	if artifact.Server.Version != manifest.Config.QdrantServerVersion || artifact.Server.LocalMode ||
		artifact.Server.Identity == "" || !strings.Contains(artifact.Server.Identity, "|reopened_pid:") ||
		artifact.Server.Deployment != "standalone" ||
		artifact.Server.BinarySHA256 != manifest.Config.QdrantBinarySHA256 ||
		artifact.Server.Config["dense"] != manifest.Config.DenseVectorName ||
		artifact.Server.Config["sparse"] != manifest.Config.SparseVectorName ||
		artifact.Server.Config["full_scan_threshold_unit"] != "KiB" ||
		!exactOK || exact || !fullScanOK || fullScanThresholdKB != 10 ||
		artifact.Server.IndexProof.IndexedVectorsCount < len(manifest.Chunks) {
		return fmt.Errorf("Qdrant artifact lacks pinned standalone server/index configuration")
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
		!artifact.Reopen.Attempted || !artifact.Reopen.Succeeded ||
		artifact.Reopen.Version != manifest.Config.QdrantServerVersion ||
		artifact.Reopen.PointCount != len(manifest.Chunks) || artifact.Reopen.Seconds <= 0 ||
		artifact.Reopen.Seconds > float64(manifest.Config.PhaseTimeoutSeconds) {
		return fmt.Errorf("Qdrant artifact failed or lacks successful bounded build/query/durable reopen")
	}
	if artifact.Resources.DurableBytes <= 0 ||
		artifact.Resources.HostPIDMetrics != "observed_process_samples_across_pre_and_post_restart_segments" ||
		len(artifact.Resources.ProcessSamples) < 4 || artifact.Resources.PeakObservedRSSBytes <= 0 ||
		artifact.Resources.CPUSeconds <= 0 {
		return fmt.Errorf("Qdrant artifact lacks authoritative standalone process/storage evidence")
	}
	processes := map[int]bool{}
	for _, sample := range artifact.Resources.ProcessSamples {
		if sample.PID <= 0 || sample.RSSBytes <= 0 || sample.CPUSeconds < 0 || sample.CapturedUnixNanos <= 0 {
			return fmt.Errorf("Qdrant artifact contains invalid process sample")
		}
		processes[sample.PID] = true
	}
	if len(processes) < 2 {
		return fmt.Errorf("Qdrant artifact lacks distinct pre/post-restart process identities")
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
			cell.TimingSemantics != "total_ms spans query_points, point-ID extraction, bounded retrieve, payload ordering/validation, leakage/accounting, and sample recording; search_ms and fetch_ms are nested subtimers" ||
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
		if !comparisonFloatMatches(cell.Summary.LatencyMSP50, qdrantNearestRank(latencies, .50)) ||
			!comparisonFloatMatches(cell.Summary.LatencyMSP95, qdrantNearestRank(latencies, .95)) ||
			!comparisonFloatMatches(cell.Summary.LatencyMSP99, qdrantNearestRank(latencies, .99)) {
			return fmt.Errorf("Qdrant artifact cell %s/%s latency summary does not match raw samples", cell.Route, cell.Filter)
		}
		if !qdrantQualityMatches(cell.Quality, quality) {
			return fmt.Errorf("Qdrant artifact cell %s/%s quality does not match raw rankings", cell.Route, cell.Filter)
		}
		qpsMean := 0.0
		for rep, performance := range cell.RepetitionPerformance {
			expectedQPS := float64(manifest.Config.SamplesPerCell) / performance.WallSeconds
			if performance.Repetition != rep || performance.Samples != manifest.Config.SamplesPerCell ||
				performance.WallSeconds <= 0 || !comparisonFloatMatches(performance.QPS, expectedQPS) {
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

func compareApplicationEvidence(manifestPath, treePath, qdrantPath, outputPath, markdownPath string) error {
	var manifest applicationComparisonManifest
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
	report := applicationComparisonReport{Schema: applicationComparisonSchema, State: "validated",
		HarnessRevision: tree.HarnessRevision, TreeDBBinarySHA256: tree.BinarySHA256,
		TreeDBProcessResources: tree.ProcessResources, QdrantResources: qdrant.Resources,
		QdrantClientVersion: qdrant.ClientVersion, QdrantServerVersion: qdrant.Server.Version,
		QdrantServerBinarySHA256: qdrant.Server.BinarySHA256,
		QdrantReleaseAssetSHA256: manifest.Config.QdrantReleaseAssetSHA256,
		ManifestSHA256:           manifestSHA, FixtureSHA256: manifest.FixtureSHA256,
		SemanticVectorSHA256: manifest.SemanticVectorSHA256, ConfigSHA256: manifest.ConfigSHA256,
		Rows: treeRows, Dispositions: []string{
			"All TreeDB-versus-Qdrant latency rows are directional: lexical scoring differs; TreeDB dense/hybrid uses declared_column_graph_exact, while Qdrant HNSW is indexed and exact=false is requested but server planner selection is opaque.",
			"Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.",
			"The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.",
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
	fmt.Fprintf(&md, "# TreeDB / Qdrant bounded RAG comparison\n\nState: **validated**  \nManifest SHA-256: `%s`  \nHarness revision: `%s`  \nTreeDB binary SHA-256: `%s`  \nTreeDB process CPU / peak RSS: `%.6fs` / `%d bytes`  \nTreeDB resource semantics: `%s`; `%s`; `%s`  \nQdrant client/server: `%s` / `%s`  \nQdrant server binary SHA-256: `%s`  \nQdrant release asset SHA-256: `%s`  \nQdrant process CPU / observed peak RSS / durable bytes: `%.6fs` / `%d bytes` / `%d bytes`\n\n", manifestSHA, report.HarnessRevision, report.TreeDBBinarySHA256, report.TreeDBProcessResources.CPUSeconds, report.TreeDBProcessResources.PeakRSSBytes, report.TreeDBProcessResources.CPUSemantics, report.TreeDBProcessResources.RSSSemantics, report.TreeDBProcessResources.Scope, report.QdrantClientVersion, report.QdrantServerVersion, report.QdrantServerBinarySHA256, report.QdrantReleaseAssetSHA256, report.QdrantResources.CPUSeconds, report.QdrantResources.PeakObservedRSSBytes, report.QdrantResources.DurableBytes)
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
