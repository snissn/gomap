package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	runtimemetrics "runtime/metrics"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/collections/chunking"
	"github.com/snissn/gomap/TreeDB/collections/embedding"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
)

const (
	applicationReportSchema     = "treedb_rag_application_baseline/v3"
	applicationCollection       = "rag_application"
	applicationSourceCollection = "rag_application_sources"
	applicationVectorIndex      = "embedding"
	applicationVectorField      = "embedding"
	applicationTextField        = "content"
	semanticProviderName        = "rag-minilm-l6-v2-1110a243"
	capabilityErrorType         = "*main.capabilityError"
)

var (
	applicationRoutes      = []string{"text_only", "vector_only", "hybrid"}
	applicationProjections = []string{"score_only", "fetch_topk"}
	applicationSurfaces    = []string{"direct_collection", "http_service"}
	applicationEmbeddings  = []string{"hashing_regression", "semantic_minilm"}
	applicationClients     = []int{1, 4}
)

type applicationConfig struct {
	TopK            int
	CandidateLimit  int
	EfSearch        int
	M               int
	WarmupQueries   int
	Repetitions     int
	SamplesPerRep   int
	IngestionReps   int
	Dir             string
	KeepDir         bool
	ProductBaseSHA  string
	HarnessRevision string
	HostNote        string
	FinalEvidence   bool
	Command         []string
}

func defaultApplicationConfig() applicationConfig {
	return applicationConfig{
		TopK: 10, CandidateLimit: 32, EfSearch: 64, M: 8,
		WarmupQueries: 24, Repetitions: 3, SamplesPerRep: 336,
		IngestionReps: 5, FinalEvidence: true,
	}
}

type capabilityError struct {
	Code           string `json:"code"`
	RequiredIssues []int  `json:"required_issues,omitempty"`
	Message        string `json:"message"`
}

func (e *capabilityError) Error() string { return e.Code + ": " + e.Message }

type capabilityEvidence struct {
	ErrorType       string `json:"error_type"`
	Code            string `json:"code"`
	Message         string `json:"message"`
	RequiredIssues  []int  `json:"required_issues,omitempty"`
	ResultsReturned int    `json:"results_returned"`
	FailClosed      bool   `json:"fail_closed"`
}

type applicationCellIdentity struct {
	Route       string `json:"route"`
	Projection  string `json:"projection"`
	Filter      string `json:"filter"`
	Collapse    string `json:"collapse"`
	Surface     string `json:"surface"`
	Embedding   string `json:"embedding"`
	VectorRoute string `json:"vector_route"`
	Clients     int    `json:"clients"`
}

type querySample struct {
	Repetition    int     `json:"repetition"`
	Ordinal       int     `json:"ordinal"`
	QueryID       string  `json:"query_id"`
	Millis        float64 `json:"millis"`
	RequestBytes  int64   `json:"request_bytes"`
	ResponseBytes int64   `json:"response_bytes"`
	Error         string  `json:"error,omitempty"`
}

type repetitionPerformance struct {
	Repetition  int     `json:"repetition"`
	Order       string  `json:"order"`
	Samples     int     `json:"samples"`
	WallSeconds float64 `json:"wall_seconds"`
	QPS         float64 `json:"qps"`
}

type qualityMetrics struct {
	PrecisionAt5                  float64 `json:"precision_at_5"`
	PrecisionAt10                 float64 `json:"precision_at_10"`
	NDCGAt5                       float64 `json:"ndcg_at_5"`
	NDCGAt10                      float64 `json:"ndcg_at_10"`
	MRRAt10                       float64 `json:"mrr_at_10"`
	HitRateAt10                   float64 `json:"hit_rate_at_10"`
	ChunkRecallAt5                float64 `json:"chunk_recall_at_5"`
	ChunkRecallAt10               float64 `json:"chunk_recall_at_10"`
	ParentRecallAt5               float64 `json:"parent_recall_at_5"`
	ParentRecallAt10              float64 `json:"parent_recall_at_10"`
	RelevantChunksMean            float64 `json:"relevant_chunks_mean"`
	RelevantParentsMean           float64 `json:"relevant_parents_mean"`
	MaxAchievableChunkRecallAt5   float64 `json:"max_achievable_chunk_recall_at_5"`
	MaxAchievableChunkRecallAt10  float64 `json:"max_achievable_chunk_recall_at_10"`
	MaxAchievableParentRecallAt5  float64 `json:"max_achievable_parent_recall_at_5"`
	MaxAchievableParentRecallAt10 float64 `json:"max_achievable_parent_recall_at_10"`
	MaxPerParentResults           int     `json:"max_per_parent_results"`
	CollapseRejections            int     `json:"collapse_rejections"`
	CollapseExhaustions           int     `json:"collapse_exhaustions"`
	TextAttributedResults         int     `json:"text_attributed_results"`
	VectorAttributedResults       int     `json:"vector_attributed_results"`
	TextVectorOverlapResults      int     `json:"text_vector_overlap_results"`
	AttributionMode               string  `json:"attribution_mode"`
}

type applicationIndexQuerySnapshot struct {
	TextChildIDs      []string
	VectorChildIDs    []string
	ScalarDocumentIDs []string
}

type applicationRow struct {
	Cell               applicationCellIdentity `json:"cell"`
	Status             string                  `json:"status"`
	Capability         *capabilityEvidence     `json:"capability,omitempty"`
	Quality            qualityMetrics          `json:"quality"`
	Samples            []querySample           `json:"samples,omitempty"`
	Repetitions        []repetitionPerformance `json:"repetitions,omitempty"`
	QPSMean            float64                 `json:"qps_mean"`
	LatencyMSMean      float64                 `json:"latency_ms_mean"`
	LatencyMSP50       float64                 `json:"latency_ms_p50"`
	LatencyMSP95       float64                 `json:"latency_ms_p95"`
	LatencyMSP99       float64                 `json:"latency_ms_p99"`
	BytesPerOp         float64                 `json:"bytes_per_op"`
	AllocsPerOp        float64                 `json:"allocs_per_op"`
	CPUSecondsPerQuery float64                 `json:"cpu_seconds_per_query,omitempty"`
	GoMemSysBytes      uint64                  `json:"go_mem_sys_bytes"`
	Counters           map[string]float64      `json:"counters"`
	HTTPRequests       int                     `json:"http_requests"`
	HTTPRequestBytes   int64                   `json:"http_request_bytes"`
	HTTPResponseBytes  int64                   `json:"http_response_bytes"`
	Errors             int                     `json:"errors"`
	Comparison         comparisonIdentity      `json:"comparison"`
}

type exactControlRow struct {
	Embedding     string         `json:"embedding"`
	Quality       qualityMetrics `json:"quality"`
	TopK          int            `json:"top_k"`
	CorpusVectors int            `json:"corpus_vectors"`
	Method        string         `json:"method"`
}

type ingestionRepetition struct {
	Repetition              int               `json:"repetition"`
	FreshDB                 bool              `json:"fresh_db"`
	SourceDocs              int               `json:"source_docs"`
	ChunkDocs               int               `json:"chunk_docs"`
	IngestSourcesSeconds    float64           `json:"ingest_sources_seconds"`
	EndToEndSeconds         float64           `json:"end_to_end_seconds"`
	SourceDocsPerSec        float64           `json:"source_docs_per_sec"`
	ChunkDocsPerSec         float64           `json:"chunk_docs_per_sec"`
	ChunkStageShare         float64           `json:"chunk_stage_share"`
	EmbedStageShare         float64           `json:"embed_stage_share"`
	IndexMutationStageShare float64           `json:"index_mutation_stage_share"`
	IndexPublicationShare   float64           `json:"index_publication_share"`
	CheckpointShare         float64           `json:"checkpoint_share"`
	BytesPerSource          float64           `json:"bytes_per_source"`
	BytesPerChunk           float64           `json:"bytes_per_chunk"`
	BytesPerOp              float64           `json:"bytes_per_op"`
	AllocsPerOp             float64           `json:"allocs_per_op"`
	StorageBytes            int64             `json:"storage_bytes"`
	CheckpointSeconds       float64           `json:"checkpoint_seconds"`
	ReopenSeconds           float64           `json:"reopen_seconds"`
	ReopenParity            bool              `json:"reopen_parity"`
	StorageCounters         map[string]string `json:"storage_counters"`
	Failure                 string            `json:"failure,omitempty"`
}

type ingestionSummary struct {
	Repetitions          int     `json:"repetitions"`
	MedianDocsPerSec     float64 `json:"median_docs_per_sec"`
	P95DocsPerSec        float64 `json:"p95_docs_per_sec"`
	MedianBytesPerOp     float64 `json:"median_bytes_per_op"`
	P95BytesPerOp        float64 `json:"p95_bytes_per_op"`
	HistoricalReproduced bool    `json:"historical_37_59_docs_s_132gb_reproduced"`
}

type lifecycleEvidence struct {
	InitialSources    int                `json:"initial_sources"`
	FinalSources      int                `json:"final_sources"`
	InitialChunks     int                `json:"initial_chunks"`
	FinalChunks       int                `json:"final_chunks"`
	UnchangedReingest bool               `json:"unchanged_reingest"`
	UpdatedSource     string             `json:"updated_source"`
	DeletedSource     string             `json:"deleted_source"`
	ColdReopenParity  bool               `json:"cold_reopen_parity"`
	TextIndexParity   bool               `json:"text_index_parity"`
	VectorIndexParity bool               `json:"vector_index_parity"`
	ScalarIndexParity bool               `json:"scalar_index_parity"`
	FaultBoundary     string             `json:"fault_boundary"`
	FaultEvidence     capabilityEvidence `json:"fault_evidence"`
}

type frozenGate struct {
	CrossTenantResults         int     `json:"cross_tenant_results"`
	CrossWorkspaceResults      int     `json:"cross_workspace_results"`
	FullDocumentScanFallbacks  int     `json:"full_document_scan_fallbacks"`
	MinTimedQueriesPerCell     int     `json:"min_timed_queries_per_cell"`
	MinRepetitionsPerCell      int     `json:"min_repetitions_per_cell"`
	MaxUnaffectedQPSRegression float64 `json:"max_unaffected_qps_regression_pct"`
	CandidateMinDocsPerSec     float64 `json:"candidate_min_docs_per_sec"`
	CandidateMaxBytesPerOp     float64 `json:"candidate_max_bytes_per_op"`
	NoisePolicy                string  `json:"noise_policy"`
	Rationale                  string  `json:"rationale"`
}

type applicationProvenance struct {
	ProductBaseSHA          string            `json:"product_base_sha"`
	HarnessRevision         string            `json:"harness_revision"`
	BinarySHA256            string            `json:"binary_sha256"`
	FixtureSHA256           string            `json:"fixture_sha256"`
	ConfigSHA256            string            `json:"config_sha256"`
	SemanticVectorSHA256    string            `json:"semantic_vector_sha256"`
	HashingRegressionSHA256 string            `json:"hashing_regression_sha256"`
	GoVersion               string            `json:"go_version"`
	GOOS                    string            `json:"goos"`
	GOARCH                  string            `json:"goarch"`
	CGOEnabled              string            `json:"cgo_enabled"`
	Hostname                string            `json:"hostname"`
	HostNote                string            `json:"host_note,omitempty"`
	Command                 []string          `json:"command"`
	RepetitionOrder         string            `json:"repetition_order"`
	Environment             map[string]string `json:"environment"`
}

type applicationReport struct {
	Schema            string                       `json:"schema"`
	Authority         string                       `json:"authority"`
	GeneratedAtUTC    string                       `json:"generated_at_utc"`
	Provenance        applicationProvenance        `json:"provenance"`
	Fixture           applicationFixture           `json:"fixture"`
	SemanticVectors   semanticVectorBundle         `json:"semantic_vectors"`
	Dimensions        map[string]dimensionContract `json:"dimensions"`
	TimingBoundary    timingBoundary               `json:"timing_boundary"`
	IngestionBoundary ingestionBoundary            `json:"ingestion_boundary"`
	IngestionRuns     []ingestionRepetition        `json:"ingestion_runs"`
	IngestionSummary  ingestionSummary             `json:"ingestion_summary"`
	Lifecycle         map[string]lifecycleEvidence `json:"lifecycle"`
	Rows              []applicationRow             `json:"rows"`
	ExactControls     []exactControlRow            `json:"exact_controls"`
	Failures          []string                     `json:"failures"`
	Gate              frozenGate                   `json:"frozen_gate"`
}

type semanticFixtureEmbedder struct {
	bundle *semanticVectorBundle
}

func (e *semanticFixtureEmbedder) Dimensions() int { return e.bundle.Dimensions }

func (e *semanticFixtureEmbedder) EmbedBatch(ctx context.Context, texts [][]byte) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, embedding.ErrEmptyBatch
	}
	out := make([][]float32, len(texts))
	for i, text := range texts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		vector, ok := e.bundle.Vectors[string(text)]
		if !ok {
			return nil, fmt.Errorf("semantic fixture vector missing for text digest %s: %w", shortTextDigest(text), embedding.ErrInvalidOutput)
		}
		out[i] = append([]float32(nil), vector...)
	}
	return out, nil
}

func shortTextDigest(text []byte) string {
	sum := sha256.Sum256(text)
	return hex.EncodeToString(sum[:8])
}

var semanticRegisterOnce sync.Once
var semanticRegisterErr error

func registerSemanticProvider(bundle *semanticVectorBundle) error {
	semanticRegisterOnce.Do(func() {
		semanticRegisterErr = embedding.DefaultRegistry().Register(semanticProviderName, func(cfg embedding.Config) (embedding.Embedder, error) {
			if cfg.Dimensions != bundle.Dimensions {
				return nil, fmt.Errorf("semantic fixture dimensions %d != %d: %w", cfg.Dimensions, bundle.Dimensions, embedding.ErrDimensionMismatch)
			}
			return &semanticFixtureEmbedder{bundle: bundle}, nil
		})
	})
	return semanticRegisterErr
}

func validateApplicationConfig(cfg applicationConfig) error {
	if cfg.TopK < 10 || cfg.CandidateLimit < cfg.TopK || cfg.EfSearch < cfg.TopK {
		return fmt.Errorf("config: top_k>=10 and candidate/ef budgets >= top_k required")
	}
	if cfg.Repetitions <= 0 || cfg.SamplesPerRep <= 0 || cfg.IngestionReps <= 0 {
		return fmt.Errorf("config: repetitions, samples, and ingestion reps must be positive")
	}
	if cfg.FinalEvidence && (!isFullRevision(cfg.ProductBaseSHA) || !isFullRevision(cfg.HarnessRevision)) {
		return fmt.Errorf("config: final baseline requires full 40-character hexadecimal product and harness revisions")
	}
	if cfg.FinalEvidence && (cfg.Repetitions < 3 || cfg.SamplesPerRep*cfg.Repetitions < 1000 || cfg.IngestionReps < 5) {
		return fmt.Errorf("config: final baseline requires >=3 reps, >=1000 samples/cell, and >=5 ingestion reps")
	}
	return nil
}

func isFullRevision(revision string) bool {
	if len(revision) != 40 {
		return false
	}
	_, err := hex.DecodeString(revision)
	return err == nil
}

func applicationConfigDigest(cfg applicationConfig) string {
	raw, _ := json.Marshal(struct {
		TopK, CandidateLimit, EfSearch, M         int
		WarmupQueries, Repetitions, SamplesPerRep int
		IngestionReps                             int
	}{
		TopK: cfg.TopK, CandidateLimit: cfg.CandidateLimit, EfSearch: cfg.EfSearch, M: cfg.M,
		WarmupQueries: cfg.WarmupQueries, Repetitions: cfg.Repetitions,
		SamplesPerRep: cfg.SamplesPerRep, IngestionReps: cfg.IngestionReps,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func resolveApplicationHarnessRevision(cfg applicationConfig, settings map[string]string, buildInfoOK bool) (string, error) {
	revision := strings.TrimSpace(settings["vcs.revision"])
	if !buildInfoOK || !isFullRevision(revision) {
		if cfg.FinalEvidence {
			return "", fmt.Errorf("provenance: final evidence binary is not bound to a full debug.ReadBuildInfo vcs.revision")
		}
		return "unavailable", nil
	}
	if cfg.HarnessRevision != "" && cfg.HarnessRevision != revision {
		return "", fmt.Errorf("provenance: requested harness revision %q does not match binary vcs.revision %q", cfg.HarnessRevision, revision)
	}
	if cfg.FinalEvidence && settings["vcs.modified"] != "false" {
		return "", fmt.Errorf("provenance: final evidence binary is dirty or lacks vcs.modified=false")
	}
	return revision, nil
}

func runApplicationBaseline(cfg applicationConfig) (*applicationReport, error) {
	if err := validateApplicationConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.FinalEvidence {
		settings, ok := runtimeBuildInfo()
		if _, err := resolveApplicationHarnessRevision(cfg, settings, ok); err != nil {
			return nil, err
		}
	}
	fixture := buildApplicationFixture()
	if err := validateApplicationFixture(&fixture); err != nil {
		return nil, err
	}
	bundle, err := loadSemanticVectors()
	if err != nil {
		return nil, err
	}
	if err := validateSemanticVectors(&fixture, bundle); err != nil {
		return nil, err
	}
	if err := registerSemanticProvider(bundle); err != nil {
		return nil, err
	}
	root := cfg.Dir
	if root == "" {
		root, err = os.MkdirTemp("", "treedb_rag_application_*")
		if err != nil {
			return nil, err
		}
		if !cfg.KeepDir {
			defer os.RemoveAll(root)
		}
	} else {
		if err := os.RemoveAll(root); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(root, 0o755); err != nil {
			return nil, err
		}
	}

	authority := "DIAGNOSTIC_NOT_FINAL_EVIDENCE"
	if cfg.FinalEvidence {
		authority = "M1_RETAINED_BASELINE"
	}
	report := &applicationReport{
		Schema: applicationReportSchema, Authority: authority,
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), Fixture: fixture,
		Provenance: applicationProvenance{Environment: map[string]string{}},
		SemanticVectors: semanticVectorBundle{
			Schema: bundle.Schema, Model: bundle.Model, Revision: bundle.Revision, License: bundle.License,
			Preprocessing: bundle.Preprocessing, Dimensions: bundle.Dimensions, CorpusLicense: bundle.CorpusLicense,
			GenerationCommand: bundle.GenerationCommand,
		},
		Dimensions: map[string]dimensionContract{}, TimingBoundary: timingBoundary{},
		IngestionBoundary: ingestionBoundary{SourceRowUsesIngestSources: true, GeneratedChunkRowsLabel: "generated_chunk_rows_per_sec"},
		Lifecycle:         map[string]lifecycleEvidence{},
	}
	if err := report.TimingBoundary.validate(); err != nil {
		return nil, err
	}
	if err := report.IngestionBoundary.validate(); err != nil {
		return nil, err
	}

	for rep := range cfg.IngestionReps {
		ingest, ingestErr := runApplicationIngestionRep(cfg, &fixture, filepath.Join(root, fmt.Sprintf("ingestion-%02d", rep)), rep)
		report.IngestionRuns = append(report.IngestionRuns, ingest)
		if ingestErr != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("ingestion rep %d: %v", rep, ingestErr))
		}
	}
	if len(report.Failures) > 0 {
		return nil, fmt.Errorf("ingestion baseline failed: %s", strings.Join(report.Failures, "; "))
	}
	report.IngestionSummary = summarizeIngestion(report.IngestionRuns)

	for _, embeddingCell := range applicationEmbeddings {
		dims, provider := embeddingCellConfig(embeddingCell, bundle)
		dimensions := dimensionContract{Config: dims, Corpus: dims, Queries: dims, Index: dims, Vectors: dims}
		if err := dimensions.validate(); err != nil {
			return nil, err
		}
		report.Dimensions[embeddingCell] = dimensions
		env, lifecycle, openErr := openApplicationEnvironment(cfg, &fixture, bundle, embeddingCell, provider, dims, filepath.Join(root, embeddingCell))
		if openErr != nil {
			return nil, fmt.Errorf("open %s environment: %w", embeddingCell, openErr)
		}
		report.Lifecycle[embeddingCell] = lifecycle
		queryVectors, vectorErr := applicationQueryVectors(&fixture, bundle, embeddingCell, provider, dims)
		if vectorErr != nil {
			env.close()
			return nil, vectorErr
		}
		control, controlErr := buildExactControl(&fixture, bundle, embeddingCell, provider, dims, cfg.TopK)
		if controlErr != nil {
			env.close()
			return nil, controlErr
		}
		report.ExactControls = append(report.ExactControls, control)
		for _, cell := range applicationCellMatrix(embeddingCell) {
			row, rowErr := runApplicationCell(cfg, &fixture, env, queryVectors, cell)
			if rowErr != nil {
				env.close()
				return nil, fmt.Errorf("cell %+v: %w", cell, rowErr)
			}
			report.Rows = append(report.Rows, row)
		}
		report.Provenance.Environment["storage_stats_"+embeddingCell] = stableStringMap(env.storageStats)
		env.close()
	}

	report.IngestionSummary = summarizeIngestion(report.IngestionRuns)
	report.Gate = freezeApplicationGate(report.IngestionSummary)
	provenance, err := buildApplicationProvenance(cfg, &fixture, bundle)
	if err != nil {
		return nil, err
	}
	for key, value := range report.Provenance.Environment {
		provenance.Environment[key] = value
	}
	report.Provenance = provenance
	if err := validateApplicationReport(report, cfg); err != nil {
		return nil, err
	}
	return report, nil
}

func embeddingCellConfig(cell string, bundle *semanticVectorBundle) (int, string) {
	if cell == "semantic_minilm" {
		return bundle.Dimensions, semanticProviderName
	}
	return 64, embedding.ProviderHashing
}

func applicationCellMatrix(embeddingCell string) []applicationCellIdentity {
	rows := make([]applicationCellIdentity, 0, len(applicationRoutes)*len(applicationProjections)*len(applicationFilterOrder)*2*len(applicationSurfaces)*len(applicationClients))
	for _, route := range applicationRoutes {
		for _, projection := range applicationProjections {
			for _, filter := range applicationFilterOrder {
				for _, collapse := range []string{"disabled", "enabled_cap_2"} {
					for _, surface := range applicationSurfaces {
						for _, clients := range applicationClients {
							cell := applicationCellIdentity{Route: route, Projection: projection, Filter: filter, Collapse: collapse, Surface: surface, Embedding: embeddingCell, Clients: clients}
							cell.VectorRoute = applicationVectorRoute(cell)
							rows = append(rows, cell)
						}
					}
				}
			}
		}
	}
	return rows
}

func applicationMaxChunksPerParent(cell applicationCellIdentity) int {
	if cell.Collapse == "enabled_cap_2" {
		return 2
	}
	return 0
}
func applicationVectorRoute(cell applicationCellIdentity) string {
	if cell.Route == "text_only" {
		return "none"
	}
	if cell.Surface == "http_service" && cell.Route == "vector_only" && cell.Filter == filterUnfiltered {
		return "declared_column_graph_ann"
	}
	return "declared_column_graph_exact"
}

func unsupportedCapability(cell applicationCellIdentity) *capabilityError {
	issues := []int{}
	codes := []string{}
	if cell.Collapse != "disabled" && applicationMaxChunksPerParent(cell) == 0 {
		codes = append(codes, "parent_collapse_shape_unavailable")
	}
	if applicationMaxChunksPerParent(cell) > 0 && cell.Surface == "http_service" && cell.Route == "vector_only" {
		codes = append(codes, "http_vector_parent_collapse_unavailable")
	}
	if cell.Surface == "http_service" && cell.Projection == "score_only" {
		codes = append(codes, "http_score_only_route_unavailable")
	}
	if len(codes) == 0 {
		return nil
	}
	return &capabilityError{Code: strings.Join(codes, "+"), RequiredIssues: issues, Message: "required product capability is absent on exact baseline; no query was issued and no partial ranking was accepted"}
}

type applicationEnvironment struct {
	db           *backenddb.DB
	col          *collections.Collection
	service      *documentservice.Service
	server       *httptest.Server
	client       *http.Client
	storageStats map[string]string
}

func (e *applicationEnvironment) close() {
	if e.server != nil {
		e.server.Close()
	}
	if e.service != nil {
		_ = e.service.Close()
	}
	if e.db != nil {
		_ = e.db.Close()
	}
}

func openApplicationEnvironment(cfg applicationConfig, fixture *applicationFixture, bundle *semanticVectorBundle, embeddingCell string, provider string, dims int, dir string) (*applicationEnvironment, lifecycleEvidence, error) {
	var lifecycle lifecycleEvidence
	if err := os.RemoveAll(dir); err != nil {
		return nil, lifecycle, err
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		return nil, lifecycle, err
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(applicationSourceCollectionMeta(cfg, dims)); err != nil {
		_ = db.Close()
		return nil, lifecycle, err
	}
	sourceCol, err := manager.OpenCollection(applicationSourceCollection)
	if err != nil {
		_ = db.Close()
		return nil, lifecycle, err
	}
	initialSources := applicationSourceDocuments(fixture, false)
	result, err := sourceCol.IngestSources(context.Background(), initialSources, applicationIngestConfig(provider, dims))
	if err != nil {
		_ = db.Close()
		return nil, lifecycle, err
	}
	lifecycle.InitialSources = len(initialSources)
	lifecycle.InitialChunks = countIngestedChunks(result)

	unchanged := sourceDocumentFor(fixture.Sources[1], fixture.Sources[1].InitialBody)
	if _, err := sourceCol.IngestSources(context.Background(), []collections.SourceDocument{unchanged}, applicationIngestConfig(provider, dims)); err != nil {
		_ = db.Close()
		return nil, lifecycle, fmt.Errorf("unchanged reingest: %w", err)
	}
	lifecycle.UnchangedReingest = true
	updated := fixture.Sources[0]
	if _, err := sourceCol.IngestSources(context.Background(), []collections.SourceDocument{sourceDocumentFor(updated, updated.FinalBody)}, applicationIngestConfig(provider, dims)); err != nil {
		_ = db.Close()
		return nil, lifecycle, fmt.Errorf("update source: %w", err)
	}
	lifecycle.UpdatedSource = updated.ID
	deleted := fixture.Sources[len(fixture.Sources)-1]
	children, err := sourceCol.ChunkChildren([]byte(deleted.ID))
	if err != nil {
		return nil, lifecycle, err
	}
	deleteIDs := append(children, []byte(deleted.ID))
	if _, err := sourceCol.DeleteBatch(deleteIDs); err != nil {
		return nil, lifecycle, err
	}
	lifecycle.DeletedSource = deleted.ID
	if err := db.Checkpoint(); err != nil {
		return nil, lifecycle, err
	}
	beforeDigest, beforeChildIDs, beforeChildDocs, err := sourceChildSnapshot(sourceCol, fixture, bundle, embeddingCell, provider, dims)
	if err != nil {
		return nil, lifecycle, err
	}
	beforeIndexes, err := queryApplicationIndexes(sourceCol, fixture, beforeChildIDs, beforeChildDocs, cfg.EfSearch)
	if err != nil {
		return nil, lifecycle, fmt.Errorf("before reopen index queries: %w", err)
	}
	if err := db.Close(); err != nil {
		return nil, lifecycle, err
	}

	db, err = backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		return nil, lifecycle, err
	}
	manager = collections.NewCollectionManager(db)
	sourceCol, err = manager.OpenCollection(applicationSourceCollection)
	if err != nil {
		_ = db.Close()
		return nil, lifecycle, err
	}
	afterDigest, childIDs, childDocs, err := sourceChildSnapshot(sourceCol, fixture, bundle, embeddingCell, provider, dims)
	if err != nil {
		_ = db.Close()
		return nil, lifecycle, err
	}
	afterIndexes, err := queryApplicationIndexes(sourceCol, fixture, childIDs, childDocs, cfg.EfSearch)
	if err != nil {
		_ = db.Close()
		return nil, lifecycle, fmt.Errorf("after reopen index queries: %w", err)
	}
	expectedIndexes := expectedApplicationIndexQuerySnapshot(fixture, childIDs)
	lifecycle.ColdReopenParity = beforeDigest == afterDigest
	lifecycle.TextIndexParity = equalStrings(beforeIndexes.TextChildIDs, afterIndexes.TextChildIDs)
	lifecycle.VectorIndexParity = equalStrings(beforeIndexes.VectorChildIDs, afterIndexes.VectorChildIDs)
	lifecycle.ScalarIndexParity = equalStrings(beforeIndexes.ScalarDocumentIDs, afterIndexes.ScalarDocumentIDs)
	lifecycle.FinalSources = len(applicationSourceDocuments(fixture, true))
	lifecycle.FinalChunks = len(childIDs)
	if err := validateApplicationIndexQueryParity(beforeIndexes, afterIndexes, expectedIndexes); err != nil {
		_ = db.Close()
		return nil, lifecycle, err
	}
	if err := validateLifecycleEvidence("cold reopen", lifecycle); err != nil {
		_ = db.Close()
		return nil, lifecycle, err
	}

	service := documentservice.New(manager)
	if _, err := service.CreateIndex(context.Background(), applicationIndexRequest(cfg, dims)); err != nil {
		_ = service.Close()
		_ = db.Close()
		return nil, lifecycle, err
	}
	queryCol, err := manager.OpenCollection(applicationCollection)
	if err != nil {
		_ = service.Close()
		_ = db.Close()
		return nil, lifecycle, err
	}
	if _, err := queryCol.InsertBatch(childIDs, childDocs); err != nil {
		_ = service.Close()
		_ = db.Close()
		return nil, lifecycle, fmt.Errorf("project ingested children into query collection: %w", err)
	}
	if _, err := queryCol.RebuildVectorIndex(applicationVectorIndex); err != nil {
		_ = service.Close()
		_ = db.Close()
		return nil, lifecycle, fmt.Errorf("rebuild projected vector index: %w", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = service.Close()
		_ = db.Close()
		return nil, lifecycle, err
	}
	lifecycle.FaultBoundary = "IngestSources publishes each source replacement as one dependency-closed durable root selection"
	lifecycle.FaultEvidence = capabilityEvidence{ErrorType: capabilityErrorType, Code: "storage_boundary_fault_injection_out_of_process", Message: "the external application harness verifies public lifecycle and reopen parity; package fault tests cover private publication and command-WAL cuts", ResultsReturned: 0, FailClosed: true}
	server := httptest.NewServer(documentservice.NewHandler(service))
	env := &applicationEnvironment{db: db, col: queryCol, service: service, server: server, client: server.Client(), storageStats: relevantStorageStats(db.Stats())}
	return env, lifecycle, nil
}

func applicationIndexRequest(cfg applicationConfig, dims int) documentservice.CreateIndexRequest {
	return documentservice.CreateIndexRequest{
		Name: applicationCollection, Dimension: dims, Metric: documentservice.MetricCosine,
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: cfg.M, EfConstruction: 64, EfSearch: cfg.EfSearch},
		ScalarFields: []documentservice.ScalarFieldDeclaration{
			{Field: "meta.tenant_id", ValueType: documentservice.ScalarFieldString},
			{Field: "meta.workspace_id", ValueType: documentservice.ScalarFieldString},
			{Field: "meta.updated_year", ValueType: documentservice.ScalarFieldInt64},
		},
	}
}

func applicationSourceCollectionMeta(cfg applicationConfig, dims int) *collections.CollectionMeta {
	return &collections.CollectionMeta{
		Name:    applicationSourceCollection,
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
		Indexes: []collections.IndexDefinition{
			{Name: "meta_tenant_id", Field: "meta.tenant_id", ValueType: collections.IndexValueString},
			{Name: "meta_workspace_id", Field: "meta.workspace_id", ValueType: collections.IndexValueString},
			{Name: "meta_updated_year", Field: "meta.updated_year", ValueType: collections.IndexValueInt64},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name: applicationVectorIndex, Field: applicationVectorField,
			Metric: collections.VectorMetricCosine, Dimensions: dims, M: cfg.M,
			Encoding: collections.VectorIndexEncodingFloat32,
			Strategy: collections.VectorIndexStrategyNativeRuntime,
		}},
		TextIndexes: []collections.TextIndexDefinition{{
			Name:     applicationTextField,
			Fields:   []collections.TextIndexField{{Field: applicationTextField}},
			Analyzer: collections.TextAnalyzerSimple, StorePositions: true,
		}},
	}
}

func applicationIngestConfig(provider string, dims int) collections.IngestSourcesConfig {
	return collections.IngestSourcesConfig{
		Chunking:        chunking.Config{Strategy: chunking.StrategyFixedWindow, SizeUnit: chunking.SizeUnitRunes, Size: applicationChunkSize, Overlap: 0},
		Embedding:       embedding.Config{Provider: provider, Dimensions: dims},
		VectorIndexName: applicationVectorIndex, TextField: applicationTextField, Concurrency: 4,
	}
}

func applicationSourceDocuments(fixture *applicationFixture, final bool) []collections.SourceDocument {
	sources := make([]collections.SourceDocument, 0, len(fixture.Sources))
	for _, source := range fixture.Sources {
		if final && source.Deleted {
			continue
		}
		body := source.InitialBody
		if final {
			body = source.FinalBody
		}
		sources = append(sources, sourceDocumentFor(source, body))
	}
	return sources
}

func sourceDocumentFor(source applicationSource, body string) collections.SourceDocument {
	return collections.SourceDocument{
		ID: []byte(source.ID), Fields: map[string]any{applicationTextField: body},
		Meta: sourceMetadata(source),
	}
}

func sourceMetadata(source applicationSource) map[string]any {
	return map[string]any{
		"tenant_id": source.Tenant, "workspace_id": source.Workspace,
		"source_uri": source.SourceURI, "source_type": source.SourceType,
		"acl": append([]string(nil), source.ACL...), "updated_year": source.UpdatedYear,
	}
}

func sourceChildSnapshot(col *collections.Collection, fixture *applicationFixture, bundle *semanticVectorBundle, embeddingCell, provider string, dims int) (string, [][]byte, [][]byte, error) {
	type childRow struct {
		id, document []byte
	}
	rows := make([]childRow, 0, len(fixture.Sources)*applicationChunksPerSource)
	var expectedEmbedder embedding.Embedder
	if embeddingCell != "semantic_minilm" {
		var err error
		expectedEmbedder, err = embedding.DefaultRegistry().Create(embedding.Config{Provider: provider, Dimensions: dims})
		if err != nil {
			return "", nil, nil, err
		}
	}
	for _, source := range fixture.Sources {
		children, err := col.ChunkChildren([]byte(source.ID))
		if err != nil {
			return "", nil, nil, err
		}
		parent, err := col.Get([]byte(source.ID))
		if err != nil {
			return "", nil, nil, err
		}
		if source.Deleted {
			if len(children) != 0 || parent != nil {
				return "", nil, nil, fmt.Errorf("deleted source %q remains parent=%t children=%d", source.ID, parent != nil, len(children))
			}
			continue
		}
		if parent == nil || len(children) != applicationChunksPerSource {
			return "", nil, nil, fmt.Errorf("live source %q parent=%t children=%d", source.ID, parent != nil, len(children))
		}
		var decoded map[string]any
		if err := json.Unmarshal(parent, &decoded); err != nil {
			return "", nil, nil, err
		}
		meta, ok := decoded["meta"].(map[string]any)
		if !ok || meta["tenant_id"] != source.Tenant || meta["workspace_id"] != source.Workspace {
			return "", nil, nil, fmt.Errorf("source %q parent metadata mismatch", source.ID)
		}
		parentBody, ok := decoded[applicationTextField].(string)
		if !ok || parentBody != source.FinalBody {
			return "", nil, nil, fmt.Errorf("source %q parent body does not match final fixture bytes", source.ID)
		}
		finalRunes := []rune(source.FinalBody)
		for _, childID := range children {
			document, err := col.Get(childID)
			if err != nil {
				return "", nil, nil, err
			}
			if document == nil {
				return "", nil, nil, fmt.Errorf("source %q child %q missing", source.ID, childID)
			}
			parentID, ordinal, ok := chunking.ParseChildID(string(childID))
			if !ok || parentID != source.ID {
				return "", nil, nil, fmt.Errorf("source %q child ID %q is invalid", source.ID, childID)
			}
			start := ordinal * fixture.ChunkSize
			end := min(start+fixture.ChunkSize, len(finalRunes))
			if start < 0 || start >= end || end > len(finalRunes) {
				return "", nil, nil, fmt.Errorf("source %q child %q ordinal is outside final fixture", source.ID, childID)
			}
			expectedText := string(finalRunes[start:end])
			var decodedChild struct {
				Content   string    `json:"content"`
				Embedding []float32 `json:"embedding"`
			}
			if err := json.Unmarshal(document, &decodedChild); err != nil {
				return "", nil, nil, fmt.Errorf("decode source %q child %q: %w", source.ID, childID, err)
			}
			if decodedChild.Content != expectedText {
				return "", nil, nil, fmt.Errorf("source %q child %q content does not match final fixture bytes", source.ID, childID)
			}
			var expectedVector []float32
			if embeddingCell == "semantic_minilm" {
				if bundle == nil {
					return "", nil, nil, fmt.Errorf("semantic fixture bundle unavailable")
				}
				expectedVector, ok = bundle.Vectors[expectedText]
				if !ok {
					return "", nil, nil, fmt.Errorf("semantic vector missing for source %q child %q", source.ID, childID)
				}
			} else {
				vectors, embedErr := expectedEmbedder.EmbedBatch(context.Background(), [][]byte{[]byte(expectedText)})
				if embedErr != nil || len(vectors) != 1 {
					return "", nil, nil, fmt.Errorf("expected vector for source %q child %q: %w", source.ID, childID, embedErr)
				}
				expectedVector = vectors[0]
			}
			if !equalFloat32Bits(decodedChild.Embedding, expectedVector) {
				return "", nil, nil, fmt.Errorf("source %q child %q embedding does not match final fixture vector", source.ID, childID)
			}
			if err := chunking.ValidateChunkChild(string(childID), document); err != nil {
				return "", nil, nil, err
			}
			rows = append(rows, childRow{id: append([]byte(nil), childID...), document: append([]byte(nil), document...)})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].id, rows[j].id) < 0 })
	hash := sha256.New()
	ids := make([][]byte, len(rows))
	documents := make([][]byte, len(rows))
	for i, row := range rows {
		ids[i], documents[i] = row.id, row.document
		hash.Write(row.id)
		hash.Write([]byte{0})
		hash.Write(row.document)
		hash.Write([]byte{0xff})
	}
	return hex.EncodeToString(hash.Sum(nil)), ids, documents, nil
}

func equalFloat32Bits(left, right []float32) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if math.Float32bits(left[i]) != math.Float32bits(right[i]) {
			return false
		}
	}
	return true
}

func queryApplicationIndexes(col *collections.Collection, fixture *applicationFixture, childIDs, childDocs [][]byte, efSearch int) (applicationIndexQuerySnapshot, error) {
	var snapshot applicationIndexQuerySnapshot
	expectedTextIDs := make(map[string]bool, len(childIDs)+len(fixture.Sources))
	for _, id := range childIDs {
		expectedTextIDs[string(id)] = true
	}
	for _, source := range fixture.Sources {
		if !source.Deleted {
			expectedTextIDs[source.ID] = true
		}
	}
	textTopK := len(expectedTextIDs)
	text, err := col.SearchText(collections.TextSearchOptions{
		IndexName: applicationTextField, Query: "guidance", TopK: textTopK,
		ResultMode:     collections.TextSearchResultModeScoreOnly,
		CandidateLimit: textTopK + 1, MaxPostingsScanned: textTopK * 32,
	})
	if err != nil {
		return snapshot, err
	}
	if text.Stats.FailClosed != 0 || text.Stats.FullDocumentScanFallbacks != 0 || text.Stats.DocumentsFetched != 0 {
		return snapshot, fmt.Errorf("text index returned unhealthy stats: %+v", text.Stats)
	}
	for _, result := range text.Results {
		id := string(result.DocumentID)
		if !expectedTextIDs[id] {
			return snapshot, fmt.Errorf("text index returned stale or duplicate ID %q", id)
		}
		delete(expectedTextIDs, id)
		if _, _, child := chunking.ParseChildID(id); child {
			snapshot.TextChildIDs = append(snapshot.TextChildIDs, id)
		}
	}
	if len(expectedTextIDs) != 0 {
		return snapshot, fmt.Errorf("text index omitted %d live parent/child IDs", len(expectedTextIDs))
	}
	sort.Strings(snapshot.TextChildIDs)

	var buffer collections.VectorIndexSearchBuffer
	for i, document := range childDocs {
		var decoded struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := json.Unmarshal(document, &decoded); err != nil {
			return snapshot, fmt.Errorf("decode vector query document %q: %w", childIDs[i], err)
		}
		if len(decoded.Embedding) == 0 {
			return snapshot, fmt.Errorf("vector query document %q has no embedding", childIDs[i])
		}
		response, err := col.SearchVectorIndexWithBuffer(collections.VectorIndexSearchOptions{
			IndexName: applicationVectorIndex, Query: decoded.Embedding,
			QueryMode: collections.VectorIndexQueryModeExact, TopK: 1,
			EfSearch: max(efSearch, len(childIDs)), StatsMode: collections.VectorIndexSearchStatsModeProduction,
		}, &buffer)
		if err != nil {
			return snapshot, fmt.Errorf("vector index query %q: %w", childIDs[i], err)
		}
		if response.Path != collections.VectorIndexSearchPathNativeRuntime || len(response.Results) != 1 {
			return snapshot, fmt.Errorf("vector index query %q path=%q results=%d", childIDs[i], response.Path, len(response.Results))
		}
		snapshot.VectorChildIDs = append(snapshot.VectorChildIDs, string(response.Results[0].ID))
	}
	sort.Strings(snapshot.VectorChildIDs)

	years := map[int]bool{}
	for _, source := range fixture.Sources {
		years[source.UpdatedYear] = true
	}
	orderedYears := make([]int, 0, len(years))
	for year := range years {
		orderedYears = append(orderedYears, year)
	}
	sort.Ints(orderedYears)
	for _, year := range orderedYears {
		ids, err := col.FindByIndexValue("meta_updated_year", int64(year))
		if err != nil {
			return snapshot, fmt.Errorf("scalar index query updated_year=%d: %w", year, err)
		}
		for _, id := range ids {
			snapshot.ScalarDocumentIDs = append(snapshot.ScalarDocumentIDs, string(id))
		}
	}
	sort.Strings(snapshot.ScalarDocumentIDs)
	return snapshot, nil
}

func expectedApplicationIndexQuerySnapshot(fixture *applicationFixture, childIDs [][]byte) applicationIndexQuerySnapshot {
	expected := applicationIndexQuerySnapshot{
		TextChildIDs: make([]string, len(childIDs)), VectorChildIDs: make([]string, len(childIDs)),
	}
	for i, id := range childIDs {
		expected.TextChildIDs[i], expected.VectorChildIDs[i] = string(id), string(id)
		expected.ScalarDocumentIDs = append(expected.ScalarDocumentIDs, string(id))
	}
	for _, source := range fixture.Sources {
		if !source.Deleted {
			expected.ScalarDocumentIDs = append(expected.ScalarDocumentIDs, source.ID)
		}
	}
	sort.Strings(expected.ScalarDocumentIDs)
	return expected
}

func validateApplicationIndexQueryParity(before, after, expected applicationIndexQuerySnapshot) error {
	for name, values := range map[string][3][]string{
		"text":   {before.TextChildIDs, after.TextChildIDs, expected.TextChildIDs},
		"vector": {before.VectorChildIDs, after.VectorChildIDs, expected.VectorChildIDs},
		"scalar": {before.ScalarDocumentIDs, after.ScalarDocumentIDs, expected.ScalarDocumentIDs},
	} {
		if !equalStrings(values[0], values[2]) {
			return fmt.Errorf("%s index before reopen does not match fixture live set: got=%q want=%q", name, values[0], values[2])
		}
		if !equalStrings(values[1], values[2]) {
			return fmt.Errorf("%s index after reopen does not match fixture live set: got=%q want=%q", name, values[1], values[2])
		}
	}
	return nil
}

func validateLifecycleEvidence(name string, lifecycle lifecycleEvidence) error {
	if !lifecycle.ColdReopenParity {
		return fmt.Errorf("report: %s cold reopen parity is false", name)
	}
	if !lifecycle.TextIndexParity || !lifecycle.VectorIndexParity || !lifecycle.ScalarIndexParity {
		return fmt.Errorf("report: %s queried index parity text/vector/scalar=%t/%t/%t",
			name, lifecycle.TextIndexParity, lifecycle.VectorIndexParity, lifecycle.ScalarIndexParity)
	}
	return nil
}

func countIngestedChunks(result collections.IngestResult) int {
	total := 0
	for _, source := range result.Ingested {
		total += len(source.ChildIDs)
	}
	return total
}

func applicationQueryVectors(fixture *applicationFixture, bundle *semanticVectorBundle, embeddingCell, provider string, dims int) (map[string][]float32, error) {
	vectors := make(map[string][]float32, len(fixture.Queries))
	if embeddingCell == "semantic_minilm" {
		for _, query := range fixture.Queries {
			vectors[query.ID] = append([]float32(nil), bundle.Queries[query.ID]...)
		}
		return vectors, nil
	}
	embedder, err := embedding.DefaultRegistry().Create(embedding.Config{Provider: provider, Dimensions: dims})
	if err != nil {
		return nil, err
	}
	texts := make([][]byte, len(fixture.Queries))
	for i, query := range fixture.Queries {
		texts[i] = []byte(query.Text)
	}
	out, err := embedder.EmbedBatch(context.Background(), texts)
	if err != nil {
		return nil, err
	}
	for i, query := range fixture.Queries {
		vectors[query.ID] = out[i]
	}
	return vectors, nil
}

type queryResult struct {
	IDs           []string
	Counters      map[string]float64
	RequestBytes  int64
	ResponseBytes int64
	Sources       map[string][2]bool
}

func applicationWarmupQuery(fixture *applicationFixture, ordinal int) applicationQuery {
	return fixture.Queries[ordinal%len(fixture.Queries)]
}

func runApplicationCell(cfg applicationConfig, fixture *applicationFixture, env *applicationEnvironment, queryVectors map[string][]float32, cell applicationCellIdentity) (applicationRow, error) {
	row := applicationRow{Cell: cell, Status: "supported", Counters: map[string]float64{}}
	qualityDigest := applicationFixtureDigest(fixture)
	workRaw, _ := json.Marshal(struct {
		Cell                           applicationCellIdentity
		TopK, CandidateLimit, EfSearch int
	}{cell, cfg.TopK, cfg.CandidateLimit, cfg.EfSearch})
	workSum := sha256.Sum256(workRaw)
	row.Comparison = comparisonIdentity{WorkDigest: hex.EncodeToString(workSum[:]), Projection: cell.Projection, QualityDigest: qualityDigest}
	if capability := unsupportedCapability(cell); capability != nil {
		row.Status = "unsupported"
		row.Capability = &capabilityEvidence{ErrorType: capabilityErrorType, Code: capability.Code, Message: capability.Message, RequiredIssues: capability.RequiredIssues, ResultsReturned: 0, FailClosed: true}
		return row, nil
	}
	call := func(query applicationQuery) (queryResult, error) {
		if cell.Surface == "http_service" {
			return runHTTPQuery(cfg, env, query, queryVectors[query.ID], cell)
		}
		return runDirectQuery(cfg, env.col, query, queryVectors[query.ID], cell, false)
	}
	qualityCall := call
	attributionMode := "untimed_projection_query_sources"
	if cell.Surface == "direct_collection" && cell.Projection == "score_only" {
		qualityCall = func(query applicationQuery) (queryResult, error) {
			return runDirectQuery(cfg, env.col, query, queryVectors[query.ID], cell, true)
		}
		attributionMode = "untimed_compact_same_work_route_filter"
	}
	quality, err := measureApplicationQuality(fixture, cell.Filter, cfg.TopK, qualityCall)
	if err != nil {
		return row, err
	}
	quality.AttributionMode = attributionMode
	row.Quality = quality
	for i := range cfg.WarmupQueries {
		query := applicationWarmupQuery(fixture, i)
		if _, err := call(query); err != nil {
			return row, fmt.Errorf("warmup query %s: %w", query.ID, err)
		}
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	cpuBefore := runtimeCPUSeconds()
	for rep := range cfg.Repetitions {
		repSamples, perf, counters, err := runApplicationRepetition(cfg, fixture, cell, rep, call)
		if err != nil {
			return row, err
		}
		row.Samples = append(row.Samples, repSamples...)
		row.Repetitions = append(row.Repetitions, perf)
		for key, value := range counters {
			row.Counters[key] += value
		}
	}
	cpuAfter := runtimeCPUSeconds()
	runtime.ReadMemStats(&after)
	if cfg.FinalEvidence {
		if err := (measurementClaim{Samples: len(row.Samples), Repetitions: len(row.Repetitions), Label: "final_qps_p99"}).validate(); err != nil {
			return row, err
		}
	}
	latencies := make([]float64, 0, len(row.Samples))
	for _, sample := range row.Samples {
		latencies = append(latencies, sample.Millis)
		row.HTTPRequestBytes += sample.RequestBytes
		row.HTTPResponseBytes += sample.ResponseBytes
		if sample.Error != "" {
			row.Errors++
		}
	}
	row.HTTPRequests = 0
	if cell.Surface == "http_service" {
		row.HTTPRequests = len(row.Samples)
	}
	row.QPSMean = 0
	for _, rep := range row.Repetitions {
		row.QPSMean += rep.QPS
	}
	row.QPSMean /= float64(len(row.Repetitions))
	row.LatencyMSMean = mean(latencies)
	row.LatencyMSP50, _ = percentile(latencies, 50)
	row.LatencyMSP95, _ = percentile(latencies, 95)
	row.LatencyMSP99, _ = percentile(latencies, 99)
	row.BytesPerOp = float64(after.TotalAlloc-before.TotalAlloc) / float64(len(row.Samples))
	row.AllocsPerOp = float64(after.Mallocs-before.Mallocs) / float64(len(row.Samples))
	row.GoMemSysBytes = after.Sys
	if cpuAfter > cpuBefore {
		row.CPUSecondsPerQuery = (cpuAfter - cpuBefore) / float64(len(row.Samples))
	}
	for key, value := range row.Counters {
		row.Counters[key] = value / float64(len(row.Samples))
	}
	row.Quality.CollapseRejections = int(math.Ceil(row.Counters["collapse_rejections"]))
	row.Quality.CollapseExhaustions = int(math.Ceil(row.Counters["collapse_exhaustions"]))
	collapseCap := applicationMaxChunksPerParent(cell)
	guard := artifactGuard{Filter: cell.Filter, CrossTenantResults: int(row.Counters["cross_tenant_results"]), CrossWorkspaceResults: int(row.Counters["cross_workspace_results"]), DocumentsFetched: int(math.Ceil(row.Counters["documents_fetched"])), TopK: cfg.TopK, FullDocumentScanFallbacks: int(row.Counters["full_document_scan_fallbacks"]), CollapseEnabled: collapseCap > 0, ParentCap: collapseCap, PerParentCounts: map[string]int{"observed_max": row.Quality.MaxPerParentResults}}
	if err := guard.validate(); err != nil {
		return row, err
	}
	return row, nil
}

func applicationDirectScalarFilter(cell applicationCellIdentity) *collections.HybridScalarFilter {
	tenant := collections.HybridScalarFilter{IndexName: "meta_tenant_id", Value: "alpha"}
	if cell.Filter == filterTenantAlpha {
		return &tenant
	}
	workspace := collections.HybridScalarFilter{IndexName: "meta_workspace_id", Value: "red"}
	if cell.Filter == filterTenantAlphaWorkspaceRed {
		return &collections.HybridScalarFilter{And: []collections.HybridScalarFilter{tenant, workspace}}
	}
	if cell.Filter == filterModerateRange {
		year := collections.HybridScalarFilter{
			IndexName: "meta_updated_year",
			Range: &collections.IndexRangeOptions{
				Lower: collections.IndexRangeBound{Value: int64(2024), Inclusive: true},
				Upper: collections.IndexRangeBound{Unbounded: true},
			},
		}
		return &collections.HybridScalarFilter{And: []collections.HybridScalarFilter{tenant, workspace, year}}
	}
	return nil
}

func applicationServiceFilter(cell applicationCellIdentity) *documentservice.Filter {
	tenant := documentservice.Filter{Field: "meta.tenant_id", Operator: "==", Value: "alpha"}
	if cell.Filter == filterTenantAlpha {
		return &tenant
	}
	workspace := documentservice.Filter{Field: "meta.workspace_id", Operator: "==", Value: "red"}
	if cell.Filter == filterTenantAlphaWorkspaceRed {
		return &documentservice.Filter{Operator: "AND", Conditions: []documentservice.Filter{tenant, workspace}}
	}
	if cell.Filter == filterModerateRange {
		year := documentservice.Filter{Field: "meta.updated_year", Operator: ">=", Value: 2024}
		return &documentservice.Filter{Operator: "AND", Conditions: []documentservice.Filter{tenant, workspace, year}}
	}
	return nil
}

func applicationScopeViolations(fixture *applicationFixture, filter string, ids []string) (int, int, int) {
	if filter == filterUnfiltered {
		return 0, 0, 0
	}
	crossTenant, crossWorkspace, crossRange := 0, 0, 0
	for _, id := range ids {
		parentID := id
		if parent, _, ok := chunking.ParseChildID(id); ok {
			parentID = parent
		}
		var source *applicationSource
		for i := range fixture.Sources {
			if fixture.Sources[i].ID == parentID {
				source = &fixture.Sources[i]
				break
			}
		}
		if source == nil {
			crossTenant++
			crossWorkspace++
			crossRange++
			continue
		}
		if source.Tenant != "alpha" {
			crossTenant++
		}
		if (filter == filterTenantAlphaWorkspaceRed || filter == filterModerateRange) && source.Workspace != "red" {
			crossWorkspace++
		}
		if filter == filterModerateRange && source.UpdatedYear < 2024 {
			crossRange++
		}
	}
	return crossTenant, crossWorkspace, crossRange
}
func runApplicationRepetition(cfg applicationConfig, fixture *applicationFixture, cell applicationCellIdentity, rep int, call func(applicationQuery) (queryResult, error)) ([]querySample, repetitionPerformance, map[string]float64, error) {
	order := "forward"
	if rep%2 == 1 {
		order = "reverse"
	}
	jobs := make(chan int)
	results := make(chan struct {
		sample querySample
		result queryResult
		err    error
	}, cfg.SamplesPerRep)
	start := time.Now()
	var workers sync.WaitGroup
	for range cell.Clients {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ordinal := range jobs {
				queryIndex := ordinal % len(fixture.Queries)
				if order == "reverse" {
					queryIndex = len(fixture.Queries) - 1 - queryIndex
				}
				query := fixture.Queries[queryIndex]
				queryStart := time.Now()
				result, err := call(query)
				sample := querySample{Repetition: rep, Ordinal: ordinal, QueryID: query.ID, Millis: time.Since(queryStart).Seconds() * 1000, RequestBytes: result.RequestBytes, ResponseBytes: result.ResponseBytes}
				if err != nil {
					sample.Error = err.Error()
				}
				results <- struct {
					sample querySample
					result queryResult
					err    error
				}{sample, result, err}
			}
		}()
	}
	go func() {
		for ordinal := range cfg.SamplesPerRep {
			jobs <- ordinal
		}
		close(jobs)
		workers.Wait()
		close(results)
	}()
	samples := make([]querySample, 0, cfg.SamplesPerRep)
	counters := map[string]float64{}
	var firstErr error
	for result := range results {
		samples = append(samples, result.sample)
		if firstErr == nil && result.err != nil {
			firstErr = result.err
		}
		for key, value := range result.result.Counters {
			counters[key] += value
		}
	}
	wall := time.Since(start).Seconds()
	sort.Slice(samples, func(i, j int) bool { return samples[i].Ordinal < samples[j].Ordinal })
	qps := 0.0
	if wall > 0 {
		qps = float64(len(samples)) / wall
	}
	perf := repetitionPerformance{Repetition: rep, Order: order, Samples: len(samples), WallSeconds: wall, QPS: qps}
	return samples, perf, counters, firstErr
}

func runDirectQuery(cfg applicationConfig, col *collections.Collection, query applicationQuery, vector []float32, cell applicationCellIdentity, attributionQuery bool) (queryResult, error) {
	opts := collections.HybridSearchOptions{TopK: cfg.TopK}
	opts.MaxChunksPerParent = applicationMaxChunksPerParent(cell)
	opts.ScalarFilter = applicationDirectScalarFilter(cell)
	if cell.Route == "text_only" || cell.Route == "hybrid" {
		opts.Text = &collections.HybridTextQuery{IndexName: "content", Query: query.Text, CandidateLimit: cfg.CandidateLimit}
	}
	if cell.Route == "vector_only" || cell.Route == "hybrid" {
		opts.Vector = &collections.HybridVectorQuery{IndexName: applicationVectorIndex, Query: vector, CandidateLimit: cfg.CandidateLimit, EfSearch: cfg.EfSearch, QueryMode: collections.VectorIndexQueryModeExact}
	}
	switch {
	case cell.Projection == "fetch_topk":
		opts.ResultMode = collections.HybridResultModeFull
		opts.IncludeDocuments = true
		opts.DocumentFetchOptions = collections.DocumentFetchOptions{ExcludePaths: []string{applicationVectorField}}
	case attributionQuery:
		opts.ResultMode = collections.HybridResultModeCompact
	default:
		opts.ResultMode = collections.HybridResultModeScoreOnly
	}
	response, err := col.SearchHybrid(opts)
	result := queryResult{Counters: map[string]float64{}, Sources: map[string][2]bool{}}
	accumulateCounters(result.Counters, response.Stats)
	for _, hit := range response.Results {
		id := string(hit.ID)
		result.IDs = append(result.IDs, id)
		var attribution [2]bool
		for _, source := range hit.Sources {
			if source.Source == collections.HybridCandidateSourceText {
				attribution[0] = true
			}
			if source.Source == collections.HybridCandidateSourceVector {
				attribution[1] = true
			}
		}
		result.Sources[id] = attribution
	}
	return result, err
}

func runHTTPQuery(cfg applicationConfig, env *applicationEnvironment, query applicationQuery, vector []float32, cell applicationCellIdentity) (queryResult, error) {
	var path string
	var request any
	filteredVectorEndpoint := cell.Route == "vector_only" && cell.Filter != filterUnfiltered
	hybridEndpoint := cell.Route == "text_only" || cell.Route == "hybrid" || filteredVectorEndpoint
	if hybridEndpoint {
		hybrid := documentservice.HybridSearchRequest{
			TopK: cfg.TopK, CandidateLimit: cfg.CandidateLimit, EfSearch: cfg.EfSearch,
			MaxChunksPerParent: applicationMaxChunksPerParent(cell), Filter: applicationServiceFilter(cell),
		}
		if cell.Route == "text_only" || cell.Route == "hybrid" {
			hybrid.Query = query.Text
		}
		if cell.Route == "vector_only" || cell.Route == "hybrid" {
			hybrid.QueryEmbedding = vector
		}
		path = "/v1/indexes/" + applicationCollection + "/search/hybrid"
		request = hybrid
	} else if cell.Route == "vector_only" {
		path = "/v1/indexes/" + applicationCollection + "/search/vector"
		request = documentservice.DenseVectorSearchRequest{QueryEmbedding: vector, TopK: cfg.TopK, EfSearch: cfg.EfSearch, Route: documentservice.RouteAnn}
	} else {
		return queryResult{}, fmt.Errorf("unknown route %q", cell.Route)
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return queryResult{}, err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, env.server.URL+path, bytes.NewReader(raw))
	if err != nil {
		return queryResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := env.client.Do(req)
	if err != nil {
		return queryResult{RequestBytes: int64(len(raw))}, err
	}
	payload, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	result := queryResult{RequestBytes: int64(len(raw)), ResponseBytes: int64(len(payload)), Counters: map[string]float64{}, Sources: map[string][2]bool{}}
	if readErr != nil {
		return result, readErr
	}
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("http status %d: %s", resp.StatusCode, payload)
	}
	if hybridEndpoint {
		var parsed documentservice.HybridSearchResponse
		if err := json.Unmarshal(payload, &parsed); err != nil {
			return result, err
		}
		for _, doc := range parsed.Documents {
			result.IDs = append(result.IDs, doc.ID)
			attribution, err := httpHybridAttribution(doc)
			if err != nil {
				return result, err
			}
			result.Sources[doc.ID] = attribution
		}
		accumulateCounters(result.Counters, parsed.Stats)
		return result, nil
	}
	var parsed documentservice.DenseVectorSearchResponse
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return result, err
	}
	for _, doc := range parsed.Documents {
		result.IDs = append(result.IDs, doc.ID)
		result.Sources[doc.ID] = [2]bool{false, true}
	}
	result.Counters["documents_fetched"] = float64(len(parsed.Documents))
	return result, nil
}

func httpHybridAttribution(doc documentservice.Document) ([2]bool, error) {
	var attribution [2]bool
	searchMeta, ok := doc.Meta["_treedb_search"].(map[string]any)
	if !ok {
		return attribution, fmt.Errorf("HTTP hybrid document %q lacks _treedb_search metadata", doc.ID)
	}
	rawSources, ok := searchMeta["sources"].([]any)
	if !ok || len(rawSources) == 0 {
		return attribution, fmt.Errorf("HTTP hybrid document %q lacks source attribution", doc.ID)
	}
	for i, raw := range rawSources {
		source, ok := raw.(map[string]any)
		if !ok {
			return attribution, fmt.Errorf("HTTP hybrid document %q source[%d] has invalid shape", doc.ID, i)
		}
		switch source["source"] {
		case "text":
			attribution[0] = true
		case "vector":
			attribution[1] = true
		default:
			return attribution, fmt.Errorf("HTTP hybrid document %q has unknown source %v", doc.ID, source["source"])
		}
	}
	return attribution, nil
}

func measureApplicationQuality(fixture *applicationFixture, filter string, topK int, call func(applicationQuery) (queryResult, error)) (qualityMetrics, error) {
	var total qualityMetrics
	judgments := applicationJudgmentMap(fixture)
	for _, query := range fixture.Queries {
		result, err := call(query)
		if err != nil {
			return total, err
		}
		crossTenant, crossWorkspace, crossRange := applicationScopeViolations(fixture, filter, result.IDs)
		if crossTenant != 0 || crossWorkspace != 0 || crossRange != 0 {
			return total, fmt.Errorf("quality query %s filter=%s leaked tenant/workspace/range results=%d/%d/%d", query.ID, filter, crossTenant, crossWorkspace, crossRange)
		}
		rankedIDs := result.IDs
		if len(rankedIDs) < topK {
			scalarBounded := result.Counters["scalar_filter_lookups"] > 0 && result.Counters["scalar_filter_final_ids"] <= float64(topK)
			if result.Counters["collapse_exhaustions"] == 0 && !scalarBounded {
				return total, fmt.Errorf("quality query %s ranking depth=%d below top_k=%d without bounded filter/collapse exhaustion", query.ID, len(rankedIDs), topK)
			}
			rankedIDs = append([]string(nil), rankedIDs...)
			for len(rankedIDs) < topK {
				rankedIDs = append(rankedIDs, fmt.Sprintf("\x00bounded-empty-rank-%d", len(rankedIDs)))
			}
		}
		judgment := judgments[query.ID+"\x00"+filter]
		chunkRelevant := stringSet(judgment.RelevantChunks)
		parentRelevant := stringSet(judgment.RelevantParents)
		p5, err := precisionAtK(rankedIDs, chunkRelevant, 5)
		if err != nil {
			return total, err
		}
		p10, err := precisionAtK(rankedIDs, chunkRelevant, 10)
		if err != nil {
			return total, err
		}
		r5, _ := recallAtK(rankedIDs, chunkRelevant, 5)
		r10, _ := recallAtK(rankedIDs, chunkRelevant, 10)
		mrr, _ := mrrAtK(rankedIDs, chunkRelevant, 10)
		parents := make([]string, 0, len(result.IDs))
		perParent := map[string]int{}
		for _, id := range result.IDs {
			parent, _, ok := chunking.ParseChildID(id)
			if !ok {
				return total, fmt.Errorf("quality result %q is not a valid chunk ID", id)
			}
			parents = append(parents, parent)
			perParent[parent]++
			if perParent[parent] > total.MaxPerParentResults {
				total.MaxPerParentResults = perParent[parent]
			}
			attribution := result.Sources[id]
			if attribution[0] {
				total.TextAttributedResults++
			}
			if attribution[1] {
				total.VectorAttributedResults++
			}
			if attribution[0] && attribution[1] {
				total.TextVectorOverlapResults++
			}
		}
		parentR5 := parentRecallAtK(parents, parentRelevant, 5)
		parentR10 := parentRecallAtK(parents, parentRelevant, 10)
		total.PrecisionAt5 += p5
		total.PrecisionAt10 += p10
		total.NDCGAt5 += ndcgAtK(rankedIDs, chunkRelevant, 5)
		total.NDCGAt10 += ndcgAtK(rankedIDs, chunkRelevant, 10)
		total.MRRAt10 += mrr
		if mrr > 0 {
			total.HitRateAt10++
		}
		total.ChunkRecallAt5 += r5
		total.ChunkRecallAt10 += r10
		total.ParentRecallAt5 += parentR5
		total.ParentRecallAt10 += parentR10
		total.RelevantChunksMean += float64(len(chunkRelevant))
		total.RelevantParentsMean += float64(len(parentRelevant))
		total.MaxAchievableChunkRecallAt5 += maxAchievableRecall(len(chunkRelevant), 5)
		total.MaxAchievableChunkRecallAt10 += maxAchievableRecall(len(chunkRelevant), 10)
		total.MaxAchievableParentRecallAt5 += maxAchievableRecall(len(parentRelevant), 5)
		total.MaxAchievableParentRecallAt10 += maxAchievableRecall(len(parentRelevant), 10)
	}
	n := float64(len(fixture.Queries))
	total.PrecisionAt5 /= n
	total.PrecisionAt10 /= n
	total.NDCGAt5 /= n
	total.NDCGAt10 /= n
	total.MRRAt10 /= n
	total.HitRateAt10 /= n
	total.ChunkRecallAt5 /= n
	total.ChunkRecallAt10 /= n
	total.ParentRecallAt5 /= n
	total.ParentRecallAt10 /= n
	total.RelevantChunksMean /= n
	total.RelevantParentsMean /= n
	total.MaxAchievableChunkRecallAt5 /= n
	total.MaxAchievableChunkRecallAt10 /= n
	total.MaxAchievableParentRecallAt5 /= n
	total.MaxAchievableParentRecallAt10 /= n
	return total, nil
}

func precisionAtK(ranked []string, relevant map[string]bool, k int) (float64, error) {
	if len(ranked) < k || len(relevant) == 0 || k <= 0 {
		return 0, fmt.Errorf("precision@%d: invalid ranking depth=%d relevant=%d", k, len(ranked), len(relevant))
	}
	hits := 0
	for _, id := range ranked[:k] {
		if relevant[id] {
			hits++
		}
	}
	return float64(hits) / float64(k), nil
}

func ndcgAtK(ranked []string, relevant map[string]bool, k int) float64 {
	dcg := 0.0
	for i, id := range ranked[:k] {
		if relevant[id] {
			dcg += 1 / math.Log2(float64(i+2))
		}
	}
	ideal := 0.0
	limit := min(k, len(relevant))
	for i := range limit {
		ideal += 1 / math.Log2(float64(i+2))
	}
	if ideal == 0 {
		return 0
	}
	return dcg / ideal
}

func parentRecallAtK(parents []string, relevant map[string]bool, k int) float64 {
	seen := map[string]bool{}
	hits := 0
	for _, parent := range parents[:min(k, len(parents))] {
		if relevant[parent] && !seen[parent] {
			seen[parent] = true
			hits++
		}
	}
	return float64(hits) / float64(len(relevant))
}

func maxAchievableRecall(relevant, k int) float64 {
	return float64(min(relevant, k)) / float64(relevant)
}

func applicationJudgmentMap(fixture *applicationFixture) map[string]applicationJudgment {
	out := make(map[string]applicationJudgment, len(fixture.Judgments))
	for _, judgment := range fixture.Judgments {
		out[judgment.QueryID+"\x00"+judgment.Filter] = judgment
	}
	return out
}

func stringSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func buildExactControl(fixture *applicationFixture, bundle *semanticVectorBundle, embeddingCell, provider string, dims, topK int) (exactControlRow, error) {
	queryVectors, err := applicationQueryVectors(fixture, bundle, embeddingCell, provider, dims)
	if err != nil {
		return exactControlRow{}, err
	}
	type corpusVector struct {
		id     string
		vector []float32
	}
	vectors := make([]corpusVector, 0)
	var embedder embedding.Embedder
	if embeddingCell == "hashing_regression" {
		embedder, err = embedding.DefaultRegistry().Create(embedding.Config{Provider: provider, Dimensions: dims})
		if err != nil {
			return exactControlRow{}, err
		}
	}
	for _, source := range fixture.Sources {
		if source.Deleted {
			continue
		}
		for ordinal := range applicationChunksPerSource {
			text := source.FinalBody[ordinal*applicationChunkSize : (ordinal+1)*applicationChunkSize]
			var vector []float32
			if embeddingCell == "semantic_minilm" {
				vector = bundle.Vectors[text]
			} else {
				embedded, embedErr := embedder.EmbedBatch(context.Background(), [][]byte{[]byte(text)})
				if embedErr != nil {
					return exactControlRow{}, embedErr
				}
				vector = embedded[0]
			}
			vectors = append(vectors, corpusVector{id: chunking.ChildDocumentID(source.ID, ordinal), vector: vector})
		}
	}
	results := make(map[string][]string, len(fixture.Queries))
	for _, query := range fixture.Queries {
		scores := make([]struct {
			id    string
			score float64
		}, len(vectors))
		for i, item := range vectors {
			scores[i] = struct {
				id    string
				score float64
			}{item.id, cosine(queryVectors[query.ID], item.vector)}
		}
		sort.Slice(scores, func(i, j int) bool {
			if scores[i].score == scores[j].score {
				return scores[i].id < scores[j].id
			}
			return scores[i].score > scores[j].score
		})
		for _, score := range scores[:topK] {
			results[query.ID] = append(results[query.ID], score.id)
		}
	}
	quality, err := measureApplicationQuality(fixture, filterUnfiltered, topK, func(query applicationQuery) (queryResult, error) {
		return queryResult{IDs: results[query.ID], Sources: map[string][2]bool{}}, nil
	})
	return exactControlRow{Embedding: embeddingCell, Quality: quality, TopK: topK, CorpusVectors: len(vectors), Method: "offline exhaustive cosine over hash-bound final vectors; excluded from product QPS and fallback counters"}, err
}

func cosine(a, b []float32) float64 {
	var dot, an, bn float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		an += x * x
		bn += y * y
	}
	return dot / math.Sqrt(an*bn)
}

func runApplicationIngestionRep(cfg applicationConfig, fixture *applicationFixture, dir string, rep int) (ingestionRepetition, error) {
	row := ingestionRepetition{Repetition: rep, FreshDB: true}
	if err := os.RemoveAll(dir); err != nil {
		return row, err
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	db, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		return row, err
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(applicationSourceCollectionMeta(cfg, 64)); err != nil {
		_ = db.Close()
		return row, err
	}
	col, err := manager.OpenCollection(applicationSourceCollection)
	if err != nil {
		_ = db.Close()
		return row, err
	}
	sources := applicationSourceDocuments(fixture, true)
	row.SourceDocs = len(sources)
	endToEndStart := time.Now()
	ingestStart := time.Now()
	result, err := col.IngestSources(context.Background(), sources, applicationIngestConfig(embedding.ProviderHashing, 64))
	row.IngestSourcesSeconds = time.Since(ingestStart).Seconds()
	if err != nil {
		row.Failure = err.Error()
		_ = db.Close()
		return row, err
	}
	row.ChunkDocs = countIngestedChunks(result)
	publishStart := time.Now()
	if _, err := col.RebuildVectorIndex(applicationVectorIndex); err != nil {
		_ = db.Close()
		return row, fmt.Errorf("rebuild source vector index: %w", err)
	}
	publishSeconds := time.Since(publishStart).Seconds()
	checkpointStart := time.Now()
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		return row, err
	}
	row.CheckpointSeconds = time.Since(checkpointStart).Seconds()
	row.EndToEndSeconds = time.Since(endToEndStart).Seconds()
	runtime.ReadMemStats(&after)
	row.SourceDocsPerSec = float64(row.SourceDocs) / row.EndToEndSeconds
	row.ChunkDocsPerSec = float64(row.ChunkDocs) / row.EndToEndSeconds
	row.ChunkStageShare = float64(result.ChunkNanos) / 1e9 / row.EndToEndSeconds
	row.EmbedStageShare = float64(result.EmbedNanos) / 1e9 / row.EndToEndSeconds
	row.IndexMutationStageShare = float64(result.IndexNanos) / 1e9 / row.EndToEndSeconds
	row.IndexPublicationShare = publishSeconds / row.EndToEndSeconds
	row.CheckpointShare = row.CheckpointSeconds / row.EndToEndSeconds
	row.BytesPerOp = float64(after.TotalAlloc-before.TotalAlloc) / float64(row.SourceDocs)
	row.AllocsPerOp = float64(after.Mallocs-before.Mallocs) / float64(row.SourceDocs)
	row.StorageBytes, err = dirSize(dir)
	if err != nil {
		_ = db.Close()
		return row, err
	}
	row.BytesPerSource = float64(row.StorageBytes) / float64(row.SourceDocs)
	row.BytesPerChunk = float64(row.StorageBytes) / float64(row.ChunkDocs)
	row.StorageCounters = relevantStorageStats(db.Stats())
	beforeDigest, _, _, err := sourceChildSnapshot(col, fixture, nil, "hashing", embedding.ProviderHashing, 64)
	if err != nil {
		_ = db.Close()
		return row, err
	}
	if err := db.Close(); err != nil {
		return row, err
	}
	reopenStart := time.Now()
	db, err = backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	row.ReopenSeconds = time.Since(reopenStart).Seconds()
	if err != nil {
		return row, err
	}
	manager = collections.NewCollectionManager(db)
	col, err = manager.OpenCollection(applicationSourceCollection)
	if err != nil {
		_ = db.Close()
		return row, err
	}
	afterDigest, _, _, err := sourceChildSnapshot(col, fixture, nil, "hashing", embedding.ProviderHashing, 64)
	row.ReopenParity = err == nil && beforeDigest == afterDigest
	_ = db.Close()
	if err != nil {
		return row, err
	}
	if !row.ReopenParity {
		return row, fmt.Errorf("source child snapshot changed across reopen")
	}
	return row, nil
}

func summarizeIngestion(rows []ingestionRepetition) ingestionSummary {
	docs, bytesPerOp := make([]float64, 0, len(rows)), make([]float64, 0, len(rows))
	for _, row := range rows {
		docs = append(docs, row.SourceDocsPerSec)
		bytesPerOp = append(bytesPerOp, row.BytesPerOp)
	}
	medianDocs, _ := percentile(docs, 50)
	p95Docs, _ := percentile(docs, 95)
	medianBytes, _ := percentile(bytesPerOp, 50)
	p95Bytes, _ := percentile(bytesPerOp, 95)
	return ingestionSummary{Repetitions: len(rows), MedianDocsPerSec: medianDocs, P95DocsPerSec: p95Docs, MedianBytesPerOp: medianBytes, P95BytesPerOp: p95Bytes, HistoricalReproduced: medianDocs <= 75 || medianBytes >= 66*(1<<30)}
}

func freezeApplicationGate(summary ingestionSummary) frozenGate {
	gate := frozenGate{CrossTenantResults: 0, CrossWorkspaceResults: 0, FullDocumentScanFallbacks: 0, MinTimedQueriesPerCell: 1000, MinRepetitionsPerCell: 3, MaxUnaffectedQPSRegression: 10}
	if summary.HistoricalReproduced {
		gate.CandidateMinDocsPerSec = summary.MedianDocsPerSec * 2
		gate.CandidateMaxBytesPerOp = summary.MedianBytesPerOp * 0.5
		gate.Rationale = "historical 37.59 docs/s or 132 GiB/op regime reproduced; apply #4284 default >=2x throughput and >=50% lower B/op"
	} else {
		gate.CandidateMinDocsPerSec = summary.MedianDocsPerSec * 1.15
		gate.CandidateMaxBytesPerOp = summary.MedianBytesPerOp * 0.90
		gate.Rationale = "historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction"
	}
	gate.NoisePolicy = "fresh DB; five repetitions; median is decision statistic; p95 disclosed; >10% unaffected QPS or p99 regression blocks; quality/work/projection digests must match"
	return gate
}

func validateApplicationReport(report *applicationReport, cfg applicationConfig) error {
	if report == nil || report.Schema != applicationReportSchema {
		return fmt.Errorf("report: missing schema")
	}
	if got, want := report.Provenance.ConfigSHA256, applicationConfigDigest(cfg); got != want {
		return fmt.Errorf("report: config SHA-256=%q want workload digest %q", got, want)
	}
	if len(report.Lifecycle) != len(applicationEmbeddings) {
		return fmt.Errorf("report: lifecycle rows=%d want %d", len(report.Lifecycle), len(applicationEmbeddings))
	}
	for name, lifecycle := range report.Lifecycle {
		if err := validateLifecycleEvidence(name, lifecycle); err != nil {
			return err
		}
	}
	if cfg.FinalEvidence {
		settings, ok := runtimeBuildInfo()
		revision, err := resolveApplicationHarnessRevision(cfg, settings, ok)
		if err != nil {
			return err
		}
		if report.Provenance.HarnessRevision != revision {
			return fmt.Errorf("report: harness revision %q does not equal binary vcs.revision %q", report.Provenance.HarnessRevision, revision)
		}
	}
	if cfg.FinalEvidence && len(report.IngestionRuns) < 5 {
		return fmt.Errorf("report: final evidence has %d ingestion repetitions, want >=5", len(report.IngestionRuns))
	}
	wantRows := len(applicationEmbeddings) * len(applicationRoutes) * len(applicationProjections) * len(applicationFilterOrder) * 2 * len(applicationSurfaces) * len(applicationClients)
	if len(report.Rows) != wantRows {
		return fmt.Errorf("report: rows=%d want %d", len(report.Rows), wantRows)
	}
	supported := 0
	for _, row := range report.Rows {
		if row.Status == "unsupported" {
			if row.Capability == nil || row.Capability.ErrorType != capabilityErrorType || !row.Capability.FailClosed || row.Capability.ResultsReturned != 0 {
				return fmt.Errorf("report: unsupported row lacks exact typed fail-closed evidence: %+v", row.Cell)
			}
			continue
		}
		supported++
		if cfg.FinalEvidence {
			if err := (measurementClaim{Samples: len(row.Samples), Repetitions: len(row.Repetitions), Label: "final_qps_p99"}).validate(); err != nil {
				return err
			}
		}
		if row.Errors != 0 || row.Counters["full_document_scan_fallbacks"] != 0 {
			return fmt.Errorf("report: supported row errors/full scans %+v", row.Cell)
		}
		if row.Cell.Projection == "score_only" && row.Counters["documents_fetched"] != 0 {
			return fmt.Errorf("report: score-only row fetched documents %+v", row.Cell)
		}
		if row.Cell.Projection == "fetch_topk" && row.Counters["documents_fetched"] > float64(cfg.TopK) {
			return fmt.Errorf("report: fetch row exceeded topK %+v", row.Cell)
		}
		if row.Quality.AttributionMode == "" {
			return fmt.Errorf("report: supported row lacks quality attribution semantics %+v", row.Cell)
		}
		if row.Cell.Surface == "direct_collection" && row.Cell.Projection == "score_only" {
			if row.Quality.AttributionMode != "untimed_compact_same_work_route_filter" {
				return fmt.Errorf("report: direct score-only attribution mode=%q %+v", row.Quality.AttributionMode, row.Cell)
			}
			if row.Cell.Route != "vector_only" && row.Quality.TextAttributedResults == 0 {
				return fmt.Errorf("report: direct score-only text attribution unavailable %+v", row.Cell)
			}
			if row.Cell.Route != "text_only" && row.Quality.VectorAttributedResults == 0 {
				return fmt.Errorf("report: direct score-only vector attribution unavailable %+v", row.Cell)
			}
		}
	}
	if supported == 0 || len(report.ExactControls) != len(applicationEmbeddings) {
		return fmt.Errorf("report: no supported rows or exact controls")
	}
	return nil
}

func relevantStorageStats(all map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range all {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "root") || strings.Contains(lower, "publish") || strings.Contains(lower, "reach") || strings.Contains(lower, "wal") || strings.Contains(lower, "page") {
			out[key] = value
		}
	}
	return out
}

func stableStringMap(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&b, "%s=%s;", key, values[key])
	}
	return b.String()
}

func runtimeCPUSeconds() float64 {
	samples := []runtimemetrics.Sample{{Name: "/cpu/classes/total:cpu-seconds"}}
	runtimemetrics.Read(samples)
	if samples[0].Value.Kind() != runtimemetrics.KindFloat64 {
		return 0
	}
	return samples[0].Value.Float64()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func buildApplicationProvenance(cfg applicationConfig, fixture *applicationFixture, bundle *semanticVectorBundle) (applicationProvenance, error) {
	hostname, _ := os.Hostname()
	binaryHash := "unavailable"
	if executable, err := os.Executable(); err == nil {
		if raw, readErr := os.ReadFile(executable); readErr == nil {
			sum := sha256.Sum256(raw)
			binaryHash = hex.EncodeToString(sum[:])
		}
	}
	if cfg.FinalEvidence && binaryHash == "unavailable" {
		return applicationProvenance{}, fmt.Errorf("provenance: final evidence binary SHA-256 unavailable")
	}
	hashingSum := sha256.Sum256([]byte("embedding.ProviderHashing|dims=64|fixture=" + applicationFixtureDigest(fixture)))
	info, buildInfoOK := runtimeBuildInfo()
	revision, err := resolveApplicationHarnessRevision(cfg, info, buildInfoOK)
	if err != nil {
		return applicationProvenance{}, err
	}
	cgo := "unknown"
	if buildInfoOK {
		cgo = info["CGO_ENABLED"]
	}
	if len(cfg.Command) == 0 {
		cfg.Command = append([]string(nil), os.Args...)
	}
	return applicationProvenance{
		ProductBaseSHA: cfg.ProductBaseSHA, HarnessRevision: revision, BinarySHA256: binaryHash,
		FixtureSHA256: applicationFixtureDigest(fixture), ConfigSHA256: applicationConfigDigest(cfg),
		SemanticVectorSHA256: bundle.Digest(), HashingRegressionSHA256: hex.EncodeToString(hashingSum[:]),
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, CGOEnabled: cgo,
		Hostname: hostname, HostNote: cfg.HostNote, Command: cfg.Command,
		RepetitionOrder: "query reps forward/reverse/forward; ingestion fresh DB reps 0..4; final candidate must use paired ABBA interleave",
		Environment: map[string]string{
			"GOROOT": os.Getenv("GOROOT"), "runtime_goroot": runtime.GOROOT(), "GOMAXPROCS": fmt.Sprint(runtime.GOMAXPROCS(0)),
			"database_root":     cfg.Dir,
			"resource_teardown": "every DB, document service, and HTTP server is closed; an explicit database root is retained for inspection",
		},
	}, nil
}

func runtimeBuildInfo() (map[string]string, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil, false
	}
	settings := map[string]string{}
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	return settings, true
}
