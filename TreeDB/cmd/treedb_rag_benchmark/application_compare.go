package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

const (
	qdrantComparisonArtifactSchema = "treedb-rag-qdrant-comparison/v1"
	applicationComparisonSchema    = "treedb-rag-system-comparison/v1"
)

type qdrantComparisonServer struct {
	Version    string         `json:"version"`
	Deployment string         `json:"deployment"`
	Image      string         `json:"image"`
	Identity   string         `json:"identity"`
	LocalMode  bool           `json:"local_mode"`
	Config     map[string]any `json:"config"`
}

type qdrantComparisonResources struct {
	HostPIDMetrics       string           `json:"host_pid_metrics"`
	DockerStats          map[string]any   `json:"docker_stats,omitempty"`
	ProcessSamples       []map[string]any `json:"process_samples,omitempty"`
	PeakObservedRSSBytes int64            `json:"peak_observed_rss_bytes"`
	CPUSeconds           float64          `json:"cpu_seconds"`
	DurableBytes         int64            `json:"durable_bytes"`
}

type qdrantComparisonReopen struct {
	Attempted  bool   `json:"attempted"`
	Succeeded  bool   `json:"succeeded"`
	Version    string `json:"version"`
	PointCount int    `json:"point_count"`
}

type qdrantComparisonRouteProof struct {
	API              string `json:"api"`
	NamedVector      string `json:"named_vector"`
	Fusion           string `json:"fusion,omitempty"`
	Fallbacks        int    `json:"fallbacks"`
	ExhaustiveSearch bool   `json:"exhaustive_search"`
	BoundedFetch     bool   `json:"bounded_fetch"`
}

type qdrantComparisonSummary struct {
	QPS          float64 `json:"qps"`
	LatencyMSP50 float64 `json:"latency_ms_p50"`
	LatencyMSP95 float64 `json:"latency_ms_p95"`
	LatencyMSP99 float64 `json:"latency_ms_p99"`
}

type qdrantComparisonCell struct {
	Route         string                     `json:"route"`
	Filter        string                     `json:"filter"`
	Equivalence   string                     `json:"equivalence"`
	Warmups       int                        `json:"warmups"`
	Repetitions   int                        `json:"repetitions"`
	Samples       []map[string]any           `json:"samples"`
	Summary       qdrantComparisonSummary    `json:"summary"`
	Quality       qualityMetrics             `json:"quality"`
	Leakage       int                        `json:"leakage"`
	Errors        int                        `json:"errors"`
	Timeouts      int                        `json:"timeouts"`
	FetchMaxCount int                        `json:"fetch_max_count"`
	RouteProof    qdrantComparisonRouteProof `json:"route_proof"`
}

type qdrantComparisonArtifact struct {
	Schema               string                    `json:"schema"`
	Backend              string                    `json:"backend"`
	ManifestSHA256       string                    `json:"manifest_sha256"`
	FixtureSHA256        string                    `json:"fixture_sha256"`
	SemanticVectorSHA256 string                    `json:"semantic_vector_sha256"`
	ConfigSHA256         string                    `json:"config_sha256"`
	SourceCount          int                       `json:"source_count"`
	ChunkCount           int                       `json:"chunk_count"`
	QueryCount           int                       `json:"query_count"`
	Server               qdrantComparisonServer    `json:"server"`
	Resources            qdrantComparisonResources `json:"resources"`
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
	Schema               string                     `json:"schema"`
	State                string                     `json:"state"`
	ManifestSHA256       string                     `json:"manifest_sha256"`
	FixtureSHA256        string                     `json:"fixture_sha256"`
	SemanticVectorSHA256 string                     `json:"semantic_vector_sha256"`
	ConfigSHA256         string                     `json:"config_sha256"`
	Rows                 []applicationComparisonRow `json:"rows"`
	Dispositions         []string                   `json:"dispositions"`
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

func selectedTreeDBComparisonRows(report *applicationReport, manifest applicationComparisonManifest) ([]applicationComparisonRow, error) {
	if report.Schema != applicationReportSchema || report.Provenance.FixtureSHA256 != manifest.FixtureSHA256 ||
		report.Provenance.SemanticVectorSHA256 != manifest.SemanticVectorSHA256 || applicationFixtureDigest(&report.Fixture) != manifest.FixtureSHA256 ||
		report.SemanticVectors.Digest() != manifest.SemanticVectorSHA256 {
		return nil, fmt.Errorf("TreeDB artifact manifest/fixture/vector binding mismatch")
	}
	if len(report.Failures) != 0 || !report.Lifecycle["semantic_minilm"].ColdReopenParity {
		return nil, fmt.Errorf("TreeDB artifact has failures or missing semantic reopen parity")
	}
	routeNames := map[string]string{"text_only": "lexical", "vector_only": "dense", "hybrid": "hybrid"}
	wantedFilters := map[string]bool{}
	for _, filter := range manifest.Filters {
		wantedFilters[filter.ID] = true
	}
	seen := map[string]bool{}
	rows := make([]applicationComparisonRow, 0, 12)
	for _, row := range report.Rows {
		route, ok := routeNames[row.Cell.Route]
		if !ok || !wantedFilters[row.Cell.Filter] || row.Cell.Projection != "fetch_topk" || row.Cell.Collapse != "disabled" ||
			row.Cell.Surface != "direct_collection" || row.Cell.Embedding != "semantic_minilm" || row.Cell.Clients != 1 {
			continue
		}
		key := route + "\x00" + row.Cell.Filter
		if seen[key] {
			return nil, fmt.Errorf("TreeDB artifact has duplicate comparison cell %s/%s", route, row.Cell.Filter)
		}
		seen[key] = true
		if row.Status != "supported" || row.Errors != 0 || row.Counters["full_document_scan_fallbacks"] != 0 ||
			row.Counters["cross_tenant_results"] != 0 || row.Counters["cross_workspace_results"] != 0 ||
			row.Counters["documents_fetched"] > float64(manifest.Config.TopK) || len(row.Repetitions) < manifest.Config.Repetitions ||
			len(row.Samples) < manifest.Config.SamplesPerCell*manifest.Config.Repetitions {
			return nil, fmt.Errorf("TreeDB artifact cell %s/%s is partial, leaking, unbounded, failed, or fell back", route, row.Cell.Filter)
		}
		equivalence := "direct"
		if route != "dense" {
			equivalence = "directional"
		}
		rows = append(rows, applicationComparisonRow{Backend: "treedb", Route: route, Filter: row.Cell.Filter,
			Equivalence: equivalence, Samples: len(row.Samples), Repetitions: len(row.Repetitions), QPS: row.QPSMean,
			LatencyMSP50: row.LatencyMSP50, LatencyMSP95: row.LatencyMSP95, LatencyMSP99: row.LatencyMSP99, Quality: row.Quality})
	}
	if len(rows) != 12 {
		return nil, fmt.Errorf("TreeDB artifact comparison cells=%d want 12", len(rows))
	}
	return rows, nil
}

func validateQdrantComparisonArtifact(artifact *qdrantComparisonArtifact, manifest applicationComparisonManifest, manifestSHA string) error {
	if artifact.Schema != qdrantComparisonArtifactSchema || artifact.Backend != "qdrant_server" || artifact.ManifestSHA256 != manifestSHA ||
		artifact.FixtureSHA256 != manifest.FixtureSHA256 || artifact.SemanticVectorSHA256 != manifest.SemanticVectorSHA256 ||
		artifact.ConfigSHA256 != manifest.ConfigSHA256 || artifact.SourceCount != len(manifest.Sources) ||
		artifact.ChunkCount != len(manifest.Chunks) || artifact.QueryCount != len(manifest.Queries) {
		return fmt.Errorf("Qdrant artifact manifest/hash/cardinality binding mismatch")
	}
	if artifact.Server.Version != "1.19.0" || artifact.Server.LocalMode || artifact.Server.Identity == "" ||
		(artifact.Server.Deployment != "docker" && artifact.Server.Deployment != "standalone") {
		return fmt.Errorf("Qdrant artifact lacks pinned external server identity")
	}
	if artifact.Server.Deployment == "docker" && !strings.Contains(artifact.Server.Image, "sha256:057ee3a8da769fe7310dd3537b4dc7583bf87a95ce8ac43c0af5a46bc580d1fc") {
		return fmt.Errorf("Qdrant artifact image is not digest pinned")
	}
	if len(artifact.Failures) != 0 || !artifact.Reopen.Attempted || !artifact.Reopen.Succeeded || artifact.Reopen.Version != "1.19.0" || artifact.Reopen.PointCount != len(manifest.Chunks) {
		return fmt.Errorf("Qdrant artifact failed or lacks successful durable reopen")
	}
	if artifact.Resources.DurableBytes <= 0 || artifact.Resources.HostPIDMetrics == "" {
		return fmt.Errorf("Qdrant artifact lacks resource/storage semantics")
	}
	if artifact.Server.Deployment == "docker" && len(artifact.Resources.DockerStats) == 0 {
		return fmt.Errorf("Qdrant Docker artifact lacks container resource evidence")
	}
	if artifact.Server.Deployment == "standalone" && (len(artifact.Resources.ProcessSamples) < 4 || artifact.Resources.PeakObservedRSSBytes <= 0 || artifact.Resources.CPUSeconds <= 0) {
		return fmt.Errorf("Qdrant standalone artifact lacks process RSS/CPU evidence")
	}
	seen := map[string]bool{}
	for _, cell := range artifact.Cells {
		key := cell.Route + "\x00" + cell.Filter
		if seen[key] {
			return fmt.Errorf("Qdrant artifact duplicate cell %s/%s", cell.Route, cell.Filter)
		}
		seen[key] = true
		if !containsString([]string{"lexical", "dense", "hybrid"}, cell.Route) || !containsString(applicationFilterOrder, cell.Filter) ||
			cell.Warmups != manifest.Config.WarmupsPerCell*manifest.Config.Repetitions || cell.Repetitions != manifest.Config.Repetitions ||
			len(cell.Samples) != manifest.Config.SamplesPerCell*manifest.Config.Repetitions || cell.Errors != 0 || cell.Timeouts != 0 ||
			cell.Leakage != 0 || cell.FetchMaxCount > manifest.Config.TopK || cell.RouteProof.Fallbacks != 0 ||
			cell.RouteProof.ExhaustiveSearch || !cell.RouteProof.BoundedFetch || cell.RouteProof.API != "qdrant.query_points" {
			return fmt.Errorf("Qdrant artifact cell %s/%s is partial, leaking, unbounded, failed, exhaustive, or fell back", cell.Route, cell.Filter)
		}
		if cell.Route == "hybrid" && cell.RouteProof.Fusion != "rrf" {
			return fmt.Errorf("Qdrant hybrid cell %s lacks Query API RRF proof", cell.Filter)
		}
		if cell.Route != "dense" && cell.Equivalence != "directional" {
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
	var tree applicationReport
	if _, err := readJSONFile(treePath, &tree); err != nil {
		return err
	}
	treeRows, err := selectedTreeDBComparisonRows(&tree, manifest)
	if err != nil {
		return err
	}
	var qdrant qdrantComparisonArtifact
	if _, err := readJSONFile(qdrantPath, &qdrant); err != nil {
		return err
	}
	if err := validateQdrantComparisonArtifact(&qdrant, manifest, manifestSHA); err != nil {
		return err
	}
	report := applicationComparisonReport{Schema: applicationComparisonSchema, State: "validated",
		ManifestSHA256: manifestSHA, FixtureSHA256: manifest.FixtureSHA256,
		SemanticVectorSHA256: manifest.SemanticVectorSHA256, ConfigSHA256: manifest.ConfigSHA256,
		Rows: treeRows, Dispositions: []string{
			"TreeDB-versus-Qdrant lexical and hybrid rows are directional because Qdrant uses client-generated sparse BM25 and native Query API RRF, not exact TreeDB BM25F scoring/fusion parity.",
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
	fmt.Fprintf(&md, "# TreeDB / Qdrant bounded RAG comparison\n\nState: **validated**  \nManifest SHA-256: `%s`\n\n", manifestSHA)
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
