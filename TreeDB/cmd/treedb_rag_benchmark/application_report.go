package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type artifactFileHash struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int    `json:"bytes"`
}

type applicationArtifactManifest struct {
	Schema               string             `json:"schema"`
	ProductBaseSHA       string             `json:"product_base_sha"`
	HarnessRevision      string             `json:"harness_revision"`
	FixtureSHA256        string             `json:"fixture_sha256"`
	ConfigSHA256         string             `json:"config_sha256"`
	SemanticVectorSHA256 string             `json:"semantic_vector_sha256"`
	BinarySHA256         string             `json:"binary_sha256"`
	Artifacts            []artifactFileHash `json:"artifacts"`
}

func writeApplicationArtifacts(report *applicationReport, outDir string) (jsonPath, markdownPath, manifestPath string, err error) {
	if report == nil {
		return "", "", "", fmt.Errorf("report: nil application report")
	}
	for name, lifecycle := range report.Lifecycle {
		if err := validateLifecycleEvidence(name, lifecycle); err != nil {
			return "", "", "", err
		}
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", "", err
	}
	jsonPath = filepath.Join(outDir, "treedb_rag_application_baseline.json")
	markdownPath = filepath.Join(outDir, "treedb_rag_application_baseline.md")
	manifestPath = filepath.Join(outDir, "treedb_rag_application_manifest.json")
	jsonBytes, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", "", "", err
	}
	jsonBytes = append(jsonBytes, '\n')
	markdownBytes := renderApplicationMarkdown(report)
	if err := os.WriteFile(jsonPath, jsonBytes, 0o644); err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(markdownPath, markdownBytes, 0o644); err != nil {
		return "", "", "", err
	}
	manifest := applicationArtifactManifest{
		Schema:         "treedb_rag_application_manifest/v1",
		ProductBaseSHA: report.Provenance.ProductBaseSHA, HarnessRevision: report.Provenance.HarnessRevision,
		FixtureSHA256: report.Provenance.FixtureSHA256, ConfigSHA256: report.Provenance.ConfigSHA256,
		SemanticVectorSHA256: report.Provenance.SemanticVectorSHA256, BinarySHA256: report.Provenance.BinarySHA256,
		Artifacts: []artifactFileHash{
			artifactHash(filepath.Base(jsonPath), jsonBytes), artifactHash(filepath.Base(markdownPath), markdownBytes),
		},
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", "", "", err
	}
	manifestBytes = append(manifestBytes, '\n')
	if err := os.WriteFile(manifestPath, manifestBytes, 0o644); err != nil {
		return "", "", "", err
	}
	return jsonPath, markdownPath, manifestPath, nil
}

func artifactHash(path string, raw []byte) artifactFileHash {
	sum := sha256.Sum256(raw)
	return artifactFileHash{Path: path, SHA256: hex.EncodeToString(sum[:]), Bytes: len(raw)}
}

func renderApplicationMarkdown(report *applicationReport) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "# TreeDB retained RAG application baseline (#4289)\n\n")
	fmt.Fprintf(&b, "Authority: `%s`; schema: `%s`. This is the repaired M1 application baseline, not the historical C1 claim.\n\n", report.Authority, report.Schema)
	fmt.Fprintf(&b, "## Exact bindings\n\n")
	fmt.Fprintf(&b, "- product base: `%s`\n- harness revision: `%s`\n- binary SHA-256: `%s`\n- fixture SHA-256: `%s`\n- config SHA-256: `%s`\n- semantic vectors SHA-256: `%s`\n- hashing regression SHA-256: `%s`\n- Go/host: `%s` `%s/%s` `%s`\n- command: `%s`\n\n",
		report.Provenance.ProductBaseSHA, report.Provenance.HarnessRevision, report.Provenance.BinarySHA256,
		report.Provenance.FixtureSHA256, report.Provenance.ConfigSHA256, report.Provenance.SemanticVectorSHA256,
		report.Provenance.HashingRegressionSHA256, report.Provenance.GoVersion, report.Provenance.GOOS,
		report.Provenance.GOARCH, report.Provenance.Hostname, strings.Join(report.Provenance.Command, " "))
	fmt.Fprintf(&b, "## Independent semantic evidence\n\n- model: `%s`\n- revision: `%s`\n- license: `%s`\n- dimensions: `%d`\n- preprocessing: %s\n- corpus license: %s\n- generation: `%s`\n\n",
		report.SemanticVectors.Model, report.SemanticVectors.Revision, report.SemanticVectors.License,
		report.SemanticVectors.Dimensions, report.SemanticVectors.Preprocessing,
		report.SemanticVectors.CorpusLicense, report.SemanticVectors.GenerationCommand)
	fmt.Fprintf(&b, "## Actual source ingestion (`IngestSources`)\n\n")
	fmt.Fprintf(&b, "Five fresh-DB rows include embedding, index publication, and checkpoint in end-to-end source docs/s.\n\n")
	fmt.Fprintf(&b, "| rep | sources | chunks | end-to-end s | source docs/s | chunk docs/s | B/source | allocs/source | storage bytes | reopen |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
	for _, row := range report.IngestionRuns {
		fmt.Fprintf(&b, "| %d | %d | %d | %.6f | %.2f | %.2f | %.0f | %.0f | %d | %t |\n",
			row.Repetition, row.SourceDocs, row.ChunkDocs, row.EndToEndSeconds, row.SourceDocsPerSec,
			row.ChunkDocsPerSec, row.BytesPerOp, row.AllocsPerOp, row.StorageBytes, row.ReopenParity)
	}
	fmt.Fprintf(&b, "\nMedian/p95 docs/s: **%.2f / %.2f**. Median/p95 B/source: **%.0f / %.0f**. Historical 37.59 docs/s / 132 GiB regime reproduced: **%t**.\n\n",
		report.IngestionSummary.MedianDocsPerSec, report.IngestionSummary.P95DocsPerSec,
		report.IngestionSummary.MedianBytesPerOp, report.IngestionSummary.P95BytesPerOp,
		report.IngestionSummary.HistoricalReproduced)
	fmt.Fprintf(&b, "Frozen #4284 gate: source docs/s >= **%.2f**, B/source <= **%.0f**. %s\n\n", report.Gate.CandidateMinDocsPerSec, report.Gate.CandidateMaxBytesPerOp, report.Gate.Rationale)

	fmt.Fprintf(&b, "## Supported retained rows\n\n")
	fmt.Fprintf(&b, "Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.\n\n")
	fmt.Fprintf(&b, "Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents. Declared bounded scalar-intersection or parent-collapse exhaustions are scored with nonrelevant empty ranks through TopK; any other short ranking fails closed.\n\n")
	fmt.Fprintf(&b, "| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |\n")
	b.WriteString("|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	rows := append([]applicationRow(nil), report.Rows...)
	sort.Slice(rows, func(i, j int) bool {
		a, c := rows[i].Cell, rows[j].Cell
		return fmt.Sprintf("%s/%s/%02d/%s/%s/%s/%s", a.Embedding, a.Surface, a.Clients, a.Route, a.Projection, a.Filter, a.Collapse) < fmt.Sprintf("%s/%s/%02d/%s/%s/%s/%s", c.Embedding, c.Surface, c.Clients, c.Route, c.Projection, c.Filter, c.Collapse)
	})
	for _, row := range rows {
		if row.Status != "supported" {
			continue
		}
		fmt.Fprintf(&b, "| %s | %s | %d | %s | %s | %s | %s | %s | %.1f | %.3f | %.3f | %.3f | %.4f | %.4f | %.4f | %.0f | %.1f |\n",
			row.Cell.Embedding, row.Cell.Surface, row.Cell.Clients, row.Cell.Route, row.Cell.VectorRoute,
			row.Cell.Projection, row.Cell.Filter, row.Cell.Collapse,
			row.QPSMean, row.LatencyMSP50, row.LatencyMSP95, row.LatencyMSP99,
			row.Quality.ChunkRecallAt10, row.Quality.ParentRecallAt10, row.Quality.NDCGAt10,
			row.BytesPerOp, row.AllocsPerOp)
	}
	capabilityCounts := map[string]int{}
	for _, row := range rows {
		if row.Capability != nil {
			capabilityCounts[row.Capability.Code]++
		}
	}
	fmt.Fprintf(&b, "\n## Unsupported capability evidence\n\n")
	keys := make([]string, 0, len(capabilityCounts))
	for key := range capabilityCounts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&b, "- `%s`: %d rows; `%s`; zero results; fail closed.\n", key, capabilityCounts[key], capabilityErrorType)
	}
	fmt.Fprintf(&b, "\n## Exact controls\n\n")
	for _, control := range report.ExactControls {
		fmt.Fprintf(&b, "- `%s`: %d vectors, chunk recall@10 %.4f, parent recall@10 %.4f, nDCG@10 %.4f; %s.\n",
			control.Embedding, control.CorpusVectors, control.Quality.ChunkRecallAt10,
			control.Quality.ParentRecallAt10, control.Quality.NDCGAt10, control.Method)
	}
	fmt.Fprintf(&b, "\n## Lifecycle and durability\n\n")
	lifecycleKeys := make([]string, 0, len(report.Lifecycle))
	for key := range report.Lifecycle {
		lifecycleKeys = append(lifecycleKeys, key)
	}
	sort.Strings(lifecycleKeys)
	for _, key := range lifecycleKeys {
		row := report.Lifecycle[key]
		fmt.Fprintf(&b, "- `%s`: re-ingest=%t update=`%s` delete=`%s` source reopen=%t measured collection reopen=%t text/vector/scalar parity=%t/%t/%t; %s.\n",
			key, row.UnchangedReingest, row.UpdatedSource, row.DeletedSource, row.ColdReopenParity, row.QueryCollectionReopened,
			row.TextIndexParity, row.VectorIndexParity, row.ScalarIndexParity, row.FaultBoundary)
	}
	fmt.Fprintf(&b, "\n## Frozen structural/noise policy\n\n- cross-tenant results = 0\n- cross-workspace results = 0\n- full-document-scan fallbacks = 0\n- score-only document fetches = 0\n- fetch rows <= TopK documents\n- %s\n", report.Gate.NoisePolicy)
	return []byte(b.String())
}

type semanticInput struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Text string `json:"text"`
}

type semanticInputManifest struct {
	Schema        string          `json:"schema"`
	FixtureSHA256 string          `json:"fixture_sha256"`
	Inputs        []semanticInput `json:"inputs"`
}

func buildSemanticInputManifest(fixture *applicationFixture) semanticInputManifest {
	manifest := semanticInputManifest{Schema: "treedb-rag-semantic-inputs/v1", FixtureSHA256: applicationFixtureDigest(fixture)}
	seen := map[string]bool{}
	for _, source := range fixture.Sources {
		for _, phase := range []struct {
			name, body string
		}{{"initial", source.InitialBody}, {"final", source.FinalBody}} {
			for ordinal := range len(phase.body) / applicationChunkSize {
				text := phase.body[ordinal*applicationChunkSize : (ordinal+1)*applicationChunkSize]
				if seen[text] {
					continue
				}
				seen[text] = true
				manifest.Inputs = append(manifest.Inputs, semanticInput{Kind: "chunk", ID: fmt.Sprintf("%s:%s:%d", source.ID, phase.name, ordinal), Text: text})
			}
		}
	}
	for _, query := range fixture.Queries {
		manifest.Inputs = append(manifest.Inputs, semanticInput{Kind: "query", ID: query.ID, Text: query.Text})
	}
	return manifest
}

func writeSemanticInputManifest(path string) error {
	fixture := buildApplicationFixture()
	if err := validateApplicationFixture(&fixture); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(buildSemanticInputManifest(&fixture), "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}
