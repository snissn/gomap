package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestApplicationComparisonManifestDeterministic(t *testing.T) {
	first, err := applicationComparisonManifestBytes()
	if err != nil {
		t.Fatal(err)
	}
	second, err := applicationComparisonManifestBytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("comparison manifest export is not deterministic")
	}
	manifest, err := buildApplicationComparisonManifest()
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Sources) != 18 || len(manifest.Chunks) != 54 || len(manifest.Queries) != 3 || len(manifest.Filters) != 4 {
		t.Fatalf("cardinality=%d/%d/%d/%d", len(manifest.Sources), len(manifest.Chunks), len(manifest.Queries), len(manifest.Filters))
	}
	if manifest.FixtureSHA256 != applicationFixtureExpectedDigest || manifest.SemanticVectorSHA256 != semanticVectorsExpectedDigest || manifest.ConfigSHA256 != applicationComparisonConfigDigest(manifest.Config) {
		t.Fatal("comparison manifest hash bindings changed")
	}
	for _, chunk := range manifest.Chunks {
		if len(chunk.DenseVector) != 384 || chunk.ParentID == "" || chunk.Content == "" {
			t.Fatalf("invalid chunk %q", chunk.ID)
		}
	}
}

func validQdrantComparisonArtifact(t *testing.T) (applicationComparisonManifest, string, qdrantComparisonArtifact) {
	t.Helper()
	manifest, err := buildApplicationComparisonManifest()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := applicationComparisonManifestBytes()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	manifestSHA := hex.EncodeToString(sum[:])
	cardinalities := map[string]int{
		filterUnfiltered: 54, filterTenantAlpha: 27,
		filterTenantAlphaWorkspaceRed: 18, filterModerateRange: 9,
	}
	harness := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	artifact := qdrantComparisonArtifact{
		Schema: qdrantComparisonArtifactSchema, Backend: "qdrant_server", HarnessRevision: harness,
		ClientVersion: "1.19.0", ManifestSHA256: manifestSHA,
		FixtureSHA256: manifest.FixtureSHA256, SemanticVectorSHA256: manifest.SemanticVectorSHA256,
		ConfigSHA256: manifest.ConfigSHA256, SourceCount: 18, ChunkCount: 54, QueryCount: 3,
		Server: qdrantComparisonServer{
			Version: "1.19.0", Deployment: "standalone", BinarySHA256: manifest.Config.QdrantBinarySHA256, Identity: "pid:1|reopened_pid:2",
			Config: map[string]any{
				"dense": manifest.Config.DenseVectorName, "sparse": manifest.Config.SparseVectorName, "exact": false,
				"full_scan_threshold": float64(10), "full_scan_threshold_unit": "KiB",
			},
			IndexProof: qdrantComparisonIndexProof{IndexedVectorsCount: 54, FilterCardinalities: cardinalities},
		},
		Resources: qdrantComparisonResources{
			HostPIDMetrics: "observed_process_samples_across_pre_and_post_restart_segments",
			ProcessSamples: []qdrantComparisonProcessSample{
				{PID: 1, RSSBytes: 1, CapturedUnixNanos: 1},
				{PID: 1, RSSBytes: 1, CPUSeconds: 1, CapturedUnixNanos: 2},
				{PID: 2, RSSBytes: 1, CapturedUnixNanos: 3},
				{PID: 2, RSSBytes: 1, CPUSeconds: 1, CapturedUnixNanos: 4},
			},
			PeakObservedRSSBytes: 1, CPUSeconds: 1, DurableBytes: 1,
		},
		Build: qdrantComparisonBuild{Seconds: 1, Points: 54}, QuerySeconds: 1,
		Reopen: qdrantComparisonReopen{Attempted: true, Succeeded: true, Version: "1.19.0", PointCount: 54, Seconds: 1},
	}
	for _, route := range []string{"lexical", "dense", "hybrid"} {
		for _, filter := range applicationFilterOrder {
			var filterSpec applicationComparisonFilter
			for _, candidate := range manifest.Filters {
				if candidate.ID == filter {
					filterSpec = candidate
					break
				}
			}
			resultID := ""
			for _, chunk := range manifest.Chunks {
				if (filterSpec.Tenant == "" || chunk.Tenant == filterSpec.Tenant) &&
					(filterSpec.Workspace == "" || chunk.Workspace == filterSpec.Workspace) &&
					(filterSpec.UpdatedYearGTE == 0 || chunk.UpdatedYear >= filterSpec.UpdatedYearGTE) {
					resultID = chunk.ID
					break
				}
			}
			vectors := []string{manifest.Config.SparseVectorName}
			if route == "dense" {
				vectors = []string{manifest.Config.DenseVectorName}
			} else if route == "hybrid" {
				vectors = []string{manifest.Config.SparseVectorName, manifest.Config.DenseVectorName}
			}
			cell := qdrantComparisonCell{
				Route: route, Filter: filter, Equivalence: "directional",
				TimingSemantics: "total_ms spans query_points, point-ID extraction, bounded retrieve, payload ordering/validation, leakage/accounting, and sample recording; search_ms and fetch_ms are nested subtimers",
				Warmups:         20, Repetitions: 3, FetchMaxCount: 1,
				RouteProof: qdrantComparisonRouteProof{API: "qdrant.query_points", NamedVectors: vectors, BoundedFetch: true},
			}
			for repetition := range manifest.Config.Repetitions {
				cell.RepetitionPerformance = append(cell.RepetitionPerformance, qdrantComparisonRepetition{Repetition: repetition, Samples: 100, WallSeconds: 1, QPS: 100})
				for ordinal := range manifest.Config.SamplesPerCell {
					cell.Samples = append(cell.Samples, qdrantComparisonSample{
						Repetition: repetition, Ordinal: ordinal,
						QueryID:  manifest.Queries[ordinal%len(manifest.Queries)].ID,
						SearchMS: 1, FetchMS: 1, TotalMS: 2.1, ResultIDs: []string{resultID},
						FetchedCount: 1, FetchedBytes: 1,
					})
				}
			}
			cell.Summary.QPS = 100
			latencies, quality, err := recomputeQdrantCellEvidence(cell, manifest)
			if err != nil {
				t.Fatal(err)
			}
			cell.Summary.LatencyMSP50 = qdrantNearestRank(latencies, .50)
			cell.Summary.LatencyMSP95 = qdrantNearestRank(latencies, .95)
			cell.Summary.LatencyMSP99 = qdrantNearestRank(latencies, .99)
			cell.Quality = quality
			if route == "hybrid" {
				cell.RouteProof.Fusion = "rrf"
			}
			artifact.Cells = append(artifact.Cells, cell)
		}
	}
	return manifest, manifestSHA, artifact
}

func TestQdrantComparisonValidatorRejectsInvalidEvidence(t *testing.T) {
	cases := []struct {
		name string
		edit func(*qdrantComparisonArtifact)
	}{
		{"local mode", func(a *qdrantComparisonArtifact) { a.Server.LocalMode = true }},
		{"Docker deployment", func(a *qdrantComparisonArtifact) { a.Server.Deployment = "docker" }},
		{"missing identity", func(a *qdrantComparisonArtifact) { a.Server.Identity = "" }},
		{"wrong binary hash", func(a *qdrantComparisonArtifact) { a.Server.BinarySHA256 = strings.Repeat("0", 64) }},
		{"exact search", func(a *qdrantComparisonArtifact) { a.Server.Config["exact"] = true }},
		{"full scan threshold", func(a *qdrantComparisonArtifact) { a.Server.Config["full_scan_threshold"] = float64(1) }},
		{"missing indexed vectors", func(a *qdrantComparisonArtifact) { a.Server.IndexProof.IndexedVectorsCount = 0 }},
		{"wrong filter cardinality", func(a *qdrantComparisonArtifact) { a.Server.IndexProof.FilterCardinalities[filterUnfiltered] = 53 }},
		{"missing route", func(a *qdrantComparisonArtifact) { a.Cells = a.Cells[:len(a.Cells)-1] }},
		{"fallback", func(a *qdrantComparisonArtifact) { a.Cells[0].RouteProof.Fallbacks = 1 }},
		{"exhaustive", func(a *qdrantComparisonArtifact) { a.Cells[0].RouteProof.ExhaustiveSearch = true }},
		{"leakage", func(a *qdrantComparisonArtifact) { a.Cells[0].Leakage = 1 }},
		{"partial samples", func(a *qdrantComparisonArtifact) { a.Cells[0].Samples = a.Cells[0].Samples[:299] }},
		{"build timeout", func(a *qdrantComparisonArtifact) { a.Build.Seconds = 91 }},
		{"query timeout", func(a *qdrantComparisonArtifact) { a.QuerySeconds = 91 }},
		{"reopen failure", func(a *qdrantComparisonArtifact) { a.Reopen.Succeeded = false }},
		{"reopen timeout", func(a *qdrantComparisonArtifact) { a.Reopen.Seconds = 91 }},
		{"unbounded fetch", func(a *qdrantComparisonArtifact) { a.Cells[0].FetchMaxCount = 11 }},
		{"wrong manifest", func(a *qdrantComparisonArtifact) { a.ManifestSHA256 = "wrong" }},
		{"missing process samples", func(a *qdrantComparisonArtifact) { a.Resources.ProcessSamples = nil }},
		{"missing CPU", func(a *qdrantComparisonArtifact) { a.Resources.CPUSeconds = 0 }},
		{"wrong harness", func(a *qdrantComparisonArtifact) { a.HarnessRevision = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }},
		{"wrong client", func(a *qdrantComparisonArtifact) { a.ClientVersion = "1.18.0" }},
		{"wrong named vectors", func(a *qdrantComparisonArtifact) { a.Cells[0].RouteProof.NamedVectors = []string{"dense_minilm"} }},
		{"wrong warmups", func(a *qdrantComparisonArtifact) { a.Cells[0].Warmups = 60 }},
		{"wrong timing semantics", func(a *qdrantComparisonArtifact) { a.Cells[0].TimingSemantics = "search only" }},
		{"missing total work", func(a *qdrantComparisonArtifact) { a.Cells[0].Samples[0].TotalMS = 1 }},
		{"missing repetition wall", func(a *qdrantComparisonArtifact) { a.Cells[0].RepetitionPerformance[0].WallSeconds = 0 }},
		{"wrong QPS mean", func(a *qdrantComparisonArtifact) { a.Cells[0].Summary.QPS = 99 }},
		{"wrong latency summary", func(a *qdrantComparisonArtifact) { a.Cells[0].Summary.LatencyMSP95++ }},
		{"wrong quality summary", func(a *qdrantComparisonArtifact) { a.Cells[0].Quality.NDCGAt10++ }},
		{"unknown query", func(a *qdrantComparisonArtifact) { a.Cells[0].Samples[0].QueryID = "unknown" }},
		{"invalid result ranking", func(a *qdrantComparisonArtifact) { a.Cells[0].Samples[0].ResultIDs[0] = "unknown" }},
		{"duplicate result ranking", func(a *qdrantComparisonArtifact) {
			sample := &a.Cells[0].Samples[0]
			sample.ResultIDs = append(sample.ResultIDs, sample.ResultIDs[0])
			sample.FetchedCount = len(sample.ResultIDs)
		}},
		{"direct equivalence", func(a *qdrantComparisonArtifact) { a.Cells[0].Equivalence = "direct" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			manifest, manifestSHA, artifact := validQdrantComparisonArtifact(t)
			test.edit(&artifact)
			if err := validateQdrantComparisonArtifact(&artifact, manifest, manifestSHA, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
				t.Fatal("invalid Qdrant comparison evidence accepted")
			}
		})
	}
}

func TestQdrantComparisonValidatorAcceptsCompleteEvidence(t *testing.T) {
	manifest, manifestSHA, artifact := validQdrantComparisonArtifact(t)
	if err := validateQdrantComparisonArtifact(&artifact, manifest, manifestSHA, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err != nil {
		t.Fatal(err)
	}
}
