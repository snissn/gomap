package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
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
func TestQdrantClientLockDigestIsBound(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "benchmarks", "vector_db_compare", "qdrant-client-darwin-arm64-py313.lock"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != qdrantClientLockSHA256 {
		t.Fatalf("Qdrant client lock SHA-256=%s, want %s", got, qdrantClientLockSHA256)
	}
}
func TestValidateComparisonPathsRejectsAliases(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.json")
	if err := os.WriteFile(input, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validateComparisonPaths(input, filepath.Join(dir, ".", "input.json")); err == nil {
		t.Fatal("accepted relative alias")
	}
	link := filepath.Join(dir, "input-link.json")
	if err := os.Symlink(input, link); err == nil {
		if err := validateComparisonPaths(input, link); err == nil {
			t.Fatal("accepted symlink alias")
		}
	}
	danglingTarget := filepath.Join(dir, "comparison.md")
	danglingLink := filepath.Join(dir, "comparison.json")
	if err := os.Symlink(danglingTarget, danglingLink); err == nil {
		if err := validateComparisonPaths(danglingLink, danglingTarget); err == nil {
			t.Fatal("accepted dangling output symlink alias")
		}
	}
	hardLink := filepath.Join(dir, "input-hardlink.json")
	if err := os.Link(input, hardLink); err == nil {
		if err := validateComparisonPaths(input, hardLink); err == nil {
			t.Fatal("accepted hard-link alias")
		}
	}
	if err := validateComparisonPaths(filepath.Join(dir, "comparison.JSON"), filepath.Join(dir, "comparison.json")); err == nil {
		t.Fatal("accepted case-only output alias")
	}
	storage := filepath.Join(dir, "qdrant-storage")
	if err := os.Mkdir(storage, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validateComparisonPaths(input, storage, filepath.Join(storage, "comparison.json")); err == nil {
		t.Fatal("accepted output nested in storage path")
	}
	if err := validateComparisonPaths(input, filepath.Join(dir, "output.json"), filepath.Join(dir, "report.md")); err != nil {
		t.Fatalf("rejected distinct paths: %v", err)
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
	sparseVectorSHA, err := expectedQdrantSparseVectorSHA256(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cardinalities := map[string]int{
		filterUnfiltered: 54, filterTenantAlpha: 27,
		filterTenantAlphaWorkspaceRed: 18, filterModerateRange: 9,
	}
	harness := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	artifact := qdrantComparisonArtifact{
		Schema: qdrantComparisonArtifactSchema, Backend: "qdrant_server", HarnessRevision: harness,
		ClientVersion: "1.19.0", ClientLockSHA256: qdrantClientLockSHA256,
		PythonVersion: "3.13.11", PythonPlatform: "macOS-26.1-arm64-arm-64bit-Mach-O",
		PythonImplementation: "CPython", PythonExecutableSHA256: strings.Repeat("2", 64), ManifestSHA256: manifestSHA,
		FixtureSHA256: manifest.FixtureSHA256, SemanticVectorSHA256: manifest.SemanticVectorSHA256,
		SparseVectorSHA256: sparseVectorSHA, ConfigSHA256: manifest.ConfigSHA256,
		SourceCount: 18, ChunkCount: 54, QueryCount: 3,
		Server: qdrantComparisonServer{
			Version: "1.19.0", Deployment: "standalone", BinarySHA256: manifest.Config.QdrantBinarySHA256,
			ReleaseAssetSHA256: manifest.Config.QdrantReleaseAssetSHA256,
			Identity:           "pid:1:Tue Sep  1 22:55:00 2026 /qdrant|reopened_pid:2",
			Config: map[string]any{
				"dense": manifest.Config.DenseVectorName, "sparse": manifest.Config.SparseVectorName,
				"dense_size": float64(384), "dense_distance": "cosine", "sparse_on_disk": false,
				"exact": false, "full_scan_threshold": float64(10), "full_scan_threshold_unit": "KiB",
				"hnsw_m": float64(16), "hnsw_ef_construct": float64(100),
				"indexing_threshold": float64(1), "max_optimization_threads": float64(1),
				"query_hnsw_ef": float64(manifest.Config.QdrantHNSWEF),
			},
			IndexProof: qdrantComparisonIndexProof{IndexedVectorsCount: 108, FilterCardinalities: cardinalities},
		},
		Resources: qdrantComparisonResources{
			HostPIDMetrics: "observed_process_samples_across_pre_and_post_restart_segments",
			ProcessSamples: []qdrantComparisonProcessSample{
				{PID: 1, RSSBytes: 10, CPUSeconds: 1, CapturedUnixNanos: 1},
				{PID: 1, RSSBytes: 20, CPUSeconds: 2, CapturedUnixNanos: 2},
				{PID: 2, RSSBytes: 30, CPUSeconds: 3, CapturedUnixNanos: 3},
				{PID: 2, RSSBytes: 25, CPUSeconds: 5, CapturedUnixNanos: 4},
			},
			PeakObservedRSSBytes: 30, CPUSeconds: 6, DurableBytes: 1, DurableBytesSemantics: "live_before_server_shutdown",
		},
		Build: qdrantComparisonBuild{Seconds: 1, Points: 54}, QuerySeconds: 1,
		Reopen: qdrantComparisonReopen{
			Attempted: true, Succeeded: true, OptimizerUpdateTriggered: true, Version: "1.19.0", Status: "green", PointCount: 54,
			IndexedVectorsCount: 108, PayloadIndexes: []string{"tenant", "updated_year", "workspace"}, Seconds: 1,
		},
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
				TimingSemantics: "total_ms spans query_points, point-ID extraction, bounded retrieve, and payload ordering/validation; benchmark quality/byte bookkeeping is excluded; search_ms and fetch_ms are nested subtimers",
				Warmups:         20, Repetitions: 3, FetchMaxCount: 1,
				RouteProof: qdrantComparisonRouteProof{API: "qdrant.query_points", NamedVectors: vectors, BoundedFetch: true},
			}
			for repetition := range manifest.Config.Repetitions {
				order := "forward"
				if repetition%2 == 1 {
					order = "reverse"
				}
				wall := 5.17 + float64(10*repetition)
				cell.RepetitionPerformance = append(cell.RepetitionPerformance, qdrantComparisonRepetition{Repetition: repetition, Order: order, Samples: 100, WallSeconds: wall, QPS: 100 / wall})
				for ordinal := range manifest.Config.SamplesPerCell {
					queryIndex := ordinal % len(manifest.Queries)
					if order == "reverse" {
						queryIndex = len(manifest.Queries) - 1 - queryIndex
					}
					cell.Samples = append(cell.Samples, qdrantComparisonSample{
						Repetition: repetition, Ordinal: ordinal,
						QueryID:  manifest.Queries[queryIndex].ID,
						SearchMS: 1, FetchMS: 1, TotalMS: 2.1 + float64(repetition*manifest.Config.SamplesPerCell+ordinal), ResultIDs: []string{resultID},
						FetchedCount: 1, FetchedBytes: 1,
					})
				}
			}
			for _, repetition := range cell.RepetitionPerformance {
				cell.Summary.QPS += repetition.QPS / float64(len(cell.RepetitionPerformance))
			}
			latencies, quality, err := recomputeQdrantCellEvidence(cell, manifest)
			if err != nil {
				t.Fatal(err)
			}
			cell.Summary.LatencyMSP50, _ = percentile(latencies, 50)
			cell.Summary.LatencyMSP95, _ = percentile(latencies, 95)
			cell.Summary.LatencyMSP99, _ = percentile(latencies, 99)
			cell.Quality = quality
			if route == "hybrid" {
				cell.RouteProof.Fusion = "rrf"
			}
			artifact.Cells = append(artifact.Cells, cell)
		}
	}
	return manifest, manifestSHA, artifact
}

func TestQdrantSparseVectorDigestMatchesPythonCanonicalAlgorithm(t *testing.T) {
	manifest, err := buildApplicationComparisonManifest()
	if err != nil {
		t.Fatal(err)
	}
	got, err := expectedQdrantSparseVectorSHA256(manifest)
	if err != nil {
		t.Fatal(err)
	}
	const want = "beb0a6c2a32ef155cb8ad1e3f4d00ebc52abbef79e2e3368c0fa2e0feded6a7b"
	if got != want {
		t.Fatalf("sparse vector SHA-256=%s want Python canonical digest %s", got, want)
	}
	mutated := manifest
	mutated.Chunks = append([]applicationComparisonChunk(nil), manifest.Chunks...)
	mutated.Chunks[0].Content += " mutation"
	changed, err := expectedQdrantSparseVectorSHA256(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if changed == want {
		t.Fatal("sparse vector digest ignored frozen content mutation")
	}
}

func TestQdrantComparisonValidatorRejectsInvalidEvidence(t *testing.T) {
	cases := []struct {
		name string
		edit func(*qdrantComparisonArtifact)
	}{
		{"local mode", func(a *qdrantComparisonArtifact) { a.Server.LocalMode = true }},
		{"Docker deployment", func(a *qdrantComparisonArtifact) { a.Server.Deployment = "docker" }},
		{"wrong release asset hash", func(a *qdrantComparisonArtifact) { a.Server.ReleaseAssetSHA256 = strings.Repeat("0", 64) }},
		{"missing identity", func(a *qdrantComparisonArtifact) { a.Server.Identity = "" }},
		{"wrong binary hash", func(a *qdrantComparisonArtifact) { a.Server.BinarySHA256 = strings.Repeat("0", 64) }},
		{"malformed identity", func(a *qdrantComparisonArtifact) { a.Server.Identity = "pid:1|reopened_pid:2" }},
		{"identity initial PID mismatch", func(a *qdrantComparisonArtifact) { a.Server.Identity = "pid:3:identity|reopened_pid:2" }},
		{"identity reopened PID mismatch", func(a *qdrantComparisonArtifact) { a.Server.Identity = "pid:1:identity|reopened_pid:3" }},
		{"exact search", func(a *qdrantComparisonArtifact) { a.Server.Config["exact"] = true }},
		{"full scan threshold", func(a *qdrantComparisonArtifact) { a.Server.Config["full_scan_threshold"] = float64(1) }},
		{"wrong HNSW M", func(a *qdrantComparisonArtifact) { a.Server.Config["hnsw_m"] = float64(15) }},
		{"wrong HNSW ef_construct", func(a *qdrantComparisonArtifact) { a.Server.Config["hnsw_ef_construct"] = float64(99) }},
		{"wrong dense distance", func(a *qdrantComparisonArtifact) { a.Server.Config["dense_distance"] = "dot" }},
		{"sparse index on disk", func(a *qdrantComparisonArtifact) { a.Server.Config["sparse_on_disk"] = true }},
		{"wrong indexing threshold", func(a *qdrantComparisonArtifact) { a.Server.Config["indexing_threshold"] = float64(2) }},
		{"wrong query HNSW ef", func(a *qdrantComparisonArtifact) { a.Server.Config["query_hnsw_ef"] = float64(32) }},
		{"wrong sparse vector digest", func(a *qdrantComparisonArtifact) { a.SparseVectorSHA256 = strings.Repeat("0", 64) }},
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
		{"reopen optimizer update missing", func(a *qdrantComparisonArtifact) { a.Reopen.OptimizerUpdateTriggered = false }},
		{"wrong repetition order", func(a *qdrantComparisonArtifact) { a.Cells[0].RepetitionPerformance[1].Order = "forward" }},
		{"wrong reverse query mix", func(a *qdrantComparisonArtifact) { a.Cells[0].Samples[100].QueryID = a.Cells[0].Samples[99].QueryID }},
		{"reopen timeout", func(a *qdrantComparisonArtifact) { a.Reopen.Seconds = 91 }},
		{"reopen not green", func(a *qdrantComparisonArtifact) { a.Reopen.Status = "yellow" }},
		{"reopen index missing", func(a *qdrantComparisonArtifact) { a.Reopen.IndexedVectorsCount = 53 }},
		{"reopen payload index missing", func(a *qdrantComparisonArtifact) { a.Reopen.PayloadIndexes = a.Reopen.PayloadIndexes[:2] }},
		{"unbounded fetch", func(a *qdrantComparisonArtifact) { a.Cells[0].FetchMaxCount = 11 }},
		{"wrong manifest", func(a *qdrantComparisonArtifact) { a.ManifestSHA256 = "wrong" }},
		{"wrong durable-byte semantics", func(a *qdrantComparisonArtifact) { a.Resources.DurableBytesSemantics = "unstable" }},
		{"missing process samples", func(a *qdrantComparisonArtifact) { a.Resources.ProcessSamples = nil }},
		{"missing CPU", func(a *qdrantComparisonArtifact) { a.Resources.CPUSeconds = 0 }},
		{"unordered process samples", func(a *qdrantComparisonArtifact) { a.Resources.ProcessSamples[1].CapturedUnixNanos = 1 }},
		{"CPU regression", func(a *qdrantComparisonArtifact) { a.Resources.ProcessSamples[1].CPUSeconds = .5 }},
		{"restarted startup CPU omitted", func(a *qdrantComparisonArtifact) { a.Resources.CPUSeconds = 3 }},
		{"wrong peak RSS aggregate", func(a *qdrantComparisonArtifact) { a.Resources.PeakObservedRSSBytes = 29 }},
		{"wrong CPU aggregate", func(a *qdrantComparisonArtifact) { a.Resources.CPUSeconds = 4 }},
		{"noncontiguous PID lifecycle", func(a *qdrantComparisonArtifact) { a.Resources.ProcessSamples[3].PID = 1 }},
		{"wrong harness", func(a *qdrantComparisonArtifact) { a.HarnessRevision = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }},
		{"wrong client", func(a *qdrantComparisonArtifact) { a.ClientVersion = "1.18.0" }},
		{"wrong named vectors", func(a *qdrantComparisonArtifact) { a.Cells[0].RouteProof.NamedVectors = []string{"dense_minilm"} }},
		{"wrong warmups", func(a *qdrantComparisonArtifact) { a.Cells[0].Warmups = 60 }},
		{"missing client lock digest", func(a *qdrantComparisonArtifact) { a.ClientLockSHA256 = "" }},
		{"wrong client lock digest", func(a *qdrantComparisonArtifact) { a.ClientLockSHA256 = strings.Repeat("1", 64) }},
		{"wrong Python version", func(a *qdrantComparisonArtifact) { a.PythonVersion = "3.14.0" }},
		{"wrong Python platform", func(a *qdrantComparisonArtifact) { a.PythonPlatform = "Linux-x86_64" }},
		{"wrong Python implementation", func(a *qdrantComparisonArtifact) { a.PythonImplementation = "PyPy" }},
		{"missing Python executable digest", func(a *qdrantComparisonArtifact) { a.PythonExecutableSHA256 = "" }},
		{"wrong timing semantics", func(a *qdrantComparisonArtifact) { a.Cells[0].TimingSemantics = "search only" }},
		{"missing total work", func(a *qdrantComparisonArtifact) { a.Cells[0].Samples[0].TotalMS = 1 }},
		{"nearest-rank latency", func(a *qdrantComparisonArtifact) { a.Cells[0].Summary.LatencyMSP99 = a.Cells[0].Samples[296].TotalMS }},
		{"wall shorter than retained samples", func(a *qdrantComparisonArtifact) {
			performance := &a.Cells[0].RepetitionPerformance[0]
			performance.WallSeconds /= 2
			performance.QPS = 100 / performance.WallSeconds
			qps := 0.0
			for _, repetition := range a.Cells[0].RepetitionPerformance {
				qps += repetition.QPS
			}
			a.Cells[0].Summary.QPS = qps / float64(len(a.Cells[0].RepetitionPerformance))
		}},
		{"missing repetition wall", func(a *qdrantComparisonArtifact) { a.Cells[0].RepetitionPerformance[0].WallSeconds = 0 }},
		{"wrong QPS mean", func(a *qdrantComparisonArtifact) { a.Cells[0].Summary.QPS = 99 }},
		{"wrong latency summary", func(a *qdrantComparisonArtifact) { a.Cells[0].Summary.LatencyMSP95++ }},
		{"wrong quality summary", func(a *qdrantComparisonArtifact) { a.Cells[0].Quality.NDCGAt10++ }},
		{"wrong relevant chunks", func(a *qdrantComparisonArtifact) { a.Cells[0].Quality.RelevantChunksMean++ }},
		{"wrong relevant parents", func(a *qdrantComparisonArtifact) { a.Cells[0].Quality.RelevantParentsMean++ }},
		{"wrong achievable chunk recall", func(a *qdrantComparisonArtifact) { a.Cells[0].Quality.MaxAchievableChunkRecallAt5++ }},
		{"wrong achievable parent recall", func(a *qdrantComparisonArtifact) { a.Cells[0].Quality.MaxAchievableParentRecallAt10++ }},
		{"wrong max parent multiplicity", func(a *qdrantComparisonArtifact) { a.Cells[0].Quality.MaxPerParentResults++ }},
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

func TestQdrantComparisonValidatorRejectsVariedMeasuredRanking(t *testing.T) {
	manifest, manifestSHA, artifact := validQdrantComparisonArtifact(t)
	cell := &artifact.Cells[0]
	sample := &cell.Samples[0]
	filterSpec := manifest.Filters[0]
	for _, candidate := range manifest.Filters {
		if candidate.ID == cell.Filter {
			filterSpec = candidate
		}
	}
	relevant := map[string]bool{}
	for _, query := range manifest.Queries {
		if query.ID == sample.QueryID {
			for _, judgment := range query.Cases {
				if judgment.Filter == cell.Filter {
					relevant = stringSet(judgment.RelevantChunks)
				}
			}
		}
	}
	for _, chunk := range manifest.Chunks {
		if !relevant[chunk.ID] &&
			(filterSpec.Tenant == "" || chunk.Tenant == filterSpec.Tenant) &&
			(filterSpec.Workspace == "" || chunk.Workspace == filterSpec.Workspace) &&
			(filterSpec.UpdatedYearGTE == 0 || chunk.UpdatedYear >= filterSpec.UpdatedYearGTE) {
			sample.ResultIDs = []string{chunk.ID}
			break
		}
	}
	if err := validateQdrantComparisonArtifact(&artifact, manifest, manifestSHA, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err == nil ||
		!strings.Contains(err.Error(), "quality does not match raw rankings") {
		t.Fatalf("unaggregated measured ranking quality err=%v", err)
	}
}

func TestQdrantComparisonValidatorAcceptsCompleteEvidence(t *testing.T) {
	manifest, manifestSHA, artifact := validQdrantComparisonArtifact(t)
	if err := validateQdrantComparisonArtifact(&artifact, manifest, manifestSHA, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err != nil {
		t.Fatal(err)
	}
}

func TestQdrantComparisonPercentilesMatchTreeDBInterpolation(t *testing.T) {
	manifest, manifestSHA, artifact := validQdrantComparisonArtifact(t)
	cell := &artifact.Cells[0]
	latencies, _, err := recomputeQdrantCellEvidence(*cell, manifest)
	if err != nil {
		t.Fatal(err)
	}
	want, err := percentile(latencies, 99)
	if err != nil {
		t.Fatal(err)
	}
	if !comparisonFloatMatches(cell.Summary.LatencyMSP99, want) {
		t.Fatalf("Qdrant p99=%v want TreeDB interpolation %v", cell.Summary.LatencyMSP99, want)
	}
	nearestRank := cell.Samples[296].TotalMS
	if comparisonFloatMatches(want, nearestRank) {
		t.Fatalf("fixture does not distinguish interpolation %v from nearest-rank %v", want, nearestRank)
	}
	cell.Summary.LatencyMSP99 = nearestRank
	if err := validateQdrantComparisonArtifact(&artifact, manifest, manifestSHA, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
		t.Fatal("nearest-rank Qdrant percentile accepted")
	}
}
