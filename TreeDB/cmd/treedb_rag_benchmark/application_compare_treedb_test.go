package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func validTreeDBComparisonArtifact(t *testing.T) (applicationComparisonManifest, string, treeDBComparisonArtifact) {
	t.Helper()
	manifest, err := buildApplicationComparisonManifest()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := applicationComparisonManifestBytes()
	if err != nil {
		t.Fatal(err)
	}
	manifestSHA, err := validateComparisonManifest(raw, &manifest)
	if err != nil {
		t.Fatal(err)
	}
	artifact := treeDBComparisonArtifact{
		Schema: treeDBComparisonArtifactSchema, Authority: "BOUNDED_COMPARISON_EVIDENCE",
		ManifestSHA256: manifestSHA, ProductBaseSHA: manifest.ProductBaseSHA,
		HarnessRevision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BinarySHA256:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		FixtureSHA256:   manifest.FixtureSHA256, SemanticVectorSHA256: manifest.SemanticVectorSHA256,
		ConfigSHA256: manifest.ConfigSHA256, Config: manifest.Config,
		SourceCount: 18, ChunkCount: 54, QueryCount: 3, BuildReopenSeconds: 1, QuerySeconds: 1, StorageBytes: 1,
		Lifecycle: lifecycleEvidence{ColdReopenParity: true, TextIndexParity: true, VectorIndexParity: true, ScalarIndexParity: true},
		ProcessResources: comparisonProcessResources{
			Available: true, CPUSeconds: 1, PeakRSSBytes: 1024,
			Before:       comparisonProcessUsage{Available: true, CPUSeconds: 2, PeakRSSBytes: 512, CapturedUnixNanos: 1},
			After:        comparisonProcessUsage{Available: true, CPUSeconds: 3, PeakRSSBytes: 1024, CapturedUnixNanos: 2},
			CPUSemantics: "getrusage(RUSAGE_SELF) user+system CPU; cumulative before/after snapshots, aggregate is after-before",
			RSSSemantics: "getrusage(RUSAGE_SELF) process high-water RSS; before/after snapshots, aggregate is after high-water; Darwin bytes, Linux KiB normalized to bytes",
			Scope:        "fresh comparison process; build, lifecycle reopen, and all 12 query cells",
		},
	}
	for _, route := range applicationRoutes {
		for _, filter := range applicationFilterOrder {
			vectorRoute := "none"
			counters := map[string]float64{}
			if route == "vector_only" || route == "hybrid" {
				vectorRoute = "declared_column_graph_exact"
				counters["vector_candidates_examined"] = 54
				counters["vector_candidates_returned"] = 10
			}
			if route == "hybrid" {
				counters["text_candidates_returned"] = 10
				counters["candidates_fused"] = 20
			}
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
			source := [2]bool{route != "vector_only", route != "text_only"}
			row := applicationRow{
				Cell: applicationCellIdentity{
					Route: route, Projection: "fetch_topk", Filter: filter, Collapse: "disabled",
					Surface: "direct_collection", Embedding: "semantic_minilm", Clients: 1, VectorRoute: vectorRoute,
				},
				Status: "supported", Counters: counters,
			}
			for repetition := range manifest.Config.Repetitions {
				order := "forward"
				if repetition%2 == 1 {
					order = "reverse"
				}
				row.Repetitions = append(row.Repetitions, repetitionPerformance{
					Repetition: repetition, Order: order, Samples: manifest.Config.SamplesPerCell, WallSeconds: 1, QPS: 100,
				})
				for ordinal := range manifest.Config.SamplesPerCell {
					queryIndex := ordinal % len(manifest.Queries)
					if repetition%2 == 1 {
						queryIndex = len(manifest.Queries) - 1 - queryIndex
					}
					row.Samples = append(row.Samples, querySample{
						Repetition: repetition, Ordinal: ordinal, QueryID: manifest.Queries[queryIndex].ID, Millis: 2.1,
						ResultIDs: []string{resultID}, ResultSources: map[string][2]bool{resultID: source},
					})
				}
			}
			latencies, quality, err := recomputeTreeDBCellEvidence(row, manifest)
			if err != nil {
				t.Fatal(err)
			}
			row.QPSMean = 100
			row.LatencyMSMean = mean(latencies)
			row.LatencyMSP50, _ = percentile(latencies, 50)
			row.LatencyMSP95, _ = percentile(latencies, 95)
			row.LatencyMSP99, _ = percentile(latencies, 99)
			row.Quality = quality
			artifact.Rows = append(artifact.Rows, row)
		}
	}
	return manifest, manifestSHA, artifact
}

func TestTreeDBComparisonValidatorRejectsMismatchedEvidence(t *testing.T) {
	cases := []struct {
		name string
		edit func(*treeDBComparisonArtifact)
	}{
		{"old application schema", func(a *treeDBComparisonArtifact) { a.Schema = applicationReportSchema }},
		{"wrong authority", func(a *treeDBComparisonArtifact) { a.Authority = "DIAGNOSTIC_NOT_FINAL_EVIDENCE" }},
		{"wrong product base", func(a *treeDBComparisonArtifact) { a.ProductBaseSHA = "3b3235" }},
		{"short harness revision", func(a *treeDBComparisonArtifact) { a.HarnessRevision = "abc" }},
		{"wrong config", func(a *treeDBComparisonArtifact) { a.Config.WarmupsPerCell = 24 }},
		{"partial samples", func(a *treeDBComparisonArtifact) { a.Rows[0].Samples = a.Rows[0].Samples[:299] }},
		{"phase timeout", func(a *treeDBComparisonArtifact) { a.QuerySeconds = 91 }},
		{"reopen failure", func(a *treeDBComparisonArtifact) { a.Lifecycle.ColdReopenParity = false }},
		{"missing CPU", func(a *treeDBComparisonArtifact) { a.ProcessResources.CPUSeconds = 0 }},
		{"missing peak RSS", func(a *treeDBComparisonArtifact) { a.ProcessResources.PeakRSSBytes = 0 }},
		{"wrong resource semantics", func(a *treeDBComparisonArtifact) { a.ProcessResources.RSSSemantics = "current RSS" }},
		{"missing before snapshot", func(a *treeDBComparisonArtifact) { a.ProcessResources.Before.Available = false }},
		{"missing after snapshot", func(a *treeDBComparisonArtifact) { a.ProcessResources.After.Available = false }},
		{"unordered snapshots", func(a *treeDBComparisonArtifact) { a.ProcessResources.After.CapturedUnixNanos = 1 }},
		{"cumulative CPU regression", func(a *treeDBComparisonArtifact) { a.ProcessResources.After.CPUSeconds = 1 }},
		{"wrong CPU aggregate", func(a *treeDBComparisonArtifact) { a.ProcessResources.CPUSeconds = .5 }},
		{"wrong peak RSS aggregate", func(a *treeDBComparisonArtifact) { a.ProcessResources.PeakRSSBytes = 1023 }},
		{"high-water RSS regression", func(a *treeDBComparisonArtifact) { a.ProcessResources.After.PeakRSSBytes = 511 }},
		{"wrong exact vector route", func(a *treeDBComparisonArtifact) { a.Rows[4].Cell.VectorRoute = "brute_force" }},
		{"missing vector counters", func(a *treeDBComparisonArtifact) { a.Rows[4].Counters["vector_candidates_examined"] = 0 }},
		{"missing hybrid fusion counters", func(a *treeDBComparisonArtifact) { a.Rows[8].Counters["candidates_fused"] = 0 }},
		{"wrong QPS", func(a *treeDBComparisonArtifact) { a.Rows[0].QPSMean++ }},
		{"wrong latency", func(a *treeDBComparisonArtifact) { a.Rows[0].LatencyMSP95++ }},
		{"wrong quality", func(a *treeDBComparisonArtifact) { a.Rows[0].Quality.NDCGAt10++ }},
		{"wrong quality metadata", func(a *treeDBComparisonArtifact) { a.Rows[0].Quality.RelevantChunksMean++ }},
		{"unknown query", func(a *treeDBComparisonArtifact) { a.Rows[0].Samples[0].QueryID = "unknown" }},
		{"invalid result", func(a *treeDBComparisonArtifact) { a.Rows[0].Samples[0].ResultIDs[0] = "unknown" }},
		{"duplicate result", func(a *treeDBComparisonArtifact) {
			sample := &a.Rows[0].Samples[0]
			sample.ResultIDs = append(sample.ResultIDs, sample.ResultIDs[0])
		}},
		{"invalid attribution", func(a *treeDBComparisonArtifact) {
			sample := &a.Rows[0].Samples[0]
			sample.ResultSources[sample.ResultIDs[0]] = [2]bool{}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			manifest, manifestSHA, artifact := validTreeDBComparisonArtifact(t)
			test.edit(&artifact)
			if _, err := validateTreeDBComparisonArtifact(&artifact, manifest, manifestSHA); err == nil {
				t.Fatal("invalid TreeDB comparison evidence accepted")
			}
		})
	}
}

func TestTreeDBComparisonValidatorAcceptsExactEvidence(t *testing.T) {
	manifest, manifestSHA, artifact := validTreeDBComparisonArtifact(t)
	rows, err := validateTreeDBComparisonArtifact(&artifact, manifest, manifestSHA)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Equivalence != "directional" {
			t.Fatalf("TreeDB %s/%s equivalence=%q want directional", row.Route, row.Filter, row.Equivalence)
		}
	}
}

func TestCreateTreeDBComparisonRootRejectsExistingDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "comparison")
	if err := createTreeDBComparisonRoot(root); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(root, "keep")
	if err := os.WriteFile(sentinel, []byte("caller data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := createTreeDBComparisonRoot(root); err == nil {
		t.Fatal("existing caller-selected comparison directory accepted")
	}
	if raw, err := os.ReadFile(sentinel); err != nil || string(raw) != "caller data" {
		t.Fatalf("existing caller data was changed: %q err=%v", raw, err)
	}
}

func TestComparisonProcessUsageSnapshot(t *testing.T) {
	snapshot, err := comparisonProcessUsageSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
		if !snapshot.Available || snapshot.CPUSeconds < 0 || snapshot.PeakRSSBytes <= 0 || snapshot.CapturedUnixNanos <= 0 {
			t.Fatalf("invalid getrusage snapshot: %+v", snapshot)
		}
	}
}
