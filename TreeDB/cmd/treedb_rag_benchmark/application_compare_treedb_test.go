package main

import (
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
			CPUSemantics: "getrusage(RUSAGE_SELF) user+system CPU delta",
			RSSSemantics: "getrusage(RUSAGE_SELF) process high-water RSS; Darwin bytes, Linux KiB normalized to bytes",
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
			artifact.Rows = append(artifact.Rows, applicationRow{
				Cell: applicationCellIdentity{
					Route: route, Projection: "fetch_topk", Filter: filter, Collapse: "disabled",
					Surface: "direct_collection", Embedding: "semantic_minilm", Clients: 1, VectorRoute: vectorRoute,
				},
				Status: "supported", Samples: make([]querySample, 300), Repetitions: make([]repetitionPerformance, 3),
				Counters: counters,
			})
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
		{"wrong exact vector route", func(a *treeDBComparisonArtifact) { a.Rows[4].Cell.VectorRoute = "brute_force" }},
		{"missing vector counters", func(a *treeDBComparisonArtifact) { a.Rows[4].Counters["vector_candidates_examined"] = 0 }},
		{"missing hybrid fusion counters", func(a *treeDBComparisonArtifact) { a.Rows[8].Counters["candidates_fused"] = 0 }},
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

func TestComparisonProcessUsageSnapshot(t *testing.T) {
	snapshot, err := comparisonProcessUsageSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
		if !snapshot.Available || snapshot.CPUSeconds < 0 || snapshot.PeakRSSBytes <= 0 {
			t.Fatalf("invalid getrusage snapshot: %+v", snapshot)
		}
	}
}
