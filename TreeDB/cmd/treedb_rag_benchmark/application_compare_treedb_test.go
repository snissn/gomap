package main

import "testing"

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
		Schema: treeDBComparisonArtifactSchema, Authority: "M1_RETAINED_BASELINE",
		ManifestSHA256: manifestSHA, ProductBaseSHA: manifest.ProductBaseSHA,
		HarnessRevision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BinarySHA256:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		FixtureSHA256:   manifest.FixtureSHA256, SemanticVectorSHA256: manifest.SemanticVectorSHA256,
		ConfigSHA256: manifest.ConfigSHA256, Config: manifest.Config,
		SourceCount: 18, ChunkCount: 54, QueryCount: 3, BuildReopenSeconds: 1, QuerySeconds: 1, StorageBytes: 1,
		Lifecycle: lifecycleEvidence{ColdReopenParity: true, TextIndexParity: true, VectorIndexParity: true, ScalarIndexParity: true},
	}
	for _, route := range applicationRoutes {
		for _, filter := range applicationFilterOrder {
			artifact.Rows = append(artifact.Rows, applicationRow{
				Cell:   applicationCellIdentity{Route: route, Projection: "fetch_topk", Filter: filter, Collapse: "disabled", Surface: "direct_collection", Embedding: "semantic_minilm", Clients: 1},
				Status: "supported", Samples: make([]querySample, 300), Repetitions: make([]repetitionPerformance, 3),
				Counters: map[string]float64{},
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
	if _, err := validateTreeDBComparisonArtifact(&artifact, manifest, manifestSHA); err != nil {
		t.Fatal(err)
	}
}
