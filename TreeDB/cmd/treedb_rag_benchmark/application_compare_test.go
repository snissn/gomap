package main

import (
	"crypto/sha256"
	"encoding/hex"
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
	harness := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	artifact := qdrantComparisonArtifact{
		Schema: qdrantComparisonArtifactSchema, Backend: "qdrant_server", HarnessRevision: harness,
		ClientVersion: "1.19.0", ManifestSHA256: manifestSHA,
		FixtureSHA256: manifest.FixtureSHA256, SemanticVectorSHA256: manifest.SemanticVectorSHA256,
		ConfigSHA256: manifest.ConfigSHA256, SourceCount: 18, ChunkCount: 54, QueryCount: 3,
		Server: qdrantComparisonServer{Version: "1.19.0", Deployment: "docker", Identity: "container-id",
			Image:  "qdrant/qdrant:v1.19.0@sha256:057ee3a8da769fe7310dd3537b4dc7583bf87a95ce8ac43c0af5a46bc580d1fc",
			Config: map[string]any{"dense": manifest.Config.DenseVectorName, "sparse": manifest.Config.SparseVectorName}},
		Resources: qdrantComparisonResources{HostPIDMetrics: "unavailable_for_docker_container", DockerStats: map[string]any{"memory_usage": "1MiB"}, DurableBytes: 1},
		Reopen:    qdrantComparisonReopen{Attempted: true, Succeeded: true, Version: "1.19.0", PointCount: 54},
	}
	for _, route := range []string{"lexical", "dense", "hybrid"} {
		for _, filter := range applicationFilterOrder {
			vectors := []string{manifest.Config.SparseVectorName}
			if route == "dense" {
				vectors = []string{manifest.Config.DenseVectorName}
			} else if route == "hybrid" {
				vectors = []string{manifest.Config.SparseVectorName, manifest.Config.DenseVectorName}
			}
			cell := qdrantComparisonCell{Route: route, Filter: filter, Equivalence: "direct", Warmups: 20,
				Repetitions: 3, Samples: make([]map[string]any, 300), FetchMaxCount: 10,
				RouteProof: qdrantComparisonRouteProof{API: "qdrant.query_points", NamedVectors: vectors, BoundedFetch: true}}
			if route != "dense" {
				cell.Equivalence = "directional"
			}
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
		{"missing identity", func(a *qdrantComparisonArtifact) { a.Server.Identity = "" }},
		{"missing route", func(a *qdrantComparisonArtifact) { a.Cells = a.Cells[:len(a.Cells)-1] }},
		{"fallback", func(a *qdrantComparisonArtifact) { a.Cells[0].RouteProof.Fallbacks = 1 }},
		{"exhaustive", func(a *qdrantComparisonArtifact) { a.Cells[0].RouteProof.ExhaustiveSearch = true }},
		{"leakage", func(a *qdrantComparisonArtifact) { a.Cells[0].Leakage = 1 }},
		{"partial samples", func(a *qdrantComparisonArtifact) { a.Cells[0].Samples = a.Cells[0].Samples[:299] }},
		{"reopen failure", func(a *qdrantComparisonArtifact) { a.Reopen.Succeeded = false }},
		{"unbounded fetch", func(a *qdrantComparisonArtifact) { a.Cells[0].FetchMaxCount = 11 }},
		{"wrong manifest", func(a *qdrantComparisonArtifact) { a.ManifestSHA256 = "wrong" }},
		{"missing resources", func(a *qdrantComparisonArtifact) { a.Resources.DockerStats = nil }},
		{"wrong harness", func(a *qdrantComparisonArtifact) { a.HarnessRevision = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }},
		{"wrong client", func(a *qdrantComparisonArtifact) { a.ClientVersion = "1.18.0" }},
		{"wrong named vectors", func(a *qdrantComparisonArtifact) { a.Cells[0].RouteProof.NamedVectors = []string{"dense_minilm"} }},
		{"wrong warmups", func(a *qdrantComparisonArtifact) { a.Cells[0].Warmups = 60 }},
		{"missing standalone binary hash", func(a *qdrantComparisonArtifact) {
			a.Server.Deployment = "standalone"
			a.Server.Image = ""
		}},
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
