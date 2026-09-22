package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
)

const (
	applicationFixtureVersion        = "treedb-rag-application/v1"
	applicationChunkSize             = 128
	applicationChunksPerSource       = 3
	applicationFixtureExpectedDigest = "df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2"
	semanticVectorsExpectedDigest    = "aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4"
	filterUnfiltered                 = "unfiltered"
	filterTenantAlpha                = "tenant_alpha"
	filterTenantAlphaWorkspaceRed    = "tenant_alpha_workspace_red"
	filterModerateRange              = "tenant_alpha_workspace_red_updated_ge_2024"
)

var applicationFilterOrder = []string{
	filterUnfiltered,
	filterTenantAlpha,
	filterTenantAlphaWorkspaceRed,
	filterModerateRange,
}

type applicationSource struct {
	ID          string   `json:"id"`
	Tenant      string   `json:"tenant"`
	Workspace   string   `json:"workspace"`
	SourceURI   string   `json:"source_uri"`
	SourceType  string   `json:"source_type"`
	ACL         []string `json:"acl"`
	UpdatedYear int      `json:"updated_year"`
	InitialBody string   `json:"initial_body"`
	FinalBody   string   `json:"final_body"`
	Deleted     bool     `json:"deleted"`
}

type applicationQuery struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

type applicationJudgment struct {
	QueryID         string   `json:"query_id"`
	Filter          string   `json:"filter"`
	RelevantChunks  []string `json:"relevant_chunks"`
	RelevantParents []string `json:"relevant_parents"`
}

type applicationFixture struct {
	Version   string                `json:"version"`
	ChunkSize int                   `json:"chunk_size"`
	Sources   []applicationSource   `json:"sources"`
	Queries   []applicationQuery    `json:"queries"`
	Judgments []applicationJudgment `json:"judgments"`
}

type dimensionContract struct {
	Config  int `json:"config"`
	Corpus  int `json:"corpus"`
	Queries int `json:"queries"`
	Index   int `json:"index"`
	Vectors int `json:"vectors"`
}

func (d dimensionContract) validate() error {
	if d.Config <= 0 {
		return fmt.Errorf("dimensions: config must be positive, got %d", d.Config)
	}
	for name, got := range map[string]int{"corpus": d.Corpus, "queries": d.Queries, "index": d.Index, "vectors": d.Vectors} {
		if got != d.Config {
			return fmt.Errorf("dimensions: %s=%d does not match config=%d", name, got, d.Config)
		}
	}
	return nil
}

type timingBoundary struct {
	EmbeddingIncludesJudgments bool `json:"embedding_includes_judgments"`
}

func (b timingBoundary) validate() error {
	if b.EmbeddingIncludesJudgments {
		return fmt.Errorf("timing: embedding window includes judgment derivation")
	}
	return nil
}

type ingestionBoundary struct {
	SourceRowUsesIngestSources bool   `json:"source_row_uses_ingest_sources"`
	GeneratedChunkRowsLabel    string `json:"generated_chunk_rows_label"`
}

func (b ingestionBoundary) validate() error {
	if !b.SourceRowUsesIngestSources {
		return fmt.Errorf("ingestion: source row did not call Collection.IngestSources")
	}
	if b.GeneratedChunkRowsLabel == "ingest_docs_per_sec" || b.GeneratedChunkRowsLabel == "source_docs_per_sec" {
		return fmt.Errorf("ingestion: generated chunk-row inserts mislabeled as source ingestion")
	}
	return nil
}

type measurementClaim struct {
	Samples     int    `json:"samples"`
	Repetitions int    `json:"repetitions"`
	Label       string `json:"label"`
}

func (c measurementClaim) validate() error {
	if c.Label == "final_qps_p99" {
		if c.Samples < 1000 {
			return fmt.Errorf("measurement: final QPS/p99 requires >=1000 timed queries, got %d", c.Samples)
		}
		if c.Repetitions < 3 {
			return fmt.Errorf("measurement: final QPS/p99 requires >=3 repetitions, got %d", c.Repetitions)
		}
	}
	return nil
}

type comparisonIdentity struct {
	WorkDigest    string `json:"work_digest"`
	Projection    string `json:"projection"`
	QualityDigest string `json:"quality_digest"`
}

func validateComparable(base, final comparisonIdentity) error {
	if base.WorkDigest != final.WorkDigest {
		return fmt.Errorf("comparison: work digest mismatch %q != %q", base.WorkDigest, final.WorkDigest)
	}
	if base.Projection != final.Projection {
		return fmt.Errorf("comparison: projection mismatch %q != %q", base.Projection, final.Projection)
	}
	if base.QualityDigest != final.QualityDigest {
		return fmt.Errorf("comparison: quality digest mismatch %q != %q", base.QualityDigest, final.QualityDigest)
	}
	return nil
}

type artifactGuard struct {
	Filter                    string         `json:"filter"`
	CrossTenantResults        int            `json:"cross_tenant_results"`
	CrossWorkspaceResults     int            `json:"cross_workspace_results"`
	DocumentsFetched          int            `json:"documents_fetched"`
	TopK                      int            `json:"top_k"`
	FullDocumentScanFallbacks int            `json:"full_document_scan_fallbacks"`
	ParentCap                 int            `json:"parent_cap"`
	PerParentCounts           map[string]int `json:"per_parent_counts"`
	CollapseEnabled           bool           `json:"collapse_enabled"`
}

func (g artifactGuard) validate() error {
	if g.Filter != filterUnfiltered && g.CrossTenantResults != 0 {
		return fmt.Errorf("artifact: cross-tenant results=%d", g.CrossTenantResults)
	}
	if g.Filter == filterTenantAlphaWorkspaceRed || g.Filter == filterModerateRange {
		if g.CrossWorkspaceResults != 0 {
			return fmt.Errorf("artifact: cross-workspace results=%d", g.CrossWorkspaceResults)
		}
	}
	if g.DocumentsFetched > g.TopK {
		return fmt.Errorf("artifact: documents fetched=%d exceeds top_k=%d", g.DocumentsFetched, g.TopK)
	}
	if g.FullDocumentScanFallbacks != 0 {
		return fmt.Errorf("artifact: full-document-scan fallbacks=%d", g.FullDocumentScanFallbacks)
	}
	if g.CollapseEnabled {
		if g.ParentCap <= 0 {
			return fmt.Errorf("artifact: collapse enabled with parent cap=%d", g.ParentCap)
		}
		for parent, count := range g.PerParentCounts {
			if count > g.ParentCap {
				return fmt.Errorf("artifact: parent %q count=%d exceeds cap=%d", parent, count, g.ParentCap)
			}
		}
	}
	return nil
}

func buildApplicationFixture() applicationFixture {
	variants := []struct {
		tenant, workspace, freshness string
		year                         int
	}{
		{"alpha", "red", "new", 2025},
		{"alpha", "red", "old", 2023},
		{"alpha", "blue", "new", 2025},
		{"beta", "red", "new", 2025},
		{"beta", "blue", "new", 2025},
		{"beta", "blue", "old", 2023},
	}
	fixture := applicationFixture{Version: applicationFixtureVersion, ChunkSize: applicationChunkSize}
	for _, theme := range []string{"billing", "outage", "access"} {
		for _, variant := range variants {
			id := fmt.Sprintf("src-%s-%s-%s-%s", theme, variant.tenant, variant.workspace, variant.freshness)
			version := "current"
			initialVersion := version
			if id == "src-billing-alpha-red-new" {
				initialVersion = "legacy"
			}
			fixture.Sources = append(fixture.Sources, applicationSource{
				ID: id, Tenant: variant.tenant, Workspace: variant.workspace,
				SourceURI:  fmt.Sprintf("https://fixture.example/%s/%s", theme, id),
				SourceType: "text/plain", ACL: []string{"rag-readers", "tenant-" + variant.tenant},
				UpdatedYear: variant.year,
				InitialBody: applicationSourceBody(theme, variant.tenant, variant.workspace, variant.freshness, initialVersion),
				FinalBody:   applicationSourceBody(theme, variant.tenant, variant.workspace, variant.freshness, version),
			})
		}
	}
	fixture.Sources = append(fixture.Sources, applicationSource{
		ID: "src-lifecycle-beta-blue-old", Tenant: "beta", Workspace: "blue",
		SourceURI: "s3://fixture/lifecycle/deleted.txt", SourceType: "text/plain",
		ACL: []string{"rag-readers", "tenant-beta"}, UpdatedYear: 2022,
		InitialBody: applicationSourceBody("lifecycle", "beta", "blue", "old", "deleted"),
		FinalBody:   applicationSourceBody("lifecycle", "beta", "blue", "old", "deleted"), Deleted: true,
	})
	fixture.Queries = []applicationQuery{
		{ID: "q-billing", Text: "refund invoice charge dispute account ledger remedy"},
		{ID: "q-outage", Text: "service outage incident recovery availability failover"},
		{ID: "q-access", Text: "account access login permission authentication recovery"},
	}

	parents := map[string]map[string][]string{
		"q-billing": applicationJudgedParents("billing"),
		"q-outage":  applicationJudgedParents("outage"),
		"q-access":  applicationJudgedParents("access"),
	}
	for _, query := range fixture.Queries {
		for _, filter := range applicationFilterOrder {
			ps := append([]string(nil), parents[query.ID][filter]...)
			chunks := make([]string, 0, len(ps)*applicationChunksPerSource)
			for _, parent := range ps {
				for ordinal := 0; ordinal < applicationChunksPerSource; ordinal++ {
					chunks = append(chunks, chunking.ChildDocumentID(parent, ordinal))
				}
			}
			fixture.Judgments = append(fixture.Judgments, applicationJudgment{QueryID: query.ID, Filter: filter, RelevantChunks: chunks, RelevantParents: ps})
		}
	}
	return fixture
}

func applicationJudgedParents(theme string) map[string][]string {
	prefix := "src-" + theme + "-"
	return map[string][]string{
		filterUnfiltered: {
			prefix + "alpha-red-new", prefix + "alpha-red-old", prefix + "alpha-blue-new",
			prefix + "beta-red-new", prefix + "beta-blue-new", prefix + "beta-blue-old",
		},
		filterTenantAlpha: {
			prefix + "alpha-red-new", prefix + "alpha-red-old", prefix + "alpha-blue-new",
		},
		filterTenantAlphaWorkspaceRed: {
			prefix + "alpha-red-new", prefix + "alpha-red-old",
		},
		filterModerateRange: {prefix + "alpha-red-new"},
	}
}

func applicationSourceBody(theme, tenant, workspace, freshness, version string) string {
	parts := make([]string, applicationChunksPerSource)
	for ordinal := range parts {
		seed := fmt.Sprintf("%s guidance %s %s workspace %s update %s part %d. ", applicationThemeTerms(theme), tenant, freshness, workspace, version, ordinal+1)
		parts[ordinal] = fixedASCII(seed, applicationChunkSize)
	}
	return strings.Join(parts, "")
}

func applicationThemeTerms(theme string) string {
	switch theme {
	case "billing":
		return "billing refund invoice charge dispute account ledger remedy"
	case "outage":
		return "service outage incident recovery availability failover status"
	case "access":
		return "account access login permission authentication recovery security"
	default:
		return "lifecycle deletion tombstone reopen durability evidence"
	}
}

func fixedASCII(seed string, size int) string {
	if len(seed) >= size {
		return seed[:size]
	}
	var b strings.Builder
	b.Grow(size)
	for b.Len() < size {
		remaining := size - b.Len()
		if remaining >= len(seed) {
			b.WriteString(seed)
		} else {
			b.WriteString(seed[:remaining])
		}
	}
	return b.String()
}

func applicationFixtureDigest(fixture *applicationFixture) string {
	raw, _ := json.Marshal(fixture)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validateApplicationFixture(fixture *applicationFixture) error {
	if fixture == nil || fixture.Version != applicationFixtureVersion || fixture.ChunkSize != applicationChunkSize {
		return fmt.Errorf("fixture: version/chunk size mismatch")
	}
	if len(fixture.Sources) < 4 || len(fixture.Queries) == 0 {
		return fmt.Errorf("fixture: insufficient sources or queries")
	}
	sources := make(map[string]applicationSource, len(fixture.Sources))
	tenants, workspaces := map[string]bool{}, map[string]bool{}
	for _, source := range fixture.Sources {
		if err := chunking.ValidateParentID(source.ID); err != nil {
			return fmt.Errorf("fixture: source %q: %w", source.ID, err)
		}
		if _, exists := sources[source.ID]; exists {
			return fmt.Errorf("fixture: duplicate source %q", source.ID)
		}
		if source.Tenant == "" || source.Workspace == "" || source.SourceURI == "" || source.SourceType == "" || len(source.ACL) == 0 {
			return fmt.Errorf("fixture: source %q missing tenant/workspace/source/ACL metadata", source.ID)
		}
		if len(source.InitialBody)%applicationChunkSize != 0 || len(source.FinalBody)%applicationChunkSize != 0 || len(source.FinalBody)/applicationChunkSize < 2 {
			return fmt.Errorf("fixture: source %q has unstable chunk shape", source.ID)
		}
		sources[source.ID] = source
		tenants[source.Tenant], workspaces[source.Workspace] = true, true
	}
	if len(tenants) < 2 || len(workspaces) < 2 {
		return fmt.Errorf("fixture: need at least two tenants and workspaces")
	}
	queries := make(map[string]bool, len(fixture.Queries))
	for _, query := range fixture.Queries {
		if query.ID == "" || strings.TrimSpace(query.Text) == "" || queries[query.ID] {
			return fmt.Errorf("fixture: invalid query %q", query.ID)
		}
		queries[query.ID] = true
	}
	seen := make(map[string]bool, len(fixture.Judgments))
	duplicateHeavy := false
	for _, judgment := range fixture.Judgments {
		key := judgment.QueryID + "\x00" + judgment.Filter
		if !queries[judgment.QueryID] || seen[key] || !containsString(applicationFilterOrder, judgment.Filter) {
			return fmt.Errorf("fixture: invalid or duplicate judgment %q/%q", judgment.QueryID, judgment.Filter)
		}
		seen[key] = true
		if len(judgment.RelevantChunks) == 0 || len(judgment.RelevantParents) == 0 {
			return fmt.Errorf("fixture: degenerate judgment %q/%q", judgment.QueryID, judgment.Filter)
		}
		parentSet := make(map[string]bool, len(judgment.RelevantParents))
		for _, parent := range judgment.RelevantParents {
			source, ok := sources[parent]
			if !ok || source.Deleted {
				return fmt.Errorf("fixture: judgment references absent parent %q", parent)
			}
			if err := validateSourceAuthorized(source, judgment.Filter); err != nil {
				return fmt.Errorf("fixture: %s judgment parent %q: %w", judgment.Filter, parent, err)
			}
			parentSet[parent] = true
		}
		perParent := map[string]int{}
		for _, child := range judgment.RelevantChunks {
			parent, ordinal, ok := chunking.ParseChildID(child)
			if !ok || ordinal >= applicationChunksPerSource || !parentSet[parent] {
				return fmt.Errorf("fixture: judgment child %q does not belong to declared parent set", child)
			}
			perParent[parent]++
			if perParent[parent] >= 2 {
				duplicateHeavy = true
			}
		}
	}
	if len(seen) != len(fixture.Queries)*len(applicationFilterOrder) {
		return fmt.Errorf("fixture: missing query/filter judgments: got %d want %d", len(seen), len(fixture.Queries)*len(applicationFilterOrder))
	}
	if !duplicateHeavy {
		return fmt.Errorf("fixture: no duplicate-heavy parent judgment")
	}
	return nil
}

func validateSourceAuthorized(source applicationSource, filter string) error {
	switch filter {
	case filterUnfiltered:
		return nil
	case filterTenantAlpha:
		if source.Tenant != "alpha" {
			return fmt.Errorf("tenant=%q want alpha", source.Tenant)
		}
	case filterTenantAlphaWorkspaceRed:
		if source.Tenant != "alpha" {
			return fmt.Errorf("tenant=%q want alpha", source.Tenant)
		}
		if source.Workspace != "red" {
			return fmt.Errorf("workspace=%q want red", source.Workspace)
		}
	case filterModerateRange:
		if source.Tenant != "alpha" {
			return fmt.Errorf("tenant=%q want alpha", source.Tenant)
		}
		if source.Workspace != "red" {
			return fmt.Errorf("workspace=%q want red", source.Workspace)
		}
		if source.UpdatedYear < 2024 {
			return fmt.Errorf("updated_year=%d want >=2024", source.UpdatedYear)
		}
	default:
		return fmt.Errorf("unknown filter %q", filter)
	}
	return nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

//go:embed testdata/semantic_vectors.json
var semanticVectorsJSON []byte

type semanticVectorBundle struct {
	Schema            string               `json:"schema"`
	Model             string               `json:"model"`
	Revision          string               `json:"revision"`
	License           string               `json:"license"`
	Preprocessing     string               `json:"preprocessing"`
	Dimensions        int                  `json:"dimensions"`
	CorpusLicense     string               `json:"corpus_license"`
	GenerationCommand string               `json:"generation_command"`
	Vectors           map[string][]float32 `json:"vectors"`
	Queries           map[string][]float32 `json:"queries"`
}

func loadSemanticVectors() (*semanticVectorBundle, error) {
	var bundle semanticVectorBundle
	if err := json.Unmarshal(semanticVectorsJSON, &bundle); err != nil {
		return nil, fmt.Errorf("semantic vectors: decode: %w", err)
	}
	return &bundle, nil
}

func (b *semanticVectorBundle) Digest() string {
	raw, _ := json.Marshal(b)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validateSemanticVectors(fixture *applicationFixture, bundle *semanticVectorBundle) error {
	if bundle == nil || bundle.Schema != "treedb-rag-semantic-vectors/v1" || bundle.Model != "sentence-transformers/all-MiniLM-L6-v2" || bundle.Revision != "1110a243fdf4706b3f48f1d95db1a4f5529b4d41" || bundle.License != "Apache-2.0" || bundle.Dimensions != 384 {
		return fmt.Errorf("semantic vectors: provenance or dimensions mismatch")
	}
	for _, source := range fixture.Sources {
		for _, body := range []string{source.InitialBody, source.FinalBody} {
			for ordinal := range len(body) / applicationChunkSize {
				text := body[ordinal*applicationChunkSize : (ordinal+1)*applicationChunkSize]
				vector, ok := bundle.Vectors[text]
				if !ok || len(vector) != bundle.Dimensions {
					return fmt.Errorf("semantic vectors: missing/wrong-width child vector source=%q ordinal=%d", source.ID, ordinal)
				}
			}
		}
	}
	for _, query := range fixture.Queries {
		vector, ok := bundle.Queries[query.ID]
		if !ok || len(vector) != bundle.Dimensions {
			return fmt.Errorf("semantic vectors: missing/wrong-width query vector %q", query.ID)
		}
	}
	return nil
}
