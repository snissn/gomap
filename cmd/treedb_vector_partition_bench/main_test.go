package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func fixturePath(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "testdata", "vector_partition_10k")
}

func runWithHermeticProvenance(t *testing.T, args []string, stdout io.Writer) error {
	t.Helper()
	t.Setenv("BASE_SHA", strings.Repeat("a", 40))
	t.Setenv("GITHUB_SHA", strings.Repeat("b", 40))
	t.Setenv("GITHUB_EVENT_PATH", filepath.Join(t.TempDir(), "must-not-be-read.json"))
	return run(args, stdout)
}

func writeFixtureForTest(t *testing.T, vectors, queries, dims int) string {
	t.Helper()
	m, _, _ := smallFixtureForTest(vectors, queries, dims)
	m.Checksum = fixtureChecksum(vectors, queries, dims, m.Seed)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fixture_manifest.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func smallFixtureForTest(vectors, queries, dims int) (fixtureManifest, [][]float64, [][]float64) {
	m := fixtureManifest{
		SchemaVersion: schemaVersion,
		Fixture:       "small",
		Generator:     "treedb_vector_partition_fixture_v1",
		Vectors:       vectors,
		Queries:       queries,
		Dimensions:    dims,
		Metric:        "cosine",
		Seed:          1,
		Checksum:      strings.Repeat("0", 64),
	}
	v, q := deterministicFixture(m)
	return m, v, q
}

func TestFixtureTruthDeterministicAndChecksumStable(t *testing.T) {
	m, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateFixture(m); err != nil {
		t.Fatal(err)
	}
	a, aq := deterministicFixture(m)
	if got := fixtureChecksumFromData(a, aq); got != m.Checksum {
		t.Fatalf("checksum=%s want %s", got, m.Checksum)
	}
	b, bq := deterministicFixture(m)
	if len(a) != len(b) || len(aq) != len(bq) || a[0][0] != b[0][0] || aq[0][0] != bq[0][0] {
		t.Fatal("fixture is not deterministic")
	}
}

func TestFixtureChecksumSupportsFewerThanTenVectors(t *testing.T) {
	for vectors := 1; vectors < 10; vectors++ {
		first := fixtureChecksum(vectors, 3, 4, 1)
		second := fixtureChecksum(vectors, 3, 4, 1)
		if first != second || len(first) != 64 {
			t.Fatalf("vectors=%d checksum first=%q second=%q", vectors, first, second)
		}
	}
}

func TestTruthOracleTieOrderingAndAllPartitionParity(t *testing.T) {
	m, _ := loadFixture(fixturePath(t))
	vectors, queries := deterministicFixture(m)
	_ = vectors
	_ = queries
	got := exactTopK([][]float64{{1, 0}, {1, 0}, {0, 1}}, []float64{1, 0}, 2)
	if got[0].ID != "doc-000000" || got[1].ID != "doc-000001" {
		t.Fatalf("ties not ordered by stable ID: %#v", got[:2])
	}
	cfg := config{partitions: 4, topK: 10, recallTarget: .9, seed: 1, stages: stageSet("exact_global_top_k,partition_oracle")}
	r, err := simulate(cfg, m, vectors, queries, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Stages[1].RecallAtK != 1 {
		t.Fatalf("all partition oracle recall=%f", r.Stages[1].RecallAtK)
	}
	if r.Stages[0].RecallAtK != r.Stages[1].RecallAtK {
		t.Fatal("oracle parity differs")
	}
	for i, query := range queries {
		want := exactTopK(vectors, query, cfg.topK)
		got := partitionTopK(vectors, query, cfg.topK, cfg.partitions, cfg.partitions)
		if !orderedEqual(want, got) {
			t.Fatalf("query %d all-partition ordered ID+distance mismatch:\nwant=%+v\ngot=%+v", i, want, got)
		}
	}
}

func TestEveryLossStageIndependentlyLabeled(t *testing.T) {
	m, vectors, queries := smallFixtureForTest(32, 4, 8)
	hnsw, err := newTreeDBPartitionHNSW(vectors, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hnsw.Close() }()
	for _, name := range knownStages {
		cfg := config{partitions: 4, topK: 4, recallTarget: .9, seed: 1, stages: stageSet(name)}
		if name == "treedb_partition_local_hnsw" {
			cfg.hnsw = hnsw
		}
		r, err := simulate(cfg, m, vectors, queries, 1, .2)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(r.Stages) != 1 || r.Stages[0].Name != name || !r.Stages[0].Enabled || !r.Stages[0].Available {
			t.Fatalf("%s independently reported stages=%+v", name, r.Stages)
		}
		if r.ResultKind != "simulation_only" || r.ProductionEvidence {
			t.Fatalf("%s simulation label=%+v", name, r)
		}
	}
}

func TestSingleEnabledStageIsTheOnlyReportedStage(t *testing.T) {
	m, _ := loadFixture(fixturePath(t))
	v, q := deterministicFixture(m)
	r, err := simulate(config{partitions: 4, topK: 10, recallTarget: .9, seed: 1, stages: stageSet("partition_oracle")}, m, v, q, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Stages) != 1 || r.Stages[0].Name != "partition_oracle" {
		t.Fatalf("stages=%+v", r.Stages)
	}
}

func TestTreeDBHNSWStageUsesExactSearchPackAndMatchesHighEFLocalTruth(t *testing.T) {
	m, vectors, queries := smallFixtureForTest(48, 6, 8)
	hnsw, err := newTreeDBPartitionHNSW(vectors, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hnsw.Close() }()

	allPartitions := []int{0, 1, 2, 3}
	for queryIndex, query := range queries {
		got, err := hnsw.search(queryIndex, query, allPartitions, 6)
		if err != nil {
			t.Fatalf("query %d: %v", queryIndex, err)
		}
		want := exactTopK(vectors, query, 6)
		if len(got.Neighbors) != len(want) {
			t.Fatalf("query %d neighbors=%d want %d", queryIndex, len(got.Neighbors), len(want))
		}
		for i := range want {
			if got.Neighbors[i].ID != want[i].ID || math.Abs(got.Neighbors[i].Distance-want[i].Distance) > 1e-5 {
				t.Fatalf("query %d rank %d HNSW=%+v exact=%+v", queryIndex, i, got.Neighbors[i], want[i])
			}
		}
		if got.Evidence.SearchRouteHNSWSearchPack != 4 || got.Evidence.HNSWSearchPackActive != 4 || got.Evidence.HNSWSearchPackFallbacks != 0 {
			t.Fatalf("query %d route evidence=%+v", queryIndex, got.Evidence)
		}
	}

	cfg := config{
		partitions:   4,
		topK:         6,
		recallTarget: .9,
		seed:         1,
		stages:       stageSet("treedb_partition_local_hnsw"),
		hnsw:         hnsw,
	}
	first, err := simulate(cfg, m, vectors, queries, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	physicalAfterFirst := hnsw.physicalSearches
	second, err := simulate(cfg, m, vectors, queries, 2, .2)
	if err != nil {
		t.Fatal(err)
	}
	if hnsw.physicalSearches != physicalAfterFirst {
		t.Fatalf("overlap-only row repeated HNSW work: before=%d after=%d", physicalAfterFirst, hnsw.physicalSearches)
	}
	if first.Stages[0].ExecutedSearches == 0 || first.Stages[0].CachedSearches != 0 {
		t.Fatalf("first HNSW row evidence=%+v", first.Stages[0])
	}
	if second.Stages[0].ExecutedSearches != 0 || second.Stages[0].CachedSearches != second.Stages[0].Searches {
		t.Fatalf("cached HNSW row evidence=%+v", second.Stages[0])
	}

	beforeDifferentSet := hnsw.physicalSearches
	if _, err := hnsw.search(0, queries[0], []int{0}, 6); err != nil {
		t.Fatal(err)
	}
	afterFirstSet := hnsw.physicalSearches
	if _, err := hnsw.search(0, queries[0], []int{1}, 6); err != nil {
		t.Fatal(err)
	}
	if afterFirstSet <= beforeDifferentSet || hnsw.physicalSearches <= afterFirstSet {
		t.Fatal("same-sized selected partition sets aliased in HNSW cache")
	}
	dir := hnsw.dir
	if err := hnsw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("temporary HNSW DB was not removed: stat error=%v", err)
	}
}

func TestMalformedCapAndFiniteInputsRejectBeforeSimulation(t *testing.T) {
	for _, raw := range [][]string{{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "0", "-probes", "1"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "5"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-overlap", "NaN"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-recall-target", "NaN"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-stages", "unknown"}} {
		if _, err := parseConfig(raw); err == nil {
			t.Fatalf("accepted malformed config %#v", raw)
		}
	}
	bad := fixtureManifest{SchemaVersion: 1, Fixture: "bad", Generator: "treedb_vector_partition_fixture_v1", Vectors: maxVectors + 1, Queries: 1, Dimensions: 1, Metric: "cosine", Checksum: strings.Repeat("0", 64)}
	if err := validateFixture(bad); err == nil {
		t.Fatal("accepted capped fixture")
	}
	bad.Vectors = 1
	bad.Dimensions = 1
	bad.Checksum = "bad"
	if err := validateFixture(bad); err == nil {
		t.Fatal("accepted malformed checksum")
	}
}

func TestRunRejectsTopKAboveFixtureBeforeOracleAllocation(t *testing.T) {
	err := runWithHermeticProvenance(t, []string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-top-k", "10001"}, io.Discard)
	if err == nil {
		t.Fatal("accepted top-k above fixture")
	}
}

func TestConfigurableFixtureCapsRejectBeforeAllocation(t *testing.T) {
	cfg, err := parseConfig([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-max-vectors", "9999"})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := loadFixture(fixturePath(t))
	if err := validateFixtureWithCaps(m, cfg.maxVectors, cfg.maxBytes); err == nil {
		t.Fatal("accepted fixture beyond configured count cap")
	}
	if _, err := parseConfig([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-max-fixture-bytes", "7"}); err == nil {
		t.Fatal("accepted unsafe byte cap")
	}
	m.Vectors = 6
	m.Queries = 5
	m.Dimensions = 1
	if err := validateFixtureWithCaps(m, 10, 1024); err == nil || !strings.Contains(err.Error(), "combined") {
		t.Fatalf("combined vector/query count cap error=%v", err)
	}
}

func TestModeledPeakMemoryBudgetEnforcedBeforeGeneration(t *testing.T) {
	const (
		vectors = 32
		queries = 4
		dims    = 8
	)
	dataset := writeFixtureForTest(t, vectors, queries, dims)
	m, err := loadFixture(dataset)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{
		partitions: 4,
		probes:     []int{1, 4},
		topK:       4,
		stages:     stageSet("all"),
	}
	plan, err := planBenchmarkMemory(cfg, m)
	if err != nil {
		t.Fatal(err)
	}
	rawFixtureBytes := int64(vectors+queries) * dims * 8
	if plan.FixtureResidentBytes <= rawFixtureBytes || plan.HNSWInsertWorkBytes == 0 || plan.HNSWCacheBytes == 0 || plan.ModeledPeakBytes <= plan.FixtureResidentBytes {
		t.Fatalf("incomplete memory plan: %+v raw_fixture=%d", plan, rawFixtureBytes)
	}
	rejectedOut := t.TempDir()
	err = runWithHermeticProvenance(t, []string{
		"-dataset", dataset,
		"-out", rejectedOut,
		"-partitions", "4",
		"-probes", "1,4",
		"-top-k", "4",
		"-max-fixture-bytes", strconv.FormatInt(plan.ModeledPeakBytes-1, 10),
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "modeled peak") {
		t.Fatalf("below-peak cap error=%v plan=%+v", err, plan)
	}
	entries, readErr := os.ReadDir(rejectedOut)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected memory budget emitted evidence: %+v", entries)
	}

	acceptedOut := t.TempDir()
	if err := runWithHermeticProvenance(t, []string{
		"-dataset", dataset,
		"-out", acceptedOut,
		"-partitions", "4",
		"-probes", "1,4",
		"-top-k", "4",
		"-max-fixture-bytes", strconv.FormatInt(plan.ModeledPeakBytes, 10),
	}, io.Discard); err != nil {
		t.Fatalf("exact modeled peak cap rejected: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(acceptedOut, artifactBasename(runResult{Seed: 1, Probes: 4, TopK: 4})+".json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.MemoryBudgetBytes != plan.ModeledPeakBytes || result.ModeledPeakBytes != plan.ModeledPeakBytes || result.MemoryBudgetScope != memoryBudgetScope {
		t.Fatalf("memory evidence=%+v plan=%+v", result, plan)
	}
}

func TestRunRejectsSeedThatDoesNotMatchFixtureBeforeEvidence(t *testing.T) {
	out := t.TempDir()
	err := runWithHermeticProvenance(t, []string{
		"-dataset", fixturePath(t),
		"-out", out,
		"-partitions", "4",
		"-probes", "1",
		"-top-k", "10",
		"-seed", "2",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "does not match fixture seed") {
		t.Fatalf("seed mismatch error=%v", err)
	}
	entries, readErr := os.ReadDir(out)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("seed mismatch emitted evidence: %+v", entries)
	}
}

func TestManifestRejectsTrailingJSON(t *testing.T) {
	d := t.TempDir()
	raw, err := os.ReadFile(filepath.Join(fixturePath(t), "fixture_manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "fixture_manifest.json"), append(raw, []byte("{}")...), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixture(d); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}

func TestManifestReadCapRejectsOversizedFile(t *testing.T) {
	d := t.TempDir()
	raw := bytes.Repeat([]byte(" "), int(maxManifestBytes)+1)
	if err := os.WriteFile(filepath.Join(d, "fixture_manifest.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixture(d); err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("oversized manifest error=%v", err)
	}
}

func TestFixtureChecksumMismatchFailsBeforeEvidence(t *testing.T) {
	d := t.TempDir()
	raw, err := os.ReadFile(filepath.Join(fixturePath(t), "fixture_manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m fixtureManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	m.Checksum = strings.Repeat("0", 64)
	raw, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "fixture_manifest.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	err = runWithHermeticProvenance(t, []string{"-dataset", d, "-out", out, "-partitions", "4", "-probes", "1", "-stages", "treedb_partition_local_hnsw"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum mismatch error=%v", err)
	}
	entries, readErr := os.ReadDir(out)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("checksum mismatch emitted evidence: %+v", entries)
	}
}

func TestProvenanceAutomaticEnvironmentAndInvalidSHA(t *testing.T) {
	t.Run("automatic", func(t *testing.T) {
		t.Setenv("BASE_SHA", "")
		t.Setenv("GITHUB_SHA", "")
		t.Setenv("GITHUB_EVENT_PATH", "")
		wantHead, err := exec.Command("git", "rev-parse", "HEAD").Output()
		if err != nil {
			t.Skip("local git provenance unavailable")
		}
		if _, err := exec.Command("git", "merge-base", "HEAD", "origin/main").Output(); err != nil {
			t.Skip("origin/main merge-base unavailable")
		}
		base, head, err := provenance()
		if err != nil {
			t.Fatal(err)
		}
		if head != strings.TrimSpace(string(wantHead)) || len(base) != 40 {
			t.Fatalf("automatic provenance base=%q head=%q want_head=%q", base, head, strings.TrimSpace(string(wantHead)))
		}
	})
	t.Run("environment", func(t *testing.T) {
		base := strings.Repeat("a", 40)
		head := strings.Repeat("b", 40)
		t.Setenv("BASE_SHA", base)
		t.Setenv("GITHUB_SHA", head)
		gotBase, gotHead, err := provenance()
		if err != nil {
			t.Fatal(err)
		}
		if gotBase != base || gotHead != head {
			t.Fatalf("environment provenance base=%q head=%q", gotBase, gotHead)
		}
	})
	t.Run("pull_request_event", func(t *testing.T) {
		base := strings.Repeat("c", 40)
		head := strings.Repeat("e", 40)
		path := filepath.Join(t.TempDir(), "event.json")
		raw := `{"pull_request":{"base":{"sha":"` + base + `"},"head":{"sha":"` + head + `"}},"ignored_by_typed_projection":true}`
		if err := os.WriteFile(path, []byte(raw), 0644); err != nil {
			t.Fatal(err)
		}
		t.Setenv("BASE_SHA", "")
		t.Setenv("GITHUB_SHA", strings.Repeat("d", 40))
		t.Setenv("GITHUB_EVENT_PATH", path)
		gotBase, gotHead, err := provenance()
		if err != nil {
			t.Fatal(err)
		}
		if gotBase != base || gotHead != head {
			t.Fatalf("event base/head=%q/%q want %q/%q", gotBase, gotHead, base, head)
		}
	})
	for _, tc := range []struct {
		name string
		base string
		head string
	}{
		{name: "base", base: "invalid", head: strings.Repeat("e", 40)},
		{name: "head", base: strings.Repeat("c", 40), head: "invalid"},
	} {
		t.Run("event_invalid_"+tc.name+"_sha", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "event.json")
			raw := `{"pull_request":{"base":{"sha":"` + tc.base + `"},"head":{"sha":"` + tc.head + `"}}}`
			if err := os.WriteFile(path, []byte(raw), 0644); err != nil {
				t.Fatal(err)
			}
			t.Setenv("BASE_SHA", "")
			t.Setenv("GITHUB_SHA", strings.Repeat("d", 40))
			t.Setenv("GITHUB_EVENT_PATH", path)
			if _, _, err := provenance(); err == nil {
				t.Fatalf("accepted invalid event %s SHA", tc.name)
			}
		})
	}
	for _, tc := range []struct {
		name string
		base string
		head string
	}{
		{name: "base", base: "not-a-sha", head: strings.Repeat("b", 40)},
		{name: "head", base: strings.Repeat("a", 40), head: strings.Repeat("g", 40)},
	} {
		t.Run("invalid_"+tc.name, func(t *testing.T) {
			t.Setenv("BASE_SHA", tc.base)
			t.Setenv("GITHUB_SHA", tc.head)
			if _, _, err := provenance(); err == nil {
				t.Fatal("accepted invalid SHA provenance")
			}
		})
	}
}

func TestGitHubEventPayloadIsBoundedAndSingleValue(t *testing.T) {
	oversized := filepath.Join(t.TempDir(), "oversized.json")
	if err := os.WriteFile(oversized, bytes.Repeat([]byte(" "), int(maxGitHubEventBytes)+1), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := pullRequestSHAsFromEvent(oversized); err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("oversized event error=%v", err)
	}
	trailing := filepath.Join(t.TempDir(), "trailing.json")
	if err := os.WriteFile(trailing, []byte(`{} {}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := pullRequestSHAsFromEvent(trailing); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing event error=%v", err)
	}
}

func TestCloseOverlapValuesHaveCollisionFreeArtifacts(t *testing.T) {
	a := 0.2
	b := math.Nextafter(a, 1)
	if artifactBasename(runResult{Probes: 1, Overlap: a, TopK: 10}) == artifactBasename(runResult{Probes: 1, Overlap: b, TopK: 10}) {
		t.Fatal("adjacent float64 overlap values collide")
	}
	if artifactBasename(runResult{Seed: 1, Probes: 1, Overlap: a, TopK: 10}) == artifactBasename(runResult{Seed: 2, Probes: 1, Overlap: a, TopK: 10}) {
		t.Fatal("different fixture seeds collide")
	}
	out := t.TempDir()
	if err := runWithHermeticProvenance(t, []string{
		"-dataset", fixturePath(t),
		"-partitions", "4",
		"-probes", "1",
		"-overlap", "0.2," + strconv.FormatFloat(b, 'g', -1, 64),
		"-top-k", "10",
		"-stages", "exact_global_top_k",
		"-format", "json",
		"-out", out,
	}, io.Discard); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("close overlap artifacts=%d want 4", len(entries))
	}
}

func TestRunWithExplicitProvenanceOutsideGitCheckout(t *testing.T) {
	dataset := writeFixtureForTest(t, 9, 3, 4)
	out := t.TempDir()
	t.Chdir(t.TempDir())
	if err := runWithHermeticProvenance(t, []string{
		"-dataset", dataset,
		"-partitions", "3",
		"-probes", "3",
		"-overlap", "0",
		"-top-k", "9",
		"-stages", "exact_global_top_k",
		"-format", "json",
		"-out", out,
	}, io.Discard); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(out, artifactBasename(runResult{Seed: 1, Probes: 3, TopK: 9})+".json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.BaseSHA != strings.Repeat("a", 40) || result.HeadSHA != strings.Repeat("b", 40) {
		t.Fatalf("explicit provenance not preserved: base=%q head=%q", result.BaseSHA, result.HeadSHA)
	}
}

func TestCanonicalRunWritesJSONAndMarkdown(t *testing.T) {
	out := t.TempDir()
	var stdout bytes.Buffer
	if err := runWithHermeticProvenance(t, []string{"-dataset", fixturePath(t), "-partitions", "4", "-probes", "1,2,4", "-overlap", "0,0.20", "-top-k", "10", "-recall-target", "0.90", "-seed", "1", "-format", "json", "-out", out}, &stdout); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 12 {
		t.Fatalf("artifacts=%d want 12", len(entries))
	}
	raw, err := os.ReadFile(filepath.Join(out, artifactBasename(runResult{Seed: 1, Probes: 4, Overlap: .2, TopK: 10})+".json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.Command[0] != "treedb_vector_partition_bench" || result.Metrics.MeasurementStatus != "simulation_not_measured" || result.Metrics.SelectedPartitions != 4 {
		t.Fatalf("artifact metadata=%+v", result)
	}
	var hnsw stageResult
	for _, stage := range result.Stages {
		if stage.Name == "treedb_partition_local_hnsw" {
			hnsw = stage
		}
	}
	if !hnsw.Available || hnsw.RouteKind != string(collections.VectorIndexSearchRouteExactHNSWSearchPackV1) || hnsw.SearchRouteHNSWSearchPack == 0 || hnsw.HNSWSearchPackFallbacks != 0 {
		t.Fatalf("HNSW stage evidence=%+v", hnsw)
	}
	for _, field := range []string{`"executed_searches": 0`, `"hnsw_search_pack_fallbacks": 0`} {
		if !bytes.Contains(raw, []byte(field)) {
			t.Fatalf("artifact omits explicit zero HNSW evidence field %s", field)
		}
	}
	md, err := os.ReadFile(filepath.Join(out, artifactBasename(runResult{Seed: 1, Probes: 4, Overlap: .2, TopK: 10})+".md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(md), "Simulation only; not production Raft evidence.") {
		t.Fatal("missing simulation disclaimer")
	}
	if !strings.Contains(string(md), "exact_hnsw_search_pack_v1") || !strings.Contains(string(md), "cached=") {
		t.Fatal("missing separately attributed HNSW route/cache evidence")
	}
}

func TestResultDecoderRejectsUnknownVersionAndTrailingValue(t *testing.T) {
	r := runResult{SchemaVersion: 99, ResultKind: "simulation_only", Stages: []stageResult{{Method: "x"}}, Metrics: metricsV1{MeasurementStatus: "simulation_not_measured"}}
	b, _ := json.Marshal(r)
	if _, err := decodeResult(b); err == nil {
		t.Fatal("accepted unknown schema")
	}
	r.SchemaVersion = 1
	b, _ = json.Marshal(r)
	if _, err := decodeResult(append(b, []byte("{}")...)); err == nil {
		t.Fatal("accepted trailing result")
	}
}

func TestValidateResultRejectsNonFiniteMetrics(t *testing.T) {
	r := runResult{SchemaVersion: 1, ResultKind: "simulation_only", Stages: []stageResult{{RecallAtK: math.NaN()}}}
	if err := validateResult(r); err == nil {
		t.Fatal("accepted nonfinite metric")
	}
}

func TestHNSWEvidenceJSONIncludesExplicitZeroCounters(t *testing.T) {
	raw, err := json.Marshal(stageResult{
		Name:      "treedb_partition_local_hnsw",
		Method:    "test",
		Enabled:   true,
		Available: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		`"searches":0`,
		`"executed_searches":0`,
		`"cached_searches":0`,
		`"search_route_hnsw_search_pack":0`,
		`"hnsw_search_pack_active":0`,
		`"hnsw_search_pack_fallbacks":0`,
	} {
		if !bytes.Contains(raw, []byte(field)) {
			t.Fatalf("explicit zero field %s absent from %s", field, raw)
		}
	}
}

func TestValidateResultRequiresExactSHASeedAndMemoryEvidence(t *testing.T) {
	r := runResult{
		SchemaVersion:     schemaVersion,
		ResultKind:        "simulation_only",
		BaseSHA:           strings.Repeat("a", 40),
		HeadSHA:           strings.Repeat("b", 40),
		Dataset:           fixtureManifest{Seed: 1},
		Seed:              1,
		MemoryBudgetBytes: 1024,
		ModeledPeakBytes:  512,
		MemoryBudgetScope: memoryBudgetScope,
		Stages:            []stageResult{{Name: "exact_global_top_k", Method: "test", Enabled: true, Available: true}},
		Metrics:           metricsV1{MeasurementStatus: "simulation_not_measured"},
	}
	if err := validateResult(r); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	for _, mutate := range []func(*runResult){
		func(result *runResult) { result.BaseSHA = "abcdef0" },
		func(result *runResult) { result.HeadSHA = strings.Repeat("g", 40) },
		func(result *runResult) { result.Seed++ },
		func(result *runResult) { result.ModeledPeakBytes = result.MemoryBudgetBytes + 1 },
	} {
		bad := r
		mutate(&bad)
		if err := validateResult(bad); err == nil {
			t.Fatalf("accepted invalid result: %+v", bad)
		}
	}
}
