package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func refreshTestM3VariantIdentityV1(t testing.TB, descriptor *m3VariantDescriptorV1) {
	t.Helper()
	descriptor.OverlapRequested = int(math.Floor(descriptor.OverlapRatio * float64(descriptor.SourceRows)))
	descriptor.OverlapRealized = descriptor.OverlapMemberships
	descriptor.OverlapRejected = descriptor.OverlapRequested - descriptor.OverlapRealized
	descriptor.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(*descriptor)
	descriptor.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{
		Capacity: uint64(descriptor.Capacity), Budget: uint64(math.Floor(descriptor.OverlapRatio * float64(descriptor.SourceRows))),
		Realized: uint64(descriptor.OverlapMemberships), Unspent: uint64(descriptor.OverlapRejected),
		BuildIdentityDigest: descriptor.BuildIdentityDigest,
	})
}

func TestM8ProductionMatrixRequiresLikeForLikeVariantsAndOverlapStorageV1(t *testing.T) {
	hash := strings.Repeat("a", 40)
	fixture := fixtureManifest{Checksum: strings.Repeat("b", 64)}
	cfg := config{baseSHA: hash, headSHA: hash, partitions: 16, command: []string{"bench"}}
	common := m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, Probes: []int{4, 16}, TopK: 10, RecallTarget: .9, Concurrency: []int{1}, Warmup: 1, EfSearch: []int{128}, RouterCandidates: 1024, Seed: 1}
	pass := m8ProductionGateLedgerV1{ExhaustiveParity: "pass", FailureHonesty: "pass", PartitionPackReachability: "pass", Recall: "pass", ProbeReduction: "pass", EndToEndQPS: "pass", TailLatency: "pass", Balance: "pass", ResourceBounds: "pass"}
	reports := make([]m8ProductionReportV1, 0, 3)
	for _, variant := range []struct {
		id, assignment string
		overlap        float64
		bytes          uint64
	}{
		{"graph-disjoint-v1", partitionAssignmentGraphV1, 0, 100},
		{"graph-overlap-020-v1", partitionAssignmentGraphV1, .2, 120},
		{"stable-id-hash-disjoint-v1", partitionAssignmentStableIDHashV1, 0, 101},
	} {
		descriptor := testM3VariantDescriptorV1(t.TempDir())
		descriptor.VariantID, descriptor.AssignmentBasis, descriptor.OverlapRatio = variant.id, variant.assignment, variant.overlap
		descriptor.SourceRows, descriptor.OverlapMemberships = 8, 0
		if variant.assignment == partitionAssignmentStableIDHashV1 {
			descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
		}
		if variant.overlap > 0 {
			descriptor.OverlapMemberships = 1
		}
		refreshTestM3VariantIdentityV1(t, &descriptor)
		config := common
		config.Overlap = []float64{variant.overlap}
		variantGates := pass
		if variant.assignment == partitionAssignmentStableIDHashV1 {
			variantGates.Recall, variantGates.ProbeReduction, variantGates.EndToEndQPS, variantGates.TailLatency = "fail", "fail", "fail", "fail"
		}
		reports = append(reports, m8ProductionReportV1{
			BaseSHA: hash, HeadSHA: hash, Dataset: fixture, Config: config, Variant: &descriptor, GateLedger: variantGates,
			Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: variant.bytes},
			Rows: []m8ProductionRowV1{
				{Status: "pass", VariantID: variant.id, Probes: 16, EfSearch: 128, Concurrency: 1, Samples: 32, RecallAtK: 1, QPS: 100, P95Nanos: 100, Attribution: m8ProductionAttributionV1{ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true}},
				{Status: "pass", VariantID: variant.id, Probes: 4, EfSearch: 128, Concurrency: 1, Samples: 32, RecallAtK: .95, QPS: 116, P95Nanos: 99},
			},
		})
	}
	matrix, err := m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.Status != "local_gate_pass" || matrix.Gates.PartitionPackReachability != "pass" || matrix.Gates.OverlapStorage != "pass" || matrix.OverlapStorageRatio != 1.2 || len(matrix.Comparison) != 6 {
		t.Fatalf("matrix=%+v", matrix)
	}
	for _, reachability := range []string{"", "fail"} {
		reports[0].GateLedger.PartitionPackReachability = reachability
		matrix, err = m8BuildProductionMatrixV1(cfg, fixture, reports)
		if err != nil {
			t.Fatal(err)
		}
		if matrix.Status != "experimental_gate_failures" || matrix.Gates.PartitionPackReachability != "fail" {
			t.Fatalf("reachability=%q matrix=%+v", reachability, matrix)
		}
	}
	reports[0].GateLedger.PartitionPackReachability = "pass"
	reports[0].Dirty = true
	if _, err := m8BuildProductionMatrixV1(cfg, fixture, reports); err == nil || !strings.Contains(err.Error(), "dirty") {
		t.Fatalf("dirty child report err=%v", err)
	}
	reports[0].Dirty = false
	reports[0].GateLedger.TailLatency = "fail"
	reports[1].GateLedger.Recall = "fail"
	reports[0].Rows[1].P95Nanos = 101
	reports[1].Rows[1].QPS = 114
	matrix, err = m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.Status != "experimental_gate_failures" || matrix.Gates.Recall != "pass" || matrix.Gates.TailLatency != "pass" || matrix.Gates.CoupledGraph != "fail" {
		t.Fatalf("split graph acceptance matrix=%+v", matrix)
	}
	reports[0].GateLedger.TailLatency = "pass"
	reports[1].GateLedger.Recall = "pass"
	reports[0].Rows[1].P95Nanos = 99
	reports[1].Rows[1].QPS = 116
	reports[1].Resources.PersistentAssetBytes = 135
	matrix, err = m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.Status != "experimental_gate_failures" || matrix.Gates.OverlapStorage != "fail" {
		t.Fatalf("storage failure matrix=%+v", matrix)
	}
	reports[2].Config.TopK = 11
	if _, err := m8BuildProductionMatrixV1(cfg, fixture, reports); err == nil {
		t.Fatal("accepted non-like-for-like variant configuration")
	}
}

func TestM8CoupledGraphGateRequiresOneMatchedOperatingPointV1(t *testing.T) {
	report := m8ProductionReportV1{
		Variant: &m3VariantDescriptorV1{AssignmentBasis: partitionAssignmentGraphV1},
		Config:  m8ProductionConfigEvidenceV1{Partitions: 16, RecallTarget: .9},
		Rows: []m8ProductionRowV1{
			{Status: "pass", Probes: 16, EfSearch: 64, Concurrency: 1, RecallAtK: .95, QPS: 100, P95Nanos: 100, Attribution: m8ProductionAttributionV1{ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true}},
			{Status: "pass", Probes: 4, EfSearch: 64, Concurrency: 1, RecallAtK: .95, QPS: 116, P95Nanos: 101},
			{Status: "pass", Probes: 16, EfSearch: 128, Concurrency: 1, RecallAtK: .95, QPS: 100, P95Nanos: 100, Attribution: m8ProductionAttributionV1{ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true}},
			{Status: "pass", Probes: 4, EfSearch: 128, Concurrency: 1, RecallAtK: .95, QPS: 114, P95Nanos: 99},
		},
	}
	if got := m8AnyGraphVariantCoupledGatesPassV1([]m8ProductionReportV1{report}); got != "fail" {
		t.Fatalf("split operating-point gate=%q want fail", got)
	}
	report.Rows[3].QPS = 116
	if got := m8AnyGraphVariantCoupledGatesPassV1([]m8ProductionReportV1{report}); got != "pass" {
		t.Fatalf("coupled operating-point gate=%q want pass", got)
	}
	report.Rows[2].Status = "fail"
	report.Rows[2].ExactParityPassed = false
	report.Rows[2].RecallAtK = .95
	if got := m8AnyGraphVariantCoupledGatesPassV1([]m8ProductionReportV1{report}); got != "fail" {
		t.Fatalf("failed exhaustive baseline must be excluded: gate=%q want fail", got)
	}
}

func TestM8AllPartitionANNDoesNotOwnExactParityV1(t *testing.T) {
	report := m8ProductionReportV1{
		Config: m8ProductionConfigEvidenceV1{Partitions: 16, RecallTarget: .9},
		Rows: []m8ProductionRowV1{{
			Status: "pass", Probes: 16, EfSearch: 64, Concurrency: 1, Samples: 1,
			// The measured all-partition HNSW path is approximate: it may miss a
			// neighbor without turning the exact-union correctness gate into fail.
			RecallAtK: .9998, ExactParityChecked: false, ExactParityPassed: false,
			Attribution: m8ProductionAttributionV1{ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true, ExhaustivePartitionRecallAtK: 1},
		}},
	}
	if got := m8ProductionGateLedgerForReportV1(report).ExhaustiveParity; got != "pass" {
		t.Fatalf("all-partition ANN conflated with exact union: %q", got)
	}
}

func TestM8TruthCacheIdentityMismatchFailsClosedV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("a", 64), Vectors: 1, Dimensions: 128, Metric: "cosine"}
	identity := m8TruthCacheIdentityV1(fixture, 10)
	dir := t.TempDir()
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: strings.Repeat("b", 64), Dimensions: 128, Metric: "cosine", TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "m8_canonical_truth_"+identity+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{}, fixture, [][]float64{{1}}, 10); err == nil || !strings.Contains(err.Error(), "identity/schema mismatch") {
		t.Fatalf("cache mismatch err=%v", err)
	}
}

func TestM8TruthCacheOversizedInputFailsBeforeDecodeV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("a", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	path := filepath.Join(dir, "m8_canonical_truth_"+identity+".json")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 70<<10)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{}, fixture, [][]float64{{1, 0}}, 1); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized cache err=%v", err)
	}
}

func TestM8TruthCacheHitReusesCanonicalRowsV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("c", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	want := [][]m8CanonicalResultV1{{{ID: "doc-000000", Score: .5}}}
	truthSHA256, err := m8TruthContentSHA256V1(want)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: collections.VectorPartitionCanonicalScoreContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 1, TruthSHA256: truthSHA256, Truth: want})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "m8_canonical_truth_"+identity+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	got, evidence, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1)
	if err != nil || evidence.Status != "reused" || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v evidence=%+v err=%v", got, evidence, err)
	}
}

func TestM8TruthCacheRejectsTrailingJSONV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("f", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	truth := [][]m8CanonicalResultV1{{{ID: "doc-000000", Score: .5}}}
	digest, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 1, TruthSHA256: digest, Truth: truth})
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, []byte(" {}")...)
	if err := os.WriteFile(filepath.Join(dir, "m8_canonical_truth_"+identity+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1); err == nil || !strings.Contains(err.Error(), "trailing JSON") {
		t.Fatalf("trailing JSON err=%v", err)
	}
}

func TestM8TruthCacheRefusesContentCorruptionAndSemanticMalformationV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("d", 64), Vectors: 2, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 2), t.TempDir()
	path := filepath.Join(dir, "m8_canonical_truth_"+identity+".json")
	good := [][]m8CanonicalResultV1{{{ID: "doc-000000", Score: .9}, {ID: "doc-000001", Score: .8}}}
	contentSHA, err := m8TruthContentSHA256V1(good)
	if err != nil {
		t.Fatal(err)
	}
	file := m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: collections.VectorPartitionCanonicalScoreContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 2, TruthSHA256: contentSHA, Truth: good}
	write := func(t *testing.T, file m8TruthCacheFileV1) {
		t.Helper()
		raw, marshalErr := json.Marshal(file)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if writeErr := os.WriteFile(path, raw, 0o644); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	write(t, file)
	file.Truth[0][0].Score = .7 // JSON remains valid but the content digest does not.
	write(t, file)
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 2}, fixture, [][]float64{{1, 0}}, 2); err == nil || !strings.Contains(err.Error(), "truth_sha256") {
		t.Fatalf("content corruption err=%v", err)
	}
	file.Truth[0][0].Score = .7
	file.Truth[0][1].Score = .8 // ascending scores are semantically malformed.
	file.TruthSHA256, err = m8TruthContentSHA256V1(file.Truth)
	if err != nil {
		t.Fatal(err)
	}
	write(t, file)
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 2}, fixture, [][]float64{{1, 0}}, 2); err == nil || !strings.Contains(err.Error(), "noncanonical") {
		t.Fatalf("semantic malformation err=%v", err)
	}
}

func TestM8TruthCacheRefusesIDsOutsideFixtureDomainV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("e", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	truth := [][]m8CanonicalResultV1{{{ID: "doc-000001", Score: .5}}}
	digest, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 1, TruthSHA256: digest, Truth: truth})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "m8_canonical_truth_"+identity+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1); err == nil || !strings.Contains(err.Error(), "outside deterministic fixture domain") {
		t.Fatalf("out-of-range cache ID err=%v", err)
	}
}

func TestCommittedV1QualificationLedgerArtifactsV1(t *testing.T) {
	type publishedArtifact struct {
		Role   string `json:"role"`
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	}
	type ledger struct {
		SchemaVersion    int                 `json:"schema_version"`
		Status           string              `json:"status"`
		BlockingFollowUp string              `json:"blocking_follow_up"`
		ParentAcceptance string              `json:"parent_acceptance"`
		Gates            map[string]string   `json:"gates"`
		Artifacts        []publishedArtifact `json:"raw_artifacts"`
		Commands         struct {
			Canonical string `json:"canonical"`
		} `json:"commands"`
	}
	root := filepath.Join("..", "..")
	ledgerPath := filepath.Join(root, "TreeDB", "docs", "spec", "artifacts", "vector-partition-v1-qualification-4015.json")
	raw, err := os.ReadFile(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	var got ledger
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || got.Status != "experimental_off_gate_failures_not_1m_qualified" || got.BlockingFollowUp != "#4022" || got.ParentAcceptance != "https://github.com/snissn/gomap/issues/4012#issuecomment-5123955990" {
		t.Fatalf("ledger linkage/schema/status=%+v", got)
	}
	if got.Gates["quarter_probe_recall_ge_090"] == "pass" || got.Gates["required_1m"] == "pass" || !strings.HasPrefix(got.Gates["required_1m"], "deferred") {
		t.Fatalf("ledger incorrectly permits qualification: gates=%v", got.Gates)
	}
	for _, required := range []string{"-mode", "-dataset", "-out", "-m8-existing-db", "-m8-max-exact-truth-visits"} {
		if !strings.Contains(got.Commands.Canonical, required) {
			t.Fatalf("canonical replay command omits %s: %s", required, got.Commands.Canonical)
		}
	}
	if len(got.Artifacts) != 13 {
		t.Fatalf("published artifact count=%d want 13", len(got.Artifacts))
	}
	for _, artifact := range got.Artifacts {
		if artifact.Role == "" || artifact.Path == "" || artifact.SHA256 == "" || filepath.IsAbs(artifact.Path) {
			t.Fatalf("invalid published artifact=%+v", artifact)
		}
		content, err := os.ReadFile(filepath.Join(root, artifact.Path))
		if err != nil {
			t.Fatalf("read %s: %v", artifact.Path, err)
		}
		sum := sha256.Sum256(content)
		if hex.EncodeToString(sum[:]) != artifact.SHA256 {
			t.Fatalf("digest mismatch %s", artifact.Path)
		}
	}
}

func TestM8GitDirtyRequiresExternalOutputsAndPreservesSourceChangesV1(t *testing.T) {
	repo := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repo
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	runGit("init", "-q")
	runGit("config", "user.name", "M8 Test")
	runGit("config", "user.email", "m8@example.invalid")
	tracked := filepath.Join(repo, "tracked.txt")
	if err := os.WriteFile(tracked, []byte("clean\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.txt")
	runGit("commit", "-qm", "initial")
	artifactRoot := t.TempDir()
	out, profiles := filepath.Join(artifactRoot, "out"), filepath.Join(artifactRoot, "profiles")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "report.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if m8GitDirtyInV1(repo, out, profiles) {
		t.Fatal("declared benchmark output made clean repository dirty")
	}
	insideRoot := filepath.Join(repo, "TreeDB")
	if !m8GitDirtyInV1(repo, insideRoot) {
		t.Fatal("accepted an evidence root inside the source repository")
	}
	if !m8GitDirtyInV1(repo, repo) {
		t.Fatal("accepted the source repository itself as an evidence root")
	}
	if err := os.WriteFile(tracked, []byte("changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !m8GitDirtyInV1(repo, out, profiles) {
		t.Fatal("source change was hidden by benchmark output exclusions")
	}
}

func TestM8GitDirtyRejectsSymlinkedInRepositoryOutputV1(t *testing.T) {
	repo := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repo
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	runGit("init", "-q")
	runGit("config", "user.name", "M8 Test")
	runGit("config", "user.email", "m8@example.invalid")
	if err := os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("clean\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.txt")
	runGit("commit", "-qm", "initial")
	link := filepath.Join(t.TempDir(), "repo-link")
	if err := os.Symlink(repo, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	out := filepath.Join(link, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "report.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !m8GitDirtyInV1(link, out, filepath.Join(link, "profiles")) {
		t.Fatal("accepted a symlinked evidence root inside the source repository")
	}
}

func TestM8VariantDBsParseStrictThreePathsV1(t *testing.T) {
	base := []string{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4"}
	cfg, err := parseConfig(append(append([]string(nil), base...), "-m8-variant-dbs", "/a,/b,/c"))
	if err != nil || len(cfg.m8VariantDBs) != 3 {
		t.Fatalf("variant paths=%v err=%v", cfg.m8VariantDBs, err)
	}
	for _, value := range []string{"/a,/b", "/a,/a,/c", "/a,/b,/c,/d"} {
		if _, err := parseConfig(append(append([]string(nil), base...), "-m8-variant-dbs", value)); err == nil {
			t.Fatalf("accepted malformed variant paths %q", value)
		}
	}
	if _, err := parseConfig(append(append([]string(nil), base...), "positional")); err == nil {
		t.Fatal("accepted positional argument")
	}
}

func TestM8MatrixParentDoesNotMaterializeFixtureV1(t *testing.T) {
	cfg := config{m8VariantDBs: []string{"/a", "/b", "/c"}}
	fixture := fixtureManifest{Vectors: maxVectors, Queries: maxVectors, Dimensions: 4096}
	vectors, queries, err := m8ProductionFixtureDataV1(cfg, fixture)
	if err != nil || vectors != nil || queries != nil {
		t.Fatalf("matrix parent fixture data vectors=%v queries=%v err=%v", vectors, queries, err)
	}
}

func TestM8VariantProcessArgsForceFreshSingleVariantV1(t *testing.T) {
	command := []string{"treedb_vector_partition_bench", "-mode", m8ProductionMultiGroupModeV1, "-dataset", "format", "-m8-variant-dbs", "/a,/b,/c", "-overlap=.1", "-format", "text", "-profiles", "/old", "-m8-matrix-out", "/old-out", "-m8-matrix-profiles", "/old-profiles"}
	got, err := m8VariantProcessArgsV1(command, "/variant", .2, "/profiles/variant", "/matrix-out", "/matrix-profiles")
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"-m8-existing-db", "/variant", "-overlap", "0.2", "-format", "json", "-profiles", "/profiles/variant", "-m8-matrix-out", "/matrix-out", "-m8-matrix-profiles", "/matrix-profiles"}
	if len(got) < len(wantPrefix) || !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("child args=%v want prefix=%v", got, wantPrefix)
	}
	for _, arg := range got {
		if strings.HasPrefix(arg, "-m8-variant-dbs") || strings.HasPrefix(arg, "--m8-variant-dbs") || arg == "/a,/b,/c" || arg == "/old" || arg == "/old-out" || arg == "/old-profiles" {
			t.Fatalf("child args retained matrix/old-profile argument: %v", got)
		}
	}
	if !slices.Contains(got, "format") {
		t.Fatalf("child args dropped positional value matching a filtered flag: %v", got)
	}
}

func TestM8ProductionMatrixRejectsUnderMaterializedOverlapDescriptorV1(t *testing.T) {
	hash := strings.Repeat("a", 40)
	fixture := fixtureManifest{Checksum: strings.Repeat("b", 64)}
	cfg := config{baseSHA: hash, headSHA: hash, partitions: 16, command: []string{"bench"}}
	common := m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, Probes: []int{4}, TopK: 10, RecallTarget: .9, Concurrency: []int{1}, Warmup: 1, EfSearch: []int{128}, RouterCandidates: 1024, Seed: 1}
	pass := m8ProductionGateLedgerV1{ExhaustiveParity: "pass", FailureHonesty: "pass", PartitionPackReachability: "pass", Recall: "pass", ProbeReduction: "pass", EndToEndQPS: "pass", TailLatency: "pass", Balance: "pass", ResourceBounds: "pass"}
	reports := make([]m8ProductionReportV1, 0, 3)
	for _, variant := range []struct {
		id, assignment string
		overlap        float64
		memberships    int
	}{
		{"graph-disjoint-v1", partitionAssignmentGraphV1, 0, 0},
		{"graph-overlap-020-v1", partitionAssignmentGraphV1, .2, 1},
		{"stable-id-hash-disjoint-v1", partitionAssignmentStableIDHashV1, 0, 0},
	} {
		descriptor := testM3VariantDescriptorV1(t.TempDir())
		descriptor.VariantID, descriptor.AssignmentBasis, descriptor.OverlapRatio = variant.id, variant.assignment, variant.overlap
		descriptor.SourceRows, descriptor.OverlapMemberships = 10, variant.memberships
		if variant.assignment == partitionAssignmentStableIDHashV1 {
			descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
		}
		refreshTestM3VariantIdentityV1(t, &descriptor)
		config := common
		config.Overlap = []float64{variant.overlap}
		reports = append(reports, m8ProductionReportV1{
			BaseSHA: hash, HeadSHA: hash, Dataset: fixture, Config: config, Variant: &descriptor, GateLedger: pass,
			Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: 100},
			Rows:      []m8ProductionRowV1{{Status: "pass", VariantID: variant.id, Probes: 4, EfSearch: 128, Concurrency: 1, Samples: 1}},
		})
	}
	matrix, err := m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err == nil || !strings.Contains(err.Error(), "malformed M3 variant descriptor") {
		t.Fatalf("under-materialized overlap err=%v matrix=%+v", err, matrix)
	}
}

func TestM8MatrixIdentityIncludesComparisonAndResourceCapsV1(t *testing.T) {
	cfg := config{baseSHA: strings.Repeat("b", 40), headSHA: strings.Repeat("a", 40), m8MaxRSSBytes: 100, m8MaxAssetBytes: 200}
	descriptors := []m3VariantDescriptorV1{testM3VariantDescriptorV1(t.TempDir())}
	one, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	cfg.m8MaxRSSBytes++
	two, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if one == two {
		t.Fatal("matrix identity ignored the configured RSS acceptance bound")
	}
	cfg.m8MaxRSSBytes--
	cfg.m8MaxAssetBytes++
	three, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if one == three {
		t.Fatal("matrix identity ignored the configured persistent-asset acceptance bound")
	}
	cfg.m8MaxAssetBytes--
	cfg.baseSHA = strings.Repeat("c", 40)
	four, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if one == four {
		t.Fatal("matrix identity ignored the comparison base SHA")
	}
	cfg.baseSHA = strings.Repeat("b", 40)
	descriptors[0].DatabaseDirectory = "/relocated/variant"
	five, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if one != five {
		t.Fatal("matrix content identity changed after directory-only relocation")
	}
}

func TestM8VariantBuildCompatibilityRejectsMixedRetainedBuildsV1(t *testing.T) {
	makeVariants := func() []m3VariantDescriptorV1 {
		variants := make([]m3VariantDescriptorV1, 0, 3)
		for _, item := range []struct {
			id, assignment string
			overlap        float64
		}{
			{"graph-disjoint-v1", partitionAssignmentGraphV1, 0},
			{"graph-overlap-020-v1", partitionAssignmentGraphV1, .2},
			{"stable-id-hash-disjoint-v1", partitionAssignmentStableIDHashV1, 0},
		} {
			descriptor := testM3VariantDescriptorV1(t.TempDir())
			descriptor.VariantID, descriptor.AssignmentBasis, descriptor.OverlapRatio = item.id, item.assignment, item.overlap
			descriptor.OverlapMemberships = int(math.Floor(item.overlap * float64(descriptor.SourceRows)))
			if item.assignment == partitionAssignmentStableIDHashV1 {
				descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
			}
			refreshTestM3VariantIdentityV1(t, &descriptor)
			variants = append(variants, descriptor)
		}
		return variants
	}
	if err := m8ValidateVariantBuildCompatibilityV1(makeVariants()); err != nil {
		t.Fatalf("compatible graph/stable build rejected: %v", err)
	}
	for name, mutate := range map[string]func([]m3VariantDescriptorV1){
		"graph digest": func(variants []m3VariantDescriptorV1) { variants[2].GraphArtifactSHA256 = strings.Repeat("d", 64) },
		"graph assignment artifact": func(variants []m3VariantDescriptorV1) {
			variants[1].ArtifactSHA256 = strings.Repeat("d", 64)
			variants[1].GraphArtifactSHA256 = variants[1].ArtifactSHA256
		},
		"source":             func(variants []m3VariantDescriptorV1) { variants[2].Source.SourceID = "different" },
		"index definition":   func(variants []m3VariantDescriptorV1) { variants[2].IndexDefinitionDigest = strings.Repeat("d", 64) },
		"local HNSW M":       func(variants []m3VariantDescriptorV1) { variants[2].PartitionHNSWM-- },
		"graph router model": func(variants []m3VariantDescriptorV1) { variants[1].RouterModelDigest = strings.Repeat("d", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			variants := makeVariants()
			mutate(variants)
			for i := range variants {
				refreshTestM3VariantIdentityV1(t, &variants[i])
			}
			if err := m8ValidateVariantBuildCompatibilityV1(variants); err == nil {
				t.Fatal("accepted mixed retained variant builds")
			}
		})
	}
}
