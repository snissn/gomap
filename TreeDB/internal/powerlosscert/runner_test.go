package powerlosscert

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolveRunnerPathsRequiresOutputDisjointFromRepository(t *testing.T) {
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, output := range map[string]string{
		"repository":          repo,
		"inside-repository":   filepath.Join(repo, "evidence"),
		"contains-repository": filepath.Dir(repo),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, _, err := resolveRunnerPaths(RunnerConfig{
				RepoRoot: repo, InventoryPath: "inventory.json", PlanPath: "plan.json", OutputRoot: output,
			})
			if err == nil || !strings.Contains(err.Error(), "must be disjoint") {
				t.Fatalf("resolveRunnerPaths output=%q error=%v", output, err)
			}
		})
	}
	disjoint := filepath.Join(t.TempDir(), "evidence")
	if _, _, _, _, err := resolveRunnerPaths(RunnerConfig{
		RepoRoot: repo, InventoryPath: "inventory.json", PlanPath: "plan.json", OutputRoot: disjoint,
	}); err != nil {
		t.Fatalf("resolveRunnerPaths disjoint output: %v", err)
	}
}

func TestRequireExactCleanRepositoryRejectsUntrackedSource(t *testing.T) {
	repo := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	runGit("init")
	runGit("config", "user.email", "powerlosscert@example.invalid")
	runGit("config", "user.name", "powerlosscert test")
	if err := os.WriteFile(filepath.Join(repo, "tracked.go"), []byte("package fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.go")
	runGit("commit", "-m", "fixture")
	head := runGit("rev-parse", "HEAD")
	runGit("update-ref", CertifiedRepositoryRef, head)
	if err := requireExactCleanRepository(repo, CertifiedRepositoryRef, head); err != nil {
		t.Fatalf("clean repository: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "untracked.go"), []byte("package fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireExactCleanRepository(repo, CertifiedRepositoryRef, head); err == nil || !strings.Contains(err.Error(), "untracked") {
		t.Fatalf("untracked source error=%v", err)
	}
}

func TestRequireExactCleanRepositoryRejectsHeadNotAtCertifiedMain(t *testing.T) {
	repo := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	runGit("init")
	runGit("config", "user.email", "powerlosscert@example.invalid")
	runGit("config", "user.name", "powerlosscert test")
	if err := os.WriteFile(filepath.Join(repo, "tracked.go"), []byte("package fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.go")
	runGit("commit", "-m", "main")
	mainSHA := runGit("rev-parse", "HEAD")
	runGit("update-ref", CertifiedRepositoryRef, mainSHA)
	if err := os.WriteFile(filepath.Join(repo, "tracked.go"), []byte("package changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.go")
	runGit("commit", "-m", "feature")
	featureSHA := runGit("rev-parse", "HEAD")
	if err := requireExactCleanRepository(repo, CertifiedRepositoryRef, featureSHA); err == nil || !strings.Contains(err.Error(), "certified ref") {
		t.Fatalf("non-main exact head error=%v", err)
	}
}

func TestRequireExactCleanRepositoryIgnoresInheritedGitRepositoryOverrides(t *testing.T) {
	repo := t.TempDir()
	runGit := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	runGit(repo, "init")
	runGit(repo, "config", "user.email", "powerlosscert@example.invalid")
	runGit(repo, "config", "user.name", "powerlosscert test")
	if err := os.WriteFile(filepath.Join(repo, "tracked.go"), []byte("package fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(repo, "add", "tracked.go")
	runGit(repo, "commit", "-m", "fixture")
	head := runGit(repo, "rev-parse", "HEAD")
	runGit(repo, "update-ref", CertifiedRepositoryRef, head)

	decoy := filepath.Join(t.TempDir(), "decoy")
	runGit(t.TempDir(), "clone", repo, decoy)
	runGit(decoy, "update-ref", CertifiedRepositoryRef, head)
	if err := os.WriteFile(filepath.Join(repo, "untracked.go"), []byte("package fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_DIR", filepath.Join(decoy, ".git"))
	t.Setenv("GIT_WORK_TREE", decoy)
	t.Setenv("GIT_INDEX_FILE", filepath.Join(decoy, ".git", "index"))
	if err := requireExactCleanRepository(repo, CertifiedRepositoryRef, head); err == nil || !strings.Contains(err.Error(), "untracked") {
		t.Fatalf("inherited Git repository overrides bypassed source-tree validation: %v", err)
	}
}

func TestRequirePullRequestProvenanceBindsClaimsToCertifiedGraph(t *testing.T) {
	repo := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	writeSource := func(contents string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(repo, "tracked.go"), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	runGit("init", "-b", "main")
	runGit("config", "user.email", "powerlosscert@example.invalid")
	runGit("config", "user.name", "powerlosscert test")
	writeSource("package fixture\n")
	runGit("add", "tracked.go")
	runGit("commit", "-m", "base")
	baseSHA := runGit("rev-parse", "HEAD")
	runGit("checkout", "-b", "feature")
	writeSource("package fixture\n\nconst Feature = true\n")
	runGit("add", "tracked.go")
	runGit("commit", "-m", "feature head")
	headSHA := runGit("rev-parse", "HEAD")
	runGit("checkout", "main")
	runGit("merge", "--squash", "feature")
	runGit("commit", "-m", "Feature (#42)")
	mergeSHA := runGit("rev-parse", "HEAD")

	valid := []PullRequest{{Number: 42, HeadSHA: headSHA, MergeSHA: mergeSHA}}
	if err := requirePullRequestProvenance(repo, mergeSHA, valid); err != nil {
		t.Fatalf("valid squash-merge provenance: %v", err)
	}

	tests := []struct {
		name          string
		repositorySHA string
		pullRequests  []PullRequest
		want          string
	}{
		{
			name:          "missing-head",
			repositorySHA: mergeSHA,
			pullRequests:  []PullRequest{{Number: 42, HeadSHA: strings.Repeat("f", 40), MergeSHA: mergeSHA}},
			want:          "is not an exact commit",
		},
		{
			name:          "wrong-pr-number",
			repositorySHA: mergeSHA,
			pullRequests:  []PullRequest{{Number: 43, HeadSHA: headSHA, MergeSHA: mergeSHA}},
			want:          "does not end in (#43)",
		},
		{
			name:          "unrelated-head-tree",
			repositorySHA: mergeSHA,
			pullRequests:  []PullRequest{{Number: 42, HeadSHA: baseSHA, MergeSHA: mergeSHA}},
			want:          "neither a merge parent nor tree-identical",
		},
		{
			name:          "merge-not-reachable-from-repository",
			repositorySHA: baseSHA,
			pullRequests:  valid,
			want:          "is not an ancestor",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := requirePullRequestProvenance(repo, tt.repositorySHA, tt.pullRequests); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("requirePullRequestProvenance error=%v want substring %q", err, tt.want)
			}
		})
	}
}

func TestCertificationExecutableResolutionIgnoresInheritedPATH(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell executable shadow fixture is Unix-specific")
	}
	shadowDir := t.TempDir()
	for _, name := range []string{"git", "go"} {
		if err := os.WriteFile(filepath.Join(shadowDir, name), []byte("#!/bin/sh\nexit 97\n"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", shadowDir)
	t.Setenv("GOROOT", "")

	gitBinary, err := resolveGitBinary()
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(gitBinary) == shadowDir {
		t.Fatalf("resolved inherited PATH Git shim %q", gitBinary)
	}
	goBinary, err := resolveGoBinary("go")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(goBinary) == shadowDir {
		t.Fatalf("resolved inherited PATH Go shim %q", goBinary)
	}
}

func TestDefaultGoResolutionRejectsInheritedGOROOT(t *testing.T) {
	t.Setenv("GOROOT", filepath.Join(t.TempDir(), "shadow-goroot"))
	if _, err := resolveGoBinary("go"); err == nil || !strings.Contains(err.Error(), "inherited GOROOT") {
		t.Fatalf("default Go resolution with inherited GOROOT error=%v", err)
	}
}

func TestBuildTestBinariesUsesPrivateExactSHACheckout(t *testing.T) {
	repo := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	runGit("init")
	runGit("config", "user.email", "powerlosscert@example.invalid")
	runGit("config", "user.name", "powerlosscert test")
	files := map[string]string{
		".gitignore":                 "fixturepkg/ignored_override_test.go\n",
		"go.mod":                     "module powerlosscert-fixture\n\ngo 1.25\n",
		"fixturepkg/fixture.go":      "package fixturepkg\n\nfunc Value() int { return 1 }\n",
		"fixturepkg/fixture_test.go": "package fixturepkg\n\nimport \"testing\"\n\nfunc TestValue(t *testing.T) { if Value() != 1 { t.Fatal(Value()) } }\n",
	}
	for path, contents := range files {
		fullPath := filepath.Join(repo, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runGit("add", ".")
	runGit("commit", "-m", "fixture")
	head := runGit("rev-parse", "HEAD")
	runGit("update-ref", CertifiedRepositoryRef, head)
	ignoredSource := filepath.Join(repo, "fixturepkg", "ignored_override_test.go")
	if err := os.WriteFile(ignoredSource, []byte("package fixturepkg\n\nfunc ignoredInputDoesNotCompile(\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireExactCleanRepository(repo, CertifiedRepositoryRef, head); err != nil {
		t.Fatalf("ignored source should demonstrate the status gap: %v", err)
	}

	goBinary, err := resolveGoBinary(filepath.Join(runtime.GOROOT(), "bin", "go"+executableSuffix()))
	if err != nil {
		t.Fatal(err)
	}
	directOutput := t.TempDir()
	if err := os.MkdirAll(filepath.Join(directOutput, "binaries"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := buildTestBinaries(repo, directOutput, goBinary, []RunCase{{Package: "./fixturepkg"}}); err == nil {
		t.Fatal("ignored source fixture did not affect a direct worktree build")
	}

	sourceRoot, cleanup, err := createExactSourceCheckout(repo, CertifiedRepositoryRef, head)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Errorf("cleanup exact-SHA checkout: %v", err)
		}
	}()
	if _, err := os.Stat(filepath.Join(sourceRoot, "fixturepkg", "ignored_override_test.go")); !os.IsNotExist(err) {
		t.Fatalf("private exact-SHA checkout contains ignored source: %v", err)
	}
	output := t.TempDir()
	if err := os.MkdirAll(filepath.Join(output, "binaries"), 0o755); err != nil {
		t.Fatal(err)
	}
	artifacts, paths, err := buildTestBinaries(sourceRoot, output, goBinary, []RunCase{{Package: "./fixturepkg"}})
	if err != nil {
		t.Fatalf("build from private exact-SHA checkout: %v", err)
	}
	if artifacts["./fixturepkg"].SHA256 == "" {
		t.Fatal("private exact-SHA build omitted binary digest")
	}
	if _, err := os.Stat(paths["./fixturepkg"]); err != nil {
		t.Fatalf("private exact-SHA build output: %v", err)
	}
}

func TestRequireCommittedRiskInventoryRejectsReducedCopy(t *testing.T) {
	repo := t.TempDir()
	path := filepath.Join(repo, "TreeDB", "testdata", "power_loss_risk_inventory.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	committed := []byte(`{"schema_version":"treedb-power-loss-risk-inventory/v1","dimensions":{}}`)
	if err := os.WriteFile(path, committed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireCommittedRiskInventory(repo, committed); err != nil {
		t.Fatal(err)
	}
	if err := requireCommittedRiskInventory(repo, []byte(`{"schema_version":"treedb-power-loss-risk-inventory/v1"}`)); err == nil || !strings.Contains(err.Error(), "byte-identical") {
		t.Fatalf("reduced inventory error=%v", err)
	}
}

func TestBoundedBufferCapsRetainedBytesAndCountsDiscardedBytes(t *testing.T) {
	buffer := boundedBuffer{limit: 4}
	written, err := buffer.Write([]byte("123456"))
	if err != nil {
		t.Fatal(err)
	}
	if written != 6 || buffer.total != 6 || !buffer.overflow || buffer.String() != "1234" {
		t.Fatalf("bounded buffer written=%d total=%d overflow=%t retained=%q", written, buffer.total, buffer.overflow, buffer.String())
	}
}

func TestCertificationRuntimeEnvironmentDoesNotInheritProcessConfiguration(t *testing.T) {
	t.Setenv("GOFLAGS", "-overlay=attacker.json")
	t.Setenv("TREEDB_PROFILE", "cached")
	environment := certificationRuntimeEnvironment(map[string]string{"TREEDB_POWERLOSS_CASE_ID": "case-a"})
	if environment["GOFLAGS"] != "" || environment["GOENV"] != "off" || environment["GOTOOLCHAIN"] != "local" || environment["GOWORK"] != "off" {
		t.Fatalf("unsafe Go environment: %+v", environment)
	}
	if _, ok := environment["TREEDB_PROFILE"]; ok {
		t.Fatalf("runtime environment inherited unrelated TreeDB configuration: %+v", environment)
	}
	if environment["TREEDB_POWERLOSS_CASE_ID"] != "case-a" {
		t.Fatalf("runtime environment omitted explicit replay selector: %+v", environment)
	}
}

func TestValidateExecutedRecoveryMatchesFrozenExpectation(t *testing.T) {
	runCase := RunCase{
		ID:               "accepted-read-only",
		Profile:          "command_wal_durable",
		Seed:             3674,
		CutID:            "cut/checkpoint/after-meta-write/002",
		CutPoint:         "after-meta-write",
		VariantID:        "variant/target-meta",
		ReopenMode:       powerLossReopenModeReadOnly,
		ExpectedRecovery: RecoveryExpectation{CommitSeq: 11, AppliedLSN: 7},
	}
	trace := operationTraceArtifact{
		CutID:              runCase.CutID,
		VariantID:          runCase.VariantID,
		Seed:               "3674",
		DeclaredCutPoint:   runCase.CutPoint,
		ObservedEventCount: 3,
	}
	recovery := recoveryTraceArtifact{
		ReadOnly: true, CommitSeq: 11, AppliedLSN: 7,
		Stats: map[string]string{
			"treedb.profile.resolved":                 "command_wal_durable",
			"treedb.commit_seq":                       "11",
			"treedb.applied_command_lsn":              "7",
			"treedb.durable_root.selected_slot":       "1",
			"treedb.durable_root.commit_seq":          "11",
			"treedb.durable_root.durable_seq":         "11",
			"treedb.durable_root.freelist.generation": "3",
			"treedb.durable_root.manifest.entries":    "2",
			"treedb.durable_root.slot0.commit_seq":    "10",
			"treedb.durable_root.slot1.commit_seq":    "11",
			"treedb.command_wal.durable_wal_lsn":      "7",
		},
	}
	runCase.State = observedWitnessState(recovery)
	if err := validateExecutedRecovery(runCase, trace, recovery); err != nil {
		t.Fatal(err)
	}
}

func TestValidateExecutedRecoveryRejectsPostHocOutcomeChange(t *testing.T) {
	runCase := RunCase{
		ID:               "rejected-read-write",
		Seed:             3674,
		CutID:            "cut/checkpoint/after-meta-write/000",
		CutPoint:         "after-meta-write",
		VariantID:        "variant/torn-meta",
		ReopenMode:       powerLossReopenModeReadWrite,
		ExpectedRecovery: RecoveryExpectation{Rejected: true, ErrorType: "*treedb.CorruptionError"},
	}
	trace := operationTraceArtifact{
		CutID:              runCase.CutID,
		VariantID:          runCase.VariantID,
		Seed:               "3674",
		DeclaredCutPoint:   runCase.CutPoint,
		ObservedEventCount: 1,
	}
	recovery := recoveryTraceArtifact{Rejected: false}
	err := validateExecutedRecovery(runCase, trace, recovery)
	if err == nil || !strings.Contains(err.Error(), "want=(rejected=true") {
		t.Fatalf("validateExecutedRecovery error=%v", err)
	}
}
