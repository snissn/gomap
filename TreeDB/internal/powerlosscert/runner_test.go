package powerlosscert

import (
	"os"
	"os/exec"
	"path/filepath"
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
		Seed:             3674,
		CutID:            "cut/checkpoint/after-meta-write/002",
		CutPoint:         "after-meta-write",
		VariantID:        "variant/target-meta",
		ReopenMode:       reopenModeReadOnly,
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
		ReopenMode:       reopenModeReadWrite,
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
