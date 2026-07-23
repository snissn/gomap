package powerlosscert

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const testRepositorySHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestCommittedRiskInventoryIsValid(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "power_loss_risk_inventory.json"))
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := ParseRiskInventory(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRiskInventory(inventory); err != nil {
		t.Fatal(err)
	}
}

func TestParseChildManifestRejectsUnknownFields(t *testing.T) {
	data := []byte(`{"schema_version":"treedb-power-loss-child/v3","unexpected":true}`)
	if _, err := ParseChildManifest(data); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("ParseChildManifest error=%v, want unknown-field rejection", err)
	}
}

func TestValidateBundleRejectsStaleSHAAndDuplicateWitnessIDs(t *testing.T) {
	inventory := testRiskInventory()
	manifest := testChildManifest("witness-a")
	manifest.RepositorySHA = strings.Repeat("b", 40)
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "stale repository_sha") {
		t.Fatalf("ValidateBundle stale SHA error=%v", err)
	}

	manifest = testChildManifest("witness-a")
	duplicate := testChildManifest("witness-a")
	duplicate.ManifestID = "dur-02"
	duplicate.Issue = 3675
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest, duplicate}); err == nil || !strings.Contains(err.Error(), "duplicate witness id") {
		t.Fatalf("ValidateBundle duplicate error=%v", err)
	}
}

func TestValidateBundleRejectsPriorChildManifestSchema(t *testing.T) {
	manifest := testChildManifest("witness-a")
	manifest.SchemaVersion = "treedb-power-loss-child/v2"
	if err := ValidateBundle(testRepositorySHA, testRiskInventory(), []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("ValidateBundle prior child schema error=%v", err)
	}
}

func TestValidateBundleBindsReplayWindowToCommandVariant(t *testing.T) {
	manifest := testChildManifest("witness-a")
	witness := &manifest.Witnesses[0]
	witness.ReplayWindow = "variant-b"
	witness.Command.Env[powerLossReplayWindowEnv] = witness.ReplayWindow
	if err := ValidateBundle(testRepositorySHA, testRiskInventory(), []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "does not match command variant") {
		t.Fatalf("ValidateBundle replay-window variant binding error=%v", err)
	}
}

func TestValidateBundleRejectsModeledEvidenceReuseAcrossWitnesses(t *testing.T) {
	manifest := testChildManifest("witness-a")
	reused := manifest.Witnesses[0]
	reused.ID = "witness-b"
	manifest.Witnesses = append(manifest.Witnesses, reused)

	err := ValidateBundle(testRepositorySHA, testRiskInventory(), []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "reuses modeled evidence directory") {
		t.Fatalf("ValidateBundle modeled evidence reuse error=%v", err)
	}
}

func TestValidateBundleRejectsPassingWitnessWithoutDeclaredCutOrArtifacts(t *testing.T) {
	inventory := testRiskInventory()
	manifest := testChildManifest("witness-a")
	manifest.Witnesses[0].CutExercised = false
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "did not exercise its declared cut") {
		t.Fatalf("ValidateBundle cut error=%v", err)
	}

	manifest = testChildManifest("witness-a")
	manifest.Witnesses[0].Artifacts = nil
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "no hashed artifacts") {
		t.Fatalf("ValidateBundle artifact error=%v", err)
	}

	manifest = testChildManifest("witness-a")
	manifest.Witnesses[0].ObservedEventCount = 0
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "observed event count") {
		t.Fatalf("ValidateBundle observed-cut error=%v", err)
	}

	manifest = testChildManifest("witness-a")
	manifest.Witnesses[0].Artifacts = manifest.Witnesses[0].Artifacts[:1]
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "missing required artifact kind") {
		t.Fatalf("ValidateBundle artifact-kind error=%v", err)
	}

	manifest = testChildManifest("witness-a")
	for index := range manifest.Witnesses[0].Artifacts {
		manifest.Witnesses[0].Artifacts[index].Path = "artifacts/one-file-for-every-kind.json"
	}
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "reuses artifact path") {
		t.Fatalf("ValidateBundle artifact-path reuse error=%v", err)
	}
}

func TestValidateBundleAcceptsZeroBasedFirstCutOccurrence(t *testing.T) {
	inventory := testRiskInventory()
	manifest := testChildManifest("witness-a")
	witness := &manifest.Witnesses[0]
	witness.CutID = "cut/checkpoint-generation-2/after-meta-write/000"
	witness.CutOccurrence = 0
	witness.ObservedEventCount = 1
	witness.Command.Env["TREEDB_POWERLOSS_CUT_ID"] = witness.CutID
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err != nil {
		t.Fatalf("ValidateBundle first zero-based occurrence: %v", err)
	}

	witness.ObservedEventCount = 0
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "observed event count") {
		t.Fatalf("ValidateBundle unobserved first occurrence error=%v", err)
	}
}

func TestValidateBundleRequiresModeledReopenMode(t *testing.T) {
	inventory := testRiskInventory()
	for _, mode := range []string{"", "reader-ish"} {
		manifest := testChildManifest("witness-a")
		manifest.Witnesses[0].Command.Env[powerLossReopenModeEnv] = mode
		if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), powerLossReopenModeEnv) {
			t.Fatalf("ValidateBundle reopen mode=%q error=%v", mode, err)
		}
	}
}

func TestValidateBundleRecoveryDirectoryContract(t *testing.T) {
	for _, dir := range []string{"", "recovery-input", "recovery-input/db"} {
		manifest := testChildManifest("witness-a")
		manifest.Witnesses[0].ExpectedRecoveryDir = dir
		if err := ValidateBundle(testRepositorySHA, testRiskInventory(), []ChildManifest{manifest}); err != nil {
			t.Fatalf("ValidateBundle dir=%q: %v", dir, err)
		}
	}
	for _, dir := range []string{"/recovery-input/db", "../db", "recovery-input/../db", "recovery-input//db", `recovery-input\db`, "other/db"} {
		manifest := testChildManifest("witness-a")
		manifest.Witnesses[0].ExpectedRecoveryDir = dir
		if err := ValidateBundle(testRepositorySHA, testRiskInventory(), []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "recovery directory") {
			t.Fatalf("ValidateBundle dir=%q error=%v", dir, err)
		}
	}
}

func TestValidateChildManifestScopesCutMetadataToModeledCrashes(t *testing.T) {
	manifest := testChildManifest("witness-a")
	for _, tier := range []EvidenceTier{EvidenceTierCleanProcess, EvidenceTierBlockDevice} {
		corroborating := manifest.Witnesses[0]
		corroborating.ID = string(tier) + "-witness"
		corroborating.EvidenceTier = tier
		corroborating.Seed = 0
		corroborating.CutID = ""
		corroborating.CutPoint = ""
		corroborating.CutOccurrence = 0
		corroborating.ObservedEventCount = 0
		corroborating.CutExercised = false
		manifest.Witnesses = append(manifest.Witnesses, corroborating)
	}

	if err := validateChildManifest(manifest); err != nil {
		t.Fatalf("validateChildManifest clean-process witness: %v", err)
	}
}

func TestValidateChildManifestAllowsRepeatedOrderedCommandArguments(t *testing.T) {
	manifest := testChildManifest("witness-a")
	manifest.Witnesses[0].Command.Args = []string{"-test.run", "first", "-test.run", "second"}

	if err := validateChildManifest(manifest); err != nil {
		t.Fatalf("validateChildManifest repeated command arguments: %v", err)
	}
}

func TestValidateBundleRejectsUnclassifiedModeledOutcome(t *testing.T) {
	manifest := testChildManifest("witness-a")
	manifest.Witnesses[0].ExpectedOutcome = "old-root"
	manifest.Witnesses[0].ActualOutcome = "old-root"

	err := ValidateBundle(testRepositorySHA, testRiskInventory(), []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "accepted or rejected public-open outcome") {
		t.Fatalf("ValidateBundle outcome class error=%v", err)
	}
}

func TestBuildCoverageReportRequiresModeledCrashOwnership(t *testing.T) {
	inventory := testRiskInventory()
	manifest := testChildManifest("witness-a")
	report, err := BuildCoverageReport(inventory, []ChildManifest{manifest})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Complete || len(report.Gaps) != 0 {
		t.Fatalf("complete report=%+v", report)
	}

	manifest.Witnesses[0].EvidenceTier = EvidenceTierCleanProcess
	report, err = BuildCoverageReport(inventory, []ChildManifest{manifest})
	if err != nil {
		t.Fatal(err)
	}
	if report.Complete || len(report.Gaps) == 0 {
		t.Fatalf("clean/process evidence incorrectly satisfied modeled-crash inventory: %+v", report)
	}
}

func TestSelectRepresentativeCasesIsDeterministicAndKeepsMandatoryRows(t *testing.T) {
	inventory := testRiskInventory()
	manifest := testChildManifest("broad")
	mandatory := manifest.Witnesses[0]
	mandatory.ID = "mandatory-counterexample"
	mandatory.CounterexampleID = "counterexample-a"
	mandatory.NegativeControlID = "negative-a"

	narrow := mandatory
	narrow.ID = "narrow"
	narrow.CounterexampleID = ""
	narrow.NegativeControlID = ""
	narrow.ResourceShapes = nil
	narrow.StorageBoundaries = nil
	narrow.FailureClasses = nil

	manifest.Witnesses = []Witness{narrow, mandatory}
	first, err := SelectRepresentativeCases(inventory, []ChildManifest{manifest})
	if err != nil {
		t.Fatal(err)
	}
	secondManifest := manifest
	secondManifest.Witnesses = []Witness{mandatory, narrow}
	second, err := SelectRepresentativeCases(inventory, []ChildManifest{secondManifest})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("selection depends on manifest order:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if got, want := first.CaseIDs, []string{"mandatory-counterexample"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected cases=%v want=%v", got, want)
	}
}

func TestValidateBundleRejectsUndeclaredRiskValuesAndUnownedInteractions(t *testing.T) {
	inventory := testRiskInventory()
	manifest := testChildManifest("witness-a")
	manifest.Witnesses[0].ResourceShapes = append(manifest.Witnesses[0].ResourceShapes, "undeclared-shape")
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "undeclared resource_shape") {
		t.Fatalf("ValidateBundle undeclared value error=%v", err)
	}

	manifest = testChildManifest("witness-a")
	inventory.RequiredInteractions[0].Members[1].Value = "unowned-boundary"
	inventory.Dimensions[DimensionStorageBoundary] = append(inventory.Dimensions[DimensionStorageBoundary], "unowned-boundary")
	if err := ValidateBundle(testRepositorySHA, inventory, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "interaction:") {
		t.Fatalf("ValidateBundle interaction gap error=%v", err)
	}
}

func TestValidateBundleRejectsArtifactPathAliases(t *testing.T) {
	t.Run("artifact kinds", func(t *testing.T) {
		manifest := testChildManifest("witness-a")
		manifest.Witnesses[0].Artifacts[1].Path = "artifacts/witness-a/subdir/../operation_trace.json"

		err := ValidateBundle(testRepositorySHA, testRiskInventory(), []ChildManifest{manifest})
		if err == nil || !strings.Contains(err.Error(), "non-canonical path") {
			t.Fatalf("ValidateBundle artifact path alias error=%v", err)
		}
	})
	t.Run("test binary", func(t *testing.T) {
		manifest := testChildManifest("witness-a")
		manifest.Witnesses[0].Artifacts[0].Path = "bin/subdir/../TreeDB.test"

		err := ValidateBundle(testRepositorySHA, testRiskInventory(), []ChildManifest{manifest})
		if err == nil || !strings.Contains(err.Error(), "non-canonical path") {
			t.Fatalf("ValidateBundle test-binary path alias error=%v", err)
		}
	})
}

func TestValidateArtifactRejectsStandaloneNonCanonicalPath(t *testing.T) {
	artifact := Artifact{
		Kind:   ArtifactKindMetrics,
		Path:   "artifacts/witness-a/subdir/../metrics.json",
		SHA256: strings.Repeat("a", 64),
	}

	if err := validateArtifact("test artifact", artifact); err == nil || !strings.Contains(err.Error(), "non-canonical") {
		t.Fatalf("validateArtifact non-canonical path error=%v", err)
	}
}

func testRiskInventory() RiskInventory {
	return RiskInventory{
		SchemaVersion: RiskInventorySchemaVersion,
		Dimensions: map[string][]string{
			DimensionProfile:          {"command_wal_durable"},
			DimensionAcknowledgement:  {"checkpoint"},
			DimensionResourceShape:    {"forced-value-log-pointer"},
			DimensionStorageBoundary:  {"target-meta-write-sync"},
			DimensionWritebackVariant: {"target-metadata-alone"},
			DimensionFailureClass:     {"fallback-older-complete-root"},
		},
		RetainedCounterexamples:  []string{"counterexample-a"},
		RequiredNegativeControls: []string{"negative-a"},
		RequiredInteractions: []RequiredInteraction{{
			ID: "checkpoint-target-meta",
			Members: []CoverageRequirement{
				{Dimension: DimensionAcknowledgement, Value: "checkpoint"},
				{Dimension: DimensionStorageBoundary, Value: "target-meta-write-sync"},
			},
		}},
	}
}

func testChildManifest(witnessID string) ChildManifest {
	return ChildManifest{
		SchemaVersion: ChildManifestSchemaVersion,
		ManifestID:    "dur-01",
		RepositorySHA: testRepositorySHA,
		Issue:         3674,
		PullRequests: []PullRequest{{
			Number:   3706,
			HeadSHA:  strings.Repeat("b", 40),
			MergeSHA: strings.Repeat("c", 40),
		}},
		Environment: Environment{
			GoVersion:       "go1.26.0",
			ToolVersion:     "powerloss-cert/v1",
			OS:              "darwin",
			Architecture:    "arm64",
			FilesystemModel: "stable-dirty-v1",
		},
		TestBinaries:  []Artifact{{Kind: ArtifactKindTestBinary, Path: "bin/TreeDB.test", SHA256: strings.Repeat("d", 64)}},
		ClaimBoundary: "modeled stable-byte crash images only; no block-device claim",
		Witnesses: []Witness{{
			ID:                     witnessID,
			EvidenceTier:           EvidenceTierModeledCrash,
			Profile:                "command_wal_durable",
			Acknowledgement:        "checkpoint",
			ResourceShapes:         []string{"forced-value-log-pointer"},
			DependencyGraph:        "meta -> index -> freelist -> value-log",
			StorageBoundaries:      []string{"target-meta-write-sync"},
			WritebackVariant:       "target-metadata-alone",
			FailureClasses:         []string{"fallback-older-complete-root"},
			ExpectedDurableHorizon: "older dependency-closed durable root",
			ExpectedOutcome:        "accepted:old-root",
			ActualOutcome:          "accepted:old-root",
			TypedError:             "none",
			State: WitnessState{
				RootMetaGeneration:  "old=1,new=2",
				FreelistGeneration:  "generation-2",
				ExternalFrontiers:   "value-log-generation-2",
				NamespaceGeneration: "namespace-generation-2",
				WALLineage:          "none",
				DurableLSN:          "not-applicable",
				CleanupPins:         "older-root-pin",
			},
			CounterexampleID:   "counterexample-a",
			NegativeControlID:  "negative-a",
			Seed:               1,
			CutID:              "cut/checkpoint-generation-2/after-meta-write/001",
			CutPoint:           "after-meta-write",
			CutOccurrence:      1,
			ObservedEventCount: 2,
			Command: TestCommand{
				BinaryPath: "bin/TreeDB.test",
				Package:    "./TreeDB",
				TestName:   "TestPowerLossOracleCounterexampleNewMetaMissingClosure",
				Args:       []string{"-test.run", "^TestPowerLossOracleCounterexampleNewMetaMissingClosure$", "-test.v"},
				Env: map[string]string{
					"TREEDB_POWERLOSS_CUT_ID":           "cut/checkpoint-generation-2/after-meta-write/001",
					"TREEDB_POWERLOSS_VARIANT_ID":       "variant-a",
					"TREEDB_POWERLOSS_SEED":             "1",
					"TREEDB_POWERLOSS_EVIDENCE_DIR":     "artifacts/witness-a",
					"TREEDB_POWERLOSS_EXPECT_CUT_POINT": "after-meta-write",
					"TREEDB_POWERLOSS_REOPEN_MODE":      "read-only",
					"TREEDB_POWERLOSS_PROFILE":          "command_wal_durable",
				},
			},
			CutExercised:  true,
			ClaimBoundary: "normal public reopen from modeled stable bytes",
			Artifacts: []Artifact{
				{Kind: ArtifactKindOperationTrace, Path: "artifacts/witness-a/operation_trace.json", SHA256: strings.Repeat("e", 64)},
				{Kind: ArtifactKindStableImageTree, Path: "artifacts/witness-a/stable_image_tree.json", SHA256: strings.Repeat("e", 64)},
				{Kind: ArtifactKindDirtyImageTree, Path: "artifacts/witness-a/dirty_image_tree.json", SHA256: strings.Repeat("e", 64)},
				{Kind: ArtifactKindRecoveryTrace, Path: "artifacts/witness-a/recovery_trace.json", SHA256: strings.Repeat("e", 64)},
				{Kind: ArtifactKindMetrics, Path: "artifacts/witness-a/metrics.json", SHA256: strings.Repeat("e", 64)},
				{Kind: ArtifactKindLog, Path: "artifacts/witness-a/command_log.json", SHA256: strings.Repeat("e", 64)},
			},
		}},
	}
}
