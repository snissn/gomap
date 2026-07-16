package powerlosscert

import (
	"reflect"
	"strings"
	"testing"
)

const testRepositorySHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestParseChildManifestRejectsUnknownFields(t *testing.T) {
	data := []byte(`{"schema_version":"treedb-power-loss-child/v1","unexpected":true}`)
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
		TestBinaries:  []Artifact{{Path: "bin/TreeDB.test", SHA256: strings.Repeat("d", 64)}},
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
			ExpectedOutcome:        "old-root",
			ActualOutcome:          "old-root",
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
			CounterexampleID:  "counterexample-a",
			NegativeControlID: "negative-a",
			Seed:              1,
			Command: TestCommand{
				BinaryPath: "bin/TreeDB.test",
				Package:    "./TreeDB",
				TestName:   "TestPowerLossOracleCounterexampleNewMetaMissingClosure",
			},
			CutExercised:  true,
			ClaimBoundary: "normal public reopen from modeled stable bytes",
			Artifacts:     []Artifact{{Path: "artifacts/witness-a.log", SHA256: strings.Repeat("e", 64)}},
		}},
	}
}
