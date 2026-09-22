package powerlosscert

import (
	"strings"
	"testing"
)

func TestParseRunPlanRejectsUnknownFields(t *testing.T) {
	if _, err := ParseRunPlan([]byte(`{"schema_version":"treedb-power-loss-run-plan/v4","unknown":true}`)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("ParseRunPlan unknown-field error=%v", err)
	}
}

func TestValidateRunPlanAcceptsFrozenExpectedRecovery(t *testing.T) {
	plan := testRunPlan()
	if err := ValidateRunPlan(testRiskInventory(), plan); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRunPlanRejectsPriorSchemaAndMismatchedReplayWindow(t *testing.T) {
	plan := testRunPlan()
	plan.SchemaVersion = "treedb-power-loss-run-plan/v3"
	if err := ValidateRunPlan(testRiskInventory(), plan); err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("ValidateRunPlan prior schema error=%v", err)
	}

	plan = testRunPlan()
	plan.Cases[0].ReplayWindow = "another-variant"
	if err := ValidateRunPlan(testRiskInventory(), plan); err == nil || !strings.Contains(err.Error(), "does not match variant id") {
		t.Fatalf("ValidateRunPlan mismatched replay window error=%v", err)
	}
}

func TestValidateRunPlanRecoveryDirectoryContract(t *testing.T) {
	for _, dir := range []string{"", "recovery-input", "recovery-input/db"} {
		t.Run("accept-"+strings.ReplaceAll(dir, "/", "-"), func(t *testing.T) {
			plan := testRunPlan()
			plan.Cases[0].ExpectedRecovery.Dir = dir
			if err := ValidateRunPlan(testRiskInventory(), plan); err != nil {
				t.Fatalf("ValidateRunPlan dir=%q: %v", dir, err)
			}
		})
	}
	for _, dir := range []string{"/recovery-input/db", "../db", "recovery-input/../db", "recovery-input//db", `recovery-input\db`, "other/db"} {
		t.Run("reject-"+strings.NewReplacer("/", "-", `\`, "-").Replace(dir), func(t *testing.T) {
			plan := testRunPlan()
			plan.Cases[0].ExpectedRecovery.Dir = dir
			if err := ValidateRunPlan(testRiskInventory(), plan); err == nil || !strings.Contains(err.Error(), "recovery directory") {
				t.Fatalf("ValidateRunPlan dir=%q error=%v", dir, err)
			}
		})
	}
}

func TestValidateRunPlanRejectsIncompleteFrozenRiskCoverage(t *testing.T) {
	plan := testRunPlan()
	plan.Cases[0].CounterexampleID = ""
	plan.Cases[0].NegativeControlID = ""

	err := ValidateRunPlan(testRiskInventory(), plan)
	if err == nil || !strings.Contains(err.Error(), "incomplete frozen risk coverage") {
		t.Fatalf("ValidateRunPlan incomplete-coverage error=%v", err)
	}
}

func TestValidateRunPlanRejectsCircularOrMismatchedOutcomeContracts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RunCase)
		want   string
	}{
		{
			name: "accepted-with-error",
			mutate: func(runCase *RunCase) {
				runCase.ExpectedTypedError = "*errors.errorString"
				runCase.ExpectedRecovery.ErrorType = "*errors.errorString"
			},
			want: "inconsistent accepted",
		},
		{
			name: "rejected-without-type",
			mutate: func(runCase *RunCase) {
				runCase.ExpectedOutcome = "rejected:recovery-required"
				runCase.ExpectedRecovery.Rejected = true
			},
			want: "inconsistent rejected",
		},
		{
			name: "cut-point-mismatch",
			mutate: func(runCase *RunCase) {
				runCase.CutPoint = "after-index-data-sync"
			},
			want: "cut_id point",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := testRunPlan()
			tt.mutate(&plan.Cases[0])
			if err := ValidateRunPlan(testRiskInventory(), plan); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateRunPlan error=%v want substring %q", err, tt.want)
			}
		})
	}
}

func testRunPlan() RunPlan {
	witness := testChildManifest("case-a").Witnesses[0]
	return RunPlan{
		SchemaVersion:          RunPlanSchemaVersion,
		RepositoryRef:          CertifiedRepositoryRef,
		RepositorySHA:          testRepositorySHA,
		Issue:                  3684,
		PullRequests:           testChildManifest("case-a").PullRequests,
		ToolVersion:            "powerloss-cert/v1",
		FilesystemModel:        "deterministic-stable-dirty-v1",
		ClaimBoundary:          "modeled stable-byte images; no block-device claim",
		CaseTimeoutSeconds:     120,
		MaxCaseEvidenceBytes:   64 << 20,
		MaxCapturedOutputBytes: 8 << 20,
		MaxBundleBytes:         1 << 30,
		Cases: []RunCase{{
			ID:                     "case-a",
			Package:                "./TreeDB",
			TestName:               "TestPowerLossOracleCounterexampleNewMetaMissingClosure",
			Profile:                witness.Profile,
			Acknowledgement:        witness.Acknowledgement,
			ResourceShapes:         witness.ResourceShapes,
			DependencyGraph:        witness.DependencyGraph,
			StorageBoundaries:      witness.StorageBoundaries,
			WritebackVariant:       witness.WritebackVariant,
			FailureClasses:         witness.FailureClasses,
			ExpectedDurableHorizon: witness.ExpectedDurableHorizon,
			ExpectedOutcome:        witness.ExpectedOutcome,
			ExpectedTypedError:     witness.TypedError,
			State:                  witness.State,
			CounterexampleID:       witness.CounterexampleID,
			NegativeControlID:      witness.NegativeControlID,
			Seed:                   witness.Seed,
			CutID:                  witness.CutID,
			VariantID:              "variant-a",
			CutPoint:               witness.CutPoint,
			ReopenMode:             powerLossReopenModeReadOnly,
			ExpectedRecovery: RecoveryExpectation{
				CommitSeq: 1,
			},
			ClaimBoundary: witness.ClaimBoundary,
		}},
	}
}
