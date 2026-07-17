package powerlosscert

import (
	"strings"
	"testing"
)

func TestParseRunPlanRejectsUnknownFields(t *testing.T) {
	if _, err := ParseRunPlan([]byte(`{"schema_version":"treedb-power-loss-run-plan/v1","unknown":true}`)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("ParseRunPlan unknown-field error=%v", err)
	}
}

func TestValidateRunPlanAcceptsFrozenExpectedRecovery(t *testing.T) {
	plan := testRunPlan()
	if err := ValidateRunPlan(testRiskInventory(), plan); err != nil {
		t.Fatal(err)
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
		SchemaVersion:   RunPlanSchemaVersion,
		RepositorySHA:   testRepositorySHA,
		Issue:           3684,
		PullRequests:    testChildManifest("case-a").PullRequests,
		ToolVersion:     "powerloss-cert/v1",
		FilesystemModel: "deterministic-stable-dirty-v1",
		ClaimBoundary:   "modeled stable-byte images; no block-device claim",
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
			ReopenMode:             reopenModeReadOnly,
			ExpectedRecovery: RecoveryExpectation{
				CommitSeq: 1,
			},
			ClaimBoundary: witness.ClaimBoundary,
		}},
	}
}
