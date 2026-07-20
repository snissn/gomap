package powerlosscert

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateWitnessContractsRejectsCoverageRelabeling(t *testing.T) {
	plan := testRunPlan()
	contracts := testWitnessContracts(plan)
	if err := ValidateWitnessContracts(plan, contracts); err != nil {
		t.Fatal(err)
	}
	plan.Cases[0].ResourceShapes = []string{"unrelated-risk-label"}
	if err := ValidateWitnessContracts(plan, contracts); err == nil || !strings.Contains(err.Error(), "not identical") {
		t.Fatalf("coverage relabel error=%v", err)
	}
}

func TestValidateWitnessContractsRejectsPriorSchema(t *testing.T) {
	plan := testRunPlan()
	contracts := testWitnessContracts(plan)
	contracts.SchemaVersion = "treedb-power-loss-witness-contracts/v3"
	if err := ValidateWitnessContracts(plan, contracts); err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("ValidateWitnessContracts prior schema error=%v", err)
	}
}

func TestValidateWitnessContractsRejectsRecoveryDirectorySubstitution(t *testing.T) {
	plan := testRunPlan()
	plan.Cases[0].ExpectedRecovery.Dir = "recovery-input/db"
	contracts := testWitnessContracts(plan)
	plan.Cases[0].ExpectedRecovery.Dir = defaultRecoveryDir
	if err := ValidateWitnessContracts(plan, contracts); err == nil || !strings.Contains(err.Error(), "not identical") {
		t.Fatalf("recovery directory substitution error=%v", err)
	}
}

func TestValidateWitnessContractsRejectsIssueOrPullRequestSubstitution(t *testing.T) {
	base := testRunPlan()
	contracts := testWitnessContracts(base)
	tests := []struct {
		name   string
		mutate func(*RunPlan)
		want   string
	}{
		{
			name: "issue",
			mutate: func(plan *RunPlan) {
				plan.Issue++
			},
			want: "want committed certification issue",
		},
		{
			name: "missing pull request",
			mutate: func(plan *RunPlan) {
				plan.PullRequests = plan.PullRequests[:len(plan.PullRequests)-1]
			},
			want: "want committed graph entries",
		},
		{
			name: "extra pull request",
			mutate: func(plan *RunPlan) {
				plan.PullRequests = append(plan.PullRequests, PullRequest{Number: 9999, HeadSHA: testRepositorySHA, MergeSHA: testRepositorySHA})
			},
			want: "want committed graph entries",
		},
		{
			name: "substituted pull request",
			mutate: func(plan *RunPlan) {
				plan.PullRequests[0].Number++
			},
			want: "want committed graph PR",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := base
			plan.PullRequests = append([]PullRequest(nil), base.PullRequests...)
			tt.mutate(&plan)
			if err := ValidateWitnessContracts(plan, contracts); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateWitnessContracts error=%v want substring %q", err, tt.want)
			}
		})
	}
}

func TestValidateWitnessContractsRejectsInvalidCommittedPullRequestSequence(t *testing.T) {
	plan := testRunPlan()
	contracts := testWitnessContracts(plan)
	contracts.RequiredPullRequests = append(contracts.RequiredPullRequests, contracts.RequiredPullRequests[0])
	if err := ValidateWitnessContracts(plan, contracts); err == nil || !strings.Contains(err.Error(), "invalid or duplicate") {
		t.Fatalf("ValidateWitnessContracts duplicate contract PR error=%v", err)
	}
}

func TestCommittedWitnessContractsOwnCompleteFrozenInventory(t *testing.T) {
	inventoryData, err := os.ReadFile(filepath.Join("..", "..", "testdata", "power_loss_risk_inventory.json"))
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := ParseRiskInventory(inventoryData)
	if err != nil {
		t.Fatal(err)
	}
	contractData, err := os.ReadFile(filepath.Join("..", "..", "testdata", "power_loss_witness_contracts.json"))
	if err != nil {
		t.Fatal(err)
	}
	contracts, err := ParseWitnessContracts(contractData)
	if err != nil {
		t.Fatal(err)
	}
	plan := testRunPlan()
	plan.Issue = contracts.Issue
	plan.PullRequests = make([]PullRequest, len(contracts.RequiredPullRequests))
	for index, number := range contracts.RequiredPullRequests {
		plan.PullRequests[index] = PullRequest{Number: number, HeadSHA: testRepositorySHA, MergeSHA: testRepositorySHA}
	}
	plan.Cases = contracts.Cases
	if err := ValidateRunPlan(inventory, plan); err != nil {
		t.Fatal(err)
	}
	if err := ValidateWitnessContracts(plan, contracts); err != nil {
		t.Fatal(err)
	}
}

func testWitnessContracts(plan RunPlan) WitnessContracts {
	required := make([]int, len(plan.PullRequests))
	for index, pr := range plan.PullRequests {
		required[index] = pr.Number
	}
	return WitnessContracts{
		SchemaVersion:        WitnessContractsSchemaVersion,
		Issue:                plan.Issue,
		RequiredPullRequests: required,
		Cases:                append([]RunCase(nil), plan.Cases...),
	}
}
