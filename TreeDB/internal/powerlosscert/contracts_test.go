package powerlosscert

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCommittedContractsDeclareStreamFallbackRotation(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "power_loss_witness_contracts.json"))
	if err != nil {
		t.Fatal(err)
	}
	var contracts struct {
		Cases               []RunCase `json:"cases"`
		StreamFallbackCases []RunCase `json:"stream_fallback_cases"`
	}
	if err = json.Unmarshal(data, &contracts); err != nil {
		t.Fatal(err)
	}
	for _, fallback := range contracts.StreamFallbackCases {
		if fallback.ID == "grouped-rotation-external-closure" {
			if fallback.CutID != "cut/public-grouped-rotation-external-stable-image/after-dependency-file-sync/003" {
				t.Fatalf("stream rotation cut=%q", fallback.CutID)
			}
			return
		}
	}
	t.Fatal("committed contracts omit the stream-fallback rotation selector /003")
}

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

func TestResolveWitnessContractsPhysicalPlatforms(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "power_loss_witness_contracts.json"))
	if err != nil {
		t.Fatal(err)
	}
	contracts, err := ParseWitnessContracts(data)
	if err != nil {
		t.Fatal(err)
	}
	before := mustJSON(t, contracts)
	fixed, err := ResolveWitnessContracts(contracts, true)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := ResolveWitnessContracts(contracts, false)
	if err != nil {
		t.Fatal(err)
	}
	if mustJSON(t, contracts) != before || !reflect.DeepEqual(fixed.Cases, contracts.Cases) {
		t.Fatal("platform projection mutated the committed input or fixed cases")
	}
	if len(fixed.Cases) != len(stream.Cases) {
		t.Fatal("platform projection changed case count")
	}
	plan := testRunPlan()
	plan.Issue = contracts.Issue
	plan.PullRequests = nil
	for _, number := range contracts.RequiredPullRequests {
		plan.PullRequests = append(plan.PullRequests, PullRequest{Number: number})
	}
	plan.Cases = fixed.Cases
	if err = ValidateWitnessContracts(plan, fixed); err != nil {
		t.Fatal(err)
	}
	if err = ValidateWitnessContracts(plan, stream); err == nil || !strings.Contains(err.Error(), "not identical") {
		t.Fatalf("fixed plan accepted as stream: %v", err)
	}
	plan.Cases = stream.Cases
	if err = ValidateWitnessContracts(plan, stream); err != nil {
		t.Fatal(err)
	}
	if err = ValidateWitnessContracts(plan, fixed); err == nil || !strings.Contains(err.Error(), "not identical") {
		t.Fatalf("stream plan accepted as fixed: %v", err)
	}
	differences := 0
	for index, base := range fixed.Cases {
		if base.ID != stream.Cases[index].ID {
			t.Fatal("projection changed case order")
		}
		if !reflect.DeepEqual(base, stream.Cases[index]) {
			differences++
			if base.ID != "grouped-rotation-external-closure" ||
				!strings.HasSuffix(base.CutID, "/004") || !strings.HasSuffix(stream.Cases[index].CutID, "/003") {
				t.Fatalf("unexpected physical projection: %s -> %s", base.CutID, stream.Cases[index].CutID)
			}
		}
	}
	if differences != 1 {
		t.Fatalf("projected cases=%d want1", differences)
	}
	// Caller mutation of a projected plan cannot relabel the parsed contract.
	stream.Cases[0].ResourceShapes[0] = "wrong-risk"
	stream.RequiredPullRequests[0] = 99999
	if mustJSON(t, contracts) != before {
		t.Fatal("projected plan aliases the original contract")
	}
}

func TestResolveWitnessContractsRejectsFallbackAuthorityChanges(t *testing.T) {
	base := testWitnessContracts(testRunPlan())
	fallback := base.Cases[0]
	fallback.CutID = fallback.CutID + "-stream"
	base.StreamFallbackCases = []RunCase{fallback}
	for name, mutate := range map[string]func(*WitnessContracts){
		"unknown": func(c *WitnessContracts) { c.StreamFallbackCases[0].ID = "unknown" },
		"duplicate": func(c *WitnessContracts) {
			c.StreamFallbackCases = append(c.StreamFallbackCases, c.StreamFallbackCases[0])
		},
		"duplicate-base": func(c *WitnessContracts) { c.Cases = append(c.Cases, c.Cases[0]) },
		"risk":           func(c *WitnessContracts) { c.StreamFallbackCases[0].ResourceShapes = []string{"wrong-risk"} },
		"test":           func(c *WitnessContracts) { c.StreamFallbackCases[0].TestName = "TestOther" },
		"profile":        func(c *WitnessContracts) { c.StreamFallbackCases[0].Profile = "other-profile" },
		"seed":           func(c *WitnessContracts) { c.StreamFallbackCases[0].Seed++ },
		"recovery":       func(c *WitnessContracts) { c.StreamFallbackCases[0].ExpectedRecovery.AppliedLSN++ },
		"state":          func(c *WitnessContracts) { c.StreamFallbackCases[0].State.DurableLSN = "other" },
		"boundary":       func(c *WitnessContracts) { c.StreamFallbackCases[0].StorageBoundaries = []string{"other"} },
		"empty":          func(c *WitnessContracts) { c.StreamFallbackCases[0].ExpectedDurableHorizon = "" },
	} {
		t.Run(name, func(t *testing.T) {
			c := base
			c.Cases = cloneContractCases(base.Cases)
			c.StreamFallbackCases = cloneContractCases(base.StreamFallbackCases)
			mutate(&c)
			for _, fixed := range []bool{true, false} {
				if _, err := ResolveWitnessContracts(c, fixed); err == nil {
					t.Fatalf("accepted invalid override with fixed=%t", fixed)
				}
			}
		})
	}
}
