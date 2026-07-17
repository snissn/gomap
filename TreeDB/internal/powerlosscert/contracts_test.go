package powerlosscert

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateWitnessContractsRejectsCoverageRelabeling(t *testing.T) {
	plan := testRunPlan()
	contracts := WitnessContracts{SchemaVersion: WitnessContractsSchemaVersion, Cases: append([]RunCase(nil), plan.Cases...)}
	if err := ValidateWitnessContracts(plan, contracts); err != nil {
		t.Fatal(err)
	}
	plan.Cases[0].ResourceShapes = []string{"unrelated-risk-label"}
	if err := ValidateWitnessContracts(plan, contracts); err == nil || !strings.Contains(err.Error(), "not identical") {
		t.Fatalf("coverage relabel error=%v", err)
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
	plan.Cases = contracts.Cases
	if err := ValidateRunPlan(inventory, plan); err != nil {
		t.Fatal(err)
	}
	if err := ValidateWitnessContracts(plan, contracts); err != nil {
		t.Fatal(err)
	}
}
