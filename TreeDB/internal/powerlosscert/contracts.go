package powerlosscert

import (
	"fmt"
	"reflect"
)

const WitnessContractsSchemaVersion = "treedb-power-loss-witness-contracts/v1"

// WitnessContracts is the committed binding between a replay selector and the
// risk dimensions it is allowed to own. A caller-supplied plan may add exact
// repository provenance and resource bounds, but it may not relabel coverage.
type WitnessContracts struct {
	SchemaVersion string    `json:"schema_version"`
	Cases         []RunCase `json:"cases"`
}

func ParseWitnessContracts(data []byte) (WitnessContracts, error) {
	var contracts WitnessContracts
	if err := decodeStrict(data, &contracts); err != nil {
		return WitnessContracts{}, fmt.Errorf("powerlosscert: decode committed witness contracts: %w", err)
	}
	return contracts, nil
}

func ValidateWitnessContracts(plan RunPlan, contracts WitnessContracts) error {
	if contracts.SchemaVersion != WitnessContractsSchemaVersion {
		return fmt.Errorf("powerlosscert: witness contracts schema_version=%q want=%q", contracts.SchemaVersion, WitnessContractsSchemaVersion)
	}
	if len(contracts.Cases) != len(plan.Cases) {
		return fmt.Errorf("powerlosscert: run plan cases=%d want committed witness contracts=%d", len(plan.Cases), len(contracts.Cases))
	}
	seen := make(map[string]bool, len(contracts.Cases))
	for index, contract := range contracts.Cases {
		if contract.ID == "" || seen[contract.ID] {
			return fmt.Errorf("powerlosscert: committed witness contract %d has empty or duplicate id %q", index, contract.ID)
		}
		seen[contract.ID] = true
		if !reflect.DeepEqual(plan.Cases[index], contract) {
			return fmt.Errorf("powerlosscert: run plan case %d (%q) is not identical to the committed replay and risk-ownership contract", index, plan.Cases[index].ID)
		}
	}
	return nil
}
