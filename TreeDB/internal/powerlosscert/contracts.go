package powerlosscert

import (
	"fmt"
	"reflect"
)

const WitnessContractsSchemaVersion = "treedb-power-loss-witness-contracts/v4"

// WitnessContracts is the committed binding between the certification issue,
// required graph PRs, replay selectors, and the risk dimensions they are
// allowed to own. A caller-supplied plan adds exact PR head/merge SHAs and
// resource bounds, but it may not omit or substitute graph provenance or
// relabel coverage.
type WitnessContracts struct {
	SchemaVersion        string    `json:"schema_version"`
	Issue                int       `json:"issue"`
	RequiredPullRequests []int     `json:"required_pull_requests"`
	Cases                []RunCase `json:"cases"`
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
	if contracts.Issue <= 0 {
		return fmt.Errorf("powerlosscert: committed witness contracts have invalid issue %d", contracts.Issue)
	}
	if plan.Issue != contracts.Issue {
		return fmt.Errorf("powerlosscert: run plan issue=%d want committed certification issue=%d", plan.Issue, contracts.Issue)
	}
	if len(contracts.RequiredPullRequests) == 0 {
		return fmt.Errorf("powerlosscert: committed witness contracts have no required pull requests")
	}
	seenPRs := make(map[int]bool, len(contracts.RequiredPullRequests))
	for index, number := range contracts.RequiredPullRequests {
		if number <= 0 || seenPRs[number] {
			return fmt.Errorf("powerlosscert: committed required pull request %d has invalid or duplicate number %d", index, number)
		}
		seenPRs[number] = true
	}
	if len(plan.PullRequests) != len(contracts.RequiredPullRequests) {
		return fmt.Errorf("powerlosscert: run plan pull-request provenance entries=%d want committed graph entries=%d", len(plan.PullRequests), len(contracts.RequiredPullRequests))
	}
	for index, number := range contracts.RequiredPullRequests {
		if plan.PullRequests[index].Number != number {
			return fmt.Errorf("powerlosscert: run plan pull-request provenance index %d has number=%d want committed graph PR=%d", index, plan.PullRequests[index].Number, number)
		}
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
