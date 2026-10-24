package powerlosscert

import (
	"fmt"
	"reflect"
	"slices"
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
	StreamFallbackCases  []RunCase `json:"stream_fallback_cases,omitempty"`
}

// ResolveWitnessContracts projects the committed physical replay selectors for
// plan construction and exact-SHA execution. It never rewrites a supplied plan.
// Fallbacks may change only the cut address and its physical explanatory text;
// provenance, risk ownership, recovery state, order, and case count stay fixed.
// All overrides are validated even when fixed-WAL creation is supported.
func ResolveWitnessContracts(contracts WitnessContracts, fixedWAL bool) (WitnessContracts, error) {
	resolved := contracts
	resolved.RequiredPullRequests = slices.Clone(contracts.RequiredPullRequests)
	resolved.Cases = cloneContractCases(contracts.Cases)
	resolved.StreamFallbackCases = cloneContractCases(contracts.StreamFallbackCases)
	byID := make(map[string]int, len(contracts.Cases))
	for index, contract := range contracts.Cases {
		if _, exists := byID[contract.ID]; contract.ID == "" || exists {
			return WitnessContracts{}, fmt.Errorf("powerlosscert: committed witness contract has empty or duplicate id %q", contract.ID)
		}
		byID[contract.ID] = index
	}
	seen := make(map[string]bool, len(contracts.StreamFallbackCases))
	for _, fallback := range contracts.StreamFallbackCases {
		index, exists := byID[fallback.ID]
		if !exists || seen[fallback.ID] {
			return WitnessContracts{}, fmt.Errorf("powerlosscert: stream fallback has unknown or duplicate id %q", fallback.ID)
		}
		seen[fallback.ID] = true
		base := contracts.Cases[index]
		if fallback.CutID == "" || fallback.DependencyGraph == "" || fallback.ExpectedDurableHorizon == "" || fallback.ClaimBoundary == "" {
			return WitnessContracts{}, fmt.Errorf("powerlosscert: stream fallback %q has empty physical replay description", fallback.ID)
		}
		invariant := fallback
		invariant.CutID = base.CutID
		invariant.DependencyGraph = base.DependencyGraph
		invariant.ExpectedDurableHorizon = base.ExpectedDurableHorizon
		invariant.ClaimBoundary = base.ClaimBoundary
		if !reflect.DeepEqual(invariant, base) {
			return WitnessContracts{}, fmt.Errorf("powerlosscert: stream fallback %q changes coverage, provenance, or recovery authority", fallback.ID)
		}
		if !fixedWAL {
			resolved.Cases[index] = cloneContractCases([]RunCase{fallback})[0]
		}
	}
	return resolved, nil
}

func cloneContractCases(cases []RunCase) []RunCase {
	if cases == nil {
		return nil
	}
	cloned := slices.Clone(cases)
	for index := range cloned {
		cloned[index].ResourceShapes = slices.Clone(cases[index].ResourceShapes)
		cloned[index].StorageBoundaries = slices.Clone(cases[index].StorageBoundaries)
		cloned[index].FailureClasses = slices.Clone(cases[index].FailureClasses)
	}
	return cloned
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
