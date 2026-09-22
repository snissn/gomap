package powerlosscert

import (
	"fmt"
	"sort"
	"strings"
)

type CoverageRow struct {
	Dimension  string   `json:"dimension"`
	Value      string   `json:"value"`
	WitnessIDs []string `json:"witness_ids"`
}

type CoverageReport struct {
	SchemaVersion string        `json:"schema_version"`
	Complete      bool          `json:"complete"`
	Rows          []CoverageRow `json:"rows"`
	Gaps          []string      `json:"gaps"`
}

type SelectionPlan struct {
	SchemaVersion string   `json:"schema_version"`
	CaseIDs       []string `json:"case_ids"`
	Covered       []string `json:"covered"`
}

func BuildCoverageReport(inventory RiskInventory, manifests []ChildManifest) (CoverageReport, error) {
	if err := validateRiskInventory(inventory); err != nil {
		return CoverageReport{}, err
	}
	owners := make(map[string]map[string]bool)
	for _, witness := range sortedWitnesses(manifests) {
		if witness.EvidenceTier != EvidenceTierModeledCrash {
			continue
		}
		for _, key := range witnessCoverageKeys(inventory, witness) {
			if owners[key] == nil {
				owners[key] = make(map[string]bool)
			}
			owners[key][witness.ID] = true
		}
	}
	required := inventoryCoverageKeys(inventory)
	report := CoverageReport{SchemaVersion: CoverageReportSchemaVersion}
	for _, key := range required {
		dimension, value, _ := strings.Cut(key, ":")
		ids := sortedSet(owners[key])
		report.Rows = append(report.Rows, CoverageRow{Dimension: dimension, Value: value, WitnessIDs: ids})
		if len(ids) == 0 {
			report.Gaps = append(report.Gaps, key)
		}
	}
	report.Complete = len(report.Gaps) == 0
	return report, nil
}

func SelectRepresentativeCases(inventory RiskInventory, manifests []ChildManifest) (SelectionPlan, error) {
	report, err := BuildCoverageReport(inventory, manifests)
	if err != nil {
		return SelectionPlan{}, err
	}
	if !report.Complete {
		return SelectionPlan{}, fmt.Errorf("powerlosscert: cannot select from incomplete coverage: %s", strings.Join(report.Gaps, ", "))
	}
	witnesses := sortedWitnesses(manifests)
	byID := make(map[string]Witness, len(witnesses))
	for _, witness := range witnesses {
		if witness.EvidenceTier == EvidenceTierModeledCrash {
			byID[witness.ID] = witness
		}
	}
	selected := make(map[string]bool)
	covered := make(map[string]bool)
	selectWitness := func(witness Witness) {
		if selected[witness.ID] {
			return
		}
		selected[witness.ID] = true
		for _, key := range witnessCoverageKeys(inventory, witness) {
			covered[key] = true
		}
	}

	for _, counterexample := range inventory.RetainedCounterexamples {
		witness, ok := findWitness(witnesses, func(candidate Witness) bool {
			return candidate.EvidenceTier == EvidenceTierModeledCrash && candidate.CounterexampleID == counterexample
		})
		if !ok {
			return SelectionPlan{}, fmt.Errorf("powerlosscert: retained counterexample %q has no modeled witness", counterexample)
		}
		selectWitness(witness)
	}
	for _, control := range inventory.RequiredNegativeControls {
		witness, ok := findWitness(witnesses, func(candidate Witness) bool {
			return candidate.EvidenceTier == EvidenceTierModeledCrash && candidate.NegativeControlID == control
		})
		if !ok {
			return SelectionPlan{}, fmt.Errorf("powerlosscert: negative control %q has no modeled witness", control)
		}
		selectWitness(witness)
	}

	required := inventoryCoverageKeys(inventory)
	for {
		var uncovered []string
		for _, key := range required {
			if !covered[key] {
				uncovered = append(uncovered, key)
			}
		}
		if len(uncovered) == 0 {
			break
		}
		uncoveredSet := make(map[string]bool, len(uncovered))
		for _, key := range uncovered {
			uncoveredSet[key] = true
		}
		bestID := ""
		bestGain := 0
		for id, witness := range byID {
			if selected[id] {
				continue
			}
			gain := 0
			for _, key := range witnessCoverageKeys(inventory, witness) {
				if uncoveredSet[key] {
					gain++
				}
			}
			if gain > bestGain || (gain == bestGain && gain > 0 && (bestID == "" || id < bestID)) {
				bestID = id
				bestGain = gain
			}
		}
		if bestGain == 0 {
			return SelectionPlan{}, fmt.Errorf("powerlosscert: covering selector stalled with gaps: %s", strings.Join(uncovered, ", "))
		}
		selectWitness(byID[bestID])
	}

	caseIDs := sortedSet(selected)
	coveredKeys := sortedSet(covered)
	return SelectionPlan{SchemaVersion: SelectionPlanSchemaVersion, CaseIDs: caseIDs, Covered: coveredKeys}, nil
}

func inventoryCoverageKeys(inventory RiskInventory) []string {
	var keys []string
	for _, dimension := range requiredDimensions {
		for _, value := range inventory.Dimensions[dimension] {
			keys = append(keys, dimension+":"+value)
		}
	}
	for _, value := range inventory.RetainedCounterexamples {
		keys = append(keys, DimensionCounterexample+":"+value)
	}
	for _, value := range inventory.RequiredNegativeControls {
		keys = append(keys, DimensionNegativeControl+":"+value)
	}
	for _, interaction := range inventory.RequiredInteractions {
		keys = append(keys, "interaction:"+interaction.ID)
	}
	sort.Strings(keys)
	return keys
}

func witnessCoverageKeys(inventory RiskInventory, witness Witness) []string {
	keys := []string{
		DimensionProfile + ":" + witness.Profile,
		DimensionAcknowledgement + ":" + witness.Acknowledgement,
		DimensionWritebackVariant + ":" + witness.WritebackVariant,
	}
	for _, value := range witness.ResourceShapes {
		keys = append(keys, DimensionResourceShape+":"+value)
	}
	for _, value := range witness.StorageBoundaries {
		keys = append(keys, DimensionStorageBoundary+":"+value)
	}
	for _, value := range witness.FailureClasses {
		keys = append(keys, DimensionFailureClass+":"+value)
	}
	if witness.CounterexampleID != "" {
		keys = append(keys, DimensionCounterexample+":"+witness.CounterexampleID)
	}
	if witness.NegativeControlID != "" {
		keys = append(keys, DimensionNegativeControl+":"+witness.NegativeControlID)
	}
	for _, interaction := range inventory.RequiredInteractions {
		if witnessSatisfiesInteraction(witness, interaction) {
			keys = append(keys, "interaction:"+interaction.ID)
		}
	}
	sort.Strings(keys)
	return compactStrings(keys)
}

func witnessSatisfiesInteraction(witness Witness, interaction RequiredInteraction) bool {
	for _, member := range interaction.Members {
		var values []string
		switch member.Dimension {
		case DimensionProfile:
			values = []string{witness.Profile}
		case DimensionAcknowledgement:
			values = []string{witness.Acknowledgement}
		case DimensionResourceShape:
			values = witness.ResourceShapes
		case DimensionStorageBoundary:
			values = witness.StorageBoundaries
		case DimensionWritebackVariant:
			values = []string{witness.WritebackVariant}
		case DimensionFailureClass:
			values = witness.FailureClasses
		default:
			return false
		}
		if !containsString(values, member.Value) {
			return false
		}
	}
	return true
}

func findWitness(witnesses []Witness, matches func(Witness) bool) (Witness, bool) {
	for _, witness := range witnesses {
		if matches(witness) {
			return witness, true
		}
	}
	return Witness{}, false
}

func sortedSet(set map[string]bool) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}
