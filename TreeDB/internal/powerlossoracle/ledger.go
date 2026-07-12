package powerlossoracle

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

const CounterexampleLedgerSchemaVersion = 1

type CounterexampleDisposition string

const (
	DispositionKnown    CounterexampleDisposition = "known-counterexample"
	DispositionResolved CounterexampleDisposition = "resolved"
)

type ExpectedResult string

const (
	ExpectedOldRoot       ExpectedResult = "old-root"
	ExpectedNewRoot       ExpectedResult = "new-root"
	ExpectedSuffixDiscard ExpectedResult = "suffix-discard"
	ExpectedTypedError    ExpectedResult = "typed-error"
	ExpectedCorruption    ExpectedResult = "corruption"
)

func isExpectedResult(result ExpectedResult) bool {
	switch result {
	case ExpectedOldRoot, ExpectedNewRoot, ExpectedSuffixDiscard, ExpectedTypedError, ExpectedCorruption:
		return true
	default:
		return false
	}
}

// CounterexampleLedger is checked in as JSON so later graph nodes can consume
// the same invariant and replay inventory without parsing prose.
type CounterexampleLedger struct {
	SchemaVersion        int                         `json:"schema_version"`
	MaxVariantsPerCut    int                         `json:"max_variants_per_cut"`
	KnownCounterexamples []string                    `json:"known_counterexamples"`
	Entries              []CounterexampleLedgerEntry `json:"entries"`
}

type CounterexampleLedgerEntry struct {
	ID                string                    `json:"id"`
	Invariant         string                    `json:"invariant"`
	ProducerOperation string                    `json:"producer_operation"`
	CutID             string                    `json:"cut_id"`
	CutPoint          CutPoint                  `json:"cut_point"`
	VariantID         string                    `json:"variant_id"`
	Seed              uint64                    `json:"seed"`
	VariantFamilies   []VariantFamily           `json:"variant_families"`
	Expected          ExpectedResult            `json:"expected_result"`
	Owner             string                    `json:"owner"`
	Disposition       CounterexampleDisposition `json:"disposition"`
	Replay            string                    `json:"replay_command"`
}

func ParseCounterexampleLedger(data []byte) (CounterexampleLedger, error) {
	var ledger CounterexampleLedger
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ledger); err != nil {
		return CounterexampleLedger{}, fmt.Errorf("powerlossoracle: decode counterexample ledger: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return CounterexampleLedger{}, errorsf("counterexample ledger contains a second JSON value")
		}
		return CounterexampleLedger{}, fmt.Errorf("powerlossoracle: decode counterexample ledger trailing data: %w", err)
	}
	return ledger, nil
}

// RequireRetainedCounterexamples keeps a code-owned inventory independent of
// the ledger. Deleting a known witness from both ledger collections therefore
// still fails until code explicitly records the review decision.
func RequireRetainedCounterexamples(ledger CounterexampleLedger, required []string) error {
	known := make(map[string]bool, len(ledger.KnownCounterexamples))
	for _, id := range ledger.KnownCounterexamples {
		known[id] = true
	}
	entries := make(map[string]CounterexampleDisposition, len(ledger.Entries))
	for _, entry := range ledger.Entries {
		entries[entry.ID] = entry.Disposition
	}
	for _, id := range required {
		if !known[id] {
			return errorsf("required known counterexample %q disappeared from ledger inventory", id)
		}
		disposition, exists := entries[id]
		if !exists {
			return errorsf("required known counterexample %q disappeared from ledger entries", id)
		}
		if disposition != DispositionKnown && disposition != DispositionResolved {
			return errorsf("required known counterexample %q has no explicit disposition", id)
		}
	}
	return nil
}

// ValidateCounterexampleLedger fails closed on schema drift, unknown required
// families, lost known counterexamples, stale replay addresses, and silently
// skipped generator coverage.
func ValidateCounterexampleLedger(ledger CounterexampleLedger, generated map[string][]Variant) error {
	if ledger.SchemaVersion != CounterexampleLedgerSchemaVersion {
		return errorsf("counterexample ledger schema=%d want=%d", ledger.SchemaVersion, CounterexampleLedgerSchemaVersion)
	}
	if ledger.MaxVariantsPerCut != MaxVariantsPerCut {
		return errorsf("counterexample ledger max_variants_per_cut=%d want=%d", ledger.MaxVariantsPerCut, MaxVariantsPerCut)
	}
	if len(ledger.KnownCounterexamples) == 0 || len(ledger.Entries) == 0 {
		return errorsf("counterexample ledger must retain known ids and entries")
	}
	entryByID := make(map[string]CounterexampleLedgerEntry, len(ledger.Entries))
	for _, entry := range ledger.Entries {
		if entry.ID == "" || entry.Invariant == "" || entry.ProducerOperation == "" || entry.CutID == "" || entry.CutPoint == "" || entry.VariantID == "" || entry.Seed == 0 || entry.Owner == "" || entry.Replay == "" {
			return errorsf("counterexample ledger entry %q has an empty required field", entry.ID)
		}
		if _, duplicate := entryByID[entry.ID]; duplicate {
			return errorsf("counterexample ledger duplicate entry %q", entry.ID)
		}
		entryByID[entry.ID] = entry
		if entry.Disposition != DispositionKnown && entry.Disposition != DispositionResolved {
			return errorsf("counterexample ledger entry %q has unknown disposition %q", entry.ID, entry.Disposition)
		}
		if !isExpectedResult(entry.Expected) {
			return errorsf("counterexample ledger entry %q has unknown expected result %q", entry.ID, entry.Expected)
		}
		if len(entry.VariantFamilies) == 0 {
			return errorsf("counterexample ledger entry %q has no required variant families", entry.ID)
		}
		if err := validateRequiredFamilies(entry.VariantFamilies); err != nil {
			return fmt.Errorf("entry %s: %w", entry.ID, err)
		}
		if !strings.Contains(entry.Replay, EnvReplayCut+"=") || !strings.Contains(entry.Replay, EnvReplayVariant+"=") || !strings.Contains(entry.Replay, EnvReplaySeed+"=") {
			return errorsf("counterexample ledger entry %q replay omits stable cut/variant/seed selectors", entry.ID)
		}
		if !strings.Contains(entry.Replay, entry.CutID) || !strings.Contains(entry.Replay, entry.VariantID) || !strings.Contains(entry.Replay, fmt.Sprint(entry.Seed)) {
			return errorsf("counterexample ledger entry %q replay does not address its exact cut/variant/seed", entry.ID)
		}
		variants := generated[entry.CutID]
		foundVariant := false
		generatedExpected := ExpectedResult("")
		families := make(map[VariantFamily]bool)
		for _, variant := range variants {
			families[variant.Family] = true
			if variant.ID == entry.VariantID && variant.Seed == entry.Seed {
				foundVariant = true
				generatedExpected = variant.Expected
			}
		}
		if !foundVariant {
			return errorsf("counterexample ledger entry %q addresses an ungenerated variant", entry.ID)
		}
		if generatedExpected != entry.Expected {
			return errorsf("counterexample ledger entry %q expected result=%q generated=%q", entry.ID, entry.Expected, generatedExpected)
		}
		for _, family := range entry.VariantFamilies {
			if !families[family] {
				return errorsf("counterexample ledger entry %q silently skipped required family %s", entry.ID, family)
			}
		}
	}

	known := append([]string(nil), ledger.KnownCounterexamples...)
	sort.Strings(known)
	for i, id := range known {
		if i > 0 && known[i-1] == id {
			return errorsf("counterexample ledger known id %q is duplicated", id)
		}
		entry, exists := entryByID[id]
		if !exists {
			return errorsf("known counterexample %q disappeared; retain it with disposition %q when fixed", id, DispositionResolved)
		}
		if entry.Disposition != DispositionKnown && entry.Disposition != DispositionResolved {
			return errorsf("known counterexample %q has no explicit disposition", id)
		}
	}
	return nil
}
