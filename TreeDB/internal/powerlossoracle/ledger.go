package powerlossoracle

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

const CounterexampleLedgerSchemaVersion = 2

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

const InvariantRequiredNamespaceEntryMissing = "required-namespace-entry-missing"

func isExpectedResult(result ExpectedResult) bool {
	switch result {
	case ExpectedOldRoot, ExpectedNewRoot, ExpectedSuffixDiscard, ExpectedTypedError, ExpectedCorruption:
		return true
	default:
		return false
	}
}

type KnownViolationKind string

const (
	KnownViolationInvariant KnownViolationKind = "named-invariant"
	KnownViolationSentinel  KnownViolationKind = "typed-sentinel"
)

type KnownViolation struct {
	Kind      KnownViolationKind `json:"kind"`
	Invariant string             `json:"invariant,omitempty"`
	Sentinel  string             `json:"sentinel,omitempty"`
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
	Observed          ExpectedResult            `json:"observed_result"`
	Owner             string                    `json:"owner"`
	Disposition       CounterexampleDisposition `json:"disposition"`
	KnownViolation    *KnownViolation           `json:"known_violation,omitempty"`
	Replay            ReplaySpec                `json:"replay"`
}

// ReplaySpec keeps exact replay selectors machine-readable. Command renders
// the shell command only after validation, so ledger integrity never depends
// on substring matching a free-form command.
type ReplaySpec struct {
	Package   string `json:"package"`
	TestName  string `json:"test_name"`
	CutID     string `json:"cut_id"`
	VariantID string `json:"variant_id"`
	Seed      uint64 `json:"seed"`
}

func (r ReplaySpec) Command() string {
	return fmt.Sprintf("%s=%s %s=%s %s=%d GOWORK=off go test %s -run '^%s$' -count=1",
		EnvReplayCut, r.CutID, EnvReplayVariant, r.VariantID, EnvReplaySeed, r.Seed, r.Package, r.TestName)
}

type CounterexampleWitness struct {
	ID       string
	Package  string
	TestName string
}

// CounterexampleWitnesses is the code-owned registry of real integration
// witnesses. The checked-in ledger must match it exactly in both directions.
var CounterexampleWitnesses = []CounterexampleWitness{
	{ID: "new-meta-before-index-closure", Package: "./TreeDB", TestName: "TestPowerLossOracleCounterexampleNewMetaMissingClosure"},
	{ID: "new-meta-before-value-log-closure", Package: "./TreeDB", TestName: "TestPowerLossOracleCounterexampleNewMetaMissingClosure"},
	{ID: "new-meta-before-outer-leaf-closure", Package: "./TreeDB", TestName: "TestPowerLossOracleCounterexampleNewMetaMissingClosure"},
	{ID: "new-file-bytes-before-namespace", Package: "./TreeDB", TestName: "TestPowerLossOracleAdversarialNewFileNamespaceMismatch"},
	{ID: "torn-target-meta", Package: "./TreeDB", TestName: "TestPowerLossOracleCounterexampleNewMetaMissingClosure"},
	{ID: "relaxed-command-frame-before-rid", Package: "./TreeDB", TestName: "TestPowerLossOracleCounterexampleRelaxedCommandFrameMissingRID"},
	{ID: "chunked-sync-intermediate-root", Package: "./TreeDB", TestName: "TestPowerLossOracleCounterexampleChunkedSyncIntermediateRoot"},
	{ID: "older-meta-live-page-reused", Package: "./TreeDB/db", TestName: "TestPowerLossOracleCounterexampleRecoverablePageReuse"},
	{ID: "stale-build-base-root-publication", Package: "./TreeDB/db", TestName: "TestPowerLossCertificationStaleBuildBasePublicReopen"},
}

// VariantObservation is the structured result of one real public Open. Tests
// derive it from exact root/key/LSN state, an errors.Is/As sentinel, or an
// exact Scenario invariant; free-form error strings are not classifications.
type VariantObservation struct {
	Opened         bool
	Result         ExpectedResult
	NamedInvariant string
	TypedSentinel  string
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
		if entry.ID == "" || entry.Invariant == "" || entry.ProducerOperation == "" || entry.CutID == "" || entry.CutPoint == "" || entry.VariantID == "" || entry.Seed == 0 || entry.Owner == "" || entry.Replay.Package == "" || entry.Replay.TestName == "" {
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
		if !isExpectedResult(entry.Observed) {
			return errorsf("counterexample ledger entry %q has unknown observed result %q", entry.ID, entry.Observed)
		}
		switch entry.Disposition {
		case DispositionResolved:
			if entry.Observed != entry.Expected {
				return errorsf("resolved counterexample ledger entry %q observed=%q expected=%q", entry.ID, entry.Observed, entry.Expected)
			}
			if entry.KnownViolation != nil {
				return errorsf("resolved counterexample ledger entry %q retains a known violation", entry.ID)
			}
		case DispositionKnown:
			if err := validateKnownViolation(entry); err != nil {
				return err
			}
		}
		if len(entry.VariantFamilies) == 0 {
			return errorsf("counterexample ledger entry %q has no required variant families", entry.ID)
		}
		if err := validateRequiredFamilies(entry.VariantFamilies); err != nil {
			return fmt.Errorf("entry %s: %w", entry.ID, err)
		}
		if entry.Replay.CutID != entry.CutID || entry.Replay.VariantID != entry.VariantID || entry.Replay.Seed != entry.Seed {
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
	return ValidateCounterexampleWitnessRegistry(ledger)
}

// ValidateCounterexampleWitnessRegistry rejects ledger witnesses that are not
// owned by real code and registry witnesses that disappear from the ledger.
func ValidateCounterexampleWitnessRegistry(ledger CounterexampleLedger) error {
	registered := make(map[string]CounterexampleWitness, len(CounterexampleWitnesses))
	for _, witness := range CounterexampleWitnesses {
		if witness.ID == "" || witness.Package == "" || witness.TestName == "" {
			return errorsf("code-owned counterexample witness has an empty field: %+v", witness)
		}
		if _, duplicate := registered[witness.ID]; duplicate {
			return errorsf("code-owned counterexample witness %q is duplicated", witness.ID)
		}
		registered[witness.ID] = witness
	}
	seen := make(map[string]bool, len(ledger.Entries))
	for _, entry := range ledger.Entries {
		witness, ok := registered[entry.ID]
		if !ok {
			return errorsf("counterexample ledger entry %q has no code-owned real witness", entry.ID)
		}
		if entry.Replay.Package != witness.Package || entry.Replay.TestName != witness.TestName {
			return errorsf("counterexample ledger entry %q replay witness=(%s,%s) code=(%s,%s)", entry.ID, entry.Replay.Package, entry.Replay.TestName, witness.Package, witness.TestName)
		}
		seen[entry.ID] = true
	}
	for id := range registered {
		if !seen[id] {
			return errorsf("code-owned counterexample witness %q disappeared from ledger", id)
		}
	}
	return nil
}

func validateKnownViolation(entry CounterexampleLedgerEntry) error {
	if entry.KnownViolation == nil {
		return errorsf("known counterexample ledger entry %q has no structured known violation", entry.ID)
	}
	switch entry.KnownViolation.Kind {
	case KnownViolationInvariant:
		if entry.KnownViolation.Invariant == "" || entry.KnownViolation.Invariant != entry.Invariant || entry.KnownViolation.Sentinel != "" {
			return errorsf("known counterexample ledger entry %q has invalid named invariant violation", entry.ID)
		}
	case KnownViolationSentinel:
		if entry.KnownViolation.Sentinel == "" || entry.KnownViolation.Invariant != "" {
			return errorsf("known counterexample ledger entry %q has invalid typed sentinel violation", entry.ID)
		}
		if entry.Observed != ExpectedTypedError {
			return errorsf("known counterexample ledger entry %q typed sentinel observed=%q want=%q", entry.ID, entry.Observed, ExpectedTypedError)
		}
	default:
		return errorsf("known counterexample ledger entry %q has unknown violation kind %q", entry.ID, entry.KnownViolation.Kind)
	}
	return nil
}

// BindCounterexampleWitnesses anchors ledger replay identities to variants
// generated by the real named integration witness, rather than only to a
// synthetic inventory fixture.
func BindCounterexampleWitnesses(ledger CounterexampleLedger, testName string, variants []Variant) (map[string]CounterexampleLedgerEntry, error) {
	if err := ValidateCounterexampleWitnessRegistry(ledger); err != nil {
		return nil, err
	}
	generated := make(map[string]Variant, len(variants))
	for _, variant := range variants {
		if _, duplicate := generated[variant.ID]; duplicate {
			return nil, errorsf("real witness %s generated duplicate variant selector %s", testName, variant.ID)
		}
		generated[variant.ID] = variant
	}
	bound := make(map[string]CounterexampleLedgerEntry)
	for _, entry := range ledger.Entries {
		if entry.Replay.TestName != testName {
			continue
		}
		variant, exists := generated[entry.VariantID]
		if !exists || variant.CutID != entry.CutID || variant.Seed != entry.Seed {
			return nil, errorsf("ledger entry %q is detached from real witness %s", entry.ID, testName)
		}
		if variant.Expected != entry.Expected {
			return nil, errorsf("ledger entry %q expected=%q real variant=%q", entry.ID, entry.Expected, variant.Expected)
		}
		if _, duplicate := bound[variant.ID]; duplicate {
			return nil, errorsf("real witness %s has multiple ledger entries for variant %s", testName, variant.ID)
		}
		bound[variant.ID] = entry
	}
	if len(bound) == 0 {
		return nil, errorsf("real witness %s has no counterexample ledger binding", testName)
	}
	return bound, nil
}

// ValidateVariantObservation enforces a real public-Open classification. A
// positive or resolved image must equal Expected. A known mismatch is accepted
// only when the exact bound ledger entry records the same structured invariant
// or typed sentinel and retains an owner.
func ValidateVariantObservation(variant Variant, observation VariantObservation, entry *CounterexampleLedgerEntry) error {
	if !isExpectedResult(observation.Result) {
		return errorsf("variant %s has unknown observed result %q", variant.ID, observation.Result)
	}
	switch observation.Result {
	case ExpectedOldRoot, ExpectedNewRoot, ExpectedSuffixDiscard:
		if !observation.Opened || observation.TypedSentinel != "" {
			return errorsf("variant %s result %q lacks an exact successful public-Open classification", variant.ID, observation.Result)
		}
	case ExpectedTypedError:
		if observation.Opened || observation.TypedSentinel == "" || observation.NamedInvariant != "" {
			return errorsf("variant %s typed-error observation lacks an errors.Is/As sentinel", variant.ID)
		}
	case ExpectedCorruption:
		if !observation.Opened || observation.NamedInvariant == "" || observation.TypedSentinel != "" {
			return errorsf("variant %s corruption observation lacks a successful Open and exact named invariant", variant.ID)
		}
	}
	if entry == nil {
		if observation.NamedInvariant != "" && observation.Result != ExpectedCorruption {
			return errorsf("variant %s has unbound named invariant %q", variant.ID, observation.NamedInvariant)
		}
		if observation.Result != variant.Expected {
			return errorsf("variant %s observed=%q expected=%q without a bound known counterexample", variant.ID, observation.Result, variant.Expected)
		}
		return nil
	}
	if entry.VariantID != variant.ID || entry.CutID != variant.CutID || entry.Seed != variant.Seed || entry.Expected != variant.Expected {
		return errorsf("variant %s observation is bound to stale ledger entry %q", variant.ID, entry.ID)
	}
	if observation.Result != entry.Observed {
		return errorsf("variant %s observed=%q ledger=%q", variant.ID, observation.Result, entry.Observed)
	}
	if entry.Disposition == DispositionResolved {
		if observation.Result != variant.Expected || observation.NamedInvariant != "" || observation.TypedSentinel != "" {
			return errorsf("resolved variant %s observed=%q expected=%q", variant.ID, observation.Result, variant.Expected)
		}
		return nil
	}
	if entry.Disposition != DispositionKnown || entry.Owner == "" || entry.KnownViolation == nil {
		return errorsf("variant %s mismatch has no retained known-counterexample owner", variant.ID)
	}
	switch entry.KnownViolation.Kind {
	case KnownViolationInvariant:
		if !observation.Opened || observation.NamedInvariant != entry.KnownViolation.Invariant || observation.TypedSentinel != "" {
			return errorsf("variant %s named invariant=%q ledger=%q", variant.ID, observation.NamedInvariant, entry.KnownViolation.Invariant)
		}
	case KnownViolationSentinel:
		if observation.TypedSentinel != entry.KnownViolation.Sentinel {
			return errorsf("variant %s typed sentinel=%q ledger=%q", variant.ID, observation.TypedSentinel, entry.KnownViolation.Sentinel)
		}
	default:
		return errorsf("variant %s has unknown ledger violation kind %q", variant.ID, entry.KnownViolation.Kind)
	}
	return nil
}
