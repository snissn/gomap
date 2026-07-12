package powerlossoracle

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestGenerateVariantsDeterministicBoundedCoverageAndNegativeControls(t *testing.T) {
	first := variantFixture(t, "one")
	second := variantFixture(t, "different/depth")
	// A map-derived order must not affect IDs, seeds, or coverage.
	resources := map[string]DirtyResource{}
	for _, resource := range second.Dependencies {
		resources[resource.ID] = resource
	}
	second.Dependencies = second.Dependencies[:0]
	for _, resource := range resources {
		second.Dependencies = append(second.Dependencies, resource)
	}

	started := time.Now()
	gotFirst, coverage, err := GenerateVariants(first)
	generationRuntime := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	gotSecond, secondCoverage, err := GenerateVariants(second)
	if err != nil {
		t.Fatal(err)
	}
	if coverage.Generated != 14 || secondCoverage.Generated != coverage.Generated {
		t.Fatalf("generated=%d second=%d want=14 coverage=%+v", coverage.Generated, secondCoverage.Generated, coverage)
	}
	for _, family := range first.RequiredFamilies {
		if coverage.ByFamily[family] == 0 {
			t.Fatalf("required family %s was silently skipped: %+v", family, coverage)
		}
	}
	for _, format := range []FormatKind{FormatMeta, FormatRootRecord, FormatFreelist, FormatIndexPage} {
		if coverage.ByFormat[format] != 1 {
			t.Fatalf("format %s coverage=%d want=1: %+v", format, coverage.ByFormat[format], coverage)
		}
	}
	if idsAndSeeds(gotFirst) == "" || idsAndSeeds(gotFirst) != idsAndSeeds(gotSecond) {
		t.Fatalf("IDs/seeds changed across map order or host path layout:\nfirst=%s\nsecond=%s", idsAndSeeds(gotFirst), idsAndSeeds(gotSecond))
	}
	for _, variant := range gotFirst {
		if !isExpectedResult(variant.Expected) {
			t.Fatalf("variant %s has no expected result", variant.ID)
		}
	}

	dataOnly := requireVariantFamily(t, gotFirst, VariantDataWithoutNamespace)
	if containsString(dataOnly.Model.StablePaths(), first.Dependencies[0].Path) {
		t.Fatalf("data-without-namespace unexpectedly persisted new name %q", first.Dependencies[0].Path)
	}
	nameOnly := requireVariantFamily(t, gotFirst, VariantNamespaceWithoutData)
	if !containsString(nameOnly.Model.StablePaths(), first.Dependencies[0].Path) {
		t.Fatalf("namespace-without-data omitted new name %q", first.Dependencies[0].Path)
	}
	if nameOnly.Model.StableSizeBytes() >= requireVariantFamily(t, gotFirst, VariantFullWriteback).Model.StableSizeBytes() {
		t.Fatal("namespace-without-data did not retain a truncated/missing required file body")
	}
	if requireVariantFamily(t, gotFirst, VariantTargetMetaOnly).Model.StableFingerprint() == requireVariantFamily(t, gotFirst, VariantSyncedOnly).Model.StableFingerprint() {
		t.Fatal("target-meta-only negative control persisted no target metadata")
	}
	if requireVariantFamily(t, gotFirst, VariantOldPageReuse).Model.StableFingerprint() == requireVariantFamily(t, gotFirst, VariantSyncedOnly).Model.StableFingerprint() {
		t.Fatal("old-page-reuse negative control persisted no later page bytes")
	}

	seen := map[string]bool{}
	min, max := len(gotFirst), 0
	shardSizes := make([]int, 4)
	for shard := 0; shard < 4; shard++ {
		selected, err := ShardVariants(gotFirst, shard, 4)
		if err != nil {
			t.Fatal(err)
		}
		if len(selected) < min {
			min = len(selected)
		}
		if len(selected) > max {
			max = len(selected)
		}
		shardSizes[shard] = len(selected)
		for _, variant := range selected {
			if seen[variant.ID] {
				t.Fatalf("variant %s appeared in multiple shards", variant.ID)
			}
			seen[variant.ID] = true
		}
	}
	if len(seen) != len(gotFirst) || max-min > 1 {
		t.Fatalf("shard balance=(%d,%d) selected=%d want=%d", min, max, len(seen), len(gotFirst))
	}
	peakBytes := int64(0)
	for _, variant := range gotFirst {
		if size := variant.Model.StableSizeBytes(); size > peakBytes {
			peakBytes = size
		}
	}
	t.Logf("committed generator fixture: images=%d limit=%d runtime=%s peak_temp_storage_bytes=%d shard_sizes=%v", len(gotFirst), MaxVariantsPerCut, generationRuntime, peakBytes, shardSizes)
}

func TestGenerateVariantsFailsClosedOnUnknownSkippedAndUnboundedFamilies(t *testing.T) {
	spec := variantFixture(t, "fail-closed")
	spec.RequiredFamilies = append(spec.RequiredFamilies, VariantFamily("future-family"))
	if _, _, err := GenerateVariants(spec); err == nil || !strings.Contains(err.Error(), "unknown required variant family") {
		t.Fatalf("unknown family err=%v", err)
	}
	spec = variantFixture(t, "skip")
	spec.RequiredFamilies = []VariantFamily{VariantOldPageReuse}
	spec.OldPageWrites = nil
	if _, _, err := GenerateVariants(spec); err == nil || !strings.Contains(err.Error(), "silently skipped") {
		t.Fatalf("skipped family err=%v", err)
	}
	spec = variantFixture(t, "bound")
	spec.MaxVariants = 3
	if _, _, err := GenerateVariants(spec); err == nil || !strings.Contains(err.Error(), "bounded variant limit") {
		t.Fatalf("bounded variant err=%v", err)
	}
	spec = variantFixture(t, "expected")
	delete(spec.ExpectedByFamily, VariantSyncedOnly)
	if _, _, err := GenerateVariants(spec); err == nil || !strings.Contains(err.Error(), "without an expected result") {
		t.Fatalf("missing expected result err=%v", err)
	}
}

func TestReplaySelectorUsesExactStableCutVariantAndSeed(t *testing.T) {
	variants, _, err := GenerateVariants(variantFixture(t, "replay"))
	if err != nil {
		t.Fatal(err)
	}
	want := variants[len(variants)/2]
	selected, err := SelectReplayVariant(variants, ReplaySelector{CutID: want.CutID, VariantID: want.ID, Seed: want.Seed})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].ID != want.ID {
		t.Fatalf("selected=%v want=%s", selected, want.ID)
	}
	if _, err := SelectReplayVariant(variants, ReplaySelector{CutID: want.CutID, VariantID: want.ID, Seed: want.Seed + 1}); err == nil {
		t.Fatal("wrong replay seed unexpectedly matched")
	}
}

func TestCounterexampleLedgerFailsClosedOnDisappearanceAndCoverageDrift(t *testing.T) {
	variants, _, err := GenerateVariants(variantFixture(t, "ledger"))
	if err != nil {
		t.Fatal(err)
	}
	variant := requireVariantFamily(t, variants, VariantTargetMetaOnly)
	entry := CounterexampleLedgerEntry{
		ID:                "known-meta-dependency-hole",
		Replay:            ReplaySpec{Package: "./TreeDB", TestName: "TestWitness", CutID: variant.CutID, VariantID: variant.ID, Seed: variant.Seed},
		Invariant:         InvariantIncompleteRecoverableRoot,
		ProducerOperation: "DB.Checkpoint",
		CutID:             variant.CutID,
		CutPoint:          AfterMetaWrite,
		VariantID:         variant.ID,
		Seed:              variant.Seed,
		VariantFamilies:   []VariantFamily{VariantTargetMetaOnly, VariantOneMissingDependency},
		Expected:          ExpectedCorruption,
		Observed:          ExpectedCorruption,
		Owner:             "#3679",
		Disposition:       DispositionKnown,
		KnownViolation:    &KnownViolation{Kind: KnownViolationInvariant, Invariant: InvariantIncompleteRecoverableRoot},
	}
	originalWitnesses := CounterexampleWitnesses
	CounterexampleWitnesses = []CounterexampleWitness{{ID: entry.ID, Package: entry.Replay.Package, TestName: entry.Replay.TestName}}
	t.Cleanup(func() { CounterexampleWitnesses = originalWitnesses })
	ledger := CounterexampleLedger{SchemaVersion: CounterexampleLedgerSchemaVersion, MaxVariantsPerCut: MaxVariantsPerCut, KnownCounterexamples: []string{entry.ID}, Entries: []CounterexampleLedgerEntry{entry}}
	generated := map[string][]Variant{variant.CutID: variants}
	if err := ValidateCounterexampleLedger(ledger, generated); err != nil {
		t.Fatal(err)
	}
	missing := ledger
	missing.Entries = nil
	if err := ValidateCounterexampleLedger(missing, generated); err == nil {
		t.Fatal("disappearing known counterexample unexpectedly validated")
	}
	deletedEverywhere := ledger
	deletedEverywhere.KnownCounterexamples = nil
	deletedEverywhere.Entries = nil
	if err := RequireRetainedCounterexamples(deletedEverywhere, []string{entry.ID}); err == nil {
		t.Fatal("counterexample deleted from both ledger collections unexpectedly satisfied retained inventory")
	}
	drift := ledger
	drift.Entries = append([]CounterexampleLedgerEntry(nil), ledger.Entries...)
	drift.Entries[0].VariantFamilies = append(drift.Entries[0].VariantFamilies, VariantOldPageReuse)
	generatedWithoutReuse := append([]Variant(nil), variants...)
	for i := len(generatedWithoutReuse) - 1; i >= 0; i-- {
		if generatedWithoutReuse[i].Family == VariantOldPageReuse {
			generatedWithoutReuse = append(generatedWithoutReuse[:i], generatedWithoutReuse[i+1:]...)
		}
	}
	if err := ValidateCounterexampleLedger(drift, map[string][]Variant{variant.CutID: generatedWithoutReuse}); err == nil || !strings.Contains(err.Error(), "silently skipped") {
		t.Fatalf("coverage drift err=%v", err)
	}
	extra := ledger
	extraEntry := entry
	extraEntry.ID = "json-only-witness"
	extra.Entries = append(append([]CounterexampleLedgerEntry(nil), ledger.Entries...), extraEntry)
	extra.KnownCounterexamples = append(append([]string(nil), ledger.KnownCounterexamples...), extraEntry.ID)
	if err := ValidateCounterexampleLedger(extra, generated); err == nil || !strings.Contains(err.Error(), "no code-owned real witness") {
		t.Fatalf("json-only witness err=%v", err)
	}
}

func TestValidateVariantObservationRequiresExactPublicClassification(t *testing.T) {
	variant := Variant{CutID: "cut", ID: "variant", Seed: 1, Expected: ExpectedOldRoot}
	if err := ValidateVariantObservation(variant, VariantObservation{Result: ExpectedOldRoot}, nil); err == nil {
		t.Fatal("old-root without successful Open unexpectedly validated")
	}
	variant.Expected = ExpectedTypedError
	if err := ValidateVariantObservation(variant, VariantObservation{Result: ExpectedTypedError}, nil); err == nil {
		t.Fatal("typed error without sentinel unexpectedly validated")
	}
	variant.Expected = ExpectedCorruption
	if err := ValidateVariantObservation(variant, VariantObservation{Opened: true, Result: ExpectedCorruption}, nil); err == nil {
		t.Fatal("corruption without named invariant unexpectedly validated")
	}
	variant.Expected = ExpectedSuffixDiscard
	if err := ValidateVariantObservation(variant, VariantObservation{Result: ExpectedSuffixDiscard}, nil); err == nil {
		t.Fatal("suffix discard without successful Open unexpectedly validated")
	}
}

func TestValidateVariantObservationAllowsOrthogonalKnownInvariant(t *testing.T) {
	variant := Variant{CutID: "cut", ID: "variant", Seed: 1, Expected: ExpectedOldRoot}
	entry := CounterexampleLedgerEntry{
		ID: "known", CutID: variant.CutID, VariantID: variant.ID, Seed: variant.Seed,
		Expected: ExpectedOldRoot, Observed: ExpectedOldRoot, Owner: "#1", Disposition: DispositionKnown,
		KnownViolation: &KnownViolation{Kind: KnownViolationInvariant, Invariant: InvariantRequiredNamespaceEntryMissing},
	}
	observation := VariantObservation{Opened: true, Result: ExpectedOldRoot, NamedInvariant: InvariantRequiredNamespaceEntryMissing}
	if err := ValidateVariantObservation(variant, observation, &entry); err != nil {
		t.Fatal(err)
	}
	observation.NamedInvariant = InvariantKeyStateMismatch
	if err := ValidateVariantObservation(variant, observation, &entry); err == nil {
		t.Fatal("wrong named invariant unexpectedly validated")
	}
}

func TestGenerateVariantsRejectsInvalidTornBoundaries(t *testing.T) {
	for _, test := range []struct {
		name      string
		boundary  TornBoundary
		extra     *TornBoundary
		unchanged bool
	}{
		{name: "unknown-format", boundary: TornBoundary{ID: "bad", Format: FormatKind("future"), Offset: 0, Length: 8, Persisted: 4}},
		{name: "outside-changed-range", boundary: TornBoundary{ID: "bad", Format: FormatMeta, Offset: 8, Length: 8, Persisted: 4}},
		{name: "declared-but-unchanged", boundary: TornBoundary{ID: "bad", Format: FormatMeta, Offset: 0, Length: 8, Persisted: 4}, unchanged: true},
		{name: "duplicate-id", boundary: TornBoundary{ID: "same", Format: FormatMeta, Offset: 0, Length: 8, Persisted: 4}, extra: &TornBoundary{ID: "same", Format: FormatRootRecord, Offset: 0, Length: 8, Persisted: 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := variantFixture(t, test.name)
			if test.unchanged {
				id := spec.Model.volatile[spec.TargetMeta.Path]
				copy(spec.Model.inodes[id].volatile[:8], spec.Model.inodes[id].stable[:8])
			}
			spec.TargetMeta.Torn = []TornBoundary{test.boundary}
			if test.extra != nil {
				spec.TargetMeta.Torn = append(spec.TargetMeta.Torn, *test.extra)
			}
			if _, _, err := GenerateVariants(spec); err == nil {
				t.Fatal("invalid torn boundary unexpectedly generated")
			}
		})
	}
}

func variantFixture(t *testing.T, layout string) CutSpec {
	t.Helper()
	root := t.TempDir()
	prefix := filepath.FromSlash(layout)
	assets := filepath.Join(root, prefix, "assets")
	if err := os.MkdirAll(assets, 0o755); err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{
		"index":    filepath.Join(prefix, "index.db"),
		"value":    filepath.Join(prefix, "assets", "value-0002.vlog"),
		"outer":    filepath.Join(prefix, "outer.leaf"),
		"root":     filepath.Join(prefix, "root.record"),
		"freelist": filepath.Join(prefix, "freelist.page"),
	}
	for _, key := range []string{"index", "outer", "root", "freelist"} {
		if err := os.WriteFile(filepath.Join(root, paths[key]), []byte("00000000000000000000000000000000"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"index", "outer", "root", "freelist"} {
		if err := os.WriteFile(filepath.Join(root, paths[key]), []byte("11111111222222223333333344444444"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, paths["value"]), []byte("new-value-log-body"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := model.Overlay(root); err != nil {
		t.Fatal(err)
	}
	resource := func(kind ResourceKind, id, path string, format FormatKind) DirtyResource {
		out := DirtyResource{Kind: kind, ID: id, Path: filepath.ToSlash(path)}
		if format != "" {
			out.Torn = []TornBoundary{{ID: "header-body", Format: format, Offset: 0, Length: 8, Persisted: 4}}
		}
		return out
	}
	value := resource(ResourceValueLog, "value-segment-2", paths["value"], "")
	value.NewName = true
	value.NamespaceDirs = []string{filepath.ToSlash(filepath.Join(prefix, "assets"))}
	dependencies := []DirtyResource{
		value,
		resource(ResourceOuterLeaf, "outer-generation-2", paths["outer"], FormatIndexPage),
		resource(ResourceAuxiliary, "root-record-2", paths["root"], FormatRootRecord),
		resource(ResourceFreelist, "freelist-generation-2", paths["freelist"], FormatFreelist),
	}
	target := resource(ResourceIndex, "meta-slot-1", paths["index"], FormatMeta)
	target.Ranges = []ByteRange{{Offset: 0, Length: 8}}
	return CutSpec{
		ID:               "checkpoint-generation-2",
		Point:            AfterMetaWrite,
		Occurrence:       1,
		Model:            model,
		TargetMeta:       &target,
		Dependencies:     dependencies,
		OldPageWrites:    []DirtyResource{{Kind: ResourceIndex, ID: "old-live-page-7", Path: filepath.ToSlash(paths["index"]), Ranges: []ByteRange{{Offset: 16, Length: 8}}}},
		RequiredFamilies: append([]VariantFamily(nil), VariantFamilies...),
		ExpectedByFamily: map[VariantFamily]ExpectedResult{
			VariantSyncedOnly:           ExpectedOldRoot,
			VariantTargetMetaOnly:       ExpectedCorruption,
			VariantOneMissingDependency: ExpectedCorruption,
			VariantDataWithoutNamespace: ExpectedOldRoot,
			VariantNamespaceWithoutData: ExpectedOldRoot,
			VariantFullWriteback:        ExpectedNewRoot,
			VariantTornFormat:           ExpectedOldRoot,
			VariantOldPageReuse:         ExpectedCorruption,
		},
	}
}

func idsAndSeeds(variants []Variant) string {
	parts := make([]string, 0, len(variants))
	for _, variant := range variants {
		parts = append(parts, fmt.Sprintf("%s=%d", variant.ID, variant.Seed))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

func requireVariantFamily(t *testing.T, variants []Variant, family VariantFamily) Variant {
	t.Helper()
	for _, variant := range variants {
		if variant.Family == family {
			return variant
		}
	}
	t.Fatalf("missing family %s in %v", family, variants)
	return Variant{}
}

func containsString(values []string, want string) bool {
	position := sort.SearchStrings(values, want)
	return position < len(values) && values[position] == want
}
