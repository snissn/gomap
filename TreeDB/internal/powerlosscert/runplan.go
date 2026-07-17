package powerlosscert

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

const RunPlanSchemaVersion = "treedb-power-loss-run-plan/v1"

const (
	reopenModeReadWrite = "read-write"
	reopenModeReadOnly  = "read-only"
)

var (
	caseIDPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	testNamePattern = regexp.MustCompile(`^Test[A-Za-z0-9_]+$`)
)

// RecoveryExpectation is fixed before a certification command runs. The
// runner compares it with recovery_trace.json and only then records the case
// as an observed outcome in a child manifest.
type RecoveryExpectation struct {
	Rejected   bool   `json:"rejected"`
	ErrorType  string `json:"error_type"`
	CommitSeq  uint64 `json:"commit_seq"`
	AppliedLSN uint64 `json:"applied_lsn"`
}

// RunCase is the immutable semantic contract for one exact replay. Runtime
// fields such as observed event count, artifact hashes, and captured output do
// not belong here; the runner derives them from the completed command.
type RunCase struct {
	ID                     string              `json:"id"`
	Package                string              `json:"package"`
	TestName               string              `json:"test_name"`
	Profile                string              `json:"profile"`
	Acknowledgement        string              `json:"acknowledgement_shape"`
	ResourceShapes         []string            `json:"data_resource_shapes"`
	DependencyGraph        string              `json:"dependency_graph"`
	StorageBoundaries      []string            `json:"storage_boundaries"`
	WritebackVariant       string              `json:"writeback_variant"`
	FailureClasses         []string            `json:"failure_classes"`
	ExpectedDurableHorizon string              `json:"expected_durable_horizon"`
	ExpectedOutcome        string              `json:"expected_outcome"`
	ExpectedTypedError     string              `json:"expected_typed_error"`
	State                  WitnessState        `json:"state"`
	CounterexampleID       string              `json:"counterexample_id,omitempty"`
	NegativeControlID      string              `json:"negative_control_id,omitempty"`
	Seed                   uint64              `json:"seed"`
	CutID                  string              `json:"cut_id"`
	VariantID              string              `json:"variant_id"`
	CutPoint               string              `json:"cut_point"`
	ReopenMode             string              `json:"reopen_mode"`
	ExpectedRecovery       RecoveryExpectation `json:"expected_recovery"`
	ClaimBoundary          string              `json:"claim_boundary"`
}

// RunPlan freezes repository provenance and the exact modeled cases before
// execution. PullRequests includes every implementation/review merge whose
// behavior is certified by RepositorySHA.
type RunPlan struct {
	SchemaVersion   string        `json:"schema_version"`
	RepositorySHA   string        `json:"repository_sha"`
	Issue           int           `json:"issue"`
	PullRequests    []PullRequest `json:"pull_requests"`
	ToolVersion     string        `json:"tool_version"`
	FilesystemModel string        `json:"filesystem_model"`
	ClaimBoundary   string        `json:"claim_boundary"`
	Cases           []RunCase     `json:"cases"`
}

func ParseRunPlan(data []byte) (RunPlan, error) {
	var plan RunPlan
	if err := decodeStrict(data, &plan); err != nil {
		return RunPlan{}, fmt.Errorf("powerlosscert: decode run plan: %w", err)
	}
	return plan, nil
}

func ValidateRunPlan(inventory RiskInventory, plan RunPlan) error {
	if err := validateRiskInventory(inventory); err != nil {
		return err
	}
	if plan.SchemaVersion != RunPlanSchemaVersion {
		return fmt.Errorf("powerlosscert: run plan schema_version=%q want=%q", plan.SchemaVersion, RunPlanSchemaVersion)
	}
	if !validHex(plan.RepositorySHA, 40) || plan.Issue <= 0 || plan.ToolVersion == "" || plan.FilesystemModel == "" || plan.ClaimBoundary == "" {
		return fmt.Errorf("powerlosscert: run plan has incomplete provenance or claim metadata")
	}
	if len(plan.PullRequests) == 0 {
		return fmt.Errorf("powerlosscert: run plan has no pull-request provenance")
	}
	seenPRs := make(map[int]bool, len(plan.PullRequests))
	for _, pr := range plan.PullRequests {
		if pr.Number <= 0 || !validHex(pr.HeadSHA, 40) || !validHex(pr.MergeSHA, 40) {
			return fmt.Errorf("powerlosscert: run plan has invalid pull-request provenance: %+v", pr)
		}
		if seenPRs[pr.Number] {
			return fmt.Errorf("powerlosscert: run plan duplicates pull request %d", pr.Number)
		}
		seenPRs[pr.Number] = true
	}
	if len(plan.Cases) == 0 {
		return fmt.Errorf("powerlosscert: run plan has no cases")
	}
	seenCases := make(map[string]bool, len(plan.Cases))
	coverageManifest := ChildManifest{Witnesses: make([]Witness, 0, len(plan.Cases))}
	for _, runCase := range plan.Cases {
		if seenCases[runCase.ID] {
			return fmt.Errorf("powerlosscert: run plan duplicates case id %q", runCase.ID)
		}
		seenCases[runCase.ID] = true
		if err := validateRunCase(inventory, runCase); err != nil {
			return err
		}
		coverageManifest.Witnesses = append(coverageManifest.Witnesses, Witness{
			ID:                runCase.ID,
			EvidenceTier:      EvidenceTierModeledCrash,
			Profile:           runCase.Profile,
			Acknowledgement:   runCase.Acknowledgement,
			ResourceShapes:    runCase.ResourceShapes,
			StorageBoundaries: runCase.StorageBoundaries,
			WritebackVariant:  runCase.WritebackVariant,
			FailureClasses:    runCase.FailureClasses,
			CounterexampleID:  runCase.CounterexampleID,
			NegativeControlID: runCase.NegativeControlID,
		})
	}
	coverage, err := BuildCoverageReport(inventory, []ChildManifest{coverageManifest})
	if err != nil {
		return err
	}
	if !coverage.Complete {
		return fmt.Errorf("powerlosscert: run plan has incomplete frozen risk coverage: %s", strings.Join(coverage.Gaps, ", "))
	}
	return nil
}

func validateRunCase(inventory RiskInventory, runCase RunCase) error {
	prefix := fmt.Sprintf("powerlosscert: run case %q", runCase.ID)
	if !caseIDPattern.MatchString(runCase.ID) {
		return fmt.Errorf("%s has unsafe id", prefix)
	}
	cleanPackage := filepath.ToSlash(filepath.Clean(filepath.FromSlash(runCase.Package)))
	if !strings.HasPrefix(runCase.Package, "./TreeDB") || cleanPackage != strings.TrimPrefix(runCase.Package, "./") || strings.Contains(cleanPackage, "..") {
		return fmt.Errorf("%s has unsupported package %q", prefix, runCase.Package)
	}
	if !testNamePattern.MatchString(runCase.TestName) {
		return fmt.Errorf("%s has unsafe test name %q", prefix, runCase.TestName)
	}
	if runCase.VariantID == "" {
		return fmt.Errorf("%s has empty variant id", prefix)
	}
	cutPoint, occurrence, err := parseCutAddress(runCase.CutID)
	if err != nil {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	if cutPoint != runCase.CutPoint {
		return fmt.Errorf("%s cut_id point=%q want=%q", prefix, cutPoint, runCase.CutPoint)
	}
	if runCase.ReopenMode != reopenModeReadWrite && runCase.ReopenMode != reopenModeReadOnly {
		return fmt.Errorf("%s has invalid reopen mode %q", prefix, runCase.ReopenMode)
	}
	class := modeledOutcomeClass(runCase.ExpectedOutcome)
	if runCase.ExpectedRecovery.Rejected {
		if class != "rejected" || runCase.ExpectedTypedError == "" || runCase.ExpectedTypedError == "none" || runCase.ExpectedRecovery.ErrorType != runCase.ExpectedTypedError {
			return fmt.Errorf("%s has inconsistent rejected outcome contract", prefix)
		}
	} else if class != "accepted" || runCase.ExpectedTypedError != "none" || runCase.ExpectedRecovery.ErrorType != "" {
		return fmt.Errorf("%s has inconsistent accepted outcome contract", prefix)
	}

	// Reuse the child-manifest validator with deterministic placeholder hashes.
	// This keeps execution-plan and retained-manifest semantics from drifting.
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	binaryPath := "binaries/" + runCase.ID + ".test"
	evidenceDir := "evidence/" + runCase.ID
	env := map[string]string{
		"TREEDB_POWERLOSS_CASE_ID":          runCase.ID,
		"TREEDB_POWERLOSS_CUT_ID":           runCase.CutID,
		"TREEDB_POWERLOSS_VARIANT_ID":       runCase.VariantID,
		"TREEDB_POWERLOSS_SEED":             fmt.Sprintf("%d", runCase.Seed),
		"TREEDB_POWERLOSS_EXPECT_CUT_POINT": runCase.CutPoint,
		"TREEDB_POWERLOSS_EVIDENCE_DIR":     evidenceDir,
		powerLossReopenModeEnv:              runCase.ReopenMode,
		"TREEDB_POWERLOSS_PROFILE":          runCase.Profile,
	}
	witness := Witness{
		ID:                     runCase.ID,
		EvidenceTier:           EvidenceTierModeledCrash,
		Profile:                runCase.Profile,
		Acknowledgement:        runCase.Acknowledgement,
		ResourceShapes:         runCase.ResourceShapes,
		DependencyGraph:        runCase.DependencyGraph,
		StorageBoundaries:      runCase.StorageBoundaries,
		WritebackVariant:       runCase.WritebackVariant,
		FailureClasses:         runCase.FailureClasses,
		ExpectedDurableHorizon: runCase.ExpectedDurableHorizon,
		ExpectedOutcome:        runCase.ExpectedOutcome,
		ActualOutcome:          runCase.ExpectedOutcome,
		TypedError:             runCase.ExpectedTypedError,
		State:                  runCase.State,
		CounterexampleID:       runCase.CounterexampleID,
		NegativeControlID:      runCase.NegativeControlID,
		Seed:                   runCase.Seed,
		CutID:                  runCase.CutID,
		CutPoint:               runCase.CutPoint,
		CutOccurrence:          occurrence,
		ObservedEventCount:     occurrence + 1,
		Command: TestCommand{
			BinaryPath: binaryPath,
			Package:    runCase.Package,
			TestName:   runCase.TestName,
			Args:       []string{"-test.run", "^" + runCase.TestName + "$", "-test.v"},
			Env:        env,
		},
		CutExercised:  true,
		ClaimBoundary: runCase.ClaimBoundary,
	}
	for kind, name := range modeledArtifactNames {
		witness.Artifacts = append(witness.Artifacts, Artifact{Kind: kind, Path: evidenceDir + "/" + name, SHA256: digest})
	}
	if err := validateWitness(prefix, witness, map[string]bool{binaryPath: true}); err != nil {
		return err
	}
	return validateWitnessInventory(inventory, "run-plan", witness)
}
