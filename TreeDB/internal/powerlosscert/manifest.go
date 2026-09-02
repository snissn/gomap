// Package powerlosscert validates and selects the committed evidence used by
// TreeDB's current-main power-loss certification. It is test infrastructure;
// production packages do not import it.
package powerlosscert

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
)

const (
	ChildManifestSchemaVersion  = "treedb-power-loss-child/v3"
	RiskInventorySchemaVersion  = "treedb-power-loss-risk-inventory/v1"
	CoverageReportSchemaVersion = "treedb-power-loss-coverage/v1"
	SelectionPlanSchemaVersion  = "treedb-power-loss-selection/v1"
)

const (
	EvidenceTierCleanProcess EvidenceTier = "clean-process"
	EvidenceTierModeledCrash EvidenceTier = "modeled-crash"
	EvidenceTierBlockDevice  EvidenceTier = "block-device"
)

const (
	powerLossReopenModeEnv       = "TREEDB_POWERLOSS_REOPEN_MODE"
	powerLossReplayWindowEnv     = "TREEDB_POWERLOSS_REPLAY_WINDOW"
	powerLossReopenModeReadWrite = "read-write"
	powerLossReopenModeReadOnly  = "read-only"
	powerLossProfileEnv          = "TREEDB_POWERLOSS_PROFILE"
)

const (
	ArtifactKindTestBinary      ArtifactKind = "test-binary"
	ArtifactKindOperationTrace  ArtifactKind = "operation-trace"
	ArtifactKindStableImageTree ArtifactKind = "stable-image-tree"
	ArtifactKindDirtyImageTree  ArtifactKind = "dirty-image-tree"
	ArtifactKindRecoveryTrace   ArtifactKind = "recovery-trace"
	ArtifactKindMetrics         ArtifactKind = "metrics"
	ArtifactKindLog             ArtifactKind = "log"
	ArtifactKindBenchmark       ArtifactKind = "benchmark-output"
)

const (
	DimensionProfile          = "profile"
	DimensionAcknowledgement  = "acknowledgement"
	DimensionResourceShape    = "resource_shape"
	DimensionStorageBoundary  = "storage_boundary"
	DimensionWritebackVariant = "writeback_variant"
	DimensionFailureClass     = "failure_class"
	DimensionCounterexample   = "counterexample"
	DimensionNegativeControl  = "negative_control"
)

var productionProfiles = map[string]bool{
	"command_wal_durable": true,
	"command_wal_relaxed": true,
	"no_wal_fast":         true,
}

var requiredDimensions = []string{
	DimensionProfile,
	DimensionAcknowledgement,
	DimensionResourceShape,
	DimensionStorageBoundary,
	DimensionWritebackVariant,
	DimensionFailureClass,
}

var requiredModeledArtifactKinds = []ArtifactKind{
	ArtifactKindOperationTrace,
	ArtifactKindStableImageTree,
	ArtifactKindDirtyImageTree,
	ArtifactKindRecoveryTrace,
	ArtifactKindMetrics,
	ArtifactKindLog,
}

type EvidenceTier string
type ArtifactKind string

type PullRequest struct {
	Number   int    `json:"number"`
	HeadSHA  string `json:"head_sha"`
	MergeSHA string `json:"merge_sha"`
}

type Environment struct {
	GoVersion       string `json:"go_version"`
	ToolVersion     string `json:"tool_version"`
	OS              string `json:"os"`
	Architecture    string `json:"architecture"`
	FilesystemModel string `json:"filesystem_model"`
}

type Artifact struct {
	Kind   ArtifactKind `json:"kind"`
	Path   string       `json:"path"`
	SHA256 string       `json:"sha256"`
}

type TestCommand struct {
	BinaryPath string            `json:"binary_path"`
	Package    string            `json:"package"`
	TestName   string            `json:"test_name"`
	Args       []string          `json:"args"`
	Env        map[string]string `json:"env,omitempty"`
}

type WitnessState struct {
	RootMetaGeneration  string `json:"root_meta_generation"`
	FreelistGeneration  string `json:"freelist_generation"`
	ExternalFrontiers   string `json:"external_resource_ids_frontiers"`
	NamespaceGeneration string `json:"namespace_generations"`
	WALLineage          string `json:"wal_lineage"`
	DurableLSN          string `json:"durable_lsn"`
	CleanupPins         string `json:"cleanup_maintenance_pins"`
}

type Witness struct {
	ID                     string       `json:"id"`
	EvidenceTier           EvidenceTier `json:"evidence_tier"`
	Profile                string       `json:"profile"`
	Acknowledgement        string       `json:"acknowledgement_shape"`
	ResourceShapes         []string     `json:"data_resource_shapes"`
	DependencyGraph        string       `json:"dependency_graph"`
	StorageBoundaries      []string     `json:"storage_boundaries"`
	WritebackVariant       string       `json:"writeback_variant"`
	FailureClasses         []string     `json:"failure_classes"`
	ExpectedDurableHorizon string       `json:"expected_durable_horizon"`
	ExpectedOutcome        string       `json:"expected_outcome"`
	ActualOutcome          string       `json:"actual_outcome"`
	TypedError             string       `json:"typed_error"`
	ExpectedRecoveryDir    string       `json:"expected_recovery_dir,omitempty"`
	State                  WitnessState `json:"state"`
	StateComparison        string       `json:"state_comparison,omitempty"`
	ReplayWindow           string       `json:"replay_window,omitempty"`
	CounterexampleID       string       `json:"counterexample_id,omitempty"`
	NegativeControlID      string       `json:"negative_control_id,omitempty"`
	Seed                   uint64       `json:"seed"`
	CutID                  string       `json:"cut_id"`
	CutPoint               string       `json:"cut_point"`
	CutOccurrence          int          `json:"cut_occurrence"`
	ObservedEventCount     int          `json:"observed_event_count"`
	Command                TestCommand  `json:"command"`
	CutExercised           bool         `json:"cut_exercised"`
	ClaimBoundary          string       `json:"claim_boundary"`
	Artifacts              []Artifact   `json:"artifacts"`
}

type ChildManifest struct {
	SchemaVersion string        `json:"schema_version"`
	ManifestID    string        `json:"manifest_id"`
	RepositorySHA string        `json:"repository_sha"`
	Issue         int           `json:"issue"`
	PullRequests  []PullRequest `json:"pull_requests"`
	Environment   Environment   `json:"environment"`
	TestBinaries  []Artifact    `json:"test_binaries"`
	ClaimBoundary string        `json:"claim_boundary"`
	Witnesses     []Witness     `json:"witnesses"`
}

type RiskInventory struct {
	SchemaVersion            string                `json:"schema_version"`
	Dimensions               map[string][]string   `json:"dimensions"`
	RetainedCounterexamples  []string              `json:"retained_counterexamples"`
	RequiredNegativeControls []string              `json:"required_negative_controls"`
	RequiredInteractions     []RequiredInteraction `json:"required_interactions"`
}

type CoverageRequirement struct {
	Dimension string `json:"dimension"`
	Value     string `json:"value"`
}

type RequiredInteraction struct {
	ID      string                `json:"id"`
	Members []CoverageRequirement `json:"members"`
}

func ParseChildManifest(data []byte) (ChildManifest, error) {
	var manifest ChildManifest
	if err := decodeStrict(data, &manifest); err != nil {
		return ChildManifest{}, fmt.Errorf("powerlosscert: decode child manifest: %w", err)
	}
	return manifest, nil
}

func ParseRiskInventory(data []byte) (RiskInventory, error) {
	var inventory RiskInventory
	if err := decodeStrict(data, &inventory); err != nil {
		return RiskInventory{}, fmt.Errorf("powerlosscert: decode risk inventory: %w", err)
	}
	return inventory, nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("contains a second JSON value")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

func ValidateBundle(expectedSHA string, inventory RiskInventory, manifests []ChildManifest) error {
	if !validHex(expectedSHA, 40) {
		return fmt.Errorf("powerlosscert: expected repository SHA %q is not a full 40-character hex SHA", expectedSHA)
	}
	if err := validateRiskInventory(inventory); err != nil {
		return err
	}
	if len(manifests) == 0 {
		return fmt.Errorf("powerlosscert: no child manifests")
	}
	manifestIDs := make(map[string]bool, len(manifests))
	witnessIDs := make(map[string]string)
	modeledEvidenceDirs := make(map[string]string)
	for i := range manifests {
		manifest := &manifests[i]
		if manifest.RepositorySHA != expectedSHA {
			return fmt.Errorf("powerlosscert: manifest %q has stale repository_sha=%s want=%s", manifest.ManifestID, manifest.RepositorySHA, expectedSHA)
		}
		if err := validateChildManifest(*manifest); err != nil {
			return err
		}
		if manifestIDs[manifest.ManifestID] {
			return fmt.Errorf("powerlosscert: duplicate manifest id %q", manifest.ManifestID)
		}
		manifestIDs[manifest.ManifestID] = true
		for _, witness := range manifest.Witnesses {
			if err := validateWitnessInventory(inventory, manifest.ManifestID, witness); err != nil {
				return err
			}
			if owner, duplicate := witnessIDs[witness.ID]; duplicate {
				return fmt.Errorf("powerlosscert: duplicate witness id %q in manifests %q and %q", witness.ID, owner, manifest.ManifestID)
			}
			witnessIDs[witness.ID] = manifest.ManifestID
			if err := registerModeledEvidenceDir(manifest.ManifestID, witness, modeledEvidenceDirs); err != nil {
				return err
			}
		}
	}
	report, err := BuildCoverageReport(inventory, manifests)
	if err != nil {
		return err
	}
	if !report.Complete {
		return fmt.Errorf("powerlosscert: incomplete frozen risk coverage: %s", strings.Join(report.Gaps, ", "))
	}
	return nil
}

func registerModeledEvidenceDir(manifestID string, witness Witness, seen map[string]string) error {
	if witness.EvidenceTier != EvidenceTierModeledCrash {
		return nil
	}
	rawDir := witness.Command.Env["TREEDB_POWERLOSS_EVIDENCE_DIR"]
	dir := normalizedArtifactPath(rawDir)
	if rawDir == "" || filepath.IsAbs(rawDir) || dir == "." || strings.HasPrefix(dir, "../") || dir != rawDir {
		return fmt.Errorf("powerlosscert: manifest %q witness %q has unsafe or non-canonical modeled evidence directory %q", manifestID, witness.ID, rawDir)
	}
	if owner, reused := seen[dir]; reused {
		return fmt.Errorf("powerlosscert: witness %q reuses modeled evidence directory %q owned by witness %q", witness.ID, dir, owner)
	}
	seen[dir] = witness.ID
	return nil
}

func validateWitnessInventory(inventory RiskInventory, manifestID string, witness Witness) error {
	prefix := fmt.Sprintf("powerlosscert: manifest %q witness %q", manifestID, witness.ID)
	valuesByDimension := map[string][]string{
		DimensionProfile:          {witness.Profile},
		DimensionAcknowledgement:  {witness.Acknowledgement},
		DimensionResourceShape:    witness.ResourceShapes,
		DimensionStorageBoundary:  witness.StorageBoundaries,
		DimensionWritebackVariant: {witness.WritebackVariant},
		DimensionFailureClass:     witness.FailureClasses,
	}
	for _, dimension := range requiredDimensions {
		allowed := inventory.Dimensions[dimension]
		for _, value := range valuesByDimension[dimension] {
			if !containsString(allowed, value) {
				return fmt.Errorf("%s has undeclared %s=%q", prefix, dimension, value)
			}
		}
	}
	if witness.CounterexampleID != "" && !containsString(inventory.RetainedCounterexamples, witness.CounterexampleID) {
		return fmt.Errorf("%s has undeclared counterexample=%q", prefix, witness.CounterexampleID)
	}
	if witness.NegativeControlID != "" && !containsString(inventory.RequiredNegativeControls, witness.NegativeControlID) {
		return fmt.Errorf("%s has undeclared negative_control=%q", prefix, witness.NegativeControlID)
	}
	return nil
}

func validateRiskInventory(inventory RiskInventory) error {
	if inventory.SchemaVersion != RiskInventorySchemaVersion {
		return fmt.Errorf("powerlosscert: risk inventory schema_version=%q want=%q", inventory.SchemaVersion, RiskInventorySchemaVersion)
	}
	if len(inventory.Dimensions) != len(requiredDimensions) {
		return fmt.Errorf("powerlosscert: risk inventory dimensions=%d want=%d", len(inventory.Dimensions), len(requiredDimensions))
	}
	for _, dimension := range requiredDimensions {
		values, ok := inventory.Dimensions[dimension]
		if !ok || len(values) == 0 {
			return fmt.Errorf("powerlosscert: risk inventory dimension %q is missing or empty", dimension)
		}
		if err := validateUniqueStrings("risk inventory "+dimension, values); err != nil {
			return err
		}
	}
	if err := validateUniqueStrings("retained counterexamples", inventory.RetainedCounterexamples); err != nil {
		return err
	}
	if len(inventory.RetainedCounterexamples) == 0 {
		return fmt.Errorf("powerlosscert: retained counterexample inventory is empty")
	}
	if err := validateUniqueStrings("required negative controls", inventory.RequiredNegativeControls); err != nil {
		return err
	}
	if len(inventory.RequiredNegativeControls) == 0 {
		return fmt.Errorf("powerlosscert: required negative-control inventory is empty")
	}
	if len(inventory.RequiredInteractions) == 0 {
		return fmt.Errorf("powerlosscert: required interaction inventory is empty")
	}
	interactionIDs := make(map[string]bool, len(inventory.RequiredInteractions))
	for _, interaction := range inventory.RequiredInteractions {
		if interaction.ID == "" {
			return fmt.Errorf("powerlosscert: required interaction has an empty id")
		}
		if interactionIDs[interaction.ID] {
			return fmt.Errorf("powerlosscert: duplicate required interaction id %q", interaction.ID)
		}
		interactionIDs[interaction.ID] = true
		if len(interaction.Members) < 2 {
			return fmt.Errorf("powerlosscert: required interaction %q has %d members want at least 2", interaction.ID, len(interaction.Members))
		}
		members := make(map[string]bool, len(interaction.Members))
		for _, member := range interaction.Members {
			values, ok := inventory.Dimensions[member.Dimension]
			if !ok {
				return fmt.Errorf("powerlosscert: required interaction %q references unknown dimension %q", interaction.ID, member.Dimension)
			}
			if !containsString(values, member.Value) {
				return fmt.Errorf("powerlosscert: required interaction %q references undeclared %s=%q", interaction.ID, member.Dimension, member.Value)
			}
			key := member.Dimension + ":" + member.Value
			if members[key] {
				return fmt.Errorf("powerlosscert: required interaction %q duplicates member %q", interaction.ID, key)
			}
			members[key] = true
		}
	}
	return nil
}

func validateChildManifest(manifest ChildManifest) error {
	prefix := fmt.Sprintf("powerlosscert: manifest %q", manifest.ManifestID)
	if manifest.SchemaVersion != ChildManifestSchemaVersion {
		return fmt.Errorf("%s schema_version=%q want=%q", prefix, manifest.SchemaVersion, ChildManifestSchemaVersion)
	}
	if manifest.ManifestID == "" || manifest.Issue <= 0 || manifest.ClaimBoundary == "" {
		return fmt.Errorf("%s has an empty required manifest field", prefix)
	}
	if !validHex(manifest.RepositorySHA, 40) {
		return fmt.Errorf("%s repository_sha is not a full SHA", prefix)
	}
	if len(manifest.PullRequests) == 0 {
		return fmt.Errorf("%s has no pull-request provenance", prefix)
	}
	for _, pr := range manifest.PullRequests {
		if pr.Number <= 0 || !validHex(pr.HeadSHA, 40) || !validHex(pr.MergeSHA, 40) {
			return fmt.Errorf("%s has invalid pull-request provenance: %+v", prefix, pr)
		}
	}
	if manifest.Environment.GoVersion == "" || manifest.Environment.ToolVersion == "" || manifest.Environment.OS == "" || manifest.Environment.Architecture == "" || manifest.Environment.FilesystemModel == "" {
		return fmt.Errorf("%s has incomplete environment metadata", prefix)
	}
	if len(manifest.TestBinaries) == 0 {
		return fmt.Errorf("%s has no hashed test binary", prefix)
	}
	binaries := make(map[string]bool, len(manifest.TestBinaries))
	for _, artifact := range manifest.TestBinaries {
		if artifact.Kind != ArtifactKindTestBinary {
			return fmt.Errorf("%s test binary %q has kind %q want %q", prefix, artifact.Path, artifact.Kind, ArtifactKindTestBinary)
		}
		if err := validateArtifact(prefix+" test binary", artifact); err != nil {
			return err
		}
		pathKey := normalizedArtifactPath(artifact.Path)
		if binaries[pathKey] {
			return fmt.Errorf("%s duplicates test binary path %q", prefix, artifact.Path)
		}
		binaries[pathKey] = true
	}
	if len(manifest.Witnesses) == 0 {
		return fmt.Errorf("%s has no witnesses", prefix)
	}
	for _, witness := range manifest.Witnesses {
		if err := validateWitness(prefix, witness, binaries); err != nil {
			return err
		}
	}
	return nil
}

func validateWitness(prefix string, witness Witness, binaries map[string]bool) error {
	prefix += fmt.Sprintf(" witness %q", witness.ID)
	if witness.ID == "" || witness.Acknowledgement == "" || witness.DependencyGraph == "" || witness.WritebackVariant == "" || witness.ExpectedDurableHorizon == "" || witness.ExpectedOutcome == "" || witness.ActualOutcome == "" || witness.TypedError == "" || witness.ClaimBoundary == "" {
		return fmt.Errorf("%s has an empty required field", prefix)
	}
	switch witness.EvidenceTier {
	case EvidenceTierCleanProcess, EvidenceTierModeledCrash, EvidenceTierBlockDevice:
	default:
		return fmt.Errorf("%s has unknown evidence tier %q", prefix, witness.EvidenceTier)
	}
	if !productionProfiles[witness.Profile] {
		return fmt.Errorf("%s selects non-production or unknown profile %q", prefix, witness.Profile)
	}
	if len(witness.ResourceShapes) == 0 || len(witness.StorageBoundaries) == 0 || len(witness.FailureClasses) == 0 {
		return fmt.Errorf("%s has an empty risk dimension", prefix)
	}
	if err := validateUniqueStrings(prefix+" resource shapes", witness.ResourceShapes); err != nil {
		return err
	}
	if err := validateUniqueStrings(prefix+" storage boundaries", witness.StorageBoundaries); err != nil {
		return err
	}
	if err := validateUniqueStrings(prefix+" failure classes", witness.FailureClasses); err != nil {
		return err
	}
	if witness.ExpectedOutcome != witness.ActualOutcome {
		return fmt.Errorf("%s actual_outcome=%q want expected_outcome=%q", prefix, witness.ActualOutcome, witness.ExpectedOutcome)
	}
	if witness.EvidenceTier == EvidenceTierModeledCrash && modeledOutcomeClass(witness.ActualOutcome) == "" {
		return fmt.Errorf("%s actual_outcome=%q must identify an accepted or rejected public-open outcome", prefix, witness.ActualOutcome)
	}
	state := witness.State
	if state.RootMetaGeneration == "" || state.FreelistGeneration == "" || state.ExternalFrontiers == "" || state.NamespaceGeneration == "" || state.WALLineage == "" || state.DurableLSN == "" || state.CleanupPins == "" {
		return fmt.Errorf("%s has incomplete recovery-state metadata", prefix)
	}
	if witness.StateComparison != stateComparisonExact && witness.StateComparison != stateComparisonLogicalHorizon {
		return fmt.Errorf("%s has invalid state comparison %q", prefix, witness.StateComparison)
	}
	if got := witness.Command.Env[powerLossReplayWindowEnv]; got != witness.ReplayWindow {
		return fmt.Errorf("%s command env %s=%q does not match replay window %q", prefix, powerLossReplayWindowEnv, got, witness.ReplayWindow)
	}
	if witness.ReplayWindow != "" && witness.Command.Env["TREEDB_POWERLOSS_VARIANT_ID"] != witness.ReplayWindow {
		return fmt.Errorf("%s replay window %q does not match command variant %q", prefix, witness.ReplayWindow, witness.Command.Env["TREEDB_POWERLOSS_VARIANT_ID"])
	}
	if witness.EvidenceTier == EvidenceTierModeledCrash {
		if _, err := normalizeRecoveryDir(witness.ExpectedRecoveryDir); err != nil {
			return fmt.Errorf("%s: %w", prefix, err)
		}
		if witness.Seed == 0 {
			return fmt.Errorf("%s has zero seed", prefix)
		}
		if witness.CutID == "" || witness.CutPoint == "" || witness.CutOccurrence < 0 {
			return fmt.Errorf("%s has incomplete declared cut metadata", prefix)
		}
		if witness.ObservedEventCount <= witness.CutOccurrence {
			return fmt.Errorf("%s observed event count=%d does not reach declared occurrence=%d", prefix, witness.ObservedEventCount, witness.CutOccurrence)
		}
		if !witness.CutExercised {
			return fmt.Errorf("%s passed but did not exercise its declared cut", prefix)
		}
		switch witness.Command.Env[powerLossReopenModeEnv] {
		case powerLossReopenModeReadWrite, powerLossReopenModeReadOnly:
		default:
			return fmt.Errorf("%s command env %s=%q is invalid", prefix, powerLossReopenModeEnv, witness.Command.Env[powerLossReopenModeEnv])
		}
		if profile := witness.Command.Env[powerLossProfileEnv]; profile != witness.Profile {
			return fmt.Errorf("%s command env %s=%q does not match witness profile %q", prefix, powerLossProfileEnv, profile, witness.Profile)
		}
	}
	if witness.Command.BinaryPath == "" || witness.Command.Package == "" || witness.Command.TestName == "" || len(witness.Command.Args) == 0 {
		return fmt.Errorf("%s has incomplete structured command", prefix)
	}
	if !binaries[normalizedArtifactPath(witness.Command.BinaryPath)] {
		return fmt.Errorf("%s references unregistered test binary %q", prefix, witness.Command.BinaryPath)
	}
	if len(witness.Artifacts) == 0 {
		return fmt.Errorf("%s has no hashed artifacts", prefix)
	}
	artifactKinds := make(map[ArtifactKind]bool, len(witness.Artifacts))
	artifactPaths := make(map[string]ArtifactKind, len(witness.Artifacts))
	for _, artifact := range witness.Artifacts {
		if err := validateArtifact(prefix+" artifact", artifact); err != nil {
			return err
		}
		pathKey := normalizedArtifactPath(artifact.Path)
		if binaries[pathKey] {
			return fmt.Errorf("%s reuses test-binary path %q as artifact kind %q", prefix, artifact.Path, artifact.Kind)
		}
		if priorKind, duplicate := artifactPaths[pathKey]; duplicate {
			return fmt.Errorf("%s reuses artifact path %q for kinds %q and %q", prefix, artifact.Path, priorKind, artifact.Kind)
		}
		artifactPaths[pathKey] = artifact.Kind
		if artifactKinds[artifact.Kind] {
			return fmt.Errorf("%s duplicates artifact kind %q", prefix, artifact.Kind)
		}
		artifactKinds[artifact.Kind] = true
	}
	if witness.EvidenceTier == EvidenceTierModeledCrash {
		for _, kind := range requiredModeledArtifactKinds {
			if !artifactKinds[kind] {
				return fmt.Errorf("%s missing required artifact kind %q", prefix, kind)
			}
		}
	}
	return nil
}

func modeledOutcomeClass(outcome string) string {
	for _, class := range []string{"accepted", "rejected"} {
		prefix := class + ":"
		if strings.HasPrefix(outcome, prefix) && len(outcome) > len(prefix) {
			return class
		}
	}
	return ""
}

func validateArtifact(prefix string, artifact Artifact) error {
	if !validArtifactKind(artifact.Kind) {
		return fmt.Errorf("%s has unknown kind %q", prefix, artifact.Kind)
	}
	cleanPath := normalizedArtifactPath(artifact.Path)
	if artifact.Path == "" || filepath.IsAbs(artifact.Path) || cleanPath == "." || strings.HasPrefix(cleanPath, "../") || cleanPath != artifact.Path {
		return fmt.Errorf("%s has unsafe path or non-canonical path %q", prefix, artifact.Path)
	}
	if !validHex(artifact.SHA256, 64) {
		return fmt.Errorf("%s %q has invalid sha256", prefix, artifact.Path)
	}
	return nil
}

func normalizedArtifactPath(path string) string {
	return filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
}

func validArtifactKind(kind ArtifactKind) bool {
	switch kind {
	case ArtifactKindTestBinary, ArtifactKindOperationTrace, ArtifactKindStableImageTree,
		ArtifactKindDirtyImageTree, ArtifactKindRecoveryTrace, ArtifactKindMetrics,
		ArtifactKindLog, ArtifactKindBenchmark:
		return true
	default:
		return false
	}
}

func validateUniqueStrings(label string, values []string) error {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if value == "" {
			return fmt.Errorf("powerlosscert: %s contains an empty value", label)
		}
		if seen[value] {
			return fmt.Errorf("powerlosscert: %s duplicates %q", label, value)
		}
		seen[value] = true
	}
	return nil
}

func validHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, r := range value {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func sortedWitnesses(manifests []ChildManifest) []Witness {
	var witnesses []Witness
	for _, manifest := range manifests {
		witnesses = append(witnesses, manifest.Witnesses...)
	}
	sort.Slice(witnesses, func(i, j int) bool { return witnesses[i].ID < witnesses[j].ID })
	return witnesses
}
