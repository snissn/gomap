package powerlosscert

import (
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

const (
	operationTraceSchemaVersion = "treedb-power-loss-operation-trace/v2"
	imageTreeSchemaVersion      = "treedb-power-loss-image-tree/v1"
	recoveryTraceSchemaVersion  = "treedb-power-loss-recovery-trace/v2"
	metricsSchemaVersion        = "treedb-power-loss-metrics/v1"
	commandLogSchemaVersion     = "treedb-power-loss-command-log/v1"
)

type operationTraceArtifact struct {
	SchemaVersion      string   `json:"schema_version"`
	CutID              string   `json:"cut_id"`
	VariantID          string   `json:"variant_id"`
	Seed               string   `json:"seed"`
	DeclaredCutPoint   string   `json:"declared_cut_point"`
	ReplayWindow       string   `json:"replay_window,omitempty"`
	ObservedEventCount int      `json:"observed_event_count"`
	Events             []string `json:"events"`
}

type imageTreeArtifact struct {
	SchemaVersion string                  `json:"schema_version"`
	Kind          string                  `json:"kind"`
	Directories   []string                `json:"directories"`
	Files         []imageTreeFileArtifact `json:"files"`
	TotalBytes    int64                   `json:"total_bytes"`
}

type imageTreeFileArtifact struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type recoveryTraceArtifact struct {
	SchemaVersion      string            `json:"schema_version"`
	PublicAPI          string            `json:"public_api"`
	Dir                string            `json:"dir"`
	PreOpenSnapshotDir string            `json:"pre_open_snapshot_dir"`
	InputTreeSHA256    string            `json:"input_image_tree_sha256"`
	StableFingerprint  string            `json:"stable_fingerprint"`
	ReadOnly           bool              `json:"read_only"`
	Rejected           bool              `json:"rejected"`
	ErrorType          string            `json:"error_type"`
	Error              string            `json:"error"`
	CommitSeq          uint64            `json:"commit_seq"`
	AppliedLSN         uint64            `json:"applied_lsn"`
	Stats              map[string]string `json:"stats"`
}

var requiredRecoveryStats = []string{
	"treedb.profile.resolved",
	"treedb.commit_seq",
	"treedb.applied_command_lsn",
	"treedb.durable_root.selected_slot",
	"treedb.durable_root.commit_seq",
	"treedb.durable_root.durable_seq",
	"treedb.durable_root.freelist.generation",
	"treedb.durable_root.manifest.entries",
	"treedb.durable_root.slot0.commit_seq",
	"treedb.durable_root.slot1.commit_seq",
	"treedb.command_wal.durable_wal_lsn",
}

type metricsArtifact struct {
	SchemaVersion     string `json:"schema_version"`
	StableImageBytes  int64  `json:"stable_image_bytes"`
	DirtyImageBytes   int64  `json:"dirty_image_bytes"`
	StableFiles       int    `json:"stable_files"`
	DirtyFiles        int    `json:"dirty_files"`
	TraceEvents       int    `json:"trace_events"`
	StableFingerprint string `json:"stable_fingerprint"`
}

type commandLogArtifact struct {
	SchemaVersion string            `json:"schema_version"`
	RepositorySHA string            `json:"repository_sha"`
	BinaryPath    string            `json:"binary_path"`
	BinarySHA256  string            `json:"binary_sha256"`
	Package       string            `json:"package"`
	TestName      string            `json:"test_name"`
	Args          []string          `json:"args"`
	Env           map[string]string `json:"env"`
	Outcome       string            `json:"outcome"`
	Completed     bool              `json:"completed"`
	ExitCode      int               `json:"exit_code"`
	Stdout        string            `json:"stdout"`
	Stderr        string            `json:"stderr"`
}

var modeledArtifactNames = map[ArtifactKind]string{
	ArtifactKindOperationTrace:  "operation_trace.json",
	ArtifactKindStableImageTree: "stable_image_tree.json",
	ArtifactKindDirtyImageTree:  "dirty_image_tree.json",
	ArtifactKindRecoveryTrace:   "recovery_trace.json",
	ArtifactKindMetrics:         "metrics.json",
	ArtifactKindLog:             "command_log.json",
}

type Bundle struct {
	Root      string
	Inventory RiskInventory
	Manifests []ChildManifest
}

func LoadBundle(root string) (Bundle, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return Bundle{}, fmt.Errorf("powerlosscert: resolve bundle root: %w", err)
	}
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return Bundle{}, fmt.Errorf("powerlosscert: resolve bundle root symlinks: %w", err)
	}
	inventoryData, err := readBundleMetadata(root, realRoot, filepath.Join(root, "risk_inventory.json"), "risk inventory")
	if err != nil {
		return Bundle{}, err
	}
	inventory, err := ParseRiskInventory(inventoryData)
	if err != nil {
		return Bundle{}, err
	}
	paths, err := filepath.Glob(filepath.Join(root, "manifests", "*.json"))
	if err != nil {
		return Bundle{}, fmt.Errorf("powerlosscert: enumerate child manifests: %w", err)
	}
	if len(paths) == 0 {
		return Bundle{}, fmt.Errorf("powerlosscert: bundle %q contains no child manifests", root)
	}
	manifests := make([]ChildManifest, 0, len(paths))
	for _, path := range paths {
		data, err := readBundleMetadata(root, realRoot, path, fmt.Sprintf("child manifest %q", path))
		if err != nil {
			return Bundle{}, err
		}
		manifest, err := ParseChildManifest(data)
		if err != nil {
			return Bundle{}, fmt.Errorf("powerlosscert: child manifest %q: %w", path, err)
		}
		manifests = append(manifests, manifest)
	}
	sort.Slice(manifests, func(i, j int) bool {
		if manifests[i].ManifestID == manifests[j].ManifestID {
			return manifests[i].Issue < manifests[j].Issue
		}
		return manifests[i].ManifestID < manifests[j].ManifestID
	})
	return Bundle{Root: root, Inventory: inventory, Manifests: manifests}, nil
}

func readBundleMetadata(root, realRoot, path, label string) ([]byte, error) {
	if !pathWithin(root, path) {
		return nil, fmt.Errorf("powerlosscert: %s has unsafe path %q", label, path)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("powerlosscert: read %s: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("powerlosscert: %s %q is a symlink", label, path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("powerlosscert: %s %q is not a regular file", label, path)
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("powerlosscert: resolve %s: %w", label, err)
	}
	if !pathWithin(realRoot, resolvedPath) {
		return nil, fmt.Errorf("powerlosscert: %s %q resolves through a symlink outside the bundle", label, path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("powerlosscert: read %s: %w", label, err)
	}
	return data, nil
}

func VerifyArtifacts(root string, manifests []ChildManifest) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("powerlosscert: resolve artifact root: %w", err)
	}
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("powerlosscert: resolve artifact root symlinks: %w", err)
	}
	seen := make(map[string]string)
	modeledEvidenceDirs := make(map[string]string)
	resolvedEvidenceDirs := make(map[string]string)
	for _, manifest := range manifests {
		for _, witness := range manifest.Witnesses {
			if err := registerModeledEvidenceDir(manifest.ManifestID, witness, modeledEvidenceDirs); err != nil {
				return err
			}
		}
		artifacts := append([]Artifact(nil), manifest.TestBinaries...)
		for _, witness := range manifest.Witnesses {
			artifacts = append(artifacts, witness.Artifacts...)
		}
		for _, artifact := range artifacts {
			prefix := fmt.Sprintf("powerlosscert: manifest %q artifact", manifest.ManifestID)
			if err := validateArtifact(prefix, artifact); err != nil {
				return err
			}
			pathKey := normalizedArtifactPath(artifact.Path)
			if prior, ok := seen[pathKey]; ok {
				if prior != artifact.SHA256 {
					return fmt.Errorf("%s %q has conflicting sha256 values %s and %s", prefix, artifact.Path, prior, artifact.SHA256)
				}
				continue
			}
			seen[pathKey] = artifact.SHA256
			fullPath := filepath.Join(root, filepath.FromSlash(pathKey))
			if !pathWithin(root, fullPath) {
				return fmt.Errorf("%s has unsafe path %q", prefix, artifact.Path)
			}
			info, err := os.Lstat(fullPath)
			if err != nil {
				return fmt.Errorf("%s %q: %w", prefix, artifact.Path, err)
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("%s %q is a symlink", prefix, artifact.Path)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("%s %q is not a regular file", prefix, artifact.Path)
			}
			resolvedPath, err := filepath.EvalSymlinks(fullPath)
			if err != nil {
				return fmt.Errorf("%s resolve %q: %w", prefix, artifact.Path, err)
			}
			if !pathWithin(realRoot, resolvedPath) {
				return fmt.Errorf("%s %q resolves through a symlink outside the bundle", prefix, artifact.Path)
			}
			digest, err := fileSHA256(fullPath)
			if err != nil {
				return fmt.Errorf("%s hash %q: %w", prefix, artifact.Path, err)
			}
			if digest != artifact.SHA256 {
				return fmt.Errorf("%s %q sha256=%s want=%s", prefix, artifact.Path, digest, artifact.SHA256)
			}
		}
		for _, witness := range manifest.Witnesses {
			if witness.EvidenceTier != EvidenceTierModeledCrash {
				continue
			}
			if err := verifyModeledEvidenceDir(root, realRoot, manifest.ManifestID, witness, resolvedEvidenceDirs); err != nil {
				return err
			}
			if err := verifyModeledEvidence(root, manifest, witness); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyModeledEvidenceDir(root, realRoot, manifestID string, witness Witness, seen map[string]string) error {
	if witness.EvidenceTier != EvidenceTierModeledCrash {
		return nil
	}
	dir := normalizedArtifactPath(witness.Command.Env["TREEDB_POWERLOSS_EVIDENCE_DIR"])
	fullPath := root
	for _, component := range strings.Split(filepath.FromSlash(dir), string(filepath.Separator)) {
		fullPath = filepath.Join(fullPath, component)
		info, err := os.Lstat(fullPath)
		if err != nil {
			return fmt.Errorf("powerlosscert: manifest %q witness %q inspect evidence directory %q: %w", manifestID, witness.ID, dir, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("powerlosscert: manifest %q witness %q has symlinked evidence directory component %q", manifestID, witness.ID, fullPath)
		}
		if !info.IsDir() {
			return fmt.Errorf("powerlosscert: manifest %q witness %q evidence directory component %q is not a directory", manifestID, witness.ID, fullPath)
		}
	}
	resolved, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		return fmt.Errorf("powerlosscert: manifest %q witness %q resolve evidence directory %q: %w", manifestID, witness.ID, dir, err)
	}
	if !pathWithin(realRoot, resolved) {
		return fmt.Errorf("powerlosscert: manifest %q witness %q evidence directory %q resolves outside the bundle", manifestID, witness.ID, dir)
	}
	key := filepath.Clean(resolved)
	if owner, reused := seen[key]; reused {
		return fmt.Errorf("powerlosscert: witness %q reuses resolved modeled evidence directory %q owned by witness %q", witness.ID, dir, owner)
	}
	seen[key] = witness.ID
	return nil
}

func verifyModeledEvidence(root string, manifest ChildManifest, witness Witness) error {
	prefix := fmt.Sprintf("powerlosscert: manifest %q witness %q", manifest.ManifestID, witness.ID)
	evidenceDir := normalizedArtifactPath(witness.Command.Env["TREEDB_POWERLOSS_EVIDENCE_DIR"])
	if evidenceDir == "." || strings.HasPrefix(evidenceDir, "../") {
		return fmt.Errorf("%s has unsafe evidence directory %q", prefix, witness.Command.Env["TREEDB_POWERLOSS_EVIDENCE_DIR"])
	}
	artifacts := make(map[ArtifactKind]Artifact, len(witness.Artifacts))
	for _, artifact := range witness.Artifacts {
		artifacts[artifact.Kind] = artifact
		name, modeled := modeledArtifactNames[artifact.Kind]
		if modeled {
			wantPath := normalizedArtifactPath(filepath.ToSlash(filepath.Join(evidenceDir, name)))
			if normalizedArtifactPath(artifact.Path) != wantPath {
				return fmt.Errorf("%s artifact kind %q path=%q want=%q", prefix, artifact.Kind, artifact.Path, wantPath)
			}
		}
	}
	trace, err := verifyOperationTrace(root, manifest.ManifestID, witness, artifacts[ArtifactKindOperationTrace])
	if err != nil {
		return err
	}
	stableTree, err := verifyImageTree(root, evidenceDir, manifest.ManifestID, witness.ID, artifacts[ArtifactKindStableImageTree], "stable-image")
	if err != nil {
		return err
	}
	dirtyTree, err := verifyImageTree(root, evidenceDir, manifest.ManifestID, witness.ID, artifacts[ArtifactKindDirtyImageTree], "dirty-image")
	if err != nil {
		return err
	}
	metrics, err := verifyMetrics(root, manifest.ManifestID, witness, artifacts[ArtifactKindMetrics], trace, stableTree, dirtyTree)
	if err != nil {
		return err
	}
	recovery, err := verifyRecoveryTrace(root, evidenceDir, manifest.ManifestID, witness, artifacts[ArtifactKindRecoveryTrace], artifacts[ArtifactKindStableImageTree], metrics)
	if err != nil {
		return err
	}
	wantReadOnly := false
	switch mode := witness.Command.Env[powerLossReopenModeEnv]; mode {
	case powerLossReopenModeReadOnly:
		wantReadOnly = true
	case powerLossReopenModeReadWrite:
	default:
		return fmt.Errorf("powerlosscert: manifest %q witness %q command env %s=%q is invalid", manifest.ManifestID, witness.ID, powerLossReopenModeEnv, mode)
	}
	if recovery.ReadOnly != wantReadOnly {
		return fmt.Errorf("powerlosscert: manifest %q witness %q recovery read_only=%t does not match command reopen mode %q", manifest.ManifestID, witness.ID, recovery.ReadOnly, witness.Command.Env[powerLossReopenModeEnv])
	}
	if _, err := verifyImageDirectory(root, evidenceDir, manifest.ManifestID, witness.ID, recovery.PreOpenSnapshotDir, stableTree); err != nil {
		return err
	}
	if recovery.ReadOnly {
		if _, err := verifyImageDirectory(root, evidenceDir, manifest.ManifestID, witness.ID, "recovery-input", stableTree); err != nil {
			return err
		}
	}
	return verifyCommandLog(root, manifest, witness, artifacts[ArtifactKindLog])
}

func verifyOperationTrace(root, manifestID string, witness Witness, traceArtifact Artifact) (operationTraceArtifact, error) {
	prefix := fmt.Sprintf("powerlosscert: manifest %q witness %q operation trace", manifestID, witness.ID)
	if traceArtifact.Kind != ArtifactKindOperationTrace {
		return operationTraceArtifact{}, fmt.Errorf("%s is missing", prefix)
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(normalizedArtifactPath(traceArtifact.Path))))
	if err != nil {
		return operationTraceArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	var trace operationTraceArtifact
	if err := decodeStrict(data, &trace); err != nil {
		return operationTraceArtifact{}, fmt.Errorf("%s decode: %w", prefix, err)
	}
	if trace.SchemaVersion != operationTraceSchemaVersion {
		return operationTraceArtifact{}, fmt.Errorf("%s schema_version=%q want=%q", prefix, trace.SchemaVersion, operationTraceSchemaVersion)
	}
	if trace.CutID != witness.CutID {
		return operationTraceArtifact{}, fmt.Errorf("%s cut_id=%q want=%q", prefix, trace.CutID, witness.CutID)
	}
	if trace.DeclaredCutPoint != witness.CutPoint {
		return operationTraceArtifact{}, fmt.Errorf("%s declared_cut_point=%q want=%q", prefix, trace.DeclaredCutPoint, witness.CutPoint)
	}
	cutPoint, occurrence, err := parseCutAddress(trace.CutID)
	if err != nil {
		return operationTraceArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	if cutPoint != witness.CutPoint || occurrence != witness.CutOccurrence {
		return operationTraceArtifact{}, fmt.Errorf("%s cut address=(%s,%d) want=(%s,%d)", prefix, cutPoint, occurrence, witness.CutPoint, witness.CutOccurrence)
	}
	wantSeed := strconv.FormatUint(witness.Seed, 10)
	if trace.Seed != wantSeed {
		return operationTraceArtifact{}, fmt.Errorf("%s seed=%q want=%q", prefix, trace.Seed, wantSeed)
	}
	if trace.VariantID == "" {
		return operationTraceArtifact{}, fmt.Errorf("%s has empty variant_id", prefix)
	}
	if trace.ReplayWindow != witness.ReplayWindow {
		return operationTraceArtifact{}, fmt.Errorf("%s replay_window=%q want=%q", prefix, trace.ReplayWindow, witness.ReplayWindow)
	}
	if trace.ReplayWindow != "" && trace.ReplayWindow != trace.VariantID {
		return operationTraceArtifact{}, fmt.Errorf("%s replay_window=%q does not match variant_id=%q", prefix, trace.ReplayWindow, trace.VariantID)
	}
	matchingEvents, err := replayWindowCutCount(trace.Events, witness.CutPoint, trace.ReplayWindow)
	if err != nil {
		return operationTraceArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	if trace.ObservedEventCount != matchingEvents || trace.ObservedEventCount != witness.ObservedEventCount {
		return operationTraceArtifact{}, fmt.Errorf("%s observed_event_count=%d matching_events=%d witness=%d", prefix, trace.ObservedEventCount, matchingEvents, witness.ObservedEventCount)
	}
	if matchingEvents != witness.CutOccurrence+1 {
		return operationTraceArtifact{}, fmt.Errorf("%s matching events=%d does not end at declared occurrence=%d", prefix, matchingEvents, witness.CutOccurrence)
	}
	wantEnv := map[string]string{
		"TREEDB_POWERLOSS_CUT_ID":           witness.CutID,
		"TREEDB_POWERLOSS_VARIANT_ID":       trace.VariantID,
		"TREEDB_POWERLOSS_SEED":             wantSeed,
		"TREEDB_POWERLOSS_EXPECT_CUT_POINT": witness.CutPoint,
	}
	if witness.ReplayWindow != "" {
		wantEnv[powerLossReplayWindowEnv] = witness.ReplayWindow
	} else if got := witness.Command.Env[powerLossReplayWindowEnv]; got != "" {
		return operationTraceArtifact{}, fmt.Errorf("%s command env %s=%q want empty", prefix, powerLossReplayWindowEnv, got)
	}
	for name, want := range wantEnv {
		if got := witness.Command.Env[name]; got != want {
			return operationTraceArtifact{}, fmt.Errorf("%s command env %s=%q want=%q", prefix, name, got, want)
		}
	}
	if witness.Command.Env["TREEDB_POWERLOSS_EVIDENCE_DIR"] == "" {
		return operationTraceArtifact{}, fmt.Errorf("%s command env TREEDB_POWERLOSS_EVIDENCE_DIR is empty", prefix)
	}
	return trace, nil
}

func replayWindowCutCount(events []string, cutPoint, replayWindow string) (int, error) {
	cutPrefix := "cut:" + cutPoint + ":"
	windowMarker := "replay-window:" + replayWindow
	matching := 0
	markerCount := 0
	inWindow := replayWindow == ""
	for _, event := range events {
		if strings.HasPrefix(event, "replay-window:") {
			if replayWindow == "" {
				return 0, fmt.Errorf("operation trace contains replay-window marker without a declared replay window")
			}
			if event != windowMarker {
				return 0, fmt.Errorf("operation trace replay-window marker %q does not match declared replay window %q", event, replayWindow)
			}
			markerCount++
			if markerCount > 1 {
				return 0, fmt.Errorf("operation trace requires exactly one replay-window marker %q; found %d", windowMarker, markerCount)
			}
			inWindow = true
			continue
		}
		if inWindow && strings.HasPrefix(event, cutPrefix) {
			matching++
		}
	}
	if replayWindow != "" && markerCount != 1 {
		return 0, fmt.Errorf("operation trace requires exactly one replay-window marker %q; found %d", windowMarker, markerCount)
	}
	if replayWindow != "" && matching == 0 {
		return 0, fmt.Errorf("operation trace replay-window marker %q does not precede a matching cut %q", windowMarker, cutPoint)
	}
	return matching, nil
}

func verifyImageTree(root, evidenceDir, manifestID, witnessID string, artifact Artifact, kind string) (imageTreeArtifact, error) {
	prefix := fmt.Sprintf("powerlosscert: manifest %q witness %q %s image tree", manifestID, witnessID, strings.TrimSuffix(kind, "-image"))
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(normalizedArtifactPath(artifact.Path))))
	if err != nil {
		return imageTreeArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	var tree imageTreeArtifact
	if err := decodeStrict(data, &tree); err != nil {
		return imageTreeArtifact{}, fmt.Errorf("%s decode: %w", prefix, err)
	}
	if tree.SchemaVersion != imageTreeSchemaVersion {
		return imageTreeArtifact{}, fmt.Errorf("%s schema_version=%q want=%q", prefix, tree.SchemaVersion, imageTreeSchemaVersion)
	}
	if tree.Kind != kind {
		return imageTreeArtifact{}, fmt.Errorf("%s kind=%q want=%q", prefix, tree.Kind, kind)
	}
	if err := validateDeclaredImageTree(prefix, tree); err != nil {
		return imageTreeArtifact{}, err
	}
	return verifyImageDirectory(root, evidenceDir, manifestID, witnessID, kind, tree)
}

func validateDeclaredImageTree(prefix string, tree imageTreeArtifact) error {
	priorDirectory := ""
	for index, directory := range tree.Directories {
		pathKey := normalizedArtifactPath(directory)
		if directory == "" || filepath.IsAbs(directory) || pathKey == "." || strings.HasPrefix(pathKey, "../") || pathKey != directory {
			return fmt.Errorf("%s directory %d has unsafe or non-canonical path %q", prefix, index, directory)
		}
		if priorDirectory != "" && directory <= priorDirectory {
			return fmt.Errorf("%s directories are not strictly sorted at %q", prefix, directory)
		}
		priorDirectory = directory
	}
	var total int64
	prior := ""
	for index, file := range tree.Files {
		pathKey := normalizedArtifactPath(file.Path)
		if file.Path == "" || filepath.IsAbs(file.Path) || pathKey == "." || strings.HasPrefix(pathKey, "../") || pathKey != file.Path {
			return fmt.Errorf("%s file %d has unsafe or non-canonical path %q", prefix, index, file.Path)
		}
		if prior != "" && file.Path <= prior {
			return fmt.Errorf("%s files are not strictly sorted at %q", prefix, file.Path)
		}
		if file.Bytes < 0 || !validHex(file.SHA256, 64) {
			return fmt.Errorf("%s file %q has invalid bytes or sha256", prefix, file.Path)
		}
		if file.Bytes > int64(^uint64(0)>>1)-total {
			return fmt.Errorf("%s byte total overflows", prefix)
		}
		total += file.Bytes
		prior = file.Path
	}
	if tree.TotalBytes != total {
		return fmt.Errorf("%s total_bytes=%d want=%d", prefix, tree.TotalBytes, total)
	}
	return nil
}

func verifyImageDirectory(root, evidenceDir, manifestID, witnessID, kind string, declared imageTreeArtifact) (imageTreeArtifact, error) {
	prefix := fmt.Sprintf("powerlosscert: manifest %q witness %q %s image", manifestID, witnessID, strings.TrimSuffix(kind, "-image"))
	dir := filepath.Join(root, filepath.FromSlash(evidenceDir), kind)
	if !pathWithin(root, dir) {
		return imageTreeArtifact{}, fmt.Errorf("%s has unsafe directory", prefix)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return imageTreeArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return imageTreeArtifact{}, fmt.Errorf("%s directory is not a real directory", prefix)
	}
	actual := imageTreeArtifact{SchemaVersion: imageTreeSchemaVersion, Kind: declared.Kind}
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != dir {
				rel, err := filepath.Rel(dir, path)
				if err != nil {
					return err
				}
				actual.Directories = append(actual.Directories, filepath.ToSlash(rel))
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("%s contains non-regular path %q", prefix, path)
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		entryInfo, err := entry.Info()
		if err != nil {
			return err
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return err
		}
		actual.Files = append(actual.Files, imageTreeFileArtifact{Path: filepath.ToSlash(rel), Bytes: entryInfo.Size(), SHA256: digest})
		actual.TotalBytes += entryInfo.Size()
		return nil
	})
	if err != nil {
		return imageTreeArtifact{}, err
	}
	sort.Strings(actual.Directories)
	sort.Slice(actual.Files, func(i, j int) bool { return actual.Files[i].Path < actual.Files[j].Path })
	if !reflect.DeepEqual(actual.Directories, declared.Directories) || !reflect.DeepEqual(actual.Files, declared.Files) || actual.TotalBytes != declared.TotalBytes {
		return imageTreeArtifact{}, fmt.Errorf("%s contents do not match hashed tree manifest", prefix)
	}
	return actual, nil
}

func verifyMetrics(root, manifestID string, witness Witness, artifact Artifact, trace operationTraceArtifact, stable, dirty imageTreeArtifact) (metricsArtifact, error) {
	prefix := fmt.Sprintf("powerlosscert: manifest %q witness %q metrics", manifestID, witness.ID)
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(normalizedArtifactPath(artifact.Path))))
	if err != nil {
		return metricsArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	var metrics metricsArtifact
	if err := decodeStrict(data, &metrics); err != nil {
		return metricsArtifact{}, fmt.Errorf("%s decode: %w", prefix, err)
	}
	if metrics.SchemaVersion != metricsSchemaVersion {
		return metricsArtifact{}, fmt.Errorf("%s schema_version=%q want=%q", prefix, metrics.SchemaVersion, metricsSchemaVersion)
	}
	if metrics.StableImageBytes != stable.TotalBytes || metrics.DirtyImageBytes != dirty.TotalBytes || metrics.StableFiles != len(stable.Files) || metrics.DirtyFiles != len(dirty.Files) || metrics.TraceEvents != len(trace.Events) {
		return metricsArtifact{}, fmt.Errorf("%s does not match operation trace and captured image trees", prefix)
	}
	if !validHex(metrics.StableFingerprint, 64) {
		return metricsArtifact{}, fmt.Errorf("%s has invalid stable_fingerprint", prefix)
	}
	return metrics, nil
}

func verifyRecoveryTrace(root, evidenceDir, manifestID string, witness Witness, artifact, stableArtifact Artifact, metrics metricsArtifact) (recoveryTraceArtifact, error) {
	prefix := fmt.Sprintf("powerlosscert: manifest %q witness %q recovery trace", manifestID, witness.ID)
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(normalizedArtifactPath(artifact.Path))))
	if err != nil {
		return recoveryTraceArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	var recovery recoveryTraceArtifact
	if err := decodeStrict(data, &recovery); err != nil {
		return recoveryTraceArtifact{}, fmt.Errorf("%s decode: %w", prefix, err)
	}
	expectedRecoveryDir, err := normalizeRecoveryDir(witness.ExpectedRecoveryDir)
	if err != nil {
		return recoveryTraceArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	if actualRecoveryDir, normalizeErr := normalizeRecoveryDir(recovery.Dir); recovery.Dir == "" || normalizeErr != nil || actualRecoveryDir != expectedRecoveryDir {
		return recoveryTraceArtifact{}, fmt.Errorf("%s dir=%q does not match expected recovery directory %q", prefix, recovery.Dir, expectedRecoveryDir)
	}
	if recovery.SchemaVersion != recoveryTraceSchemaVersion || recovery.PublicAPI != "treedb.Open" || recovery.PreOpenSnapshotDir != "recovery-preopen" {
		return recoveryTraceArtifact{}, fmt.Errorf("%s has invalid schema, public_api, dir, or pre_open_snapshot_dir", prefix)
	}
	if recovery.InputTreeSHA256 != stableArtifact.SHA256 || recovery.StableFingerprint != metrics.StableFingerprint {
		return recoveryTraceArtifact{}, fmt.Errorf("%s does not identify the captured stable image", prefix)
	}
	if recovery.Rejected {
		if recovery.ErrorType == "" || recovery.Error == "" || len(recovery.Stats) != 0 || witness.TypedError != recovery.ErrorType || modeledOutcomeClass(witness.ActualOutcome) != "rejected" {
			return recoveryTraceArtifact{}, fmt.Errorf("%s rejected result does not match outcome=%q and typed_error=%q", prefix, witness.ActualOutcome, witness.TypedError)
		}
	} else if recovery.ErrorType != "" || recovery.Error != "" || witness.TypedError != "none" || modeledOutcomeClass(witness.ActualOutcome) != "accepted" {
		return recoveryTraceArtifact{}, fmt.Errorf("%s accepted result does not match outcome=%q and typed_error=%q", prefix, witness.ActualOutcome, witness.TypedError)
	}
	if err := validateRecoveryStats(recovery, witness.Command.Env[powerLossProfileEnv]); err != nil {
		return recoveryTraceArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	got, err := observedWitnessStateForComparison(recovery, witness.StateComparison)
	if err != nil {
		return recoveryTraceArtifact{}, fmt.Errorf("%s: %w", prefix, err)
	}
	if got != witness.State {
		return recoveryTraceArtifact{}, fmt.Errorf("%s observed state=%+v does not match manifest state=%+v", prefix, got, witness.State)
	}
	recoveryDir := filepath.Join(root, filepath.FromSlash(evidenceDir), recovery.Dir)
	if !pathWithin(root, recoveryDir) {
		return recoveryTraceArtifact{}, fmt.Errorf("%s has unsafe recovery directory", prefix)
	}
	info, err := os.Lstat(recoveryDir)
	if err != nil {
		return recoveryTraceArtifact{}, fmt.Errorf("%s recovery directory: %w", prefix, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return recoveryTraceArtifact{}, fmt.Errorf("%s recovery directory is not a real directory", prefix)
	}
	return recovery, nil
}

func validateRecoveryStats(recovery recoveryTraceArtifact, expectedProfile string) error {
	if recovery.Rejected {
		if len(recovery.Stats) != 0 {
			return fmt.Errorf("rejected recovery unexpectedly reports selected-state stats")
		}
		if recovery.CommitSeq != 0 || recovery.AppliedLSN != 0 {
			return fmt.Errorf("rejected recovery unexpectedly reports scalar selected state")
		}
		return nil
	}
	if len(recovery.Stats) != len(requiredRecoveryStats) {
		return fmt.Errorf("accepted recovery stats=%d want=%d", len(recovery.Stats), len(requiredRecoveryStats))
	}
	for _, key := range requiredRecoveryStats {
		value, ok := recovery.Stats[key]
		if !ok || value == "" {
			return fmt.Errorf("accepted recovery omits required stat %q", key)
		}
		if key != "treedb.profile.resolved" {
			if _, err := strconv.ParseUint(value, 10, 64); err != nil {
				return fmt.Errorf("accepted recovery stat %s=%q is not uint64", key, value)
			}
		}
	}
	resolvedProfile := recovery.Stats["treedb.profile.resolved"]
	if !productionProfiles[resolvedProfile] {
		return fmt.Errorf("accepted recovery has non-production profile %q", resolvedProfile)
	}
	if resolvedProfile != expectedProfile {
		return fmt.Errorf("accepted recovery resolved profile %q does not match command-required profile %q", resolvedProfile, expectedProfile)
	}
	if recovery.Stats["treedb.commit_seq"] != strconv.FormatUint(recovery.CommitSeq, 10) || recovery.Stats["treedb.applied_command_lsn"] != strconv.FormatUint(recovery.AppliedLSN, 10) {
		return fmt.Errorf("accepted recovery scalar state does not match reported stats")
	}
	return nil
}

func verifyCommandLog(root string, manifest ChildManifest, witness Witness, artifact Artifact) error {
	prefix := fmt.Sprintf("powerlosscert: manifest %q witness %q command log", manifest.ManifestID, witness.ID)
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(normalizedArtifactPath(artifact.Path))))
	if err != nil {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	var log commandLogArtifact
	if err := decodeStrict(data, &log); err != nil {
		return fmt.Errorf("%s decode: %w", prefix, err)
	}
	var binarySHA string
	for _, binary := range manifest.TestBinaries {
		if normalizedArtifactPath(binary.Path) == normalizedArtifactPath(witness.Command.BinaryPath) {
			binarySHA = binary.SHA256
			break
		}
	}
	if log.SchemaVersion != commandLogSchemaVersion || log.RepositorySHA != manifest.RepositorySHA || normalizedArtifactPath(log.BinaryPath) != normalizedArtifactPath(witness.Command.BinaryPath) || log.BinarySHA256 != binarySHA || log.Package != witness.Command.Package || log.TestName != witness.Command.TestName || !reflect.DeepEqual(log.Args, witness.Command.Args) || !reflect.DeepEqual(log.Env, witness.Command.Env) || log.Outcome != witness.ActualOutcome {
		return fmt.Errorf("%s does not match the manifest command and test binary", prefix)
	}
	if !log.Completed || log.ExitCode != 0 || log.Stdout == "" && log.Stderr == "" {
		return fmt.Errorf("%s does not record a completed successful command with captured output", prefix)
	}
	return nil
}

func parseCutAddress(cutID string) (string, int, error) {
	parts := strings.Split(cutID, "/")
	if len(parts) != 4 || parts[0] != "cut" || parts[1] == "" {
		return "", 0, fmt.Errorf("invalid cut_id %q", cutID)
	}
	cutPoint, err := url.PathUnescape(parts[2])
	if err != nil || cutPoint == "" {
		return "", 0, fmt.Errorf("invalid cut point in cut_id %q", cutID)
	}
	occurrence, err := strconv.Atoi(parts[3])
	if err != nil || occurrence < 0 {
		return "", 0, fmt.Errorf("invalid cut occurrence in cut_id %q", cutID)
	}
	return cutPoint, occurrence, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func pathWithin(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !filepath.IsAbs(rel) && (rel == "." || len(rel) < 3 || rel[:3] != ".."+string(filepath.Separator))
}
