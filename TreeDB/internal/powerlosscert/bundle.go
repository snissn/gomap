package powerlosscert

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const operationTraceSchemaVersion = "treedb-power-loss-operation-trace/v1"

type operationTraceArtifact struct {
	SchemaVersion      string   `json:"schema_version"`
	CutID              string   `json:"cut_id"`
	VariantID          string   `json:"variant_id"`
	Seed               string   `json:"seed"`
	DeclaredCutPoint   string   `json:"declared_cut_point"`
	ObservedEventCount int      `json:"observed_event_count"`
	Events             []string `json:"events"`
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
	inventoryData, err := os.ReadFile(filepath.Join(root, "risk_inventory.json"))
	if err != nil {
		return Bundle{}, fmt.Errorf("powerlosscert: read risk inventory: %w", err)
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
		data, err := os.ReadFile(path)
		if err != nil {
			return Bundle{}, fmt.Errorf("powerlosscert: read child manifest %q: %w", path, err)
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
	for _, manifest := range manifests {
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
			if err := verifyOperationTrace(root, manifest.ManifestID, witness); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyOperationTrace(root, manifestID string, witness Witness) error {
	prefix := fmt.Sprintf("powerlosscert: manifest %q witness %q operation trace", manifestID, witness.ID)
	var traceArtifact *Artifact
	for index := range witness.Artifacts {
		if witness.Artifacts[index].Kind == ArtifactKindOperationTrace {
			traceArtifact = &witness.Artifacts[index]
			break
		}
	}
	if traceArtifact == nil {
		return fmt.Errorf("%s is missing", prefix)
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(normalizedArtifactPath(traceArtifact.Path))))
	if err != nil {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	var trace operationTraceArtifact
	if err := decodeStrict(data, &trace); err != nil {
		return fmt.Errorf("%s decode: %w", prefix, err)
	}
	if trace.SchemaVersion != operationTraceSchemaVersion {
		return fmt.Errorf("%s schema_version=%q want=%q", prefix, trace.SchemaVersion, operationTraceSchemaVersion)
	}
	if trace.CutID != witness.CutID {
		return fmt.Errorf("%s cut_id=%q want=%q", prefix, trace.CutID, witness.CutID)
	}
	if trace.DeclaredCutPoint != witness.CutPoint {
		return fmt.Errorf("%s declared_cut_point=%q want=%q", prefix, trace.DeclaredCutPoint, witness.CutPoint)
	}
	cutPoint, occurrence, err := parseCutAddress(trace.CutID)
	if err != nil {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	if cutPoint != witness.CutPoint || occurrence != witness.CutOccurrence {
		return fmt.Errorf("%s cut address=(%s,%d) want=(%s,%d)", prefix, cutPoint, occurrence, witness.CutPoint, witness.CutOccurrence)
	}
	wantSeed := strconv.FormatUint(witness.Seed, 10)
	if trace.Seed != wantSeed {
		return fmt.Errorf("%s seed=%q want=%q", prefix, trace.Seed, wantSeed)
	}
	if trace.VariantID == "" {
		return fmt.Errorf("%s has empty variant_id", prefix)
	}
	matchingEvents := 0
	cutPrefix := "cut:" + witness.CutPoint + ":"
	for _, event := range trace.Events {
		if strings.HasPrefix(event, cutPrefix) {
			matchingEvents++
		}
	}
	if trace.ObservedEventCount != matchingEvents || trace.ObservedEventCount != witness.ObservedEventCount {
		return fmt.Errorf("%s observed_event_count=%d matching_events=%d witness=%d", prefix, trace.ObservedEventCount, matchingEvents, witness.ObservedEventCount)
	}
	if matchingEvents <= witness.CutOccurrence {
		return fmt.Errorf("%s matching events=%d do not reach occurrence=%d", prefix, matchingEvents, witness.CutOccurrence)
	}
	wantEnv := map[string]string{
		"TREEDB_POWERLOSS_CUT_ID":           witness.CutID,
		"TREEDB_POWERLOSS_VARIANT_ID":       trace.VariantID,
		"TREEDB_POWERLOSS_SEED":             wantSeed,
		"TREEDB_POWERLOSS_EXPECT_CUT_POINT": witness.CutPoint,
	}
	for name, want := range wantEnv {
		if got := witness.Command.Env[name]; got != want {
			return fmt.Errorf("%s command env %s=%q want=%q", prefix, name, got, want)
		}
	}
	if witness.Command.Env["TREEDB_POWERLOSS_EVIDENCE_DIR"] == "" {
		return fmt.Errorf("%s command env TREEDB_POWERLOSS_EVIDENCE_DIR is empty", prefix)
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
