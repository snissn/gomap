package powerlossoracle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	EnvEvidenceDir      = "TREEDB_POWERLOSS_EVIDENCE_DIR"
	EnvEvidenceCutPoint = "TREEDB_POWERLOSS_EXPECT_CUT_POINT"
)

type EvidenceSession struct {
	Root               string
	CutPoint           string
	ObservedEventCount int
}

type evidenceTrace struct {
	SchemaVersion      string   `json:"schema_version"`
	CutID              string   `json:"cut_id"`
	VariantID          string   `json:"variant_id"`
	Seed               string   `json:"seed"`
	DeclaredCutPoint   string   `json:"declared_cut_point"`
	ObservedEventCount int      `json:"observed_event_count"`
	Events             []string `json:"events"`
}

type evidenceTree struct {
	SchemaVersion string             `json:"schema_version"`
	Kind          string             `json:"kind"`
	Files         []evidenceTreeFile `json:"files"`
	TotalBytes    int64              `json:"total_bytes"`
}

type evidenceTreeFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type evidenceMetrics struct {
	SchemaVersion     string `json:"schema_version"`
	StableImageBytes  int64  `json:"stable_image_bytes"`
	DirtyImageBytes   int64  `json:"dirty_image_bytes"`
	StableFiles       int    `json:"stable_files"`
	DirtyFiles        int    `json:"dirty_files"`
	TraceEvents       int    `json:"trace_events"`
	StableFingerprint string `json:"stable_fingerprint"`
}

func BeginEvidenceFromEnv(model *Model) (*EvidenceSession, error) {
	root := os.Getenv(EnvEvidenceDir)
	cutPoint := os.Getenv(EnvEvidenceCutPoint)
	if root == "" && cutPoint == "" {
		return nil, nil
	}
	if root == "" || cutPoint == "" {
		return nil, errorsf("evidence capture requires %s and %s", EnvEvidenceDir, EnvEvidenceCutPoint)
	}
	if err := requireEmptyEvidenceRoot(root); err != nil {
		return nil, err
	}
	trace := model.Trace()
	cutPrefix := "cut:" + cutPoint + ":"
	observed := 0
	for _, event := range trace {
		if strings.HasPrefix(event, cutPrefix) {
			observed++
		}
	}
	if observed == 0 {
		return nil, errorsf("declared cut point %q was not observed", cutPoint)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	stableDir := filepath.Join(root, "stable-image")
	dirtyDir := filepath.Join(root, "dirty-image")
	if err := model.MaterializeStable(stableDir); err != nil {
		return nil, fmt.Errorf("powerlossoracle: materialize stable evidence: %w", err)
	}
	if err := model.MaterializeVolatile(dirtyDir); err != nil {
		return nil, fmt.Errorf("powerlossoracle: materialize dirty evidence: %w", err)
	}
	stableTree, err := buildEvidenceTree(stableDir, "stable-image")
	if err != nil {
		return nil, err
	}
	dirtyTree, err := buildEvidenceTree(dirtyDir, "dirty-image")
	if err != nil {
		return nil, err
	}
	if err := writeEvidenceJSON(filepath.Join(root, "operation_trace.json"), evidenceTrace{
		SchemaVersion:      "treedb-power-loss-operation-trace/v1",
		CutID:              os.Getenv(EnvReplayCut),
		VariantID:          os.Getenv(EnvReplayVariant),
		Seed:               os.Getenv(EnvReplaySeed),
		DeclaredCutPoint:   cutPoint,
		ObservedEventCount: observed,
		Events:             trace,
	}); err != nil {
		return nil, err
	}
	if err := writeEvidenceJSON(filepath.Join(root, "stable_image_tree.json"), stableTree); err != nil {
		return nil, err
	}
	if err := writeEvidenceJSON(filepath.Join(root, "dirty_image_tree.json"), dirtyTree); err != nil {
		return nil, err
	}
	if err := writeEvidenceJSON(filepath.Join(root, "metrics.json"), evidenceMetrics{
		SchemaVersion:     "treedb-power-loss-metrics/v1",
		StableImageBytes:  stableTree.TotalBytes,
		DirtyImageBytes:   dirtyTree.TotalBytes,
		StableFiles:       len(stableTree.Files),
		DirtyFiles:        len(dirtyTree.Files),
		TraceEvents:       len(trace),
		StableFingerprint: model.StableFingerprint(),
	}); err != nil {
		return nil, err
	}
	return &EvidenceSession{Root: root, CutPoint: cutPoint, ObservedEventCount: observed}, nil
}

func (session *EvidenceSession) StableImageDir() string {
	if session == nil {
		return ""
	}
	return filepath.Join(session.Root, "stable-image")
}

func (session *EvidenceSession) RecordRecovery(value any) error {
	if session == nil {
		return nil
	}
	return writeEvidenceJSON(filepath.Join(session.Root, "recovery_trace.json"), value)
}

func requireEmptyEvidenceRoot(root string) error {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("powerlossoracle: inspect evidence directory %q: %w", root, err)
	}
	if len(entries) != 0 {
		return errorsf("evidence directory %q is not empty", root)
	}
	return nil
}

func buildEvidenceTree(root, kind string) (evidenceTree, error) {
	tree := evidenceTree{SchemaVersion: "treedb-power-loss-image-tree/v1", Kind: kind}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("powerlossoracle: evidence image contains non-regular path %q", path)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		digest, err := evidenceFileSHA256(path)
		if err != nil {
			return err
		}
		tree.Files = append(tree.Files, evidenceTreeFile{Path: filepath.ToSlash(rel), Bytes: info.Size(), SHA256: digest})
		tree.TotalBytes += info.Size()
		return nil
	})
	if err != nil {
		return evidenceTree{}, err
	}
	sort.Slice(tree.Files, func(i, j int) bool { return tree.Files[i].Path < tree.Files[j].Path })
	return tree, nil
}

func evidenceFileSHA256(path string) (string, error) {
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

func writeEvidenceJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("powerlossoracle: create evidence %q: %w", path, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
