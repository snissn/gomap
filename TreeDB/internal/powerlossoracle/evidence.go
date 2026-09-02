package powerlossoracle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	EnvEvidenceDir          = "TREEDB_POWERLOSS_EVIDENCE_DIR"
	EnvEvidenceCutPoint     = "TREEDB_POWERLOSS_EXPECT_CUT_POINT"
	EnvEvidenceReopenMode   = "TREEDB_POWERLOSS_REOPEN_MODE"
	EnvEvidenceReplayWindow = "TREEDB_POWERLOSS_REPLAY_WINDOW"

	EvidenceReopenReadWrite = "read-write"
	EvidenceReopenReadOnly  = "read-only"
)

type EvidenceRequest struct {
	Root         string
	CutPoint     string
	Selector     ReplaySelector
	Occurrence   int
	ReopenMode   string
	ReplayWindow string
}

func (request EvidenceRequest) Enabled() bool {
	return request.Root != ""
}

func (request EvidenceRequest) ReadOnly() bool {
	return request.ReopenMode == EvidenceReopenReadOnly
}

type EvidenceSession struct {
	Root               string
	CutPoint           string
	ObservedEventCount int
	stableTreeSHA256   string
	stableFingerprint  string
}

type evidenceTrace struct {
	SchemaVersion      string   `json:"schema_version"`
	CutID              string   `json:"cut_id"`
	VariantID          string   `json:"variant_id"`
	Seed               string   `json:"seed"`
	DeclaredCutPoint   string   `json:"declared_cut_point"`
	ReplayWindow       string   `json:"replay_window,omitempty"`
	ObservedEventCount int      `json:"observed_event_count"`
	Events             []string `json:"events"`
}

type evidenceTree struct {
	SchemaVersion string             `json:"schema_version"`
	Kind          string             `json:"kind"`
	Directories   []string           `json:"directories"`
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

func EvidenceRequestFromEnv() (EvidenceRequest, error) {
	root := os.Getenv(EnvEvidenceDir)
	cutPoint := os.Getenv(EnvEvidenceCutPoint)
	reopenMode := os.Getenv(EnvEvidenceReopenMode)
	replayWindow := os.Getenv(EnvEvidenceReplayWindow)
	if root == "" && cutPoint == "" && reopenMode == "" && replayWindow == "" {
		return EvidenceRequest{}, nil
	}
	if root == "" || cutPoint == "" || reopenMode == "" {
		return EvidenceRequest{}, errorsf("evidence capture requires %s, %s, and %s", EnvEvidenceDir, EnvEvidenceCutPoint, EnvEvidenceReopenMode)
	}
	if reopenMode != EvidenceReopenReadWrite && reopenMode != EvidenceReopenReadOnly {
		return EvidenceRequest{}, errorsf("invalid evidence reopen mode %q", reopenMode)
	}
	selector, err := ReplaySelectorFromEnv()
	if err != nil {
		return EvidenceRequest{}, err
	}
	if selector == (ReplaySelector{}) {
		return EvidenceRequest{}, errorsf("evidence capture requires %s, %s, and %s", EnvReplayCut, EnvReplayVariant, EnvReplaySeed)
	}
	replayCutPoint, replayOccurrence, err := ParseReplayCutAddress(selector.CutID)
	if err != nil {
		return EvidenceRequest{}, err
	}
	if replayCutPoint != cutPoint {
		return EvidenceRequest{}, errorsf("replay cut point %q does not match declared cut point %q", replayCutPoint, cutPoint)
	}
	if replayWindow != "" && replayWindow != selector.VariantID {
		return EvidenceRequest{}, errorsf("evidence replay window %q does not match replay variant %q", replayWindow, selector.VariantID)
	}
	return EvidenceRequest{
		Root:         root,
		CutPoint:     cutPoint,
		Selector:     selector,
		Occurrence:   replayOccurrence,
		ReopenMode:   reopenMode,
		ReplayWindow: replayWindow,
	}, nil
}

func BeginEvidenceFromEnv(model *Model, readOnly bool) (*EvidenceSession, error) {
	request, err := EvidenceRequestFromEnv()
	if err != nil {
		return nil, err
	}
	if !request.Enabled() {
		return nil, nil
	}
	if request.ReadOnly() != readOnly {
		return nil, errorsf("evidence reopen mode %q does not match readOnly=%t", request.ReopenMode, readOnly)
	}
	if err := requireEmptyEvidenceRoot(request.Root); err != nil {
		return nil, err
	}
	trace := portableEvidenceTrace(model.Trace())
	observed, err := replayWindowCutCount(trace, request.CutPoint, request.ReplayWindow)
	if err != nil {
		return nil, err
	}
	if observed == 0 {
		return nil, errorsf("declared cut point %q was not observed", request.CutPoint)
	}
	if observed != request.Occurrence+1 {
		return nil, errorsf("observed cut events=%d does not end at replay occurrence=%d", observed, request.Occurrence)
	}
	if err := os.MkdirAll(request.Root, 0o755); err != nil {
		return nil, err
	}
	stableDir := filepath.Join(request.Root, "stable-image")
	dirtyDir := filepath.Join(request.Root, "dirty-image")
	preOpenDir := filepath.Join(request.Root, "recovery-preopen")
	recoveryDir := filepath.Join(request.Root, "recovery-input")
	if err := model.MaterializeStable(stableDir); err != nil {
		return nil, fmt.Errorf("powerlossoracle: materialize stable evidence: %w", err)
	}
	if err := model.MaterializeVolatile(dirtyDir); err != nil {
		return nil, fmt.Errorf("powerlossoracle: materialize dirty evidence: %w", err)
	}
	if err := model.MaterializeStable(preOpenDir); err != nil {
		return nil, fmt.Errorf("powerlossoracle: materialize pre-open recovery evidence: %w", err)
	}
	if err := model.MaterializeStable(recoveryDir); err != nil {
		return nil, fmt.Errorf("powerlossoracle: materialize recovery input: %w", err)
	}
	stableTree, err := buildEvidenceTree(stableDir, "stable-image")
	if err != nil {
		return nil, err
	}
	dirtyTree, err := buildEvidenceTree(dirtyDir, "dirty-image")
	if err != nil {
		return nil, err
	}
	if err := writeEvidenceJSON(filepath.Join(request.Root, "operation_trace.json"), evidenceTrace{
		SchemaVersion:      "treedb-power-loss-operation-trace/v2",
		CutID:              request.Selector.CutID,
		VariantID:          request.Selector.VariantID,
		Seed:               strconv.FormatUint(request.Selector.Seed, 10),
		DeclaredCutPoint:   request.CutPoint,
		ReplayWindow:       request.ReplayWindow,
		ObservedEventCount: observed,
		Events:             trace,
	}); err != nil {
		return nil, err
	}
	stableTreePath := filepath.Join(request.Root, "stable_image_tree.json")
	if err := writeEvidenceJSON(stableTreePath, stableTree); err != nil {
		return nil, err
	}
	stableTreeSHA256, err := evidenceFileSHA256(stableTreePath)
	if err != nil {
		return nil, fmt.Errorf("powerlossoracle: hash stable image tree: %w", err)
	}
	if err := writeEvidenceJSON(filepath.Join(request.Root, "dirty_image_tree.json"), dirtyTree); err != nil {
		return nil, err
	}
	if err := writeEvidenceJSON(filepath.Join(request.Root, "metrics.json"), evidenceMetrics{
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
	return &EvidenceSession{
		Root:               request.Root,
		CutPoint:           request.CutPoint,
		ObservedEventCount: observed,
		stableTreeSHA256:   stableTreeSHA256,
		stableFingerprint:  model.StableFingerprint(),
	}, nil
}

func replayWindowCutCount(trace []string, cutPoint, replayWindow string) (int, error) {
	cutPrefix := "cut:" + cutPoint + ":"
	windowMarker := replayWindowMarker(replayWindow)
	observed := 0
	markerCount := 0
	inWindow := replayWindow == ""
	for _, event := range trace {
		if strings.HasPrefix(event, "replay-window:") {
			if replayWindow == "" {
				return 0, errorsf("operation trace contains replay-window marker without a declared replay window")
			}
			if event != windowMarker {
				return 0, errorsf("operation trace replay-window marker %q does not match declared replay window %q", event, replayWindow)
			}
			markerCount++
			if markerCount > 1 {
				return 0, errorsf("operation trace requires exactly one replay-window marker %q; found %d", windowMarker, markerCount)
			}
			inWindow = true
			continue
		}
		if inWindow && strings.HasPrefix(event, cutPrefix) {
			observed++
		}
	}
	if replayWindow != "" && markerCount != 1 {
		return 0, errorsf("operation trace requires exactly one replay-window marker %q; found %d", windowMarker, markerCount)
	}
	if replayWindow != "" && observed == 0 {
		return 0, errorsf("operation trace replay-window marker %q does not precede a matching cut %q", windowMarker, cutPoint)
	}
	return observed, nil
}

func portableEvidenceTrace(trace []string) []string {
	out := append([]string(nil), trace...)
	for index, event := range out {
		if strings.HasPrefix(event, "overlay:") {
			out[index] = "overlay:<source-root>"
		}
	}
	return out
}

func (session *EvidenceSession) StableImageDir() string {
	if session == nil {
		return ""
	}
	return filepath.Join(session.Root, "stable-image")
}

func (session *EvidenceSession) RecoveryInputDir() string {
	if session == nil {
		return ""
	}
	return filepath.Join(session.Root, "recovery-input")
}

// StableImageTreeSHA256 binds recovery evidence to the immutable manifest of
// the exact stable-byte image copied into RecoveryInputDir before public open.
func (session *EvidenceSession) StableImageTreeSHA256() string {
	if session == nil {
		return ""
	}
	return session.stableTreeSHA256
}

// StableFingerprint identifies the modeled stable namespace, identities, and
// bytes from which the immutable image and recovery input were materialized.
func (session *EvidenceSession) StableFingerprint() string {
	if session == nil {
		return ""
	}
	return session.stableFingerprint
}

func (session *EvidenceSession) RecordRecovery(value any) error {
	if session == nil {
		return nil
	}
	return writeEvidenceJSON(filepath.Join(session.Root, "recovery_trace.json"), value)
}

func requireEmptyEvidenceRoot(root string) error {
	if err := EnsureNoSymlinkComponents(root); err != nil {
		return fmt.Errorf("powerlossoracle: inspect evidence directory %q: %w", root, err)
	}
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("powerlossoracle: inspect evidence directory %q: %w", root, err)
	}
	if !info.IsDir() {
		return errorsf("evidence directory %q is not a directory", root)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("powerlossoracle: inspect evidence directory %q: %w", root, err)
	}
	if len(entries) != 0 {
		return errorsf("evidence directory %q is not empty", root)
	}
	return nil
}

// EnsureNoSymlinkComponents rejects any existing symlink in path, including
// ancestors of a final component that does not exist yet.
func EnsureNoSymlinkComponents(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve path %q: %w", path, err)
	}
	for current := filepath.Clean(absolute); ; {
		info, err := os.Lstat(current)
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path component %q is a symlink", current)
		}
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("inspect path component %q: %w", current, err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
		current = parent
	}
}

// ParseReplayCutAddress returns the cut point and zero-based occurrence encoded
// by a stable replay cut ID.
func ParseReplayCutAddress(cutID string) (string, int, error) {
	parts := strings.Split(cutID, "/")
	if len(parts) != 4 || parts[0] != "cut" || parts[1] == "" {
		return "", 0, errorsf("invalid replay cut id %q", cutID)
	}
	cutPoint, err := url.PathUnescape(parts[2])
	if err != nil || cutPoint == "" {
		return "", 0, errorsf("invalid replay cut point in %q", cutID)
	}
	occurrence, err := strconv.Atoi(parts[3])
	if err != nil || occurrence < 0 {
		return "", 0, errorsf("invalid replay cut occurrence in %q", cutID)
	}
	return cutPoint, occurrence, nil
}

func buildEvidenceTree(root, kind string) (evidenceTree, error) {
	tree := evidenceTree{SchemaVersion: "treedb-power-loss-image-tree/v1", Kind: kind}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root {
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				tree.Directories = append(tree.Directories, filepath.ToSlash(rel))
			}
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
	sort.Strings(tree.Directories)
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
