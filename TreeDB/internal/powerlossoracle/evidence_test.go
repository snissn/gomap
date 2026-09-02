package powerlossoracle

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestBeginEvidenceFromEnvPersistsImagesTraceAndMetrics(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(source, "empty-wal"), 0o700); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Write("index.db", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := model.Overlay(source); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
		t.Fatal(err)
	}
	evidenceDir := filepath.Join(canonicalTempDir(t), "evidence")
	t.Setenv(EnvEvidenceDir, evidenceDir)
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadWrite)
	t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(EnvReplayVariant, "variant-a")
	t.Setenv(EnvReplaySeed, "1")

	session, err := BeginEvidenceFromEnv(model, false)
	if err != nil {
		t.Fatal(err)
	}
	if session == nil || session.ObservedEventCount != 1 {
		t.Fatalf("session=%+v", session)
	}
	if len(session.StableImageTreeSHA256()) != 64 || len(session.StableFingerprint()) != 64 {
		t.Fatalf("session evidence identities tree=%q fingerprint=%q", session.StableImageTreeSHA256(), session.StableFingerprint())
	}
	for _, path := range []string{
		"operation_trace.json",
		"stable_image_tree.json",
		"dirty_image_tree.json",
		"metrics.json",
		"stable-image/index.db",
		"dirty-image/index.db",
	} {
		if _, err := os.Stat(filepath.Join(evidenceDir, filepath.FromSlash(path))); err != nil {
			t.Fatalf("missing evidence %s: %v", path, err)
		}
	}
	traceData, err := os.ReadFile(filepath.Join(evidenceDir, "operation_trace.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(traceData), source) {
		t.Fatalf("operation trace leaked host-specific source path: %s", traceData)
	}
	stableTreeData, err := os.ReadFile(filepath.Join(evidenceDir, "stable_image_tree.json"))
	if err != nil {
		t.Fatal(err)
	}
	var stableTree evidenceTree
	if err := json.Unmarshal(stableTreeData, &stableTree); err != nil {
		t.Fatal(err)
	}
	if len(stableTree.Directories) != 1 || stableTree.Directories[0] != "empty-wal" {
		t.Fatalf("stable image directories=%v want [empty-wal]", stableTree.Directories)
	}
	for _, image := range []string{"stable-image", "dirty-image", "recovery-preopen", "recovery-input"} {
		info, err := os.Stat(filepath.Join(evidenceDir, image, "empty-wal"))
		if err != nil || !info.IsDir() {
			t.Fatalf("%s empty directory info=%v error=%v", image, info, err)
		}
	}
	if err := session.RecordRecovery(map[string]any{"opened": true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(evidenceDir, "recovery_trace.json")); err != nil {
		t.Fatal(err)
	}
}

func TestBeginEvidenceFromEnvRejectsUnobservedDeclaredCut(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvEvidenceDir, filepath.Join(canonicalTempDir(t), "evidence"))
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadWrite)
	t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(EnvReplayVariant, "variant-a")
	t.Setenv(EnvReplaySeed, "1")
	if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "was not observed") {
		t.Fatalf("BeginEvidenceFromEnv error=%v", err)
	}
}

func TestBeginEvidenceFromEnvRequiresReplaySelector(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvEvidenceDir, filepath.Join(canonicalTempDir(t), "evidence"))
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadWrite)
	t.Setenv(EnvReplayCut, "")
	t.Setenv(EnvReplayVariant, "")
	t.Setenv(EnvReplaySeed, "")

	if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "evidence capture requires") {
		t.Fatalf("BeginEvidenceFromEnv missing replay selector error=%v", err)
	}
}

func TestEvidenceRequestFromEnvRequiresValidReopenMode(t *testing.T) {
	t.Setenv(EnvEvidenceDir, filepath.Join(canonicalTempDir(t), "evidence"))
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(EnvReplayVariant, "variant-a")
	t.Setenv(EnvReplaySeed, "1")

	if _, err := EvidenceRequestFromEnv(); err == nil || !strings.Contains(err.Error(), EnvEvidenceReopenMode) {
		t.Fatalf("EvidenceRequestFromEnv missing reopen mode error=%v", err)
	}
	t.Setenv(EnvEvidenceReopenMode, "reader-ish")
	if _, err := EvidenceRequestFromEnv(); err == nil || !strings.Contains(err.Error(), "invalid evidence reopen mode") {
		t.Fatalf("EvidenceRequestFromEnv invalid reopen mode error=%v", err)
	}
	for _, mode := range []string{EvidenceReopenReadWrite, EvidenceReopenReadOnly} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(EnvEvidenceReopenMode, mode)
			request, err := EvidenceRequestFromEnv()
			if err != nil {
				t.Fatal(err)
			}
			if !request.Enabled() || request.Occurrence != 0 || request.ReadOnly() != (mode == EvidenceReopenReadOnly) {
				t.Fatalf("request=%+v", request)
			}
		})
	}
}

func TestEvidenceRequestFromEnvRejectsReplayWindowVariantMismatch(t *testing.T) {
	t.Setenv(EnvEvidenceDir, filepath.Join(canonicalTempDir(t), "evidence"))
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadOnly)
	t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(EnvReplayVariant, "variant-a")
	t.Setenv(EnvReplaySeed, "1")
	t.Setenv(EnvEvidenceReplayWindow, "variant-b")

	if _, err := EvidenceRequestFromEnv(); err == nil || !strings.Contains(err.Error(), "does not match replay variant") {
		t.Fatalf("EvidenceRequestFromEnv replay-window mismatch error=%v", err)
	}
}

func TestBeginEvidenceFromEnvRejectsReopenModeMismatchBeforeCapture(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
		t.Fatal(err)
	}
	evidenceDir := filepath.Join(canonicalTempDir(t), "evidence")
	t.Setenv(EnvEvidenceDir, evidenceDir)
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadOnly)
	t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(EnvReplayVariant, "variant-a")
	t.Setenv(EnvReplaySeed, "1")

	if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "does not match readOnly=false") {
		t.Fatalf("BeginEvidenceFromEnv reopen-mode mismatch error=%v", err)
	}
	if _, err := os.Stat(evidenceDir); !os.IsNotExist(err) {
		t.Fatalf("mismatched reopen mode created evidence root: %v", err)
	}
}

func TestBeginEvidenceFromEnvRequiresTraceToEndAtReplayOccurrence(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv(EnvEvidenceDir, filepath.Join(canonicalTempDir(t), "evidence"))
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadWrite)
	t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(EnvReplayVariant, "variant-a")
	t.Setenv(EnvReplaySeed, "1")

	if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "does not end at replay occurrence") {
		t.Fatalf("BeginEvidenceFromEnv extra matching cut event error=%v", err)
	}
}

func TestBeginEvidenceFromEnvScopesOccurrenceAfterReplayWindowAndRetainsPrefix(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.BeginReplayWindow("variant-a"); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
		t.Fatal(err)
	}

	evidenceDir := filepath.Join(canonicalTempDir(t), "evidence")
	t.Setenv(EnvEvidenceDir, evidenceDir)
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadWrite)
	t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(EnvReplayVariant, "variant-a")
	t.Setenv(EnvReplaySeed, "1")
	t.Setenv(EnvEvidenceReplayWindow, "variant-a")

	session, err := BeginEvidenceFromEnv(model, false)
	if err != nil {
		t.Fatal(err)
	}
	if session.ObservedEventCount != 1 {
		t.Fatalf("observed event count=%d want window-relative 1", session.ObservedEventCount)
	}
	var trace evidenceTrace
	data, err := os.ReadFile(filepath.Join(evidenceDir, "operation_trace.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	if got, err := replayWindowCutCount(trace.Events, string(durabilitycut.AfterMetaWrite), "variant-a"); err != nil || got != 1 {
		t.Fatalf("window-relative trace cuts=%d error=%v want 1", got, err)
	}
	if trace.ReplayWindow != "variant-a" {
		t.Fatalf("trace replay window=%q want variant-a", trace.ReplayWindow)
	}
	allCuts := 0
	for _, event := range trace.Events {
		if strings.HasPrefix(event, "cut:"+string(durabilitycut.AfterMetaWrite)+":") {
			allCuts++
		}
	}
	if allCuts != 3 {
		t.Fatalf("retained full-trace cuts=%d want 3", allCuts)
	}
}

func TestBeginEvidenceFromEnvRejectsInvalidReplayWindowTrace(t *testing.T) {
	newModel := func(t *testing.T) (*Model, string) {
		t.Helper()
		source := t.TempDir()
		if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		model, err := Capture(source)
		if err != nil {
			t.Fatal(err)
		}
		return model, source
	}
	setEnv := func(t *testing.T) {
		t.Helper()
		t.Setenv(EnvEvidenceDir, filepath.Join(canonicalTempDir(t), "evidence"))
		t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
		t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadWrite)
		t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
		t.Setenv(EnvReplayVariant, "variant-a")
		t.Setenv(EnvReplaySeed, "1")
		t.Setenv(EnvEvidenceReplayWindow, "variant-a")
	}
	observe := func(t *testing.T, model *Model, source string) {
		t.Helper()
		if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("missing-marker", func(t *testing.T) {
		model, source := newModel(t)
		observe(t, model, source)
		setEnv(t)
		if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "exactly one replay-window marker") {
			t.Fatalf("BeginEvidenceFromEnv missing-marker error=%v", err)
		}
	})

	t.Run("duplicate-marker", func(t *testing.T) {
		model, source := newModel(t)
		if err := model.BeginReplayWindow("variant-a"); err != nil {
			t.Fatal(err)
		}
		if err := model.BeginReplayWindow("variant-a"); err != nil {
			t.Fatal(err)
		}
		observe(t, model, source)
		setEnv(t)
		if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "exactly one replay-window marker") {
			t.Fatalf("BeginEvidenceFromEnv duplicate-marker error=%v", err)
		}
	})

	t.Run("marker-after-last-cut", func(t *testing.T) {
		model, source := newModel(t)
		observe(t, model, source)
		if err := model.BeginReplayWindow("variant-a"); err != nil {
			t.Fatal(err)
		}
		setEnv(t)
		if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "does not precede a matching cut") {
			t.Fatalf("BeginEvidenceFromEnv misordered-marker error=%v", err)
		}
	})

	t.Run("undeclared-marker", func(t *testing.T) {
		model, source := newModel(t)
		if err := model.BeginReplayWindow("variant-a"); err != nil {
			t.Fatal(err)
		}
		observe(t, model, source)
		setEnv(t)
		t.Setenv(EnvEvidenceReplayWindow, "")
		if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "without a declared replay window") {
			t.Fatalf("BeginEvidenceFromEnv undeclared-marker error=%v", err)
		}
	})
}

func TestBeginEvidenceFromEnvRejectsSymlinkedEvidenceRoot(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
		t.Fatal(err)
	}
	realRoot := filepath.Join(t.TempDir(), "real-evidence")
	if err := os.Mkdir(realRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	linkRoot := filepath.Join(t.TempDir(), "evidence")
	if err := os.Symlink(realRoot, linkRoot); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	t.Setenv(EnvEvidenceDir, linkRoot)
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadWrite)
	t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(EnvReplayVariant, "variant-a")
	t.Setenv(EnvReplaySeed, "1")

	if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("BeginEvidenceFromEnv symlinked root error=%v", err)
	}
}

func TestBeginEvidenceFromEnvRejectsSymlinkedEvidenceRootAncestor(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
		t.Fatal(err)
	}
	realParent := filepath.Join(t.TempDir(), "real-parent")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	linkParent := filepath.Join(t.TempDir(), "linked-parent")
	if err := os.Symlink(realParent, linkParent); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	t.Setenv(EnvEvidenceDir, filepath.Join(linkParent, "new-evidence"))
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(EnvEvidenceReopenMode, EvidenceReopenReadWrite)
	t.Setenv(EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(EnvReplayVariant, "variant-a")
	t.Setenv(EnvReplaySeed, "1")

	if _, err := BeginEvidenceFromEnv(model, false); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("BeginEvidenceFromEnv symlinked ancestor error=%v", err)
	}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}
