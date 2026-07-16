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
	evidenceDir := filepath.Join(t.TempDir(), "evidence")
	t.Setenv(EnvEvidenceDir, evidenceDir)
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))

	session, err := BeginEvidenceFromEnv(model)
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
	for _, image := range []string{"stable-image", "dirty-image", "recovery-input"} {
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
	t.Setenv(EnvEvidenceDir, filepath.Join(t.TempDir(), "evidence"))
	t.Setenv(EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	if _, err := BeginEvidenceFromEnv(model); err == nil || !strings.Contains(err.Error(), "was not observed") {
		t.Fatalf("BeginEvidenceFromEnv error=%v", err)
	}
}
