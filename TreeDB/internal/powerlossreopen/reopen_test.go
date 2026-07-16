package powerlossreopen_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

func TestStableAtPreservesCallerOwnedCrashImage(t *testing.T) {
	source := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, source)
	opts.DisableBackgroundPrune = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("stable"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "preserved")
	result, reopened, closeFn, err := powerlossreopen.StableAt(destination, model, opts, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.Dir != destination || result.Rejected || reopened == nil {
		t.Fatalf("StableAt result=%+v reopened=%v", result, reopened)
	}
	got, err := reopened.Get([]byte("stable"))
	if err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("Get stable=%q err=%v", got, err)
	}
	if err := closeFn(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(destination); err != nil {
		t.Fatalf("caller-owned image removed: %v", err)
	}
}

func TestStableCapturesEvidenceWhenRequested(t *testing.T) {
	source := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, source)
	opts.DisableBackgroundPrune = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("stable"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(source, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta}); err != nil {
		t.Fatal(err)
	}
	evidenceDir := filepath.Join(t.TempDir(), "evidence")
	t.Setenv(powerlossoracle.EnvEvidenceDir, evidenceDir)
	t.Setenv(powerlossoracle.EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	result, reopened, closeFn, err := powerlossreopen.Stable(model, opts, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.Dir != filepath.Join(evidenceDir, "recovery-input") || result.Rejected || reopened == nil {
		t.Fatalf("Stable evidence result=%+v reopened=%v", result, reopened)
	}
	if err := reopened.SetSync([]byte("recovery/mutation"), []byte("must-not-change-evidence")); err != nil {
		t.Fatal(err)
	}
	if err := closeFn(); err != nil {
		t.Fatal(err)
	}
	recoveryData, err := os.ReadFile(filepath.Join(evidenceDir, "recovery_trace.json"))
	if err != nil {
		t.Fatalf("missing recovery evidence: %v", err)
	}
	stableTreeData, err := os.ReadFile(filepath.Join(evidenceDir, "stable_image_tree.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recovery struct {
		PreOpenSnapshotDir string `json:"pre_open_snapshot_dir"`
		InputTreeSHA256    string `json:"input_image_tree_sha256"`
		StableFingerprint  string `json:"stable_fingerprint"`
	}
	if err := json.Unmarshal(recoveryData, &recovery); err != nil {
		t.Fatal(err)
	}
	stableTreeDigest := sha256.Sum256(stableTreeData)
	if recovery.PreOpenSnapshotDir != "recovery-preopen" || recovery.InputTreeSHA256 != fmt.Sprintf("%x", stableTreeDigest) || len(recovery.StableFingerprint) != 64 {
		t.Fatalf("recovery evidence binding=%+v", recovery)
	}
	if _, err := os.Stat(filepath.Join(evidenceDir, "stable-image")); err != nil {
		t.Fatalf("missing immutable stable image: %v", err)
	}
	if _, err := os.Stat(filepath.Join(evidenceDir, "recovery-preopen", "recovery", "mutation")); !os.IsNotExist(err) {
		t.Fatalf("pre-open recovery snapshot was mutated: %v", err)
	}
}
