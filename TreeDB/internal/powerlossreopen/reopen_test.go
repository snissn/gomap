package powerlossreopen_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	opts.ValueLog.PointerThreshold = 1
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
	destination := filepath.Join(canonicalTempDir(t), "preserved")
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
	opts.ValueLog.PointerThreshold = 1
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
	evidenceDir := filepath.Join(canonicalTempDir(t), "evidence")
	t.Setenv(powerlossoracle.EnvEvidenceDir, evidenceDir)
	t.Setenv(powerlossoracle.EnvEvidenceCutPoint, string(durabilitycut.AfterMetaWrite))
	t.Setenv(powerlossoracle.EnvEvidenceReopenMode, powerlossoracle.EvidenceReopenReadWrite)
	t.Setenv(powerlossoracle.EnvReplayCut, "cut/checkpoint-generation-2/after-meta-write/000")
	t.Setenv(powerlossoracle.EnvReplayVariant, "variant-a")
	t.Setenv(powerlossoracle.EnvReplaySeed, "1")
	result, reopened, closeFn, err := powerlossreopen.Stable(model, opts, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.Dir != filepath.Join(evidenceDir, "recovery-input") || result.Rejected || reopened == nil {
		t.Fatalf("Stable evidence result=%+v reopened=%v", result, reopened)
	}
	got, err := reopened.Get([]byte("stable"))
	if err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("Get stable=%q err=%v", got, err)
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
	if _, _, _, err := powerlossreopen.Stable(model, opts, false); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Fatalf("second Stable reused evidence root: %v", err)
	}
}

func TestStableAtRejectsSymlinkedDestination(t *testing.T) {
	source := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, source)
	opts.DisableBackgroundPrune = true
	opts.ValueLog.PointerThreshold = 1
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
	realDestination := filepath.Join(t.TempDir(), "real-destination")
	if err := os.Mkdir(realDestination, 0o700); err != nil {
		t.Fatal(err)
	}
	linkDestination := filepath.Join(t.TempDir(), "destination")
	if err := os.Symlink(realDestination, linkDestination); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	result, reopened, closeFn, err := powerlossreopen.StableAt(linkDestination, model, opts, true)
	if closeFn != nil {
		_ = closeFn()
	}
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("StableAt symlinked destination result=%+v reopened=%v error=%v", result, reopened, err)
	}
}

func TestStableAtRejectsSymlinkedDestinationAncestor(t *testing.T) {
	source := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, source)
	opts.DisableBackgroundPrune = true
	opts.ValueLog.PointerThreshold = 1
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
	realParent := filepath.Join(t.TempDir(), "real-parent")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	linkParent := filepath.Join(t.TempDir(), "linked-parent")
	if err := os.Symlink(realParent, linkParent); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	destination := filepath.Join(linkParent, "new-destination")

	result, reopened, closeFn, err := powerlossreopen.StableAt(destination, model, opts, true)
	if closeFn != nil {
		_ = closeFn()
	}
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("StableAt symlinked destination ancestor result=%+v reopened=%v error=%v", result, reopened, err)
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
