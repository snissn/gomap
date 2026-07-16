package powerlosscert

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadBundleIsStrictAndDeterministic(t *testing.T) {
	root := t.TempDir()
	writeBundleFixture(t, root, "b.json", testChildManifest("witness-b"))
	writeBundleFixture(t, root, "a.json", testChildManifest("witness-a"))

	bundle, err := LoadBundle(root)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := []string{bundle.Manifests[0].ManifestID, bundle.Manifests[1].ManifestID}, []string{"dur-01", "dur-02"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("manifest order=%v want=%v", got, want)
	}

	if err := os.WriteFile(filepath.Join(root, "risk_inventory.json"), []byte(`{"schema_version":"treedb-power-loss-risk-inventory/v1","unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBundle(root); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("LoadBundle strict error=%v", err)
	}
}

func TestVerifyArtifactsChecksContentAndRejectsEscapes(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	for index := range manifest.Witnesses[0].Artifacts {
		artifact := manifest.Witnesses[0].Artifacts[index]
		contents := string(artifact.Kind)
		if artifact.Kind == ArtifactKindOperationTrace {
			contents = testOperationTraceJSON(t, manifest.Witnesses[0], "after-meta-write")
		}
		manifest.Witnesses[0].Artifacts[index] = writeArtifactFixture(t, root, artifact.Kind, artifact.Path, contents)
	}
	if err := VerifyArtifacts(root, []ChildManifest{manifest}); err != nil {
		t.Fatal(err)
	}

	manifest.Witnesses[0].Artifacts[0].SHA256 = strings.Repeat("f", 64)
	if err := VerifyArtifacts(root, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("VerifyArtifacts digest error=%v", err)
	}

	manifest = testChildManifest("witness-a")
	manifest.TestBinaries[0].Path = "../escape"
	if err := VerifyArtifacts(root, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "unsafe path") {
		t.Fatalf("VerifyArtifacts escape error=%v", err)
	}
}

func TestVerifyArtifactsRejectsOperationTraceThatDoesNotMatchWitness(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	for index := range manifest.Witnesses[0].Artifacts {
		artifact := manifest.Witnesses[0].Artifacts[index]
		contents := string(artifact.Kind)
		if artifact.Kind == ArtifactKindOperationTrace {
			contents = testOperationTraceJSON(t, manifest.Witnesses[0], "after-index-sync")
		}
		manifest.Witnesses[0].Artifacts[index] = writeArtifactFixture(t, root, artifact.Kind, artifact.Path, contents)
	}

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "declared_cut_point") {
		t.Fatalf("VerifyArtifacts operation-trace mismatch error=%v", err)
	}
}

func TestVerifyArtifactsRejectsSymlinkOutsideBundle(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "binary")
	if err := os.WriteFile(outside, []byte("binary"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "bin", "TreeDB.test")
	if err := os.MkdirAll(filepath.Dir(link), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	manifest := testChildManifest("witness-a")
	digest := sha256.Sum256([]byte("binary"))
	manifest.TestBinaries[0].SHA256 = fmt.Sprintf("%x", digest)

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("VerifyArtifacts symlink error=%v", err)
	}
}

func writeBundleFixture(t *testing.T, root, name string, manifest ChildManifest) {
	t.Helper()
	if name == "a.json" {
		manifest.ManifestID = "dur-01"
		manifest.Issue = 3674
	} else {
		manifest.ManifestID = "dur-02"
		manifest.Issue = 3675
	}
	if err := os.MkdirAll(filepath.Join(root, "manifests"), 0o700); err != nil {
		t.Fatal(err)
	}
	inventoryData, err := json.Marshal(testRiskInventory())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "risk_inventory.json"), inventoryData, 0o600); err != nil {
		t.Fatal(err)
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifests", name), manifestData, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeArtifactFixture(t *testing.T, root string, kind ArtifactKind, path, contents string) Artifact {
	t.Helper()
	fullPath := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(contents))
	return Artifact{Kind: kind, Path: path, SHA256: fmt.Sprintf("%x", digest)}
}

func testOperationTraceJSON(t *testing.T, witness Witness, cutPoint string) string {
	t.Helper()
	events := make([]string, witness.ObservedEventCount)
	for index := range events {
		events[index] = "cut:" + cutPoint + ":meta:meta.db:0"
	}
	trace := map[string]any{
		"schema_version":       "treedb-power-loss-operation-trace/v1",
		"cut_id":               witness.CutID,
		"variant_id":           witness.Command.Env["TREEDB_POWERLOSS_VARIANT_ID"],
		"seed":                 fmt.Sprint(witness.Seed),
		"declared_cut_point":   cutPoint,
		"observed_event_count": witness.ObservedEventCount,
		"events":               events,
	}
	data, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
