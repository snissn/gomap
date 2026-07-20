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

func TestLoadBundleRejectsSymlinkedMetadataOutsideBundle(t *testing.T) {
	for _, name := range []string{"risk inventory", "child manifest"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writeBundleFixture(t, root, "a.json", testChildManifest("witness-a"))
			path := filepath.Join(root, "risk_inventory.json")
			if name == "child manifest" {
				path = filepath.Join(root, "manifests", "a.json")
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(t.TempDir(), filepath.Base(path))
			if err := os.WriteFile(outside, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, path); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}

			if _, err := LoadBundle(root); err == nil || !strings.Contains(err.Error(), "symlink") {
				t.Fatalf("LoadBundle %s symlink error=%v", name, err)
			}
		})
	}
}

func TestLoadBundleRejectsManifestParentSymlinkOutsideBundle(t *testing.T) {
	root := t.TempDir()
	writeBundleFixture(t, root, "a.json", testChildManifest("witness-a"))
	manifestData, err := os.ReadFile(filepath.Join(root, "manifests", "a.json"))
	if err != nil {
		t.Fatal(err)
	}
	outsideManifests := filepath.Join(t.TempDir(), "manifests")
	if err := os.MkdirAll(outsideManifests, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outsideManifests, "a.json"), manifestData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "manifests")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideManifests, filepath.Join(root, "manifests")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	if _, err := LoadBundle(root); err == nil || !strings.Contains(err.Error(), "outside the bundle") {
		t.Fatalf("LoadBundle parent symlink error=%v", err)
	}
}

func TestVerifyArtifactsChecksContentAndRejectsEscapes(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
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

func TestVerifyArtifactsAcceptsFrozenChildRecoveryDirectory(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("fresh-layout")
	manifest.Witnesses[0].ExpectedRecoveryDir = "recovery-input/db"
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
	if err := VerifyArtifacts(root, []ChildManifest{manifest}); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyArtifactsRejectsRecoveryDirectorySubstitution(t *testing.T) {
	for _, dir := range []string{"", "recovery-input", "../db", "/recovery-input/db", `recovery-input\db`} {
		t.Run(strings.NewReplacer("/", "-", `\`, "-").Replace(dir), func(t *testing.T) {
			root := t.TempDir()
			manifest := testChildManifest("fresh-layout")
			manifest.Witnesses[0].ExpectedRecoveryDir = "recovery-input/db"
			for index := range manifest.TestBinaries {
				manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
			}
			writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
			rewriteArtifactJSONField(t, root, &manifest.Witnesses[0], ArtifactKindRecoveryTrace, "dir", dir)
			if err := VerifyArtifacts(root, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "expected recovery directory") {
				t.Fatalf("VerifyArtifacts dir=%q error=%v", dir, err)
			}
		})
	}
}

func TestVerifyArtifactsRejectsOperationTraceThatDoesNotMatchWitness(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-index-sync")

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "declared_cut_point") {
		t.Fatalf("VerifyArtifacts operation-trace mismatch error=%v", err)
	}
}

func TestVerifyArtifactsBindsRecoveryTraceToReopenMode(t *testing.T) {
	for _, mode := range []string{powerLossReopenModeReadOnly, powerLossReopenModeReadWrite} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			manifest := testChildManifest("witness-a")
			manifest.Witnesses[0].Command.Env[powerLossReopenModeEnv] = mode
			for index := range manifest.TestBinaries {
				manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
			}
			writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
			if err := VerifyArtifacts(root, []ChildManifest{manifest}); err != nil {
				t.Fatalf("VerifyArtifacts valid reopen mode: %v", err)
			}

			witness := &manifest.Witnesses[0]
			rewriteArtifactJSONField(t, root, witness, ArtifactKindRecoveryTrace, "read_only", mode != powerLossReopenModeReadOnly)
			if err := VerifyArtifacts(root, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "does not match command reopen mode") {
				t.Fatalf("VerifyArtifacts mismatched reopen mode error=%v", err)
			}
		})
	}
}

func TestVerifyArtifactsBindsRecoveryTraceToCommandProfile(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	witness := &manifest.Witnesses[0]
	witness.Profile = "no_wal_fast"
	witness.Command.Env[powerLossProfileEnv] = witness.Profile
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "does not match command-required profile") {
		t.Fatalf("VerifyArtifacts mismatched recovery profile error=%v", err)
	}
}

func TestVerifyArtifactsRequiresTraceToEndAtDeclaredCutOccurrence(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	witness := &manifest.Witnesses[0]
	witness.CutID = "cut/checkpoint-generation-2/after-meta-write/000"
	witness.CutOccurrence = 0
	witness.Command.Env["TREEDB_POWERLOSS_CUT_ID"] = witness.CutID
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "does not end at declared occurrence") {
		t.Fatalf("VerifyArtifacts extra matching cut event error=%v", err)
	}
}

func TestReplayWindowCutCountRetainsPrefixButScopesAddress(t *testing.T) {
	events := []string{
		"cut:after-meta-write:meta",
		"cut:after-meta-write:meta",
		"replay-window:variant-a",
		"cut:after-meta-write:meta",
	}
	if got, err := replayWindowCutCount(events, "after-meta-write", "variant-a"); err != nil || got != 1 {
		t.Fatalf("windowed matching events=%d error=%v want 1", got, err)
	}
	for _, test := range []struct {
		name   string
		events []string
		window string
		want   string
	}{
		{name: "missing", events: []string{"cut:after-meta-write:meta"}, window: "variant-a", want: "exactly one"},
		{name: "duplicate", events: []string{"replay-window:variant-a", "replay-window:variant-a", "cut:after-meta-write:meta"}, window: "variant-a", want: "exactly one"},
		{name: "misordered", events: []string{"cut:after-meta-write:meta", "replay-window:variant-a"}, window: "variant-a", want: "does not precede"},
		{name: "wrong-marker", events: []string{"replay-window:variant-b", "cut:after-meta-write:meta"}, window: "variant-a", want: "does not match"},
		{name: "undeclared", events: events, want: "without a declared"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := replayWindowCutCount(test.events, "after-meta-write", test.window); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("replayWindowCutCount error=%v want substring %q", err, test.want)
			}
		})
	}
}

func TestVerifyArtifactsRejectsModeledEvidenceReuseAcrossWitnesses(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
	reused := manifest.Witnesses[0]
	reused.ID = "witness-b"
	manifest.Witnesses = append(manifest.Witnesses, reused)

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "reuses modeled evidence directory") {
		t.Fatalf("VerifyArtifacts modeled evidence reuse error=%v", err)
	}
}

func TestVerifyArtifactsRejectsMalformedOrUnboundModeledArtifacts(t *testing.T) {
	tests := []struct {
		name  string
		kind  ArtifactKind
		field string
		value any
		want  string
	}{
		{name: "stable tree schema", kind: ArtifactKindStableImageTree, field: "schema_version", value: "wrong/v1", want: "stable image tree"},
		{name: "operation trace schema", kind: ArtifactKindOperationTrace, field: "schema_version", value: "treedb-power-loss-operation-trace/v1", want: "operation trace"},
		{name: "stable tree directory escape", kind: ArtifactKindStableImageTree, field: "directories", value: []string{"../escape"}, want: "unsafe or non-canonical"},
		{name: "dirty tree totals", kind: ArtifactKindDirtyImageTree, field: "total_bytes", value: 999, want: "dirty image tree"},
		{name: "metrics cross reference", kind: ArtifactKindMetrics, field: "trace_events", value: 999, want: "does not match"},
		{name: "recovery input binding", kind: ArtifactKindRecoveryTrace, field: "input_image_tree_sha256", value: strings.Repeat("0", 64), want: "does not identify"},
		{name: "command completion", kind: ArtifactKindLog, field: "completed", value: false, want: "completed successful"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			manifest := testChildManifest("witness-a")
			for index := range manifest.TestBinaries {
				manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
			}
			writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
			rewriteArtifactJSONField(t, root, &manifest.Witnesses[0], test.kind, test.field, test.value)

			err := VerifyArtifacts(root, []ChildManifest{manifest})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("VerifyArtifacts error=%v want substring %q", err, test.want)
			}
		})
	}
}

func TestVerifyArtifactsRejectsInvalidDeclaredReplayWindow(t *testing.T) {
	for _, test := range []struct {
		name   string
		events []string
		want   string
	}{
		{name: "missing-marker", events: []string{"cut:after-meta-write:meta:meta.db:0"}, want: "exactly one replay-window marker"},
		{name: "duplicate-marker", events: []string{"replay-window:variant-a", "replay-window:variant-a", "cut:after-meta-write:meta:meta.db:0"}, want: "exactly one replay-window marker"},
		{name: "marker-after-last-cut", events: []string{"cut:after-meta-write:meta:meta.db:0", "replay-window:variant-a"}, want: "does not precede a matching cut"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			manifest := testChildManifest("witness-a")
			witness := &manifest.Witnesses[0]
			witness.ReplayWindow = "variant-a"
			witness.Command.Env[powerLossReplayWindowEnv] = witness.ReplayWindow
			witness.CutID = "cut/checkpoint-generation-2/after-meta-write/000"
			witness.CutOccurrence = 0
			witness.ObservedEventCount = 1
			witness.Command.Env["TREEDB_POWERLOSS_CUT_ID"] = witness.CutID
			for index := range manifest.TestBinaries {
				manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
			}
			writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
			rewriteArtifactJSONField(t, root, witness, ArtifactKindOperationTrace, "events", test.events)

			if err := VerifyArtifacts(root, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("VerifyArtifacts error=%v want substring %q", err, test.want)
			}
		})
	}

	t.Run("trace-variant-mismatch", func(t *testing.T) {
		root := t.TempDir()
		manifest := testChildManifest("witness-a")
		witness := &manifest.Witnesses[0]
		witness.ReplayWindow = "variant-a"
		witness.Command.Env[powerLossReplayWindowEnv] = witness.ReplayWindow
		witness.CutID = "cut/checkpoint-generation-2/after-meta-write/000"
		witness.CutOccurrence = 0
		witness.ObservedEventCount = 1
		witness.Command.Env["TREEDB_POWERLOSS_CUT_ID"] = witness.CutID
		for index := range manifest.TestBinaries {
			manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
		}
		writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
		rewriteArtifactJSONField(t, root, witness, ArtifactKindOperationTrace, "variant_id", "variant-b")

		if err := VerifyArtifacts(root, []ChildManifest{manifest}); err == nil || !strings.Contains(err.Error(), "does not match variant_id") {
			t.Fatalf("VerifyArtifacts trace-variant mismatch error=%v", err)
		}
	})
}

func TestVerifyArtifactsRejectsImageBytesThatDoNotMatchTree(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
	if err := os.WriteFile(filepath.Join(root, "artifacts", "witness-a", "stable-image", "index.db"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "contents do not match") {
		t.Fatalf("VerifyArtifacts image mismatch error=%v", err)
	}
}

func TestVerifyArtifactsRejectsImageDirectoryMismatch(t *testing.T) {
	for _, imageDir := range []string{"stable-image", "recovery-input"} {
		t.Run(imageDir, func(t *testing.T) {
			root := t.TempDir()
			manifest := testChildManifest("witness-a")
			for index := range manifest.TestBinaries {
				manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
			}
			writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
			unexpected := filepath.Join(root, "artifacts", "witness-a", imageDir, "empty-wal")
			if err := os.MkdirAll(unexpected, 0o700); err != nil {
				t.Fatal(err)
			}

			err := VerifyArtifacts(root, []ChildManifest{manifest})
			if err == nil || !strings.Contains(err.Error(), "contents do not match") {
				t.Fatalf("VerifyArtifacts image directory mismatch error=%v", err)
			}
		})
	}
}

func TestVerifyArtifactsRejectsAcceptedOutcomeForRejectedOpen(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
	manifest.Witnesses[0].TypedError = "*errors.errorString"
	rewriteArtifactJSONFields(t, root, &manifest.Witnesses[0], ArtifactKindRecoveryTrace, map[string]any{
		"rejected":   true,
		"error_type": "*errors.errorString",
		"error":      "rejected modeled image",
	})

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "outcome") {
		t.Fatalf("VerifyArtifacts rejected-open outcome error=%v", err)
	}
}

func TestVerifyArtifactsRejectsScalarSelectedStateForRejectedOpen(t *testing.T) {
	for name, scalars := range map[string]map[string]any{
		"commit sequence": {"commit_seq": 1, "applied_lsn": 0},
		"applied LSN":     {"commit_seq": 0, "applied_lsn": 1},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			manifest := testChildManifest("witness-a")
			for index := range manifest.TestBinaries {
				manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
			}
			writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
			witness := &manifest.Witnesses[0]
			witness.ActualOutcome = "rejected:modeled-image"
			witness.TypedError = "*errors.errorString"
			witness.State = observedWitnessState(recoveryTraceArtifact{Rejected: true})
			fields := map[string]any{
				"rejected":   true,
				"error_type": witness.TypedError,
				"error":      "rejected modeled image",
				"stats":      map[string]string{},
			}
			for field, value := range scalars {
				fields[field] = value
			}
			rewriteArtifactJSONFields(t, root, witness, ArtifactKindRecoveryTrace, fields)

			err := VerifyArtifacts(root, []ChildManifest{manifest})
			if err == nil || !strings.Contains(err.Error(), "scalar selected state") {
				t.Fatalf("VerifyArtifacts rejected scalar state error=%v", err)
			}
		})
	}
}

func TestVerifyArtifactsRejectsTamperedWritableRecoveryPreOpenSnapshot(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	manifest.Witnesses[0].Command.Env[powerLossReopenModeEnv] = powerLossReopenModeReadWrite
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
	snapshot := filepath.Join(root, "artifacts", "witness-a", "recovery-preopen", "index.db")
	if err := os.WriteFile(snapshot, []byte("not the modeled input"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "recovery-preopen") {
		t.Fatalf("VerifyArtifacts writable recovery pre-open binding error=%v", err)
	}
}

func TestVerifyArtifactsRejectsUnstructuredModeledEvidence(t *testing.T) {
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

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "stable image tree") {
		t.Fatalf("VerifyArtifacts unstructured modeled evidence error=%v", err)
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

func TestVerifyArtifactsRejectsSymlinkedEvidenceDirectoryComponent(t *testing.T) {
	root := t.TempDir()
	manifest := testChildManifest("witness-a")
	for index := range manifest.TestBinaries {
		manifest.TestBinaries[index] = writeArtifactFixture(t, root, manifest.TestBinaries[index].Kind, manifest.TestBinaries[index].Path, "binary")
	}
	writeModeledEvidenceFixture(t, root, &manifest, "after-meta-write")
	artifactsDir := filepath.Join(root, "artifacts")
	realArtifactsDir := filepath.Join(root, "real-artifacts")
	if err := os.Rename(artifactsDir, realArtifactsDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realArtifactsDir, artifactsDir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	err := VerifyArtifacts(root, []ChildManifest{manifest})
	if err == nil || !strings.Contains(err.Error(), "symlinked evidence directory") {
		t.Fatalf("VerifyArtifacts evidence-directory symlink error=%v", err)
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

func writeModeledEvidenceFixture(t *testing.T, root string, manifest *ChildManifest, cutPoint string) {
	t.Helper()
	witness := &manifest.Witnesses[0]
	evidenceDir := filepath.Join(root, filepath.FromSlash(witness.Command.Env["TREEDB_POWERLOSS_EVIDENCE_DIR"]))
	recoveryDir, err := normalizeRecoveryDir(witness.ExpectedRecoveryDir)
	if err != nil {
		t.Fatal(err)
	}
	imageFile := "index.db"
	var imageDirectories []string
	if child := strings.TrimPrefix(recoveryDir, defaultRecoveryDir+"/"); child != recoveryDir {
		imageFile = filepath.Join(filepath.FromSlash(child), "index.db")
		parts := strings.Split(child, "/")
		for index := range parts {
			imageDirectories = append(imageDirectories, strings.Join(parts[:index+1], "/"))
		}
	}
	stableContents := []byte("stable")
	dirtyContents := []byte("dirty")
	for _, image := range []struct {
		dir      string
		contents []byte
	}{
		{dir: "stable-image", contents: stableContents},
		{dir: "dirty-image", contents: dirtyContents},
		{dir: "recovery-preopen", contents: stableContents},
		{dir: "recovery-input", contents: stableContents},
	} {
		if err := os.MkdirAll(filepath.Join(evidenceDir, image.dir), 0o700); err != nil {
			t.Fatal(err)
		}
		imagePath := filepath.Join(evidenceDir, image.dir, imageFile)
		if err := os.MkdirAll(filepath.Dir(imagePath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(imagePath, image.contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	stableDigest := sha256.Sum256(stableContents)
	dirtyDigest := sha256.Sum256(dirtyContents)
	stableTree := imageTreeArtifact{
		SchemaVersion: imageTreeSchemaVersion,
		Kind:          "stable-image",
		Directories:   imageDirectories,
		Files:         []imageTreeFileArtifact{{Path: filepath.ToSlash(imageFile), Bytes: int64(len(stableContents)), SHA256: fmt.Sprintf("%x", stableDigest)}},
		TotalBytes:    int64(len(stableContents)),
	}
	dirtyTree := imageTreeArtifact{
		SchemaVersion: imageTreeSchemaVersion,
		Kind:          "dirty-image",
		Directories:   imageDirectories,
		Files:         []imageTreeFileArtifact{{Path: filepath.ToSlash(imageFile), Bytes: int64(len(dirtyContents)), SHA256: fmt.Sprintf("%x", dirtyDigest)}},
		TotalBytes:    int64(len(dirtyContents)),
	}
	stableFingerprint := strings.Repeat("a", 64)
	contents := map[ArtifactKind]string{
		ArtifactKindOperationTrace:  testOperationTraceJSON(t, *witness, cutPoint),
		ArtifactKindStableImageTree: mustJSON(t, stableTree),
		ArtifactKindDirtyImageTree:  mustJSON(t, dirtyTree),
		ArtifactKindMetrics: mustJSON(t, metricsArtifact{
			SchemaVersion:     metricsSchemaVersion,
			StableImageBytes:  stableTree.TotalBytes,
			DirtyImageBytes:   dirtyTree.TotalBytes,
			StableFiles:       len(stableTree.Files),
			DirtyFiles:        len(dirtyTree.Files),
			TraceEvents:       witness.ObservedEventCount,
			StableFingerprint: stableFingerprint,
		}),
	}
	for index := range witness.Artifacts {
		artifact := witness.Artifacts[index]
		if content, ok := contents[artifact.Kind]; ok {
			witness.Artifacts[index] = writeArtifactFixture(t, root, artifact.Kind, artifact.Path, content)
		}
	}
	stableArtifact := artifactByKind(t, *witness, ArtifactKindStableImageTree)
	readOnly := witness.Command.Env[powerLossReopenModeEnv] == powerLossReopenModeReadOnly
	recovery := recoveryTraceArtifact{
		SchemaVersion:      recoveryTraceSchemaVersion,
		PublicAPI:          "treedb.Open",
		Dir:                recoveryDir,
		PreOpenSnapshotDir: "recovery-preopen",
		InputTreeSHA256:    stableArtifact.SHA256,
		StableFingerprint:  stableFingerprint,
		ReadOnly:           readOnly,
		Stats: map[string]string{
			"treedb.profile.resolved":                 "command_wal_durable",
			"treedb.commit_seq":                       "0",
			"treedb.applied_command_lsn":              "0",
			"treedb.durable_root.selected_slot":       "1",
			"treedb.durable_root.commit_seq":          "2",
			"treedb.durable_root.durable_seq":         "2",
			"treedb.durable_root.freelist.generation": "2",
			"treedb.durable_root.manifest.entries":    "1",
			"treedb.durable_root.slot0.commit_seq":    "1",
			"treedb.durable_root.slot1.commit_seq":    "2",
			"treedb.command_wal.durable_wal_lsn":      "0",
		},
	}
	witness.State = observedWitnessState(recovery)
	contents[ArtifactKindRecoveryTrace] = mustJSON(t, recovery)
	contents[ArtifactKindLog] = mustJSON(t, commandLogArtifact{
		SchemaVersion: commandLogSchemaVersion,
		RepositorySHA: manifest.RepositorySHA,
		BinaryPath:    witness.Command.BinaryPath,
		BinarySHA256:  manifest.TestBinaries[0].SHA256,
		Package:       witness.Command.Package,
		TestName:      witness.Command.TestName,
		Args:          witness.Command.Args,
		Env:           witness.Command.Env,
		Outcome:       witness.ActualOutcome,
		Completed:     true,
		ExitCode:      0,
		Stdout:        "PASS\n",
	})
	for index := range witness.Artifacts {
		artifact := witness.Artifacts[index]
		if content, ok := contents[artifact.Kind]; ok && (artifact.Kind == ArtifactKindRecoveryTrace || artifact.Kind == ArtifactKindLog) {
			witness.Artifacts[index] = writeArtifactFixture(t, root, artifact.Kind, artifact.Path, content)
		}
	}
}

func artifactByKind(t *testing.T, witness Witness, kind ArtifactKind) Artifact {
	t.Helper()
	for _, artifact := range witness.Artifacts {
		if artifact.Kind == kind {
			return artifact
		}
	}
	t.Fatalf("missing artifact kind %q", kind)
	return Artifact{}
}

func rewriteArtifactJSONField(t *testing.T, root string, witness *Witness, kind ArtifactKind, field string, value any) {
	t.Helper()
	rewriteArtifactJSONFields(t, root, witness, kind, map[string]any{field: value})
}

func rewriteArtifactJSONFields(t *testing.T, root string, witness *Witness, kind ArtifactKind, fields map[string]any) {
	t.Helper()
	for index, artifact := range witness.Artifacts {
		if artifact.Kind != kind {
			continue
		}
		path := filepath.Join(root, filepath.FromSlash(artifact.Path))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := json.Unmarshal(data, &document); err != nil {
			t.Fatal(err)
		}
		for field, value := range fields {
			document[field] = value
		}
		witness.Artifacts[index] = writeArtifactFixture(t, root, kind, artifact.Path, mustJSON(t, document))
		return
	}
	t.Fatalf("missing artifact kind %q", kind)
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func testOperationTraceJSON(t *testing.T, witness Witness, cutPoint string) string {
	t.Helper()
	events := make([]string, 0, witness.ObservedEventCount+1)
	if witness.ReplayWindow != "" {
		events = append(events, "replay-window:"+witness.ReplayWindow)
	}
	for range witness.ObservedEventCount {
		events = append(events, "cut:"+cutPoint+":meta:meta.db:0")
	}
	trace := map[string]any{
		"schema_version":       operationTraceSchemaVersion,
		"cut_id":               witness.CutID,
		"variant_id":           witness.Command.Env["TREEDB_POWERLOSS_VARIANT_ID"],
		"seed":                 fmt.Sprint(witness.Seed),
		"declared_cut_point":   cutPoint,
		"observed_event_count": witness.ObservedEventCount,
		"events":               events,
	}
	if witness.ReplayWindow != "" {
		trace["replay_window"] = witness.ReplayWindow
	}
	data, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
