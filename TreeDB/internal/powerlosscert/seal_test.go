package powerlosscert

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBundleSealBindsEveryRetainedFile(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "evidence", "case-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(root, "evidence", "case-a", "trace.json")
	if err := os.WriteFile(artifact, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := WriteBundleSeal(root, testRepositorySHA)
	if err != nil {
		t.Fatal(err)
	}
	if !validHex(digest, 64) {
		t.Fatalf("seal digest=%q", digest)
	}
	if err := VerifyBundleSeal(root, testRepositorySHA); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact, []byte("after"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyBundleSeal(root, testRepositorySHA); err == nil || !strings.Contains(err.Error(), "do not match") {
		t.Fatalf("tampered bundle error=%v", err)
	}
}

func TestBundleSealRejectsUnlistedExtraFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "summary.md"), []byte("summary"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteBundleSeal(root, testRepositorySHA); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "extra.txt"), []byte("extra"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyBundleSeal(root, testRepositorySHA); err == nil || !strings.Contains(err.Error(), "do not match") {
		t.Fatalf("extra file error=%v", err)
	}
}
