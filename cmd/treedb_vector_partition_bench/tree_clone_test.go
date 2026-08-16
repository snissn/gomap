package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVectorPartitionCloneTreeCommandV1(t *testing.T) {
	if os.Getenv("GOOS") != "" {
		t.Skip("test requires the native cp implementation")
	}
	source := t.TempDir()
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "asset"), []byte("stable"), 0o640); err != nil {
		t.Fatal(err)
	}
	output, err := vectorPartitionCloneTreeCommandV1(source, target).CombinedOutput()
	if err != nil {
		t.Fatalf("clone tree: %v: %s", err, output)
	}
	raw, err := os.ReadFile(filepath.Join(target, "asset"))
	if err != nil || string(raw) != "stable" {
		t.Fatalf("cloned asset=%q err=%v", raw, err)
	}
}
