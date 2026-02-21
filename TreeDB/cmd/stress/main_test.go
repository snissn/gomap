package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func captureStressStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()
	fn()
	_ = w.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	return buf.String()
}

func TestStressZeroDurationSmoke(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	oldDir, oldDuration := *dir, *duration
	oldWorkers, oldKeys, oldOps := *workers, *keyRange, *opsCount
	oldValSize, oldKeepRecent := *valSize, *keepRecent
	oldCPU, oldMem := *cpuprofile, *memprofile
	t.Cleanup(func() {
		*dir = oldDir
		*duration = oldDuration
		*workers = oldWorkers
		*keyRange = oldKeys
		*opsCount = oldOps
		*valSize = oldValSize
		*keepRecent = oldKeepRecent
		*cpuprofile = oldCPU
		*memprofile = oldMem
	})

	*dir = dbDir
	*duration = 0 * time.Second
	*workers = 1
	*keyRange = 1
	*opsCount = 0
	*valSize = 16
	*keepRecent = 1
	*cpuprofile = ""
	*memprofile = ""

	out := captureStressStdout(t, func() { main() })
	if !strings.Contains(out, "--- Results ---") {
		t.Fatalf("expected results output, got %q", out)
	}
}
