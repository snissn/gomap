package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const debugOpenHelperEnv = "GO_WANT_DEBUG_OPEN_HELPER"

func TestDebugOpenMainHelper(t *testing.T) {
	if os.Getenv(debugOpenHelperEnv) != "1" {
		return
	}
	args := os.Args
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			os.Args = append([]string{"debug_open"}, args[i+1:]...)
			main()
			return
		}
	}
	os.Args = []string{"debug_open"}
	main()
}

func runDebugOpen(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmdArgs := []string{"-test.run=TestDebugOpenMainHelper", "--"}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), debugOpenHelperEnv+"=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestDebugOpenMissingDirFails(t *testing.T) {
	out, err := runDebugOpen(t)
	if err == nil {
		t.Fatalf("expected non-zero exit, output=%q", out)
	}
	if !strings.Contains(out, "Usage: debug_open <dir>") {
		t.Fatalf("expected usage error, got %q", out)
	}
}

func TestDebugOpenSuccessPath(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"debug_open", dbDir}
	main()
}
