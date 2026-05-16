package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const verifyHelperEnv = "GO_WANT_VERIFY_HELPER"

func TestVerifyMainHelper(t *testing.T) {
	if os.Getenv(verifyHelperEnv) != "1" {
		return
	}
	args := os.Args
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			os.Args = append([]string{"verify"}, args[i+1:]...)
			main()
			return
		}
	}
	os.Args = []string{"verify"}
	main()
}

func runVerify(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmdArgs := []string{"-test.run=TestVerifyMainHelper", "--"}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), verifyHelperEnv+"=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestVerifyMissingDirFails(t *testing.T) {
	out, err := runVerify(t)
	if err == nil {
		t.Fatalf("expected non-zero exit, output=%q", out)
	}
	if !strings.Contains(out, "Please provide -dir") {
		t.Fatalf("expected missing dir message, got %q", out)
	}
}

func TestVerifyOnFreshDirSucceeds(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	out, err := runVerify(t, "-dir", dbDir, "-report=false", "-vacuum-index=false")
	if err != nil {
		t.Fatalf("verify helper failed: %v output=%q", err, out)
	}
	if !strings.Contains(out, "Verification successful. Items:") {
		t.Fatalf("expected success output, got %q", out)
	}
}
