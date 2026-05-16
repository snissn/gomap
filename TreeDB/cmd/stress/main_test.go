package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const stressHelperEnv = "GO_WANT_STRESS_HELPER"

func TestStressMainHelper(t *testing.T) {
	if os.Getenv(stressHelperEnv) != "1" {
		return
	}
	args := os.Args
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			os.Args = append([]string{"stress"}, args[i+1:]...)
			main()
			return
		}
	}
	os.Args = []string{"stress"}
	main()
}

func runStress(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmdArgs := []string{"-test.run=TestStressMainHelper", "--"}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), stressHelperEnv+"=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestStressZeroDurationSmoke(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	out, err := runStress(t,
		"-dir", dbDir,
		"-duration", "0s",
		"-workers", "1",
		"-keys", "1",
		"-ops", "0",
		"-valsize", "16",
		"-keeprecent", "1",
	)
	if err != nil {
		t.Fatalf("stress helper failed: %v output=%q", err, out)
	}
	if !strings.Contains(out, "--- Results ---") {
		t.Fatalf("expected results output, got %q", out)
	}
}
