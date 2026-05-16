package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

const unifiedBenchHelperEnv = "GO_WANT_UNIFIED_BENCH_HELPER"

func TestUnifiedBenchMainHelper(t *testing.T) {
	if os.Getenv(unifiedBenchHelperEnv) != "1" {
		return
	}
	args := os.Args
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			os.Args = append([]string{"unified_bench"}, args[i+1:]...)
			main()
			return
		}
	}
	os.Args = []string{"unified_bench"}
	main()
}

func runUnifiedBenchMain(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmdArgs := []string{"-test.run=TestUnifiedBenchMainHelper", "--"}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), unifiedBenchHelperEnv+"=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestUnifiedBenchUnknownSuiteFails(t *testing.T) {
	out, err := runUnifiedBenchMain(t, "-suite", "does_not_exist")
	if err == nil {
		t.Fatalf("expected non-zero exit, output=%q", out)
	}
	if !strings.Contains(out, "unknown suite") {
		t.Fatalf("expected unknown suite error, got %q", out)
	}
}

func TestUnifiedBenchUnknownCaseFails(t *testing.T) {
	out, err := runUnifiedBenchMain(t, "-suite", "vlog_autotune", "-case", "does_not_exist")
	if err == nil {
		t.Fatalf("expected non-zero exit, output=%q", out)
	}
	if !strings.Contains(out, "unknown case") {
		t.Fatalf("expected unknown case error, got %q", out)
	}
}
