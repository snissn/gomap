package main

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRunValidatesCommittedSmokeManifest(t *testing.T) {
	var stdout, stderr bytes.Buffer
	path := smokeFixturePath(t, "canonical_manifest.json")
	if err := run([]string{"-manifest", path}, &stdout, &stderr); err != nil {
		t.Fatalf("run() error = %v, stderr = %s", err, stderr.String())
	}
	for _, want := range []string{`"valid":true`, `"independent_attempts":5`, `"requested_profile":"durable"`} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("output %q does not contain %q", stdout.String(), want)
		}
	}
}

func TestRunRejectsMissingManifestFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(nil, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "-manifest is required") {
		t.Fatalf("run() error = %v", err)
	}
}

func smokeFixturePath(t *testing.T, name string) string {
	t.Helper()
	var pkgDir string
	if _, current, _, ok := runtime.Caller(0); ok && filepath.IsAbs(current) {
		if _, err := os.Stat(current); err == nil {
			pkgDir = filepath.Dir(current)
		}
	}
	if pkgDir == "" {
		wd, err := os.Getwd()
		if err != nil {
			t.Fatalf("getwd: %v", err)
		}
		pkgDir = wd
	}
	return filepath.Join(pkgDir, "..", "..", "internal", "jsonbenchcontract", "testdata", name)
}
