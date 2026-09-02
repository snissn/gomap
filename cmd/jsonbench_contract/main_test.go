package main

import (
	"bytes"
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
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(current), "..", "..", "internal", "jsonbenchcontract", "testdata", name)
}
