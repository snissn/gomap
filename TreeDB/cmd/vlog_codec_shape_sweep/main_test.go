package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSweepInput(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.jsonl")
	data := strings.Join(lines, "\n")
	if !strings.HasSuffix(data, "\n") {
		data += "\n"
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
	return path
}

func TestResolveInputEncoding_AutoDefaultsToStringWhenEncodingMissing(t *testing.T) {
	rec := kvRecord{Val: "Zm9v"}
	if got := resolveInputEncoding("auto", rec); got != "string" {
		t.Fatalf("resolveInputEncoding(auto)=%q want=string", got)
	}
}

func TestLoadValues_InvalidJSONReturnsError(t *testing.T) {
	path := writeSweepInput(t,
		`{"val":"ok","encoding":"string"}`,
		`not-json`,
	)
	_, _, err := loadValues(path, 2, 0, 0, "auto")
	if err == nil {
		t.Fatalf("expected invalid-json error")
	}
	if !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("expected line number in error, got: %v", err)
	}
}

func TestLoadValues_AutoUsesExplicitRecordEncoding(t *testing.T) {
	path := writeSweepInput(t, `{"val":"Zm9v","encoding":"base64"}`)
	train, eval, err := loadValues(path, 1, 0, 0, "auto")
	if err != nil {
		t.Fatalf("loadValues: %v", err)
	}
	if len(eval) != 0 {
		t.Fatalf("unexpected eval values: %d", len(eval))
	}
	if len(train) != 1 {
		t.Fatalf("unexpected train values: %d", len(train))
	}
	if got := string(train[0]); got != "foo" {
		t.Fatalf("unexpected decoded payload: got=%q want=%q", got, "foo")
	}
}
