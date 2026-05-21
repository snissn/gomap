package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestColumnGraphDemoRunsCloseReopenNativeReaderPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"-dir", dir,
		"-reset",
		"-rows", "64",
		"-dims", "8",
		"-degree", "4",
		"-top-k", "3",
		"-ef-search", "32",
		"-max-decoded-blocks", "1",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run demo: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	out := stdout.String()
	required := []string{
		"TreeDB column_graph native-reader demo",
		"rebuild status=column_graph_loaded loaded=true",
		"search path=column_graph_native_reader status=column_graph_loaded loaded=true",
		"stats candidates=",
		"row_fetches=",
		"decoded_blocks=",
		"max_resident_B=",
		"result[0] id=doc-000000 ordinal=0",
	}
	for _, needle := range required {
		if !strings.Contains(out, needle) {
			t.Fatalf("demo output missing %q\nstdout:\n%s\nstderr:\n%s", needle, out, stderr.String())
		}
	}
	if !strings.Contains(stderr.String(), "OpenVectorIndexSearcher") {
		t.Fatalf("demo stderr missing steady-state searcher tip: %s", stderr.String())
	}
}

func TestValidateDemoResetDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	got, err := validateDemoResetDir(dir)
	if err != nil {
		t.Fatalf("validateDemoResetDir(%q): %v", dir, err)
	}
	want, err := filepath.Abs(dir)
	if err != nil {
		t.Fatalf("Abs(%q): %v", dir, err)
	}
	if got != filepath.Clean(want) {
		t.Fatalf("validateDemoResetDir=%q want %q", got, filepath.Clean(want))
	}

	for _, unsafe := range []string{"", ".", "..", string(os.PathSeparator), os.TempDir()} {
		if _, err := validateDemoResetDir(unsafe); err == nil {
			t.Fatalf("validateDemoResetDir(%q) succeeded, want rejection", unsafe)
		}
	}
}

func TestLoadGloveRowsRejectsInvalidNumericValues(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "parse error",
			content: "bad 0.1 nope\n",
			want:    `GloVe row "bad" dim 1 parse`,
		},
		{
			name:    "non finite",
			content: "bad 0.1 NaN\n",
			want:    `GloVe row "bad" dim 1 has non-finite value`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "glove.txt")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			_, _, err := loadGloveRows(path, 1)
			if err == nil {
				t.Fatal("loadGloveRows succeeded, want error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err, tt.want)
			}
		})
	}
}
