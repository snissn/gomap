package main

import (
	"bytes"
	"fmt"
	"math"
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
		"stats candidate_rows=",
		"visited_nodes=",
		"vector_B=",
		"adjacency_B=",
		"row_fetches=",
		"decoded_blocks=",
		"max_resident_B=",
	}
	for _, needle := range required {
		if !strings.Contains(out, needle) {
			t.Fatalf("demo output missing %q\nstdout:\n%s\nstderr:\n%s", needle, out, stderr.String())
		}
	}
	// These synthetic vectors are identical; the native reader breaks their
	// score ties by physical ordinal, which may change with graph construction.
	want := map[string]bool{"doc-000000": true, "doc-000023": true, "doc-000046": true}
	count, previousOrdinal := 0, -1
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "result[") {
			continue
		}
		var rank, ordinal int
		var id string
		var score float64
		if n, err := fmt.Sscanf(line, "result[%d] id=%s ordinal=%d score=%f", &rank, &id, &ordinal, &score); err != nil || n != 4 || rank != count || !want[id] || math.IsNaN(score) || math.Abs(score-1) > 1e-6 || ordinal <= previousOrdinal {
			t.Fatalf("unexpected duplicate-vector result %q: parsed=%d err=%v", line, n, err)
		}
		delete(want, id)
		count++
		previousOrdinal = ordinal
	}
	if count != 3 || len(want) != 0 {
		t.Fatalf("duplicate-vector results=%d missing=%v\n%s", count, want, out)
	}
	if !strings.Contains(stderr.String(), "OpenVectorIndexSearcher") {
		t.Fatalf("demo stderr missing steady-state searcher tip: %s", stderr.String())
	}
}

func TestColumnGraphDemoIncludeDocsUsesProjectionPreset(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"-dir", dir,
		"-reset",
		"-rows", "32",
		"-dims", "6",
		"-degree", "4",
		"-top-k", "2",
		"-ef-search", "16",
		"-include-docs",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run demo with include docs: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	out := stdout.String()
	for _, needle := range []string{"include_docs=true doc_projection=exclude_embedding", "docs_fetched=2", "doc_fields_skipped="} {
		if !strings.Contains(out, needle) {
			t.Fatalf("demo output missing %q\nstdout:\n%s\nstderr:\n%s", needle, out, stderr.String())
		}
	}
}

func TestDemoVectorSearchOptionsProjectionModes(t *testing.T) {
	base := demoConfig{TopK: 3, EfSearch: 16}
	opts, projection, err := demoVectorSearchOptions(base, []float32{1, 0, 0})
	if err != nil {
		t.Fatalf("no-doc options: %v", err)
	}
	if opts.IncludeDocuments || projection != "none" || len(opts.DocumentFetchOptions.ExcludePaths) != 0 {
		t.Fatalf("no-doc opts=%+v projection=%q", opts, projection)
	}

	projected := base
	projected.IncludeDocs = true
	opts, projection, err = demoVectorSearchOptions(projected, []float32{1, 0, 0})
	if err != nil {
		t.Fatalf("projected options: %v", err)
	}
	if !opts.IncludeDocuments || projection != "exclude_embedding" || len(opts.DocumentFetchOptions.ExcludePaths) != 1 || opts.DocumentFetchOptions.ExcludePaths[0] != "embedding" {
		t.Fatalf("projected opts=%+v projection=%q", opts, projection)
	}

	full := projected
	full.IncludeDocEmbedding = true
	opts, projection, err = demoVectorSearchOptions(full, []float32{1, 0, 0})
	if err != nil {
		t.Fatalf("full options: %v", err)
	}
	if !opts.IncludeDocuments || projection != "full_document_embedding_echo" || len(opts.DocumentFetchOptions.ExcludePaths) != 0 {
		t.Fatalf("full opts=%+v projection=%q", opts, projection)
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
	parentTraversal := dir + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "escaped"
	if _, err := validateDemoResetDir(parentTraversal); err == nil {
		t.Fatal("validateDemoResetDir accepted parent traversal")
	}
	if demoResetDirWithinBase(filepath.Join(string(os.PathSeparator), "usr", "local"), string(os.PathSeparator)) {
		t.Fatal("filesystem root was accepted as a reset base")
	}
}

func TestDemoResetDirWithinBaseRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	allowed := filepath.Join(root, "allowed")
	outside := filepath.Join(root, "outside")
	if err := os.MkdirAll(allowed, 0o755); err != nil {
		t.Fatalf("mkdir allowed: %v", err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	link := filepath.Join(allowed, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if demoResetDirWithinBase(filepath.Join(link, "db"), allowed) {
		t.Fatal("symlink escape was accepted as inside allowed base")
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
