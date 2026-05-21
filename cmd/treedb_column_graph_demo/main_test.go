package main

import (
	"bytes"
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
