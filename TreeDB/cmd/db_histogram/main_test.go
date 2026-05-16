package main

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

const dbHistogramHelperEnv = "GO_WANT_DB_HISTOGRAM_HELPER"

func TestDBHistogramMainHelper(t *testing.T) {
	if os.Getenv(dbHistogramHelperEnv) != "1" {
		return
	}
	args := os.Args
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			os.Args = append([]string{"db_histogram"}, args[i+1:]...)
			main()
			return
		}
	}
	os.Args = []string{"db_histogram"}
	main()
}

func runDBHistogram(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmdArgs := []string{"-test.run=TestDBHistogramMainHelper", "--"}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), dbHistogramHelperEnv+"=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fn()
	_ = w.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	return buf.String()
}

func TestNextPow2(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{
		{0, 1},
		{1, 2},
		{2, 4},
		{3, 6},
		{8, 16},
	}
	for _, tc := range cases {
		if got := nextPow2(tc.in); got != tc.want {
			t.Fatalf("nextPow2(%d)=%d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestPrintHistPrintsKnownBuckets(t *testing.T) {
	out := captureStdout(t, func() {
		printHist(map[int]int{0: 2, 4: 7})
	})
	if !strings.Contains(out, "       0: 2") {
		t.Fatalf("printHist output missing zero bucket: %q", out)
	}
	if !strings.Contains(out, "       4: 7") {
		t.Fatalf("printHist output missing bucket 4: %q", out)
	}
}

func TestDBHistogramMissingDirFails(t *testing.T) {
	out, err := runDBHistogram(t)
	if err == nil {
		t.Fatalf("expected non-zero exit, output=%q", out)
	}
	if !strings.Contains(out, "Usage: db_histogram <db_dir>") {
		t.Fatalf("expected usage error, got %q", out)
	}
}

func TestDBHistogramMainSuccess(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	db, err := treedb.Open(treedb.Options{Dir: dbDir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Set([]byte("k1"), []byte("value-1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set([]byte("k22"), []byte("value-2")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"db_histogram", dbDir}

	out := captureStdout(t, func() { main() })
	if !strings.Contains(out, "Total Keys: 2") {
		t.Fatalf("expected key count in output, got %q", out)
	}
	if !strings.Contains(out, "Value Size Histogram") {
		t.Fatalf("expected histogram output, got %q", out)
	}
}
