package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestIntegration_NoHangLargeKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Build the unified-bench executable first to ensure we test the latest version
	buildCmd := exec.Command("go", "build", "-o", "../../bin/unified-bench", ".")
	buildCmd.Dir = "." // The test is already running from ./cmd/unified_bench
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("failed to build unified-bench: %v", err)
	}

	cmd := exec.Command(
		"../../bin/unified-bench",
		"-test", "batch_write,random_write,batch_delete",
		"-dbs", "treedb",
		"-profile", "wal_on_fast", // Apply profile via command line argument
		"-keys", "200000",
		"-valsize", "128", // Original problematic value size
		"-format", "text", // Use text format for easier parsing if needed, markdown is fine too
		"-checkpoint-between-tests",
		"-treedb-force-value-pointers",
		"-max-wall", "180s", // Per-subtest wall clock cap for hang detection
	)

	// Keep this comfortably above per-test -max-wall (180s) across the three
	// subtests plus startup/build overhead and teardown on slower CI hosts.
	timeout := 9*time.Minute + 30*time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	timer := time.AfterFunc(timeout, func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		t.Errorf("benchmark process timed out externally after %v", timeout)
	})
	defer timer.Stop()

	err := cmd.Run()

	if err != nil {
		if strings.Contains(stderr.String(), "guard: max-wall exceeded") {
			t.Errorf("benchmark exited due to MaxWall being exceeded, which indicates a hang/extreme slowness. This is a regression. Output:\n%s\n%s", stdout.String(), stderr.String())
		} else {
			t.Fatalf("benchmark failed with error: %v\nStdout:\n%s\nStderr:\n%s", err, stdout.String(), stderr.String())
		}
	}

	combined := stdout.String() + "\n" + stderr.String()
	// Check if results are reported, indicating successful completion
	if !strings.Contains(combined, "Batch Write / TreeDB") ||
		!strings.Contains(combined, "Random Write / TreeDB") ||
		!strings.Contains(combined, "Batch Delete / TreeDB") {
		t.Errorf("benchmark did not report expected results, or completed too quickly without full results. Output:\n%s\n%s", stdout.String(), stderr.String())
	}

	t.Logf("Benchmark completed successfully.\nStdout:\n%s\nStderr:\n%s", stdout.String(), stderr.String())
}

func TestIntegration_ProfileDir_AutoRunsBenchprof(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Build the unified-bench executable first to test the CLI path.
	buildCmd := exec.Command("go", "build", "-o", "../../bin/unified-bench", ".")
	buildCmd.Dir = "."
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("failed to build unified-bench: %v", err)
	}

	profileDir := t.TempDir()

	cmd := exec.Command(
		"../../bin/unified-bench",
		"-test", "sequential_write",
		"-dbs", "treedb",
		"-keys", "128",
		"-format", "table",
		"-profile-dir", profileDir,
		"-progress=false",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	timeout := 120 * time.Second
	timer := time.AfterFunc(timeout, func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		t.Errorf("benchmark process timed out after %v", timeout)
	})
	defer timer.Stop()

	if err := cmd.Run(); err != nil {
		t.Fatalf("benchmark failed: %v\nStdout:\n%s\nStderr:\n%s", err, stdout.String(), stderr.String())
	}

	if _, err := os.Stat(filepath.Join(profileDir, "benchprof_results.json")); err != nil {
		t.Fatalf("expected benchprof_results.json: %v", err)
	}
	if _, err := os.Stat(filepath.Join(profileDir, "insights.md")); err != nil {
		t.Fatalf("expected generated insights.md: %v\nStdout:\n%s\nStderr:\n%s", err, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(profileDir, "insights.json")); err != nil {
		t.Fatalf("expected generated insights.json: %v\nStdout:\n%s\nStderr:\n%s", err, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(profileDir, "insights.html")); err != nil {
		t.Fatalf("expected generated insights.html: %v\nStdout:\n%s\nStderr:\n%s", err, stdout.String(), stderr.String())
	}
}

func listBenchDirs(prefix string) map[string]struct{} {
	pattern := filepath.Join(os.TempDir(), "bench-"+prefix+"*")
	matches, _ := filepath.Glob(pattern)
	out := make(map[string]struct{}, len(matches))
	for _, m := range matches {
		out[m] = struct{}{}
	}
	return out
}

func diffBenchDirs(before map[string]struct{}, prefix string) []string {
	after := listBenchDirs(prefix)
	out := make([]string, 0, len(after))
	for path := range after {
		if _, ok := before[path]; !ok {
			out = append(out, path)
		}
	}
	sort.Strings(out)
	return out
}

func TestIntegration_TreeDBLevelDBParity500k(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("RUN_UNIFIED_BENCH_PARITY_500K") != "1" {
		t.Skip("set RUN_UNIFIED_BENCH_PARITY_500K=1 to enable 500k parity gate")
	}

	buildCmd := exec.Command("go", "build", "-o", "../../bin/unified-bench", ".")
	buildCmd.Dir = "."
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("failed to build unified-bench: %v", err)
	}

	beforeTree := listBenchDirs("treedb")
	beforeLevel := listBenchDirs("leveldb")

	cmd := exec.Command(
		"../../bin/unified-bench",
		"-dbs", "treedb,leveldb",
		"-profile", "fast",
		"-keys", "500000",
		"-progress=false",
		"-format", "markdown",
		// Parity compares final DB state; run only mutating tests to keep gate time bounded.
		"-test", "sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_write_steady,batch_random,batch_delete,batch_small_seq,random_delete",
		"-keep",
		"-checkpoint-between-tests",
		"-treedb-index-outer-leaf-mode", "v1_leaflog_route",
		"-treedb-force-value-pointers=false",
		"-valsize", "100",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	timeout := 25 * time.Minute
	timer := time.AfterFunc(timeout, func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		t.Errorf("benchmark process timed out after %v", timeout)
	})
	defer timer.Stop()

	if err := cmd.Run(); err != nil {
		t.Fatalf("unified-bench failed: %v\nStdout:\n%s\nStderr:\n%s", err, stdout.String(), stderr.String())
	}

	newTree := diffBenchDirs(beforeTree, "treedb")
	newLevel := diffBenchDirs(beforeLevel, "leveldb")
	if len(newTree) != 1 || len(newLevel) != 1 {
		t.Fatalf("expected one new treedb and one new leveldb dir, got treedb=%v leveldb=%v\nStdout:\n%s\nStderr:\n%s", newTree, newLevel, stdout.String(), stderr.String())
	}
	treeDir := newTree[0]
	levelDir := newLevel[0]

	opts := treedb.OptionsFor(treedb.ProfileDurable, treeDir)
	opts.ReadOnly = true
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1LeafLogRoute
	treeDB, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open treedb readonly: %v (dir=%s)", err, treeDir)
	}
	defer func() { _ = treeDB.Close() }()

	levelDB, err := leveldb.OpenFile(levelDir, nil)
	if err != nil {
		t.Fatalf("open leveldb: %v (dir=%s)", err, levelDir)
	}
	defer func() { _ = levelDB.Close() }()

	treeIt, err := treeDB.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("treedb iterator: %v", err)
	}
	defer func() { _ = treeIt.Close() }()

	levelIt := levelDB.NewIterator(nil, nil)
	defer levelIt.Release()

	levelValid := levelIt.First()
	treeValid := treeIt.Valid()
	compared := 0

	for treeValid || levelValid {
		if treeValid != levelValid {
			if treeValid {
				t.Fatalf("iterator length mismatch at index=%d: treedb has extra key=%x", compared, treeIt.Key())
			}
			t.Fatalf("iterator length mismatch at index=%d: leveldb has extra key=%x", compared, levelIt.Key())
		}
		treeKey := treeIt.Key()
		levelKey := levelIt.Key()
		if cmp := bytes.Compare(treeKey, levelKey); cmp != 0 {
			t.Fatalf("key mismatch at index=%d treedb=%x leveldb=%x", compared, treeKey, levelKey)
		}
		treeVal := treeIt.Value()
		levelVal := levelIt.Value()
		if !bytes.Equal(treeVal, levelVal) {
			t.Fatalf("value mismatch at index=%d key=%x treedb_len=%d leveldb_len=%d", compared, treeKey, len(treeVal), len(levelVal))
		}
		compared++
		treeIt.Next()
		treeValid = treeIt.Valid()
		levelValid = levelIt.Next()
	}

	if err := treeIt.Error(); err != nil {
		t.Fatalf("treedb iterator error after %d keys: %v", compared, err)
	}
	if err := levelIt.Error(); err != nil {
		t.Fatalf("leveldb iterator error after %d keys: %v", compared, err)
	}

	t.Logf("parity matched across %d key/value pairs (treedb=%s leveldb=%s)", compared, treeDir, levelDir)
	fmt.Fprintf(os.Stderr, "unified bench parity ok: keys=%d treedb=%s leveldb=%s\n", compared, treeDir, levelDir)
}
