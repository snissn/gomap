package main

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
	"time"
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
		"-keys", "2000000",
		"-valsize", "128", // Original problematic value size
		"-format", "text", // Use text format for easier parsing if needed, markdown is fine too
		"-checkpoint-between-tests",
		"-treedb-force-value-pointers",
		"-max-wall", "120s", // Set max wall time to detect hangs
	)

	// Set a generous timeout for this test
	timeout := 150 * time.Second // Overall test timeout, slightly longer than -max-wall
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	timer := time.AfterFunc(timeout, func() {
		cmd.Process.Kill()
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
