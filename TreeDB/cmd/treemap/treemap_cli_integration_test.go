package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

type cliRunResult struct {
	stdout   string
	stderr   string
	exitCode int
}

type cliEntry struct {
	key string
	val string
}

func TestTreemapCLI_UsageAndUnknownCommand(t *testing.T) {
	t.Run("no_args_prints_usage_and_exits_2", func(t *testing.T) {
		res := runTreemapCLI(t)
		if res.exitCode != 2 {
			t.Fatalf("exit code: got %d, want %d (stderr=%q)", res.exitCode, 2, res.stderr)
		}
		if !strings.Contains(res.stderr, "Usage:") {
			t.Fatalf("stderr missing usage text: %q", res.stderr)
		}
	})

	t.Run("help_prints_usage_and_exits_0", func(t *testing.T) {
		res := runTreemapCLI(t, "help")
		if res.exitCode != 0 {
			t.Fatalf("exit code: got %d, want %d (stderr=%q)", res.exitCode, 0, res.stderr)
		}
		if !strings.Contains(res.stderr, "Usage:") {
			t.Fatalf("stderr missing usage text: %q", res.stderr)
		}
	})

	t.Run("unknown_command_prints_usage_and_exits_2", func(t *testing.T) {
		res := runTreemapCLI(t, "unknown-cmd", t.TempDir())
		if res.exitCode != 2 {
			t.Fatalf("exit code: got %d, want %d (stderr=%q)", res.exitCode, 2, res.stderr)
		}
		if !strings.Contains(res.stderr, `unknown command "unknown-cmd"`) {
			t.Fatalf("stderr missing unknown command message: %q", res.stderr)
		}
		if !strings.Contains(res.stderr, "Usage:") {
			t.Fatalf("stderr missing usage text: %q", res.stderr)
		}
	})
}

func TestTreemapCLI_HappyPaths_GetKeysScanScanJSONLImportJSONL(t *testing.T) {
	srcDir := t.TempDir()
	seedTreemapDB(t, srcDir, []cliEntry{
		{key: "app:root", val: "ignore"},
		{key: "user:1", val: "alice"},
		{key: "user:2", val: "bob"},
	})

	getRes := runTreemapCLI(t, "get", srcDir, "-allow-values", "user:1")
	if getRes.exitCode != 0 {
		t.Fatalf("get exit code: got %d, want 0 (stderr=%q)", getRes.exitCode, getRes.stderr)
	}
	if got, want := getRes.stdout, "alice\n"; got != want {
		t.Fatalf("get stdout: got %q, want %q", got, want)
	}

	keysRes := runTreemapCLI(t, "keys", srcDir, "-prefix", "user:")
	if keysRes.exitCode != 0 {
		t.Fatalf("keys exit code: got %d, want 0 (stderr=%q)", keysRes.exitCode, keysRes.stderr)
	}
	if got, want := keysRes.stdout, "user:1\nuser:2\n"; got != want {
		t.Fatalf("keys stdout: got %q, want %q", got, want)
	}

	scanRes := runTreemapCLI(t, "scan", srcDir, "-prefix", "user:", "-allow-values")
	if scanRes.exitCode != 0 {
		t.Fatalf("scan exit code: got %d, want 0 (stderr=%q)", scanRes.exitCode, scanRes.stderr)
	}
	if got, want := scanRes.stdout, "user:1\talice\nuser:2\tbob\n"; got != want {
		t.Fatalf("scan stdout: got %q, want %q", got, want)
	}

	scanJSONLRes := runTreemapCLI(t, "scan-jsonl", srcDir, "-prefix", "user:", "-allow-values", "-encoding", "string", "-omit-encoding")
	if scanJSONLRes.exitCode != 0 {
		t.Fatalf("scan-jsonl exit code: got %d, want 0 (stderr=%q)", scanJSONLRes.exitCode, scanJSONLRes.stderr)
	}
	gotJSONLRecords := parseJSONLRecords(t, scanJSONLRes.stdout)
	wantJSONLRecords := []jsonKV{
		{Key: "user:1", Val: "alice"},
		{Key: "user:2", Val: "bob"},
	}
	if !reflect.DeepEqual(gotJSONLRecords, wantJSONLRecords) {
		t.Fatalf("scan-jsonl records mismatch: got %#v, want %#v", gotJSONLRecords, wantJSONLRecords)
	}

	jsonlPath := filepath.Join(t.TempDir(), "import.jsonl")
	if err := os.WriteFile(jsonlPath, []byte(scanJSONLRes.stdout), 0o600); err != nil {
		t.Fatalf("write jsonl file: %v", err)
	}

	dstDir := t.TempDir()
	importRes := runTreemapCLI(t, "import-jsonl", dstDir, "-input", jsonlPath, "-input-encoding", "string", "-batch", "2")
	if importRes.exitCode != 0 {
		t.Fatalf("import-jsonl exit code: got %d, want 0 (stderr=%q)", importRes.exitCode, importRes.stderr)
	}
	if !strings.Contains(importRes.stdout, "Imported 2 records") {
		t.Fatalf("import-jsonl stdout missing count: %q", importRes.stdout)
	}

	verifyImportRes := runTreemapCLI(t, "get", dstDir, "-allow-values", "user:2")
	if verifyImportRes.exitCode != 0 {
		t.Fatalf("import verification get exit code: got %d, want 0 (stderr=%q)", verifyImportRes.exitCode, verifyImportRes.stderr)
	}
	if got, want := verifyImportRes.stdout, "bob\n"; got != want {
		t.Fatalf("import verification get stdout: got %q, want %q", got, want)
	}
}

func TestTreemapCLI_SafetyGuards(t *testing.T) {
	dir := t.TempDir()

	checkpointRes := runTreemapCLI(t, "checkpoint", dir)
	if checkpointRes.exitCode != 1 {
		t.Fatalf("checkpoint guard exit code: got %d, want %d (stderr=%q)", checkpointRes.exitCode, 1, checkpointRes.stderr)
	}
	if !strings.Contains(checkpointRes.stderr, "checkpoint requires -rw") {
		t.Fatalf("checkpoint guard stderr mismatch: %q", checkpointRes.stderr)
	}

	scanRes := runTreemapCLI(t, "scan", dir)
	if scanRes.exitCode != 1 {
		t.Fatalf("scan guard exit code: got %d, want %d (stderr=%q)", scanRes.exitCode, 1, scanRes.stderr)
	}
	if !strings.Contains(scanRes.stderr, "scan requires -allow-values") {
		t.Fatalf("scan guard stderr mismatch: %q", scanRes.stderr)
	}
}

func TestTreemapCLI_CheckpointRWSuccess(t *testing.T) {
	dir := t.TempDir()
	seedTreemapDB(t, dir, []cliEntry{
		{key: "k1", val: "v1"},
		{key: "k2", val: "v2"},
	})

	res := runTreemapCLI(t, "checkpoint", dir, "-rw")
	if res.exitCode != 0 {
		t.Fatalf("checkpoint -rw exit code: got %d, want 0 (stderr=%q)", res.exitCode, res.stderr)
	}
	if !strings.Contains(res.stdout, "checkpoint ") {
		t.Fatalf("checkpoint -rw stdout mismatch: %q", res.stdout)
	}
}

func TestHelperTreemapCLI(t *testing.T) {
	if os.Getenv("TREEMAP_CLI_HELPER") != "1" {
		t.Skip("helper")
	}

	rawArgs := os.Getenv("TREEMAP_CLI_ARGS_JSON")
	if rawArgs == "" {
		t.Fatalf("missing TREEMAP_CLI_ARGS_JSON")
	}

	var args []string
	if err := json.Unmarshal([]byte(rawArgs), &args); err != nil {
		t.Fatalf("unmarshal TREEMAP_CLI_ARGS_JSON: %v", err)
	}

	os.Args = append([]string{"treemap"}, args...)
	main()
	os.Exit(0)
}

func runTreemapCLI(t *testing.T, args ...string) cliRunResult {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	rawArgs, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperTreemapCLI$")
	cmd.Env = append(os.Environ(),
		"TREEMAP_CLI_HELPER=1",
		"TREEMAP_CLI_ARGS_JSON="+string(rawArgs),
	)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()

	res := cliRunResult{
		stdout: stdout.String(),
		stderr: stderr.String(),
	}
	if runErr == nil {
		res.exitCode = 0
		return res
	}

	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		res.exitCode = exitErr.ExitCode()
		return res
	}

	t.Fatalf("run helper subprocess: %v", runErr)
	return cliRunResult{}
}

func seedTreemapDB(t *testing.T, dir string, entries []cliEntry) {
	t.Helper()

	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	for _, entry := range entries {
		if err := db.Set([]byte(entry.key), []byte(entry.val)); err != nil {
			_ = db.Close()
			t.Fatalf("set key %q: %v", entry.key, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func parseJSONLRecords(t *testing.T, raw string) []jsonKV {
	t.Helper()

	scanner := bufio.NewScanner(strings.NewReader(raw))
	records := make([]jsonKV, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec jsonKV
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("unmarshal jsonl line %q: %v", line, err)
		}
		records = append(records, rec)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan jsonl output: %v", err)
	}
	return records
}
