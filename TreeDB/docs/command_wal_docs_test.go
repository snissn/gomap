package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandWALDocsRejectActiveCollectionWALReferencesOutsideDeprecatedDoc(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	deprecated := filepath.Join(treeRoot, "docs", "spec", "collection-wal-durability-plan.md")
	for _, p := range markdownDocs(t) {
		if p == deprecated {
			continue
		}
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		lower := strings.ToLower(string(content))
		lines := strings.Split(lower, "\n")
		for i, line := range lines {
			if !strings.Contains(line, "internal/collectionwal") && !strings.Contains(line, "wal/collection-l") {
				continue
			}
			context := line
			if i > 0 {
				context = lines[i-1] + " " + context
			}
			if i+1 < len(lines) {
				context += " " + lines[i+1]
			}
			if strings.Contains(context, "deprecated") || strings.Contains(context, "historical") || strings.Contains(context, "superseded") || strings.Contains(context, "not an active") || strings.Contains(context, "no active") || strings.Contains(context, "no new") || strings.Contains(context, "active implementation targets") || strings.Contains(context, "do not describe") || strings.Contains(context, "old collection") || strings.Contains(context, "must not create") || strings.Contains(context, "must not add") {
				continue
			}
			t.Fatalf("%s:%d points active docs at deprecated collection WAL implementation", p, i+1)
		}
	}
}

func TestCommandWALExistingCoverageInventoryMapsLegacyWALTests(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	content, err := os.ReadFile(filepath.Join(treeRoot, "docs", "spec", "user-command-wal-test-migration.md"))
	if err != nil {
		t.Fatalf("read test migration inventory: %v", err)
	}
	text := string(content)
	for _, want := range []string{
		"TestCommitLogWriteReadBatch",
		"TestCommitLogCorruptCRC",
		"TestCommitLogAppendBatchRejectsMixedSequence",
		"TestCommitLogTruncatedPayload",
		"FuzzCommitLogReader",
		"TestCrashRecovery_WALReplayIsCoherent",
		"TestRecovery_RIDJoinReplaysValueLog",
		"TestRecovery_PartialCommitBatchIgnored",
		"TestReadOnlyDoesNotReplayOrRemoveCommitLog",
		"TestCachingDB_Checkpoint_TrimsWAL",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("migration inventory missing %s", want)
		}
	}
}

func TestCommandWALLegacyRawEncodingTestsHaveTypedFrameEquivalents(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	content, err := os.ReadFile(filepath.Join(treeRoot, "docs", "spec", "user-command-wal-test-migration.md"))
	if err != nil {
		t.Fatalf("read test migration inventory: %v", err)
	}
	rows := migrationInventoryRows(string(content))
	pairs := map[string]string{
		"TestCommitLogWriteReadBatch":                  "TestCommandWALFormatGoldenV1RawKVBatch",
		"TestCommitLogCorruptCRC":                      "TestCommandWALFormatRejectsFrameCRCMismatch",
		"TestCommitLogAppendBatchRejectsMixedSequence": "TestCommandWALDuplicateLSNFailsClosed",
		"TestCommitLogTruncatedPayload":                "TestCommandWALTerminalShortHeaderIgnored",
		"FuzzCommitLogReader":                          "FuzzCommandWALDecodeFrame",
		"TestCrashRecovery_WALReplayIsCoherent":        "TestCommandWALRawSetDeleteBatchReplaysThroughNormalExecutor",
		"TestRecovery_RIDJoinReplaysValueLog":          "TestCommandWALRIDFencePreservedForRawKVBatch",
		"TestRecovery_PartialCommitBatchIgnored":       "TestCommandWALOpenAllowsActivePartialFirstFrameTail",
		"TestReadOnlyDoesNotReplayOrRemoveCommitLog":   "TestCommandWALReadOnlyOpenWithUnappliedFrameFailsRecoveryRequired",
		"TestCachingDB_Checkpoint_TrimsWAL":            "TestCommandWALCheckpointCleanupDeletesOnlyCoveredSegments",
	}
	for legacy, typed := range pairs {
		row, ok := rows[legacy]
		if !ok || !strings.Contains(row, typed) {
			t.Fatalf("migration inventory missing mapping %s -> %s", legacy, typed)
		}
	}
}

func TestCommandWALLegacySurfaceInventoryCoversRemovalStack(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "command-wal-legacy-surface-inventory.md")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy surface inventory: %v", err)
	}
	text := string(content)
	for _, want := range []string{
		"https://github.com/snissn/gomap/issues/3612",
		"https://github.com/snissn/gomap/issues/3613",
		"`DisableWAL` / `disableJournal`",
		"`DurabilityWALOffRelaxed`",
		"legacy cached redo journal",
		"checkpoint-only benchmark",
		"Public command-WAL opens enter cached mode as `DisableWAL=true`",
		"`WriteAfterCommandWALAppend`",
		"`writeRangeBatch(sync)`",
		"`replayWALIntoBackend`",
		"#3614",
		"#3615",
		"#3616",
		"#3617",
		"#3618",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("%s missing inventory/ownership wording %q", path, want)
		}
	}
}

func TestCommandWALLegacyTermsInCurrentDocsNeedContext(t *testing.T) {
	treeRoot, repoRoot := repoRoots(t)
	paths := []string{
		filepath.Join(treeRoot, "README.md"),
		filepath.Join(treeRoot, "docs", "spec", "README.md"),
		filepath.Join(treeRoot, "docs", "spec", "storage-format.md"),
		filepath.Join(treeRoot, "docs", "spec", "write-path-and-durability.md"),
		filepath.Join(treeRoot, "docs", "spec", "recovery.md"),
		filepath.Join(treeRoot, "docs", "spec", "value-log-lifecycle.md"),
		filepath.Join(treeRoot, "docs", "spec", "contracts.md"),
		filepath.Join(treeRoot, "docs", "spec", "verification.md"),
		filepath.Join(treeRoot, "docs", "spec", "command-wal-legacy-surface-inventory.md"),
		filepath.Join(repoRoot, "docs", "README.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_CONCEPTS.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_PROFILES.md"),
		filepath.Join(repoRoot, "docs", "contracts", "DURABILITY.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_RECOVERY.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_TUNING.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_VALUELOG_AUTOTUNE.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_WRITE_PATHS.md"),
		filepath.Join(repoRoot, "cmd", "unified_bench", "README.md"),
	}
	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		lines := strings.Split(string(content), "\n")
		for i, line := range lines {
			if !mentionsLegacyWALTerm(line) {
				continue
			}
			context := strings.ToLower(line)
			if i > 0 {
				context = strings.ToLower(lines[i-1]) + " " + context
			}
			if i+1 < len(lines) {
				context += " " + strings.ToLower(lines[i+1])
			}
			if hasLegacyWALContext(context) {
				continue
			}
			t.Fatalf("%s:%d mentions legacy WAL terminology without legacy/compatibility/benchmark context: %s", p, i+1, line)
		}
	}
}

func mentionsLegacyWALTerm(line string) bool {
	lower := strings.ToLower(line)
	for _, term := range []string{
		"legacy_wal",
		"wal_on_fast",
		"no_wal_fast",
		"profilefast",
		"profilewalonfast",
		"profilelegacy",
		"profilenowalfast",
		"durabilitywaloffrelaxed",
		"durabilitywalonrelaxed",
		"disablejournal",
		"disablewal",
		"disable wal",
		"wal-on",
		"wal on/off",
		"wal-off",
		"wal off",
	} {
		if strings.Contains(lower, term) {
			return true
		}
	}
	return false
}

func hasLegacyWALContext(context string) bool {
	for _, term := range []string{
		"legacy",
		"compatib",
		"historical",
		"deprecated",
		"unsafe",
		"benchmark",
		"internal",
		"forensic",
		"reject",
		"not a treedb server profile",
		"not public",
		"not the current",
		"not normal",
		"should not present",
		"must be removed",
		"quarantin",
		"archive",
		"cross-db",
		"old",
		"low-level",
		"process-crash",
		"power-loss",
		"fsync",
		"checkpoint",
		"recoverable",
		"recoverability",
		"durable-at-ack",
		"performance",
		"throughput",
		"backend-only",
		"commit sequence",
		"mutation sequence",
		"mutation revision",
		"writesync",
		"sync boundaries",
		"command-wal",
	} {
		if strings.Contains(context, term) {
			return true
		}
	}
	return false
}

func migrationInventoryRows(text string) map[string]string {
	rows := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "| ") || strings.HasPrefix(line, "|---") {
			continue
		}
		cells := strings.Split(line, "|")
		if len(cells) < 3 {
			continue
		}
		name := strings.TrimSpace(cells[1])
		if name == "" || name == "Existing test" {
			continue
		}
		rows[name] = line
	}
	return rows
}
