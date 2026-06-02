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
