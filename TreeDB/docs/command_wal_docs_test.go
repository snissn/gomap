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

func TestCommandWALCurrentDocsStateDurabilityContract(t *testing.T) {
	_, repoRoot := repoRoots(t)
	for _, tc := range []struct {
		rel   string
		wants []string
	}{
		{
			rel: filepath.Join("docs", "contracts", "DURABILITY.md"),
			wants: []string{
				"`command_wal_durable` ordinary acknowledgements",
				"`command_wal_relaxed` ordinary acknowledgements",
				"`no_wal_fast` ordinary acknowledgements",
				"`Flush` and `FlushAll` are visibility/drain operations",
				"Current command-WAL directories fail closed on replayable legacy redo",
			},
		},
		{
			rel: filepath.Join("docs", "TREEDB_RECOVERY.md"),
			wants: []string{
				"Public command-WAL profiles persist command frames",
				"Legacy cached redo-journal replay is a compatibility path",
				"`AllowLegacyCachedRedoJournalReplay`",
				"`DeleteRange`, `Batch.Write`, and `Batch.WriteSync`",
				"Read-only opens do not perform mutating recovery",
			},
		},
		{
			rel: filepath.Join("docs", "TREEDB_WRITE_PATHS.md"),
			wants: []string{
				"`SetSync`, `Delete`, `DeleteSync`, `DeleteRange`",
				"today that includes callback-based `Update`",
				"legacy cached redo-journal",
			},
		},
		{
			rel: filepath.Join("docs", "TREEDB_DOWNSTREAM_VALIDATION.md"),
			wants: []string{
				"Pin the gomap commit or tag",
				"`treedb.cache.redo_log.mode=external_command_wal`",
				"Treat `WriteSync` / `Batch.WriteSync` as command-WAL sync boundaries",
				"`DeleteRange`, `Batch.Write`, and `Batch.WriteSync` are",
				"benchmark scripts fail if the accepted load window is too short to interpret",
			},
		},
	} {
		text, err := os.ReadFile(filepath.Join(repoRoot, tc.rel))
		if err != nil {
			t.Fatalf("read %s: %v", tc.rel, err)
		}
		for _, want := range tc.wants {
			if !strings.Contains(string(text), want) {
				t.Fatalf("%s missing current command-WAL contract wording %q", tc.rel, want)
			}
		}
	}
}

func TestDurabilityProfileChildEvidenceManifestCoversSuccessorInputs(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "durability-profile-child-evidence-3683.md")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read child evidence manifest: %v", err)
	}
	text := string(content)
	for _, want := range []string{
		"#3683 -> #3684",
		"TestDurabilityProfilePublicEntrypointInventory",
		"TestProductionProfileLifecycleFrontiersMatchFrozenContract",
		"TestProductionProfilesForcedPointersDeleteReuseRotationReopen",
		"TestCrashRecovery_DurabilityTiers",
		"TestProductionAuthorityExecutableCompositeOmissionMatrix",
		"TestCommandWALIntentResolvedProfileControlsOrdinaryStagedAppendSync",
		"TestFlushCoordinatorHardOverloadFallsBackToForegroundAssist",
		"benchmarks/dgraph_durability",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("%s missing successor evidence %q", path, want)
		}
	}
}

func TestBenchmarkProfileWrappersUseCanonicalBenchUnsafe(t *testing.T) {
	_, repoRoot := repoRoots(t)
	checks := map[string][]string{
		filepath.Join(repoRoot, "scripts", "leafgen_cached_dwell_validate.sh"):        {"PROFILE=${TREEDB_PROFILE:-bench_unsafe}"},
		filepath.Join(repoRoot, "scripts", "leafgen_target_sweep.sh"):                 {"PROFILE=${TREEDB_PROFILE:-bench_unsafe}"},
		filepath.Join(repoRoot, "scripts", "treedb_collection_compression_matrix.sh"): {"PROFILE=\"${PROFILE:-bench_unsafe}\""},
		filepath.Join(repoRoot, "scripts", "bench_collections_report.sh"):             {"TREEDB_COLLECTION_BENCH_ENGINE:-bench_unsafe"},
		filepath.Join(repoRoot, "scripts", "treedb_insert_compression_profile.sh"):    {"TREEDB_COLLECTION_BENCH_ENGINE:-bench_unsafe"},
		filepath.Join(repoRoot, "scripts", "bench_collections_matrix.sh"):             {"bench_unsafe_data_vlog_index_leaf bench_unsafe"},
		filepath.Join(repoRoot, "scripts", "mongo_gateway_scaling_bench.sh"):          {"TREEDB_PROFILE:-bench_unsafe", "Default: bench_unsafe"},
		filepath.Join(repoRoot, "scripts", "mongo_gateway_compare.sh"):                {"TREEDB_PROFILE:-bench_unsafe", "Default: bench_unsafe"},
		filepath.Join(repoRoot, "scripts", "mongo_gateway_ycsb_attribution.sh"):       {"TREEDB_PROFILE:-bench_unsafe"},
		filepath.Join(repoRoot, "cmd", "mongo_gateway_bench", "README.md"):            {"-treedb-profile bench_unsafe"},
		filepath.Join(repoRoot, "cmd", "treedb_vector_search_demo", "README.md"):      {"`bench_unsafe` profile", "|bench_unsafe`"},
	}
	for path, required := range checks {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(content)
		for _, want := range required {
			if !strings.Contains(text, want) {
				t.Fatalf("%s missing canonical benchmark profile token %q", path, want)
			}
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
		filepath.Join(repoRoot, "docs", "TREEDB_CACHED_VS_BACKEND.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_CONCEPTS.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_PROFILES.md"),
		filepath.Join(repoRoot, "docs", "contracts", "DURABILITY.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_RECOVERY.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_TUNING.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_VALUELOG_AUTOTUNE.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_VLOG_GENERATIONAL_RUNBOOK.md"),
		filepath.Join(repoRoot, "docs", "TREEDB_WRITE_PATHS.md"),
		filepath.Join(repoRoot, "docs", "BENCHMARK_SPEC.md"),
		filepath.Join(repoRoot, "cmd", "unified_bench", "README.md"),
	}
	guidePaths, err := filepath.Glob(filepath.Join(treeRoot, "docs", "guides", "*.md"))
	if err != nil {
		t.Fatalf("glob TreeDB guide docs: %v", err)
	}
	paths = append(paths, guidePaths...)
	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		lines := strings.Split(string(content), "\n")
		for i, line := range lines {
			lowerLine := strings.ToLower(line)
			legacyProfileAlias := mentionsLegacyProfileAlias(lowerLine)
			if !legacyProfileAlias && !mentionsLegacyWALTerm(line) {
				continue
			}
			context := lowerLine
			if i > 0 {
				context = strings.ToLower(lines[i-1]) + " " + context
			}
			if i+1 < len(lines) {
				context += " " + strings.ToLower(lines[i+1])
			}
			if legacyProfileAlias {
				if hasLegacyProfileAliasContext(context) {
					continue
				}
				t.Fatalf("%s:%d mentions legacy profile alias without legacy/compatibility/benchmark context: %s", p, i+1, line)
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
		"profilefast",
		"profilewalonfast",
		"profilelegacy",
		"profiledurable",
		"durabilitywaloffrelaxed",
		"durabilitywalonrelaxed",
		"disablejournal",
		"disablewal",
		"disable wal",
		"no-wal",
		"no wal",
	} {
		if strings.Contains(lower, term) {
			return true
		}
	}
	if mentionsLegacyWALModePhrase(lower) {
		return true
	}
	return mentionsLegacyProfileAlias(lower)
}

func TestCommandWALLegacyTermMatcherCatchesDelimitedProfileAliases(t *testing.T) {
	for _, line := range []string{
		"`durable`",
		"`fast`",
		"`walonfast`",
		"`wal_on_fast`",
		"-profile durable",
		"-profile=durable",
		"--profile fast",
		"--profile=fast",
		"-treedb-profile walonfast",
		"--treedb-profile=wal_on_fast",
		"TREEDB_OPEN_PROFILE=fast",
		"TREEDB_PROFILE=wal_on_fast",
		"BENCH_PROFILE=fast",
		"BENCH_PROFILE=durable",
		"profile=durable",
		"no-WAL benchmark ceiling",
		"no WAL benchmark ceiling",
	} {
		if !mentionsLegacyWALTerm(line) {
			t.Fatalf("mentionsLegacyWALTerm(%q) = false, want true", line)
		}
	}
	for _, line := range []string{
		"the fast path remains current",
		"durable command-WAL writes",
		"fast checkpoint cleanup",
		"Command-WAL-only protection remains current",
	} {
		if mentionsLegacyWALTerm(line) {
			t.Fatalf("mentionsLegacyWALTerm(%q) = true, want false", line)
		}
	}
}

func TestCommandWALLegacyProfileAliasesRequireLegacyOrBenchmarkContext(t *testing.T) {
	for _, context := range []string{
		"`fast` is a legacy benchmark-runner ceiling preset",
		"`durable` is a cross-DB benchmark preset, not a TreeDB server profile",
		"`wal_on_fast` remains a compatibility alias for historical artifacts",
	} {
		if !hasLegacyProfileAliasContext(context) {
			t.Fatalf("hasLegacyProfileAliasContext(%q) = false, want true", context)
		}
	}
	for _, context := range []string{
		"`fast` is the current command-WAL profile",
		"`wal_on_fast` is a command-WAL profile",
		"`durable` is current command-WAL guidance",
	} {
		if hasLegacyProfileAliasContext(context) {
			t.Fatalf("hasLegacyProfileAliasContext(%q) = true, want false", context)
		}
	}
}

func mentionsLegacyProfileAlias(lower string) bool {
	for _, alias := range []string{
		"legacy_wal_relaxed_fast",
		"legacy_wal_durable",
		"wal_on_fast",
		"walonfast",
		"durable",
		"fast",
	} {
		if containsDelimitedLegacyProfileAlias(lower, alias) {
			return true
		}
	}
	return false
}

func mentionsLegacyWALModePhrase(lower string) bool {
	for _, phrase := range []string{
		"wal on/off",
		"wal on",
		"wal off",
		"wal-on",
		"wal-off",
	} {
		if containsLegacyWALModePhrase(lower, phrase) {
			return true
		}
	}
	return false
}

func containsLegacyWALModePhrase(lower, phrase string) bool {
	for {
		idx := strings.Index(lower, phrase)
		if idx < 0 {
			return false
		}
		if !hasCommandWALPrefix(lower, idx) && hasLegacyWALPhraseBoundary(lower, idx, len(phrase)) {
			return true
		}
		lower = lower[idx+len(phrase):]
	}
}

func hasCommandWALPrefix(lower string, idx int) bool {
	const prefix = "command-"
	return idx >= len(prefix) && lower[idx-len(prefix):idx] == prefix
}

func hasLegacyWALPhraseBoundary(lower string, idx, phraseLen int) bool {
	beforeOK := idx == 0 || isLegacyWALPhraseBoundaryByte(lower[idx-1])
	afterIdx := idx + phraseLen
	afterOK := afterIdx == len(lower) || isLegacyWALPhraseBoundaryByte(lower[afterIdx])
	return beforeOK && afterOK
}

func isLegacyWALPhraseBoundaryByte(b byte) bool {
	switch b {
	case ' ', '\t', '`', '"', '\'', ',', '.', ';', ':', '(', '[', '{', ')', ']', '}', '/', '\\':
		return true
	default:
		return false
	}
}

func containsDelimitedLegacyProfileAlias(lower, alias string) bool {
	for _, quoted := range []string{
		"`" + alias + "`",
		`"` + alias + `"`,
		"'" + alias + "'",
	} {
		if strings.Contains(lower, quoted) {
			return true
		}
	}
	for _, prefix := range []string{
		"-profile ",
		"-profile=",
		"--profile ",
		"--profile=",
		"-treedb-profile ",
		"-treedb-profile=",
		"--treedb-profile ",
		"--treedb-profile=",
		"profile=",
		"profile: ",
		"bench_profile=",
		"bench_profile: ",
		"treedb_open_profile=",
		"treedb_profile=",
		"treedb_profile: ",
	} {
		if containsLegacyProfileValue(lower, prefix, alias) {
			return true
		}
	}
	return false
}

func containsLegacyProfileValue(lower, prefix, alias string) bool {
	for {
		idx := strings.Index(lower, prefix)
		if idx < 0 {
			return false
		}
		value := strings.TrimLeft(lower[idx+len(prefix):], " \t`\"'")
		if strings.HasPrefix(value, alias) && isLegacyProfileAliasBoundary(value, len(alias)) {
			return true
		}
		lower = lower[idx+len(prefix):]
	}
}

func isLegacyProfileAliasBoundary(value string, aliasLen int) bool {
	if len(value) == aliasLen {
		return true
	}
	switch value[aliasLen] {
	case ' ', '\t', '`', '"', '\'', ',', '.', ';', ':', ')', ']', '}':
		return true
	default:
		return false
	}
}

func hasLegacyProfileAliasContext(context string) bool {
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
		"old wal",
		"old/raw",
		"legacy/raw",
	} {
		if strings.Contains(context, term) {
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
		"old wal",
		"old/raw",
		"legacy/raw",
		"commit sequence",
		"mutation sequence",
		"mutation revision",
		"writesync",
		"sync boundaries",
		"current command-wal",
		"command-wal profile",
		"command-wal durable",
		"command-wal relaxed",
		"durable command-wal",
		"no_wal_fast",
		"profilenowalfast",
		"no-wal production",
		"no wal production",
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
