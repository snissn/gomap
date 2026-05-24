package docs_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func repoRoots(t *testing.T) (treeRoot, repoRoot string) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	treeRoot = filepath.Clean(filepath.Join(filepath.Dir(thisFile), ".."))
	repoRoot = filepath.Clean(filepath.Join(treeRoot, ".."))
	return treeRoot, repoRoot
}

func markdownDocs(t *testing.T) []string {
	t.Helper()
	treeRoot, repoRoot := repoRoots(t)
	roots := []string{
		filepath.Join(treeRoot, "README.md"),
		filepath.Join(treeRoot, "AGENTS.md"),
		filepath.Join(treeRoot, "AUDIT_TRACKING.md"),
		filepath.Join(treeRoot, "docs", "spec"),
		filepath.Join(repoRoot, "docs"),
	}

	seen := make(map[string]bool)
	var paths []string
	for _, root := range roots {
		info, err := os.Stat(root)
		if err != nil {
			t.Fatalf("stat %s: %v", root, err)
		}
		if !info.IsDir() {
			if strings.HasSuffix(root, ".md") && !seen[root] {
				seen[root] = true
				paths = append(paths, root)
			}
			continue
		}
		err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "benchmarks" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".md") && !seen[path] {
				seen[path] = true
				paths = append(paths, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	sort.Strings(paths)
	return paths
}

func TestDocs_NoTreeDBSlabTerminology(t *testing.T) {
	treeRoot, _ := repoRoots(t)

	paths := []string{
		filepath.Join(treeRoot, "README.md"),
		filepath.Join(treeRoot, "AGENTS.md"),
		filepath.Join(treeRoot, "AUDIT_TRACKING.md"),
	}
	specPaths, err := filepath.Glob(filepath.Join(treeRoot, "docs", "spec", "*.md"))
	if err != nil {
		t.Fatalf("glob spec docs: %v", err)
	}
	paths = append(paths, specPaths...)
	allowedLegacyFields := regexp.MustCompile(`\b(activeslabid|activeslabtail)\b`)

	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		text := strings.ToLower(string(content))
		// Preserve on-disk identifier accuracy where code still uses legacy
		// field names in MetaPageBody.
		text = allowedLegacyFields.ReplaceAllString(text, "")
		if strings.Contains(text, "slab") {
			t.Fatalf("legacy slab terminology found in %s; use persistent value-log wording", p)
		}
	}
}

func TestDocs_CanonicalStoragePaths(t *testing.T) {
	staleValueLogPath := regexp.MustCompile(`wal/value-l(?:\*|<|\d|-)`)
	for _, p := range markdownDocs(t) {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		for i, line := range strings.Split(string(content), "\n") {
			lower := strings.ToLower(line)
			if strings.Contains(lower, "legacy") {
				continue
			}
			if staleValueLogPath.MatchString(lower) || strings.Contains(lower, "maindb/wal/value") {
				t.Fatalf("%s:%d uses stale value-log path; canonical value-log path is maindb/value_vlog/value-l*.log", p, i+1)
			}
			mentionsValueLog := strings.Contains(lower, "value-log") || strings.Contains(lower, "value log") || strings.Contains(lower, "large values")
			if mentionsValueLog && (strings.Contains(lower, "dir/maindb/wal") || strings.Contains(lower, "options.dir/maindb/wal") || strings.Contains(lower, "maindb/wal/")) {
				t.Fatalf("%s:%d places value-log data under wal; canonical value-log path is maindb/value_vlog/", p, i+1)
			}
		}
	}
}

func TestTypedStorageStorageFormatDocsMentionCompatibilityDirectory(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "storage-format.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read storage-format doc: %v", err)
	}
	doc := string(data)
	pathNeedle := "`column_assets/<namespace>/assets/segments/segment-*.tca`"
	if !strings.Contains(doc, pathNeedle) {
		t.Fatalf("storage-format doc missing exact typed asset manager path %q", pathNeedle)
	}

	normalizedDoc := strings.Join(strings.Fields(doc), " ")
	for _, want := range []string{
		"- typed asset manager segments under `column_assets/<namespace>/assets/segments/segment-*.tca` for production typed-storage physical assets",
		"`column_assets` remains the compatibility directory name",
		"Production typed-storage physical data is stored in typed asset manager segments under the compatibility `column_assets` directory",
		"typed-row payloads, typed-column part payloads, and derived accelerator payloads",
	} {
		if !strings.Contains(normalizedDoc, want) {
			t.Fatalf("storage-format doc missing typed-storage compatibility wording %q", want)
		}
	}
}

func TestDocs_NullableTypedColumnSemantics(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	storagePath := filepath.Join(treeRoot, "docs", "spec", "storage-format.md")
	storageData, err := os.ReadFile(storagePath)
	if err != nil {
		t.Fatalf("read storage-format doc: %v", err)
	}
	storageDoc := strings.Join(strings.Fields(string(storageData)), " ")
	for _, want := range []string{
		"nullable int64 column uses the `nullable_int64` encoding",
		"the null bitmap marks rows whose JSON path was present with an explicit `null`",
		"the default/missing bitmap marks rows whose declared path was omitted",
		"positive optimization expectation, not only a no-regression gate",
		"actively remove existing avoidable allocations and obvious local overhead in the same touched path",
		"target 0 allocs/op after setup when benchmarking the core typed-column loop separately from document materialization",
		"Touched inner loops must be measurably no worse, and preferably better, on `B/op` and `allocs/op`",
		"Checksum, lifetime, schema, null/missing, and fail-closed validation must not be weakened",
		"Production `float32_vector` and `adjacency_list` nullable/missing support remains staged and fail-closed",
	} {
		if !strings.Contains(storageDoc, want) {
			t.Fatalf("storage-format doc missing nullable typed-column wording %q", want)
		}
	}

	adapterPath := filepath.Join(treeRoot, "docs", "spec", "typed-column-adapter.md")
	adapterData, err := os.ReadFile(adapterPath)
	if err != nil {
		t.Fatalf("read typed-column adapter doc: %v", err)
	}
	adapterDoc := strings.Join(strings.Fields(string(adapterData)), " ")
	for _, want := range []string{
		"present/non-null rows write the declared path and value, explicit-null rows write the declared path with JSON null, and missing/default rows leave the declared path absent",
		"the scan fails closed with `ErrColumnQueryPlanUnsupported`; it must not fall back to full-document reconstruction/materialization",
		"Direct typed-column predicate paths must preserve hot-path allocation discipline and should actively remove existing avoidable allocations",
		"Touched inner loops must be measurably no worse, and preferably better, on `B/op` and `allocs/op`",
		"baseline-versus-final `B/op`/`allocs/op` evidence and an allocation profile/top",
	} {
		if !strings.Contains(adapterDoc, want) {
			t.Fatalf("typed-column adapter doc missing nullable query/reconstruction wording %q", want)
		}
	}
}

func TestDocs_DurabilityMatrixSingleOwner(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	owner := filepath.Join(treeRoot, "docs", "spec", "write-path-and-durability.md")
	heading := regexp.MustCompile(`(?im)^#{1,6}\s+.*durability matrix`)
	for _, p := range markdownDocs(t) {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		if p != owner && heading.Match(content) {
			t.Fatalf("%s defines a durability matrix; link to %s instead", p, owner)
		}
	}
}

func TestDocs_CollectionWALCurrentTargetLabels(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	ownerDocs := map[string]bool{
		filepath.Join(treeRoot, "docs", "spec", "backup-restore.md"):                 true,
		filepath.Join(treeRoot, "docs", "spec", "collection-wal-durability-plan.md"): true,
		filepath.Join(treeRoot, "docs", "spec", "collections-write-domain.md"):       true,
		filepath.Join(treeRoot, "docs", "spec", "contracts.md"):                      true,
		filepath.Join(treeRoot, "docs", "spec", "native-query-raft-roadmap.md"):      true,
		filepath.Join(treeRoot, "docs", "spec", "native-wire-protocol.md"):           true,
		filepath.Join(treeRoot, "docs", "spec", "recovery.md"):                       true,
		filepath.Join(treeRoot, "docs", "spec", "storage-format.md"):                 true,
		filepath.Join(treeRoot, "docs", "spec", "value-log-lifecycle.md"):            true,
		filepath.Join(treeRoot, "docs", "spec", "verification.md"):                   true,
		filepath.Join(treeRoot, "docs", "spec", "write-path-and-durability.md"):      true,
		filepath.Join(treeRoot, "docs", "spec", "GOMAP_TREEDB_COLUMN_STORE_RFC.md"):  true,
		filepath.Join(treeRoot, "docs", "spec", "COMPRESSION_TECHNOLOGY_SPEC.md"):    true,
	}
	terms := []string{"collection wal", "durable-at-ack", "applied watermark", "side ref", "root group"}
	phaseTerms := []string{"current behavior", "target behavior", "target contract", "planned", "until collection wal lands", "after the collection wal gate", "before collection wal lands", "once collection wal", "target collection", "current shipped", "future collection wal"}

	for _, p := range markdownDocs(t) {
		if ownerDocs[p] {
			continue
		}
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		text := strings.ToLower(string(content))
		hasTerm := false
		for _, term := range terms {
			if strings.Contains(text, term) {
				hasTerm = true
				break
			}
		}
		if !hasTerm {
			continue
		}
		hasPhase := false
		for _, term := range phaseTerms {
			if strings.Contains(text, term) {
				hasPhase = true
				break
			}
		}
		if !hasPhase {
			t.Fatalf("%s mentions collection WAL terms without current/target phase language", p)
		}
	}
}

func TestDocs_SpecManifestFilesExist(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	readmePath := filepath.Join(treeRoot, "docs", "spec", "README.md")
	content, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("read %s: %v", readmePath, err)
	}
	re := regexp.MustCompile("TreeDB/docs/spec/([^`\\s]+\\.md)")
	matches := re.FindAllSubmatch(content, -1)
	if len(matches) == 0 {
		t.Fatalf("no spec manifest links found in %s", readmePath)
	}
	for _, match := range matches {
		name := string(match[1])
		path := filepath.Join(treeRoot, "docs", "spec", name)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("spec manifest references missing file %s: %v", path, err)
		}
	}
}

func TestDocs_NativeWireRaftLocalWALSeparation(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	paths := []string{
		filepath.Join(treeRoot, "docs", "spec", "collection-wal-durability-plan.md"),
		filepath.Join(treeRoot, "docs", "spec", "native-query-raft-roadmap.md"),
		filepath.Join(treeRoot, "docs", "spec", "native-wire-protocol.md"),
	}
	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		text := strings.ToLower(string(content))
		if strings.Contains(text, "raft") && strings.Contains(text, "collection wal") {
			hasLocalPhysical := strings.Contains(text, "local physical")
			hasNotRaftLog := strings.Contains(text, "not a raft log")
			if !hasLocalPhysical || !hasNotRaftLog {
				t.Fatalf("%s mentions Raft and collection WAL without stating that collection WAL is local physical state and not a Raft log entry", p)
			}
		}
	}
}
