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
