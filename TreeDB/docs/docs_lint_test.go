package docs_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestDocs_NoTreeDBSlabTerminology(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	treeRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), ".."))

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
