package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocs_ColumnGraphNativeReconstructionInventory(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	inventoryPath := filepath.Join(treeRoot, "docs", "spec", "column-graph-native-reconstruction-inventory.md")
	content, err := os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatalf("read inventory: %v", err)
	}
	text := string(content)

	required := []string{
		"Base branch: `codex/colgranule-m15c-column-asset-rewrite`",
		"Base PR: #1636",
		"Base commit: `3ed99fd05fa28687ed681b51a87e1bac2e10c402`",
		"Base issue: #1621",
		"Post-V1 performance tracker: #1634",
		"Native vector tracker: #1646",
		"## Missing Generic Column-Store Primitives",
		"ordinal-to-granule or ordinal-to-block lookup",
		"bounded decoded-block cache semantics",
		"## Old PR Stack Disposition",
		"| #1642 |",
		"| #1643 |",
		"| #1644 |",
		"| #1645 |",
		"## Copy Forward",
		"## Adapt Forward",
		"## Keep As Oracle Or Comparator",
		"## Quarantine",
		"## Do Not Copy",
		"## Required PR Body Controls",
		"`Test Plan Start`",
		"`Performance Plan Start`",
		"`AI Review Loop`",
	}
	for _, needle := range required {
		if !strings.Contains(text, needle) {
			t.Fatalf("inventory missing required text %q", needle)
		}
	}

	forbiddenNativeClaims := []string{
		"decoded `ColumnVectorGraph` path is native",
		"decoded path is native",
		"decoded path proves native reader search",
		"full decoded `ColumnVectorGraph` copy is native",
	}
	for _, needle := range forbiddenNativeClaims {
		if strings.Contains(text, needle) {
			t.Fatalf("inventory contains misleading native-reader claim %q", needle)
		}
	}
}

func TestDocs_ColumnGraphNativePRTemplateSections(t *testing.T) {
	_, repoRoot := repoRoots(t)
	templatePath := filepath.Join(repoRoot, ".github", "PULL_REQUEST_TEMPLATE", "opt_sprint.md")
	content, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("read PR template: %v", err)
	}
	text := string(content)

	required := []string{
		"Column Graph Native Workstream",
		"Copied/Adapted From Old Stack",
		"Path Identity",
		"Base And Dependency State",
		"Test Plan Start",
		"Performance Plan Start",
		"Test Plan Close",
		"Performance Plan Close",
		"AI Review Loop",
		"Codex latest-head review",
		"Copilot latest-head review",
		"CodeRabbit latest-head review",
	}
	for _, needle := range required {
		if !strings.Contains(text, needle) {
			t.Fatalf("PR template missing #1646 section %q", needle)
		}
	}
}
