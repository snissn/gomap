package mongogateway

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

var updateMongoCompatibilityDocs = flag.Bool("update-mongo-compatibility-docs", false, "rewrite the generated compatibility matrix in COMPATIBILITY.md")

const (
	compatibilityMatrixDocPath = "COMPATIBILITY.md"
	compatibilityMatrixBegin   = "<!-- mongo-compatibility-matrix:begin -->"
	compatibilityMatrixEnd     = "<!-- mongo-compatibility-matrix:end -->"
)

func TestMongoCompatibilityMatrixDocumentationUpToDate(t *testing.T) {
	raw, err := os.ReadFile(compatibilityMatrixDocPath)
	if err != nil {
		t.Fatalf("read %s: %v", compatibilityMatrixDocPath, err)
	}
	want, err := replaceGeneratedCompatibilityMatrix(string(raw), mongoCompatibilityMatrixRows())
	if err != nil {
		t.Fatalf("replace generated compatibility matrix: %v", err)
	}

	if *updateMongoCompatibilityDocs {
		if string(raw) != want {
			if err := os.WriteFile(compatibilityMatrixDocPath, []byte(want), 0o644); err != nil {
				t.Fatalf("write %s: %v", compatibilityMatrixDocPath, err)
			}
		}
		return
	}

	if string(raw) != want {
		t.Fatalf("%s generated matrix is stale; run: GOWORK=off go test ./TreeDB/mongo_gateway -run %s -update-mongo-compatibility-docs",
			compatibilityMatrixDocPath, t.Name())
	}
}

func replaceGeneratedCompatibilityMatrix(doc string, rows []mongoCompatibilityMatrixRow) (string, error) {
	start := strings.Index(doc, compatibilityMatrixBegin)
	if start < 0 {
		return "", fmt.Errorf("missing begin marker %q", compatibilityMatrixBegin)
	}
	endRel := strings.Index(doc[start:], compatibilityMatrixEnd)
	if endRel < 0 {
		return "", fmt.Errorf("missing end marker %q", compatibilityMatrixEnd)
	}
	end := start + endRel + len(compatibilityMatrixEnd)
	return doc[:start] + generatedCompatibilityMatrix(rows) + doc[end:], nil
}

func generatedCompatibilityMatrix(rows []mongoCompatibilityMatrixRow) string {
	var b strings.Builder
	b.WriteString(compatibilityMatrixBegin)
	b.WriteByte('\n')
	b.WriteString("| Category | Feature | Status |\n")
	b.WriteString("|---|---|---|\n")
	for _, row := range rows {
		fmt.Fprintf(&b, "| %s | %s | %s |\n",
			markdownTableCell(row.category),
			markdownTableCell(row.feature),
			markdownTableCell(row.status))
	}
	b.WriteString(compatibilityMatrixEnd)
	return b.String()
}

func markdownTableCell(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "|", `\|`)
}
