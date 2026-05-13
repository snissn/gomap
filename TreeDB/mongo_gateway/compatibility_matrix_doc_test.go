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
	return doc[:start] + generatedCompatibilityMatrix(rows, markerLineNewline(doc, start)) + doc[end:], nil
}

func generatedCompatibilityMatrix(rows []mongoCompatibilityMatrixRow, newline string) string {
	var b strings.Builder
	b.WriteString(compatibilityMatrixBegin)
	b.WriteString(newline)
	b.WriteString("| Category | Feature | Status |")
	b.WriteString(newline)
	b.WriteString("|---|---|---|")
	b.WriteString(newline)
	for _, row := range rows {
		fmt.Fprintf(&b, "| %s | %s | %s |%s",
			markdownTableCell(row.category),
			markdownTableCell(row.feature),
			markdownTableCell(row.status),
			newline)
	}
	b.WriteString(compatibilityMatrixEnd)
	return b.String()
}

func markerLineNewline(doc string, markerStart int) string {
	if markerStart >= 0 && markerStart < len(doc) {
		if rel := strings.IndexByte(doc[markerStart:], '\n'); rel >= 0 {
			lineEnd := markerStart + rel
			if lineEnd > 0 && doc[lineEnd-1] == '\r' {
				return "\r\n"
			}
			return "\n"
		}
	}
	return dominantDocumentNewline(doc)
}

func dominantDocumentNewline(doc string) string {
	crlf := strings.Count(doc, "\r\n")
	lf := strings.Count(strings.ReplaceAll(doc, "\r\n", ""), "\n")
	if crlf > lf {
		return "\r\n"
	}
	return "\n"
}

func TestMarkerLineNewline(t *testing.T) {
	mixed := strings.Join([]string{
		"# title\n",
		compatibilityMatrixBegin + "\r\n",
		"old\r\n",
		compatibilityMatrixEnd + "\r\n",
	}, "")
	if got := markerLineNewline(mixed, strings.Index(mixed, compatibilityMatrixBegin)); got != "\r\n" {
		t.Fatalf("markerLineNewline=%q want CRLF", got)
	}
}

func markdownTableCell(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "|", `\|`)
}
