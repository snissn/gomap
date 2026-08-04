package mongogateway

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

var updateMongoCapabilityDocs = flag.Bool("update-mongo-capability-docs", false, "rewrite generated Mongo capability documentation")
var updateMongoCompatibilityDocs = flag.Bool("update-mongo-compatibility-docs", false, "deprecated alias for -update-mongo-capability-docs")

const (
	compatibilityMatrixDocPath = "COMPATIBILITY.md"
	compatibilityMatrixBegin   = "<!-- mongo-compatibility-matrix:begin -->"
	compatibilityMatrixEnd     = "<!-- mongo-compatibility-matrix:end -->"
	capabilitySummaryDocPath   = "README.md"
	capabilitySummaryBegin     = "<!-- mongo-capability-summary:begin -->"
	capabilitySummaryEnd       = "<!-- mongo-capability-summary:end -->"
)

func TestMongoCompatibilityMatrixDocumentationUpToDate(t *testing.T) {
	manifest := MongoGatewayCapabilities()
	if err := ValidateMongoGatewayCapabilityManifest(manifest); err != nil {
		t.Fatalf("validate capability manifest: %v", err)
	}

	tests := []struct {
		path    string
		replace func(string) (string, error)
	}{
		{
			path: compatibilityMatrixDocPath,
			replace: func(doc string) (string, error) {
				return replaceGeneratedCompatibilityMatrix(doc, manifest)
			},
		},
		{
			path: capabilitySummaryDocPath,
			replace: func(doc string) (string, error) {
				return replaceGeneratedCapabilitySummary(doc, manifest)
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.path, func(t *testing.T) {
			raw, err := os.ReadFile(tc.path)
			if err != nil {
				t.Fatalf("read %s: %v", tc.path, err)
			}
			want, err := tc.replace(string(raw))
			if err != nil {
				t.Fatalf("generate %s: %v", tc.path, err)
			}
			if *updateMongoCapabilityDocs || *updateMongoCompatibilityDocs {
				if string(raw) != want {
					if err := os.WriteFile(tc.path, []byte(want), 0o644); err != nil {
						t.Fatalf("write %s: %v", tc.path, err)
					}
				}
				return
			}
			if string(raw) != want {
				t.Fatalf("%s generated capability surface is stale; run: GOWORK=off go test ./TreeDB/mongo_gateway -run %s -update-mongo-capability-docs", tc.path, strings.Split(t.Name(), "/")[0])
			}
		})
	}
}

func replaceGeneratedCompatibilityMatrix(doc string, manifest MongoGatewayCapabilityManifest) (string, error) {
	return replaceGeneratedCapabilityBlock(doc, compatibilityMatrixBegin, compatibilityMatrixEnd, func(newline string) (string, error) {
		return generatedCompatibilityMatrix(manifest, newline), nil
	})
}

func replaceGeneratedCapabilitySummary(doc string, manifest MongoGatewayCapabilityManifest) (string, error) {
	return replaceGeneratedCapabilityBlock(doc, capabilitySummaryBegin, capabilitySummaryEnd, func(newline string) (string, error) {
		return generatedCapabilitySummary(manifest, newline)
	})
}

func replaceGeneratedCapabilityBlock(doc, begin, end string, generate func(string) (string, error)) (string, error) {
	start := strings.Index(doc, begin)
	if start < 0 {
		return "", fmt.Errorf("missing begin marker %q", begin)
	}
	endRel := strings.Index(doc[start:], end)
	if endRel < 0 {
		return "", fmt.Errorf("missing end marker %q", end)
	}
	blockEnd := start + endRel + len(end)
	generated, err := generate(markerLineNewline(doc, start))
	if err != nil {
		return "", err
	}
	return doc[:start] + generated + doc[blockEnd:], nil
}

func generatedCompatibilityMatrix(manifest MongoGatewayCapabilityManifest, newline string) string {
	var b strings.Builder
	b.WriteString(compatibilityMatrixBegin)
	b.WriteString(newline)
	b.WriteString("| Category | Feature | Status | Capability ID |")
	b.WriteString(newline)
	b.WriteString("|---|---|---|---|")
	b.WriteString(newline)
	for _, capability := range manifest.Capabilities {
		fmt.Fprintf(&b, "| %s | %s | %s | `%s` |%s",
			markdownTableCell(capability.Category),
			markdownTableCell(capability.Feature),
			markdownTableCell(string(capability.Status)),
			markdownTableCell(capability.ID),
			newline)
	}
	b.WriteString(compatibilityMatrixEnd)
	return b.String()
}

func generatedCapabilitySummary(manifest MongoGatewayCapabilityManifest, newline string) (string, error) {
	var b strings.Builder
	b.WriteString(capabilitySummaryBegin)
	b.WriteString(newline)
	b.WriteString("## Executable capability summary")
	b.WriteString(newline)
	b.WriteString(newline)
	fmt.Fprintf(&b, "Manifest: `%s`%s%s", mongoGatewayCapabilityIdentityForManifest(manifest), newline, newline)
	b.WriteString("| Surface | Status | Boundary |")
	b.WriteString(newline)
	b.WriteString("|---|---|---|")
	b.WriteString(newline)
	for _, summary := range manifest.Summaries {
		status, err := mongoGatewayCapabilitySummaryStatus(manifest, summary)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "| %s | %s | %s |%s",
			markdownTableCell(summary.Label),
			markdownTableCell(string(status)),
			markdownTableCell(summary.Note),
			newline)
	}
	b.WriteString(capabilitySummaryEnd)
	return b.String(), nil
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
	return "\n"
}

func TestMarkerLineNewlineDetectsCRLF(t *testing.T) {
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
