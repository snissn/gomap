package valuelog

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCompactLeafNamespaceVolumeOptimizationPreservesLexicalContract(t *testing.T) {
	fileID := mustEncodeFileID(t, ReservedLeafLogLaneID, 1)
	valueID := mustEncodeFileID(t, 0, 1)
	paths := []string{
		"", ".", "leaf_vlog", "leaf_vlog/f", "leaf_vlog/f/",
		"other/f", "./leaf_vlog/f", "other/../leaf_vlog/f",
		"/leaf_vlog/f", "//leaf_vlog/f", "/other//leaf_vlog/f",
		`C:\leaf_vlog\f`, `C:leaf_vlog\f`, `C:\\leaf_vlog\f`,
		`C:C:leaf_vlog\f`, `C:/leaf_vlog/f`, `C:\other\..\leaf_vlog\f`,
		`\\host\leaf_vlog\f`, `\\host\share\leaf_vlog\f`,
		`\\host\share\\leaf_vlog\f`, `//host/share/leaf_vlog/f`,
		`\\?\C:\leaf_vlog\f`, `\\?\UNC\host\share\leaf_vlog\f`,
	}
	for _, path := range paths {
		want := path != "" && filepath.Base(filepath.Dir(path)) == compactLeafPagePayloadDirName
		if got := allowsCompactLeafLogPayload(fileID, path); got != want {
			t.Errorf("path %q: eligible=%t want original classification %t", path, got, want)
		}
		if allowsCompactLeafLogPayload(valueID, path) {
			t.Errorf("value-log namespace accepted as leaf for %q", path)
		}
	}
	canonical := filepath.Join(t.TempDir(), compactLeafPagePayloadDirName, "f")
	if n := testing.AllocsPerRun(100, func() {
		if !allowsCompactLeafLogPayload(fileID, canonical) {
			panic("canonical leaf namespace refused")
		}
	}); n != 0 {
		t.Fatalf("canonical namespace allocs=%g want0", n)
	}
}

// Compare the admission predicate with its original lexical definition. Prefix
// normalization must never change the decision, including unusual Unix bytes
// and path strings that Windows interprets as volumes or device namespaces.
func TestCompactLeafNamespaceDifferentialPaths(t *testing.T) {
	leafID := mustEncodeFileID(t, ReservedLeafLogLaneID, 1)
	valueID := mustEncodeFileID(t, 0, 1)
	prefixes := []string{"", "/", "//", "///", "./", "../", "/a/", "/a//", "/a/./", "/a/../", "/../", "a/../../", `C:/`, `C:\`, `\\host\share\`, `\\?\C:\`, "/a\b/", "/a\x00/"}
	parents := []string{"leaf_vlog", "leaf_vlog/.", "leaf_vlog/..", "other/../leaf_vlog", "leaf_vlog/../other", ".", "..", "", "other"}
	leaves := []string{"f", "f/", "f//", ".", "..", "", "a.b", ".f", "...", `a\b`, "f/../g", "f/./g", "f//g", "\x00"}
	for _, prefix := range prefixes {
		for _, parent := range parents {
			for _, leaf := range leaves {
				path := prefix + parent + "/" + leaf
				for _, id := range []uint32{leafID, valueID, 0} {
					want := isLeafLogFileID(id) && path != "" && filepath.Base(filepath.Dir(path)) == compactLeafPagePayloadDirName
					if got := allowsCompactLeafLogPayload(id, path); got != want {
						t.Fatalf("id=%d path=%q got=%t want=%t", id, path, got, want)
					}
				}
			}
		}
	}
	// Deterministic generated lexical combinations complement named edge cases.
	parts := []string{"", ".", "..", "leaf_vlog", "other", `C:`, `\\?`, "f"}
	for _, a := range parts {
		for _, b := range parts {
			for _, c := range parts {
				for _, d := range parts {
					path := strings.Join([]string{a, b, c, d}, "/")
					want := filepath.Base(filepath.Dir(path)) == compactLeafPagePayloadDirName
					if got := allowsCompactLeafLogPayload(leafID, path); got != want {
						t.Fatalf("generated path=%q got=%t want=%t", path, got, want)
					}
				}
			}
		}
	}
}
