package valuelog

import (
	"path/filepath"
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
