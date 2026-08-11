package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestValidateLocalHNSWQuerySplitPairV1(t *testing.T) {
	trustedTruth := strings.Repeat("a", 64)
	f := fixtureManifest{Checksum: "fixture", Queries: 256}
	c, h := localHNSWQuerySplitV1{Schema: "vector_partition_4105_query_split_v1", DatasetChecksum: "fixture", TruthArtifactSHA256: trustedTruth, Selection: localHNSWQuerySplitSelectionV1}, localHNSWQuerySplitV1{Schema: "vector_partition_4105_query_split_v1", DatasetChecksum: "fixture", TruthArtifactSHA256: trustedTruth, Selection: localHNSWQuerySplitSelectionV1}
	for i := 0; i < f.Queries; i++ {
		if localHNSWCalibrationOrdinalV1(i) {
			c.Ordinals = append(c.Ordinals, i)
		} else {
			h.Ordinals = append(h.Ordinals, i)
		}
	}
	if err := validateLocalHNSWQuerySplitPairV1(c, h, f, trustedTruth); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		c    localHNSWQuerySplitV1
		h    localHNSWQuerySplitV1
	}{
		{"wrong selection", func() localHNSWQuerySplitV1 { x := c; x.Selection = "wrong"; return x }(), h},
		{"wrong classification", func() localHNSWQuerySplitV1 {
			x := c
			x.Ordinals = append([]int(nil), c.Ordinals...)
			x.Ordinals[0] = h.Ordinals[0]
			slices.Sort(x.Ordinals)
			return x
		}(), h},
		{"missing coverage", c, func() localHNSWQuerySplitV1 { x := h; x.Ordinals = x.Ordinals[:len(x.Ordinals)-1]; return x }()},
		{"untrusted truth", func() localHNSWQuerySplitV1 { x := c; x.TruthArtifactSHA256 = strings.Repeat("b", 64); return x }(), h},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateLocalHNSWQuerySplitPairV1(tc.c, tc.h, f, trustedTruth); err == nil {
				t.Fatal("invalid split accepted")
			}
		})
	}
	write := func(name, body string) string {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, tc := range []struct{ name, body string }{
		{"unknown field", `{"unknown":true}`},
		{"trailing JSON", `{} {}`},
		{"over cap", strings.Repeat("x", localHNSWQuerySplitMaxBytesV1+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := loadLocalHNSWQuerySplitV1(write(tc.name, tc.body)); err == nil {
				t.Fatal("invalid split file accepted")
			}
		})
	}
}
