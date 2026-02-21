package main

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/caching"
)

func TestBuildVlogAutotuneCasesIncludesMarquee(t *testing.T) {
	cases := buildVlogAutotuneCases()
	if len(cases) == 0 {
		t.Fatalf("buildVlogAutotuneCases returned no cases")
	}
	found := false
	for _, c := range cases {
		if c.Name == "marquee_regime_shift" && c.Kind == "marquee" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected marquee case in suite")
	}
}

func TestBuildSegmentsUnknownWorkload(t *testing.T) {
	_, err := buildSegments([]vlogSegmentSpec{{
		Name:     "bad",
		Workload: "does_not_exist",
	}})
	if err == nil {
		t.Fatalf("expected error for unknown workload")
	}
}

func TestFindMode(t *testing.T) {
	modes := []vlogAutotuneModeReport{
		{Mode: "off"},
		{Mode: "autotune"},
	}
	got := findMode(modes, "autotune")
	if got == nil || got.Mode != "autotune" {
		t.Fatalf("findMode returned %v, want autotune mode", got)
	}
	if findMode(modes, "missing") != nil {
		t.Fatalf("findMode should return nil for missing mode")
	}
}

func TestEvalVlogAutotuneMarksMissingAutotune(t *testing.T) {
	c := vlogAutotuneCase{Name: "x", Kind: "io_bound_compressible"}
	marks := evalVlogAutotuneMarks(c, []vlogAutotuneModeReport{{Mode: "off"}})
	if len(marks) == 0 {
		t.Fatalf("expected at least one mark")
	}
	if marks[0].Name != "autotune_present" || marks[0].Pass {
		t.Fatalf("unexpected first mark: %+v", marks[0])
	}
}

func TestSanityMarksFlagsInvalidFractionsAndRatios(t *testing.T) {
	result := caching.VlogAutotuneBenchResult{
		Segments: []caching.VlogAutotuneBenchSegmentResult{
			{
				Name:              "s0",
				KeptFrac:          0.8,
				AttemptedFrac:     0.2,
				ObservedRatio:     2.0,
				PublishOrderingOK: false,
			},
		},
	}
	marks := sanityMarks(result)
	if len(marks) != 3 {
		t.Fatalf("sanityMarks len=%d, want 3", len(marks))
	}
	if marks[0].Pass {
		t.Fatalf("expected fraction mark to fail: %+v", marks[0])
	}
	if marks[1].Pass {
		t.Fatalf("expected ratio mark to fail: %+v", marks[1])
	}
	if marks[2].Pass {
		t.Fatalf("expected publish_order mark to fail: %+v", marks[2])
	}
}
