package main

import (
	"os"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWAttributionTimingV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	vectors := make([][]float64, 16)
	for i := range vectors {
		vectors[i] = []float64{float64(i + 1), float64(i%3 + 1), 1}
	}
	source, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	sourceDir := source.dir
	source.owned = false
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(sourceDir)
	source, err = openM8ProductionExistingAssetSetV1(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	native, err := materializeRetainedLocalHNSWVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantNativeV1, 9987)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	overlay, err := materializeRetainedLocalHNSWVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantOverlayCurrentV1, 9988)
	if err != nil {
		t.Fatal(err)
	}
	defer overlay.Close()
	var ordinals []int
	for ordinal := 0; len(ordinals) < 2 && ordinal < 1024; ordinal++ {
		if localHNSWCalibrationOrdinalV1(ordinal) {
			ordinals = append(ordinals, ordinal)
		}
	}
	if len(ordinals) != 2 {
		t.Fatalf("calibration ordinals=%v", ordinals)
	}
	cases := []localHNSWAttributionTimingCaseV1{{Ordinal: ordinals[0], Query: []float32{1, 1, 1}, LowRoute: []uint32{0, 1}, HighRoute: []uint32{0, 1, 2, 3}}, {Ordinal: ordinals[1], Query: []float32{2, 1, 1}, LowRoute: []uint32{0, 1}, HighRoute: []uint32{0, 1, 2, 3}}}
	for i := range cases {
		cases[i].QueryFP32SHA256 = localHNSWAttributionQueryFP32SHA256V1(cases[i].Query)
	}
	evidence, err := localHNSWAttributionTimingV1(t.Context(), source, native, overlay, cases)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Schema != localHNSWAttributionTimingSchemaV1 || len(evidence.Cells) != 16 {
		t.Fatalf("evidence=%+v", evidence)
	}
	want := [4][4]struct {
		variant string
		probes  int
	}{{{"native", 2}, {"overlay", 2}, {"native", 4}, {"overlay", 4}}, {{"overlay", 4}, {"native", 4}, {"overlay", 2}, {"native", 2}}, {{"native", 4}, {"overlay", 4}, {"native", 2}, {"overlay", 2}}, {{"overlay", 2}, {"native", 2}, {"overlay", 4}, {"native", 4}}}
	seen := map[string][]string{}
	for i, cell := range evidence.Cells {
		w := want[i/4][i%4]
		if cell.Repetition != i/4 || cell.Variant != w.variant || cell.Probes != w.probes || cell.QueryCount != len(cases) || cell.ElapsedNanos == 0 || cell.QPS <= 0 || cell.P50Nanos == 0 || cell.P50Nanos > cell.P95Nanos || cell.P95Nanos > cell.P99Nanos || cell.Candidates == 0 || cell.Edges == 0 || len(cell.ResultSHA256) != len(cases) || cell.CPUAvailable && cell.CPUDeltaNanos < 0 {
			t.Fatalf("cell=%+v", cell)
		}
		key := cell.Variant + string(rune('0'+cell.Probes))
		if old, ok := seen[key]; ok && !slices.Equal(old, cell.ResultSHA256) {
			t.Fatalf("unstable result digest %q: %v != %v", key, old, cell.ResultSHA256)
		}
		seen[key] = cell.ResultSHA256
	}
	bad := append([]localHNSWAttributionTimingCaseV1(nil), cases...)
	bad[0].QueryFP32SHA256 = "wrong"
	if _, err := localHNSWAttributionTimingV1(t.Context(), source, native, overlay, bad); err == nil {
		t.Fatal("expected malformed query case rejection")
	}
	bad = append([]localHNSWAttributionTimingCaseV1(nil), cases...)
	bad[0].HighRoute = []uint32{0, 0, 1, 2}
	if _, err := localHNSWAttributionTimingV1(t.Context(), source, native, overlay, bad); err == nil {
		t.Fatal("expected malformed route rejection")
	}
	bad = append([]localHNSWAttributionTimingCaseV1(nil), cases...)
	for attempts := 0; attempts < 256 && localHNSWCalibrationOrdinalV1(bad[0].Ordinal); attempts++ {
		bad[0].Ordinal++
	}
	if localHNSWCalibrationOrdinalV1(bad[0].Ordinal) {
		t.Fatal("could not find non-calibration ordinal")
	}
	if err := localHNSWAttributionTimingCasesV1(bad, len(source.manifest.Assets)); err == nil {
		t.Fatal("expected non-calibration ordinal rejection")
	}
	if err := localHNSWAttributionTimingCasesV1(nil, len(source.manifest.Assets)); err == nil {
		t.Fatal("expected empty timing cases rejection")
	}
}
