package main

import "testing"

func TestLocalHNSWRepairMTimingGateV1(t *testing.T) {
	var cells []localHNSWRepairCalibrationTimingCellV1
	for repetition := 0; repetition < 4; repetition++ {
		for _, item := range []struct {
			variant string
			probes  int
		}{{"m16_efc128", 2}, {"m18_efc256", 2}, {"m16_efc128", 16}, {"m18_efc256", 16}} {
			cells = append(cells, localHNSWRepairCalibrationTimingCellV1{Repetition: repetition, Variant: item.variant, Probes: item.probes, QueryCount: 806, ElapsedNanos: 1, QPS: 100, P50Nanos: 1, P95Nanos: 2, P99Nanos: 3, Candidates: 1, NativeEdges: 1, ResultSHA256: make([]string, 806)})
			for i := range cells[len(cells)-1].ResultSHA256 {
				cells[len(cells)-1].ResultSHA256[i] = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
			}
		}
	}
	gate, err := localHNSWRepairMTimingGateV1Build(cells)
	if err != nil || gate.Disposition != "calibration_timing_gate_pass" || gate.P2QPSCandidateOverBaseline != 1 || gate.P16P95CandidateOverBaseline != 1 {
		t.Fatalf("gate=%+v err=%v", gate, err)
	}
	for i := range cells {
		if cells[i].Variant == "m18_efc256" && cells[i].Probes == 2 {
			cells[i].QPS = 80
		}
	}
	if gate, err = localHNSWRepairMTimingGateV1Build(cells); err != nil || gate.Disposition != "calibration_timing_gate_fail" {
		t.Fatalf("failed gate=%+v err=%v", gate, err)
	}
	if err := run([]string{"local-hnsw-repair-m-timing"}, nil); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
}
