package main

import (
	"io"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWFixedBudgetScreenContractV1(t *testing.T) {
	report := localHNSWFixedBudgetScreenReportV1{
		Schema:       localHNSWFixedBudgetScreenSchemaV1,
		ResultKind:   "local_hnsw_fixed_budget_screen_v1",
		Status:       "valid",
		VariantPacks: append([]uint32(nil), localHNSWM18EdgeDiagnosisPacksV1...),
		Probes:       2,
		EFSearch:     append([]int(nil), localHNSWM18EdgeDiagnosisEFV1...),
		Queries:      806,
		Arms:         make([]localHNSWFixedBudgetScreenArmResultV1, len(localHNSWFixedBudgetScreenArmsV1)),
	}
	for i, arm := range localHNSWFixedBudgetScreenArmsV1 {
		report.Arms[i].Arm = arm
		report.Arms[i].Build = localHNSWAttributionBuildEvidenceV1{Variant: string(arm.Variant), Partitions: 40, PackBytes: 1}
		report.Arms[i].SelectedDiagnostics = make([]collections.VectorPartitionPackDiagnosticsV1, len(report.VariantPacks))
		for j := range report.Arms[i].SelectedDiagnostics {
			report.Arms[i].SelectedDiagnostics[j].Rows = 1
		}
		report.Arms[i].Cells = make([]localHNSWM18EdgeDiagnosisCellV1, len(report.EFSearch))
		report.Arms[i].TruthHitSlots = make([]uint64, len(report.EFSearch))
		for j, ef := range report.EFSearch {
			report.Arms[i].Cells[j] = localHNSWM18EdgeDiagnosisCellV1{EFSearch: ef, Queries: 806, Work: localHNSWAttributionQueryWorkV1{Candidates: 1}}
		}
	}
	if err := localHNSWFixedBudgetScreenContractV1(report); err != nil {
		t.Fatalf("valid screen contract rejected: %v", err)
	}
	report.Arms[0].Cells[0].EFSearch = 64
	if err := localHNSWFixedBudgetScreenContractV1(report); err == nil {
		t.Fatal("wrong EF grid accepted")
	}
}

func TestLocalHNSWFixedBudgetScreenRejectsMissingInputsV1(t *testing.T) {
	if err := runLocalHNSWFixedBudgetScreenV1(nil, io.Discard); err == nil {
		t.Fatal("screen accepted missing frozen inputs")
	}
}
