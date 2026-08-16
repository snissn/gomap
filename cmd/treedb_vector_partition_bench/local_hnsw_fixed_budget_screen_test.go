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
		Provenance:   localHNSWAttributionProvenanceV1{BaseSHA: "2a7d01443d3c842990c259b08bd442a4d0109511", HeadSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExecutableSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Calibration:  localHNSWAttributionFileInputV1{SHA256: localHNSWAttributionCalibrationSHA256V1},
		Truth:        localHNSWAttributionFileInputV1{SHA256: localHNSWAttributionTruthSHA256V1},
		Descriptor:   localHNSWAttributionFileInputV1{SHA256: localHNSWM18DescriptorSHA256V1},
		Manifest:     "manifest",
		Source:       localHNSWAttributionSourceEvidenceV1{ManifestIntegrity: "manifest", Descriptor: m3VariantDescriptorV1{ArtifactSHA256: localHNSWM18AssignmentSHA256V1, GraphArtifactSHA256: localHNSWM18GraphSHA256V1, ShardGenerationDigest: localHNSWM18ShardGenerationSHA256V1}},
	}
	for i, arm := range localHNSWFixedBudgetScreenArmsV1 {
		report.Arms[i].Arm = arm
		report.Arms[i].Build = localHNSWAttributionBuildEvidenceV1{Variant: string(arm.Variant), VariantIdentity: "identity", FileID: 4172000 + uint32(i), Partitions: 5, PackBytes: 1}
		report.Arms[i].SelectedDiagnostics = make([]collections.VectorPartitionPackDiagnosticsV1, len(report.VariantPacks))
		for j := range report.Arms[i].SelectedDiagnostics {
			report.Arms[i].SelectedDiagnostics[j].Rows = 1
		}
		report.Arms[i].Cells = make([]localHNSWFixedBudgetScreenCellV1, len(report.EFSearch))
		for j, ef := range report.EFSearch {
			report.Arms[i].Cells[j] = localHNSWFixedBudgetScreenCellV1{EFSearch: ef, QueryPackOpportunities: 1, Work: localHNSWAttributionQueryWorkV1{Candidates: 1}, PerPack: make([]localHNSWFixedBudgetPackWorkV1, len(report.VariantPacks))}
		}
		if i == len(report.Arms)-1 {
			report.Arms[i].Control = make([]localHNSWFixedBudgetControlPackV1, len(report.VariantPacks))
			for j := range report.Arms[i].Control {
				report.Arms[i].Control[j] = localHNSWFixedBudgetControlPackV1{CandidateChecksum: "x", CanonicalChecksum: "x", CandidateBytes: 1, CanonicalBytes: 1}
			}
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
