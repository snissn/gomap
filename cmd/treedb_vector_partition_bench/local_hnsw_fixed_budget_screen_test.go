package main

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func localHNSWFixedBudgetScreenCloneV1(t *testing.T, report localHNSWFixedBudgetScreenReportV1) localHNSWFixedBudgetScreenReportV1 {
	t.Helper()
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var clone localHNSWFixedBudgetScreenReportV1
	if err := json.Unmarshal(encoded, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

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
		Limitations:  []string{"offline calibration-only selected-pack screen; no holdout outcomes opened", "same-budget postfill and robust-prune treatments only; no candidate extension, insertion-order, Vamana, router, probe, top-k, EF policy, or production-default changes"},
	}
	for i, arm := range localHNSWFixedBudgetScreenArmsV1 {
		identity, err := collections.VectorPartitionLocalGraphVariantIdentityV1(arm.Variant)
		if err != nil {
			t.Fatal(err)
		}
		report.Arms[i].Arm = arm
		report.Arms[i].Build = localHNSWAttributionBuildEvidenceV1{Variant: string(arm.Variant), VariantIdentity: identity, FileID: 4172000 + uint32(i), Partitions: 5, PackBytes: 1}
		report.Arms[i].SelectedDiagnostics = make([]localHNSWFixedBudgetDiagnosticV1, len(report.VariantPacks))
		for j := range report.Arms[i].SelectedDiagnostics {
			report.Arms[i].SelectedDiagnostics[j] = localHNSWFixedBudgetDiagnosticV1{Partition: report.VariantPacks[j], Diagnostics: collections.VectorPartitionPackDiagnosticsV1{Rows: 7500, EdgesByLayer: []uint64{270000}}}
		}
		report.Arms[i].SelectedNeighborhood = make([]localHNSWFixedBudgetPackNeighborhoodV1, len(report.VariantPacks))
		report.Arms[i].Neighborhood = localHNSWAttributionNeighborhoodOracleV1{Schema: localHNSWAttributionNeighborhoodOracleSchemaV1, OriginOrder: localHNSWAttributionConstructionOriginOrderV1, ExactK: localHNSWAttributionNeighborhoodExactKV1}
		for j, partition := range report.VariantPacks {
			one := localHNSWAttributionNeighborhoodOracleV1{Schema: localHNSWAttributionNeighborhoodOracleSchemaV1, OriginOrder: localHNSWAttributionConstructionOriginOrderV1, ExactK: localHNSWAttributionNeighborhoodExactKV1, CandidateSamples: 1, CandidateTruthNeighbors: 1, CandidateTruthRecovered: 1, FinalSamples: 1, FinalSampleTruthNeighbors: 1, FinalSampleTruthRecovered: 1, PackDiagnostics: []collections.VectorPartitionPackDiagnosticsV1{report.Arms[i].SelectedDiagnostics[j].Diagnostics}}
			report.Arms[i].SelectedNeighborhood[j] = localHNSWFixedBudgetPackNeighborhoodV1{Partition: partition, Neighborhood: one}
			report.Arms[i].Neighborhood.CandidateSamples++
			report.Arms[i].Neighborhood.CandidateTruthNeighbors++
			report.Arms[i].Neighborhood.CandidateTruthRecovered++
			report.Arms[i].Neighborhood.FinalSamples++
			report.Arms[i].Neighborhood.FinalSampleTruthNeighbors++
			report.Arms[i].Neighborhood.FinalSampleTruthRecovered++
			report.Arms[i].Neighborhood.PackDiagnostics = append(report.Arms[i].Neighborhood.PackDiagnostics, report.Arms[i].SelectedDiagnostics[j].Diagnostics)
		}
		report.Arms[i].Cells = make([]localHNSWFixedBudgetScreenCellV1, len(report.EFSearch))
		for j, ef := range report.EFSearch {
			packs := make([]localHNSWFixedBudgetPackWorkV1, len(report.VariantPacks))
			packs[0] = localHNSWFixedBudgetPackWorkV1{Partition: report.VariantPacks[0], Opportunities: 1, Work: localHNSWAttributionQueryWorkV1{Candidates: 1}}
			for k := 1; k < len(packs); k++ {
				packs[k].Partition = report.VariantPacks[k]
				packs[k].Opportunities = 1
				packs[k].Work.Candidates = 1
			}
			report.Arms[i].Cells[j] = localHNSWFixedBudgetScreenCellV1{EFSearch: ef, QueryPackOpportunities: uint64(len(packs)), Work: localHNSWAttributionQueryWorkV1{Candidates: uint64(len(packs))}, PerPack: packs}
		}
		if arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MBackfillOnV1 {
			report.Arms[i].Control = make([]localHNSWFixedBudgetControlPackV1, len(report.VariantPacks))
			for j := range report.Arms[i].Control {
				report.Arms[i].Control[j] = localHNSWFixedBudgetControlPackV1{Partition: report.VariantPacks[j], CandidateChecksum: localHNSWM18GraphSHA256V1, CanonicalChecksum: localHNSWM18GraphSHA256V1, CandidateGraphSHA256: localHNSWM18GraphSHA256V1, CanonicalGraphSHA256: localHNSWM18GraphSHA256V1, CandidateBytes: 1, CanonicalBytes: 1}
			}
		}
		if arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1 || arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1 {
			report.Arms[i].PostfillBudget = make([]localHNSWFixedBudgetPostfillBudgetV1, len(report.VariantPacks))
			for j := range report.Arms[i].PostfillBudget {
				report.Arms[i].PostfillBudget[j] = localHNSWFixedBudgetPostfillBudgetV1{Partition: report.VariantPacks[j], Rows: 7500, Layer0Edges: 270000, TargetLayer0Edges: 270000, CandidateBytes: 1, CanonicalBytes: 1}
			}
		}
	}
	if err := localHNSWFixedBudgetScreenContractV1(report); err != nil {
		t.Fatalf("valid screen contract rejected: %v", err)
	}

	// The fifth arm's final origins are query-visible native edges. Exercise a
	// real nonzero quality_postfill bucket through the cell contract rather
	// than relying on an all-zero synthetic utility.
	qualityUtility := localHNSWAttributionQueryUtilityV1{
		ExaminedNative:     1,
		NewlyVisited:       1,
		Scored:             1,
		TopAdmissions:      1,
		FrontierAdmissions: 1,
		QualityPostfill: localHNSWAttributionQueryOriginUtilityV1{
			Examined:           1,
			NewlyVisited:       1,
			Scored:             1,
			TopAdmissions:      1,
			FrontierAdmissions: 1,
		},
	}
	qualityArm := -1
	for i, arm := range report.Arms {
		if arm.Arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1 {
			qualityArm = i
			break
		}
	}
	if qualityArm < 0 {
		t.Fatal("missing quality-postfill arm")
	}
	for i := range report.Arms[qualityArm].Cells {
		cell := &report.Arms[qualityArm].Cells[i]
		var aggregate localHNSWAttributionQueryWorkV1
		for j := range cell.PerPack {
			cell.PerPack[j].Work = localHNSWAttributionQueryWorkV1{Candidates: 1, Edges: 1, FrontierAdmissions: 1, Utility: qualityUtility}
			if err := localHNSWM18EdgeDiagnosisWorkAddV1(&aggregate, 1, 1, 1, qualityUtility); err != nil {
				t.Fatal(err)
			}
		}
		cell.Work = aggregate
	}
	if err := localHNSWFixedBudgetScreenContractV1(report); err != nil {
		t.Fatalf("quality-postfill cell rejected: %v", err)
	}
	badQualityUtility := localHNSWFixedBudgetScreenCloneV1(t, report)
	badQualityUtility.Arms[qualityArm].Cells[0].PerPack[0].Work.Utility.QualityPostfill.Examined--
	if err := localHNSWFixedBudgetScreenContractV1(badQualityUtility); err == nil {
		t.Fatal("tampered quality-postfill utility accepted")
	}
	wrongEF := localHNSWFixedBudgetScreenCloneV1(t, report)
	wrongEF.Arms[0].Cells[0].EFSearch = 64
	if err := localHNSWFixedBudgetScreenContractV1(wrongEF); err == nil {
		t.Fatal("wrong EF grid accepted")
	}

	reorderedDiagnostics := localHNSWFixedBudgetScreenCloneV1(t, report)
	reorderedDiagnostics.Arms[0].SelectedDiagnostics[0], reorderedDiagnostics.Arms[0].SelectedDiagnostics[1] = reorderedDiagnostics.Arms[0].SelectedDiagnostics[1], reorderedDiagnostics.Arms[0].SelectedDiagnostics[0]
	if err := localHNSWFixedBudgetScreenContractV1(reorderedDiagnostics); err == nil {
		t.Fatal("reordered selected diagnostics accepted")
	}

	mismatchedPerPack := localHNSWFixedBudgetScreenCloneV1(t, report)
	mismatchedPerPack.Arms[0].Cells[0].PerPack[0].Partition = 999
	if err := localHNSWFixedBudgetScreenContractV1(mismatchedPerPack); err == nil {
		t.Fatal("mismatched per-pack partition accepted")
	}

	badAggregate := localHNSWFixedBudgetScreenCloneV1(t, report)
	badAggregate.Arms[0].Cells[0].Work.Candidates++
	if err := localHNSWFixedBudgetScreenContractV1(badAggregate); err == nil {
		t.Fatal("tampered aggregate work accepted")
	}

	reorderedControl := localHNSWFixedBudgetScreenCloneV1(t, report)
	control := 3
	reorderedControl.Arms[control].Control[0], reorderedControl.Arms[control].Control[1] = reorderedControl.Arms[control].Control[1], reorderedControl.Arms[control].Control[0]
	if err := localHNSWFixedBudgetScreenContractV1(reorderedControl); err == nil {
		t.Fatal("reordered M18 control accepted")
	}
	tamperedControl := localHNSWFixedBudgetScreenCloneV1(t, report)
	tamperedControl.Arms[control].Control[0].CandidateGraphSHA256 = localHNSWM18AssignmentSHA256V1
	if err := localHNSWFixedBudgetScreenContractV1(tamperedControl); err == nil {
		t.Fatal("identity-neutral M18 control mismatch accepted")
	}

	// Candidate membership identity is intentionally domain separated from the
	// canonical M18 pack. The control binds graph bytes and graph identity, not
	// the raw pack checksum.
	domainSeparatedControl := localHNSWFixedBudgetScreenCloneV1(t, report)
	domainSeparatedControl.Arms[control].Control[0].CandidateChecksum = localHNSWM18AssignmentSHA256V1
	if err := localHNSWFixedBudgetScreenContractV1(domainSeparatedControl); err != nil {
		t.Fatalf("domain-separated raw candidate checksum rejected: %v", err)
	}

	badPostfill := localHNSWFixedBudgetScreenCloneV1(t, report)
	badPostfill.Arms[qualityArm].PostfillBudget[0].Layer0Edges--
	if err := localHNSWFixedBudgetScreenContractV1(badPostfill); err == nil {
		t.Fatal("underfilled least-redundant separation postfill accepted")
	}
	wrongPostfillArm := localHNSWFixedBudgetScreenCloneV1(t, report)
	wrongPostfillArm.Arms[0].PostfillBudget = append([]localHNSWFixedBudgetPostfillBudgetV1(nil), report.Arms[len(report.Arms)-1].PostfillBudget...)
	if err := localHNSWFixedBudgetScreenContractV1(wrongPostfillArm); err == nil {
		t.Fatal("postfill budget accepted on a non-postfill arm")
	}

	reorderedNeighborhood := localHNSWFixedBudgetScreenCloneV1(t, report)
	reorderedNeighborhood.Arms[0].SelectedNeighborhood[0], reorderedNeighborhood.Arms[0].SelectedNeighborhood[1] = reorderedNeighborhood.Arms[0].SelectedNeighborhood[1], reorderedNeighborhood.Arms[0].SelectedNeighborhood[0]
	if err := localHNSWFixedBudgetScreenContractV1(reorderedNeighborhood); err == nil {
		t.Fatal("reordered selected neighborhood accepted")
	}

	badNeighborhoodAggregate := localHNSWFixedBudgetScreenCloneV1(t, report)
	badNeighborhoodAggregate.Arms[0].Neighborhood.CandidateSamples++
	if err := localHNSWFixedBudgetScreenContractV1(badNeighborhoodAggregate); err == nil {
		t.Fatal("tampered neighborhood aggregate accepted")
	}

	varyingOpportunities := localHNSWFixedBudgetScreenCloneV1(t, report)
	varyingOpportunities.Arms[1].Cells[0].PerPack[0].Opportunities++
	varyingOpportunities.Arms[1].Cells[0].QueryPackOpportunities++
	if err := localHNSWFixedBudgetScreenContractV1(varyingOpportunities); err == nil {
		t.Fatal("EF or arm-dependent query opportunities accepted")
	}
}

func TestLocalHNSWFixedBudgetScreenRejectsMissingInputsV1(t *testing.T) {
	if err := runLocalHNSWFixedBudgetScreenV1(nil, io.Discard); err == nil {
		t.Fatal("screen accepted missing frozen inputs")
	}
}
