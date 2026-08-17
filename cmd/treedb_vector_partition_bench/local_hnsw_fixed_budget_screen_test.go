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

func localHNSWFixedBudgetScreenTestDiagnosticV1() collections.VectorPartitionPackDiagnosticsV1 {
	return collections.VectorPartitionPackDiagnosticsV1{
		Rows:                    localHNSWFixedBudgetRetainedRowsV1,
		ReachableRows:           localHNSWFixedBudgetRetainedRowsV1,
		TraversalRoots:          1,
		RowsByLayer:             []uint64{localHNSWFixedBudgetRetainedRowsV1},
		EdgesByLayer:            []uint64{localHNSWFixedBudgetTargetLayer0EdgesV1},
		Layer0DegreeLimit:       localHNSWFixedBudgetLayer0SlotsV1,
		Layer0SaturatedRows:     localHNSWFixedBudgetRetainedRowsV1,
		Layer0ReciprocalEdges:   localHNSWFixedBudgetTargetLayer0EdgesV1,
		Layer0ReciprocalRatio:   1,
		Layer0DegreeHistogram:   map[uint64]uint64{localHNSWFixedBudgetLayer0SlotsV1: localHNSWFixedBudgetRetainedRowsV1},
		Layer0IndegreeHistogram: map[uint64]uint64{localHNSWFixedBudgetLayer0SlotsV1: localHNSWFixedBudgetRetainedRowsV1},
		Layer0StrongComponents:  1,
		Layer0LargestComponent:  localHNSWFixedBudgetRetainedRowsV1,
		Layer0Distances:         collections.VectorPartitionLocalGraphDistanceDistributionV1{Count: localHNSWFixedBudgetTargetLayer0EdgesV1},
		AuxiliaryCSRBytes:       (localHNSWFixedBudgetRetainedRowsV1 + 1) * 8,
		CombinedReachableRows:   localHNSWFixedBudgetRetainedRowsV1,
	}
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
		Provenance:   localHNSWAttributionProvenanceV1{BaseSHA: localHNSWFixedBudgetScreenBaseSHAV1, HeadSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExecutableSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Calibration:  localHNSWAttributionFileInputV1{SHA256: localHNSWAttributionCalibrationSHA256V1},
		Truth:        localHNSWAttributionFileInputV1{SHA256: localHNSWAttributionTruthSHA256V1},
		Descriptor:   localHNSWAttributionFileInputV1{SHA256: localHNSWM18DescriptorSHA256V1},
		Manifest:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Source:       localHNSWAttributionSourceEvidenceV1{IndexName: "embedding", PartitionGeneration: 1, Partitions: localHNSWFixedBudgetScreenSourcePartitionsV1, ManifestIntegrity: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ReadySetDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SourceGeneration: 1, SourceChecksum: 1, SourceSchemaHash: 1, SourceRows: localHNSWFixedBudgetScreenSourceRowsV1, RouterGeneration: 1, RouterModelDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RouterRepresentatives: 1, PartitionLoads: make([]uint64, localHNSWFixedBudgetScreenSourcePartitionsV1), Descriptor: m3VariantDescriptorV1{ArtifactSHA256: localHNSWM18AssignmentSHA256V1, GraphArtifactSHA256: localHNSWM18GraphSHA256V1, ShardGenerationDigest: localHNSWM18ShardGenerationSHA256V1, ManifestIntegrity: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ReadySetDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SourceGeneration: 1, SourceChecksum: 1, SourceSchemaHash: 1, SourceRows: localHNSWFixedBudgetScreenSourceRowsV1, PartitionGeneration: 1, RouterGeneration: 1, Partitions: localHNSWFixedBudgetScreenSourcePartitionsV1, PartitionHNSWM: 18, PartitionHNSWEfC: 256, RouterModelDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RouterRepresentatives: 1, PartitionLoads: make([]int, localHNSWFixedBudgetScreenSourcePartitionsV1)}},
		Limitations:  []string{localHNSWFixedBudgetScreenCalibrationLimitationV1, localHNSWFixedBudgetScreenTreatmentLimitationV1},
	}
	for i := range report.Source.PartitionLoads {
		report.Source.PartitionLoads[i] = 6250
		report.Source.Descriptor.PartitionLoads[i] = 6250
	}
	for i, arm := range localHNSWFixedBudgetScreenArmsV1 {
		identity, err := collections.VectorPartitionLocalGraphVariantIdentityV1(arm.Variant)
		if err != nil {
			t.Fatal(err)
		}
		report.Arms[i].Arm = arm
		report.Arms[i].Build = localHNSWAttributionBuildEvidenceV1{Schema: localHNSWAttributionBuildSchemaV1, Variant: string(arm.Variant), VariantIdentity: identity, FileID: 4172000 + uint32(i), Partitions: len(report.VariantPacks), PackBytes: 1}
		report.Arms[i].SelectedDiagnostics = make([]localHNSWFixedBudgetDiagnosticV1, len(report.VariantPacks))
		for j := range report.Arms[i].SelectedDiagnostics {
			report.Arms[i].SelectedDiagnostics[j] = localHNSWFixedBudgetDiagnosticV1{Partition: report.VariantPacks[j], Diagnostics: localHNSWFixedBudgetScreenTestDiagnosticV1()}
		}
		report.Arms[i].SelectedNeighborhood = make([]localHNSWFixedBudgetPackNeighborhoodV1, len(report.VariantPacks))
		report.Arms[i].Neighborhood = localHNSWAttributionNeighborhoodOracleV1{Schema: localHNSWAttributionNeighborhoodOracleSchemaV1, OriginOrder: localHNSWAttributionConstructionOriginOrderV1, ExactK: localHNSWAttributionNeighborhoodExactKV1}
		for j, partition := range report.VariantPacks {
			one := localHNSWAttributionNeighborhoodOracleV1{Schema: localHNSWAttributionNeighborhoodOracleSchemaV1, OriginOrder: localHNSWAttributionConstructionOriginOrderV1, ExactK: localHNSWAttributionNeighborhoodExactKV1, CandidateSamples: localHNSWFixedBudgetScreenCanonicalSampleCountV1, CandidateTruthNeighbors: localHNSWFixedBudgetScreenCanonicalSampleCountV1, CandidateTruthRecovered: localHNSWFixedBudgetScreenCanonicalSampleCountV1, FinalSamples: localHNSWFixedBudgetScreenCanonicalSampleCountV1, FinalSampleTruthNeighbors: localHNSWFixedBudgetScreenCanonicalSampleCountV1 * uint64(localHNSWAttributionNeighborhoodExactKV1), FinalSampleTruthRecovered: localHNSWFixedBudgetScreenCanonicalSampleCountV1, PackDiagnostics: []collections.VectorPartitionPackDiagnosticsV1{report.Arms[i].SelectedDiagnostics[j].Diagnostics}}
			if arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1 || arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1 {
				one.AngularPairs = localHNSWFixedBudgetScreenCanonicalSampleCountV1 * localHNSWFixedBudgetLayer0SlotsV1 * (localHNSWFixedBudgetLayer0SlotsV1 - 1) / 2
				one.AngularCosineDistanceMean = 0.25
			}
			one.FinalEdgesByOrigin[0], one.FinalTruthByOrigin[0] = localHNSWFixedBudgetScreenCanonicalSampleCountV1, localHNSWFixedBudgetScreenCanonicalSampleCountV1
			switch arm.Variant {
			case collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1:
				one.FinalEdgesByOrigin[0] = localHNSWFixedBudgetScreenCanonicalSampleCountV1 * (localHNSWFixedBudgetLayer0SlotsV1 - 1)
				one.FinalEdgesByOrigin[5] = localHNSWFixedBudgetScreenCanonicalSampleCountV1
			case collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1:
				one.FinalEdgesByOrigin[0] = localHNSWFixedBudgetScreenCanonicalSampleCountV1 * (localHNSWFixedBudgetLayer0SlotsV1 - 2)
				one.FinalEdgesByOrigin[6], one.FinalEdgesByOrigin[7] = localHNSWFixedBudgetScreenCanonicalSampleCountV1, localHNSWFixedBudgetScreenCanonicalSampleCountV1
			}
			report.Arms[i].SelectedNeighborhood[j] = localHNSWFixedBudgetPackNeighborhoodV1{Partition: partition, Neighborhood: one}
			report.Arms[i].Neighborhood.CandidateSamples += one.CandidateSamples
			report.Arms[i].Neighborhood.CandidateTruthNeighbors += one.CandidateTruthNeighbors
			report.Arms[i].Neighborhood.CandidateTruthRecovered += one.CandidateTruthRecovered
			report.Arms[i].Neighborhood.FinalSamples += one.FinalSamples
			report.Arms[i].Neighborhood.FinalSampleTruthNeighbors += one.FinalSampleTruthNeighbors
			report.Arms[i].Neighborhood.FinalSampleTruthRecovered += one.FinalSampleTruthRecovered
			report.Arms[i].Neighborhood.AngularPairs += one.AngularPairs
			for origin := range one.FinalEdgesByOrigin {
				report.Arms[i].Neighborhood.FinalEdgesByOrigin[origin] += one.FinalEdgesByOrigin[origin]
				report.Arms[i].Neighborhood.FinalTruthByOrigin[origin] += one.FinalTruthByOrigin[origin]
			}
			report.Arms[i].Neighborhood.PackDiagnostics = append(report.Arms[i].Neighborhood.PackDiagnostics, report.Arms[i].SelectedDiagnostics[j].Diagnostics)
		}
		if report.Arms[i].Neighborhood.AngularPairs != 0 {
			report.Arms[i].Neighborhood.AngularCosineDistanceMean = 0.25
		}
		report.Arms[i].Cells = make([]localHNSWFixedBudgetScreenCellV1, len(report.EFSearch))
		for j, ef := range report.EFSearch {
			packs := make([]localHNSWFixedBudgetPackWorkV1, len(report.VariantPacks))
			for k := range packs {
				packs[k].Partition = report.VariantPacks[k]
				packs[k].Opportunities = localHNSWFixedBudgetScreenExpectedPerPackOpportunitiesV1[k]
				packs[k].Work.Candidates = localHNSWFixedBudgetScreenExpectedPerPackOpportunitiesV1[k]
			}
			report.Arms[i].Cells[j] = localHNSWFixedBudgetScreenCellV1{EFSearch: ef, QueryPackOpportunities: localHNSWFixedBudgetScreenExpectedOpportunitiesV1, Work: localHNSWAttributionQueryWorkV1{Candidates: localHNSWFixedBudgetScreenExpectedOpportunitiesV1}, PerPack: packs}
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
	badSourceBinding := localHNSWFixedBudgetScreenCloneV1(t, report)
	badSourceBinding.Source.Descriptor.SourceRows--
	if err := localHNSWFixedBudgetScreenContractV1(badSourceBinding); err == nil {
		t.Fatal("mismatched retained source descriptor accepted")
	}
	badCandidateRecovery := localHNSWFixedBudgetScreenCloneV1(t, report)
	badCandidateRecovery.Arms[0].SelectedNeighborhood[0].Neighborhood.CandidateTruthRecovered++
	if err := localHNSWFixedBudgetScreenContractV1(badCandidateRecovery); err == nil {
		t.Fatal("impossible per-pack candidate recovery accepted")
	}
	badCandidateBound := localHNSWFixedBudgetScreenCloneV1(t, report)
	badCandidateBound.Arms[0].SelectedNeighborhood[0].Neighborhood.CandidateTruthNeighbors = localHNSWFixedBudgetScreenCanonicalSampleCountV1*uint64(localHNSWAttributionNeighborhoodExactKV1) + 1
	if err := localHNSWFixedBudgetScreenContractV1(badCandidateBound); err == nil || err.Error() != "invalid fixed-budget neighborhood truth bound" {
		t.Fatalf("per-pack candidate truth beyond exact-k sample bound err=%v", err)
	}
	badFinalBound := localHNSWFixedBudgetScreenCloneV1(t, report)
	badFinalBound.Arms[0].SelectedNeighborhood[0].Neighborhood.FinalSampleTruthNeighbors = localHNSWFixedBudgetScreenCanonicalSampleCountV1*uint64(localHNSWAttributionNeighborhoodExactKV1) + 1
	if err := localHNSWFixedBudgetScreenContractV1(badFinalBound); err == nil || err.Error() != "invalid fixed-budget neighborhood truth bound" {
		t.Fatalf("per-pack final truth beyond exact-k sample bound err=%v", err)
	}
	shortFinalDenominator := localHNSWFixedBudgetScreenCloneV1(t, report)
	shortFinalDenominator.Arms[0].SelectedNeighborhood[0].Neighborhood.FinalSampleTruthNeighbors--
	if err := localHNSWFixedBudgetScreenContractV1(shortFinalDenominator); err == nil || err.Error() != "invalid fixed-budget neighborhood truth bound" {
		t.Fatalf("per-pack final truth below exact-k sample denominator err=%v", err)
	}
	badTruthBoundOverflow := localHNSWFixedBudgetScreenCloneV1(t, report)
	badTruthBoundOverflow.Arms[0].SelectedNeighborhood[0].Neighborhood.CandidateSamples = ^uint64(0)
	if err := localHNSWFixedBudgetScreenContractV1(badTruthBoundOverflow); err == nil || err.Error() != "fixed-budget neighborhood truth bound overflow" {
		t.Fatalf("overflowing per-pack exact-k truth bound err=%v", err)
	}
	badSampleBinding := localHNSWFixedBudgetScreenCloneV1(t, report)
	badSampleBinding.Arms[0].SelectedNeighborhood[0].Neighborhood.CandidateSamples++
	if err := localHNSWFixedBudgetScreenContractV1(badSampleBinding); err == nil {
		t.Fatal("mismatched per-pack candidate and final sample counts accepted")
	}
	crossArmDenominator := localHNSWFixedBudgetScreenCloneV1(t, report)
	crossArmPack := &crossArmDenominator.Arms[1].SelectedNeighborhood[0].Neighborhood
	crossArmPack.CandidateSamples++
	crossArmPack.CandidateTruthNeighbors++
	crossArmPack.FinalSamples++
	crossArmPack.FinalSampleTruthNeighbors++
	crossArmAggregate := &crossArmDenominator.Arms[1].Neighborhood
	crossArmAggregate.CandidateSamples++
	crossArmAggregate.CandidateTruthNeighbors++
	crossArmAggregate.FinalSamples++
	crossArmAggregate.FinalSampleTruthNeighbors++
	if err := localHNSWFixedBudgetScreenContractV1(crossArmDenominator); err == nil {
		t.Fatal("cross-arm sample denominators accepted")
	}
	uniformlyReducedSamples := localHNSWFixedBudgetScreenCloneV1(t, report)
	for i := range uniformlyReducedSamples.Arms {
		arm := &uniformlyReducedSamples.Arms[i]
		for j := range arm.SelectedNeighborhood {
			one := &arm.SelectedNeighborhood[j].Neighborhood
			one.CandidateSamples--
			one.CandidateTruthNeighbors--
			one.CandidateTruthRecovered--
			one.FinalSamples--
			one.FinalSampleTruthNeighbors--
			one.FinalSampleTruthRecovered--
			one.FinalEdgesByOrigin = [8]uint64{}
			one.FinalTruthByOrigin = [8]uint64{}
			one.FinalEdgesByOrigin[0] = localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1
			one.FinalTruthByOrigin[0] = localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1
			one.AngularPairs = 0
			one.AngularCosineDistanceMean = 0
			switch arm.Arm.Variant {
			case collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1:
				one.FinalEdgesByOrigin[0] = (localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1) * (localHNSWFixedBudgetLayer0SlotsV1 - 1)
				one.FinalEdgesByOrigin[5] = localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1
				one.AngularPairs = (localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1) * localHNSWFixedBudgetLayer0SlotsV1 * (localHNSWFixedBudgetLayer0SlotsV1 - 1) / 2
				one.AngularCosineDistanceMean = 0.25
			case collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1:
				one.FinalEdgesByOrigin[0] = (localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1) * (localHNSWFixedBudgetLayer0SlotsV1 - 2)
				one.FinalEdgesByOrigin[6], one.FinalEdgesByOrigin[7] = localHNSWFixedBudgetScreenCanonicalSampleCountV1-1, localHNSWFixedBudgetScreenCanonicalSampleCountV1-1
				one.AngularPairs = (localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1) * localHNSWFixedBudgetLayer0SlotsV1 * (localHNSWFixedBudgetLayer0SlotsV1 - 1) / 2
				one.AngularCosineDistanceMean = 0.25
			}
		}
		localHNSWFixedBudgetScreenTestRecomposeNeighborhoodV1(t, arm)
	}
	if err := localHNSWFixedBudgetScreenContractV1(uniformlyReducedSamples); err == nil {
		t.Fatal("uniformly reduced canonical construction samples accepted")
	}
	badPerPackAngularMean := localHNSWFixedBudgetScreenCloneV1(t, report)
	badPerPackAngularMean.Arms[0].SelectedNeighborhood[0].Neighborhood.AngularCosineDistanceMean = -1
	if err := localHNSWFixedBudgetScreenContractV1(badPerPackAngularMean); err == nil {
		t.Fatal("out-of-range per-pack angular cosine distance accepted")
	}
	badFinalCapacity := localHNSWFixedBudgetScreenCloneV1(t, report)
	badFinalCapacity.Arms[0].SelectedNeighborhood[0].Neighborhood.FinalEdgesByOrigin[0] = localHNSWFixedBudgetScreenCanonicalSampleCountV1*localHNSWFixedBudgetLayer0SlotsV1 + 1
	if err := localHNSWFixedBudgetScreenContractV1(badFinalCapacity); err == nil {
		t.Fatal("per-pack final edge capacity overflow accepted")
	}
	badDegreeLimit := localHNSWFixedBudgetScreenCloneV1(t, report)
	badDegreeLimit.Arms[0].SelectedDiagnostics[0].Diagnostics.Layer0DegreeLimit++
	badDegreeLimit.Arms[0].SelectedNeighborhood[0].Neighborhood.PackDiagnostics[0].Layer0DegreeLimit++
	badDegreeLimit.Arms[0].Neighborhood.PackDiagnostics[0].Layer0DegreeLimit++
	badDegreeLimit.Arms[0].SelectedNeighborhood[0].Neighborhood.FinalEdgesByOrigin[0] = localHNSWFixedBudgetLayer0SlotsV1 + 1
	badDegreeLimit.Arms[0].Neighborhood.FinalEdgesByOrigin[0] += localHNSWFixedBudgetLayer0SlotsV1
	if err := localHNSWFixedBudgetScreenContractV1(badDegreeLimit); err == nil {
		t.Fatal("non-fixed per-pack final edge capacity accepted")
	}
	badDiagnostic := localHNSWFixedBudgetScreenCloneV1(t, report)
	badDiagnostic.Arms[0].SelectedDiagnostics[0].Diagnostics.Layer0StrongComponents = 0
	if err := localHNSWFixedBudgetScreenContractV1(badDiagnostic); err == nil {
		t.Fatal("structurally impossible selected diagnostic accepted")
	}
	missingAuxiliaryDiagnostics := localHNSWFixedBudgetScreenCloneV1(t, report)
	for i := range missingAuxiliaryDiagnostics.Arms {
		for j := range missingAuxiliaryDiagnostics.Arms[i].SelectedDiagnostics {
			missingAuxiliaryDiagnostics.Arms[i].SelectedDiagnostics[j].Diagnostics.AuxiliaryEdges = 0
			missingAuxiliaryDiagnostics.Arms[i].SelectedDiagnostics[j].Diagnostics.AuxiliaryCSRBytes = 0
			missingAuxiliaryDiagnostics.Arms[i].SelectedDiagnostics[j].Diagnostics.AuxiliaryMaxDegree = 0
			missingAuxiliaryDiagnostics.Arms[i].SelectedNeighborhood[j].Neighborhood.PackDiagnostics[0].AuxiliaryEdges = 0
			missingAuxiliaryDiagnostics.Arms[i].SelectedNeighborhood[j].Neighborhood.PackDiagnostics[0].AuxiliaryCSRBytes = 0
			missingAuxiliaryDiagnostics.Arms[i].SelectedNeighborhood[j].Neighborhood.PackDiagnostics[0].AuxiliaryMaxDegree = 0
			missingAuxiliaryDiagnostics.Arms[i].Neighborhood.PackDiagnostics[j].AuxiliaryEdges = 0
			missingAuxiliaryDiagnostics.Arms[i].Neighborhood.PackDiagnostics[j].AuxiliaryCSRBytes = 0
			missingAuxiliaryDiagnostics.Arms[i].Neighborhood.PackDiagnostics[j].AuxiliaryMaxDegree = 0
		}
	}
	if err := localHNSWFixedBudgetScreenContractV1(missingAuxiliaryDiagnostics); err == nil {
		t.Fatal("missing fixed-budget auxiliary diagnostics accepted")
	}
	badAngularPairs := localHNSWFixedBudgetScreenCloneV1(t, report)
	badAngularPack := &badAngularPairs.Arms[0].SelectedNeighborhood[0].Neighborhood
	badAngularPack.FinalEdgesByOrigin[0] = 1
	badAngularPack.FinalTruthByOrigin[0] = 1
	badAngularPack.FinalSampleTruthRecovered = 1
	badAngularPack.AngularPairs = 1
	badAngularPack.AngularCosineDistanceMean = 0.25
	badAngularPairs.Arms[0].Neighborhood.FinalEdgesByOrigin[0] -= localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1
	badAngularPairs.Arms[0].Neighborhood.FinalTruthByOrigin[0] -= localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1
	badAngularPairs.Arms[0].Neighborhood.FinalSampleTruthRecovered -= localHNSWFixedBudgetScreenCanonicalSampleCountV1 - 1
	badAngularPairs.Arms[0].Neighborhood.AngularPairs = 1
	badAngularPairs.Arms[0].Neighborhood.AngularCosineDistanceMean = 0.25
	if err := localHNSWFixedBudgetScreenContractV1(badAngularPairs); err == nil {
		t.Fatal("angular pair without two sampled final edges accepted")
	}
	badBuildSchema := localHNSWFixedBudgetScreenCloneV1(t, report)
	badBuildSchema.Arms[0].Build.Schema = ""
	if err := localHNSWFixedBudgetScreenContractV1(badBuildSchema); err == nil {
		t.Fatal("arm build without attribution schema accepted")
	}
	badOriginRecovery := localHNSWFixedBudgetScreenCloneV1(t, report)
	badOriginRecovery.Arms[0].SelectedNeighborhood[0].Neighborhood.FinalTruthByOrigin[0]++
	if err := localHNSWFixedBudgetScreenContractV1(badOriginRecovery); err == nil {
		t.Fatal("impossible per-pack final-origin recovery accepted")
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
	noOpTreatment := localHNSWFixedBudgetScreenCloneV1(t, report)
	for i := range noOpTreatment.Arms[qualityArm].SelectedNeighborhood {
		noOpTreatment.Arms[qualityArm].SelectedNeighborhood[i].Neighborhood.FinalEdgesByOrigin[5] = 0
	}
	noOpTreatment.Arms[qualityArm].Neighborhood.FinalEdgesByOrigin[5] = 0
	if err := localHNSWFixedBudgetScreenContractV1(noOpTreatment); err == nil {
		t.Fatal("quality-postfill no-op treatment accepted")
	}
	robustArm := localHNSWFixedBudgetScreenArmIndexV1(t, report, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1)
	zeroResidual := localHNSWFixedBudgetScreenCloneV1(t, report)
	for i := range zeroResidual.Arms[robustArm].SelectedNeighborhood {
		counts := &zeroResidual.Arms[robustArm].SelectedNeighborhood[i].Neighborhood.FinalEdgesByOrigin
		counts[0] += counts[7]
		counts[7] = 0
	}
	zeroResidual.Arms[robustArm].Neighborhood.FinalEdgesByOrigin[0] += zeroResidual.Arms[robustArm].Neighborhood.FinalEdgesByOrigin[7]
	zeroResidual.Arms[robustArm].Neighborhood.FinalEdgesByOrigin[7] = 0
	if err := localHNSWFixedBudgetScreenContractV1(zeroResidual); err != nil {
		t.Fatalf("robust treatment with no residual fill rejected: %v", err)
	}
	noOpRobust := localHNSWFixedBudgetScreenCloneV1(t, report)
	for i := range noOpRobust.Arms[robustArm].SelectedNeighborhood {
		noOpRobust.Arms[robustArm].SelectedNeighborhood[i].Neighborhood.FinalEdgesByOrigin[6] = 0
	}
	noOpRobust.Arms[robustArm].Neighborhood.FinalEdgesByOrigin[6] = 0
	if err := localHNSWFixedBudgetScreenContractV1(noOpRobust); err == nil {
		t.Fatal("robust-prune no-op treatment accepted")
	}
	wrongArmOrigin := localHNSWFixedBudgetScreenCloneV1(t, report)
	wrongArmOrigin.Arms[0].SelectedNeighborhood[0].Neighborhood.FinalEdgesByOrigin[6] = 1
	wrongArmOrigin.Arms[0].Neighborhood.FinalEdgesByOrigin[6]++
	if err := localHNSWFixedBudgetScreenContractV1(wrongArmOrigin); err == nil {
		t.Fatal("non-robust arm carrying robust-prune origin accepted")
	}
	wrongResidualOrigin := localHNSWFixedBudgetScreenCloneV1(t, report)
	wrongResidualOrigin.Arms[0].SelectedNeighborhood[0].Neighborhood.FinalEdgesByOrigin[7] = 1
	wrongResidualOrigin.Arms[0].Neighborhood.FinalEdgesByOrigin[7]++
	if err := localHNSWFixedBudgetScreenContractV1(wrongResidualOrigin); err == nil {
		t.Fatal("non-robust arm carrying robust residual origin accepted")
	}
	backfillUtility := localHNSWAttributionQueryUtilityV1{
		ExaminedNative:     1,
		NewlyVisited:       1,
		Scored:             1,
		TopAdmissions:      1,
		FrontierAdmissions: 1,
		TruthRecovered:     1,
		Backfill: localHNSWAttributionQueryOriginUtilityV1{
			Examined:           1,
			NewlyVisited:       1,
			Scored:             1,
			TopAdmissions:      1,
			FrontierAdmissions: 1,
			TruthRecovered:     1,
		},
	}
	backfillOffOrigin := localHNSWFixedBudgetScreenCloneV1(t, report)
	localHNSWFixedBudgetScreenTestMoveOriginV1(t, &backfillOffOrigin.Arms[0], 1, backfillUtility)
	if err := localHNSWFixedBudgetScreenContractV1(backfillOffOrigin); err == nil {
		t.Fatal("backfill-off arm carrying backfill origin accepted")
	}
	if err := localHNSWFixedBudgetScreenQueryUtilityV1(backfillOffOrigin.Arms[0].Arm.Variant, backfillUtility, 1); err == nil {
		t.Fatal("backfill-off query utility accepted backfill origin")
	}
	overlayUtility := backfillUtility
	overlayUtility.Backfill = localHNSWAttributionQueryOriginUtilityV1{}
	overlayUtility.Overlay = localHNSWAttributionQueryOriginUtilityV1{
		Examined:           1,
		NewlyVisited:       1,
		Scored:             1,
		TopAdmissions:      1,
		FrontierAdmissions: 1,
		TruthRecovered:     1,
	}
	overlayOrigin := localHNSWFixedBudgetScreenCloneV1(t, report)
	localHNSWFixedBudgetScreenTestMoveOriginV1(t, &overlayOrigin.Arms[1], 4, overlayUtility)
	if err := localHNSWFixedBudgetScreenContractV1(overlayOrigin); err == nil {
		t.Fatal("auxiliary-navigation arm carrying overlay origin accepted")
	}
	if err := localHNSWFixedBudgetScreenQueryUtilityV1(overlayOrigin.Arms[1].Arm.Variant, overlayUtility, 1); err == nil {
		t.Fatal("auxiliary-navigation query utility accepted overlay origin")
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
	// Work.Candidates and Work.FrontierAdmissions are persisted separately from
	// utility. Exercise a nontrivial per-pack residual so the screen cannot
	// claim fewer candidates or admissions than that utility consumed.
	counterUtility := qualityUtility
	counterUtility.ExaminedNative = 2
	counterUtility.NewlyVisited = 2
	counterUtility.Scored = 2
	counterUtility.TopAdmissions = 2
	counterUtility.FrontierAdmissions = 2
	counterUtility.QualityPostfill = localHNSWAttributionQueryOriginUtilityV1{
		Examined:           2,
		NewlyVisited:       2,
		Scored:             2,
		TopAdmissions:      2,
		FrontierAdmissions: 2,
	}
	counterConsistent := localHNSWFixedBudgetScreenCloneV1(t, report)
	counterCell := &counterConsistent.Arms[qualityArm].Cells[0]
	counterCell.PerPack[0].Work = localHNSWAttributionQueryWorkV1{Candidates: 2, Edges: 2, FrontierAdmissions: 2, Utility: counterUtility}
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, counterCell)
	if err := localHNSWFixedBudgetScreenContractV1(counterConsistent); err != nil {
		t.Fatalf("counter-consistent fixed-budget work rejected: %v", err)
	}
	underreportedCandidates := localHNSWFixedBudgetScreenCloneV1(t, counterConsistent)
	underreportedCandidatesCell := &underreportedCandidates.Arms[qualityArm].Cells[0]
	underreportedCandidatesCell.PerPack[0].Work.Candidates = 1
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, underreportedCandidatesCell)
	if err := localHNSWFixedBudgetScreenContractV1(underreportedCandidates); err == nil {
		t.Fatal("work candidates below newly visited utility accepted")
	}
	underreportedAdmissions := localHNSWFixedBudgetScreenCloneV1(t, counterConsistent)
	underreportedAdmissionsCell := &underreportedAdmissions.Arms[qualityArm].Cells[0]
	underreportedAdmissionsCell.PerPack[0].Work.FrontierAdmissions = 1
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, underreportedAdmissionsCell)
	if err := localHNSWFixedBudgetScreenContractV1(underreportedAdmissions); err == nil {
		t.Fatal("work frontier admissions below attributed utility accepted")
	}
	impossibleSeedResidual := localHNSWFixedBudgetScreenCloneV1(t, counterConsistent)
	impossibleSeedResidualCell := &impossibleSeedResidual.Arms[qualityArm].Cells[0]
	impossibleSeedResidualCell.PerPack[0].Work.FrontierAdmissions = 3
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, impossibleSeedResidualCell)
	if err := localHNSWFixedBudgetScreenContractV1(impossibleSeedResidual); err == nil {
		t.Fatal("seed admissions exceeding seed candidates accepted")
	}
	utilityTruthOverflow := localHNSWFixedBudgetScreenCloneV1(t, report)
	truthCell := &utilityTruthOverflow.Arms[0].Cells[0]
	truthUtility := &truthCell.PerPack[0].Work.Utility
	truthUtility.Unattributed.TruthRecovered = truthCell.PerPack[0].Opportunities*10 + 1
	truthUtility.TruthRecovered = truthUtility.Unattributed.TruthRecovered
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, truthCell)
	if err := localHNSWFixedBudgetScreenContractV1(utilityTruthOverflow); err == nil {
		t.Fatal("query utility truth recovery beyond exact-k opportunity capacity accepted")
	}
	wrongQualityUtilityOrigin := localHNSWFixedBudgetScreenCloneV1(t, report)
	qualityOriginCell := &wrongQualityUtilityOrigin.Arms[0].Cells[0]
	qualityOriginCell.PerPack[0].Work.Edges = 1
	qualityOriginCell.PerPack[0].Work.FrontierAdmissions = 1
	qualityOriginCell.PerPack[0].Work.Utility = qualityUtility
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, qualityOriginCell)
	if err := localHNSWFixedBudgetScreenContractV1(wrongQualityUtilityOrigin); err == nil {
		t.Fatal("non-postfill arm carrying quality-postfill query utility accepted")
	}
	robustUtility := qualityUtility
	robustUtility.QualityPostfill = localHNSWAttributionQueryOriginUtilityV1{}
	robustUtility.RobustPrune = localHNSWAttributionQueryOriginUtilityV1{Examined: 1, NewlyVisited: 1, Scored: 1, TopAdmissions: 1, FrontierAdmissions: 1}
	wrongRobustUtilityOrigin := localHNSWFixedBudgetScreenCloneV1(t, report)
	robustOriginCell := &wrongRobustUtilityOrigin.Arms[0].Cells[0]
	robustOriginCell.PerPack[0].Work.Edges = 1
	robustOriginCell.PerPack[0].Work.FrontierAdmissions = 1
	robustOriginCell.PerPack[0].Work.Utility = robustUtility
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, robustOriginCell)
	if err := localHNSWFixedBudgetScreenContractV1(wrongRobustUtilityOrigin); err == nil {
		t.Fatal("non-robust arm carrying robust-prune query utility accepted")
	}
	robustResidualUtility := robustUtility
	robustResidualUtility.RobustPrune = localHNSWAttributionQueryOriginUtilityV1{}
	robustResidualUtility.RobustPruneResidual = localHNSWAttributionQueryOriginUtilityV1{Examined: 1, NewlyVisited: 1, Scored: 1, TopAdmissions: 1, FrontierAdmissions: 1}
	wrongRobustResidualUtilityOrigin := localHNSWFixedBudgetScreenCloneV1(t, report)
	robustResidualOriginCell := &wrongRobustResidualUtilityOrigin.Arms[0].Cells[0]
	robustResidualOriginCell.PerPack[0].Work.Edges = 1
	robustResidualOriginCell.PerPack[0].Work.FrontierAdmissions = 1
	robustResidualOriginCell.PerPack[0].Work.Utility = robustResidualUtility
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, robustResidualOriginCell)
	if err := localHNSWFixedBudgetScreenContractV1(wrongRobustResidualUtilityOrigin); err == nil {
		t.Fatal("non-robust arm carrying robust residual query utility accepted")
	}
	perPackHitOverflow := localHNSWFixedBudgetScreenCloneV1(t, report)
	perPackHitOverflow.Arms[qualityArm].Cells[0].PerPack[0].TruthHitSlots = 10*perPackHitOverflow.Arms[qualityArm].Cells[0].PerPack[0].Opportunities + 1
	perPackHitOverflow.Arms[qualityArm].Cells[0].LocalTruthHitSlots = perPackHitOverflow.Arms[qualityArm].Cells[0].PerPack[0].TruthHitSlots
	if err := localHNSWFixedBudgetScreenContractV1(perPackHitOverflow); err == nil {
		t.Fatal("per-pack truth-hit capacity overflow accepted")
	}
	truthHitsBeyondAggregateRecovery := localHNSWFixedBudgetScreenCloneV1(t, report)
	truthHitsBeyondAggregateRecovery.Arms[qualityArm].Cells[0].PerPack[0].TruthHitSlots = 1
	truthHitsBeyondAggregateRecovery.Arms[qualityArm].Cells[0].LocalTruthHitSlots = 1
	if err := localHNSWFixedBudgetScreenContractV1(truthHitsBeyondAggregateRecovery); err == nil {
		t.Fatal("returned truth hits beyond aggregate recovered truth accepted")
	}
	truthHitsBeyondPerPackRecovery := localHNSWFixedBudgetScreenCloneV1(t, report)
	truthCell = &truthHitsBeyondPerPackRecovery.Arms[qualityArm].Cells[0]
	truthCell.PerPack[0].TruthHitSlots = 1
	recovered := &truthCell.PerPack[1].Work.Utility
	recovered.QualityPostfill.TruthRecovered = 1
	recovered.TruthRecovered = 1
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, truthCell)
	truthCell.LocalTruthHitSlots = 1
	if err := localHNSWFixedBudgetScreenContractV1(truthHitsBeyondPerPackRecovery); err == nil {
		t.Fatal("returned truth hits beyond per-pack recovered truth accepted")
	}
	perPackUtility := localHNSWFixedBudgetScreenCloneV1(t, report)
	first := &perPackUtility.Arms[qualityArm].Cells[0].PerPack[0].Work.Utility
	second := &perPackUtility.Arms[qualityArm].Cells[0].PerPack[1].Work.Utility
	first.Scored++
	first.QualityPostfill.Scored++
	second.Scored--
	second.QualityPostfill.Scored--
	if err := localHNSWFixedBudgetScreenContractV1(perPackUtility); err == nil {
		t.Fatal("offsetting invalid per-pack query utility accepted")
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
	control := localHNSWFixedBudgetScreenArmIndexV1(t, report, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MBackfillOnV1)
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
	missingSaturatedAngularPairs := localHNSWFixedBudgetScreenCloneV1(t, report)
	saturatedPairCount := localHNSWFixedBudgetScreenCanonicalSampleCountV1 * localHNSWFixedBudgetLayer0SlotsV1 * (localHNSWFixedBudgetLayer0SlotsV1 - 1) / 2
	missingSaturatedAngularPairs.Arms[qualityArm].SelectedNeighborhood[0].Neighborhood.AngularPairs = 0
	missingSaturatedAngularPairs.Arms[qualityArm].SelectedNeighborhood[0].Neighborhood.AngularCosineDistanceMean = 0
	missingSaturatedAngularPairs.Arms[qualityArm].Neighborhood.AngularPairs -= saturatedPairCount
	if err := localHNSWFixedBudgetScreenContractV1(missingSaturatedAngularPairs); err == nil {
		t.Fatal("missing saturated-arm angular pairs accepted")
	}
	missingSaturatedFinalEdges := localHNSWFixedBudgetScreenCloneV1(t, report)
	missingSaturatedFinalEdges.Arms[qualityArm].SelectedNeighborhood[0].Neighborhood.FinalEdgesByOrigin[0]--
	missingSaturatedFinalEdges.Arms[qualityArm].Neighborhood.FinalEdgesByOrigin[0]--
	if err := localHNSWFixedBudgetScreenContractV1(missingSaturatedFinalEdges); err == nil {
		t.Fatal("missing saturated-arm final edges accepted")
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

	badAngularMean := localHNSWFixedBudgetScreenCloneV1(t, report)
	badAngularMean.Arms[0].Neighborhood.AngularCosineDistanceMean = 0.5
	if err := localHNSWFixedBudgetScreenContractV1(badAngularMean); err == nil {
		t.Fatal("tampered neighborhood angular mean accepted")
	}

	nearAngularMean := localHNSWFixedBudgetScreenCloneV1(t, report)
	nearAngularMean.Arms[0].Neighborhood.AngularCosineDistanceMean += 1e-14
	if err := localHNSWFixedBudgetScreenContractV1(nearAngularMean); err != nil {
		t.Fatalf("honest angular recomposition roundoff rejected: %v", err)
	}

	varyingOpportunities := localHNSWFixedBudgetScreenCloneV1(t, report)
	varyingOpportunities.Arms[1].Cells[0].PerPack[0].Opportunities++
	varyingOpportunities.Arms[1].Cells[0].QueryPackOpportunities++
	if err := localHNSWFixedBudgetScreenContractV1(varyingOpportunities); err == nil {
		t.Fatal("EF or arm-dependent query opportunities accepted")
	}

	uniformlyReducedOpportunities := localHNSWFixedBudgetScreenCloneV1(t, report)
	for i := range uniformlyReducedOpportunities.Arms {
		for j := range uniformlyReducedOpportunities.Arms[i].Cells {
			cell := &uniformlyReducedOpportunities.Arms[i].Cells[j]
			cell.QueryPackOpportunities--
			cell.PerPack[0].Opportunities--
		}
	}
	if err := localHNSWFixedBudgetScreenContractV1(uniformlyReducedOpportunities); err == nil {
		t.Fatal("uniformly reduced frozen-route opportunities accepted")
	}
}

func TestLocalHNSWFixedBudgetScreenRejectsMissingInputsV1(t *testing.T) {
	if err := runLocalHNSWFixedBudgetScreenV1(nil, io.Discard); err == nil {
		t.Fatal("screen accepted missing frozen inputs")
	}
}

func localHNSWFixedBudgetScreenArmIndexV1(t *testing.T, report localHNSWFixedBudgetScreenReportV1, variant collections.VectorPartitionLocalGraphVariantV1) int {
	t.Helper()
	for i, arm := range report.Arms {
		if arm.Arm.Variant == variant {
			return i
		}
	}
	t.Fatalf("missing fixed-budget screen arm variant=%q", variant)
	return -1
}

func localHNSWFixedBudgetScreenTestRecomposeCellV1(t *testing.T, cell *localHNSWFixedBudgetScreenCellV1) {
	t.Helper()
	var aggregate localHNSWAttributionQueryWorkV1
	for _, pack := range cell.PerPack {
		if err := localHNSWM18EdgeDiagnosisWorkAddV1(&aggregate, pack.Work.Candidates, pack.Work.Edges, pack.Work.FrontierAdmissions, pack.Work.Utility); err != nil {
			t.Fatal(err)
		}
	}
	cell.Work = aggregate
}

func localHNSWFixedBudgetScreenTestRecomposeNeighborhoodV1(t *testing.T, arm *localHNSWFixedBudgetScreenArmResultV1) {
	t.Helper()
	aggregate := localHNSWAttributionNeighborhoodOracleV1{Schema: localHNSWAttributionNeighborhoodOracleSchemaV1, OriginOrder: localHNSWAttributionConstructionOriginOrderV1, ExactK: localHNSWAttributionNeighborhoodExactKV1}
	for _, pack := range arm.SelectedNeighborhood {
		if err := localHNSWFixedBudgetScreenNeighborhoodAddV1(&aggregate, pack.Neighborhood); err != nil {
			t.Fatal(err)
		}
		aggregate.PackDiagnostics = append(aggregate.PackDiagnostics, pack.Neighborhood.PackDiagnostics[0])
	}
	arm.Neighborhood = aggregate
}

func localHNSWFixedBudgetScreenTestMoveOriginV1(t *testing.T, arm *localHNSWFixedBudgetScreenArmResultV1, origin int, utility localHNSWAttributionQueryUtilityV1) {
	t.Helper()
	selected := &arm.SelectedNeighborhood[0].Neighborhood
	selected.FinalEdgesByOrigin[0]--
	selected.FinalEdgesByOrigin[origin]++
	selected.FinalTruthByOrigin[0]--
	selected.FinalTruthByOrigin[origin]++
	arm.Neighborhood.FinalEdgesByOrigin[0]--
	arm.Neighborhood.FinalEdgesByOrigin[origin]++
	arm.Neighborhood.FinalTruthByOrigin[0]--
	arm.Neighborhood.FinalTruthByOrigin[origin]++
	cell := &arm.Cells[0]
	pack := &cell.PerPack[0]
	pack.Work.Edges = 1
	pack.Work.FrontierAdmissions = 1
	pack.Work.Utility = utility
	pack.TruthHitSlots = 1
	localHNSWFixedBudgetScreenTestRecomposeCellV1(t, cell)
	cell.LocalTruthHitSlots = 1
}
