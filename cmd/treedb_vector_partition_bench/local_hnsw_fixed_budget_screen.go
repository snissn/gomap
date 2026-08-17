package main

// local-hnsw-fixed-budget-screen is an offline-only, calibration-only screen
// for the four #4172 selection coordinates plus one exact-budget, final-stage
// least-redundant separation postfill follow-up. It neither publishes an
// asset nor changes a serving default. The caller must explicitly invoke it;
// tests only exercise its contract and reducer helpers.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	localHNSWFixedBudgetScreenSchemaV1                       = "treedb_local_hnsw_fixed_budget_screen_v3"
	localHNSWFixedBudgetScreenBaseSHAV1                      = "2a7d01443d3c842990c259b08bd442a4d0109511"
	localHNSWFixedBudgetScreenSourcePartitionsV1             = 40
	localHNSWFixedBudgetScreenSourceRowsV1            uint64 = 250000
	localHNSWFixedBudgetScreenCalibrationLimitationV1        = "offline calibration-only selected-pack screen; no holdout outcomes opened"
	localHNSWFixedBudgetScreenTreatmentLimitationV1          = "same-budget postfill and robust-prune treatments only; no candidate extension, insertion-order, full Vamana conversion, router, probe, top-k, EF policy, or production-default changes"
	localHNSWFixedBudgetRetainedRowsV1                uint64 = 7500
	localHNSWFixedBudgetLayer0SlotsV1                 uint64 = 36
	localHNSWFixedBudgetTargetLayer0EdgesV1                  = localHNSWFixedBudgetRetainedRowsV1 * localHNSWFixedBudgetLayer0SlotsV1
	// The frozen 806-query, two-probe calibration route tally that intersects
	// the five retained packs. This is deliberately a source-bound contract
	// value rather than a value taken from an arbitrary report cell.
	localHNSWFixedBudgetScreenExpectedOpportunitiesV1 uint64 = 277
)

var localHNSWFixedBudgetScreenExpectedPerPackOpportunitiesV1 = [...]uint64{26, 56, 30, 85, 80}

func localHNSWFixedBudgetConstructionVariantV1(variant collections.VectorPartitionLocalGraphVariantV1) bool {
	for _, arm := range localHNSWFixedBudgetScreenArmsV1 {
		if arm.Variant == variant {
			return true
		}
	}
	return false
}

type localHNSWFixedBudgetScreenArmV1 struct {
	Name    string                                         `json:"name"`
	Variant collections.VectorPartitionLocalGraphVariantV1 `json:"variant"`
}

var localHNSWFixedBudgetScreenArmsV1 = []localHNSWFixedBudgetScreenArmV1{
	{"m_backfill_off", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0InitialMBackfillOffV1},
	{"m_backfill_on", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0InitialMBackfillOnV1},
	{"2m_backfill_off", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MBackfillOffV1},
	{"2m_backfill_on", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MBackfillOnV1},
	{"2m_least_redundant_separation_postfill", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1},
	{"2m_robust_prune_alpha_1_2", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1},
}

type localHNSWFixedBudgetScreenArmResultV1 struct {
	Arm                  localHNSWFixedBudgetScreenArmV1          `json:"arm"`
	Build                localHNSWAttributionBuildEvidenceV1      `json:"build"`
	SelectedDiagnostics  []localHNSWFixedBudgetDiagnosticV1       `json:"selected_pack_diagnostics"`
	Neighborhood         localHNSWAttributionNeighborhoodOracleV1 `json:"selected_pack_neighborhood_oracle"`
	SelectedNeighborhood []localHNSWFixedBudgetPackNeighborhoodV1 `json:"selected_pack_neighborhood"`
	Cells                []localHNSWFixedBudgetScreenCellV1       `json:"cells"`
	Control              []localHNSWFixedBudgetControlPackV1      `json:"canonical_m18_control,omitempty"`
	PostfillBudget       []localHNSWFixedBudgetPostfillBudgetV1   `json:"least_redundant_separation_postfill_budget,omitempty"`
}

// PostfillBudget records the exact L0 capacity and encoded-byte budget of the
// one postfill follow-up arm. It is not a byte-identity claim: the candidate
// graph deliberately differs from canonical M18, while its encoded capacity
// must be identical.
type localHNSWFixedBudgetPostfillBudgetV1 struct {
	Partition         uint32 `json:"partition"`
	Rows              uint64 `json:"rows"`
	Layer0Edges       uint64 `json:"layer0_edges"`
	TargetLayer0Edges uint64 `json:"target_layer0_edges"`
	CandidateBytes    uint64 `json:"candidate_bytes"`
	CanonicalBytes    uint64 `json:"canonical_m18_bytes"`
}
type localHNSWFixedBudgetDiagnosticV1 struct {
	Partition   uint32                                       `json:"partition"`
	Diagnostics collections.VectorPartitionPackDiagnosticsV1 `json:"diagnostics"`
}

type localHNSWFixedBudgetPackNeighborhoodV1 struct {
	Partition    uint32                                   `json:"partition"`
	Neighborhood localHNSWAttributionNeighborhoodOracleV1 `json:"neighborhood"`
}

// Every cell is restricted to a route entry that is one of SelectedPacks.
// It is deliberately not end-to-end recall: unselected route entries are not
// searched or merged in this first fixed-budget screen.
type localHNSWFixedBudgetScreenCellV1 struct {
	EFSearch               int                              `json:"ef_search"`
	QueryPackOpportunities uint64                           `json:"query_pack_opportunities"`
	LocalTruthHitSlots     uint64                           `json:"local_truth_hit_slots"`
	Work                   localHNSWAttributionQueryWorkV1  `json:"work"`
	PerPack                []localHNSWFixedBudgetPackWorkV1 `json:"per_pack"`
}

type localHNSWFixedBudgetPackWorkV1 struct {
	Partition     uint32                          `json:"partition"`
	Opportunities uint64                          `json:"opportunities"`
	TruthHitSlots uint64                          `json:"local_truth_hit_slots"`
	Work          localHNSWAttributionQueryWorkV1 `json:"work"`
}

// Control records are emitted only for the 2M/on arm. They permit a later
// exact retained-M18 checksum and structural comparison without treating that
// comparison as a screen outcome.
type localHNSWFixedBudgetControlPackV1 struct {
	Partition            uint32 `json:"partition"`
	CandidateChecksum    string `json:"candidate_checksum"`
	CanonicalChecksum    string `json:"canonical_m18_checksum"`
	CandidateGraphSHA256 string `json:"candidate_identity_neutral_sha256"`
	CanonicalGraphSHA256 string `json:"canonical_m18_identity_neutral_sha256"`
	CandidateBytes       uint64 `json:"candidate_bytes"`
	CanonicalBytes       uint64 `json:"canonical_m18_bytes"`
}

type localHNSWFixedBudgetScreenReportV1 struct {
	Schema       string                                  `json:"schema"`
	ResultKind   string                                  `json:"result_kind"`
	Status       string                                  `json:"status"`
	VariantPacks []uint32                                `json:"selected_packs"`
	Probes       int                                     `json:"probes"`
	EFSearch     []int                                   `json:"ef_search"`
	Queries      int                                     `json:"queries"`
	Arms         []localHNSWFixedBudgetScreenArmResultV1 `json:"arms"`
	Provenance   localHNSWAttributionProvenanceV1        `json:"provenance"`
	Calibration  localHNSWAttributionFileInputV1         `json:"calibration_split"`
	Truth        localHNSWAttributionFileInputV1         `json:"truth_artifact"`
	Descriptor   localHNSWAttributionFileInputV1         `json:"retained_descriptor"`
	Manifest     string                                  `json:"manifest_integrity_digest"`
	Source       localHNSWAttributionSourceEvidenceV1    `json:"source"`
	Limitations  []string                                `json:"limitations"`
}

func localHNSWFixedBudgetScreenContractV1(report localHNSWFixedBudgetScreenReportV1) error {
	if report.Schema != localHNSWFixedBudgetScreenSchemaV1 || report.ResultKind != "local_hnsw_fixed_budget_screen_v1" || report.Status != "valid" || !slices.Equal(report.VariantPacks, localHNSWM18EdgeDiagnosisPacksV1) || report.Probes != 2 || !slices.Equal(report.EFSearch, localHNSWM18EdgeDiagnosisEFV1) || report.Queries != 806 || len(report.Arms) != len(localHNSWFixedBudgetScreenArmsV1) || !slices.Equal(report.Limitations, []string{localHNSWFixedBudgetScreenCalibrationLimitationV1, localHNSWFixedBudgetScreenTreatmentLimitationV1}) || report.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || report.Descriptor.SHA256 != localHNSWM18DescriptorSHA256V1 || report.Provenance.BaseSHA != localHNSWFixedBudgetScreenBaseSHAV1 || report.Provenance.SourceDirty || !m8QualificationGitSHAV1(report.Provenance.HeadSHA) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.Source.Descriptor.ArtifactSHA256 != localHNSWM18AssignmentSHA256V1 || report.Source.Descriptor.GraphArtifactSHA256 != localHNSWM18GraphSHA256V1 || report.Source.Descriptor.ShardGenerationDigest != localHNSWM18ShardGenerationSHA256V1 {
		return errors.New("invalid fixed-budget screen contract")
	}
	if err := localHNSWFixedBudgetScreenSourceV1(report.Source, report.Manifest); err != nil {
		return err
	}
	if len(localHNSWFixedBudgetScreenExpectedPerPackOpportunitiesV1) != len(report.VariantPacks) {
		return errors.New("invalid fixed-budget expected opportunity binding")
	}
	for i, arm := range report.Arms {
		identity, identityErr := collections.VectorPartitionLocalGraphVariantIdentityV1(arm.Arm.Variant)
		if identityErr != nil || arm.Arm != localHNSWFixedBudgetScreenArmsV1[i] || arm.Build.Schema != localHNSWAttributionBuildSchemaV1 || arm.Build.Variant != string(arm.Arm.Variant) || arm.Build.VariantIdentity != identity || arm.Build.FileID != 4172000+uint32(i) || arm.Build.Partitions != len(report.VariantPacks) || arm.Build.PackBytes == 0 || len(arm.SelectedDiagnostics) != len(report.VariantPacks) || len(arm.SelectedNeighborhood) != len(report.VariantPacks) || len(arm.Cells) != len(report.EFSearch) {
			return errors.New("invalid fixed-budget screen arm")
		}
		for j, d := range arm.SelectedDiagnostics {
			if d.Partition != report.VariantPacks[j] {
				return errors.New("invalid fixed-budget selected diagnostic binding")
			}
			if err := localHNSWFixedBudgetScreenDiagnosticV1(d.Diagnostics); err != nil {
				return fmt.Errorf("invalid fixed-budget selected diagnostic partition=%d: %w", d.Partition, err)
			}
		}
		if err := localHNSWFixedBudgetScreenNeighborhoodV1(arm.Neighborhood, arm.SelectedNeighborhood, arm.SelectedDiagnostics, report.VariantPacks); err != nil {
			return err
		}
		if i > 0 {
			for j, pack := range arm.SelectedNeighborhood {
				baseline := report.Arms[0].SelectedNeighborhood[j].Neighborhood
				if pack.Neighborhood.CandidateSamples != baseline.CandidateSamples || pack.Neighborhood.CandidateTruthNeighbors != baseline.CandidateTruthNeighbors || pack.Neighborhood.FinalSamples != baseline.FinalSamples || pack.Neighborhood.FinalSampleTruthNeighbors != baseline.FinalSampleTruthNeighbors {
					return fmt.Errorf("invalid fixed-budget cross-arm sample binding arm=%s partition=%d", arm.Arm.Name, pack.Partition)
				}
			}
		}
		if err := localHNSWFixedBudgetScreenTreatmentOriginsV1(arm); err != nil {
			return err
		}
		for j, cell := range arm.Cells {
			if cell.EFSearch != report.EFSearch[j] || cell.QueryPackOpportunities != localHNSWFixedBudgetScreenExpectedOpportunitiesV1 || cell.LocalTruthHitSlots > cell.QueryPackOpportunities*10 || len(cell.PerPack) != len(report.VariantPacks) || cell.Work.Candidates == 0 || !localHNSWAttributionQueryUtilityConservedV1(cell.Work.Utility, cell.Work.Edges) {
				return fmt.Errorf("invalid fixed-budget screen cell arm=%s ef=%d expected_ef=%d opportunities=%d hits=%d per_pack=%d candidates=%d edges=%d frontier=%d utility=%+v", arm.Arm.Name, cell.EFSearch, report.EFSearch[j], cell.QueryPackOpportunities, cell.LocalTruthHitSlots, len(cell.PerPack), cell.Work.Candidates, cell.Work.Edges, cell.Work.FrontierAdmissions, cell.Work.Utility)
			}
			if err := localHNSWFixedBudgetScreenQueryUtilityV1(arm.Arm.Variant, cell.Work.Utility, cell.QueryPackOpportunities); err != nil {
				return fmt.Errorf("invalid fixed-budget screen cell utility arm=%s ef=%d: %w", arm.Arm.Name, cell.EFSearch, err)
			}
			var opportunities, hits uint64
			var work localHNSWAttributionQueryWorkV1
			for k, p := range cell.PerPack {
				if p.Partition != report.VariantPacks[k] || p.Opportunities != localHNSWFixedBudgetScreenExpectedPerPackOpportunitiesV1[k] || p.TruthHitSlots > p.Opportunities*10 || p.Work.Candidates == 0 || !localHNSWAttributionQueryUtilityConservedV1(p.Work.Utility, p.Work.Edges) || ^uint64(0)-opportunities < p.Opportunities || ^uint64(0)-hits < p.TruthHitSlots || localHNSWM18EdgeDiagnosisWorkAddV1(&work, p.Work.Candidates, p.Work.Edges, p.Work.FrontierAdmissions, p.Work.Utility) != nil {
					return fmt.Errorf("invalid fixed-budget per-pack decomposition arm=%s ef=%d slot=%d partition=%d expected_partition=%d opportunities=%d hits=%d candidates=%d edges=%d frontier=%d utility=%+v", arm.Arm.Name, cell.EFSearch, k, p.Partition, report.VariantPacks[k], p.Opportunities, p.TruthHitSlots, p.Work.Candidates, p.Work.Edges, p.Work.FrontierAdmissions, p.Work.Utility)
				}
				if err := localHNSWFixedBudgetScreenQueryUtilityV1(arm.Arm.Variant, p.Work.Utility, p.Opportunities); err != nil {
					return fmt.Errorf("invalid fixed-budget per-pack utility arm=%s ef=%d partition=%d: %w", arm.Arm.Name, cell.EFSearch, p.Partition, err)
				}
				opportunities += p.Opportunities
				hits += p.TruthHitSlots
			}
			if opportunities != cell.QueryPackOpportunities || hits != cell.LocalTruthHitSlots || work != cell.Work {
				return errors.New("fixed-budget aggregate decomposition")
			}
		}
		isControl := arm.Arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MBackfillOnV1
		if !isControl && len(arm.Control) != 0 || isControl && len(arm.Control) != len(report.VariantPacks) {
			return errors.New("invalid fixed-budget screen control")
		}
		if isControl {
			for j, control := range arm.Control {
				if control.Partition != report.VariantPacks[j] || !localHNSWAttributionSHA256V1(control.CandidateChecksum) || !localHNSWAttributionSHA256V1(control.CanonicalChecksum) || !localHNSWAttributionSHA256V1(control.CandidateGraphSHA256) || control.CandidateGraphSHA256 != control.CanonicalGraphSHA256 || control.CandidateBytes == 0 || control.CandidateBytes != control.CanonicalBytes {
					return errors.New("fixed-budget 2M/on control mismatch")
				}
			}
		}
		isExactBudget := arm.Arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1 || arm.Arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1
		if !isExactBudget && len(arm.PostfillBudget) != 0 || isExactBudget && len(arm.PostfillBudget) != len(report.VariantPacks) {
			return errors.New("invalid fixed-budget postfill budget")
		}
		if isExactBudget {
			for j, budget := range arm.PostfillBudget {
				diagnostic := arm.SelectedDiagnostics[j].Diagnostics
				if budget.Partition != report.VariantPacks[j] || budget.Rows != diagnostic.Rows || len(diagnostic.EdgesByLayer) == 0 || budget.Layer0Edges != diagnostic.EdgesByLayer[0] || budget.Rows != localHNSWFixedBudgetRetainedRowsV1 || budget.TargetLayer0Edges != localHNSWFixedBudgetTargetLayer0EdgesV1 || budget.Layer0Edges != budget.TargetLayer0Edges || budget.CandidateBytes == 0 || budget.CandidateBytes != budget.CanonicalBytes {
					return errors.New("fixed-budget exact refinement budget mismatch")
				}
				neighborhood := arm.SelectedNeighborhood[j].Neighborhood
				pairsPerSample := localHNSWFixedBudgetLayer0SlotsV1 * (localHNSWFixedBudgetLayer0SlotsV1 - 1) / 2
				if neighborhood.AngularPairs != neighborhood.FinalSamples*pairsPerSample {
					return fmt.Errorf("invalid fixed-budget saturated angular pairs arm=%s partition=%d", arm.Arm.Name, budget.Partition)
				}
			}
		}
	}
	return nil
}

// localHNSWFixedBudgetScreenSourceV1 keeps the accepted screen tied to the
// retained M18 source rather than merely to three descriptor artifact hashes.
func localHNSWFixedBudgetScreenSourceV1(source localHNSWAttributionSourceEvidenceV1, manifest string) error {
	if source.IndexName == "" || source.PartitionGeneration == 0 || source.Partitions != localHNSWFixedBudgetScreenSourcePartitionsV1 || source.ManifestIntegrity != manifest || !localHNSWAttributionSHA256V1(source.ManifestIntegrity) || !localHNSWAttributionSHA256V1(source.ReadySetDigest) || source.SourceGeneration == 0 || source.SourceChecksum == 0 || source.SourceSchemaHash == 0 || source.SourceRows != localHNSWFixedBudgetScreenSourceRowsV1 || source.RouterGeneration != source.PartitionGeneration || !localHNSWAttributionSHA256V1(source.RouterModelDigest) || source.RouterRepresentatives == 0 || source.RouterRepresentatives > source.SourceRows || len(source.PartitionLoads) != int(source.Partitions) {
		return errors.New("invalid fixed-budget screen source")
	}
	descriptor := source.Descriptor
	if descriptor.Partitions != source.Partitions || descriptor.ManifestIntegrity != source.ManifestIntegrity || descriptor.ReadySetDigest != source.ReadySetDigest || descriptor.SourceGeneration != source.SourceGeneration || descriptor.SourceChecksum != source.SourceChecksum || descriptor.SourceSchemaHash != source.SourceSchemaHash || descriptor.SourceRows != source.SourceRows || descriptor.PartitionGeneration != source.PartitionGeneration || descriptor.RouterGeneration != source.RouterGeneration || descriptor.RouterModelDigest != source.RouterModelDigest || descriptor.RouterRepresentatives != source.RouterRepresentatives || descriptor.PartitionHNSWM != 18 || descriptor.PartitionHNSWEfC != 256 || len(descriptor.PartitionLoads) != len(source.PartitionLoads) {
		return errors.New("invalid fixed-budget screen source descriptor binding")
	}
	var rows uint64
	for i, load := range source.PartitionLoads {
		if ^uint64(0)-rows < load || load > uint64(^uint(0)>>1) || descriptor.PartitionLoads[i] < 0 || uint64(descriptor.PartitionLoads[i]) != load {
			return errors.New("invalid fixed-budget screen source loads")
		}
		rows += load
	}
	if rows != source.SourceRows {
		return errors.New("invalid fixed-budget screen source row total")
	}
	return nil
}

// localHNSWFixedBudgetScreenDiagnosticV1 validates the topology and summary
// relationships emitted by PackDiagnosticsV1 for one retained M18 pack. The
// report is offline evidence, so its nested diagnostics must remain
// structurally possible instead of merely being nonzero and self-consistent
// with the neighborhood summary that copied them.
func localHNSWFixedBudgetScreenDiagnosticV1(d collections.VectorPartitionPackDiagnosticsV1) error {
	if d.Rows != localHNSWFixedBudgetRetainedRowsV1 || d.ReachableRows != d.Rows || d.TraversalRoots != 1 || d.CombinedReachableRows != d.Rows || d.MaxLayer < 0 || len(d.RowsByLayer) != d.MaxLayer+1 || len(d.EdgesByLayer) != len(d.RowsByLayer) || d.RowsByLayer[0] != d.Rows || d.Layer0DegreeLimit != localHNSWFixedBudgetLayer0SlotsV1 || d.Layer0SaturatedRows > d.Rows || d.Layer0ZeroIndegreeRows > d.Rows || d.Layer0DuplicateEdges > d.EdgesByLayer[0] || d.Layer0ReciprocalEdges > d.EdgesByLayer[0] || d.Layer0StrongComponents == 0 || d.Layer0StrongComponents > d.Rows || d.Layer0LargestComponent == 0 || d.Layer0LargestComponent > d.Rows || d.Layer0StrongComponents == 1 && d.Layer0LargestComponent != d.Rows || d.Layer0Distances.Count != d.EdgesByLayer[0] || !validM8GraphDistanceDistributionV1(d.Layer0Distances) || !validM8ReciprocalRatioV1(d.Layer0ReciprocalEdges, d.EdgesByLayer[0], d.Layer0ReciprocalRatio) || d.AuxiliaryDistances.Count != d.AuxiliaryEdges || !validM8GraphDistanceDistributionV1(d.AuxiliaryDistances) {
		return errors.New("invalid fixed-budget pack diagnostics")
	}

	maxUint64 := ^uint64(0)
	for layer, rows := range d.RowsByLayer {
		if rows == 0 || layer > 0 && rows > d.RowsByLayer[layer-1] {
			return errors.New("invalid fixed-budget diagnostic layers")
		}
		degreeLimit := d.Layer0DegreeLimit
		if layer > 0 {
			degreeLimit /= 2
		}
		if degreeLimit == 0 || rows > maxUint64/degreeLimit || d.EdgesByLayer[layer] > rows*degreeLimit {
			return errors.New("invalid fixed-budget diagnostic edge capacity")
		}
	}

	var degreeRows, degreeEdges, saturatedRows uint64
	for degree, rows := range d.Layer0DegreeHistogram {
		if rows == 0 || degree > d.Layer0DegreeLimit || maxUint64-degreeRows < rows || degree != 0 && rows > maxUint64/degree || maxUint64-degreeEdges < degree*rows {
			return errors.New("invalid fixed-budget layer0 degree histogram")
		}
		degreeRows += rows
		degreeEdges += degree * rows
		if degree >= d.Layer0DegreeLimit {
			if maxUint64-saturatedRows < rows {
				return errors.New("fixed-budget layer0 saturation overflow")
			}
			saturatedRows += rows
		}
	}
	if degreeRows != d.Rows || degreeEdges != d.EdgesByLayer[0] || saturatedRows != d.Layer0SaturatedRows {
		return errors.New("invalid fixed-budget layer0 degree totals")
	}

	var indegreeRows, indegreeEdges uint64
	for degree, rows := range d.Layer0IndegreeHistogram {
		if rows == 0 || maxUint64-indegreeRows < rows || degree != 0 && rows > maxUint64/degree || maxUint64-indegreeEdges < degree*rows {
			return errors.New("invalid fixed-budget layer0 indegree histogram")
		}
		indegreeRows += rows
		indegreeEdges += degree * rows
	}
	if indegreeRows != d.Rows || indegreeEdges != d.EdgesByLayer[0] || d.Layer0IndegreeHistogram[0] != d.Layer0ZeroIndegreeRows {
		return errors.New("invalid fixed-budget layer0 indegree totals")
	}

	auxiliaryPresent := d.AuxiliaryEdges != 0 || d.AuxiliaryCSRBytes != 0 || d.AuxiliaryMaxDegree != 0
	if auxiliaryPresent {
		if d.Rows == maxUint64 || d.Rows+1 > maxUint64/8 || d.AuxiliaryEdges > maxUint64/4 || d.AuxiliaryMaxDegree == 0 || d.Rows > maxUint64/d.AuxiliaryMaxDegree || d.AuxiliaryEdges > d.Rows*d.AuxiliaryMaxDegree || (d.AuxiliaryEdges == 0) != (d.AuxiliaryMaxDegree == 0) {
			return errors.New("invalid fixed-budget auxiliary diagnostics")
		}
		offsetBytes, edgeBytes := (d.Rows+1)*8, d.AuxiliaryEdges*4
		if edgeBytes > maxUint64-offsetBytes || d.AuxiliaryCSRBytes != offsetBytes+edgeBytes {
			return errors.New("invalid fixed-budget auxiliary diagnostic bytes")
		}
	}
	return nil
}

func localHNSWFixedBudgetScreenTreatmentOriginsV1(arm localHNSWFixedBudgetScreenArmResultV1) error {
	wantQuality := arm.Arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1
	wantRobust := arm.Arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1
	for _, one := range arm.SelectedNeighborhood {
		counts := one.Neighborhood.FinalEdgesByOrigin
		if (!wantQuality && counts[5] != 0) || (!wantRobust && (counts[6] != 0 || counts[7] != 0)) {
			return fmt.Errorf("invalid fixed-budget treatment origins arm=%s partition=%d counts=%v", arm.Arm.Name, one.Partition, counts)
		}
	}
	counts := arm.Neighborhood.FinalEdgesByOrigin
	// RobustPrune can fill the complete fixed degree budget by itself. The
	// closest-candidate residual is therefore observable when used, but is not
	// required for a treatment to be real; the independently checked degree and
	// byte budget prove capacity in either case.
	if (wantQuality && counts[5] == 0) || (!wantQuality && counts[5] != 0) || (wantRobust && counts[6] == 0) || (!wantRobust && (counts[6] != 0 || counts[7] != 0)) {
		return fmt.Errorf("invalid fixed-budget treatment aggregate arm=%s counts=%v", arm.Arm.Name, counts)
	}
	return nil
}

// localHNSWFixedBudgetScreenQueryUtilityV1 applies the fixed screen's
// per-query exact-k capacity and construction-treatment namespace to every
// persisted utility, before per-pack values are folded into a cell aggregate.
// The generic utility conservation helper intentionally permits unattributed
// seed truth, so this contract supplies the screen-specific ten-result bound.
func localHNSWFixedBudgetScreenQueryUtilityV1(variant collections.VectorPartitionLocalGraphVariantV1, utility localHNSWAttributionQueryUtilityV1, opportunities uint64) error {
	const exactK uint64 = 10
	if opportunities > ^uint64(0)/exactK || utility.TruthRecovered > opportunities*exactK {
		return errors.New("invalid fixed-budget query utility truth capacity")
	}
	wantQuality := variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1
	wantRobust := variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1
	if !wantQuality && utility.QualityPostfill != (localHNSWAttributionQueryOriginUtilityV1{}) {
		return errors.New("invalid fixed-budget quality-postfill query utility origin")
	}
	if !wantRobust && (utility.RobustPrune != (localHNSWAttributionQueryOriginUtilityV1{}) || utility.RobustPruneResidual != (localHNSWAttributionQueryOriginUtilityV1{})) {
		return errors.New("invalid fixed-budget robust-prune query utility origin")
	}
	return nil
}

func localHNSWFixedBudgetScreenNeighborhoodV1(aggregate localHNSWAttributionNeighborhoodOracleV1, perPack []localHNSWFixedBudgetPackNeighborhoodV1, diagnostics []localHNSWFixedBudgetDiagnosticV1, partitions []uint32) error {
	if aggregate.Schema != localHNSWAttributionNeighborhoodOracleSchemaV1 || aggregate.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || aggregate.ExactK != localHNSWAttributionNeighborhoodExactKV1 || aggregate.CandidateSamples == 0 || aggregate.FinalSamples == 0 || len(perPack) != len(partitions) || len(diagnostics) != len(partitions) || len(aggregate.PackDiagnostics) != len(partitions) {
		return errors.New("invalid fixed-budget neighborhood oracle")
	}
	var totals localHNSWAttributionNeighborhoodOracleV1
	for i, partition := range partitions {
		one := perPack[i].Neighborhood
		if err := localHNSWFixedBudgetScreenNeighborhoodTruthBoundsV1(one); err != nil {
			return err
		}
		if one.CandidateSamples != one.FinalSamples {
			return errors.New("invalid fixed-budget per-pack sample binding")
		}
		if !localHNSWFixedBudgetScreenAngularCosineDistanceValidV1(one.AngularCosineDistanceMean) {
			return errors.New("invalid fixed-budget per-pack angular cosine distance")
		}
		var finalEdgesByOrigin, finalTruthByOrigin uint64
		for origin := range one.FinalEdgesByOrigin {
			if one.FinalTruthByOrigin[origin] > one.FinalEdgesByOrigin[origin] || ^uint64(0)-finalEdgesByOrigin < one.FinalEdgesByOrigin[origin] || ^uint64(0)-finalTruthByOrigin < one.FinalTruthByOrigin[origin] {
				return errors.New("invalid fixed-budget per-pack neighborhood origins")
			}
			finalEdgesByOrigin += one.FinalEdgesByOrigin[origin]
			finalTruthByOrigin += one.FinalTruthByOrigin[origin]
		}
		degreeLimit := diagnostics[i].Diagnostics.Layer0DegreeLimit
		if degreeLimit != localHNSWFixedBudgetLayer0SlotsV1 || one.FinalSamples > ^uint64(0)/degreeLimit || finalEdgesByOrigin > one.FinalSamples*degreeLimit {
			return errors.New("invalid fixed-budget per-pack final edge capacity")
		}
		maxAngularPairs := degreeLimit * (degreeLimit - 1) / 2
		if one.FinalSamples > ^uint64(0)/maxAngularPairs || one.AngularPairs > one.FinalSamples*maxAngularPairs {
			return errors.New("invalid fixed-budget per-pack angular pair capacity")
		}
		if perPack[i].Partition != partition || one.Schema != localHNSWAttributionNeighborhoodOracleSchemaV1 || one.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || one.ExactK != localHNSWAttributionNeighborhoodExactKV1 || one.CandidateSamples == 0 || one.CandidateTruthRecovered > one.CandidateTruthNeighbors || one.FinalSamples == 0 || one.FinalSampleTruthRecovered > one.FinalSampleTruthNeighbors || finalTruthByOrigin != one.FinalSampleTruthRecovered || len(one.PackDiagnostics) != 1 || !reflect.DeepEqual(one.PackDiagnostics[0], diagnostics[i].Diagnostics) || !reflect.DeepEqual(aggregate.PackDiagnostics[i], diagnostics[i].Diagnostics) {
			return errors.New("invalid fixed-budget selected neighborhood binding")
		}
		if err := localHNSWFixedBudgetScreenNeighborhoodAddV1(&totals, one); err != nil {
			return err
		}
	}
	if totals.CandidateSamples != aggregate.CandidateSamples || totals.CandidateTruthNeighbors != aggregate.CandidateTruthNeighbors || totals.CandidateTruthRecovered != aggregate.CandidateTruthRecovered || totals.FinalSamples != aggregate.FinalSamples || totals.FinalSampleTruthNeighbors != aggregate.FinalSampleTruthNeighbors || totals.FinalSampleTruthRecovered != aggregate.FinalSampleTruthRecovered || totals.AngularPairs != aggregate.AngularPairs || !localHNSWFixedBudgetScreenAngularMeanEqualV1(totals.AngularCosineDistanceMean, aggregate.AngularCosineDistanceMean) || totals.FinalEdgesByOrigin != aggregate.FinalEdgesByOrigin || totals.FinalTruthByOrigin != aggregate.FinalTruthByOrigin {
		return errors.New("fixed-budget neighborhood aggregate decomposition")
	}
	return nil
}

// localHNSWFixedBudgetScreenNeighborhoodTruthBoundsV1 bounds each exact-kNN
// recovery denominator independently of report aggregation. This prevents a
// self-consistent per-pack report from claiming more exact neighbors than its
// sampled rows can have, while checking multiplication before it can wrap.
func localHNSWFixedBudgetScreenNeighborhoodTruthBoundsV1(one localHNSWAttributionNeighborhoodOracleV1) error {
	if one.ExactK <= 0 {
		return errors.New("invalid fixed-budget neighborhood exact k")
	}
	exactK := uint64(one.ExactK)
	if one.CandidateSamples > ^uint64(0)/exactK || one.FinalSamples > ^uint64(0)/exactK {
		return errors.New("fixed-budget neighborhood truth bound overflow")
	}
	if one.CandidateTruthNeighbors > one.CandidateSamples*exactK || one.FinalSampleTruthNeighbors > one.FinalSamples*exactK {
		return errors.New("invalid fixed-budget neighborhood truth bound")
	}
	return nil
}

// localHNSWFixedBudgetScreenAngularMeanEqualV1 tolerates only the bounded
// roundoff introduced by reducing five per-pack means instead of replaying
// every pair in one floating-point accumulation. Five per-pack reductions
// need at most a small epsilon-scale allowance; 64 epsilons at max(1, |x|)
// accepts the retained artifact's sub-1e-15 recomposition delta while still
// rejecting material report tampering.
func localHNSWFixedBudgetScreenAngularMeanEqualV1(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) || math.IsInf(a, 0) || math.IsInf(b, 0) {
		return false
	}
	scale := math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
	epsilon := math.Nextafter(1, 2) - 1
	return math.Abs(a-b) <= 64*epsilon*scale
}

// localHNSWFixedBudgetScreenAngularCosineDistanceValidV1 bounds a cosine
// distance mean independently for every selected pack. A later aggregate
// comparison cannot cancel an invalid negative or greater-than-two mean.
func localHNSWFixedBudgetScreenAngularCosineDistanceValidV1(mean float64) bool {
	if math.IsNaN(mean) || math.IsInf(mean, 0) {
		return false
	}
	epsilon := math.Nextafter(1, 2) - 1
	return mean >= -64*epsilon && mean <= 2+64*epsilon
}

func localHNSWFixedBudgetScreenNeighborhoodAddV1(dst *localHNSWAttributionNeighborhoodOracleV1, src localHNSWAttributionNeighborhoodOracleV1) error {
	if dst == nil {
		return errors.New("invalid fixed-budget neighborhood total")
	}
	if !localHNSWFixedBudgetScreenAngularCosineDistanceValidV1(src.AngularCosineDistanceMean) || (src.AngularPairs == 0 && src.AngularCosineDistanceMean != 0) || !localHNSWFixedBudgetScreenAngularCosineDistanceValidV1(dst.AngularCosineDistanceMean) || (dst.AngularPairs == 0 && dst.AngularCosineDistanceMean != 0) {
		return errors.New("invalid fixed-budget angular cosine distance")
	}
	fields := [][2]uint64{{dst.CandidateSamples, src.CandidateSamples}, {dst.CandidateTruthNeighbors, src.CandidateTruthNeighbors}, {dst.CandidateTruthRecovered, src.CandidateTruthRecovered}, {dst.FinalSamples, src.FinalSamples}, {dst.FinalSampleTruthNeighbors, src.FinalSampleTruthNeighbors}, {dst.FinalSampleTruthRecovered, src.FinalSampleTruthRecovered}, {dst.AngularPairs, src.AngularPairs}}
	for _, field := range fields {
		if ^uint64(0)-field[0] < field[1] {
			return errors.New("fixed-budget neighborhood overflow")
		}
	}
	for i := range dst.FinalEdgesByOrigin {
		if ^uint64(0)-dst.FinalEdgesByOrigin[i] < src.FinalEdgesByOrigin[i] || ^uint64(0)-dst.FinalTruthByOrigin[i] < src.FinalTruthByOrigin[i] {
			return errors.New("fixed-budget neighborhood origin overflow")
		}
	}
	dst.CandidateSamples += src.CandidateSamples
	dst.CandidateTruthNeighbors += src.CandidateTruthNeighbors
	dst.CandidateTruthRecovered += src.CandidateTruthRecovered
	dst.FinalSamples += src.FinalSamples
	dst.FinalSampleTruthNeighbors += src.FinalSampleTruthNeighbors
	dst.FinalSampleTruthRecovered += src.FinalSampleTruthRecovered
	angularDistanceSum := dst.AngularCosineDistanceMean*float64(dst.AngularPairs) + src.AngularCosineDistanceMean*float64(src.AngularPairs)
	dst.AngularPairs += src.AngularPairs
	if dst.AngularPairs == 0 {
		dst.AngularCosineDistanceMean = 0
	} else {
		dst.AngularCosineDistanceMean = angularDistanceSum / float64(dst.AngularPairs)
		if !localHNSWFixedBudgetScreenAngularCosineDistanceValidV1(dst.AngularCosineDistanceMean) {
			return errors.New("invalid fixed-budget angular cosine distance total")
		}
	}
	for i := range dst.FinalEdgesByOrigin {
		dst.FinalEdgesByOrigin[i] += src.FinalEdgesByOrigin[i]
		dst.FinalTruthByOrigin[i] += src.FinalTruthByOrigin[i]
	}
	return nil
}

func localHNSWFixedBudgetScreenSelectedDiagnosticsV1(all []collections.VectorPartitionPackDiagnosticsV1) ([]localHNSWFixedBudgetDiagnosticV1, error) {
	if len(all) != len(localHNSWM18EdgeDiagnosisPacksV1) {
		return nil, errors.New("invalid fixed-budget diagnostics")
	}
	out := make([]localHNSWFixedBudgetDiagnosticV1, len(all))
	for i, diagnostic := range all {
		if diagnostic.Rows == 0 {
			return nil, errors.New("invalid fixed-budget selected diagnostic")
		}
		out[i] = localHNSWFixedBudgetDiagnosticV1{Partition: localHNSWM18EdgeDiagnosisPacksV1[i], Diagnostics: diagnostic}
	}
	return out, nil
}

func localHNSWFixedBudgetScreenBuildV1(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, tempRoot string, calibration localHNSWAttributionCalibrationV1) ([]localHNSWFixedBudgetScreenArmResultV1, error) {
	if source == nil || len(calibration.Queries) != 806 || len(calibration.Truth) != 806 {
		return nil, errors.New("invalid fixed-budget screen inputs")
	}
	vectors, err := localHNSWAttributionNeighborhoodVectorsV1(&localHNSWVariantHarnessV1{assets: source})
	if err != nil {
		return nil, err
	}
	out := make([]localHNSWFixedBudgetScreenArmResultV1, len(localHNSWFixedBudgetScreenArmsV1))
	for i, arm := range localHNSWFixedBudgetScreenArmsV1 {
		h, err := materializeRetainedLocalHNSWVariantPartitionsV1(source, tempRoot, arm.Variant, 4172000+uint32(i), localHNSWM18EdgeDiagnosisPacksV1)
		if err != nil {
			return nil, fmt.Errorf("fixed-budget arm %s materialize: %w", arm.Name, err)
		}
		if len(h.constructionEvidence.Partitions) != len(localHNSWM18EdgeDiagnosisPacksV1) || len(h.searchers) != len(localHNSWM18EdgeDiagnosisPacksV1) {
			_ = h.Close()
			return nil, errors.New("fixed-budget screen construction partitions")
		}
		for _, partition := range h.constructionEvidence.Partitions {
			if partition.TraceMode != "compact" || len(partition.Events) != 0 || len(partition.FinalOrigins) == 0 {
				_ = h.Close()
				return nil, errors.New("fixed-budget screen retained construction history")
			}
		}
		diagnostics, err := localHNSWAttributionPackDiagnosticsV1(h.searchers)
		if err == nil {
			out[i].SelectedDiagnostics, err = localHNSWFixedBudgetScreenSelectedDiagnosticsV1(diagnostics)
		}
		if err == nil {
			out[i].Neighborhood, err = localHNSWAttributionNeighborhoodOracleWithVectorsV1(h, diagnostics, vectors)
		}
		if err == nil {
			out[i].SelectedNeighborhood, err = localHNSWFixedBudgetScreenSelectedNeighborhoodV1(h, diagnostics, vectors)
		}
		if err == nil && (arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1 || arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MRobustPruneV1) {
			out[i].PostfillBudget, err = localHNSWFixedBudgetScreenPostfillBudgetV1(source, h, diagnostics)
		}
		if err == nil {
			out[i].Cells, err = localHNSWFixedBudgetScreenCellsV1(ctx, source, h, calibration)
		}
		if err != nil {
			_ = h.Close()
			return nil, fmt.Errorf("fixed-budget arm %s evidence: %w", arm.Name, err)
		}
		out[i].Arm = arm
		identity, identityErr := collections.VectorPartitionLocalGraphVariantIdentityV1(arm.Variant)
		if identityErr != nil {
			_ = h.Close()
			return nil, identityErr
		}
		out[i].Build = localHNSWAttributionBuildEvidenceV1{Schema: localHNSWAttributionBuildSchemaV1, Variant: string(arm.Variant), VariantIdentity: identity, FileID: 4172000 + uint32(i), Partitions: len(localHNSWM18EdgeDiagnosisPacksV1)}
		for _, asset := range h.packAssets {
			out[i].Build.PackBytes += asset.Bytes
		}
		if arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MBackfillOnV1 {
			out[i].Control, err = localHNSWFixedBudgetScreenControlV1(source, h)
			if err != nil {
				_ = h.Close()
				return nil, fmt.Errorf("fixed-budget arm %s control: %w", arm.Name, err)
			}
		}
		if err := h.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func localHNSWFixedBudgetScreenPostfillBudgetV1(source *m8ProductionMultiGroupAssetsV1, h *localHNSWVariantHarnessV1, diagnostics []collections.VectorPartitionPackDiagnosticsV1) ([]localHNSWFixedBudgetPostfillBudgetV1, error) {
	if source == nil || h == nil || len(h.packAssets) != len(localHNSWM18EdgeDiagnosisPacksV1) || len(diagnostics) != len(h.packAssets) {
		return nil, errors.New("invalid fixed-budget postfill inputs")
	}
	canonical := make(map[uint32]collections.VectorPartitionAssetV1, len(source.manifest.Assets))
	for _, asset := range source.manifest.Assets {
		canonical[asset.PartitionID] = asset
	}
	out := make([]localHNSWFixedBudgetPostfillBudgetV1, len(h.packAssets))
	for i, asset := range h.packAssets {
		control, ok := canonical[asset.PartitionID]
		if !ok || control.Bytes == 0 || diagnostics[i].Rows == 0 || len(diagnostics[i].EdgesByLayer) == 0 {
			return nil, errors.New("missing fixed-budget postfill canonical budget")
		}
		target := uint64(270000)
		if diagnostics[i].Rows != 7500 || diagnostics[i].EdgesByLayer[0] != target || asset.Bytes != control.Bytes {
			return nil, errors.New("fixed-budget postfill encoded capacity mismatch")
		}
		out[i] = localHNSWFixedBudgetPostfillBudgetV1{Partition: asset.PartitionID, Rows: diagnostics[i].Rows, Layer0Edges: diagnostics[i].EdgesByLayer[0], TargetLayer0Edges: target, CandidateBytes: asset.Bytes, CanonicalBytes: control.Bytes}
	}
	return out, nil
}

func localHNSWFixedBudgetScreenSelectedNeighborhoodV1(h *localHNSWVariantHarnessV1, diagnostics []collections.VectorPartitionPackDiagnosticsV1, vectors map[string][]float32) ([]localHNSWFixedBudgetPackNeighborhoodV1, error) {
	if h == nil || len(h.searchers) != len(localHNSWM18EdgeDiagnosisPacksV1) || len(h.documentIDs) != len(h.searchers) || len(h.packAssets) != len(h.searchers) || len(h.constructionEvidence.Partitions) != len(h.searchers) || len(diagnostics) != len(h.searchers) || len(vectors) == 0 {
		return nil, errors.New("invalid fixed-budget selected neighborhood inputs")
	}
	out := make([]localHNSWFixedBudgetPackNeighborhoodV1, len(h.searchers))
	for i, partition := range localHNSWM18EdgeDiagnosisPacksV1 {
		if h.packAssets[i].PartitionID != partition || h.constructionEvidence.Partitions[i].PartitionID != partition {
			return nil, errors.New("invalid fixed-budget selected neighborhood order")
		}
		one := *h
		one.searchers = []*collections.VectorPartitionLocalSearcherV1{h.searchers[i]}
		one.documentIDs = [][]string{h.documentIDs[i]}
		one.packAssets = []collections.VectorPartitionAssetV1{h.packAssets[i]}
		one.constructionEvidence = h.constructionEvidence
		one.constructionEvidence.Partitions = []collections.VectorPartitionConstructionPartitionEvidenceV1{h.constructionEvidence.Partitions[i]}
		neighborhood, err := localHNSWAttributionNeighborhoodOracleWithVectorsV1(&one, []collections.VectorPartitionPackDiagnosticsV1{diagnostics[i]}, vectors)
		if err != nil {
			return nil, fmt.Errorf("fixed-budget partition %d neighborhood: %w", partition, err)
		}
		out[i] = localHNSWFixedBudgetPackNeighborhoodV1{Partition: partition, Neighborhood: neighborhood}
	}
	return out, nil
}

func localHNSWFixedBudgetScreenCellsV1(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, h *localHNSWVariantHarnessV1, calibration localHNSWAttributionCalibrationV1) ([]localHNSWFixedBudgetScreenCellV1, error) {
	if source == nil || h == nil || len(h.searchers) != len(localHNSWM18EdgeDiagnosisPacksV1) || len(calibration.Queries) != 806 || len(calibration.Truth) != 806 {
		return nil, errors.New("invalid fixed-budget selected query inputs")
	}
	byPartition := make(map[uint32]int, len(h.searchers))
	for i, asset := range h.packAssets {
		byPartition[asset.PartitionID] = i
	}
	out := make([]localHNSWFixedBudgetScreenCellV1, len(localHNSWM18EdgeDiagnosisEFV1))
	for i, ef := range localHNSWM18EdgeDiagnosisEFV1 {
		out[i].EFSearch = ef
		out[i].PerPack = make([]localHNSWFixedBudgetPackWorkV1, len(h.searchers))
		for j, asset := range h.packAssets {
			out[i].PerPack[j].Partition = asset.PartitionID
		}
	}
	candidates := min(256, int(source.status.Representatives))
	for qi, query := range calibration.Queries {
		route, err := localHNSWAttributionQueryRouteV1(ctx, source, query, candidates, 2)
		if err != nil {
			return nil, err
		}
		truth := localHNSWAttributionResultIDSetV1(calibration.Truth[qi])
		for _, partition := range route {
			local, selected := byPartition[partition]
			if !selected {
				continue
			}
			for ci := range out {
				found, metrics, trace, err := h.searchers[local].SearchWithAttributionV1(ctx, query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: out[ci].EFSearch})
				if err != nil || !localHNSWAttributionSearchValidV1(trace) {
					return nil, errors.New("invalid fixed-budget selected attributed search")
				}
				utility, err := localHNSWAttributionQueryUtilityReduceV1(metrics, trace, h.finalOrigins[local], h.documentIDs[local], truth)
				if err != nil {
					return nil, err
				}
				edges, err := localHNSWAttributionMetricEdgesV1(metrics)
				if err != nil || localHNSWM18EdgeDiagnosisWorkAddV1(&out[ci].Work, metrics.Candidates, edges, trace.FrontierAdmissions, utility) != nil || localHNSWM18EdgeDiagnosisWorkAddV1(&out[ci].PerPack[local].Work, metrics.Candidates, edges, trace.FrontierAdmissions, utility) != nil {
					return nil, errors.New("fixed-budget selected work")
				}
				out[ci].QueryPackOpportunities++
				out[ci].PerPack[local].Opportunities++
				for _, result := range found {
					if _, hit := truth[result.ID]; hit {
						out[ci].LocalTruthHitSlots++
						out[ci].PerPack[local].TruthHitSlots++
					}
				}
			}
		}
	}
	return out, nil
}

func localHNSWFixedBudgetScreenControlV1(source *m8ProductionMultiGroupAssetsV1, h *localHNSWVariantHarnessV1) ([]localHNSWFixedBudgetControlPackV1, error) {
	if source == nil || h == nil || len(h.packAssets) != len(localHNSWM18EdgeDiagnosisPacksV1) {
		return nil, errors.New("invalid fixed-budget control")
	}
	canonical := make(map[uint32]collections.VectorPartitionAssetV1, len(source.manifest.Assets))
	for _, asset := range source.manifest.Assets {
		canonical[asset.PartitionID] = asset
	}
	out := make([]localHNSWFixedBudgetControlPackV1, len(h.packAssets))
	for i, asset := range h.packAssets {
		control, ok := canonical[asset.PartitionID]
		if !ok || control.Checksum == "" || control.Bytes == 0 {
			return nil, errors.New("missing fixed-budget canonical control")
		}
		candidateGraphSHA, err := h.searchers[i].PackIdentityNeutralSHA256ForOfflineV1()
		if err != nil {
			return nil, fmt.Errorf("fixed-budget candidate control partition %d: %w", asset.PartitionID, err)
		}
		canonicalSearcher, err := source.collection.OpenVectorPartitionLocalSearcherForOfflineAssetVariantWithContextV1(context.Background(), source.manifest.IndexName, source.manifest, control, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1)
		if err != nil {
			return nil, fmt.Errorf("fixed-budget canonical control partition %d: %w", asset.PartitionID, err)
		}
		canonicalGraphSHA, digestErr := canonicalSearcher.PackIdentityNeutralSHA256ForOfflineV1()
		closeErr := canonicalSearcher.Close()
		if digestErr != nil || closeErr != nil {
			return nil, fmt.Errorf("fixed-budget canonical control digest partition %d: %w", asset.PartitionID, errors.Join(digestErr, closeErr))
		}
		out[i] = localHNSWFixedBudgetControlPackV1{Partition: asset.PartitionID, CandidateChecksum: asset.Checksum, CanonicalChecksum: control.Checksum, CandidateGraphSHA256: candidateGraphSHA, CanonicalGraphSHA256: canonicalGraphSHA, CandidateBytes: asset.Bytes, CanonicalBytes: control.Bytes}
	}
	return out, nil
}

func runLocalHNSWFixedBudgetScreenV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-fixed-budget-screen", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, retainedDB, calibrationPath, truthPath, tempRoot, out, baseSHA, headSHA, sourceCheckout string
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&retainedDB, "retained-db", "", "literal retained M18 database")
	fs.StringVar(&calibrationPath, "calibration-split", "", "frozen calibration manifest")
	fs.StringVar(&truthPath, "truth-artifact", "", "frozen truth artifact")
	fs.StringVar(&tempRoot, "temp-root", "", "existing fast temporary root")
	fs.StringVar(&out, "out", "", "new screen JSON path")
	fs.StringVar(&baseSHA, "base-sha", "", "exact main base SHA")
	fs.StringVar(&headSHA, "head-sha", "", "exact screen implementation SHA")
	fs.StringVar(&sourceCheckout, "source-checkout", "", "clean exact-head checkout")
	if fs.Parse(args) != nil || fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationPath == "" || truthPath == "" || tempRoot == "" || out == "" || baseSHA == "" || headSHA == "" || sourceCheckout == "" {
		return errors.New("local-hnsw-fixed-budget-screen requires frozen inputs and fresh output")
	}
	var err error
	for ptr, value := range map[*string]string{&dataset: dataset, &retainedDB: retainedDB, &calibrationPath: calibrationPath, &truthPath: truthPath, &tempRoot: tempRoot, &out: out, &sourceCheckout: sourceCheckout} {
		if *ptr, err = m8CanonicalPathV1(value); err != nil {
			return err
		}
	}
	baseSHA, headSHA, err = provenanceWithExplicitV1(baseSHA, headSHA)
	if err != nil || baseSHA != localHNSWFixedBudgetScreenBaseSHAV1 || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("fixed-budget screen source provenance")
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil {
		return errors.New("fixed-budget screen source checkout")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	executable, err = m8CanonicalPathV1(executable)
	if err != nil {
		return err
	}
	executableSHA, err := m8BenchmarkExecutableSHA256V1(executable)
	if err != nil || !m8QualificationBenchmarkExecutableV1(sourceCheckout, executable, headSHA, executableSHA) {
		return errors.New("fixed-budget screen executable provenance")
	}
	if filepath.Ext(out) != ".json" {
		return errors.New("fixed-budget screen output must be JSON")
	}
	if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
		return errors.New("fixed-budget screen output exists")
	}
	fixture, err := loadFixture(dataset)
	if err != nil || !localHNSWAttributionFixtureV1(fixture) {
		return errors.New("fixed-budget screen fixture identity")
	}
	calibration, calibrationSHA, err := loadLocalHNSWQuerySplitV1(calibrationPath)
	if err != nil || calibrationSHA != localHNSWAttributionCalibrationSHA256V1 || len(calibration.Ordinals) != 806 {
		return errors.New("fixed-budget screen calibration identity")
	}
	truthSHA, err := localHNSWAttributionRegularFileSHA256V1(truthPath, m8ProfileArtifactMaxBytesV1)
	if err != nil || truthSHA != localHNSWAttributionTruthSHA256V1 {
		return errors.New("fixed-budget screen truth identity")
	}
	descriptorPath := filepath.Join(retainedDB, m3VariantDescriptorFileV1)
	descriptorSHA, err := localHNSWAttributionRegularFileSHA256V1(descriptorPath, m3VariantDescriptorMaxBytesV1)
	if err != nil || descriptorSHA != localHNSWM18DescriptorSHA256V1 {
		return errors.New("fixed-budget screen descriptor identity")
	}
	source, err := openM8ProductionExistingAssetSetV1(retainedDB)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, source.Close()) }()
	if err := localHNSWRepairCalibrationBindDescriptorV1(source, fixture); err != nil {
		return err
	}
	if source.descriptor == nil || source.manifest.PartitionCount != 40 || source.descriptor.ArtifactSHA256 != localHNSWM18AssignmentSHA256V1 || source.descriptor.GraphArtifactSHA256 != localHNSWM18GraphSHA256V1 || source.descriptor.ShardGenerationDigest != localHNSWM18ShardGenerationSHA256V1 {
		return errors.New("fixed-budget screen retained source binding")
	}
	queries, err := localHNSWAttributionCalibrationV1Build(source, fixture, calibration.Ordinals)
	if err != nil {
		return err
	}
	arms, err := localHNSWFixedBudgetScreenBuildV1(context.Background(), source, tempRoot, queries)
	if err != nil {
		return err
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil {
		return err
	}
	report := localHNSWFixedBudgetScreenReportV1{Schema: localHNSWFixedBudgetScreenSchemaV1, ResultKind: "local_hnsw_fixed_budget_screen_v1", Status: "valid", VariantPacks: append([]uint32(nil), localHNSWM18EdgeDiagnosisPacksV1...), Probes: 2, EFSearch: append([]int(nil), localHNSWM18EdgeDiagnosisEFV1...), Queries: 806, Arms: arms, Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-fixed-budget-screen", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationPath, SHA256: calibrationSHA}, Truth: localHNSWAttributionFileInputV1{Path: truthPath, SHA256: truthSHA}, Descriptor: localHNSWAttributionFileInputV1{Path: descriptorPath, SHA256: descriptorSHA}, Manifest: source.manifest.IntegrityDigest, Source: localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor}, Limitations: []string{localHNSWFixedBudgetScreenCalibrationLimitationV1, localHNSWFixedBudgetScreenTreatmentLimitationV1}}
	if err := localHNSWFixedBudgetScreenContractV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		return err
	}
	var reread localHNSWFixedBudgetScreenReportV1
	if json.Unmarshal(raw, &reread) != nil || localHNSWFixedBudgetScreenContractV1(reread) != nil {
		return errors.New("fixed-budget screen report reread")
	}
	_, err = fmt.Fprintf(stdout, "report=%s arms=%d queries=806 probes=2 ef=%v selected_packs=%v\n", out, len(arms), localHNSWM18EdgeDiagnosisEFV1, localHNSWM18EdgeDiagnosisPacksV1)
	return err
}
