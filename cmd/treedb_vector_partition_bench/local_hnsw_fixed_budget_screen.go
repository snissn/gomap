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
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWFixedBudgetScreenSchemaV1 = "treedb_local_hnsw_fixed_budget_screen_v2"

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
	if report.Schema != localHNSWFixedBudgetScreenSchemaV1 || report.ResultKind != "local_hnsw_fixed_budget_screen_v1" || report.Status != "valid" || !slices.Equal(report.VariantPacks, localHNSWM18EdgeDiagnosisPacksV1) || report.Probes != 2 || !slices.Equal(report.EFSearch, localHNSWM18EdgeDiagnosisEFV1) || report.Queries != 806 || len(report.Arms) != len(localHNSWFixedBudgetScreenArmsV1) || report.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || report.Descriptor.SHA256 != localHNSWM18DescriptorSHA256V1 || report.Provenance.BaseSHA != "2a7d01443d3c842990c259b08bd442a4d0109511" || report.Provenance.SourceDirty || !m8QualificationGitSHAV1(report.Provenance.HeadSHA) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.Source.ManifestIntegrity != report.Manifest || report.Source.Descriptor.ArtifactSHA256 != localHNSWM18AssignmentSHA256V1 || report.Source.Descriptor.GraphArtifactSHA256 != localHNSWM18GraphSHA256V1 || report.Source.Descriptor.ShardGenerationDigest != localHNSWM18ShardGenerationSHA256V1 {
		return errors.New("invalid fixed-budget screen contract")
	}
	var expectedOpportunities uint64
	expectedPerPackOpportunities := make([]uint64, len(report.VariantPacks))
	haveExpectedOpportunities := false
	for i, arm := range report.Arms {
		identity, identityErr := collections.VectorPartitionLocalGraphVariantIdentityV1(arm.Arm.Variant)
		if identityErr != nil || arm.Arm != localHNSWFixedBudgetScreenArmsV1[i] || arm.Build.Variant != string(arm.Arm.Variant) || arm.Build.VariantIdentity != identity || arm.Build.FileID != 4172000+uint32(i) || arm.Build.Partitions != len(report.VariantPacks) || arm.Build.PackBytes == 0 || len(arm.SelectedDiagnostics) != len(report.VariantPacks) || len(arm.SelectedNeighborhood) != len(report.VariantPacks) || len(arm.Cells) != len(report.EFSearch) {
			return errors.New("invalid fixed-budget screen arm")
		}
		if err := localHNSWFixedBudgetScreenNeighborhoodV1(arm.Neighborhood, arm.SelectedNeighborhood, arm.SelectedDiagnostics, report.VariantPacks); err != nil {
			return err
		}
		for j, d := range arm.SelectedDiagnostics {
			if d.Partition != report.VariantPacks[j] || d.Diagnostics.Rows == 0 {
				return errors.New("invalid fixed-budget selected diagnostic binding")
			}
		}
		for j, cell := range arm.Cells {
			if cell.EFSearch != report.EFSearch[j] || cell.QueryPackOpportunities == 0 || cell.QueryPackOpportunities > ^uint64(0)/10 || cell.LocalTruthHitSlots > cell.QueryPackOpportunities*10 || len(cell.PerPack) != len(report.VariantPacks) || cell.Work.Candidates == 0 || !localHNSWAttributionQueryUtilityConservedV1(cell.Work.Utility, cell.Work.Edges) {
				return errors.New("invalid fixed-budget screen cell")
			}
			var opportunities, hits uint64
			var work localHNSWAttributionQueryWorkV1
			for k, p := range cell.PerPack {
				if p.Partition != report.VariantPacks[k] || p.Opportunities == 0 || p.Work.Candidates == 0 || ^uint64(0)-opportunities < p.Opportunities || ^uint64(0)-hits < p.TruthHitSlots || localHNSWM18EdgeDiagnosisWorkAddV1(&work, p.Work.Candidates, p.Work.Edges, p.Work.FrontierAdmissions, p.Work.Utility) != nil {
					return errors.New("invalid fixed-budget per-pack decomposition")
				}
				opportunities += p.Opportunities
				hits += p.TruthHitSlots
			}
			if opportunities != cell.QueryPackOpportunities || hits != cell.LocalTruthHitSlots || work != cell.Work {
				return errors.New("fixed-budget aggregate decomposition")
			}
			if !haveExpectedOpportunities {
				expectedOpportunities = cell.QueryPackOpportunities
				for k := range cell.PerPack {
					expectedPerPackOpportunities[k] = cell.PerPack[k].Opportunities
				}
				haveExpectedOpportunities = true
			} else if cell.QueryPackOpportunities != expectedOpportunities {
				return errors.New("fixed-budget query opportunities vary by arm or EF")
			} else {
				for k := range cell.PerPack {
					if cell.PerPack[k].Opportunities != expectedPerPackOpportunities[k] {
						return errors.New("fixed-budget per-pack opportunities vary by arm or EF")
					}
				}
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
		isPostfill := arm.Arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1
		if !isPostfill && len(arm.PostfillBudget) != 0 || isPostfill && len(arm.PostfillBudget) != len(report.VariantPacks) {
			return errors.New("invalid fixed-budget postfill budget")
		}
		if isPostfill {
			for j, budget := range arm.PostfillBudget {
				diagnostic := arm.SelectedDiagnostics[j].Diagnostics
				if budget.Partition != report.VariantPacks[j] || budget.Rows != diagnostic.Rows || len(diagnostic.EdgesByLayer) == 0 || budget.Layer0Edges != diagnostic.EdgesByLayer[0] || budget.Rows != 7500 || budget.TargetLayer0Edges != 270000 || budget.Layer0Edges != budget.TargetLayer0Edges || budget.CandidateBytes == 0 || budget.CandidateBytes != budget.CanonicalBytes {
					return errors.New("fixed-budget least-redundant separation postfill budget mismatch")
				}
			}
		}
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
		if perPack[i].Partition != partition || one.Schema != localHNSWAttributionNeighborhoodOracleSchemaV1 || one.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || one.ExactK != localHNSWAttributionNeighborhoodExactKV1 || one.CandidateSamples == 0 || one.FinalSamples == 0 || len(one.PackDiagnostics) != 1 || !reflect.DeepEqual(one.PackDiagnostics[0], diagnostics[i].Diagnostics) || !reflect.DeepEqual(aggregate.PackDiagnostics[i], diagnostics[i].Diagnostics) {
			return errors.New("invalid fixed-budget selected neighborhood binding")
		}
		if err := localHNSWFixedBudgetScreenNeighborhoodAddV1(&totals, one); err != nil {
			return err
		}
	}
	if totals.CandidateSamples != aggregate.CandidateSamples || totals.CandidateTruthNeighbors != aggregate.CandidateTruthNeighbors || totals.CandidateTruthRecovered != aggregate.CandidateTruthRecovered || totals.FinalSamples != aggregate.FinalSamples || totals.FinalSampleTruthNeighbors != aggregate.FinalSampleTruthNeighbors || totals.FinalSampleTruthRecovered != aggregate.FinalSampleTruthRecovered || totals.AngularPairs != aggregate.AngularPairs || totals.FinalEdgesByOrigin != aggregate.FinalEdgesByOrigin || totals.FinalTruthByOrigin != aggregate.FinalTruthByOrigin {
		return errors.New("fixed-budget neighborhood aggregate decomposition")
	}
	return nil
}

func localHNSWFixedBudgetScreenNeighborhoodAddV1(dst *localHNSWAttributionNeighborhoodOracleV1, src localHNSWAttributionNeighborhoodOracleV1) error {
	if dst == nil {
		return errors.New("invalid fixed-budget neighborhood total")
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
	dst.AngularPairs += src.AngularPairs
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
		if err == nil && arm.Variant == collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1 {
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
	if err != nil || baseSHA != "2a7d01443d3c842990c259b08bd442a4d0109511" || m8GitDirtyInV1(sourceCheckout) {
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
	report := localHNSWFixedBudgetScreenReportV1{Schema: localHNSWFixedBudgetScreenSchemaV1, ResultKind: "local_hnsw_fixed_budget_screen_v1", Status: "valid", VariantPacks: append([]uint32(nil), localHNSWM18EdgeDiagnosisPacksV1...), Probes: 2, EFSearch: append([]int(nil), localHNSWM18EdgeDiagnosisEFV1...), Queries: 806, Arms: arms, Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-fixed-budget-screen", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationPath, SHA256: calibrationSHA}, Truth: localHNSWAttributionFileInputV1{Path: truthPath, SHA256: truthSHA}, Descriptor: localHNSWAttributionFileInputV1{Path: descriptorPath, SHA256: descriptorSHA}, Manifest: source.manifest.IntegrityDigest, Source: localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor}, Limitations: []string{"offline calibration-only selected-pack screen; no holdout outcomes opened", "no postfill, candidate extension, insertion-order, Vamana, router, probe, top-k, or EF policy changes"}}
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
