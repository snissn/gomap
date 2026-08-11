package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWRepairMCurveSchemaV1 = "treedb_local_hnsw_repair_m_curve_v1"

var localHNSWRepairMCurvePointsV1 = []localHNSWRepairConstructionPointV1{
	{M: 16, EfConstruction: 256, Variant: collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1},
	{M: 24, EfConstruction: 256, Variant: collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1},
	{M: 32, EfConstruction: 256, Variant: collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1},
}

type localHNSWRepairMCurveReportV1 struct {
	Schema      string                                   `json:"schema"`
	ResultKind  string                                   `json:"result_kind"`
	Status      string                                   `json:"status"`
	GeneratedAt string                                   `json:"generated_at"`
	Provenance  localHNSWAttributionProvenanceV1         `json:"provenance"`
	Host        m8ProductionHostEvidenceV1               `json:"host"`
	Inputs      localHNSWAttributionInputsEvidenceV1     `json:"inputs"`
	Source      localHNSWAttributionSourceEvidenceV1     `json:"source"`
	TopK        int                                      `json:"top_k"`
	EFSearch    int                                      `json:"ef_search"`
	ProbeCounts []int                                    `json:"probe_counts"`
	Points      []localHNSWRepairConstructionCurveCellV1 `json:"points"`
	Disposition string                                   `json:"disposition"`
	Limitations []string                                 `json:"limitations"`
}

func localHNSWRepairMCurveV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, tempRoot string, ordinals []int, queries [][]float32, truth [][]m8CanonicalResultV1) ([]localHNSWRepairConstructionCurveCellV1, error) {
	return localHNSWRepairConstructionPointsV1Build(ctx, source, tempRoot, localHNSWRepairMCurvePointsV1, ordinals, queries, truth)
}

func localHNSWRepairMCurveDispositionV1(cells []localHNSWRepairConstructionCurveCellV1) (string, error) {
	if len(cells) != len(localHNSWRepairMCurvePointsV1) {
		return "", errors.New("invalid local HNSW repair M cells")
	}
	for i, cell := range cells {
		point := localHNSWRepairMCurvePointsV1[i]
		if cell.M != point.M || cell.EfConstruction != point.EfConstruction || !localHNSWAttributionFiniteRecallV1(cell.Quality.RoutingRecall.Mean) || !localHNSWAttributionFiniteRecallV1(cell.Quality.P2Recall.Mean) || !localHNSWAttributionFiniteRecallV1(cell.Quality.P16Recall.Mean) {
			return "", errors.New("invalid local HNSW repair M cell")
		}
		if cell.Quality.P2Recall.Mean >= .95 && math.Abs(cell.Quality.P2Recall.Mean-cell.Quality.P16Recall.Mean) <= .002 {
			return fmt.Sprintf("smallest_m_passes_local_quality_m_%d", cell.M), nil
		}
	}
	return "no_point_passes_local_quality", nil
}

func validateLocalHNSWRepairMCurveReportV1(report localHNSWRepairMCurveReportV1) error {
	if report.Schema != localHNSWRepairMCurveSchemaV1 || report.ResultKind != "local_hnsw_repair_m_curve_v1" || report.Status != "valid" || report.Provenance.BaseSHA != localHNSWAttributionSourceLockV1 || report.Provenance.SourceDirty || !validLowerSHA(report.Provenance.HeadSHA) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.TopK != 10 || report.EFSearch != 128 || !slices.Equal(report.ProbeCounts, []int{2, 16}) || len(report.Points) != len(localHNSWRepairMCurvePointsV1) || !localHNSWAttributionFixtureV1(report.Inputs.Fixture) || report.Inputs.DatasetManifest.SHA256 != localHNSWAttributionFixtureManifestSHA256V1 || report.Inputs.Descriptor.SHA256 != localHNSWAttributionDescriptorSHA256V1 || report.Inputs.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Inputs.Holdout.SHA256 != localHNSWAttributionHoldoutSHA256V1 || report.Inputs.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || report.Inputs.CalibrationRows != 806 || report.Inputs.HoldoutRows != 194 || report.Inputs.HoldoutStatus != "manifest_validated_query_outcomes_unopened" || report.Inputs.TruthStatus != "sha256_only_not_decoded" || report.Source.Partitions != 16 || report.Source.SourceRows != 250000 || len(report.Source.PartitionLoads) != 16 {
		return errors.New("invalid local HNSW repair M report")
	}
	routes := ""
	for i, point := range report.Points {
		want := localHNSWRepairMCurvePointsV1[i]
		if point.M != want.M || point.EfConstruction != want.EfConstruction || point.Build.Variant != string(want.Variant) || point.Build.VariantIdentity == "" || point.Build.Partitions != 16 || point.Build.PackBytes == 0 || point.Graph.Rows != 300000 || point.Graph.CombinedReachableRows != 300000 || point.Graph.NativeTraversalRoots < 16 || point.Graph.AuxiliaryEdges != 2*(point.Graph.NativeTraversalRoots-16) || point.Graph.AuxiliaryMaxDegree > 9 || !localHNSWAttributionSHA256V1(point.DefinitionDigest) || !localHNSWAttributionSHA256V1(point.PackMembershipSHA256) || !localHNSWAttributionSHA256V1(point.PackChecksumsSHA256) || point.Quality.EFSearch != 128 || point.Quality.QueryCount != 806 || !localHNSWAttributionSHA256V1(point.Quality.RoutesSHA256) || point.Quality.P2Work.Candidates == 0 || point.Quality.P16Work.Candidates == 0 || point.Quality.P2Work.NativeEdges == 0 || point.Quality.P16Work.NativeEdges == 0 || !localHNSWAttributionFiniteRecallV1(point.Quality.P2Recall.Mean) || !localHNSWAttributionFiniteRecallV1(point.Quality.P16Recall.Mean) || !localHNSWAttributionFiniteRecallV1(point.Quality.RoutingRecall.Mean) {
			return errors.New("invalid local HNSW repair M point")
		}
		if routes != "" && routes != point.Quality.RoutesSHA256 {
			return errors.New("local HNSW repair M route drift")
		}
		routes = point.Quality.RoutesSHA256
	}
	want, err := localHNSWRepairMCurveDispositionV1(report.Points)
	if err != nil || report.Disposition != want {
		return errors.New("invalid local HNSW repair M disposition")
	}
	return nil
}
