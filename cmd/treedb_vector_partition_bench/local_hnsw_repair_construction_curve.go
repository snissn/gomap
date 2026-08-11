package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWRepairConstructionCurveSchemaV1 = "treedb_local_hnsw_repair_construction_curve_v1"

var localHNSWRepairConstructionCurvePointsV1 = []int{128, 256, 512}

type localHNSWRepairConstructionCurveCellV1 struct {
	EfConstruction       int                                 `json:"ef_construction"`
	DefinitionDigest     string                              `json:"definition_digest"`
	PackMembershipSHA256 string                              `json:"pack_membership_sha256"`
	PackChecksumsSHA256  string                              `json:"pack_checksums_sha256"`
	Build                localHNSWAttributionBuildEvidenceV1 `json:"build"`
	Graph                localHNSWRepairCalibrationGraphV1   `json:"graph"`
	Quality              localHNSWRepairEFCurveCellV1        `json:"quality"`
}

type localHNSWRepairConstructionPointResultV1 struct {
	M                    int
	EfConstruction       int
	DefinitionDigest     string
	PackMembershipSHA256 string
	PackChecksumsSHA256  string
	Build                localHNSWAttributionBuildEvidenceV1
	Graph                localHNSWRepairCalibrationGraphV1
	Quality              localHNSWRepairEFCurveCellV1
}

type localHNSWRepairConstructionPointV1 struct {
	M              int
	EfConstruction int
	Variant        collections.VectorPartitionLocalGraphVariantV1
}

type localHNSWRepairConstructionCurveReportV1 struct {
	Schema      string                                   `json:"schema"`
	ResultKind  string                                   `json:"result_kind"`
	Status      string                                   `json:"status"`
	GeneratedAt string                                   `json:"generated_at"`
	Provenance  localHNSWAttributionProvenanceV1         `json:"provenance"`
	Host        m8ProductionHostEvidenceV1               `json:"host"`
	Inputs      localHNSWAttributionInputsEvidenceV1     `json:"inputs"`
	Source      localHNSWAttributionSourceEvidenceV1     `json:"source"`
	TopK        int                                      `json:"top_k"`
	M           int                                      `json:"m"`
	EFSearch    int                                      `json:"ef_search"`
	ProbeCounts []int                                    `json:"probe_counts"`
	Points      []localHNSWRepairConstructionCurveCellV1 `json:"points"`
	Disposition string                                   `json:"disposition"`
	Limitations []string                                 `json:"limitations"`
}

func localHNSWRepairConstructionCurveVariantV1(efConstruction int) (collections.VectorPartitionLocalGraphVariantV1, error) {
	switch efConstruction {
	case 128:
		return collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, nil
	case 256:
		return collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1, nil
	case 512:
		return collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1, nil
	default:
		return "", errors.New("invalid local HNSW repair construction point")
	}
}

func localHNSWRepairConstructionCurvePointsValidV1(points []int) bool {
	return slices.Equal(points, localHNSWRepairConstructionCurvePointsV1)
}

func localHNSWRepairConstructionCurvePackIdentityV1(harness *localHNSWVariantHarnessV1) (string, string, error) {
	if harness == nil || len(harness.packAssets) == 0 || len(harness.packAssets) != len(harness.searchers) {
		return "", "", errors.New("invalid local HNSW construction packs")
	}
	memberships, checksums := sha256.New(), sha256.New()
	memberships.Write([]byte("treedb-4106-local-hnsw-construction-memberships-v1/"))
	checksums.Write([]byte("treedb-4106-local-hnsw-construction-checksums-v1/"))
	var raw [4]byte
	for partition, asset := range harness.packAssets {
		if asset.PartitionID != uint32(partition) || !localHNSWAttributionSHA256V1(asset.MembershipDigest) || !localHNSWAttributionSHA256V1(asset.Checksum) {
			return "", "", errors.New("invalid local HNSW construction pack")
		}
		binary.LittleEndian.PutUint32(raw[:], asset.PartitionID)
		memberships.Write(raw[:])
		memberships.Write([]byte(asset.MembershipDigest))
		checksums.Write(raw[:])
		checksums.Write([]byte(asset.Checksum))
	}
	return fmt.Sprintf("%x", memberships.Sum(nil)), fmt.Sprintf("%x", checksums.Sum(nil)), nil
}

func localHNSWRepairConstructionCurveV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, tempRoot string, ordinals []int, queries [][]float32, truth [][]m8CanonicalResultV1) ([]localHNSWRepairConstructionCurveCellV1, error) {
	if !localHNSWRepairConstructionCurvePointsValidV1(localHNSWRepairConstructionCurvePointsV1) {
		return nil, errors.New("invalid local HNSW repair construction points")
	}
	points := make([]localHNSWRepairConstructionPointV1, len(localHNSWRepairConstructionCurvePointsV1))
	for i, efConstruction := range localHNSWRepairConstructionCurvePointsV1 {
		variant, err := localHNSWRepairConstructionCurveVariantV1(efConstruction)
		if err != nil {
			return nil, err
		}
		points[i] = localHNSWRepairConstructionPointV1{M: 16, EfConstruction: efConstruction, Variant: variant}
	}
	raw, err := localHNSWRepairConstructionPointsV1Build(ctx, source, tempRoot, points, ordinals, queries, truth)
	if err != nil {
		return nil, err
	}
	out := make([]localHNSWRepairConstructionCurveCellV1, len(raw))
	for i, point := range raw {
		out[i] = localHNSWRepairConstructionCurveCellV1{EfConstruction: point.EfConstruction, DefinitionDigest: point.DefinitionDigest, PackMembershipSHA256: point.PackMembershipSHA256, PackChecksumsSHA256: point.PackChecksumsSHA256, Build: point.Build, Graph: point.Graph, Quality: point.Quality}
	}
	return out, nil
}

func localHNSWRepairConstructionPointsV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, tempRoot string, points []localHNSWRepairConstructionPointV1, ordinals []int, queries [][]float32, truth [][]m8CanonicalResultV1) ([]localHNSWRepairConstructionPointResultV1, error) {
	if source == nil || len(points) == 0 || len(ordinals) == 0 || len(ordinals) != len(queries) || len(ordinals) != len(truth) {
		return nil, errors.New("invalid local HNSW repair construction inputs")
	}
	var sourceDefinition collections.VectorIndexDefinition
	for _, definition := range source.collection.Meta().VectorIndexes {
		if definition.Name == source.manifest.IndexName {
			sourceDefinition = definition
			break
		}
	}
	if sourceDefinition.M != 16 || sourceDefinition.EfConstruction != 128 || sourceDefinition.EfSearch != 128 {
		return nil, errors.New("invalid local HNSW repair construction source definition")
	}
	out := make([]localHNSWRepairConstructionPointResultV1, 0, len(points))
	for pointIndex, point := range points {
		if point.M < 2 || point.EfConstruction < point.M {
			return nil, errors.New("invalid local HNSW repair construction point")
		}
		harness, build, err := localHNSWAttributionBuildVariantV1(source, tempRoot, point.Variant, uint32(4106200+pointIndex))
		if err != nil {
			return nil, err
		}
		graph, graphErr := localHNSWRepairCalibrationGraphV1Build(harness)
		curve, curveErr := localHNSWRepairEFCurveV1Build(ctx, source, harness, []int{128}, ordinals, queries, truth)
		membershipSHA, checksumSHA, identityErr := localHNSWRepairConstructionCurvePackIdentityV1(harness)
		closeErr := harness.Close()
		if graphErr != nil || curveErr != nil || identityErr != nil || closeErr != nil || len(curve) != 1 || curve[0].EFSearch != 128 {
			return nil, errors.Join(graphErr, curveErr, identityErr, closeErr, errors.New("invalid local HNSW repair construction point"))
		}
		definition := sourceDefinition
		definition.M, definition.EfConstruction = point.M, point.EfConstruction
		if graph.Rows == 0 || graph.CombinedReachableRows != graph.Rows || build.Variant != string(point.Variant) || build.VariantIdentity == "" || !localHNSWAttributionSHA256V1(membershipSHA) || !localHNSWAttributionSHA256V1(checksumSHA) {
			return nil, errors.New("invalid local HNSW repair construction evidence")
		}
		out = append(out, localHNSWRepairConstructionPointResultV1{M: point.M, EfConstruction: point.EfConstruction, DefinitionDigest: collections.VectorIndexDefinitionDigestV1(definition), PackMembershipSHA256: membershipSHA, PackChecksumsSHA256: checksumSHA, Build: build, Graph: graph, Quality: curve[0]})
	}
	return out, nil
}

func localHNSWRepairConstructionCurveDispositionV1(cells []localHNSWRepairConstructionCurveCellV1) (string, error) {
	if len(cells) != len(localHNSWRepairConstructionCurvePointsV1) {
		return "", errors.New("invalid local HNSW repair construction cells")
	}
	for i, cell := range cells {
		if cell.EfConstruction != localHNSWRepairConstructionCurvePointsV1[i] || !localHNSWAttributionFiniteRecallV1(cell.Quality.RoutingRecall.Mean) || !localHNSWAttributionFiniteRecallV1(cell.Quality.P2Recall.Mean) || !localHNSWAttributionFiniteRecallV1(cell.Quality.P16Recall.Mean) {
			return "", errors.New("invalid local HNSW repair construction cell")
		}
		if cell.Quality.P2Recall.Mean >= .95 && math.Abs(cell.Quality.P2Recall.Mean-cell.Quality.P16Recall.Mean) <= .002 && cell.Quality.RoutingRecall.Mean >= .998 {
			return fmt.Sprintf("smallest_point_passes_ef_construction_%d", cell.EfConstruction), nil
		}
	}
	return "no_point_passes", nil
}

func runLocalHNSWRepairConstructionCurveV1(args []string, stdout io.Writer) error {
	return runLocalHNSWRepairConstructionCurveModeV1(args, stdout, false)
}

func runLocalHNSWRepairMCurveV1(args []string, stdout io.Writer) error {
	return runLocalHNSWRepairConstructionCurveModeV1(args, stdout, true)
}

func runLocalHNSWRepairConstructionCurveModeV1(args []string, stdout io.Writer, mCurve bool) (runErr error) {
	command := "local-hnsw-repair-construction-curve"
	if mCurve {
		command = "local-hnsw-repair-m-curve"
	}
	fs := flag.NewFlagSet("treedb_vector_partition_bench "+command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, retainedDB, calibrationSplit, holdoutSplit, truthArtifact, historicalCSV, tempRoot, out, baseSHA, headSHA, sourceCheckout string
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&retainedDB, "retained-db", "", "literal retained 250k database")
	fs.StringVar(&calibrationSplit, "calibration-split", "", "frozen calibration manifest")
	fs.StringVar(&holdoutSplit, "holdout-split", "", "sealed holdout manifest")
	fs.StringVar(&truthArtifact, "truth-artifact", "", "sealed trusted truth artifact")
	fs.StringVar(&historicalCSV, "historical-search", "", "three comma-separated retained search reports")
	fs.StringVar(&tempRoot, "temp-root", "", "existing fast temporary root")
	fs.StringVar(&out, "out", "", "fresh report path")
	fs.StringVar(&baseSHA, "base-sha", "", "source-lock base SHA")
	fs.StringVar(&headSHA, "head-sha", "", "exact implementation head SHA")
	fs.StringVar(&sourceCheckout, "source-checkout", "", "clean exact-head checkout")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationSplit == "" || holdoutSplit == "" || truthArtifact == "" || historicalCSV == "" || tempRoot == "" || out == "" || baseSHA == "" || headSHA == "" || sourceCheckout == "" {
		return errors.New("local-hnsw-repair-construction-curve requires all frozen inputs, paths, provenance, and no positional arguments")
	}
	var err error
	for destination, value := range map[*string]string{&dataset: dataset, &retainedDB: retainedDB, &calibrationSplit: calibrationSplit, &holdoutSplit: holdoutSplit, &truthArtifact: truthArtifact, &tempRoot: tempRoot, &out: out, &sourceCheckout: sourceCheckout} {
		*destination, err = m8CanonicalPathV1(value)
		if err != nil {
			return err
		}
	}
	baseSHA, headSHA, err = provenanceWithExplicitV1(baseSHA, headSHA)
	if err != nil || baseSHA != localHNSWAttributionSourceLockV1 {
		return errors.New("local HNSW repair construction source lock")
	}
	sourceCheckout, err = localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA)
	if err != nil || m8GitDirtyInV1(sourceCheckout) || filepath.Ext(out) != ".json" {
		return errors.New("invalid local HNSW repair construction provenance or output")
	}
	if info, statErr := os.Lstat(tempRoot); statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("invalid local HNSW repair construction temporary root")
	}
	if _, statErr := os.Lstat(out); !errors.Is(statErr, os.ErrNotExist) {
		return errors.New("local HNSW repair construction output exists")
	}
	parts := strings.Split(historicalCSV, ",")
	if len(parts) != 3 {
		return errors.New("local HNSW repair construction requires three historical reports")
	}
	var historicalPaths [3]string
	for i, path := range parts {
		historicalPaths[i], err = m8CanonicalPathV1(strings.TrimSpace(path))
		if err != nil {
			return err
		}
	}
	datasetManifest := filepath.Join(dataset, "fixture_manifest.json")
	if digest, hashErr := localHNSWAttributionRegularFileSHA256V1(datasetManifest, maxManifestBytes); hashErr != nil || digest != localHNSWAttributionFixtureManifestSHA256V1 {
		return errors.New("local HNSW repair construction dataset identity")
	}
	fixture, err := loadFixture(dataset)
	if err != nil || !localHNSWAttributionFixtureV1(fixture) {
		return errors.New("local HNSW repair construction fixture identity")
	}
	inputConfig := localHNSWAttributionInputConfigV1{Fixture: fixture, RetainedDB: retainedDB, Descriptor: filepath.Join(retainedDB, m3VariantDescriptorFileV1), CalibrationSplit: calibrationSplit, HoldoutSplit: holdoutSplit, TruthArtifact: truthArtifact, HistoricalSearchReports: historicalPaths, DescriptorSHA256: localHNSWAttributionDescriptorSHA256V1, CalibrationSplitSHA256: localHNSWAttributionCalibrationSHA256V1, HoldoutSplitSHA256: localHNSWAttributionHoldoutSHA256V1, TruthArtifactSHA256: localHNSWAttributionTruthSHA256V1, HistoricalReportSHA256: localHNSWAttributionHistoricalSHA256V1}
	inputs, err := localHNSWAttributionInputsV1(inputConfig)
	if err != nil {
		return err
	}
	historical, err := localHNSWAttributionHistoricalBaselineV1(inputConfig)
	if err != nil {
		return err
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
	if err != nil {
		return err
	}
	source, err := openM8ProductionExistingAssetSetV1(retainedDB)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, source.Close()) }()
	if err := localHNSWRepairCalibrationBindDescriptorV1(source, fixture); err != nil {
		return err
	}
	calibration, err := localHNSWAttributionCalibrationV1Build(source, fixture, inputs.Calibration.Ordinals)
	if err != nil || calibration.Schema != localHNSWAttributionCalibrationSchemaV1 || len(calibration.Ordinals) != 806 {
		return errors.New("local HNSW repair construction query source")
	}
	var points []localHNSWRepairConstructionCurveCellV1
	var mPoints []localHNSWRepairMCurveCellV1
	if mCurve {
		mPoints, err = localHNSWRepairMCurveV1Build(context.Background(), source, tempRoot, calibration.Ordinals, calibration.Queries, calibration.Truth)
	} else {
		points, err = localHNSWRepairConstructionCurveV1Build(context.Background(), source, tempRoot, calibration.Ordinals, calibration.Queries, calibration.Truth)
	}
	if err != nil {
		return err
	}
	if _, err := localHNSWAttributionInputsV1(inputConfig); err != nil {
		return fmt.Errorf("local HNSW repair construction inputs changed: %w", err)
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW repair construction source changed")
	}
	if digest, hashErr := m8BenchmarkExecutableSHA256V1(executable); hashErr != nil || digest != executableSHA {
		return errors.New("local HNSW repair construction executable changed")
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil || source.descriptor == nil {
		return errors.New("local HNSW repair construction source evidence")
	}
	var disposition string
	if mCurve {
		disposition, err = localHNSWRepairMCurveDispositionV1(mPoints)
	} else {
		disposition, err = localHNSWRepairConstructionCurveDispositionV1(points)
	}
	if err != nil {
		return err
	}
	provenance := localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1(command, args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA}
	inputEvidence := localHNSWAttributionInputsEvidenceV1{DatasetManifest: localHNSWAttributionFileInputV1{Path: datasetManifest, SHA256: localHNSWAttributionFixtureManifestSHA256V1}, Fixture: fixture, RetainedDB: retainedDB, Descriptor: localHNSWAttributionFileInputV1{Path: inputConfig.Descriptor, SHA256: inputConfig.DescriptorSHA256}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationSplit, SHA256: inputConfig.CalibrationSplitSHA256}, CalibrationRows: len(inputs.Calibration.Ordinals), Holdout: localHNSWAttributionFileInputV1{Path: holdoutSplit, SHA256: inputConfig.HoldoutSplitSHA256}, HoldoutRows: len(inputs.Holdout.Ordinals), HoldoutStatus: "manifest_validated_query_outcomes_unopened", Truth: localHNSWAttributionFileInputV1{Path: truthArtifact, SHA256: inputConfig.TruthArtifactSHA256}, TruthStatus: "sha256_only_not_decoded", Historical: historical}
	sourceEvidence := localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor}
	limitations := []string{"offline calibration-only construction-quality pre-gate; not product qualification", "holdout query outcomes and trusted truth contents remained unopened", "profiles, repeated timing, 100k, and distributed guardrails are deferred"}
	if mCurve {
		report := localHNSWRepairMCurveReportV1{Schema: localHNSWRepairMCurveSchemaV1, ResultKind: "local_hnsw_repair_m_curve_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: provenance, Host: m8ProductionHostV1(config{out: out, dataset: dataset}, retainedDB), Inputs: inputEvidence, Source: sourceEvidence, TopK: 10, EFSearch: 128, ProbeCounts: []int{2, int(source.manifest.PartitionCount)}, Points: mPoints, Disposition: disposition, Limitations: append(limitations, "local-quality disposition excludes the separately reported invariant routing recall; it is not the full #4106 gate")}
		if err := validateLocalHNSWRepairMCurveReportV1(report); err != nil {
			return err
		}
		if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "report=%s points=m16,m24,m32 disposition=%s\n", out, disposition)
		return err
	}
	report := localHNSWRepairConstructionCurveReportV1{Schema: localHNSWRepairConstructionCurveSchemaV1, ResultKind: "local_hnsw_repair_construction_curve_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: provenance, Host: m8ProductionHostV1(config{out: out, dataset: dataset}, retainedDB), Inputs: inputEvidence, Source: sourceEvidence, TopK: 10, M: 16, EFSearch: 128, ProbeCounts: []int{2, int(source.manifest.PartitionCount)}, Points: points, Disposition: disposition, Limitations: limitations}
	if err := validateLocalHNSWRepairConstructionCurveReportV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "report=%s points=%v disposition=%s\n", out, localHNSWRepairConstructionCurvePointsV1, disposition)
	return err
}

func validateLocalHNSWRepairConstructionCurveReportV1(report localHNSWRepairConstructionCurveReportV1) error {
	if report.Schema != localHNSWRepairConstructionCurveSchemaV1 || report.ResultKind != "local_hnsw_repair_construction_curve_v1" || report.Status != "valid" || report.Provenance.BaseSHA != localHNSWAttributionSourceLockV1 || report.Provenance.SourceDirty || !validLowerSHA(report.Provenance.HeadSHA) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.TopK != 10 || report.M != 16 || report.EFSearch != 128 || !slices.Equal(report.ProbeCounts, []int{2, 16}) || len(report.Points) != 3 || !localHNSWAttributionFixtureV1(report.Inputs.Fixture) || report.Inputs.DatasetManifest.SHA256 != localHNSWAttributionFixtureManifestSHA256V1 || report.Inputs.Descriptor.SHA256 != localHNSWAttributionDescriptorSHA256V1 || report.Inputs.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Inputs.Holdout.SHA256 != localHNSWAttributionHoldoutSHA256V1 || report.Inputs.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || report.Inputs.CalibrationRows != 806 || report.Inputs.HoldoutRows != 194 || report.Inputs.HoldoutStatus != "manifest_validated_query_outcomes_unopened" || report.Inputs.TruthStatus != "sha256_only_not_decoded" || report.Source.Partitions != 16 || report.Source.SourceRows != 250000 || len(report.Source.PartitionLoads) != 16 || !localHNSWAttributionSHA256V1(report.Source.Descriptor.IndexDefinitionDigest) {
		return errors.New("invalid local HNSW repair construction report")
	}
	for i, historical := range report.Inputs.Historical {
		if historical.SHA256 != localHNSWAttributionHistoricalSHA256V1[i] || historical.TopologyIdentitySHA256 != localHNSWAttributionHistoricalTopologySHA256V1[i] || historical.Probe2.Probes != 2 || historical.Probe16.Probes != 16 {
			return errors.New("invalid local HNSW repair construction historical context")
		}
	}
	routes := ""
	memberships := map[string]struct{}{}
	checksums := map[string]struct{}{}
	for i, point := range report.Points {
		variant, err := localHNSWRepairConstructionCurveVariantV1(point.EfConstruction)
		if err != nil || point.EfConstruction != localHNSWRepairConstructionCurvePointsV1[i] || point.Build.Variant != string(variant) || point.Build.VariantIdentity == "" || point.Build.Partitions != 16 || point.Build.PackBytes == 0 || point.Graph.Rows != 300000 || point.Graph.CombinedReachableRows != 300000 || point.Graph.NativeTraversalRoots < 16 || point.Graph.AuxiliaryEdges != 2*(point.Graph.NativeTraversalRoots-16) || point.Graph.AuxiliaryMaxDegree > 9 || !localHNSWAttributionSHA256V1(point.DefinitionDigest) || !localHNSWAttributionSHA256V1(point.PackMembershipSHA256) || !localHNSWAttributionSHA256V1(point.PackChecksumsSHA256) || point.Quality.EFSearch != 128 || point.Quality.QueryCount != 806 || !localHNSWAttributionSHA256V1(point.Quality.RoutesSHA256) || point.Quality.P2Work.Candidates == 0 || point.Quality.P16Work.Candidates == 0 || point.Quality.P2Work.NativeEdges == 0 || point.Quality.P16Work.NativeEdges == 0 || !localHNSWAttributionFiniteRecallV1(point.Quality.P2Recall.Mean) || !localHNSWAttributionFiniteRecallV1(point.Quality.P16Recall.Mean) || !localHNSWAttributionFiniteRecallV1(point.Quality.RoutingRecall.Mean) {
			return errors.New("invalid local HNSW repair construction point")
		}
		if i == 0 && point.DefinitionDigest != report.Source.Descriptor.IndexDefinitionDigest {
			return errors.New("invalid local HNSW repair construction source definition")
		}
		if routes != "" && routes != point.Quality.RoutesSHA256 {
			return errors.New("local HNSW repair construction route drift")
		}
		routes = point.Quality.RoutesSHA256
		memberships[point.PackMembershipSHA256] = struct{}{}
		checksums[point.PackChecksumsSHA256] = struct{}{}
	}
	if len(memberships) != len(report.Points) || len(checksums) != len(report.Points) {
		return errors.New("local HNSW repair construction pack identity drift")
	}
	want, err := localHNSWRepairConstructionCurveDispositionV1(report.Points)
	if err != nil || report.Disposition != want {
		return errors.New("invalid local HNSW repair construction disposition")
	}
	return nil
}
