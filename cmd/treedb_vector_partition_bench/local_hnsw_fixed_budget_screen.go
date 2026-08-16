package main

// local-hnsw-fixed-budget-screen is an offline-only, calibration-only screen
// for the four #4172 layer-0 selection coordinates. It neither publishes an
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
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWFixedBudgetScreenSchemaV1 = "treedb_local_hnsw_fixed_budget_screen_v1"

type localHNSWFixedBudgetScreenArmV1 struct {
	Name    string                                         `json:"name"`
	Variant collections.VectorPartitionLocalGraphVariantV1 `json:"variant"`
}

var localHNSWFixedBudgetScreenArmsV1 = []localHNSWFixedBudgetScreenArmV1{
	{"m_backfill_off", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0InitialMBackfillOffV1},
	{"m_backfill_on", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0InitialMBackfillOnV1},
	{"2m_backfill_off", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MBackfillOffV1},
	{"2m_backfill_on", collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MBackfillOnV1},
}

type localHNSWFixedBudgetScreenArmResultV1 struct {
	Arm                 localHNSWFixedBudgetScreenArmV1                `json:"arm"`
	Build               localHNSWAttributionBuildEvidenceV1            `json:"build"`
	SelectedDiagnostics []collections.VectorPartitionPackDiagnosticsV1 `json:"selected_pack_diagnostics"`
	Cells               []localHNSWM18EdgeDiagnosisCellV1              `json:"cells"`
	TruthHitSlots       []uint64                                       `json:"truth_hit_slots"`
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
	Limitations  []string                                `json:"limitations"`
}

func localHNSWFixedBudgetScreenContractV1(report localHNSWFixedBudgetScreenReportV1) error {
	if report.Schema != localHNSWFixedBudgetScreenSchemaV1 || report.ResultKind != "local_hnsw_fixed_budget_screen_v1" || report.Status != "valid" || !slices.Equal(report.VariantPacks, localHNSWM18EdgeDiagnosisPacksV1) || report.Probes != 2 || !slices.Equal(report.EFSearch, localHNSWM18EdgeDiagnosisEFV1) || report.Queries != 806 || len(report.Arms) != len(localHNSWFixedBudgetScreenArmsV1) {
		return errors.New("invalid fixed-budget screen contract")
	}
	for i, arm := range report.Arms {
		if arm.Arm != localHNSWFixedBudgetScreenArmsV1[i] || arm.Build.Variant != string(arm.Arm.Variant) || arm.Build.Partitions != 40 || arm.Build.PackBytes == 0 || len(arm.SelectedDiagnostics) != len(report.VariantPacks) || len(arm.Cells) != len(report.EFSearch) || len(arm.TruthHitSlots) != len(report.EFSearch) {
			return errors.New("invalid fixed-budget screen arm")
		}
		for j, cell := range arm.Cells {
			if cell.EFSearch != report.EFSearch[j] || cell.Queries != report.Queries || arm.TruthHitSlots[j] > uint64(report.Queries*10) || cell.Work.Candidates == 0 || !localHNSWAttributionQueryUtilityConservedV1(cell.Work.Utility, cell.Work.Edges) {
				return errors.New("invalid fixed-budget screen cell")
			}
		}
	}
	return nil
}

func localHNSWFixedBudgetScreenSelectedDiagnosticsV1(all []collections.VectorPartitionPackDiagnosticsV1) ([]collections.VectorPartitionPackDiagnosticsV1, error) {
	if len(all) != 40 {
		return nil, errors.New("invalid fixed-budget diagnostics")
	}
	out := make([]collections.VectorPartitionPackDiagnosticsV1, len(localHNSWM18EdgeDiagnosisPacksV1))
	for i, partition := range localHNSWM18EdgeDiagnosisPacksV1 {
		if int(partition) >= len(all) || all[partition].Rows == 0 {
			return nil, errors.New("invalid fixed-budget selected diagnostic")
		}
		out[i] = all[partition]
	}
	return out, nil
}

func localHNSWFixedBudgetScreenBuildV1(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, tempRoot string, calibration localHNSWAttributionCalibrationV1) ([]localHNSWFixedBudgetScreenArmResultV1, error) {
	if source == nil || len(calibration.Queries) != 806 || len(calibration.Truth) != 806 {
		return nil, errors.New("invalid fixed-budget screen inputs")
	}
	out := make([]localHNSWFixedBudgetScreenArmResultV1, len(localHNSWFixedBudgetScreenArmsV1))
	for i, arm := range localHNSWFixedBudgetScreenArmsV1 {
		h, build, err := localHNSWAttributionBuildVariantV1(source, tempRoot, arm.Variant, 4172000+uint32(i))
		if err != nil {
			return nil, err
		}
		if len(h.constructionEvidence.Partitions) != 40 {
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
			out[i].Cells, err = localHNSWM18EdgeDiagnosisBuildV1(ctx, source, h, calibration)
		}
		closeErr := h.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		out[i].Arm, out[i].Build = arm, build
		out[i].TruthHitSlots = make([]uint64, len(out[i].Cells))
		for j, cell := range out[i].Cells {
			out[i].TruthHitSlots[j] = uint64(math.Round(cell.EndToEndRecall.Mean * float64(len(calibration.Queries)*10)))
		}
	}
	return out, nil
}

func runLocalHNSWFixedBudgetScreenV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-fixed-budget-screen", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, retainedDB, calibrationPath, tempRoot, out string
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&retainedDB, "retained-db", "", "literal retained M18 database")
	fs.StringVar(&calibrationPath, "calibration-split", "", "frozen calibration manifest")
	fs.StringVar(&tempRoot, "temp-root", "", "existing fast temporary root")
	fs.StringVar(&out, "out", "", "new screen JSON path")
	if fs.Parse(args) != nil || fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationPath == "" || tempRoot == "" || out == "" {
		return errors.New("local-hnsw-fixed-budget-screen requires frozen inputs and fresh output")
	}
	var err error
	for ptr, value := range map[*string]string{&dataset: dataset, &retainedDB: retainedDB, &calibrationPath: calibrationPath, &tempRoot: tempRoot, &out: out} {
		if *ptr, err = m8CanonicalPathV1(value); err != nil {
			return err
		}
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
	calibration, _, err := loadLocalHNSWQuerySplitV1(calibrationPath)
	if err != nil || len(calibration.Ordinals) != 806 {
		return errors.New("fixed-budget screen calibration identity")
	}
	source, err := openM8ProductionExistingAssetSetV1(retainedDB)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, source.Close()) }()
	if err := localHNSWRepairCalibrationBindDescriptorV1(source, fixture); err != nil {
		return err
	}
	queries, err := localHNSWAttributionCalibrationV1Build(source, fixture, calibration.Ordinals)
	if err != nil {
		return err
	}
	arms, err := localHNSWFixedBudgetScreenBuildV1(context.Background(), source, tempRoot, queries)
	if err != nil {
		return err
	}
	report := localHNSWFixedBudgetScreenReportV1{Schema: localHNSWFixedBudgetScreenSchemaV1, ResultKind: "local_hnsw_fixed_budget_screen_v1", Status: "valid", VariantPacks: append([]uint32(nil), localHNSWM18EdgeDiagnosisPacksV1...), Probes: 2, EFSearch: append([]int(nil), localHNSWM18EdgeDiagnosisEFV1...), Queries: 806, Arms: arms, Limitations: []string{"offline calibration-only selected-pack screen; no holdout outcomes opened", "no postfill, candidate extension, insertion-order, Vamana, router, probe, top-k, or EF policy changes"}}
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
