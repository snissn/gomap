package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"
)

var m8RequiredVariantIDsV1 = []string{"graph-disjoint-v1", "graph-overlap-020-v1", "stable-id-hash-disjoint-v1"}

type m8ProductionMatrixV1 struct {
	SchemaVersion       int                        `json:"schema_version"`
	ResultKind          string                     `json:"result_kind"`
	Status              string                     `json:"status"`
	Disposition         string                     `json:"disposition"`
	GeneratedAt         time.Time                  `json:"generated_at"`
	Command             []string                   `json:"exact_command"`
	BaseSHA             string                     `json:"base_sha"`
	HeadSHA             string                     `json:"head_sha"`
	Dataset             fixtureManifest            `json:"dataset"`
	RequiredVariants    []string                   `json:"required_variants"`
	Variants            []m8ProductionReportV1     `json:"variants"`
	Comparison          []m8ProductionComparisonV1 `json:"comparison"`
	Gates               m8ProductionMatrixGatesV1  `json:"gates"`
	OverlapStorageRatio float64                    `json:"overlap_storage_ratio"`
	Limitations         []string                   `json:"limitations"`
}

type m8ProductionComparisonV1 struct {
	VariantID            string  `json:"variant_id"`
	AssignmentBasis      string  `json:"assignment_basis"`
	Overlap              float64 `json:"overlap"`
	ArtifactSHA256       string  `json:"artifact_sha256"`
	ReadySetDigest       string  `json:"ready_set_digest"`
	RouterModelDigest    string  `json:"router_model_digest"`
	Probes               int     `json:"probes"`
	EfSearch             int     `json:"ef_search"`
	Concurrency          int     `json:"concurrency"`
	Samples              int     `json:"samples"`
	RecallAtK            float64 `json:"recall_at_k"`
	QPS                  float64 `json:"qps"`
	P50Nanos             uint64  `json:"p50_nanos"`
	P95Nanos             uint64  `json:"p95_nanos"`
	P99Nanos             uint64  `json:"p99_nanos"`
	RPCs                 uint64  `json:"rpcs"`
	RequestBytes         uint64  `json:"request_bytes"`
	CandidateBytes       uint64  `json:"candidate_bytes"`
	ResponseBytes        uint64  `json:"response_bytes"`
	PersistentAssetBytes uint64  `json:"persistent_asset_bytes"`
	PeakRSSBytes         int64   `json:"peak_rss_bytes"`
}

type m8ProductionMatrixGatesV1 struct {
	RequiredVariants string `json:"required_variants"`
	ExhaustiveParity string `json:"exhaustive_correctness"`
	FailureHonesty   string `json:"failure_honesty"`
	Recall           string `json:"recall"`
	ProbeReduction   string `json:"probe_reduction"`
	EndToEndQPS      string `json:"matched_recall_qps"`
	TailLatency      string `json:"matched_recall_tail"`
	Balance          string `json:"balance"`
	OverlapStorage   string `json:"overlap_storage"`
	ResourceBounds   string `json:"resource_bounds"`
	ExistingBehavior string `json:"existing_behavior"`
}

func runM8ProductionMultiGroupV1(cfg config, fixture fixtureManifest, vectors, queries [][]float64, stdout io.Writer) error {
	if len(cfg.m8VariantDBs) == 0 {
		return runM8ProductionSingleVariantV1(cfg, fixture, vectors, queries, stdout)
	}
	pathsByVariant := make(map[string]string, len(cfg.m8VariantDBs))
	for _, dir := range cfg.m8VariantDBs {
		descriptor, err := m3ReadVariantDescriptorV1(dir)
		if err != nil {
			return fmt.Errorf("M8 matrix variant %q: %w", dir, err)
		}
		if descriptor.FixtureChecksum != fixture.Checksum || descriptor.Partitions != uint32(cfg.partitions) {
			return fmt.Errorf("M8 matrix variant %q does not match configured fixture/partitions", descriptor.VariantID)
		}
		if _, duplicate := pathsByVariant[descriptor.VariantID]; duplicate {
			return fmt.Errorf("M8 matrix duplicate variant %q", descriptor.VariantID)
		}
		pathsByVariant[descriptor.VariantID] = dir
	}
	for _, required := range m8RequiredVariantIDsV1 {
		if pathsByVariant[required] == "" {
			return fmt.Errorf("M8 matrix missing required variant %q", required)
		}
	}

	reports := make([]m8ProductionReportV1, 0, len(m8RequiredVariantIDsV1))
	for _, variantID := range m8RequiredVariantIDsV1 {
		descriptor, err := m3ReadVariantDescriptorV1(pathsByVariant[variantID])
		if err != nil {
			return err
		}
		variantCfg := cfg
		variantCfg.m8VariantDBs = nil
		variantCfg.m8ExistingDB = pathsByVariant[variantID]
		variantCfg.overlaps = []float64{descriptor.OverlapRatio}
		variantCfg.format = "json"
		if cfg.profiles != "" {
			variantCfg.profiles = filepath.Join(cfg.profiles, variantID)
		}
		var encoded bytes.Buffer
		if err := runM8ProductionSingleVariantV1(variantCfg, fixture, vectors, queries, &encoded); err != nil {
			return fmt.Errorf("M8 matrix variant %s: %w", variantID, err)
		}
		var report m8ProductionReportV1
		if err := json.Unmarshal(encoded.Bytes(), &report); err != nil {
			return fmt.Errorf("decode M8 matrix variant %s: %w", variantID, err)
		}
		if report.Variant == nil || report.Variant.VariantID != variantID {
			return fmt.Errorf("M8 matrix variant %s lost immutable descriptor identity", variantID)
		}
		reports = append(reports, report)
	}
	matrix, err := m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(matrix, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		return err
	}
	orderedDescriptors := make([]m3VariantDescriptorV1, len(reports))
	for i := range reports {
		orderedDescriptors[i] = *reports[i].Variant
	}
	identity, err := json.Marshal(struct {
		HeadSHA  string
		Variants []m3VariantDescriptorV1
		Config   m8ProductionConfigEvidenceV1
	}{HeadSHA: cfg.headSHA, Variants: orderedDescriptors, Config: reports[0].Config})
	if err != nil {
		return err
	}
	digest := sha256.Sum256(identity)
	path := filepath.Join(cfg.out, fmt.Sprintf("vector_partition_m8_matrix_%s_%x.json", cfg.headSHA[:provenanceSuffixBytes], digest[:6]))
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return err
	}
	if cfg.format == "json" {
		_, err = stdout.Write(raw)
	} else {
		_, err = fmt.Fprintf(stdout, "M8 matrix status=%s disposition=%s artifact=%s rows=%d\n", matrix.Status, matrix.Disposition, path, len(matrix.Comparison))
	}
	return err
}

func m8BuildProductionMatrixV1(cfg config, fixture fixtureManifest, reports []m8ProductionReportV1) (m8ProductionMatrixV1, error) {
	matrix := m8ProductionMatrixV1{
		SchemaVersion: 3, ResultKind: "m8_production_multi_variant_matrix_v3", Status: "incomplete", GeneratedAt: time.Now().UTC(),
		Command: append([]string(nil), cfg.command...), BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, Dataset: fixture,
		RequiredVariants: append([]string(nil), m8RequiredVariantIDsV1...), Variants: reports,
		Limitations: []string{"single-host loopback production-shaped topology; multi-host qualification remains owned by #3983", "no external-system or paper-scale comparison is claimed"},
	}
	if len(reports) != len(m8RequiredVariantIDsV1) {
		return m8ProductionMatrixV1{}, errors.New("M8 matrix requires exactly three reports")
	}
	byID := make(map[string]*m8ProductionReportV1, len(reports))
	commonConfig := reports[0].Config
	commonConfig.Overlap = nil
	for i := range matrix.Variants {
		report := &matrix.Variants[i]
		comparisonConfig := report.Config
		comparisonConfig.Overlap = nil
		if report.Variant == nil || report.Dataset.Checksum != fixture.Checksum || report.Config.Partitions != cfg.partitions ||
			report.BaseSHA != cfg.baseSHA || report.HeadSHA != cfg.headSHA || !reflect.DeepEqual(comparisonConfig, commonConfig) ||
			len(report.Config.Overlap) != 1 || report.Config.Overlap[0] != report.Variant.OverlapRatio {
			return m8ProductionMatrixV1{}, errors.New("M8 matrix report identity mismatch")
		}
		if byID[report.Variant.VariantID] != nil {
			return m8ProductionMatrixV1{}, fmt.Errorf("M8 matrix duplicate report %q", report.Variant.VariantID)
		}
		byID[report.Variant.VariantID] = report
		for _, row := range report.Rows {
			if row.Status == "unsupported" {
				return m8ProductionMatrixV1{}, errors.New("M8 matrix contains an unsupported comparison row")
			}
			matrix.Comparison = append(matrix.Comparison, m8ProductionComparisonV1{
				VariantID: report.Variant.VariantID, AssignmentBasis: report.Variant.AssignmentBasis, Overlap: report.Variant.OverlapRatio,
				ArtifactSHA256: report.Variant.ArtifactSHA256, ReadySetDigest: report.Variant.ReadySetDigest, RouterModelDigest: report.Variant.RouterModelDigest,
				Probes: row.Probes, EfSearch: row.EfSearch, Concurrency: row.Concurrency, Samples: row.Samples, RecallAtK: row.RecallAtK,
				QPS: row.QPS, P50Nanos: row.P50Nanos, P95Nanos: row.P95Nanos, P99Nanos: row.P99Nanos, RPCs: row.RPCs,
				RequestBytes: row.RequestBytes, CandidateBytes: row.CandidateBytes, ResponseBytes: row.ResponseBytes,
				PersistentAssetBytes: report.Resources.PersistentAssetBytes, PeakRSSBytes: report.Resources.PeakRSSBytes,
			})
		}
	}
	for _, required := range m8RequiredVariantIDsV1 {
		if byID[required] == nil {
			return m8ProductionMatrixV1{}, fmt.Errorf("M8 matrix missing report %q", required)
		}
	}
	disjointBytes := byID["graph-disjoint-v1"].Resources.PersistentAssetBytes
	overlapBytes := byID["graph-overlap-020-v1"].Resources.PersistentAssetBytes
	if disjointBytes == 0 {
		return m8ProductionMatrixV1{}, errors.New("M8 matrix disjoint persistent bytes are zero")
	}
	matrix.OverlapStorageRatio = float64(overlapBytes) / float64(disjointBytes)
	overlapGate := "fail"
	if matrix.OverlapStorageRatio < 1.35 {
		overlapGate = "pass"
	}
	for i := range matrix.Variants {
		matrix.Variants[i].GateLedger.OverlapStorage = overlapGate
	}
	matrix.Gates = m8ProductionMatrixGatesV1{
		RequiredVariants: "pass", ExhaustiveParity: m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.ExhaustiveParity }),
		FailureHonesty:   m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.FailureHonesty }),
		Recall:           m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.Recall }),
		ProbeReduction:   m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.ProbeReduction }),
		EndToEndQPS:      m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.EndToEndQPS }),
		TailLatency:      m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.TailLatency }),
		Balance:          m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.Balance }),
		OverlapStorage:   overlapGate,
		ResourceBounds:   m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.ResourceBounds }),
		ExistingBehavior: "pending_latest_head_required_suites",
	}
	matrix.Status = "local_gate_pass"
	matrix.Disposition = "local_gate_pass_multi_host_still_deferred"
	for _, gate := range []string{matrix.Gates.RequiredVariants, matrix.Gates.ExhaustiveParity, matrix.Gates.FailureHonesty, matrix.Gates.Recall, matrix.Gates.ProbeReduction, matrix.Gates.EndToEndQPS, matrix.Gates.TailLatency, matrix.Gates.Balance, matrix.Gates.OverlapStorage, matrix.Gates.ResourceBounds} {
		if gate != "pass" {
			matrix.Status = "experimental_gate_failures"
			matrix.Disposition = "enablement_off_follow_up_required"
			break
		}
	}
	sort.Slice(matrix.Comparison, func(i, j int) bool {
		a, b := matrix.Comparison[i], matrix.Comparison[j]
		if a.VariantID != b.VariantID {
			return a.VariantID < b.VariantID
		}
		if a.Probes != b.Probes {
			return a.Probes < b.Probes
		}
		if a.EfSearch != b.EfSearch {
			return a.EfSearch < b.EfSearch
		}
		return a.Concurrency < b.Concurrency
	})
	return matrix, nil
}

func m8AggregateVariantGateV1(reports []m8ProductionReportV1, value func(m8ProductionGateLedgerV1) string) string {
	for _, report := range reports {
		if value(report.GateLedger) != "pass" {
			return "fail"
		}
	}
	return "pass"
}
