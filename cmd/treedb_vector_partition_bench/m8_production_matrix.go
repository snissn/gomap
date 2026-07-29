package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

var m8RequiredVariantIDsV1 = []string{"graph-disjoint-v1", "graph-overlap-020-v1", "stable-id-hash-disjoint-v1"}

type m8ProductionMatrixV1 struct {
	SchemaVersion               int                        `json:"schema_version"`
	ResultKind                  string                     `json:"result_kind"`
	Status                      string                     `json:"status"`
	Disposition                 string                     `json:"disposition"`
	GeneratedAt                 time.Time                  `json:"generated_at"`
	Command                     []string                   `json:"exact_command"`
	BaseSHA                     string                     `json:"base_sha"`
	HeadSHA                     string                     `json:"head_sha"`
	Dataset                     fixtureManifest            `json:"dataset"`
	RequiredVariants            []string                   `json:"required_variants"`
	Variants                    []m8ProductionReportV1     `json:"variants"`
	Comparison                  []m8ProductionComparisonV1 `json:"comparison"`
	Gates                       m8ProductionMatrixGatesV1  `json:"gates"`
	OverlapMaterializationRatio float64                    `json:"overlap_materialization_ratio"`
	OverlapStorageRatio         float64                    `json:"overlap_storage_ratio"`
	Limitations                 []string                   `json:"limitations"`
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
	MaxTotalNanos        uint64  `json:"max_total_nanos"`
	RPCs                 uint64  `json:"rpcs"`
	RequestBytes         uint64  `json:"request_bytes"`
	CandidateBytes       uint64  `json:"candidate_bytes"`
	ResponseBytes        uint64  `json:"response_bytes"`
	PersistentAssetBytes uint64  `json:"persistent_asset_bytes"`
	PeakRSSBytes         int64   `json:"peak_rss_bytes"`
}

type m8ProductionMatrixGatesV1 struct {
	RequiredVariants          string `json:"required_variants"`
	ExhaustiveParity          string `json:"exhaustive_correctness"`
	FailureHonesty            string `json:"failure_honesty"`
	PartitionPackReachability string `json:"partition_pack_reachability"`
	Recall                    string `json:"recall"`
	ProbeReduction            string `json:"probe_reduction"`
	EndToEndQPS               string `json:"matched_recall_qps"`
	TailLatency               string `json:"matched_recall_tail"`
	CoupledGraph              string `json:"coupled_graph_acceptance"`
	Balance                   string `json:"balance"`
	OverlapStorage            string `json:"overlap_storage"`
	ResourceBounds            string `json:"resource_bounds"`
	ExistingBehavior          string `json:"existing_behavior"`
}

func runM8ProductionMultiGroupV1(cfg config, fixture fixtureManifest, vectors, queries [][]float64, stdout io.Writer) error {
	if len(cfg.m8VariantDBs) == 0 {
		return runM8ProductionSingleVariantV1(cfg, fixture, vectors, queries, stdout)
	}
	initialDirty := m8GitDirtyV1(cfg.out, cfg.profiles)
	type variantSource struct {
		dir        string
		descriptor m3VariantDescriptorV1
	}
	sourcesByVariant := make(map[string]variantSource, len(cfg.m8VariantDBs))
	for _, dir := range cfg.m8VariantDBs {
		descriptor, err := m3ReadVariantDescriptorV1(dir)
		if err != nil {
			return fmt.Errorf("M8 matrix variant %q: %w", dir, err)
		}
		if cfg.partitions < 0 || descriptor.FixtureChecksum != fixture.Checksum || uint64(descriptor.Partitions) != uint64(cfg.partitions) {
			return fmt.Errorf("M8 matrix variant %q does not match configured fixture/partitions", descriptor.VariantID)
		}
		if _, duplicate := sourcesByVariant[descriptor.VariantID]; duplicate {
			return fmt.Errorf("M8 matrix duplicate variant %q", descriptor.VariantID)
		}
		sourcesByVariant[descriptor.VariantID] = variantSource{dir: dir, descriptor: descriptor}
	}
	for _, required := range m8RequiredVariantIDsV1 {
		if sourcesByVariant[required].dir == "" {
			return fmt.Errorf("M8 matrix missing required variant %q", required)
		}
	}
	preflightDescriptors := make([]m3VariantDescriptorV1, 0, len(m8RequiredVariantIDsV1))
	for _, required := range m8RequiredVariantIDsV1 {
		preflightDescriptors = append(preflightDescriptors, sourcesByVariant[required].descriptor)
	}
	if err := m8ValidateVariantBuildCompatibilityV1(preflightDescriptors); err != nil {
		return err
	}

	reports := make([]m8ProductionReportV1, 0, len(m8RequiredVariantIDsV1))
	for _, variantID := range m8RequiredVariantIDsV1 {
		source := sourcesByVariant[variantID]
		var encoded bytes.Buffer
		variantProfiles := ""
		if cfg.profiles != "" {
			variantProfiles = filepath.Join(cfg.profiles, variantID)
		}
		if err := runM8ProductionVariantProcessV1(cfg, source.dir, source.descriptor.OverlapRatio, variantProfiles, &encoded); err != nil {
			return fmt.Errorf("M8 matrix variant %s: %w", variantID, err)
		}
		var report m8ProductionReportV1
		if err := json.Unmarshal(encoded.Bytes(), &report); err != nil {
			return fmt.Errorf("decode M8 matrix variant %s: %w", variantID, err)
		}
		report.Dirty = initialDirty || report.Dirty
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
	digest, err := m8MatrixIdentityV1(cfg, orderedDescriptors, reports[0].Config)
	if err != nil {
		return err
	}
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

func runM8ProductionVariantProcessV1(cfg config, dir string, overlap float64, profiles string, stdout io.Writer) error {
	args, err := m8VariantProcessArgsV1(cfg.command, dir, overlap, profiles, cfg.out, cfg.profiles)
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve M8 benchmark executable: %w", err)
	}
	cmd := exec.Command(executable, args...)
	var stderr bytes.Buffer
	cmd.Stdout = stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("fresh M8 variant process: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func m8VariantProcessArgsV1(command []string, dir string, overlap float64, profiles, matrixOut, matrixProfiles string) ([]string, error) {
	if len(command) == 0 || dir == "" || math.IsNaN(overlap) || math.IsInf(overlap, 0) || overlap < 0 || overlap > 1 {
		return nil, errors.New("M8 variant process requires a command, database, and finite overlap in [0,1]")
	}
	drop := map[string]bool{"m8-variant-dbs": true, "m8-existing-db": true, "overlap": true, "format": true, "profiles": true, "m8-matrix-out": true, "m8-matrix-profiles": true}
	// Forced child identity flags must precede every inherited argument. This is
	// safe even for a defensively supplied positional argument because Go flag
	// parsing stops at the first positional token.
	args := []string{"-m8-existing-db", dir, "-overlap", strconv.FormatFloat(overlap, 'g', -1, 64), "-format", "json"}
	if profiles != "" {
		args = append(args, "-profiles", profiles)
	}
	if matrixOut != "" {
		args = append(args, "-m8-matrix-out", matrixOut)
	}
	if matrixProfiles != "" {
		args = append(args, "-m8-matrix-profiles", matrixProfiles)
	}
	args = slices.Grow(args, len(command)-1)
	for i := 1; i < len(command); i++ {
		arg := command[i]
		if !strings.HasPrefix(arg, "-") {
			args = append(args, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if at := strings.IndexByte(name, '='); at >= 0 {
			name = name[:at]
			if drop[name] {
				continue
			}
		} else if drop[name] {
			if i+1 >= len(command) {
				return nil, fmt.Errorf("M8 matrix command flag %q is missing its value", arg)
			}
			i++
			continue
		}
		args = append(args, arg)
	}
	return args, nil
}

func m8MatrixIdentityV1(cfg config, variants []m3VariantDescriptorV1, evidence m8ProductionConfigEvidenceV1) ([sha256.Size]byte, error) {
	portableVariants := append([]m3VariantDescriptorV1(nil), variants...)
	for i := range portableVariants {
		portableVariants[i].DatabaseDirectory = ""
	}
	identity, err := json.Marshal(struct {
		BaseSHA                 string
		HeadSHA                 string
		Variants                []m3VariantDescriptorV1
		Config                  m8ProductionConfigEvidenceV1
		MaxRSSBytes             uint64
		MaxPersistentAssetBytes uint64
	}{BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, Variants: portableVariants, Config: evidence, MaxRSSBytes: cfg.m8MaxRSSBytes, MaxPersistentAssetBytes: cfg.m8MaxAssetBytes})
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(identity), nil
}

func m8ValidateVariantBuildCompatibilityV1(variants []m3VariantDescriptorV1) error {
	if len(variants) != len(m8RequiredVariantIDsV1) {
		return errors.New("M8 matrix build identity requires exactly three variants")
	}
	byID := make(map[string]m3VariantDescriptorV1, len(variants))
	for _, variant := range variants {
		if err := validateM3VariantDescriptorV1(variant); err != nil {
			return fmt.Errorf("M8 matrix malformed variant build identity: %w", err)
		}
		if _, duplicate := byID[variant.VariantID]; duplicate {
			return fmt.Errorf("M8 matrix duplicate variant build identity %q", variant.VariantID)
		}
		byID[variant.VariantID] = variant
	}
	base, ok := byID[m8RequiredVariantIDsV1[0]]
	if !ok {
		return fmt.Errorf("M8 matrix missing variant build identity %q", m8RequiredVariantIDsV1[0])
	}
	for _, required := range m8RequiredVariantIDsV1[1:] {
		variant, ok := byID[required]
		if !ok {
			return fmt.Errorf("M8 matrix missing variant build identity %q", required)
		}
		if variant.FixtureChecksum != base.FixtureChecksum || variant.Source != base.Source || variant.Partitions != base.Partitions ||
			variant.GraphArtifactSHA256 != base.GraphArtifactSHA256 || variant.IndexDefinitionDigest != base.IndexDefinitionDigest || variant.PartitionHNSWM != base.PartitionHNSWM {
			return fmt.Errorf("M8 matrix variant %q was not built from the common source, graph, partition count, and local HNSW configuration", required)
		}
	}
	graphOverlap := byID["graph-overlap-020-v1"]
	if graphOverlap.ArtifactSHA256 != base.ArtifactSHA256 {
		return errors.New("M8 matrix graph variants do not share the same assignment artifact")
	}
	if graphOverlap.RouterModelDigest != base.RouterModelDigest {
		return errors.New("M8 matrix graph variants do not share the same router model")
	}
	return nil
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
	descriptors := make([]m3VariantDescriptorV1, 0, len(reports))
	for i := range reports {
		if reports[i].Dirty {
			return m8ProductionMatrixV1{}, errors.New("M8 matrix rejects dirty child reports")
		}
		if reports[i].Variant == nil {
			return m8ProductionMatrixV1{}, errors.New("M8 matrix report is missing variant identity")
		}
		descriptors = append(descriptors, *reports[i].Variant)
	}
	if err := m8ValidateVariantBuildCompatibilityV1(descriptors); err != nil {
		return m8ProductionMatrixV1{}, err
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
				QPS: row.QPS, P50Nanos: row.P50Nanos, P95Nanos: row.P95Nanos, P99Nanos: row.P99Nanos, MaxTotalNanos: row.MaxTotalNanos, RPCs: row.RPCs,
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
	overlapDescriptor := byID["graph-overlap-020-v1"].Variant
	wantOverlapMemberships := uint64(math.Floor(overlapDescriptor.OverlapRatio * float64(overlapDescriptor.SourceRows)))
	gotOverlapMemberships := uint64(overlapDescriptor.OverlapMemberships)
	if overlapDescriptor.SourceRows > 0 {
		matrix.OverlapMaterializationRatio = float64(gotOverlapMemberships) / float64(overlapDescriptor.SourceRows)
	}
	overlapMaterialized := wantOverlapMemberships > 0 && gotOverlapMemberships == wantOverlapMemberships
	overlapGate := "fail"
	if overlapMaterialized && matrix.OverlapStorageRatio < 1.35 {
		overlapGate = "pass"
	}
	for i := range matrix.Variants {
		matrix.Variants[i].GateLedger.OverlapStorage = overlapGate
	}
	requiredVariantsGate := "fail"
	if overlapMaterialized {
		requiredVariantsGate = "pass"
	}
	matrix.Gates = m8ProductionMatrixGatesV1{
		RequiredVariants: requiredVariantsGate, ExhaustiveParity: m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.ExhaustiveParity }),
		FailureHonesty:            m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.FailureHonesty }),
		PartitionPackReachability: m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.PartitionPackReachability }),
		Recall:                    m8AnyGraphVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.Recall }),
		ProbeReduction:            m8AnyGraphVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.ProbeReduction }),
		EndToEndQPS:               m8AnyGraphVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.EndToEndQPS }),
		TailLatency:               m8AnyGraphVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.TailLatency }),
		CoupledGraph:              m8AnyGraphVariantCoupledGatesPassV1(matrix.Variants),
		Balance:                   m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.Balance }),
		OverlapStorage:            overlapGate,
		ResourceBounds:            m8AggregateVariantGateV1(matrix.Variants, func(l m8ProductionGateLedgerV1) string { return l.ResourceBounds }),
		ExistingBehavior:          "pending_latest_head_required_suites",
	}
	matrix.Status = "local_gate_pass"
	matrix.Disposition = "local_gate_pass_multi_host_still_deferred"
	for _, gate := range []string{matrix.Gates.RequiredVariants, matrix.Gates.ExhaustiveParity, matrix.Gates.FailureHonesty, matrix.Gates.PartitionPackReachability, matrix.Gates.Recall, matrix.Gates.ProbeReduction, matrix.Gates.EndToEndQPS, matrix.Gates.TailLatency, matrix.Gates.CoupledGraph, matrix.Gates.Balance, matrix.Gates.OverlapStorage, matrix.Gates.ResourceBounds} {
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

func m8AnyGraphVariantGateV1(reports []m8ProductionReportV1, value func(m8ProductionGateLedgerV1) string) string {
	for _, report := range reports {
		if report.Variant != nil && report.Variant.AssignmentBasis == partitionAssignmentGraphV1 && value(report.GateLedger) == "pass" {
			return "pass"
		}
	}
	return "fail"
}

func m8AnyGraphVariantCoupledGatesPassV1(reports []m8ProductionReportV1) string {
	for _, report := range reports {
		if report.Variant == nil || report.Variant.AssignmentBasis != partitionAssignmentGraphV1 {
			continue
		}
		if m8ReportHasCoupledGateOperatingPointV1(report) {
			return "pass"
		}
	}
	return "fail"
}

func m8ReportHasCoupledGateOperatingPointV1(report m8ProductionReportV1) bool {
	for _, candidate := range report.Rows {
		if candidate.Status != "pass" || candidate.RecallAtK < report.Config.RecallTarget || candidate.Probes*4 > report.Config.Partitions {
			continue
		}
		for _, base := range report.Rows {
			if !base.ExactParityChecked || base.RecallAtK < report.Config.RecallTarget || candidate.EfSearch != base.EfSearch || candidate.Concurrency != base.Concurrency {
				continue
			}
			if candidate.QPS >= base.QPS*1.15 && candidate.P95Nanos <= base.P95Nanos {
				return true
			}
		}
	}
	return false
}
