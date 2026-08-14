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
	"runtime"
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
	ExecutionStartedAt          time.Time                  `json:"execution_started_at"`
	ExecutionCompletedAt        time.Time                  `json:"execution_completed_at"`
	Command                     []string                   `json:"exact_command"`
	ExecutableSHA256            string                     `json:"executable_sha256"`
	BaseSHA                     string                     `json:"base_sha"`
	HeadSHA                     string                     `json:"head_sha"`
	Dataset                     fixtureManifest            `json:"dataset"`
	RequiredVariants            []string                   `json:"required_variants"`
	Variants                    []m8ProductionReportV1     `json:"variants"`
	Comparison                  []m8ProductionComparisonV1 `json:"comparison"`
	Gates                       m8ProductionMatrixGatesV1  `json:"gates"`
	OverlapMaterializationRatio float64                    `json:"overlap_materialization_ratio"`
	OverlapStorageRatio         float64                    `json:"overlap_storage_ratio"`
	OverlapDiagnostics          m8OverlapDiagnosticsV1     `json:"overlap_diagnostics"`
	Decision                    []m8DecisionRowV1          `json:"decision_report"`
	Limitations                 []string                   `json:"limitations"`
}

type m8OverlapDiagnosticsV1 struct {
	Requested                    int     `json:"requested_replicas"`
	Useful                       int     `json:"positive_gain_replicas"`
	Filler                       int     `json:"forced_or_filler_replicas"`
	Rejected                     int     `json:"rejected_replicas"`
	UnusedCapacity               int     `json:"unused_capacity"`
	EdgeCutBefore                int     `json:"directed_edge_cut_before"`
	EdgeCutAfter                 int     `json:"directed_edge_cut_after"`
	CutReductionPerUsefulReplica float64 `json:"directed_cut_reduction_per_useful_replica"`
	// Accounted records that the realized overlap is an internally consistent
	// useful-only outcome: at most the requested replicas, zero filler, and
	// every realized replica useful. It is deliberately not "hit the exact
	// ratio", which useful-only never promises.
	Accounted bool `json:"useful_only_accounted"`
}

type m8DecisionRowV1 struct {
	VariantID   string  `json:"variant_id"`
	Probes      int     `json:"probes"`
	EfSearch    int     `json:"ef_search"`
	Concurrency int     `json:"concurrency"`
	Stage       string  `json:"stage"`
	Owner       string  `json:"owner"`
	FromRecall  float64 `json:"from_recall_at_k"`
	ToRecall    float64 `json:"to_recall_at_k"`
	Delta       float64 `json:"delta_at_k"`
}

type m8ProductionComparisonV1 struct {
	VariantID            string  `json:"variant_id"`
	Status               string  `json:"status"`
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

func runM8ProductionMultiGroupV1(cfg config, fixture fixtureManifest, vectors, queries [][]float64, stdout io.Writer) (runErr error) {
	if len(cfg.m8VariantDBs) == 0 {
		return runM8ProductionSingleVariantV1(cfg, fixture, vectors, queries, stdout)
	}
	executionStartedAt := time.Now().UTC()
	initialDirty := m8GitDirtyInV1(cfg.sourceCheckout, cfg.out, cfg.profiles)
	executableSHA256, err := m8BenchmarkExecutableSHA256V1(cfg.command[0])
	if err != nil {
		return fmt.Errorf("hash M8 benchmark executable: %w", err)
	}
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
		if err := m8ValidateRetainedM3ProvenanceV1(cfg, descriptor, executableSHA256); err != nil {
			return fmt.Errorf("M8 matrix variant %q: %w", descriptor.VariantID, err)
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
	matrixConfig := m8QualificationCommandConfigV1(cfg)
	matrixConfig.Overlap = []float64{sourcesByVariant[m8RequiredVariantIDsV1[0]].descriptor.OverlapRatio}
	matrixPath, err := m8PreflightProductionMatrixOutputV1(cfg, preflightDescriptors, matrixConfig)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		return err
	}
	ownedProfiles, err := m8CreateProductionMatrixProfileLeavesV1(cfg.profiles)
	if err != nil {
		return err
	}
	matrixPublished := false
	defer func() {
		if matrixPublished {
			return
		}
		runErr = errors.Join(runErr, m8RemoveProductionMatrixProfileLeavesV1(ownedProfiles))
	}()

	reports := make([]m8ProductionReportV1, 0, len(m8RequiredVariantIDsV1))
	expectedTruthCacheDigest := cfg.m8TruthCacheSHA256
	for _, variantID := range m8RequiredVariantIDsV1 {
		source := sourcesByVariant[variantID]
		var encoded bytes.Buffer
		variantProfiles := ""
		if cfg.profiles != "" {
			variantProfiles = filepath.Join(cfg.profiles, variantID)
		}
		if err := runM8ProductionVariantProcessV1(cfg, source.dir, source.descriptor.OverlapRatio, variantProfiles, expectedTruthCacheDigest, &encoded); err != nil {
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
		if cfg.m8TruthCache != "" && expectedTruthCacheDigest == "" {
			if report.TruthCache.Status != "computed" || report.TruthCache.ArtifactSHA256 == "" {
				return errors.New("first M8 matrix child must compute authoritative truth cache")
			}
			expectedTruthCacheDigest = report.TruthCache.ArtifactSHA256
		}
		reports = append(reports, report)
	}
	matrix, err := m8BuildProductionMatrixWithExecutionIntervalV1(cfg, fixture, reports, executionStartedAt, time.Now().UTC())
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(matrix, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	matrixPublished, err = m8WriteProductionMatrixV1(matrixPath, raw)
	if err != nil {
		return err
	}
	if cfg.format == "json" {
		_, err = stdout.Write(raw)
	} else {
		if _, err = fmt.Fprintf(stdout, "M8 matrix status=%s disposition=%s artifact=%s rows=%d\n", matrix.Status, matrix.Disposition, matrixPath, len(matrix.Comparison)); err == nil {
			for _, decision := range matrix.Decision {
				_, err = fmt.Fprintf(stdout, "decision variant=%s probes=%d ef=%d concurrency=%d stage=%s owner=%s delta=%+.6f\n", decision.VariantID, decision.Probes, decision.EfSearch, decision.Concurrency, decision.Stage, decision.Owner, decision.Delta)
				if err != nil {
					break
				}
			}
		}
	}
	return err
}

// m8CreateProductionMatrixProfileLeavesV1 reserves only this matrix's final
// profile leaves. A failed reservation removes just leaves it created.
func m8CreateProductionMatrixProfileLeavesV1(root string) (leaves []string, err error) {
	if root == "" {
		return nil, nil
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create M8 profile root: %w", err)
	}
	for _, variantID := range m8RequiredVariantIDsV1 {
		path := filepath.Join(root, variantID)
		if err := os.Mkdir(path, 0o755); err != nil {
			for _, leaf := range leaves {
				err = errors.Join(err, os.RemoveAll(leaf))
			}
			return nil, fmt.Errorf("reserve M8 profile output %s: %w", path, err)
		}
		leaves = append(leaves, path)
	}
	return leaves, nil
}

func m8RemoveProductionMatrixProfileLeavesV1(leaves []string) (err error) {
	for _, path := range leaves {
		err = errors.Join(err, os.RemoveAll(path))
	}
	return err
}

// m8PreflightProductionMatrixOutputV1 prevents an exact retained command from
// modifying its evidence bundle before child profile capture begins.
func m8PreflightProductionMatrixOutputV1(cfg config, descriptors []m3VariantDescriptorV1, evidence m8ProductionConfigEvidenceV1) (string, error) {
	digest, err := m8MatrixIdentityV1(cfg, descriptors, evidence)
	if err != nil {
		return "", err
	}
	path := filepath.Join(cfg.out, fmt.Sprintf("vector_partition_m8_matrix_%s_%x.json", cfg.headSHA[:provenanceSuffixBytes], digest[:6]))
	if _, err := os.Lstat(path); err == nil {
		return "", fmt.Errorf("M8 retained matrix output already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect M8 matrix output %s: %w", path, err)
	}
	if cfg.profiles != "" {
		for _, variantID := range m8RequiredVariantIDsV1 {
			path := filepath.Join(cfg.profiles, variantID)
			if _, err := os.Lstat(path); err == nil {
				return "", fmt.Errorf("M8 retained profile output already exists: %s", path)
			} else if !errors.Is(err, os.ErrNotExist) {
				return "", fmt.Errorf("inspect M8 profile output %s: %w", path, err)
			}
		}
	}
	return path, nil
}

func m8WriteProductionMatrixV1(path string, raw []byte) (bool, error) {
	return m8PublishProductionMatrixV1(path, func(w io.Writer) error {
		n, err := w.Write(raw)
		if err != nil {
			return err
		}
		if n != len(raw) {
			return io.ErrShortWrite
		}
		return nil
	})
}

// m8PublishProductionMatrixV1 exposes a complete matrix only after it is
// closed and atomically linked into its final no-replace name.
func m8PublishProductionMatrixV1(path string, write func(io.Writer) error) (bool, error) {
	return m8PublishProductionMatrixWithDirectorySyncV1(path, write, m8SyncDirectoryV1)
}

func m8PublishProductionMatrixWithDirectorySyncV1(path string, write func(io.Writer) error, syncDirectory func(string) error) (bool, error) {
	file, err := os.CreateTemp(filepath.Dir(path), ".m8_matrix_*.tmp")
	if err != nil {
		return false, fmt.Errorf("create temporary immutable M8 matrix: %w", err)
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	defer file.Close()
	if err := write(file); err != nil {
		return false, err
	}
	if err := file.Chmod(0o644); err != nil {
		return false, err
	}
	if err := file.Sync(); err != nil {
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	if err := os.Link(tempPath, path); err != nil {
		return false, fmt.Errorf("publish immutable M8 matrix: %w", err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return true, fmt.Errorf("sync immutable M8 matrix directory: %w", err)
	}
	return true, nil
}

func m8SyncDirectoryV1(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func runM8ProductionVariantProcessV1(cfg config, dir string, overlap float64, profiles, expectedTruthCacheDigest string, stdout io.Writer) error {
	args, err := m8VariantProcessArgsV1(cfg.command, dir, overlap, profiles, cfg.out, cfg.profiles, expectedTruthCacheDigest)
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

func m8VariantProcessArgsV1(command []string, dir string, overlap float64, profiles, matrixOut, matrixProfiles, expectedTruthCacheDigest string) ([]string, error) {
	if len(command) == 0 || dir == "" || math.IsNaN(overlap) || math.IsInf(overlap, 0) || overlap < 0 || overlap > 1 {
		return nil, errors.New("M8 variant process requires a command, database, and finite overlap in [0,1]")
	}
	drop := map[string]bool{"m8-variant-dbs": true, "m8-existing-db": true, "overlap": true, "format": true, "profiles": true, "m8-matrix-out": true, "m8-matrix-profiles": true, "m8-truth-cache-sha256": true}
	// Forced child identity flags must precede every inherited argument. This is
	// safe even for a defensively supplied positional argument because Go flag
	// parsing stops at the first positional token.
	args := []string{"-m8-existing-db", dir, "-overlap", strconv.FormatFloat(overlap, 'g', -1, 64), "-format", "json"}
	if expectedTruthCacheDigest != "" {
		args = append(args, "-m8-truth-cache-sha256", expectedTruthCacheDigest)
	}
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

func m8ReplayCommandWithTruthCacheDigestV1(command []string, digest string) ([]string, error) {
	if len(command) == 0 || len(digest) != sha256.Size*2 || digest != strings.ToLower(digest) || !m8SHA256V1(digest) {
		return nil, errors.New("M8 replay command requires a command and truth-cache digest")
	}
	args := []string{command[0], "-m8-truth-cache-sha256", digest}
	args = slices.Grow(args, len(command)-1)
	for i := 1; i < len(command); i++ {
		arg := command[i]
		if !strings.HasPrefix(arg, "-") {
			args = append(args, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if at := strings.IndexByte(name, '='); at >= 0 {
			if name[:at] == "m8-truth-cache-sha256" {
				continue
			}
		} else if name == "m8-truth-cache-sha256" {
			if i+1 >= len(command) {
				return nil, fmt.Errorf("M8 replay command flag %q is missing its value", arg)
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
		if variant.BaseSHA != base.BaseSHA || variant.HeadSHA != base.HeadSHA || variant.FixtureChecksum != base.FixtureChecksum || variant.Source != base.Source || variant.Partitions != base.Partitions ||
			variant.IndexDefinitionDigest != base.IndexDefinitionDigest || variant.PartitionHNSWM != base.PartitionHNSWM || variant.PartitionConfig != base.PartitionConfig ||
			variant.RouterRepresentatives != base.RouterRepresentatives || variant.RouterMaxScalarWork != base.RouterMaxScalarWork || variant.RouterConfig != base.RouterConfig || variant.GraphBuildSHA256 != base.GraphBuildSHA256 {
			return fmt.Errorf("M8 matrix variant %q does not match the common source, partition count, partition configuration, index definition, local HNSW, router representative count, and router scalar-work configuration", required)
		}
		// Equal partition counts are not equal geometry. Two byte-bounded plans
		// can round to the same partition count from different envelopes and
		// still differ in home/overlap capacity, which changes what
		// capacity-constrained overlap construction can realize and confounds a
		// supposedly like-for-like comparison. Comparison variants must share
		// one plan, which -shard-plan-overlap-ratio exists to make possible.
		if variant.ShardPlan != base.ShardPlan {
			return fmt.Errorf("M8 matrix variant %q shard plan %+v does not match the common plan %+v", required, variant.ShardPlan, base.ShardPlan)
		}
	}
	graphOverlap := byID["graph-overlap-020-v1"]
	if graphOverlap.ArtifactSHA256 != base.ArtifactSHA256 || graphOverlap.GraphArtifactSHA256 != base.GraphArtifactSHA256 || graphOverlap.KaHIPPythonSHA256 != base.KaHIPPythonSHA256 || graphOverlap.KaHIPAdapterSHA256 != base.KaHIPAdapterSHA256 {
		return errors.New("M8 matrix graph variants do not share the same assignment artifact")
	}
	return nil
}

func m8BuildProductionMatrixV1(cfg config, fixture fixtureManifest, reports []m8ProductionReportV1) (m8ProductionMatrixV1, error) {
	now := time.Now().UTC()
	return m8BuildProductionMatrixWithExecutionIntervalV1(cfg, fixture, reports, now, now.Add(time.Nanosecond))
}

func m8BuildProductionMatrixWithExecutionIntervalV1(cfg config, fixture fixtureManifest, reports []m8ProductionReportV1, executionStartedAt, executionCompletedAt time.Time) (m8ProductionMatrixV1, error) {
	matrix := m8ProductionMatrixV1{
		SchemaVersion: 5, ResultKind: "m8_production_multi_variant_matrix_v5", Status: "incomplete", GeneratedAt: time.Now().UTC(), ExecutionStartedAt: executionStartedAt, ExecutionCompletedAt: executionCompletedAt,
		Command: append([]string(nil), cfg.command...), BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, Dataset: fixture,
		RequiredVariants: append([]string(nil), m8RequiredVariantIDsV1...), Variants: reports,
		Limitations: []string{"single-host loopback production-shaped topology; multi-host qualification remains owned by #3983", "no external-system or paper-scale comparison is claimed"},
	}
	if len(reports) != len(m8RequiredVariantIDsV1) {
		return m8ProductionMatrixV1{}, errors.New("M8 matrix requires exactly three reports")
	}
	matrix.ExecutableSHA256 = reports[0].ExecutableSHA256
	if !m8QualificationSHA256V1(matrix.ExecutableSHA256) {
		return m8ProductionMatrixV1{}, errors.New("M8 matrix report has invalid benchmark executable digest")
	}
	if reports[0].TruthCache.ArtifactSHA256 != "" {
		var err error
		matrix.Command, err = m8ReplayCommandWithTruthCacheDigestV1(matrix.Command, reports[0].TruthCache.ArtifactSHA256)
		if err != nil {
			return m8ProductionMatrixV1{}, err
		}
	}
	descriptors := make([]m3VariantDescriptorV1, 0, len(reports))
	for i := range reports {
		if reports[i].ExecutableSHA256 != matrix.ExecutableSHA256 {
			return m8ProductionMatrixV1{}, errors.New("M8 matrix reports use different benchmark executables")
		}
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
			matrix.Comparison = append(matrix.Comparison, m8ProductionComparisonForRowV1(*report, row))
		}
	}
	for _, required := range m8RequiredVariantIDsV1 {
		if byID[required] == nil {
			return m8ProductionMatrixV1{}, fmt.Errorf("M8 matrix missing report %q", required)
		}
	}
	// The generation record is required to reopen a retained byte-bounded
	// database, so it is durable storage the comparison must account for. It
	// also scales with realized memberships, which is exactly the quantity this
	// ratio is measuring.
	disjointBytes := byID["graph-disjoint-v1"].Resources.PersistentAssetBytes + byID["graph-disjoint-v1"].Resources.ShardGenerationBytes
	overlapBytes := byID["graph-overlap-020-v1"].Resources.PersistentAssetBytes + byID["graph-overlap-020-v1"].Resources.ShardGenerationBytes
	if disjointBytes == 0 {
		return m8ProductionMatrixV1{}, errors.New("M8 matrix disjoint persistent bytes are zero")
	}
	matrix.OverlapStorageRatio = float64(overlapBytes) / float64(disjointBytes)
	overlapDescriptor := byID["graph-overlap-020-v1"].Variant
	matrix.OverlapDiagnostics = m8OverlapDiagnosticsV1{
		Requested: overlapDescriptor.OverlapRequested, Useful: overlapDescriptor.OverlapUseful, Filler: overlapDescriptor.OverlapFiller,
		Rejected: overlapDescriptor.OverlapRejected, UnusedCapacity: overlapDescriptor.OverlapUnusedCapacity,
		EdgeCutBefore: overlapDescriptor.EdgeCutBefore, EdgeCutAfter: overlapDescriptor.EdgeCutAfter,
	}
	if overlapDescriptor.OverlapUseful > 0 {
		matrix.OverlapDiagnostics.CutReductionPerUsefulReplica = float64(overlapDescriptor.EdgeCutBefore-overlapDescriptor.EdgeCutAfter) / float64(overlapDescriptor.OverlapUseful)
	}
	wantOverlapMemberships := uint64(math.Floor(overlapDescriptor.OverlapRatio * float64(overlapDescriptor.SourceRows)))
	gotOverlapMemberships := uint64(overlapDescriptor.OverlapMemberships)
	if overlapDescriptor.SourceRows > 0 {
		matrix.OverlapMaterializationRatio = float64(gotOverlapMemberships) / float64(overlapDescriptor.SourceRows)
	}
	// Useful-only overlap realizes at most the ratio-derived request and stops
	// once cut-reducing proposals are exhausted, so an internally consistent
	// shortfall is a legal variant rather than a materialization failure. On a
	// corpus whose partition already has zero edge cut the correct realization
	// is zero replicas. Requiring exact fill here would make every such variant
	// unable to pass the matrix. What must hold is that the variant requested a
	// real overlap, realized no more than it requested, realized no filler, and
	// realized exactly as many useful replicas as memberships.
	overlapAccounted := wantOverlapMemberships > 0 &&
		gotOverlapMemberships <= wantOverlapMemberships &&
		overlapDescriptor.OverlapFiller == 0 &&
		overlapDescriptor.OverlapUseful == overlapDescriptor.OverlapRealized &&
		gotOverlapMemberships == uint64(overlapDescriptor.OverlapRealized)
	matrix.OverlapDiagnostics.Accounted = overlapAccounted
	overlapGate := "fail"
	if overlapAccounted && matrix.OverlapStorageRatio < 1.35 {
		overlapGate = "pass"
	}
	for i := range matrix.Variants {
		matrix.Variants[i].GateLedger.OverlapStorage = overlapGate
	}
	requiredVariantsGate := "fail"
	if overlapAccounted {
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
	matrix.Decision = m8DecisionReportV1(matrix.Variants)
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
	if err := validateM8ProductionMatrixV1(matrix); err != nil {
		return m8ProductionMatrixV1{}, err
	}
	return matrix, nil
}

func m8ProductionComparisonForRowV1(report m8ProductionReportV1, row m8ProductionRowV1) m8ProductionComparisonV1 {
	return m8ProductionComparisonV1{
		VariantID: report.Variant.VariantID, Status: row.Status, AssignmentBasis: report.Variant.AssignmentBasis, Overlap: report.Variant.OverlapRatio,
		ArtifactSHA256: report.Variant.ArtifactSHA256, ReadySetDigest: report.Variant.ReadySetDigest, RouterModelDigest: report.Variant.RouterModelDigest,
		Probes: row.Probes, EfSearch: row.EfSearch, Concurrency: row.Concurrency, Samples: row.Samples, RecallAtK: row.RecallAtK,
		QPS: row.QPS, P50Nanos: row.P50Nanos, P95Nanos: row.P95Nanos, P99Nanos: row.P99Nanos, MaxTotalNanos: row.MaxTotalNanos, RPCs: row.RPCs,
		RequestBytes: row.RequestBytes, CandidateBytes: row.CandidateBytes, ResponseBytes: row.ResponseBytes,
		PersistentAssetBytes: report.Resources.PersistentAssetBytes, PeakRSSBytes: report.Resources.PeakRSSBytes,
	}
}

// validateM8ProductionMatrixV1 binds every flattened comparison row to its
// source child measurement, including its measured outcome.
func validateM8ProductionMatrixV1(matrix m8ProductionMatrixV1) error {
	if !m8QualificationSHA256V1(matrix.ExecutableSHA256) {
		return errors.New("M8 matrix has an invalid benchmark executable digest")
	}
	if matrix.ExecutionStartedAt.IsZero() || matrix.ExecutionCompletedAt.IsZero() || !matrix.ExecutionCompletedAt.After(matrix.ExecutionStartedAt) {
		return errors.New("M8 matrix has an invalid execution interval")
	}
	type key struct {
		variantID               string
		probes, ef, concurrency int
	}
	sources := make(map[key]m8ProductionComparisonV1)
	for _, report := range matrix.Variants {
		if report.Variant == nil {
			return errors.New("M8 matrix report is missing variant identity")
		}
		if report.ExecutableSHA256 != matrix.ExecutableSHA256 {
			return errors.New("M8 matrix report uses a different benchmark executable")
		}
		for _, row := range report.Rows {
			if row.Status == "unsupported" {
				continue
			}
			if row.Status != "pass" && row.Status != "fail" && row.Status != "candidate_coverage_shortfall" {
				return errors.New("M8 matrix contains an invalid measured row status")
			}
			k := key{report.Variant.VariantID, row.Probes, row.EfSearch, row.Concurrency}
			if _, exists := sources[k]; exists {
				return errors.New("M8 matrix contains duplicate child comparison rows")
			}
			sources[k] = m8ProductionComparisonForRowV1(report, row)
		}
	}
	if len(matrix.Comparison) != len(sources) {
		return errors.New("M8 matrix comparison rows do not match child measurements")
	}
	for _, comparison := range matrix.Comparison {
		if comparison.Status != "pass" && comparison.Status != "fail" && comparison.Status != "candidate_coverage_shortfall" {
			return errors.New("M8 matrix comparison has an invalid measured status")
		}
		k := key{comparison.VariantID, comparison.Probes, comparison.EfSearch, comparison.Concurrency}
		if expected, ok := sources[k]; !ok || comparison != expected {
			return errors.New("M8 matrix comparison does not match child measurement")
		}
		delete(sources, k)
	}
	if len(sources) != 0 {
		return errors.New("M8 matrix omits child comparison rows")
	}
	return nil
}

// m8DecisionReportV1 emits one deterministic quarter-budget attribution view
// per retained variant, never a product-enablement claim.
func m8DecisionReportV1(reports []m8ProductionReportV1) []m8DecisionRowV1 {
	out := make([]m8DecisionRowV1, 0, len(reports)*6)
	for _, report := range reports {
		if report.Variant == nil {
			continue
		}
		targetProbes := max(1, report.Config.Partitions/4)
		var selected *m8ProductionRowV1
		for i := range report.Rows {
			row := &report.Rows[i]
			if row.Status != "pass" || row.Probes != targetProbes || selected != nil && (row.EfSearch > selected.EfSearch || row.EfSearch == selected.EfSearch && row.Concurrency >= selected.Concurrency) {
				continue
			}
			selected = row
		}
		if selected == nil {
			out = append(out, m8DecisionRowV1{VariantID: report.Variant.VariantID, Probes: targetProbes, Stage: "none", Owner: "no_quarter_probe_operating_point"})
			continue
		}
		added := false
		for _, stage := range selected.Attribution.StageOwners {
			if !stage.Active {
				continue
			}
			out = append(out, m8DecisionRowV1{VariantID: report.Variant.VariantID, Probes: selected.Probes, EfSearch: selected.EfSearch, Concurrency: selected.Concurrency, Stage: stage.Stage, Owner: stage.Owner, FromRecall: stage.FromRecall, ToRecall: stage.ToRecall, Delta: stage.Delta})
			added = true
		}
		if !added {
			out = append(out, m8DecisionRowV1{VariantID: report.Variant.VariantID, Probes: selected.Probes, EfSearch: selected.EfSearch, Concurrency: selected.Concurrency, Stage: "none", Owner: "none_observed"})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].VariantID != out[j].VariantID {
			return out[i].VariantID < out[j].VariantID
		}
		return out[i].Stage < out[j].Stage
	})
	return out
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
			if base.Status != "pass" || base.Probes != report.Config.Partitions || !base.Attribution.ExhaustivePartitionIDParity || !base.Attribution.ExhaustivePartitionScoreParity || base.Attribution.ExhaustivePartitionRecallAtK != 1 || base.RecallAtK < report.Config.RecallTarget || candidate.EfSearch != base.EfSearch || candidate.Concurrency != base.Concurrency {
				continue
			}
			if candidate.QPS >= base.QPS*1.15 && candidate.P95Nanos <= base.P95Nanos {
				return true
			}
		}
	}
	return false
}
