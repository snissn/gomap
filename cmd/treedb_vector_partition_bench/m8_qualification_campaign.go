package main

import (
	"crypto/sha256"
	"encoding/hex"
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
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
)

// m8QualificationCampaignV1 is deliberately an evidence-only reader.  M8
// emits one strict matrix per run; qualification needs three independently
// hashed matrices before it can make a repeat/variance claim.
type m8QualificationCampaignV1 struct {
	FixtureChecksum string                         `json:"fixture_checksum"`
	BaseSHA         string                         `json:"base_sha"`
	HeadSHA         string                         `json:"head_sha"`
	Runs            []m8QualificationCampaignRunV1 `json:"runs"`
}

type m8QualificationCampaignRunV1 struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type m8QualificationCampaignSummaryV1 struct {
	P4QPSMin     float64 `json:"p4_qps_min"`
	P4QPSMedian  float64 `json:"p4_qps_median"`
	P4QPSMax     float64 `json:"p4_qps_max"`
	P16QPSMin    float64 `json:"p16_qps_min"`
	P16QPSMedian float64 `json:"p16_qps_median"`
	P16QPSMax    float64 `json:"p16_qps_max"`
	P4P95Max     uint64  `json:"p4_p95_max"`
	P16P95Max    uint64  `json:"p16_p95_max"`
}

func runValidateQualification(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench validate-qualification", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var index string
	fs.StringVar(&index, "index", "", "qualification campaign index JSON; retained artifacts must be below its directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || index == "" {
		return errors.New("validate-qualification requires -index")
	}
	index, err := filepath.Abs(index)
	if err != nil {
		return fmt.Errorf("qualification index path: %w", err)
	}
	index, err = filepath.EvalSymlinks(index)
	if err != nil {
		return fmt.Errorf("resolve qualification index: %w", err)
	}
	info, err := os.Stat(index)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("qualification index is not a regular file")
	}
	raw, err := os.ReadFile(index)
	if err != nil {
		return fmt.Errorf("read qualification index: %w", err)
	}
	var campaign m8QualificationCampaignV1
	if err := json.Unmarshal(raw, &campaign); err != nil {
		return fmt.Errorf("decode qualification index: %w", err)
	}
	summary, err := m8ValidateQualificationCampaignV1(filepath.Dir(index), campaign)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(summary)
}

func m8ValidateQualificationCampaignV1(root string, campaign m8QualificationCampaignV1) (m8QualificationCampaignSummaryV1, error) {
	if !m8QualificationSHA256V1(campaign.FixtureChecksum) || !m8QualificationGitSHAV1(campaign.BaseSHA) || !m8QualificationGitSHAV1(campaign.HeadSHA) || len(campaign.Runs) != 3 {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification campaign requires one fixture/head and exactly three runs")
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return m8QualificationCampaignSummaryV1{}, fmt.Errorf("resolve qualification root: %w", err)
	}
	rootInfo, err := os.Stat(resolvedRoot)
	if err != nil || !rootInfo.IsDir() {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification root is not a directory")
	}
	var p4QPS, p16QPS []float64
	var summary m8QualificationCampaignSummaryV1
	variantArtifacts := make(map[string]string, len(m8RequiredVariantIDsV1))
	truthArtifact := ""
	var dataset fixtureManifest
	configs := make(map[string]m8ProductionConfigEvidenceV1, len(m8RequiredVariantIDsV1))
	paths, digests := make(map[string]bool, len(campaign.Runs)), make(map[string]bool, len(campaign.Runs))
	for runIndex, run := range campaign.Runs {
		if run.Path == "" || filepath.IsAbs(run.Path) || !m8QualificationSHA256V1(run.SHA256) {
			return summary, errors.New("qualification campaign has malformed retained artifact identity")
		}
		cleanPath := filepath.Clean(run.Path)
		if cleanPath == "." || cleanPath == ".." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) || paths[cleanPath] || digests[run.SHA256] {
			return summary, errors.New("qualification campaign has duplicate or escaping retained artifact identity")
		}
		paths[cleanPath], digests[run.SHA256] = true, true
		resolvedPath, err := filepath.EvalSymlinks(filepath.Join(resolvedRoot, cleanPath))
		if err != nil {
			return summary, fmt.Errorf("resolve qualification artifact %s: %w", cleanPath, err)
		}
		rel, err := filepath.Rel(resolvedRoot, resolvedPath)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return summary, errors.New("qualification campaign artifact escapes root")
		}
		info, err := os.Stat(resolvedPath)
		if err != nil || !info.Mode().IsRegular() {
			return summary, errors.New("qualification campaign artifact is not a regular file")
		}
		raw, err := os.ReadFile(resolvedPath)
		if err != nil {
			return summary, err
		}
		digest := sha256.Sum256(raw)
		if hex.EncodeToString(digest[:]) != run.SHA256 {
			return summary, fmt.Errorf("qualification campaign artifact digest mismatch: %s", run.Path)
		}
		var matrix m8ProductionMatrixV1
		if err := json.Unmarshal(raw, &matrix); err != nil {
			return summary, fmt.Errorf("decode qualification matrix %s: %w", run.Path, err)
		}
		if matrix.SchemaVersion != 4 || matrix.ResultKind != "m8_production_multi_variant_matrix_v4" || matrix.Status != "local_gate_pass" || matrix.BaseSHA != campaign.BaseSHA || matrix.HeadSHA != campaign.HeadSHA || !m8QualificationGitSHAV1(matrix.BaseSHA) || !m8QualificationGitSHAV1(matrix.HeadSHA) {
			return summary, fmt.Errorf("qualification matrix %s has invalid schema/provenance/status", cleanPath)
		}
		if err := validateM8ProductionMatrixV1(matrix); err != nil {
			return summary, fmt.Errorf("validate qualification matrix %s: %w", run.Path, err)
		}
		if matrix.Dataset.Checksum != campaign.FixtureChecksum || len(matrix.Variants) != len(m8RequiredVariantIDsV1) || !slices.Equal(matrix.RequiredVariants, m8RequiredVariantIDsV1) || matrix.Gates.RequiredVariants != "pass" || matrix.Gates.ExhaustiveParity != "pass" || matrix.Gates.FailureHonesty != "pass" || matrix.Gates.PartitionPackReachability != "pass" || matrix.Gates.Balance != "pass" || matrix.Gates.ResourceBounds != "pass" || matrix.Gates.OverlapStorage != "pass" || matrix.OverlapStorageRatio >= 1.35 {
			return summary, fmt.Errorf("qualification matrix %s does not bind the required identity/gates", run.Path)
		}
		if dataset.Checksum != "" && dataset != matrix.Dataset {
			return summary, fmt.Errorf("qualification matrix %s changes dataset manifest", cleanPath)
		}
		dataset = matrix.Dataset
		var selected *m8ProductionReportV1
		seenVariants := make(map[string]bool, len(m8RequiredVariantIDsV1))
		for i := range matrix.Variants {
			report := &matrix.Variants[i]
			if err := validateM8ProductionReportV1(*report); err != nil {
				return summary, fmt.Errorf("validate qualification child %s: %w", cleanPath, err)
			}
			if runIndex == 0 && !m8QualificationHasFullLadderV1(*report) {
				return summary, fmt.Errorf("qualification matrix %s omits the required p1/2/4/8/16 ladder", cleanPath)
			}
			if report.BaseSHA != campaign.BaseSHA || report.HeadSHA != campaign.HeadSHA || report.Dataset.Checksum != campaign.FixtureChecksum || report.Dirty || !m8QualificationSHA256V1(report.TruthCache.ArtifactSHA256) || report.Variant == nil || seenVariants[report.Variant.VariantID] || !slices.Contains(m8RequiredVariantIDsV1, report.Variant.VariantID) || !m8QualificationSHA256V1(report.Variant.ArtifactSHA256) {
				return summary, fmt.Errorf("qualification matrix %s has unbound child identity", run.Path)
			}
			seenVariants[report.Variant.VariantID] = true
			config := report.Config
			config.Probes = nil // repeat one records the full p1/2/4/8/16 ladder; repeats two and three retain p4/p16.
			if prior, ok := configs[report.Variant.VariantID]; ok && !reflect.DeepEqual(prior, config) {
				return summary, fmt.Errorf("qualification matrix %s changes %s topology/configuration", cleanPath, report.Variant.VariantID)
			}
			configs[report.Variant.VariantID] = config
			if prior := variantArtifacts[report.Variant.VariantID]; prior != "" && prior != report.Variant.ArtifactSHA256 {
				return summary, fmt.Errorf("qualification matrix %s changes variant identity", run.Path)
			}
			variantArtifacts[report.Variant.VariantID] = report.Variant.ArtifactSHA256
			if truthArtifact != "" && truthArtifact != report.TruthCache.ArtifactSHA256 {
				return summary, fmt.Errorf("qualification matrix %s changes truth identity", run.Path)
			}
			truthArtifact = report.TruthCache.ArtifactSHA256
			if report.Variant.VariantID == "graph-overlap-020-v1" {
				selected = report
			}
		}
		if err := m8ValidateQualificationMatrixDerivationV1(matrix); err != nil {
			return summary, fmt.Errorf("derive qualification matrix %s: %w", cleanPath, err)
		}
		if len(seenVariants) != len(m8RequiredVariantIDsV1) || selected == nil {
			return summary, fmt.Errorf("qualification matrix %s has no graph-overlap candidate", run.Path)
		}
		p4, p16 := m8QualificationRowsV1(*selected)
		if p4 == nil || p16 == nil || p4.RecallAtK < .90 || p16.RecallAtK < .90 || p4.Attribution.FinalMembershipOracleRecallAtK < .90 || math.Abs(p4.Attribution.ExactToApproximateLossAtK) > .01 || p4.QPS < p16.QPS*1.15 || p4.P95Nanos > p16.P95Nanos {
			return summary, fmt.Errorf("qualification matrix %s misses the selected p4/p16 gate", run.Path)
		}
		p4QPS, p16QPS = append(p4QPS, p4.QPS), append(p16QPS, p16.QPS)
		summary.P4P95Max, summary.P16P95Max = max(summary.P4P95Max, p4.P95Nanos), max(summary.P16P95Max, p16.P95Nanos)
	}
	minMedianMax := func(values []float64) (float64, float64, float64) {
		sort.Float64s(values)
		return values[0], values[len(values)/2], values[len(values)-1]
	}
	summary.P4QPSMin, summary.P4QPSMedian, summary.P4QPSMax = minMedianMax(p4QPS)
	summary.P16QPSMin, summary.P16QPSMedian, summary.P16QPSMax = minMedianMax(p16QPS)
	return summary, nil
}

func m8ValidateQualificationMatrixDerivationV1(matrix m8ProductionMatrixV1) error {
	if len(matrix.Variants) == 0 {
		return errors.New("qualification matrix has no child reports")
	}
	cfg := config{baseSHA: matrix.BaseSHA, headSHA: matrix.HeadSHA, partitions: matrix.Variants[0].Config.Partitions, command: append([]string(nil), matrix.Command...)}
	expected, err := m8BuildProductionMatrixV1(cfg, matrix.Dataset, append([]m8ProductionReportV1(nil), matrix.Variants...))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(matrix.RequiredVariants, expected.RequiredVariants) || !reflect.DeepEqual(matrix.Gates, expected.Gates) || matrix.Status != expected.Status || matrix.Disposition != expected.Disposition || matrix.OverlapMaterializationRatio != expected.OverlapMaterializationRatio || matrix.OverlapStorageRatio != expected.OverlapStorageRatio || !reflect.DeepEqual(matrix.OverlapDiagnostics, expected.OverlapDiagnostics) || !reflect.DeepEqual(matrix.Decision, expected.Decision) || !reflect.DeepEqual(matrix.Comparison, expected.Comparison) {
		return errors.New("qualification matrix derived evidence does not match child reports")
	}
	return nil
}

func m8QualificationHasFullLadderV1(report m8ProductionReportV1) bool {
	seen := make(map[int]bool, 5)
	for _, row := range report.Rows {
		if row.Status == "pass" && row.EfSearch == 64 && row.Concurrency == 1 && row.RouterMode == collections.VectorPartitionRouterModeApproxV1 && row.RouterCandidates == 64 {
			seen[row.Probes] = true
		}
	}
	return seen[1] && seen[2] && seen[4] && seen[8] && seen[16]
}

func m8QualificationSHA256V1(value string) bool {
	return len(value) == sha256.Size*2 && value == strings.ToLower(value) && m8SHA256V1(value)
}

func m8QualificationGitSHAV1(value string) bool {
	return len(value) == 40 && value == strings.ToLower(value) && validSHA(value)
}

func m8QualificationRowsV1(report m8ProductionReportV1) (*m8ProductionRowV1, *m8ProductionRowV1) {
	var p4, p16 *m8ProductionRowV1
	for i := range report.Rows {
		row := &report.Rows[i]
		if row.Status != "pass" || row.EfSearch != 64 || row.Concurrency != 1 || row.RouterMode != collections.VectorPartitionRouterModeApproxV1 || row.RouterCandidates != 64 {
			continue
		}
		switch row.Probes {
		case 4:
			p4 = row
		case 16:
			p16 = row
		}
	}
	return p4, p16
}
