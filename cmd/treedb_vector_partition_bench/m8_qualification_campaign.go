package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
)

// m8QualificationCampaignV1 is deliberately an evidence-only reader.  M8
// emits one strict matrix per run; qualification needs three independently
// hashed matrices before it can make a repeat/variance claim.
type m8QualificationCampaignV1 struct {
	FixtureChecksum string
	HeadSHA         string
	Runs            []m8QualificationCampaignRunV1
}

type m8QualificationCampaignRunV1 struct {
	Path   string
	SHA256 string
}

type m8QualificationCampaignSummaryV1 struct {
	P4QPSMin, P4QPSMedian, P4QPSMax    float64
	P16QPSMin, P16QPSMedian, P16QPSMax float64
	P4P95Max, P16P95Max                uint64
}

func m8ValidateQualificationCampaignV1(root string, campaign m8QualificationCampaignV1) (m8QualificationCampaignSummaryV1, error) {
	if campaign.FixtureChecksum == "" || !validSHA(campaign.HeadSHA) || len(campaign.Runs) != 3 {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification campaign requires one fixture/head and exactly three runs")
	}
	var p4QPS, p16QPS []float64
	var summary m8QualificationCampaignSummaryV1
	variantArtifacts := make(map[string]string, len(m8RequiredVariantIDsV1))
	truthArtifact := ""
	for _, run := range campaign.Runs {
		if run.Path == "" || filepath.IsAbs(run.Path) || !m8SHA256V1(run.SHA256) {
			return summary, errors.New("qualification campaign has malformed retained artifact identity")
		}
		raw, err := os.ReadFile(filepath.Join(root, run.Path))
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
		if err := validateM8ProductionMatrixV1(matrix); err != nil {
			return summary, fmt.Errorf("validate qualification matrix %s: %w", run.Path, err)
		}
		if matrix.HeadSHA != campaign.HeadSHA || matrix.Dataset.Checksum != campaign.FixtureChecksum || len(matrix.Variants) != len(m8RequiredVariantIDsV1) || !slices.Equal(matrix.RequiredVariants, m8RequiredVariantIDsV1) || matrix.Gates.RequiredVariants != "pass" || matrix.Gates.ExhaustiveParity != "pass" || matrix.Gates.FailureHonesty != "pass" || matrix.Gates.PartitionPackReachability != "pass" || matrix.Gates.Balance != "pass" || matrix.Gates.ResourceBounds != "pass" || matrix.Gates.OverlapStorage != "pass" || matrix.OverlapStorageRatio >= 1.35 {
			return summary, fmt.Errorf("qualification matrix %s does not bind the required identity/gates", run.Path)
		}
		var selected *m8ProductionReportV1
		seenVariants := make(map[string]bool, len(m8RequiredVariantIDsV1))
		for i := range matrix.Variants {
			report := &matrix.Variants[i]
			if report.HeadSHA != campaign.HeadSHA || report.Dataset.Checksum != campaign.FixtureChecksum || report.Dirty || !m8SHA256V1(report.TruthCache.ArtifactSHA256) || report.Variant == nil || seenVariants[report.Variant.VariantID] || !slices.Contains(m8RequiredVariantIDsV1, report.Variant.VariantID) || !m8SHA256V1(report.Variant.ArtifactSHA256) {
				return summary, fmt.Errorf("qualification matrix %s has unbound child identity", run.Path)
			}
			seenVariants[report.Variant.VariantID] = true
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
