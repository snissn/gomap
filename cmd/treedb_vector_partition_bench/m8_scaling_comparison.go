package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"reflect"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
)

// This joins existing reports; it is not a campaign runner or final acceptance
// authority. Replay pins are externally frozen at publication, coordinates
// prospectively. No best-row selection or historical campaign relaxation.
type m8ScalingComparisonPlanV1 struct {
	Reports []struct {
		Name       string   `json:"name"`
		ReplayArgs []string `json:"replay_args"`
	} `json:"reports"`
	Pairs []m8ScalingPairV1 `json:"pairs"`
}

type m8ScalingPairV1 struct {
	Name            string `json:"name"`
	Kind            string `json:"kind"`
	Baseline        string `json:"baseline"`
	Candidate       string `json:"candidate"`
	BaselineProbes  int    `json:"baseline_probes"`
	CandidateProbes int    `json:"candidate_probes"`
}

type m8ScalingPairResultV1 struct {
	Pair               m8ScalingPairV1    `json:"pair"`
	Blocks             []m8ScalingBlockV1 `json:"blocks"`
	AllMatchedQuality  bool               `json:"all_matched_quality"`
	AllQPS15Percent    bool               `json:"all_qps_at_least_1_15x"`
	AllP95NoRegression bool               `json:"all_p95_no_regression"`
}

type m8ScalingBlockV1 struct {
	Repetition     int     `json:"repetition"`
	Concurrency    int     `json:"concurrency"`
	BaselineRow    int     `json:"baseline_row"`
	CandidateRow   int     `json:"candidate_row"`
	MatchedQuality bool    `json:"matched_quality"`
	QPSRatio       float64 `json:"qps_ratio"`
	P95Ratio       float64 `json:"p95_ratio"`
}

func runM8ScalingComparisonV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("compare-m8-scaling", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var planPath, planSHA string
	fs.StringVar(&planPath, "plan", "", "bounded named report/pair plan JSON")
	fs.StringVar(&planSHA, "plan-sha256", "", "external SHA256 of the published frozen plan")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || planPath == "" || !m8SHA256V1(planSHA) {
		return errors.New("comparison requires -plan and external -plan-sha256")
	}
	raw, err := readBoundedRegularFileV1(planPath, 64<<10)
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != planSHA {
		return errors.New("comparison plan differs from frozen digest")
	}
	var plan m8ScalingComparisonPlanV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("comparison plan has trailing JSON")
	}
	if len(plan.Reports) < 1 || len(plan.Reports) > 8 || len(plan.Pairs) < 1 || len(plan.Pairs) > 8 {
		return errors.New("comparison requires 1..8 named reports and pairs")
	}
	reports := make(map[string]m8ProductionReportV1, len(plan.Reports))
	receipts := make(map[string]string, len(plan.Reports))
	unions := make(map[string]string, len(plan.Reports))
	for _, input := range plan.Reports {
		if input.Name == "" || len(input.Name) > 80 || len(input.ReplayArgs) > 32 {
			return errors.New("invalid comparison report name/arguments")
		}
		if _, exists := reports[input.Name]; exists {
			return errors.New("duplicate comparison report")
		}
		var report m8ProductionReportV1
		var receipt bytes.Buffer
		if err := replayM8ReportV1(input.ReplayArgs, &receipt, &report); err != nil {
			return fmt.Errorf("%s: %w", input.Name, err)
		}
		assets, err := openM8ProductionExistingAssetSetV1(report.Variant.DatabaseDirectory)
		if err != nil {
			return err
		}
		// Rebind after replay so a changed retained layout cannot supply a new
		// union for an old report. No corpus allocation or new truth pass.
		bindErr := m8BindRetainedM3DescriptorWithPolicyV1(assets, report.Dataset, true)
		if bindErr == nil && !reflect.DeepEqual(assets.descriptor, report.Variant) {
			bindErr = errors.New("comparison assets changed after replay")
		}
		digest, unionErr := m8LogicalMembershipDigestV1(assets.manifest)
		if err := errors.Join(bindErr, unionErr, assets.Close()); err != nil {
			return err
		}
		reports[input.Name], receipts[input.Name], unions[input.Name] = report, receipt.String(), digest
	}
	results := make([]m8ScalingPairResultV1, 0, len(plan.Pairs))
	names := map[string]bool{}
	for _, pair := range plan.Pairs {
		if pair.Name == "" || len(pair.Name) > 80 || names[pair.Name] {
			return errors.New("invalid/duplicate comparison pair name")
		}
		names[pair.Name] = true
		baseline, bOK := reports[pair.Baseline]
		candidate, cOK := reports[pair.Candidate]
		if !bOK || !cOK {
			return errors.New("comparison pair names an absent report")
		}
		result, err := m8CompareScalingPairV1(pair, baseline, candidate, unions[pair.Baseline], unions[pair.Candidate])
		if err != nil {
			return fmt.Errorf("%s: %w", pair.Name, err)
		}
		results = append(results, result)
	}
	// Retain every original row/status and ledger, not just compared successes.
	// Large raw attempt arrays remain in their separately pinned strict reports.
	for name, report := range reports {
		report.Rows = slices.Clone(report.Rows)
		for i := range report.Rows {
			if report.Rows[i].Accounting != nil {
				accounting := *report.Rows[i].Accounting
				accounting.Attempts = nil
				report.Rows[i].Accounting = &accounting
			}
		}
		reports[name] = report
	}
	return json.NewEncoder(stdout).Encode(struct {
		Status                   string                          `json:"status"`
		PlanSHA256               string                          `json:"plan_sha256"`
		Scope                    string                          `json:"scope"`
		ReplayReceipts           map[string]string               `json:"replay_receipts"`
		LogicalMembershipDigests map[string]string               `json:"logical_membership_digests"`
		Reports                  map[string]m8ProductionReportV1 `json:"report_projections"`
		Pairs                    []m8ScalingPairResultV1         `json:"pairs"`
	}{"COMPARISON_REDUCED_NOT_QUALIFICATION", planSHA,
		"same frozen source/runtime, profiled native TCP boundary; five complete blocks at c1/c32, C256/EF96/top10/recall>=.95; all report rows retained, raw attempts in pinned reports; CPU/source-host admission and full F envelope remain external obligations; not predecessor or ordinary-HNSW comparison",
		receipts, unions, reports, results})
}

func m8CompareScalingPairV1(pair m8ScalingPairV1, baseline, candidate m8ProductionReportV1, baselineUnion, candidateUnion string) (m8ScalingPairResultV1, error) {
	result := m8ScalingPairResultV1{Pair: pair, AllMatchedQuality: true, AllQPS15Percent: true, AllP95NoRegression: true}
	for _, r := range []m8ProductionReportV1{baseline, candidate} {
		if r.Config.GraphVariant != string(collections.VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1) {
			return result, errors.New("comparison requires selected connectivity-preserving Vamana")
		}
		if r.Variant == nil || r.Config.MeasurementAccounting != m8CompleteAttemptsV1 || r.Config.MeasuredRepetitions != 5 || r.Config.TopK != 10 || r.Config.RecallTarget < .95 || r.Config.RouterScoreBudget != 256 || !slices.Equal(r.Config.EfSearch, []int{96}) || !slices.Equal(r.Config.Concurrency, []int{1, 32}) || r.Config.DomainCount < 1 || len(r.Config.Overlap) != 1 {
			return result, errors.New("comparison requires complete five-block C256/EF96/top10/c1,c32/recall>=.95 reports")
		}
	}
	if !reflect.DeepEqual(baseline.Dataset, candidate.Dataset) || baseline.TruthCache.Identity != candidate.TruthCache.Identity || baseline.TruthCache.ArtifactSHA256 != candidate.TruthCache.ArtifactSHA256 || baseline.Host != candidate.Host || baseline.GoVersion != candidate.GoVersion || baseline.GOOS != candidate.GOOS || baseline.GOARCH != candidate.GOARCH || baseline.GOMAXPROCS != candidate.GOMAXPROCS || baseline.GoMemoryLimitBytes != candidate.GoMemoryLimitBytes || baseline.LogicalCPUs != candidate.LogicalCPUs || baseline.BaseSHA != candidate.BaseSHA || baseline.HeadSHA != candidate.HeadSHA || baseline.ExecutableSHA256 != candidate.ExecutableSHA256 {
		return result, errors.New("comparison fixture/truth/host/runtime/source mismatch")
	}
	bv, cv := baseline.Variant, candidate.Variant
	if !reflect.DeepEqual(bv.PartitionConfig, cv.PartitionConfig) || !reflect.DeepEqual(bv.RouterConfig, cv.RouterConfig) || bv.GraphBuildSHA256 != cv.GraphBuildSHA256 || bv.AssignmentBasis != cv.AssignmentBasis || bv.ArtifactBackend != cv.ArtifactBackend || bv.KaHIPPythonSHA256 != cv.KaHIPPythonSHA256 || bv.KaHIPAdapterSHA256 != cv.KaHIPAdapterSHA256 || bv.IndexDefinitionDigest != cv.IndexDefinitionDigest || bv.PartitionHNSWM != cv.PartitionHNSWM || m3DescriptorPartitionHNSWEfCV1(*bv) != m3DescriptorPartitionHNSWEfCV1(*cv) || bv.SourceOrdinalDigest != cv.SourceOrdinalDigest {
		return result, errors.New("comparison graph/assignment/router/source-index mismatch")
	}
	bc, cc := baseline.Config, candidate.Config
	bc.Probes, cc.Probes = nil, nil // Only explicitly named probe coordinates may differ.
	switch pair.Kind {
	case "selected_exhaustive":
		if pair.Baseline != pair.Candidate || baseline.ExecutionID != candidate.ExecutionID || pair.BaselineProbes != bc.DomainCount || pair.CandidateProbes >= pair.BaselineProbes {
			return result, errors.New("selected/exhaustive must be two coordinates of the same report")
		}
	case "disjoint_overlap":
		if bv.OverlapRatio != 0 || cv.OverlapRatio != .2 {
			return result, errors.New("overlap comparison requires disjoint baseline and .20 candidate")
		}
		bc.Overlap, cc.Overlap = nil, nil
	case "logical_packing":
		if bv.Partitions != uint32(bc.DomainCount) || cv.Partitions <= bv.Partitions || !m8SHA256V1(baselineUnion) || baselineUnion != candidateUnion || pair.BaselineProbes != pair.CandidateProbes {
			return result, errors.New("packing requires one-pack baseline and identical logical unions/probes across a multi-pack candidate")
		}
		bc.Partitions, cc.Partitions = 0, 0
		bc.PacksPerDomain, cc.PacksPerDomain = nil, nil
	default:
		return result, errors.New("unsupported comparison kind")
	}
	if !reflect.DeepEqual(bc, cc) || pair.BaselineProbes < 1 || pair.CandidateProbes < 1 || pair.BaselineProbes > bc.DomainCount || pair.CandidateProbes > cc.DomainCount {
		return result, errors.New("comparison changed an undeclared serving coordinate")
	}
	// Five paired blocks, including failed windows; no median cherry-picking.
	for repetition := range 5 {
		for _, concurrency := range []int{1, 32} {
			bi, err := m8ScalingRowIndexV1(baseline, pair.BaselineProbes, concurrency, repetition)
			if err != nil {
				return result, err
			}
			ci, err := m8ScalingRowIndexV1(candidate, pair.CandidateProbes, concurrency, repetition)
			if err != nil {
				return result, err
			}
			b, c := baseline.Rows[bi], candidate.Rows[ci]
			quality := func(r m8ProductionRowV1, n int, target float64) bool {
				return r.Status == "pass" && r.Accounting != nil && r.Samples == n && r.Accounting.Summary.Succeeded == n && r.Accounting.Summary.ServiceRecall >= target && r.RecallAtK >= target && r.QPS > 0 && r.P95Nanos > 0
			}
			matched := quality(b, baseline.Dataset.Queries, baseline.Config.RecallTarget) && quality(c, candidate.Dataset.Queries, candidate.Config.RecallTarget)
			block := m8ScalingBlockV1{Repetition: repetition, Concurrency: concurrency, BaselineRow: bi, CandidateRow: ci, MatchedQuality: matched}
			if matched {
				block.QPSRatio, block.P95Ratio = c.QPS/b.QPS, float64(c.P95Nanos)/float64(b.P95Nanos)
			}
			result.AllMatchedQuality = result.AllMatchedQuality && matched
			result.AllQPS15Percent = result.AllQPS15Percent && matched && block.QPSRatio >= 1.15
			result.AllP95NoRegression = result.AllP95NoRegression && matched && block.P95Ratio <= 1
			result.Blocks = append(result.Blocks, block)
		}
	}
	return result, nil
}

func m8ScalingRowIndexV1(report m8ProductionReportV1, probes, concurrency, repetition int) (int, error) {
	index := -1
	for i, row := range report.Rows {
		if row.Probes != probes || row.Concurrency != concurrency || row.Repetition != repetition || row.EfSearch != 96 || row.Overlap != report.Config.Overlap[0] {
			continue
		}
		if index >= 0 {
			return -1, errors.New("duplicate comparison block")
		}
		index = i
	}
	if index < 0 {
		return -1, errors.New("missing comparison block (cannot reduce successful subset)")
	}
	return index, nil
}

func m8LogicalMembershipDigestV1(manifest collections.VectorPartitionManifestV1) (string, error) {
	owners, _, err := m8QualityPackOwnersV1(manifest)
	if err != nil {
		return "", err
	}
	type member struct {
		domain  uint32
		ordinal uint64
	}
	members := make([]member, 0, len(manifest.Memberships)+len(manifest.OverlapMemberships))
	add := func(pack uint32, ordinal uint64) error {
		if int(pack) >= len(owners) || ordinal >= manifest.SourceRowCount {
			return errors.New("logical union has invalid pack/ordinal")
		}
		members = append(members, member{owners[pack], ordinal})
		return nil
	}
	for _, m := range manifest.Memberships {
		if err := add(m.PartitionID, m.VectorOrdinal); err != nil {
			return "", err
		}
	}
	for _, m := range manifest.OverlapMemberships {
		if err := add(m.PartitionID, m.VectorOrdinal); err != nil {
			return "", err
		}
	}
	slices.SortFunc(members, func(a, b member) int {
		if a.domain < b.domain {
			return -1
		}
		if a.domain > b.domain {
			return 1
		}
		if a.ordinal < b.ordinal {
			return -1
		}
		if a.ordinal > b.ordinal {
			return 1
		}
		return 0
	})
	members = slices.Compact(members)
	h := sha256.New()
	h.Write([]byte("treedb/m8/logical-membership-unions/v1\x00"))
	var raw [12]byte
	binary.LittleEndian.PutUint32(raw[:4], manifest.DomainCount)
	binary.LittleEndian.PutUint64(raw[4:], manifest.SourceRowCount)
	h.Write(raw[:])
	for _, m := range members {
		binary.LittleEndian.PutUint32(raw[:4], m.domain)
		binary.LittleEndian.PutUint64(raw[4:], m.ordinal)
		h.Write(raw[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
