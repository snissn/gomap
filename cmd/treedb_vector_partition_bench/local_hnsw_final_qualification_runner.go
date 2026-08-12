package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	localHNSWFinalApprovalEvidencePathV1         = "TreeDB/docs/evidence/vector-partition-revalidation-4093"
	localHNSWFinalApprovalPullRequestV1          = 4115
	localHNSWFinalQualificationM18TimingSHA256V1 = "b89f6e8dfb04406c916eb8243cb3e9f7905d0977f4ca5f7f35bf2a39e2a2f655"
)

func runLocalHNSWFinalQualificationV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-final-qualification", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset250, dataset100, truth250, truth100, calibrationPath, holdoutPath, m18Curve, m18Timing string
	var baseline250, candidate250, baseline100, candidate100, artifacts, out string
	var baseSHA, headSHA, approvalSHA, sourceCheckout string
	fs.StringVar(&dataset250, "dataset-250k", "", "frozen 250k fixture directory")
	fs.StringVar(&dataset100, "dataset-100k", "", "frozen 100k fixture directory")
	fs.StringVar(&truth250, "truth-cache-250k", "", "frozen 250k truth-cache directory")
	fs.StringVar(&truth100, "truth-cache-100k", "", "frozen 100k truth-cache directory")
	fs.StringVar(&calibrationPath, "calibration-split", "", "frozen 250k calibration manifest")
	fs.StringVar(&holdoutPath, "holdout-split", "", "sealed 250k holdout manifest")
	fs.StringVar(&m18Curve, "m18-ef-curve", "", "selected M18/eFC256 lower-EF calibration curve")
	fs.StringVar(&m18Timing, "m18-timing", "", "selected M18/eFC256 lower-EF calibration timing report")
	fs.StringVar(&baseline250, "baseline-db-250k", "", "retained 250k M16/eFC128 database")
	fs.StringVar(&candidate250, "candidate-db-250k", "", "retained 250k M18/eFC256 database")
	fs.StringVar(&baseline100, "baseline-db-100k", "", "retained 100k M16/eFC128 database")
	fs.StringVar(&candidate100, "candidate-db-100k", "", "retained 100k M18/eFC256 database")
	fs.StringVar(&artifacts, "artifacts", "", "fresh empty child-artifact directory")
	fs.StringVar(&out, "out", "", "fresh final report path")
	fs.StringVar(&baseSHA, "base-sha", "", "source-lock base SHA")
	fs.StringVar(&headSHA, "head-sha", "", "exact implementation head SHA")
	fs.StringVar(&approvalSHA, "approval-sha", "", "merged #4093 approval SHA")
	fs.StringVar(&sourceCheckout, "source-checkout", "", "clean exact-head checkout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	paths := []*string{&dataset250, &dataset100, &truth250, &truth100, &calibrationPath, &holdoutPath, &m18Curve, &m18Timing, &baseline250, &candidate250, &baseline100, &candidate100, &artifacts, &out, &sourceCheckout}
	if fs.NArg() != 0 || baseSHA == "" || headSHA == "" || approvalSHA == "" {
		return errors.New("local-hnsw-final-qualification requires all frozen inputs, retained databases, outputs, provenance, and no positional arguments")
	}
	for _, path := range paths {
		if *path == "" {
			return errors.New("local-hnsw-final-qualification requires all frozen inputs, retained databases, outputs, provenance, and no positional arguments")
		}
		resolved, err := m8CanonicalPathV1(*path)
		if err != nil {
			return err
		}
		*path = resolved
	}
	var err error
	baseSHA, headSHA, err = provenanceWithExplicitV1(baseSHA, headSHA)
	if err != nil || baseSHA != localHNSWAttributionSourceLockV1 || !validLowerSHA(approvalSHA) {
		return errors.New("local HNSW final qualification source lock")
	}
	sourceCheckout, err = localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA)
	if err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW final qualification requires clean exact-head source containing the approval SHA")
	}
	if err := localHNSWFinalQualificationApprovalV1(sourceCheckout, approvalSHA, headSHA); err != nil {
		return err
	}
	if filepath.Ext(out) != ".json" {
		return errors.New("local HNSW final qualification report must use .json")
	}
	if info, statErr := os.Lstat(artifacts); statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("invalid local HNSW final qualification artifact directory")
	}
	if entries, readErr := os.ReadDir(artifacts); readErr != nil || len(entries) != 0 {
		return errors.New("local HNSW final qualification artifact directory must be empty")
	}
	if _, statErr := os.Lstat(out); !errors.Is(statErr, os.ErrNotExist) {
		return errors.New("local HNSW final qualification report exists")
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
	if !m8QualificationBenchmarkExecutableV1(sourceCheckout, executable, headSHA, executableSHA) {
		return errors.New("local HNSW final qualification executable does not bind the exact clean head")
	}
	curve, curveSHA, err := loadLocalHNSWRepairM18EFCurveV1(m18Curve)
	if err != nil || curveSHA != localHNSWRepairMTimingSelectedCurveSHA256V1 || curve.Disposition != "smallest_point_passes_ef_120" {
		return errors.New("local HNSW final qualification M18 curve provenance")
	}
	rawTiming, err := readBoundedRegularFileV1(m18Timing, m8QualificationMatrixMaxBytesV1)
	if err != nil {
		return err
	}
	var timing localHNSWRepairMTimingReportV1
	decoder := json.NewDecoder(bytes.NewReader(rawTiming))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&timing); err != nil || decoder.Decode(&struct{}{}) != io.EOF || validateLocalHNSWRepairMTimingReportV1(timing) != nil {
		return errors.New("local HNSW final qualification M18 timing provenance")
	}
	timingDigest := sha256.Sum256(rawTiming)
	if hex.EncodeToString(timingDigest[:]) != localHNSWFinalQualificationM18TimingSHA256V1 || timing.SelectedCurve.SHA256 != curveSHA || timing.BaselineEFSearch != localHNSWFinalQualificationBaselineEFV1 || timing.CandidateEFSearch != localHNSWFinalQualificationCandidateEFV1 {
		return errors.New("local HNSW final qualification M18 timing identity")
	}

	fixtures := map[string]fixtureManifest{}
	datasets := map[string]string{localHNSWFinalQualificationCorpus250KV1: dataset250, localHNSWFinalQualificationCorpus100KV1: dataset100}
	manifestEvidence := map[string]localHNSWAttributionFileInputV1{}
	for corpus, dataset := range datasets {
		fixture, loadErr := loadFixture(dataset)
		if loadErr != nil || !m8QualificationFixtureV1(fixture) || (corpus == localHNSWFinalQualificationCorpus250KV1) != (fixture.Vectors == 250000) {
			return errors.New("local HNSW final qualification fixture identity")
		}
		manifest := filepath.Join(dataset, "fixture_manifest.json")
		digest, hashErr := localHNSWAttributionRegularFileSHA256V1(manifest, maxManifestBytes)
		if hashErr != nil {
			return hashErr
		}
		fixtures[corpus] = fixture
		manifestEvidence[corpus] = localHNSWAttributionFileInputV1{Path: manifest, SHA256: digest}
	}

	calibration, calibrationSHA, err := loadLocalHNSWQuerySplitV1(calibrationPath)
	if err != nil || calibrationSHA != localHNSWAttributionCalibrationSHA256V1 {
		return errors.New("local HNSW final qualification calibration split identity")
	}
	holdout, holdoutSHA, err := loadLocalHNSWQuerySplitV1(holdoutPath)
	if err != nil || holdoutSHA != localHNSWAttributionHoldoutSHA256V1 {
		return errors.New("local HNSW final qualification holdout split identity")
	}
	anchor250, ok := m8QualificationTruthCacheAnchorV1(fixtures[localHNSWFinalQualificationCorpus250KV1])
	if !ok {
		return errors.New("local HNSW final qualification 250k truth anchor")
	}
	if _, err := localHNSWFinalQualificationUnionV1(calibration, holdout, fixtures[localHNSWFinalQualificationCorpus250KV1], anchor250.ArtifactSHA256); err != nil {
		return err
	}

	roots := localHNSWFinalQualificationRootsV1{Baseline250K: baseline250, Candidate250K: candidate250, Baseline100K: baseline100, Candidate100K: candidate100}
	descriptorEvidence := map[string][2]localHNSWAttributionFileInputV1{}
	descriptors := map[string][2]m3VariantDescriptorV1{}
	for _, corpus := range []string{localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationCorpus100KV1} {
		baselineRoot, candidateRoot := roots.database(corpus, localHNSWFinalQualificationBaselineV1), roots.database(corpus, localHNSWFinalQualificationCandidateV1)
		baselineDescriptor, readErr := m3ReadVariantDescriptorV1(baselineRoot)
		if readErr != nil {
			return readErr
		}
		candidateDescriptor, readErr := m3ReadVariantDescriptorV1(candidateRoot)
		if readErr != nil {
			return readErr
		}
		if err := localHNSWFinalQualificationDescriptorsV1(fixtures[corpus], baselineDescriptor, candidateDescriptor, config{baseSHA: baseSHA, headSHA: headSHA}, executableSHA); err != nil {
			return err
		}
		var evidence [2]localHNSWAttributionFileInputV1
		for i, root := range []string{baselineRoot, candidateRoot} {
			path := filepath.Join(root, m3VariantDescriptorFileV1)
			digest, hashErr := localHNSWAttributionRegularFileSHA256V1(path, m3VariantDescriptorMaxBytesV1)
			if hashErr != nil {
				return hashErr
			}
			evidence[i] = localHNSWAttributionFileInputV1{Path: path, SHA256: digest}
		}
		descriptorEvidence[corpus] = evidence
		descriptors[corpus] = [2]m3VariantDescriptorV1{baselineDescriptor, candidateDescriptor}
	}

	inputs := map[string]localHNSWFinalQualificationCorpusInputV1{}
	corpusEvidence := make([]localHNSWFinalQualificationCorpusEvidenceV1, 0, 2)
	truthRoots := map[string]string{localHNSWFinalQualificationCorpus250KV1: truth250, localHNSWFinalQualificationCorpus100KV1: truth100}
	for _, corpus := range []string{localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationCorpus100KV1} {
		fixture := fixtures[corpus]
		anchor, ok := m8QualificationTruthCacheAnchorV1(fixture)
		if !ok {
			return errors.New("local HNSW final qualification truth anchor")
		}
		truthPath := m8TruthCacheArtifactPathV1(truthRoots[corpus], anchor.Identity)
		if err := localHNSWAttributionMatchFileSHA256V1(truthPath, m8ProfileArtifactMaxBytesV1, anchor.ArtifactSHA256); err != nil {
			return err
		}
		truth, artifactSHA, readErr := m8ReadTruthCacheV1(truthPath, fixture, fixture.Queries, localHNSWFinalTopKV1, uint64(fixture.Vectors), anchor.ArtifactSHA256)
		if readErr != nil {
			return readErr
		}
		truthSHA, hashErr := m8TruthContentSHA256V1(truth)
		if hashErr != nil || artifactSHA != anchor.ArtifactSHA256 || truthSHA != anchor.TruthSHA256 {
			return errors.New("local HNSW final qualification truth identity")
		}
		vectors, queries := fixtureData(fixture)
		inputs[corpus] = localHNSWFinalQualificationCorpusInputV1{Fixture: fixture, Dataset: datasets[corpus], TruthCache: truthRoots[corpus], TruthCacheSHA256: artifactSHA, Truth: truth, Vectors: vectors, Queries: queries}
		descriptors := descriptorEvidence[corpus]
		corpusEvidence = append(corpusEvidence, localHNSWFinalQualificationCorpusEvidenceV1{Corpus: corpus, Fixture: fixture, DatasetManifest: manifestEvidence[corpus], TruthCache: localHNSWAttributionFileInputV1{Path: truthPath, SHA256: artifactSHA}, TruthSHA256: truthSHA, BaselineDB: roots.database(corpus, localHNSWFinalQualificationBaselineV1), CandidateDB: roots.database(corpus, localHNSWFinalQualificationCandidateV1), BaselineDescriptor: descriptors[0], CandidateDescriptor: descriptors[1]})
	}

	base := config{out: artifacts, baseSHA: baseSHA, headSHA: headSHA, sourceCheckout: sourceCheckout, command: []string{executable}}
	children, err := localHNSWFinalQualificationInvokeV1(base, inputs, roots, runM8ProductionSingleVariantV1, stdout)
	if err != nil {
		return err
	}
	for _, child := range children {
		descriptor := descriptors[child.Corpus][0]
		if child.Variant == localHNSWFinalQualificationCandidateV1 {
			descriptor = descriptors[child.Corpus][1]
		}
		if child.SourceIdentitySHA256 != descriptor.Source.Checksum || child.VariantIdentitySHA256 != descriptor.BuildIdentityDigest {
			return errors.New("local HNSW final qualification child descriptor drift")
		}
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW final qualification source changed")
	}
	if digest, hashErr := m8BenchmarkExecutableSHA256V1(executable); hashErr != nil || digest != executableSHA {
		return errors.New("local HNSW final qualification executable changed")
	}
	for _, corpus := range corpusEvidence {
		for _, file := range []localHNSWAttributionFileInputV1{corpus.DatasetManifest, corpus.TruthCache, corpus.BaselineDescriptor, corpus.CandidateDescriptor} {
			if err := localHNSWAttributionMatchFileSHA256V1(file.Path, m8ProfileArtifactMaxBytesV1, file.SHA256); err != nil {
				return fmt.Errorf("local HNSW final qualification input changed: %w", err)
			}
		}
	}
	for _, file := range []localHNSWAttributionFileInputV1{{Path: calibrationPath, SHA256: calibrationSHA}, {Path: holdoutPath, SHA256: holdoutSHA}} {
		if err := localHNSWAttributionMatchFileSHA256V1(file.Path, localHNSWQuerySplitMaxBytesV1, file.SHA256); err != nil {
			return fmt.Errorf("local HNSW final qualification split changed: %w", err)
		}
	}
	for _, file := range []localHNSWAttributionFileInputV1{{Path: m18Curve, SHA256: curveSHA}, {Path: m18Timing, SHA256: localHNSWFinalQualificationM18TimingSHA256V1}} {
		if err := localHNSWFinalQualificationM18EvidenceV1(file); err != nil {
			return err
		}
	}
	report := localHNSWFinalQualificationReportV1{
		Schema: localHNSWFinalQualificationSchemaV1, ResultKind: "local_hnsw_final_qualification_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-final-qualification", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA},
		Inputs:     localHNSWFinalQualificationInputsEvidenceV1{Corpora: corpusEvidence, Calibration: localHNSWAttributionFileInputV1{Path: calibrationPath, SHA256: calibrationSHA}, CalibrationRows: len(calibration.Ordinals), Holdout: localHNSWAttributionFileInputV1{Path: holdoutPath, SHA256: holdoutSHA}, HoldoutRows: len(holdout.Ordinals), QueryUnionRows: localHNSWFinalQueryCountV1, ApprovalSHA: approvalSHA, Artifacts: artifacts, M18Curve: localHNSWAttributionFileInputV1{Path: m18Curve, SHA256: curveSHA}, M18Timing: localHNSWAttributionFileInputV1{Path: m18Timing, SHA256: localHNSWFinalQualificationM18TimingSHA256V1}},
		Children:   children, Disposition: "pass", Limitations: []string{"single-host loopback production topology", "final 250k holdout and independent 100k control only; no external-system comparison"},
	}
	if err := localHNSWFinalQualificationReportValidV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "report=%s children=%d disposition=%s\n", out, len(children), report.Disposition)
	return err
}

func localHNSWFinalQualificationM18EvidenceV1(file localHNSWAttributionFileInputV1) error {
	if err := localHNSWAttributionMatchFileSHA256V1(file.Path, m8QualificationMatrixMaxBytesV1, file.SHA256); err != nil {
		return fmt.Errorf("local HNSW final qualification M18 evidence changed: %w", err)
	}
	return nil
}

func localHNSWFinalQualificationApprovalV1(checkout, approvalSHA, headSHA string) error {
	commitRaw, err := exec.Command("git", "-C", checkout, "rev-parse", "--verify", approvalSHA+"^{commit}").Output()
	if err != nil || strings.TrimSpace(string(commitRaw)) != approvalSHA || exec.Command("git", "-C", checkout, "merge-base", "--is-ancestor", approvalSHA, headSHA).Run() != nil {
		return errors.New("local HNSW final qualification approval is not an exact ancestor commit")
	}
	parentsRaw, err := exec.Command("git", "-C", checkout, "show", "-s", "--format=%P", approvalSHA).Output()
	if err != nil || len(strings.Fields(string(parentsRaw))) != 2 {
		return errors.New("local HNSW final qualification approval is not the #4115 merge commit")
	}
	subjectRaw, err := exec.Command("git", "-C", checkout, "show", "-s", "--format=%s", approvalSHA).Output()
	subject := strings.TrimSpace(string(subjectRaw))
	if err != nil || !strings.HasPrefix(subject, fmt.Sprintf("Merge pull request #%d from ", localHNSWFinalApprovalPullRequestV1)) {
		return errors.New("local HNSW final qualification approval commit does not identify PR #4115")
	}
	firstParentRaw, err := exec.Command("git", "-C", checkout, "rev-list", "--first-parent", headSHA).Output()
	if err != nil || !slices.Contains(strings.Fields(string(firstParentRaw)), approvalSHA) {
		return errors.New("local HNSW final qualification approval is not on the final first-parent chain")
	}
	diff := exec.Command("git", "-C", checkout, "diff", "--quiet", approvalSHA+"^1", approvalSHA, "--", localHNSWFinalApprovalEvidencePathV1).Run()
	exit, changed := diff.(*exec.ExitError)
	if !changed || exit.ExitCode() != 1 {
		return errors.New("local HNSW final qualification approval did not introduce the #4093 evidence")
	}
	approvalTree, err := exec.Command("git", "-C", checkout, "rev-parse", "--verify", approvalSHA+":"+localHNSWFinalApprovalEvidencePathV1).Output()
	if err != nil {
		return errors.New("local HNSW final qualification approval evidence tree is missing")
	}
	headTree, err := exec.Command("git", "-C", checkout, "rev-parse", "--verify", headSHA+":"+localHNSWFinalApprovalEvidencePathV1).Output()
	if err != nil || strings.TrimSpace(string(approvalTree)) != strings.TrimSpace(string(headTree)) {
		return errors.New("local HNSW final qualification #4093 evidence changed after approval")
	}
	treeType, err := exec.Command("git", "-C", checkout, "cat-file", "-t", strings.TrimSpace(string(approvalTree))).Output()
	if err != nil || strings.TrimSpace(string(treeType)) != "tree" {
		return errors.New("local HNSW final qualification approval evidence is not a tree")
	}
	return nil
}

func localHNSWFinalQualificationDescriptorsV1(fixture fixtureManifest, baseline, candidate m3VariantDescriptorV1, cfg config, executableSHA string) error {
	if !m8QualificationFixtureV1(fixture) || !m8QualificationM3BuildCapsV1(baseline, fixture) || !m8QualificationM3BuildCapsV1(candidate, fixture) || baseline.PartitionHNSWM != 16 || candidate.PartitionHNSWM != 18 || baseline.FixtureChecksum != fixture.Checksum || candidate.FixtureChecksum != fixture.Checksum || baseline.SourceRows != uint64(fixture.Vectors) || candidate.SourceRows != uint64(fixture.Vectors) || baseline.Partitions != 16 || candidate.Partitions != 16 || baseline.OverlapRatio != .2 || candidate.OverlapRatio != .2 || !m8QualificationVariantBackendV1(baseline, fixture) || !m8QualificationVariantBackendV1(candidate, fixture) {
		return errors.New("local HNSW final qualification retained definition")
	}
	if err := m8ValidateRetainedM3ProvenanceV1(cfg, baseline, executableSHA); err != nil {
		return err
	}
	if err := m8ValidateRetainedM3ProvenanceV1(cfg, candidate, executableSHA); err != nil {
		return err
	}
	if !m8SHA256V1(baseline.SourceOrdinalDigest) || baseline.SourceOrdinalDigest != candidate.SourceOrdinalDigest || baseline.Source != candidate.Source || baseline.VariantID != candidate.VariantID || baseline.AssignmentBasis != candidate.AssignmentBasis || baseline.ArtifactSHA256 != candidate.ArtifactSHA256 || baseline.GraphArtifactSHA256 != candidate.GraphArtifactSHA256 || baseline.GraphBuildSHA256 != candidate.GraphBuildSHA256 || baseline.RouterConfig != candidate.RouterConfig || baseline.IndexDefinitionDigest != candidate.IndexDefinitionDigest || baseline.RouterAssetChecksum != candidate.RouterAssetChecksum || baseline.RouterModelDigest != candidate.RouterModelDigest || baseline.SourceGeneration != candidate.SourceGeneration || baseline.SourceChecksum != candidate.SourceChecksum || baseline.SourceSchemaHash != candidate.SourceSchemaHash || baseline.SourceRows != candidate.SourceRows {
		return errors.New("local HNSW final qualification retained source drift")
	}
	return nil
}
