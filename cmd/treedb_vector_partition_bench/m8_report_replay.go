package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// These are required external trust inputs, not another retained schema. Fixture
// and command pins belong to the preregistered plan; receipt pins are frozen at
// publication. Taking all pins from an untrusted report is not verification.
type m8ReportReplayPinsV1 struct {
	Report, Fixture, Command, Executable, Variant, TruthArtifact, TruthContent string
}

func runReplayM8ReportV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench replay-m8-report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var root, path string
	var pins m8ReportReplayPinsV1
	fs.StringVar(&root, "root", "", "canonical retained root containing source, executable and all artifacts")
	fs.StringVar(&path, "report", "", "canonical existing M8 child report JSON path below root")
	fs.StringVar(&pins.Report, "report-sha256", "", "independently frozen report file SHA256")
	fs.StringVar(&pins.Fixture, "fixture-sha256", "", "preregistered SHA256 of json.Marshal(fixtureManifest)")
	fs.StringVar(&pins.Command, "command-sha256", "", "preregistered SHA256 of json.Marshal(report.Command)")
	fs.StringVar(&pins.Executable, "executable-sha256", "", "frozen benchmark executable SHA256")
	fs.StringVar(&pins.Variant, "variant-descriptor-sha256", "", "frozen SHA256 of canonical full m3VariantDescriptorJSONV1 bytes")
	fs.StringVar(&pins.TruthArtifact, "truth-artifact-sha256", "", "frozen canonical truth-cache file SHA256")
	fs.StringVar(&pins.TruthContent, "truth-content-sha256", "", "frozen canonical truth semantic SHA256")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || root == "" || path == "" || !pins.valid() {
		return errors.New("replay-m8-report requires canonical -root, -report and all seven external SHA256 pins")
	}
	canonicalRoot, err := m8CanonicalPathV1(root)
	if err != nil || canonicalRoot != root {
		return errors.New("replay root is not canonical")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return errors.New("replay root is not a directory")
	}
	canonicalPath, err := m8QualificationContainedPathV1(root, path, "replay report")
	if err != nil || canonicalPath != path {
		return errors.New("replay report is not canonical and contained in root")
	}
	raw, err := readBoundedRegularFileV1(path, m8QualificationMatrixMaxBytesV1)
	if err != nil {
		return fmt.Errorf("read replay report: %w", err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != pins.Report {
		return errors.New("replay report differs from frozen digest")
	}
	var report m8ProductionReportV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return fmt.Errorf("decode replay report: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("replay report contains trailing JSON")
	}
	// Check all cheap frozen identities before profiles, external commands, truth
	// decoding or corpus allocation. The loaded fixture is also compared with the
	// report by the retained-variant verifier below, before attribution replay.
	if err := m8ReportReplayIdentityV1(report, pins); err != nil {
		return err
	}
	if _, ok := m8QualificationProfilesV1(root, report.Profiles); !ok || report.Profiles.Status != "captured_production_query_and_fault_boundary" {
		return errors.New("replay requires contained captured production profiles")
	}
	if err := validateM8ProductionReportV1(report, m8QualificationResourceCapsV1()); err != nil {
		return fmt.Errorf("validate replay report: %w", err)
	}
	if !m8QualificationCommandWithExecutableV1(root, filepath.Dir(path), report, m8QualificationBenchmarkExecutableV1) {
		return errors.New("replay command, clean source, executable or work admission is not bound")
	}
	anchor := m8QualificationTruthAnchorV1{
		Identity:       m8TruthCacheIdentityV1(report.Dataset, report.Config.TopK),
		ArtifactSHA256: pins.TruthArtifact, TruthSHA256: pins.TruthContent,
	}
	truth, err := m8QualificationReadTruthCacheWithAnchorV1(root, report, anchor)
	if err != nil {
		return fmt.Errorf("replay anchored truth: %w", err)
	}
	transcript, err := m8QualificationMeasurementTranscriptOutcomesV1(root, report, truth)
	if err != nil {
		return fmt.Errorf("replay transcript: %w", err)
	}
	if !m8QualificationResourcesV1(report, report.Dataset, transcript) {
		return errors.New("replay resources do not match retained transcript or fixed caps")
	}
	derived := m8ProductionGateLedgerForReportV1(report)
	// Overlap storage is a cross-variant matrix comparison, not a child replay
	// claim. Preserve its recorded value just as the historical campaign does.
	derived.OverlapStorage = report.GateLedger.OverlapStorage
	if derived != report.GateLedger {
		return errors.New("replay report has a stale derived gate ledger")
	}
	if err := m8QualificationRetainedVariantV1(root, report); err != nil {
		return fmt.Errorf("replay retained variant: %w", err)
	}
	if err := m8QualificationRetainedAttributionV1(root, report, truth, transcript); err != nil {
		return fmt.Errorf("replay retained attribution: %w", err)
	}
	_, err = fmt.Fprintf(stdout, "REPLAY_ACCEPTED_NOT_QUALIFICATION report_sha256=%s rows=%d\n", pins.Report, len(report.Rows))
	return err
}

func (p m8ReportReplayPinsV1) valid() bool {
	for _, value := range []string{p.Report, p.Fixture, p.Command, p.Executable, p.Variant, p.TruthArtifact, p.TruthContent} {
		if !m8QualificationSHA256V1(value) {
			return false
		}
	}
	return true
}

func m8ReportReplayIdentityV1(report m8ProductionReportV1, pins m8ReportReplayPinsV1) error {
	fixture, err := json.Marshal(report.Dataset)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(fixture)) != pins.Fixture {
		return errors.New("replay fixture differs from preregistered digest")
	}
	command, err := json.Marshal(report.Command)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(command)) != pins.Command {
		return errors.New("replay command differs from preregistered digest")
	}
	if report.Variant == nil {
		return errors.New("replay requires a retained variant descriptor")
	}
	descriptor, err := m3VariantDescriptorJSONV1(*report.Variant)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(descriptor)) != pins.Variant {
		return errors.New("replay variant descriptor differs from frozen digest")
	}
	if report.Dirty || report.Variant.BuildDirty ||
		!m8QualificationGitSHAV1(report.BaseSHA) || !m8QualificationGitSHAV1(report.HeadSHA) ||
		report.Variant.BaseSHA != report.BaseSHA || report.Variant.HeadSHA != report.HeadSHA ||
		report.ExecutableSHA256 != pins.Executable || report.Variant.ExecutableSHA256 != pins.Executable ||
		report.TruthCache.ArtifactSHA256 != pins.TruthArtifact {
		return errors.New("replay source, executable, variant or truth identity differs from frozen pins")
	}
	return nil
}
