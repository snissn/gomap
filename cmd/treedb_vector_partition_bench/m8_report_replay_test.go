package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayM8ReportRequiresFrozenPinsV1(t *testing.T) {
	err := run([]string{"replay-m8-report"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "replay-m8-report requires") {
		t.Fatalf("missing replay pin admission: %v", err)
	}
}

func testM8ReportReplayPinsV1(t *testing.T) (m8ProductionReportV1, m8ReportReplayPinsV1) {
	t.Helper()
	fixture := m8QualificationFixturesV1[0]
	fixture.QueryOrdinalOffset = 1000
	report := m8ProductionReportV1{
		Dataset: fixture, Command: []string{"/not-executed/benchmark", "-m8-quality-diagnostics"},
		BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), ExecutableSHA256: strings.Repeat("c", 64),
		Variant:    &m3VariantDescriptorV1{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), ExecutableSHA256: strings.Repeat("c", 64), ArtifactSHA256: strings.Repeat("d", 64)},
		TruthCache: m8TruthCacheEvidenceV1{ArtifactSHA256: strings.Repeat("e", 64)},
	}
	fixtureRaw, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	commandRaw, err := json.Marshal(report.Command)
	if err != nil {
		t.Fatal(err)
	}
	descriptorRaw, err := m3VariantDescriptorJSONV1(*report.Variant)
	if err != nil {
		t.Fatal(err)
	}
	return report, m8ReportReplayPinsV1{Report: strings.Repeat("f", 64), Fixture: fmt.Sprintf("%x", sha256.Sum256(fixtureRaw)), Command: fmt.Sprintf("%x", sha256.Sum256(commandRaw)), Executable: report.ExecutableSHA256, Variant: fmt.Sprintf("%x", sha256.Sum256(descriptorRaw)), TruthArtifact: report.TruthCache.ArtifactSHA256, TruthContent: strings.Repeat("0", 64)}
}

func testM8ReportReplayArgsV1(root, report string, p m8ReportReplayPinsV1) []string {
	return []string{"replay-m8-report", "-root", root, "-report", report,
		"-report-sha256", p.Report, "-fixture-sha256", p.Fixture, "-command-sha256", p.Command,
		"-executable-sha256", p.Executable, "-variant-descriptor-sha256", p.Variant,
		"-truth-artifact-sha256", p.TruthArtifact, "-truth-content-sha256", p.TruthContent}
}

func TestReplayM8ReportFrozenIdentityV1(t *testing.T) {
	report, pins := testM8ReportReplayPinsV1(t)
	if !pins.valid() || m8ReportReplayIdentityV1(report, pins) != nil {
		t.Fatal("matching frozen identities rejected")
	}
	for name, mutate := range map[string]func(*m8ProductionReportV1){
		"query_offset":       func(r *m8ProductionReportV1) { r.Dataset.QueryOrdinalOffset++ },
		"corpus_seed":        func(r *m8ProductionReportV1) { r.Dataset.Seed++ },
		"command":            func(r *m8ProductionReportV1) { r.Command = append(r.Command, "-ef-search", "512") },
		"dirty_source":       func(r *m8ProductionReportV1) { r.Dirty = true },
		"dirty_build":        func(r *m8ProductionReportV1) { r.Variant.BuildDirty = true },
		"source_head":        func(r *m8ProductionReportV1) { r.HeadSHA = strings.Repeat("1", 40) },
		"source_base":        func(r *m8ProductionReportV1) { r.BaseSHA = strings.Repeat("1", 40) },
		"executable":         func(r *m8ProductionReportV1) { r.ExecutableSHA256 = strings.Repeat("1", 64) },
		"variant_executable": func(r *m8ProductionReportV1) { r.Variant.ExecutableSHA256 = strings.Repeat("1", 64) },
		"variant_artifact":   func(r *m8ProductionReportV1) { r.Variant.ArtifactSHA256 = strings.Repeat("1", 64) },
		"router_model":       func(r *m8ProductionReportV1) { r.Variant.RouterModelDigest = strings.Repeat("1", 64) },
		"router_config":      func(r *m8ProductionReportV1) { r.Variant.RouterConfig.MaxDepth++ },
		"partition_policy":   func(r *m8ProductionReportV1) { r.Variant.OverlapPolicy = "changed" },
		"missing_variant":    func(r *m8ProductionReportV1) { r.Variant = nil },
		"truth_artifact":     func(r *m8ProductionReportV1) { r.TruthCache.ArtifactSHA256 = strings.Repeat("1", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			changed, _ := testM8ReportReplayPinsV1(t)
			mutate(&changed)
			if err := m8ReportReplayIdentityV1(changed, pins); err == nil {
				t.Fatal("accepted drift from externally frozen identity")
			}
		})
	}
}

func TestReplayM8ReportAdmissionBeforeIOV1(t *testing.T) {
	_, pins := testM8ReportReplayPinsV1(t)
	args := testM8ReportReplayArgsV1("/missing-root", "/missing-report", pins)
	for i := 5; i < len(args); i += 2 {
		t.Run(args[i], func(t *testing.T) {
			missing := append([]string(nil), args[:i]...)
			missing = append(missing, args[i+2:]...)
			if err := run(missing, io.Discard); err == nil || !strings.Contains(err.Error(), "requires") {
				t.Fatalf("missing pin reached filesystem: %v", err)
			}
			malformed := append([]string(nil), args...)
			malformed[i+1] = "not-sha256"
			if err := run(malformed, io.Discard); err == nil || !strings.Contains(err.Error(), "requires") {
				t.Fatalf("malformed pin reached filesystem: %v", err)
			}
		})
	}
}

func TestReplayM8ReportBoundedReceiptV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	report, pins := testM8ReportReplayPinsV1(t)
	path := filepath.Join(root, "report.json")
	write := func(report m8ProductionReportV1, suffix string) []string {
		t.Helper()
		raw, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, suffix...)
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		p := pins
		p.Report = fmt.Sprintf("%x", sha256.Sum256(raw))
		return testM8ReportReplayArgsV1(root, path, p)
	}
	reject := func(args []string, want string) {
		t.Helper()
		var output strings.Builder
		if err := run(args, &output); err == nil || !strings.Contains(err.Error(), want) || output.Len() != 0 {
			t.Fatalf("want %q, err=%v output=%q", want, err, output.String())
		}
	}
	// This intentionally incomplete receipt must stop before trying to execute
	// the nonexistent benchmark or loading any corpus. Matching hashes alone
	// are never accepted as an artifact replay.
	reject(write(report, ""), "contained captured production profiles")
	changed := report
	changed.Dataset.QueryOrdinalOffset++
	reject(write(changed, ""), "fixture differs")
	changed = report
	changed.Command = append([]string(nil), report.Command...)
	changed.Command[0] = "/untrusted-program"
	reject(write(changed, ""), "command differs")
	reject(write(report, " {}"), "trailing JSON")
	args := write(report, "")
	args[6] = strings.Repeat("0", 64)
	reject(args, "report differs")
	args = write(report, "")
	args[2] = root + "/."
	reject(args, "root is not canonical")
	outside, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outsidePath := filepath.Join(outside, "outside.json")
	if err := os.WriteFile(outsidePath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "escaping.json")
	t.Run("escaping_symlink", func(t *testing.T) {
		if err := os.Symlink(outsidePath, link); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		if err := run(testM8ReportReplayArgsV1(root, link, pins), io.Discard); err == nil || !strings.Contains(err.Error(), "contained in root") {
			t.Fatalf("escaping symlink accepted: %v", err)
		}
	})
	reject(testM8ReportReplayArgsV1(root, outsidePath, pins), "contained in root")
	file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	err = file.Truncate(m8QualificationMatrixMaxBytesV1 + 1)
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("oversized receipt setup: %v %v", err, closeErr)
	}
	reject(testM8ReportReplayArgsV1(root, path, pins), "invalid byte length")
}
