package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func readMinimaArtifact(path string) (minimaArtifact, error) {
	var artifact minimaArtifact
	raw, err := os.ReadFile(path)
	if err != nil {
		return artifact, err
	}
	if err := json.Unmarshal(raw, &artifact); err != nil {
		return artifact, err
	}
	return artifact, nil
}

func readMinimaBackendEvidence(path, backend string) (minimaArtifact, error) {
	artifact, err := readMinimaArtifact(path)
	if err != nil {
		return artifact, fmt.Errorf("decode %s evidence: %w", backend, err)
	}
	if artifact.State != "partial" || artifact.Passing || artifact.Recommendation != "not_evaluated" {
		return artifact, fmt.Errorf("%s evidence is not fail-closed partial evidence", backend)
	}
	if err := validateMinimaArtifact(&artifact); err != nil {
		return artifact, fmt.Errorf("validate %s partial evidence: %w", backend, err)
	}
	if len(artifact.Backends) != 1 || artifact.Backends[0].Name != backend {
		return artifact, fmt.Errorf("%s evidence has wrong backend envelope", backend)
	}
	for _, scenario := range artifact.Scenarios {
		if scenario.Backend != backend {
			return artifact, fmt.Errorf("%s evidence contains scenario for %q", backend, scenario.Backend)
		}
	}
	return artifact, nil
}

func combineMinimaEvidence(treedb, qdrant minimaArtifact, recommendation string) minimaArtifact {
	combined := minimaArtifact{
		Schema: minimaArtifactSchema, State: "pass", Passing: true, Manifest: treedb.Manifest,
		Backends:       append(append([]minimaBackendEvidence(nil), treedb.Backends...), qdrant.Backends...),
		Scenarios:      append(append([]minimaScenarioEvidence(nil), treedb.Scenarios...), qdrant.Scenarios...),
		Recommendation: recommendation,
	}
	combined.Failures = append(combined.Failures, treedb.Failures...)
	combined.Failures = append(combined.Failures, qdrant.Failures...)
	if treedb.Manifest.CorpusSHA256 != qdrant.Manifest.CorpusSHA256 ||
		treedb.Manifest.QuerySHA256 != qdrant.Manifest.QuerySHA256 ||
		treedb.Manifest.OperationSHA256 != qdrant.Manifest.OperationSHA256 ||
		treedb.Manifest.ExpectedStateSHA256 != qdrant.Manifest.ExpectedStateSHA256 {
		combined.Failures = append(combined.Failures, "TreeDB and Qdrant evidence embed different manifests")
	}
	if len(combined.Failures) != 0 {
		combined.State, combined.Passing, combined.Recommendation = "partial", false, "not_evaluated"
	}
	return combined
}

func writeMinimaComparisonArtifacts(artifact minimaArtifact, jsonPath, reportPath string) error {
	if jsonPath == "" || reportPath == "" {
		return errors.New("Minima comparison output and report paths are required")
	}
	if err := os.MkdirAll(filepath.Dir(jsonPath), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(reportPath), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(jsonPath, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	var report strings.Builder
	fmt.Fprintf(&report, "# Minima filtered-ANN qualification\n\nState: **%s**  \nRecommendation: **%s**\n\n", artifact.State, artifact.Recommendation)
	if len(artifact.Failures) > 0 {
		report.WriteString("## Failures\n\n")
		for _, failure := range artifact.Failures {
			fmt.Fprintf(&report, "- %s\n", failure)
		}
		report.WriteByte('\n')
	}
	report.WriteString("## Scenario evidence\n\n| Backend | Scenario | Plan | Membership | Recall | Overlap | Search ms |\n|---|---|---|---|---:|---:|---:|\n")
	for _, row := range artifact.Scenarios {
		fmt.Fprintf(&report, "| %s | %s | %s | %s | %.3f | %.3f | %.3f |\n",
			row.Backend, row.Scenario, row.Route.Plan, row.Route.MembershipSource, row.Recall, row.Overlap, row.Timing.SearchMillis)
	}
	report.WriteString("\nThe JSON artifact is authoritative; this report contains no independently inferred pass state.\n")
	return os.WriteFile(reportPath, []byte(report.String()), 0o644)
}

func compareMinimaEvidence(treedbPath, qdrantPath, jsonPath, reportPath, recommendation string) error {
	treedb, err := readMinimaBackendEvidence(treedbPath, "treedb")
	if err != nil {
		return err
	}
	qdrant, err := readMinimaBackendEvidence(qdrantPath, "qdrant")
	if err != nil {
		return err
	}
	combined := combineMinimaEvidence(treedb, qdrant, recommendation)
	validationErr := validateMinimaArtifact(&combined)
	if validationErr != nil {
		combined.State, combined.Passing, combined.Recommendation = "partial", false, "not_evaluated"
		combined.Failures = append(combined.Failures, "fail-closed validator: "+validationErr.Error())
	}
	if err := writeMinimaComparisonArtifacts(combined, jsonPath, reportPath); err != nil {
		return err
	}
	return validationErr
}
