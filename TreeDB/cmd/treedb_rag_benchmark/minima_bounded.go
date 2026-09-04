package main

import "fmt"

const minimaBoundedManifestSchema = "treedb_rag_minima_manifest/v2"
const minimaBoundedArtifactSchema = "treedb_rag_application/minima_diagnostic_v1"
const minimaNativeProofSchema = "treedb_minima_native_path_proof/v1"

// These counters describe the full lifecycle. Unavailable is never zero. M1-M4
// must supply measured counters and per-scenario route evidence before a native
// qualification schema can be enabled; this diagnostic schema cannot qualify.
var minimaNativeCounterNames = []string{
	"indexed_json_reads", "native_runtime_dispatches", "primary_document_scans",
	"ann_base_searches", "base_candidates", "overlay_searches", "overlay_candidates",
	"typed_exact_searches", "retained_payload_decodes", "documents_fetched",
	"scalar_candidates", "text_index_updates", "copied_bytes", "fold_debt_rows",
}

type minimaNativePathProof struct {
	Schema       string             `json:"schema"`
	Strategy     string             `json:"strategy"`
	Availability string             `json:"availability"`
	Reason       string             `json:"reason,omitempty"`
	Counters     map[string]*uint64 `json:"counters"`
}

func validateMinimaNativePathProof(proof *minimaNativePathProof) error {
	if proof == nil || proof.Schema != minimaNativeProofSchema || proof.Strategy != "column_graph" || proof.Availability != "measured" {
		return fmt.Errorf("minima native path proof: measured column_graph counters unavailable")
	}
	for _, key := range minimaNativeCounterNames {
		if proof.Counters[key] == nil {
			return fmt.Errorf("minima native path proof: %s unavailable", key)
		}
	}
	for _, key := range []string{"indexed_json_reads", "native_runtime_dispatches", "primary_document_scans"} {
		if *proof.Counters[key] != 0 {
			return fmt.Errorf("minima native path proof: forbidden %s", key)
		}
	}
	for _, key := range []string{"ann_base_searches", "base_candidates", "overlay_searches", "overlay_candidates"} {
		if *proof.Counters[key] == 0 {
			return fmt.Errorf("minima native path proof: no positive %s", key)
		}
	}
	return nil
}

func buildMinimaBoundedManifest(total int) (minimaManifest, error) {
	if total != 50000 && total != 250000 {
		return minimaManifest{}, fmt.Errorf("minima bounded total rows must be 50000 or 250000")
	}
	return buildMinimaManifestForRows(total), nil
}

func validateMinimaPeakRSS(resource minimaRawResourceMeasurement) error {
	// Old artifacts did not capture high-water RSS and remain readable.
	if resource.PeakRSSAvailability == "" && resource.PeakRSSBytes == nil && resource.PeakRSSScope == "" {
		return nil
	}
	if resource.PeakRSSScope != "max_process_lifetime_highwater_through_segment_endpoints" {
		return fmt.Errorf("minima peak RSS: invalid scope")
	}
	var maximum int64
	available := len(resource.Segments) > 0
	for _, segment := range resource.Segments {
		p := segment.End.PeakRSS
		if p == nil || p.Availability == "unavailable" {
			if p != nil && p.Bytes != nil {
				return fmt.Errorf("minima peak RSS: unavailable is not zero")
			}
			available = false
			continue
		}
		if p.Availability != "measured" || p.Bytes == nil || *p.Bytes <= 0 || p.PID == nil || *p.PID <= 0 || p.ProcessIdentity == "" || p.Source != "/proc/<pid>/status:VmHWM" || p.Scope != "process_lifetime_through_sample" {
			return fmt.Errorf("minima peak RSS: invalid process measurement")
		}
		maximum = max(maximum, *p.Bytes)
	}
	if available {
		if resource.PeakRSSAvailability != "measured" || resource.PeakRSSBytes == nil || *resource.PeakRSSBytes != maximum {
			return fmt.Errorf("minima peak RSS: expected max across process lifetimes, not sum")
		}
	} else if resource.PeakRSSAvailability != "unavailable" || resource.PeakRSSBytes != nil {
		return fmt.Errorf("minima peak RSS: incomplete process evidence")
	}
	return nil
}

func validateMinimaCompletedBounded(artifact *minimaArtifact, raw minimaRawBackendEvidence) error {
	if artifact.NativePathProof == nil || artifact.NativePathProof.Strategy != "native_runtime" || artifact.Backends[0].Configuration["vector_strategy"] != "native_runtime" {
		return fmt.Errorf("minima bounded diagnostic: completed strategy must be native_runtime until M4")
	}
	if raw.ResourceMeasurement.PeakRSSAvailability == "" {
		return fmt.Errorf("minima bounded diagnostic: missing peak RSS availability")
	}
	if err := validateMinimaResourceMeasurement("treedb", raw.ResourceMeasurement); err != nil {
		return err
	}
	specs, queries := minimaScenarioMap(&artifact.Manifest), minimaQueryMap(&artifact.Manifest)
	if len(artifact.Scenarios) != len(specs) || len(raw.NativeRouteResponses) != len(specs) {
		return fmt.Errorf("minima bounded diagnostic: missing scenario route evidence")
	}
	seen := make(map[string]bool, len(specs))
	for _, row := range artifact.Scenarios {
		spec, ok := specs[row.Scenario]
		if !ok || seen[row.Scenario] || row.Backend != "treedb" {
			return fmt.Errorf("minima bounded diagnostic: duplicate or unknown scenario")
		}
		seen[row.Scenario] = true
		if err := validateMinimaScenarioEvidence(row, spec, queries[row.Scenario]); err != nil {
			return fmt.Errorf("minima bounded diagnostic: %s: %w", row.Scenario, err)
		}
		if err := validateMinimaNativeRouteResponse(raw.NativeRouteResponses, row); err != nil {
			return err
		}
	}
	return nil
}
