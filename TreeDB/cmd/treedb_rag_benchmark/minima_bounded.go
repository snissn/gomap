package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const minimaBoundedManifestSchema = "treedb_rag_minima_manifest/v2"
const minimaBoundedArtifactSchema = "treedb_rag_application/minima_diagnostic_v1"
const minimaNativeProofSchema = "treedb_minima_native_path_proof/v1"

type minimaNativePathProof struct {
	Schema       string             `json:"schema"`
	Strategy     string             `json:"strategy"`
	Availability string             `json:"availability"`
	Reason       string             `json:"reason,omitempty"`
	Counters     map[string]*uint64 `json:"counters"`
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
		for _, segment := range resource.Segments {
			if segment.Baseline.PeakRSS != nil || segment.End.PeakRSS != nil {
				return fmt.Errorf("minima peak RSS: endpoint measurements require aggregate availability")
			}
		}
		if (resource.Baseline != nil && resource.Baseline.PeakRSS != nil) || (resource.End != nil && resource.End.PeakRSS != nil) {
			return fmt.Errorf("minima peak RSS: envelope measurements require aggregate availability")
		}
		return nil
	}
	if resource.PeakRSSScope != "max_process_lifetime_highwater_through_segment_endpoints" {
		return fmt.Errorf("minima peak RSS: invalid scope")
	}
	var maximum int64
	available := len(resource.Segments) > 0
	for _, segment := range resource.Segments {
		for _, p := range []*minimaProcessPeakRSS{segment.Baseline.PeakRSS, segment.End.PeakRSS} {
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
		start, end := segment.Baseline.PeakRSS, segment.End.PeakRSS
		if start != nil && end != nil && start.Availability == "measured" && end.Availability == "measured" && *end.Bytes < *start.Bytes {
			return fmt.Errorf("minima peak RSS: process lifetime highwater decreased")
		}
	}
	if len(resource.Segments) > 0 {
		for _, pair := range []struct {
			envelope *minimaRawResourceSnapshot
			endpoint minimaRawResourceSnapshot
		}{
			{resource.Baseline, resource.Segments[0].Baseline},
			{resource.End, resource.Segments[len(resource.Segments)-1].End},
		} {
			if pair.envelope != nil && (pair.envelope.PID != pair.endpoint.PID || pair.envelope.LinuxProcessIdentity != pair.endpoint.LinuxProcessIdentity || !reflect.DeepEqual(pair.envelope.PeakRSS, pair.endpoint.PeakRSS)) {
				return fmt.Errorf("minima peak RSS: envelope does not match segment endpoint")
			}
		}
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

func validateMinimaPeakRSSLifetimes(raw minimaRawBackendEvidence) error {
	r := raw.ResourceMeasurement
	if err := validateMinimaPeakRSS(r); err != nil {
		return err
	}
	if r.PeakRSSAvailability == "" {
		return nil
	} // Historical no-peak artifacts.
	restart := raw.RestartBoundary
	for i, segment := range r.Segments {
		start, end := segment.Baseline.PeakRSS, segment.End.PeakRSS
		if (start == nil || start.Availability != "measured") && (end == nil || end.Availability != "measured") {
			continue
		}
		pid, identity := segment.Baseline.PID, segment.Baseline.LinuxProcessIdentity
		if (restart.OldPID != 0 || restart.NewPID != 0) && (!restart.Verified || !restart.PIDChanged || restart.OldPID == restart.NewPID) {
			return fmt.Errorf("minima peak RSS: service restart is not verified")
		}
		if restart.Verified {
			if len(r.Segments) != 2 {
				return fmt.Errorf("minima peak RSS: restart requires ordered old/new resource segments")
			}
			pid, identity = restart.OldPID, restart.OldLinuxProcessIdentity
			if i == 1 {
				pid, identity = restart.NewPID, restart.NewLinuxProcessIdentity
			}
		} else if len(r.Segments) != 1 {
			return fmt.Errorf("minima peak RSS: multiple lifetimes without verified restart")
		}
		prefix := fmt.Sprintf("%d:", pid)
		ticks, err := strconv.ParseUint(strings.TrimPrefix(identity, prefix), 10, 64)
		if pid <= 0 || !strings.HasPrefix(identity, prefix) || err != nil || ticks == 0 ||
			segment.Baseline.PID != pid || segment.Baseline.LinuxProcessIdentity != identity ||
			(segment.End.PID != 0 && segment.End.PID != pid) || (segment.End.LinuxProcessIdentity != "" && segment.End.LinuxProcessIdentity != identity) {
			return fmt.Errorf("minima peak RSS: segment %d does not match service process lifetime", i)
		}
		for _, peak := range []*minimaProcessPeakRSS{start, end} {
			if peak != nil && peak.Availability == "measured" && (*peak.PID != pid || peak.ProcessIdentity != identity) {
				return fmt.Errorf("minima peak RSS: segment %d endpoint does not match service process lifetime", i)
			}
		}
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
	if err := validateMinimaBackendLifecycle(artifact.Backends[0], &artifact.Manifest); err != nil {
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
	}
	return validateMinimaRawEvidence(artifact, map[string]minimaBackendEvidence{"treedb": artifact.Backends[0]})
}
