package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func measuredTestPointer[T any](v T) *T { return &v }
func measuredTestWork(pid int) workstats.Snapshot {
	return workstats.Snapshot{SchemaVersion: "treedb-work-v1", Scope: "process", PID: pid, OriginKind: "go_package_init", OriginUnixNano: 1, SnapshotUnixNano: 2,
		Available: workstats.Availability{IndexedJSON: true, Typed: true, RuntimeQuery: true, Replay: true, AttributedScans: true, RowIndexCache: true, Graph: true, Output: true, Fold: true},
		Graph:     workstats.GraphStats{Empty: 1, Exact: 1, HNSW: 1, BaseANNScored: 1, DeltaScored: 1},
		Typed:     workstats.TypedStats{ScalarRows: 1, TextRows: 1}, Output: workstats.OutputWorkStats{GetMany: workstats.OutputStats{OperationStats: workstats.OperationStats{Attempts: 1, Completed: 1}}},
		Fold: workstats.FoldStats{Public: workstats.OperationStats{Attempts: 1, Completed: 1}}}
}
func measuredTestDense(eligible uint64, count int) *documentservice.DenseSearchWork {
	graph := collections.ColumnGraphQueryWork{Available: true, Completed: true, Route: "typed_empty", Filter: collections.ColumnGraphFilterWork{Attempted: true, Completed: true, EligibleRows: eligible},
		Snapshot: collections.ColumnGraphQuerySnapshot{Available: true, SchemaHash: 1, SchemaGeneration: 1, BaseManifest: collections.ColumnGraphManifestWork{Format: "tcs1", Version: 1}, CurrentManifest: collections.ColumnGraphManifestWork{Format: "tcs1", Version: 1}}}
	if eligible > 4096 {
		graph.Route = "typed_hnsw"
		graph.BaseANNScored = 1
	} else if eligible > 0 {
		graph.Route = "typed_exact"
		graph.ExactBaseScored = eligible
	}
	return &documentservice.DenseSearchWork{Version: 1, Completed: true, Graph: graph, Output: documentservice.DenseSearchOutputWork{Attempted: true, Completed: true, Requested: uint64(count), Fetched: uint64(count), OutputBytes: uint64(count) * 50, JSONReconstructionRows: uint64(count)}}
}

func TestMinimaMeasuredRequestedOutputRetainedPayload(t *testing.T) {
	// Exact request 200 from the actual 91106108 bounded native CLI. Its five
	// requested documents use retained payload plus typed-row reconstruction.
	raw, err := os.ReadFile("testdata/minima_measured_requested_output.json")
	if err != nil {
		t.Fatal(err)
	}
	var request minimaMeasuredRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	if request.DenseWork.Output.RetainedPayloadFetches != 5 || request.DenseWork.Output.TypedColumnRows != 0 {
		t.Fatal("fixture no longer exercises the actual requested-output producer")
	}
	if err := validateMinimaDenseRequest(request); err != nil {
		t.Fatalf("actual retained requested output rejected: %v", err)
	}
	for _, change := range []string{"excess_retained", "requested", "fetched", "missing", "reconstruction", "output_bytes"} {
		t.Run(change, func(t *testing.T) {
			var r minimaMeasuredRequest
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatal(err)
			}
			o := &r.DenseWork.Output
			switch change {
			case "excess_retained":
				o.RetainedPayloadFetches = o.Fetched + 1
			case "requested":
				o.Requested++
			case "fetched":
				o.Fetched--
			case "missing":
				o.Missing++
			case "reconstruction":
				o.JSONReconstructionRows--
			case "output_bytes":
				o.OutputBytes = 0
			}
			if err := validateMinimaDenseRequest(r); err == nil {
				t.Fatal("inconsistent requested output accepted")
			}
		})
	}
	// Output reads cannot substitute for the independent indexed JSON absence
	// proof, nor can an uncharged request prefix borrow process output work.
	w := measuredTestWork(123)
	if err := validateMinimaWork(w, 123, 0); err != nil {
		t.Fatal(err)
	}
	w.IndexedJSON.MaterializationRows = 1
	if err := validateMinimaWork(w, 123, 0); err == nil || !strings.Contains(err.Error(), "forbidden indexed JSON") {
		t.Fatalf("indexed JSON extraction accepted: %v", err)
	}
	a, _ := measuredTestArtifact(t)
	evidence := a.RawEvidence["treedb"]
	for i := range evidence.RequestEvidence {
		r := &evidence.RequestEvidence[i]
		if r.DenseWork != nil && r.DenseWork.Output.Fetched != 0 {
			r.DenseWork.Output.RetainedPayloadFetches = 1
			if err := validateMinimaMeasuredRequestTotals(evidence); err == nil {
				t.Fatal("uncharged retained payload request prefix accepted")
			}
			return
		}
	}
	t.Fatal("fixture has no positive output request")
}

func measuredTestArtifact(t *testing.T) (minimaArtifact, *minimaMeasuredFreeze) {
	t.Helper()
	manifest := buildMinimaManifestForRows(50000)
	a := cloneMinimaArtifact(t, validMinimaArtifactForManifest(manifest))
	a.Schema = minimaMeasuredSchema
	a.State = "partial"
	a.Passing = false
	a.Recommendation = "not_evaluated"
	a.FreezeSHA256 = strings.Repeat("f", 64)
	f := &minimaMeasuredFreeze{Version: 1, HarnessCommit: strings.Repeat("a", 40), ReviewedProductCommit: strings.Repeat("b", 40), SHA256: a.FreezeSHA256, CalibrationStatus: "pending", BoundedPairSHA256: []string{}, Configuration: map[string]map[string]string{},
		Manifest: minimaMeasuredFreezeManifest{Schema: manifest.Schema, Fixture: manifest.Fixture, InputSHA256: strings.Repeat("c", 64), CorpusSHA256: manifest.CorpusSHA256, QuerySHA256: manifest.QuerySHA256, OperationSHA256: manifest.OperationSHA256, ExpectedStateSHA256: manifest.ExpectedStateSHA256}}
	for bi := range a.Backends {
		b := &a.Backends[bi]
		config := map[string]string{}
		keys := append([]string(nil), minimaMeasuredCommonConfigKeys...)
		if b.Name == "treedb" {
			keys = append(keys, minimaMeasuredTreeConfigKeys...)
		} else {
			keys = append(keys, minimaMeasuredQdrantConfigKeys...)
		}
		for _, key := range keys {
			v := b.Configuration[key]
			if v == "" {
				v = "test"
			}
			if strings.HasSuffix(key, "sha256") {
				v = strings.Repeat("d", 64)
			}
			config[key] = v
		}
		config["harness_commit"] = f.HarnessCommit
		config["reviewed_product_commit"] = f.ReviewedProductCommit
		config["manifest_file_sha256"] = f.Manifest.InputSHA256
		config["measurement_mode"] = "measured"
		if b.Name == "treedb" {
			config["vector_strategy"] = "column_graph"
			config["transport"] = "native"
			config["control_transport"] = "http"
			config["block_profile_rate"] = "0"
			config["mutex_profile_fraction"] = "0"
		} else {
			config["deployment"] = "docker"
			config["image"] = "qdrant/qdrant:v1.19.0@sha256:" + strings.Repeat("e", 64)
		}
		f.Configuration[b.Name] = config
		for key, v := range config {
			b.Configuration[key] = v
		}
		raw := a.RawEvidence[b.Name]
		phases := minimaTestPhaseAttribution()
		for i := range phases.Phases {
			p := &phases.Phases[i]
			p.StartNanos = int64(i)*22_000_000 + 1_000_000
			p.EndNanos = p.StartNanos + 20_000_000
			p.DurationNanos = 20_000_000
		}
		phases.TotalStartNanos = phases.Phases[0].StartNanos
		phases.TotalEndNanos = phases.Phases[7].EndNanos + 10
		phases.TotalDurationNanos = phases.TotalEndNanos - phases.TotalStartNanos
		phases.UnattributedNanos = phases.TotalDurationNanos - 8*20_000_000
		raw.PhaseAttribution = &phases
		raw.SetupInterval = &minimaMeasuredInterval{StartedMonotonicNS: 100, EndedMonotonicNS: 1000}
		// Keep existing observed ordering and writer-overlap intervals; offset each
		// existing trace into its phase, without generating any new workload schedule.
		for i := range b.Operations.TimedExecutionTrace.Queries {
			q := &b.Operations.TimedExecutionTrace.Queries[i]
			q.StartedMonotonicNS += phases.Phases[2].StartNanos
			q.EndedMonotonicNS += phases.Phases[2].StartNanos
		}
		for i := range b.Operations.TimedExecutionTrace.Rounds {
			r := &b.Operations.TimedExecutionTrace.Rounds[i]
			r.WriterStartedMonotonicNS += phases.Phases[2].StartNanos
			r.WriterEndedMonotonicNS += phases.Phases[2].StartNanos
		}
		for i := range b.Operations.ReindexExecutionTrace.Operations {
			o := &b.Operations.ReindexExecutionTrace.Operations[i]
			o.MutationStartedMonotonicNS += phases.Phases[3].StartNanos
			o.MutationEndedMonotonicNS += phases.Phases[3].StartNanos
			for j := range o.ReaderQueries {
				q := &o.ReaderQueries[j]
				q.StartedMonotonicNS += phases.Phases[3].StartNanos
				q.EndedMonotonicNS += phases.Phases[3].StartNanos
			}
		}
		b.Operations.TimedExecutionSHA256 = minimaTimedExecutionDigest(b.Operations.TimedExecutionTrace)
		raw.TimedOverlap.TimedExecutionSHA256 = b.Operations.TimedExecutionSHA256
		b.Operations.ReindexExecutionSHA256 = minimaReindexExecutionDigest(b.Operations.ReindexExecutionTrace)
		specs := minimaScenarioMap(&manifest)
		add := func(operation, scenario string, phase int, start, end int64, count int) uint64 {
			seq := uint64(len(raw.RequestEvidence) + 1)
			life := 0
			if phase > 5 {
				life = 1
			}
			r := minimaMeasuredRequest{RequestSequence: seq, Operation: "search", OperationName: operation, Scenario: scenario, Phase: phases.Phases[phase].Name, LifetimeOrdinal: life, StartedMonotonicNS: start, EndedMonotonicNS: end, Outcome: "success", Transport: "http", ResultCount: measuredTestPointer(uint64(count))}
			if b.Name == "treedb" {
				r.Transport = "native"
				r.CommandVersion = measuredTestPointer(uint64(2))
				r.ExpectedGeneration = measuredTestPointer(uint64(1))
				r.DenseWork = measuredTestDense(uint64(specs[scenario].EligibleRows), count)
			}
			raw.RequestEvidence = append(raw.RequestEvidence, r)
			return seq
		}
		for _, op := range manifest.Operations {
			phase := 3
			if op.Name == "warmup_search" {
				phase = 1
			}
			if op.Name == "final_manifest_and_oracle_comparison" {
				phase = 7
			}
			for _, step := range op.Schedule {
				add(op.Name, step.Scenario, phase, phases.Phases[phase].StartNanos+1, phases.Phases[phase].StartNanos+2, len(minimaQueryMap(&manifest)[step.Scenario].InitialOracleIDs))
			}
		}
		for _, phase := range []int{4, 6} {
			operation := "preclose_reopen_baseline"
			if phase == 6 {
				operation = "post_reopen_parity"
			}
			for _, spec := range manifest.Corpora {
				add(operation, spec.Name, phase, phases.Phases[phase].StartNanos+1, phases.Phases[phase].StartNanos+2, len(minimaQueryMap(&manifest)[spec.Name].InitialOracleIDs))
			}
		}

		for i := range b.Operations.TimedExecutionTrace.Queries {
			q := &b.Operations.TimedExecutionTrace.Queries[i]
			q.RequestSequence = add(manifest.Operations[3].Name, q.Scenario, 2, q.StartedMonotonicNS, q.EndedMonotonicNS, len(q.ActualIDs))
		}
		for i := range b.Operations.ReindexExecutionTrace.Operations {
			o := &b.Operations.ReindexExecutionTrace.Operations[i]
			for j := range o.ReaderQueries {
				q := &o.ReaderQueries[j]
				q.RequestSequence = add(manifest.Operations[o.OperationOrdinal].Name, q.Scenario, 3, q.StartedMonotonicNS, q.EndedMonotonicNS, len(q.ActualIDs))
			}
		}
		publicCall := func(name, op, scenario string, phase int, batchStart, rows *uint64, ids []string, deleted bool, first, last int64) {
			phaseName, life := "setup", 0
			if phase >= 0 {
				phaseName = phases.Phases[phase].Name
				if first == 0 {
					first = phases.Phases[phase].StartNanos + 100
					last = first + 1
				}
				if phase >= 5 {
					life = 1
				}
			} else {
				first = 101
				last = 102
			}
			r := minimaMeasuredRequest{OperationName: name, Operation: op, Scenario: scenario, Phase: phaseName, LifetimeOrdinal: life, StartedMonotonicNS: first, EndedMonotonicNS: last, Outcome: "success", Transport: "http", BatchStart: batchStart, RequestedCount: rows, RequestedIDs: ids}
			if b.Name == "treedb" && (batchStart != nil || op == "fetch") {
				r.Transport = "native"
				r.CommandVersion = measuredTestPointer(uint64(1))
				r.ExpectedGeneration = measuredTestPointer(uint64(1))
				r.ResultCount = rows
			}
			if op == "fetch" {
				r.Projection = "payload_only_batch"
				if b.Name == "treedb" {
					r.Projection = "full_fp32_document"
					r.CommandVersion = measuredTestPointer(uint64(2))
				}
				n := len(ids)
				r.ResultIDs = ids
				if deleted {
					n = 0
					r.ResultIDs = nil
				}
				r.ResultCount = measuredTestPointer(uint64(n))
				r.MissingCount = measuredTestPointer(uint64(len(ids) - n))
			}
			r.RequestSequence = uint64(len(raw.RequestEvidence) + 1)
			raw.RequestEvidence = append(raw.RequestEvidence, r)
		}
		control := func(name, op, scenario string, phase int) {
			publicCall(name, op, scenario, phase, nil, nil, nil, false, 0, 0)
		}
		control("ensure_compatible_collection", "write", "all", -1)
		control("initial_batch_insert", "control", "all", 0)
		control("initial_batch_insert", "control", "all", 0)
		if b.Name == "treedb" {
			control("column_graph_initial_build", "control", "all", 0)
			control("column_graph_fold", "control", "all", 3)
			control("column_graph_reopen_schema", "control", "all", 5)
			control("column_graph_reopen_ensure", "control", "all", 5)
			// Reconnect follows the first real new-lifetime observation.
			for i := len(raw.RequestEvidence) - 2; i < len(raw.RequestEvidence); i++ {
				raw.RequestEvidence[i].StartedMonotonicNS += 3_000_000
				raw.RequestEvidence[i].EndedMonotonicNS += 3_000_000
			}
			control("idempotent_ensure_after_reopen", "control", "all", 6)
			control("idempotent_ensure_after_reopen", "control", "all", 6)
		} else {
			control("restore_production_configuration", "control", "all", 0)
			control("idempotent_ensure_after_reopen", "write", "all", 5)
			control("idempotent_ensure_after_reopen", "control", "all", 5)
			control("idempotent_ensure_after_reopen", "control", "all", 5)
		}
		control("final_manifest_and_oracle_comparison", "control", "all", 7)
		for _, op := range manifest.Operations {
			phase := 3
			if op.Name == "initial_batch_insert" {
				phase = 0
			}
			if op.TimedPlan != nil {
				phase = 2
			}
			ranges := op.InsertRanges
			if op.TimedPlan != nil {
				ranges = nil
				for _, round := range op.TimedPlan.Rounds {
					ranges = append(ranges, round.InsertRange)
				}
			}
			for rangeOrdinal, insertion := range ranges {
				first, last := int64(0), int64(0)
				if op.TimedPlan != nil {
					observed := b.Operations.TimedExecutionTrace.Rounds[rangeOrdinal]
					first = observed.WriterStartedMonotonicNS
					last = observed.WriterEndedMonotonicNS
				}
				for offset := 0; offset < insertion.Rows; offset += manifest.Config.BatchSize {
					publicCall(op.Name, "write", insertion.Scenario, phase, measuredTestPointer(uint64(insertion.Start+offset)), measuredTestPointer(uint64(min(manifest.Config.BatchSize, insertion.Rows-offset))), nil, false, first, last)
				}
				if op.TimedPlan != nil {
					ids := make([]string, insertion.Rows)
					for i := range ids {
						ids[i] = fmt.Sprintf("minima/%s/%06d", insertion.Scenario, insertion.Start+i)
					}
					publicCall(op.Name, "fetch", insertion.Scenario, phase, nil, measuredTestPointer(uint64(len(ids))), ids, false, last+1, last+2)
					if b.Name == "qdrant" {
						control(op.Name, "control", insertion.Scenario, phase)
					}
				}
			}
			first, last := int64(0), int64(0)
			for _, observed := range b.Operations.ReindexExecutionTrace.Operations {
				if observed.OperationOrdinal == op.Ordinal {
					first = observed.MutationStartedMonotonicNS
					last = observed.MutationEndedMonotonicNS
				}
			}
			if len(op.Documents) > 0 {
				for offset := 0; offset < len(op.Documents); offset += manifest.Config.BatchSize {
					start := offset
					if n, err := strconv.Atoi(strings.TrimPrefix(op.Documents[offset].ID, "minima/"+op.Target+"/")); err == nil {
						start = n
					}
					publicCall(op.Name, "write", op.Target, phase, measuredTestPointer(uint64(start)), measuredTestPointer(uint64(min(manifest.Config.BatchSize, len(op.Documents)-offset))), nil, false, first, last)
				}
				ids := make([]string, len(op.Documents))
				for i, doc := range op.Documents {
					ids[i] = doc.ID
				}
				publicCall(op.Name, "fetch", op.Target, phase, nil, measuredTestPointer(uint64(len(ids))), ids, false, 0, 0)
				if b.Name == "qdrant" {
					control(op.Name, "control", op.Target, phase)
				}
			}
			if op.Effect == "delete" {
				publicCall(op.Name, "write", op.Target, phase, nil, nil, nil, false, first, last)
				if b.Name == "qdrant" {
					control(op.Name, "control", op.Target, phase)
				}
				if op.Filter != nil {
					control(op.Name, "control", op.Target, phase)
					raw.RequestEvidence[len(raw.RequestEvidence)-1].ResultCount = measuredTestPointer(uint64(0))
				} else {
					publicCall(op.Name, "fetch", op.Target, phase, nil, measuredTestPointer(uint64(len(op.IDs))), op.IDs, true, 0, 0)
				}
			}
		}

		// Give controls and visibility checks the actual runner's causal order.
		// Concurrent reader and writer intervals above remain unchanged.
		for _, op := range manifest.Operations {
			if op.TimedPlan != nil || op.ConcurrentPlan != nil || op.Name == "initial_batch_insert" {
				continue
			}
			for i := range raw.RequestEvidence {
				r := &raw.RequestEvidence[i]
				if r.OperationName == op.Name && r.Phase == "lifecycle_mutations" {
					r.StartedMonotonicNS = phases.Phases[3].StartNanos + int64(op.Ordinal)*1_000_000
					r.EndedMonotonicNS = r.StartedMonotonicNS + 1
				}
			}
		}
		writeEnds := map[string]int64{}
		var initialEnd int64
		for _, r := range raw.RequestEvidence {
			if r.Operation == "write" {
				key := r.OperationName + "/" + r.Scenario
				writeEnds[key] = max(writeEnds[key], r.EndedMonotonicNS)
				if r.BatchStart != nil && r.Phase == "initial_durable_load" {
					initialEnd = max(initialEnd, r.EndedMonotonicNS)
				}
			}
		}
		controlOffsets := map[string]int64{}
		for i := range raw.RequestEvidence {
			r := &raw.RequestEvidence[i]
			start := int64(0)
			switch {
			case r.OperationName == "column_graph_initial_build" || r.OperationName == "restore_production_configuration":
				start = initialEnd + 10
			case r.OperationName == "initial_batch_insert" && r.Operation == "control":
				start = initialEnd + 20 + controlOffsets["initial"]
				controlOffsets["initial"] += 2
			case r.OperationName == "column_graph_fold":
				start = phases.Phases[3].EndNanos - 100
			case r.OperationName == "column_graph_reopen_ensure":
				start = phases.Phases[5].StartNanos + 3_000_104
			case r.OperationName == "idempotent_ensure_after_reopen":
				start = phases.Phases[6].StartNanos + 100
				if b.Name == "qdrant" {
					start = phases.Phases[5].StartNanos + 3_000_100
				}
				start += controlOffsets["ensure"]
				controlOffsets["ensure"] += 2
			case r.Operation == "control" || r.Operation == "fetch":
				key := r.OperationName + "/" + r.Scenario
				if end := writeEnds[key]; end > 0 {
					start = end + 10 + controlOffsets[key]
					if r.Operation == "fetch" {
						start += 100
					}
					controlOffsets[key] += 2
				}
			}
			if start > 0 {
				r.StartedMonotonicNS, r.EndedMonotonicNS = start, start+1
			}
		}

		raw.RestartBoundary.OldLinuxProcessIdentity = "linux-old"
		raw.RestartBoundary.NewLinuxProcessIdentity = "linux-new"
		for i := range phases.Phases {
			p := &phases.Phases[i]
			for j := range p.ResourceSegments {
				segment := &p.ResourceSegments[j]
				life := 0
				if i > 5 || i == 5 && j == 1 {
					life = 1
				}
				id := "linux-old"
				if life == 1 {
					id = "linux-new"
				}
				for endpointOrdinal, endpoint := range []*minimaRawPhaseResourceEndpoint{&segment.Start, &segment.End} {
					endpoint.LifetimeOrdinal = measuredTestPointer(life)
					endpoint.LinuxProcessIdentity = id
					if b.Name == "treedb" {
						w := measuredTestWork(endpoint.PID)
						capture := p.StartNanos - 2
						if endpointOrdinal == 1 {
							capture = p.EndNanos + 1
						}
						if i == 5 && j == 0 && endpointOrdinal == 1 {
							capture = p.StartNanos + 1_000_000
						}
						if i == 5 && j == 1 && endpointOrdinal == 0 {
							capture = p.StartNanos + 2_000_000
						}
						endpoint.WorkSnapshot = &minimaWorkObservation{Availability: "measured", StartedMonotonicNS: capture, EndedMonotonicNS: capture + 1, Work: &w}
					}
				}
			}
		}
		if b.Name == "treedb" {
			for life, pid := range []int{raw.RestartBoundary.OldPID, raw.RestartBoundary.NewPID} {
				id := "linux-old"
				first, last := int64(10), phases.Phases[5].StartNanos+1_000_000
				if life == 1 {
					id = "linux-new"
					first, last = phases.Phases[5].StartNanos+2_000_000, phases.TotalEndNanos+1
				}

				prefix := func(at int64) workstats.Snapshot {
					w := measuredTestWork(pid)
					for _, request := range raw.RequestEvidence {
						if request.LifetimeOrdinal != life || request.EndedMonotonicNS > at {
							continue
						}

						if request.Operation == "fetch" && request.Transport == "native" {
							w.Output.GetMany.Attempts++
							w.Output.GetMany.Completed++
							w.Output.GetMany.Requested += *request.RequestedCount
							w.Output.GetMany.Fetched += *request.ResultCount
							w.Output.GetMany.Missing += *request.MissingCount
						}
						if request.DenseWork == nil {
							continue
						}
						local, out := request.DenseWork.Graph, request.DenseWork.Output
						w.Graph.Requests.Attempts++
						w.Graph.Requests.Completed++
						w.Graph.Filters.Attempts++
						w.Graph.Filters.Completed++
						switch local.Route {
						case "typed_empty":
							w.Graph.Empty++
						case "typed_exact":
							w.Graph.Exact++
						case "typed_hnsw":
							w.Graph.HNSW++
						}
						w.Graph.BaseANNScored += local.BaseANNScored
						w.Graph.ExactBaseScored += local.ExactBaseScored
						w.Output.Search.Attempts++
						w.Output.Search.Completed++
						w.Output.Search.Requested += out.Requested
						w.Output.Search.Fetched += out.Fetched
						w.Output.Search.OutputBytes += out.OutputBytes
						w.Output.Search.JSONReconstructionRows += out.JSONReconstructionRows
					}
					return w
				}
				// Synthetic producer prefixes increase only after fixture calls.
				for i := range raw.PhaseAttribution.Phases {
					for j := range raw.PhaseAttribution.Phases[i].ResourceSegments {
						seg := &raw.PhaseAttribution.Phases[i].ResourceSegments[j]
						for _, endpoint := range []*minimaRawPhaseResourceEndpoint{&seg.Start, &seg.End} {
							if *endpoint.LifetimeOrdinal == life {
								w := prefix(endpoint.WorkSnapshot.StartedMonotonicNS)
								endpoint.WorkSnapshot.Work = &w
							}
						}
					}
				}
				firstWork, w := prefix(first), prefix(last)

				raw.ProcessLifetimes = append(raw.ProcessLifetimes, minimaProcessLifetime{Ordinal: life, PID: pid, LinuxProcessIdentity: id, FirstWork: minimaWorkObservation{Availability: "measured", StartedMonotonicNS: first, EndedMonotonicNS: first + 1, Work: &firstWork}, LastLiveWork: minimaWorkObservation{Availability: "measured", StartedMonotonicNS: last, EndedMonotonicNS: last + 1, Work: &w}, TerminalWork: &minimaTerminalWork{Event: "treedb_document_service_terminal_work", Version: 1, ContractVersion: documentservice.ContractVersion, CleanupCompleted: true, Work: w}, Exit: minimaOwnedExit{Availability: "measured", PID: pid, LinuxProcessIdentity: id, ObservedMonotonicNS: last + 2, ExitCode: measuredTestPointer(0), PeakRSSBytes: measuredTestPointer(int64(10000)), Source: "linux_wait4_ru_maxrss_kib_times_1024", Scope: "owned_process_start_through_exit"}})
			}
		}
		if raw.ResourceMeasurement.End != nil {
			raw.PhaseAttribution.Phases[7].ResourceSegments[0].End.DiskBytes = raw.ResourceMeasurement.End.DiskBytes
		}
		a.RawEvidence[b.Name] = raw
	}
	return a, f
}

func TestMinimaMeasuredProducerPresence(t *testing.T) {
	raw, _ := json.Marshal(measuredTestDense(4096, 5))
	if err := minimaMeasuredJSON(raw, reflect.TypeFor[documentservice.DenseSearchWork](), "dense_work"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(string(raw), `"base_ann_scored":0,`, "", 1), strings.Replace(string(raw), `"base_ann_scored":0`, `"base_ann_scored":null`, 1), strings.Replace(string(raw), `"base_ann_scored":0`, `"base_ann_scored":false`, 1), strings.Replace(string(raw), `"base_ann_scored":0`, `"base_ann_scored":-1`, 1), strings.Replace(string(raw), `"base_ann_scored":0`, `"base_ann_scored":18446744073709551616`, 1), strings.Replace(string(raw), `"version":1`, `"version":1,"version":1`, 1)} {
		if err := minimaMeasuredJSON([]byte(bad), reflect.TypeFor[documentservice.DenseSearchWork](), "dense_work"); err == nil {
			t.Fatalf("accepted malformed producer %s", bad)
		}
	}
}

func TestMinimaMeasuredQdrantRequiresPinnedOwnedDocker(t *testing.T) {
	_, freeze := measuredTestArtifact(t)
	if err := validateMinimaMeasuredFreeze(freeze); err != nil {
		t.Fatal(err)
	}
	for _, deployment := range []string{"standalone", "external"} {
		freeze.Configuration["qdrant"]["deployment"] = deployment
		if err := validateMinimaMeasuredFreeze(freeze); err == nil {
			t.Fatal("unowned measured deployment accepted")
		}
	}
	freeze.Configuration["qdrant"]["deployment"] = "docker"
	for _, image := range []string{"qdrant/qdrant:v1.19.0", "qdrant/qdrant@sha256:bad", "@sha256:" + strings.Repeat("d", 64)} {
		freeze.Configuration["qdrant"]["image"] = image
		if err := validateMinimaMeasuredFreeze(freeze); err == nil {
			t.Fatal("unpinned measured image accepted")
		}
	}
}

func TestMinimaMeasuredPublicCallCausalOrder(t *testing.T) {
	a, _ := measuredTestArtifact(t)
	for _, backend := range a.Backends {
		t.Run(backend.Name, func(t *testing.T) {
			if err := validateMinimaMeasuredPublicCalls(&a.Manifest, backend, a.RawEvidence[backend.Name]); err != nil {
				t.Fatal(err)
			}
			cases := []string{"build_before_writes", "readiness_before_build", "fetch_before_mutation", "count_before_delete", "reopen_before_schema"}
			if backend.Name == "treedb" {
				cases = append(cases, "fold_before_mutation")
			}
			for _, change := range cases {
				t.Run(change, func(t *testing.T) {
					raw := cloneMinimaArtifact(t, a).RawEvidence[backend.Name]
					changed := false
					for i := range raw.RequestEvidence {
						r := &raw.RequestEvidence[i]
						match := change == "build_before_writes" && (r.OperationName == "column_graph_initial_build" || r.OperationName == "restore_production_configuration") ||
							change == "readiness_before_build" && r.OperationName == "initial_batch_insert" && r.Operation == "control" ||
							change == "fetch_before_mutation" && r.Operation == "fetch" && r.OperationName == "explicit_update" ||
							change == "count_before_delete" && r.Operation == "control" && r.OperationName == "reindex_delete_by_user_and_fpath_while_reading" ||
							change == "fold_before_mutation" && r.OperationName == "column_graph_fold" ||
							change == "reopen_before_schema" && (r.OperationName == "column_graph_reopen_ensure" || backend.Name == "qdrant" && r.OperationName == "idempotent_ensure_after_reopen" && r.Operation == "control")
						if match {
							for _, phase := range raw.PhaseAttribution.Phases {
								if phase.Name == r.Phase {
									r.StartedMonotonicNS, r.EndedMonotonicNS = phase.StartNanos+1, phase.StartNanos+2
								}
							}
							changed = true
							break
						}
					}
					if !changed {
						t.Fatal("fixture did not exercise correction")
					}
					if err := validateMinimaMeasuredPublicCalls(&a.Manifest, backend, raw); err == nil {
						t.Fatal("causally reordered public calls accepted")
					}
				})
			}
		})
	}
}

func TestMinimaMeasuredPublicCallCoverage(t *testing.T) {
	a, _ := measuredTestArtifact(t)
	for _, backend := range a.Backends {
		t.Run(backend.Name, func(t *testing.T) {
			base := a.RawEvidence[backend.Name]
			for _, category := range []string{"write", "control", "fetch"} {
				t.Run("missing_"+category, func(t *testing.T) {
					raw := base
					raw.RequestEvidence = nil
					for _, r := range base.RequestEvidence {
						if r.Operation != category {
							raw.RequestEvidence = append(raw.RequestEvidence, r)
						}
					}
					if err := validateMinimaMeasuredPublicCalls(&a.Manifest, backend, raw); err == nil {
						t.Fatal("missing public category accepted")
					}
				})
			}
			for _, change := range []string{"batch_size", "batch_identity", "writer_interval", "fetch_ids", "fixture_in_setup", "missing_build", "missing_ensure", "extra_control", "filtered_count"} {
				t.Run(change, func(t *testing.T) {
					copy := cloneMinimaArtifact(t, a)
					raw := copy.RawEvidence[backend.Name]
					changed := false
					for i := range raw.RequestEvidence {
						r := &raw.RequestEvidence[i]
						switch {
						case change == "batch_size" && r.BatchStart != nil:
							*r.RequestedCount++
							changed = true
						case change == "batch_identity" && r.BatchStart != nil:
							*r.BatchStart++
							changed = true
						case change == "writer_interval" && r.BatchStart != nil && r.OperationName == "timed_search_with_batch_insert":
							r.StartedMonotonicNS++
							changed = true
						case change == "fetch_ids" && r.Operation == "fetch" && len(r.RequestedIDs) > 0:
							r.RequestedIDs[0] = "wrong-id"
							changed = true
						case change == "fixture_in_setup" && r.BatchStart != nil:
							r.Phase = "setup"
							changed = true
						case change == "missing_build" && (r.OperationName == "column_graph_initial_build" || r.OperationName == "restore_production_configuration"):
							r.OperationName = "missing"
							changed = true
						case change == "missing_ensure" && (r.OperationName == "column_graph_reopen_ensure" || r.OperationName == "idempotent_ensure_after_reopen"):
							r.OperationName = "missing"
							changed = true
						case change == "filtered_count" && r.OperationName == "reindex_delete_by_user_and_fpath_while_reading" && r.Operation == "control" && r.ResultCount != nil:
							*r.ResultCount = 1
							changed = true
						case change == "extra_control" && r.Operation == "control":
							raw.RequestEvidence = append(raw.RequestEvidence, *r)
							changed = true
						}
						if changed {
							break
						}
					}
					if !changed {
						t.Fatal("fixture did not exercise control")
					}
					if err := validateMinimaMeasuredPublicCalls(&a.Manifest, backend, raw); err == nil {
						t.Fatal("doctored public call accepted")
					}
				})
			}
		})
	}
}

func TestMinimaMeasuredStartupCannotPayForLaterRequests(t *testing.T) {
	a, _ := measuredTestArtifact(t)
	raw := a.RawEvidence["treedb"]
	*raw.ProcessLifetimes[0].FirstWork.Work = *raw.ProcessLifetimes[0].LastLiveWork.Work
	if err := validateMinimaMeasuredRequestTotals(raw); err == nil {
		t.Fatal("startup work paid for later calls")
	}
	a, _ = measuredTestArtifact(t)
	raw = a.RawEvidence["treedb"]
	raw.ProcessLifetimes[0].LastLiveWork.Work.Graph.BaseCandidates++
	if err := validateMinimaMeasuredRequestTotals(raw); err != nil {
		t.Fatalf("additional internal work rejected: %v", err)
	}
	phase := &raw.PhaseAttribution.Phases[2]
	*phase.ResourceSegments[0].End.WorkSnapshot.Work = *phase.ResourceSegments[0].Start.WorkSnapshot.Work
	if err := validateMinimaMeasuredRequestTotals(raw); err == nil {
		t.Fatal("another phase paid for timed work")
	}
}

func TestMinimaMeasuredObservationBoundaryControls(t *testing.T) {
	a, _ := measuredTestArtifact(t)
	for _, name := range []string{"old_exit", "new_startup", "new_before_origin", "observer_gap", "drained_boundary", "final_storage"} {
		t.Run(name, func(t *testing.T) {
			copy := cloneMinimaArtifact(t, a)
			raw := copy.RawEvidence["treedb"]
			p := raw.PhaseAttribution.Phases
			switch name {
			case "old_exit":
				raw.ProcessLifetimes[0].Exit.ObservedMonotonicNS = p[5].EndNanos + 1
			case "new_startup":
				raw.ProcessLifetimes[1].FirstWork.EndedMonotonicNS = p[5].EndNanos + 1
			case "new_before_origin":
				p[5].ResourceSegments[1].Start.WorkSnapshot.StartedMonotonicNS--
			case "observer_gap":
				p[2].ResourceSegments[0].End.WorkSnapshot.EndedMonotonicNS = p[3].StartNanos + 1
			case "drained_boundary":
				p[2].ResourceSegments[0].Start.WorkSnapshot.Work.Typed.TextRows++
			case "final_storage":
				raw.ResourceMeasurement.End.DiskBytes++
			}
			if err := validateMinimaMeasuredObservationWindows(raw); err == nil {
				t.Fatal("invalid observation boundary accepted")
			}
		})
	}
}
func TestMinimaMeasuredWorkMonotonicGauges(t *testing.T) {
	before := measuredTestWork(1)
	before.Memory = workstats.MemoryStats{TotalAlloc: 10, Mallocs: 10, HeapAlloc: 100, HeapSys: 100, Sys: 100, NumGC: 1}
	before.RowIndexCache = workstats.RowIndexCacheStats{Entries: 10, RetainedBytes: 100, ByteLimit: 100, Hits: 2}
	after := before
	after.Memory.HeapAlloc = 0
	after.Memory.HeapSys = 0
	after.Memory.Sys = 0
	after.RowIndexCache.Entries = 0
	after.RowIndexCache.RetainedBytes = 0
	after.RowIndexCache.ByteLimit = 0
	if !minimaWorkMonotonic(before, after) {
		t.Fatal("rejected release of gauges")
	}
	after.Memory.Mallocs--
	if minimaWorkMonotonic(before, after) {
		t.Fatal("accepted cumulative reset")
	}
}
func TestMinimaMeasuredCompleteRawCannotBypass(t *testing.T) {
	a, f := measuredTestArtifact(t)
	if err := validateMinimaArtifact(&a, f); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"treedb", "qdrant"} {
		t.Run(name, func(t *testing.T) {
			copy := cloneMinimaArtifact(t, a)
			for _, b := range copy.Backends {
				if b.Name == name {
					copy.Backends = []minimaBackendEvidence{b}
					break
				}
			}
			copy.RawEvidence = map[string]minimaRawBackendEvidence{name: copy.RawEvidence[name]}
			copy.Scenarios = nil
			if err := validateMinimaArtifact(&copy, f); err == nil {
				t.Fatal("completed raw partial bypassed scenario validation")
			}
		})
	}
	if err := validateMinimaArtifact(&a); err == nil {
		t.Fatal("accepted artifact-selected trust")
	}
	altered := *f
	altered.SHA256 = strings.Repeat("0", 64)
	if err := validateMinimaArtifact(&a, &altered); err == nil {
		t.Fatal("accepted different externally pinned freeze")
	}
	path := filepath.Join(t.TempDir(), "artifact.json")
	raw, _ := json.Marshal(a)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	decoded, err := readMinimaArtifact(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateMinimaArtifact(&decoded, f); err != nil {
		t.Fatal(err)
	}
}
func TestMinimaMeasuredFiveCapsUseRawIntervals(t *testing.T) {
	a, _ := measuredTestArtifact(t)
	gates, err := minimaMeasuredGates(&a)
	if err != nil {
		t.Fatal(err)
	}
	if len(gates) != 5 || gates[3].Value != 1 {
		t.Fatal(gates)
	}
	// A shutdown-only highwater dominates the live endpoint; an unrelated pooled
	// latency summary cannot replace all 1024 observed outer query durations.
	raw := a.RawEvidence["treedb"]
	raw.ProcessLifetimes[1].Exit.PeakRSSBytes = measuredTestPointer(minimaMeasuredRSSCap + 1)
	raw.PhaseLatencyDistributions = map[string]minimaRawLatencyDistribution{"search": {P50Nanos: 0}}
	a.RawEvidence["treedb"] = raw
	gates, err = minimaMeasuredGates(&a)
	if err != nil || gates[2].Value != minimaMeasuredRSSCap+1 {
		t.Fatal(gates, err)
	}
	a.Backends[0].Operations.TimedExecutionTrace.Queries = a.Backends[0].Operations.TimedExecutionTrace.Queries[:1023]
	if _, err := minimaMeasuredGates(&a); err == nil {
		t.Fatal("accepted selected subset of timed queries")
	}
}

func TestMinimaMeasuredRejectsDoctoredJoinsAndLifetimeWork(t *testing.T) {
	base, freeze := measuredTestArtifact(t)
	cases := map[string]func(*minimaArtifact){
		"missing request": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.RequestEvidence = r.RequestEvidence[1:]
			a.RawEvidence["treedb"] = r
		},
		"extra request": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			q := r.RequestEvidence[0]
			q.RequestSequence = uint64(len(r.RequestEvidence) + 1)
			r.RequestEvidence = append(r.RequestEvidence, q)
			a.RawEvidence["treedb"] = r
		},
		"changed phase": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.RequestEvidence[0].Phase = "lifecycle_mutations"
			p := r.PhaseAttribution.Phases[3]
			r.RequestEvidence[0].StartedMonotonicNS = p.StartNanos + 1
			r.RequestEvidence[0].EndedMonotonicNS = p.StartNanos + 2
			a.RawEvidence["treedb"] = r
		},
		"wrong transport": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.RequestEvidence[0].Transport = "http"
			r.RequestEvidence[0].CommandVersion = nil
			a.RawEvidence["treedb"] = r
		},
		"wrong ordinal": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.PhaseAttribution.Phases[0].ResourceSegments[0].Start.LifetimeOrdinal = measuredTestPointer(2)
			a.RawEvidence["treedb"] = r
		},
		"missing terminal": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.ProcessLifetimes[0].TerminalWork = nil
			a.RawEvidence["treedb"] = r
		},
		"shutdown failure": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.ProcessLifetimes[0].TerminalWork.ShutdownFailures = 1
			a.RawEvidence["treedb"] = r
		},
		"work origin": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.ProcessLifetimes[1].TerminalWork.Work.OriginUnixNano++
			a.RawEvidence["treedb"] = r
		},
		"startup JSON work": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.ProcessLifetimes[0].FirstWork.Work.IndexedJSON.ColumnRows = 1
			a.RawEvidence["treedb"] = r
		},
		"error query": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.RequestEvidence[0].Outcome = "error"
			r.RequestEvidence[0].Error = "cancelled"
			a.RawEvidence["treedb"] = r
		},
		"exact route": func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			for i := range r.RequestEvidence {
				if r.RequestEvidence[i].DenseWork.Graph.Filter.EligibleRows == 4097 {
					r.RequestEvidence[i].DenseWork.Graph.Route = "typed_exact"
					break
				}
			}
			a.RawEvidence["treedb"] = r
		},
		"incomplete parity": func(a *minimaArtifact) {
			a.Backends[0].Operations.ManifestOrdered = false
			a.Failures = []string{"failed"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			a := cloneMinimaArtifact(t, base)
			mutate(&a)
			if err := validateMinimaArtifact(&a, freeze); err == nil {
				t.Fatal("accepted doctored measured evidence")
			}
		})
	}
	for _, key := range []string{"service_binary_sha256", "shared_runner_sha256", "client_sha256", "product_source_sha256", "harness_source_sha256", "comparator_binary_sha256", "column_graph_serving", "manifest_file_sha256"} {
		t.Run(key, func(t *testing.T) {
			a := cloneMinimaArtifact(t, base)
			a.Backends[0].Configuration[key] = strings.Repeat("e", 64)
			if err := validateMinimaArtifact(&a, freeze); err == nil {
				t.Fatal("accepted replacement observed identity/options")
			}
		})
	}
	legacy := base
	legacy.Schema = minimaArtifactSchema
	if err := validateMinimaArtifact(&legacy, freeze); err == nil {
		t.Fatal("trusted invocation accepted legacy schema downgrade")
	}
}

func TestMinimaMeasuredEachInclusiveCap(t *testing.T) {
	base, _ := measuredTestArtifact(t)
	caps := []int64{minimaMeasuredLoadCap, minimaMeasuredRestartCap, minimaMeasuredRSSCap, minimaMeasuredSearchCap, minimaMeasuredDiskCap}
	for index, capValue := range caps {
		for _, delta := range []int64{0, 1} {
			a := cloneMinimaArtifact(t, base)
			r := a.RawEvidence["treedb"]
			value := capValue + delta
			switch index {
			case 0, 1:
				phase := 0
				if index == 1 {
					phase = 5
				}
				p := &r.PhaseAttribution.Phases[phase]
				p.EndNanos = p.StartNanos + value
				p.DurationNanos = value
			case 2:
				r.ProcessLifetimes[1].Exit.PeakRSSBytes = measuredTestPointer(value)
			case 3:
				for i := range a.Backends[0].Operations.TimedExecutionTrace.Queries {
					q := &a.Backends[0].Operations.TimedExecutionTrace.Queries[i]
					q.EndedMonotonicNS = q.StartedMonotonicNS + value
				}
			case 4:
				r.ResourceMeasurement.End.DiskBytes = value
			}
			a.RawEvidence["treedb"] = r
			gates, err := minimaMeasuredGates(&a)
			if err != nil {
				t.Fatal(err)
			}
			if gates[index].Value != value {
				t.Fatal(gates[index])
			}
			if err = validateMinimaMeasuredGateCaps(gates); (err == nil) != (delta == 0) {
				t.Fatalf("cap %d delta %d: %v", index, delta, err)
			}
		}
	}
}

func TestMinimaMeasuredReadRejectsMissingNullAndDuplicatePresence(t *testing.T) {
	a, _ := measuredTestArtifact(t)
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{`"request_sequence":1,`, ""}, {`"request_sequence":1`, `"request_sequence":null`}, {`"request_sequence":1`, `"request_sequence":18446744073709551616`}, {`"freeze_sha256":`, `"freeze_sha256":"duplicate","freeze_sha256":`}, {`"request_evidence":`, `"request_evidence":[],"request_evidence":`}} {
		changed := strings.Replace(string(raw), pair[0], pair[1], 1)
		if changed == string(raw) {
			t.Fatalf("fixture did not contain %s", pair[0])
		}
		if err := minimaMeasuredPresence([]byte(changed)); err == nil {
			t.Fatalf("accepted replacement %s", pair[1])
		}
	}
	if err := minimaMeasuredJSON([]byte(`{"a":"one","a":"two"}`), reflect.TypeFor[map[string]string](), "configuration"); err == nil {
		t.Fatal("collapsed duplicate config key")
	}
	for _, raw := range []string{
		`{"phase_attribution":{"phases":[{"resource_segments":[{"start":{"lifetime_ordinal":null,"lifetime_ordinal":0}}]}]}}`,
		`{"timed_execution_trace":{"queries":[{"request_sequence":null,"request_sequence":1}]}}`,
		`{"resource_segments":[{"end":null,"end":{"work_snapshot":{}}}]}`,
		`{"reindex_execution_trace":{"operations":[],"operations":[]}}`,
	} {
		if err := minimaMeasuredNoDuplicateKeys([]byte(raw)); err == nil {
			t.Fatal("collapsed intermediate measured carrier duplicate")
		}
	}
}

func TestMinimaMeasuredComparisonRetainsRejectedRawInput(t *testing.T) {
	a, freeze := measuredTestArtifact(t)
	dir := t.TempDir()
	paths := map[string]string{}
	for _, name := range []string{"treedb", "qdrant"} {
		one := cloneMinimaArtifact(t, a)
		one.Backends = nil
		one.Scenarios = nil
		for _, b := range a.Backends {
			if b.Name == name {
				one.Backends = append(one.Backends, b)
			}
		}
		for _, s := range a.Scenarios {
			if s.Backend == name {
				one.Scenarios = append(one.Scenarios, s)
			}
		}
		one.RawEvidence = map[string]minimaRawBackendEvidence{name: one.RawEvidence[name]}
		if name == "treedb" {
			one.Backends[0].Configuration["client_sha256"] = strings.Repeat("e", 64)
		}
		raw, _ := json.Marshal(one)
		paths[name] = filepath.Join(dir, name+".json")
		if err := os.WriteFile(paths[name], raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	out, report := filepath.Join(dir, "combined.json"), filepath.Join(dir, "combined.md")
	if err := compareMinimaEvidence(paths["treedb"], paths["qdrant"], out, report, "ready_with_alpha_limitations", freeze.HarnessCommit, freeze); err == nil {
		t.Fatal("accepted raw trust mismatch")
	}
	retained, err := readMinimaArtifact(out)
	if err != nil {
		t.Fatal(err)
	}
	if retained.Passing || retained.State != "partial" || len(retained.Failures) == 0 || len(retained.RawEvidence) != 2 {
		t.Fatalf("failed input evidence dropped: %+v", retained)
	}
	if _, err := os.Stat(report); err != nil {
		t.Fatal(err)
	}
}
