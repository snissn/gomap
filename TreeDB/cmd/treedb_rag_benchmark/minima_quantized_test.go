package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
)

func minimaQuantizedTestRequest(t *testing.T) (minimaManifest, minimaQuantizedProfile, minimaScenarioSpec, minimaQuantizedRequest) {
	t.Helper()
	manifest, err := buildMinimaBoundedManifest(50000)
	if err != nil {
		t.Fatal(err)
	}
	spec := minimaScenarioMap(&manifest)["small"]
	query := minimaQueryMap(&manifest)["small"]
	snapshot := collections.ColumnGraphQuerySnapshot{
		Available: true, SchemaHash: 7, SchemaGeneration: 1,
		BaseManifest:    collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
		CurrentManifest: collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3},
		BaseCoverageLSN: 2, CurrentCoverageLSN: 2,
	}
	resultCount := min(manifest.Config.TopK, spec.EligibleRows)
	results := make([]minimaQuantizedResult, resultCount)
	for i := range results {
		var document minimaGeneratedDocument
		for ordinal := spec.EligibleStart; ordinal < spec.EligibleStart+spec.EligibleRows; ordinal++ {
			candidate, documentErr := minimaDocumentAt(spec, ordinal)
			if documentErr != nil {
				t.Fatal(documentErr)
			}
			if candidate.ID == query.InitialOracleIDs[i] {
				document = candidate
				break
			}
		}
		results[i] = minimaQuantizedResult{ID: document.ID, Content: document.Content,
			UserID: document.UserID, FPath: document.FPath, Score: query.InitialOracleScores[i]}
	}
	profile := minimaQuantizedProfile{
		Schema: minimaQuantizedProfileSchema, Name: "minima_sq8", QueryMode: collections.VectorIndexQueryModeQuantizedRerank,
		IndexName: "minima_sq8", Codec: "scalar_u8", Version: 1, Calibration: "legacy",
		QuantizedConfigHash: 0, RequestedEFSearch: 64, RequestedRerankCandidates: 64,
		NativeCommandVersion: 3,
	}
	work := &documentservice.DenseSearchWork{
		Version: 1, Completed: true,
		Graph: collections.ColumnGraphQueryWork{
			Available: true, Completed: true, Route: "typed_exact",
			ExactBaseScored: uint64(spec.EligibleRows), BaseResultIDs: uint64(spec.EligibleRows),
			Filter: collections.ColumnGraphFilterWork{Attempted: true, Completed: true,
				EligibleRows: uint64(spec.EligibleRows)}, Snapshot: snapshot,
		},
		Output: documentservice.DenseSearchOutputWork{Attempted: true, Completed: true,
			Requested: uint64(resultCount), Fetched: uint64(resultCount),
			OutputBytes:            uint64(resultCount),
			RetainedPayloadFetches: uint64(resultCount), JSONReconstructionRows: uint64(resultCount),
			TypedColumnRows: uint64(resultCount)},
	}
	proof := &collections.ColumnGraphScorePlaneWork{
		Version: 1, Available: true, Completed: true,
		RequestedMode: collections.VectorIndexQueryModeQuantizedRerank,
		EffectiveMode: collections.VectorIndexQueryModeQuantizedRerank, Route: "typed_exact",
		QuantizedIndexName: "minima_sq8", QuantizedCodec: "scalar_u8", QuantizedVersion: 1,
		RequestedTopK: uint64(manifest.Config.TopK), RequestedEFSearch: 64, RequestedRerankCandidates: 64,
		NormalizedCandidateWidth: 64, RawCandidateWidth: 64, RerankCandidateCap: 64,
		ExactSmallFilterScoreCalls: uint64(spec.EligibleRows),
		ExactBaseVectorBytesRead:   uint64(spec.EligibleRows * manifest.Config.Dimension * 4), Snapshot: snapshot,
	}
	return manifest, profile, spec, minimaQuantizedRequest{
		OperationName: "warmup_search", Scenario: "small", RequestSequence: 1,
		Phase: "warmup_search", LifetimeOrdinal: 0,
		StartedMonotonicNS: 10, EndedMonotonicNS: 20, Outcome: "success",
		Transport: "native", CommandVersion: 3, ExpectedGeneration: 1,
		ResultCount: uint64(resultCount), Results: results, DenseWork: work, ScorePlane: proof,
	}
}

func minimaQuantizedTestServingJSON(t *testing.T) string {
	t.Helper()
	options := collections.ColumnGraphServingOptions{
		Publication: collections.ColumnGraphPublicationLimits{Rows: 1, Tombstones: 1, ValueSlots: 1, OwnedBytes: 1, EncodedOutputBytes: 1},
		Owners: collections.ColumnGraphReadOwnerLimits{Owners: 1, States: 1, StateBytes: 1, AssetBytes: 1,
			Cold: collections.ColumnGraphColdLimits{ManifestRecords: 1, ManifestBytes: 1, AssetBytes: 1, DecodedTermBytes: 1}},
		CandidateOutput: collections.ColumnGraphCandidateOutputLimits{Bytes: 1, AppenderAttempts: 1},
		Maintenance: collections.ColumnGraphMaintenanceLimits{NativeEntries: 1, ColumnSegments: 1, ManifestRecords: 1,
			LifecycleEntries: 1, NativeBytes: 1, ColumnBytes: 1, ManifestBytes: 1, RetainedBytes: 1, PagerPages: 1},
		Filter:   collections.ColumnGraphFilterLimits{SourceIDs: 1, SourceBytes: 1, RetainedBytes: 1, MappingWork: 1, InspectedEntries: 1},
		FoldRows: 1, SearchCandidates: 1 << 20,
	}
	raw, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func minimaQuantizedTestPlan(t *testing.T, manifest minimaManifest, profile minimaQuantizedProfile) minimaQuantizedPlan {
	t.Helper()
	var serving collections.ColumnGraphServingOptions
	if err := json.Unmarshal([]byte(minimaQuantizedTestServingJSON(t)), &serving); err != nil {
		t.Fatal(err)
	}
	return minimaQuantizedPlan{
		Schema: minimaQuantizedPlanSchema,
		Manifest: minimaQuantizedPlanManifest{
			Schema: manifest.Schema, Fixture: manifest.Fixture, Config: manifest.Config,
			CorpusSHA256: manifest.CorpusSHA256, QuerySHA256: manifest.QuerySHA256,
			OperationSHA256: manifest.OperationSHA256, ExpectedStateSHA256: manifest.ExpectedStateSHA256,
		},
		QuantizedProfile: profile, VectorStrategy: "column_graph", Transport: "native",
		DurabilityProfile: "command_wal_durable", VectorM: 16, EFConstruction: 32, Serving: serving,
		SHA256: strings.Repeat("a", 64),
	}
}

func minimaQuantizedTestSnapshot(state minimaQuantizedAllowedState) collections.ColumnGraphQuerySnapshot {
	baseManifest := collections.ColumnGraphManifestWork{Generation: 1, Format: "tcs1", Version: 1, Checksum: 3}
	currentManifest := collections.ColumnGraphManifestWork{
		Generation: 1 + state.OwnerAdvance, Format: "tcs1", Version: 1, Checksum: 3 + state.OwnerAdvance,
	}
	snapshot := collections.ColumnGraphQuerySnapshot{
		Available: true, SchemaHash: 7, SchemaGeneration: 1,
		BaseManifest: baseManifest, CurrentManifest: currentManifest,
		BaseCoverageLSN: 2, CurrentCoverageLSN: 2 + state.OwnerAdvance,
	}
	if state.Folded {
		snapshot.CurrentManifest.Checksum += 1000
		snapshot.BaseManifest = snapshot.CurrentManifest
		snapshot.BaseCoverageLSN = snapshot.CurrentCoverageLSN
	}
	return snapshot
}

func TestMinimaQuantizedRequestProofMatrix(t *testing.T) {
	manifest, profile, spec, base := minimaQuantizedTestRequest(t)
	if err := validateMinimaQuantizedRequest(base, profile, spec, &manifest); err != nil {
		t.Fatalf("valid request: %v", err)
	}
	_, _, _, zeroSchemaHash := minimaQuantizedTestRequest(t)
	zeroSchemaHash.DenseWork.Graph.Snapshot.SchemaHash = 0
	zeroSchemaHash.ScorePlane.Snapshot.SchemaHash = 0
	if err := validateMinimaQuantizedRequest(zeroSchemaHash, profile, spec, &manifest); err == nil {
		t.Fatal("accepted available owner with zero schema hash")
	}
	overflow := &collections.ColumnGraphScorePlaneWork{QuantizedScoreCalls: ^uint64(0)}
	if minimaQuantizedByteCountersValid(overflow, 8) {
		t.Fatal("accepted overflowing byte counters")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*minimaQuantizedRequest)
	}{
		{"native_version", func(r *minimaQuantizedRequest) { r.CommandVersion = 2 }},
		{"missing_work", func(r *minimaQuantizedRequest) { r.DenseWork = nil }},
		{"missing_score_plane", func(r *minimaQuantizedRequest) { r.ScorePlane = nil }},
		{"wrong_name", func(r *minimaQuantizedRequest) { r.ScorePlane.QuantizedIndexName = "other" }},
		{"wrong_r", func(r *minimaQuantizedRequest) { r.ScorePlane.RequestedRerankCandidates-- }},
		{"stale_owner", func(r *minimaQuantizedRequest) { r.ScorePlane.Snapshot.SchemaGeneration++ }},
		{"sibling_snapshot", func(r *minimaQuantizedRequest) { r.DenseWork.Graph.Snapshot.SchemaHash++ }},
		{"small_labeled_hnsw", func(r *minimaQuantizedRequest) {
			r.ScorePlane.Route, r.DenseWork.Graph.Route = "quantized_rerank", "typed_hnsw"
		}},
		{"unreachable_eligible_state", func(r *minimaQuantizedRequest) {
			r.DenseWork.Graph.Filter.EligibleRows = 15
			r.DenseWork.Graph.ExactBaseScored, r.DenseWork.Graph.BaseResultIDs = 15, 15
			r.ScorePlane.ExactSmallFilterScoreCalls = 15
			r.ScorePlane.ExactBaseVectorBytesRead = 15 * uint64(manifest.Config.Dimension*4)
		}},
		{"duplicate_result", func(r *minimaQuantizedRequest) { r.Results[1].ID = r.Results[0].ID }},
		{"nonfinite_score", func(r *minimaQuantizedRequest) { r.Results[0].Score = math.NaN() }},
		{"wrong_fp32_score", func(r *minimaQuantizedRequest) { r.Results[0].Score -= 0.01 }},
		{"missing_full_field", func(r *minimaQuantizedRequest) { r.Results[0].Content = "" }},
		{"zero_output_bytes", func(r *minimaQuantizedRequest) { r.DenseWork.Output.OutputBytes = 0 }},
		{"exact_retained_candidates", func(r *minimaQuantizedRequest) { r.ScorePlane.RawRetainedCandidates = 1 }},
		{"exact_base_candidates", func(r *minimaQuantizedRequest) { r.DenseWork.Graph.BaseCandidates = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, request := minimaQuantizedTestRequest(t)
			tc.mutate(&request)
			if err := validateMinimaQuantizedRequest(request, profile, spec, &manifest); err == nil {
				t.Fatal("accepted hostile quantized request proof")
			}
		})
	}
}

func TestMinimaQuantizedSchemaIsBoundedAndPresenceChecked(t *testing.T) {
	manifest, _ := buildMinimaBoundedManifest(50000)
	profile := minimaQuantizedProfile{
		Schema: minimaQuantizedProfileSchema, Name: "minima_sq8", QueryMode: collections.VectorIndexQueryModeQuantizedRerank,
		IndexName: "minima_sq8", Codec: "scalar_u8", Version: 1, Calibration: "legacy",
		QuantizedConfigHash: 0, RequestedEFSearch: 64, RequestedRerankCandidates: 64,
		NativeCommandVersion: 3,
	}
	plan := minimaQuantizedTestPlan(t, manifest, profile)
	artifact := minimaArtifact{
		Schema: minimaQuantizedArtifactSchema, State: "partial", Manifest: manifest,
		Recommendation: "not_evaluated", QuantizedProfile: &profile, QuantizedPlanSHA256: plan.SHA256,
		Failures: []string{"constructor failed before execution"},
	}
	if err := validateMinimaArtifactTrusted(&artifact, nil, &plan); err != nil {
		t.Fatalf("valid failed pre-execution quantized envelope: %v", err)
	}
	artifact.Failures = nil
	if validateMinimaArtifactTrusted(&artifact, nil, &plan) == nil {
		t.Fatal("accepted pre-execution quantized envelope without a retained failure")
	}
	artifact.Failures = []string{"constructor failed before execution"}
	if validateMinimaArtifact(&artifact) == nil {
		t.Fatal("quantized diagnostic validated without its trusted plan")
	}
	envelope, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"freeze_sha256":"",`, `"native_path_proof":null,`} {
		changed := strings.Replace(string(envelope), "{", "{"+field, 1)
		if minimaQuantizedPresence([]byte(changed)) == nil {
			t.Fatalf("accepted explicit forbidden quantized field %s", field)
		}
	}
	artifact.Passing = true
	if validateMinimaArtifactTrusted(&artifact, nil, &plan) == nil {
		t.Fatal("quantized diagnostic emitted a passing verdict")
	}
	artifact.Passing = false
	artifact.Manifest.Schema = minimaManifestSchema
	if validateMinimaArtifactTrusted(&artifact, nil, &plan) == nil {
		t.Fatal("quantized diagnostic accepted the full qualification manifest")
	}

	_, _, _, request := minimaQuantizedTestRequest(t)
	raw, err := json.Marshal([]minimaQuantizedRequest{request})
	if err != nil {
		t.Fatal(err)
	}
	var producerShape []map[string]any
	if err := json.Unmarshal(raw, &producerShape); err != nil {
		t.Fatal(err)
	}
	proof := producerShape[0]["score_plane"].(map[string]any)
	proof["reason"], proof["quantized_config_hash"] = "", float64(0)
	raw, err = json.Marshal(producerShape)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []minimaQuantizedRequest
	if err := minimaQuantizedRequestPresence(raw); err != nil {
		t.Fatalf("complete request presence: %v", err)
	}
	if err := json.Unmarshal(raw, &decoded); err != nil || len(decoded) != 1 {
		t.Fatalf("round trip: len=%d err=%v", len(decoded), err)
	}
	missingZero := []byte(string(raw))
	missingZero = []byte(strings.Replace(string(missingZero), `"quantized_config_hash":0,`, "", 1))
	if err := minimaQuantizedRequestPresence(missingZero); err == nil {
		t.Fatal("accepted missing explicit zero score-plane field")
	}
	for _, hostile := range []string{
		`{"schema":"treedb_rag_application/minima_v4","quantized_profile":{},"backend_raw_evidence":{}}`,
		`{"schema":"treedb_rag_application/minima_v4","backend_raw_evidence":{"treedb":{"quantized_request_evidence":[]}}}`,
	} {
		if err := minimaNonQuantizedPresence([]byte(hostile)); err == nil {
			t.Fatal("nonquantized schema accepted an explicit quantized field")
		}
	}
}

func TestMinimaQuantizedPlanBytesAndBinding(t *testing.T) {
	manifest, _ := buildMinimaBoundedManifest(50000)
	profile := minimaQuantizedProfile{
		Schema: minimaQuantizedProfileSchema, Name: "minima_sq8", QueryMode: collections.VectorIndexQueryModeQuantizedRerank,
		IndexName: "minima_sq8", Codec: "scalar_u8", Version: 1, Calibration: "legacy",
		RequestedEFSearch: 64, RequestedRerankCandidates: 64, NativeCommandVersion: 3,
	}
	plan := minimaQuantizedTestPlan(t, manifest, profile)
	plan.SHA256 = ""
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	path := t.TempDir() + "/plan.json"
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadMinimaQuantizedPlan(path, digest)
	if err != nil || loaded.SHA256 != digest {
		t.Fatalf("load exact plan bytes: plan=%+v err=%v", loaded, err)
	}
	artifact := minimaArtifact{
		Schema: minimaQuantizedArtifactSchema, State: "partial", Manifest: manifest,
		Recommendation: "not_evaluated", QuantizedProfile: &profile, QuantizedPlanSHA256: digest,
	}
	if err := validateMinimaQuantizedPlanBinding(loaded, &artifact); err != nil {
		t.Fatalf("bind exact plan: %v", err)
	}
	changed := artifact
	changed.QuantizedPlanSHA256 = strings.Repeat("b", 64)
	if validateMinimaQuantizedPlanBinding(loaded, &changed) == nil {
		t.Fatal("accepted artifact with another plan digest")
	}
	changed = artifact
	changed.QuantizedProfile.RequestedEFSearch++
	if validateMinimaQuantizedPlanBinding(loaded, &changed) == nil {
		t.Fatal("accepted artifact profile drift from plan")
	}
	if _, err := loadMinimaQuantizedPlan(path, strings.Repeat("A", 64)); err == nil {
		t.Fatal("accepted a non-lowercase external pin")
	}
	wrongM := *loaded
	wrongM.VectorM = 32
	if validateMinimaQuantizedPlan(&wrongM) == nil {
		t.Fatal("accepted a quantized plan with another graph M")
	}
	duplicate := []byte(`{"schema":"treedb_minima_quantized_plan/v1","manifest":{"schema":"x","schema":"x"}}`)
	duplicatePath := t.TempDir() + "/duplicate.json"
	if err := os.WriteFile(duplicatePath, duplicate, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadMinimaQuantizedPlan(duplicatePath, fmt.Sprintf("%x", sha256.Sum256(duplicate))); err == nil {
		t.Fatal("accepted nested duplicate plan key")
	}
	large := []byte(`{"padding":"` + strings.Repeat("x", (1<<20)+1) + `"}`)
	largePath := t.TempDir() + "/large.json"
	if err := os.WriteFile(largePath, large, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadMinimaQuantizedPlan(largePath, fmt.Sprintf("%x", sha256.Sum256(large))); err == nil {
		t.Fatal("accepted oversized plan")
	}
}

func minimaQuantizedCompletedTestRequest(t *testing.T, manifest *minimaManifest, profile minimaQuantizedProfile,
	spec minimaScenarioSpec, operation, phase string, sequence uint64, start, end int64,
	ids []string, scores []float64, requestedState ...minimaQuantizedAllowedState) minimaQuantizedRequest {
	t.Helper()
	if len(ids) != len(scores) {
		t.Fatal("test result cardinality")
	}
	var state minimaQuantizedAllowedState
	if len(requestedState) == 1 {
		state = requestedState[0]
	} else if states, ok := minimaQuantizedStaticStates(manifest, operation); ok {
		state = states[0]
	} else {
		ordinal := 4
		if operation == "reindex_replacement_insert_while_reading" {
			ordinal = 5
		}
		states, ok := minimaQuantizedConcurrentStates(manifest, ordinal)
		if !ok || operation != manifest.Operations[ordinal].Name {
			t.Fatalf("test request %q has no lifecycle state", operation)
		}
		state = states[0]
		if ordinal == 5 {
			state = states[1]
		}
	}
	stage := state.Stage
	results := make([]minimaQuantizedResult, len(ids))
	for i, id := range ids {
		candidate, live := minimaQuantizedDocumentAtStage(manifest, spec, id, stage)
		if live && math.Abs(minimaDocumentScore(candidate)-scores[i]) <= manifest.Config.ScoreTolerance {
			results[i] = minimaQuantizedResult{ID: id, Content: candidate.Content,
				UserID: candidate.UserID, FPath: candidate.FPath, Score: scores[i]}
		}
		if results[i].ID == "" {
			t.Fatalf("no admissible test projection for %s", id)
		}
	}
	eligible, ok := minimaQuantizedEligibleRowsAtStage(manifest, spec, stage)
	if !ok {
		t.Fatal("invalid test state")
	}
	requested := collections.ColumnGraphScorePlaneWork{RequestedTopK: uint64(manifest.Config.TopK),
		RequestedEFSearch: profile.RequestedEFSearch, RequestedRerankCandidates: profile.RequestedRerankCandidates}
	stateWork, ok := minimaQuantizedStateWorkAtState(manifest, spec, state, requested)
	if !ok || len(stateWork.Plans) == 0 || stateWork.LiveBase+stateWork.LiveSuffix != eligible {
		t.Fatal("invalid test state work")
	}
	plan := stateWork.Plans[0]
	snapshot := minimaQuantizedTestSnapshot(state)
	graphRoute, proofRoute := "typed_empty", "typed_empty"
	var quantizedCalls, exactRerank, exactSmall uint64
	if eligible > minimaLookupLimit {
		graphRoute, proofRoute = "typed_hnsw", "quantized_rerank"
		quantizedCalls, exactRerank = profile.RequestedEFSearch, profile.RequestedRerankCandidates
	} else if eligible > 0 {
		graphRoute, proofRoute = "typed_exact", "typed_exact"
		exactSmall = stateWork.LiveBase
	}
	exactBase := exactRerank + exactSmall
	work := &documentservice.DenseSearchWork{Version: 1, Completed: true,
		Graph: collections.ColumnGraphQueryWork{Available: true, Completed: true, Route: graphRoute,
			BaseANNScored: quantizedCalls, BaseCandidates: quantizedCalls,
			ExactBaseScored: exactBase, BaseResultIDs: exactBase,
			DeltaScored: stateWork.LiveSuffix, BaseShadowed: plan.ShadowAllowance,
			Filter:   collections.ColumnGraphFilterWork{Attempted: true, Completed: true, EligibleRows: eligible},
			Snapshot: snapshot},
		Output: documentservice.DenseSearchOutputWork{Attempted: true, Completed: true,
			Requested: uint64(len(results)), Fetched: uint64(len(results)),
			OutputBytes:            uint64(len(results)),
			RetainedPayloadFetches: uint64(len(results)), JSONReconstructionRows: uint64(len(results)),
			TypedColumnRows: uint64(len(results))}}
	proof := &collections.ColumnGraphScorePlaneWork{Version: 1, Available: true, Completed: true,
		RequestedMode: profile.QueryMode, EffectiveMode: profile.QueryMode, Route: proofRoute,
		QuantizedIndexName: profile.IndexName, QuantizedCodec: profile.Codec,
		QuantizedVersion: profile.Version, RequestedTopK: uint64(manifest.Config.TopK),
		RequestedEFSearch:         profile.RequestedEFSearch,
		RequestedRerankCandidates: profile.RequestedRerankCandidates,
		NormalizedCandidateWidth:  plan.EffectiveEF, RawCandidateWidth: plan.RawWidth,
		RerankCandidateCap:    plan.RerankCap,
		RawRetainedCandidates: quantizedCalls, LiveShortlistCandidates: quantizedCalls,
		ActualRerankCandidates: exactRerank, QuantizedScoreCalls: quantizedCalls,
		QuantizedCodeBytesRead:    quantizedCalls * uint64(manifest.Config.Dimension),
		ExactBaseRerankScoreCalls: exactRerank, ExactSmallFilterScoreCalls: exactSmall,
		ExactSuffixScoreCalls:      stateWork.LiveSuffix,
		ExactBaseVectorBytesRead:   exactBase * uint64(manifest.Config.Dimension*4),
		ExactSuffixVectorBytesRead: stateWork.LiveSuffix * uint64(manifest.Config.Dimension*4), Snapshot: snapshot}
	lifetime := 0
	if phase == "post_reopen" || phase == "final_state_scroll_artifact_work" {
		lifetime = 1
	}
	return minimaQuantizedRequest{OperationName: operation, Scenario: spec.Name, RequestSequence: sequence,
		Phase: phase, LifetimeOrdinal: lifetime, StartedMonotonicNS: start, EndedMonotonicNS: end, Outcome: "success",
		Transport: "native", CommandVersion: 3, ExpectedGeneration: 1,
		ResultCount: uint64(len(results)), Results: results, DenseWork: work, ScorePlane: proof}
}

func TestMinimaQuantizedStateWorkTracksPreparedFilterAlternatives(t *testing.T) {
	manifest, _ := buildMinimaBoundedManifest(50000)
	specs := minimaScenarioMap(&manifest)
	proof := collections.ColumnGraphScorePlaneWork{RequestedTopK: uint64(manifest.Config.TopK),
		RequestedEFSearch: 64, RequestedRerankCandidates: 64}
	cases := []struct {
		name         string
		scenario     string
		state        minimaQuantizedAllowedState
		base, suffix uint64
		plans        []minimaQuantizedPlanOption
	}{
		{"all-match timed suffix", "all_match", minimaQuantizedAllowedState{Stage: 3, OwnerAdvance: 2}, 7360, 256,
			[]minimaQuantizedPlanOption{{EffectiveEF: 64, RawWidth: 64, RerankCap: 64}}},
		{"mixed current empty or cached shadow", "mixed_broad_narrow", minimaQuantizedAllowedState{Stage: 4, OwnerAdvance: 9}, 0, 0,
			[]minimaQuantizedPlanOption{{}, {EffectiveEF: 5, RawWidth: 5, RerankCap: 5, ShadowAllowance: 5}}},
		{"mixed suffix replacement", "mixed_broad_narrow", minimaQuantizedAllowedState{Stage: 5, OwnerAdvance: 10}, 0, 5,
			[]minimaQuantizedPlanOption{{}, {EffectiveEF: 5, RawWidth: 5, RerankCap: 5, ShadowAllowance: 5}}},
		{"small update", "small", minimaQuantizedAllowedState{Stage: 7, OwnerAdvance: 11}, 15, 1,
			[]minimaQuantizedPlanOption{{EffectiveEF: 15, RawWidth: 15, RerankCap: 15},
				{EffectiveEF: 16, RawWidth: 16, RerankCap: 16, ShadowAllowance: 1}}},
		{"small delete", "small", minimaQuantizedAllowedState{Stage: 9, OwnerAdvance: 12}, 14, 1,
			[]minimaQuantizedPlanOption{{EffectiveEF: 14, RawWidth: 14, RerankCap: 14},
				{EffectiveEF: 16, RawWidth: 16, RerankCap: 16, ShadowAllowance: 2}}},
		{"folded final", "small", minimaQuantizedAllowedState{Stage: 9, OwnerAdvance: 12, Folded: true}, 15, 0,
			[]minimaQuantizedPlanOption{{EffectiveEF: 15, RawWidth: 15, RerankCap: 15}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			work, ok := minimaQuantizedStateWorkAtState(&manifest, specs[tc.scenario], tc.state, proof)
			if !ok || work.LiveBase != tc.base || work.LiveSuffix != tc.suffix || !reflect.DeepEqual(work.Plans, tc.plans) {
				t.Fatalf("state work=%+v ok=%v want base=%d suffix=%d plans=%+v", work, ok, tc.base, tc.suffix, tc.plans)
			}
		})
	}
}

func TestMinimaQuantizedStateWorkRejectsWrongEligibleAndScoreSplit(t *testing.T) {
	manifest, _ := buildMinimaBoundedManifest(50000)
	profile := minimaQuantizedProfile{Schema: minimaQuantizedProfileSchema, Name: "minima_sq8",
		QueryMode: collections.VectorIndexQueryModeQuantizedRerank, IndexName: "minima_sq8",
		Codec: "scalar_u8", Version: 1, Calibration: "legacy", RequestedEFSearch: 64,
		RequestedRerankCandidates: 64, NativeCommandVersion: 3}
	specs, queries := minimaScenarioMap(&manifest), minimaQueryMap(&manifest)
	clone := func(request minimaQuantizedRequest) minimaQuantizedRequest {
		work, proof := *request.DenseWork, *request.ScorePlane
		request.DenseWork, request.ScorePlane = &work, &proof
		return request
	}

	smallState := minimaQuantizedAllowedState{Stage: 1}
	smallIDs, smallScores, err := minimaQuantizedExactRankingAtStage(&manifest, specs["small"], 1)
	if err != nil {
		t.Fatal(err)
	}
	small := minimaQuantizedCompletedTestRequest(t, &manifest, profile, specs["small"],
		"warmup_search", "warmup_search", 1, 1, 2, smallIDs, smallScores, smallState)
	if err := validateMinimaQuantizedRequest(small, profile, specs["small"], &manifest); err != nil ||
		len(minimaQuantizedMatchingStates(small, &manifest, specs["small"], []minimaQuantizedAllowedState{smallState})) != 1 {
		t.Fatalf("valid initial small request: %v", err)
	}
	shifted := clone(small)
	shifted.ScorePlane.ExactSmallFilterScoreCalls = 0
	shifted.ScorePlane.ExactSuffixScoreCalls = 16
	shifted.ScorePlane.ExactBaseVectorBytesRead = 0
	shifted.ScorePlane.ExactSuffixVectorBytesRead = 16 * uint64(manifest.Config.Dimension*4)
	shifted.DenseWork.Graph.ExactBaseScored = 0
	shifted.DenseWork.Graph.BaseResultIDs = 0
	shifted.DenseWork.Graph.DeltaScored = 16
	if err := validateMinimaQuantizedRequest(shifted, profile, specs["small"], &manifest); err != nil {
		t.Fatalf("hostile split should reach state binding: %v", err)
	}
	if got := minimaQuantizedMatchingStates(shifted, &manifest, specs["small"], []minimaQuantizedAllowedState{smallState}); len(got) != 0 {
		t.Fatalf("accepted base work relabeled as suffix: %+v", got)
	}
	wrongEligible := clone(small)
	wrongEligible.DenseWork.Graph.Filter.EligibleRows = 15
	wrongEligible.ScorePlane.ExactSmallFilterScoreCalls = 15
	wrongEligible.ScorePlane.ExactBaseVectorBytesRead = 15 * uint64(manifest.Config.Dimension*4)
	wrongEligible.DenseWork.Graph.ExactBaseScored = 15
	wrongEligible.DenseWork.Graph.BaseResultIDs = 15
	if err := validateMinimaQuantizedRequest(wrongEligible, profile, specs["small"], &manifest); err == nil {
		if got := minimaQuantizedMatchingStates(wrongEligible, &manifest, specs["small"], []minimaQuantizedAllowedState{smallState}); len(got) != 0 {
			t.Fatalf("accepted wrong state eligible count: %+v", got)
		}
	}

	updatedState := minimaQuantizedAllowedState{Stage: 7, OwnerAdvance: 11}
	updatedIDs, updatedScores, err := minimaQuantizedExactRankingAtStage(&manifest, specs["small"], 7)
	if err != nil {
		t.Fatal(err)
	}
	updated := minimaQuantizedCompletedTestRequest(t, &manifest, profile, specs["small"],
		"update_visibility_probe", "lifecycle_mutations", 1, 7, 8, updatedIDs, updatedScores, updatedState)
	updatedWork, _ := minimaQuantizedStateWorkAtState(&manifest, specs["small"], updatedState, *updated.ScorePlane)
	if len(updatedWork.Plans) != 2 {
		t.Fatalf("updated state plans=%+v want current and cached", updatedWork.Plans)
	}
	crossed := clone(updated)
	crossed.ScorePlane.NormalizedCandidateWidth = updatedWork.Plans[1].EffectiveEF
	crossed.ScorePlane.RawCandidateWidth = updatedWork.Plans[1].RawWidth
	crossed.ScorePlane.RerankCandidateCap = updatedWork.Plans[1].RerankCap
	if got := minimaQuantizedMatchingStates(crossed, &manifest, specs["small"], []minimaQuantizedAllowedState{updatedState}); len(got) != 0 {
		t.Fatalf("accepted cached widths with current shadow count: %+v", got)
	}
	crossed = clone(updated)
	crossed.DenseWork.Graph.BaseShadowed = updatedWork.Plans[1].ShadowAllowance
	if got := minimaQuantizedMatchingStates(crossed, &manifest, specs["small"], []minimaQuantizedAllowedState{updatedState}); len(got) != 0 {
		t.Fatalf("accepted current widths with cached shadow count: %+v", got)
	}

	allState := minimaQuantizedAllowedState{Stage: 3, OwnerAdvance: 2}
	all := minimaQuantizedCompletedTestRequest(t, &manifest, profile, specs["all_match"],
		"timed_search_with_batch_insert", "timed_search_write_overlap", 2, 3, 4,
		queries["all_match"].InitialOracleIDs, queries["all_match"].InitialOracleScores, allState)
	if all.ScorePlane.ExactSuffixScoreCalls != 256 {
		t.Fatalf("post-insert suffix=%d want 256", all.ScorePlane.ExactSuffixScoreCalls)
	}
	erased := clone(all)
	erased.ScorePlane.ExactSuffixScoreCalls = 0
	erased.ScorePlane.ExactSuffixVectorBytesRead = 0
	erased.DenseWork.Graph.DeltaScored = 0
	if err := validateMinimaQuantizedRequest(erased, profile, specs["all_match"], &manifest); err != nil {
		t.Fatalf("hostile erased suffix should reach state binding: %v", err)
	}
	if got := minimaQuantizedMatchingStates(erased, &manifest, specs["all_match"], []minimaQuantizedAllowedState{allState}); len(got) != 0 {
		t.Fatalf("accepted erased suffix work: %+v", got)
	}

	emptyState := minimaQuantizedAllowedState{Stage: 4, OwnerAdvance: 9}
	empty := minimaQuantizedCompletedTestRequest(t, &manifest, profile, specs["mixed_broad_narrow"],
		"reindex_replacement_insert_while_reading", "lifecycle_mutations", 3, 5, 6, nil, nil, emptyState)
	stateWork, _ := minimaQuantizedStateWorkAtState(&manifest, specs["mixed_broad_narrow"], emptyState, *empty.ScorePlane)
	if got := minimaQuantizedMatchingStates(empty, &manifest, specs["mixed_broad_narrow"], []minimaQuantizedAllowedState{emptyState}); len(got) != 1 {
		t.Fatalf("current prepared empty did not match: %+v", got)
	}
	cached := clone(empty)
	cachedPlan := stateWork.Plans[1]
	cached.ScorePlane.NormalizedCandidateWidth = cachedPlan.EffectiveEF
	cached.ScorePlane.RawCandidateWidth = cachedPlan.RawWidth
	cached.ScorePlane.RerankCandidateCap = cachedPlan.RerankCap
	if got := minimaQuantizedMatchingStates(cached, &manifest, specs["mixed_broad_narrow"], []minimaQuantizedAllowedState{emptyState}); len(got) != 1 {
		t.Fatalf("cached shadowed empty did not match: %+v", got)
	}
	cached.ScorePlane.NormalizedCandidateWidth--
	cached.ScorePlane.RawCandidateWidth--
	cached.ScorePlane.RerankCandidateCap--
	if got := minimaQuantizedMatchingStates(cached, &manifest, specs["mixed_broad_narrow"], []minimaQuantizedAllowedState{emptyState}); len(got) != 0 {
		t.Fatalf("accepted width outside both legal plans: %+v", got)
	}
}

func TestMinimaQuantizedCompletedArtifactAndExactLedger(t *testing.T) {
	manifest, _ := buildMinimaBoundedManifest(50000)
	base := validMinimaArtifactForManifest(manifest)
	base.Schema, base.State, base.Passing, base.Recommendation = minimaQuantizedArtifactSchema, "partial", false, "not_evaluated"
	base.NativePathProof = nil
	base.QuantizedProfile = &minimaQuantizedProfile{Schema: minimaQuantizedProfileSchema,
		Name: "minima_sq8", QueryMode: collections.VectorIndexQueryModeQuantizedRerank,
		IndexName: "minima_sq8", Codec: "scalar_u8", Version: 1, Calibration: "legacy",
		RequestedEFSearch: 64, RequestedRerankCandidates: 64, NativeCommandVersion: 3}
	plan := minimaQuantizedTestPlan(t, manifest, *base.QuantizedProfile)
	base.QuantizedPlanSHA256 = plan.SHA256
	base.Backends = base.Backends[:1]
	backend := &base.Backends[0]
	for key, value := range map[string]string{
		"vector_strategy": "column_graph", "transport": "native", "query_mode": "quantized_rerank", "vector_m": "16",
		"quantized_profile": "minima_sq8", "quantized_index_name": "minima_sq8", "quantized_codec": "scalar_u8",
		"quantized_version": "1", "quantized_calibration": "legacy", "quantized_config_hash": "0",
		"ef_search": "64", "quantized_rerank_candidates": "64", "native_command_version": "3",
		"dimension": "8", "metric": "cosine", "scalar_fields": "meta.user_id,meta.fpath",
		"control_transport": "http", "profile": "command_wal_durable", "collection": "owned",
		"ef_construction_requested": "32", "ef_construction_effective": "32",
		"column_graph_serving": minimaQuantizedTestServingJSON(t),
	} {
		backend.Configuration[key] = value
	}
	effective := documentservice.IndexInfo{TypedInput: true, Name: "owned", Dimension: 8,
		Metric: documentservice.MetricCosine, Generation: 1, ContractVersion: "treedb-document-service/v1alpha2",
		EmbeddingField: "embedding", VectorIndexName: "embedding", VectorStrategy: collections.VectorIndexStrategyColumnGraph,
		VectorM: 16, VectorEfConstruction: 32, VectorEfSearch: 64, TextField: "content", TextIndexName: "content",
		DocumentType:     "treedb_document_service_v1",
		QuantizedIndexes: []documentservice.QuantizedIndexInfo{{Name: "minima_sq8", Codec: "scalar_u8", Version: 1}},
		ScalarFields: []documentservice.ScalarFieldInfo{
			{Field: "meta.fpath", IndexName: "meta_fpath", ValueType: documentservice.ScalarFieldString},
			{Field: "meta.user_id", IndexName: "meta_user_id", ValueType: documentservice.ScalarFieldString},
		}, Capabilities: minimaQuantizedIndexCapabilities()}
	encoded, _ := json.Marshal(effective)
	backend.Configuration["effective_collection"] = string(encoded)
	mutateEffective := func(artifact *minimaArtifact, mutate func(*documentservice.IndexInfo)) {
		var info documentservice.IndexInfo
		if err := json.Unmarshal([]byte(artifact.Backends[0].Configuration["effective_collection"]), &info); err != nil {
			t.Fatal(err)
		}
		mutate(&info)
		raw, err := json.Marshal(info)
		if err != nil {
			t.Fatal(err)
		}
		artifact.Backends[0].Configuration["effective_collection"] = string(raw)
	}
	for round := range backend.Operations.TimedExecutionTrace.Rounds {
		observed := &backend.Operations.TimedExecutionTrace.Rounds[round]
		base := int64(2_000_000 + round*1_000_000)
		observed.WriterStartedMonotonicNS, observed.WriterEndedMonotonicNS = base+100, base+900
	}
	for i := range backend.Operations.TimedExecutionTrace.Queries {
		query := &backend.Operations.TimedExecutionTrace.Queries[i]
		base := int64(2_000_000 + query.Round*1_000_000)
		query.StartedMonotonicNS = base + 200 + int64(query.Ordinal%128)*2
		query.EndedMonotonicNS = query.StartedMonotonicNS + 1
	}
	backend.Operations.TimedExecutionSHA256 = minimaTimedExecutionDigest(backend.Operations.TimedExecutionTrace)
	for index := range backend.Operations.ReindexExecutionTrace.Operations {
		operation := &backend.Operations.ReindexExecutionTrace.Operations[index]
		base := int64(12_000_000 + index*1_000_000)
		operation.MutationStartedMonotonicNS, operation.MutationEndedMonotonicNS = base+100, base+900
		for reader := range operation.ReaderQueries {
			query := &operation.ReaderQueries[reader]
			query.StartedMonotonicNS = base + 200 + int64(reader)*10
			query.EndedMonotonicNS = query.StartedMonotonicNS + 1
		}
	}
	backend.Operations.ReindexExecutionSHA256 = minimaReindexExecutionDigest(backend.Operations.ReindexExecutionTrace)
	base.Scenarios = base.Scenarios[:len(manifest.Corpora)]
	for i := range base.Scenarios {
		base.Scenarios[i].Route.Identity = "typed_column_graph_quantized_rerank"
	}
	delete(base.RawEvidence, "qdrant")
	raw := base.RawEvidence["treedb"]
	raw.NativeRouteResponses = nil
	phaseEnds := []int64{1_000_000, 2_000_000, 12_000_000, 15_000_000, 16_000_000, 17_000_000, 18_000_000, 19_000_000}
	phaseStart := int64(1)
	for i := range raw.PhaseAttribution.Phases {
		phase := &raw.PhaseAttribution.Phases[i]
		phase.StartNanos, phase.EndNanos = phaseStart, phaseEnds[i]
		phase.DurationNanos, phase.SampleDurationNanos = phase.EndNanos-phase.StartNanos, 1
		phaseStart = phase.EndNanos
	}
	raw.PhaseAttribution.TotalStartNanos, raw.PhaseAttribution.TotalEndNanos = 1, phaseStart
	raw.PhaseAttribution.TotalDurationNanos, raw.PhaseAttribution.UnattributedNanos = phaseStart-1, 0
	raw.TimedOverlap.TimedExecutionSHA256 = backend.Operations.TimedExecutionSHA256
	specs, queries := minimaScenarioMap(&manifest), minimaQueryMap(&manifest)
	rows := make(map[string]minimaScenarioEvidence)
	for _, row := range base.Scenarios {
		rows[row.Scenario] = row
	}
	sequence := uint64(0)
	add := func(operation, phase, scenario string, start, end int64, ids []string, scores []float64,
		state ...minimaQuantizedAllowedState) uint64 {
		sequence++
		raw.QuantizedRequestEvidence = append(raw.QuantizedRequestEvidence,
			minimaQuantizedCompletedTestRequest(t, &manifest, *base.QuantizedProfile, specs[scenario],
				operation, phase, sequence, start, end, ids, scores, state...))
		return sequence
	}
	phaseNext := map[string]int64{
		"warmup_search": 1_000_100, "lifecycle_mutations": 14_000_100,
		"pre_close_queries": 15_000_100, "post_reopen": 17_000_100,
		"final_state_scroll_artifact_work": 18_000_100,
	}
	ordinary := func(operation, phase, scenario string, final bool) {
		query := queries[scenario]
		ids, scores := query.InitialOracleIDs, query.InitialOracleScores
		if final {
			ids, scores = query.FinalOracleIDs, query.FinalOracleScores
		}
		started := phaseNext[phase]
		add(operation, phase, scenario, started, started+1, ids, scores)
		phaseNext[phase] += 2
	}
	for _, operation := range manifest.Operations {
		switch operation.Name {
		case "warmup_search":
			for _, step := range operation.Schedule {
				ordinary(operation.Name, "warmup_search", step.Scenario, false)
			}
		case "timed_search_with_batch_insert":
			for i := range backend.Operations.TimedExecutionTrace.Queries {
				query := &backend.Operations.TimedExecutionTrace.Queries[i]
				states, _ := minimaQuantizedTimedStates(&manifest, *query)
				query.RequestSequence = add(operation.Name, "timed_search_write_overlap", query.Scenario,
					query.StartedMonotonicNS, query.EndedMonotonicNS, query.ActualIDs, query.ActualScores, states[0])
			}
		case "reindex_delete_by_user_and_fpath_while_reading", "reindex_replacement_insert_while_reading":
			for i := range backend.Operations.ReindexExecutionTrace.Operations {
				observed := &backend.Operations.ReindexExecutionTrace.Operations[i]
				if observed.OperationOrdinal != operation.Ordinal {
					continue
				}
				for j := range observed.ReaderQueries {
					query := &observed.ReaderQueries[j]
					query.RequestSequence = add(operation.Name, "lifecycle_mutations", query.Scenario,
						query.StartedMonotonicNS, query.EndedMonotonicNS, query.ActualIDs, query.ActualScores)
				}
			}
		case "reindex_visibility_probe", "delete_visibility_probe", "empty_user_and_file_probes":
			for _, step := range operation.Schedule {
				ordinary(operation.Name, "lifecycle_mutations", step.Scenario, true)
			}
		case "update_visibility_probe":
			for _, step := range operation.Schedule {
				ordinary(operation.Name, "lifecycle_mutations", step.Scenario, false)
			}
		case "close":
			for _, spec := range manifest.Corpora {
				ordinary("preclose_reopen_baseline", "pre_close_queries", spec.Name, true)
			}
		case "reopen":
			for _, spec := range manifest.Corpora {
				ordinary("post_reopen_parity", "post_reopen", spec.Name, true)
			}
		case "final_manifest_and_oracle_comparison":
			for _, step := range operation.Schedule {
				ordinary(operation.Name, "final_state_scroll_artifact_work", step.Scenario, true)
			}
		}
	}
	base.RawEvidence["treedb"] = raw
	want := 0
	for _, count := range minimaExpectedQuantizedSearchCounts(&manifest) {
		want += count
	}
	if want != 1093 || len(raw.QuantizedRequestEvidence) != want {
		t.Fatalf("search ledger=%d want=%d derived=%d", len(raw.QuantizedRequestEvidence), 1093, want)
	}
	if err := validateMinimaArtifactTrusted(&base, nil, &plan); err != nil {
		t.Fatalf("valid completed quantized artifact: %v", err)
	}
	approximate := cloneMinimaArtifact(t, base)
	approximateRaw := approximate.RawEvidence["treedb"]
	for i := range approximate.Backends[0].Operations.TimedExecutionTrace.Queries {
		query := &approximate.Backends[0].Operations.TimedExecutionTrace.Queries[i]
		if query.Round != 0 || query.Scenario != "all_match" {
			continue
		}
		request := &approximateRaw.QuantizedRequestEvidence[query.RequestSequence-1]
		for result := range request.Results {
			document, live := minimaQuantizedDocumentAtStage(&manifest, specs[query.Scenario],
				fmt.Sprintf("minima/all_match/%06d", 20+result), 1)
			if !live {
				t.Fatal("alternate approximate test result is not live")
			}
			request.Results[result] = minimaQuantizedResult{ID: document.ID, Content: document.Content,
				UserID: document.UserID, FPath: document.FPath, Score: minimaDocumentScore(document)}
			query.ActualIDs[result], query.ActualScores[result] = document.ID, minimaDocumentScore(document)
		}
		break
	}
	approximate.Backends[0].Operations.TimedExecutionSHA256 = minimaTimedExecutionDigest(approximate.Backends[0].Operations.TimedExecutionTrace)
	approximateRaw.TimedOverlap.TimedExecutionSHA256 = approximate.Backends[0].Operations.TimedExecutionSHA256
	approximate.RawEvidence["treedb"] = approximateRaw
	if err := validateMinimaArtifactTrusted(&approximate, nil, &plan); err != nil {
		t.Fatalf("valid non-oracle approximate lifecycle result: %v", err)
	}
	if _, ok := minimaQuantizedAddOwnerAdvance(^uint64(0), 1); ok {
		t.Fatal("accepted overflowing lifecycle owner advance")
	}
	for _, mutation := range []string{"missing_call", "duplicate_sequence", "wrong_generation", "wrong_dimension", "wrong_vector_m",
		"incomplete_serving", "search_budget", "rerank_budget", "exact_work_exceeds_eligible", "filtered_candidate_proof", "wrong_exact_live_rank",
		"timed_wrong_round_state", "warmup_owner_replay", "timed_pre_result_post_owner", "same_advance_different_owner",
		"schema_hash_drift", "shrunken_widths", "shadowed_exceeds_state", "duplicate_effective_metadata",
		"scalar_index_swap", "scalar_index_duplicate", "vector_ef_search_drift", "vector_identity_drift",
		"text_identity_drift", "required_capability_false", "forbidden_capability_true", "unknown_capability",
		"unfolded_preclose", "fold_coverage_advance", "reopen_owner_drift", "constant_owner_replay",
		"trace_score_mismatch", "phase_escape", "retained_failure", "old_schema_pass"} {
		t.Run(mutation, func(t *testing.T) {
			artifact := cloneMinimaArtifact(t, base)
			switch mutation {
			case "missing_call":
				r := artifact.RawEvidence["treedb"]
				r.QuantizedRequestEvidence = r.QuantizedRequestEvidence[:len(r.QuantizedRequestEvidence)-1]
				artifact.RawEvidence["treedb"] = r
			case "duplicate_sequence":
				r := artifact.RawEvidence["treedb"]
				r.QuantizedRequestEvidence[1].RequestSequence = 1
				artifact.RawEvidence["treedb"] = r
			case "wrong_generation":
				r := artifact.RawEvidence["treedb"]
				r.QuantizedRequestEvidence[0].ExpectedGeneration = 2
				artifact.RawEvidence["treedb"] = r
			case "wrong_dimension":
				artifact.Backends[0].Configuration["dimension"] = "9"
			case "wrong_vector_m":
				artifact.Backends[0].Configuration["vector_m"] = "32"
			case "incomplete_serving":
				artifact.Backends[0].Configuration["column_graph_serving"] = `{}`
			case "search_budget":
				r := artifact.RawEvidence["treedb"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.ScorePlane.Route != "quantized_rerank" {
						continue
					}
					request.DenseWork.Graph.BaseANNScored = (1 << 20) + 1
					request.ScorePlane.QuantizedScoreCalls = (1 << 20) + 1
					request.ScorePlane.QuantizedCodeBytesRead = ((1 << 20) + 1) * uint64(manifest.Config.Dimension)
					break
				}
				artifact.RawEvidence["treedb"] = r
			case "rerank_budget":
				var serving collections.ColumnGraphServingOptions
				if err := json.Unmarshal([]byte(artifact.Backends[0].Configuration["column_graph_serving"]), &serving); err != nil {
					t.Fatal(err)
				}
				serving.SearchCandidates = 64
				encoded, err := json.Marshal(serving)
				if err != nil {
					t.Fatal(err)
				}
				artifact.Backends[0].Configuration["column_graph_serving"] = string(encoded)
			case "exact_work_exceeds_eligible":
				r := artifact.RawEvidence["treedb"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.ScorePlane.Route != "quantized_rerank" {
						continue
					}
					excess := request.DenseWork.Graph.Filter.EligibleRows - request.DenseWork.Graph.ExactBaseScored + 1
					request.DenseWork.Graph.DeltaScored = excess
					request.ScorePlane.ExactSuffixScoreCalls = excess
					request.ScorePlane.ExactSuffixVectorBytesRead = excess * uint64(manifest.Config.Dimension*4)
					break
				}
				artifact.RawEvidence["treedb"] = r
			case "filtered_candidate_proof":
				r := artifact.RawEvidence["treedb"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.ScorePlane.Route == "quantized_rerank" {
						request.DenseWork.Graph.BaseCandidates = request.ScorePlane.RawRetainedCandidates - 1
						break
					}
				}
				artifact.RawEvidence["treedb"] = r
			case "wrong_exact_live_rank":
				r := artifact.RawEvidence["treedb"]
				spec := specs["small"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.OperationName != "update_visibility_probe" {
						continue
					}
					for result := range request.Results {
						document, _ := minimaQuantizedDocumentAtStage(&manifest, spec,
							fmt.Sprintf("minima/small/%06d", spec.EligibleStart+1+result), 7)
						request.Results[result] = minimaQuantizedResult{ID: document.ID, Content: document.Content,
							UserID: document.UserID, FPath: document.FPath, Score: minimaDocumentScore(document)}
					}
					break
				}
				artifact.RawEvidence["treedb"] = r
			case "timed_wrong_round_state":
				r := artifact.RawEvidence["treedb"]
				for _, query := range artifact.Backends[0].Operations.TimedExecutionTrace.Queries {
					if query.Round == 0 && query.Scenario == "all_match" {
						request := &r.QuantizedRequestEvidence[query.RequestSequence-1]
						request.DenseWork.Graph.Filter.EligibleRows = uint64(specs[query.Scenario].EligibleRows)
						break
					}
				}
				artifact.RawEvidence["treedb"] = r
			case "warmup_owner_replay":
				r := artifact.RawEvidence["treedb"]
				initial := r.QuantizedRequestEvidence[0].ScorePlane.Snapshot
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.OperationName == "empty_user_and_file_probes" {
						request.DenseWork.Graph.Snapshot, request.ScorePlane.Snapshot = initial, initial
						break
					}
				}
				artifact.RawEvidence["treedb"] = r
			case "timed_pre_result_post_owner":
				r := artifact.RawEvidence["treedb"]
				for _, query := range artifact.Backends[0].Operations.TimedExecutionTrace.Queries {
					insertion := artifact.Manifest.Operations[3].InsertRanges[query.Round]
					if query.Scenario != insertion.Scenario {
						continue
					}
					states, _ := minimaQuantizedTimedStates(&artifact.Manifest, query)
					before, _ := minimaQuantizedEligibleRowsAtStage(&artifact.Manifest, specs[query.Scenario], states[0].Stage)
					after, _ := minimaQuantizedEligibleRowsAtStage(&artifact.Manifest, specs[query.Scenario], states[1].Stage)
					if before == after {
						continue
					}
					snapshot := minimaQuantizedTestSnapshot(states[1])
					request := &r.QuantizedRequestEvidence[query.RequestSequence-1]
					request.DenseWork.Graph.Snapshot, request.ScorePlane.Snapshot = snapshot, snapshot
					break
				}
				artifact.RawEvidence["treedb"] = r
			case "same_advance_different_owner":
				r := artifact.RawEvidence["treedb"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.OperationName == "timed_search_with_batch_insert" {
						request.ScorePlane.Snapshot.CurrentManifest.Checksum++
						request.DenseWork.Graph.Snapshot = request.ScorePlane.Snapshot
						break
					}
				}
				artifact.RawEvidence["treedb"] = r
			case "schema_hash_drift":
				r := artifact.RawEvidence["treedb"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.OperationName == "timed_search_with_batch_insert" {
						request.ScorePlane.Snapshot.SchemaHash++
						request.DenseWork.Graph.Snapshot = request.ScorePlane.Snapshot
						break
					}
				}
				artifact.RawEvidence["treedb"] = r
			case "shrunken_widths":
				r := artifact.RawEvidence["treedb"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.ScorePlane.Route != "quantized_rerank" {
						continue
					}
					width := uint64(len(request.Results))
					request.ScorePlane.NormalizedCandidateWidth = width
					request.ScorePlane.RawCandidateWidth = width
					request.ScorePlane.RerankCandidateCap = width
					request.ScorePlane.RawRetainedCandidates = width
					request.ScorePlane.LiveShortlistCandidates = width
					request.ScorePlane.ActualRerankCandidates = width
					request.ScorePlane.ExactBaseRerankScoreCalls = width
					request.ScorePlane.ExactBaseVectorBytesRead = width * uint64(manifest.Config.Dimension*4)
					request.DenseWork.Graph.BaseCandidates = width
					request.DenseWork.Graph.ExactBaseScored = width
					request.DenseWork.Graph.BaseResultIDs = width
					break
				}
				artifact.RawEvidence["treedb"] = r
			case "shadowed_exceeds_state":
				r := artifact.RawEvidence["treedb"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.OperationName != "warmup_search" || request.ScorePlane.Route != "quantized_rerank" {
						continue
					}
					request.DenseWork.Graph.BaseShadowed = 1
					request.ScorePlane.LiveShortlistCandidates--
					request.ScorePlane.ActualRerankCandidates--
					request.ScorePlane.ExactBaseRerankScoreCalls--
					request.ScorePlane.ExactBaseVectorBytesRead -= uint64(manifest.Config.Dimension * 4)
					request.DenseWork.Graph.ExactBaseScored--
					request.DenseWork.Graph.BaseResultIDs--
					break
				}
				artifact.RawEvidence["treedb"] = r
			case "duplicate_effective_metadata":
				encoded := artifact.Backends[0].Configuration["effective_collection"]
				artifact.Backends[0].Configuration["effective_collection"] = strings.Replace(
					encoded, `"name":"owned"`, `"name":"owned","name":"owned"`, 1,
				)
			case "scalar_index_swap":
				mutateEffective(&artifact, func(info *documentservice.IndexInfo) {
					info.ScalarFields[0].IndexName, info.ScalarFields[1].IndexName =
						info.ScalarFields[1].IndexName, info.ScalarFields[0].IndexName
				})
			case "scalar_index_duplicate":
				mutateEffective(&artifact, func(info *documentservice.IndexInfo) {
					info.ScalarFields[1].IndexName = info.ScalarFields[0].IndexName
				})
			case "vector_ef_search_drift":
				mutateEffective(&artifact, func(info *documentservice.IndexInfo) { info.VectorEfSearch++ })
			case "vector_identity_drift":
				mutateEffective(&artifact, func(info *documentservice.IndexInfo) { info.VectorIndexName = "other" })
			case "text_identity_drift":
				mutateEffective(&artifact, func(info *documentservice.IndexInfo) { info.TextIndexName = "other" })
			case "required_capability_false":
				mutateEffective(&artifact, func(info *documentservice.IndexInfo) {
					info.Capabilities.ExactColumnGraphSearch = false
				})
			case "forbidden_capability_true":
				mutateEffective(&artifact, func(info *documentservice.IndexInfo) {
					info.Capabilities.ExactDenseScoring = true
				})
			case "unknown_capability":
				encoded := artifact.Backends[0].Configuration["effective_collection"]
				artifact.Backends[0].Configuration["effective_collection"] = strings.Replace(
					encoded, `"dense_vector_search":true`, `"dense_vector_search":true,"unknown":true`, 1,
				)
			case "unfolded_preclose":
				r := artifact.RawEvidence["treedb"]
				initial := r.QuantizedRequestEvidence[0].ScorePlane.Snapshot
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.OperationName == "preclose_reopen_baseline" {
						request.ScorePlane.Snapshot.BaseManifest = initial.BaseManifest
						request.ScorePlane.Snapshot.BaseCoverageLSN = initial.BaseCoverageLSN
						request.DenseWork.Graph.Snapshot = request.ScorePlane.Snapshot
						break
					}
				}
				artifact.RawEvidence["treedb"] = r
			case "fold_coverage_advance":
				r := artifact.RawEvidence["treedb"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.OperationName == "preclose_reopen_baseline" {
						request.ScorePlane.Snapshot.BaseCoverageLSN++
						request.ScorePlane.Snapshot.CurrentCoverageLSN++
						request.DenseWork.Graph.Snapshot = request.ScorePlane.Snapshot
						break
					}
				}
				artifact.RawEvidence["treedb"] = r
			case "reopen_owner_drift":
				r := artifact.RawEvidence["treedb"]
				for i := range r.QuantizedRequestEvidence {
					request := &r.QuantizedRequestEvidence[i]
					if request.OperationName == "post_reopen_parity" {
						request.ScorePlane.Snapshot.CurrentManifest.Checksum++
						request.ScorePlane.Snapshot.BaseManifest = request.ScorePlane.Snapshot.CurrentManifest
						request.DenseWork.Graph.Snapshot = request.ScorePlane.Snapshot
						break
					}
				}
				artifact.RawEvidence["treedb"] = r
			case "constant_owner_replay":
				r := artifact.RawEvidence["treedb"]
				initial := r.QuantizedRequestEvidence[0].ScorePlane.Snapshot
				for i := range r.QuantizedRequestEvidence {
					r.QuantizedRequestEvidence[i].DenseWork.Graph.Snapshot = initial
					r.QuantizedRequestEvidence[i].ScorePlane.Snapshot = initial
				}
				artifact.RawEvidence["treedb"] = r
			case "trace_score_mismatch":
				query := &artifact.Backends[0].Operations.TimedExecutionTrace.Queries[0]
				query.ActualScores[0] += 0.0000001
				artifact.Backends[0].Operations.TimedExecutionSHA256 = minimaTimedExecutionDigest(artifact.Backends[0].Operations.TimedExecutionTrace)
				r := artifact.RawEvidence["treedb"]
				r.TimedOverlap.TimedExecutionSHA256 = artifact.Backends[0].Operations.TimedExecutionSHA256
				artifact.RawEvidence["treedb"] = r
			case "phase_escape":
				r := artifact.RawEvidence["treedb"]
				boundary := r.PhaseAttribution.Phases[1]
				r.QuantizedRequestEvidence[0].StartedMonotonicNS = boundary.StartNanos - 1
				r.QuantizedRequestEvidence[0].EndedMonotonicNS = boundary.StartNanos
				artifact.RawEvidence["treedb"] = r
			case "retained_failure":
				artifact.Failures = []string{"query failed"}
			case "old_schema_pass":
				artifact.Schema, artifact.State, artifact.Passing = minimaMeasuredSchema, "pass", true
			}
			if validateMinimaArtifactTrusted(&artifact, nil, &plan) == nil {
				t.Fatal("accepted hostile quantized artifact")
			}
		})
	}
}
