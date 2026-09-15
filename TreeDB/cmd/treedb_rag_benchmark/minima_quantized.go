package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
)

const (
	minimaQuantizedArtifactSchema = "treedb_rag_application/minima_quantized_diagnostic_v1"
	minimaQuantizedProfileSchema  = "treedb_minima_quantized_profile/v1"
	minimaQuantizedPlanSchema     = "treedb_minima_quantized_plan/v1"
)

type minimaQuantizedProfile struct {
	Schema                    string                           `json:"schema"`
	Name                      string                           `json:"name"`
	QueryMode                 collections.VectorIndexQueryMode `json:"query_mode"`
	IndexName                 string                           `json:"index_name"`
	Codec                     string                           `json:"codec"`
	Version                   uint16                           `json:"version"`
	Calibration               string                           `json:"calibration"`
	QuantizedConfigHash       uint64                           `json:"quantized_config_hash"`
	RequestedEFSearch         uint64                           `json:"requested_ef_search"`
	RequestedRerankCandidates uint64                           `json:"requested_rerank_candidates"`
	NativeCommandVersion      uint64                           `json:"native_command_version"`
}

type minimaQuantizedPlanManifest struct {
	Schema              string               `json:"schema"`
	Fixture             string               `json:"fixture"`
	Config              minimaWorkloadConfig `json:"config"`
	CorpusSHA256        string               `json:"corpus_sha256"`
	QuerySHA256         string               `json:"query_sha256"`
	OperationSHA256     string               `json:"operation_sha256"`
	ExpectedStateSHA256 string               `json:"expected_state_sha256"`
}

type minimaQuantizedPlan struct {
	Schema            string                                `json:"schema"`
	Manifest          minimaQuantizedPlanManifest           `json:"manifest"`
	QuantizedProfile  minimaQuantizedProfile                `json:"quantized_profile"`
	VectorStrategy    string                                `json:"vector_strategy"`
	Transport         string                                `json:"transport"`
	DurabilityProfile string                                `json:"durability_profile"`
	VectorM           int                                   `json:"vector_m"`
	EFConstruction    int                                   `json:"ef_construction"`
	Serving           collections.ColumnGraphServingOptions `json:"serving"`
	SHA256            string                                `json:"-"`
}

func loadMinimaQuantizedPlan(path, expected string) (*minimaQuantizedPlan, error) {
	if path == "" && expected == "" {
		return nil, nil
	}
	if path == "" || !minimaExactHex(expected, sha256.Size) {
		return nil, fmt.Errorf("minima quantized plan requires path and external SHA256 pin")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 1<<20 {
		return nil, fmt.Errorf("minima quantized plan exceeds 1 MiB")
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != expected {
		return nil, fmt.Errorf("minima quantized plan bytes do not match external pin")
	}
	if err := minimaMeasuredJSON(raw, reflect.TypeFor[minimaQuantizedPlan](), "quantized_plan"); err != nil {
		return nil, err
	}
	var plan minimaQuantizedPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return nil, err
	}
	plan.SHA256 = expected
	if err := validateMinimaQuantizedPlan(&plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

func validateMinimaQuantizedPlan(plan *minimaQuantizedPlan) error {
	if plan == nil || plan.Schema != minimaQuantizedPlanSchema || plan.Manifest.Schema != minimaBoundedManifestSchema ||
		plan.Manifest.Fixture == "" || plan.VectorStrategy != "column_graph" || plan.Transport != "native" ||
		plan.DurabilityProfile != "command_wal_durable" || plan.VectorM != 16 || plan.EFConstruction <= 0 ||
		!minimaExactHex(plan.SHA256, sha256.Size) {
		return fmt.Errorf("minima quantized: invalid trusted plan")
	}
	rows := map[string]int{
		"bounded-50k": 50000, "bounded-250k": 250000,
		"bounded-500k": 500000, "bounded-1000k": 1000000,
	}[plan.Manifest.Fixture]
	frozen, err := buildMinimaBoundedManifest(rows)
	wantManifest := minimaQuantizedPlanManifest{
		Schema: frozen.Schema, Fixture: frozen.Fixture, Config: frozen.Config,
		CorpusSHA256: frozen.CorpusSHA256, QuerySHA256: frozen.QuerySHA256,
		OperationSHA256: frozen.OperationSHA256, ExpectedStateSHA256: frozen.ExpectedStateSHA256,
	}
	if rows == 0 || err != nil || !reflect.DeepEqual(plan.Manifest, wantManifest) {
		return fmt.Errorf("minima quantized: trusted plan does not select a frozen bounded manifest")
	}
	if err := validateMinimaQuantizedProfile(&plan.QuantizedProfile, plan.Manifest.Config.TopK); err != nil {
		return err
	}
	serving, err := json.Marshal(plan.Serving)
	if err != nil {
		return err
	}
	if _, err := minimaQuantizedServingOptions(string(serving)); err != nil {
		return fmt.Errorf("minima quantized: trusted plan serving limits: %w", err)
	}
	return nil
}

func validateMinimaQuantizedPlanBinding(plan *minimaQuantizedPlan, artifact *minimaArtifact) error {
	if err := validateMinimaQuantizedPlan(plan); err != nil {
		return err
	}
	manifest := artifact.Manifest
	wantManifest := minimaQuantizedPlanManifest{
		Schema: manifest.Schema, Fixture: manifest.Fixture, Config: manifest.Config,
		CorpusSHA256: manifest.CorpusSHA256, QuerySHA256: manifest.QuerySHA256,
		OperationSHA256: manifest.OperationSHA256, ExpectedStateSHA256: manifest.ExpectedStateSHA256,
	}
	if artifact.QuantizedPlanSHA256 != plan.SHA256 || !reflect.DeepEqual(plan.Manifest, wantManifest) ||
		artifact.QuantizedProfile == nil || *artifact.QuantizedProfile != plan.QuantizedProfile {
		return fmt.Errorf("minima quantized: artifact does not match trusted plan")
	}
	return nil
}

type minimaQuantizedResult struct {
	ID      string  `json:"id"`
	Content string  `json:"content"`
	UserID  string  `json:"user_id"`
	FPath   string  `json:"fpath"`
	Score   float64 `json:"score"`
}

type minimaQuantizedRequest struct {
	OperationName      string                                 `json:"operation_name"`
	Scenario           string                                 `json:"scenario"`
	RequestSequence    uint64                                 `json:"request_sequence"`
	Phase              string                                 `json:"phase"`
	LifetimeOrdinal    int                                    `json:"lifetime_ordinal"`
	StartedMonotonicNS int64                                  `json:"started_monotonic_ns"`
	EndedMonotonicNS   int64                                  `json:"ended_monotonic_ns"`
	Outcome            string                                 `json:"outcome"`
	Error              string                                 `json:"error,omitempty"`
	Transport          string                                 `json:"transport"`
	CommandVersion     uint64                                 `json:"command_version"`
	ExpectedGeneration uint64                                 `json:"expected_generation"`
	ResultCount        uint64                                 `json:"result_count"`
	Results            []minimaQuantizedResult                `json:"results"`
	DenseWork          *documentservice.DenseSearchWork       `json:"dense_work,omitempty"`
	ScorePlane         *collections.ColumnGraphScorePlaneWork `json:"score_plane,omitempty"`
}

func minimaQuantizedRequestPresence(raw []byte) error {
	if err := minimaMeasuredJSON(raw, reflect.TypeFor[[]minimaQuantizedRequest](), "quantized_request_evidence"); err != nil {
		return err
	}
	var requests []json.RawMessage
	if err := json.Unmarshal(raw, &requests); err != nil {
		return err
	}
	for i, encoded := range requests {
		request, err := minimaMeasuredObject(encoded)
		if err != nil {
			return err
		}
		for _, key := range []string{"result_count", "results"} {
			if _, ok := request[key]; !ok {
				return fmt.Errorf("quantized_request_evidence[%d].%s: missing", i, key)
			}
		}
		var outcome string
		if err := json.Unmarshal(request["outcome"], &outcome); err != nil {
			return err
		}
		if outcome == "success" {
			for _, key := range []string{"dense_work", "score_plane"} {
				if _, ok := request[key]; !ok {
					return fmt.Errorf("quantized_request_evidence[%d].%s: missing", i, key)
				}
			}
		}
		encodedProof, ok := request["score_plane"]
		if !ok {
			continue
		}
		proof, err := minimaMeasuredObject(encodedProof)
		if err != nil {
			return err
		}
		for _, key := range []string{"reason", "quantized_index_name", "quantized_codec", "quantized_version", "quantized_config_hash"} {
			if _, ok := proof[key]; !ok {
				return fmt.Errorf("quantized_request_evidence[%d].score_plane.%s: missing", i, key)
			}
		}
	}
	return nil
}

func minimaQuantizedPresence(raw []byte) error {
	root, err := minimaMeasuredObject(raw)
	if err != nil {
		return err
	}
	for _, forbidden := range []string{"freeze_sha256", "native_path_proof"} {
		if _, ok := root[forbidden]; ok {
			return fmt.Errorf("minima quantized: %s is forbidden", forbidden)
		}
	}
	if err := minimaMeasuredJSON(root["quantized_profile"], reflect.TypeFor[minimaQuantizedProfile](), "quantized_profile"); err != nil {
		return err
	}
	if _, ok := root["quantized_plan_sha256"]; !ok {
		return fmt.Errorf("minima quantized: quantized_plan_sha256 is missing")
	}
	backendRaw, err := minimaMeasuredObject(root["backend_raw_evidence"])
	if err != nil {
		// A pre-execution envelope may have an empty raw-evidence object.
		return err
	}
	if len(backendRaw) == 0 {
		return nil
	}
	tree, ok := backendRaw["treedb"]
	if !ok || len(backendRaw) != 1 {
		return fmt.Errorf("minima quantized: raw evidence must contain only treedb")
	}
	fields, err := minimaMeasuredObject(tree)
	if err != nil {
		return err
	}
	if _, measured := fields["request_evidence"]; measured {
		return fmt.Errorf("minima quantized: measured-v1 request evidence is forbidden")
	}
	requests, ok := fields["quantized_request_evidence"]
	if !ok {
		return nil // Incomplete execution is dispositioned by the semantic validator.
	}
	return minimaQuantizedRequestPresence(requests)
}

// Reject even explicit empty quantized fields under legacy/bounded schemas;
// semantic zero values cannot distinguish those from absent JSON fields.
func minimaNonQuantizedPresence(raw []byte) error {
	root, err := minimaMeasuredObject(raw)
	if err != nil {
		return err
	}
	if _, ok := root["quantized_profile"]; ok {
		return fmt.Errorf("minima artifact: quantized profile is forbidden under nonquantized schema")
	}
	if _, ok := root["quantized_plan_sha256"]; ok {
		return fmt.Errorf("minima artifact: quantized plan is forbidden under nonquantized schema")
	}
	backends, ok := root["backend_raw_evidence"]
	if !ok {
		return nil
	}
	backendRaw, err := minimaMeasuredObject(backends)
	if err != nil {
		return err
	}
	for name, encoded := range backendRaw {
		fields, err := minimaMeasuredObject(encoded)
		if err != nil {
			return err
		}
		if _, ok := fields["quantized_request_evidence"]; ok {
			return fmt.Errorf("minima artifact: %s quantized request evidence is forbidden under nonquantized schema", name)
		}
	}
	return nil
}

func validateMinimaQuantizedProfile(profile *minimaQuantizedProfile, topK int) error {
	if profile == nil || profile.Schema != minimaQuantizedProfileSchema || profile.Name != "minima_sq8" ||
		profile.QueryMode != collections.VectorIndexQueryModeQuantizedRerank || profile.IndexName != profile.Name ||
		profile.Codec != "scalar_u8" || profile.Version != 1 || profile.Calibration != "legacy" ||
		profile.QuantizedConfigHash != 0 || topK <= 0 || profile.RequestedEFSearch < uint64(topK) ||
		profile.RequestedEFSearch >= uint64(1)<<63 ||
		profile.RequestedRerankCandidates != profile.RequestedEFSearch || profile.NativeCommandVersion != 3 {
		return fmt.Errorf("minima quantized: invalid declared profile")
	}
	return nil
}

func minimaQuantizedSnapshotValid(snapshot collections.ColumnGraphQuerySnapshot, generation uint64) bool {
	manifest := func(value collections.ColumnGraphManifestWork) bool {
		return value.Generation != 0 && value.Format == "tcs1" && value.Version == 1 && value.Checksum != 0
	}
	return snapshot.Available && snapshot.SchemaGeneration == generation && snapshot.SchemaHash != 0 &&
		manifest(snapshot.BaseManifest) && manifest(snapshot.CurrentManifest) &&
		snapshot.BaseCoverageLSN != 0 && snapshot.CurrentCoverageLSN >= snapshot.BaseCoverageLSN &&
		snapshot.CurrentManifest.Generation >= snapshot.BaseManifest.Generation
}

func minimaQuantizedCounterSums(proof *collections.ColumnGraphScorePlaneWork) (uint64, uint64, bool) {
	if proof == nil || proof.ExactSmallFilterScoreCalls > ^uint64(0)-proof.ExactBaseRerankScoreCalls {
		return 0, 0, false
	}
	base := proof.ExactBaseRerankScoreCalls + proof.ExactSmallFilterScoreCalls
	if proof.ExactSuffixScoreCalls > ^uint64(0)-base {
		return 0, 0, false
	}
	return base, base + proof.ExactSuffixScoreCalls, true
}

func minimaQuantizedByteCountersValid(proof *collections.ColumnGraphScorePlaneWork, dimension uint64) bool {
	base, _, ok := minimaQuantizedCounterSums(proof)
	if !ok || dimension == 0 || proof.QuantizedScoreCalls > ^uint64(0)/dimension || dimension > ^uint64(0)/4 {
		return false
	}
	exactBytes := dimension * 4
	if base > ^uint64(0)/exactBytes || proof.ExactSuffixScoreCalls > ^uint64(0)/exactBytes {
		return false
	}
	return proof.QuantizedCodeBytesRead == proof.QuantizedScoreCalls*dimension &&
		proof.ExactBaseVectorBytesRead == base*exactBytes &&
		proof.ExactSuffixVectorBytesRead == proof.ExactSuffixScoreCalls*exactBytes
}

func minimaQuantizedInitialEligibleRows(spec minimaScenarioSpec, batchSize int) uint64 {
	tail := min(batchSize, max(1, spec.CorpusRows/8))
	loaded := spec.CorpusRows - tail
	rows := min(spec.EligibleStart+spec.EligibleRows, loaded) - spec.EligibleStart
	return uint64(max(0, rows))
}

// The proof's eligible count is snapshot state, not the static manifest total.
// Bind it to the small set of states reachable at the named public call.
func minimaQuantizedEligibleRowsAllowed(request minimaQuantizedRequest, spec minimaScenarioSpec, manifest *minimaManifest) bool {
	observed := request.DenseWork.Graph.Filter.EligibleRows
	initial, current := minimaQuantizedInitialEligibleRows(spec, manifest.Config.BatchSize), uint64(spec.EligibleRows)
	switch request.OperationName {
	case "warmup_search":
		return observed == initial
	case "timed_search_with_batch_insert":
		return observed == initial || observed == current
	}
	for _, operation := range manifest.Operations[4:] {
		before, after := current, current
		if operation.Target == spec.Name {
			switch operation.Effect {
			case "delete":
				if uint64(len(operation.IDs)) > after {
					return false
				}
				after -= uint64(len(operation.IDs))
			case "insert":
				for _, document := range operation.Documents {
					if minimaDocumentMatches(spec, document) {
						after++
					}
				}
			}
		}
		if operation.Name == request.OperationName {
			if operation.Target == spec.Name && (operation.Effect == "delete" || operation.Effect == "insert") {
				return observed == before || observed == after
			}
			return observed == before
		}
		current = after
	}
	switch request.OperationName {
	case "preclose_reopen_baseline", "post_reopen_parity", "final_manifest_and_oracle_comparison":
		return observed == current
	default:
		return false
	}
}

func minimaQuantizedAdmissibleDocuments(manifest *minimaManifest, spec minimaScenarioSpec, id string) []minimaGeneratedDocument {
	var out []minimaGeneratedDocument
	parts := strings.Split(id, "/")
	if len(parts) == 3 && parts[0] == "minima" && parts[1] == spec.Name {
		if ordinal, err := strconv.Atoi(parts[2]); err == nil {
			if document, err := minimaDocumentAt(spec, ordinal); err == nil && document.ID == id {
				out = append(out, document)
			}
		}
	}
	for _, operation := range manifest.Operations {
		if operation.Target != spec.Name {
			continue
		}
		for _, document := range operation.Documents {
			if document.ID == id {
				out = append(out, document)
			}
		}
	}
	return out
}

func validateMinimaQuantizedResults(results []minimaQuantizedResult, manifest *minimaManifest, spec minimaScenarioSpec) error {
	seen := make(map[string]bool, len(results))
	for i, result := range results {
		if result.ID == "" || result.Content == "" || result.UserID == "" || result.FPath == "" ||
			!finiteNonnegative(math.Abs(result.Score)) || result.Score < -1.000001 || result.Score > 1.000001 || seen[result.ID] {
			return fmt.Errorf("result %d has missing, duplicate or nonfinite fields", i)
		}
		seen[result.ID] = true
		if result.UserID != spec.UserID || (spec.Filter == "user_id+fpath" && result.FPath != spec.FPath) {
			return fmt.Errorf("result %d violates declared scalar filter", i)
		}
		matched := false
		for _, document := range minimaQuantizedAdmissibleDocuments(manifest, spec, result.ID) {
			if document.Content == result.Content && document.UserID == result.UserID && document.FPath == result.FPath &&
				math.Abs(minimaDocumentScore(document)-result.Score) <= manifest.Config.ScoreTolerance {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("result %d is not a manifest-derived full FP32 projection", i)
		}
		if i > 0 && (results[i-1].Score < result.Score ||
			(results[i-1].Score == result.Score && results[i-1].ID > result.ID)) {
			return fmt.Errorf("results are not stable (-score,id) order")
		}
	}
	return nil
}

func minimaQuantizedBaseOrdinal(spec minimaScenarioSpec, id string) (int, bool) {
	prefix := "minima/" + spec.Name + "/"
	if !strings.HasPrefix(id, prefix) {
		return 0, false
	}
	raw := strings.TrimPrefix(id, prefix)
	if len(raw) != 6 {
		return 0, false
	}
	ordinal, err := strconv.Atoi(raw)
	return ordinal, err == nil && ordinal >= 0 && ordinal < spec.CorpusRows
}

// Resolve one ID against a complete post-operation state without materializing
// the representative corpus. The manifest contains only two base insert ranges
// and a handful of explicit mutation documents, so this stays bounded.
func minimaQuantizedDocumentAtStage(manifest *minimaManifest, spec minimaScenarioSpec, id string, stage int) (minimaGeneratedDocument, bool) {
	var document minimaGeneratedDocument
	ordinal, base := minimaQuantizedBaseOrdinal(spec, id)
	live := false
	for _, operation := range manifest.Operations {
		if operation.Ordinal > stage {
			break
		}
		if base {
			for _, insertion := range operation.InsertRanges {
				if insertion.Scenario == spec.Name && ordinal >= insertion.Start && ordinal < insertion.Start+insertion.Rows {
					document, _ = minimaDocumentAt(spec, ordinal)
					live = true
				}
			}
		}
		if operation.Target != spec.Name {
			continue
		}
		if slices.Contains(operation.IDs, id) {
			live = false
		}
		for _, candidate := range operation.Documents {
			if candidate.ID != id {
				continue
			}
			if operation.Effect == "insert" {
				live = true
			}
			if live {
				document = candidate
			}
		}
	}
	return document, live
}

func minimaQuantizedEligibleRowsAtStage(manifest *minimaManifest, spec minimaScenarioSpec, stage int) (uint64, bool) {
	loaded := 0
	if stage >= 1 {
		loaded = spec.CorpusRows - min(manifest.Config.BatchSize, max(1, spec.CorpusRows/8))
	}
	if stage >= 3 {
		loaded = spec.CorpusRows
	}
	start, end := spec.EligibleStart, spec.EligibleStart+spec.EligibleRows
	count := max(0, min(end, loaded)-start)
	for _, operation := range manifest.Operations {
		if operation.Ordinal < 4 || operation.Ordinal > stage || operation.Target != spec.Name {
			continue
		}
		for _, id := range operation.IDs {
			document, live := minimaQuantizedDocumentAtStage(manifest, spec, id, operation.Ordinal-1)
			if live && minimaDocumentMatches(spec, document) {
				count--
			}
		}
		for _, document := range operation.Documents {
			before, live := minimaQuantizedDocumentAtStage(manifest, spec, document.ID, operation.Ordinal-1)
			beforeMatches := live && minimaDocumentMatches(spec, before)
			afterMatches := (live || operation.Effect == "insert") && minimaDocumentMatches(spec, document)
			if beforeMatches != afterMatches {
				if afterMatches {
					count++
				} else {
					count--
				}
			}
		}
	}
	return uint64(count), count >= 0
}

func minimaQuantizedExactRankingAtStage(manifest *minimaManifest, spec minimaScenarioSpec, stage int) ([]string, []float64, error) {
	eligible, ok := minimaQuantizedEligibleRowsAtStage(manifest, spec, stage)
	if !ok || eligible > minimaLookupLimit {
		return nil, nil, fmt.Errorf("state is not a typed-exact population")
	}
	loaded := 0
	if stage >= 1 {
		loaded = spec.CorpusRows - min(manifest.Config.BatchSize, max(1, spec.CorpusRows/8))
	}
	if stage >= 3 {
		loaded = spec.CorpusRows
	}
	candidates := make([]minimaScoredDocument, 0, min(int(eligible), manifest.Config.TopK)+8)
	seen := make(map[string]bool)
	for ordinal := spec.EligibleStart; ordinal < min(spec.EligibleStart+spec.EligibleRows, loaded); ordinal++ {
		id := fmt.Sprintf("minima/%s/%06d", spec.Name, ordinal)
		document, live := minimaQuantizedDocumentAtStage(manifest, spec, id, stage)
		if live && minimaDocumentMatches(spec, document) {
			candidates = append(candidates, minimaScoredDocument{ID: id, Score: minimaDocumentScore(document)})
			seen[id] = true
		}
	}
	for _, operation := range manifest.Operations {
		if operation.Ordinal > stage || operation.Target != spec.Name {
			continue
		}
		for _, candidate := range operation.Documents {
			if seen[candidate.ID] {
				continue
			}
			document, live := minimaQuantizedDocumentAtStage(manifest, spec, candidate.ID, stage)
			if live && minimaDocumentMatches(spec, document) {
				candidates = append(candidates, minimaScoredDocument{ID: document.ID, Score: minimaDocumentScore(document)})
				seen[document.ID] = true
			}
		}
	}
	slices.SortFunc(candidates, func(a, b minimaScoredDocument) int {
		if a.Score > b.Score {
			return -1
		}
		if a.Score < b.Score {
			return 1
		}
		return strings.Compare(a.ID, b.ID)
	})
	count := min(manifest.Config.TopK, len(candidates))
	ids, scores := make([]string, count), make([]float64, count)
	for i := range count {
		ids[i], scores[i] = candidates[i].ID, candidates[i].Score
	}
	return ids, scores, nil
}

func minimaQuantizedMatchingStates(request minimaQuantizedRequest, manifest *minimaManifest,
	spec minimaScenarioSpec, states []minimaQuantizedAllowedState) []minimaQuantizedAllowedState {
	matched := make([]minimaQuantizedAllowedState, 0, len(states))
	for _, state := range states {
		work, ok := minimaQuantizedStateWorkAtState(manifest, spec, state, *request.ScorePlane)
		if !ok || !minimaQuantizedStateWorkMatchesRequest(work, request) {
			continue
		}
		valid := true
		for _, result := range request.Results {
			document, live := minimaQuantizedDocumentAtStage(manifest, spec, result.ID, state.Stage)
			if !live || !minimaDocumentMatches(spec, document) || document.Content != result.Content ||
				document.UserID != result.UserID || document.FPath != result.FPath ||
				math.Abs(minimaDocumentScore(document)-result.Score) > manifest.Config.ScoreTolerance {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		if work.LiveBase+work.LiveSuffix <= minimaLookupLimit {
			ids, scores, err := minimaQuantizedExactRankingAtStage(manifest, spec, state.Stage)
			if err != nil {
				continue
			}
			actualIDs, actualScores := make([]string, len(request.Results)), make([]float64, len(request.Results))
			for i, result := range request.Results {
				actualIDs[i], actualScores[i] = result.ID, result.Score
			}
			if _, _, err := validateMinimaRanking(actualIDs, actualScores, ids, scores,
				manifest.Config.OrderTolerance, manifest.Config.ScoreTolerance); err != nil {
				continue
			}
		}
		matched = append(matched, state)
	}
	return matched
}

type minimaQuantizedPlanOption struct {
	EffectiveEF, RawWidth, RerankCap, ShadowAllowance uint64
}

type minimaQuantizedStateWork struct {
	LiveBase, LiveSuffix uint64
	Plans                []minimaQuantizedPlanOption
}

func minimaQuantizedPlanOptionFor(baseDomain, shadowAllowance uint64,
	proof collections.ColumnGraphScorePlaneWork) minimaQuantizedPlanOption {
	effectiveEF := min(baseDomain, max(proof.RequestedTopK, proof.RequestedEFSearch))
	rawWidth := baseDomain
	if shadowAllowance < baseDomain-effectiveEF {
		rawWidth = effectiveEF + shadowAllowance
	}
	rerankCap := min(baseDomain, effectiveEF, proof.RequestedRerankCandidates)
	return minimaQuantizedPlanOption{EffectiveEF: effectiveEF, RawWidth: rawWidth,
		RerankCap: rerankCap, ShadowAllowance: shadowAllowance}
}

// Reproduce both producer filter-preparation paths at one exact lifecycle
// state. A request-local current selection contains only surviving base rows;
// a cached immutable-base selection also retains every shadowed original row.
// Both share the same live base/suffix work and owner.
func minimaQuantizedStateWorkAtState(manifest *minimaManifest, spec minimaScenarioSpec,
	state minimaQuantizedAllowedState, proof collections.ColumnGraphScorePlaneWork) (minimaQuantizedStateWork, bool) {
	eligible, ok := minimaQuantizedEligibleRowsAtStage(manifest, spec, state.Stage)
	if !ok {
		return minimaQuantizedStateWork{}, false
	}
	if state.Folded {
		return minimaQuantizedStateWork{LiveBase: eligible,
			Plans: []minimaQuantizedPlanOption{minimaQuantizedPlanOptionFor(eligible, 0, proof)}}, true
	}
	initialBase := minimaQuantizedInitialEligibleRows(spec, manifest.Config.BatchSize)
	shadowed := make(map[string]bool)
	for _, operation := range manifest.Operations {
		if operation.Ordinal < 4 || operation.Ordinal > state.Stage || operation.Effect == "none" {
			continue
		}
		ids := append([]string(nil), operation.IDs...)
		for _, document := range operation.Documents {
			ids = append(ids, document.ID)
		}
		for _, id := range ids {
			if shadowed[id] {
				continue
			}
			document, live := minimaQuantizedDocumentAtStage(manifest, spec, id, 1)
			if live && minimaDocumentMatches(spec, document) {
				shadowed[id] = true
			}
		}
	}
	shadowAllowance := uint64(len(shadowed))
	if shadowAllowance > initialBase {
		return minimaQuantizedStateWork{}, false
	}
	liveBase := initialBase - shadowAllowance
	if eligible < liveBase {
		return minimaQuantizedStateWork{}, false
	}
	work := minimaQuantizedStateWork{LiveBase: liveBase, LiveSuffix: eligible - liveBase}
	work.Plans = append(work.Plans, minimaQuantizedPlanOptionFor(liveBase, 0, proof))
	cached := minimaQuantizedPlanOptionFor(liveBase+shadowAllowance, shadowAllowance, proof)
	if cached != work.Plans[0] {
		work.Plans = append(work.Plans, cached)
	}
	return work, true
}

func minimaQuantizedStateWorkMatchesRequest(work minimaQuantizedStateWork, request minimaQuantizedRequest) bool {
	if request.DenseWork == nil || request.ScorePlane == nil ||
		work.LiveBase > ^uint64(0)-work.LiveSuffix ||
		request.DenseWork.Graph.Filter.EligibleRows != work.LiveBase+work.LiveSuffix ||
		request.ScorePlane.ExactSuffixScoreCalls != work.LiveSuffix ||
		(request.ScorePlane.Route == "typed_exact" && request.ScorePlane.ExactSmallFilterScoreCalls != work.LiveBase) ||
		(request.ScorePlane.Route != "typed_exact" && request.ScorePlane.ExactSmallFilterScoreCalls != 0) {
		return false
	}
	for _, plan := range work.Plans {
		if request.ScorePlane.NormalizedCandidateWidth != plan.EffectiveEF ||
			request.ScorePlane.RawCandidateWidth != plan.RawWidth ||
			request.ScorePlane.RerankCandidateCap != plan.RerankCap {
			continue
		}
		switch request.ScorePlane.Route {
		case "typed_empty":
			if request.DenseWork.Graph.BaseShadowed == 0 {
				return true
			}
		case "typed_exact":
			if request.DenseWork.Graph.BaseShadowed == plan.ShadowAllowance {
				return true
			}
		case "quantized_rerank":
			if request.DenseWork.Graph.BaseShadowed <= plan.ShadowAllowance {
				return true
			}
		}
	}
	return false
}

func minimaQuantizedAddOwnerAdvance(value, advance uint64) (uint64, bool) {
	if value > ^uint64(0)-advance {
		return 0, false
	}
	return value + advance, true
}

func minimaQuantizedSnapshotMatchesLifecycle(snapshot, initial collections.ColumnGraphQuerySnapshot,
	state minimaQuantizedAllowedState) bool {
	wantGeneration, generationOK := minimaQuantizedAddOwnerAdvance(initial.CurrentManifest.Generation, state.OwnerAdvance)
	wantCoverage, coverageOK := minimaQuantizedAddOwnerAdvance(initial.CurrentCoverageLSN, state.OwnerAdvance)
	if !generationOK || !coverageOK || snapshot.SchemaGeneration != initial.SchemaGeneration ||
		snapshot.SchemaHash != initial.SchemaHash ||
		snapshot.CurrentManifest.Generation != wantGeneration || snapshot.CurrentCoverageLSN != wantCoverage {
		return false
	}
	if state.Folded {
		return snapshot.BaseManifest == snapshot.CurrentManifest &&
			snapshot.BaseCoverageLSN == snapshot.CurrentCoverageLSN
	}
	return snapshot.BaseManifest == initial.BaseManifest && snapshot.BaseCoverageLSN == initial.BaseCoverageLSN
}

func validateMinimaQuantizedRequest(request minimaQuantizedRequest, profile minimaQuantizedProfile,
	spec minimaScenarioSpec, manifest *minimaManifest) error {
	if err := validateMinimaQuantizedProfile(&profile, manifest.Config.TopK); err != nil {
		return err
	}
	if request.OperationName == "" || request.Scenario != spec.Name || request.RequestSequence == 0 ||
		request.Phase == "" || request.LifetimeOrdinal < 0 || request.LifetimeOrdinal > 1 ||
		request.StartedMonotonicNS < 0 || request.EndedMonotonicNS <= request.StartedMonotonicNS ||
		request.Outcome != "success" || request.Error != "" || request.Transport != "native" ||
		request.CommandVersion != profile.NativeCommandVersion || request.ExpectedGeneration == 0 ||
		request.DenseWork == nil || request.ScorePlane == nil {
		return fmt.Errorf("minima quantized request: incomplete public-call envelope")
	}
	work, proof := request.DenseWork, request.ScorePlane
	exactBaseCalls, exactCalls, countersOK := minimaQuantizedCounterSums(proof)
	if work.Version != 1 || !work.Completed || !work.Graph.Available || !work.Graph.Completed ||
		!work.Output.Attempted || !work.Output.Completed || work.Output.Missing != 0 ||
		proof.Version != 1 || !proof.Available || !proof.Completed || proof.Reason != "" ||
		proof.RequestedMode != profile.QueryMode || proof.EffectiveMode != profile.QueryMode ||
		proof.QuantizedIndexName != profile.IndexName || proof.QuantizedCodec != profile.Codec ||
		proof.QuantizedVersion != profile.Version || proof.QuantizedConfigHash != profile.QuantizedConfigHash ||
		proof.RequestedTopK != uint64(manifest.Config.TopK) || proof.RequestedEFSearch != profile.RequestedEFSearch ||
		proof.RequestedRerankCandidates != profile.RequestedRerankCandidates ||
		!reflect.DeepEqual(work.Graph.Snapshot, proof.Snapshot) ||
		!minimaQuantizedSnapshotValid(proof.Snapshot, request.ExpectedGeneration) {
		return fmt.Errorf("minima quantized request: profile or owner proof mismatch")
	}
	filter, output := work.Graph.Filter, work.Output
	if !countersOK || !minimaQuantizedEligibleRowsAllowed(request, spec, manifest) {
		return fmt.Errorf("minima quantized request: counter overflow or unreachable eligible-row state")
	}
	expectedResults := min(uint64(manifest.Config.TopK), filter.EligibleRows)
	if !filter.Attempted || !filter.Completed ||
		request.ResultCount != expectedResults || uint64(len(request.Results)) != expectedResults ||
		output.Requested != expectedResults || output.Fetched != expectedResults ||
		output.RetainedPayloadFetches != expectedResults ||
		output.JSONReconstructionRows != expectedResults || output.TypedColumnRows > expectedResults ||
		(expectedResults == 0 && output.OutputBytes != 0) || (expectedResults > 0 && output.OutputBytes == 0) ||
		work.Graph.BaseEdges != 0 || proof.QuantizedScoreCalls != work.Graph.BaseANNScored ||
		work.Graph.BaseCandidates > proof.QuantizedScoreCalls ||
		exactBaseCalls != work.Graph.ExactBaseScored ||
		work.Graph.BaseResultIDs != work.Graph.ExactBaseScored || proof.ExactSuffixScoreCalls != work.Graph.DeltaScored {
		return fmt.Errorf("minima quantized request: work/result cardinality mismatch (eligible=%d results=%d/%d output=%d/%d/%d/%d graph=%d/%d/%d proof=%d/%d/%d)",
			filter.EligibleRows, request.ResultCount, len(request.Results), output.Requested, output.Fetched,
			output.RetainedPayloadFetches, output.JSONReconstructionRows, work.Graph.BaseANNScored,
			work.Graph.ExactBaseScored, work.Graph.BaseResultIDs, proof.QuantizedScoreCalls,
			proof.ExactBaseRerankScoreCalls, proof.ExactSmallFilterScoreCalls)
	}
	normalizedLimit := max(proof.RequestedTopK, proof.RequestedEFSearch)
	expectedCap := min(proof.NormalizedCandidateWidth, proof.RequestedRerankCandidates)
	if proof.NormalizedCandidateWidth > normalizedLimit || proof.NormalizedCandidateWidth > proof.RawCandidateWidth ||
		proof.RerankCandidateCap != expectedCap || proof.LiveShortlistCandidates > proof.NormalizedCandidateWidth ||
		proof.RawRetainedCandidates > proof.RawCandidateWidth ||
		!minimaQuantizedByteCountersValid(proof, uint64(manifest.Config.Dimension)) {
		return fmt.Errorf("minima quantized request: planning or byte proof mismatch")
	}
	switch proof.Route {
	case "typed_empty":
		if work.Graph.Route != "typed_empty" || filter.EligibleRows != 0 || request.ResultCount != 0 ||
			proof.QuantizedScoreCalls != 0 || exactCalls != 0 || work.Graph.BaseCandidates != 0 ||
			proof.RawRetainedCandidates != 0 || proof.LiveShortlistCandidates != 0 ||
			proof.ActualRerankCandidates != 0 || work.Graph.BaseShadowed != 0 {
			return fmt.Errorf("minima quantized request: invalid typed-empty route")
		}
	case "typed_exact":
		if work.Graph.Route != "typed_exact" || filter.EligibleRows == 0 || filter.EligibleRows > minimaLookupLimit ||
			proof.QuantizedScoreCalls != 0 || proof.ExactBaseRerankScoreCalls != 0 ||
			work.Graph.BaseCandidates != 0 || proof.RawRetainedCandidates != 0 ||
			proof.LiveShortlistCandidates != 0 || proof.ActualRerankCandidates != 0 ||
			proof.ExactSmallFilterScoreCalls+proof.ExactSuffixScoreCalls != filter.EligibleRows ||
			exactCalls != filter.EligibleRows {
			return fmt.Errorf("minima quantized request: invalid typed-exact route")
		}
	case "quantized_rerank":
		if work.Graph.Route != "typed_hnsw" || filter.EligibleRows <= minimaLookupLimit ||
			proof.QuantizedScoreCalls == 0 || proof.ExactSmallFilterScoreCalls != 0 ||
			proof.NormalizedCandidateWidth == 0 || proof.RawCandidateWidth == 0 || proof.RerankCandidateCap == 0 ||
			proof.RawRetainedCandidates > proof.QuantizedScoreCalls ||
			proof.RawRetainedCandidates > work.Graph.BaseCandidates ||
			work.Graph.BaseShadowed > proof.RawRetainedCandidates ||
			proof.LiveShortlistCandidates != min(proof.NormalizedCandidateWidth, proof.RawRetainedCandidates-work.Graph.BaseShadowed) ||
			proof.ActualRerankCandidates != min(proof.LiveShortlistCandidates, proof.RerankCandidateCap) ||
			proof.ActualRerankCandidates != proof.ExactBaseRerankScoreCalls ||
			exactCalls > filter.EligibleRows ||
			request.ResultCount != min(proof.RequestedTopK, exactCalls) {
			return fmt.Errorf("minima quantized request: invalid quantized-rerank route")
		}
	default:
		return fmt.Errorf("minima quantized request: unknown route")
	}
	if err := validateMinimaQuantizedResults(request.Results, manifest, spec); err != nil {
		return fmt.Errorf("minima quantized request: %w", err)
	}
	return nil
}

type minimaQuantizedSearchKey struct {
	Operation string
	Scenario  string
}

func minimaQuantizedPhase(operation string) (string, int, bool) {
	switch operation {
	case "warmup_search":
		return "warmup_search", 0, true
	case "timed_search_with_batch_insert":
		return "timed_search_write_overlap", 0, true
	case "reindex_delete_by_user_and_fpath_while_reading", "reindex_replacement_insert_while_reading",
		"reindex_visibility_probe", "update_visibility_probe", "delete_visibility_probe", "empty_user_and_file_probes":
		return "lifecycle_mutations", 0, true
	case "preclose_reopen_baseline":
		return "pre_close_queries", 0, true
	case "post_reopen_parity":
		return "post_reopen", 1, true
	case "final_manifest_and_oracle_comparison":
		return "final_state_scroll_artifact_work", 1, true
	default:
		return "", 0, false
	}
}

func minimaExpectedQuantizedSearchCounts(manifest *minimaManifest) map[minimaQuantizedSearchKey]int {
	out := make(map[minimaQuantizedSearchKey]int)
	add := func(operation, scenario string) { out[minimaQuantizedSearchKey{operation, scenario}]++ }
	for _, operation := range manifest.Operations {
		for _, step := range operation.Schedule {
			add(operation.Name, step.Scenario)
		}
		if operation.TimedPlan != nil {
			for ordinal := 0; ordinal < operation.TimedPlan.QueryCount; ordinal++ {
				order := operation.TimedPlan.ScenarioOrder
				add(operation.Name, order[ordinal%len(order)])
			}
		}
		if operation.ConcurrentPlan != nil {
			for _, assignment := range operation.ConcurrentPlan.ReaderAssignments {
				add(operation.Name, assignment.Scenario)
			}
		}
		switch operation.Name {
		case "close":
			for _, spec := range manifest.Corpora {
				add("preclose_reopen_baseline", spec.Name)
			}
		case "reopen":
			for _, spec := range manifest.Corpora {
				add("post_reopen_parity", spec.Name)
			}
		}
	}
	return out
}

func minimaQuantizedOperationRank(operation string) (int, bool) {
	switch operation {
	case "warmup_search":
		return 2, true
	case "timed_search_with_batch_insert":
		return 3, true
	case "reindex_delete_by_user_and_fpath_while_reading":
		return 4, true
	case "reindex_replacement_insert_while_reading":
		return 5, true
	case "reindex_visibility_probe":
		return 6, true
	case "update_visibility_probe":
		return 8, true
	case "delete_visibility_probe":
		return 10, true
	case "empty_user_and_file_probes":
		return 11, true
	case "preclose_reopen_baseline":
		return 12, true
	case "post_reopen_parity":
		return 13, true
	case "final_manifest_and_oracle_comparison":
		return 15, true
	default:
		return 0, false
	}
}

type minimaQuantizedAllowedState struct {
	Stage        int
	OwnerAdvance uint64
	Folded       bool
}

func minimaQuantizedStaticStates(manifest *minimaManifest, operation string) ([]minimaQuantizedAllowedState, bool) {
	timedRounds := uint64(len(manifest.Operations[3].InsertRanges))
	switch operation {
	case "warmup_search":
		return []minimaQuantizedAllowedState{{Stage: 1}}, true
	case "reindex_visibility_probe":
		return []minimaQuantizedAllowedState{{Stage: 5, OwnerAdvance: timedRounds + 2}}, true
	case "update_visibility_probe":
		return []minimaQuantizedAllowedState{{Stage: 7, OwnerAdvance: timedRounds + 3}}, true
	case "delete_visibility_probe", "empty_user_and_file_probes":
		return []minimaQuantizedAllowedState{{Stage: 9, OwnerAdvance: timedRounds + 4}}, true
	case "preclose_reopen_baseline", "post_reopen_parity", "final_manifest_and_oracle_comparison":
		return []minimaQuantizedAllowedState{{Stage: 9, OwnerAdvance: timedRounds + 4, Folded: true}}, true
	default:
		return nil, false
	}
}

func minimaQuantizedTimedStates(manifest *minimaManifest, query minimaTimedQueryObservation) ([]minimaQuantizedAllowedState, bool) {
	plan := manifest.Operations[3].TimedPlan
	insertionRound := -1
	for index, round := range plan.Rounds {
		if round.InsertRange.Scenario == query.Scenario {
			insertionRound = index
			break
		}
	}
	if insertionRound < 0 {
		return nil, false
	}
	if query.Round < 0 || query.Round >= len(plan.Rounds) {
		return nil, false
	}
	stageAtAdvance := func(advance uint64) int {
		if uint64(insertionRound) < advance {
			return 3
		}
		return 1
	}
	before := uint64(query.Round)
	return []minimaQuantizedAllowedState{
		{Stage: stageAtAdvance(before), OwnerAdvance: before},
		{Stage: stageAtAdvance(before + 1), OwnerAdvance: before + 1},
	}, true
}

func minimaQuantizedConcurrentStates(manifest *minimaManifest, operationOrdinal int) ([]minimaQuantizedAllowedState, bool) {
	timedRounds := uint64(len(manifest.Operations[3].InsertRanges))
	switch operationOrdinal {
	case 4:
		return []minimaQuantizedAllowedState{
			{Stage: 3, OwnerAdvance: timedRounds},
			{Stage: 4, OwnerAdvance: timedRounds + 1},
		}, true
	case 5:
		return []minimaQuantizedAllowedState{
			{Stage: 4, OwnerAdvance: timedRounds + 1},
			{Stage: 5, OwnerAdvance: timedRounds + 2},
		}, true
	default:
		return nil, false
	}
}

func validateMinimaQuantizedLedger(artifact *minimaArtifact, raw minimaRawBackendEvidence) error {
	expected := minimaExpectedQuantizedSearchCounts(&artifact.Manifest)
	wantTotal := 0
	for _, count := range expected {
		wantTotal += count
	}
	if len(raw.QuantizedRequestEvidence) != wantTotal {
		return fmt.Errorf("minima quantized: request ledger has %d calls, want %d", len(raw.QuantizedRequestEvidence), wantTotal)
	}
	specs := minimaScenarioMap(&artifact.Manifest)
	actual := make(map[minimaQuantizedSearchKey]int)
	requests := make(map[uint64]minimaQuantizedRequest, len(raw.QuantizedRequestEvidence))
	allowedStates := make(map[uint64][]minimaQuantizedAllowedState, len(raw.QuantizedRequestEvidence))
	if raw.PhaseAttribution == nil {
		return fmt.Errorf("minima quantized: phase attribution is missing")
	}
	phaseBoundaries := make(map[string]minimaRawPhaseBoundary, len(raw.PhaseAttribution.Phases))
	for _, phase := range raw.PhaseAttribution.Phases {
		phaseBoundaries[phase.Name] = phase
	}
	lastOperationRank := -1
	for i, request := range raw.QuantizedRequestEvidence {
		if request.RequestSequence != uint64(i+1) {
			return fmt.Errorf("minima quantized: request sequences are not contiguous")
		}
		spec, ok := specs[request.Scenario]
		if !ok {
			return fmt.Errorf("minima quantized: request has unknown scenario")
		}
		if err := validateMinimaQuantizedRequest(request, *artifact.QuantizedProfile, spec, &artifact.Manifest); err != nil {
			return fmt.Errorf("minima quantized request %d: %w", request.RequestSequence, err)
		}
		phase, lifetime, ok := minimaQuantizedPhase(request.OperationName)
		if !ok || request.Phase != phase || request.LifetimeOrdinal != lifetime {
			return fmt.Errorf("minima quantized request %d: phase/lifetime mismatch", request.RequestSequence)
		}
		boundary, ok := phaseBoundaries[phase]
		if !ok || request.StartedMonotonicNS < boundary.StartNanos || request.EndedMonotonicNS > boundary.EndNanos {
			return fmt.Errorf("minima quantized request %d: interval is outside its attributed phase", request.RequestSequence)
		}
		rank, ok := minimaQuantizedOperationRank(request.OperationName)
		if !ok || rank < lastOperationRank {
			return fmt.Errorf("minima quantized request %d: operation chronology mismatch", request.RequestSequence)
		}
		lastOperationRank = rank
		if states, static := minimaQuantizedStaticStates(&artifact.Manifest, request.OperationName); static {
			allowedStates[request.RequestSequence] = states
		}
		actual[minimaQuantizedSearchKey{request.OperationName, request.Scenario}]++
		requests[request.RequestSequence] = request
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("minima quantized: request operation/scenario multiset mismatch")
	}
	joined := make(map[uint64]bool)
	join := func(sequence uint64, operation, scenario string, start, end int64, ids []string, scores []float64,
		states []minimaQuantizedAllowedState) error {
		request, ok := requests[sequence]
		gotIDs, gotScores := make([]string, len(request.Results)), make([]float64, len(request.Results))
		for i := range request.Results {
			gotIDs[i], gotScores[i] = request.Results[i].ID, request.Results[i].Score
		}
		if !ok || joined[sequence] || request.OperationName != operation || request.Scenario != scenario ||
			request.StartedMonotonicNS != start || request.EndedMonotonicNS != end ||
			!slices.Equal(gotIDs, ids) || !slices.Equal(gotScores, scores) || len(states) == 0 {
			return fmt.Errorf("trace does not join request %d", sequence)
		}
		joined[sequence] = true
		allowedStates[sequence] = states
		return nil
	}
	timedOperation := artifact.Manifest.Operations[3].Name
	for _, query := range artifact.Backends[0].Operations.TimedExecutionTrace.Queries {
		states, ok := minimaQuantizedTimedStates(&artifact.Manifest, query)
		if !ok {
			return fmt.Errorf("minima quantized: timed request has no insertion state")
		}
		if err := join(query.RequestSequence, timedOperation, query.Scenario,
			query.StartedMonotonicNS, query.EndedMonotonicNS, query.ActualIDs, query.ActualScores, states); err != nil {
			return err
		}
	}
	for _, operation := range artifact.Backends[0].Operations.ReindexExecutionTrace.Operations {
		name := artifact.Manifest.Operations[operation.OperationOrdinal].Name
		states, ok := minimaQuantizedConcurrentStates(&artifact.Manifest, operation.OperationOrdinal)
		if !ok {
			return fmt.Errorf("minima quantized: concurrent request has no owner state")
		}
		for _, query := range operation.ReaderQueries {
			if err := join(query.RequestSequence, name, query.Scenario,
				query.StartedMonotonicNS, query.EndedMonotonicNS, query.ActualIDs, query.ActualScores,
				states); err != nil {
				return err
			}
		}
	}
	var initialSnapshot collections.ColumnGraphQuerySnapshot
	initialAvailable := false
	for _, request := range raw.QuantizedRequestEvidence {
		if request.OperationName != "warmup_search" {
			continue
		}
		snapshot := request.ScorePlane.Snapshot
		if !initialAvailable {
			initialSnapshot, initialAvailable = snapshot, true
		} else if !reflect.DeepEqual(snapshot, initialSnapshot) {
			return fmt.Errorf("minima quantized: warmup owner snapshots differ")
		}
	}
	if !initialAvailable || initialSnapshot.BaseManifest != initialSnapshot.CurrentManifest ||
		initialSnapshot.BaseCoverageLSN != initialSnapshot.CurrentCoverageLSN {
		return fmt.Errorf("minima quantized: initial graph owner is not a folded build boundary")
	}
	type ownerStateKey struct {
		Advance uint64
		Folded  bool
	}
	ownerSnapshots := make(map[ownerStateKey]collections.ColumnGraphQuerySnapshot)
	for _, request := range raw.QuantizedRequestEvidence {
		if (request.OperationName == timedOperation ||
			request.OperationName == artifact.Manifest.Operations[4].Name ||
			request.OperationName == artifact.Manifest.Operations[5].Name) && !joined[request.RequestSequence] {
			return fmt.Errorf("minima quantized: concurrent request %d is not joined to its trace", request.RequestSequence)
		}
		states := allowedStates[request.RequestSequence]
		if len(states) == 0 {
			return fmt.Errorf("minima quantized: request %d has no operation state", request.RequestSequence)
		}
		matched := minimaQuantizedMatchingStates(request, &artifact.Manifest, specs[request.Scenario], states)
		owned := matched[:0]
		for _, state := range matched {
			if minimaQuantizedSnapshotMatchesLifecycle(request.ScorePlane.Snapshot, initialSnapshot, state) {
				owned = append(owned, state)
			}
		}
		if len(owned) != 1 {
			return fmt.Errorf("minima quantized request %d: results and owner do not identify one exact lifecycle state", request.RequestSequence)
		}
		key := ownerStateKey{Advance: owned[0].OwnerAdvance, Folded: owned[0].Folded}
		if prior, ok := ownerSnapshots[key]; ok && !reflect.DeepEqual(prior, request.ScorePlane.Snapshot) {
			return fmt.Errorf("minima quantized request %d: owner identity differs at the same lifecycle advance", request.RequestSequence)
		}
		ownerSnapshots[key] = request.ScorePlane.Snapshot
	}
	rows := make(map[string]minimaScenarioEvidence, len(artifact.Scenarios))
	for _, row := range artifact.Scenarios {
		rows[row.Scenario] = row
	}
	warmSeen := make(map[string]bool)
	for _, request := range raw.QuantizedRequestEvidence {
		row := rows[request.Scenario]
		ids, scores := make([]string, len(request.Results)), make([]float64, len(request.Results))
		for i, result := range request.Results {
			ids[i], scores[i] = result.ID, result.Score
		}
		switch request.OperationName {
		case "warmup_search":
			if !warmSeen[request.Scenario] {
				if !slices.Equal(ids, row.InitialActualIDs) || !slices.Equal(scores, row.InitialActualScores) {
					return fmt.Errorf("minima quantized: initial scenario evidence does not join request ledger")
				}
				warmSeen[request.Scenario] = true
			}
		case "preclose_reopen_baseline", "post_reopen_parity":
			if !slices.Equal(ids, row.ReopenIDs) || !slices.Equal(scores, row.ActualScores) {
				return fmt.Errorf("minima quantized: reopen parity does not join request ledger")
			}
		case "final_manifest_and_oracle_comparison":
			if !slices.Equal(ids, row.ActualIDs) || !slices.Equal(scores, row.ActualScores) {
				return fmt.Errorf("minima quantized: final scenario evidence does not join request ledger")
			}
		}
	}
	return nil
}

func minimaQuantizedServingOptions(raw string) (collections.ColumnGraphServingOptions, error) {
	var options collections.ColumnGraphServingOptions
	if err := minimaMeasuredJSON([]byte(raw), reflect.TypeFor[collections.ColumnGraphServingOptions](), "column_graph_serving"); err != nil {
		return options, err
	}
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		return options, err
	}
	p, o, f, m := options.Publication, options.Owners, options.Filter, options.Maintenance
	if p.Rows <= 0 || p.Tombstones <= 0 || p.ValueSlots <= 0 || p.OwnedBytes <= 0 || p.EncodedOutputBytes <= 0 ||
		o.Owners <= 0 || o.States <= 0 || o.StateBytes <= 0 || o.AssetBytes <= 0 ||
		o.Cold.ManifestRecords <= 0 || o.Cold.ManifestBytes <= 0 || o.Cold.AssetBytes <= 0 || o.Cold.DecodedTermBytes <= 0 ||
		options.CandidateOutput.Bytes <= 0 || options.CandidateOutput.AppenderAttempts <= 0 ||
		options.FoldRows <= 0 || options.SearchCandidates <= 0 ||
		f.SourceIDs <= 0 || f.SourceBytes <= 0 || f.RetainedBytes <= 0 || f.MappingWork <= 0 || f.InspectedEntries <= 0 ||
		m.NativeEntries <= 0 || m.ColumnSegments <= 0 || m.ManifestRecords <= 0 || m.LifecycleEntries <= 0 ||
		m.NativeBytes <= 0 || m.ColumnBytes <= 0 || m.ManifestBytes <= 0 || m.RetainedBytes <= 0 || m.PagerPages == 0 {
		return options, fmt.Errorf("minima quantized: column_graph serving limits are incomplete")
	}
	return options, nil
}

func minimaQuantizedIndexCapabilities() documentservice.IndexCapabilities {
	return documentservice.IndexCapabilities{
		DenseVectorSearch: true, MetadataFilters: true, KeywordSearch: true, HybridSearch: true,
		KeywordMetadataFilters: true, HybridMetadataFilters: true, BenchmarkLifecycle: true,
		VectorIndexMaintenance: true, NoDocumentVectorSearch: true, ColumnGraphVectorSearch: true,
		ExactColumnGraphSearch: true, QuantizedVectorSearch: true, QuantizedRerank: true,
		ScalarU8QuantizedRerank: true, TypedDenseQuantizedRerank: true,
	}
}

func validateMinimaQuantizedBackendConfig(backend minimaBackendEvidence, manifest *minimaManifest,
	profile minimaQuantizedProfile, plan *minimaQuantizedPlan) (uint64, uint64, error) {
	want := map[string]string{
		"vector_strategy": "column_graph", "transport": "native", "query_mode": "quantized_rerank",
		"vector_m":          "16",
		"quantized_profile": profile.Name, "quantized_index_name": profile.IndexName,
		"quantized_codec": profile.Codec, "quantized_version": "1", "quantized_calibration": "legacy",
		"quantized_config_hash": "0", "ef_search": strconv.FormatUint(profile.RequestedEFSearch, 10),
		"quantized_rerank_candidates": strconv.FormatUint(profile.RequestedRerankCandidates, 10),
		"native_command_version":      "3", "dimension": strconv.Itoa(manifest.Config.Dimension),
		"metric": manifest.Config.Metric, "scalar_fields": "meta.user_id,meta.fpath",
		"control_transport": "http", "profile": "command_wal_durable",
	}
	for key, value := range want {
		if backend.Configuration[key] != value {
			return 0, 0, fmt.Errorf("minima quantized: backend configuration %s mismatch", key)
		}
	}
	requestedConstruction, requestedErr := strconv.Atoi(backend.Configuration["ef_construction_requested"])
	effectiveConstruction, effectiveErr := strconv.Atoi(backend.Configuration["ef_construction_effective"])
	serving, servingErr := minimaQuantizedServingOptions(backend.Configuration["column_graph_serving"])
	if requestedErr != nil || effectiveErr != nil || servingErr != nil || requestedConstruction <= 0 ||
		effectiveConstruction != requestedConstruction || plan == nil || requestedConstruction != plan.EFConstruction ||
		serving != plan.Serving {
		return 0, 0, fmt.Errorf("minima quantized: graph construction or serving configuration mismatch")
	}
	var effective documentservice.IndexInfo
	effectiveRaw := []byte(backend.Configuration["effective_collection"])
	if minimaMeasuredJSON(effectiveRaw, reflect.TypeFor[documentservice.IndexInfo](), "effective_collection") != nil ||
		json.Unmarshal(effectiveRaw, &effective) != nil ||
		effective.Name == "" || effective.Name != backend.Configuration["collection"] ||
		effective.Dimension != manifest.Config.Dimension || string(effective.Metric) != manifest.Config.Metric ||
		effective.Generation == 0 || effective.ContractVersion != "treedb-document-service/v1alpha2" ||
		effective.EmbeddingField != manifest.Config.VectorField || effective.VectorIndexName != manifest.Config.VectorField ||
		effective.VectorStrategy != "column_graph" || effective.VectorM != plan.VectorM ||
		effective.VectorEfConstruction != requestedConstruction || effective.VectorEfSearch != 64 ||
		effective.TextField != manifest.Config.ContentField || effective.TextIndexName != manifest.Config.ContentField ||
		effective.DocumentType != "treedb_document_service_v1" || !effective.TypedInput ||
		effective.Capabilities != minimaQuantizedIndexCapabilities() || len(effective.QuantizedIndexes) != 1 {
		return 0, 0, fmt.Errorf("minima quantized: effective collection metadata is incomplete")
	}
	wantScalarFields := []documentservice.ScalarFieldInfo{
		{Field: "meta.fpath", IndexName: "meta_fpath", ValueType: documentservice.ScalarFieldString},
		{Field: "meta.user_id", IndexName: "meta_user_id", ValueType: documentservice.ScalarFieldString},
	}
	if !slices.Equal(effective.ScalarFields, wantScalarFields) {
		return 0, 0, fmt.Errorf("minima quantized: effective scalar-field metadata mismatch")
	}
	index := effective.QuantizedIndexes[0]
	if index.Name != profile.IndexName || index.Codec != profile.Codec || index.Version != uint32(profile.Version) ||
		index.ScalarU8Calibration != nil {
		return 0, 0, fmt.Errorf("minima quantized: effective score-plane declaration mismatch")
	}
	return effective.Generation, uint64(serving.SearchCandidates), nil
}

func validateMinimaQuantizedScenario(row minimaScenarioEvidence, spec minimaScenarioSpec, query minimaQuerySpec, topK int) error {
	if row.Backend != "treedb" || row.Scenario != spec.Name || row.CorpusRows != spec.CorpusRows ||
		row.ExpectedMatches != spec.EligibleRows || row.Selectivity != spec.Selectivity ||
		!slices.Equal(row.InitialOracleIDs, query.InitialOracleIDs) || !slices.Equal(row.FinalOracleIDs, query.FinalOracleIDs) ||
		!slices.Equal(row.InitialOracleScores, query.InitialOracleScores) || !slices.Equal(row.FinalOracleScores, query.FinalOracleScores) ||
		row.Errors != 0 || row.Timeouts != 0 || row.Correctness.CrossUserResults != 0 ||
		row.Correctness.StaleInsertIDs != 0 || row.Correctness.StaleUpdateIDs != 0 || row.Correctness.StaleDeleteIDs != 0 ||
		!row.ReopenParity || !slices.Equal(row.ReopenIDs, row.ActualIDs) || !row.Timing.Captured || !row.Resource.Captured ||
		!row.Visibility.GenerationConsistent || row.Route.Identity != "typed_column_graph_quantized_rerank" {
		return fmt.Errorf("minima quantized scenario: incomplete identity, lifecycle or correctness evidence")
	}
	if len(row.InitialActualIDs) != len(row.InitialActualScores) || len(row.ActualIDs) != len(row.ActualScores) ||
		len(row.ActualIDs) != min(spec.EligibleRows, topK) {
		return fmt.Errorf("minima quantized scenario: result cardinality mismatch")
	}
	intersection := 0
	for _, id := range row.ActualIDs {
		if slices.Contains(query.FinalOracleIDs, id) {
			intersection++
		}
	}
	union := len(row.ActualIDs) + len(query.FinalOracleIDs) - intersection
	recall := 1.0
	if len(query.FinalOracleIDs) > 0 {
		recall = float64(intersection) / float64(len(query.FinalOracleIDs))
	} else if len(row.ActualIDs) != 0 {
		recall = 0
	}
	overlap := 1.0
	if union > 0 {
		overlap = float64(intersection) / float64(union)
	}
	if !finiteFraction(row.Recall) || !finiteFraction(row.Overlap) ||
		math.Abs(row.Recall-recall) > 1e-12 || math.Abs(row.Overlap-overlap) > 1e-12 ||
		(spec.EligibleRows <= minimaLookupLimit && (!slices.Equal(row.ActualIDs, query.FinalOracleIDs) || recall != 1)) {
		return fmt.Errorf("minima quantized scenario: invalid quality observation")
	}
	return nil
}

func validateMinimaQuantizedArtifact(artifact *minimaArtifact, plan *minimaQuantizedPlan) error {
	if artifact == nil || artifact.Schema != minimaQuantizedArtifactSchema ||
		artifact.Manifest.Schema != minimaBoundedManifestSchema || artifact.State != "partial" || artifact.Passing ||
		artifact.Recommendation != "not_evaluated" || artifact.FreezeSHA256 != "" || artifact.NativePathProof != nil {
		return fmt.Errorf("minima quantized: invalid nonqualifying envelope")
	}
	if err := validateMinimaQuantizedProfile(artifact.QuantizedProfile, artifact.Manifest.Config.TopK); err != nil {
		return err
	}
	if err := validateMinimaQuantizedPlanBinding(plan, artifact); err != nil {
		return err
	}
	if err := validateMinimaManifest(&artifact.Manifest); err != nil {
		return err
	}
	if len(artifact.Backends) == 0 {
		if len(artifact.Scenarios) != 0 || len(artifact.RawEvidence) != 0 ||
			strings.TrimSpace(strings.Join(artifact.Failures, "")) == "" {
			return fmt.Errorf("minima quantized: pre-execution envelope requires one retained failure and no execution evidence")
		}
		return nil
	}
	if len(artifact.Backends) != 1 || artifact.Backends[0].Name != "treedb" {
		return fmt.Errorf("minima quantized: completed execution requires one TreeDB backend")
	}
	backend := artifact.Backends[0]
	if !backend.Operations.ManifestOrdered {
		if strings.TrimSpace(strings.Join(artifact.Failures, "")) == "" {
			return fmt.Errorf("minima quantized: incomplete execution lacks a retained failure")
		}
		return nil
	}
	if len(artifact.Failures) != 0 {
		return fmt.Errorf("minima quantized: completed execution carries retained failures")
	}
	generation, searchCandidates, err := validateMinimaQuantizedBackendConfig(backend, &artifact.Manifest, *artifact.QuantizedProfile, plan)
	if err != nil {
		return err
	}
	if err := validateMinimaBackendLifecycleMode(backend, &artifact.Manifest, false); err != nil {
		return err
	}
	raw, ok := artifact.RawEvidence["treedb"]
	if !ok || len(artifact.RawEvidence) != 1 {
		return fmt.Errorf("minima quantized: missing namespaced raw evidence")
	}
	if err := validateMinimaPeakRSSLifetimes(raw); err != nil {
		return err
	}
	if err := validateMinimaRawEvidence(artifact, map[string]minimaBackendEvidence{"treedb": backend}); err != nil {
		return err
	}
	if len(artifact.Scenarios) != len(artifact.Manifest.Corpora) {
		return fmt.Errorf("minima quantized: missing scenario evidence")
	}
	specs, queries := minimaScenarioMap(&artifact.Manifest), minimaQueryMap(&artifact.Manifest)
	seen := make(map[string]bool, len(artifact.Scenarios))
	for _, row := range artifact.Scenarios {
		spec, ok := specs[row.Scenario]
		if !ok || seen[row.Scenario] {
			return fmt.Errorf("minima quantized: duplicate or unknown scenario")
		}
		seen[row.Scenario] = true
		if err := validateMinimaQuantizedScenario(row, spec, queries[row.Scenario], artifact.Manifest.Config.TopK); err != nil {
			return fmt.Errorf("minima quantized: %s: %w", row.Scenario, err)
		}
	}
	if err := validateMinimaQuantizedLedger(artifact, raw); err != nil {
		return err
	}
	for _, request := range raw.QuantizedRequestEvidence {
		if request.ExpectedGeneration != generation {
			return fmt.Errorf("minima quantized: request generation differs from effective collection")
		}
		if request.DenseWork == nil {
			return fmt.Errorf("minima quantized: request exceeded declared search-candidate admission")
		}
		graph := request.DenseWork.Graph
		if graph.BaseANNScored > searchCandidates {
			return fmt.Errorf("minima quantized: request exceeded declared search-candidate admission")
		}
		remaining := searchCandidates - graph.BaseANNScored
		if graph.ExactBaseScored > remaining {
			return fmt.Errorf("minima quantized: request exceeded declared search-candidate admission")
		}
		if graph.DeltaScored > remaining-graph.ExactBaseScored {
			return fmt.Errorf("minima quantized: request exceeded declared search-candidate admission")
		}
	}
	return nil
}
