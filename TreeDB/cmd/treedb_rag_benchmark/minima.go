package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

const (
	minimaManifestSchema   = "treedb_rag_minima_manifest/v1"
	minimaArtifactSchema   = "treedb_rag_application/minima_v4"
	minimaDimension        = 8
	minimaLookupLimit      = 4096
	minimaRepresentative   = 500000
	minimaOrderTolerance   = 0
	minimaScoreTolerance   = 0.000001
	minimaGenerator        = "ordinal-v3:id=minima/<scenario>/<ordinal:06d>;content=minima:<scenario>:<ordinal>;vector=[s,sqrt(1-s*s),0x6],s=0.9-ordinal*0.000003;oracle=cosine(float32(vector),float32([1,0x7]));defaults=<scenario>-other-user-%02d(ordinal%31),/<scenario>/other/%02d.txt(ordinal%97)"
	minimaPayloadGenerator = "id=minima/<scenario>/<ordinal:06d>;content=minima:<scenario>:<ordinal>;defaults=<scenario>-other-user-%02d(ordinal%31),/<scenario>/other/%02d.txt(ordinal%97)"
)

type minimaWorkloadConfig struct {
	Collection        string   `json:"collection"`
	VectorField       string   `json:"vector_field"`
	ContentField      string   `json:"content_field"`
	Dimension         int      `json:"dimension"`
	Metric            string   `json:"metric"`
	ScalarFields      []string `json:"scalar_fields"`
	TopK              int      `json:"top_k"`
	BatchSize         int      `json:"batch_size"`
	ReaderConcurrency int      `json:"reader_concurrency"`
	WriterConcurrency int      `json:"writer_concurrency"`
	WarmupQueries     int      `json:"warmup_queries"`
	TimedQueries      int      `json:"timed_queries"`
	LookupLimit       int      `json:"lookup_limit"`
	OrderTolerance    int      `json:"order_tolerance"`
	ScoreTolerance    float64  `json:"score_tolerance"`
	Ordering          string   `json:"ordering"`
	Completion        string   `json:"completion_boundary"`
	Timing            string   `json:"timing_boundary"`
}

type minimaScenarioSpec struct {
	Name           string  `json:"name"`
	Shape          string  `json:"shape"`
	CorpusRows     int     `json:"corpus_rows"`
	EligibleStart  int     `json:"eligible_start"`
	EligibleRows   int     `json:"eligible_rows"`
	BroadStart     int     `json:"broad_start,omitempty"`
	BroadRows      int     `json:"broad_rows,omitempty"`
	NarrowStart    int     `json:"narrow_start,omitempty"`
	NarrowRows     int     `json:"narrow_rows,omitempty"`
	Filter         string  `json:"filter"`
	UserID         string  `json:"user_id,omitempty"`
	FPath          string  `json:"fpath,omitempty"`
	Selectivity    float64 `json:"selectivity"`
	DistractorRows int     `json:"closer_cross_tenant_distractor_rows"`
	Generator      string  `json:"generator"`
}

type minimaQuerySpec struct {
	Scenario            string    `json:"scenario"`
	Vector              []float64 `json:"vector"`
	InitialOracleIDs    []string  `json:"initial_oracle_ids"`
	InitialOracleScores []float64 `json:"initial_oracle_scores"`
	FinalOracleIDs      []string  `json:"final_oracle_ids"`
	FinalOracleScores   []float64 `json:"final_oracle_scores"`
}

type minimaInsertRange struct {
	Scenario string `json:"scenario"`
	Start    int    `json:"start"`
	Rows     int    `json:"rows"`
}

type minimaFilterInput struct {
	UserID string `json:"user_id"`
	FPath  string `json:"fpath"`
}

type minimaInterleaveStep struct {
	Ordinal      int    `json:"ordinal"`
	Actor        string `json:"actor"`
	Scenario     string `json:"scenario"`
	QueryOrdinal int    `json:"query_ordinal,omitempty"`
	InsertStart  int    `json:"insert_start,omitempty"`
	InsertRows   int    `json:"insert_rows,omitempty"`
}
type minimaTimedRound struct {
	Ordinal      int               `json:"ordinal"`
	QueryStart   int               `json:"query_start"`
	QueryCount   int               `json:"query_count"`
	InsertRange  minimaInsertRange `json:"insert_range"`
	StartBarrier string            `json:"start_barrier"`
	EndBarrier   string            `json:"end_barrier"`
}

type minimaTimedReaderPlan struct {
	QueryCount        int                `json:"query_count"`
	ScenarioOrder     []string           `json:"scenario_order"`
	ReaderConcurrency int                `json:"reader_concurrency"`
	WriterConcurrency int                `json:"writer_concurrency"`
	Assignment        string             `json:"assignment"`
	Rounds            []minimaTimedRound `json:"rounds"`
}
type minimaMutationReaderAssignment struct {
	Reader       int    `json:"reader"`
	QueryOrdinal int    `json:"query_ordinal"`
	Scenario     string `json:"scenario"`
}

type minimaConcurrentMutationPlan struct {
	Mutation          string                           `json:"mutation"`
	ReaderConcurrency int                              `json:"reader_concurrency"`
	ReaderAssignments []minimaMutationReaderAssignment `json:"reader_assignments"`
	StartBarrier      string                           `json:"start_barrier"`
	EndBarrier        string                           `json:"end_barrier"`
}

type minimaOperationSpec struct {
	Ordinal        int                           `json:"ordinal"`
	Name           string                        `json:"name"`
	Target         string                        `json:"target"`
	Timed          bool                          `json:"timed"`
	Effect         string                        `json:"effect"`
	InsertRanges   []minimaInsertRange           `json:"insert_ranges,omitempty"`
	Filter         *minimaFilterInput            `json:"filter,omitempty"`
	IDs            []string                      `json:"ids,omitempty"`
	Documents      []minimaGeneratedDocument     `json:"documents,omitempty"`
	Schedule       []minimaInterleaveStep        `json:"schedule,omitempty"`
	TimedPlan      *minimaTimedReaderPlan        `json:"timed_reader_plan,omitempty"`
	ConcurrentPlan *minimaConcurrentMutationPlan `json:"concurrent_mutation_plan,omitempty"`
}

type minimaManifest struct {
	Schema              string                `json:"schema"`
	Config              minimaWorkloadConfig  `json:"config"`
	Corpora             []minimaScenarioSpec  `json:"corpora"`
	Queries             []minimaQuerySpec     `json:"queries"`
	Operations          []minimaOperationSpec `json:"operations"`
	CorpusSHA256        string                `json:"corpus_sha256"`
	QuerySHA256         string                `json:"query_sha256"`
	OperationSHA256     string                `json:"operation_sha256"`
	ExpectedStateSHA256 string                `json:"expected_state_sha256"`
}

type minimaGeneratedDocument struct {
	ID      string    `json:"id"`
	Content string    `json:"content"`
	Vector  []float64 `json:"vector"`
	UserID  string    `json:"user_id"`
	FPath   string    `json:"fpath"`
}

func defaultMinimaWorkloadConfig() minimaWorkloadConfig {
	return minimaWorkloadConfig{
		Collection: "minima", VectorField: "embedding", ContentField: "content",
		Dimension: minimaDimension, Metric: "cosine", ScalarFields: []string{"user_id", "fpath"},
		TopK: 5, BatchSize: 256, ReaderConcurrency: 4, WriterConcurrency: 1,
		WarmupQueries: 32, TimedQueries: 1024, LookupLimit: minimaLookupLimit,
		OrderTolerance: minimaOrderTolerance, ScoreTolerance: minimaScoreTolerance,
		Ordering:   "manifest_ordinal_serial; timed_search_round_robin",
		Completion: "successful_mutation_response_before_visibility_probe",
		Timing:     "storage_calls_only; embeddings_and_llm_excluded; fetch_and_decode_separate",
	}
}

func buildMinimaManifest() minimaManifest {
	corpora := []minimaScenarioSpec{
		{Name: "small", Shape: "small", CorpusRows: 128, EligibleStart: 16, EligibleRows: 16, Filter: "user_id", UserID: "small-user", DistractorRows: 16},
		{Name: "all_match", Shape: "representative", CorpusRows: minimaRepresentative, EligibleStart: 0, EligibleRows: minimaRepresentative, Filter: "user_id", UserID: "all-user"},
		{Name: "over_limit_4097", Shape: "representative", CorpusRows: minimaRepresentative, EligibleStart: 1000, EligibleRows: minimaLookupLimit + 1, Filter: "user_id", UserID: "over-user", DistractorRows: 1000},
		{Name: "broad_10pct", Shape: "representative", CorpusRows: minimaRepresentative, EligibleStart: 20000, EligibleRows: 50000, Filter: "user_id", UserID: "broad-user", DistractorRows: 20000},
		{Name: "sparse_over_limit", Shape: "representative", CorpusRows: minimaRepresentative, EligibleStart: 100000, EligibleRows: minimaLookupLimit + 1, Filter: "user_id", UserID: "sparse-user", DistractorRows: 100000},
		{Name: "mixed_broad_narrow", Shape: "representative", CorpusRows: minimaRepresentative, EligibleStart: 10020, EligibleRows: 5, BroadStart: 10000, BroadRows: 50000, NarrowStart: 10020, NarrowRows: 5, Filter: "user_id+fpath", UserID: "mixed-user", FPath: "/mixed/target.txt", DistractorRows: 10000},
		{Name: "empty_user", Shape: "small", CorpusRows: 128, EligibleStart: 0, EligibleRows: 0, Filter: "user_id", UserID: "missing-user"},
		{Name: "empty_file", Shape: "small", CorpusRows: 128, EligibleStart: 0, EligibleRows: 0, BroadStart: 16, BroadRows: 16, Filter: "user_id+fpath", UserID: "empty-file-user", FPath: "/empty_file/missing.txt"},
	}
	for i := range corpora {
		corpora[i].Selectivity = float64(corpora[i].EligibleRows) / float64(corpora[i].CorpusRows)
		corpora[i].Generator = minimaGenerator
	}
	queries := make([]minimaQuerySpec, 0, len(corpora))
	for _, corpus := range corpora {
		ids, scores, _ := minimaGlobalOracle(corpora, corpus)
		vector := make([]float64, minimaDimension)
		vector[0] = 1
		queries = append(queries, minimaQuerySpec{Scenario: corpus.Name, Vector: vector, InitialOracleIDs: ids, InitialOracleScores: scores})
	}
	initialRanges, concurrentRanges := minimaInsertRanges(corpora, defaultMinimaWorkloadConfig().BatchSize)
	mixed := corpora[5]
	mixedIDs, _ := minimaOracle(mixed)
	replacements := make([]minimaGeneratedDocument, len(mixedIDs))
	for i := range replacements {
		replacements[i], _ = minimaDocumentAt(mixed, mixed.EligibleStart+i)
		replacements[i].ID = fmt.Sprintf("minima/%s/replacement/%06d", mixed.Name, i)
		replacements[i].Content = fmt.Sprintf("minima:%s:replacement:%d", mixed.Name, i)
	}
	updated, _ := minimaDocumentAt(corpora[0], corpora[0].EligibleStart)
	updated.Content = "minima:small:16:updated"
	deleted, _ := minimaDocumentAt(corpora[0], corpora[0].EligibleStart+1)
	filter := &minimaFilterInput{UserID: mixed.UserID, FPath: mixed.FPath}
	operations := []minimaOperationSpec{
		{Ordinal: 0, Name: "ensure_compatible_collection", Target: "all", Effect: "none"},
		{Ordinal: 1, Name: "initial_batch_insert", Target: "all", Effect: "insert", InsertRanges: initialRanges},
		{Ordinal: 2, Name: "warmup_search", Target: "all", Effect: "none", Schedule: minimaReaderSchedule(corpora, defaultMinimaWorkloadConfig().WarmupQueries)},
		{Ordinal: 3, Name: "timed_search_with_batch_insert", Target: "all", Timed: true, Effect: "insert", InsertRanges: concurrentRanges, TimedPlan: minimaTimedPlan(corpora, concurrentRanges, defaultMinimaWorkloadConfig())},
		{Ordinal: 4, Name: "reindex_delete_by_user_and_fpath_while_reading", Target: mixed.Name, Effect: "delete", Filter: filter, IDs: mixedIDs, ConcurrentPlan: newMinimaConcurrentMutationPlan("delete_by_user_id_and_fpath", mixed.Name, defaultMinimaWorkloadConfig())},
		{Ordinal: 5, Name: "reindex_replacement_insert_while_reading", Target: mixed.Name, Effect: "insert", Documents: replacements, ConcurrentPlan: newMinimaConcurrentMutationPlan("replacement_insert", mixed.Name, defaultMinimaWorkloadConfig())},
		{Ordinal: 6, Name: "reindex_visibility_probe", Target: mixed.Name, Effect: "none", Schedule: minimaReaderSchedule([]minimaScenarioSpec{mixed}, 1)},
		{Ordinal: 7, Name: "explicit_update", Target: corpora[0].Name, Effect: "update", Documents: []minimaGeneratedDocument{updated}},
		{Ordinal: 8, Name: "update_visibility_probe", Target: corpora[0].Name, Effect: "none", Schedule: minimaReaderSchedule(corpora[:1], 1)},
		{Ordinal: 9, Name: "explicit_delete", Target: corpora[0].Name, Effect: "delete", IDs: []string{deleted.ID}},
		{Ordinal: 10, Name: "delete_visibility_probe", Target: corpora[0].Name, Effect: "none", Schedule: minimaReaderSchedule(corpora[:1], 1)},
		{Ordinal: 11, Name: "empty_user_and_file_probes", Target: "empty_user,empty_file", Effect: "none", Schedule: minimaReaderSchedule(corpora[6:], 2)},
		{Ordinal: 12, Name: "close", Target: "all", Effect: "none"},
		{Ordinal: 13, Name: "reopen", Target: "all", Effect: "none"},
		{Ordinal: 14, Name: "idempotent_ensure_after_reopen", Target: "all", Effect: "none"},
		{Ordinal: 15, Name: "final_manifest_and_oracle_comparison", Target: "all", Effect: "none", Schedule: minimaReaderSchedule(corpora, len(corpora))},
	}
	manifest := minimaManifest{Schema: minimaManifestSchema, Config: defaultMinimaWorkloadConfig(), Corpora: corpora, Queries: queries, Operations: operations}
	manifest.CorpusSHA256 = minimaDigest(corpora)
	manifest.OperationSHA256 = minimaDigest(operations)
	applied, _ := minimaApplyOperations(&manifest)
	manifest.ExpectedStateSHA256 = minimaDigest(applied.Summary)
	for i, corpus := range corpora {
		manifest.Queries[i].FinalOracleIDs, manifest.Queries[i].FinalOracleScores = minimaFinalOracleFromState(corpus, applied)
	}
	manifest.QuerySHA256 = minimaDigest(manifest.Queries)
	return manifest
}
func minimaInsertRanges(corpora []minimaScenarioSpec, batchSize int) ([]minimaInsertRange, []minimaInsertRange) {
	initial := make([]minimaInsertRange, 0, len(corpora))
	concurrent := make([]minimaInsertRange, 0, len(corpora))
	for _, corpus := range corpora {
		rows := min(batchSize, max(1, corpus.CorpusRows/8))
		initial = append(initial, minimaInsertRange{Scenario: corpus.Name, Start: 0, Rows: corpus.CorpusRows - rows})
		concurrent = append(concurrent, minimaInsertRange{Scenario: corpus.Name, Start: corpus.CorpusRows - rows, Rows: rows})
	}
	return initial, concurrent
}

func minimaReaderSchedule(corpora []minimaScenarioSpec, count int) []minimaInterleaveStep {
	steps := make([]minimaInterleaveStep, count)
	for i := range steps {
		steps[i] = minimaInterleaveStep{Ordinal: i, Actor: "reader", Scenario: corpora[i%len(corpora)].Name, QueryOrdinal: i}
	}
	return steps
}

func minimaTimedPlan(corpora []minimaScenarioSpec, ranges []minimaInsertRange, config minimaWorkloadConfig) *minimaTimedReaderPlan {
	perRound := config.TimedQueries / len(ranges)
	plan := &minimaTimedReaderPlan{
		QueryCount: config.TimedQueries, ReaderConcurrency: config.ReaderConcurrency, WriterConcurrency: config.WriterConcurrency,
		Assignment: fmt.Sprintf("round=ordinal/%d;reader=ordinal%%%d;scenario=scenario_order[ordinal%%%d]", perRound, config.ReaderConcurrency, len(corpora)),
	}
	for _, corpus := range corpora {
		plan.ScenarioOrder = append(plan.ScenarioOrder, corpus.Name)
	}
	for i, insertion := range ranges {
		plan.Rounds = append(plan.Rounds, minimaTimedRound{
			Ordinal: i, QueryStart: i * perRound, QueryCount: perRound, InsertRange: insertion,
			StartBarrier: "round_start_readers_and_writer", EndBarrier: "round_end_queries_and_insert_complete",
		})
	}
	return plan
}

func newMinimaConcurrentMutationPlan(mutation, scenario string, config minimaWorkloadConfig) *minimaConcurrentMutationPlan {
	plan := &minimaConcurrentMutationPlan{
		Mutation: mutation, ReaderConcurrency: config.ReaderConcurrency,
		StartBarrier: "reindex_start_all_readers_and_writer",
		EndBarrier:   "reindex_end_all_readers_and_mutation_complete",
	}
	for reader := range config.ReaderConcurrency {
		plan.ReaderAssignments = append(plan.ReaderAssignments, minimaMutationReaderAssignment{
			Reader: reader, QueryOrdinal: reader, Scenario: scenario,
		})
	}
	return plan
}

type minimaStateDocument struct {
	ID      string `json:"id"`
	Content string `json:"content"`
	UserID  string `json:"user_id"`
	FPath   string `json:"fpath"`
}

type minimaStateMutation struct {
	Ordinal      int                   `json:"ordinal"`
	Effect       string                `json:"effect"`
	Target       string                `json:"target"`
	InsertRanges []minimaInsertRange   `json:"insert_ranges,omitempty"`
	IDs          []string              `json:"ids,omitempty"`
	Documents    []minimaStateDocument `json:"documents,omitempty"`
}

type minimaPostOperationState struct {
	BasePayloadSHA256 string                `json:"base_payload_sha256"`
	LiveRows          map[string]int        `json:"live_rows"`
	Mutations         []minimaStateMutation `json:"mutations"`
}

type minimaAppliedState struct {
	Summary   minimaPostOperationState
	Deleted   map[string]map[string]bool
	Documents map[string]map[string]minimaGeneratedDocument
}

func minimaApplyOperations(manifest *minimaManifest) (minimaAppliedState, error) {
	applied := minimaAppliedState{
		Summary: minimaPostOperationState{BasePayloadSHA256: minimaPayloadCorpusDigest(manifest.Corpora), LiveRows: map[string]int{}},
		Deleted: map[string]map[string]bool{}, Documents: map[string]map[string]minimaGeneratedDocument{},
	}
	specs := minimaScenarioMap(manifest)
	for name := range specs {
		applied.Deleted[name] = map[string]bool{}
		applied.Documents[name] = map[string]minimaGeneratedDocument{}
	}
	for _, operation := range manifest.Operations {
		switch operation.Effect {
		case "none":
			continue
		case "insert":
			for _, insertion := range operation.InsertRanges {
				spec, ok := specs[insertion.Scenario]
				if !ok || insertion.Start < 0 || insertion.Rows <= 0 || insertion.Start+insertion.Rows > spec.CorpusRows {
					return minimaAppliedState{}, fmt.Errorf("minima state: invalid insert range in operation %d", operation.Ordinal)
				}
				applied.Summary.LiveRows[insertion.Scenario] += insertion.Rows
			}
			if len(operation.Documents) > 0 {
				if _, ok := specs[operation.Target]; !ok {
					return minimaAppliedState{}, fmt.Errorf("minima state: insert targets unknown scenario %q", operation.Target)
				}
				applied.Summary.LiveRows[operation.Target] += len(operation.Documents)
				for _, document := range operation.Documents {
					delete(applied.Deleted[operation.Target], document.ID)
					applied.Documents[operation.Target][document.ID] = document
				}
			}
		case "delete":
			if _, ok := specs[operation.Target]; !ok || len(operation.IDs) == 0 {
				return minimaAppliedState{}, fmt.Errorf("minima state: invalid delete operation %d", operation.Ordinal)
			}
			applied.Summary.LiveRows[operation.Target] -= len(operation.IDs)
			for _, id := range operation.IDs {
				applied.Deleted[operation.Target][id] = true
				delete(applied.Documents[operation.Target], id)
			}
		case "update":
			if _, ok := specs[operation.Target]; !ok || len(operation.Documents) == 0 {
				return minimaAppliedState{}, fmt.Errorf("minima state: invalid update operation %d", operation.Ordinal)
			}
			for _, document := range operation.Documents {
				applied.Documents[operation.Target][document.ID] = document
			}
		default:
			return minimaAppliedState{}, fmt.Errorf("minima state: unknown effect %q", operation.Effect)
		}
		applied.Summary.Mutations = append(applied.Summary.Mutations, minimaStateMutation{
			Ordinal: operation.Ordinal, Effect: operation.Effect, Target: operation.Target,
			InsertRanges: operation.InsertRanges, IDs: operation.IDs, Documents: minimaStateDocuments(operation.Documents),
		})
	}
	for name, spec := range specs {
		want := spec.CorpusRows
		if name == "small" {
			want--
		}
		if applied.Summary.LiveRows[name] != want {
			return minimaAppliedState{}, fmt.Errorf("minima state: %s live rows=%d want %d", name, applied.Summary.LiveRows[name], want)
		}
	}
	return applied, nil
}

func minimaExpectedStateHash(manifest *minimaManifest) (string, error) {
	applied, err := minimaApplyOperations(manifest)
	if err != nil {
		return "", err
	}
	return minimaDigest(applied.Summary), nil
}

type minimaPayloadCorpusSpec struct {
	Name             string  `json:"name"`
	Shape            string  `json:"shape"`
	CorpusRows       int     `json:"corpus_rows"`
	EligibleStart    int     `json:"eligible_start"`
	EligibleRows     int     `json:"eligible_rows"`
	BroadStart       int     `json:"broad_start,omitempty"`
	BroadRows        int     `json:"broad_rows,omitempty"`
	NarrowStart      int     `json:"narrow_start,omitempty"`
	NarrowRows       int     `json:"narrow_rows,omitempty"`
	Filter           string  `json:"filter"`
	UserID           string  `json:"user_id,omitempty"`
	FPath            string  `json:"fpath,omitempty"`
	Selectivity      float64 `json:"selectivity"`
	PayloadGenerator string  `json:"payload_generator"`
}

func minimaPayloadCorpusDigest(corpora []minimaScenarioSpec) string {
	payload := make([]minimaPayloadCorpusSpec, len(corpora))
	for i, corpus := range corpora {
		payload[i] = minimaPayloadCorpusSpec{
			Name: corpus.Name, Shape: corpus.Shape, CorpusRows: corpus.CorpusRows,
			EligibleStart: corpus.EligibleStart, EligibleRows: corpus.EligibleRows,
			BroadStart: corpus.BroadStart, BroadRows: corpus.BroadRows,
			NarrowStart: corpus.NarrowStart, NarrowRows: corpus.NarrowRows,
			Filter: corpus.Filter, UserID: corpus.UserID, FPath: corpus.FPath, Selectivity: corpus.Selectivity,
			PayloadGenerator: minimaPayloadGenerator,
		}
	}
	return minimaDigest(payload)
}

func minimaStateDocuments(documents []minimaGeneratedDocument) []minimaStateDocument {
	out := make([]minimaStateDocument, len(documents))
	for i, document := range documents {
		out[i] = minimaStateDocument{ID: document.ID, Content: document.Content, UserID: document.UserID, FPath: document.FPath}
	}
	return out
}

func minimaDigest(value any) string {
	raw, _ := json.Marshal(value)
	return artifactHash("", raw).SHA256
}

type minimaScoredDocument struct {
	ID    string
	Score float64
}

func minimaFinalOracleFromState(spec minimaScenarioSpec, applied minimaAppliedState) ([]string, []float64) {
	deleted := applied.Deleted[spec.Name]
	documents := applied.Documents[spec.Name]
	candidates := make([]minimaScoredDocument, 0, 5+len(deleted)+len(documents))
	seen := make(map[string]bool, cap(candidates))
	needed := 5 + len(deleted) + len(documents)
	for ordinal := spec.EligibleStart; ordinal < spec.EligibleStart+spec.EligibleRows && len(candidates) < needed; ordinal++ {
		document, _ := minimaDocumentAt(spec, ordinal)
		if deleted[document.ID] {
			continue
		}
		if updated, ok := documents[document.ID]; ok {
			document = updated
		}
		if minimaDocumentMatches(spec, document) {
			candidates = append(candidates, minimaScoredDocument{ID: document.ID, Score: minimaDocumentScore(document)})
			seen[document.ID] = true
		}
	}
	for id, document := range documents {
		if !seen[id] && !deleted[id] && minimaDocumentMatches(spec, document) {
			candidates = append(candidates, minimaScoredDocument{ID: id, Score: minimaDocumentScore(document)})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return candidates[i].ID < candidates[j].ID
		}
		return candidates[i].Score > candidates[j].Score
	})
	count := min(5, len(candidates))
	ids := make([]string, count)
	scores := make([]float64, count)
	for i := range count {
		ids[i], scores[i] = candidates[i].ID, candidates[i].Score
	}
	return ids, scores
}

func minimaDocumentMatches(spec minimaScenarioSpec, document minimaGeneratedDocument) bool {
	if spec.Filter == "user_id" {
		return document.UserID == spec.UserID
	}
	return document.UserID == spec.UserID && document.FPath == spec.FPath
}

func minimaDocumentScore(document minimaGeneratedDocument) float64 {
	var squared float64
	for _, value := range document.Vector {
		stored := float32(value)
		squared += float64(stored) * float64(stored)
	}
	if squared == 0 {
		return math.Inf(-1)
	}
	return float64(float32(document.Vector[0])) / math.Sqrt(squared)
}

func minimaDocumentAt(spec minimaScenarioSpec, ordinal int) (minimaGeneratedDocument, error) {
	if ordinal < 0 || ordinal >= spec.CorpusRows {
		return minimaGeneratedDocument{}, fmt.Errorf("minima document ordinal %d outside [0,%d)", ordinal, spec.CorpusRows)
	}
	userID, fpath := minimaDocumentScalarsAt(spec, ordinal)
	// FMA fixes the fixture's rounding across architectures and compiler optimizations.
	score := math.FMA(-float64(ordinal), 0.000003, 0.9)
	vector := make([]float64, minimaDimension)
	vector[0], vector[1] = score, math.Sqrt(math.FMA(-score, score, 1))
	return minimaGeneratedDocument{
		ID: fmt.Sprintf("minima/%s/%06d", spec.Name, ordinal), Content: fmt.Sprintf("minima:%s:%d", spec.Name, ordinal),
		Vector: vector, UserID: userID, FPath: fpath,
	}, nil
}

func minimaDefaultScalarsAt(spec minimaScenarioSpec, ordinal int) (string, string) {
	return fmt.Sprintf("%s-other-user-%02d", spec.Name, ordinal%31),
		fmt.Sprintf("/%s/other/%02d.txt", spec.Name, ordinal%97)
}

func minimaDocumentScalarsAt(spec minimaScenarioSpec, ordinal int) (string, string) {
	userID, fpath := minimaDefaultScalarsAt(spec, ordinal)
	if spec.Filter == "user_id" && ordinal >= spec.EligibleStart && ordinal < spec.EligibleStart+spec.EligibleRows {
		userID = spec.UserID
	}
	if spec.Filter == "user_id+fpath" {
		if ordinal >= spec.BroadStart && ordinal < spec.BroadStart+spec.BroadRows {
			userID = spec.UserID
		}
		if ordinal >= spec.NarrowStart && ordinal < spec.NarrowStart+spec.NarrowRows {
			fpath = spec.FPath
		}
	}
	return userID, fpath
}

func minimaGlobalOracle(corpora []minimaScenarioSpec, target minimaScenarioSpec) ([]string, []float64, int) {
	candidates := make([]minimaScoredDocument, 0, len(corpora)*5)
	matches := 0
	for _, corpus := range corpora {
		if corpus.UserID != target.UserID {
			continue
		}
		start, rows := 0, 0
		switch {
		case target.Filter == "user_id" && corpus.Filter == "user_id":
			start, rows = corpus.EligibleStart, corpus.EligibleRows
		case target.Filter == "user_id" && corpus.Filter == "user_id+fpath":
			start, rows = corpus.BroadStart, corpus.BroadRows
		case target.Filter == "user_id+fpath" && corpus.Filter == "user_id+fpath" && corpus.FPath == target.FPath:
			start, rows = corpus.NarrowStart, corpus.NarrowRows
		}
		matches += rows
		for ordinal := start; ordinal < start+min(5, rows); ordinal++ {
			document, _ := minimaDocumentAt(corpus, ordinal)
			if minimaDocumentMatches(target, document) {
				candidates = append(candidates, minimaScoredDocument{ID: document.ID, Score: minimaDocumentScore(document)})
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return candidates[i].ID < candidates[j].ID
		}
		return candidates[i].Score > candidates[j].Score
	})
	count := min(5, len(candidates))
	ids := make([]string, count)
	scores := make([]float64, count)
	for i := range count {
		ids[i], scores[i] = candidates[i].ID, candidates[i].Score
	}
	return ids, scores, matches
}

func minimaOracle(spec minimaScenarioSpec) ([]string, []float64) {
	count := min(5, spec.EligibleRows)
	ids := make([]string, count)
	scores := make([]float64, count)
	for i := range count {
		document, _ := minimaDocumentAt(spec, spec.EligibleStart+i)
		ids[i], scores[i] = document.ID, minimaDocumentScore(document)
	}
	return ids, scores
}

func validateMinimaManifest(manifest *minimaManifest) error {
	if manifest == nil || manifest.Schema != minimaManifestSchema {
		return fmt.Errorf("minima manifest: missing schema")
	}
	if manifest.CorpusSHA256 != minimaDigest(manifest.Corpora) || manifest.QuerySHA256 != minimaDigest(manifest.Queries) || manifest.OperationSHA256 != minimaDigest(manifest.Operations) {
		return fmt.Errorf("minima manifest: corpus/query/operation hash mismatch")
	}
	if err := validateMinimaScalarNamespaces(manifest); err != nil {
		return err
	}
	if err := validateMinimaTimedPlan(manifest); err != nil {
		return err
	}
	if err := validateMinimaConcurrentMutationPlans(manifest); err != nil {
		return err
	}
	stateHash, err := minimaExpectedStateHash(manifest)
	if err != nil || manifest.ExpectedStateSHA256 != stateHash {
		return fmt.Errorf("minima manifest: post-operation state hash mismatch: %v", err)
	}
	frozen := buildMinimaManifest()
	if manifest.CorpusSHA256 != frozen.CorpusSHA256 || manifest.QuerySHA256 != frozen.QuerySHA256 || manifest.OperationSHA256 != frozen.OperationSHA256 || manifest.ExpectedStateSHA256 != frozen.ExpectedStateSHA256 || minimaDigest(manifest.Config) != minimaDigest(frozen.Config) {
		return fmt.Errorf("minima manifest: not the frozen workload")
	}
	if len(manifest.Corpora) != len(manifest.Queries) {
		return fmt.Errorf("minima manifest: corpus/query cardinality mismatch")
	}
	applied, err := minimaApplyOperations(manifest)
	if err != nil {
		return err
	}
	for i, corpus := range manifest.Corpora {
		if corpus.CorpusRows <= 0 || corpus.EligibleRows < 0 || corpus.EligibleStart < 0 || corpus.EligibleStart+corpus.EligibleRows > corpus.CorpusRows {
			return fmt.Errorf("minima manifest: invalid cardinality for %s", corpus.Name)
		}
		selectivity := float64(corpus.EligibleRows) / float64(corpus.CorpusRows)
		if corpus.Selectivity != selectivity {
			return fmt.Errorf("minima manifest: selectivity mismatch for %s", corpus.Name)
		}
		query := manifest.Queries[i]
		initialIDs, initialScores, globalMatches := minimaGlobalOracle(manifest.Corpora, corpus)
		finalIDs, finalScores := minimaFinalOracleFromState(corpus, applied)
		if query.Scenario != corpus.Name ||
			minimaDigest(query.InitialOracleIDs) != minimaDigest(initialIDs) ||
			minimaDigest(query.InitialOracleScores) != minimaDigest(initialScores) ||
			minimaDigest(query.FinalOracleIDs) != minimaDigest(finalIDs) ||
			minimaDigest(query.FinalOracleScores) != minimaDigest(finalScores) ||
			globalMatches != corpus.EligibleRows ||
			len(query.Vector) != manifest.Config.Dimension {
			return fmt.Errorf("minima manifest: exact initial/final oracle mismatch for %s", corpus.Name)
		}
	}
	byName := minimaScenarioMap(manifest)
	if byName["over_limit_4097"].EligibleRows != minimaLookupLimit+1 || byName["all_match"].EligibleRows != byName["all_match"].CorpusRows {
		return fmt.Errorf("minima manifest: over-limit/all-match cardinality mismatch")
	}
	if byName["broad_10pct"].Selectivity != 0.10 {
		return fmt.Errorf("minima manifest: broad selectivity mismatch")
	}
	sparse := byName["sparse_over_limit"]
	if sparse.EligibleRows <= minimaLookupLimit || sparse.Selectivity >= 0.01 {
		return fmt.Errorf("minima manifest: sparse selectivity mismatch")
	}
	mixed := byName["mixed_broad_narrow"]
	if mixed.BroadRows <= minimaLookupLimit || mixed.NarrowRows != mixed.EligibleRows || mixed.EligibleRows != 5 {
		return fmt.Errorf("minima manifest: mixed cardinality mismatch")
	}
	return nil
}

func validateMinimaScalarNamespaces(manifest *minimaManifest) error {
	names := make(map[string]bool, len(manifest.Corpora))
	declaredUsers := make(map[string]string, len(manifest.Corpora))
	declaredPaths := make(map[string]string, len(manifest.Corpora))
	for _, corpus := range manifest.Corpora {
		if corpus.Name == "" || names[corpus.Name] || corpus.UserID == "" || corpus.Generator != minimaGenerator {
			return fmt.Errorf("minima manifest: invalid or duplicate scalar namespace for %s", corpus.Name)
		}
		if owner := declaredUsers[corpus.UserID]; owner != "" {
			return fmt.Errorf("minima manifest: user_id %q shared by %s and %s", corpus.UserID, owner, corpus.Name)
		}
		if corpus.FPath != "" {
			if owner := declaredPaths[corpus.FPath]; owner != "" {
				return fmt.Errorf("minima manifest: fpath %q shared by %s and %s", corpus.FPath, owner, corpus.Name)
			}
			declaredPaths[corpus.FPath] = corpus.Name
		}
		names[corpus.Name] = true
		declaredUsers[corpus.UserID] = corpus.Name
	}
	defaultUsers := make(map[string]string, len(manifest.Corpora)*31)
	defaultPaths := make(map[string]string, len(manifest.Corpora)*97)
	for _, corpus := range manifest.Corpora {
		for ordinal := range 31 {
			userID, _ := minimaDefaultScalarsAt(corpus, ordinal)
			if owner := declaredUsers[userID]; owner != "" || defaultUsers[userID] != "" {
				return fmt.Errorf("minima manifest: default user_id %q is not scenario-unique", userID)
			}
			defaultUsers[userID] = corpus.Name
		}
		for ordinal := range 97 {
			_, fpath := minimaDefaultScalarsAt(corpus, ordinal)
			if owner := declaredPaths[fpath]; owner != "" || defaultPaths[fpath] != "" {
				return fmt.Errorf("minima manifest: default fpath %q is not scenario-unique", fpath)
			}
			defaultPaths[fpath] = corpus.Name
		}
	}
	return nil
}
func validateMinimaTimedPlan(manifest *minimaManifest) error {
	var timed *minimaOperationSpec
	for i := range manifest.Operations {
		if manifest.Operations[i].Timed {
			if timed != nil {
				return fmt.Errorf("minima manifest: multiple timed operations")
			}
			timed = &manifest.Operations[i]
		}
	}
	if timed == nil || timed.Name != "timed_search_with_batch_insert" || timed.TimedPlan == nil || len(timed.Schedule) != 0 {
		return fmt.Errorf("minima manifest: missing unambiguous timed reader plan")
	}
	plan := timed.TimedPlan
	config := manifest.Config
	if plan.QueryCount != config.TimedQueries ||
		plan.ReaderConcurrency != config.ReaderConcurrency ||
		plan.WriterConcurrency != config.WriterConcurrency ||
		len(plan.ScenarioOrder) != len(manifest.Corpora) ||
		len(plan.Rounds) == 0 ||
		len(plan.Rounds) != len(timed.InsertRanges) ||
		plan.QueryCount%len(plan.Rounds) != 0 {
		return fmt.Errorf("minima manifest: timed reader counts/concurrency mismatch")
	}
	for i, corpus := range manifest.Corpora {
		if plan.ScenarioOrder[i] != corpus.Name {
			return fmt.Errorf("minima manifest: timed scenario order mismatch")
		}
	}
	perRound := plan.QueryCount / len(plan.Rounds)
	assignment := fmt.Sprintf("round=ordinal/%d;reader=ordinal%%%d;scenario=scenario_order[ordinal%%%d]", perRound, config.ReaderConcurrency, len(manifest.Corpora))
	if plan.Assignment != assignment {
		return fmt.Errorf("minima manifest: timed assignment mismatch")
	}
	for i, round := range plan.Rounds {
		if round.Ordinal != i || round.QueryStart != i*perRound || round.QueryCount != perRound ||
			round.InsertRange != timed.InsertRanges[i] ||
			round.StartBarrier != "round_start_readers_and_writer" ||
			round.EndBarrier != "round_end_queries_and_insert_complete" {
			return fmt.Errorf("minima manifest: timed round %d mismatch", i)
		}
	}
	return nil
}

func validateMinimaConcurrentMutationPlans(manifest *minimaManifest) error {
	expectedMutations := map[int]string{
		4: "delete_by_user_id_and_fpath",
		5: "replacement_insert",
	}
	seen := make(map[int]bool, len(expectedMutations))
	for i := range manifest.Operations {
		operation := &manifest.Operations[i]
		if operation.ConcurrentPlan == nil {
			continue
		}
		mutation, ok := expectedMutations[operation.Ordinal]
		plan := operation.ConcurrentPlan
		if !ok || seen[operation.Ordinal] || len(operation.Schedule) != 0 ||
			plan.Mutation != mutation ||
			plan.ReaderConcurrency != manifest.Config.ReaderConcurrency ||
			len(plan.ReaderAssignments) != manifest.Config.ReaderConcurrency ||
			plan.StartBarrier != "reindex_start_all_readers_and_writer" ||
			plan.EndBarrier != "reindex_end_all_readers_and_mutation_complete" {
			return fmt.Errorf("minima manifest: concurrent mutation operation %d mismatch", operation.Ordinal)
		}
		for reader, assignment := range plan.ReaderAssignments {
			if assignment.Reader != reader || assignment.QueryOrdinal != reader || assignment.Scenario != operation.Target {
				return fmt.Errorf("minima manifest: concurrent mutation operation %d reader %d mismatch", operation.Ordinal, reader)
			}
		}
		seen[operation.Ordinal] = true
	}
	if len(seen) != len(expectedMutations) {
		return fmt.Errorf("minima manifest: missing concurrent delete/replacement plans")
	}
	return nil
}

type minimaTimedQueryObservation struct {
	Ordinal            int    `json:"ordinal"`
	Round              int    `json:"round"`
	Reader             int    `json:"reader"`
	Scenario           string `json:"scenario"`
	StartedMonotonicNS int64  `json:"started_monotonic_ns"`
	EndedMonotonicNS   int64  `json:"ended_monotonic_ns"`
}

type minimaObservedTimedRound struct {
	Ordinal                  int               `json:"ordinal"`
	QueryStart               int               `json:"query_start"`
	QueryCount               int               `json:"query_count"`
	InsertRange              minimaInsertRange `json:"insert_range"`
	StartBarrier             string            `json:"start_barrier"`
	EndBarrier               string            `json:"end_barrier"`
	WriterStartedMonotonicNS int64             `json:"writer_started_monotonic_ns"`
	WriterEndedMonotonicNS   int64             `json:"writer_ended_monotonic_ns"`
}

type minimaTimedExecutionTrace struct {
	Queries []minimaTimedQueryObservation `json:"queries"`
	Rounds  []minimaObservedTimedRound    `json:"rounds"`
}

func minimaExpectedTimedExecution(plan *minimaTimedReaderPlan) minimaTimedExecutionTrace {
	var trace minimaTimedExecutionTrace
	for _, round := range plan.Rounds {
		base := int64(round.Ordinal+1) * 1_000_000
		trace.Rounds = append(trace.Rounds, minimaObservedTimedRound{
			Ordinal: round.Ordinal, QueryStart: round.QueryStart, QueryCount: round.QueryCount,
			InsertRange: round.InsertRange, StartBarrier: round.StartBarrier, EndBarrier: round.EndBarrier,
			WriterStartedMonotonicNS: base + 100, WriterEndedMonotonicNS: base + 900,
		})
	}
	roundIndex := 0
	for ordinal := range plan.QueryCount {
		for ordinal >= plan.Rounds[roundIndex].QueryStart+plan.Rounds[roundIndex].QueryCount {
			roundIndex++
		}
		base := int64(plan.Rounds[roundIndex].Ordinal+1) * 1_000_000
		started := base + 200 + int64(ordinal-plan.Rounds[roundIndex].QueryStart)*2
		trace.Queries = append(trace.Queries, minimaTimedQueryObservation{
			Ordinal: ordinal, Round: roundIndex, Reader: ordinal % plan.ReaderConcurrency,
			Scenario:           plan.ScenarioOrder[ordinal%len(plan.ScenarioOrder)],
			StartedMonotonicNS: started, EndedMonotonicNS: started + 1,
		})
	}
	return trace
}

func minimaTimedExecutionDigest(observed minimaTimedExecutionTrace) string {
	var trace strings.Builder
	for _, query := range observed.Queries {
		fmt.Fprintf(&trace, "query|ordinal=%d|round=%d|reader=%d|scenario=%s|started_monotonic_ns=%d|ended_monotonic_ns=%d\n",
			query.Ordinal, query.Round, query.Reader, query.Scenario, query.StartedMonotonicNS, query.EndedMonotonicNS)
	}
	for _, round := range observed.Rounds {
		fmt.Fprintf(&trace, "round|ordinal=%d|query_start=%d|query_count=%d|insert=%s:%d:%d|start=%s|end=%s|writer_started_monotonic_ns=%d|writer_ended_monotonic_ns=%d\n",
			round.Ordinal, round.QueryStart, round.QueryCount,
			round.InsertRange.Scenario, round.InsertRange.Start, round.InsertRange.Rows,
			round.StartBarrier, round.EndBarrier, round.WriterStartedMonotonicNS, round.WriterEndedMonotonicNS)
	}
	return artifactHash("", []byte(trace.String())).SHA256
}

func minimaValidInterval(start, end int64) bool {
	return start >= 0 && start < end
}

func minimaIntervalsOverlap(firstStart, firstEnd, secondStart, secondEnd int64) bool {
	return minimaValidInterval(firstStart, firstEnd) &&
		minimaValidInterval(secondStart, secondEnd) &&
		firstStart < secondEnd && secondStart < firstEnd
}

func validateMinimaObservedTimedExecution(observed minimaTimedExecutionTrace, plan *minimaTimedReaderPlan) error {
	if len(observed.Rounds) != len(plan.Rounds) || len(observed.Queries) != plan.QueryCount {
		return fmt.Errorf("timed trace cardinality mismatch")
	}
	overlap := make([][]bool, len(plan.Rounds))
	for round := range plan.Rounds {
		planned, actual := plan.Rounds[round], observed.Rounds[round]
		if actual.Ordinal != planned.Ordinal || actual.QueryStart != planned.QueryStart ||
			actual.QueryCount != planned.QueryCount || actual.InsertRange != planned.InsertRange ||
			actual.StartBarrier != planned.StartBarrier || actual.EndBarrier != planned.EndBarrier ||
			!minimaValidInterval(actual.WriterStartedMonotonicNS, actual.WriterEndedMonotonicNS) {
			return fmt.Errorf("timed round %d raw writer interval mismatch", round)
		}
		overlap[round] = make([]bool, plan.ReaderConcurrency)
	}
	expected := minimaExpectedTimedExecution(plan)
	seenQueries := make([]bool, plan.QueryCount)
	for _, query := range observed.Queries {
		if query.Ordinal < 0 || query.Ordinal >= plan.QueryCount || seenQueries[query.Ordinal] {
			return fmt.Errorf("timed query ordinal mismatch")
		}
		assignment := expected.Queries[query.Ordinal]
		if query.Round != assignment.Round || query.Reader != assignment.Reader ||
			query.Scenario != assignment.Scenario || !minimaValidInterval(query.StartedMonotonicNS, query.EndedMonotonicNS) {
			return fmt.Errorf("timed query %d assignment/interval mismatch", query.Ordinal)
		}
		writer := observed.Rounds[query.Round]
		if minimaIntervalsOverlap(
			query.StartedMonotonicNS, query.EndedMonotonicNS,
			writer.WriterStartedMonotonicNS, writer.WriterEndedMonotonicNS,
		) {
			overlap[query.Round][query.Reader] = true
		}
		seenQueries[query.Ordinal] = true
	}
	for round := range overlap {
		for reader := range overlap[round] {
			if !overlap[round][reader] {
				return fmt.Errorf("timed round %d reader %d did not overlap writer", round, reader)
			}
		}
	}
	return nil
}

type minimaObservedReindexQuery struct {
	Reader             int       `json:"reader"`
	QueryOrdinal       int       `json:"query_ordinal"`
	Scenario           string    `json:"scenario"`
	StartedMonotonicNS int64     `json:"started_monotonic_ns"`
	EndedMonotonicNS   int64     `json:"ended_monotonic_ns"`
	ResultCaptured     bool      `json:"result_captured"`
	ActualIDs          []string  `json:"actual_ids"`
	ActualScores       []float64 `json:"actual_scores"`
}

type minimaObservedReindexOperation struct {
	OperationOrdinal           int                          `json:"operation_ordinal"`
	Mutation                   string                       `json:"mutation"`
	StartBarrier               string                       `json:"start_barrier"`
	EndBarrier                 string                       `json:"end_barrier"`
	MutationStartedMonotonicNS int64                        `json:"mutation_started_monotonic_ns"`
	MutationEndedMonotonicNS   int64                        `json:"mutation_ended_monotonic_ns"`
	ReaderQueries              []minimaObservedReindexQuery `json:"reader_queries"`
}

type minimaReindexExecutionTrace struct {
	Operations []minimaObservedReindexOperation `json:"operations"`
}

func minimaExpectedReindexExecution(manifest *minimaManifest) minimaReindexExecutionTrace {
	var trace minimaReindexExecutionTrace
	queries := minimaQueryMap(manifest)
	for _, operation := range manifest.Operations {
		if operation.ConcurrentPlan == nil {
			continue
		}
		plan := operation.ConcurrentPlan
		base := int64(operation.Ordinal+1) * 1_000_000
		observation := minimaObservedReindexOperation{
			OperationOrdinal: operation.Ordinal, Mutation: plan.Mutation,
			StartBarrier: plan.StartBarrier, EndBarrier: plan.EndBarrier,
			MutationStartedMonotonicNS: base + 100, MutationEndedMonotonicNS: base + 900,
		}
		for _, assignment := range plan.ReaderAssignments {
			started := base + 200 + int64(assignment.Reader)*10
			query := queries[assignment.Scenario]
			ids, scores := query.InitialOracleIDs, query.InitialOracleScores
			if plan.Mutation == "replacement_insert" {
				ids, scores = query.FinalOracleIDs, query.FinalOracleScores
			}
			observation.ReaderQueries = append(observation.ReaderQueries, minimaObservedReindexQuery{
				Reader: assignment.Reader, QueryOrdinal: assignment.QueryOrdinal, Scenario: assignment.Scenario,
				StartedMonotonicNS: started, EndedMonotonicNS: started + 1,
				ResultCaptured: true, ActualIDs: ids, ActualScores: scores,
			})
		}
		trace.Operations = append(trace.Operations, observation)
	}
	return trace
}

func minimaReindexExecutionDigest(observed minimaReindexExecutionTrace) string {
	var trace strings.Builder
	for _, operation := range observed.Operations {
		fmt.Fprintf(&trace, "reindex|operation=%d|mutation=%s|start=%s|end=%s|mutation_started_monotonic_ns=%d|mutation_ended_monotonic_ns=%d\n",
			operation.OperationOrdinal, operation.Mutation, operation.StartBarrier, operation.EndBarrier,
			operation.MutationStartedMonotonicNS, operation.MutationEndedMonotonicNS)
		for _, query := range operation.ReaderQueries {
			fmt.Fprintf(&trace, "reindex_query|operation=%d|reader=%d|query_ordinal=%d|scenario=%s|started_monotonic_ns=%d|ended_monotonic_ns=%d\n",
				operation.OperationOrdinal, query.Reader, query.QueryOrdinal, query.Scenario,
				query.StartedMonotonicNS, query.EndedMonotonicNS)
		}
	}
	return artifactHash("", []byte(trace.String())).SHA256
}

func validateMinimaObservedReindexExecution(observed minimaReindexExecutionTrace, manifest *minimaManifest) error {
	expected := minimaExpectedReindexExecution(manifest)
	queries := minimaQueryMap(manifest)
	if len(observed.Operations) != len(expected.Operations) {
		return fmt.Errorf("reindex operation count mismatch")
	}
	for index, operation := range observed.Operations {
		expectedOperation := expected.Operations[index]
		manifestOperation := manifest.Operations[expectedOperation.OperationOrdinal]
		plan := manifestOperation.ConcurrentPlan
		if operation.OperationOrdinal != expectedOperation.OperationOrdinal || plan == nil ||
			operation.Mutation != plan.Mutation ||
			operation.StartBarrier != plan.StartBarrier ||
			operation.EndBarrier != plan.EndBarrier ||
			!minimaValidInterval(operation.MutationStartedMonotonicNS, operation.MutationEndedMonotonicNS) ||
			len(operation.ReaderQueries) != len(plan.ReaderAssignments) {
			return fmt.Errorf("reindex operation %d trace mismatch", operation.OperationOrdinal)
		}
		seenReaders := make(map[int]bool, plan.ReaderConcurrency)
		for _, query := range operation.ReaderQueries {
			if query.Reader < 0 || query.Reader >= plan.ReaderConcurrency || seenReaders[query.Reader] {
				return fmt.Errorf("reindex operation %d reader assignment mismatch", operation.OperationOrdinal)
			}
			assignment := plan.ReaderAssignments[query.Reader]
			if query.QueryOrdinal != assignment.QueryOrdinal || query.Scenario != assignment.Scenario ||
				!minimaValidInterval(query.StartedMonotonicNS, query.EndedMonotonicNS) ||
				!minimaIntervalsOverlap(
					query.StartedMonotonicNS, query.EndedMonotonicNS,
					operation.MutationStartedMonotonicNS, operation.MutationEndedMonotonicNS,
				) {
				return fmt.Errorf("reindex operation %d reader %d did not overlap mutation", operation.OperationOrdinal, query.Reader)
			}
			oracle, ok := queries[query.Scenario]
			if !ok || !query.ResultCaptured {
				return fmt.Errorf("reindex operation %d reader %d result was not captured", operation.OperationOrdinal, query.Reader)
			}
			var preIDs, postIDs []string
			var preScores, postScores []float64
			switch operation.Mutation {
			case "delete_by_user_id_and_fpath":
				preIDs, preScores = oracle.InitialOracleIDs, oracle.InitialOracleScores
			case "replacement_insert":
				postIDs, postScores = oracle.FinalOracleIDs, oracle.FinalOracleScores
			default:
				return fmt.Errorf("reindex operation %d has unsupported mutation", operation.OperationOrdinal)
			}
			_, _, preErr := validateMinimaRanking(query.ActualIDs, query.ActualScores, preIDs, preScores, manifest.Config.OrderTolerance, manifest.Config.ScoreTolerance)
			_, _, postErr := validateMinimaRanking(query.ActualIDs, query.ActualScores, postIDs, postScores, manifest.Config.OrderTolerance, manifest.Config.ScoreTolerance)
			if preErr != nil && postErr != nil {
				return fmt.Errorf("reindex operation %d reader %d returned an impossible mixed mutation state", operation.OperationOrdinal, query.Reader)
			}
			seenReaders[query.Reader] = true
		}
		if len(seenReaders) != plan.ReaderConcurrency {
			return fmt.Errorf("reindex operation %d missing reader overlap", operation.OperationOrdinal)
		}
	}
	return nil
}

func minimaScenarioMap(manifest *minimaManifest) map[string]minimaScenarioSpec {
	out := make(map[string]minimaScenarioSpec, len(manifest.Corpora))
	for _, scenario := range manifest.Corpora {
		out[scenario.Name] = scenario
	}
	return out
}
func minimaQueryMap(manifest *minimaManifest) map[string]minimaQuerySpec {
	out := make(map[string]minimaQuerySpec, len(manifest.Queries))
	for _, query := range manifest.Queries {
		out[query.Scenario] = query
	}
	return out
}

func writeMinimaManifest(path string) error {
	manifest := buildMinimaManifest()
	if err := validateMinimaManifest(&manifest); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

type minimaManifestHashes struct {
	CorpusSHA256    string `json:"corpus_sha256"`
	QuerySHA256     string `json:"query_sha256"`
	OperationSHA256 string `json:"operation_sha256"`
}

type minimaOperationEvidence struct {
	ManifestOrdered           bool                        `json:"manifest_ordered"`
	BatchInsertDuringSearch   bool                        `json:"batch_insert_during_search"`
	ReindexDeleteReplace      bool                        `json:"reindex_delete_replace"`
	ExplicitUpdateVisible     bool                        `json:"explicit_update_visible"`
	ExplicitDeleteVisible     bool                        `json:"explicit_delete_visible"`
	EmptyCasesChecked         bool                        `json:"empty_cases_checked"`
	TimedQueriesExecuted      int                         `json:"timed_queries_executed"`
	TimedRoundsCompleted      int                         `json:"timed_rounds_completed"`
	TimedExecutionSHA256      string                      `json:"timed_execution_sha256"`
	TimedExecutionTrace       minimaTimedExecutionTrace   `json:"timed_execution_trace"`
	ReindexOperationsExecuted int                         `json:"reindex_operations_executed"`
	ReindexExecutionSHA256    string                      `json:"reindex_execution_sha256"`
	ReindexExecutionTrace     minimaReindexExecutionTrace `json:"reindex_execution_trace"`
}

type minimaReopenEvidence struct {
	Attempted          bool   `json:"attempted"`
	CommittedParity    bool   `json:"committed_parity"`
	ResultManifestHash string `json:"result_manifest_hash"`
}

type minimaBackendEvidence struct {
	Name          string                  `json:"name"`
	ServerVersion string                  `json:"server_version"`
	ClientVersion string                  `json:"client_version"`
	Durability    string                  `json:"durability"`
	Configuration map[string]string       `json:"configuration"`
	Environment   map[string]string       `json:"environment"`
	Manifest      minimaManifestHashes    `json:"manifest"`
	Operations    minimaOperationEvidence `json:"operations"`
	Reopen        minimaReopenEvidence    `json:"reopen"`
}

type minimaCorrectnessEvidence struct {
	CrossUserResults int `json:"cross_user_results"`
	StaleInsertIDs   int `json:"stale_insert_ids"`
	StaleUpdateIDs   int `json:"stale_update_ids"`
	StaleDeleteIDs   int `json:"stale_delete_ids"`
}

type minimaRouteEvidence struct {
	Identity                     string `json:"identity"`
	DeclaredScalarFiltering      bool   `json:"declared_scalar_filtering"`
	NativeBasePlusLiveDelta      bool   `json:"native_base_plus_live_delta"`
	FullDocumentScanFallbacks    *int   `json:"full_document_scan_fallbacks"`
	ScalarFilterUnbounded        *int   `json:"scalar_filter_unbounded"`
	ProbeIDs                     *int   `json:"probe_ids"`
	CandidateIDs                 *int   `json:"candidate_ids"`
	RetainedCandidateIDs         *int   `json:"retained_candidate_ids"`
	RefinedCandidateIDs          *int   `json:"refined_candidate_ids"`
	MembershipSource             string `json:"membership_source"`
	Plan                         string `json:"plan"`
	AllowedIDMaterializationRows *int   `json:"allowed_id_materialization_rows"`
	PrimaryDocumentScans         *int   `json:"primary_document_scans"`
	VisitedCandidates            *int   `json:"visited_candidates"`
	ScoredCandidates             *int   `json:"scored_candidates"`
	AdmittedCandidates           *int   `json:"admitted_candidates"`
}

type minimaVisibilityEvidence struct {
	GenerationConsistent bool `json:"generation_consistent"`
	MismatchCount        *int `json:"visibility_mismatch_count"`
	RetryCount           *int `json:"visibility_retry_count"`
}

type minimaTimingEvidence struct {
	Captured          bool    `json:"captured"`
	WriterMillis      float64 `json:"writer_millis"`
	SearchMillis      float64 `json:"search_millis"`
	FetchMillis       float64 `json:"fetch_millis"`
	DecodeMillis      float64 `json:"decode_millis"`
	EmbeddingIncluded bool    `json:"embedding_included"`
	LLMIncluded       bool    `json:"llm_included"`
}

type minimaResourceEvidence struct {
	Captured               bool     `json:"captured"`
	BytesPerOp             *float64 `json:"bytes_per_op"`
	AllocsPerOp            *float64 `json:"allocs_per_op"`
	AllocationAvailability string   `json:"allocation_availability"`
	RSSBytes               int64    `json:"rss_bytes"`
	CPUSeconds             float64  `json:"cpu_seconds"`
	DiskBytes              int64    `json:"disk_bytes"`
}

type minimaScenarioEvidence struct {
	Backend             string                    `json:"backend"`
	Scenario            string                    `json:"scenario"`
	CorpusRows          int                       `json:"corpus_rows"`
	ExpectedMatches     int                       `json:"expected_matches"`
	Selectivity         float64                   `json:"selectivity"`
	InitialOracleIDs    []string                  `json:"initial_oracle_ids"`
	InitialOracleScores []float64                 `json:"initial_oracle_scores"`
	FinalOracleIDs      []string                  `json:"final_oracle_ids"`
	FinalOracleScores   []float64                 `json:"final_oracle_scores"`
	InitialActualIDs    []string                  `json:"initial_actual_ids"`
	InitialActualScores []float64                 `json:"initial_actual_scores"`
	ActualIDs           []string                  `json:"actual_ids"`
	ActualScores        []float64                 `json:"actual_scores"`
	ReopenIDs           []string                  `json:"reopen_ids"`
	ReopenParity        bool                      `json:"reopen_parity"`
	Recall              float64                   `json:"recall"`
	Overlap             float64                   `json:"overlap"`
	OrderTolerance      int                       `json:"order_tolerance"`
	ScoreTolerance      float64                   `json:"score_tolerance"`
	Errors              int                       `json:"errors"`
	Timeouts            int                       `json:"timeouts"`
	Correctness         minimaCorrectnessEvidence `json:"correctness"`
	Route               minimaRouteEvidence       `json:"route"`
	Visibility          minimaVisibilityEvidence  `json:"visibility"`
	Timing              minimaTimingEvidence      `json:"timing"`
	Resource            minimaResourceEvidence    `json:"resource"`
}

type minimaArtifact struct {
	Schema         string                              `json:"schema"`
	State          string                              `json:"state"`
	Passing        bool                                `json:"passing"`
	Manifest       minimaManifest                      `json:"manifest"`
	Backends       []minimaBackendEvidence             `json:"backends"`
	Scenarios      []minimaScenarioEvidence            `json:"scenarios"`
	Failures       []string                            `json:"failures"`
	Recommendation string                              `json:"readiness_recommendation"`
	RawEvidence    map[string]minimaRawBackendEvidence `json:"backend_raw_evidence"`
}

func validateMinimaArtifact(artifact *minimaArtifact) error {
	if artifact == nil || artifact.Schema != minimaArtifactSchema {
		return fmt.Errorf("minima artifact: missing schema")
	}
	if err := validateMinimaManifest(&artifact.Manifest); err != nil {
		return err
	}
	if artifact.State == "partial" {
		if artifact.Passing || artifact.Recommendation != "not_evaluated" {
			return fmt.Errorf("minima artifact: partial run marked passing or recommended")
		}
		return nil
	}
	if artifact.State != "pass" || !artifact.Passing || len(artifact.Failures) != 0 {
		return fmt.Errorf("minima artifact: final state is not a clean pass")
	}
	if artifact.Recommendation != "ready_direct" && artifact.Recommendation != "ready_with_alpha_limitations" && artifact.Recommendation != "not_ready" && artifact.Recommendation != "unsuitable" {
		return fmt.Errorf("minima artifact: missing readiness recommendation")
	}
	backends := make(map[string]minimaBackendEvidence, len(artifact.Backends))
	for _, backend := range artifact.Backends {
		if backend.Name != "treedb" && backend.Name != "qdrant" {
			return fmt.Errorf("minima artifact: unknown backend %q", backend.Name)
		}
		if _, exists := backends[backend.Name]; exists {
			return fmt.Errorf("minima artifact: duplicate backend %q", backend.Name)
		}
		if backend.ServerVersion == "" || backend.ClientVersion == "" || backend.Durability == "" || len(backend.Configuration) == 0 || len(backend.Environment) == 0 {
			return fmt.Errorf("minima artifact: %s missing environment/version/durability/config", backend.Name)
		}
		for _, key := range []string{"os", "arch", "cpu", "memory"} {
			if backend.Environment[key] == "" {
				return fmt.Errorf("minima artifact: %s missing environment %s", backend.Name, key)
			}
		}
		if backend.Manifest.CorpusSHA256 != artifact.Manifest.CorpusSHA256 || backend.Manifest.QuerySHA256 != artifact.Manifest.QuerySHA256 || backend.Manifest.OperationSHA256 != artifact.Manifest.OperationSHA256 {
			return fmt.Errorf("minima artifact: %s did not consume identical manifests", backend.Name)
		}
		timedPlan := artifact.Manifest.Operations[3].TimedPlan
		observedTimedTrace := backend.Operations.TimedExecutionTrace
		observedTimedDigest := minimaTimedExecutionDigest(observedTimedTrace)
		expectedReindexTrace := minimaExpectedReindexExecution(&artifact.Manifest)
		observedReindexTrace := backend.Operations.ReindexExecutionTrace
		observedReindexDigest := minimaReindexExecutionDigest(observedReindexTrace)
		if !backend.Operations.ManifestOrdered || !backend.Operations.BatchInsertDuringSearch || !backend.Operations.ReindexDeleteReplace || !backend.Operations.ExplicitUpdateVisible || !backend.Operations.ExplicitDeleteVisible || !backend.Operations.EmptyCasesChecked ||
			backend.Operations.TimedQueriesExecuted != len(observedTimedTrace.Queries) ||
			backend.Operations.TimedRoundsCompleted != len(observedTimedTrace.Rounds) ||
			backend.Operations.TimedQueriesExecuted != timedPlan.QueryCount ||
			backend.Operations.TimedRoundsCompleted != len(timedPlan.Rounds) ||
			backend.Operations.TimedExecutionSHA256 != observedTimedDigest ||
			backend.Operations.ReindexOperationsExecuted != len(observedReindexTrace.Operations) ||
			backend.Operations.ReindexOperationsExecuted != len(expectedReindexTrace.Operations) ||
			backend.Operations.ReindexExecutionSHA256 != observedReindexDigest {
			return fmt.Errorf("minima artifact: %s missing or incomplete observed operation execution evidence", backend.Name)
		}
		if err := validateMinimaObservedTimedExecution(observedTimedTrace, timedPlan); err != nil {
			return fmt.Errorf("minima artifact: %s: %w", backend.Name, err)
		}
		if err := validateMinimaObservedReindexExecution(observedReindexTrace, &artifact.Manifest); err != nil {
			return fmt.Errorf("minima artifact: %s: %w", backend.Name, err)
		}
		if !backend.Reopen.Attempted || !backend.Reopen.CommittedParity || backend.Reopen.ResultManifestHash != artifact.Manifest.ExpectedStateSHA256 {
			return fmt.Errorf("minima artifact: %s reopen state hash mismatch", backend.Name)
		}
		backends[backend.Name] = backend
	}
	if len(backends) != 2 {
		return fmt.Errorf("minima artifact: requires TreeDB and Qdrant evidence")
	}
	queries := minimaQueryMap(&artifact.Manifest)
	specs := minimaScenarioMap(&artifact.Manifest)
	seen := make(map[string]bool, len(artifact.Scenarios))
	for _, row := range artifact.Scenarios {
		if _, ok := backends[row.Backend]; !ok {
			return fmt.Errorf("minima artifact: scenario has unknown backend %q", row.Backend)
		}
		spec, ok := specs[row.Scenario]
		if !ok {
			return fmt.Errorf("minima artifact: unknown scenario %q", row.Scenario)
		}
		key := row.Backend + "/" + row.Scenario
		if seen[key] {
			return fmt.Errorf("minima artifact: duplicate scenario %s", key)
		}
		seen[key] = true
		if err := validateMinimaScenarioEvidence(row, spec, queries[row.Scenario]); err != nil {
			return fmt.Errorf("minima artifact: %s: %w", key, err)
		}
	}
	if len(seen) != len(specs)*len(backends) {
		return fmt.Errorf("minima artifact: missing per-backend scenario evidence")
	}
	return validateMinimaRawEvidence(artifact, backends)
}

func validateMinimaScenarioEvidence(row minimaScenarioEvidence, spec minimaScenarioSpec, query minimaQuerySpec) error {
	if row.CorpusRows != spec.CorpusRows || row.ExpectedMatches != spec.EligibleRows || row.Selectivity != spec.Selectivity {
		return fmt.Errorf("missing or incorrect selectivity/cardinality")
	}
	if minimaDigest(row.InitialOracleIDs) != minimaDigest(query.InitialOracleIDs) ||
		minimaDigest(row.InitialOracleScores) != minimaDigest(query.InitialOracleScores) ||
		minimaDigest(row.FinalOracleIDs) != minimaDigest(query.FinalOracleIDs) ||
		minimaDigest(row.FinalOracleScores) != minimaDigest(query.FinalOracleScores) ||
		row.OrderTolerance != minimaOrderTolerance || row.ScoreTolerance != minimaScoreTolerance {
		return fmt.Errorf("missing exact initial/final oracle evidence")
	}
	initialRecall, initialOverlap, err := validateMinimaRanking(
		row.InitialActualIDs, row.InitialActualScores,
		query.InitialOracleIDs, query.InitialOracleScores,
		row.OrderTolerance, row.ScoreTolerance,
	)
	if err != nil || initialRecall != 1 || initialOverlap != 1 {
		return fmt.Errorf("initial actual results do not match initial oracle: %v", err)
	}
	recall, overlap, err := validateMinimaRanking(
		row.ActualIDs, row.ActualScores,
		query.FinalOracleIDs, query.FinalOracleScores,
		row.OrderTolerance, row.ScoreTolerance,
	)
	if err != nil || !finiteFraction(row.Recall) || !finiteFraction(row.Overlap) || math.Abs(row.Recall-recall) > 1e-12 || math.Abs(row.Overlap-overlap) > 1e-12 {
		return fmt.Errorf("reported recall/overlap does not match final actual IDs: %v", err)
	}
	if !row.ReopenParity || minimaDigest(row.ReopenIDs) != minimaDigest(row.ActualIDs) {
		return fmt.Errorf("missing per-scenario final reopen evidence")
	}
	if row.Errors != 0 || row.Timeouts != 0 {
		return fmt.Errorf("errors or timeouts recorded")
	}
	if row.Correctness.CrossUserResults != 0 || row.Correctness.StaleInsertIDs != 0 || row.Correctness.StaleUpdateIDs != 0 || row.Correctness.StaleDeleteIDs != 0 {
		return fmt.Errorf("tenant leakage or stale visibility")
	}
	if row.Route.Identity == "" {
		return fmt.Errorf("missing route identity")
	}
	if row.Backend == "treedb" {
		routeCounters := []*int{
			row.Route.FullDocumentScanFallbacks, row.Route.ScalarFilterUnbounded, row.Route.ProbeIDs,
			row.Route.CandidateIDs, row.Route.RetainedCandidateIDs, row.Route.RefinedCandidateIDs,
			row.Route.AllowedIDMaterializationRows, row.Route.PrimaryDocumentScans,
			row.Route.VisitedCandidates, row.Route.ScoredCandidates, row.Route.AdmittedCandidates,
		}
		for _, counter := range routeCounters {
			if counter == nil {
				return fmt.Errorf("missing native route/probe/candidate counter")
			}
		}
		if *row.Route.FullDocumentScanFallbacks != 0 {
			return fmt.Errorf("native fallback used")
		}
		if spec.EligibleRows > 0 && (*row.Route.CandidateIDs <= 0 || *row.Route.VisitedCandidates <= 0 || *row.Route.ScoredCandidates <= 0 || *row.Route.AdmittedCandidates <= 0) {
			return fmt.Errorf("missing candidate counters")
		}
		if row.Route.Identity != "native_base_plus_live_delta" || !row.Route.NativeBasePlusLiveDelta || !row.Route.DeclaredScalarFiltering {
			return fmt.Errorf("wrong or missing native route")
		}
		if *row.Route.ScalarFilterUnbounded != 0 {
			return fmt.Errorf("scalar_filter_unbounded cardinality-only failure")
		}
		if *row.Route.ProbeIDs > minimaLookupLimit {
			return fmt.Errorf("probe exceeds lookup limit")
		}
		if *row.Route.AllowedIDMaterializationRows >= spec.CorpusRows && spec.CorpusRows > 0 {
			return fmt.Errorf("collection-sized allowed-ID materialization")
		}
		if *row.Route.PrimaryDocumentScans != 0 {
			return fmt.Errorf("primary-document scan on ANN path")
		}
		if spec.EligibleRows > minimaLookupLimit && (row.Route.MembershipSource != "vector_aligned_scalar" || row.Route.Plan != "vector_aligned_ann") {
			return fmt.Errorf("over-limit row lacks vector-aligned evidence")
		}
		if spec.Name == "mixed_broad_narrow" && (*row.Route.RetainedCandidateIDs < spec.NarrowRows || *row.Route.RefinedCandidateIDs != spec.EligibleRows || row.Route.MembershipSource != "bounded_candidate_refinement" || (row.Route.Plan != "mixed_refined" && row.Route.Plan != "complete_finite_ann")) {
			return fmt.Errorf("mixed row lacks retained/refined exact or finite plan")
		}
	} else if !row.Route.DeclaredScalarFiltering || row.Route.MembershipSource == "" || row.Route.Plan == "" {
		return fmt.Errorf("missing comparator filter route evidence")
	}
	if !row.Visibility.GenerationConsistent {
		return fmt.Errorf("mixed-generation result")
	}
	if row.Visibility.MismatchCount == nil || row.Visibility.RetryCount == nil {
		return fmt.Errorf("missing visibility mismatch/retry counters")
	}
	if !row.Timing.Captured || row.Timing.EmbeddingIncluded || row.Timing.LLMIncluded || !finiteNonnegative(row.Timing.WriterMillis) || !finiteNonnegative(row.Timing.SearchMillis) || !finiteNonnegative(row.Timing.FetchMillis) || !finiteNonnegative(row.Timing.DecodeMillis) {
		return fmt.Errorf("contaminated or missing timing evidence")
	}
	if !row.Resource.Captured || row.Resource.RSSBytes < 0 || !finiteNonnegative(row.Resource.CPUSeconds) || row.Resource.DiskBytes < 0 {
		return fmt.Errorf("missing resource evidence")
	}
	switch row.Resource.AllocationAvailability {
	case "unavailable":
		if row.Resource.BytesPerOp != nil || row.Resource.AllocsPerOp != nil {
			return fmt.Errorf("unavailable allocation evidence has numeric values")
		}
	case "measured":
		if row.Resource.BytesPerOp == nil || row.Resource.AllocsPerOp == nil ||
			!finiteNonnegative(*row.Resource.BytesPerOp) || !finiteNonnegative(*row.Resource.AllocsPerOp) {
			return fmt.Errorf("measured allocation evidence is missing or non-finite")
		}
	default:
		return fmt.Errorf("allocation evidence availability is missing")
	}
	return nil
}

func validateMinimaRanking(actualIDs []string, actualScores []float64, oracleIDs []string, oracleScores []float64, orderTolerance int, scoreTolerance float64) (float64, float64, error) {
	if len(actualIDs) != len(oracleIDs) || len(actualScores) != len(oracleIDs) || len(oracleScores) != len(oracleIDs) {
		return 0, 0, fmt.Errorf("ranking length mismatch")
	}
	recall, overlap, ranks, err := minimaQuality(actualIDs, oracleIDs)
	if err != nil {
		return 0, 0, err
	}
	for i, score := range actualScores {
		rank, ok := ranks[actualIDs[i]]
		if !ok {
			return 0, 0, fmt.Errorf("actual ID %q lacks an exact oracle score", actualIDs[i])
		}
		if absInt(rank-i) > orderTolerance {
			return 0, 0, fmt.Errorf("actual ordering exceeds tolerance")
		}
		if !finiteNonnegative(score) || math.Abs(score-oracleScores[rank]) > scoreTolerance {
			return 0, 0, fmt.Errorf("actual score exceeds tolerance")
		}
	}
	return recall, overlap, nil
}

func minimaQuality(actual, oracle []string) (float64, float64, map[string]int, error) {
	ranks := make(map[string]int, len(oracle))
	for i, id := range oracle {
		if id == "" {
			return 0, 0, nil, fmt.Errorf("empty oracle ID")
		}
		if _, duplicate := ranks[id]; duplicate {
			return 0, 0, nil, fmt.Errorf("duplicate oracle ID %q", id)
		}
		ranks[id] = i
	}
	actualSet := make(map[string]bool, len(actual))
	intersection := 0
	for _, id := range actual {
		if id == "" || actualSet[id] {
			return 0, 0, nil, fmt.Errorf("empty or duplicate actual ID %q", id)
		}
		actualSet[id] = true
		if _, ok := ranks[id]; ok {
			intersection++
		}
	}
	if len(oracle) == 0 && len(actual) == 0 {
		return 1, 1, ranks, nil
	}
	recall := float64(intersection) / float64(len(oracle))
	union := len(oracle) + len(actualSet) - intersection
	return recall, float64(intersection) / float64(union), ranks, nil
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func finiteFraction(value float64) bool {
	return finiteNonnegative(value) && value <= 1
}

func finiteNonnegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}
