package main

// This opt-in diagnostic drives external command binaries. Its request timings
// include the real client/transport/response boundary. It is deliberately not a
// Go benchmark: driver B/op would not measure allocations in the server process.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

const commandPerfProfile = "command_wal_durable"

type commandPerfReport struct {
	Version                                                           string `json:"version"`
	HTTPBinary, NativeBinary, HTTPBinarySHA256, NativeBinarySHA256    string
	Command, Operation, Label, Directory, Phase                       string
	Profile, MutationACK, RequestBoundary, Transport, CleanupEvidence string
	LegacyFlag                                                        bool
	Requested, Completed, SeedRows                                    int
	Index                                                             documentservice.IndexInfo
	DurationSeconds, OperationsPerSecond                              *float64
	LatencyNanoseconds                                                *commandPerfLatency
	OperationNanoseconds                                              []int64
	Failure                                                           string   `json:"failure,omitempty"`
	FailedOperation                                                   *int     `json:"failed_operation,omitempty"`
	CleanupErrors                                                     []string `json:"cleanup_errors,omitempty"`
	Before, After                                                     commandPerfResources
	CloseNanoseconds                                                  int64
	ServerAllocatedBytes, ServerMallocs                               *uint64
	AllocationEvidence                                                string
	FirstIDs                                                          []string
	LastKeywordStats                                                  documentservice.KeywordSearchStats
	LastHybridStats                                                   collections.HybridSearchStats
	LastHybridPlan                                                    collections.HybridSearchPlan
	LastHybridTextIndex, LastHybridVectorIndex                        string
	LastDenseRoute                                                    documentservice.Route
	// Verification is collected after the timed requests and resource snapshots.
	// Raw responses retain the result and modern route evidence for inspection.
	Verification                              map[string]json.RawMessage
	Quality                                   map[string]commandPerfQuality
	MaintenanceNanoseconds, ReopenNanoseconds int64
	ReopenCloseNanoseconds                    int64
}
type commandPerfQuality struct {
	NativeANN *commandPerfANNQuality    `json:"native_ann,omitempty"`
	Hybrid    *commandPerfHybridQuality `json:"hybrid,omitempty"`
}

// These metrics describe only the fixed fixture, with no acceptance threshold.
// Recall credits any ID tied at the exact corpus cutoff; regret uses true cosine.
type commandPerfANNQuality struct {
	TopK, CorpusRows, ExactStrictlyAboveCutoff, ExactTiedAtCutoff int
	ExactCutoffCosine, TieAwareRecallAtK, MeanCosineRegret        float64
}
type commandPerfHybridQuality struct {
	DocumentsChecked, TextContributions, VectorContributions, BothSources int
	MaximumRRFError, MeanReturnedVectorCosine                             float64
}
type commandPerfHybridSource struct {
	Source      string   `json:"source"`
	IndexName   string   `json:"index_name"`
	SourceRank  int      `json:"source_rank"`
	Score       *float64 `json:"score"`
	ScoreKind   string   `json:"score_kind"`
	FusionScore *float64 `json:"fusion_score"`
}
type commandPerfHybridMeta struct {
	Type         string                    `json:"type"`
	Rank         int                       `json:"rank"`
	FusionMethod string                    `json:"fusion_method"`
	FusedScore   *float64                  `json:"fused_score"`
	Sources      []commandPerfHybridSource `json:"sources"`
}
type commandPerfLatency struct{ P50, P95, P99, Max int64 }
type commandPerfResources struct {
	RSSBytes, FileBytes int64
	Diagnostics         *documentservice.DiagnosticsSnapshot `json:"diagnostics,omitempty"`
	Missing             []string                             `json:"missing,omitempty"`
}
type commandPerfProcess struct {
	cmd                     *exec.Cmd
	log                     *os.File
	http, diagnostics, wire string
	client                  *http.Client
	closed                  bool
}

// Invocation and fixed ordering are in the source-bound construction handoff.
// Each invocation owns fresh data/log/output names and exactly one process cell.
func TestCommandPublicPerformance(t *testing.T) {
	if os.Getenv("GOMAP_COMMAND_PERF") != "1" {
		t.Skip("opt-in actual-command diagnostic")
	}
	prefix := os.Getenv("GOMAP_COMMAND_PERF_OUTPUT")
	if prefix == "" {
		t.Fatal("GOMAP_COMMAND_PERF_OUTPUT must be a new absolute artifact prefix")
	}
	if !filepath.IsAbs(prefix) {
		t.Fatal("artifact prefix must be absolute")
	}
	output, err := os.OpenFile(prefix+".json", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	report := commandPerfReport{Version: "actual-command-v2", Command: os.Getenv("GOMAP_COMMAND_PERF_COMMAND"), Operation: os.Getenv("GOMAP_COMMAND_PERF_OPERATION"), Label: os.Getenv("GOMAP_COMMAND_PERF_LABEL"), HTTPBinary: os.Getenv("GOMAP_COMMAND_PERF_HTTP_BINARY"), NativeBinary: os.Getenv("GOMAP_COMMAND_PERF_NATIVE_BINARY"), Directory: prefix + ".db", Requested: 1000, AllocationEvidence: "Server TotalAlloc/Mallocs are not exported by these command diagnostics. Driver Go B/op is inapplicable; use the separately reviewed in-process service benchmark for allocation attribution."}
	defer func() {
		if err := json.NewEncoder(output).Encode(report); err != nil {
			t.Error(err)
		}
		if report.Failure != "" || len(report.CleanupErrors) > 0 {
			t.Errorf("cell failed: %s cleanup=%v", report.Failure, report.CleanupErrors)
		}
	}()
	if raw := os.Getenv("GOMAP_COMMAND_PERF_OPERATIONS"); raw != "" {
		report.Requested, err = strconv.Atoi(raw)
		if err != nil || report.Requested <= 0 {
			report.Failure = "operations must be a positive integer"
			return
		}
	}
	report.LegacyFlag = os.Getenv("GOMAP_COMMAND_PERF_LEGACY") == "1"
	report.Profile = commandPerfProfile
	report.Transport = "HTTP JSON"
	if report.Command == "native" || report.Operation == "get" || report.Operation == "range" || report.Operation == "crud" {
		report.Transport = "nativewire (HTTP command uses its native listener)"
	}
	report.CleanupEvidence = "Records explicit Flushed FlushAll, Synced Checkpoint, both process exits and logs; source inspection of baseline ea129 and the candidate is required to establish that command owner Close errors propagate."
	report.MutationACK = "nativewire AckSynced; seed HTTP uses selected durable profile"
	report.RequestBoundary = "one synchronous client round trip plus response validation; CRUD is Insert/Get/Replace/Delete (four round trips, three Synced mutations); setup/warmup/maintenance/Close excluded and reported separately"

	if err := runCommandPerf(t.Context(), prefix, &report); err != nil {
		report.Failure = err.Error()
	}
}

func runCommandPerf(parent context.Context, prefix string, r *commandPerfReport) error {
	if r.Command != "http" && r.Command != "native" {
		return errors.New("command must be http or native")
	}
	switch r.Operation {
	case "keyword", "hybrid", "dense", "get", "range", "crud":
	default:
		return errors.New("unknown operation")
	}
	if r.Command == "native" && (r.Operation == "keyword" || r.Operation == "hybrid") {
		return errors.New("nativewire has no Keyword/Hybrid request; no transport substitution")
	}
	for _, bin := range []struct {
		path string
		sum  *string
	}{{r.HTTPBinary, &r.HTTPBinarySHA256}, {r.NativeBinary, &r.NativeBinarySHA256}} {
		if !filepath.IsAbs(bin.path) {
			return errors.New("both binary paths must be absolute")
		}
		file, err := os.Open(bin.path)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return err
		}
		*bin.sum = hex.EncodeToString(hash.Sum(nil))
	}
	if err := os.Mkdir(r.Directory, 0700); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Minute)
	defer cancel()
	r.Phase = "seed-start"
	seed, err := startCommandPerf(ctx, prefix+".seed.log", r.HTTPBinary, r.Directory, true, r.LegacyFlag)
	if err != nil {
		return err
	}
	defer func() {
		if err := seed.stop(); err != nil {
			r.CleanupErrors = append(r.CleanupErrors, "seed: "+err.Error())
		}
	}()
	r.Phase = "create"
	declaration := documentservice.CreateIndexRequest{Name: "docs", Dimension: 8, VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 16, EfConstruction: 128, EfSearch: 64}, ScalarFields: []documentservice.ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: documentservice.ScalarFieldString}, {Field: "meta.rank", ValueType: documentservice.ScalarFieldInt64}}}
	var created struct {
		Index documentservice.IndexInfo `json:"index"`
	}
	if err := seed.post(ctx, "/v1/indexes", declaration, &created); err != nil {
		return err
	}
	r.Index = created.Index
	if r.Index.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
		return fmt.Errorf("declared column_graph, got %q", r.Index.VectorStrategy)
	}
	r.Phase = "load"
	for first := 0; first < 4096; first += 128 {
		docs := make([]documentservice.Document, 128)
		for j := range docs {
			i := first + j
			vector := make([]float32, 8)
			vector[0] = 1
			for d := 1; d < 8; d++ {
				vector[d] = float32((i+d)%17) / 17
			}
			docs[j] = documentservice.Document{ID: fmt.Sprintf("d%06d", i), Content: "refund policy " + strings.Repeat("payload ", 32), Embedding: vector, Meta: map[string]any{"tenant": fmt.Sprintf("t%02d", i%16), "rank": i}}
		}
		var response documentservice.UpsertDocumentsResponse
		if err := seed.post(ctx, "/v1/indexes/docs/documents/upsert", documentservice.UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}, &response); err != nil {
			return err
		}
		if response.Upserted != 128 {
			return fmt.Errorf("load prefix%d upserted%d", first, response.Upserted)
		}
		r.SeedRows += response.Upserted
	}
	r.Phase = "optimize"
	if err := seed.post(ctx, "/v1/indexes/docs/optimize", documentservice.OptimizeIndexRequest{}, nil); err != nil {
		return err
	}
	r.Phase = "seed-close"
	if err := seed.stop(); err != nil {
		return err
	}
	r.Phase = "serve-start"
	bin := r.HTTPBinary
	if r.Command == "native" {
		bin = r.NativeBinary
	}
	p, err := startCommandPerf(ctx, prefix+".serve.log", bin, r.Directory, r.Command == "http", r.LegacyFlag)
	if err != nil {
		return err
	}
	defer func() {
		if p.closed {
			return
		}
		start := time.Now()
		if err := p.stop(); err != nil {
			r.CleanupErrors = append(r.CleanupErrors, "serve: "+err.Error())
		}
		r.CloseNanoseconds = time.Since(start).Nanoseconds()
	}()
	wire, err := nativewire.DialContext(ctx, "tcp", p.wire)
	if err != nil {
		return err
	}
	defer wire.Close()
	if r.Operation == "crud" {
		if _, err := wire.CreateCollection(ctx, collections.CollectionMeta{Name: "crud", Indexes: []collections.IndexDefinition{{Name: "rank", Field: "rank", ValueType: collections.IndexValueInt64}}}); err != nil {
			return err
		}
	}
	request, err := commandPerfRequest(p, wire, r)
	if err != nil {
		return err
	}
	r.Phase = "warmup"
	// Exactly one warm request; a failure is retained and never retried.
	if err := request(ctx); err != nil {
		return err
	}
	r.OperationNanoseconds = make([]int64, r.Requested)
	r.Before = commandPerfSample(ctx, p, r.Directory, r.Command == "http")
	r.Phase = "requests"
	start := time.Now()
	for i := 0; i < r.Requested; i++ {
		requestStart := time.Now()
		requestErr := request(ctx)
		r.OperationNanoseconds[i] = time.Since(requestStart).Nanoseconds()
		if requestErr != nil {
			r.FailedOperation = &i
			r.OperationNanoseconds = r.OperationNanoseconds[:i+1]
			return requestErr
		}
		r.Completed++
	}
	elapsed := time.Since(start).Seconds()
	rate := float64(r.Completed) / elapsed
	r.DurationSeconds, r.OperationsPerSecond = &elapsed, &rate
	sorted := append([]int64(nil), r.OperationNanoseconds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	percentile := func(q float64) int64 {
		return sorted[max(0, min(len(sorted)-1, int(math.Ceil(q*float64(len(sorted))))-1))]
	}
	r.LatencyNanoseconds = &commandPerfLatency{P50: percentile(.50), P95: percentile(.95), P99: percentile(.99), Max: sorted[len(sorted)-1]}
	r.After = commandPerfSample(ctx, p, r.Directory, r.Command == "http")
	r.Phase = "verify-after-requests"
	r.Verification = make(map[string]json.RawMessage)
	r.Quality = make(map[string]commandPerfQuality)
	if err := commandPerfVerify(ctx, p, wire, r, "after_requests"); err != nil {
		return err
	}
	r.Phase = "maintenance"
	maintenanceStart := time.Now()
	if err := wire.FlushAllWithAck(ctx, nativewire.AckFlushed); err != nil {
		return err
	}
	if err := wire.CheckpointWithAck(ctx, nativewire.AckSynced); err != nil {
		return err
	}
	r.MaintenanceNanoseconds = time.Since(maintenanceStart).Nanoseconds()
	if err := wire.Close(); err != nil {
		return err
	}
	r.Phase = "serve-close"
	closeStart := time.Now()
	closeErr := p.stop()
	r.CloseNanoseconds = time.Since(closeStart).Nanoseconds()
	if closeErr != nil {
		return closeErr
	}
	r.Phase = "second-fresh-reopen"
	reopenStart := time.Now()
	reopened, err := startCommandPerf(ctx, prefix+".reopen.log", bin, r.Directory, r.Command == "http", r.LegacyFlag)
	if err != nil {
		return err
	}
	r.ReopenNanoseconds = time.Since(reopenStart).Nanoseconds()
	defer func() {
		start := time.Now()
		if err := reopened.stop(); err != nil {
			r.CleanupErrors = append(r.CleanupErrors, "reopen: "+err.Error())
		}
		r.ReopenCloseNanoseconds = time.Since(start).Nanoseconds()
	}()
	reopenWire, err := nativewire.DialContext(ctx, "tcp", reopened.wire)
	if err != nil {
		return err
	}
	defer reopenWire.Close()
	if err := commandPerfVerify(ctx, reopened, reopenWire, r, "second_reopen"); err != nil {
		return err
	}
	r.Phase = "complete"
	return nil
}

func commandPerfRequest(p *commandPerfProcess, wire *nativewire.Client, r *commandPerfReport) (func(context.Context) error, error) {
	filter := &documentservice.Filter{Field: "meta.tenant", Operator: "==", Value: "t00"}
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	switch r.Operation {
	case "keyword":
		return func(ctx context.Context) error {
			var response documentservice.KeywordSearchResponse
			if err := p.post(ctx, "/v1/indexes/docs/search/keyword", documentservice.KeywordSearchRequest{Query: "policy", TopK: 10, Filter: filter}, &response); err != nil {
				return err
			}
			r.LastKeywordStats = response.Stats
			// Filtered keyword includes normal final TopK trimming in Truncated.
			// Work-limit failures remain errors/fail-closed, not partial rankings.
			if response.Stats.FullDocumentScanFallbacks != 0 || response.Stats.FailClosed != 0 || response.Stats.Unavailable {
				return fmt.Errorf("keyword route: %+v", response.Stats)
			}
			return commandPerfDocuments(r, response.Documents)
		}, nil
	case "hybrid":
		return func(ctx context.Context) error {
			var response documentservice.HybridSearchResponse
			if err := p.post(ctx, "/v1/indexes/docs/search/hybrid", documentservice.HybridSearchRequest{Query: "policy", QueryEmbedding: query, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 64}, &response); err != nil {
				return err
			}
			r.LastHybridStats = response.Stats
			r.LastHybridPlan = response.Plan
			r.LastHybridTextIndex, r.LastHybridVectorIndex = response.TextIndex, response.VectorIndex
			if response.TextIndex != "content" || response.VectorIndex != "embedding" || response.Stats.TextCandidatesReturned == 0 || response.Stats.VectorCandidatesReturned == 0 {
				return fmt.Errorf("mixed Hybrid source identity: text=%q vector=%q stats=%+v", response.TextIndex, response.VectorIndex, response.Stats)
			}
			if response.Stats.FullDocumentScanFallbacks != 0 || response.Stats.FailClosed != 0 {
				return fmt.Errorf("hybrid route: %+v", response.Stats)
			}
			return commandPerfDocuments(r, response.Documents)
		}, nil
	case "dense":
		return func(ctx context.Context) error {
			if r.Command == "http" {
				var response documentservice.DenseVectorSearchResponse
				if err := p.post(ctx, "/v1/indexes/docs/search/vector", documentservice.DenseVectorSearchRequest{QueryEmbedding: query, TopK: 10, Route: documentservice.RouteExact, EfSearch: 64, Filter: filter}, &response); err != nil {
					return err
				}
				r.LastDenseRoute = response.Route
				if response.Route != documentservice.RouteExact || !response.Exact || response.ExactFallbacks != 0 {
					return fmt.Errorf("Dense route: %+v", response)
				}
				return commandPerfDocuments(r, response.Documents)
			}
			response, err := wire.DenseVectorSearch(ctx, nativewire.DenseVectorSearchRequest{Index: "docs", Query: query, TopK: 10, EfSearch: 64})
			if err != nil {
				return err
			}
			r.LastDenseRoute = response.Route
			if response.Route != documentservice.RouteAnn || response.ExactFallbacks != 0 || response.FullDocumentScanFallbacks != 0 || len(response.Results) != 10 {
				return fmt.Errorf("wire Dense route/results: %+v", response)
			}
			var seen [4096]bool
			for _, row := range response.Results {
				ordinal, e := commandPerfOrdinal(string(row.ID))
				if e != nil || seen[ordinal] {
					return fmt.Errorf("wire Dense duplicate/noncorpus ID %q", row.ID)
				}
				seen[ordinal] = true
			}
			if r.FirstIDs == nil {
				for _, row := range response.Results {
					r.FirstIDs = append(r.FirstIDs, string(row.ID))
				}
			}
			return nil
		}, nil
	case "get":
		return func(ctx context.Context) error {
			docs, present, err := wire.GetMany(ctx, "docs", [][]byte{[]byte("d000000")})
			if err != nil {
				return err
			}
			if len(present) != 1 || !present[0] || !bytes.Contains(docs[0], []byte("refund policy")) {
				return errors.New("Get lost seeded document")
			}
			return nil
		}, nil
	case "range":
		var index string
		for _, field := range r.Index.ScalarFields {
			if field.Field == "meta.rank" {
				index = field.IndexName
			}
		}
		if index == "" {
			return nil, errors.New("missing declared scalar rank index")
		}
		return func(ctx context.Context) error {
			ids, truncated, err := wire.IndexRange(ctx, "docs", index, nativewire.IndexRange{Lower: nativewire.Scalar{Type: collections.IndexValueInt64, Value: int64(0)}, LowerInclusive: true, Upper: nativewire.Scalar{Type: collections.IndexValueInt64, Value: int64(255)}, UpperInclusive: true})
			if err != nil {
				return err
			}
			if truncated || len(ids) != 256 {
				return fmt.Errorf("range count=%d truncated=%v", len(ids), truncated)
			}
			var seen [256]bool
			for _, id := range ids {
				ordinal, e := commandPerfOrdinal(string(id))
				if e != nil || ordinal > 255 || seen[ordinal] {
					return fmt.Errorf("range duplicate/noncorpus/outside membership ID %q", id)
				}
				seen[ordinal] = true
			}
			return nil
		}, nil
	case "crud":
		return func(ctx context.Context) error {
			ids := [][]byte{[]byte("mutation")}
			inserted, err := wire.InsertBatch(ctx, "crud", collections.DocumentFormatJSON, ids, [][]byte{[]byte(`{"rank":1}`)}, nativewire.AckSynced)
			if err != nil {
				return err
			}
			if len(inserted) != 1 || !bytes.Equal(inserted[0], ids[0]) {
				return errors.New("insert ACK lost ID")
			}
			docs, present, err := wire.GetMany(ctx, "crud", ids)
			if err != nil {
				return err
			}
			if len(present) != 1 || !present[0] || !bytes.Equal(docs[0], []byte(`{"rank":1}`)) {
				return errors.New("read after insert differs")
			}
			matched, modified, err := wire.ReplaceBatch(ctx, "crud", collections.DocumentFormatJSON, ids, [][]byte{[]byte(`{"rank":2}`)}, nativewire.AckSynced)
			if err != nil {
				return err
			}
			if matched != 1 || modified != 1 {
				return fmt.Errorf("replace counts %d/%d", matched, modified)
			}
			deleted, err := wire.DeleteBatch(ctx, "crud", ids, nativewire.AckSynced)
			if err != nil {
				return err
			}
			if deleted != 1 {
				return fmt.Errorf("delete count %d", deleted)
			}
			return nil
		}, nil
	}
	return nil, errors.New("unreachable operation")
}
func commandPerfDocuments(r *commandPerfReport, docs []documentservice.Document) error {
	if len(docs) != 10 {
		return fmt.Errorf("results=%d want10", len(docs))
	}
	var seen [4096]bool
	for _, doc := range docs {
		ordinal, err := commandPerfOrdinal(doc.ID)
		if err != nil || (r.Operation != "hybrid" && (ordinal%16 != 0 || doc.Meta["tenant"] != "t00")) || seen[ordinal] || len(doc.Embedding) != 0 || !strings.Contains(doc.Content, "policy") {
			return fmt.Errorf("unexpected/duplicate projected result %q", doc.ID)
		}
		seen[ordinal] = true
	}
	if r.FirstIDs == nil {
		for _, doc := range docs {
			r.FirstIDs = append(r.FirstIDs, doc.ID)
		}
	}
	return nil
}

// These additional requests run only after the timed interval and its resource
// snapshots, and again after a second fresh process open. They never warm the
// measured requests or replace their existing per-request checks.
func commandPerfVerify(ctx context.Context, p *commandPerfProcess, wire *nativewire.Client, r *commandPerfReport, phase string) error {
	store := func(value any) error {
		raw, err := json.Marshal(value)
		if err == nil {
			r.Verification[phase] = raw
		}
		return err
	}
	filter := &documentservice.Filter{Field: "meta.tenant", Operator: "==", Value: "t00"}
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	switch r.Operation {
	case "keyword":
		var response documentservice.KeywordSearchResponse
		if err := p.post(ctx, "/v1/indexes/docs/search/keyword", documentservice.KeywordSearchRequest{Query: "policy", TopK: 10, Filter: filter}, &response); err != nil {
			return err
		}
		if err := store(response); err != nil {
			return err
		}
		return commandPerfVerifyDocuments(response.Documents, true, false)
	case "hybrid":
		var response documentservice.HybridSearchResponse
		if err := p.post(ctx, "/v1/indexes/docs/search/hybrid", documentservice.HybridSearchRequest{Query: "policy", QueryEmbedding: query, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 64}, &response); err != nil {
			return err
		}
		if err := store(response); err != nil {
			return err
		}
		if response.TextIndex != "content" || response.VectorIndex != "embedding" || response.Stats.TextCandidatesReturned == 0 || response.Stats.VectorCandidatesReturned == 0 || response.Stats.VectorCandidatesExamined == 0 || response.Stats.VectorEdgesVisited == 0 || response.Stats.FullDocumentScanFallbacks != 0 || response.Stats.FailClosed != 0 {
			return fmt.Errorf("verification Hybrid source/work: %+v", response.Stats)
		}
		if err := commandPerfVerifyDocuments(response.Documents, false, false); err != nil {
			return err
		}
		quality, err := commandPerfVerifyHybridRRF(response)
		r.Quality[phase] = commandPerfQuality{Hybrid: &quality}
		return err
	case "dense":
		if r.Command == "http" {
			var response documentservice.DenseVectorSearchResponse
			if err := p.post(ctx, "/v1/indexes/docs/search/vector", documentservice.DenseVectorSearchRequest{QueryEmbedding: query, TopK: 10, Route: documentservice.RouteExact, EfSearch: 64, Filter: filter}, &response); err != nil {
				return err
			}
			if err := store(response); err != nil {
				return err
			}
			if response.Route != documentservice.RouteExact || !response.Exact || response.ExactFallbacks != 0 || response.FullDocumentScanFallbacks != 0 {
				return fmt.Errorf("verification HTTP exact route: %+v", response)
			}
			return commandPerfVerifyDocuments(response.Documents, true, true)
		}
		response, err := wire.DenseVectorSearch(ctx, nativewire.DenseVectorSearchRequest{Index: "docs", Query: query, TopK: 10, EfSearch: 64})
		if err != nil {
			return err
		}
		// Marshal while the borrowed response ID/document buffers are live.
		if err := store(response); err != nil {
			return err
		}
		if err := commandPerfVerifyNativeANN(response); err != nil {
			return err
		}
		ordinals := make([]int, len(response.Results))
		for i, row := range response.Results {
			ordinals[i], _ = commandPerfOrdinal(string(row.ID))
		}
		quality := commandPerfMeasureANNQuality(ordinals)
		r.Quality[phase] = commandPerfQuality{NativeANN: &quality}
		return nil
	case "get", "range":
		request, err := commandPerfRequest(p, wire, r)
		if err != nil {
			return err
		}
		if err := request(ctx); err != nil {
			return err
		}
		return store(map[string]any{"operation": r.Operation, "original_membership_check": "passed"})
	case "crud":
		docs, present, err := wire.GetMany(ctx, "crud", [][]byte{[]byte("mutation")})
		if err != nil {
			return err
		}
		if err := store(map[string]any{"documents": docs, "present": present}); err != nil {
			return err
		}
		if len(present) != 1 || present[0] {
			return errors.New("verification CRUD deleted row reappeared")
		}
		return nil
	}
	return errors.New("unknown verification operation")
}

// commandPerfVerifyNativeANN checks the two supported ordinary graph routes
// and the existing corpus/score invariants outside timing. It does not impose
// an exact top-K identity or a tie-breaking policy on approximate results.
func commandPerfVerifyNativeANN(response nativewire.DenseVectorSearchResponse) error {
	modernGraph := (response.ColumnGraphPreparedSearch == 1 && response.SearchPackSearch == 0) ||
		(response.ColumnGraphPreparedSearch == 0 && response.SearchPackSearch == 1)
	if response.Route != documentservice.RouteAnn || response.TypedColumnGraph || response.Exact || !modernGraph || response.DocumentsScanned != 0 || response.ExactFallbacks != 0 || response.FullDocumentScanFallbacks != 0 || len(response.Results) != 10 {
		return fmt.Errorf("verification native modern ANN route: %+v", response)
	}
	var seen [4096]bool
	previous := math.Inf(1)
	for _, row := range response.Results {
		ordinal, err := commandPerfOrdinal(string(row.ID))
		if err != nil || seen[ordinal] || math.IsNaN(row.Score) || math.IsInf(row.Score, 0) || row.Score > previous+1e-6 || math.Abs(row.Score-commandPerfCosine(ordinal)) > 1e-5 {
			return fmt.Errorf("verification ANN result/score %q=%g", row.ID, row.Score)
		}
		seen[ordinal], previous = true, row.Score
	}
	return nil
}

func commandPerfCosine(ordinal int) float64 {
	norm := 1.0
	for d := 1; d < 8; d++ {
		value := float64(float32((ordinal+d)%17) / 17)
		norm += value * value
	}
	return 1 / math.Sqrt(norm)
}

// This full-corpus calculation never queries the server or requires a tied ID
// ordering. Input ordinals have already passed the unique corpus-ID checks.
func commandPerfMeasureANNQuality(ordinals []int) commandPerfANNQuality {
	exact := make([]float64, 4096)
	for i := range exact {
		exact[i] = commandPerfCosine(i)
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(exact)))
	k := len(ordinals)
	q := commandPerfANNQuality{TopK: k, CorpusRows: len(exact), ExactCutoffCosine: exact[k-1]}
	for _, score := range exact {
		if score > q.ExactCutoffCosine+1e-12 {
			q.ExactStrictlyAboveCutoff++
		} else if math.Abs(score-q.ExactCutoffCosine) <= 1e-12 {
			q.ExactTiedAtCutoff++
		}
	}
	above, tied := 0, 0
	for i, ordinal := range ordinals {
		score := commandPerfCosine(ordinal)
		q.MeanCosineRegret += exact[i] - score
		if score > q.ExactCutoffCosine+1e-12 {
			above++
		} else if math.Abs(score-q.ExactCutoffCosine) <= 1e-12 {
			tied++
		}
	}
	q.TieAwareRecallAtK = float64(above+min(tied, k-q.ExactStrictlyAboveCutoff)) / float64(k)
	q.MeanCosineRegret = math.Max(0, q.MeanCosineRegret/float64(k))
	return q
}

// Recompute the fixed default RRF formula without calling production fusion.
// Only returned source contributions are visible: this proves their scores and
// provenance are consistent, not that unseen candidates could not rank higher.
func commandPerfVerifyHybridRRF(response documentservice.HybridSearchResponse) (commandPerfHybridQuality, error) {
	var q commandPerfHybridQuality
	if response.Plan.FusionMethod != collections.HybridFusionMethodRRF || response.Plan.TextCandidateLimit != 64 || response.Plan.VectorCandidateLimit != 64 || response.Plan.FinalTopK != 10 {
		return q, fmt.Errorf("verification Hybrid fixed RRF plan: %+v", response.Plan)
	}
	seenRanks := map[string]map[int]bool{"text": {}, "vector": {}}
	for i, doc := range response.Documents {
		raw, err := json.Marshal(doc.Meta["_treedb_search"])
		if err != nil {
			return q, err
		}
		var meta commandPerfHybridMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return q, err
		}
		if meta.Type != "hybrid" || meta.Rank != i+1 || meta.FusionMethod != "rrf" || meta.FusedScore == nil || len(meta.Sources) < 1 || len(meta.Sources) > 2 || doc.Score == nil || math.IsNaN(*doc.Score) || math.IsInf(*doc.Score, 0) {
			return q, fmt.Errorf("verification Hybrid metadata %q: %+v", doc.ID, meta)
		}
		ordinal, err := commandPerfOrdinal(doc.ID)
		if err != nil {
			return q, err
		}
		seenSource := map[string]bool{}
		fused := 0.0
		for _, source := range meta.Sources {
			if (source.Source != "text" && source.Source != "vector") || seenSource[source.Source] || source.SourceRank < 1 || source.SourceRank > 64 || seenRanks[source.Source][source.SourceRank] || source.Score == nil || source.FusionScore == nil {
				return q, fmt.Errorf("verification Hybrid source %q: %+v", doc.ID, source)
			}
			seenSource[source.Source], seenRanks[source.Source][source.SourceRank] = true, true
			if source.Source == "text" {
				if source.IndexName != "content" || (source.ScoreKind != "bm25" && source.ScoreKind != "bm25f") || *source.Score <= 0 {
					return q, fmt.Errorf("verification Hybrid text source %q: %+v", doc.ID, source)
				}
				q.TextContributions++
			} else {
				if source.IndexName != "embedding" || source.ScoreKind != "vector_similarity" || math.Abs(*source.Score-commandPerfCosine(ordinal)) > 1e-5 {
					return q, fmt.Errorf("verification Hybrid vector source %q: %+v", doc.ID, source)
				}
				q.VectorContributions++
				q.MeanReturnedVectorCosine += commandPerfCosine(ordinal)
			}
			contribution := 1 / float64(60+source.SourceRank)
			q.MaximumRRFError = math.Max(q.MaximumRRFError, math.Abs(*source.FusionScore-contribution))
			fused += contribution
		}
		q.MaximumRRFError = math.Max(q.MaximumRRFError, math.Max(math.Abs(*meta.FusedScore-fused), math.Abs(*doc.Score-fused)))
		if q.MaximumRRFError > 1e-12 {
			return q, fmt.Errorf("verification Hybrid RRF score %q error=%g", doc.ID, q.MaximumRRFError)
		}
		if len(meta.Sources) == 2 {
			q.BothSources++
		}
		q.DocumentsChecked++
	}
	if q.VectorContributions > 0 {
		q.MeanReturnedVectorCosine /= float64(q.VectorContributions)
	}
	return q, nil
}

func commandPerfVerifyDocuments(docs []documentservice.Document, filtered, exact bool) error {
	if len(docs) != 10 {
		return fmt.Errorf("verification result count=%d", len(docs))
	}
	var expected []float64
	if exact {
		for ordinal := 0; ordinal < 4096; ordinal += 16 {
			expected = append(expected, commandPerfCosine(ordinal))
		}
		sort.Sort(sort.Reverse(sort.Float64Slice(expected)))
	}
	var seen [4096]bool
	previous := math.Inf(1)
	for i, doc := range docs {
		ordinal, err := commandPerfOrdinal(doc.ID)
		if err != nil || seen[ordinal] || (filtered && ordinal%16 != 0) || len(doc.Embedding) != 0 || doc.Content != "refund policy "+strings.Repeat("payload ", 32) || doc.Meta["tenant"] != fmt.Sprintf("t%02d", ordinal%16) || fmt.Sprint(doc.Meta["rank"]) != strconv.Itoa(ordinal) || doc.Score == nil || math.IsNaN(*doc.Score) || math.IsInf(*doc.Score, 0) || *doc.Score > previous+1e-6 {
			return fmt.Errorf("verification result content/membership/score %q", doc.ID)
		}
		if exact && (math.Abs(*doc.Score-commandPerfCosine(ordinal)) > 1e-5 || math.Abs(*doc.Score-expected[i]) > 1e-5) {
			return fmt.Errorf("verification exact rank%d id%s score%g want%g", i, doc.ID, *doc.Score, expected[i])
		}
		seen[ordinal], previous = true, *doc.Score
	}
	return nil
}

// Reject alternative spellings as well as values outside the fixed seed corpus.
func commandPerfOrdinal(id string) (int, error) {
	if len(id) != 7 || id[0] != 'd' {
		return 0, fmt.Errorf("noncanonical corpus ID %q", id)
	}
	ordinal, err := strconv.Atoi(id[1:])
	if err != nil || ordinal < 0 || ordinal >= 4096 || fmt.Sprintf("d%06d", ordinal) != id {
		return 0, fmt.Errorf("noncanonical corpus ID %q", id)
	}
	return ordinal, nil
}

func TestCommandPerfResultValidation(t *testing.T) {
	docs := make([]documentservice.Document, 10)
	for i := range docs {
		docs[i] = documentservice.Document{ID: fmt.Sprintf("d%06d", i*16), Content: "refund policy", Meta: map[string]any{"tenant": "t00"}}
	}
	if err := commandPerfDocuments(&commandPerfReport{}, docs); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{docs[1].ID, "d000001", "d004096", "d+00000", "d00000"} {
		t.Run(id, func(t *testing.T) {
			changed := append([]documentservice.Document(nil), docs...)
			changed[0].ID = id
			if err := commandPerfDocuments(&commandPerfReport{}, changed); err == nil {
				t.Fatalf("accepted invalid result %q", id)
			}
		})
	}
	t.Run("outside-timed-native-ann", func(t *testing.T) {
		// Distinct corpus members with independently derived scores exercise the
		// verifier without claiming an exact ANN top-K or tied-ID policy.
		ordinals := make([]int, 17)
		for i := range ordinals {
			ordinals[i] = i
		}
		sort.Slice(ordinals, func(i, j int) bool { return commandPerfCosine(ordinals[i]) > commandPerfCosine(ordinals[j]) })
		results := make([]nativewire.DenseVectorSearchResult, 10)
		for i, ordinal := range ordinals[:10] {
			results[i] = nativewire.DenseVectorSearchResult{ID: []byte(fmt.Sprintf("d%06d", ordinal)), Score: commandPerfCosine(ordinal)}
		}
		for _, route := range []struct {
			name           string
			prepared, pack uint64
		}{{"prepared", 1, 0}, {"search-pack", 0, 1}} {
			t.Run(route.name, func(t *testing.T) {
				valid := nativewire.DenseVectorSearchResponse{Route: documentservice.RouteAnn, ColumnGraphPreparedSearch: route.prepared, SearchPackSearch: route.pack, Results: results}
				if err := commandPerfVerifyNativeANN(valid); err != nil {
					t.Fatal(err)
				}
				for _, mutation := range []string{"no-route", "both-routes", "multiple-prepared", "multiple-search-pack", "counter-wrap", "exact-route", "exact-result", "typed-result", "document-scan", "exact-fallback", "scan-fallback", "missing-result", "extra-result", "duplicate-id", "noncorpus-id", "noncanonical-id", "wrong-score", "nan-score", "positive-infinity", "negative-infinity", "unordered-results"} {
					t.Run(mutation, func(t *testing.T) {
						changed := valid
						changed.Results = append([]nativewire.DenseVectorSearchResult(nil), valid.Results...)
						switch mutation {
						case "no-route":
							changed.ColumnGraphPreparedSearch, changed.SearchPackSearch = 0, 0
						case "both-routes":
							changed.ColumnGraphPreparedSearch, changed.SearchPackSearch = 1, 1
						case "multiple-prepared":
							changed.ColumnGraphPreparedSearch, changed.SearchPackSearch = 2, 0
						case "multiple-search-pack":
							changed.ColumnGraphPreparedSearch, changed.SearchPackSearch = 0, 2
						case "counter-wrap":
							changed.ColumnGraphPreparedSearch, changed.SearchPackSearch = ^uint64(0), 2
						case "exact-route":
							changed.Route = documentservice.RouteExact
						case "exact-result":
							changed.Exact = true
						case "typed-result":
							changed.TypedColumnGraph = true
						case "document-scan":
							changed.DocumentsScanned = 1
						case "exact-fallback":
							changed.ExactFallbacks = 1
						case "scan-fallback":
							changed.FullDocumentScanFallbacks = 1
						case "missing-result":
							changed.Results = changed.Results[:9]
						case "extra-result":
							changed.Results = append(changed.Results, changed.Results[0])
						case "duplicate-id":
							changed.Results[1] = changed.Results[0]
						case "noncorpus-id":
							changed.Results[0].ID = []byte("d004096")
						case "noncanonical-id":
							changed.Results[0].ID = []byte("d+00000")
						case "wrong-score":
							changed.Results[0].Score += 0.1
						case "nan-score":
							changed.Results[0].Score = math.NaN()
						case "positive-infinity":
							changed.Results[0].Score = math.Inf(1)
						case "negative-infinity":
							changed.Results[0].Score = math.Inf(-1)
						case "unordered-results":
							changed.Results[0], changed.Results[9] = changed.Results[9], changed.Results[0]
						}
						if err := commandPerfVerifyNativeANN(changed); err == nil {
							t.Fatalf("accepted corrupted native ANN response %s", mutation)
						}
					})
				}
			})
		}
	})
	t.Run("outside-timed-exact-oracle", func(t *testing.T) {
		ordinals := make([]int, 0, 256)
		for ordinal := 0; ordinal < 4096; ordinal += 16 {
			ordinals = append(ordinals, ordinal)
		}
		sort.SliceStable(ordinals, func(i, j int) bool { return commandPerfCosine(ordinals[i]) > commandPerfCosine(ordinals[j]) })
		var exact []documentservice.Document
		for _, ordinal := range ordinals[:10] {
			score := commandPerfCosine(ordinal)
			exact = append(exact, documentservice.Document{ID: fmt.Sprintf("d%06d", ordinal), Content: "refund policy " + strings.Repeat("payload ", 32), Meta: map[string]any{"tenant": "t00", "rank": float64(ordinal)}, Score: &score})
		}
		if err := commandPerfVerifyDocuments(exact, true, true); err != nil {
			t.Fatal(err)
		}
		for _, mutation := range []string{"wrong-score", "nan", "wrong-payload", "wrong-rank", "non-top-result"} {
			t.Run(mutation, func(t *testing.T) {
				changed := append([]documentservice.Document(nil), exact...)
				switch mutation {
				case "wrong-score":
					score := 0.0
					changed[0].Score = &score
				case "nan":
					score := math.NaN()
					changed[0].Score = &score
				case "wrong-payload":
					changed[0].Content = "policy"
				case "wrong-rank":
					changed[0].Meta = map[string]any{"tenant": "t00", "rank": -1}
				case "non-top-result":
					ordinal := ordinals[len(ordinals)-1]
					score := commandPerfCosine(ordinal)
					changed[9] = documentservice.Document{ID: fmt.Sprintf("d%06d", ordinal), Content: exact[0].Content, Meta: map[string]any{"tenant": "t00", "rank": ordinal}, Score: &score}
				}
				if err := commandPerfVerifyDocuments(changed, true, true); err == nil {
					t.Fatalf("accepted corrupted oracle result %s", mutation)
				}
			})
		}
	})
	t.Run("outside-timed-ann-quality-ties", func(t *testing.T) {
		best, worst := 0, 0
		for i := 1; i < 17; i++ {
			if commandPerfCosine(i) > commandPerfCosine(best) {
				best = i
			}
			if commandPerfCosine(i) < commandPerfCosine(worst) {
				worst = i
			}
		}
		ordinals := make([]int, 10)
		for i := range ordinals {
			// Deliberately use later tied IDs, not a canonical exact-ID top ten.
			ordinals[i] = best + 17*(i+100)
		}
		q := commandPerfMeasureANNQuality(ordinals)
		if q.TieAwareRecallAtK != 1 || q.MeanCosineRegret != 0 || q.ExactTiedAtCutoff < 10 || q.ExactStrictlyAboveCutoff != 0 {
			t.Fatalf("tied optimum quality=%+v", q)
		}
		ordinals[9] = worst
		q = commandPerfMeasureANNQuality(ordinals)
		if q.TieAwareRecallAtK != .9 || q.MeanCosineRegret <= 0 {
			t.Fatalf("one low-quality result not measured: %+v", q)
		}
		for i := range ordinals {
			ordinals[i] = worst + 17*i
		}
		q = commandPerfMeasureANNQuality(ordinals)
		if q.TieAwareRecallAtK != 0 || q.MeanCosineRegret <= 0 {
			t.Fatalf("valid but poor ANN quality not measured: %+v", q)
		}
	})
	t.Run("outside-timed-hybrid-rrf", func(t *testing.T) {
		makeResponse := func() documentservice.HybridSearchResponse {
			response := documentservice.HybridSearchResponse{Plan: collections.HybridSearchPlan{FusionMethod: collections.HybridFusionMethodRRF, TextCandidateLimit: 64, VectorCandidateLimit: 64, FinalTopK: 10}}
			for i := 0; i < 10; i++ {
				text, vector, contribution := 1.0, commandPerfCosine(i), 1/float64(61+i)
				fused := 2 * contribution
				meta := commandPerfHybridMeta{Type: "hybrid", Rank: i + 1, FusionMethod: "rrf", FusedScore: &fused, Sources: []commandPerfHybridSource{
					{Source: "text", IndexName: "content", SourceRank: i + 1, Score: &text, ScoreKind: "bm25f", FusionScore: &contribution},
					{Source: "vector", IndexName: "embedding", SourceRank: i + 1, Score: &vector, ScoreKind: "vector_similarity", FusionScore: &contribution},
				}}
				response.Documents = append(response.Documents, documentservice.Document{ID: fmt.Sprintf("d%06d", i), Score: &fused, Meta: map[string]any{"_treedb_search": meta}})
			}
			return response
		}
		quality, err := commandPerfVerifyHybridRRF(makeResponse())
		if err != nil || quality.DocumentsChecked != 10 || quality.TextContributions != 10 || quality.VectorContributions != 10 || quality.BothSources != 10 || quality.MaximumRRFError != 0 {
			t.Fatalf("valid returned RRF contributions: quality=%+v err=%v", quality, err)
		}
		for _, mutation := range []string{"missing-metadata", "wrong-plan", "wrong-rank", "wrong-method", "no-sources", "duplicate-source", "duplicate-source-rank", "out-of-bounds-source-rank", "wrong-index", "wrong-vector-score", "wrong-contribution", "wrong-fused-score", "wrong-document-score", "missing-source-score", "nonfinite-source-score"} {
			t.Run(mutation, func(t *testing.T) {
				response := makeResponse()
				doc := &response.Documents[0]
				meta := doc.Meta["_treedb_search"].(commandPerfHybridMeta)
				wrong := 0.5
				switch mutation {
				case "missing-metadata":
					meta = commandPerfHybridMeta{}
				case "wrong-plan":
					response.Plan.VectorCandidateLimit = 128
				case "wrong-rank":
					meta.Rank = 2
				case "wrong-method":
					meta.FusionMethod = "normalized_score"
				case "no-sources":
					meta.Sources = nil
				case "duplicate-source":
					meta.Sources[1] = meta.Sources[0]
				case "duplicate-source-rank":
					meta.Sources[0].SourceRank = 2
					contribution := 1 / float64(62)
					meta.Sources[0].FusionScore = &contribution
					fused := contribution + *meta.Sources[1].FusionScore
					meta.FusedScore, doc.Score = &fused, &fused
				case "out-of-bounds-source-rank":
					meta.Sources[1].SourceRank = 65
				case "wrong-index":
					meta.Sources[1].IndexName = "retired"
				case "wrong-vector-score":
					meta.Sources[1].Score = &wrong
				case "wrong-contribution":
					meta.Sources[1].FusionScore = &wrong
				case "wrong-fused-score":
					meta.FusedScore = &wrong
				case "wrong-document-score":
					doc.Score = &wrong
				case "missing-source-score":
					meta.Sources[1].Score = nil
				case "nonfinite-source-score":
					nan := math.NaN()
					meta.Sources[1].Score = &nan
				}
				doc.Meta["_treedb_search"] = meta
				if _, err := commandPerfVerifyHybridRRF(response); err == nil {
					t.Fatalf("accepted corrupted returned Hybrid contribution %s", mutation)
				}
			})
		}
	})
}

func startCommandPerf(ctx context.Context, logPath, bin, dir string, httpCommand, legacy bool) (*commandPerfProcess, error) {
	address := func() (string, error) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return "", err
		}
		addr := l.Addr().String()
		return addr, l.Close()
	}
	mainAddr, err := address()
	if err != nil {
		return nil, err
	}
	diag, err := address()
	if err != nil {
		return nil, err
	}
	p := &commandPerfProcess{http: "http://" + mainAddr, diagnostics: "http://" + diag, wire: mainAddr, client: &http.Client{Timeout: 30 * time.Second}}
	args := []string{"-dir", dir, "-profile", commandPerfProfile, "-addr", mainAddr, "-pprof", diag}
	if httpCommand {
		p.wire, err = address()
		if err != nil {
			return nil, err
		}
		args = append(args, "-native-addr", p.wire)
	}
	if legacy {
		args = append(args, "-legacy-storage")
	}
	p.log, err = os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	p.cmd = exec.CommandContext(ctx, bin, args...)
	p.cmd.Stdout, p.cmd.Stderr = p.log, p.log
	if err = p.cmd.Start(); err != nil {
		p.log.Close()
		return nil, err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		if httpCommand {
			req, _ := http.NewRequestWithContext(probe, http.MethodGet, p.http+"/v1/health", nil)
			res, e := p.client.Do(req)
			err = e
			if e == nil {
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if res.StatusCode != http.StatusOK {
					err = fmt.Errorf("health status%d", res.StatusCode)
				}
			}
		} else {
			var c *nativewire.Client
			c, err = nativewire.DialContext(probe, "tcp", p.wire)
			if err == nil {
				c.Close()
			}
		}
		cancel()
		if err == nil {
			return p, nil
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			stopErr := p.stop()
			return nil, errors.Join(fmt.Errorf("command readiness: %w (log %s)", err, logPath), stopErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func (p *commandPerfProcess) post(ctx context.Context, path string, input, output any) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.http+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("POST%s status%d: %s", path, res.StatusCode, body)
	}
	if output != nil {
		return json.Unmarshal(body, output)
	}
	return nil
}
func (p *commandPerfProcess) stop() error {
	if p == nil || p.closed {
		return nil
	}
	p.closed = true
	defer p.log.Close()
	p.client.CloseIdleConnections()
	signalErr := p.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		return errors.Join(signalErr, err)
	case <-time.After(30 * time.Second):
		killErr := p.cmd.Process.Kill()
		return errors.Join(signalErr, errors.New("command shutdown timed out"), killErr, <-done)
	}
}
func commandPerfSample(ctx context.Context, p *commandPerfProcess, dir string, httpCommand bool) commandPerfResources {
	var out commandPerfResources
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", p.cmd.Process.Pid))
	if err != nil {
		out.Missing = append(out.Missing, "RSS: "+err.Error())
	} else {
		fields := strings.Fields(string(raw))
		if len(fields) > 1 {
			pages, e := strconv.ParseInt(fields[1], 10, 64)
			if e == nil {
				out.RSSBytes = pages * int64(os.Getpagesize())
			} else {
				out.Missing = append(out.Missing, "RSS: "+e.Error())
			}
		}
	}
	if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, e := entry.Info()
			if e != nil {
				return e
			}
			out.FileBytes += info.Size()
		}
		return nil
	}); err != nil {
		out.Missing = append(out.Missing, "files: "+err.Error())
	}
	if !httpCommand {
		out.Missing = append(out.Missing, "native command does not export same-owner DB/collection diagnostics")
		return out
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.diagnostics+"/debug/treedb/stats", nil)
	res, err := p.client.Do(req)
	if err != nil {
		out.Missing = append(out.Missing, "diagnostics: "+err.Error())
		return out
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		out.Missing = append(out.Missing, fmt.Sprintf("diagnostics status%d", res.StatusCode))
		return out
	}
	out.Diagnostics = &documentservice.DiagnosticsSnapshot{}
	if err := json.NewDecoder(res.Body).Decode(out.Diagnostics); err != nil {
		out.Diagnostics = nil
		out.Missing = append(out.Missing, "diagnostics: "+err.Error())
	}
	return out
}
