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
	report := commandPerfReport{Version: "actual-command-v1", Command: os.Getenv("GOMAP_COMMAND_PERF_COMMAND"), Operation: os.Getenv("GOMAP_COMMAND_PERF_OPERATION"), Label: os.Getenv("GOMAP_COMMAND_PERF_LABEL"), HTTPBinary: os.Getenv("GOMAP_COMMAND_PERF_HTTP_BINARY"), NativeBinary: os.Getenv("GOMAP_COMMAND_PERF_NATIVE_BINARY"), Directory: prefix + ".db", Requested: 1000, AllocationEvidence: "Server TotalAlloc/Mallocs are not exported by these command diagnostics. Driver Go B/op is inapplicable; use the separately reviewed in-process service benchmark for allocation attribution."}
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
	report.CleanupEvidence = "Records explicit Flushed FlushAll then Synced Checkpoint, process exit and logs; baseline a621 standalone main ignores service/backend deferred Close errors, so its clean process exit does not prove those Close calls succeeded. Candidate owner Close propagates errors. No equivalence claim for unavailable baseline cleanup errors."
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
	declaration := documentservice.CreateIndexRequest{Name: "docs", Dimension: 8, VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime, M: 16, EfConstruction: 128, EfSearch: 64}, ScalarFields: []documentservice.ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: documentservice.ScalarFieldString}, {Field: "meta.rank", ValueType: documentservice.ScalarFieldInt64}}}
	var created struct {
		Index documentservice.IndexInfo `json:"index"`
	}
	if err := seed.post(ctx, "/v1/indexes", declaration, &created); err != nil {
		return err
	}
	r.Index = created.Index
	if r.Index.VectorStrategy != collections.VectorIndexStrategyNativeRuntime {
		return fmt.Errorf("declared native_runtime, got %q", r.Index.VectorStrategy)
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
	r.Phase = "maintenance"
	if err := wire.FlushAllWithAck(ctx, nativewire.AckFlushed); err != nil {
		return err
	}
	if err := wire.CheckpointWithAck(ctx, nativewire.AckSynced); err != nil {
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
			if response.Stats.FullDocumentScanFallbacks != 0 || response.Stats.FailClosed != 0 || response.Stats.Truncated || response.Stats.Unavailable {
				return fmt.Errorf("keyword route: %+v", response.Stats)
			}
			return commandPerfDocuments(r, response.Documents)
		}, nil
	case "hybrid":
		return func(ctx context.Context) error {
			var response documentservice.HybridSearchResponse
			if err := p.post(ctx, "/v1/indexes/docs/search/hybrid", documentservice.HybridSearchRequest{Query: "policy", QueryEmbedding: query, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 64, Filter: filter}, &response); err != nil {
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
				if err := p.post(ctx, "/v1/indexes/docs/search/vector", documentservice.DenseVectorSearchRequest{QueryEmbedding: query, TopK: 10, Route: documentservice.RouteAnn, EfSearch: 64, Filter: filter}, &response); err != nil {
					return err
				}
				r.LastDenseRoute = response.Route
				if response.Route != documentservice.RouteAnn || !response.NativeBasePlusLiveDelta || response.ExactFallbacks != 0 || response.FullDocumentScanFallbacks != 0 {
					return fmt.Errorf("Dense route: %+v", response)
				}
				return commandPerfDocuments(r, response.Documents)
			}
			response, err := wire.DenseVectorSearch(ctx, nativewire.DenseVectorSearchRequest{Index: "docs", Query: query, TopK: 10, EfSearch: 64, Filter: filter})
			if err != nil {
				return err
			}
			r.LastDenseRoute = response.Route
			if response.Route != documentservice.RouteAnn || !response.NativeBasePlusLiveDelta || response.ExactFallbacks != 0 || response.FullDocumentScanFallbacks != 0 || len(response.Results) != 10 {
				return fmt.Errorf("wire Dense route/results: %+v", response)
			}
			var seen [4096]bool
			for _, row := range response.Results {
				ordinal, e := commandPerfOrdinal(string(row.ID))
				if e != nil || ordinal%16 != 0 || seen[ordinal] {
					return fmt.Errorf("wire Dense duplicate/noncorpus ID outside scalar membership %q", row.ID)
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
		if err != nil || ordinal%16 != 0 || seen[ordinal] || doc.Meta["tenant"] != "t00" || len(doc.Embedding) != 0 || !strings.Contains(doc.Content, "policy") {
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
