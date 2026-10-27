package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/cmd/internal/documentserviceowner"
)

func TestDocumentServiceCommandProcess(t *testing.T) {
	if os.Getenv("GOMAP_DOCUMENT_COMMAND_CHILD") != "1" {
		return
	}
	os.Args = append([]string{os.Args[0]}, strings.Split(os.Getenv("GOMAP_DOCUMENT_COMMAND_ARGS"), "\n")...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	main()
	os.Exit(0)
}

func commandAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type documentCommand struct {
	cmd                       *exec.Cmd
	base, diagnostics, native string
	client                    *http.Client
	log                       *os.File
}

func startDocumentCommand(t *testing.T, dir, profile string, extra ...string) *documentCommand {
	t.Helper()
	p := &documentCommand{base: "http://" + commandAddress(t), diagnostics: "http://" + commandAddress(t), native: commandAddress(t), client: &http.Client{Timeout: 5 * time.Second}}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)
	p.cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDocumentServiceCommandProcess$")
	args := append([]string{"-dir", dir, "-profile", profile, "-addr", strings.TrimPrefix(p.base, "http://"), "-pprof", strings.TrimPrefix(p.diagnostics, "http://"), "-native-addr", p.native}, extra...)
	p.cmd.Env = append(os.Environ(), "GOMAP_DOCUMENT_COMMAND_CHILD=1", "GOMAP_DOCUMENT_COMMAND_ARGS="+strings.Join(args, "\n"))
	var err error
	p.log, err = os.CreateTemp(t.TempDir(), "command-*.log")
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stdout, p.cmd.Stderr = p.log, p.log
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if p.cmd.ProcessState == nil {
			_ = p.cmd.Process.Kill()
			_ = p.cmd.Wait()
		}
		_ = p.log.Close()
	})
	deadline := time.Now().Add(15 * time.Second)
	for {
		res, err := p.client.Get(p.diagnostics + "/debug/treedb/stats")
		if err == nil {
			_ = res.Body.Close()
			return p
		}
		if time.Now().After(deadline) {
			raw, _ := os.ReadFile(p.log.Name())
			t.Fatalf("command unavailable: %v\n%s", err, raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *documentCommand) snapshot(t *testing.T) documentservice.DiagnosticsSnapshot {
	t.Helper()
	res, err := p.client.Get(p.diagnostics + "/debug/treedb/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out documentservice.DiagnosticsSnapshot
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func (p *documentCommand) postResponse(t *testing.T, path string, input any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	res, err := p.client.Post(p.base+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, body
}

func (p *documentCommand) post(t *testing.T, path string, input, output any) {
	t.Helper()
	status, body := p.postResponse(t, path, input)
	if status != http.StatusOK {
		childLog, logErr := os.ReadFile(p.log.Name())
		t.Fatalf("POST %s: status=%d %s\nchild log (read error=%v):\n%s", path, status, body, logErr, childLog)
	}
	if output != nil {
		if err := json.Unmarshal(body, output); err != nil {
			t.Fatal(err)
		}
	}
}

func (p *documentCommand) postError(t *testing.T, path string, input any, wantStatus int, wantCode documentservice.ErrorCode) {
	t.Helper()
	status, body := p.postResponse(t, path, input)
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("POST %s error body: %s %v", path, body, err)
	}
	var response documentservice.Error
	if err := json.Unmarshal(envelope["error"], &response); err != nil || status != wantStatus || response.Code != wantCode || len(envelope) != 1 {
		t.Fatalf("POST %s: status=%d body=%s decode=%v", path, status, body, err)
	}
	// A refusal cannot carry a successful route/result attribution.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(envelope["error"], &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"documents", "results", "route", "snapshot"} {
		if _, ok := fields[field]; ok {
			t.Fatalf("POST %s refusal carried %s: %s", path, field, body)
		}
	}
}

func (p *documentCommand) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Wait(); err != nil {
		raw, _ := os.ReadFile(p.log.Name())
		t.Fatalf("shutdown: %v\n%s", err, raw)
	}
}

func TestDocumentServiceCommandDefaultActivatesCOW(t *testing.T) {
	p := startDocumentCommand(t, t.TempDir(), "command_wal_durable")
	if got := p.snapshot(t).Database["treedb.cache.memtable_mode"]; got != "cow_btree" {
		t.Fatalf("actual command default uses %q, want active cow_btree", got)
	}
	p.stop(t)
}

func TestDocumentServiceCommandHTTPAndWireLifecycle(t *testing.T) {
	for _, profile := range []string{"command_wal_durable", "command_wal_relaxed", "no_wal_fast"} {
		t.Run(profile, func(t *testing.T) {
			dir := t.TempDir()
			p := startDocumentCommand(t, dir, profile)
			req := documentservice.CreateIndexRequest{Name: "docs", Dimension: 2, ScalarFields: []documentservice.ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: documentservice.ScalarFieldString}, {Field: "meta.rank", ValueType: documentservice.ScalarFieldInt64}}}
			if profile != "command_wal_durable" {
				p.postError(t, "/v1/indexes", req, http.StatusInternalServerError, documentservice.CodeInternal)
				wire, err := nativewire.DialContext(t.Context(), "tcp", p.native)
				if err != nil {
					t.Fatal(err)
				}
				if metas, err := wire.ListCollections(t.Context()); err != nil || len(metas) != 0 {
					t.Fatalf("refused create retained schema: %+v %v", metas, err)
				}
				_ = wire.Close()
				p.stop(t)
				return
			}
			var first, second struct {
				Index documentservice.IndexInfo `json:"index"`
			}
			p.post(t, "/v1/indexes", req, &first)
			p.post(t, "/v1/indexes", req, &second)
			if first.Index.Generation != second.Index.Generation || second.Index.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
				t.Fatalf("idempotence/strategy: %+v %+v", first, second)
			}
			var upsert documentservice.UpsertDocumentsResponse
			p.post(t, "/v1/indexes/docs/documents/upsert", documentservice.UpsertDocumentsRequest{Documents: []documentservice.Document{
				{ID: "a", Content: "refund alpha", Embedding: []float32{1, 0}, Meta: map[string]any{"tenant": "blue", "rank": 1}},
				{ID: "b", Content: "refund beta", Embedding: []float32{0, 1}, Meta: map[string]any{"tenant": "red", "rank": 2}},
			}, DeferVectorIndexRebuild: true}, &upsert)
			stats := p.snapshot(t)
			if stats.LastOpened == nil || stats.LastOpened.Insert.Documents != 2 {
				t.Fatalf("WithStats omitted: %+v", stats.LastOpened)
			}
			p.post(t, "/v1/indexes/docs/optimize", documentservice.OptimizeIndexRequest{}, nil)
			wire, err := nativewire.DialContext(t.Context(), "tcp", p.native)
			if err != nil {
				t.Fatal(err)
			}
			defer wire.Close()
			raw, present, err := wire.GetMany(t.Context(), "docs", [][]byte{[]byte("a")})
			if err != nil || len(present) != 1 || !present[0] || !bytes.Contains(raw[0], []byte("refund alpha")) {
				t.Fatalf("wire did not see HTTP: %q %v %v", raw, present, err)
			}
			doc := []byte(`{"id":"wire","content":"refund wire","embedding":[1,0],"meta":{"tenant":"blue","rank":9}}`)
			if _, err := wire.InsertBatch(t.Context(), "docs", collections.DocumentFormatJSON, [][]byte{[]byte("wire")}, [][]byte{doc}, nativewire.AckVisible); err != nil {
				t.Fatal(err)
			}
			filter := &documentservice.Filter{Operator: "AND", Conditions: []documentservice.Filter{{Field: "meta.tenant", Operator: "==", Value: "blue"}, {Field: "meta.rank", Operator: ">=", Value: 9}}}
			check := func(p *documentCommand, graphReady bool) {
				var keyword documentservice.KeywordSearchResponse
				p.post(t, "/v1/indexes/docs/search/keyword", documentservice.KeywordSearchRequest{Query: "refund", TopK: 1, Filter: filter}, &keyword)
				if len(keyword.Documents) != 1 || keyword.Documents[0].ID != "wire" || len(keyword.Documents[0].Embedding) != 0 || keyword.Stats.FullDocumentScanFallbacks != 0 {
					t.Fatalf("keyword=%+v", keyword)
				}
				var dense documentservice.DenseVectorSearchResponse
				p.post(t, "/v1/indexes/docs/search/vector", documentservice.DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8, Filter: filter, Route: documentservice.RouteExact}, &dense)
				if len(dense.Documents) != 1 || dense.Documents[0].ID != "wire" || dense.Route != documentservice.RouteExact || !dense.Exact {
					t.Fatalf("dense=%+v", dense)
				}
				if !graphReady {
					return
				}
				var hybrid documentservice.HybridSearchResponse
				p.post(t, "/v1/indexes/docs/search/hybrid", documentservice.HybridSearchRequest{Query: "refund", QueryEmbedding: []float32{1, 0}, TopK: 1, TextCandidateLimit: 8, VectorCandidateLimit: 8, EfSearch: 8}, &hybrid)
				if len(hybrid.Documents) != 1 || !hybrid.Snapshot.Native || hybrid.Stats.FullDocumentScanFallbacks != 0 {
					t.Fatalf("hybrid=%+v", hybrid)
				}
			}
			// The same accepted wire mutation is visible to keyword/exact reads
			// before graph maintenance; ANN and mixed Hybrid must refuse it.
			check(p, false)
			p.postError(t, "/v1/indexes/docs/search/vector", documentservice.DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8}, http.StatusServiceUnavailable, documentservice.CodeIndexUnavailable)
			p.postError(t, "/v1/indexes/docs/search/hybrid", documentservice.HybridSearchRequest{Query: "refund", QueryEmbedding: []float32{1, 0}, TopK: 1, TextCandidateLimit: 8, VectorCandidateLimit: 8, EfSearch: 8, Filter: filter}, http.StatusNotImplemented, documentservice.CodeUnsupported)
			p.post(t, "/v1/indexes/docs/optimize", documentservice.OptimizeIndexRequest{}, nil)
			check(p, true)
			denseCtx, denseCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer denseCancel()
			got, err := wire.DenseVectorSearch(denseCtx, nativewire.DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1, EfSearch: 8, Filter: filter})
			if err == nil || len(got.Results) != 0 {
				t.Fatalf("filtered wire Dense must fail closed: %+v %v", got, err)
			}
			got, err = wire.DenseVectorSearch(denseCtx, nativewire.DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1, EfSearch: 8})
			if err != nil || len(got.Results) != 1 || got.Route != documentservice.RouteAnn || got.TypedColumnGraph {
				t.Fatalf("unfiltered wire Dense: %+v %v", got, err)
			}
			_ = wire.Close()
			p.stop(t)
			p = startDocumentCommand(t, dir, profile)
			check(p, true)
			p.stop(t)
		})
	}
}

func TestDocumentServiceCommandPlainWireCRUDProfiles(t *testing.T) {
	for _, profile := range []string{"command_wal_durable", "command_wal_relaxed", "no_wal_fast"} {
		t.Run(profile, func(t *testing.T) {
			p := startDocumentCommand(t, t.TempDir(), profile)
			wire, err := nativewire.DialContext(t.Context(), "tcp", p.native)
			if err != nil {
				t.Fatal(err)
			}
			defer wire.Close()
			ctx := t.Context()
			meta := collections.CollectionMeta{Name: "plain", Indexes: []collections.IndexDefinition{{Name: "tag", Field: "tag", ValueType: collections.IndexValueString}}}
			if _, err := wire.CreateCollection(ctx, meta); err != nil {
				t.Fatal(err)
			}
			ids := [][]byte{[]byte("a")}
			if _, err := wire.InsertBatch(ctx, "plain", collections.DocumentFormatJSON, ids, [][]byte{[]byte(`{"tag":"blue"}`)}, nativewire.AckVisible); err != nil {
				t.Fatal(err)
			}
			if matched, modified, err := wire.ReplaceBatch(ctx, "plain", collections.DocumentFormatJSON, ids, [][]byte{[]byte(`{"tag":"green"}`)}, nativewire.AckVisible); err != nil || matched != 1 || modified != 1 {
				t.Fatalf("plain replace=%d/%d %v", matched, modified, err)
			}
			found, truncated, err := wire.IndexLookup(ctx, "plain", "tag", "green", nativewire.CursorLimits{})
			if err != nil || truncated || len(found) != 1 || !bytes.Equal(found[0], ids[0]) {
				t.Fatalf("plain index=%q truncated=%v %v", found, truncated, err)
			}
			if deleted, err := wire.DeleteBatch(ctx, "plain", ids, nativewire.AckVisible); err != nil || deleted != 1 {
				t.Fatalf("plain delete=%d %v", deleted, err)
			}
			if _, present, err := wire.GetMany(ctx, "plain", ids); err != nil || len(present) != 1 || present[0] {
				t.Fatalf("plain deleted row visible: %v %v", present, err)
			}
			_ = wire.Close()
			p.stop(t)
		})
	}
}

func TestDocumentServiceCommandLegacyStorage(t *testing.T) {
	dir := t.TempDir()
	p := startDocumentCommand(t, dir, "command_wal_durable", "-legacy-storage")
	if p.snapshot(t).Database["treedb.cache.memtable_mode"] == "cow_btree" {
		t.Fatal("legacy flag ignored")
	}
	req := documentservice.CreateIndexRequest{Name: "advanced", Dimension: 2, VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}}
	var response struct {
		Index documentservice.IndexInfo `json:"index"`
	}
	p.post(t, "/v1/indexes", req, &response)
	if response.Index.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatal("ColumnGraph reinterpreted")
	}
	p.stop(t)
	p = startDocumentCommand(t, dir, "command_wal_durable", "-legacy-storage")
	req.VectorIndexOptions = nil
	p.post(t, "/v1/indexes", req, &response)
	if response.Index.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatal("stored ColumnGraph strategy lost")
	}
	p.stop(t)
}

func TestDocumentServiceCommandListenFailureReleasesOwner(t *testing.T) {
	for _, flagName := range []string{"-addr", "-pprof"} {
		t.Run(flagName, func(t *testing.T) {
			busy, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer busy.Close()
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDocumentServiceCommandProcess$")
			args := []string{"-dir", dir, "-addr", commandAddress(t), "-pprof", commandAddress(t), flagName, busy.Addr().String()}
			cmd.Env = append(os.Environ(), "GOMAP_DOCUMENT_COMMAND_CHILD=1", "GOMAP_DOCUMENT_COMMAND_ARGS="+strings.Join(args, "\n"))
			raw, err := cmd.CombinedOutput()
			if err == nil || !bytes.Contains(raw, []byte("Failed to listen")) {
				t.Fatalf("expected listen failure: %v %s", err, raw)
			}
			o, err := documentserviceowner.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir), false)
			if err != nil {
				t.Fatalf("startup leaked original owner: %v", err)
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
