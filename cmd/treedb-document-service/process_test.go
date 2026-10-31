package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
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

// Read addresses selected by the child, which keeps all three listeners bound.
// Reserving then closing ports in the parent permits both reuse and external theft.
func commandListenerAddresses(raw []byte) (base, diagnostics, native string, err error) {
	// Ignore a final partial line until the child finishes writing it.
	raw = raw[:bytes.LastIndexByte(raw, '\n')+1]
	for _, line := range strings.Split(string(raw), "\n") {
		for _, endpoint := range []struct {
			prefix, suffix string
			target         *string
		}{
			{"TreeDB Document Service listening on http://", "", &base},
			{"TreeDB Document Service pprof listening on http://", "/debug/pprof/", &diagnostics},
			{"TreeDB Document Service native wire listening on ", "", &native},
		} {
			_, addr, found := strings.Cut(line, endpoint.prefix)
			if !found {
				continue
			}
			addr = strings.TrimSuffix(strings.TrimSpace(addr), endpoint.suffix)
			host, port, splitErr := net.SplitHostPort(addr)
			number, numberErr := strconv.Atoi(port)
			if splitErr != nil || numberErr != nil || host != "127.0.0.1" || number <= 0 || number > 65535 {
				return "", "", "", fmt.Errorf("invalid bound listener address %q", addr)
			}
			*endpoint.target = addr
		}
	}
	if base != "" && diagnostics != "" && native != "" {
		if base == diagnostics || base == native || diagnostics == native {
			return "", "", "", fmt.Errorf("duplicate bound listener addresses: %s %s %s", base, diagnostics, native)
		}
	}
	return
}

type documentCommand struct {
	cmd                       *exec.Cmd
	base, diagnostics, native string
	client                    *http.Client
	log                       *os.File
}

func startDocumentCommand(t *testing.T, dir, profile string, extra ...string) *documentCommand {
	t.Helper()
	p := &documentCommand{client: &http.Client{Timeout: 5 * time.Second}}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)
	p.cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDocumentServiceCommandProcess$")
	args := append([]string{"-dir", dir, "-profile", profile, "-addr", "127.0.0.1:0", "-pprof", "127.0.0.1:0", "-native-addr", "127.0.0.1:0"}, extra...)
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
		raw, err := os.ReadFile(p.log.Name())
		if err != nil {
			t.Fatal(err)
		}
		base, diagnostics, native, err := commandListenerAddresses(raw)
		if err != nil {
			t.Fatalf("command addresses: %v\n%s", err, raw)
		}
		if base != "" && diagnostics != "" && native != "" {
			p.base, p.diagnostics, p.native = "http://"+base, "http://"+diagnostics, native
			res, requestErr := p.client.Get(p.diagnostics + "/debug/treedb/stats")
			err = requestErr
			if err == nil {
				_ = res.Body.Close()
				if res.StatusCode == http.StatusOK {
					return p
				}
				err = fmt.Errorf("diagnostics status %d", res.StatusCode)
			}
		}
		if time.Now().After(deadline) {
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

func TestCommandListenerAddresses(t *testing.T) {
	const app = "TreeDB Document Service listening on http://127.0.0.1:10001\n"
	const diagnostics = "2026/10/29 00:00:00 TreeDB Document Service pprof listening on http://127.0.0.1:10002/debug/pprof/\n"
	const native = "TreeDB Document Service native wire listening on 127.0.0.1:10003\n"
	for _, tc := range []struct {
		name, log  string
		wantNative string
		wantErr    bool
	}{
		{"all", app + diagnostics + native, "127.0.0.1:10003", false},
		{"reordered", native + app + diagnostics, "127.0.0.1:10003", false},
		{"partial", app + diagnostics + strings.TrimSuffix(native, "\n"), "", false},
		{"requested-zero", strings.ReplaceAll(app+diagnostics+native, ":10001", ":0"), "", true},
		{"same-address", strings.ReplaceAll(app+diagnostics+native, ":10003", ":10002"), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, diag, wire, err := commandListenerAddresses([]byte(tc.log))
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && (base != "127.0.0.1:10001" || diag != "127.0.0.1:10002" || wire != tc.wantNative) {
				t.Fatalf("addresses=%q,%q,%q", base, diag, wire)
			}
		})
	}
}

func TestDocumentServiceCommandEphemeralListeners(t *testing.T) {
	p := startDocumentCommand(t, t.TempDir(), "no_wal_fast")
	addresses := []string{strings.TrimPrefix(p.base, "http://"), strings.TrimPrefix(p.diagnostics, "http://"), p.native}
	seen := make(map[string]bool)
	for _, addr := range addresses {
		if seen[addr] {
			t.Fatalf("listeners share address %s", addr)
		}
		seen[addr] = true
		_, port, err := net.SplitHostPort(addr)
		if err != nil || port == "0" {
			t.Fatalf("unresolved listener address %q: %v", addr, err)
		}
	}
	res, err := p.client.Get(p.base + "/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("HTTP health status %d", res.StatusCode)
	}
	_ = p.snapshot(t)
	wire, err := nativewire.DialContext(t.Context(), "tcp", p.native)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wire.ListCollections(t.Context()); err != nil {
		t.Fatal(err)
	}
	_ = wire.Close()
	p.stop(t)
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
				p.postError(t, "/v1/indexes", req, http.StatusNotImplemented, documentservice.CodeUnsupported)
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
				// Unsupported filtered vector routes refuse even when the graph is
				// prepared, and after reopening. An intentional exact read remains
				// a separate supported route over the same current documents.
				for _, route := range []documentservice.Route{"", documentservice.RouteAnn} {
					p.postError(t, "/v1/indexes/docs/search/vector", documentservice.DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8, Filter: filter, Route: route}, http.StatusNotImplemented, documentservice.CodeUnsupported)
				}
				for _, query := range []string{"", "refund"} {
					p.postError(t, "/v1/indexes/docs/search/hybrid", documentservice.HybridSearchRequest{Query: query, QueryEmbedding: []float32{1, 0}, TopK: 1, TextCandidateLimit: 8, VectorCandidateLimit: 8, EfSearch: 8, Filter: filter}, http.StatusNotImplemented, documentservice.CodeUnsupported)
				}
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
			var wireErr *nativewire.WireError
			// UnsupportedFeature is stable native-wire error code 3; its enum
			// lives below TreeDB's internal import boundary.
			if !errors.As(err, &wireErr) || wireErr.Code != 3 || len(got.Results) != 0 || got.Route != "" || got.Exact || got.SearchPackSearch != 0 || got.ColumnGraphPreparedSearch != 0 || got.DocumentsScanned != 0 {
				t.Fatalf("filtered wire Dense must fail closed: %+v %v", got, err)
			}
			got, err = wire.DenseVectorSearch(denseCtx, nativewire.DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1, EfSearch: 8})
			if err != nil || len(got.Results) != 1 || got.Route != documentservice.RouteAnn || got.TypedColumnGraph || got.Exact || got.SearchPackSearch+got.ColumnGraphPreparedSearch != 1 || got.DocumentsScanned != 0 || got.ExactFallbacks != 0 || got.FullDocumentScanFallbacks != 0 {
				t.Fatalf("unfiltered wire Dense: %+v %v", got, err)
			}
			_ = wire.Close()
			p.stop(t)
			for reopen := 0; reopen < 2; reopen++ {
				p = startDocumentCommand(t, dir, profile)
				check(p, true)
				p.stop(t)
			}
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

func TestDocumentServiceCommandWireCreatedModernSchemaLifecycle(t *testing.T) {
	dir := t.TempDir()
	p := startDocumentCommand(t, dir, "command_wal_durable")
	wire, err := nativewire.DialContext(t.Context(), "tcp", p.native)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wire.Close() })
	// This declaration crosses the actual wire create decoder. It contains
	// both modern vector and text metadata, independently of service CreateIndex.
	meta := collections.CollectionMeta{
		Name: "wire_schema",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: &collections.ColumnStoreConfig{
			Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadFull, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
			Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}},
		}},
		VectorIndexes: []collections.VectorIndexDefinition{{Name: "embedding", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, Encoding: collections.VectorIndexEncodingFloat32, Strategy: collections.VectorIndexStrategyColumnGraph, SchemaGeneration: 1}},
		TextIndexes:   []collections.TextIndexDefinition{{Name: "content", Fields: []collections.TextIndexField{{Field: "content"}}, Analyzer: collections.TextAnalyzerSimple, StorePositions: true, SchemaGeneration: 1}},
	}
	created, err := wire.CreateCollection(t.Context(), meta)
	if err != nil || len(created.VectorIndexes) != 1 || created.VectorIndexes[0].Strategy != collections.VectorIndexStrategyColumnGraph || len(created.TextIndexes) != 1 || created.TextIndexes[0].Name != "content" {
		t.Fatalf("wire-created modern declaration: %+v %v", created, err)
	}
	id := []byte("wire")
	doc := []byte(`{"id":"wire","content":"refund wire","embedding":[1,0]}`)
	if _, err := wire.InsertBatch(t.Context(), meta.Name, collections.DocumentFormatJSON, [][]byte{id}, [][]byte{doc}, nativewire.AckVisible); err != nil {
		t.Fatal(err)
	}
	checkKeyword := func(p *documentCommand) {
		t.Helper()
		var out documentservice.KeywordSearchResponse
		p.post(t, "/v1/indexes/wire_schema/search/keyword", documentservice.KeywordSearchRequest{Query: "refund", TopK: 1}, &out)
		if len(out.Documents) != 1 || out.Documents[0].ID != "wire" || out.Stats.FullDocumentScanFallbacks != 0 {
			t.Fatalf("wire-created text search: %+v", out)
		}
	}
	checkKeyword(p)
	p.postError(t, "/v1/indexes/wire_schema/search/vector", documentservice.DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8}, http.StatusServiceUnavailable, documentservice.CodeIndexUnavailable)
	p.post(t, "/v1/indexes/wire_schema/optimize", documentservice.OptimizeIndexRequest{}, nil)
	checkDense := func(wire *nativewire.Client) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		out, err := wire.DenseVectorSearch(ctx, nativewire.DenseVectorSearchRequest{Index: meta.Name, Query: []float32{1, 0}, TopK: 1, EfSearch: 8})
		if err != nil || len(out.Results) != 1 || string(out.Results[0].ID) != "wire" || out.Route != documentservice.RouteAnn || out.TypedColumnGraph || out.Exact || out.SearchPackSearch+out.ColumnGraphPreparedSearch != 1 || out.DocumentsScanned != 0 || out.ExactFallbacks != 0 || out.FullDocumentScanFallbacks != 0 {
			t.Fatalf("wire-created modern prepared Dense: %+v %v", out, err)
		}
	}
	checkDense(wire)
	if err := wire.FlushAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckpointWithAck(t.Context(), nativewire.AckSynced); err != nil {
		t.Fatal(err)
	}
	_ = wire.Close()
	p.stop(t)
	for reopen := 0; reopen < 2; reopen++ {
		p = startDocumentCommand(t, dir, "command_wal_durable")
		wire, err = nativewire.DialContext(t.Context(), "tcp", p.native)
		if err != nil {
			t.Fatal(err)
		}
		metas, err := wire.ListCollections(t.Context())
		if err != nil || len(metas) != 1 || metas[0].Name != meta.Name || len(metas[0].VectorIndexes) != 1 || metas[0].VectorIndexes[0].Strategy != collections.VectorIndexStrategyColumnGraph || len(metas[0].TextIndexes) != 1 || metas[0].TextIndexes[0].Name != "content" {
			t.Fatalf("reopened wire-created declaration: %+v %v", metas, err)
		}
		got, present, err := wire.GetMany(t.Context(), meta.Name, [][]byte{id})
		if err != nil || len(present) != 1 || !present[0] || len(got) != 1 || !bytes.Equal(got[0], doc) {
			t.Fatalf("reopened wire-created document: %q %v %v", got, present, err)
		}
		checkKeyword(p)
		checkDense(wire)
		_ = wire.Close()
		p.stop(t)
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
	for _, flagName := range []string{"-addr", "-pprof", "-native-addr"} {
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
			args := []string{"-dir", dir, "-addr", "127.0.0.1:0", "-pprof", "127.0.0.1:0", flagName, busy.Addr().String()}
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
