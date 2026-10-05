package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func recallTestInput() (recallInput, recallReport) {
	config := nativewire.FixedPeerTCPConfigV1{
		Nodes:                []nativewire.FixedPeerTCPNodeV1{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}},
		Groups:               []nativewire.FixedPeerTCPGroupV1{{ID: "group-1"}},
		Credentials:          &nativewire.PeerCredentialsV1{},
		VectorInitialization: &nativewire.FixedPeerTCPVectorInitializationV1{SourceGroupID: "group-1", Generation: 1, CatalogEpoch: 1, MaxSourceRows: 10003},
	}
	config.VectorInitialization.Collection.Collection = "docs"
	config.VectorInitialization.IndexDefinition.Name = "embedding_graph"
	config.VectorInitialization.IndexDefinition.Field = "embedding"
	config.VectorInitialization.IndexDefinition.Dimensions = 128
	config.VectorInitialization.IndexDefinition.EfSearch = 64
	gen := public.GenerationIDV1{Index: "embedding_graph", Generation: 1}
	hash := strings.Repeat("a", 64)
	fresh := "campaign/dataset-" + hash + "-fresh-y"
	request := public.InsertRequestV1{Generation: gen, ID: []byte(fresh)}
	bootstrap := nativewire.FixedPeerVectorQualificationV1{Insert: goodInsert(request, 1, 8), Retry: goodInsert(request, 1, 9),
		Before: goodSearch(public.SearchRequestV1{Generation: gen}, "seed-x"), After: goodSearch(public.SearchRequestV1{Generation: gen}, fresh),
		Dataset: &nativewire.FixedPeerVectorDatasetIdentityV1{ManifestSHA256: hash, VectorsSHA256: hash, Rows: 10000, Dimensions: 128, SourceRows: 10003,
			InputBytes: uint64(10000*128*4 + 10000*len("doc-000000") + 3*128*4 + len("seed-x") + len("seed-minus-x") + len("seed-minus-y")), OraclePlaneMaxFraction: .9},
	}
	c := &bootstrap.Prepare.Command
	c.Version = 1
	c.Operation = "prepare"
	c.Collection = "docs"
	c.Index = gen.Index
	c.Group = "group-1"
	c.IndexDefinitionDigest = hash
	c.Generation = 1
	c.MaxSourceRows = 10003
	c.SourceRowCount = 10003
	c.SourceGeneration = 80
	c.SourceChecksum = 2
	c.SourceSchemaHash = 3
	c.Term = 1
	c.IndexPosition = 7
	c.CommandDigest = hash
	bootstrap.Prepare.AssetSetDigest = hash
	bootstrap.Prepare.ManifestDigest = hash
	bootstrap.Prepare.ReadySetDigest = hash
	for _, node := range config.Nodes {
		bootstrap.Readiness = append(bootstrap.Readiness, nativewire.FixedPeerReadinessV1{NodeID: node.ID, Live: true, Ready: true, VectorPhase: "active", CatalogEpoch: 1,
			Groups: []nativewire.FixedPeerGroupReadinessV1{{GroupID: "group-1", Ready: true, LocalAppliedIndex: 1000}}})
	}
	r := recallReport{Version: 1, Phase: "pre", Generation: gen, ConfigSHA256: hash, BootstrapSHA256: hash, ManifestSHA256: hash, HighestCommitIndex: 9,
		Manifest: recallManifest{Files: map[string]recallFile{"documents.f32": {SHA256: hash}}}, RPCTimeout: time.Second}
	r.Provenance = recallProvenance{Version: 1, Phase: "pre", CampaignID: "campaign", BootstrapRequestID: "campaign",
		ConfigSHA256: hash, BootstrapSHA256: hash, ManifestSHA256: hash, RuntimeSourceHead: strings.Repeat("b", 40),
		ServerBinarySHA256: hash, QualificationSourceSHA256: recallQualifierSourceSHA, InitializationReceiptSHA256: hash, CleanReopenReceiptSHA256: hash,
		Roots:        map[string]string{"a": "/owned/a", "b": "/owned/b", "c": "/owned/c", "d": "/owned/d"},
		Hosts:        map[string]string{"a": "host1", "b": "host1", "c": "host2", "d": "host2"},
		RootAccepted: true, InitializationSucceeded: true, CleanReopenSucceeded: true, ExclusiveWriterStopped: true}
	return recallInput{config: config, bootstrap: bootstrap, vectors: map[string][]float32{"seed-x": oracleVector(128, 1, 0), "seed-minus-x": oracleVector(128, -1, 0),
		"seed-minus-y": oracleVector(128, 0, -1), fresh: oracleVector(128, 0, 1)}}, r
}
func TestRecallProvenanceRefusesUnboundPopulation(t *testing.T) {
	tests := map[string]func(*recallInput, *recallReport){
		"owner":            func(i *recallInput, r *recallReport) { i.config.Groups[0].ID = "alien" },
		"root-acceptance":  func(i *recallInput, r *recallReport) { r.Provenance.RootAccepted = false },
		"writer-exclusion": func(i *recallInput, r *recallReport) { r.Provenance.ExclusiveWriterStopped = false },
		"namespace":        func(i *recallInput, r *recallReport) { r.Provenance.BootstrapRequestID = "other" },
		"qualifier-source": func(i *recallInput, r *recallReport) {
			r.Provenance.QualificationSourceSHA256 = strings.Repeat("c", 64)
		},
		"dataset-hash": func(i *recallInput, r *recallReport) { i.bootstrap.Dataset.VectorsSHA256 = strings.Repeat("c", 64) },
		"ready-prefix": func(i *recallInput, r *recallReport) { i.bootstrap.Readiness[2].Groups[0].LocalAppliedIndex = 0 },
		"duplicate-voter": func(i *recallInput, r *recallReport) {
			i.bootstrap.Readiness[2].NodeID = i.bootstrap.Readiness[1].NodeID
		},
		"source-authority": func(i *recallInput, r *recallReport) { i.bootstrap.Prepare.Command.Term = 0 },
		"prepare-digest":   func(i *recallInput, r *recallReport) { i.bootstrap.Prepare.Command.CommandDigest = "" },
		"pre-prior-writes": func(i *recallInput, r *recallReport) { r.Provenance.ProbeSHA256 = strings.Repeat("a", 64) },
	}
	in, r := recallTestInput()
	if _, err := admitBootstrap(in.config, in.bootstrap); err != nil {
		t.Fatal(err)
	}
	if err := recallValidateProvenance(&in, &r); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			in, r := recallTestInput()
			mutate(&in, &r)
			if err := recallValidateProvenance(&in, &r); err == nil {
				t.Fatal("accepted unbound population")
			}
		})
	}
}
func recallTestProbe(t *testing.T, in *recallInput, r *recallReport, freshN int) [2]struct {
	Event  string
	Report report
} {
	t.Helper()
	r.Phase = "post-only"
	r.Provenance.Phase = r.Phase
	r.Provenance.ProbeRunID = "probe"
	r.Provenance.ProbeBinarySHA256 = strings.Repeat("b", 64)
	r.Provenance.ProbeSHA256 = strings.Repeat("c", 64)
	b := in.bootstrap
	o := options{RunID: "probe", Generation: r.Generation, BootstrapID: b.Insert.VisibleID, BootstrapRevision: 1, BootstrapCommitIndex: b.Retry.CommitIndex, OwnerGroup: "group-1", Dimensions: 128,
		FreshInserts: freshN, EfSearch: 64, Timeout: time.Minute, RPCTimeout: time.Second}
	plan, err := makePlan(o)
	if err != nil {
		t.Fatal(err)
	}
	planned := report{Version: 1, RunID: o.RunID, ConfigSHA256: r.ConfigSHA256, BootstrapSHA256: r.BootstrapSHA256, BinarySHA256: r.Provenance.ProbeBinarySHA256,
		ConfigIdentity: r.ConfigIdentity, Generation: r.Generation, Timeout: o.Timeout, RPCTimeout: o.RPCTimeout, Operations: plan}
	finish(&planned)
	raw, _ := json.Marshal(planned)
	var result report
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	commit, revision := b.Retry.CommitIndex, uint64(1)
	for idx := range result.Operations {
		op := &result.Operations[idx]
		op.Outcome = "succeeded"
		op.StartNS = 1
		op.EndNS = 20
		if op.Kind == "insert" {
			op.InsertRequest.Deadline = time.Now().Add(time.Minute)
			op.EndNS = 30
			commit++
			if op.Phase != "explicit-retry" {
				revision++
			}
			response := goodInsert(*op.InsertRequest, revision, commit)
			op.InsertResponse = &response
		} else {
			op.SearchRequest.Deadline = time.Now().Add(time.Minute)
			response := goodSearch(*op.SearchRequest, op.ExpectedID)
			op.SearchResponse = &response
		}
	}
	result.Verdict = "ACCEPT_BOUNDED_FUNCTIONAL_CLIENT_OVERLAP"
	for _, state := range b.Readiness {
		result.Readiness = append(result.Readiness, observation{Round: 0, RequestedNode: string(state.NodeID), State: state})
	}
	finish(&result)
	return [2]struct {
		Event  string
		Report report
	}{{"planned", planned}, {"result", result}}
}
func recallTestProbeRaw(events [2]struct {
	Event  string
	Report report
}) []byte {
	var b bytes.Buffer
	for _, v := range events {
		_ = json.NewEncoder(&b).Encode(v)
	}
	return b.Bytes()
}
func TestRecallProbeLedgerDeduplicatesOnlyProvenRetry(t *testing.T) {
	in, r := recallTestInput()
	events := recallTestProbe(t, &in, &r, 1)
	if err := recallValidateProvenance(&in, &r); err != nil {
		t.Fatal(err)
	}
	if err := recallAdmitProbe(context.Background(), recallTestProbeRaw(events), &in, &r); err != nil {
		t.Fatal(err)
	}
	if len(in.vectors) != 5 || r.HighestCommitIndex != 11 {
		t.Fatalf("population=%d commit=%d", len(in.vectors), r.HighestCommitIndex)
	}
}
func TestRecallProbeRefusesUnknownIncompleteOrForgedLedger(t *testing.T) {
	tests := map[string]func(*[2]struct {
		Event  string
		Report report
	}){
		"unknown": func(e *[2]struct {
			Event  string
			Report report
		}) {
			e[1].Report.Operations[2].Outcome = "unknown"
		},
		"failed-verdict": func(e *[2]struct {
			Event  string
			Report report
		}) {
			e[1].Report.Verdict = "FAILED"
		},
		"missing-op": func(e *[2]struct {
			Event  string
			Report report
		}) {
			e[1].Report.Operations = e[1].Report.Operations[:69]
		},
		"retry-bytes": func(e *[2]struct {
			Event  string
			Report report
		}) {
			e[1].Report.Operations[67].InsertRequest.Document = []byte("{}")
		},
		"revision-gap": func(e *[2]struct {
			Event  string
			Report report
		}) {
			e[1].Report.Operations[2].InsertResponse.LiveRevision = 4
		},
		"forged-count": func(e *[2]struct {
			Event  string
			Report report
		}) {
			e[1].Report.Counts.Unknown = 1
		},
		"old-commit": func(e *[2]struct {
			Event  string
			Report report
		}) {
			e[1].Report.Operations[67].InsertResponse.CommitIndex = 10
		},
		"lost-ready": func(e *[2]struct {
			Event  string
			Report report
		}) {
			e[1].Report.Readiness[0].State.Ready = false
		},
		"wrong-runtime": func(e *[2]struct {
			Event  string
			Report report
		}) {
			e[1].Report.BinarySHA256 = strings.Repeat("d", 64)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			in, r := recallTestInput()
			events := recallTestProbe(t, &in, &r, 1)
			mutate(&events)
			if err := recallAdmitProbe(context.Background(), recallTestProbeRaw(events), &in, &r); err == nil {
				t.Fatal("accepted incomplete/forged mutation population")
			}
		})
	}
	in, r := recallTestInput()
	events := recallTestProbe(t, &in, &r, 1)
	if err := recallAdmitProbe(context.Background(), append(recallTestProbeRaw(events), []byte("{}\n")...), &in, &r); err == nil {
		t.Fatal("accepted trailing event")
	}
}
func recallTestQueries(t testing.TB) (recallInput, recallReport) {
	t.Helper()
	in, r := recallTestInput()
	in.vectors = map[string][]float32{}
	// Corpus directions have lower cosine than the separately proven live row.
	for i := 0; i < 12; i++ {
		id := fmtRecallID(i)
		v := oracleVector(128, 1, float32(i+1))
		in.vectors[id] = v
		in.corpusIDs = append(in.corpusIDs, id)
	}
	q := oracleVector(128, 1, 0)
	scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(q)
	if err != nil {
		t.Fatal(err)
	}
	corpus, err := recallTop10(context.Background(), scorer, in.corpusIDs, in.vectors)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		in.queries = append(in.queries, append([]float32(nil), q...))
		ids := []string{}
		for _, n := range corpus {
			ids = append(ids, n.ID)
		}
		in.exported = append(in.exported, ids)
	}
	in.vectors["live"] = append([]float32(nil), q...)
	if err := recallPlan(context.Background(), &in, &r); err != nil {
		t.Fatal(err)
	}
	return in, r
}
func fmtRecallID(i int) string { return string(rune('a' + i)) }
func TestRecallCanonicalAugmentedTruthAndScoredApproximation(t *testing.T) {
	in, r := recallTestQueries(t)
	q := r.Queries[0]
	if q.Truth[0].ID != "live" || reflect.DeepEqual(q.Truth, q.CorpusTruth) {
		t.Fatal("live population did not displace corpus truth")
	}
	response := public.SearchResponseV1{Generation: r.Generation, Neighbors: append([]public.NeighborV1(nil), q.CorpusTruth...),
		Counters: public.SearchCountersV1{SelectedPartitions: 1, HNSWServedPartitions: 1, ReadProofs: 1}}
	value, err := recallValidateResponse(&q, response, in.vectors)
	if err != nil || value != .9 {
		t.Fatalf("valid approximate response recall=%v err=%v", value, err)
	}
	// FP32 source normalization and stable ID ordering use the exported serving contract.
	scorer, _ := collections.NewCanonicalVectorPartitionCosineScorerV1(oracleVector(128, 1, 0))
	vectors := map[string][]float32{"b": oracleVector(128, 3, 4), "a": oracleVector(128, 3, 4)}
	top, err := recallTop10(context.Background(), scorer, []string{"b", "a"}, vectors)
	want, scoreErr := collections.CanonicalVectorPartitionCosineScoreV1(oracleVector(128, 1, 0), vectors["a"])
	if err != nil || scoreErr != nil || top[0].ID != "a" || math.Float32bits(top[0].Score) != math.Float32bits(want) {
		t.Fatal("canonical bits/tie contract differs")
	}
}
func TestRecallCanonicalFP32DiffersFromFloat64Cosine(t *testing.T) {
	q := oracleVector(128, 1, 1)
	scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(q)
	if err != nil {
		t.Fatal(err)
	}
	score, err := scorer.ScoreV1(q)
	if err != nil {
		t.Fatal(err)
	}
	if math.Float32bits(score) == math.Float32bits(float32(cosine(q, q))) {
		t.Fatal("fixture no longer distinguishes FP32 serving normalization")
	}
}
func TestRecallArtifactBoundsAndErrorIdentity(t *testing.T) {
	err := errors.New(strings.Repeat("x", 4096))
	summary := recallError(err)
	if len(summary) > 2200 || !strings.Contains(summary, recallSHA([]byte(err.Error()))) {
		t.Fatal("unbounded or unidentified error")
	}
	r := recallReport{Error: strings.Repeat("x", recallOutputCap)}
	var out bytes.Buffer
	if err := recallEmit(context.Background(), &out, "result", &r); err == nil || out.Len() != 0 {
		t.Fatal("emitted oversized artifact")
	}
}
func TestRecallResponseRefusesFallbackScoresAndMalformedNeighbors(t *testing.T) {
	tests := map[string]func(*public.SearchResponseV1){
		"exact-fallback":   func(r *public.SearchResponseV1) { r.Counters.ExactScanPartitions = 1 },
		"no-proof":         func(r *public.SearchResponseV1) { r.Counters.ReadProofs = 0 },
		"short":            func(r *public.SearchResponseV1) { r.Neighbors = r.Neighbors[:9] },
		"duplicate":        func(r *public.SearchResponseV1) { r.Neighbors[1] = r.Neighbors[0] },
		"unknown":          func(r *public.SearchResponseV1) { r.Neighbors[0].ID = "unknown" },
		"nonfinite":        func(r *public.SearchResponseV1) { r.Neighbors[0].Score = float32(math.NaN()) },
		"wrong-score":      func(r *public.SearchResponseV1) { r.Neighbors[0].Score = 0 },
		"wrong-order":      func(r *public.SearchResponseV1) { r.Neighbors[0], r.Neighbors[1] = r.Neighbors[1], r.Neighbors[0] },
		"wrong-generation": func(r *public.SearchResponseV1) { r.Generation.Generation++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			in, r := recallTestQueries(t)
			q := r.Queries[0]
			response := public.SearchResponseV1{Generation: r.Generation, Neighbors: append([]public.NeighborV1(nil), q.Truth...),
				Counters: public.SearchCountersV1{SelectedPartitions: 1, HNSWServedPartitions: 1, ReadProofs: 1}}
			mutate(&response)
			if _, err := recallValidateResponse(&q, response, in.vectors); err == nil {
				t.Fatal("accepted invalid native response")
			}
		})
	}
}
func TestRecallReadOnlySerialFailStopRetainsPartial(t *testing.T) {
	for _, failAt := range []int{0, 3} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			in, r := recallTestQueries(t)
			calls := 0
			client := fakeClient{insert: func(context.Context, public.InsertRequestV1) (public.InsertResponseV1, error) {
				t.Fatal("recall issued mutation")
				return public.InsertResponseV1{}, nil
			},
				search: func(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
					calls++
					if request.TopK != 10 || request.Probes != 1 || request.Limits.MergeEntries < 10 || request.Deadline.IsZero() {
						t.Fatal("invalid recall request")
					}
					if failAt > 0 && calls == failAt {
						return public.SearchResponseV1{}, errors.New("injected read failure")
					}
					return public.SearchResponseV1{Generation: r.Generation, Neighbors: append([]public.NeighborV1(nil), r.Queries[calls-1].Truth...),
						Counters: public.SearchCountersV1{SelectedPartitions: 1, HNSWServedPartitions: 1, ReadProofs: 1}}, nil
				}}
			err := recallRunQueries(context.Background(), client, &in, &r)
			if failAt == 0 {
				if err != nil || calls != 16 || r.Counts.Succeeded != 16 || r.MeanRecallAt10 == nil || *r.MeanRecallAt10 != 1 {
					t.Fatalf("success counts=%+v err=%v", r.Counts, err)
				}
			} else if err == nil || calls != failAt || r.Counts.Succeeded != failAt-1 || r.Counts.Failed != 1 || r.Counts.Unissued != 16-failAt || r.MeanRecallAt10 != nil {
				t.Fatalf("partial counts=%+v err=%v", r.Counts, err)
			}
			var output bytes.Buffer
			if err := recallEmit(context.Background(), &output, "result", &r); err != nil {
				t.Fatal(err)
			}
			if output.Len() > recallOutputCap/2 || !bytes.Contains(output.Bytes(), []byte("result")) {
				t.Fatal("missing/bounded partial artifact")
			}
		})
	}
}
func TestRecallInputBoundsAndStrictJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte("12345"), 0600); err != nil {
		t.Fatal(err)
	}
	b := recallBudget{}
	if _, err := b.read(context.Background(), path, 4); err == nil {
		t.Fatal("accepted oversized file")
	}
	b.bytes = recallInputCap - 4
	if _, err := b.read(context.Background(), path, 8); err == nil {
		t.Fatal("accepted aggregate overflow")
	}
	for _, raw := range []string{"{\"Version\":1,\"Unknown\":true}", "{\"Version\":1} {}"} {
		var p recallProvenance
		if err := recallDecode(context.Background(), []byte(raw), &p); err == nil {
			t.Fatal("accepted unknown/trailing JSON")
		}
	}
	raw := make([]byte, 128*4)
	binary.LittleEndian.PutUint32(raw, math.Float32bits(float32(math.NaN())))
	if _, err := recallFloats(context.Background(), raw, 1, 128, false); err == nil {
		t.Fatal("accepted nonfinite vector")
	}
}
func TestRecallModeRefusesConflictingOrMissingAdmissionBeforeNetwork(t *testing.T) {
	cases := [][]string{
		{"-mode", "quiescent-recall", "-fresh-inserts", "1"},
		{"-mode", "quiescent-recall", "-phase", "post-only"},
		{"-dataset", "/unused"},
	}
	for _, args := range cases {
		var out bytes.Buffer
		if err := runArgs(context.Background(), args, &out); err == nil {
			t.Fatal("accepted invalid mode/admission")
		}
	}
}

func TestRecallDatasetFreezesBytesAndRejectsHashShapeAndTruth(t *testing.T) {
	path := t.TempDir()
	docs := make([]byte, 10000*128*4)
	queries := make([]byte, 16*128*4)
	for row := 0; row < 10000; row++ {
		binary.LittleEndian.PutUint32(docs[(row*128+2)*4:], math.Float32bits(1))
	}
	for row := 0; row < 16; row++ {
		binary.LittleEndian.PutUint32(queries[(row*128+2)*4:], math.Float32bits(1))
	}
	var truth bytes.Buffer
	for row := 0; row < 16; row++ {
		ids := []string{}
		for id := 0; id < 10; id++ {
			ids = append(ids, fmt.Sprintf("doc-%06d", id))
		}
		_ = json.NewEncoder(&truth).Encode(struct {
			QueryID     string   `json:"query_id"`
			DocumentIDs []string `json:"document_ids"`
		}{fmt.Sprintf("query-%06d", row), ids})
	}
	hash := strings.Repeat("a", 64)
	m := recallManifest{Version: 1, Generator: "treedb_vector_partition_embedding_mixture_v1", Docs: 10000, Dimensions: 128, Queries: 16, TopK: 10, Metric: "cosine", Normalized: true,
		DocumentIDPattern: "doc-%06d", DocumentVectorsFile: "documents.f32", QueryVectorsFile: "queries.f32", FloatFormat: "float32_le_row_major", ExactTruthFile: "exact_truth.jsonl",
		ExactTruthQueries: 16, FixtureChecksum: hash, TruthIdentity: hash, TruthArtifactSHA256: hash, TruthSHA256: hash, Files: map[string]recallFile{}}
	for name, raw := range map[string][]byte{"documents.f32": docs, "queries.f32": queries, "exact_truth.jsonl": truth.Bytes()} {
		if err := os.WriteFile(filepath.Join(path, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
		m.Files[name] = recallFile{Bytes: int64(len(raw)), SHA256: recallSHA(raw)}
	}
	writeManifest := func() {
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "manifest.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeManifest()
	var in recallInput
	var r recallReport
	var budget recallBudget
	if err := recallReadDataset(context.Background(), &budget, path, &in, &r); err != nil {
		t.Fatal(err)
	}
	if len(in.vectors) != 10000 || len(in.queries) != 16 || len(in.exported) != 16 {
		t.Fatal("missing frozen inputs")
	}
	in.config.VectorInitialization = &nativewire.FixedPeerTCPVectorInitializationV1{}
	in.config.VectorInitialization.IndexDefinition.EfSearch = 64
	if err := recallPlan(context.Background(), &in, &r); err != nil {
		t.Fatal(err)
	}
	// Later path edits cannot mutate privately frozen vectors or oracle input.
	docs[2*4] = 0
	if err := os.WriteFile(filepath.Join(path, "documents.f32"), docs[:len(docs)-1], 0600); err != nil {
		t.Fatal(err)
	}
	if in.vectors["doc-000000"][2] != 1 {
		t.Fatal("retained vector changed with path")
	}
	var other recallInput
	var otherReport recallReport
	var otherBudget recallBudget
	if err := recallReadDataset(context.Background(), &otherBudget, path, &other, &otherReport); err == nil {
		t.Fatal("accepted changed dataset bytes")
	}
	if err := os.WriteFile(filepath.Join(path, "documents.f32"), make([]byte, len(docs)), 0600); err != nil {
		t.Fatal(err)
	}
	m.Dimensions = 127
	writeManifest()
	otherBudget = recallBudget{}
	if err := recallReadDataset(context.Background(), &otherBudget, path, &other, &otherReport); err == nil {
		t.Fatal("accepted mismatched shape")
	}
	in.exported[0][0] = "doc-000010"
	if err := recallPlan(context.Background(), &in, &r); err == nil {
		t.Fatal("silently replaced original corpus truth")
	}
}

// Err invokes a deterministic control before checking a real cancellation context.
// Tests can cancel at an observed setup boundary without sleeps or production hooks.
type recallSetupControlContext struct {
	context.Context
	check func()
}

func (c recallSetupControlContext) Err() error {
	c.check()
	return c.Context.Err()
}

type recallTestReadFunc func([]byte) (int, error)

func (f recallTestReadFunc) Read(p []byte) (int, error) { return f(p) }

func TestRecallSetupRefusesCanceledOrExpiredParentBeforeInput(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "canceled"
		if expired {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			want := error(context.Canceled)
			if expired {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				want = context.DeadlineExceeded
			} else {
				cancel()
			}
			defer cancel()
			var out bytes.Buffer
			// Missing paths would report an open error if entry did any setup work first.
			err := runRecall(ctx, recallOptions{Config: "/must-not-open", Bootstrap: "/must-not-open", Dataset: "/must-not-open", Provenance: "/must-not-open",
				Phase: "pre", RunID: "cancel-setup", Timeout: time.Minute, RPCTimeout: time.Second}, &out)
			if !errors.Is(err, want) {
				t.Fatalf("setup error %v; want %v", err, want)
			}
			var event struct {
				Event  string
				Report recallReport
			}
			d := json.NewDecoder(&out)
			if err := d.Decode(&event); err != nil {
				t.Fatal(err)
			}
			if event.Event != "result" || event.Report.Verdict != "FAILED" || len(event.Report.Queries) != 0 ||
				len(event.Report.ReadinessBefore) != 0 || len(event.Report.ReadinessAfter) != 0 || event.Report.BinarySHA256 != "" {
				t.Fatalf("canceled setup retained work or accepted: %+v", event.Report)
			}
			var extra any
			if err := d.Decode(&extra); err != io.EOF {
				t.Fatalf("unexpected planned/extra event: %v", err)
			}
		})
	}
}

func TestRecallSetupReaderChecksBeforeAndAfterRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	reader := recallContextReader{ctx: ctx, reader: recallTestReadFunc(func(p []byte) (int, error) {
		calls++
		p[0] = 'x'
		cancel()
		return 1, nil
	})}
	raw, err := io.ReadAll(reader)
	if !errors.Is(err, context.Canceled) || calls != 1 || string(raw) != "x" {
		t.Fatalf("lost in-read cancellation: raw=%q calls=%d err=%v", raw, calls, err)
	}
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("read continued after cancellation: calls=%d err=%v", calls, err)
	}
	var budget recallBudget
	if _, err := budget.read(ctx, "/must-not-open", 8); !errors.Is(err, context.Canceled) || budget.bytes != 0 {
		t.Fatalf("file admission ignored cancellation: %v", err)
	}
	var out bytes.Buffer
	if err := recallEmit(ctx, &out, "planned", &recallReport{}); !errors.Is(err, context.Canceled) || out.Len() != 0 {
		t.Fatalf("planned output ignored cancellation: bytes=%d err=%v", out.Len(), err)
	}
}

func TestRecallOracleSetupCancellationAndDeadline(t *testing.T) {
	in, r := recallTestQueries(t)
	r.Queries = nil
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	controlled := recallSetupControlContext{Context: ctx, check: func() {
		// Stop at the first completed oracle query, before constructing the other 15.
		if len(r.Queries) == 1 {
			cancel()
		}
	}}
	if err := recallPlan(controlled, &in, &r); !errors.Is(err, context.Canceled) {
		t.Fatalf("oracle ignored setup cancellation: %v", err)
	}
	recallFinish(&r)
	if len(r.Queries) != 1 || r.Queries[0].Outcome != "unissued" || r.Counts.Attempted != 0 ||
		r.Counts.Unissued != 1 || r.MeanRecallAt10 != nil {
		t.Fatalf("oracle cancellation lost partial/unissued boundary: %+v", r.Counts)
	}
	// A real elapsed deadline must refuse without scoring any query.
	expired, expireCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expireCancel()
	r.Queries = nil
	if err := recallPlan(expired, &in, &r); !errors.Is(err, context.DeadlineExceeded) || len(r.Queries) != 0 {
		t.Fatalf("oracle ignored setup deadline: queries=%d err=%v", len(r.Queries), err)
	}
	// Stop inside a scoring pass too, not merely at the next query boundary.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	checks := 0
	inside := recallSetupControlContext{Context: ctx2, check: func() {
		checks++
		if checks == 3 {
			cancel2()
		}
	}}
	scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(in.queries[0])
	if err != nil {
		t.Fatal(err)
	}
	if top, err := recallTop10(inside, scorer, in.corpusIDs, in.vectors); !errors.Is(err, context.Canceled) || top != nil || checks != 3 {
		t.Fatalf("scoring continued after cancellation: top=%v checks=%d err=%v", top, checks, err)
	}
}
