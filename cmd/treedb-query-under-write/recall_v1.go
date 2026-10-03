package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

const recallQualifierSourceSHA = "750dde6bc3866287e3201e62be9b88dc6f5e0d7b2a947d16da0e5efba074e769"
const recallInputCap = 32 << 20
const recallOutputCap = 1 << 20

type recallOptions struct {
	Config, Bootstrap, Dataset, Provenance, Probe, Phase, RunID string
	Timeout, RPCTimeout                                         time.Duration
}
type recallFile struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type recallManifest struct {
	Version             int                   `json:"version"`
	Generator           string                `json:"generator"`
	Docs                int                   `json:"docs"`
	Dimensions          int                   `json:"dimensions"`
	Queries             int                   `json:"queries"`
	TopK                int                   `json:"top_k"`
	Metric              string                `json:"metric"`
	Normalized          bool                  `json:"normalized"`
	DocumentIDPattern   string                `json:"document_id_pattern"`
	DocumentVectorsFile string                `json:"document_vectors_file"`
	QueryVectorsFile    string                `json:"query_vectors_file"`
	FloatFormat         string                `json:"float_format"`
	ExactTruthFile      string                `json:"exact_truth_file"`
	ExactTruthQueries   int                   `json:"exact_truth_queries"`
	FixtureChecksum     string                `json:"fixture_checksum"`
	TruthIdentity       string                `json:"truth_identity"`
	TruthArtifactSHA256 string                `json:"truth_artifact_sha256"`
	TruthSHA256         string                `json:"truth_sha256"`
	Files               map[string]recallFile `json:"files"`
}

// This is an operator evidence binding, not a serving or consensus capability.
// Root accepts the referenced complete initialization/reopen chain separately.
type recallProvenance struct {
	Version                                                                             int
	Phase, CampaignID, BootstrapRequestID                                               string
	ConfigSHA256, BootstrapSHA256, ManifestSHA256                                       string
	RuntimeSourceHead, ServerBinarySHA256, QualificationSourceSHA256                    string
	InitializationReceiptSHA256, CleanReopenReceiptSHA256                               string
	ProbeSHA256, ProbeBinarySHA256, ProbeRunID                                          string
	Roots, Hosts                                                                        map[string]string
	RootAccepted, InitializationSucceeded, CleanReopenSucceeded, ExclusiveWriterStopped bool
}
type recallQuery struct {
	scorer                                            *collections.CanonicalVectorPartitionCosineScorerV1 // immutable after setup; safe across read-window workers
	QueryID, RequestSHA256, Outcome, ErrorCode, Error string
	Request                                           public.SearchRequestV1
	StartNS, EndNS                                    int64
	Response                                          *public.SearchResponseV1
	CorpusTruth, Truth                                []public.NeighborV1
	RecallAt10                                        *float64
}
type recallReport struct {
	Version                                                                                    int
	Kind, Verdict, Scope, Phase, RunID, ScoreContract                                          string
	ConfigSHA256, BootstrapSHA256, ManifestSHA256, ProvenanceSHA256, ProbeSHA256, BinarySHA256 string
	PopulationSHA256                                                                           string
	ConfigIdentity                                                                             nativewire.FixedPeerConfigIdentityV1
	Generation                                                                                 public.GenerationIDV1
	Manifest                                                                                   recallManifest
	Provenance                                                                                 recallProvenance
	PopulationRows                                                                             int
	HighestCommitIndex                                                                         uint64
	Timeout, RPCTimeout                                                                        time.Duration
	Queries                                                                                    []recallQuery
	ReadinessBefore, ReadinessAfter                                                            []observation
	Counts                                                                                     counts
	MeanRecallAt10                                                                             *float64
	Error                                                                                      string
}
type recallInput struct {
	config    nativewire.FixedPeerTCPConfigV1
	bootstrap nativewire.FixedPeerVectorQualificationV1
	vectors   map[string][]float32
	queries   [][]float32
	corpusIDs []string
	exported  [][]string
}
type recallBudget struct{ bytes int64 }

func recallError(err error) string {
	s := err.Error()
	if len(s) > 2048 {
		return s[:2048] + " [full_error_sha256=" + recallSHA([]byte(s)) + "]"
	}
	return s
}
func recallSHA(raw []byte) string { s := sha256.Sum256(raw); return hex.EncodeToString(s[:]) }
func recallHex(s string, n int) bool {
	raw, err := hex.DecodeString(s)
	return err == nil && len(s) == n*2 && len(raw) == n && s == hex.EncodeToString(raw)
}

// Read checks are synchronous: no goroutine can remain blocked after cancellation.
// An OS Read already in progress must return before its cancellation is observed.
type recallContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r recallContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if canceled := r.ctx.Err(); canceled != nil {
		return n, canceled
	}
	return n, err
}
func recallDecode(ctx context.Context, raw []byte, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d := json.NewDecoder(recallContextReader{ctx: ctx, reader: bytes.NewReader(raw)})
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	err := d.Decode(&extra)
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if err != io.EOF {
		return errors.New("trailing JSON input")
	}
	return nil
}
func (b *recallBudget) read(ctx context.Context, path string, capBytes int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if capBytes > recallInputCap-b.bytes {
		capBytes = recallInputCap - b.bytes
	}
	if capBytes < 0 {
		return nil, errors.New("recall aggregate input bound exhausted")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(recallContextReader{ctx: ctx, reader: f}, capBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > capBytes {
		return nil, errors.New("recall input exceeds file/aggregate bound")
	}
	b.bytes += int64(len(raw))
	return raw, nil
}
func (b *recallBudget) json(ctx context.Context, path string, value any, capBytes int64) (string, error) {
	raw, err := b.read(ctx, path, capBytes)
	if err != nil {
		return "", err
	}
	if err := recallDecode(ctx, raw, value); err != nil {
		return "", err
	}
	return recallSHA(raw), nil
}
func recallFloats(ctx context.Context, raw []byte, rows, dims int, corpus bool) ([][]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if rows < 1 || dims != 128 || len(raw) != rows*dims*4 {
		return nil, errors.New("FP32 shape/length mismatch")
	}
	result := make([][]float32, rows)
	owned := make([]float32, rows*dims)
	for row := 0; row < rows; row++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result[row] = owned[row*dims : (row+1)*dims]
		var norm, plane float64
		for col := range result[row] {
			v := math.Float32frombits(binary.LittleEndian.Uint32(raw[(row*dims+col)*4:]))
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return nil, errors.New("nonfinite FP32 input")
			}
			result[row][col] = v
			norm += float64(v) * float64(v)
			if col < 2 {
				plane += float64(v) * float64(v)
			}
		}
		if norm == 0 || math.Abs(norm-1) > .001 || (corpus && plane > .81*norm) {
			return nil, errors.New("FP32 normalization/oracle-plane mismatch")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
func recallReadDataset(ctx context.Context, b *recallBudget, path string, in *recallInput, r *recallReport) error {
	var err error
	if r.ManifestSHA256, err = b.json(ctx, filepath.Join(path, "manifest.json"), &r.Manifest, 64<<10); err != nil {
		return err
	}
	m := r.Manifest
	if m.Version != 1 || m.Generator != "treedb_vector_partition_embedding_mixture_v1" || m.Docs != 10000 || m.Dimensions != 128 || m.Queries != 16 || m.TopK != 10 || m.ExactTruthQueries != 16 ||
		m.Metric != "cosine" || !m.Normalized || m.FloatFormat != "float32_le_row_major" || m.DocumentIDPattern != "doc-%06d" ||
		m.DocumentVectorsFile != "documents.f32" || m.QueryVectorsFile != "queries.f32" || m.ExactTruthFile != "exact_truth.jsonl" || len(m.Files) != 3 {
		return errors.New("recall requires unchanged 10000x128/16-query/top10 export")
	}
	for _, s := range []string{m.FixtureChecksum, m.TruthIdentity, m.TruthArtifactSHA256, m.TruthSHA256} {
		if !recallHex(s, 32) {
			return errors.New("invalid dataset provenance hash")
		}
	}
	load := func(name string, size int64) ([]byte, error) {
		f, ok := m.Files[name]
		if !ok || f.Bytes != size || !recallHex(f.SHA256, 32) {
			return nil, errors.New("dataset file identity/length mismatch")
		}
		raw, err := b.read(ctx, filepath.Join(path, name), size)
		if err != nil {
			return nil, err
		}
		if int64(len(raw)) != size || recallSHA(raw) != f.SHA256 {
			return nil, errors.New("dataset bytes/hash mismatch")
		}
		return raw, nil
	}
	raw, err := load("documents.f32", 10000*128*4)
	if err != nil {
		return err
	}
	docs, err := recallFloats(ctx, raw, 10000, 128, true)
	if err != nil {
		return err
	}
	in.vectors = make(map[string][]float32, 10069)
	for i, v := range docs {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := fmt.Sprintf("doc-%06d", i)
		in.corpusIDs = append(in.corpusIDs, id)
		in.vectors[id] = v
	}
	raw, err = load("queries.f32", 16*128*4)
	if err != nil {
		return err
	}
	if in.queries, err = recallFloats(ctx, raw, 16, 128, false); err != nil {
		return err
	}
	f, ok := m.Files["exact_truth.jsonl"]
	if !ok || f.Bytes < 1 || f.Bytes > 64<<10 {
		return errors.New("truth file bound mismatch")
	}
	raw, err = load("exact_truth.jsonl", f.Bytes)
	if err != nil {
		return err
	}
	d := json.NewDecoder(recallContextReader{ctx: ctx, reader: bytes.NewReader(raw)})
	d.DisallowUnknownFields()
	for i := 0; i < 16; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var row struct {
			QueryID     string   `json:"query_id"`
			DocumentIDs []string `json:"document_ids"`
		}
		if err := d.Decode(&row); err != nil {
			return err
		}
		if row.QueryID != fmt.Sprintf("query-%06d", i) || len(row.DocumentIDs) != 10 {
			return errors.New("truth query identity/count mismatch")
		}
		seen := map[string]bool{}
		for _, id := range row.DocumentIDs {
			if seen[id] || in.vectors[id] == nil {
				return errors.New("truth contains duplicate/unknown corpus ID")
			}
			seen[id] = true
		}
		in.exported = append(in.exported, row.DocumentIDs)
	}
	var extra any
	decodeErr := d.Decode(&extra)
	if err := ctx.Err(); err != nil {
		return err
	}
	if decodeErr != io.EOF {
		return errors.New("extra truth query/trailing input")
	}
	return nil
}
func recallValidateProvenance(in *recallInput, r *recallReport) error {
	p := r.Provenance
	if p.Version != 1 || p.Phase != r.Phase || !asciiID(p.CampaignID) || !asciiID(p.BootstrapRequestID) ||
		!p.RootAccepted || !p.InitializationSucceeded || !p.CleanReopenSucceeded || !p.ExclusiveWriterStopped ||
		p.ConfigSHA256 != r.ConfigSHA256 || p.BootstrapSHA256 != r.BootstrapSHA256 || p.ManifestSHA256 != r.ManifestSHA256 ||
		p.QualificationSourceSHA256 != recallQualifierSourceSHA || !recallHex(p.RuntimeSourceHead, 20) {
		return errors.New("missing/mismatched root-accepted quiescent provenance")
	}
	for _, s := range []string{p.ServerBinarySHA256, p.InitializationReceiptSHA256, p.CleanReopenReceiptSHA256} {
		if !recallHex(s, 32) {
			return errors.New("missing runtime/initialization/reopen identity")
		}
	}
	if len(p.Roots) != 4 || len(p.Hosts) != 4 {
		return errors.New("requires all four owned roots/hosts")
	}
	hosts := map[string]int{}
	roots := map[string]bool{}
	for _, node := range in.config.Nodes {
		root, host := p.Roots[string(node.ID)], p.Hosts[string(node.ID)]
		if !filepath.IsAbs(root) || len(root) > 512 || host == "" || len(host) > 255 || roots[root] {
			return errors.New("invalid owned root/host inventory")
		}
		roots[root] = true
		hosts[host]++
	}
	if len(hosts) != 2 {
		return errors.New("requires exactly two physical hosts")
	}
	for _, n := range hosts {
		if n != 2 {
			return errors.New("requires two voters per host")
		}
	}
	if r.Phase == "pre" {
		if p.ProbeSHA256 != "" || p.ProbeBinarySHA256 != "" || p.ProbeRunID != "" {
			return errors.New("pre observation cannot admit prior probe mutations")
		}
	} else if !recallHex(p.ProbeSHA256, 32) || !recallHex(p.ProbeBinarySHA256, 32) || !asciiID(p.ProbeRunID) {
		return errors.New("post-only requires frozen successful probe provenance")
	}
	d := in.bootstrap.Dataset
	if d == nil || d.Rows != 10000 || d.Dimensions != 128 || d.ManifestSHA256 != r.ManifestSHA256 || d.VectorsSHA256 != r.Manifest.Files["documents.f32"].SHA256 || d.InputBytes != uint64(10000*128*4+10000*len("doc-000000")+3*128*4+len("seed-x")+len("seed-minus-x")+len("seed-minus-y")) {
		return errors.New("bootstrap/dataset binding mismatch")
	}
	v := in.config.VectorInitialization
	c := in.bootstrap.Prepare.Command
	if c.Version != 1 || c.Operation != "prepare" || c.MaxSourceRows != v.MaxSourceRows || !recallHex(c.IndexDefinitionDigest, 32) || !recallHex(c.CommandDigest, 32) ||
		c.Collection != v.Collection.Collection || c.Index != v.IndexDefinition.Name || string(c.Group) != string(v.SourceGroupID) ||
		c.Generation != v.Generation || c.SourceRowCount != 10003 || c.SourceGeneration == 0 || c.SourceChecksum == 0 || c.SourceSchemaHash == 0 || c.Term == 0 || c.IndexPosition == 0 {
		return errors.New("bootstrap preparation source/command identity mismatch")
	}
	for _, s := range []string{in.bootstrap.Prepare.AssetSetDigest, in.bootstrap.Prepare.ManifestDigest, in.bootstrap.Prepare.ReadySetDigest} {
		if !recallHex(s, 32) {
			return errors.New("bootstrap preparation digest missing")
		}
	}
	fresh := p.BootstrapRequestID + "/dataset-" + r.ManifestSHA256 + "-fresh-y"
	if in.bootstrap.Insert.VisibleID != fresh {
		return errors.New("qualification request namespace differs from pinned algorithm")
	}
	if in.bootstrap.Insert.CommitIndex <= c.IndexPosition {
		return errors.New("qualification insert precedes prepared source")
	}
	if err := recallReadyStates(in.config, in.bootstrap.Readiness, in.bootstrap.Retry.CommitIndex); err != nil {
		return err
	}
	return nil
}
func recallReadyStates(config nativewire.FixedPeerTCPConfigV1, states []nativewire.FixedPeerReadinessV1, prefix uint64) error {
	if len(states) != len(config.Nodes) {
		return errors.New("incomplete retained all-voter readiness")
	}
	seen := map[string]bool{}
	for _, state := range states {
		id := string(state.NodeID)
		expected := false
		for _, node := range config.Nodes {
			if node.ID == state.NodeID {
				expected = true
			}
		}
		if !expected || seen[id] || !state.Live || !state.Ready || state.Draining || state.VectorPhase != "active" || state.Error != "" ||
			state.CatalogEpoch != config.VectorInitialization.CatalogEpoch || len(state.Groups) != 1 || state.Groups[0].GroupID != config.Groups[0].ID ||
			!state.Groups[0].Ready || state.Groups[0].Error != "" || state.Groups[0].LocalAppliedIndex < prefix {
			return errors.New("retained voter is not active through required prefix")
		}
		seen[id] = true
	}
	return nil
}
func recallLogicalHash(op operation) (string, error) {
	if op.Kind == "insert" && op.InsertRequest != nil && op.SearchRequest == nil {
		request := *op.InsertRequest
		request.Deadline = time.Time{}
		return hashJSON(request), nil
	}
	if op.Kind == "search" && op.SearchRequest != nil && op.InsertRequest == nil {
		request := *op.SearchRequest
		request.Deadline = time.Time{}
		return hashJSON(request), nil
	}
	return "", errors.New("probe operation has invalid request kind")
}
func recallAdmitProbe(ctx context.Context, raw []byte, in *recallInput, r *recallReport) error {
	var events [2]struct {
		Event  string
		Report report
	}
	d := json.NewDecoder(recallContextReader{ctx: ctx, reader: bytes.NewReader(raw)})
	d.DisallowUnknownFields()
	for i := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.Decode(&events[i]); err != nil {
			return err
		}
	}
	var extra any
	decodeErr := d.Decode(&extra)
	if err := ctx.Err(); err != nil {
		return err
	}
	if decodeErr != io.EOF || events[0].Event != "planned" || events[1].Event != "result" {
		return errors.New("probe needs exactly planned/result events")
	}
	planned, result := events[0].Report, events[1].Report
	if result.Verdict != "ACCEPT_BOUNDED_FUNCTIONAL_CLIENT_OVERLAP" || result.Error != "" || result.Version != 1 || planned.Version != 1 ||
		result.ConfigSHA256 != r.ConfigSHA256 || result.BootstrapSHA256 != r.BootstrapSHA256 || result.Generation != r.Generation ||
		result.BinarySHA256 != r.Provenance.ProbeBinarySHA256 || result.RunID != r.Provenance.ProbeRunID ||
		!reflect.DeepEqual(result.ConfigIdentity, r.ConfigIdentity) {
		return errors.New("probe result is incomplete/failed or bound to another campaign")
	}
	if planned.ConfigSHA256 != result.ConfigSHA256 || planned.BootstrapSHA256 != result.BootstrapSHA256 || planned.Generation != result.Generation ||
		planned.BinarySHA256 != result.BinarySHA256 || planned.RunID != result.RunID || !reflect.DeepEqual(planned.ConfigIdentity, result.ConfigIdentity) ||
		planned.Timeout != result.Timeout || planned.RPCTimeout != result.RPCTimeout {
		return errors.New("probe planned/result identity mismatch")
	}
	freshN := (len(result.Operations) - 68) / 2
	if freshN < 1 || freshN > 65 || len(result.Operations) != 68+2*freshN || len(planned.Operations) != len(result.Operations) {
		return errors.New("probe operation population mismatch")
	}
	b := in.bootstrap
	plan, err := makePlan(options{RunID: result.RunID, Generation: r.Generation, BootstrapID: b.Insert.VisibleID, BootstrapRevision: b.Insert.LiveRevision,
		BootstrapCommitIndex: b.Retry.CommitIndex, OwnerGroup: b.Insert.OwnerGroup, Timeout: result.Timeout, RPCTimeout: result.RPCTimeout, Dimensions: 128, FreshInserts: freshN, EfSearch: in.config.VectorInitialization.IndexDefinition.EfSearch})
	if err != nil {
		return err
	}
	lastCommit, revision := b.Retry.CommitIndex, uint64(1)
	for i, expected := range plan {
		if err := ctx.Err(); err != nil {
			return err
		}
		before, op := planned.Operations[i], result.Operations[i]
		if !reflect.DeepEqual(before, expected) {
			return errors.New("probe planned request differs from supported frozen plan")
		}
		h, err := recallLogicalHash(op)
		if err != nil || h != expected.RequestSHA256 || op.RequestSHA256 != h || op.Ordinal != expected.Ordinal || op.Phase != expected.Phase || op.Kind != expected.Kind ||
			op.ExpectedID != expected.ExpectedID || op.Outcome != "succeeded" || op.Error != "" || op.ErrorCode != "" || op.StartNS < 0 || op.EndNS <= op.StartNS {
			return errors.New("probe result request/outcome/interval mismatch")
		}
		if op.Kind == "search" {
			if op.SearchResponse == nil || op.InsertResponse != nil || op.SearchRequest.Deadline.IsZero() {
				return errors.New("probe search evidence missing")
			}
			if err := validateSearch(*op.SearchRequest, *op.SearchResponse, op.ExpectedID); err != nil {
				return err
			}
			continue
		}
		if op.InsertResponse == nil || op.SearchResponse != nil || op.InsertRequest.Deadline.IsZero() {
			return errors.New("probe insert evidence missing")
		}
		response := *op.InsertResponse
		if err := public.ValidateInsertResponseV1(*op.InsertRequest, response); err != nil {
			return err
		}
		if op.Phase != "explicit-retry" {
			revision++
		}
		if response.LiveRevision != revision || response.CommitIndex <= lastCommit || response.OwnerGroup != b.Insert.OwnerGroup ||
			response.PartitionID != b.Insert.PartitionID || len(response.VisibilityToken) != 0 {
			return errors.New("probe mutation sequence/owner/retry identity mismatch")
		}
		lastCommit = response.CommitIndex
		id := string(op.InsertRequest.ID)
		if op.Phase != "explicit-retry" {
			if in.vectors[id] != nil {
				return errors.New("probe replaces an existing ID")
			}
			var doc struct {
				Embedding []float32 `json:"embedding"`
				Kind      string    `json:"kind"`
			}
			decodeErr := recallDecode(ctx, op.InsertRequest.Document, &doc)
			if err := ctx.Err(); err != nil {
				return err
			}
			if decodeErr != nil || doc.Kind != "query-under-write" || !reflect.DeepEqual(doc.Embedding, op.InsertRequest.Vector) {
				return errors.New("probe document/vector mismatch")
			}
			in.vectors[id] = append([]float32(nil), op.InsertRequest.Vector...)
		}
	}
	checked := result
	finish(&checked)
	if checked.Counts != (counts{Planned: len(plan), Attempted: len(plan), Succeeded: len(plan)}) || checked.Counts != result.Counts ||
		checked.HighestCommitIndex != lastCommit || result.HighestCommitIndex != lastCommit || !reflect.DeepEqual(checked.Overlaps, result.Overlaps) {
		return errors.New("probe counts/commit/overlap ledger mismatch")
	}
	overlapProven := false
	for _, v := range checked.Overlaps {
		if v.SearchFinishedBeforeInsert {
			overlapProven = true
		}
	}
	if !overlapProven {
		return errors.New("successful probe lacks retained overlap witness")
	}
	// Retain the final full successful round, allowing earlier bounded not-ready rounds.
	if len(result.Readiness) < 4 {
		return errors.New("probe lacks final readiness")
	}
	states := make([]nativewire.FixedPeerReadinessV1, 4)
	finalRound := result.Readiness[len(result.Readiness)-1].Round
	for i, item := range result.Readiness[len(result.Readiness)-4:] {
		if item.Error != "" || item.ErrorCode != "" || item.Round != finalRound || item.RequestedNode != string(item.State.NodeID) {
			return errors.New("probe final readiness round mismatch")
		}
		states[i] = item.State
	}
	if err := recallReadyStates(in.config, states, lastCommit); err != nil {
		return err
	}
	r.HighestCommitIndex = lastCommit
	return ctx.Err()
}
func recallLess(a, b public.NeighborV1) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	return a.ID < b.ID
}
func recallTop10(ctx context.Context, s *collections.CanonicalVectorPartitionCosineScorerV1, ids []string, vectors map[string][]float32) ([]public.NeighborV1, error) {
	best := make([]public.NeighborV1, 0, 11)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		score, err := s.ScoreV1(vectors[id])
		if err != nil {
			return nil, err
		}
		best = append(best, public.NeighborV1{ID: id, Score: score})
		sort.Slice(best, func(i, j int) bool { return recallLess(best[i], best[j]) })
		if len(best) > 10 {
			best = best[:10]
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return best, nil
}
func recallPlan(ctx context.Context, in *recallInput, r *recallReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(in.queries) != 16 || len(in.exported) != 16 || len(in.corpusIDs) < 10 || in.config.VectorInitialization.IndexDefinition.EfSearch < 10 {
		return errors.New("recall needs sixteen top10 queries and configured EfSearch>=10")
	}
	ids := make([]string, 0, len(in.vectors))
	for id := range in.vectors {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	sort.Strings(ids)
	if err := ctx.Err(); err != nil {
		return err
	}
	population := sha256.New()
	var word [4]byte
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(word[:], uint32(len(id)))
		_, _ = population.Write(word[:])
		_, _ = population.Write([]byte(id))
		for _, v := range in.vectors[id] {
			binary.LittleEndian.PutUint32(word[:], math.Float32bits(v))
			_, _ = population.Write(word[:])
		}
	}
	r.PopulationSHA256 = hex.EncodeToString(population.Sum(nil))
	r.PopulationRows = len(ids)
	for i, q := range in.queries {
		if err := ctx.Err(); err != nil {
			return err
		}
		scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(q)
		if err != nil {
			return err
		}
		corpus, err := recallTop10(ctx, scorer, in.corpusIDs, in.vectors)
		if err != nil {
			return err
		}
		for j, want := range in.exported[i] {
			if corpus[j].ID != want {
				return errors.New("canonical corpus oracle differs from unchanged exported truth")
			}
		}
		truth, err := recallTop10(ctx, scorer, ids, in.vectors)
		if err != nil {
			return err
		}
		request := public.SearchRequestV1{Version: 1, Generation: r.Generation, Query: q, Metric: public.MetricCosineV1, TopK: 10, Probes: 1,
			EfSearch: in.config.VectorInitialization.IndexDefinition.EfSearch, Consistency: public.ConsistencyGenerationSnapshotV1,
			Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 32}}
		r.Queries = append(r.Queries, recallQuery{scorer: scorer, QueryID: fmt.Sprintf("query-%06d", i), RequestSHA256: hashJSON(request), Outcome: "unissued", Request: request, CorpusTruth: corpus, Truth: truth})
	}
	return ctx.Err()
}
func recallValidateResponse(q *recallQuery, response public.SearchResponseV1, vectors map[string][]float32) (float64, error) {
	c := response.Counters
	if response.Generation != q.Request.Generation || len(response.Neighbors) != 10 || c.SelectedPartitions == 0 || c.HNSWServedPartitions != c.SelectedPartitions ||
		c.ExactScanPartitions != 0 || c.ReadProofs == 0 {
		return 0, errors.New("native top10 generation/proof/fallback mismatch")
	}
	scorer := q.scorer
	if scorer == nil {
		var err error
		scorer, err = collections.NewCanonicalVectorPartitionCosineScorerV1(q.Request.Query)
		if err != nil {
			return 0, err
		}
	}
	seen := map[string]bool{}
	truth := map[string]bool{}
	hits := 0
	for _, n := range q.Truth {
		truth[n.ID] = true
	}
	for i, n := range response.Neighbors {
		v := vectors[n.ID]
		if seen[n.ID] || v == nil || math.IsNaN(float64(n.Score)) || math.IsInf(float64(n.Score), 0) {
			return 0, errors.New("duplicate/unknown/nonfinite neighbor")
		}
		seen[n.ID] = true
		score, err := scorer.ScoreV1(v)
		if err != nil || math.Float32bits(score) != math.Float32bits(n.Score) || (i > 0 && !recallLess(response.Neighbors[i-1], n)) {
			return 0, errors.New("neighbor canonical score/order mismatch")
		}
		if truth[n.ID] {
			hits++
		}
	}
	return float64(hits) / 10, nil
}
func recallFinish(r *recallReport) {
	r.Counts = counts{Planned: len(r.Queries)}
	r.MeanRecallAt10 = nil
	var sum float64
	for _, q := range r.Queries {
		if q.Outcome != "unissued" {
			r.Counts.Attempted++
		}
		switch q.Outcome {
		case "succeeded":
			r.Counts.Succeeded++
			if q.RecallAt10 != nil {
				sum += *q.RecallAt10
			}
		case "failed":
			r.Counts.Failed++
		case "canceled":
			r.Counts.Canceled++
		case "unknown":
			r.Counts.Unknown++
		default:
			r.Counts.Unissued++
		}
	}
	if r.Counts.Succeeded == 16 && r.Counts.Planned == 16 {
		mean := sum / 16
		r.MeanRecallAt10 = &mean
	}
}
func recallRunQueries(ctx context.Context, client vectorClient, in *recallInput, r *recallReport) error {
	origin := time.Now()
	defer recallFinish(r)
	for i := range r.Queries {
		if err := ctx.Err(); err != nil {
			return err
		}
		q := &r.Queries[i]
		call, cancel := context.WithTimeout(ctx, r.RPCTimeout)
		q.Request.Deadline, _ = call.Deadline()
		q.StartNS = time.Since(origin).Nanoseconds()
		response, err := client.VectorSearchStrictV1(call, q.Request)
		q.EndNS = time.Since(origin).Nanoseconds()
		cancel()
		q.Outcome = "succeeded"
		// The client response may contain untrusted/oversized neighbors. Bound retained
		// partial evidence before assigning it to the final artifact.
		raw, encodeErr := json.Marshal(response)
		if encodeErr != nil || len(raw) > 32<<10 {
			err = errors.Join(err, errors.New("search response exceeds retained evidence bound"))
		} else {
			q.Response = &response
		}
		if err == nil {
			value, e := recallValidateResponse(q, response, in.vectors)
			err = e
			if err == nil {
				q.RecallAt10 = &value
			}
		}
		if err != nil {
			q.Outcome, q.ErrorCode = errorOutcome(err, false)
			q.Error = recallError(err)
			return err
		}
	}
	return nil
}
func recallEmit(ctx context.Context, output io.Writer, event string, r *recallReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(struct {
		Event  string
		Report *recallReport
	}{event, r})
	if err != nil {
		return err
	}
	if len(raw) > recallOutputCap/2 {
		return errors.New("recall event exceeds retained byte bound")
	}
	raw = append(raw, '\n')
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err = output.Write(raw)
	return errors.Join(err, ctx.Err())
}
func runRecall(parent context.Context, o recallOptions, output io.Writer) (runErr error) {
	ctx, cancel := context.WithTimeout(parent, o.Timeout)
	defer cancel()
	r := recallReport{Version: 1, Kind: "fixed_cluster_quiescent_recall_v1", Verdict: "FAILED", Phase: o.Phase, RunID: o.RunID,
		Scope:         "operationally quiescent RF4 one-group native recall; no query watermark, paired comparison, concurrent recall or sustained capacity claim",
		ScoreContract: collections.VectorPartitionCanonicalScoreContractV1, Timeout: o.Timeout, RPCTimeout: o.RPCTimeout}
	defer func() {
		if runErr == nil {
			runErr = ctx.Err()
		}
		if runErr != nil {
			r.Verdict = "FAILED"
			r.Error = recallError(runErr)
		}
		recallFinish(&r)
		// Preserve failure evidence after cancellation; root bounds blocking output externally.
		runErr = errors.Join(runErr, recallEmit(context.WithoutCancel(ctx), output, "result", &r))
	}()
	var in recallInput
	if err := recallPrepare(ctx, o, &in, &r); err != nil {
		return err
	}
	if err := recallEmit(ctx, output, "planned", &r); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	control, err := nativewire.NewFixedPeerTCPClientV1(in.config)
	if err != nil {
		return err
	}
	defer control.Close()
	observe := func(target *[]observation) error {
		receipt := report{RPCTimeout: o.RPCTimeout}
		err := readiness(ctx, control, in.config, &receipt, r.HighestCommitIndex, 1)
		*target = receipt.Readiness
		if err != nil {
			return err
		}
		states := make([]nativewire.FixedPeerReadinessV1, len(receipt.Readiness))
		for i, v := range receipt.Readiness {
			states[i] = v.State
		}
		return recallReadyStates(in.config, states, r.HighestCommitIndex)
	}
	if err := observe(&r.ReadinessBefore); err != nil {
		return err
	}
	call, dialCancel := context.WithTimeout(ctx, o.RPCTimeout)
	client, err := nativewire.DialContext(call, "tcp", in.config.VectorInitialization.PublicAddresses[in.config.NodeID])
	dialCancel()
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, client.Close()) }()
	if err := recallRunQueries(ctx, client, &in, &r); err != nil {
		return err
	}
	if err := observe(&r.ReadinessAfter); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.Verdict = "ACCEPT_QUIESCENT_RECALL_OBSERVATION"
	return nil
}

// recallPrepare freezes and validates the accepted inputs and oracle before networking.
func recallPrepare(ctx context.Context, o recallOptions, in *recallInput, r *recallReport) error {
	if err := validateProbeTimeouts(o.Timeout, o.RPCTimeout); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.Config == "" || o.Bootstrap == "" || o.Dataset == "" || o.Provenance == "" || !asciiID(o.RunID) ||
		(o.Phase != "pre" && o.Phase != "post-only") || (o.Phase == "pre" && o.Probe != "") || (o.Phase == "post-only" && o.Probe == "") {
		return errors.New("recall requires config/bootstrap/dataset/provenance/run-id and pre or post-only with matching probe input")
	}
	var budget recallBudget
	var err error
	if r.ConfigSHA256, err = budget.json(ctx, o.Config, &in.config, 8<<20); err != nil {
		return err
	}
	if r.BootstrapSHA256, err = budget.json(ctx, o.Bootstrap, &in.bootstrap, 4<<20); err != nil {
		return err
	}
	if r.ConfigIdentity, err = nativewire.InspectFixedPeerTCPConfigV1(in.config); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Generation, err = admitBootstrap(in.config, in.bootstrap); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err = recallReadDataset(ctx, &budget, o.Dataset, in, r); err != nil {
		return err
	}
	if r.ProvenanceSHA256, err = budget.json(ctx, o.Provenance, &r.Provenance, 64<<10); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err = recallValidateProvenance(in, r); err != nil {
		return err
	}
	for id, v := range map[string][]float32{"seed-x": oracleVector(128, 1, 0), "seed-minus-x": oracleVector(128, -1, 0), "seed-minus-y": oracleVector(128, 0, -1), in.bootstrap.Insert.VisibleID: oracleVector(128, 0, 1)} {
		in.vectors[id] = v
	}
	r.HighestCommitIndex = in.bootstrap.Retry.CommitIndex
	if o.Phase == "post-only" {
		raw, err := budget.read(ctx, o.Probe, 16<<20)
		if err != nil {
			return err
		}
		r.ProbeSHA256 = recallSHA(raw)
		if r.ProbeSHA256 != r.Provenance.ProbeSHA256 {
			return errors.New("probe file/provenance hash mismatch")
		}
		if err := recallAdmitProbe(ctx, raw, in, r); err != nil {
			return err
		}
	}
	if err := recallPlan(ctx, in, r); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	f, err := os.Open(executable)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, err = io.Copy(h, recallContextReader{ctx: ctx, reader: f})
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	r.BinarySHA256 = hex.EncodeToString(h.Sum(nil))
	recallFinish(r)
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
