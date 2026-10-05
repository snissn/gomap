// treedb-query-under-write is a bounded client correctness checkpoint, not a capacity benchmark.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

const freshCount = 32
const concurrentSearchCount = 64

type vectorClient interface {
	VectorSearchStrictV1(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error)
	VectorInsertV1(context.Context, public.InsertRequestV1) (public.InsertResponseV1, error)
}

// phaseClients prepares fresh connections before the serial phases. Preparation
// never retries an operation and remains outside its dispatch/timing record.
type phaseClients struct {
	writer, reader func(context.Context) (vectorClient, error)
}
type ownedVectorClient interface {
	vectorClient
	Close() error
}

// renewPhaseClient retires an idle client before dialing, never after a failed
// mutation. Clearing ownership prevents deferred cleanup from closing it twice.
func renewPhaseClient(ctx context.Context, current *ownedVectorClient, dial func(context.Context) (ownedVectorClient, error)) (vectorClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	previous := *current
	*current = nil
	if previous != nil {
		if err := previous.Close(); err != nil {
			return nil, err
		}
	}
	next, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	*current = next
	return next, nil
}

type operation struct {
	Ordinal                                int
	Phase, Kind, ExpectedID, RequestSHA256 string
	SearchRequest                          *public.SearchRequestV1
	InsertRequest                          *public.InsertRequestV1
	Outcome, ErrorCode, Error              string
	StartNS, EndNS                         int64
	SearchResponse                         *public.SearchResponseV1
	InsertResponse                         *public.InsertResponseV1
}
type overlap struct {
	SearchOrdinal, InsertOrdinal int
	SearchFinishedBeforeInsert   bool
}
type counts struct{ Planned, Attempted, Succeeded, Failed, Canceled, Unknown, Unissued int }
type observation struct {
	Round                    int
	RequestedNode, ErrorCode string
	State                    nativewire.FixedPeerReadinessV1
	Error                    string
}
type report struct {
	Version                                                                          int
	Verdict, Scope, OverlapBasis, ConfigSHA256, BootstrapSHA256, BinarySHA256, RunID string
	ConfigIdentity                                                                   nativewire.FixedPeerConfigIdentityV1
	Generation                                                                       public.GenerationIDV1
	RPCTimeout, Timeout                                                              time.Duration
	Operations                                                                       []operation
	Overlaps                                                                         []overlap
	Counts                                                                           counts
	Readiness                                                                        []observation
	HighestCommitIndex                                                               uint64
	Error                                                                            string
}
type options struct {
	RunID                                   string
	Generation                              public.GenerationIDV1
	BootstrapID                             string
	BootstrapRevision, BootstrapCommitIndex uint64
	OwnerGroup                              string
	Timeout, RPCTimeout                     time.Duration
	Dimensions, FreshInserts, EfSearch      int
}

func hashJSON(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func asciiID(id string) bool {
	return len(id) > 0 && len(id) <= 64 && strings.Trim(id, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") == ""
}
func effectiveFreshCount(o options) int {
	if o.FreshInserts == 0 {
		return freshCount
	}
	return o.FreshInserts
}
func oracleVector(dimensions int, x, y float32) []float32 {
	v := make([]float32, dimensions)
	v[0], v[1] = x, y
	return v
}
func validateProbeTimeouts(timeout, rpcTimeout time.Duration) error {
	if timeout < time.Second || timeout > 10*time.Minute || rpcTimeout < time.Millisecond || rpcTimeout > time.Minute || rpcTimeout > timeout {
		return errors.New("timeout requires 1s..10m; RPC timeout requires 1ms..1m within overall timeout")
	}
	return nil
}
func makePlan(o options) ([]operation, error) {
	if !asciiID(o.RunID) || o.BootstrapID == "" || o.Generation.Index == "" || o.Generation.Generation == 0 || o.BootstrapRevision != 1 || o.BootstrapCommitIndex == 0 || o.OwnerGroup == "" {
		return nil, errors.New("requires a fresh, successfully qualified generation, live revision 1, and bounded ASCII run ID")
	}
	if err := validateProbeTimeouts(o.Timeout, o.RPCTimeout); err != nil {
		return nil, err
	}
	dimensions := o.Dimensions
	if dimensions == 0 {
		dimensions = 2
	}
	freshN := effectiveFreshCount(o)
	efSearch := o.EfSearch
	if efSearch == 0 {
		efSearch = 8
	}
	if efSearch < 1 || efSearch > 4096 {
		return nil, errors.New("probe efSearch outside1..4096")
	}
	if dimensions < 2 || dimensions > 4096 || freshN < 1 || freshN > 65 {
		return nil, errors.New("probe requires dimensions2..4096 and fresh inserts1..65")
	}
	var plan []operation
	search := func(phase string, vector []float32, id string) {
		request := public.SearchRequestV1{Version: 1, Generation: o.Generation, Query: vector, Metric: public.MetricCosineV1,
			TopK: 1, Probes: 1, EfSearch: efSearch, Consistency: public.ConsistencyGenerationSnapshotV1,
			Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 8}}
		plan = append(plan, operation{Ordinal: len(plan), Phase: phase, Kind: "search", ExpectedID: id, SearchRequest: &request, RequestSHA256: hashJSON(request), Outcome: "unissued"})
	}
	search("preflight-writer", oracleVector(dimensions, 1, 0), "seed-x")
	search("preflight-reader", oracleVector(dimensions, 1, 0), "seed-x")
	vectors := make([][]float32, freshN)
	ids := make([]string, freshN)
	for i := 0; i < freshN; i++ {
		angle := math.Pi/2 + float64(i+1)*math.Pi/(2*float64(freshN+1))
		vectors[i] = oracleVector(dimensions, float32(math.Cos(angle)), float32(math.Sin(angle)))
		ids[i] = fmt.Sprintf("%s-doc-%02d", o.RunID, i)
		if ids[i] == o.BootstrapID || ids[i] == "seed-x" || ids[i] == "seed-minus-x" || ids[i] == "seed-minus-y" {
			return nil, errors.New("run ID collides with bootstrap corpus")
		}
		document, err := json.Marshal(map[string]any{"embedding": vectors[i], "kind": "query-under-write"})
		if err != nil {
			return nil, err
		}
		request := public.InsertRequestV1{Version: 1, Generation: o.Generation, IdempotencyKey: []byte(fmt.Sprintf("%s/insert/%02d", o.RunID, i)), ID: []byte(ids[i]), Vector: vectors[i], Document: document}
		plan = append(plan, operation{Ordinal: len(plan), Phase: "concurrent", Kind: "insert", ExpectedID: ids[i], InsertRequest: &request, RequestSHA256: hashJSON(request), Outcome: "unissued"})
	}
	for i := 0; i < concurrentSearchCount; i++ {
		search("concurrent", oracleVector(dimensions, 1, 0), "seed-x")
	}
	retry := *plan[2+freshN-1].InsertRequest
	plan = append(plan, operation{Ordinal: len(plan), Phase: "explicit-retry", Kind: "insert", ExpectedID: string(retry.ID), InsertRequest: &retry, RequestSHA256: hashJSON(retry), Outcome: "unissued"})
	// Certify unique cosine winners separately from exact-ID mutation visibility.
	corpus := [][]float32{oracleVector(dimensions, 1, 0), oracleVector(dimensions, -1, 0), oracleVector(dimensions, 0, -1), oracleVector(dimensions, 0, 1)}
	corpus = append(corpus, vectors...)
	for i, vector := range vectors {
		self := cosine(vector, vector)
		for j, candidate := range corpus {
			if j == i+4 {
				continue
			}
			if self-cosine(vector, candidate) < 0.0001 {
				return nil, errors.New("self-query oracle lacks a unique winner margin")
			}
		}
		search("post-self", vector, ids[i])
	}
	search("final-anchor", oracleVector(dimensions, 1, 0), "seed-x")
	return plan, nil
}
func cosine(a, b []float32) float64 {
	var dot, aa, bb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		aa += float64(a[i]) * float64(a[i])
		bb += float64(b[i]) * float64(b[i])
	}
	return dot / math.Sqrt(aa*bb)
}
func validateSearch(request public.SearchRequestV1, response public.SearchResponseV1, id string) error {
	c := response.Counters
	if response.Generation != request.Generation || len(response.Neighbors) != 1 || response.Neighbors[0].ID != id ||
		math.IsNaN(float64(response.Neighbors[0].Score)) || math.Abs(float64(response.Neighbors[0].Score)-1) > 0.00001 ||
		c.SelectedPartitions == 0 || c.HNSWServedPartitions != c.SelectedPartitions || c.ExactScanPartitions != 0 || c.ReadProofs == 0 {
		return errors.New("strict native known-corpus cosine oracle/proof mismatch")
	}
	return nil
}
func errorOutcome(err error, mutation bool) (string, string) {
	var typed *public.ErrorV1
	if errors.As(err, &typed) {
		if typed.Code == public.ErrorCommitAmbiguousV1 {
			return "unknown", string(typed.Code)
		}
		// The client maps both local request rejection and malformed insert
		// responses to invalid_request; the latter cannot prove no commit.
		if mutation && typed.Code == public.ErrorInvalidRequestV1 {
			return "unknown", string(typed.Code)
		}
		if typed.Code == public.ErrorCanceledV1 {
			return "canceled", string(typed.Code)
		}
		return "failed", string(typed.Code)
	}
	// Unclassified mutation failures cannot prove that nothing committed.
	if mutation {
		return "unknown", "unclassified_mutation_error"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled", "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "failed", "deadline_exceeded"
	}
	return "failed", "unclassified_error"
}
func execute(ctx context.Context, client vectorClient, op *operation, origin time.Time, rpcTimeout time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	call, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	deadline, _ := call.Deadline()
	op.StartNS = time.Since(origin).Nanoseconds()
	var err error
	if op.Kind == "insert" {
		request := *op.InsertRequest
		request.Deadline = deadline
		op.InsertRequest = &request
		response, e := client.VectorInsertV1(call, request)
		op.InsertResponse, err = &response, e
		if err == nil {
			if e := public.ValidateInsertResponseV1(request, response); e != nil {
				err = &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: e}
			}
		}
	} else {
		request := *op.SearchRequest
		request.Deadline = deadline
		op.SearchRequest = &request
		response, e := client.VectorSearchStrictV1(call, request)
		op.SearchResponse, err = &response, e
		if err == nil {
			err = validateSearch(request, response, op.ExpectedID)
		}
	}
	op.EndNS = time.Since(origin).Nanoseconds()
	op.Outcome = "succeeded"
	if err != nil {
		op.Outcome, op.ErrorCode = errorOutcome(err, op.Kind == "insert")
		op.Error = err.Error()
	}
	return err
}
func finish(r *report) {
	r.Counts = counts{Planned: len(r.Operations)}
	for _, op := range r.Operations {
		if op.Outcome != "unissued" {
			r.Counts.Attempted++
		}
		switch op.Outcome {
		case "succeeded":
			r.Counts.Succeeded++
		case "failed":
			r.Counts.Failed++
		case "canceled":
			r.Counts.Canceled++
		case "unknown":
			r.Counts.Unknown++
		case "unissued":
			r.Counts.Unissued++
		}
		if op.Outcome == "succeeded" && op.InsertResponse != nil && op.InsertResponse.CommitIndex > r.HighestCommitIndex {
			r.HighestCommitIndex = op.InsertResponse.CommitIndex
		}
	}
	r.Overlaps = nil
	for _, search := range r.Operations {
		if search.Phase != "concurrent" || search.Kind != "search" || search.Outcome != "succeeded" {
			continue
		}
		for _, insert := range r.Operations {
			if insert.Phase != "concurrent" || insert.Kind != "insert" || insert.Outcome == "unissued" {
				continue
			}
			if search.StartNS < insert.EndNS && insert.StartNS < search.EndNS {
				r.Overlaps = append(r.Overlaps, overlap{search.Ordinal, insert.Ordinal, search.EndNS < insert.EndNS})
			}
		}
	}
}
func runWorkload(parent context.Context, writer, reader vectorClient, o options, r *report, phases phaseClients) error {
	freshN := effectiveFreshCount(o)
	plan, err := makePlan(o)
	if err != nil {
		return err
	}
	r.Operations = plan
	r.Verdict = "FAILED"
	r.OverlapBasis = "independent_client_API_call_intervals_one_monotonic_clock; socket_dispatch_and_server_critical_section_overlap_unproved"
	origin := time.Now()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer finish(r)
	fail := func(err error) error { cancel(); r.Error = err.Error(); return err }
	if err := execute(ctx, writer, &r.Operations[0], origin, o.RPCTimeout); err != nil {
		return fail(err)
	}
	if err := execute(ctx, reader, &r.Operations[1], origin, o.RPCTimeout); err != nil {
		return fail(err)
	}
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	results := make(chan error, 2)
	worker := func(client vectorClient, first, last int) {
		ready.Done()
		<-start
		for i := first; i < last; i++ {
			if err := execute(ctx, client, &r.Operations[i], origin, o.RPCTimeout); err != nil {
				cancel()
				results <- err
				return
			}
			if r.Operations[i].Kind == "insert" {
				response := r.Operations[i].InsertResponse
				previousCommit := o.BootstrapCommitIndex
				if i > first {
					previousCommit = r.Operations[i-1].InsertResponse.CommitIndex
				}
				if response.LiveRevision != o.BootstrapRevision+uint64(i-1) || response.CommitIndex <= previousCommit ||
					response.OwnerGroup != o.OwnerGroup || len(response.VisibilityToken) != 0 {
					op := &r.Operations[i]
					op.Outcome, op.ErrorCode, op.Error = "unknown", string(public.ErrorCommitAmbiguousV1), "fresh insert receipt violates frozen single-owner revision/commit sequence"
					cancel()
					results <- errors.New(op.Error)
					return
				}
			}
		}
		results <- nil
	}
	go worker(writer, 2, 2+freshN)
	go worker(reader, 2+freshN, 2+freshN+concurrentSearchCount)
	ready.Wait()
	close(start)
	a, b := <-results, <-results
	if err := errors.Join(a, b); err != nil {
		return fail(err)
	}
	prepare := func(connect func(context.Context) (vectorClient, error), current vectorClient) (vectorClient, error) {
		if connect == nil {
			return current, nil
		}
		call, cancel := context.WithTimeout(ctx, o.RPCTimeout)
		defer cancel()
		if err := call.Err(); err != nil {
			return nil, err
		}
		return connect(call)
	}
	writer, err = prepare(phases.writer, writer)
	if err != nil {
		return fail(fmt.Errorf("prepare explicit retry client: %w", err))
	}
	retryIndex := 2 + freshN + concurrentSearchCount
	if err := execute(ctx, writer, &r.Operations[retryIndex], origin, o.RPCTimeout); err != nil {
		return fail(err)
	}
	original, retry := r.Operations[2+freshN-1].InsertResponse, r.Operations[retryIndex].InsertResponse
	if retry.CommitIndex <= original.CommitIndex || retry.LiveRevision != original.LiveRevision || retry.VisibleID != original.VisibleID ||
		retry.OwnerGroup != original.OwnerGroup || retry.PartitionID != original.PartitionID || len(retry.VisibilityToken) != 0 {
		r.Operations[retryIndex].Outcome = "unknown"
		r.Operations[retryIndex].ErrorCode = string(public.ErrorCommitAmbiguousV1)
		r.Operations[retryIndex].Error = "explicit retry did not preserve original exact-ID live identity with new consensus evidence"
		return fail(errors.New(r.Operations[retryIndex].Error))
	}
	reader, err = prepare(phases.reader, reader)
	if err != nil {
		return fail(fmt.Errorf("prepare post-write search client: %w", err))
	}
	for i := retryIndex + 1; i < len(r.Operations); i++ {
		if err := execute(ctx, reader, &r.Operations[i], origin, o.RPCTimeout); err != nil {
			return fail(err)
		}
	}
	finish(r)
	completedDuringOutstandingWrite := false
	for _, pair := range r.Overlaps {
		if pair.SearchFinishedBeforeInsert {
			completedDuringOutstandingWrite = true
		}
	}
	if !completedDuringOutstandingWrite {
		r.Verdict = "INCONCLUSIVE_OVERLAP"
		return fail(errors.New("no successful concurrent strict search completed while an insert API call remained outstanding"))
	}
	r.Verdict = "WORKLOAD_PROVED_PENDING_ALL_VOTER_READINESS"
	return nil
}
func readJSON(path string, value any, bound int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, bound+1))
	if err != nil {
		return "", err
	}
	if int64(len(raw)) > bound {
		return "", errors.New("JSON input exceeds bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return "", err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return "", errors.New("trailing JSON input")
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func readiness(ctx context.Context, client *nativewire.FixedPeerTCPClientV1, config nativewire.FixedPeerTCPConfigV1, r *report, prefix uint64, rounds int) error {
	return readinessWith(ctx, func(call context.Context, node nativewire.FixedPeerTCPNodeV1) (nativewire.FixedPeerReadinessV1, error) {
		return client.ReadinessV1(call, node.ID)
	}, config, r, prefix, rounds)
}

func readinessWith(ctx context.Context, read func(context.Context, nativewire.FixedPeerTCPNodeV1) (nativewire.FixedPeerReadinessV1, error), config nativewire.FixedPeerTCPConfigV1, r *report, prefix uint64, rounds int) error {
	for round := 0; round < rounds; round++ {
		ready := true
		for _, node := range config.Nodes {
			call, cancel := context.WithTimeout(ctx, r.RPCTimeout)
			state, err := read(call, node)
			cancel()
			item := observation{Round: round, RequestedNode: string(node.ID), State: state}
			if err != nil {
				item.Error = err.Error()
				_, item.ErrorCode = errorOutcome(err, false)
			}
			r.Readiness = append(r.Readiness, item)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				ready = false
				continue
			}
			if state.NodeID != node.ID || !state.Live || !state.Ready || state.Draining || state.VectorPhase != "active" ||
				len(state.Groups) != 1 || state.Groups[0].GroupID != config.Groups[0].ID || !state.Groups[0].Ready ||
				state.Groups[0].LocalAppliedIndex < prefix {
				ready = false
			}
		}
		if ready {
			return nil
		}
		if round+1 < rounds {
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return errors.New("all-four active readiness/applied-prefix observation bound exhausted")
}

// admitBootstrap is the shared pre-network admission seam. A retained receipt
// cannot authorize writes against a different configured owner.
func admitBootstrap(config nativewire.FixedPeerTCPConfigV1, bootstrap nativewire.FixedPeerVectorQualificationV1) (public.GenerationIDV1, error) {
	v := config.VectorInitialization
	if config.Credentials == nil || config.Vector != nil || v == nil || len(config.Nodes) != 4 || len(config.Groups) != 1 ||
		v.MaxSourceRows > 16384 || v.IndexDefinition.Dimensions < 2 || v.IndexDefinition.Dimensions > 4096 || v.IndexDefinition.Field != "embedding" {
		return public.GenerationIDV1{}, errors.New("runner supports only authenticated four-voter one-group bounded initialized corpus")
	}
	if bootstrap.Dataset == nil {
		if v.IndexDefinition.Dimensions != 2 {
			return public.GenerationIDV1{}, errors.New("non-2D bootstrap requires eligible frozen dataset receipt")
		}
	} else {
		d := bootstrap.Dataset
		if d.Rows < 1 || d.Dimensions != v.IndexDefinition.Dimensions || d.OraclePlaneMaxFraction != 0.9 || d.SourceRows != uint64(d.Rows)+3 || d.SourceRows != bootstrap.Prepare.Command.SourceRowCount || d.SourceRows > v.MaxSourceRows || d.InputBytes > 32<<20 || len(d.ManifestSHA256) != 64 || len(d.VectorsSHA256) != 64 {
			return public.GenerationIDV1{}, errors.New("dataset bootstrap identity/count/oracle eligibility mismatch")
		}
	}
	generation := public.GenerationIDV1{Index: v.IndexDefinition.Name, Generation: v.Generation}
	if bootstrap.Insert.OwnerGroup != string(config.Groups[0].ID) || bootstrap.Insert.OwnerGroup != string(v.SourceGroupID) {
		return public.GenerationIDV1{}, errors.New("bootstrap owner differs from configured data/source group")
	}
	// The input is an externally retained receipt, not a fresh route capability.
	baselineRequest := public.InsertRequestV1{Generation: generation, ID: []byte(bootstrap.Insert.VisibleID)}
	if err := public.ValidateInsertResponseV1(baselineRequest, bootstrap.Insert); err != nil {
		return public.GenerationIDV1{}, fmt.Errorf("bootstrap insert: %w", err)
	}
	if err := public.ValidateInsertResponseV1(baselineRequest, bootstrap.Retry); err != nil {
		return public.GenerationIDV1{}, fmt.Errorf("bootstrap retry: %w", err)
	}
	if bootstrap.Insert.LiveRevision != 1 || len(bootstrap.Insert.VisibilityToken) != 0 || len(bootstrap.Retry.VisibilityToken) != 0 ||
		bootstrap.Insert.VisibleID == "" || bootstrap.Retry.LiveRevision != bootstrap.Insert.LiveRevision ||
		bootstrap.Retry.OwnerGroup != bootstrap.Insert.OwnerGroup || bootstrap.Retry.PartitionID != bootstrap.Insert.PartitionID ||
		bootstrap.Retry.CommitIndex <= bootstrap.Insert.CommitIndex || len(bootstrap.Readiness) != 4 {
		return public.GenerationIDV1{}, errors.New("bootstrap receipt lacks original and identical-retry single-owner proof")
	}
	oracleRequest := public.SearchRequestV1{Generation: generation}
	if err := validateSearch(oracleRequest, bootstrap.Before, "seed-x"); err != nil {
		return public.GenerationIDV1{}, fmt.Errorf("bootstrap before: %w", err)
	}
	if err := validateSearch(oracleRequest, bootstrap.After, bootstrap.Insert.VisibleID); err != nil {
		return public.GenerationIDV1{}, fmt.Errorf("bootstrap after: %w", err)
	}
	return generation, nil
}
func runArgs(parent context.Context, args []string, output io.Writer) (runErr error) {
	flags := flag.NewFlagSet("treedb-query-under-write", flag.ContinueOnError)
	mode := flags.String("mode", "query-under-write", "query-under-write, quiescent-recall, read-window, paced-window, or mixed-window")
	dataset := flags.String("dataset", "", "unchanged representative exported dataset directory")
	phase := flags.String("phase", "", "recall phase: pre or post-only")
	provenance := flags.String("provenance", "", "root-accepted quiescence/runtime/input provenance JSON")
	probe := flags.String("probe-receipt", "", "complete successful planned/result probe JSONL for post-only")
	configPath := flags.String("config", "", "authenticated RF4 initialization config for this driver host")
	bootstrapPath := flags.String("bootstrap-receipt", "", "retained successful sequential qualify JSON")
	freshInserts := flags.Int("fresh-inserts", freshCount, "bounded unique ordinary insert population1..65; default preserves32")
	runID := flags.String("run-id", "", "unique 1..64 ASCII identity; never reuse after any mutation attempt")
	timeout := flags.Duration("timeout", 120*time.Second, "whole checkpoint deadline, 1s..10m; default 120s")
	rpcTimeout := flags.Duration("rpc-timeout", 10*time.Second, "per-operation deadline, 1ms..1m; default 10s")
	readConcurrency := flags.Int("read-concurrency", 0, "read-window requires explicit independent workers: 1 or 4")
	readWindow := flags.Duration("read-window", 60*time.Second, "measured admission interval, 1s..60s; mixed-window up to300s")
	readWarmup := flags.Int("read-warmup", 64, "read-window warmup attempts outside measurement, 0..1024")
	readMaxAttempts := flags.Int("read-max-attempts", 65536, "read-window measured attempt cap, 1..65536; hitting cap refuses verdict")
	readOutputBytes := flags.Int("read-output-bytes", 128<<20, "read-window aggregate planned/result byte cap, 1MiB..256MiB")
	readResourceGateDir := flags.String("read-resource-gate-dir", "", "optional fresh trusted run-local directory for ready/done resource sampling acknowledgments")
	pacedInserts := flags.Int("paced-inserts", 6, "paced-window distinct ordinary insert slots, 1..10")
	mixedProfile := flags.String("mixed-profile", "", "optional changing-top10 profile; omitted preserves invariant top10")
	mixedOriginals := flags.Int("mixed-originals", 6, "mixed-window total serial originals, 6..63; default6")
	mixedInterval := flags.Duration("mixed-interval", 5*time.Second, "mixed-window serial writer interval1s..8s; (originals-1)*interval+2*rpc-timeout must fit the window")
	pacedInterval := flags.Duration("paced-interval", 5*time.Second, "paced-window minimum interval between serial insert invocation starts")
	if err := flags.Parse(args); err != nil {
		return err
	}
	readFlags, freshFlag, pacedFlags, mixedFlags := false, false, false, false
	flags.Visit(func(f *flag.Flag) {
		mixedFlags = mixedFlags || strings.HasPrefix(f.Name, "mixed-")
		pacedFlags = pacedFlags || strings.HasPrefix(f.Name, "paced-")
		readFlags = readFlags || strings.HasPrefix(f.Name, "read-")
		freshFlag = freshFlag || f.Name == "fresh-inserts"
	})
	if *mode == "mixed-window" {
		if *mixedOriginals < 6 || *mixedOriginals > 63 {
			return errors.New("mixed-originals must be6..63")
		}
		if freshFlag || pacedFlags || flags.NArg() != 0 {
			return errors.New("mixed-window refuses insert/pace/positional flags")
		}
		return runMixedWindow(parent, mixedOptions{Profile: *mixedProfile, Originals: *mixedOriginals, Window: windowOptions{Admission: recallOptions{Config: *configPath, Bootstrap: *bootstrapPath, Dataset: *dataset, Provenance: *provenance, Probe: *probe, Phase: *phase, RunID: *runID, Timeout: *timeout, RPCTimeout: *rpcTimeout}, Concurrency: *readConcurrency, Warmup: *readWarmup, Duration: *readWindow, MaxAttempts: *readMaxAttempts, OutputBytes: *readOutputBytes, ResourceGateDir: *readResourceGateDir}, Interval: *mixedInterval}, output)
	}
	if mixedFlags {
		return errors.New("mixed flags require -mode mixed-window")
	}
	if *mode == "paced-window" {
		if freshFlag || flags.NArg() != 0 {
			return errors.New("paced-window refuses legacy mutation flags and positional arguments")
		}
		return runPacedWindow(parent, pacedOptions{Window: windowOptions{Admission: recallOptions{Config: *configPath, Bootstrap: *bootstrapPath, Dataset: *dataset, Provenance: *provenance, Probe: *probe, Phase: *phase, RunID: *runID, Timeout: *timeout, RPCTimeout: *rpcTimeout},
			Concurrency: *readConcurrency, Warmup: *readWarmup, Duration: *readWindow, MaxAttempts: *readMaxAttempts, OutputBytes: *readOutputBytes, ResourceGateDir: *readResourceGateDir}, Inserts: *pacedInserts, Interval: *pacedInterval}, output)
	}
	if pacedFlags {
		return errors.New("paced flags require -mode paced-window")
	}
	if *mode == "read-window" {
		if freshFlag || flags.NArg() != 0 {
			return errors.New("read-window refuses mutation flags and positional arguments")
		}
		return runReadWindow(parent, windowOptions{Admission: recallOptions{Config: *configPath, Bootstrap: *bootstrapPath, Dataset: *dataset, Provenance: *provenance, Probe: *probe, Phase: *phase, RunID: *runID, Timeout: *timeout, RPCTimeout: *rpcTimeout},
			Concurrency: *readConcurrency, Warmup: *readWarmup, Duration: *readWindow, MaxAttempts: *readMaxAttempts, OutputBytes: *readOutputBytes, ResourceGateDir: *readResourceGateDir}, output)
	}
	if readFlags {
		return errors.New("read-window flags require -mode read-window")
	}
	if *mode == "quiescent-recall" {
		conflict := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name == "fresh-inserts" {
				conflict = true
			}
		})
		if conflict || flags.NArg() != 0 {
			return errors.New("recall refuses workload flags and positional arguments")
		}
		return runRecall(parent, recallOptions{Config: *configPath, Bootstrap: *bootstrapPath, Dataset: *dataset, Provenance: *provenance, Probe: *probe, Phase: *phase, RunID: *runID, Timeout: *timeout, RPCTimeout: *rpcTimeout}, output)
	}
	if *mode != "query-under-write" || *dataset != "" || *phase != "" || *provenance != "" || *probe != "" {
		return errors.New("invalid mode or recall-only flags supplied to workload")
	}
	if err := validateProbeTimeouts(*timeout, *rpcTimeout); err != nil {
		return err
	}
	if *freshInserts < 1 || *freshInserts > 65 {
		return errors.New("-fresh-inserts requires1..65")
	}
	if flags.NArg() != 0 || *configPath == "" || *bootstrapPath == "" {
		return errors.New("requires -config, -bootstrap-receipt and no positional arguments")
	}
	r := report{Version: 1, RunID: *runID, Timeout: *timeout, RPCTimeout: *rpcTimeout, Verdict: "FAILED",
		Scope: "bounded RF4 one-group native exact-ID insert/strict query correctness; no sustained capacity,QPS,p99,host-loss or server-critical-section claim"}
	defer func() {
		if runErr != nil {
			r.Error = runErr.Error()
			if r.Verdict == "ACCEPT_BOUNDED_FUNCTIONAL_CLIENT_OVERLAP" {
				r.Verdict = "FAILED"
			}
		}
		finish(&r)
		if err := json.NewEncoder(output).Encode(struct {
			Event  string
			Report report
		}{"result", r}); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}()
	var config nativewire.FixedPeerTCPConfigV1
	var bootstrap nativewire.FixedPeerVectorQualificationV1
	var err error
	if r.ConfigSHA256, err = readJSON(*configPath, &config, 8<<20); err != nil {
		return err
	}
	if r.BootstrapSHA256, err = readJSON(*bootstrapPath, &bootstrap, 4<<20); err != nil {
		return err
	}
	if r.ConfigIdentity, err = nativewire.InspectFixedPeerTCPConfigV1(config); err != nil {
		return err
	}
	if r.Generation, err = admitBootstrap(config, bootstrap); err != nil {
		return err
	}
	v := config.VectorInitialization
	o := options{RunID: *runID, Generation: r.Generation, BootstrapID: bootstrap.Insert.VisibleID, BootstrapRevision: bootstrap.Insert.LiveRevision, BootstrapCommitIndex: bootstrap.Retry.CommitIndex, OwnerGroup: bootstrap.Insert.OwnerGroup, Timeout: *timeout, RPCTimeout: *rpcTimeout, Dimensions: v.IndexDefinition.Dimensions, FreshInserts: *freshInserts, EfSearch: v.IndexDefinition.EfSearch}
	if r.Operations, err = makePlan(o); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := os.Open(executable)
	if err != nil {
		return err
	}
	digest := sha256.New()
	_, err = io.Copy(digest, binary)
	closeErr := binary.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	r.BinarySHA256 = hex.EncodeToString(digest.Sum(nil))
	finish(&r)
	if err := json.NewEncoder(output).Encode(struct {
		Event  string
		Report report
	}{"planned", r}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, o.Timeout)
	defer cancel()
	control, err := nativewire.NewFixedPeerTCPClientV1(config)
	if err != nil {
		return err
	}
	defer control.Close()
	if err := readiness(ctx, control, config, &r, bootstrap.Retry.CommitIndex, 1); err != nil {
		return err
	}
	dial := func(call context.Context) (ownedVectorClient, error) {
		return nativewire.DialContext(call, "tcp", v.PublicAddresses[config.NodeID])
	}
	writer, err := dial(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if writer != nil {
			runErr = errors.Join(runErr, writer.Close())
		}
	}()
	reader, err := dial(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if reader != nil {
			runErr = errors.Join(runErr, reader.Close())
		}
	}()
	phases := phaseClients{
		writer: func(call context.Context) (vectorClient, error) { return renewPhaseClient(call, &writer, dial) },
		reader: func(call context.Context) (vectorClient, error) { return renewPhaseClient(call, &reader, dial) },
	}
	if err := runWorkload(ctx, writer, reader, o, &r, phases); err != nil {
		return err
	}
	if err := readiness(ctx, control, config, &r, r.HighestCommitIndex, 64); err != nil {
		r.Verdict = "FAILED"
		return err
	}
	r.Verdict = "ACCEPT_BOUNDED_FUNCTIONAL_CLIENT_OVERLAP"
	return nil
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runArgs(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
