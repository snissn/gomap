package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func pacedTestWrite(r *recallReport, id string, vector []float32) pacedWrite {
	request := public.InsertRequestV1{Version: 1, Generation: r.Generation, ID: []byte(id), IdempotencyKey: []byte("paced-test/" + id), Vector: vector}
	request.Document, _ = json.Marshal(struct {
		Embedding []float32 `json:"embedding"`
		Kind      string    `json:"kind"`
	}{vector, "query-under-write"})
	return pacedWrite{Operation: operation{Kind: "insert", Phase: "paced-measured", ExpectedID: id, InsertRequest: &request, RequestSHA256: hashJSON(request), Outcome: "unissued"}}
}
func TestPacedFullBaselinePrefixInvariantAndCanonicalTie(t *testing.T) {
	in, r := recallTestQueries(t)
	low := pacedTestWrite(&r, "fresh-low", oracleVector(128, -1, 0))
	union, proofs, err := pacedProvePrefixes(context.Background(), &in, &r, []pacedWrite{low})
	if err != nil || len(proofs) != 2 || proofs[0].Top10SHA256 != proofs[1].Top10SHA256 || union.vectors["fresh-low"] == nil || in.vectors["fresh-low"] != nil {
		t.Fatalf("invariant/immutable union err=%v proofs=%+v", err, proofs)
	}
	replacement := pacedTestWrite(&r, "live", oracleVector(128, -1, 0))
	if _, _, err := pacedProvePrefixes(context.Background(), &in, &r, []pacedWrite{replacement}); err == nil {
		t.Fatal("baseline ID replacement admitted")
	}
	// An equal-score candidate lexically before the exact tenth ID displaces it.
	tenth := r.Queries[0].Truth[9]
	tied := pacedTestWrite(&r, "0-tied", append([]float32(nil), in.vectors[tenth.ID]...))
	score, err := r.Queries[0].scorer.ScoreV1(tied.Operation.InsertRequest.Vector)
	if err != nil || math.Float32bits(score) != math.Float32bits(tenth.Score) {
		t.Fatal("fixture does not establish exact FP32 tie")
	}
	if _, _, err := pacedProvePrefixes(context.Background(), &in, &r, []pacedWrite{tied}); err == nil || !strings.Contains(err.Error(), "changes at planned prefix 1") {
		t.Fatalf("tie-induced changed truth admitted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := pacedProvePrefixes(ctx, &in, &r, []pacedWrite{low}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled setup: %v", err)
	}
}

func TestPacedInvocationEvidenceAndFullRPCBudget(t *testing.T) {
	v := pacedInvocations{starts: map[string]int64{"future": -1, "started": 20}}
	a := windowAttempt{EndNS: 19, Response: &public.SearchResponseV1{Neighbors: []public.NeighborV1{{ID: "baseline"}, {ID: "started"}}}}
	if err := v.validate(a); err == nil {
		t.Fatal("future invocation accepted")
	}
	a.EndNS = 20
	if err := v.validate(a); err != nil {
		t.Fatal(err)
	}
	a.Response.Neighbors[1].ID = "future"
	if err := v.validate(a); err == nil {
		t.Fatal("never-invoked planned ID accepted as visible")
	}
	var calls atomic.Int32
	client := &windowTestClient{fakeClient: fakeClient{insert: func(context.Context, public.InsertRequestV1) (public.InsertResponseV1, error) {
		calls.Add(1)
		return public.InsertResponseV1{}, errors.New("must not invoke")
	}}}
	w := pacedWrite{Operation: operation{InsertRequest: &public.InsertRequestV1{}, Outcome: "unissued"}}
	if err := pacedInsert(context.Background(), client, &w, time.Now(), time.Second, nil, time.Now().Add(-time.Second)); err != nil || calls.Load() != 0 || w.Invoked || w.Operation.Outcome != "unissued" || w.SkipReason == "" {
		t.Fatalf("dispatch clipped RPC or invoked: %+v err=%v", w, err)
	}
	now := time.Unix(0, 0)
	cutoff := now.Add(60 * time.Second)
	if !pacedFits(now.Add(50*time.Second), cutoff, 10*time.Second) || pacedFits(now.Add(50*time.Second+time.Nanosecond), cutoff, 10*time.Second) {
		t.Fatal("full RPC budget boundary is incorrect")
	}
}

func TestPacedSuccessfulSharedQueryOverlapAndExplicitRetry(t *testing.T) {
	in, admission := recallTestQueries(t)
	admission.RPCTimeout = time.Second
	w := pacedTestWrite(&admission, "fresh-low", oracleVector(128, -1, 0))
	union, _, err := pacedProvePrefixes(context.Background(), &in, &admission, []pacedWrite{w})
	if err != nil {
		t.Fatal(err)
	}
	origin := time.Now()
	entered, release := make(chan struct{}), make(chan struct{})
	var writerCalls, readerCalls atomic.Int32
	writer := &windowTestClient{fakeClient: fakeClient{insert: func(ctx context.Context, request public.InsertRequestV1) (public.InsertResponseV1, error) {
		n := writerCalls.Add(1)
		if n == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return public.InsertResponseV1{}, ctx.Err()
			}
		}
		return goodInsert(request, 67, uint64(155+n)), nil
	}}}
	reader := &windowTestClient{fakeClient: fakeClient{search: func(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
		readerCalls.Add(1)
		select {
		case <-entered:
		case <-ctx.Done():
			return public.SearchResponseV1{}, ctx.Err()
		}
		return windowTestResponse(&admission), nil
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pacedInsert(ctx, writer, &w, origin, time.Second, nil, time.Time{}) }()
	a, err := windowCall(ctx, cancel, reader, union, &admission, 0, 0, "measured", origin)
	if err != nil || a.Outcome != "succeeded" || a.RecallAt10 == nil || *a.RecallAt10 != 1 {
		close(release)
		t.Fatalf("successful native validation: %+v err=%v", a, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	baseline := goodInsert(*w.Operation.InsertRequest, 66, 155)
	if err := pacedReceipt(&w, &baseline, 67, 155); err != nil {
		t.Fatal(err)
	}
	if a.StartNS >= w.Operation.EndNS || a.EndNS <= w.Operation.StartNS || a.EndNS >= w.Operation.EndNS {
		t.Fatal("channels did not establish successful completion while insert was outstanding")
	}
	request := *w.Operation.InsertRequest
	request.Deadline = time.Time{}
	retry := pacedWrite{Operation: operation{Kind: "insert", Phase: "explicit-retry", Outcome: "unissued", InsertRequest: &request}}
	if err := pacedInsert(ctx, writer, &retry, time.Now(), time.Second, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := pacedReceipt(&retry, &baseline, 67, 156); err != nil {
		t.Fatal(err)
	}
	originalHash, _ := recallLogicalHash(w.Operation)
	retryHash, _ := recallLogicalHash(retry.Operation)
	if originalHash != retryHash || writerCalls.Load() != 2 || readerCalls.Load() != 1 || retry.Operation.InsertResponse.LiveRevision != 67 {
		t.Fatal("retry identity/revision or hidden call mismatch")
	}
}

func TestPacedReadFailureImmediatelyCancelsWriterAndRetainsUnknown(t *testing.T) {
	in, wr := windowTestReport(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wr.RequestedDuration, wr.Admission.RPCTimeout = 5*time.Second, time.Second
	w := pacedTestWrite(&wr.Admission, "fresh-low", oracleVector(128, -1, 0))
	second := pacedTestWrite(&wr.Admission, "unissued", oracleVector(128, -1, 1))
	second.IntendedOffsetNS = int64(5 * time.Second)
	union, _, err := pacedProvePrefixes(ctx, &in, &wr.Admission, []pacedWrite{w, second})
	if err != nil {
		t.Fatal(err)
	}
	union.bootstrap.Insert = goodInsert(*w.Operation.InsertRequest, 1, 88)
	r := pacedReport{windowReport: wr, Writes: []pacedWrite{w, second}, PaceInterval: 5 * time.Second, BaselineLiveRevision: 66, FinalLiveRevision: 66, FinalCommitIndex: 155}
	entered := make(chan struct{})
	var calls atomic.Int32
	writer := &windowTestClient{fakeClient: fakeClient{insert: func(call context.Context, request public.InsertRequestV1) (public.InsertResponseV1, error) {
		calls.Add(1)
		close(entered)
		<-call.Done()
		return public.InsertResponseV1{}, &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: call.Err()}
	}}}
	reader := &windowTestClient{fakeClient: fakeClient{search: func(call context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
		select {
		case <-entered:
		case <-call.Done():
			return public.SearchResponseV1{}, call.Err()
		}
		response := windowTestResponse(&r.Admission)
		response.Counters.ExactScanPartitions = 1
		return response, nil
	}}}
	budget := 1 << 20
	err = pacedMeasured(ctx, []ownedVectorClient{reader}, writer, union, &r, &budget)
	pacedSummary(&r)
	if err == nil || r.Counts.Failed != 1 || r.WriteCounts.Unknown != 1 || r.WriteCounts.Unissued != 1 || calls.Load() != 1 || r.Writes[1].Invoked {
		t.Fatalf("failure/accounting err=%v reads=%+v writes=%+v calls=%d", err, r.Counts, r.WriteCounts, calls.Load())
	}
	if ctx.Err() != nil {
		t.Fatal("phase required outer timeout to cancel writer")
	}
	if r.ActualDurationNS <= r.Writes[0].Operation.EndNS {
		t.Fatal("duration fixed before writer drained")
	}
}

func TestPacedLateWriteSlotsStayUnissuedWithoutWaiting(t *testing.T) {
	in, wr := windowTestReport(t)
	wr.RequestedDuration = time.Millisecond
	wr.Admission.RPCTimeout = time.Second
	w := pacedTestWrite(&wr.Admission, "fresh-low", oracleVector(128, -1, 0))
	w.IntendedOffsetNS = int64(time.Hour)
	r := pacedReport{windowReport: wr, Writes: []pacedWrite{w}, PaceInterval: time.Second}
	var inserts atomic.Int32
	writer := &windowTestClient{fakeClient: fakeClient{insert: func(context.Context, public.InsertRequestV1) (public.InsertResponseV1, error) {
		inserts.Add(1)
		return public.InsertResponseV1{}, errors.New("must not dispatch")
	}}}
	reader := &windowTestClient{fakeClient: fakeClient{search: func(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error) {
		return windowTestResponse(&r.Admission), nil
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	budget := 1 << 20
	if err := pacedMeasured(ctx, []ownedVectorClient{reader}, writer, &in, &r, &budget); err != nil {
		t.Fatal(err)
	}
	pacedSummary(&r)
	if inserts.Load() != 0 || r.WriteCounts.Attempted != 0 || r.WriteCounts.Unissued != 1 || r.Writes[0].SkipReason == "" {
		t.Fatal("out-of-window insert did not stay unissued")
	}
}

func TestPacedModeBoundsRefuseBeforeInputOrNetwork(t *testing.T) {
	base := pacedOptions{Window: windowOptions{Admission: recallOptions{Phase: "post-only", Timeout: 120 * time.Second, RPCTimeout: 10 * time.Second}, Concurrency: 1, Warmup: 64, MaxAttempts: 65536, OutputBytes: 128 << 20, Duration: time.Minute}, Inserts: 6, Interval: 5 * time.Second}
	if err := pacedValidate(base); err != nil {
		t.Fatal(err)
	}
	for _, alter := range []func(*pacedOptions){func(o *pacedOptions) { o.Inserts = 0 }, func(o *pacedOptions) { o.Inserts = 11 }, func(o *pacedOptions) { o.Interval = 0 }, func(o *pacedOptions) { o.Window.Warmup = 0 }, func(o *pacedOptions) { o.Window.Admission.Phase = "pre" }, func(o *pacedOptions) { o.Window.Duration = time.Second }} {
		o := base
		alter(&o)
		if err := pacedValidate(o); err == nil {
			t.Fatalf("invalid bounds admitted: %+v", o)
		}
	}
}

func TestPacedReceiptFenceMismatchRemainsUnknown(t *testing.T) {
	_, admission := recallTestQueries(t)
	baseWrite := pacedTestWrite(&admission, "fresh-low", oracleVector(128, -1, 0))
	baseline := goodInsert(*baseWrite.Operation.InsertRequest, 66, 155)
	for _, change := range []func(*public.InsertResponseV1){
		func(r *public.InsertResponseV1) { r.LiveRevision = 66 },
		func(r *public.InsertResponseV1) { r.CommitIndex = 155 },
		func(r *public.InsertResponseV1) { r.PartitionID = 1 },
		func(r *public.InsertResponseV1) { r.OwnerGroup = "different-owner" },
		func(r *public.InsertResponseV1) { r.VisibilityToken = []byte("unexpected-split") },
	} {
		w := baseWrite
		response := goodInsert(*w.Operation.InsertRequest, 67, 156)
		change(&response)
		w.Operation.InsertResponse, w.Operation.Outcome, w.Invoked = &response, "succeeded", true
		if err := pacedReceipt(&w, &baseline, 67, 155); err == nil || w.Operation.Outcome != "unknown" || w.Operation.ErrorCode != string(public.ErrorCommitAmbiguousV1) {
			t.Fatalf("invalid fence lost ambiguity: %+v err=%v", w, err)
		}
	}
}
