package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type windowTestClient struct {
	fakeClient
	closed atomic.Int32
}

func (c *windowTestClient) Close() error { c.closed.Add(1); return nil }
func windowTestResponse(r *recallReport) public.SearchResponseV1 {
	return public.SearchResponseV1{Generation: r.Generation, Neighbors: append([]public.NeighborV1(nil), r.Queries[0].Truth...),
		Counters: public.SearchCountersV1{SelectedPartitions: 1, HNSWServedPartitions: 1, ReadProofs: 1}}
}
func windowTestReport(t *testing.T) (recallInput, windowReport) {
	in, admission := recallTestQueries(t)
	admission.RPCTimeout = time.Second
	return in, windowReport{Version: 1, Admission: admission, MaxAttempts: 65536, WarmupPlanned: 64, Concurrency: 1,
		RequestedDuration: time.Second, OutputBytes: 128 << 20}
}

func TestWindowInvalidBoundsAndModeAdmission(t *testing.T) {
	base := windowOptions{Admission: recallOptions{Timeout: 120 * time.Second, RPCTimeout: time.Second}, Concurrency: 1, Warmup: 64, MaxAttempts: 65536, OutputBytes: 128 << 20, Duration: time.Minute}
	if err := windowValidate(base); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*windowOptions){
		func(o *windowOptions) { o.Concurrency = 0 }, func(o *windowOptions) { o.Concurrency = 2 },
		func(o *windowOptions) { o.Warmup = -1 }, func(o *windowOptions) { o.Warmup = 1025 },
		func(o *windowOptions) { o.MaxAttempts = 0 }, func(o *windowOptions) { o.MaxAttempts = 65537 },
		func(o *windowOptions) { o.OutputBytes = (1 << 20) - 1 }, func(o *windowOptions) { o.OutputBytes = (256 << 20) + 1 },
		func(o *windowOptions) { o.Duration = time.Millisecond }, func(o *windowOptions) { o.Duration = 61 * time.Second },
		func(o *windowOptions) { o.Admission.Timeout = o.Duration },
	} {
		o := base
		mutate(&o)
		if err := windowValidate(o); err == nil {
			t.Fatalf("accepted %+v", o)
		}
	}
	for _, args := range [][]string{
		{"-mode", "read-window"}, {"-mode", "read-window", "-read-concurrency", "1", "-fresh-inserts", "1"},
		{"-read-concurrency", "1"}, {"-mode", "quiescent-recall", "-read-warmup", "64"},
		{"-mode", "read-window", "-read-concurrency", "4", "-phase", "pre"},
	} {
		var out bytes.Buffer
		if err := runArgs(context.Background(), args, &out); err == nil {
			t.Fatalf("accepted args %v", args)
		}
	}
}

func TestWindowPersistentIndependentClientsCancelJoinWithoutRetry(t *testing.T) {
	in, r := windowTestReport(t)
	r.Concurrency = 4
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered := make(chan struct{}, 4)
	calls := make([]atomic.Int32, 4)
	dialed := 0
	clients, err := windowConnect(ctx, 4, time.Second, func(context.Context) (ownedVectorClient, error) {
		worker := dialed
		dialed++
		return &windowTestClient{fakeClient: fakeClient{search: func(call context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
			calls[worker].Add(1)
			if request.TopK != 10 || request.Probes != 1 || request.EfSearch != r.Admission.Queries[0].Request.EfSearch || request.Deadline.IsZero() {
				return public.SearchResponseV1{}, errors.New("invalid request")
			}
			entered <- struct{}{}
			if worker == 0 {
				for i := 0; i < 4; i++ {
					select {
					case <-entered:
					case <-call.Done():
						return public.SearchResponseV1{}, call.Err()
					}
				}
				return public.SearchResponseV1{}, errors.New("first native call failed")
			}
			<-call.Done()
			return public.SearchResponseV1{}, call.Err()
		}}}, nil
	})
	if err != nil || len(clients) != 4 {
		t.Fatalf("connect len=%d err=%v", len(clients), err)
	}
	for i, client := range clients {
		for j := 0; j < i; j++ {
			if client == clients[j] {
				t.Fatal("shared worker client")
			}
		}
	}
	budget := 1 << 20
	err = windowPhase(ctx, clients, &in, &r, false, &budget)
	if err == nil || !strings.Contains(err.Error(), "first native call failed") {
		t.Fatalf("missing real failure: %v", err)
	}
	if r.Counts.Attempted != 4 || r.Completions != 4 || r.Counts.Failed != 1 || r.Counts.Canceled != 3 || r.Counts.Unknown != 0 || r.Counts.Unissued != r.MaxAttempts-4 || len(r.Attempts) != 4 {
		t.Fatalf("lost partial population: counts=%+v completions=%d records=%d", r.Counts, r.Completions, len(r.Attempts))
	}
	for i, client := range clients {
		if calls[i].Load() != 1 {
			t.Fatalf("worker %d calls=%d", i, calls[i].Load())
		}
		if client.(*windowTestClient).closed.Load() != 0 {
			t.Fatal("closed before worker join")
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
	windowSummarize(&r)
	if r.SuccessLatency.Samples != 0 || r.FailureLatency.Samples != 4 {
		t.Fatal("failed calls absent from separate latency population")
	}
}
func TestWindowWarmupOracleExcludedAndNormalCutoffDrains(t *testing.T) {
	in, r := windowTestReport(t)
	// Oracle setup already populated all truth and scorers, without any attempts.
	if len(r.Admission.Queries) != 16 || r.Counts.Attempted != 0 || !r.MeasuredOriginUTC.IsZero() {
		t.Fatal("oracle counted as measured work")
	}
	r.RequestedDuration = 100 * time.Millisecond
	calls := 0
	var firstMeasuredStart time.Time
	client := &windowTestClient{fakeClient: fakeClient{search: func(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
		calls++
		if calls > 64 {
			firstMeasuredStart = time.Now()
			// The interval expires while a successful call is outstanding. It must
			// drain, rather than be canceled and recorded as a false failure.
			timer := time.NewTimer(150 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return public.SearchResponseV1{}, ctx.Err()
			}
		}
		return windowTestResponse(&r.Admission), nil
	}}}
	clients := []ownedVectorClient{client}
	budget := 1 << 20
	if err := windowPhase(context.Background(), clients, &in, &r, true, &budget); err != nil {
		t.Fatal(err)
	}
	if r.WarmupCounts.Succeeded != 64 || r.WarmupCompletions != 64 || r.Counts.Attempted != 0 || !r.MeasuredOriginUTC.IsZero() {
		t.Fatalf("warmup leaked: %+v", r)
	}
	if err := windowPhase(context.Background(), clients, &in, &r, false, &budget); err != nil {
		t.Fatal(err)
	}
	windowSummarize(&r)
	if calls != 65 || r.Completions != 1 || r.Counts.Succeeded != 1 || r.StopReason != "window_elapsed" || r.SuccessLatency.Samples != 1 || r.FailureLatency.Samples != 0 || r.ActualDurationNS < int64(150*time.Millisecond) || r.MeasuredOriginUTC.After(firstMeasuredStart) {
		t.Fatalf("normal cutoff/warmup summary counts=%+v samples=%+v duration=%d calls=%d", r.Counts, r.SuccessLatency, r.ActualDurationNS, calls)
	}
	if r.AttemptsQPS != float64(r.Counts.Attempted)/(float64(r.ActualDurationNS)/float64(time.Second)) {
		t.Fatal("QPS denominator differs from actual loop duration")
	}
	if err := windowAcceptable(&r); err == nil {
		t.Fatal("accepted incomplete canonical-query coverage")
	}
	for _, q := range r.Admission.Queries {
		if q.Outcome != "unissued" || !q.Request.Deadline.IsZero() {
			t.Fatal("attempt mutated frozen oracle/request")
		}
	}
}

func TestWindowCountAndEncodedByteCapsRefuseVerdict(t *testing.T) {
	for _, byteCap := range []bool{false, true} {
		in, r := windowTestReport(t)
		budget := 1 << 20
		if byteCap {
			budget = 1
		} else {
			r.MaxAttempts = 1
		}
		client := &windowTestClient{fakeClient: fakeClient{search: func(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error) {
			return windowTestResponse(&r.Admission), nil
		}}}
		if err := windowPhase(context.Background(), []ownedVectorClient{client}, &in, &r, false, &budget); err == nil {
			t.Fatal("accepted truncated window")
		}
		windowSummarize(&r)
		if !r.Truncated || r.Counts.Attempted != 1 || r.Completions != 1 || len(r.Attempts) != 1 || r.Counts.Succeeded != 1 || r.MeanRecallAt10 != nil {
			t.Fatalf("lost capped result: %+v", r.Counts)
		}
		if byteCap && (r.Attempts[0].Response != nil || r.Attempts[0].ResponseSHA256 == "" || r.StopReason != "byte_cap") {
			t.Fatal("unbounded terminal response or missing identity")
		}
		if err := windowAcceptable(&r); err == nil {
			t.Fatal("cap did not refuse verdict")
		}
		var out bytes.Buffer
		raw, err := windowEvent("planned", &r)
		if err != nil {
			t.Fatal(err)
		}
		r.OutputBytes = len(raw)*2 + 2048
		if err := windowEmit(context.Background(), &out, "planned", &r); err != nil {
			t.Fatal(err)
		}
		r.PlannedEventBytes = out.Len()
		if err := windowEmit(context.Background(), &out, "result", &r); err != nil {
			t.Fatal(err)
		}
		if out.Len() > r.OutputBytes {
			t.Fatal("encoded pair exceeds budget")
		}
		r.OutputBytes = 1
		out.Reset()
		if err := windowEmit(context.Background(), &out, "result", &r); err == nil || out.Len() != 0 {
			t.Fatal("oversized pair emitted")
		}
	}
}

func TestWindowResponseOwnershipAndCanonicalValidation(t *testing.T) {
	in, r := windowTestReport(t)
	response := windowTestResponse(&r.Admission)
	client := fakeClient{search: func(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error) { return response, nil }}
	a, err := windowCall(context.Background(), func() {}, client, &in, &r.Admission, 0, 0, "measured", time.Now())
	if err != nil || a.Response == nil || a.RecallAt10 == nil {
		t.Fatal(err)
	}
	original := a.Response.Neighbors[0]
	response.Neighbors[0].ID = "reused-buffer"
	if a.Response.Neighbors[0] != original {
		t.Fatal("retained response aliased client buffer")
	}
	if _, err := windowCall(context.Background(), func() {}, client, &in, &r.Admission, 1, 0, "measured", time.Now()); err == nil {
		t.Fatal("unknown neighbor bypassed accepted canonical validation")
	}
	response = windowTestResponse(&r.Admission)
	response.Counters.ExactScanPartitions = 1
	if _, err := windowCall(context.Background(), func() {}, client, &in, &r.Admission, 2, 0, "measured", time.Now()); err == nil {
		t.Fatal("exact fallback accepted")
	}
}

func TestWindowNearestRankPercentilesAndFullCoverage(t *testing.T) {
	p := windowPercentiles([]int64{100, 1, 3, 2})
	if p.Samples != 4 || p.P50NS != 2 || p.P95NS != 100 || p.P99NS != 100 {
		t.Fatalf("nearest rank=%+v", p)
	}
	if p := windowPercentiles(nil); p != (windowLatency{}) {
		t.Fatal("empty population invented percentiles")
	}
	r := windowReport{StopReason: "window_elapsed", Counts: counts{Planned: 65536, Attempted: 16, Succeeded: 16, Unissued: 65520}, Completions: 16}
	for i := 0; i < 16; i++ {
		r.MeasuredQuerySucceeded[i] = 1
	}
	if err := windowAcceptable(&r); err != nil {
		t.Fatal(err)
	}
	r.Counts.Succeeded--
	r.Counts.Canceled++
	if err := windowAcceptable(&r); err == nil {
		t.Fatal("canceled population accepted")
	}
}

func TestWindowCanceledSetupAndConnectRetainUnissued(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	o := windowOptions{Admission: recallOptions{Timeout: time.Minute, RPCTimeout: time.Second}, Concurrency: 1, Duration: time.Second, MaxAttempts: 16, OutputBytes: 1 << 20}
	if err := runReadWindow(ctx, o, &out); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte("result")) || bytes.Contains(out.Bytes(), []byte("planned")) {
		t.Fatal("canceled setup lost failure or emitted plan")
	}
	calls := 0
	clients, err := windowConnect(ctx, 4, time.Second, func(context.Context) (ownedVectorClient, error) { calls++; return nil, errors.New("must not dial") })
	if !errors.Is(err, context.Canceled) || len(clients) != 0 || calls != 0 {
		t.Fatalf("canceled connect clients=%d calls=%d err=%v", len(clients), calls, err)
	}
}
