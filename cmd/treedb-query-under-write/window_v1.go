package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type windowOptions struct {
	Admission                                     recallOptions
	Concurrency, Warmup, MaxAttempts, OutputBytes int
	Duration                                      time.Duration
}
type windowAttempt struct {
	Ordinal, Worker                                          int
	Phase, QueryID, RequestSHA256, Outcome, ErrorCode, Error string
	Deadline                                                 time.Time
	StartNS, EndNS                                           int64
	Response                                                 *public.SearchResponseV1
	ResponseSHA256                                           string
	ResponseBytes                                            int
	RecallAt10                                               *float64
}
type windowLatency struct {
	Samples             int
	P50NS, P95NS, P99NS int64
}
type windowReport struct {
	Version                                                  int
	Kind, Verdict, Scope, Schedule, LatencyBasis, StopReason string
	Admission                                                recallReport
	Concurrency, WarmupPlanned, MaxAttempts, OutputBytes     int
	RequestedDuration                                        time.Duration
	MeasuredOriginUTC                                        time.Time
	ActualDurationNS                                         int64
	AttemptsQPS, SuccessfulQPS                               float64
	Counts, WarmupCounts                                     counts
	Completions, WarmupCompletions                           int
	SuccessLatency, FailureLatency                           windowLatency
	MeasuredQueryAttempts, MeasuredQuerySucceeded            [16]int
	MeanRecallAt10                                           *float64
	Attempts                                                 []windowAttempt
	Truncated                                                bool
	RetainedAttemptBytes, PlannedEventBytes                  int
	Error                                                    string
}

func windowValidate(o windowOptions) error {
	if o.Concurrency != 1 && o.Concurrency != 4 {
		return errors.New("read-window requires explicit -read-concurrency 1 or 4")
	}
	if o.Warmup < 0 || o.Warmup > 1024 || o.MaxAttempts < 1 || o.MaxAttempts > 65536 || o.OutputBytes < 1<<20 || o.OutputBytes > 256<<20 || o.Duration < time.Second || o.Duration > time.Minute {
		return errors.New("read-window bounds: warmup0..1024, attempts1..65536, output1MiB..256MiB, duration1s..60s")
	}
	if err := validateProbeTimeouts(o.Admission.Timeout, o.Admission.RPCTimeout); err != nil {
		return err
	}
	if o.Duration >= o.Admission.Timeout {
		return errors.New("read-window duration must leave time for setup/warmup/drain/readiness within total timeout")
	}
	return nil
}

func windowEvent(event string, r *windowReport) ([]byte, error) {
	raw, err := json.Marshal(struct {
		Event  string
		Report *windowReport
	}{event, r})
	return append(raw, '\n'), err
}
func windowEmit(ctx context.Context, output io.Writer, event string, r *windowReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := windowEvent(event, r)
	if err != nil {
		return err
	}
	if len(raw)+r.PlannedEventBytes > r.OutputBytes {
		return errors.New("read-window encoded planned/result pair exceeds byte cap")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := output.Write(raw)
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	return errors.Join(err, ctx.Err())
}

// Each worker owns one dialed native connection for warmup and measurement.
// No redial/retry occurs after any call; the caller joins workers before close.
func windowConnect(ctx context.Context, n int, rpcTimeout time.Duration, dial func(context.Context) (ownedVectorClient, error)) ([]ownedVectorClient, error) {
	clients := make([]ownedVectorClient, 0, n)
	for i := 0; i < n; i++ {
		call, cancel := context.WithTimeout(ctx, rpcTimeout)
		if err := call.Err(); err != nil {
			cancel()
			return clients, err
		}
		client, err := dial(call)
		cancel()
		if err != nil {
			return clients, err
		}
		clients = append(clients, client)
	}
	return clients, nil
}

func windowCall(ctx context.Context, stop context.CancelFunc, client vectorClient, in *recallInput, admission *recallReport, ordinal, worker int, phase string, origin time.Time) (windowAttempt, error) {
	q := &admission.Queries[ordinal%len(admission.Queries)]
	a := windowAttempt{Ordinal: ordinal, Worker: worker, Phase: phase, QueryID: q.QueryID, RequestSHA256: q.RequestSHA256, Outcome: "unissued"}
	if err := ctx.Err(); err != nil {
		return a, err
	}
	call, cancel := context.WithTimeout(ctx, admission.RPCTimeout)
	defer cancel()
	request := q.Request
	request.Deadline, _ = call.Deadline()
	a.Deadline = request.Deadline
	a.StartNS = time.Since(origin).Nanoseconds()
	response, err := client.VectorSearchStrictV1(call, request)
	a.EndNS = time.Since(origin).Nanoseconds()
	if err != nil {
		stop()
	}
	a.Outcome = "succeeded"
	// Retain a private response copy before the worker reuses its client. Native
	// string/slice lifetime and malformed partial responses cannot leak aliases.
	raw, encodeErr := json.Marshal(response)
	a.ResponseBytes = len(raw)
	if encodeErr != nil || len(raw) > 32<<10 {
		err = errors.Join(err, errors.New("search response exceeds retained evidence bound"))
		if encodeErr == nil {
			a.ResponseSHA256 = recallSHA(raw)
		}
	} else {
		owned := response
		owned.Generation.Index = strings.Clone(response.Generation.Index)
		owned.Neighbors = append([]public.NeighborV1(nil), response.Neighbors...)
		for i := range owned.Neighbors {
			owned.Neighbors[i].ID = strings.Clone(owned.Neighbors[i].ID)
		}
		a.Response = &owned
	}
	if err == nil {
		value, validateErr := recallValidateResponse(q, response, in.vectors)
		err = validateErr
		if err == nil {
			a.RecallAt10 = &value
		}
	}
	if err != nil {
		stop()
		a.Outcome, a.ErrorCode = errorOutcome(err, false)
		a.Error = recallError(err)
	}
	return a, err
}

func windowAccount(c *counts, a windowAttempt) {
	if a.Outcome == "unissued" {
		return
	}
	c.Attempted++
	c.Unissued--
	switch a.Outcome {
	case "succeeded":
		c.Succeeded++
	case "failed":
		c.Failed++
	case "canceled":
		c.Canceled++
	default:
		c.Unknown++
	}
}
func windowPercentiles(values []int64) windowLatency {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	r := windowLatency{Samples: len(values)}
	if len(values) == 0 {
		return r
	}
	pick := func(p float64) int64 { return values[int(math.Ceil(p*float64(len(values))))-1] }
	r.P50NS, r.P95NS, r.P99NS = pick(.50), pick(.95), pick(.99)
	return r
}

// The small admission/retention lock is client harness cost, included in loop
// QPS but outside native-call latency. In-flight calls drain at normal cutoff;
// a genuine failure cancels every worker and preserves the returned population.
func windowPhase(parent context.Context, clients []ownedVectorClient, in *recallInput, r *windowReport, warmup bool, byteBudget *int) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	origin := time.Now()
	stopAt := origin.Add(r.RequestedDuration)
	phase := "measured"
	limit := r.MaxAttempts
	c := &r.Counts
	completed := &r.Completions
	if warmup {
		phase, limit, c, completed = "warmup", r.WarmupPlanned, &r.WarmupCounts, &r.WarmupCompletions
	} else {
		r.MeasuredOriginUTC = origin.UTC()
	}
	*c = counts{Planned: limit, Unissued: limit}
	var mu sync.Mutex
	var workers sync.WaitGroup
	var firstErr error
	next := 0
	fail := func(err error, reason string) {
		// A sibling cancellation can finish first; retain the initiating failure.
		previousOutcome, outcome := "", ""
		if firstErr != nil {
			previousOutcome, _ = errorOutcome(firstErr, false)
		}
		outcome, _ = errorOutcome(err, false)
		if firstErr == nil || (previousOutcome == "canceled" && outcome != "canceled") {
			firstErr = err
		}
		if r.StopReason == "" {
			r.StopReason = reason
		}
		cancel()
	}
	for worker, client := range clients {
		workers.Add(1)
		go func(worker int, client ownedVectorClient) {
			defer workers.Done()
			for warmOrdinal := worker; ; warmOrdinal += len(clients) {
				mu.Lock()
				if err := ctx.Err(); err != nil {
					mu.Unlock()
					return
				}
				ordinal := warmOrdinal
				if warmup {
					if ordinal >= limit {
						mu.Unlock()
						return
					}
				} else {
					if !time.Now().Before(stopAt) {
						mu.Unlock()
						return
					}
					if next >= limit {
						r.Truncated = true
						fail(errors.New("measured attempt cap reached before window cutoff"), "attempt_cap")
						mu.Unlock()
						return
					}
					ordinal = next
					next++
				}
				mu.Unlock()
				a, err := windowCall(ctx, cancel, client, in, &r.Admission, ordinal, worker, phase, origin)
				mu.Lock()
				if a.Outcome != "unissued" {
					windowAccount(c, a)
					(*completed)++
					encoded, encodeErr := json.Marshal(a)
					if encodeErr != nil {
						fail(encodeErr, "evidence_encoding")
					} else {
						if len(encoded)+1 > *byteBudget {
							r.Truncated = true
							// Reserve allows at most four response-free terminal records.
							if a.Response != nil {
								raw, _ := json.Marshal(a.Response)
								a.ResponseSHA256 = recallSHA(raw)
								a.Response = nil
							}
							encoded, _ = json.Marshal(a)
							fail(errors.New("retained attempt byte cap reached"), "byte_cap")
						} else {
							*byteBudget -= len(encoded) + 1
						}
						r.RetainedAttemptBytes += len(encoded) + 1
						r.Attempts = append(r.Attempts, a)
					}
				}
				if err != nil {
					fail(err, "call_failure")
				}
				mu.Unlock()
				if err != nil {
					return
				}
			}
		}(worker, client)
	}
	workers.Wait()
	if !warmup {
		r.ActualDurationNS = time.Since(origin).Nanoseconds()
		if r.ActualDurationNS > 0 {
			seconds := float64(r.ActualDurationNS) / float64(time.Second)
			r.AttemptsQPS, r.SuccessfulQPS = float64(c.Attempted)/seconds, float64(c.Succeeded)/seconds
		}
	}
	if firstErr != nil {
		return firstErr
	}
	if err := parent.Err(); err != nil {
		return err
	}
	if !warmup {
		r.StopReason = "window_elapsed"
	}
	return nil
}

func windowSummarize(r *windowReport) {
	r.MeasuredQueryAttempts, r.MeasuredQuerySucceeded = [16]int{}, [16]int{}
	r.MeanRecallAt10 = nil
	sort.Slice(r.Attempts, func(i, j int) bool {
		if r.Attempts[i].Phase != r.Attempts[j].Phase {
			return r.Attempts[i].Phase == "warmup"
		}
		return r.Attempts[i].Ordinal < r.Attempts[j].Ordinal
	})
	var success, failure []int64
	var recallSum float64
	for _, a := range r.Attempts {
		if a.Phase != "measured" {
			continue
		}
		r.MeasuredQueryAttempts[a.Ordinal%16]++
		if a.Outcome == "succeeded" {
			r.MeasuredQuerySucceeded[a.Ordinal%16]++
			success = append(success, a.EndNS-a.StartNS)
			if a.RecallAt10 != nil {
				recallSum += *a.RecallAt10
			}
		} else {
			failure = append(failure, a.EndNS-a.StartNS)
		}
	}
	r.SuccessLatency, r.FailureLatency = windowPercentiles(success), windowPercentiles(failure)
	if r.Counts.Succeeded > 0 && !r.Truncated {
		mean := recallSum / float64(r.Counts.Succeeded)
		r.MeanRecallAt10 = &mean
	}
}

func windowAcceptable(r *windowReport) error {
	if r.Truncated || r.StopReason != "window_elapsed" || r.Counts.Succeeded == 0 || r.Counts.Attempted != r.Counts.Succeeded || r.Completions != r.Counts.Attempted ||
		r.Counts.Failed+r.Counts.Canceled+r.Counts.Unknown != 0 || r.Counts.Planned != r.Counts.Attempted+r.Counts.Unissued {
		return errors.New("read-window cannot accept failed/incomplete/truncated measured population")
	}
	for _, n := range r.MeasuredQuerySucceeded {
		if n == 0 {
			return errors.New("read-window did not successfully cover all sixteen canonical queries")
		}
	}
	return nil
}

func runReadWindow(parent context.Context, o windowOptions, output io.Writer) (runErr error) {
	ctx, cancel := context.WithTimeout(parent, o.Admission.Timeout)
	defer cancel()
	r := windowReport{Version: 1, Kind: "fixed_cluster_read_window_v1", Verdict: "FAILED", Concurrency: o.Concurrency,
		WarmupPlanned: o.Warmup, MaxAttempts: o.MaxAttempts, OutputBytes: o.OutputBytes, RequestedDuration: o.Duration,
		Scope:        "operationally quiescent read-only closed-loop native observation; paced concurrent writes OPEN; no saturation/capacity/whole-lifetime resource peak claim",
		Schedule:     "warmup ordinal=worker+n*concurrency; measured global admission ordinal; query=ordinal%16; budgets describe possible attempts, not promised issued population",
		LatencyBasis: "one monotonic clock per phase; strict native client API call start/return only; loop QPS includes validation/retention and final drain, excludes input/oracle/dial/readiness/warmup",
		Admission: recallReport{Version: 1, Kind: "fixed_cluster_read_window_admission_v1", Verdict: "INPUT_ADMISSION_PENDING", Phase: o.Admission.Phase, RunID: o.Admission.RunID,
			ScoreContract: "", Timeout: o.Admission.Timeout, RPCTimeout: o.Admission.RPCTimeout}}
	defer func() {
		if runErr == nil {
			runErr = ctx.Err()
		}
		if runErr != nil {
			r.Verdict, r.Error = "FAILED", recallError(runErr)
		}
		windowSummarize(&r)
		// Failure evidence remains best effort after cancellation, under root's outer timeout.
		if r.OutputBytes < 1<<20 || r.OutputBytes > 256<<20 {
			r.OutputBytes = 1 << 20
		}
		runErr = errors.Join(runErr, windowEmit(context.WithoutCancel(ctx), output, "result", &r))
	}()
	if err := windowValidate(o); err != nil {
		return err
	}
	r.Counts = counts{Planned: o.MaxAttempts, Unissued: o.MaxAttempts}
	r.WarmupCounts = counts{Planned: o.Warmup, Unissued: o.Warmup}
	var in recallInput
	if err := recallPrepare(ctx, o.Admission, &in, &r.Admission); err != nil {
		return err
	}
	r.Admission.Verdict = "ACCEPTED_INPUTS_ORACLE_PENDING_RUNTIME"
	// recallPrepare uses exactly the accepted canonical score contract.
	r.Admission.ScoreContract = collections.VectorPartitionCanonicalScoreContractV1
	planned, err := windowEvent("planned", &r)
	if err != nil {
		return err
	}
	baseline, err := windowEvent("result", &r)
	if err != nil {
		return err
	}
	// Reserve readiness/error fields and four small terminal records on cap exhaustion.
	budget := o.OutputBytes - len(planned) - len(baseline) - (128 << 10)
	if budget < 0 {
		return errors.New("read-window byte cap cannot retain admission and terminal evidence")
	}
	if err := windowEmit(ctx, output, "planned", &r); err != nil {
		return err
	}
	r.PlannedEventBytes = len(planned)
	control, err := nativewire.NewFixedPeerTCPClientV1(in.config)
	if err != nil {
		return err
	}
	defer control.Close()
	observe := func(target *[]observation) error {
		receipt := report{RPCTimeout: o.Admission.RPCTimeout}
		err := readiness(ctx, control, in.config, &receipt, r.Admission.HighestCommitIndex, 1)
		*target = receipt.Readiness
		if err != nil {
			return err
		}
		states := make([]nativewire.FixedPeerReadinessV1, len(receipt.Readiness))
		for i, v := range receipt.Readiness {
			states[i] = v.State
		}
		return recallReadyStates(in.config, states, r.Admission.HighestCommitIndex)
	}
	if err := observe(&r.Admission.ReadinessBefore); err != nil {
		return err
	}
	clients, err := windowConnect(ctx, o.Concurrency, o.Admission.RPCTimeout, func(call context.Context) (ownedVectorClient, error) {
		return nativewire.DialContext(call, "tcp", in.config.VectorInitialization.PublicAddresses[in.config.NodeID])
	})
	defer func() {
		for _, client := range clients {
			runErr = errors.Join(runErr, client.Close())
		}
	}()
	if err != nil {
		return err
	}
	if err := windowPhase(ctx, clients, &in, &r, true, &budget); err != nil {
		return err
	}
	if err := windowPhase(ctx, clients, &in, &r, false, &budget); err != nil {
		return err
	}
	windowSummarize(&r)
	if err := windowAcceptable(&r); err != nil {
		return err
	}
	if err := observe(&r.Admission.ReadinessAfter); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.Verdict = "ACCEPT_QUIESCENT_READ_WINDOW_OBSERVATION"
	return nil
}
