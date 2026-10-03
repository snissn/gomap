package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type pacedOptions struct {
	Window   windowOptions
	Inserts  int
	Interval time.Duration
}
type pacedWrite struct {
	Operation         operation
	IntendedOffsetNS  int64
	Invoked           bool
	InvocationStartNS int64
	SkipReason        string
	ResponseBytes     int
	ResponseSHA256    string
}
type pacedResponseEvidence struct {
	Bytes  int
	SHA256 string
}
type pacedPrefix struct {
	DistinctPrefix  int
	CandidateScores [16]float32
	Top10SHA256     string
}
type pacedReport struct {
	windowReport
	BaselineLiveRevision, FinalLiveRevision, FinalCommitIndex uint64
	BaselinePopulationRows                                    int
	BaselinePopulationSHA256                                  string
	PaceInterval                                              time.Duration
	RecallQualification, RetryRule, VisibilityBasis           string
	PrefixProofs                                              []pacedPrefix
	Writes                                                    []pacedWrite
	WriteCounts, MutationCounts, VisibilityCounts             counts
	WriteSuccessLatency, WriteFailureLatency                  windowLatency
	Retry                                                     pacedWrite
	RetryOriginUTC, VisibilityOriginUTC                       time.Time
	Visibility                                                []operation
	VisibilityEvidence                                        []pacedResponseEvidence
	PreRecall, PostRecall                                     recallReport
	OverlappingSearches, CompletedSearchesDuringInsert        int
}

func pacedValidate(o pacedOptions) error {
	if err := windowValidate(o.Window); err != nil {
		return err
	}
	if o.Window.Admission.Phase != "post-only" || o.Window.Warmup != 64 || o.Window.Duration != time.Minute || o.Inserts < 1 || o.Inserts > 10 || o.Interval < time.Second || o.Interval > time.Minute {
		return errors.New("paced-window requires post-only, warmup64, window60s, inserts1..10, interval1s..60s")
	}
	return nil
}

func pacedSameTruth(a, b []public.NeighborV1) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || math.Float32bits(a[i].Score) != math.Float32bits(b[i].Score) {
			return false
		}
	}
	return true
}

// recallPlan already scored the entire admitted baseline. For insertion only,
// merging each candidate into that exact top10 is equivalent to rescoring the
// entire population at each prefix, including score/ID ties. Retain the actual
// candidate scores and prefix truth digest rather than invent a search watermark.
func pacedProvePrefixes(ctx context.Context, in *recallInput, admission *recallReport, writes []pacedWrite) (*recallInput, []pacedPrefix, error) {
	union := *in
	union.vectors = make(map[string][]float32, len(in.vectors)+len(writes))
	for id, v := range in.vectors {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		union.vectors[id] = v
	}
	if len(admission.Queries) != 16 {
		return nil, nil, errors.New("paced oracle requires sixteen full-baseline queries")
	}
	truths := make([][]public.NeighborV1, 16)
	for i, q := range admission.Queries {
		truths[i] = append([]public.NeighborV1(nil), q.Truth...)
	}
	proofs := []pacedPrefix{{Top10SHA256: hashJSON(truths)}}
	for ordinal, w := range writes {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		request := w.Operation.InsertRequest
		if request == nil || union.vectors[string(request.ID)] != nil {
			return nil, nil, errors.New("paced ID already exists in complete baseline or candidate plan")
		}
		union.vectors[string(request.ID)] = request.Vector
		proof := pacedPrefix{DistinctPrefix: ordinal + 1}
		for i, q := range admission.Queries {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			score, err := q.scorer.ScoreV1(request.Vector)
			if err != nil {
				return nil, nil, err
			}
			proof.CandidateScores[i] = score
			top := append(append([]public.NeighborV1(nil), truths[i]...), public.NeighborV1{ID: string(request.ID), Score: score})
			sort.Slice(top, func(a, b int) bool { return recallLess(top[a], top[b]) })
			top = top[:10]
			if !pacedSameTruth(top, q.Truth) {
				return nil, nil, fmt.Errorf("concurrent recall not qualified: query %s changes at planned prefix %d", q.QueryID, ordinal+1)
			}
			truths[i] = top
		}
		proof.Top10SHA256 = hashJSON(truths)
		proofs = append(proofs, proof)
	}
	return &union, proofs, nil
}

func pacedPlan(ctx context.Context, o pacedOptions, in *recallInput, r *pacedReport) (*recallInput, error) {
	// This first checkpoint consumes only the independently accepted original
	// post65 ledger. Another paced run cannot reuse that stale baseline.
	if r.Admission.PopulationRows != 10069 || r.Admission.HighestCommitIndex != 155 || in.bootstrap.Insert.LiveRevision != 1 {
		return nil, errors.New("paced-window requires accepted trial11 post65 population10069/prefix155/live66")
	}
	r.BaselineLiveRevision, r.FinalLiveRevision, r.FinalCommitIndex = 66, 66, 155
	r.BaselinePopulationRows, r.BaselinePopulationSHA256 = r.Admission.PopulationRows, r.Admission.PopulationSHA256
	plan, err := makePlan(options{RunID: o.Window.Admission.RunID, Generation: r.Admission.Generation, BootstrapID: in.bootstrap.Insert.VisibleID,
		BootstrapRevision: 1, BootstrapCommitIndex: 155, OwnerGroup: in.bootstrap.Insert.OwnerGroup,
		Timeout: o.Window.Admission.Timeout, RPCTimeout: o.Window.Admission.RPCTimeout, Dimensions: 128, FreshInserts: o.Inserts, EfSearch: in.config.VectorInitialization.IndexDefinition.EfSearch})
	if err != nil {
		return nil, err
	}
	for _, op := range plan {
		if op.Kind == "insert" && op.Phase == "concurrent" {
			op.Ordinal, op.Phase = len(r.Writes), "paced-measured"
			r.Writes = append(r.Writes, pacedWrite{Operation: op, IntendedOffsetNS: int64(time.Duration(len(r.Writes)) * o.Interval)})
		}
	}
	r.WriteCounts = counts{Planned: len(r.Writes), Unissued: len(r.Writes)}
	union, proofs, err := pacedProvePrefixes(ctx, in, &r.Admission, r.Writes)
	if err != nil {
		return nil, err
	}
	r.PrefixProofs = proofs
	// Independent acknowledged-ID probes must use the complete possible population
	// and the same FP32 score/tie contract, not makePlan's old anchor-only guard.
	ids := make([]string, 0, len(union.vectors))
	for id := range union.vectors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for i, w := range r.Writes {
		request := *w.Operation.InsertRequest
		scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(request.Vector)
		if err != nil {
			return nil, err
		}
		top, err := recallTop10(ctx, scorer, ids, union.vectors)
		if err != nil {
			return nil, err
		}
		if top[0].ID != string(request.ID) {
			return nil, errors.New("paced acknowledged-ID visibility query has a different canonical full-population winner")
		}
		search := r.Admission.Queries[0].Request
		search.Query, search.TopK = request.Vector, 1
		r.Visibility = append(r.Visibility, operation{Ordinal: i, Phase: "post-ack-visibility", Kind: "search", ExpectedID: string(request.ID), RequestSHA256: hashJSON(search), SearchRequest: &search, Outcome: "unissued"})
		r.VisibilityEvidence = append(r.VisibilityEvidence, pacedResponseEvidence{})
	}
	r.RecallQualification = "all sixteen full-baseline canonical FP32 top10 identities and score bits invariant at every possible planned serial insert prefix; no per-response applied/live watermark"
	return union, nil
}

// Invocation evidence uses the same measured monotonic origin as searches.
// A known planned vector alone is never accepted as evidence it was invoked.
type pacedInvocations struct {
	mu     sync.Mutex
	starts map[string]int64
}

func (v *pacedInvocations) validate(a windowAttempt) error {
	if a.Response == nil {
		return errors.New("missing bounded search response")
	}
	if a.Response.Counters.Retries != 0 || a.Response.Counters.Redirects != 0 {
		return errors.New("paced strict search retried or redirected")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, n := range a.Response.Neighbors {
		if start, planned := v.starts[n.ID]; planned && (start < 0 || start > a.EndNS) {
			return errors.New("search returned a planned ID before its mutation invocation interval began")
		}
	}
	return nil
}
func pacedFits(now, cutoff time.Time, rpc time.Duration) bool { return !now.After(cutoff.Add(-rpc)) }
func pacedWait(ctx context.Context, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay := time.Until(at); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

// The invocation marker is recorded inside the client interface method, not
// during earlier admission/request preparation. It proves local API dispatch
// began; it deliberately makes no wire-submission or server-prefix claim.
type pacedInvocationClient struct {
	vectorClient
	origin  time.Time
	entered func(int64)
}

func (c pacedInvocationClient) VectorInsertV1(ctx context.Context, request public.InsertRequestV1) (public.InsertResponseV1, error) {
	c.entered(time.Since(c.origin).Nanoseconds())
	return c.vectorClient.VectorInsertV1(ctx, request)
}

func pacedInsert(ctx context.Context, client vectorClient, w *pacedWrite, origin time.Time, rpc time.Duration, invoked func(int64), cutoff time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dispatch := time.Now()
	if deadline, ok := ctx.Deadline(); ok && !pacedFits(dispatch, deadline, rpc) {
		w.SkipReason = "full_rpc_budget_does_not_fit_at_overall_dispatch"
		if cutoff.IsZero() {
			return errors.New("remaining overall budget cannot admit complete explicit retry RPC")
		}
		return nil
	}
	call, cancel := context.WithTimeout(ctx, rpc)
	defer cancel()
	request := *w.Operation.InsertRequest
	request.Deadline, _ = call.Deadline()
	if !cutoff.IsZero() && request.Deadline.After(cutoff) {
		w.SkipReason = "full_rpc_budget_does_not_fit_at_dispatch"
		return nil
	}
	w.Operation.InsertRequest = &request
	if invoked != nil {
		client = pacedInvocationClient{vectorClient: client, origin: origin, entered: func(ns int64) { w.Invoked, w.InvocationStartNS = true, ns; invoked(ns) }}
	}
	w.Operation.StartNS = time.Since(origin).Nanoseconds()
	if invoked == nil {
		w.Invoked, w.InvocationStartNS = true, w.Operation.StartNS
	}
	response, err := client.VectorInsertV1(call, request)
	w.Operation.EndNS = time.Since(origin).Nanoseconds()
	raw, encodeErr := json.Marshal(response)
	w.ResponseBytes = len(raw)
	if encodeErr != nil || len(raw) > 32<<10 {
		err = errors.Join(err, &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: errors.New("insert response exceeds retained evidence bound")})
		if encodeErr == nil {
			w.ResponseSHA256 = recallSHA(raw)
		}
	} else {
		owned := response
		owned.Generation.Index, owned.VisibilityGeneration.Index = strings.Clone(response.Generation.Index), strings.Clone(response.VisibilityGeneration.Index)
		owned.VisibleID, owned.OwnerGroup = strings.Clone(response.VisibleID), strings.Clone(response.OwnerGroup)
		owned.VisibilityToken = append([]byte(nil), response.VisibilityToken...)
		w.Operation.InsertResponse = &owned
	}
	if err == nil {
		if e := public.ValidateInsertResponseV1(request, response); e != nil {
			err = &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: e}
		}
	}
	w.Operation.Outcome = "succeeded"
	if err != nil {
		w.Operation.Outcome, w.Operation.ErrorCode = errorOutcome(err, true)
		w.Operation.Error = recallError(err)
	}
	return err
}
func pacedReceipt(w *pacedWrite, baseline *public.InsertResponseV1, revision, previous uint64) error {
	response := w.Operation.InsertResponse
	if response == nil || response.LiveRevision != revision || response.CommitIndex <= previous || response.Generation != baseline.Generation || response.OwnerGroup != baseline.OwnerGroup || response.PartitionID != baseline.PartitionID || len(response.VisibilityToken) != 0 {
		err := &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: errors.New("ordinary insert sequence/owner/partition/token fence mismatch")}
		w.Operation.Outcome, w.Operation.ErrorCode = errorOutcome(err, true)
		w.Operation.Error = recallError(err)
		return err
	}
	return nil
}

func pacedMeasured(parent context.Context, readers []ownedVectorClient, writer vectorClient, union *recallInput, r *pacedReport, budget *int) error {
	ctx, stop := context.WithCancel(parent)
	defer stop()
	invoked := pacedInvocations{starts: make(map[string]int64, len(r.Writes))}
	for _, w := range r.Writes {
		invoked.starts[string(w.Operation.InsertRequest.ID)] = -1
	}
	done := make(chan error, 1)
	control := &windowPhaseControl{Stop: stop, Validate: invoked.validate, Join: func() error { return <-done }}
	control.Start = func(phase context.Context, origin, cutoff time.Time) {
		go func() {
			var runErr error
			defer func() { done <- runErr }()
			previousStart := origin.Add(-r.PaceInterval)
			for i := range r.Writes {
				w := &r.Writes[i]
				at := origin.Add(time.Duration(w.IntendedOffsetNS))
				if next := previousStart.Add(r.PaceInterval); next.After(at) {
					at = next
				}
				if !pacedFits(at, cutoff, r.Admission.RPCTimeout) {
					w.SkipReason = "planned_offset_full_rpc_budget_does_not_fit"
					return
				}
				if deadline, ok := phase.Deadline(); ok && !pacedFits(at, deadline, r.Admission.RPCTimeout) {
					w.SkipReason = "planned_offset_exceeds_overall_rpc_budget"
					return
				}
				if err := pacedWait(phase, at); err != nil {
					runErr = err
					return
				}
				now := time.Now()
				if !pacedFits(now, cutoff, r.Admission.RPCTimeout) {
					w.SkipReason = "full_rpc_budget_does_not_fit_remaining_measured_admission"
					return
				}
				if deadline, ok := phase.Deadline(); ok && !pacedFits(now, deadline, r.Admission.RPCTimeout) {
					w.SkipReason = "full_rpc_budget_does_not_fit_remaining_overall_budget"
					return
				}
				previousStart = now
				err := pacedInsert(phase, writer, w, origin, r.Admission.RPCTimeout, func(ns int64) {
					invoked.mu.Lock()
					invoked.starts[string(w.Operation.InsertRequest.ID)] = ns
					invoked.mu.Unlock()
				}, cutoff)
				if !w.Invoked {
					return
				}
				previousStart = origin.Add(time.Duration(w.InvocationStartNS))
				if err == nil {
					err = pacedReceipt(w, &union.bootstrap.Insert, r.FinalLiveRevision+1, r.FinalCommitIndex)
				}
				if err != nil {
					runErr = err
					stop()
					return
				}
				r.FinalLiveRevision, r.FinalCommitIndex = w.Operation.InsertResponse.LiveRevision, w.Operation.InsertResponse.CommitIndex
			}
		}()
	}
	return windowPhaseControlled(ctx, readers, union, &r.windowReport, false, budget, control)
}

// Visibility probes have their own bounded response and API-call interval;
// their oracle/validation work stays outside read-window QPS and call latency.
func pacedVisibility(ctx context.Context, client vectorClient, op *operation, evidence *pacedResponseEvidence, vector []float32, origin time.Time, rpc time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	call, cancel := context.WithTimeout(ctx, rpc)
	defer cancel()
	request := *op.SearchRequest
	request.Deadline, _ = call.Deadline()
	op.SearchRequest = &request
	op.StartNS = time.Since(origin).Nanoseconds()
	response, err := client.VectorSearchStrictV1(call, request)
	op.EndNS = time.Since(origin).Nanoseconds()
	raw, encodeErr := json.Marshal(response)
	evidence.Bytes = len(raw)
	if encodeErr != nil || len(raw) > 32<<10 {
		if encodeErr == nil {
			evidence.SHA256 = recallSHA(raw)
		}
		err = errors.Join(err, errors.New("visibility response exceeds retained bound"))
	} else {
		owned := response
		owned.Generation.Index = strings.Clone(response.Generation.Index)
		owned.Neighbors = append([]public.NeighborV1(nil), response.Neighbors...)
		for i := range owned.Neighbors {
			owned.Neighbors[i].ID = strings.Clone(owned.Neighbors[i].ID)
		}
		op.SearchResponse = &owned
	}
	if err == nil {
		err = validateSearch(request, response, op.ExpectedID)
	}
	if err == nil {
		scorer, e := collections.NewCanonicalVectorPartitionCosineScorerV1(request.Query)
		if e != nil {
			err = e
		} else {
			score, e := scorer.ScoreV1(vector)
			if e != nil {
				err = e
			} else if math.Float32bits(score) != math.Float32bits(response.Neighbors[0].Score) || response.Counters.Retries != 0 || response.Counters.Redirects != 0 {
				err = errors.New("visibility canonical score/retry mismatch")
			}
		}
	}
	op.Outcome = "succeeded"
	if err != nil {
		op.Outcome, op.ErrorCode = errorOutcome(err, false)
		op.Error = recallError(err)
	}
	return err
}

func pacedSummary(r *pacedReport) {
	r.WriteCounts = counts{Planned: len(r.Writes), Unissued: len(r.Writes)}
	var good, bad []int64
	for _, w := range r.Writes {
		a := windowAttempt{Outcome: w.Operation.Outcome}
		windowAccount(&r.WriteCounts, a)
		if w.Invoked {
			d := w.Operation.EndNS - w.Operation.StartNS
			if w.Operation.Outcome == "succeeded" {
				good = append(good, d)
			} else {
				bad = append(bad, d)
			}
		}
	}
	r.WriteSuccessLatency, r.WriteFailureLatency = windowPercentiles(good), windowPercentiles(bad)
	r.MutationCounts = r.WriteCounts
	r.MutationCounts.Planned++
	r.MutationCounts.Unissued++
	windowAccount(&r.MutationCounts, windowAttempt{Outcome: r.Retry.Operation.Outcome})
	r.VisibilityCounts = counts{Planned: len(r.Visibility), Unissued: len(r.Visibility)}
	for _, op := range r.Visibility {
		windowAccount(&r.VisibilityCounts, windowAttempt{Outcome: op.Outcome})
	}
	r.OverlappingSearches, r.CompletedSearchesDuringInsert = 0, 0
	for _, a := range r.Attempts {
		if a.Phase != "measured" || a.Outcome != "succeeded" {
			continue
		}
		overlap, completed := false, false
		for _, w := range r.Writes {
			if !w.Invoked {
				continue
			}
			if a.StartNS < w.Operation.EndNS && a.EndNS > w.Operation.StartNS {
				overlap = true
			}
			if a.StartNS >= w.Operation.StartNS && a.EndNS < w.Operation.EndNS {
				completed = true
			}
		}
		if overlap {
			r.OverlappingSearches++
		}
		if completed {
			r.CompletedSearchesDuringInsert++
		}
	}
}
func pacedRecallCopy(r recallReport, phase string) recallReport {
	r.Phase, r.Queries, r.ReadinessBefore, r.ReadinessAfter = phase, append([]recallQuery(nil), r.Queries...), nil, nil
	for i := range r.Queries {
		q := &r.Queries[i]
		q.Outcome, q.ErrorCode, q.Error, q.Response, q.RecallAt10 = "unissued", "", "", nil, nil
		q.StartNS, q.EndNS = 0, 0
		q.Request.Deadline = time.Time{}
	}
	return r
}
func pacedRunRecall(ctx context.Context, client vectorClient, in *recallInput, r *recallReport) error {
	if err := recallRunQueries(ctx, client, in, r); err != nil {
		return err
	}
	for i := range r.Queries {
		q := &r.Queries[i]
		if q.Response == nil || q.Response.Counters.Retries != 0 || q.Response.Counters.Redirects != 0 {
			q.Outcome, q.Error = "failed", "paired strict recall retried or redirected"
			recallFinish(r)
			return errors.New(q.Error)
		}
	}
	return nil
}
func pacedEvent(event string, r *pacedReport) ([]byte, error) {
	raw, err := json.Marshal(struct {
		Event  string
		Report *pacedReport
	}{event, r})
	return append(raw, '\n'), err
}
func pacedEmit(output io.Writer, event string, r *pacedReport) error {
	raw, err := pacedEvent(event, r)
	if err != nil {
		return err
	}
	if len(raw)+r.PlannedEventBytes > r.OutputBytes {
		return errors.New("paced-window encoded planned/result pair exceeds cap; verdict refused")
	}
	n, err := output.Write(raw)
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	return err
}

func runPacedWindow(parent context.Context, o pacedOptions, output io.Writer) (runErr error) {
	ctx, cancel := context.WithTimeout(parent, o.Window.Admission.Timeout)
	defer cancel()
	r := pacedReport{windowReport: windowReport{Version: 1, Kind: "fixed_cluster_paced_window_v1", Verdict: "FAILED", Concurrency: o.Window.Concurrency,
		WarmupPlanned: o.Window.Warmup, MaxAttempts: o.Window.MaxAttempts, OutputBytes: o.Window.OutputBytes, RequestedDuration: o.Window.Duration, ResourceGateDir: o.Window.ResourceGateDir,
		Counts: counts{Planned: o.Window.MaxAttempts, Unissued: o.Window.MaxAttempts}, WarmupCounts: counts{Planned: o.Window.Warmup, Unissued: o.Window.Warmup},
		Scope:        "bounded paced ordinary writes with representative strict native reads; invariant-top10 recall only; no saturation/capacity/general changing-population or whole-lifetime resource claim",
		Schedule:     "sixteen unchanged queries; serial minimum-spaced writer; complete RPC budget fits before write admission cutoff; no catchup or automatic retry",
		LatencyBasis: "monotonic native client API call start/return; read QPS includes validation/retention and all measured drain, excludes setup/oracle/pre-recall/warmup/gate waits/retry/post-recall",
		Admission:    recallReport{Version: 1, Kind: "fixed_cluster_paced_window_admission_v1", Phase: o.Window.Admission.Phase, RunID: o.Window.Admission.RunID, Timeout: o.Window.Admission.Timeout, RPCTimeout: o.Window.Admission.RPCTimeout}},
		PaceInterval: o.Interval, RetryRule: "one explicit identical logical retry of last acknowledged fresh write after measured drain; never retry failed/unknown; planned idle connection renewal outside measurement",
		VisibilityBasis: "canonical full-population self-query winner preflight plus post-ack strict native top1 probes; union membership alone is not visibility proof", Retry: pacedWrite{Operation: operation{Phase: "explicit-retry", Kind: "insert", Outcome: "unissued"}}}
	defer func() {
		if runErr == nil {
			runErr = ctx.Err()
		}
		if runErr != nil {
			r.Verdict, r.Error = "FAILED", recallError(runErr)
		}
		windowSummarize(&r.windowReport)
		pacedSummary(&r)
		if r.OutputBytes < 1<<20 || r.OutputBytes > 256<<20 {
			r.OutputBytes = 1 << 20
		}
		runErr = errors.Join(runErr, pacedEmit(output, "result", &r))
	}()
	if err := pacedValidate(o); err != nil {
		return err
	}
	gate, err := newWindowResourceGate(ctx, o.Window.ResourceGateDir, o.Window.Admission.RunID)
	if err != nil {
		return err
	}
	var in recallInput
	if err := recallPrepare(ctx, o.Window.Admission, &in, &r.Admission); err != nil {
		return err
	}
	r.Admission.ScoreContract = collections.VectorPartitionCanonicalScoreContractV1
	union, err := pacedPlan(ctx, o, &in, &r)
	if err != nil {
		return err
	}
	r.Admission.Verdict = "ACCEPTED_INPUTS_INVARIANT_PENDING_RUNTIME"
	r.PreRecall = pacedRecallCopy(r.Admission, "quiescent-before-paced")
	r.PostRecall = recallReport{Version: 1, Phase: "quiescent-after-paced", Counts: counts{Planned: 16, Unissued: 16}}
	pacedSummary(&r)
	planned, err := pacedEvent("planned", &r)
	if err != nil {
		return err
	}
	baseline, err := pacedEvent("result", &r)
	if err != nil {
		return err
	}
	// Maximum ten bounded write/probe responses, paired recall and terminal read
	// records remain outside the hot-path byte budget but inside the final pair cap.
	budget := o.Window.OutputBytes - len(planned) - len(baseline) - (3 << 20)
	if budget < 0 {
		return errors.New("paced byte cap cannot retain bounded write/paired-recall/terminal evidence")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := pacedEmit(output, "planned", &r); err != nil {
		return err
	}
	r.PlannedEventBytes = len(planned)
	control, err := nativewire.NewFixedPeerTCPClientV1(in.config)
	if err != nil {
		return err
	}
	defer control.Close()
	observe := func(target *[]observation, prefix uint64) error {
		receipt := report{RPCTimeout: r.Admission.RPCTimeout}
		err := readiness(ctx, control, in.config, &receipt, prefix, 1)
		*target = receipt.Readiness
		if err != nil {
			return err
		}
		states := make([]nativewire.FixedPeerReadinessV1, len(receipt.Readiness))
		for i, v := range receipt.Readiness {
			states[i] = v.State
		}
		return recallReadyStates(in.config, states, prefix)
	}
	if err := observe(&r.Admission.ReadinessBefore, r.FinalCommitIndex); err != nil {
		return err
	}
	dial := func(call context.Context) (ownedVectorClient, error) {
		return nativewire.DialContext(call, "tcp", in.config.VectorInitialization.PublicAddresses[in.config.NodeID])
	}
	readers, err := windowConnect(ctx, o.Window.Concurrency, r.Admission.RPCTimeout, dial)
	defer func() {
		for _, c := range readers {
			if c != nil {
				runErr = errors.Join(runErr, c.Close())
			}
		}
	}()
	if err != nil {
		return err
	}
	if err := pacedRunRecall(ctx, readers[0], &in, &r.PreRecall); err != nil {
		return err
	}
	// Other workers can have been idle throughout the serial pre-recall. Renew
	// these planned setup connections before warmup, never after a failed call.
	for i := range readers {
		if _, err := renewPhaseClient(ctx, &readers[i], dial); err != nil {
			return err
		}
	}
	if err := windowPhase(ctx, readers, &in, &r.windowReport, true, &budget); err != nil {
		return err
	}
	owned, err := windowConnect(ctx, 1, r.Admission.RPCTimeout, dial)
	if err != nil {
		return err
	}
	writer := owned[0]
	defer func() {
		if writer != nil {
			runErr = errors.Join(runErr, writer.Close())
		}
	}()
	if gate != nil {
		if err := gate.wait(ctx, "ready", &r.windowReport); err != nil {
			return err
		}
	}
	phaseErr := pacedMeasured(ctx, readers, writer, union, &r, &budget)
	if gate != nil {
		phaseErr = errors.Join(phaseErr, gate.wait(ctx, "done", &r.windowReport))
	}
	windowSummarize(&r.windowReport)
	pacedSummary(&r)
	if phaseErr != nil {
		return phaseErr
	}
	if err := windowAcceptable(&r.windowReport); err != nil {
		return err
	}
	if r.WriteCounts.Attempted == 0 || r.WriteCounts.Attempted != r.WriteCounts.Succeeded || r.CompletedSearchesDuringInsert == 0 {
		return errors.New("paced window lacks successful ordinary write/client-call overlap")
	}
	last := r.WriteCounts.Succeeded - 1
	request := *r.Writes[last].Operation.InsertRequest
	request.Deadline = time.Time{}
	r.Retry.Operation.InsertRequest, r.Retry.Operation.ExpectedID, r.Retry.Operation.RequestSHA256 = &request, string(request.ID), hashJSON(request)
	retryClient, err := renewPhaseClient(ctx, &writer, dial)
	if err != nil {
		return err
	}
	retryOrigin := time.Now()
	r.RetryOriginUTC = retryOrigin.UTC()
	if err := pacedInsert(ctx, retryClient, &r.Retry, retryOrigin, r.Admission.RPCTimeout, nil, time.Time{}); err != nil {
		return err
	}
	if err := pacedReceipt(&r.Retry, &in.bootstrap.Insert, r.FinalLiveRevision, r.FinalCommitIndex); err != nil {
		return err
	}
	r.FinalCommitIndex = r.Retry.Operation.InsertResponse.CommitIndex
	if err := observe(&r.Admission.ReadinessAfter, r.FinalCommitIndex); err != nil {
		return err
	}
	// The paired post oracle contains only acknowledged distinct writes. It never
	// treats the planned union as a committed population or silently resolves UNKNOWN.
	final := in
	final.vectors = make(map[string][]float32, len(in.vectors)+last+1)
	for id, v := range in.vectors {
		final.vectors[id] = v
	}
	for i := 0; i <= last; i++ {
		request := r.Writes[i].Operation.InsertRequest
		final.vectors[string(request.ID)] = request.Vector
	}
	r.PostRecall = pacedRecallCopy(r.Admission, "quiescent-after-paced")
	r.PostRecall.Queries, r.PostRecall.HighestCommitIndex = nil, r.FinalCommitIndex
	if err := recallPlan(ctx, &final, &r.PostRecall); err != nil {
		return err
	}
	postClient, err := renewPhaseClient(ctx, &readers[0], dial)
	if err != nil {
		return err
	}
	visibilityOrigin := time.Now()
	r.VisibilityOriginUTC = visibilityOrigin.UTC()
	for i := 0; i <= last; i++ {
		op := &r.Visibility[i]
		if err := pacedVisibility(ctx, postClient, op, &r.VisibilityEvidence[i], final.vectors[op.ExpectedID], visibilityOrigin, r.Admission.RPCTimeout); err != nil {
			return err
		}
	}
	if err := pacedRunRecall(ctx, postClient, &final, &r.PostRecall); err != nil {
		return err
	}
	if r.PostRecall.PopulationRows != r.BaselinePopulationRows+last+1 {
		return errors.New("paired final population/retry dedupe mismatch")
	}
	if err := observe(&r.PostRecall.ReadinessAfter, r.FinalCommitIndex); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.Verdict = "ACCEPT_PACED_INVARIANT_RECALL_WINDOW_OBSERVATION"
	return nil
}
