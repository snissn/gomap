package main

import (
	"bytes"
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

type mixedClient interface {
	VectorReplaceV1(context.Context, public.ReplaceRequestV1) (public.MutationResponseV1, error)
	VectorDeleteV1(context.Context, public.DeleteRequestV1) (public.MutationResponseV1, error)
}

const mixedProfileChangingTop10 = "changing-top10"
const mixedReadPrefixMaxBytes = 128

type mixedOptions struct {
	Profile  string
	Window   windowOptions
	Interval time.Duration
}
type mixedWrite struct {
	Ordinal                                                                                          int
	Kind, Phase, LogicalSHA256, RequestSHA256, ResponseSHA256, Outcome, ErrorCode, Error, SkipReason string
	Replace                                                                                          *public.ReplaceRequestV1   `json:",omitempty"`
	Delete                                                                                           *public.DeleteRequestV1    `json:",omitempty"`
	Response                                                                                         *public.MutationResponseV1 `json:",omitempty"`
	IntendedOffsetNS, StartNS, EndNS                                                                 int64
	Invoked                                                                                          bool
	Deadline                                                                                         time.Time
	ResponseBytes                                                                                    int
}
type mixedChangedScore struct {
	ID        string
	Present   bool
	ScoreBits uint32
}
type mixedPrefix struct {
	Changed                       [][]mixedChangedScore
	Prefix, PopulationRows        int
	PopulationSHA256, Top10SHA256 string
	Truth                         [][]public.NeighborV1
}
type mixedReadPrefix struct {
	Ordinal, Lower, Upper, Matched int
	CompatibleMask                 uint8
	RecallAt10                     float64
}
type mixedAnchor struct{ ID, VectorSHA256 string }
type mixedReport struct {
	Profile      string `json:",omitempty"`
	Anchors      []mixedAnchor
	ReadPrefixes []mixedReadPrefix
	windowReport
	PaceInterval                                         time.Duration
	Prefixes                                             []mixedPrefix
	Writes, Retries                                      []mixedWrite
	Visibility                                           []operation
	VisibilityEvidence                                   []pacedResponseEvidence
	VisibilityOriginUTC                                  []time.Time
	RetryOriginUTC                                       time.Time
	PreRecall, PostRecall                                recallReport
	HighestNewCommitIndex                                uint64
	RequiredAppliedIndex                                 uint64
	WriteCounts, RetryCounts                             counts
	WriterLatencyNs                                      []int64
	OverlappingSearches, CompletedSearchesDuringMutation int
	AuditPlan                                            *nativewire.ColocatedAuditPlanV1 `json:",omitempty"`
	Audits                                               []nativewire.FixedPeerDiagnosticsV1
	RetryRule, AuthorityBoundary                         string
}

func mixedSameOutcome(a, b public.MutationResponseV1) error {
	// Forwards describes this call's ingress route, not its retained outcome.
	if a.Generation != b.Generation || a.OwnerGroup != b.OwnerGroup || a.CommitTerm != b.CommitTerm || a.CommitIndex != b.CommitIndex || a.Coverage != b.Coverage || a.LiveRevision != b.LiveRevision || a.Matched != b.Matched || a.Modified != b.Modified || a.Deleted != b.Deleted || !bytes.Equal(a.VisibilityToken, b.VisibilityToken) || !b.ProductionConsensus || b.AppliedIndex < a.CommitIndex {
		return errors.New("retry changed original durable outcome")
	}
	return nil
}

func mixedReadyStates(config nativewire.FixedPeerTCPConfigV1, history []observation, prefix uint64) error {
	if len(config.Nodes) == 0 || len(history) < len(config.Nodes) {
		return errors.New("incomplete mixed all-voter readiness round")
	}
	last := history[len(history)-len(config.Nodes):]
	states := make([]nativewire.FixedPeerReadinessV1, len(last))
	for i, v := range last {
		states[i] = v.State
	}
	return recallReadyStates(config, states, prefix)
}

func mixedCall(ctx context.Context, c mixedClient, w *mixedWrite, origin time.Time, rpc time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	call, cancel := context.WithTimeout(ctx, rpc)
	defer cancel()
	w.Deadline, _ = call.Deadline()
	var response public.MutationResponseV1
	var err error
	if w.Kind == "replace" && w.Replace != nil {
		request := *w.Replace
		request.Deadline = w.Deadline
		w.RequestSHA256 = hashJSON(request)
		w.StartNS = time.Since(origin).Nanoseconds()
		w.Invoked = true
		response, err = c.VectorReplaceV1(call, request)
	} else if w.Kind == "delete" && w.Delete != nil {
		request := *w.Delete
		request.Deadline = w.Deadline
		w.RequestSHA256 = hashJSON(request)
		w.StartNS = time.Since(origin).Nanoseconds()
		w.Invoked = true
		response, err = c.VectorDeleteV1(call, request)
	} else {
		return errors.New("invalid mixed operation")
	}
	w.EndNS = time.Since(origin).Nanoseconds()
	raw, e := json.Marshal(response)
	w.ResponseBytes = len(raw)
	if e != nil || len(raw) > 32<<10 {
		err = errors.Join(err, &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: errors.New("mutation response retention bound")})
	} else {
		w.ResponseSHA256 = recallSHA(raw)
		owned := response
		owned.Generation.Index = strings.Clone(response.Generation.Index)
		owned.OwnerGroup = strings.Clone(response.OwnerGroup)
		owned.VisibilityToken = bytes.Clone(response.VisibilityToken)
		w.Response = &owned
	}
	if err == nil {
		g := public.GenerationIDV1{}
		if w.Replace != nil {
			g = w.Replace.Generation
		} else {
			g = w.Delete.Generation
		}
		if e := public.ValidateMutationResponseV1(g, w.Kind == "delete", response); e != nil {
			err = &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: e}
		}
	}
	w.Outcome = "succeeded"
	if err != nil {
		w.Outcome, w.ErrorCode = errorOutcome(err, true)
		w.Error = recallError(err)
	}
	return err
}
func mixedPopulation(in *recallInput, writes []mixedWrite) (*recallInput, error) {
	out := *in
	out.vectors = make(map[string][]float32, len(in.vectors))
	for id, v := range in.vectors {
		out.vectors[id] = v
	}
	for _, w := range writes {
		if w.Replace != nil {
			id := string(w.Replace.ID)
			if out.vectors[id] == nil {
				return nil, errors.New("replace target missing in acknowledged prefix")
			}
			out.vectors[id] = w.Replace.Vector
		} else if w.Delete != nil {
			id := string(w.Delete.ID)
			if out.vectors[id] == nil {
				return nil, errors.New("delete target missing in acknowledged prefix")
			}
			delete(out.vectors, id)
		} else {
			return nil, errors.New("missing operation request")
		}
	}
	out.corpusIDs = make([]string, 0, len(in.corpusIDs))
	for _, id := range in.corpusIDs {
		if out.vectors[id] != nil {
			out.corpusIDs = append(out.corpusIDs, id)
		}
	}
	return &out, nil
}
func mixedTruth(ctx context.Context, in *recallInput, baseline recallReport, prefix int) (recallReport, mixedPrefix, error) {
	return mixedTruthForProfile(ctx, in, baseline, prefix, "")
}
func mixedTruthForProfile(ctx context.Context, in *recallInput, baseline recallReport, prefix int, profile string) (recallReport, mixedPrefix, error) {
	r := pacedRecallCopy(baseline, "mixed-prefix")
	r.Queries = nil
	var err error
	if profile == mixedProfileChangingTop10 && prefix > 0 {
		err = recallPlanChangedPopulation(ctx, in, &r, &baseline)
	} else {
		err = recallPlan(ctx, in, &r)
	}
	if err != nil {
		return r, mixedPrefix{}, err
	}
	p := mixedPrefix{Prefix: prefix, PopulationRows: r.PopulationRows, PopulationSHA256: r.PopulationSHA256}
	for i, q := range r.Queries {
		same := pacedSameTruth(q.Truth, baseline.Queries[i].Truth)
		if profile != mixedProfileChangingTop10 && !same {
			return r, p, fmt.Errorf("query%s canonical top10 changes at mixed prefix%d", q.QueryID, prefix)
		}
		if same {
			// Full canonical ID/score equality was just proved. Reuse the
			// immutable admitted row so validation can recognize identical truth.
			q.Truth = baseline.Queries[i].Truth
			r.Queries[i].Truth = q.Truth
		}
		p.Truth = append(p.Truth, q.Truth)
	}
	p.Top10SHA256 = hashJSON(p.Truth)
	return r, p, nil
}
func mixedPlan(ctx context.Context, in *recallInput, r *mixedReport) (*recallInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(in.queries) != 16 || len(r.Admission.Queries) != 16 {
		return nil, errors.New("mixed requires sixteen admitted queries")
	}
	if r.Profile != "" && r.Profile != mixedProfileChangingTop10 {
		return nil, errors.New("unsupported mixed profile")
	}
	excluded := map[string]bool{}
	for _, q := range r.Admission.Queries {
		for _, n := range q.Truth {
			excluded[n.ID] = true
		}
		for _, n := range q.CorpusTruth {
			excluded[n.ID] = true
		}
	}
	ids := make([]string, 0, 4)
	for _, id := range in.corpusIDs {
		if r.Profile == mixedProfileChangingTop10 || !excluded[id] {
			ids = append(ids, id)
			if len(ids) == 4 {
				break
			}
		}
	}
	if len(ids) != 4 {
		return nil, errors.New("four eligible existing corpus IDs required")
	}
	g := r.Admission.Generation
	replace := func(id string, v []float32, ordinal int) mixedWrite {
		document, _ := json.Marshal(struct {
			Embedding []float32 `json:"embedding"`
			Kind      string    `json:"kind"`
		}{v, fmt.Sprintf("mixed-%s-%d", r.Admission.RunID, ordinal)})
		request := public.ReplaceRequestV1{Version: 1, Generation: g, ID: []byte(id), IdempotencyKey: []byte(fmt.Sprintf("%s-mixed-%d", r.Admission.RunID, ordinal)), Vector: v, Document: document}
		return mixedWrite{Ordinal: ordinal, Kind: "replace", Phase: "mixed-measured", Replace: &request, LogicalSHA256: hashJSON(request), Outcome: "unissued", IntendedOffsetNS: int64(time.Duration(ordinal) * r.PaceInterval)}
	}
	deletion := func(id string, ordinal int) mixedWrite {
		request := public.DeleteRequestV1{Version: 1, Generation: g, ID: []byte(id), IdempotencyKey: []byte(fmt.Sprintf("%s-mixed-%d", r.Admission.RunID, ordinal))}
		return mixedWrite{Ordinal: ordinal, Kind: "delete", Phase: "mixed-measured", Delete: &request, LogicalSHA256: hashJSON(request), Outcome: "unissued", IntendedOffsetNS: int64(time.Duration(ordinal) * r.PaceInterval)}
	}
	first, second, third := in.vectors[ids[1]], in.vectors[ids[3]], in.vectors[ids[3]]
	if r.Profile == mixedProfileChangingTop10 {
		first, second, third = in.queries[0], in.queries[1], in.queries[2]
	}
	r.Writes = []mixedWrite{replace(ids[0], first, 0), replace(ids[0], second, 1), deletion(ids[1], 2), replace(ids[2], third, 3), deletion(ids[2], 4), deletion(ids[3], 5)}
	var final *recallInput
	for prefix := 0; prefix <= len(r.Writes); prefix++ {
		state, err := mixedPopulation(in, r.Writes[:prefix])
		if err != nil {
			return nil, err
		}
		_, proof, err := mixedTruthForProfile(ctx, state, r.Admission, prefix, r.Profile)
		if err != nil {
			return nil, err
		}
		for _, q := range r.Admission.Queries {
			var row []mixedChangedScore
			scorer := q.scorer
			if scorer == nil {
				scorer, err = collections.NewCanonicalVectorPartitionCosineScorerV1(q.Request.Query)
				if err != nil {
					return nil, err
				}
			}
			for _, id := range ids {
				item := mixedChangedScore{ID: id, Present: state.vectors[id] != nil}
				if item.Present {
					score, e := scorer.ScoreV1(state.vectors[id])
					if e != nil {
						return nil, e
					}
					item.ScoreBits = math.Float32bits(score)
				}
				row = append(row, item)
			}
			proof.Changed = append(proof.Changed, row)
		}
		r.Prefixes = append(r.Prefixes, proof)
		final = state
	}
	r.Admission.Verdict = "ACCEPTED_INPUTS_INVARIANT_PENDING_RUNTIME"
	if r.Profile == mixedProfileChangingTop10 {
		changed := false
		for _, p := range r.Prefixes[1:] {
			for qi, truth := range p.Truth {
				for _, n := range truth {
					present := false
					for _, initial := range r.Prefixes[0].Truth[qi] {
						present = present || initial.ID == n.ID
					}
					changed = changed || !present
				}
			}
		}
		if !changed {
			return nil, errors.New("changing-top10 profile did not change any full canonical top10 membership")
		}
		r.Admission.Verdict = "ACCEPTED_INPUTS_CHANGING_TOP10_PENDING_RUNTIME"
	}
	return final, nil
}
func mixedValidate(o mixedOptions) error {
	if o.Profile != "" && o.Profile != mixedProfileChangingTop10 {
		return errors.New("mixed-profile must be changing-top10 or omitted")
	}
	if err := windowValidate(o.Window); err != nil {
		return err
	}
	if o.Window.Duration != time.Minute || o.Window.Warmup != 64 || o.Interval < time.Second || o.Interval > 8*time.Second {
		return errors.New("mixed-window requires warmup64,window60s,interval1s..8s")
	}
	if 5*o.Interval+2*o.Window.Admission.RPCTimeout >= o.Window.Duration {
		return errors.New("mixed-window final slot must leave positive headroom after write and visibility RPC budgets")
	}
	return nil
}
func mixedSummary(r *mixedReport) {
	r.WriteCounts = counts{Planned: 6, Unissued: 6}
	r.RetryCounts = counts{Planned: 2, Unissued: 2}
	r.WriterLatencyNs = nil
	for _, w := range r.Writes {
		windowAccount(&r.WriteCounts, windowAttempt{Outcome: w.Outcome})
		if w.Invoked {
			r.WriterLatencyNs = append(r.WriterLatencyNs, w.EndNS-w.StartNS)
		}
	}
	for _, w := range r.Retries {
		windowAccount(&r.RetryCounts, windowAttempt{Outcome: w.Outcome})
	}
	r.OverlappingSearches, r.CompletedSearchesDuringMutation = 0, 0
	for _, a := range r.Attempts {
		if a.Phase != "measured" || a.Outcome != "succeeded" {
			continue
		}
		overlap, complete := false, false
		for _, w := range r.Writes {
			if !w.Invoked {
				continue
			}
			overlap = overlap || (a.StartNS < w.EndNS && a.EndNS > w.StartNS)
			complete = complete || (a.StartNS >= w.StartNS && a.EndNS < w.EndNS)
		}
		if overlap {
			r.OverlappingSearches++
		}
		if complete {
			r.CompletedSearchesDuringMutation++
		}
	}
}

// One entire response must agree with one causally permitted prefix. Changed
// IDs have precomputed exact scores/presence; unchanged vectors remain immutable.
func mixedValidatePrefix(q *recallQuery, response public.SearchResponseV1, in *recallInput, r *mixedReport, lower, upper int) (float64, error) {
	proof, err := mixedValidatePrefixProof(q, response, in, r, lower, upper)
	return proof.RecallAt10, err
}
func mixedValidatePrefixProof(q *recallQuery, response public.SearchResponseV1, in *recallInput, r *mixedReport, lower, upper int) (mixedReadPrefix, error) {
	proof := mixedReadPrefix{Lower: lower, Upper: upper, Matched: -1}
	c := response.Counters
	if response.Generation != q.Request.Generation || len(response.Neighbors) != 10 || c.SelectedPartitions == 0 || c.HNSWServedPartitions != c.SelectedPartitions || c.ExactScanPartitions != 0 || c.ReadProofs == 0 || c.Retries != 0 || c.Redirects != 0 {
		return proof, errors.New("mixed strict proof/fallback mismatch")
	}
	qi := -1
	for i, v := range r.Admission.Queries {
		if v.QueryID == q.QueryID {
			qi = i
			break
		}
	}
	if qi < 0 || len(r.Prefixes) > 7 || lower < 0 || upper >= len(r.Prefixes) || lower > upper || qi >= len(r.Prefixes[0].Changed) {
		return proof, errors.New("invalid mixed causal prefix range")
	}
	scorer := q.scorer
	if scorer == nil {
		var err error
		scorer, err = collections.NewCanonicalVectorPartitionCosineScorerV1(q.Request.Query)
		if err != nil {
			return proof, err
		}
	}
	changed := r.Prefixes[0].Changed[qi]
	for i, n := range response.Neighbors {
		if math.IsNaN(float64(n.Score)) || math.IsInf(float64(n.Score), 0) || (i > 0 && !recallLess(response.Neighbors[i-1], n)) {
			return proof, errors.New("mixed duplicate/nonfinite/order mismatch")
		}
		for _, earlier := range response.Neighbors[:i] {
			if earlier.ID == n.ID {
				return proof, errors.New("mixed duplicate/nonfinite/order mismatch")
			}
		}
		isChanged := false
		for _, v := range changed {
			isChanged = isChanged || v.ID == n.ID
		}
		if !isChanged {
			v := in.vectors[n.ID]
			if v == nil {
				return proof, errors.New("mixed unknown neighbor")
			}
			score, err := scorer.ScoreV1(v)
			if err != nil || math.Float32bits(score) != math.Float32bits(n.Score) {
				return proof, errors.New("mixed unchanged canonical score mismatch")
			}
		}
	}
	minimumHits := 10
	var countedTruth []public.NeighborV1
	countedHits := 0
	for prefix := lower; prefix <= upper; prefix++ {
		p := &r.Prefixes[prefix]
		if qi >= len(p.Changed) || qi >= len(p.Truth) || len(p.Truth[qi]) != 10 || len(p.Changed[qi]) != len(changed) {
			return proof, errors.New("missing mixed canonical prefix oracle")
		}
		for i, item := range changed {
			if p.Changed[qi][i].ID != item.ID {
				return proof, errors.New("changed mixed prefix identity")
			}
		}
		valid := true
		for _, n := range response.Neighbors {
			for _, v := range p.Changed[qi] {
				if v.ID == n.ID && (!v.Present || v.ScoreBits != math.Float32bits(n.Score)) {
					valid = false
					break
				}
			}
			if !valid {
				break
			}
		}
		if !valid {
			continue
		}
		if proof.Matched < 0 {
			proof.Matched = prefix
		}
		proof.CompatibleMask |= 1 << uint(prefix)
		truth := p.Truth[qi] // both rows have the already-checked length ten
		hits := countedHits
		if countedTruth == nil || &countedTruth[0] != &truth[0] {
			hits = 0
			for _, n := range response.Neighbors {
				for _, want := range truth {
					if n.ID == want.ID {
						hits++
						break
					}
				}
			}
			countedTruth, countedHits = truth, hits
		}
		minimumHits = min(minimumHits, hits)
	}
	if proof.Matched < 0 {
		return proof, errors.New("response matches no single causally permitted mixed prefix")
	}
	proof.RecallAt10 = float64(minimumHits) / 10
	return proof, nil
}
func mixedVisibility(ctx context.Context, c vectorClient, w mixedWrite, q recallQuery, state *recallInput, r *mixedReport) error {
	origin := time.Now()
	r.VisibilityOriginUTC = append(r.VisibilityOriginUTC, origin.UTC())
	request := q.Request
	request.VisibilityToken = bytes.Clone(w.Response.VisibilityToken)
	op := operation{Ordinal: len(r.Visibility), Kind: "search", Phase: "post-ack-current-prefix", SearchRequest: &request, RequestSHA256: hashJSON(request), Outcome: "unissued"}
	r.Visibility = append(r.Visibility, op)
	r.VisibilityEvidence = append(r.VisibilityEvidence, pacedResponseEvidence{})
	item := &r.Visibility[len(r.Visibility)-1]
	call, cancel := context.WithTimeout(ctx, r.Admission.RPCTimeout)
	defer cancel()
	request.Deadline, _ = call.Deadline()
	item.SearchRequest = &request
	item.RequestSHA256 = hashJSON(request)
	item.StartNS = time.Since(origin).Nanoseconds()
	response, err := c.VectorSearchStrictV1(call, request)
	item.EndNS = time.Since(origin).Nanoseconds()
	raw, e := json.Marshal(response)
	evidence := &r.VisibilityEvidence[len(r.VisibilityEvidence)-1]
	evidence.Bytes = len(raw)
	if e != nil || len(raw) > 32<<10 {
		err = errors.Join(err, errors.New("mixed visibility retention bound"))
	} else {
		evidence.SHA256 = recallSHA(raw)
		var owned public.SearchResponseV1
		if e = json.Unmarshal(raw, &owned); e != nil {
			err = errors.Join(err, e)
		} else {
			item.SearchResponse = &owned
		}
	}
	if err == nil {
		_, err = mixedValidatePrefix(&q, response, state, r, w.Ordinal+1, w.Ordinal+1)
	}
	if err == nil && (response.Counters.Retries != 0 || response.Counters.Redirects != 0) {
		err = errors.New("mixed current-prefix token strict proof mismatch")
	}
	item.Outcome = "succeeded"
	if err != nil {
		item.Outcome, item.ErrorCode = errorOutcome(err, false)
		item.Error = recallError(err)
	}
	return err
}
func mixedMeasured(parent context.Context, readers []ownedVectorClient, writer mixedClient, proofClient vectorClient, in *recallInput, r *mixedReport, budget *int) error {
	ctx, stop := context.WithCancel(parent)
	defer stop()
	done := make(chan error, 1)
	var ledgerMu sync.Mutex
	var issued, acknowledged [6]int64
	control := &windowPhaseControl{Stop: stop, Join: func() error { return <-done }, ResponseValidate: func(q *recallQuery, response public.SearchResponseV1, start, end int64) (float64, error) {
		ledgerMu.Lock()
		lower, upper := 0, 0
		for i := range issued {
			if acknowledged[i] > 0 && acknowledged[i] <= start {
				lower = i + 1
			}
			if issued[i] > 0 && issued[i] <= end {
				upper = i + 1
			}
		}
		ledgerMu.Unlock()
		return mixedValidatePrefix(q, response, in, r, lower, upper)
	}}
	control.Start = func(phase context.Context, origin, cutoff time.Time) {
		go func() {
			var runErr error
			defer func() { done <- runErr }()
			previous := origin.Add(-r.PaceInterval)
			for i := range r.Writes {
				w := &r.Writes[i]
				at := origin.Add(time.Duration(w.IntendedOffsetNS))
				if next := previous.Add(r.PaceInterval); next.After(at) {
					at = next
				}
				if !pacedFits(at, cutoff, 2*r.Admission.RPCTimeout) {
					w.SkipReason = "complete_write_and_visibility_budgets_do_not_fit"
					return
				}
				if err := pacedWait(phase, at); err != nil {
					runErr = err
					return
				}
				if !pacedFits(time.Now(), cutoff, 2*r.Admission.RPCTimeout) {
					w.SkipReason = "dispatch_write_and_visibility_budgets_do_not_fit"
					return
				}
				ledgerMu.Lock()
				issued[i] = time.Since(origin).Nanoseconds()
				ledgerMu.Unlock()
				err := mixedCall(phase, writer, w, origin, r.Admission.RPCTimeout)
				previous = origin.Add(time.Duration(w.StartNS))
				if err == nil {
					v := w.Response
					if v.CommitIndex <= r.HighestNewCommitIndex || v.OwnerGroup != in.bootstrap.Insert.OwnerGroup || (w.Kind == "replace" && (v.Matched != 1 || v.Modified != 1)) || (w.Kind == "delete" && v.Deleted != 1) {
						err = &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: errors.New("mixed new original outcome mismatch")}
						w.Outcome, w.ErrorCode = errorOutcome(err, true)
						w.Error = recallError(err)
					}
				}
				if err != nil {
					runErr = err
					stop()
					return
				}
				ledgerMu.Lock()
				acknowledged[i] = time.Since(origin).Nanoseconds()
				ledgerMu.Unlock()
				r.HighestNewCommitIndex = w.Response.CommitIndex
				if w.Response.AppliedIndex > r.RequiredAppliedIndex {
					r.RequiredAppliedIndex = w.Response.AppliedIndex
				}
				// Every prefix truth was fully scored before networking; the
				// unchanged vectors are immutable; changed-ID scores are prefix-local.
				err = mixedVisibility(phase, proofClient, *w, r.Admission.Queries[i%16], in, r)
				if err != nil {
					runErr = err
					stop()
					return
				}
			}
		}()
	}
	err := windowPhaseControlled(ctx, readers, in, &r.windowReport, false, budget, control)
	if err != nil {
		return err
	}
	return mixedRecheck(ctx, in, r)
}

// Exact call boundaries after writer/reader join narrow the online ledger range.
// Reuse retained recall scalars; no response copies or favorable-prefix choice.
func mixedRecheck(ctx context.Context, in *recallInput, r *mixedReport) (runErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Collection charged the online encoding conservatively. Corrected recall
	// can change its size; report the final actual encoding without refunding
	// that collection budget. Also account partial updates on a failed recheck.
	defer func() {
		retainedBytes := 0
		for _, a := range r.Attempts {
			encoded, err := json.Marshal(a)
			if err != nil {
				runErr = errors.Join(runErr, err)
				return
			}
			retainedBytes += len(encoded) + 1
		}
		r.RetainedAttemptBytes = retainedBytes
	}()
	r.ReadPrefixes = make([]mixedReadPrefix, 0, len(r.Attempts))
	for i := range r.Attempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		a := &r.Attempts[i]
		if a.Phase != "measured" {
			continue
		}
		lower, upper := 0, 0
		for i, w := range r.Writes {
			if w.Outcome == "succeeded" && w.EndNS <= a.StartNS {
				lower = i + 1
			}
			if w.Invoked && w.StartNS <= a.EndNS {
				upper = i + 1
			}
		}
		if a.Response == nil || a.Outcome != "succeeded" {
			return errors.New("missing successful mixed response at final causal proof")
		}
		q := &r.Admission.Queries[a.Ordinal%len(r.Admission.Queries)]
		proof, err := mixedValidatePrefixProof(q, *a.Response, in, r, lower, upper)
		proof.Ordinal = a.Ordinal
		r.ReadPrefixes = append(r.ReadPrefixes, proof)
		if err != nil {
			return fmt.Errorf("mixed response violates final call/ACK causal prefix: %w", err)
		}
		if a.RecallAt10 == nil {
			a.RecallAt10 = new(float64)
		}
		*a.RecallAt10 = proof.RecallAt10
	}
	return nil
}
func mixedAuditPlan(r *mixedReport) (nativewire.ColocatedAuditPlanV1, error) {
	p := nativewire.ColocatedAuditPlanV1{Version: 1, RunID: r.Admission.RunID, HighestNewCommitIndex: r.HighestNewCommitIndex, RequiredAppliedIndex: r.RequiredAppliedIndex}
	final := map[string]nativewire.ColocatedAuditStateV1{}
	for _, w := range r.Writes {
		if w.Outcome != "succeeded" || w.Response == nil {
			return p, errors.New("audit refuses incomplete acknowledged ledger")
		}
		p.Writes = append(p.Writes, nativewire.ColocatedAuditWriteV1{Replace: w.Replace, Delete: w.Delete, Response: *w.Response})
		if w.Replace != nil {
			final[string(w.Replace.ID)] = nativewire.ColocatedAuditStateV1{ID: w.Replace.ID, Document: w.Replace.Document}
		} else {
			final[string(w.Delete.ID)] = nativewire.ColocatedAuditStateV1{ID: w.Delete.ID, Absent: true}
		}
	}
	ids := make([]string, 0, len(final))
	for id := range final {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		p.Final = append(p.Final, final[id])
	}
	return p, nativewire.ValidateColocatedAuditPlanV1(context.Background(), p)
}
func mixedEmit(output io.Writer, event string, r *mixedReport) error {
	raw, err := json.Marshal(struct {
		Event  string
		Report *mixedReport
	}{event, r})
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if len(raw)+r.PlannedEventBytes > r.OutputBytes {
		return errors.New("mixed planned/result byte bound exceeded")
	}
	n, err := output.Write(raw)
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	return err
}
func runMixedWindow(parent context.Context, o mixedOptions, output io.Writer) (runErr error) {
	ctx, cancel := context.WithTimeout(parent, o.Window.Admission.Timeout)
	defer cancel()
	r := mixedReport{windowReport: windowReport{Version: 1, Kind: "fixed_cluster_mixed_window_v1", Verdict: "FAILED", Concurrency: o.Window.Concurrency, WarmupPlanned: o.Window.Warmup, MaxAttempts: o.Window.MaxAttempts, OutputBytes: o.Window.OutputBytes, RequestedDuration: o.Window.Duration, ResourceGateDir: o.Window.ResourceGateDir, Counts: counts{Planned: o.Window.MaxAttempts, Unissued: o.Window.MaxAttempts}, WarmupCounts: counts{Planned: o.Window.Warmup, Unissued: o.Window.Warmup}, Scope: "six serial colocated exact-ID mutations with invariant full canonical FP32 top10 strict reads; observational only, no capacity/host-loss/general changing-topK/full-source population claim", Schedule: "six serial original writes plus untimed token strict visibility before next mutation; no failed/UNKNOWN retry or discarded sample", LatencyBasis: "public client submit-to-visible ACK; six raw samples only, no p99/per-request allocation attribution; read QPS includes validation/retention/drain but excludes setup/full-prefix oracle/warmup/resource gate/post-join causal recheck/retry/post-recall/audit", Admission: recallReport{Version: 1, Kind: "fixed_cluster_mixed_window_admission_v1", Phase: o.Window.Admission.Phase, RunID: o.Window.Admission.RunID, Timeout: o.Window.Admission.Timeout, RPCTimeout: o.Window.Admission.RPCTimeout}}, PaceInterval: o.Interval, RetryRule: "after drain explicitly retry original replacement0 after supersession and deletion2; require original term/index/outcome counts/coverage/revision/token with applied index covering outcome; per-call route counters independently validated", AuthorityBoundary: "all-voter current-FSM audits before any shutdown prove only six witnesses and four changed IDs; ANN absence is not source authority"}
	defer func() {
		if runErr == nil {
			runErr = ctx.Err()
		}
		if runErr != nil {
			r.Verdict, r.Error = "FAILED", recallError(runErr)
		}
		windowSummarize(&r.windowReport)
		mixedSummary(&r)
		if r.OutputBytes < 1<<20 || r.OutputBytes > 256<<20 {
			r.OutputBytes = 1 << 20
		}
		runErr = errors.Join(runErr, mixedEmit(output, "result", &r))
	}()
	if err := mixedValidate(o); err != nil {
		return err
	}
	r.Profile = o.Profile
	if r.Profile == mixedProfileChangingTop10 {
		r.Scope = "six serial colocated exact-ID mutations with changing full canonical FP32 top10; conservative recall over compatible causal prefixes; observational only, no capacity/full-source population claim"
	}
	gate, err := newWindowResourceGate(ctx, o.Window.ResourceGateDir, o.Window.Admission.RunID)
	if err != nil {
		return err
	}
	var in recallInput
	if err = recallPrepare(ctx, o.Window.Admission, &in, &r.Admission); err != nil {
		return err
	}
	r.Admission.ScoreContract = collections.VectorPartitionCanonicalScoreContractV1
	r.HighestNewCommitIndex = r.Admission.HighestCommitIndex
	r.RequiredAppliedIndex = r.HighestNewCommitIndex
	corpus := map[string]bool{}
	for _, id := range in.corpusIDs {
		corpus[id] = true
	}
	for id, v := range in.vectors {
		if !corpus[id] {
			r.Anchors = append(r.Anchors, mixedAnchor{ID: id, VectorSHA256: hashJSON(v)})
		}
	}
	sort.Slice(r.Anchors, func(i, j int) bool { return r.Anchors[i].ID < r.Anchors[j].ID })
	final, err := mixedPlan(ctx, &in, &r)
	if err != nil {
		return err
	}
	r.PreRecall = pacedRecallCopy(r.Admission, "quiescent-before-mixed")
	r.PostRecall, _, err = mixedTruthForProfile(ctx, final, r.Admission, 6, r.Profile)
	if err != nil {
		return err
	}
	r.PostRecall.Phase = "quiescent-after-mixed"
	mixedSummary(&r)
	// Reserve bounded original/retry/probe/audit receipts in addition to each
	// retained read attempt. Complete pair encoding remains the final authority.
	planned, _ := json.Marshal(r)
	budget := o.Window.OutputBytes - 2*len(planned) - (4 << 20) - o.Window.MaxAttempts*mixedReadPrefixMaxBytes
	if budget < 0 {
		return errors.New("mixed evidence byte cap insufficient")
	}
	if err = mixedEmit(output, "planned", &r); err != nil {
		return err
	}
	event, _ := json.Marshal(struct {
		Event  string
		Report *mixedReport
	}{"planned", &r})
	r.PlannedEventBytes = len(event) + 1
	control, err := nativewire.NewFixedPeerTCPClientV1(in.config)
	if err != nil {
		return err
	}
	defer control.Close()
	observe := func(target *[]observation, rounds int) error {
		receipt := report{RPCTimeout: r.Admission.RPCTimeout}
		err := readiness(ctx, control, in.config, &receipt, r.RequiredAppliedIndex, rounds)
		*target = receipt.Readiness
		if err != nil {
			return err
		}
		return mixedReadyStates(in.config, receipt.Readiness, r.RequiredAppliedIndex)
	}
	if err = observe(&r.Admission.ReadinessBefore, 1); err != nil {
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
	if err = pacedRunRecall(ctx, readers[0], &in, &r.PreRecall); err != nil {
		return err
	}
	for i := range readers {
		if _, err = renewPhaseClient(ctx, &readers[i], dial); err != nil {
			return err
		}
	}
	if err = windowPhaseControlled(ctx, readers, &in, &r.windowReport, true, &budget, &windowPhaseControl{ResponseValidate: func(q *recallQuery, response public.SearchResponseV1, _, _ int64) (float64, error) {
		return mixedValidatePrefix(q, response, &in, &r, 0, 0)
	}}); err != nil {
		return err
	}
	owned, err := windowConnect(ctx, 2, r.Admission.RPCTimeout, dial)
	defer func() {
		for _, c := range owned {
			if c != nil {
				runErr = errors.Join(runErr, c.Close())
			}
		}
	}()
	if err != nil {
		return err
	}
	writer, ok := owned[0].(mixedClient)
	if !ok {
		return errors.New("public native client lacks mixed mutation capability")
	}
	if gate != nil {
		if err = gate.wait(ctx, "ready", &r.windowReport); err != nil {
			return err
		}
	}
	phaseErr := mixedMeasured(ctx, readers, writer, owned[1], &in, &r, &budget)
	if gate != nil {
		phaseErr = errors.Join(phaseErr, gate.wait(ctx, "done", &r.windowReport))
	}
	windowSummarize(&r.windowReport)
	mixedSummary(&r)
	if phaseErr != nil {
		return phaseErr
	}
	if err = windowAcceptable(&r.windowReport); err != nil {
		return err
	}
	if r.WriteCounts.Succeeded != 6 || r.CompletedSearchesDuringMutation == 0 {
		return errors.New("mixed six ACKs and completed strict-search/write overlap required")
	}
	// Idle phase connections are renewed before a planned explicit call, never
	// to retry a failed call. Old outcome position is not the highest new ACK.
	if _, err = renewPhaseClient(ctx, &owned[0], dial); err != nil {
		return err
	}
	writer = owned[0].(mixedClient)
	origin := time.Now()
	r.RetryOriginUTC = origin.UTC()
	for _, i := range []int{0, 2} {
		old := r.Writes[i]
		retry := mixedWrite{Ordinal: i, Kind: old.Kind, Phase: "after-drain-original-retry", Replace: old.Replace, Delete: old.Delete, LogicalSHA256: old.LogicalSHA256, Outcome: "unissued"}
		r.Retries = append(r.Retries, retry)
		w := &r.Retries[len(r.Retries)-1]
		if err = mixedCall(ctx, writer, w, origin, r.Admission.RPCTimeout); err != nil {
			return err
		}
		if w.Response.AppliedIndex > r.RequiredAppliedIndex {
			r.RequiredAppliedIndex = w.Response.AppliedIndex
		}
		if err = mixedSameOutcome(*old.Response, *w.Response); err != nil {
			w.Outcome, w.ErrorCode = "unknown", string(public.ErrorCommitAmbiguousV1)
			w.Error = err.Error()
			return err
		}
	}
	if err = observe(&r.Admission.ReadinessAfter, 64); err != nil {
		return err
	}
	// Reconstruct from complete ACKs, not the planned postimage or retries.
	final, err = mixedPopulation(&in, r.Writes)
	if err != nil {
		return err
	}
	r.PostRecall, _, err = mixedTruthForProfile(ctx, final, r.Admission, 6, r.Profile)
	if err != nil {
		return err
	}
	r.PostRecall.Phase = "quiescent-after-mixed"
	r.PostRecall.HighestCommitIndex = r.HighestNewCommitIndex
	post, err := renewPhaseClient(ctx, &readers[0], dial)
	if err != nil {
		return err
	}
	if err = pacedRunRecall(ctx, post, final, &r.PostRecall); err != nil {
		return err
	}
	if err = observe(&r.PostRecall.ReadinessAfter, 64); err != nil {
		return err
	}
	plan, err := mixedAuditPlan(&r)
	if err != nil {
		return err
	}
	r.AuditPlan = &plan
	for _, node := range in.config.Nodes {
		call, cancel := context.WithTimeout(ctx, max(r.Admission.RPCTimeout, 2*in.config.RequestTimeout))
		audit, e := control.DiagnosticsWithColocatedAuditV1(call, node.ID, plan)
		cancel()
		r.Audits = append(r.Audits, audit)
		if e != nil {
			return e
		}
		a := audit.ColocatedAudit
		if a == nil || a.PlanSHA256 != hashJSON(plan) || a.NodeID != string(node.ID) || a.RunID != plan.RunID || a.AppliedIndex < r.RequiredAppliedIndex || len(a.Witnesses) != 6 || len(a.Final) != 4 || a.RetainedCount != 6 {
			return errors.New("incomplete all-voter audit")
		}
		if len(r.Audits) > 1 {
			first := r.Audits[0].ColocatedAudit
			if a.RetainedCount != first.RetainedCount || a.RetainedBytes != first.RetainedBytes || a.RetainedChain != first.RetainedChain {
				return errors.New("all-voter retained witness summaries disagree")
			}
		}
	}
	r.Verdict = "ACCEPT_MIXED_INVARIANT_RECALL_WINDOW_OBSERVATION_PENDING_ROOT_SHUTDOWN_VERIFICATION"
	if r.Profile == mixedProfileChangingTop10 {
		r.Verdict = "ACCEPT_MIXED_CHANGING_TOP10_RECALL_WINDOW_OBSERVATION_PENDING_ROOT_SHUTDOWN_VERIFICATION"
	}
	return ctx.Err()
}
