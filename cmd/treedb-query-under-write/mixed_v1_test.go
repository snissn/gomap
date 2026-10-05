package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type mixedFakeV1 struct {
	calls    int
	onCall   func()
	response public.MutationResponseV1
	err      error
}

func (f *mixedFakeV1) VectorReplaceV1(context.Context, public.ReplaceRequestV1) (public.MutationResponseV1, error) {
	f.calls++
	if f.onCall != nil {
		f.onCall()
	}
	return f.response, f.err
}
func (f *mixedFakeV1) VectorDeleteV1(context.Context, public.DeleteRequestV1) (public.MutationResponseV1, error) {
	f.calls++
	if f.onCall != nil {
		f.onCall()
	}
	return f.response, f.err
}
func TestMixedOriginalOutcomeAndFailureV1(t *testing.T) {
	g := public.GenerationIDV1{Index: "embedding", Generation: 1}
	original := public.MutationResponseV1{Generation: g, OwnerGroup: "g", CommitTerm: 2, CommitIndex: 7, AppliedIndex: 7, ProductionConsensus: true, Coverage: 9, LiveRevision: 4, Matched: 1, Modified: 1, VisibilityToken: []byte("original"), Counters: public.MutationCountersV1{Routes: 1, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1}}
	retry := original
	retry.AppliedIndex = 12
	retry.Counters.Forwards = 1
	if err := mixedSameOutcome(original, retry); err != nil {
		t.Fatal(err)
	}
	if err := public.ValidateMutationResponseV1(g, false, retry); err != nil {
		t.Fatal(err)
	}
	retry.Modified = 0
	if mixedSameOutcome(original, retry) == nil {
		t.Fatal("accepted changed original outcome count")
	}
	retry.Modified = original.Modified
	retry.CommitIndex++
	if mixedSameOutcome(original, retry) == nil {
		t.Fatal("accepted new outcome position")
	}
	f := &mixedFakeV1{err: &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: errors.New("unknown")}}
	w := mixedWrite{Kind: "replace", Replace: &public.ReplaceRequestV1{Version: 1, Generation: g, ID: []byte("id"), IdempotencyKey: []byte("key"), Document: []byte(`{"embedding":[1,0]}`), Vector: []float32{1, 0}}, Outcome: "unissued"}
	if mixedCall(context.Background(), f, &w, time.Now(), time.Second) == nil || f.calls != 1 || w.Outcome != "unknown" {
		t.Fatalf("unknown was not consumed: %+v calls%d", w, f.calls)
	}
	f.response = original
	f.err = nil
	if err := mixedCall(context.Background(), f, &w, time.Now(), time.Second); err != nil {
		t.Fatal(err)
	}
	f.response.VisibilityToken[0] = 'X'
	if string(w.Response.VisibilityToken) != "original" {
		t.Fatal("response aliases client token")
	}
}

func TestMixedFullPrefixTruthAndSupersessionV1(t *testing.T) {
	in, admission := recallTestQueries(t)
	// Add a small tail below all sixteen top10s; use the established scorer and
	// exported-corpus oracle rather than canned result responses.
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("tail-%d", i)
		in.corpusIDs = append(in.corpusIDs, id)
		in.vectors[id] = oracleVector(128, -1, float32(i+1))
	}
	r := mixedReport{windowReport: windowReport{Admission: admission}, PaceInterval: time.Second}
	final, err := mixedPlan(context.Background(), &in, &r)
	if err != nil || len(r.Writes) != 6 || len(r.Prefixes) != 7 {
		t.Fatalf("plan %+v err%v", r.Prefixes, err)
	}
	pre := pacedRecallCopy(r.Admission, "quiescent-before-mixed")
	post, _, err := mixedTruth(context.Background(), final, r.Admission, 6)
	if err != nil || r.Admission.Verdict != "ACCEPTED_INPUTS_INVARIANT_PENDING_RUNTIME" || pre.Verdict != r.Admission.Verdict || post.Verdict != r.Admission.Verdict {
		t.Fatalf("missing nested input verdict: admission%q pre%q post%q err%v", r.Admission.Verdict, pre.Verdict, post.Verdict, err)
	}
	if len(final.vectors) != len(in.vectors)-3 || len(in.vectors) != 17 {
		t.Fatalf("population ownership final%d base%d", len(final.vectors), len(in.vectors))
	}
	id := string(r.Writes[0].Replace.ID)
	if !slices.Equal(final.vectors[id], r.Writes[1].Replace.Vector) || bytes.Equal(r.Writes[0].Replace.Document, r.Writes[1].Replace.Document) {
		t.Fatal("supersession did not preserve final content")
	}
	for _, p := range r.Prefixes {
		if p.Top10SHA256 != r.Prefixes[0].Top10SHA256 {
			t.Fatal("planned top10 changed")
		}
		for qi, truth := range p.Truth {
			if &truth[0] != &admission.Queries[qi].Truth[0] {
				t.Fatal("proved invariant truth did not reuse immutable admission row")
			}
		}
	}
	changed := r.Writes[0]
	copyRequest := *changed.Replace
	copyRequest.Vector = oracleVector(128, 1, 0)
	changed.Replace = &copyRequest
	changedPopulation, err := mixedPopulation(&in, []mixedWrite{changed})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = mixedTruth(context.Background(), changedPopulation, admission, 1); err == nil {
		t.Fatal("admitted changing canonical top10")
	}
	if _, err := mixedPopulation(&in, []mixedWrite{r.Writes[2], r.Writes[2]}); err == nil {
		t.Fatal("admitted deletion of already missing ID")
	}
}

func TestMixedFinalReadinessRoundRetainsFollowerLagV1(t *testing.T) {
	in, _ := recallTestInput()
	r := report{RPCTimeout: time.Second}
	calls := 0
	read := func(_ context.Context, node nativewire.FixedPeerTCPNodeV1) (nativewire.FixedPeerReadinessV1, error) {
		calls++
		if calls == 4 {
			return nativewire.FixedPeerReadinessV1{}, errors.New("follower has not applied the retry floor")
		}
		return nativewire.FixedPeerReadinessV1{NodeID: node.ID, Live: true, Ready: true, VectorPhase: "active", CatalogEpoch: 1,
			Groups: []nativewire.FixedPeerGroupReadinessV1{{GroupID: "group-1", Ready: true, LocalAppliedIndex: 42}}}, nil
	}
	if err := readinessWith(context.Background(), read, in.config, &r, 42, 2); err != nil {
		t.Fatal(err)
	}
	if calls != 8 || len(r.Readiness) != 8 || r.Readiness[3].Error == "" {
		t.Fatalf("lost the first failed round: calls%d history%+v", calls, r.Readiness)
	}
	if err := mixedReadyStates(in.config, r.Readiness, 42); err != nil {
		t.Fatal(err)
	}
	if mixedReadyStates(in.config, r.Readiness[:3], 42) == nil || mixedReadyStates(in.config, r.Readiness, 43) == nil {
		t.Fatal("accepted incomplete or lagging final round")
	}
}

func BenchmarkMixedRetainedCallV1(b *testing.B) {
	g := public.GenerationIDV1{Index: "embedding", Generation: 1}
	f := &mixedFakeV1{response: public.MutationResponseV1{Generation: g, OwnerGroup: "g", CommitTerm: 1, CommitIndex: 1, AppliedIndex: 1, ProductionConsensus: true, Coverage: 1, LiveRevision: 1, Matched: 1, Modified: 1, VisibilityToken: []byte("token"), Counters: public.MutationCountersV1{Routes: 1, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1}}}
	request := public.ReplaceRequestV1{Version: 1, Generation: g, ID: []byte("id"), IdempotencyKey: []byte("key"), Vector: []float32{1, 0}, Document: []byte(`{"embedding":[1,0]}`)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := mixedWrite{Kind: "replace", Replace: &request}
		if err := mixedCall(context.Background(), f, &w, time.Now(), time.Second); err != nil {
			b.Fatal(err)
		}
	}
}

func mixedTestPlanV1(t testing.TB) (recallInput, mixedReport) {
	t.Helper()
	in, admission := recallTestQueries(t)
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("tail-%d", i)
		in.corpusIDs = append(in.corpusIDs, id)
		in.vectors[id] = oracleVector(128, -1, float32(i+1))
	}
	r := mixedReport{windowReport: windowReport{Admission: admission, RequestedDuration: time.Second, MaxAttempts: 65536, WarmupPlanned: 64, Concurrency: 1, OutputBytes: 128 << 20}, PaceInterval: time.Second}
	if _, err := mixedPlan(context.Background(), &in, &r); err != nil {
		t.Fatal(err)
	}
	return in, r
}
func TestMixedSinglePrefixCanonicalRecallV1(t *testing.T) {
	in, r := mixedTestPlanV1(t)
	q := r.Admission.Queries[0]
	response := windowTestResponse(&r.Admission)
	add := func(p int, id string) public.NeighborV1 {
		for _, v := range r.Prefixes[p].Changed[0] {
			if v.ID == id && v.Present {
				return public.NeighborV1{ID: id, Score: math.Float32frombits(v.ScoreBits)}
			}
		}
		t.Fatal("missing oracle score")
		return public.NeighborV1{}
	}
	a, c := string(r.Writes[0].Replace.ID), string(r.Writes[3].Replace.ID)
	response.Neighbors = append(response.Neighbors[:9], add(1, a))
	sort.Slice(response.Neighbors, func(i, j int) bool { return recallLess(response.Neighbors[i], response.Neighbors[j]) })
	if value, err := mixedValidatePrefix(&q, response, &in, &r, 1, 1); err != nil || value != .9 {
		t.Fatalf("approximate canonical response must retain recall value: %v %v", value, err)
	}
	for _, bounds := range [][2]int{{0, 0}, {2, 6}} {
		if _, err := mixedValidatePrefix(&q, response, &in, &r, bounds[0], bounds[1]); err == nil {
			t.Fatalf("stale/future prefix accepted %v", bounds)
		}
	}
	response.Neighbors = append(append([]public.NeighborV1(nil), q.Truth[:8]...), add(1, a), add(4, c))
	sort.Slice(response.Neighbors, func(i, j int) bool { return recallLess(response.Neighbors[i], response.Neighbors[j]) })
	if _, err := mixedValidatePrefix(&q, response, &in, &r, 0, 6); err == nil {
		t.Fatal("mixed two different postimages into one response")
	}
	response = windowTestResponse(&r.Admission)
	deleted := string(r.Writes[2].Delete.ID)
	response.Neighbors = append(response.Neighbors[:9], add(2, deleted))
	sort.Slice(response.Neighbors, func(i, j int) bool { return recallLess(response.Neighbors[i], response.Neighbors[j]) })
	if _, err := mixedValidatePrefix(&q, response, &in, &r, 3, 6); err == nil {
		t.Fatal("deleted ID accepted after ACK floor")
	}
}

func mixedAmbiguousFixtureV2(t testing.TB) (recallInput, mixedReport, public.SearchResponseV1, *recallInput) {
	t.Helper()
	in, r := mixedTestPlanV1(t)
	q := r.Admission.Queries[0]
	response := windowTestResponse(&r.Admission)
	// Move one existing tail into the top10. The response omits that ID, so
	// its complete contents remain canonical before and after the replacement;
	// those indistinguishable populations nevertheless have different truths.
	w := r.Writes[0]
	request := *w.Replace
	request.Vector = append([]float32(nil), q.Request.Query...)
	w.Replace = &request
	r.Writes[0] = w
	state, err := mixedPopulation(&in, []mixedWrite{w})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(state.vectors))
	for id := range state.vectors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	r.Prefixes = r.Prefixes[:2]
	for qi, query := range r.Admission.Queries {
		truth, err := recallTop10(context.Background(), query.scorer, ids, state.vectors)
		if err != nil {
			t.Fatal(err)
		}
		r.Prefixes[1].Truth[qi] = truth
		for i := range r.Prefixes[1].Changed[qi] {
			changed := &r.Prefixes[1].Changed[qi][i]
			if changed.ID == string(request.ID) {
				score, err := query.scorer.ScoreV1(request.Vector)
				if err != nil {
					t.Fatal(err)
				}
				changed.ScoreBits = math.Float32bits(score)
			}
		}
	}
	if pacedSameTruth(r.Prefixes[0].Truth[0], r.Prefixes[1].Truth[0]) {
		t.Fatal("fixture failed to change full canonical top10")
	}
	if r.Profile != "" || &r.Prefixes[0].Truth[0][0] == &r.Prefixes[1].Truth[0][0] {
		t.Fatal("distinct changed truth must bypass reuse even with omitted profile")
	}
	return in, r, response, state
}

func TestMixedChangingTop10ConservativeCompatibleRecallV2(t *testing.T) {
	in, r, response, state := mixedAmbiguousFixtureV2(t)
	exportedBefore := hashJSON(in.exported)
	q := r.Admission.Queries[0]
	for _, tc := range []struct {
		name         string
		lower, upper int
		want         float64
	}{{"baseline-only", 0, 0, 1}, {"changed-only", 1, 1, .9}, {"ambiguous-minimum", 0, 1, .9}} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := mixedValidatePrefix(&q, response, &in, &r, tc.lower, tc.upper)
			if err != nil || value != tc.want {
				t.Fatalf("recall=%v want%v for compatible prefixes%d..%d: %v", value, tc.want, tc.lower, tc.upper, err)
			}
		})
	}
	baseline := pacedRecallCopy(r.Admission, "baseline-integrity")
	if err := recallPlan(context.Background(), &in, &baseline); err != nil {
		t.Fatal("unchanged exported baseline rejected:", err)
	}
	changed := pacedRecallCopy(r.Admission, "changed-baseline-refusal")
	if err := recallPlan(context.Background(), state, &changed); err == nil {
		t.Fatal("ordinary admission accepted a changed exported corpus top10")
	}
	if hashJSON(in.exported) != exportedBefore {
		t.Fatal("dynamic oracle setup mutated immutable exported truth")
	}
}
func TestMixedUnknownStopsAndRetainsUnissuedV1(t *testing.T) {
	for _, originals := range []int{6, 58} {
		for _, concurrency := range []int{1, 4} {
			t.Run(fmt.Sprintf("originals%d-readers%d", originals, concurrency), func(t *testing.T) {
				in, r := mixedTestPlanV1(t)
				if originals > 6 {
					in, r = mixedSustainedTestPlanV1(t, originals)
				}
				r.Concurrency = concurrency
				r.Admission.RPCTimeout = 100 * time.Millisecond
				r.PaceInterval = time.Millisecond
				for i := range r.Writes {
					r.Writes[i].IntendedOffsetNS = int64(i) * int64(time.Millisecond)
				}
				entered := make(chan struct{})
				readers := make([]ownedVectorClient, concurrency)
				for i := range readers {
					readers[i] = &windowTestClient{fakeClient: fakeClient{search: func(ctx context.Context, _ public.SearchRequestV1) (public.SearchResponseV1, error) {
						<-entered
						<-ctx.Done()
						return public.SearchResponseV1{}, ctx.Err()
					}}}
				}
				writer := &mixedFakeV1{onCall: func() { close(entered) }, err: &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: errors.New("original unknown")}}
				proof := fakeClient{search: func(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error) {
					t.Error("visibility after UNKNOWN")
					return public.SearchResponseV1{}, errors.New("unexpected")
				}}
				budget := 128 << 20
				if err := mixedMeasured(context.Background(), readers, writer, proof, &in, &r, &budget); err == nil {
					t.Fatal("unknown accepted")
				}
				mixedSummary(&r)
				if writer.calls != 1 || r.WriteCounts.Unknown != 1 || r.WriteCounts.Unissued != originals-1 || len(r.Retries) != 0 {
					t.Fatalf("unknown campaign not consumed %+v calls%d", r.WriteCounts, writer.calls)
				}
				if _, err := mixedAuditPlan(&r); err == nil {
					t.Fatal("incomplete ACK ledger audited")
				}
			})
		}
	}
}
func TestMixedFinalSlotHeadroomV1(t *testing.T) {
	base := mixedOptions{Window: windowOptions{Admission: recallOptions{Timeout: 120 * time.Second, RPCTimeout: 10 * time.Second}, Concurrency: 1, Warmup: 64, MaxAttempts: 65536, OutputBytes: 128 << 20, Duration: time.Minute}, Interval: 5 * time.Second}
	for _, tc := range []struct {
		interval, rpc time.Duration
		wantOK        bool
	}{{5 * time.Second, 10 * time.Second, true}, {8 * time.Second, 9 * time.Second, true}, {8 * time.Second, 10 * time.Second, false}, {8 * time.Second, 11 * time.Second, false}, {5 * time.Second, 17500 * time.Millisecond, false}} {
		o := base
		o.Interval, o.Window.Admission.RPCTimeout = tc.interval, tc.rpc
		if err := mixedValidate(o); (err == nil) != tc.wantOK {
			t.Fatalf("interval%v rpc%v: %v", tc.interval, tc.rpc, err)
		}
	}
	var out bytes.Buffer
	err := runArgs(context.Background(), []string{"-mode", "mixed-window", "-mixed-interval", "8s", "-read-concurrency", "1"}, &out)
	if err == nil || !strings.Contains(err.Error(), "positive headroom") {
		t.Fatalf("schedule must refuse before input or network access: %v", err)
	}
}

func TestMixedModeAdmissionAndCanceledNoCallV1(t *testing.T) {
	for _, args := range [][]string{{"-mode", "read-window", "-mixed-interval", "1s"}, {"-mode", "mixed-window", "-paced-inserts", "6"}, {"-mode", "mixed-window", "-fresh-inserts", "1"}, {"-mode", "mixed-window", "-read-window", "1s"}, {"-mode", "mixed-window", "-mixed-interval", "9s"}} {
		var out bytes.Buffer
		if runArgs(context.Background(), args, &out) == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := &mixedFakeV1{}
	w := mixedWrite{Kind: "delete", Delete: &public.DeleteRequestV1{}}
	if mixedCall(ctx, f, &w, time.Now(), time.Second) == nil || f.calls != 0 || w.Invoked {
		t.Fatal("canceled operation invoked")
	}
}

func TestMixedVisibilityFailureStopsNextSlotV1(t *testing.T) {
	in, r := mixedTestPlanV1(t)
	r.Admission.RPCTimeout = 100 * time.Millisecond
	r.PaceInterval = time.Millisecond
	for i := range r.Writes {
		r.Writes[i].IntendedOffsetNS = int64(i) * int64(time.Millisecond)
	}
	entered := make(chan struct{})
	reader := &windowTestClient{fakeClient: fakeClient{search: func(ctx context.Context, _ public.SearchRequestV1) (public.SearchResponseV1, error) {
		<-entered
		<-ctx.Done()
		return public.SearchResponseV1{}, ctx.Err()
	}}}
	writer := &mixedFakeV1{response: public.MutationResponseV1{Generation: r.Admission.Generation, OwnerGroup: in.bootstrap.Insert.OwnerGroup, CommitTerm: 1, CommitIndex: 42, AppliedIndex: 42, ProductionConsensus: true, Coverage: 1, LiveRevision: 1, Matched: 1, Modified: 1, VisibilityToken: []byte("token"), Counters: public.MutationCountersV1{Routes: 1, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1}}}
	calls := 0
	proof := fakeClient{search: func(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error) {
		calls++
		close(entered)
		return public.SearchResponseV1{}, errors.New("strict probe failed")
	}}
	budget := 128 << 20
	if mixedMeasured(context.Background(), []ownedVectorClient{reader}, writer, proof, &in, &r, &budget) == nil {
		t.Fatal("failed visibility accepted")
	}
	mixedSummary(&r)
	if writer.calls != 1 || calls != 1 || r.WriteCounts.Succeeded != 1 || r.WriteCounts.Unissued != 5 || len(r.Retries) != 0 || r.Visibility[0].Outcome == "succeeded" {
		t.Fatalf("failed visibility did not consume %+v", r.WriteCounts)
	}
}

func TestMixedChangingTop10ProfilePlanV2(t *testing.T) {
	in, initial := mixedTestPlanV1(t)
	admission := initial.Admission
	exported := hashJSON(in.exported)
	r := mixedReport{Profile: mixedProfileChangingTop10, windowReport: windowReport{Admission: admission}, PaceInterval: time.Second}
	final, err := mixedPlan(context.Background(), &in, &r)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Writes) != 6 || len(r.Prefixes) != 7 || r.Prefixes[0].Top10SHA256 == r.Prefixes[1].Top10SHA256 {
		t.Fatal("missing changed six-slot plan")
	}
	if string(r.Writes[0].Replace.ID) != in.corpusIDs[0] || string(r.Writes[2].Delete.ID) != in.corpusIDs[1] || string(r.Writes[3].Replace.ID) != in.corpusIDs[2] || string(r.Writes[5].Delete.ID) != in.corpusIDs[3] {
		t.Fatal("targets are not deterministic existing corpus IDs")
	}
	for prefix := range r.Prefixes {
		state, err := mixedPopulation(&in, r.Writes[:prefix])
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(state.vectors))
		for id := range state.vectors {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		if r.Prefixes[prefix].PopulationRows != len(ids) {
			t.Fatal("prefix population count")
		}
		for qi, q := range admission.Queries {
			truth, err := recallTop10(context.Background(), q.scorer, ids, state.vectors)
			if err != nil || !pacedSameTruth(truth, r.Prefixes[prefix].Truth[qi]) {
				t.Fatalf("full prefix%d query%d truth: %v", prefix, qi, err)
			}
		}
	}
	post, _, err := mixedTruthForProfile(context.Background(), final, admission, 6, r.Profile)
	if err != nil || post.OracleBasis != "derived-changing-prefix-full-canonical-fp32" {
		t.Fatalf("derived truth provenance: %+v %v", post, err)
	}
	if hashJSON(in.exported) != exported {
		t.Fatal("exported baseline was rewritten")
	}
	ordinary := pacedRecallCopy(admission, "ordinary")
	if recallPlan(context.Background(), final, &ordinary) == nil {
		t.Fatal("ordinary baseline admission weakened")
	}
	again := mixedReport{Profile: r.Profile, windowReport: windowReport{Admission: admission}, PaceInterval: time.Second}
	if _, err := mixedPlan(context.Background(), &in, &again); err != nil || hashJSON(again.Writes) != hashJSON(r.Writes) || hashJSON(again.Prefixes) != hashJSON(r.Prefixes) {
		t.Fatalf("nondeterministic plan: %v", err)
	}
	q := admission.Queries[0]
	response := windowTestResponse(&admission)
	response.Neighbors = r.Prefixes[1].Truth[0]
	if _, err := mixedValidatePrefix(&q, response, &in, &r, 0, 0); err == nil {
		t.Fatal("future postimage before invocation accepted")
	}
	response.Neighbors = r.Prefixes[0].Truth[0]
	if _, err := mixedValidatePrefix(&q, response, &in, &r, 3, 6); err == nil {
		t.Fatal("stale baseline after deletion ACK accepted")
	}
	response.Neighbors = append([]public.NeighborV1(nil), r.Prefixes[4].Truth[0]...)
	a := string(r.Writes[0].Replace.ID)
	for i := range response.Neighbors {
		if response.Neighbors[i].ID == a {
			for _, old := range r.Prefixes[0].Changed[0] {
				if old.ID == a {
					response.Neighbors[i].Score = math.Float32frombits(old.ScoreBits)
				}
			}
		}
	}
	sort.Slice(response.Neighbors, func(i, j int) bool { return recallLess(response.Neighbors[i], response.Neighbors[j]) })
	if _, err := mixedValidatePrefix(&q, response, &in, &r, 0, 6); err == nil {
		t.Fatal("mixed old A and replaced C postimages accepted")
	}
	response.Neighbors = r.Prefixes[6].Truth[0]
	if value, err := mixedValidatePrefix(&q, response, &in, &r, 6, 6); err != nil || value != 1 {
		t.Fatalf("final exact prefix: %v %v", value, err)
	}
	response.Counters.ExactScanPartitions = 1
	if _, err := mixedValidatePrefix(&q, response, &in, &r, 6, 6); err == nil {
		t.Fatal("fallback accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := hashJSON(again)
	if _, err := mixedPlan(ctx, &in, &again); !errors.Is(err, context.Canceled) || hashJSON(again) != before {
		t.Fatalf("canceled planning mutated output: %v", err)
	}
	var out bytes.Buffer
	for _, args := range [][]string{{"-mode", "read-window", "-mixed-profile", "changing-top10"}, {"-mode", "mixed-window", "-mixed-profile", "unknown"}} {
		if runArgs(context.Background(), args, &out) == nil {
			t.Fatalf("accepted invalid mode/profile %v", args)
		}
	}
}

func TestMixedChangingTop10PostJoinRecallV2(t *testing.T) {
	for _, tc := range []struct {
		name         string
		start, end   int64
		online, want float64
		mask         uint64
	}{{"ACK-before-call", 30, 40, 1, .9, 2}, {"not-yet-invoked", 1, 5, .9, 1, 1}, {"overlapping", 15, 25, .9, .9, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			in, r, response, _ := mixedAmbiguousFixtureV2(t)
			r.Writes = r.Writes[:1]
			r.Writes[0].Invoked, r.Writes[0].Outcome = true, "succeeded"
			r.Writes[0].StartNS, r.Writes[0].EndNS = 10, 20
			online, warmRecall := tc.online, 1.0
			r.Attempts = []windowAttempt{
				{Ordinal: 0, Phase: "warmup", Response: &response, Outcome: "succeeded", RecallAt10: &warmRecall},
				{Ordinal: 0, Phase: "measured", StartNS: tc.start, EndNS: tc.end, Response: &response, Outcome: "succeeded", RecallAt10: &online},
			}
			encodedBytes := func() int {
				total := 0
				for _, attempt := range r.Attempts {
					raw, err := json.Marshal(attempt)
					if err != nil {
						t.Fatal(err)
					}
					total += len(raw) + 1
				}
				return total
			}
			r.RetainedAttemptBytes = encodedBytes()
			collectionBytes := r.RetainedAttemptBytes
			r.Counts.Succeeded = 1
			if err := mixedRecheck(context.Background(), &in, &r); err != nil {
				t.Fatal(err)
			}
			windowSummarize(&r.windowReport)
			if r.Attempts[1].RecallAt10 != &online || online != tc.want || r.MeanRecallAt10 == nil || *r.MeanRecallAt10 != tc.want || r.MeasuredQuerySucceeded[0] != 1 {
				t.Fatalf("final recall not reaccounted: %+v", r.windowReport)
			}
			if len(r.ReadPrefixes) != 1 || r.ReadPrefixes[0].CompatibleMask != tc.mask || r.ReadPrefixes[0].RecallAt10 != tc.want {
				t.Fatalf("exact compatible mask: %+v", r.ReadPrefixes)
			}
			if r.RetainedAttemptBytes != encodedBytes() || warmRecall != 1 {
				t.Fatalf("final encoded attempt accounting: got%d want%d warmup%v", r.RetainedAttemptBytes, encodedBytes(), warmRecall)
			}
			wantDelta := 0
			if tc.online == 1 && tc.want == .9 {
				wantDelta = 2
			}
			if tc.online == .9 && tc.want == 1 {
				wantDelta = -2
			}
			if r.RetainedAttemptBytes-collectionBytes != wantDelta {
				t.Fatalf("recall encoding delta: got%d want%d", r.RetainedAttemptBytes-collectionBytes, wantDelta)
			}
			// A later invalid attempt must still fail, retain all evidence, and
			// account an earlier scalar update performed before that refusal.
			online = tc.online
			r.Attempts = append(r.Attempts, windowAttempt{Ordinal: 1, Phase: "measured", Outcome: "failed"})
			r.RetainedAttemptBytes = encodedBytes()
			if err := mixedRecheck(context.Background(), &in, &r); err == nil || len(r.Attempts) != 3 || r.Attempts[2].Outcome != "failed" || r.RetainedAttemptBytes != encodedBytes() {
				t.Fatalf("failed recheck evidence/accounting: %v bytes%d want%d", err, r.RetainedAttemptBytes, encodedBytes())
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := mixedRecheck(ctx, &in, &r); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled final proof: %v", err)
			}
		})
	}
	raw, err := json.Marshal(mixedReadPrefix{Ordinal: 65535, Lower: 0, Upper: 6, Matched: -1, CompatibleMask: 127, RecallAt10: .9})
	if err != nil || len(raw)+1 > mixedReadPrefixMaxBytes {
		t.Fatalf("prefix receipt exceeds reserved bound: %d %v", len(raw), err)
	}
}

// Admission/full-population oracle setup is excluded. Both cases validate ten
// retained neighbors against all seven possible prefixes with prepared scorers.
func BenchmarkMixedPrefixValidationV2(b *testing.B) {
	for _, profile := range []string{"invariant", mixedProfileChangingTop10} {
		b.Run(profile, func(b *testing.B) {
			in, r := mixedTestPlanV1(b)
			if profile == mixedProfileChangingTop10 {
				r.Writes, r.Prefixes, r.Profile = nil, nil, profile
				if _, err := mixedPlan(context.Background(), &in, &r); err != nil {
					b.Fatal(err)
				}
			}
			q := r.Admission.Queries[0]
			response := windowTestResponse(&r.Admission)
			// Exclude every mutable target so all prefixes are compatible. This is an
			// approximate response, not a forced exact-search or favorable-prefix gate.
			ids := make([]string, 0, len(in.vectors))
			vectors := make(map[string][]float32)
			for id, v := range in.vectors {
				changed := false
				for _, c := range r.Prefixes[0].Changed[0] {
					changed = changed || id == c.ID
				}
				if !changed {
					ids = append(ids, id)
					vectors[id] = v
				}
			}
			sort.Strings(ids)
			var err error
			response.Neighbors, err = recallTop10(context.Background(), q.scorer, ids, vectors)
			if err != nil {
				b.Fatal(err)
			}
			proof, err := mixedValidatePrefixProof(&q, response, &in, &r, 0, 6)
			if err != nil || proof.CompatibleMask != 127 {
				b.Fatalf("not all prefixes compatible: %+v %v", proof, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := mixedValidatePrefixProof(&q, response, &in, &r, 0, 6); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
