package main

import (
	"bytes"
	"context"
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

func mixedTestPlanV1(t *testing.T) (recallInput, mixedReport) {
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
func TestMixedUnknownStopsAndRetainsUnissuedV1(t *testing.T) {
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
	writer := &mixedFakeV1{onCall: func() { close(entered) }, err: &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: errors.New("original unknown")}}
	proof := fakeClient{search: func(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error) {
		t.Error("visibility after UNKNOWN")
		return public.SearchResponseV1{}, errors.New("unexpected")
	}}
	budget := 128 << 20
	if err := mixedMeasured(context.Background(), []ownedVectorClient{reader}, writer, proof, &in, &r, &budget); err == nil {
		t.Fatal("unknown accepted")
	}
	mixedSummary(&r)
	if writer.calls != 1 || r.WriteCounts.Unknown != 1 || r.WriteCounts.Unissued != 5 || len(r.Retries) != 0 {
		t.Fatalf("unknown campaign not consumed %+v calls%d", r.WriteCounts, writer.calls)
	}
	if _, err := mixedAuditPlan(&r); err == nil {
		t.Fatal("incomplete ACK ledger audited")
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
