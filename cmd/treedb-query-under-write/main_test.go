package main

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type fakeClient struct {
	search func(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error)
	insert func(context.Context, public.InsertRequestV1) (public.InsertResponseV1, error)
}

func (f fakeClient) VectorSearchStrictV1(ctx context.Context, r public.SearchRequestV1) (public.SearchResponseV1, error) {
	return f.search(ctx, r)
}
func (f fakeClient) VectorInsertV1(ctx context.Context, r public.InsertRequestV1) (public.InsertResponseV1, error) {
	return f.insert(ctx, r)
}
func testOptions() options {
	return options{RunID: "bounded", Generation: public.GenerationIDV1{Index: "embedding_graph", Generation: 1}, BootstrapID: "bootstrap-fresh-y",
		BootstrapRevision: 1, BootstrapCommitIndex: 9, OwnerGroup: "group-1", Timeout: time.Second, RPCTimeout: time.Second}
}
func goodSearch(r public.SearchRequestV1, id string) public.SearchResponseV1 {
	return public.SearchResponseV1{Generation: r.Generation, Neighbors: []public.NeighborV1{{ID: id, Score: 1}},
		Counters: public.SearchCountersV1{SelectedPartitions: 1, HNSWServedPartitions: 1, ReadProofs: 1}}
}
func goodInsert(r public.InsertRequestV1, revision, index uint64) public.InsertResponseV1 {
	return public.InsertResponseV1{Generation: r.Generation, VisibilityGeneration: r.Generation, VisibleID: string(r.ID), OwnerGroup: "group-1",
		CommitTerm: 1, CommitIndex: index, AppliedIndex: index, ProductionConsensus: true, LiveRevision: revision,
		Counters: public.MutationCountersV1{Routes: 1, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1}}
}
func TestPlanFreezesPopulationAndRetryIdentity(t *testing.T) {
	plan, err := makePlan(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 132 {
		t.Fatalf("population=%d", len(plan))
	}
	searches, inserts := 0, 0
	seen := map[string]bool{}
	for _, op := range plan {
		if op.Outcome != "unissued" || op.RequestSHA256 == "" {
			t.Fatal("plan has mutable/attempted state")
		}
		if op.Kind == "search" {
			searches++
			continue
		}
		inserts++
		if op.Phase != "explicit-retry" {
			if seen[op.ExpectedID] {
				t.Fatal("duplicate fresh ID")
			}
			seen[op.ExpectedID] = true
			if op.InsertRequest.Vector[0] >= 0 {
				t.Fatal("fresh vector invalidates stable anchor")
			}
		}
	}
	if searches != 99 || inserts != 33 || len(seen) != 32 {
		t.Fatalf("counts search=%d insert=%d fresh=%d", searches, inserts, len(seen))
	}
	if !reflect.DeepEqual(plan[33].InsertRequest, plan[98].InsertRequest) || plan[33].RequestSHA256 != plan[98].RequestSHA256 {
		t.Fatal("explicit retry changed logical identity")
	}
	for _, id := range []string{"", "spaces forbidden", "bad/slash"} {
		o := testOptions()
		o.RunID = id
		if _, err := makePlan(o); err == nil {
			t.Fatalf("accepted run ID %q", id)
		}
	}
}
func TestWorkloadAccountsAllOperationsAndExplicitRetry(t *testing.T) {
	o := testOptions()
	plan, err := makePlan(o)
	if err != nil {
		t.Fatal(err)
	}
	writerEntered := make(chan struct{})
	readerEntered := make(chan struct{})
	var mu sync.Mutex
	searches, insertCalls, readerCalls := 0, 0, 0
	var original public.InsertRequestV1
	search := func(ctx context.Context, r public.SearchRequestV1) (public.SearchResponseV1, error) {
		mu.Lock()
		searches++
		mu.Unlock()
		id := "seed-x"
		if r.Query[0] != 1 {
			for _, op := range plan {
				if op.Phase == "post-self" && reflect.DeepEqual(op.SearchRequest.Query, r.Query) {
					id = op.ExpectedID
					break
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return public.SearchResponseV1{}, err
		}
		return goodSearch(r, id), nil
	}
	writer := fakeClient{search: search, insert: func(ctx context.Context, r public.InsertRequestV1) (public.InsertResponseV1, error) {
		insertCalls++
		if insertCalls == 1 {
			close(writerEntered)
			select {
			case <-readerEntered:
			case <-ctx.Done():
				return public.InsertResponseV1{}, ctx.Err()
			}
		}
		if insertCalls == 32 {
			original = r
		}
		revision := uint64(insertCalls + 1)
		if insertCalls == 33 {
			compare := r
			compare.Deadline = original.Deadline
			if !reflect.DeepEqual(compare, original) {
				t.Error("runtime explicit retry changed request identity")
			}
			revision = 33
		}
		return goodInsert(r, revision, uint64(9+insertCalls)), nil
	}}
	reader := fakeClient{search: func(ctx context.Context, r public.SearchRequestV1) (public.SearchResponseV1, error) {
		readerCalls++
		if readerCalls == 2 {
			select {
			case <-writerEntered:
			case <-ctx.Done():
				return public.SearchResponseV1{}, ctx.Err()
			}
		}
		// Second concurrent read starts only after the first read's complete
		// execute record, so releasing the writer here proves completion overlap.
		if readerCalls == 3 {
			close(readerEntered)
		}
		return search(ctx, r)
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var result report
	if err := runWorkload(ctx, writer, reader, o, &result); err != nil {
		t.Fatal(err)
	}
	if searches != 99 || insertCalls != 33 || result.Counts != (counts{Planned: 132, Attempted: 132, Succeeded: 132}) {
		t.Fatalf("search=%d inserts=%d counts=%+v", searches, insertCalls, result.Counts)
	}
	if len(result.Overlaps) == 0 || result.HighestCommitIndex != 42 || result.Verdict != "WORKLOAD_PROVED_PENDING_ALL_VOTER_READINESS" {
		t.Fatalf("result=%+v", result)
	}
}
func TestAmbiguousMutationStopsWritesAndPreservesUnknown(t *testing.T) {
	calls := 0
	client := fakeClient{search: func(ctx context.Context, r public.SearchRequestV1) (public.SearchResponseV1, error) {
		if err := ctx.Err(); err != nil {
			return public.SearchResponseV1{}, err
		}
		return goodSearch(r, "seed-x"), nil
	}, insert: func(ctx context.Context, r public.InsertRequestV1) (public.InsertResponseV1, error) {
		calls++
		return goodInsert(r, 2, 10), &public.ErrorV1{Code: public.ErrorCommitAmbiguousV1, Err: errors.New("visible commit but proof lost")}
	}}
	var result report
	if err := runWorkload(context.Background(), client, client, testOptions(), &result); err == nil {
		t.Fatal("accepted ambiguous insert")
	}
	if calls != 1 || result.Counts.Unknown != 1 || result.Counts.Unissued < 64 ||
		result.Operations[2].ErrorCode != string(public.ErrorCommitAmbiguousV1) || result.Operations[2].InsertResponse.CommitIndex != 10 {
		t.Fatalf("lost unknown/partial evidence: calls=%d counts=%+v op=%+v", calls, result.Counts, result.Operations[2])
	}
	if result.Counts.Planned != result.Counts.Attempted+result.Counts.Unissued ||
		result.Counts.Attempted != result.Counts.Succeeded+result.Counts.Failed+result.Counts.Canceled+result.Counts.Unknown {
		t.Fatal("failed population does not reconcile")
	}
}
func TestFreshRevisionFailureStopsBeforeNextInsert(t *testing.T) {
	calls := 0
	client := fakeClient{search: func(_ context.Context, r public.SearchRequestV1) (public.SearchResponseV1, error) {
		return goodSearch(r, "seed-x"), nil
	},
		insert: func(_ context.Context, r public.InsertRequestV1) (public.InsertResponseV1, error) {
			calls++
			return goodInsert(r, 8, 10), nil
		}}
	var result report
	if err := runWorkload(context.Background(), client, client, testOptions(), &result); err == nil {
		t.Fatal("accepted an unexplained live revision")
	}
	if calls != 1 || result.Operations[2].Outcome != "unknown" || result.Counts.Unknown != 1 {
		t.Fatalf("calls=%d result=%+v", calls, result.Counts)
	}
}
func TestCanceledCheckpointKeepsEntireUnissuedPopulation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := fakeClient{search: func(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error) {
		t.Fatal("called canceled client")
		return public.SearchResponseV1{}, nil
	}}
	var result report
	if err := runWorkload(ctx, client, client, testOptions(), &result); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if result.Counts != (counts{Planned: 132, Unissued: 132}) {
		t.Fatalf("counts=%+v", result.Counts)
	}
}
func TestOverlapRequiresSuccessfulConcurrentSearch(t *testing.T) {
	base := []operation{
		{Ordinal: 1, Kind: "insert", Phase: "concurrent", Outcome: "succeeded", StartNS: 10, EndNS: 20},
		{Ordinal: 2, Kind: "search", Phase: "concurrent", Outcome: "succeeded", StartNS: 21, EndNS: 25},
	}
	result := report{Operations: base}
	finish(&result)
	if len(result.Overlaps) != 0 {
		t.Fatal("post-write query counted as overlapping")
	}
	result.Operations[1].StartNS = 15
	finish(&result)
	if len(result.Overlaps) != 1 || result.Overlaps[0].SearchFinishedBeforeInsert {
		t.Fatal("overlap classified incorrectly")
	}
	result.Operations[1].EndNS = 18
	finish(&result)
	if !result.Overlaps[0].SearchFinishedBeforeInsert {
		t.Fatal("lost completion during outstanding write")
	}
	result.Operations[1].Outcome = "failed"
	finish(&result)
	if len(result.Overlaps) != 0 {
		t.Fatal("failed search counted as proof")
	}
}
func TestOracleRejectsFallbackAndInvalidNativeProof(t *testing.T) {
	plan, err := makePlan(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	request := *plan[0].SearchRequest
	for _, change := range []func(*public.SearchResponseV1){
		func(r *public.SearchResponseV1) { r.Counters.ExactScanPartitions = 1 },
		func(r *public.SearchResponseV1) { r.Counters.HNSWServedPartitions = 0 },
		func(r *public.SearchResponseV1) { r.Counters.ReadProofs = 0 },
		func(r *public.SearchResponseV1) { r.Neighbors[0].ID = "wrong" },
		func(r *public.SearchResponseV1) { r.Generation.Generation++ },
	} {
		response := goodSearch(request, "seed-x")
		change(&response)
		if err := validateSearch(request, response, "seed-x"); err == nil {
			t.Fatal("accepted invalid oracle/proof")
		}
	}
}

func TestMalformedMutationResponseStaysUnknown(t *testing.T) {
	outcome, code := errorOutcome(&public.ErrorV1{Code: public.ErrorInvalidRequestV1, Err: errors.New("vector insert response does not match request")}, true)
	if outcome != "unknown" || code != string(public.ErrorInvalidRequestV1) {
		t.Fatalf("outcome=%s code=%s", outcome, code)
	}
}

func TestCancellationJoinsBothOutstandingClients(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	writerEntered := make(chan struct{})
	readerCalls := 0
	writer := fakeClient{
		search: func(_ context.Context, r public.SearchRequestV1) (public.SearchResponseV1, error) {
			return goodSearch(r, "seed-x"), nil
		},
		insert: func(call context.Context, _ public.InsertRequestV1) (public.InsertResponseV1, error) {
			close(writerEntered)
			<-call.Done()
			return public.InsertResponseV1{}, call.Err()
		},
	}
	reader := fakeClient{search: func(call context.Context, r public.SearchRequestV1) (public.SearchResponseV1, error) {
		readerCalls++
		if readerCalls == 1 {
			return goodSearch(r, "seed-x"), nil
		}
		select {
		case <-writerEntered:
		case <-call.Done():
			return public.SearchResponseV1{}, call.Err()
		}
		cancel()
		<-call.Done()
		return public.SearchResponseV1{}, call.Err()
	}}
	var result report
	if err := runWorkload(ctx, writer, reader, testOptions(), &result); err == nil {
		t.Fatal("accepted canceled outstanding operations")
	}
	want := counts{Planned: 132, Attempted: 4, Succeeded: 2, Canceled: 1, Unknown: 1, Unissued: 128}
	if result.Counts != want {
		t.Fatalf("counts=%+v want=%+v", result.Counts, want)
	}
}

func TestBootstrapAdmissionBindsOwnerBeforeNetwork(t *testing.T) {
	o := testOptions()
	config := nativewire.FixedPeerTCPConfigV1{
		Credentials:          &nativewire.PeerCredentialsV1{},
		Nodes:                make([]nativewire.FixedPeerTCPNodeV1, 4),
		Groups:               []nativewire.FixedPeerTCPGroupV1{{ID: "group-1"}},
		VectorInitialization: &nativewire.FixedPeerTCPVectorInitializationV1{SourceGroupID: "group-1", Generation: 1, MaxSourceRows: 3},
	}
	config.VectorInitialization.IndexDefinition.Name = o.Generation.Index
	config.VectorInitialization.IndexDefinition.Field = "embedding"
	config.VectorInitialization.IndexDefinition.Dimensions = 2
	request := public.InsertRequestV1{Generation: o.Generation, ID: []byte(o.BootstrapID)}
	bootstrap := nativewire.FixedPeerVectorQualificationV1{
		Insert: goodInsert(request, 1, 8), Retry: goodInsert(request, 1, 9),
		Before:    goodSearch(public.SearchRequestV1{Generation: o.Generation}, "seed-x"),
		After:     goodSearch(public.SearchRequestV1{Generation: o.Generation}, o.BootstrapID),
		Readiness: make([]nativewire.FixedPeerReadinessV1, 4),
	}
	// This pure admission function is called by runArgs before planning/control
	// construction or either DialContext. No transport is created for this test.
	if generation, err := admitBootstrap(config, bootstrap); err != nil || generation != o.Generation {
		t.Fatalf("matching owner admission generation=%+v err=%v", generation, err)
	}
	alien := bootstrap
	alien.Insert.OwnerGroup = "alien"
	alien.Retry.OwnerGroup = "alien"
	if err := public.ValidateInsertResponseV1(request, alien.Insert); err != nil {
		t.Fatal(err)
	}
	if err := public.ValidateInsertResponseV1(request, alien.Retry); err != nil {
		t.Fatal(err)
	}
	if _, err := admitBootstrap(config, alien); err == nil {
		t.Fatal("admitted unrelated but structurally valid bootstrap owner")
	}
	config.VectorInitialization.SourceGroupID = "alien"
	if _, err := admitBootstrap(config, bootstrap); err == nil {
		t.Fatal("admitted owner differing from initialization source group")
	}
	config.VectorInitialization.SourceGroupID = "group-1"
	config.Groups[0].ID = "alien"
	if _, err := admitBootstrap(config, bootstrap); err == nil {
		t.Fatal("admitted owner differing from configured data group")
	}
}
func TestExplicitRetrySplitProofStaysUnknownBeforePostSearch(t *testing.T) {
	o := testOptions()
	plan, err := makePlan(o)
	if err != nil {
		t.Fatal(err)
	}
	original := *plan[33].InsertRequest
	split := goodInsert(original, 33, 42)
	split.VisibilityToken = []byte("opaque-split-token")
	split.Counters.VisibilityProofs = 2
	if err := public.ValidateInsertResponseV1(original, split); err != nil {
		t.Fatalf("witness must pass shared split-aware validator: %v", err)
	}
	inserts := 0
	writer := fakeClient{
		search: func(_ context.Context, r public.SearchRequestV1) (public.SearchResponseV1, error) {
			return goodSearch(r, "seed-x"), nil
		},
		insert: func(_ context.Context, r public.InsertRequestV1) (public.InsertResponseV1, error) {
			inserts++
			if inserts == 33 {
				return split, nil
			}
			return goodInsert(r, uint64(inserts+1), uint64(inserts+9)), nil
		},
	}
	reader := fakeClient{search: func(_ context.Context, r public.SearchRequestV1) (public.SearchResponseV1, error) {
		return goodSearch(r, "seed-x"), nil
	}}
	var result report
	if err := runWorkload(context.Background(), writer, reader, o, &result); err == nil {
		t.Fatal("accepted split-shaped explicit retry")
	}
	retry := result.Operations[98]
	if inserts != 33 || retry.Outcome != "unknown" || retry.ErrorCode != string(public.ErrorCommitAmbiguousV1) ||
		retry.InsertResponse == nil || len(retry.InsertResponse.VisibilityToken) == 0 || result.Counts.Unknown != 1 {
		t.Fatalf("lost split retry proof: inserts=%d counts=%+v retry=%+v", inserts, result.Counts, retry)
	}
	for _, op := range result.Operations[99:] {
		if op.Outcome != "unissued" {
			t.Fatalf("post-retry search was issued: %+v", op)
		}
	}
	if result.Counts != (counts{Planned: 132, Attempted: 99, Succeeded: 98, Unknown: 1, Unissued: 33}) {
		t.Fatalf("counts=%+v", result.Counts)
	}
}

func TestDatasetProbePlanMoreThan64OrdinaryIDs(t *testing.T) {
	o := testOptions()
	o.Dimensions = 128
	o.FreshInserts = 65
	o.EfSearch = 128
	plan, err := makePlan(o)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 198 {
		t.Fatalf("population=%d", len(plan))
	}
	ids := map[string]bool{}
	for _, op := range plan {
		if op.InsertRequest != nil && op.Phase != "explicit-retry" {
			ids[op.ExpectedID] = true
			if len(op.InsertRequest.Vector) != 128 {
				t.Fatal("dimension drift")
			}
		}
		if op.SearchRequest != nil && (len(op.SearchRequest.Query) != 128 || op.SearchRequest.EfSearch != 128) {
			t.Fatal("search config drift")
		}
	}
	if len(ids) != 65 || !reflect.DeepEqual(plan[66].InsertRequest, plan[131].InsertRequest) {
		t.Fatal("ordinary population/retry identity drift")
	}
	o.FreshInserts = 66
	if _, err := makePlan(o); err == nil {
		t.Fatal("admitted excessive probe")
	}
}

func TestReadinessRetainsErrorsAndUsesRemainingRounds(t *testing.T) {
	config := nativewire.FixedPeerTCPConfigV1{Nodes: []nativewire.FixedPeerTCPNodeV1{{ID: "node"}}, Groups: []nativewire.FixedPeerTCPGroupV1{{ID: "group"}}}
	for _, mode := range []string{"transient", "exhausted", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			r := report{RPCTimeout: time.Second}
			read := func(_ context.Context, node nativewire.FixedPeerTCPNodeV1) (nativewire.FixedPeerReadinessV1, error) {
				calls++
				if calls == 1 || mode != "transient" {
					if mode == "canceled" {
						cancel()
					}
					return nativewire.FixedPeerReadinessV1{}, errors.New("transient observation failure")
				}
				state := nativewire.FixedPeerReadinessV1{NodeID: node.ID, Live: true, Ready: true, VectorPhase: "active"}
				state.Groups = append(state.Groups, nativewire.FixedPeerGroupReadinessV1{GroupID: config.Groups[0].ID, Ready: true, LocalAppliedIndex: 42})
				return state, nil
			}
			err := readinessWith(ctx, read, config, &r, 42, 2)
			if len(r.Readiness) != calls || r.Readiness[0].Error == "" {
				t.Fatal("lost failed observation")
			}
			if mode == "transient" {
				if err != nil || calls != 2 {
					t.Fatalf("calls=%d err=%v", calls, err)
				}
			} else if mode == "exhausted" {
				if err == nil || calls != 2 {
					t.Fatalf("calls=%d err=%v", calls, err)
				}
			} else if !errors.Is(err, context.Canceled) || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}
