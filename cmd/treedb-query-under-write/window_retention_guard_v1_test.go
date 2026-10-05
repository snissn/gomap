package main

import (
	"context"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// This uses only predecessor APIs/helpers and can be copied unchanged to the
// baseline. It measures ordinary strict-call validation and response retention,
// not network/consensus latency or process-attributed request allocations.
func BenchmarkWindowRetainedCallGuardV1(b *testing.B) {
	in, admission := recallTestInput()
	in.vectors = map[string][]float32{}
	in.corpusIDs = nil
	for i := 0; i < 12; i++ {
		id := fmtRecallID(i)
		in.corpusIDs = append(in.corpusIDs, id)
		in.vectors[id] = oracleVector(128, 1, float32(i+1))
	}
	query := oracleVector(128, 1, 0)
	scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(query)
	if err != nil {
		b.Fatal(err)
	}
	truth, err := recallTop10(context.Background(), scorer, in.corpusIDs, in.vectors)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		in.queries = append(in.queries, query)
		ids := make([]string, len(truth))
		for j, n := range truth {
			ids[j] = n.ID
		}
		in.exported = append(in.exported, ids)
	}
	if err := recallPlan(context.Background(), &in, &admission); err != nil {
		b.Fatal(err)
	}
	admission.RPCTimeout = time.Second
	response := windowTestResponse(&admission)
	client := fakeClient{search: func(context.Context, public.SearchRequestV1) (public.SearchResponseV1, error) { return response, nil }}
	origin := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := windowCall(context.Background(), func() {}, client, &in, &admission, i, 0, "measured", origin); err != nil {
			b.Fatal(err)
		}
	}
}
