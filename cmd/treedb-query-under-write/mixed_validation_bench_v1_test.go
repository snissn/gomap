package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

// This file intentionally uses only predecessor functions/types so root can
// overlay the identical guard on the frozen base. No RPC or oracle setup is timed.
func BenchmarkMixedPrefixValidationGuardV1(b *testing.B) {
	in, admission := recallTestInput()
	in.vectors = make(map[string][]float32)
	query := oracleVector(128, 1, 0)
	scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(query)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		id := fmt.Sprintf("bench-%02d", i)
		in.corpusIDs = append(in.corpusIDs, id)
		in.vectors[id] = oracleVector(128, 1, float32(i+1))
	}
	corpus, err := recallTop10(context.Background(), scorer, in.corpusIDs, in.vectors)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		in.queries = append(in.queries, query)
		ids := make([]string, 0, 10)
		for _, n := range corpus {
			ids = append(ids, n.ID)
		}
		in.exported = append(in.exported, ids)
	}
	in.vectors["live"] = query
	if err := recallPlan(context.Background(), &in, &admission); err != nil {
		b.Fatal(err)
	}
	r := mixedReport{windowReport: windowReport{Admission: admission}, PaceInterval: time.Second}
	if _, err := mixedPlan(context.Background(), &in, &r); err != nil {
		b.Fatal(err)
	}
	q := admission.Queries[0]
	response := windowTestResponse(&admission)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, err := mixedValidatePrefix(&q, response, &in, &r, 0, 6)
		if err != nil || value != 1 {
			b.Fatalf("validation: %v %v", value, err)
		}
	}
}
