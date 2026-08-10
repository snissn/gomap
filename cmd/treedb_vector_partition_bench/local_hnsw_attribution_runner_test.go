package main

import (
	"strings"
	"testing"
)

func TestLocalHNSWAttributionRunnerV1(t *testing.T) {
	if err := run([]string{"local-hnsw-attribution"}, &strings.Builder{}); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
	fixture := fixtureManifest{SchemaVersion: 1, Fixture: "qualification_embedding_mixture_250000", Generator: qualificationEmbeddingGeneratorV1, Arithmetic: fixtureArithmetic, Vectors: 250000, Queries: 1000, Dimensions: 128, Metric: "cosine", Seed: 4016, Checksum: "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69"}
	if !localHNSWAttributionFixtureV1(fixture) {
		t.Fatal("expected exact frozen fixture")
	}
	graph := localHNSWAttributionGraphAggregateV1{NativeUnreachableRows: 1}
	summary := localHNSWAttributionCalibrationSummaryV1{ChangedP2TopK: 1, ChangedPackVisitedDigest: 1, RoutingRecall: localHNSWAttributionRecallAggregateV1{Mean: .999}, Native: localHNSWAttributionCalibrationVariantV1{P2EndToEnd: localHNSWAttributionRecallAggregateV1{Mean: .96}, AllGlobal: localHNSWAttributionRecallAggregateV1{Mean: .97}}, Overlay: localHNSWAttributionCalibrationVariantV1{P2EndToEnd: localHNSWAttributionRecallAggregateV1{Mean: .93}, AllGlobal: localHNSWAttributionRecallAggregateV1{Mean: .94}}}
	facts := localHNSWAttributionDecisionFactsV1Build(graph, summary)
	if !facts.NativeDisconnected || !facts.OverlayConnected || !facts.MutationChangedTraversal || !facts.MutationChangedP2TopK || facts.NativeMinusOverlayP2Recall <= 0 || facts.NativeMinusOverlayAllRecall <= 0 || !facts.NativeP2TargetMet || !facts.NativeAllTargetMet || !facts.RoutingTargetMet {
		t.Fatalf("facts=%+v", facts)
	}
}
