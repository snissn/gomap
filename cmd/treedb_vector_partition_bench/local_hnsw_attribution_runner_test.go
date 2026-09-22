package main

import (
	"os/exec"
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

func TestLocalHNSWAttributionSourceCheckoutV1(t *testing.T) {
	source, base := testM8QualificationGitCheckoutV1(t, t.TempDir())
	if got, err := localHNSWAttributionSourceCheckoutV1(source, base, base); err != nil || got != source {
		t.Fatalf("source checkout=%q err=%v", got, err)
	}
	if output, err := exec.Command("git", "-C", source, "commit", "--allow-empty", "-qm", "descendant").CombinedOutput(); err != nil {
		t.Fatalf("advance source: %v: %s", err, output)
	}
	headRaw, err := exec.Command("git", "-C", source, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	head := strings.TrimSpace(string(headRaw))
	if _, err := localHNSWAttributionSourceCheckoutV1(source, base, head); err != nil {
		t.Fatalf("descendant source checkout: %v", err)
	}
	unrelatedSource, _ := testM8QualificationGitCheckoutV1(t, t.TempDir())
	if output, err := exec.Command("git", "-C", unrelatedSource, "commit", "--allow-empty", "-qm", "unrelated").CombinedOutput(); err != nil {
		t.Fatalf("advance unrelated source: %v: %s", err, output)
	}
	unrelatedRaw, err := exec.Command("git", "-C", unrelatedSource, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	unrelated := strings.TrimSpace(string(unrelatedRaw))
	if _, err := localHNSWAttributionSourceCheckoutV1(source, unrelated, head); err == nil {
		t.Fatal("accepted unrelated source-lock base")
	}
}
