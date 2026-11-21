package caching

import (
	"context"
	"errors"
	"testing"
)

func TestCOWAcceptedPressureIgnoresStaleWakeAndClosing(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before := backend.State().CommitSeq
	through := cache.cow.prefix.LogicalFrontiers()
	cache.testBeforeCheckpointFrontierCapture = func() {
		t.Error("unneeded advisory wake reached frontier capture")
	}
	if err := cache.materializeCOWPrefix(context.Background(), false, nil, cowAcceptedPressureMaterialization); err != nil {
		t.Fatal(err)
	}
	if backend.State().CommitSeq != before || cache.cow.prefix.LogicalFrontiers() != through {
		t.Fatal("stale pressure wake changed the sealed root")
	}
	cache.closing.Store(true)
	if err := cache.materializeCOWPrefix(context.Background(), false, nil, cowAcceptedPressureMaterialization); err != errDBClosing {
		t.Fatalf("pressure wake during Close: %v", err)
	}
	cache.closing.Store(false)
	cache.testBeforeCheckpointFrontierCapture = nil
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCOWAcceptedPressureStaleWakeReportsOriginalError(t *testing.T) {
	cache, _ := nativeBridgeFixture(t, true)
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("original accepted prefix failure before pressure wake")
	cache.cow.prefix.PoisonAccepted(failure)
	if err := cache.materializeCOWPrefix(context.Background(), false, nil, cowAcceptedPressureMaterialization); !errors.Is(err, failure) {
		t.Fatalf("stale wake lost the original accepted failure: %v", err)
	}
	if err := cache.Close(); !errors.Is(err, failure) {
		t.Fatalf("checked Close lost the original accepted failure: %v", err)
	}
}
