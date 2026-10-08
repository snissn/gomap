package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableTerminalDBNamespaceFailureRecordsPoisonWithoutNotification(t *testing.T) {
	injected := errors.New("terminal directory persistence failure")
	originalSync := syncDirFn
	syncDirFn = func(string) error { return injected }
	t.Cleanup(func() { syncDirFn = originalSync })

	notified := false
	backend := &DB{notifyError: func(error) { notified = true }}
	consumer := stableSegmentTerminalConsumerV1{db: backend}
	err := consumer.SyncStableSegmentDeletion(t.TempDir()+"/segment.vlog", durabilitycut.ResourceValueLog)
	if !errors.Is(err, injected) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("terminal error does not preserve persistence failure/recovery: %v", err)
	}
	if !backend.publicationPoisoned.Load() {
		t.Fatal("terminal namespace failure did not poison later publication")
	}
	if !errors.Is(backend.backgroundError(), injected) {
		t.Fatalf("foreground background error lost terminal cause: %v", backend.backgroundError())
	}
	if notified {
		t.Fatal("publisher terminal cleanup called user notification before its worker could leave")
	}
	first := backend.backgroundError()
	backend.recordTerminalErrorV1(errors.New("later failure"))
	if backend.backgroundError() != first {
		t.Fatal("terminal error replaced the first observable background cause")
	}
}

func TestStableTerminalDBMissingRegistrarRefusesBeforePlan(t *testing.T) {
	consumer := stableSegmentTerminalConsumerV1{}
	joined, err := consumer.BeginTerminalRelease()
	if joined || err != nil {
		t.Fatalf("ordinary missing-registrar join: %v %v", joined, err)
	}
	if err := consumer.ValidateSegmentRetention(nil); !errors.Is(err, rootpublication.ErrStableTerminalConsumerRequired) {
		t.Fatalf("finite validation accepted a missing registrar: %v", err)
	}
	if err := consumer.PrepareTerminalRelease(nil); !errors.Is(err, rootpublication.ErrStableTerminalConsumerRequired) {
		t.Fatalf("terminal plan accepted a missing registrar: %v", err)
	}
	if err := consumer.ReleaseSegmentRetention(nil); !errors.Is(err, rootpublication.ErrStableTerminalConsumerRequired) {
		t.Fatalf("terminal release accepted a missing plan: %v", err)
	}
	consumer.EndTerminalRelease(joined)
	if consumer.plan != nil {
		t.Fatal("failed terminal call retained a plan")
	}
}
