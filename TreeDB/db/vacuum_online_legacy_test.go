package db

import (
	"context"
	"errors"
	"testing"
)

type legacyOnlineVacuumTestCapabilityV1 struct{}

func (legacyOnlineVacuumTestCapabilityV1) allowLegacyOnlineVacuumV1() {}

func (db *DB) vacuumIndexOnlineLegacyForTest(ctx context.Context) error {
	return db.vacuumIndexOnlineLegacyV1(ctx, true, legacyOnlineVacuumTestCapabilityV1{})
}

func skipLegacyOnlineVacuumRuntimeIntegration(t *testing.T) {
	t.Helper()
	t.Skip("deferred to #3681: post-swap writes require recoverable-root-set runtime rebinding")
}

func TestVacuumIndexOnlineRequiresRecoverableRootSetFencing(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	err = db.VacuumIndexOnline(context.Background())
	if !errors.Is(err, ErrVacuumUnsupported) {
		t.Fatalf("VacuumIndexOnline error=%v, want ErrVacuumUnsupported", err)
	}
	if !errors.Is(err, ErrVacuumRecoverableRootSetRequired) {
		t.Fatalf("VacuumIndexOnline error=%v, want ErrVacuumRecoverableRootSetRequired", err)
	}
}
