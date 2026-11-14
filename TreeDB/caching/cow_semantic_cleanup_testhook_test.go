package caching

import (
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"testing"
)

func TestCOWSemanticCleanupForTestJoinGuards(t *testing.T) {
	var absentCache *DB
	if err := absentCache.JoinNativeSemanticCleanupForTest(); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("nil cache: %v", err)
	}
	if err := (&DB{}).JoinNativeSemanticCleanupForTest(); !errors.Is(err, ErrCOWUnsupported) {
		t.Fatalf("non-COW cache: %v", err)
	}
	var absentBackend *backenddb.DB
	if err := absentBackend.JoinNativeSemanticCleanupForTest(); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("nil backend: %v", err)
	}
	if err := (&backenddb.DB{}).JoinNativeSemanticCleanupForTest(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("unbound backend: %v", err)
	}
	cache, backend := nativeBridgeFixture(t, false)
	guard := cache.ownedCloseGuard
	if err := guard.EnterCleanup(); err != nil {
		t.Fatal(err)
	}
	func() {
		defer guard.LeaveCleanup()
		if err := backend.JoinNativeSemanticCleanupForTest(); err == nil {
			t.Fatal("cleanup callback joined its own worker")
		}
	}()
	if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
		t.Fatalf("ordinary caller join: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := cache.cow.budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("Close retained receipts: %+v", stats)
	}
}
