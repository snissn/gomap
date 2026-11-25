package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestOwnedCallbackCloseGuardCurrentGoroutineID(t *testing.T) {
	mainGID := CurrentGoroutineID()
	if mainGID == 0 {
		t.Fatal("CurrentGoroutineID returned 0")
	}
	if got := currentGoroutineID(); got != mainGID {
		t.Fatalf("CurrentGoroutineID()=%d, want currentGoroutineID()=%d", mainGID, got)
	}
	if again := CurrentGoroutineID(); again != mainGID {
		t.Fatalf("CurrentGoroutineID() changed on same goroutine: %d -> %d", mainGID, again)
	}

	childGIDCh := make(chan uint64, 1)
	go func() {
		childGIDCh <- CurrentGoroutineID()
	}()
	childGID := <-childGIDCh
	if childGID == 0 {
		t.Fatal("child goroutine CurrentGoroutineID returned 0")
	}
	if childGID == mainGID {
		t.Fatalf("distinct goroutines returned same ID %d", mainGID)
	}
}

func TestOwnedCallbackCloseGuardEnterCleanupWithGoroutineIDReentrancy(t *testing.T) {
	var guard OwnedCallbackCloseGuard
	gid := CurrentGoroutineID()
	if gid == 0 {
		t.Fatal("CurrentGoroutineID returned 0")
	}

	if err := guard.EnterCleanupWithGoroutineID(gid); err != nil {
		t.Fatalf("EnterCleanupWithGoroutineID(%d): %v", gid, err)
	}
	if got := guard.cleanup.Load(); got != gid {
		t.Fatalf("cleanup marker=%d, want %d", got, gid)
	}

	// Re-entrant CheckClose from the same goroutine must refuse.
	if err := guard.CheckClose(); !errors.Is(err, errPublicationCallbackCloseV6) {
		t.Fatalf("same-goroutine CheckClose=%v, want %v", err, errPublicationCallbackCloseV6)
	}

	// Concurrent CheckClose from a different goroutine must succeed.
	otherErrCh := make(chan error, 1)
	go func() {
		otherErrCh <- guard.CheckClose()
	}()
	if err := <-otherErrCh; err != nil {
		t.Fatalf("other-goroutine CheckClose=%v, want nil", err)
	}

	guard.LeaveCleanup()
	if got := guard.cleanup.Load(); got != 0 {
		t.Fatalf("cleanup marker after LeaveCleanup=%d, want 0", got)
	}
	if err := guard.CheckClose(); err != nil {
		t.Fatalf("same-goroutine CheckClose after LeaveCleanup=%v, want nil", err)
	}
}

func TestOwnedCallbackCloseGuardEnterCleanupWithGoroutineIDProtocolAndCrossLane(t *testing.T) {
	gid := CurrentGoroutineID()
	if gid == 0 {
		t.Fatal("CurrentGoroutineID returned 0")
	}

	// Nil guard must return ErrPublisherProtocol for both explicit GID and 0 fallback.
	var nilGuard *OwnedCallbackCloseGuard
	if err := nilGuard.EnterCleanupWithGoroutineID(gid); !errors.Is(err, rootpublication.ErrPublisherProtocol) {
		t.Fatalf("nil guard EnterCleanupWithGoroutineID(%d)=%v, want %v", gid, err, rootpublication.ErrPublisherProtocol)
	}
	if err := nilGuard.EnterCleanupWithGoroutineID(0); !errors.Is(err, rootpublication.ErrPublisherProtocol) {
		t.Fatalf("nil guard EnterCleanupWithGoroutineID(0)=%v, want %v", err, rootpublication.ErrPublisherProtocol)
	}
	if err := nilGuard.EnterCleanup(); !errors.Is(err, rootpublication.ErrPublisherProtocol) {
		t.Fatalf("nil guard EnterCleanup()=%v, want %v", err, rootpublication.ErrPublisherProtocol)
	}

	// Nested EnterCleanupWithGoroutineID must return ErrDurableRootOwnership.
	var guard OwnedCallbackCloseGuard
	if err := guard.EnterCleanupWithGoroutineID(gid); err != nil {
		t.Fatalf("first EnterCleanupWithGoroutineID: %v", err)
	}
	if err := guard.EnterCleanupWithGoroutineID(gid); !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
		t.Fatalf("nested EnterCleanupWithGoroutineID=%v, want %v", err, rootpublication.ErrDurableRootOwnership)
	}
	// Cross-lane Enter while cleanup lane is held on same goroutine must return ErrDurableRootOwnership.
	if err := guard.Enter(); !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
		t.Fatalf("same-goroutine Enter during cleanup=%v, want %v", err, rootpublication.ErrDurableRootOwnership)
	}
	guard.LeaveCleanup()

	// Cross-lane Enter -> EnterCleanupWithGoroutineID on same goroutine must return ErrDurableRootOwnership.
	if err := guard.Enter(); err != nil {
		t.Fatalf("Enter: %v", err)
	}
	if err := guard.EnterCleanupWithGoroutineID(gid); !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
		t.Fatalf("same-goroutine EnterCleanupWithGoroutineID during Enter=%v, want %v", err, rootpublication.ErrDurableRootOwnership)
	}
	guard.Leave()

	// Fallback when caller == 0 must compute currentGoroutineID().
	if err := guard.EnterCleanupWithGoroutineID(0); err != nil {
		t.Fatalf("EnterCleanupWithGoroutineID(0): %v", err)
	}
	if got := guard.cleanup.Load(); got != gid {
		t.Fatalf("fallback cleanup marker=%d, want %d", got, gid)
	}
	if err := guard.CheckClose(); !errors.Is(err, errPublicationCallbackCloseV6) {
		t.Fatalf("same-goroutine CheckClose after 0-fallback=%v, want %v", err, errPublicationCallbackCloseV6)
	}
	guard.LeaveCleanup()
}

func BenchmarkOwnedCallbackCloseGuardEnterCleanup(b *testing.B) {
	b.Run("Uncached", func(b *testing.B) {
		var guard OwnedCallbackCloseGuard
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := guard.EnterCleanup(); err != nil {
				b.Fatal(err)
			}
			guard.LeaveCleanup()
		}
	})

	b.Run("CachedGID", func(b *testing.B) {
		var guard OwnedCallbackCloseGuard
		gid := CurrentGoroutineID()
		if gid == 0 {
			b.Fatal("CurrentGoroutineID returned 0")
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := guard.EnterCleanupWithGoroutineID(gid); err != nil {
				b.Fatal(err)
			}
			guard.LeaveCleanup()
		}
	})
}
