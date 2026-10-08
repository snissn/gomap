package freelist

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/allocatorownership"
	"testing"
	"time"
)

func TestManagedWriterCapabilitySingleClaim5105(t *testing.T) {
	first, second := terminalAllocator5105(t, nil), terminalAllocator5105(t, nil)
	authority := allocatorownership.NewManagedWriter()
	if err := first.BindManagedIndexWriterV1(authority); err != nil {
		t.Fatal(err)
	}
	if err := second.BindManagedIndexWriterV1(authority); !errors.Is(err, ErrCandidateConsumed) || second.writerAuthority != nil {
		t.Fatal("authority reused", err)
	}
	if err := second.BindManagedIndexWriterV1(nil); !errors.Is(err, ErrGenerationFormat) {
		t.Fatal(err)
	}
}

func TestManagedWriterEscapeRefusesResidentAdoptionAndFiniteExport5105(t *testing.T) {
	for _, escaped := range []bool{false, true} {
		a := managedTerminalAllocator5105(t)
		if escaped && !a.MarkOrdinaryWriterEscapeV1() {
			t.Fatal("ordinary retaining writer refused")
		}
		account, creator := buildCreditLease5105(t)
		before := account.bytes
		_, err := a.admitResidentAllocationCreditV1(requestForCreator5108(creator), creator, 1)
		if escaped {
			if !errors.Is(err, ErrFiniteAllocationExportV1) || account.bytes != before {
				t.Fatal("prior writer escaped admission", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if a.MarkOrdinaryWriterEscapeV1() || a.rawWriterEscaped {
				t.Fatal("finite writer escaped")
			}
			if err = a.endResidentAllocationCreditV1(creator, 1); err != nil {
				t.Fatal(err)
			}
		}
		creator.release()
		authority := a.writerAuthority
		a.CloseCOWOwnersAfterShutdownV1()
		if escaped {
			if a.CanDetachManagedIndexWriterV1(authority) {
				t.Fatal("retaining writer backing detached")
			}
		} else {
			if account.released != 0 || a.cow == nil {
				t.Fatal("index writer header released early")
			}
			a.DetachManagedIndexWriterV1(authority)
			if account.released != 1 || a.cow != nil || a.writerAuthority != nil {
				t.Fatal("joined index writer failed to drain")
			}
		}
	}
}

func TestManagedWriterCondBorrowOwnsHeadersThroughShutdown5105(t *testing.T) {
	a := managedTerminalAllocator5105(t)
	_ = ownedPrepared5105(t, a, "waiter-terminal")
	account, creator := buildCreditLease5105(t)
	if _, err := a.admitResidentAllocationCreditV1(requestForCreator5108(creator), creator, 1); err != nil {
		t.Fatal(err)
	}
	if err := a.endResidentAllocationCreditV1(creator, 1); err != nil {
		t.Fatal(err)
	}
	creator.release()
	sleeping := make(chan struct{})
	TestHookCOWWaitBeforeSleep = func() { close(sleeping) }
	defer func() { TestHookCOWWaitBeforeSleep = nil }()
	done := make(chan error, 1)
	go func() { _, err := a.AllocWithAllocationRequestV1(requestForAllocator5108(a), 0); done <- err }()
	select {
	case <-sleeping:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not enter Cond")
	}
	// Wait has released a.mu before this lock succeeds. Hold wake/reacquire
	// while terminally removing the actual index writer edge.
	a.mu.Lock()
	state := a.cow
	if state.readyWaiters != 1 {
		a.mu.Unlock()
		t.Fatal("missing actual Cond borrower")
	}
	a.closeCOWOwnersLockedV1()
	a.writerDetached = true
	a.releaseClosedCOWStateCreatorLockedV1()
	if account.released != 0 || a.cow != state || state.ready == nil {
		a.mu.Unlock()
		t.Fatal("Cond borrower lost control backing")
	}
	a.mu.Unlock()
	select {
	case err := <-done:
		if !errors.Is(err, ErrCandidateConsumed) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown waiter not woken")
	}
	if account.released != 1 || a.cow != nil || a.writerAuthority != nil {
		t.Fatal("last wake/reacquire failed exact control drain")
	}
	// Permanent discriminator prevents legacy pager allocation after cow=nil.
	before := a.pager.PageCount()
	if _, err := a.AllocWithAllocationRequestV1(requestForAllocator5108(a), 0); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal(err)
	}
	if _, err := a.AllocManyWithAllocationRequestV1(requestForAllocator5108(a), 2, 0); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal(err)
	}
	if _, err := a.AllocAppendWithAllocationRequestV1(requestForAllocator5108(a)); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal(err)
	}
	if err := a.FreeWithAllocationRequestV1(requestForAllocator5108(a), scratchForAllocator5108(a), 2); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal(err)
	}
	if err := a.EnableNewCOWGenerationV1(2, 4, nil); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal(err)
	}
	if a.pager.PageCount() != before || a.cow != nil {
		t.Fatal("closed dispatch mutated pager or restored COW")
	}
}
