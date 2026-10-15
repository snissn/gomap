package freelist

import (
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestNativeCOWPublicationScopeExcludesPrivateRollbackV6(t *testing.T) {
	a := newPrivatePrepareAllocatorV6(t)
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := a.PrepareCOWCandidateV1(2, 2, candidateIDFromString("scope-prefix"), capability, 0, NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	materializeAllocatorCOWCandidateForTest(t, a.pager, prepared)
	if err := a.ActivateCOWCandidateV1(prepared); err != nil {
		t.Fatal(err)
	}
	ledger := a.cow.ledger
	privateID := candidateIDFromString("scope-private-rollback")
	if _, _, err := ledger.reserveTail(privateID, prepared.Candidate().Generation().HighWater()+100, 1, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := ledger.markTailWriteAttempted(privateID); err != nil {
		t.Fatal(err)
	}
	var scope COWPublicationScopeV6
	work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	ready, err := a.BeginCOWPublicationScopeV6(&scope, []*PreparedCOWCandidateV1{prepared}, &work)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	defer scope.Close()
	started, done := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		done <- ledger.RollbackPreVisible(privateID)
	}()
	<-started
	// The private operation needs only ledger.mu. Allocator locking alone
	// would let it mutate the geometry while the authority write is in flight.
	select {
	case err := <-done:
		t.Fatal("private rollback escaped original ledger scope", err)
	case <-time.After(10 * time.Millisecond):
	}
	copyOfScope := scope
	copyOfScope.Close() // A copied control must not unlock the original scope.
	if err := copyOfScope.Commit(capability); err == nil {
		t.Fatal("copied scope acquired publication authority")
	}
	if err := scope.Commit(capability); err != nil {
		t.Fatal(err)
	}
	if err := scope.Commit(capability); err == nil {
		t.Fatal("original ledger consumed twice")
	}
	scope.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("scope Close retained private rollback lock")
	}
	if len(ledger.burnedTails) != 1 || ledger.candidates[privateID] != nil || ledger.candidates[prepared.candidateID] != nil {
		t.Fatal("scope lost original rollback or publication custody")
	}
}

func TestNativeCOWPublicationDefersOnlyConservativeEagerReuseV6(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "native"}[native], func(t *testing.T) {
			a := newPrivatePrepareAllocatorV6(t)
			oldCapability, err := NewReuseCapability(1, 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			newCapability, err := NewReuseCapability(2, 2, 0)
			if err != nil {
				t.Fatal(err)
			}
			candidateID := candidateIDFromString("native-fence-reuse")
			prepared, err := a.PrepareCOWCandidateRetiringV1(2, 2, candidateID, oldCapability, []COWRetirementV1{{PageIDs: []uint64{3}, LastReachableCommitSeq: 1}}, 0, NewCandidatePageSinkV1())
			if err != nil {
				t.Fatal(err)
			}
			materializeAllocatorCOWCandidateForTest(t, a.pager, prepared)
			if err := a.ActivateCOWCandidateV1(prepared); err != nil {
				t.Fatal(err)
			}
			if prepared.Candidate().Generation().Allocatable(3) {
				t.Fatal("retired page reclaimed before a newer durable capability")
			}
			prefix := []*PreparedCOWCandidateV1{prepared}
			if native {
				var tooLarge *iterator.OrdinalUnitTooLarge
				ready, err := a.PublishActivatedCOWPrefixWithWorkV6(prefix, newCapability, &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20})
				if ready || !errors.As(err, &tooLarge) || prepared.published || a.cow.ledger.candidates[candidateID] == nil || len(a.cow.activated) != 1 {
					t.Fatal("pre-effect refusal consumed original publication", ready, err)
				}
				w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				ready, err = a.PublishActivatedCOWPrefixWithWorkV6(prefix, newCapability, &w)
				if !ready || err != nil || w.Records > 32 || w.Bytes > 1<<20 {
					t.Fatal("native ledger publication failed", ready, err, w)
				}
				if ready, err = a.PublishActivatedCOWPrefixWithWorkV6(prefix, newCapability, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || err == nil {
					t.Fatal("same native prefix published twice")
				}
			} else if err := a.PublishActivatedCOWPrefixV1(prefix, newCapability); err != nil {
				t.Fatal(err)
			}
			if !prepared.published || a.cow.ledger.candidates[candidateID] != nil || len(a.cow.activated) != 0 {
				t.Fatal("actual original ledger prefix was not consumed exactly once")
			}
			chunk := lookupChunk(a.cow.txn.root, 0)
			if chunk == nil || native && chunk.retired[3] != 1 || !native && chunk.retired[3] != 0 {
				t.Fatal("native changed eager reuse or ordinary lost its existing eager prune")
			}
			allocated, err := a.Alloc(3)
			if err != nil || native && allocated == 3 || !native && allocated != 3 {
				t.Fatal("native reused deferred retired page or ordinary reuse changed", allocated, err)
			}
			// This is the next actual maintained candidate preparation on the
			// SAME allocator, using the now-current durable capability. Its own
			// canonical pruning stage reclaims the conservatively deferred page.
			next, err := a.PrepareCOWCandidateV1(3, 3, candidateIDFromString("native-next-prepare"), newCapability, 0, NewCandidatePageSinkV1())
			if err != nil {
				t.Fatal(err)
			}
			if native && !next.Candidate().Generation().Allocatable(3) {
				t.Fatal("next real preparation failed to reclaim under current capability")
			}
			if err := a.AbortCOWCandidateV1(next); err != nil {
				t.Fatal(err)
			}
		})
	}
}
