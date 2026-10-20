package caching

import (
	"errors"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// Only kernel lifetime tests use this state. The collections control exercises
// the real complete catalog importer and original cold graph validation.
type physicalImportTestState struct {
	testSemanticState
	token     backenddb.StateToken
	sequence  uint64
	allocator backenddb.NativeSemanticAllocator
}

func (s *physicalImportTestState) NativeSemanticImportEvidence() transactionowner.NativeImportEvidence {
	return transactionowner.NewNativeImportEvidence(s, s.allocator, backenddb.NativeSemanticImportBasis(s.token), s.sequence)
}

// Public callers can embed the complete interface and inherit its getter, but
// its evidence still names the original state rather than this wrapper.
type physicalImportEmbeddingWrapper struct {
	backenddb.NativeSemanticPhysicalImport
}

func physicalImportTestCapture(t *testing.T, db *DB) *NativeSemanticCapture {
	t.Helper()
	if db.cow.txnOwner == nil {
		db.cow.txnOwner = transactionowner.New()
		backend := db.backend.(*backenddb.DB)
		if err := backend.BindPrivateCOWOwner(db.cow.txnOwner); err != nil {
			t.Fatal(err)
		}
		if err := db.cow.bindAcceptedPrefix(backend); err != nil {
			t.Fatal(err)
		}
	}
	capture, err := db.CaptureNativeSemanticState(db.cow.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capture.Close(); err != nil {
			t.Error(err)
		}
	})
	return capture
}
func physicalImportTestVersion(t *testing.T, capture *NativeSemanticCapture, finished *int) *physicalImportTestState {
	t.Helper()
	receipt, err := capture.AdmitNativeBytes(uint64(unsafe.Sizeof(physicalImportTestState{})))
	if err != nil {
		t.Fatal(err)
	}
	token, ok := capture.Snapshot().StateToken()
	if !ok {
		t.Fatal("missing actual physical basis")
	}
	s := &physicalImportTestState{token: token, sequence: capture.LogicalSequence(), allocator: capture.NativeAllocator()}
	s.refs.Store(1)
	s.cleanup.Work = s
	s.refund = receipt
	s.finish = func() error { (*finished)++; return nil }
	return s
}
func finishPhysicalImportTestPrivate(t *testing.T, s *physicalImportTestState) {
	t.Helper()
	record := s.Release()
	s = nil
	if record != nil {
		completion, err := record.Finish()
		record = nil
		if err != nil {
			t.Fatal(err)
		}
		completion.Complete()
	}
}

func TestCOWNativePhysicalImportPreservesCutAndHeldOwners(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	held := physicalImportTestCapture(t, db)
	capture := physicalImportTestCapture(t, db)
	raw := db.AcquireSnapshot()
	if raw == nil {
		t.Fatal("missing held raw cut")
	}
	defer raw.Close()
	opening, basis := capture.cut, capture.cut.basis
	frontiers := c.prefix.LogicalFrontiers()
	token, _ := capture.Snapshot().StateToken()
	var finished int
	state := physicalImportTestVersion(t, capture, &finished)
	adopted, err := capture.AdoptNativeSemanticImport(state)
	if err != nil || !adopted {
		t.Fatalf("adopted=%v err=%v", adopted, err)
	}
	finishPhysicalImportTestPrivate(t, state)
	if capture.cut == opening || capture.cut.basis != basis || held.cut != opening || held.State() != nil || capture.State() != state {
		t.Fatal("representation replacement changed basis/old capture or failed to retain the imported state")
	}
	if current, _ := capture.Snapshot().StateToken(); current != token || c.prefix.LogicalFrontiers() != frontiers || capture.LogicalSequence() != held.LogicalSequence() {
		t.Fatal("read-only adoption advanced physical or accepted frontiers")
	}
	next := physicalImportTestCapture(t, db)
	if next.State() != state || next.cut != capture.cut {
		t.Fatal("next capture did not reuse the same installed representation")
	}
	for _, snapshot := range []*Snapshot{raw, db.AcquireSnapshot()} {
		if snapshot == nil {
			t.Fatal("missing current raw cut")
		}
		got, err := snapshot.Get([]byte("disk"))
		if err != nil || string(got) != "basis" {
			t.Fatalf("raw cut=%q err=%v", got, err)
		}
		if snapshot != raw {
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Close withdraws the original current edge; held captures keep both original
	// basis and imported state alive. Destruction/refund follows their last Close.
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	db.drainSemanticCleanup()
	if finished != 0 {
		t.Fatal("Close destroyed an imported state still held by readers")
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	db.drainSemanticCleanup()
	if !db.semanticCleanupTerminal() {
		t.Fatal("original semantic cleanup did not reach terminal lifecycle")
	}
	if err := db.backend.Close(); err != nil {
		t.Fatal(err)
	}
	if finished != 1 || c.activeCuts.Load() != 0 || c.budget.Stats().TotalBytes != 0 {
		t.Fatalf("final resources: finished=%d active=%d budget=%+v", finished, c.activeCuts.Load(), c.budget.Stats())
	}
}

func TestCOWNativePhysicalImportRejectsStaleAndWrongBasis(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	stale := physicalImportTestCapture(t, db)
	winner := physicalImportTestCapture(t, db)
	var finished int
	candidate := physicalImportTestVersion(t, winner, &finished)
	wrapper := &physicalImportEmbeddingWrapper{NativeSemanticPhysicalImport: candidate}
	opening := c.cut
	if adopted, err := winner.AdoptNativeSemanticImport(wrapper); adopted || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || c.cut != opening || candidate.refs.Load() != 1 {
		t.Fatalf("embedded evidence authorized another receiver: adopted=%v err=%v refs=%d", adopted, err, candidate.refs.Load())
	}
	originalToken, originalSequence, originalAllocator := candidate.token, candidate.sequence, candidate.allocator
	for _, which := range []string{"token", "sequence", "allocator"} {
		switch which {
		case "token":
			candidate.token.CommitSeq++
		case "sequence":
			candidate.sequence++
		case "allocator":
			candidate.allocator = nil
		}
		if adopted, err := winner.AdoptNativeSemanticImport(candidate); adopted || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			t.Fatalf("%s admitted invalid import: adopted=%v err=%v", which, adopted, err)
		}
		candidate.token, candidate.sequence, candidate.allocator = originalToken, originalSequence, originalAllocator
	}
	if adopted, err := winner.AdoptNativeSemanticImport(candidate); err != nil || !adopted {
		t.Fatalf("winner=%v %v", adopted, err)
	}
	finishPhysicalImportTestPrivate(t, candidate)
	losing := physicalImportTestVersion(t, stale, &finished)
	installed := c.cut
	frontiers := c.prefix.LogicalFrontiers()
	if adopted, err := stale.AdoptNativeSemanticImport(losing); err != nil || adopted {
		t.Fatalf("stale=%v %v", adopted, err)
	}
	finishPhysicalImportTestPrivate(t, losing)
	if c.cut != installed || stale.State() != nil || c.prefix.LogicalFrontiers() != frontiers || finished != 1 {
		t.Fatal("stale import replaced current state/frontiers or lost private cleanup")
	}
	if err := winner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	db.drainSemanticCleanup()
	if !db.semanticCleanupTerminal() {
		t.Fatal("original semantic cleanup did not reach terminal lifecycle")
	}
	if err := db.backend.Close(); err != nil {
		t.Fatal(err)
	}
	if finished != 2 || c.budget.Stats().TotalBytes != 0 {
		t.Fatalf("losing/current custody: finished=%d budget=%+v", finished, c.budget.Stats())
	}
}

func TestCOWNativePhysicalImportRefusesPendingAndClosedCuts(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	initial := physicalImportTestCapture(t, db)
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	c.writerMu.Lock()
	prepared := cowCutPrepare(t, c, "pending raw")
	retired := prepared.publish()
	c.writerMu.Unlock()
	if retired != nil {
		if err := retired.drain(); err != nil {
			t.Fatal(err)
		}
	}
	capture := physicalImportTestCapture(t, db)
	var finished int
	state := physicalImportTestVersion(t, capture, &finished)
	opening := c.cut
	if adopted, err := capture.AdoptNativeSemanticImport(state); adopted || err != nil {
		t.Fatalf("pending=%v %v", adopted, err)
	}
	if c.cut != opening {
		t.Fatal("pending source cut changed")
	}
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	if adopted, err := capture.AdoptNativeSemanticImport(state); adopted || !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed=%v %v", adopted, err)
	}
	finishPhysicalImportTestPrivate(t, state)
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	db.drainSemanticCleanup()
	if !db.semanticCleanupTerminal() {
		t.Fatal("original semantic cleanup did not reach terminal lifecycle")
	}
	if err := db.backend.Close(); err != nil {
		t.Fatal(err)
	}
	if finished != 1 || c.budget.Stats().TotalBytes != 0 {
		t.Fatalf("closed/pending custody: finished=%d budget=%+v", finished, c.budget.Stats())
	}
}

var _ backenddb.NativeSemanticPhysicalImport = (*physicalImportTestState)(nil)
var _ rootpublication.SemanticCleanupWork = (*physicalImportTestState)(nil)
