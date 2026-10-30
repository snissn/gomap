package treedb_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
	"github.com/snissn/gomap/TreeDB/internal/recoverytrace"
)

// This opt-in diagnostic has a separate memory-only ledger. It never reinstalls
// the full file-copy durability observer. The ticker starts at child entry so a
// snapshot can precede the unchanged 15s test timeout even if preparation stalls.
type ackRecoveryDiagnostic struct {
	ledger              *recoverytrace.Ledger
	stopSignal, stopped chan struct{}
	restore             func()
	fixtureOperation    uint64
}

func startACKRecoveryDiagnostic(t *testing.T) *ackRecoveryDiagnostic {
	if os.Getenv("GOMAP_RECOVERY_LEDGER") != "1" {
		return nil
	}
	diagnostic := &ackRecoveryDiagnostic{ledger: recoverytrace.New(recoverytrace.MaxRecords), stopSignal: make(chan struct{}), stopped: make(chan struct{})}
	diagnostic.fixtureOperation = diagnostic.begin(recoverytrace.Fixture)
	go func() {
		defer close(diagnostic.stopped)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		emitted := make([]bool, diagnostic.ledger.Capacity())
		dump := func(final bool) {
			published := 0
			for index := 0; index < diagnostic.ledger.Capacity(); index++ {
				event, ready := diagnostic.ledger.Read(index)
				if !ready {
					continue
				}
				published++
				if emitted[index] {
					continue
				}
				emitted[index] = true
				// No producer invokes t.Log or holds a ledger lock through physical I/O.
				t.Logf("recovery-ledger seq=%d parent=%d ns=%d stage=%d outcome=%d root=%q kind=%q resource=%q path=%q generation=%d identity=%q/%d/%x/%d lsn=%d commit=%d handle=%t owned=%t required=%d/%d/%d/%x/%d/%d/%d certified=%t:%d/%d/%d/%x/%d/%d/%d label-truncated=%t",
					event.Sequence, event.Parent, event.ElapsedNanos, event.Stage, event.Outcome,
					event.Root.String(), event.Kind.String(), event.ResourceID.String(), event.Path.String(), event.Generation,
					event.IdentityPlatform.String(), event.IdentityVolume, event.IdentityObject, event.IdentityGeneration,
					event.LSN, event.CommitSeq, event.HandlePresent, event.OwnedOperations,
					event.Required.Bytes, event.Required.MaxLSN, event.Required.MaxRID, event.Required.RIDSetDigest, event.Required.RIDCount, event.Required.RIDMin, event.Required.RIDMax,
					event.HasCertified, event.Certified.Bytes, event.Certified.MaxLSN, event.Certified.MaxRID, event.Certified.RIDSetDigest, event.Certified.RIDCount, event.Certified.RIDMin, event.Certified.RIDMax,
					event.Root.Truncated || event.Kind.Truncated || event.ResourceID.Truncated || event.Path.Truncated || event.IdentityPlatform.Truncated)
			}
			t.Logf("recovery-ledger-summary final=%t capacity=%d attempted=%d published=%d dropped=%d; missing returns are not success; producer restore is not a join", final, diagnostic.ledger.Capacity(), diagnostic.ledger.Attempted(), published, diagnostic.ledger.Dropped())
		}
		for {
			select {
			case <-ticker.C:
				dump(false)
			case <-diagnostic.stopSignal:
				dump(true)
				return
			}
		}
	}()
	return diagnostic
}

func (diagnostic *ackRecoveryDiagnostic) install(t *testing.T) {
	diagnostic.end(recoverytrace.Fixture, diagnostic.fixtureOperation, false)
	restore, ok := recoverytrace.Install(diagnostic.ledger)
	if !ok {
		t.Fatal("recovery diagnostic ledger overlap/reuse")
	}
	diagnostic.restore = restore
}
func (diagnostic *ackRecoveryDiagnostic) begin(stage recoverytrace.Stage) uint64 {
	if diagnostic == nil {
		return 0
	}
	return diagnostic.ledger.Record(recoverytrace.Event{Stage: stage, Outcome: recoverytrace.Started})
}
func (diagnostic *ackRecoveryDiagnostic) end(stage recoverytrace.Stage, parent uint64, failed bool) {
	if diagnostic != nil {
		diagnostic.ledger.End(stage, parent, failed)
	}
}
func (diagnostic *ackRecoveryDiagnostic) close(t *testing.T, stage recoverytrace.Stage, label string, closeOriginal func() error) {
	operation := diagnostic.begin(stage)
	err := closeOrderedACKImage(label, closeOriginal)
	diagnostic.end(stage, operation, err != nil)
	if err != nil {
		t.Error(err)
	}
}
func (diagnostic *ackRecoveryDiagnostic) stop() {
	if diagnostic.restore != nil {
		diagnostic.restore()
	}
	close(diagnostic.stopSignal)
	<-diagnostic.stopped
}

// Stable's outer error reports materialization/setup failure. Ordinary public
// Open rejection is preserved separately in Result.Err with outer error nil.
func (diagnostic *ackRecoveryDiagnostic) stableOpenReturned(operation uint64, result powerlossreopen.Result, reopened *treedb.DB, err error) {
	diagnostic.end(recoverytrace.StableOpen, operation, err != nil || result.Rejected || reopened == nil)
}

func TestACKRecoveryLedgerRejectsOriginalPublicOpenFailure(t *testing.T) {
	model, err := powerlossoracle.Capture(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	// The original public validator refuses adaptive COW before mutable open.
	opts.MemtableMode = "adaptive:cow_ordered"
	diagnostic := &ackRecoveryDiagnostic{ledger: recoverytrace.New(4)}
	operation := diagnostic.begin(recoverytrace.StableOpen)
	destination, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	result, reopened, closeOriginal, outerErr := powerlossreopen.StableAt(destination, model, opts, false)
	if closeOriginal != nil {
		defer func() {
			if err := closeOriginal(); err != nil {
				t.Errorf("original rejected-open cleanup: %v", err)
			}
		}()
	}
	if outerErr != nil || !result.Rejected || reopened != nil || !errors.Is(result.Err, caching.ErrCOWUnsupported) || closeOriginal == nil {
		t.Fatalf("original rejection contract changed: result=%+v db=%v outer=%v cleanup=%t", result, reopened, outerErr, closeOriginal != nil)
	}
	originalErr := result.Err
	diagnostic.stableOpenReturned(operation, result, reopened, outerErr)
	outcome, ready := diagnostic.ledger.Read(1)
	if !ready || outcome.Parent != operation || outcome.Outcome != recoverytrace.ReturnedError || result.Err != originalErr {
		t.Fatal("diagnostic concealed or replaced original Open rejection")
	}
}

// The extraction must preserve the current always-checked cleanup callback in
// ordinary and diagnostic modes, including its single invocation and label.
func TestACKRecoveryLedgerPreservesCheckedClose(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		var diagnostic *ackRecoveryDiagnostic
		if enabled {
			diagnostic = &ackRecoveryDiagnostic{ledger: recoverytrace.New(4)}
		}
		calls := 0
		diagnostic.close(t, recoverytrace.ReopenedClose, "reopened close/cleanup", func() error {
			calls++
			return nil
		})
		if calls != 1 {
			t.Fatalf("checked callback calls=%d enabled=%t", calls, enabled)
		}
		if enabled {
			start, ready := diagnostic.ledger.Read(0)
			end, ended := diagnostic.ledger.Read(1)
			if !ready || !ended || start.Stage != recoverytrace.ReopenedClose || end.Parent != start.Sequence || end.Outcome != recoverytrace.Returned {
				t.Fatal("checked cleanup pair changed")
			}
		}
	}
}
