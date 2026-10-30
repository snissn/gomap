package treedb_test

import (
	"os"
	"testing"
	"time"

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
func (diagnostic *ackRecoveryDiagnostic) close(t *testing.T, stage recoverytrace.Stage, closeOriginal func() error) {
	operation := diagnostic.begin(stage)
	err := closeOriginal()
	diagnostic.end(stage, operation, err != nil)
	if err != nil {
		t.Errorf("original close at recovery diagnostic stage %d: %v", stage, err)
	}
}
func (diagnostic *ackRecoveryDiagnostic) stop() {
	if diagnostic.restore != nil {
		diagnostic.restore()
	}
	close(diagnostic.stopSignal)
	<-diagnostic.stopped
}
