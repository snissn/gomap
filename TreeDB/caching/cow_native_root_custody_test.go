package caching

import (
	"errors"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const testRootCustodyCharge = 256 << 10

type cowRootCustodyTestLedger struct {
	record          rootpublication.SemanticCleanup
	lease           *memtable.COWExternalLease
	onDetach        func()
	onFinish        func() error
	detach, destroy int
}

func (l *cowRootCustodyTestLedger) ReleaseNativeCustody() *rootpublication.SemanticCleanup {
	l.detach++
	if l.onDetach != nil {
		l.onDetach()
	}
	return &l.record
}

func (l *cowRootCustodyTestLedger) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	l.destroy++
	if err := l.onFinish(); err != nil {
		return rootpublication.SemanticCleanupCompletion{}, err
	}
	refund := l.lease
	l.lease = nil
	return rootpublication.SemanticCleanupCompletion{Refund: refund}, nil
}

func prepareTestRootCustody(t *testing.T, cache *DB, backend *backenddb.DB, capture backenddb.NativeSemanticCapture, trusted, optIn bool, ledger *cowRootCustodyTestLedger) (*backenddb.NativePreparedPublication, *backenddb.CommandWALIntent, *memtable.COWExternalLease) {
	t.Helper()
	lease, err := cache.cow.budget.AcquireExternal(testRootCustodyCharge)
	if err != nil {
		t.Fatal(err)
	}
	ledger.lease, ledger.record.Work = lease, ledger
	publication := &backenddb.NativePreparedPublication{
		Basis: capture.State(), StartSequence: capture.LogicalSequence(), Custody: ledger,
		Reprepare: &nativeTxnTestPlan{state: nativeBridgeState(t, capture)}, ReturnCustodyOnInstall: optIn,
	}
	document, err := bson.Marshal(bson.D{{Key: "_id", Value: "id"}, {Key: "n", Value: 1}})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("native", []commitlog.CollectionDocument{{ID: []byte("id"), Document: document}})
	if err != nil {
		t.Fatal(err)
	}
	newIntent := backend.NewCommandWALIntent
	if trusted {
		newIntent = backend.NewTrustedCommandWALIntent
	}
	intent, err := newIntent(commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	return publication, intent, lease
}

func waitRootCustodyTestEvent(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func TestCOWNativeRootCustodyChecksOptInAndRefundsBeforeSeal(t *testing.T) {
	registerNativeBridgeTestCommand()
	for _, tc := range []struct {
		name           string
		trusted, optIn bool
	}{
		{name: "root-covered", trusted: true, optIn: true},
		{name: "ordinary-native-producer", trusted: true},
		{name: "untrusted-frame", optIn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache, backend := nativeBridgeFixture(t, true)
			opening, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			defer opening.Close()
			started, finish := make(chan struct{}), make(chan struct{})
			finishOnce := sync.OnceFunc(func() { close(finish) })
			defer finishOnce()
			ledger := &cowRootCustodyTestLedger{onFinish: func() error { close(started); <-finish; return nil }}
			publication, intent, lease := prepareTestRootCustody(t, cache, backend, opening, tc.trusted, tc.optIn, ledger)
			before := cache.cow.prefix.LogicalFrontiers()
			result, err := backend.WriteNativePublication(publication, intent, false)
			if err != nil || !result.LogicalInstalled || !result.WALCompleted || result.WALStable || result.AssignedLSN != before.C+1 {
				t.Fatalf("relaxed receipt=%+v err=%v", result, err)
			}
			if after := cache.cow.prefix.LogicalFrontiers(); after.C != before.C+1 || after.V != before.V+1 || after.S != before.S {
				t.Fatalf("ACK moved the physical cutoff: %+v -> %+v", before, after)
			}
			if !tc.trusted || !tc.optIn {
				select {
				case <-started:
					t.Fatal("fallback detached before its physical seal")
				default:
				}
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			waitRootCustodyTestEvent(t, started, "the original cleanup worker")
			if err := lease.Resize(testRootCustodyCharge); err != nil {
				t.Fatalf("original receipt refunded while worker held: %v", err)
			}
			finishOnce()
			if err := cache.joinSemanticCleanupFrontier(); err != nil {
				t.Fatal(err)
			}
			if err := lease.Resize(testRootCustodyCharge); !errors.Is(err, memtable.ErrCOWClosed) || ledger.detach != 1 || ledger.destroy != 1 {
				t.Fatalf("original refund=%v detach=%d destroy=%d", err, ledger.detach, ledger.destroy)
			}
			if tc.trusted && tc.optIn {
				if after := cache.cow.prefix.LogicalFrontiers(); after.S != before.S {
					t.Fatalf("early cleanup sealed a cut: %+v -> %+v", before, after)
				}
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			if err := opening.Close(); err != nil {
				t.Fatal(err)
			}
			if err := cache.Close(); err != nil {
				t.Fatal(err)
			}
			if got := cache.COWMemoryStats(); got.TotalBytes != 0 {
				t.Fatalf("checked close retained resources: %+v", got)
			}
		})
	}
}

func TestCOWNativeRootCustodyCloseJoinsAcceptedTransferAndReportsFailure(t *testing.T) {
	registerNativeBridgeTestCommand()
	cache, backend := nativeBridgeFixture(t, true)
	opening, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer opening.Close() // Close may leave only this reader's cleanup for later.
	atDetach, detach := make(chan struct{}), make(chan struct{})
	atFinish, finish := make(chan struct{}), make(chan struct{})
	resumeDetach := sync.OnceFunc(func() { close(detach) })
	resumeFinish := sync.OnceFunc(func() { close(finish) })
	failure := errors.New("original returned native cleanup failed")
	ledger := &cowRootCustodyTestLedger{
		onDetach: func() { close(atDetach); <-detach },
		onFinish: func() error { close(atFinish); <-finish; return failure },
	}
	publication, intent, lease := prepareTestRootCustody(t, cache, backend, opening, true, true, ledger)
	writeDone := make(chan error, 1)
	go func() {
		result, err := backend.WriteNativePublication(publication, intent, false)
		if err == nil && !result.LogicalInstalled {
			err = errors.New("write did not install the canonical cut")
		}
		writeDone <- err
	}()
	defer resumeDetach()
	defer resumeFinish()
	waitRootCustodyTestEvent(t, atDetach, "accepted handoff outside the writer gates")
	if !cache.cow.writerMu.TryLock() {
		t.Fatal("accepted custody detached under COW writer exclusion")
	}
	cache.cow.writerMu.Unlock()
	if !cache.writeMu.TryLock() {
		t.Fatal("accepted custody detached while write admission was held")
	}
	cache.writeMu.Unlock()
	admissionGap := cache.cow.semanticMu.TryLock()
	if admissionGap {
		cache.cow.semanticMu.Unlock()
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- cache.Close() }()
	if admissionGap {
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before accepted transfer and lost its cleanup failure: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("accepted transfer left a gap before the Close cleanup latch")
		}
	}
	deadline := time.After(5 * time.Second)
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !cache.closing.Load() {
		select {
		case <-deadline:
			t.Fatal("Close did not start during accepted handoff")
		case <-poll.C:
		}
	}
	resumeDetach()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relaxed ACK waited for the destructor")
	}
	waitRootCustodyTestEvent(t, atFinish, "accepted destructor")
	waitRootCustodyTestEvent(t, cache.closeCh, "Close worker join")
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while accepted destruction was pending: %v", err)
	default:
	}
	resumeFinish()
	select {
	case err := <-closeDone:
		if !errors.Is(err, failure) || !errors.Is(cache.backgroundError(), failure) {
			t.Fatalf("Close lost original cleanup error: close=%v background=%v", err, cache.backgroundError())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return the accepted cleanup failure")
	}
	cache.cow.semanticMu.Lock()
	parked := cache.cow.semanticFailed == &ledger.record && ledger.record.Work == ledger && ledger.record.Err == failure
	cache.cow.semanticMu.Unlock()
	if !parked || ledger.detach != 1 || ledger.destroy != 1 || lease.Resize(testRootCustodyCharge) != nil {
		t.Fatalf("failed original custody lost: parked=%t detach=%d destroy=%d", parked, ledger.detach, ledger.destroy)
	}
}
