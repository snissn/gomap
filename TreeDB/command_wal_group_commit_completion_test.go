package treedb

import (
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Both accounting views describe the same capture-through-ready interval. They
// must consume one elapsed sample rather than successively sampling the clock.
func TestPublicCommandWALIdleCompletionSharesElapsedSample(t *testing.T) {
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSet, Key: []byte("completion-accounting"), Value: []byte("value"), Revision: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	group := &db.commandWALGroupCommit
	before := group.waitNsTotal.Load()
	var publication publicCommandWALPublication
	timing, err := db.appendPublicRawKVCommandPayloadMeasured(payload, true, &publication)
	db.finishPublicCommandWALGroupPublication(publication, err)
	if err != nil {
		t.Fatal(err)
	}
	if !timing.GroupCommitWaitObserved || timing.GroupCommitWait <= 0 {
		t.Fatalf("unobserved completion interval: %+v", timing)
	}
	if got := time.Duration(group.waitNsTotal.Load() - before); got != timing.GroupCommitWait {
		t.Fatalf("completion interval sampled twice: counter=%s request=%s", got, timing.GroupCommitWait)
	}
	if group.waitNsMax.Load() != uint64(timing.GroupCommitWait) {
		t.Fatal("maximum wait did not use the same elapsed sample")
	}
	if group.syncs.Load() != 1 || group.completedTicket != 1 || group.publishTicket != 2 {
		t.Fatalf("completion/publication frontier: syncs=%d completed=%d publish=%d", group.syncs.Load(), group.completedTicket, group.publishTicket)
	}
}

// Exercise the common completion predicate with real appends and capture, but
// retain the predecessor's publication ticket. Sync completion alone cannot
// return the successor's ACK, and awaiting publication must not sync again.
func TestPublicCommandWALCompletionRetainsPriorPublicationWait(t *testing.T) {
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var registrations [2]publicCommandWALRegistration
	for i := range registrations {
		payload, encodeErr := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
			Op: commitlog.RawKVOpSet, Key: []byte{byte('a' + i)}, Value: []byte("value"), Revision: 1,
		}})
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		registrations[i], err = db.registerPublicCommandWAL(len(payload), nil, true, nil, func(captured bool) (uint64, error) {
			return db.backend.AppendRawKVBatchPayloadCommandWALTrustedWithMode(payload, publicRawKVCommandWALAppendMode(true, captured))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		db.finishPublicCommandWALGroupPublication(registrations[0].publicCommandWALPublication, nil)
		db.finishPublicCommandWALGroupPublication(registrations[1].publicCommandWALPublication, nil)
	}()
	group := &db.commandWALGroupCommit
	db.commandWALPublicOperationGate.Lock()
	db.commandWALPublicPublishMu.Lock()
	group.mu.Lock()
	batch := group.detachPendingLocked()
	group.mu.Unlock()
	group.captureGroup(db, &batch)
	db.commandWALPublicPublishMu.Unlock()
	db.commandWALPublicOperationGate.Unlock()
	ready, err := group.syncAndCompleteGroup(db, &batch, registrations[1].ticket)
	if err != nil || ready {
		t.Fatalf("successor ready=%t err=%v before prior publication", ready, err)
	}
	if group.completedTicket != 2 || group.publishTicket != 1 || group.syncs.Load() != 1 {
		t.Fatalf("completion advanced publication: completed=%d publish=%d syncs=%d", group.completedTicket, group.publishTicket, group.syncs.Load())
	}
	done := make(chan error, 1)
	go func() {
		_, awaitErr := group.awaitRegistered(db, registrations[1].ticket, false, false, true)
		done <- awaitErr
	}()
	select {
	case err := <-done:
		t.Fatalf("ACK before prior publication: %v", err)
	default:
	}
	db.finishPublicCommandWALGroupPublication(registrations[0].publicCommandWALPublication, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prior publication did not release successor")
	}
	db.finishPublicCommandWALGroupPublication(registrations[1].publicCommandWALPublication, nil)
	if group.syncs.Load() != 1 || group.publishTicket != 3 {
		t.Fatal("publication await resynced or advanced the wrong ticket")
	}
}

// The fused eligibility check preceded physical I/O. A hook installed at that
// cut must still run outside the completion lock, before returning to publish.
func TestPublicCommandWALIdleCompletionRetainsLateWaitHook(t *testing.T) {
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	group := &db.commandWALGroupCommit
	calls := 0
	installed := false
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && e.LSN != 0 && !installed {
			installed = true
			group.mu.Lock()
			group.testAfterWait = func(ticket uint64) {
				group.mu.Lock()
				defer group.mu.Unlock()
				calls++
				if ticket != 1 || group.completedTicket != 1 || group.publishTicket != 1 || group.leaderActive {
					t.Errorf("wait hook observed wrong completion/publication cut")
				}
			}
			group.mu.Unlock()
		}
		return nil
	})
	err = db.SetSync([]byte("late-wait-hook"), []byte("value"))
	restore()
	if err != nil {
		t.Fatal(err)
	}
	if !installed || calls != 1 || group.syncs.Load() != 1 {
		t.Fatalf("late hook installed=%t calls=%d syncs=%d", installed, calls, group.syncs.Load())
	}
	requireRawKVValue(t, db, []byte("late-wait-hook"), []byte("value"))
}

func TestPublicCommandWALIdleCompletionFailureReturnsNoPublication(t *testing.T) {
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	injected := errors.New("completion physical sync failed")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && e.LSN != 0 {
			return injected
		}
		return nil
	})
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSet, Key: []byte("failed-completion"), Value: []byte("value"), Revision: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var publication publicCommandWALPublication
	timing, err := db.appendPublicRawKVCommandPayloadMeasured(payload, true, &publication)
	restore()
	if !errors.Is(err, injected) || publication.ticket != 0 {
		t.Fatalf("failed completion publication=%d err=%v", publication.ticket, err)
	}
	group := &db.commandWALGroupCommit
	if group.leaderActive || group.terminalErr == nil || group.publishTicket != 1 || group.syncs.Load() != 1 {
		t.Fatal("failed completion leaked leader or advanced publication")
	}
	if !timing.GroupCommitWaitObserved || group.waitNsTotal.Load() != uint64(timing.GroupCommitWait) {
		t.Fatal("failed completion lost shared elapsed accounting")
	}
	if err := db.SetSync([]byte("after-completion-failure"), []byte("value")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("later write after failed completion: %v", err)
	}
}
