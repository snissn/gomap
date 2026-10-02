package collections

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCollectionCommandWALPreparedOwnerRetainsAdmissionThroughFinalize(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}})
	d := openCollectionCommandWALDB(t, dir)
	safeClose := true
	defer func() {
		if safeClose {
			_ = d.Close()
		} else {
			stacks := make([]byte, 128<<10)
			n := runtime.Stack(stacks, true)
			t.Logf("skipping Close after blocked prepared-owner boundary:\n%s", stacks[:n])
		}
	}()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{{ID: []byte("u1"), Document: []byte(`{"name":"Ada"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := commandwalapply.CollectionInsertBatchByIDFrame(payload)
	if err != nil {
		t.Fatal(err)
	}
	appended, execute, finalized, exit := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-execute:
		default:
			close(execute)
		}
		select {
		case <-exit:
		default:
			close(exit)
		}
	}()
	ownerDone := make(chan error, 1)
	var escaped *CommandWALAdmittedCollection
	safeClose = false
	go func() {
		ownerDone <- col.WithPreparedCommandWALMutation(func(owner *CommandWALAdmittedCollection) error {
			escaped = owner
			coord := col.collectionSchemaCoordinator()
			if coord.schemaMu.TryLock() {
				coord.schemaMu.Unlock()
				return errors.New("schema admission absent before Append")
			}
			if coord.nativeVectorAdmissionMu.TryLock() {
				coord.nativeVectorAdmissionMu.Unlock()
				return errors.New("native admission absent before Append")
			}
			if mutation, ok := col.tryLockMutation(); ok {
				mutation.Unlock()
				return errors.New("actual mutation absent before Append")
			}
			if d.CommandWALNextLSN() != 1 {
				return fmt.Errorf("pre-Append next LSN=%d, want 1", d.CommandWALNextLSN())
			}
			appendOptions, err := owner.CommandWALAppendOptions(true)
			if err != nil {
				return err
			}
			handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, appendOptions)
			if err != nil {
				return err
			}
			complete := false
			defer func() {
				if !complete {
					commandwalapply.Abort(d, handle)
				}
			}()
			close(appended)
			<-execute
			if _, err := owner.InsertBatchWithCommandWALIntent([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"name":"Ada"}`)}, false, handle.CommandWALIntent()); err != nil {
				return err
			}
			if _, err := commandwalapply.Finalize(d, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true}); err != nil {
				return err
			}
			complete = true
			close(finalized)
			<-exit
			return nil
		})
	}()
	waitCollectionCommandWALSignal(t, appended, "prepared owner holding schema/native/mutation before actual Append")
	ordinaryDone := make(chan error, 1)
	go func() { _, err := col.Insert([]byte("u2"), []byte(`{"name":"Grace"}`)); ordinaryDone <- err }()
	waitPreparedOwnerCondition(t, func() bool {
		col.writeDomain.nativeVectorActiveMu.Lock()
		defer col.writeDomain.nativeVectorActiveMu.Unlock()
		return col.writeDomain.nativeVectorActive == 2
	}, "ordinary insert admitted and blocked on prepared mutation")
	writerDone := make(chan struct{})
	go func() { unlock := col.lockNativeVectorAdmissionWrite(); unlock(); close(writerDone) }()
	waitPreparedOwnerCondition(t, func() bool {
		mu := col.nativeVectorAdmissionMutex()
		if mu.TryRLock() {
			mu.RUnlock()
			return false
		}
		return true
	}, "exclusive native writer queued behind assigned owner and ordinary reader")
	close(execute)
	waitCollectionCommandWALSignal(t, finalized, "assigned executor reaching real Finalize behind queued native writer")
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("applied after Finalize=%d, want 1", got)
	}
	if mutation, ok := col.tryLockMutation(); ok {
		mutation.Unlock()
		t.Fatal("mutation released before prepared callback returned")
	}
	select {
	case err := <-ordinaryDone:
		t.Fatalf("ordinary insert escaped prepared callback: %v", err)
	default:
	}
	select {
	case <-writerDone:
		t.Fatal("native writer escaped prepared callback")
	default:
	}
	close(exit)
	if err := waitCollectionCommandWALErr(t, ownerDone, "prepared owner callback returning"); err != nil {
		t.Fatal(err)
	}
	if err := waitCollectionCommandWALErr(t, ordinaryDone, "ordinary insert after prepared owner exit"); err != nil {
		t.Fatal(err)
	}
	waitCollectionCommandWALSignal(t, writerDone, "native writer after prepared owner exit")
	safeClose = true
	if err := escaped.validate(); err == nil {
		t.Fatal("escaped facade still active after callback")
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	if applied, next := d.State().AppliedCommandLSN, d.CommandWALNextLSN(); applied != 2 || next != 3 {
		t.Fatalf("coverage applied=%d next=%d, want 2/3", applied, next)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	safeClose = false
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	col, err = NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	assertCollectionDocument(t, col, "u2", `{"name":"Grace"}`)
	if applied, next := reopened.State().AppliedCommandLSN, reopened.CommandWALNextLSN(); applied != 2 || next != 3 {
		t.Fatalf("reopen coverage applied=%d next=%d, want 2/3", applied, next)
	}
	safeClose = true
}

func waitPreparedOwnerCondition(t *testing.T, condition func() bool, boundary string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("blocked prepared-owner boundary: %s", boundary)
		}
		runtime.Gosched()
	}
}

func TestCollectionCommandWALPreparedOwnerRetainsMutationThroughAbort(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{{ID: []byte("u1"), Document: []byte(`{"name":"Ada"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := commandwalapply.CollectionInsertBatchByIDFrame(payload)
	if err != nil {
		t.Fatal(err)
	}
	err = col.WithPreparedCommandWALMutation(func(owner *CommandWALAdmittedCollection) error {
		appendOptions, err := owner.CommandWALAppendOptions(true)
		if err != nil {
			return err
		}
		handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, appendOptions)
		if err != nil {
			return err
		}
		commandwalapply.Abort(d, handle)
		if mutation, ok := col.tryLockMutation(); ok {
			mutation.Unlock()
			return errors.New("actual mutation released by Abort before callback exit")
		}
		coord := col.collectionSchemaCoordinator()
		if coord.schemaMu.TryLock() {
			coord.schemaMu.Unlock()
			return errors.New("schema released by Abort before callback exit")
		}
		if coord.nativeVectorAdmissionMu.TryLock() {
			coord.nativeVectorAdmissionMu.Unlock()
			return errors.New("native admission released by Abort before callback exit")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if mutation, ok := col.tryLockMutation(); !ok {
		t.Fatal("mutation not released after aborted callback")
	} else {
		mutation.Unlock()
	}
	if err := d.CheckCommandWALPublishReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("Abort readiness=%v, want recovery required", err)
	}
	if d.State().AppliedCommandLSN != 0 || d.CommandWALNextLSN() != 2 {
		t.Fatalf("Abort coverage applied=%d next=%d", d.State().AppliedCommandLSN, d.CommandWALNextLSN())
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	col, err = NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	if reopened.State().AppliedCommandLSN != 1 || reopened.CommandWALNextLSN() != 2 {
		t.Fatalf("recovered Abort coverage applied=%d next=%d", reopened.State().AppliedCommandLSN, reopened.CommandWALNextLSN())
	}
}

func TestCollectionCommandWALPreparedOwnerUnusedStagingCleanup(t *testing.T) {
	for _, outcome := range []string{"return_nil", "callback_error", "invalid_append"} {
		t.Run(outcome, func(t *testing.T) {
			dir := prepareCollectionCommandWALDir(t, CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}})
			d := openCollectionCommandWALDB(t, dir)
			safeClose := true
			defer func() {
				if safeClose {
					_ = d.Close()
				}
			}()
			col, err := NewCollectionManager(d).OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			callbackErr := errors.New("callback rejected before assignment")
			err = col.WithPreparedCommandWALMutation(func(owner *CommandWALAdmittedCollection) error {
				switch outcome {
				case "return_nil":
					return nil
				case "callback_error":
					return callbackErr
				default:
					options, err := owner.CommandWALAppendOptions(false)
					if err != nil {
						return err
					}
					_, _, err = commandwalapply.Append(d, commandwalapply.LoweredFrame{}, commandwalapply.ApplyMetadata{}, options)
					return err
				}
			})
			if outcome == "return_nil" && err != nil {
				t.Fatal(err)
			}
			if outcome == "callback_error" && !errors.Is(err, callbackErr) {
				t.Fatalf("callback error=%v", err)
			}
			if outcome == "invalid_append" && err == nil {
				t.Fatal("invalid append accepted")
			}
			if applied, next := d.State().AppliedCommandLSN, d.CommandWALNextLSN(); applied != 0 || next != 1 {
				t.Fatalf("unassigned coverage=%d/%d", applied, next)
			}
			done := make(chan error, 1)
			safeClose = false
			go func() { _, err := col.Insert([]byte("u1"), []byte("{\"name\":\"Ada\"}")); done <- err }()
			if err := waitCollectionCommandWALErr(t, done, "ordinary insert after unused prepared staging"); err != nil {
				t.Fatal(err)
			}
			safeClose = true
			if err := col.Flush(); err != nil {
				t.Fatal(err)
			}
			if applied, next := d.State().AppliedCommandLSN, d.CommandWALNextLSN(); applied != 1 || next != 2 {
				t.Fatalf("coverage=%d/%d want 1/2", applied, next)
			}
		})
	}
}

// Both foreign states are reachable before acquiring the prepared guard. Once
// acquired, the callback must keep raw until its Append handle releases it.
func TestCollectionCommandWALPreparedOwnerDrainsForeignPendingAndReservation(t *testing.T) {
	for _, publishedReservation := range []bool{false, true} {
		t.Run(fmt.Sprintf("published_reservation_%t", publishedReservation), func(t *testing.T) {
			dir := prepareCollectionCommandWALDir(t, CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}})
			d := openCollectionCommandWALDB(t, dir)
			safeClose := true
			defer func() {
				if safeClose {
					_ = d.Close()
				}
			}()
			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name:    "foreign",
				Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 100, DisableBufferedIndexedAsyncFlush: true},
				Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
			}); err != nil {
				t.Fatal(err)
			}
			col, err := mgr.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			foreign, err := mgr.OpenCollection("foreign")
			if err != nil {
				t.Fatal(err)
			}
			var releaseReservation func()
			if publishedReservation {
				unlockRaw := d.LockCommandWALStaging()
				releaseReservation, err = foreign.lockCommandWALStageCoordinatorWithHeldRawPublishLock()
				unlockRaw()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { releaseReservation() }()
			}
			if _, err := foreign.Insert([]byte("f1"), []byte("{\"city\":\"hnl\"}")); err != nil {
				t.Fatal(err)
			}
			if publishedReservation {
				if err := foreign.Flush(); err != nil {
					t.Fatal(err)
				}
				foreign.writeDomain.mu.RLock()
				pending := collectionCommandWALDomainPendingLocked(foreign.writeDomain)
				foreign.writeDomain.mu.RUnlock()
				if pending || !collectionCommandWALDomainStageReserved(foreign.writeDomain) {
					t.Fatal("fixture did not retain published foreign reservation")
				}
			}
			next := d.CommandWALNextLSN()
			payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{{ID: []byte("u1"), Document: []byte("{\"name\":\"Ada\"}")}})
			if err != nil {
				t.Fatal(err)
			}
			frame, err := commandwalapply.CollectionInsertBatchByIDFrame(payload)
			if err != nil {
				t.Fatal(err)
			}
			// Observe the real admission finalizer during pre-assignment handoff;
			// no test acquires a foreign raw guard while the owner holds it.
			yielded := make(chan struct{}, 1)
			acquire := func() func() {
				release := col.lockVectorIndexCoverageMutation()
				return func() {
					release()
					select {
					case yielded <- struct{}{}:
					default:
					}
				}
			}
			done := make(chan error, 1)
			safeClose = false
			go func() {
				done <- col.withPreparedCommandWALMutation(acquire, false, func(owner *CommandWALAdmittedCollection) error {
					foreign.writeDomain.mu.RLock()
					pending := collectionCommandWALDomainPendingLocked(foreign.writeDomain)
					foreign.writeDomain.mu.RUnlock()
					if pending || collectionCommandWALDomainStageReserved(foreign.writeDomain) {
						return errors.New("foreign owner survived pre-Append drain")
					}
					options, err := owner.CommandWALAppendOptions(true)
					if err != nil {
						return err
					}
					handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, options)
					if err != nil {
						return err
					}
					defer commandwalapply.Abort(d, handle)
					if handle.LSN() != next {
						return fmt.Errorf("assigned LSN=%d want %d", handle.LSN(), next)
					}
					if _, err := owner.InsertBatchWithCommandWALIntent([][]byte{[]byte("u1")}, [][]byte{[]byte("{\"name\":\"Ada\"}")}, false, handle.CommandWALIntent()); err != nil {
						return err
					}
					_, err = commandwalapply.Finalize(d, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
					return err
				})
			}()
			waitCollectionCommandWALSignal(t, yielded, "prepared admission yielded for existing foreign owner")
			if releaseReservation != nil {
				releaseReservation()
			}
			if err := waitCollectionCommandWALErr(t, done, "prepared owner after foreign drain"); err != nil {
				t.Fatal(err)
			}
			safeClose = true
			assertCollectionDocument(t, col, "u1", "{\"name\":\"Ada\"}")
			assertCollectionDocument(t, foreign, "f1", "{\"city\":\"hnl\"}")
			if applied, gotNext := d.State().AppliedCommandLSN, d.CommandWALNextLSN(); applied != next || gotNext != next+1 {
				t.Fatalf("coverage=%d/%d want %d/%d", applied, gotNext, next, next+1)
			}
			if err := d.CheckCommandWALPublishReady(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCollectionCommandWALPreparedOwnerExcludesForeignStagingBeforeAppend(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}})
	d := openCollectionCommandWALDB(t, dir)
	safeClose := true
	defer func() {
		if safeClose {
			_ = d.Close()
		}
	}()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "foreign",
		Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 100, DisableBufferedIndexedAsyncFlush: true},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := mgr.OpenCollection("foreign")
	if err != nil {
		t.Fatal(err)
	}
	beforeRaw, withRaw := make(chan struct{}), make(chan struct{})
	var beforeOnce, rawOnce sync.Once
	hook := func(domain *collectionWriteDomain, rawHeld bool) {
		if domain != foreign.writeDomain {
			return
		}
		if rawHeld {
			rawOnce.Do(func() { close(withRaw) })
		} else {
			beforeOnce.Do(func() { close(beforeRaw) })
		}
	}
	mgr.testCommandWALInsertStageMutationHook.Store(&hook)
	defer mgr.testCommandWALInsertStageMutationHook.Store(nil)
	foreignDone := make(chan error, 1)
	ownerDone := make(chan error, 1)
	startForeign := make(chan struct{})
	next := d.CommandWALNextLSN()
	safeClose = false
	go func() {
		ownerDone <- col.WithPreparedCommandWALMutation(func(owner *CommandWALAdmittedCollection) error {
			close(startForeign)
			select {
			case <-beforeRaw:
			case <-time.After(10 * time.Second):
				return errors.New("foreign insert did not reach raw boundary")
			}
			// The existing staging hook establishes the exact competing path.
			// A bounded negative observation detects early raw release.
			select {
			case <-withRaw:
				return errors.New("foreign staging entered prepared pre-Append window")
			case <-time.After(50 * time.Millisecond):
			}
			payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{{ID: []byte("u1"), Document: []byte("{\"name\":\"Ada\"}")}})
			if err != nil {
				return err
			}
			frame, err := commandwalapply.CollectionInsertBatchByIDFrame(payload)
			if err != nil {
				return err
			}
			options, err := owner.CommandWALAppendOptions(true)
			if err != nil {
				return err
			}
			handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, options)
			if err != nil {
				return err
			}
			defer commandwalapply.Abort(d, handle)
			if _, err := owner.InsertBatchWithCommandWALIntent([][]byte{[]byte("u1")}, [][]byte{[]byte("{\"name\":\"Ada\"}")}, false, handle.CommandWALIntent()); err != nil {
				return err
			}
			_, err = commandwalapply.Finalize(d, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
			return err
		})
	}()
	waitCollectionCommandWALSignal(t, startForeign, "prepared callback before Append")
	go func() { _, err := foreign.Insert([]byte("f1"), []byte("{\"city\":\"hnl\"}")); foreignDone <- err }()
	if err := waitCollectionCommandWALErr(t, ownerDone, "prepared Append before foreign staging"); err != nil {
		t.Fatal(err)
	}
	if err := waitCollectionCommandWALErr(t, foreignDone, "foreign staging after prepared Finalize"); err != nil {
		t.Fatal(err)
	}
	safeClose = true
	waitCollectionCommandWALSignal(t, withRaw, "foreign raw staging after Finalize")
	if err := foreign.Flush(); err != nil {
		t.Fatal(err)
	}
	assertCollectionDocument(t, col, "u1", "{\"name\":\"Ada\"}")
	assertCollectionDocument(t, foreign, "f1", "{\"city\":\"hnl\"}")
	if applied, gotNext := d.State().AppliedCommandLSN, d.CommandWALNextLSN(); applied != next+1 || gotNext != next+2 {
		t.Fatalf("coverage=%d/%d want %d/%d", applied, gotNext, next+1, next+2)
	}
	if err := d.CheckCommandWALPublishReady(); err != nil {
		t.Fatal(err)
	}
}

func TestCollectionCommandWALPreparedOwnerAbandonedAppendRequiresRecovery(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}})
	d := openCollectionCommandWALDB(t, dir)
	safeClose := true
	defer func() {
		if safeClose {
			_ = d.Close()
		}
	}()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	frame, err := commandwalapply.TestNoopFrame()
	if err != nil {
		t.Fatal(err)
	}
	err = col.WithPreparedCommandWALMutation(func(owner *CommandWALAdmittedCollection) error {
		options, err := owner.CommandWALAppendOptions(false)
		if err != nil {
			return err
		}
		_, _, err = commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, options)
		// Deliberately violate the callback's Finalize/Abort contract. The
		// wrapper must release raw but leave this frame recovery-owned.
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CheckCommandWALPublishReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("abandoned frame readiness=%v", err)
	}
	if applied, next := d.State().AppliedCommandLSN, d.CommandWALNextLSN(); applied != 0 || next != 2 {
		t.Fatalf("coverage=%d/%d want 0/2", applied, next)
	}
	rawReleased := make(chan struct{})
	safeClose = false
	go func() { unlock := d.LockCommandWALStaging(); unlock(); close(rawReleased) }()
	waitCollectionCommandWALSignal(t, rawReleased, "abandoned prepared callback releasing raw and teardown")
	safeClose = true
}

func TestCollectionCommandWALOrdinaryAppendDrainsPublishedForeignReservation(t *testing.T) {
	for _, operation := range []string{"noop", "catalog_create"} {
		t.Run(operation, func(t *testing.T) {
			dir := prepareCollectionCommandWALDir(t, CollectionMeta{Name: "foreign", Options: CollectionOptions{
				DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 100, DisableBufferedIndexedAsyncFlush: true,
			}, Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}}})
			d := openCollectionCommandWALDB(t, dir)
			safeClose := true
			defer func() {
				if safeClose {
					_ = d.Close()
				}
			}()
			mgr := NewCollectionManager(d)
			foreign, err := mgr.OpenCollection("foreign")
			if err != nil {
				t.Fatal(err)
			}
			unlockRaw := d.LockCommandWALStaging()
			releaseReservation, err := foreign.lockCommandWALStageCoordinatorWithHeldRawPublishLock()
			unlockRaw()
			if err != nil {
				t.Fatal(err)
			}
			defer releaseReservation()
			if _, err := foreign.Insert([]byte("f1"), []byte("{\"city\":\"hnl\"}")); err != nil {
				t.Fatal(err)
			}
			if err := foreign.Flush(); err != nil {
				t.Fatal(err)
			}
			foreign.writeDomain.mu.RLock()
			pending := collectionCommandWALDomainPendingLocked(foreign.writeDomain)
			foreign.writeDomain.mu.RUnlock()
			if pending || !collectionCommandWALDomainStageReserved(foreign.writeDomain) {
				t.Fatal("fixture did not retain published foreign reservation")
			}
			next := d.CommandWALNextLSN()
			draining := make(chan struct{})
			var drainOnce sync.Once
			hook := func(domain *collectionWriteDomain) {
				if domain == foreign.writeDomain {
					drainOnce.Do(func() { close(draining) })
				}
			}
			mgr.testCommandWALRawDomainDrainHook.Store(&hook)
			defer mgr.testCommandWALRawDomainDrainHook.Store(nil)
			done := make(chan error, 1)
			safeClose = false
			go func() {
				var handle commandwalapply.Handle
				defer func() { commandwalapply.Abort(d, handle) }()
				var err error
				if operation == "noop" {
					frame, frameErr := commandwalapply.TestNoopFrame()
					if frameErr != nil {
						done <- frameErr
						return
					}
					handle, _, err = commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
				} else {
					meta := CollectionMeta{Name: "created", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}
					normalized, normalizeErr := normalizeCollectionMeta(meta)
					if normalizeErr != nil {
						done <- normalizeErr
						return
					}
					encoded, encodeErr := encodeCollectionMeta(normalized)
					if encodeErr != nil {
						done <- encodeErr
						return
					}
					payload, payloadErr := commitlog.EncodeCatalogCreateCollectionPayload(meta.Name, encoded)
					if payloadErr != nil {
						done <- payloadErr
						return
					}
					frame, frameErr := commandwalapply.CatalogCreateCollectionFrame(payload)
					if frameErr != nil {
						done <- frameErr
						return
					}
					_, err = mgr.CreateCollectionWithPreparedCommandWALIntent(meta, func() (*backenddb.CommandWALIntent, error) {
						var appendErr error
						handle, _, appendErr = commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
						if appendErr != nil {
							return nil, appendErr
						}
						return handle.CommandWALIntent(), nil
					})
				}
				if err == nil && handle.LSN() != next {
					err = fmt.Errorf("assigned LSN=%d want %d", handle.LSN(), next)
				}
				if err == nil {
					_, err = commandwalapply.Finalize(d, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
				}
				done <- err
			}()
			waitCollectionCommandWALSignal(t, draining, "ordinary Append barrier observing published foreign reservation")
			if got := d.CommandWALNextLSN(); got != next {
				t.Fatalf("assigned before reservation drain: next=%d want %d", got, next)
			}
			releaseReservation()
			if err := waitCollectionCommandWALErr(t, done, "ordinary Append after foreign reservation release"); err != nil {
				t.Fatal(err)
			}
			safeClose = true
			assertCollectionDocument(t, foreign, "f1", "{\"city\":\"hnl\"}")
			if operation == "catalog_create" {
				if _, err := mgr.OpenCollection("created"); err != nil {
					t.Fatal(err)
				}
			}
			if applied, gotNext := d.State().AppliedCommandLSN, d.CommandWALNextLSN(); applied != next || gotNext != next+1 {
				t.Fatalf("coverage=%d/%d want %d/%d", applied, gotNext, next, next+1)
			}
			if err := d.CheckCommandWALPublishReady(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Reacquiring prepared admission after a foreign drain must publish any local
// prefix assigned while the owner's actual admission and mutation were absent.
func TestCollectionCommandWALPreparedOwnerReflushesAfterForeignDrain(t *testing.T) {
	for _, coveragePersistence := range []bool{false, true} {
		t.Run(fmt.Sprintf("coverage_persistence_%t", coveragePersistence), func(t *testing.T) {
			dir := prepareCollectionCommandWALDir(t, CollectionMeta{
				Name:    "users",
				Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 100, DisableBufferedIndexedAsyncFlush: true},
				Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
			})
			d := openCollectionCommandWALDB(t, dir)
			safeClose := true
			defer func() {
				if safeClose {
					_ = d.Close()
				}
			}()
			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name:    "foreign",
				Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 100, DisableBufferedIndexedAsyncFlush: true},
				Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
			}); err != nil {
				t.Fatal(err)
			}
			col, err := mgr.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			foreign, err := mgr.OpenCollection("foreign")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := foreign.Insert([]byte("f1"), []byte(`{"city":"hnl"}`)); err != nil {
				t.Fatal(err)
			}
			foreign.writeDomain.mu.RLock()
			foreignPending := collectionCommandWALDomainPendingLocked(foreign.writeDomain)
			foreign.writeDomain.mu.RUnlock()
			if !foreignPending {
				t.Fatal("fixture did not retain a real foreign pending prefix")
			}
			interveningLSN := d.CommandWALNextLSN()
			preparedLSN := interveningLSN + 1
			payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{{ID: []byte("prepared"), Document: []byte(`{"city":"sea"}`)}})
			if err != nil {
				t.Fatal(err)
			}
			frame, err := commandwalapply.CollectionInsertBatchByIDFrame(payload)
			if err != nil {
				t.Fatal(err)
			}
			acquisitions := 0
			var injectionErr error
			acquire := func() func() {
				acquisitions++
				if acquisitions == 2 {
					// Real foreign Drain has completed; both owner leases are
					// absent. Use the ordinary public buffered indexed route.
					if applied := d.State().AppliedCommandLSN; applied != interveningLSN-1 {
						injectionErr = fmt.Errorf("foreign drain applied=%d want %d", applied, interveningLSN-1)
					} else {
						_, injectionErr = col.Insert([]byte("intervening"), []byte(`{"city":"bos"}`))
					}
					if injectionErr == nil {
						col.writeDomain.mu.RLock()
						pending := collectionCommandWALDomainPendingLocked(col.writeDomain)
						col.writeDomain.mu.RUnlock()
						if !pending || d.State().AppliedCommandLSN != interveningLSN-1 || d.CommandWALNextLSN() != preparedLSN {
							injectionErr = errors.New("intervening indexed write did not retain its assigned unpublished prefix")
						}
					}
				}
				if !coveragePersistence {
					return col.lockVectorIndexCoverageMutation()
				}
				releaseAdmission := col.lockNativeVectorAdmissionWrite()
				releaseCoverage := col.lockVectorIndexCoveragePersistence()
				return func() { releaseCoverage(); releaseAdmission() }
			}
			done := make(chan error, 1)
			safeClose = false
			go func() {
				done <- col.withPreparedCommandWALMutation(acquire, coveragePersistence, func(owner *CommandWALAdmittedCollection) error {
					if injectionErr != nil {
						return injectionErr
					}
					if acquisitions != 2 {
						return fmt.Errorf("admission acquisitions=%d want 2", acquisitions)
					}
					// Fail before assignment on old source rather than poison
					// the DB to reproduce the known committed-prefix rejection.
					if applied, next := d.State().AppliedCommandLSN, d.CommandWALNextLSN(); applied != interveningLSN || next != preparedLSN {
						return fmt.Errorf("prepared pre-Append prefix applied=%d next=%d want %d/%d", applied, next, interveningLSN, preparedLSN)
					}
					options, err := owner.CommandWALAppendOptions(true)
					if err != nil {
						return err
					}
					handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, options)
					if err != nil {
						return err
					}
					defer commandwalapply.Abort(d, handle)
					if handle.LSN() != preparedLSN {
						return fmt.Errorf("prepared LSN=%d want %d", handle.LSN(), preparedLSN)
					}
					if _, err := owner.InsertBatchWithCommandWALIntent([][]byte{[]byte("prepared")}, [][]byte{[]byte(`{"city":"sea"}`)}, false, handle.CommandWALIntent()); err != nil {
						return err
					}
					_, err = commandwalapply.Finalize(d, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
					return err
				})
			}()
			err = waitCollectionCommandWALErr(t, done, "prepared owner reflush after real foreign drain")
			safeClose = true
			if err != nil {
				t.Fatal(err)
			}
			if applied, next := d.State().AppliedCommandLSN, d.CommandWALNextLSN(); applied != preparedLSN || next != preparedLSN+1 {
				t.Fatalf("finalized coverage=%d/%d want %d/%d", applied, next, preparedLSN, preparedLSN+1)
			}
			if err := d.CheckCommandWALPublishReady(); err != nil {
				t.Fatal(err)
			}
			assertCollectionDocument(t, col, "intervening", `{"city":"bos"}`)
			assertCollectionDocument(t, col, "prepared", `{"city":"sea"}`)
			assertCollectionIndexIDs(t, col, "city", "bos", "intervening")
			assertCollectionIndexIDs(t, col, "city", "sea", "prepared")
			assertCollectionDocument(t, foreign, "f1", `{"city":"hnl"}`)
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			safeClose = false
			var interveningFrames, preparedFrames int
			for _, got := range collectionCommandWALFrames(t, dir) {
				if got.LSN == interveningLSN {
					interveningFrames++
					if got.Kind != commitlog.CommandKindCollectionInsertBatchByID {
						t.Fatalf("intervening frame kind=%d", got.Kind)
					}
				}
				if got.LSN == preparedLSN {
					preparedFrames++
					if got.Kind != commitlog.CommandKindCollectionInsertBatchByID || got.PayloadFormat != commitlog.PayloadFormatCollectionInsertBatchByIDV1 {
						t.Fatalf("prepared frame kind/format=%d/%d", got.Kind, got.PayloadFormat)
					}
					decoded, err := commitlog.DecodeCollectionInsertBatchByIDPayload(got.Payload)
					if err != nil {
						t.Fatal(err)
					}
					if decoded.Collection != "users" || len(decoded.Documents) != 1 || string(decoded.Documents[0].ID) != "prepared" || string(decoded.Documents[0].Document) != `{"city":"sea"}` {
						t.Fatalf("prepared WAL payload=%+v", decoded)
					}
				}
			}
			if interveningFrames != 1 || preparedFrames != 1 {
				t.Fatalf("actual WAL frame counts=%d/%d want 1/1", interveningFrames, preparedFrames)
			}
			reopened := openCollectionCommandWALDB(t, dir)
			defer func() { _ = reopened.Close() }()
			reopenMgr := NewCollectionManager(reopened)
			col, err = reopenMgr.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			foreign, err = reopenMgr.OpenCollection("foreign")
			if err != nil {
				t.Fatal(err)
			}
			assertCollectionDocument(t, col, "intervening", `{"city":"bos"}`)
			assertCollectionDocument(t, col, "prepared", `{"city":"sea"}`)
			assertCollectionIndexIDs(t, col, "city", "bos", "intervening")
			assertCollectionIndexIDs(t, col, "city", "sea", "prepared")
			assertCollectionDocument(t, foreign, "f1", `{"city":"hnl"}`)
			if applied, next := reopened.State().AppliedCommandLSN, reopened.CommandWALNextLSN(); applied != preparedLSN || next != preparedLSN+1 {
				t.Fatalf("reopen coverage=%d/%d want %d/%d", applied, next, preparedLSN, preparedLSN+1)
			}
		})
	}
}
