package collections

import (
	"errors"
	"fmt"
	"runtime"
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
			handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
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
		handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
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
