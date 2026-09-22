package db

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestPublicCommandWALPathsPreserveTeardownBeforeRawLockOrder(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *DB) func() error
	}{
		{
			name: "noop_publish",
			prepare: func(t *testing.T, database *DB) func() error {
				intent := mustRawKVCommandWALIntent(t, database, "command-wal/lock-order", "value")
				return func() error { return database.PublishCommandWALNoop(intent, false) }
			},
		},
		{
			name: "raw_single_append",
			prepare: func(_ *testing.T, database *DB) func() error {
				return func() error {
					_, err := database.AppendRawKVSingleCommandWAL(commitlog.RawKVOperation{
						Op: commitlog.RawKVOpSet, Key: []byte("command-wal/lock-order"), Value: []byte("value"),
					}, false)
					return err
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}

			publishAtRawBarrier := make(chan struct{})
			releaseRawBarrier := make(chan struct{})
			var holdBarrier sync.Once
			unregister := database.RegisterCommandWALRawPublishBarrier(func() error {
				holdBarrier.Do(func() {
					close(publishAtRawBarrier)
					<-releaseRawBarrier
				})
				return nil
			})
			defer unregister()

			publish := test.prepare(t, database)
			publishDone := make(chan error, 1)
			go func() { publishDone <- publish() }()
			select {
			case <-publishAtRawBarrier:
			case <-time.After(5 * time.Second):
				t.Fatal("command-WAL path did not reach the raw publish barrier")
			}

			// Model Batch.write's established teardown -> raw order while this
			// publisher owns the raw lock. This reader must be able to drain after
			// Close queues an exclusive teardown request; the publisher must not
			// try to acquire a new read lease while still holding raw.
			batchPinned := make(chan struct{})
			batchDone := make(chan struct{})
			go func() {
				database.teardownMu.RLock()
				close(batchPinned)
				unlockRaw := database.lockCommandWALRawPublish()
				unlockRaw()
				database.teardownMu.RUnlock()
				close(batchDone)
			}()
			<-batchPinned

			closeDone := make(chan error, 1)
			go func() { closeDone <- database.Close() }()
			deadline := time.Now().Add(5 * time.Second)
			for database.teardownMu.TryRLock() {
				database.teardownMu.RUnlock()
				if time.Now().After(deadline) {
					t.Fatal("Close did not queue at the teardown gate")
				}
				time.Sleep(time.Millisecond)
			}
			close(releaseRawBarrier)

			select {
			case err := <-publishDone:
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Fatalf("command-WAL path: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("command-WAL path deadlocked acquiring teardown while holding raw publish")
			}
			select {
			case <-batchDone:
			case <-time.After(5 * time.Second):
				t.Fatal("batch-order participant did not drain after command-WAL path")
			}
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not finish after command-WAL publishers drained")
			}
		})
	}
}

func TestRootPublicationPathsPinTeardownThroughPostWork(t *testing.T) {
	tests := []struct {
		name       string
		commandWAL bool
		run        func(*testing.T, *DB) error
	}{
		{
			name: "Commit",
			run: func(_ *testing.T, database *DB) error {
				return database.ForceCommit(database.State().RootPageID)
			},
		},
		{
			name: "CompactIndex",
			run: func(_ *testing.T, database *DB) error {
				return database.CompactIndex()
			},
		},
		{
			name: "PublishOrderedRootIterator",
			run: func(t *testing.T, database *DB) error {
				_, err := database.PublishOrderedRootIterator(
					0,
					mustFrozenSystemMemtable(t, "ordered/root", "value").NewIterator(nil, nil),
				)
				return err
			},
		},
		{
			name: "PublishOrderedRootGroup",
			run: func(t *testing.T, database *DB) error {
				_, _, err := database.PublishOrderedRootGroup(
					mustFrozenSystemMemtable(t, "group/system", "value").NewIterator(nil, nil),
					nil,
				)
				return err
			},
		},
		{
			name: "PublishOrderedRootDeltaGroupWithSystemBuilder",
			run: func(t *testing.T, database *DB) error {
				_, _, err := database.PublishOrderedRootDeltaGroupWithSystemBuilder(
					nil,
					func([]uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenSystemMemtable(t, "group/system-builder", "value").NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
		{
			name: "PublishOrderedRootDeltaGroupWithSystemDeltaBuilder",
			run: func(t *testing.T, database *DB) error {
				_, _, err := database.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(
					nil,
					func([]uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenSystemMemtable(t, "group/system-delta", "value").NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
		{
			name:       "PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder",
			commandWAL: true,
			run: func(t *testing.T, database *DB) error {
				_, _, err := database.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
					nil,
					mustRawKVCommandWALIntent(t, database, "command-wal/group", "value"),
					func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenSystemMemtable(t, "group/command-wal-system-delta", "value").NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
		{
			name: "PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder",
			run: func(t *testing.T, database *DB) error {
				_, _, err := database.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(
					nil,
					func([]uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenSystemMemtable(t, "group/system-batch-delta", "value").NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
		{
			name: "PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder",
			run: func(t *testing.T, database *DB) error {
				_, _, err := database.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(
					nil,
					func() error { return nil },
					func([]uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenSystemMemtable(t, "group/serialized-system-batch-delta", "value").NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
		{
			name:       "PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder",
			commandWAL: true,
			run: func(t *testing.T, database *DB) error {
				_, _, err := database.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
					nil,
					mustRawKVCommandWALIntent(t, database, "command-wal/batch-group", "value"),
					func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenSystemMemtable(t, "group/command-wal-system-batch-delta", "value").NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
		{
			name: "PublishCommandWALRoots",
			run: func(_ *testing.T, database *DB) error {
				state := database.State()
				next := state.AppliedCommandLSN + 1
				return database.publishCommandWALRoots(
					state.RootPageID,
					state.SystemRootPageID,
					next,
					[]CommandWALLSNRange{{First: next, Last: next}},
					true,
				)
			},
		},
		{
			name:       "PublishCommandWALNoop",
			commandWAL: true,
			run: func(t *testing.T, database *DB) error {
				return database.PublishCommandWALNoop(
					mustRawKVCommandWALIntent(t, database, "command-wal/noop", "value"),
					false,
				)
			},
		},
		{
			name:       "PublishStagedCommandWALNoop",
			commandWAL: true,
			run: func(t *testing.T, database *DB) error {
				unlock := database.LockCommandWALStaging()
				defer unlock()
				if err := database.runCommandWALRawPublishBarriers(); err != nil {
					return err
				}
				intent := mustRawKVCommandWALIntent(t, database, "command-wal/staged-noop", "value")
				if _, err := database.AppendStagedCommandWALIntent(intent, false); err != nil {
					return err
				}
				return database.PublishStagedCommandWALNoop(intent, false)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if test.commandWAL {
				enableCommandWALFormat(t, dir)
			}
			database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := database.SetSync([]byte("seed"), []byte("value")); err != nil {
				t.Fatal(err)
			}

			prepared := make(chan struct{})
			releasePublish := make(chan struct{})
			database.testDurableRootCandidatePreparedHook = func() {
				close(prepared)
				<-releasePublish
			}
			teardownEntered := make(chan struct{})
			database.registerCaptureTeardownHook(func() error {
				close(teardownEntered)
				return nil
			})

			publishDone := make(chan error, 1)
			go func() { publishDone <- test.run(t, database) }()
			select {
			case <-prepared:
			case <-time.After(5 * time.Second):
				t.Fatal("publication did not reach prepared candidate")
			}

			missingTeardownLease := database.teardownMu.TryLock()
			if missingTeardownLease {
				database.teardownMu.Unlock()
			}
			closeDone := make(chan error, 1)
			go func() { closeDone <- database.Close() }()
			// Wait until Close is queued on (or owns) the exclusive teardown
			// gate. With the required shutdown ordering, the prepared publisher's
			// read lease keeps the coordinator alive until releasePublish. Without
			// that ordering, Close stops the coordinator before it reaches this
			// same gate and the subsequent enqueue fails deterministically.
			deadline := time.Now().Add(5 * time.Second)
			for database.teardownMu.TryRLock() {
				database.teardownMu.RUnlock()
				if time.Now().After(deadline) {
					t.Fatal("Close did not reach the teardown gate")
				}
				time.Sleep(time.Millisecond)
			}
			close(releasePublish)
			if err := <-publishDone; err != nil {
				t.Fatalf("publication: %v", err)
			}
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not finish after publication released its teardown lease")
			}
			if missingTeardownLease {
				t.Fatal("prepared publication did not hold a teardown read lease through post-work")
			}
			select {
			case <-teardownEntered:
			default:
				t.Fatal("Close did not run capture teardown after publication completed")
			}
		})
	}
}
