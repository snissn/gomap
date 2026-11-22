package caching

import (
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Exercise only the cached write admission gate. No root publication,
// allocation refusal, or durability outcome is manufactured by this fixture.
func TestRawTxnRefreshWaitKeepsExistingAdmissionFence(t *testing.T) {
	poison := errors.New("terminal WAL publication readiness refusal")
	for _, tc := range []struct {
		name         string
		raw          bool
		postFrontier bool
		wantWait     bool
		clearRefresh bool
		close        bool
		poison       bool
		want         error
	}{
		{name: "ordinary-write-still-refuses", want: memtable.ErrCOWCapacity},
		{name: "raw-waits-for-cleared-refresh", raw: true, wantWait: true, clearRefresh: true},
		{name: "raw-refuses-after-unresolved-refresh", raw: true, wantWait: true, want: memtable.ErrCOWCapacity},
		{name: "raw-post-frontier-sticky-refuses-without-wait", raw: true, postFrontier: true, want: memtable.ErrCOWCapacity},
		{name: "post-wait-publication-poison-precedes-sticky-refresh", raw: true, wantWait: true, poison: true, want: poison},
		{name: "raw-waiter-observes-close", raw: true, wantWait: true, close: true, want: backenddb.ErrClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				backend := &rawTxnWaitReadinessFixture{}
				db := &DB{cow: &cowCache{}, backend: backend}
				db.checkpointCond = sync.NewCond(&db.checkpointMu)
				db.checkpointMu.Lock()
				db.checkpointWriteCutoverActive.Store(!tc.postFrontier)
				db.checkpointPostFrontierAdmission.Store(tc.postFrontier)
				db.checkpointing.Store(true)
				db.cow.refreshRequired.Store(true)
				db.checkpointMu.Unlock()

				leaveFence := sync.OnceFunc(func() {
					db.checkpointMu.Lock()
					if tc.clearRefresh {
						db.cow.refreshRequired.Store(false)
					}
					if tc.poison {
						backend.err = poison
					}
					if tc.close {
						db.closing.Store(true)
					}
					db.checkpointWriteCutoverActive.Store(false)
					db.checkpointPostFrontierAdmission.Store(false)
					db.checkpointing.Store(false)
					db.checkpointCond.Broadcast()
					db.checkpointMu.Unlock()
				})
				defer leaveFence()

				done := make(chan error, 1)
				go func() {
					var err error
					if tc.raw {
						err = db.beginDirectWriteWithCOWRefreshWait(true)
					} else {
						err = db.beginDirectWrite()
					}
					if err == nil {
						db.writeMu.RUnlock()
					}
					done <- err
				}()
				synctest.Wait()
				if tc.wantWait {
					select {
					case got := <-done:
						t.Fatalf("returned %v before the active checkpoint fence resolved", got)
					default:
					}
					if waiting := db.writeWaitForCheckpointActive.Load(); waiting != 1 {
						t.Fatalf("required exactly one original checkpoint waiter, got %d", waiting)
					}
					if !db.writeMu.TryLock() {
						t.Fatal("waiting raw writer retained its lease across the checkpoint drain")
					}
					db.writeMu.Unlock()
					leaveFence()
					synctest.Wait()
				}

				select {
				case got := <-done:
					if got != tc.want {
						t.Fatalf("admission returned %v, want exactly %v", got, tc.want)
					}
				default:
					t.Fatal("admission did not resolve after its matching fence state")
				}
				if waiting := db.writeWaitForCheckpointActive.Load(); waiting != 0 {
					t.Fatalf("admission left %d checkpoint waiters", waiting)
				}
				if !db.writeMu.TryLock() {
					t.Fatal("resolved admission leaked the write read lease")
				}
				db.writeMu.Unlock()
			})
		})
	}
}

// Embedded methods are never called by this admission-only fixture.
type rawTxnWaitReadinessFixture struct {
	BackendDB
	err error
}

func (f *rawTxnWaitReadinessFixture) CheckCommandWALPublishReady() error { return f.err }
