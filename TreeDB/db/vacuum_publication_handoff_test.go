package db

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestVacuumRetiresCoordinatorBeforeAcceptedBatchWait(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	for _, tt := range []struct {
		name string
		sync bool
	}{
		{name: "write"},
		{name: "write_sync", sync: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts := Options{Dir: t.TempDir(), ChunkSize: 64 * 1024 * 1024}
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if database != nil {
					if err := database.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			oldRuntime := database.rootPublication
			if oldRuntime == nil || oldRuntime.coordinator == nil || database.commandWAL || database.acceptedPrefix.Load() != nil {
				t.Fatal("fixture requires ordinary queued index publication without a command WAL")
			}
			oldCoordinator := oldRuntime.coordinator
			initial := oldCoordinator.Stats().VisibleCommitSeq
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			accepted := make(chan rootpublication.PhysicalSealReceipt, 1)
			resume := make(chan struct{})
			writerDone := make(chan struct{})
			var writeErr error
			database.testBeforeAcceptedRootPublicationWaitHook = func(receipt rootpublication.PhysicalSealReceipt) {
				accepted <- receipt
				<-resume
			}
			go func() {
				defer close(writerDone)
				b := database.NewBatch()
				defer b.Close()
				writeErr = b.Set([]byte("handoff-key"), []byte("published-value"))
				if writeErr == nil {
					if tt.sync {
						writeErr = b.WriteSync()
					} else {
						writeErr = b.Write()
					}
				}
			}()
			released := false
			releaseWriter := func() {
				if !released {
					close(resume)
					released = true
				}
				<-writerDone
			}
			defer releaseWriter()
			var receipt rootpublication.PhysicalSealReceipt
			select {
			case receipt = <-accepted:
			case <-writerDone:
				t.Fatalf("batch completed before the accepted wait: %v", writeErr)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			// The writer has accepted the root, but has not waited for its
			// receipt. Vacuum must drain that exact old root, publish the
			// replacement, and dispose the old coordinator before it resumes.
			seq := oldCoordinator.Stats().VisibleCommitSeq
			if seq != initial+1 {
				t.Fatalf("old coordinator published %d, want exactly one batch after %d", seq, initial)
			}
			if err := database.VacuumIndexOnline(ctx); err != nil {
				t.Fatal(err)
			}
			if database.rootPublication == oldRuntime || oldRuntime.coordinator != nil {
				t.Fatal("vacuum did not retire the writer's coordinator")
			}
			if !receipt.Sealed() {
				t.Fatalf("vacuum did not durably cover the exact accepted batch %d", seq)
			}
			releaseWriter()
			if writeErr != nil {
				t.Fatalf("accepted batch returned an error after vacuum: %v", writeErr)
			}
			database.testBeforeAcceptedRootPublicationWaitHook = nil

			assertValue := func(database *DB) {
				t.Helper()
				value, err := database.Get([]byte("handoff-key"))
				if err != nil || string(value) != "published-value" {
					t.Fatalf("accepted value = %q, %v; want published-value", value, err)
				}
			}
			assertValue(database)
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			assertValue(reopened)
		})
	}
}
