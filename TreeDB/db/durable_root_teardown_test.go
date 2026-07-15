package db

import (
	"testing"
	"time"
)

func TestRootPublicationPathsPinTeardownThroughPostWork(t *testing.T) {
	tests := []struct {
		name string
		run  func(*DB) error
	}{
		{
			name: "Commit",
			run: func(database *DB) error {
				return database.Commit(database.State().RootPageID)
			},
		},
		{
			name: "CompactIndex",
			run: func(database *DB) error {
				return database.CompactIndex()
			},
		},
		{
			name: "PublishCommandWALRoots",
			run: func(database *DB) error {
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
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
			go func() { publishDone <- test.run(database) }()
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
