package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// The original DATA group must retain its closure, allocator debt and registry
// pin when physical cleanup refuses. A PRIMARY arena is never constructed.
func TestRootPublicationDATAGroupFailedResourceReleaseRetainsOriginal(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)

	if err := database.SetSync([]byte("borrowed"), []byte("base")); err != nil {
		t.Fatal(err)
	}
	basis := database.AcquireSnapshot()
	group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	batch := database.NewPhysicalBatch().(*Batch)
	for n := 0; n < 64; n++ {
		if err := batch.Set([]byte(fmt.Sprintf("private-%03d", n)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.SetRootPublicationBuildGroup(group, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	pages := group.tracker.Pages()
	if len(pages) == 0 || !group.registered {
		t.Fatal("fixture did not construct real DATA debt and its registry pin")
	}
	// Reuse the maintained owned-file fixture: release reaches the producer's
	// actual final Close and reports os.ErrClosed, without a disposal hook.
	original, operations, baseline := newRootCaptureCloseRefusal(t, database, path)
	group.nativeResources = original
	owner := database.valueLogIdentityPins.MetadataOwner()
	file, charge := operations.file, operations.charge
	prior := errors.New("earlier build cleanup failure")
	database.retainOriginalCleanupError(prior)
	err = group.Close()
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close result=%v, want original physical Close refusal", err)
	}
	if err != &group.dataCleanupFailure || group.dataCleanupFailure.payload != group {
		t.Fatal("cleanup lost the SAME containing group receipt")
	}
	if group.nativeResources != original || !group.registered || group.idx == nil || group.tracker == nil || len(group.tracker.Pages()) != len(pages) {
		t.Fatal("failed original closure, registry pin or DATA tracker was released")
	}
	if owner.Bytes() <= baseline || operations.file != file || charge == 0 || operations.charge != charge {
		t.Fatal("failed original physical payload or retained charge was refunded")
	}
	if !errors.Is(database.backgroundError(), err) || !errors.Is(database.backgroundError(), prior) {
		t.Fatal("existing DB error graph lost the group receipt or prior failure")
	}
	if again := group.Close(); again != err || operations.closeCalls.Load() != 1 {
		t.Fatalf("cleanup replay changed receipt or repeated physical Close: %v", again)
	}
	if got, readErr := basis.Get([]byte("borrowed")); readErr != nil || !bytes.Equal(got, []byte("base")) {
		t.Fatalf("held basis=%q %v", got, readErr)
	}
	// Cleanup refusal retains custody but must release the original basis read
	// and serialization locks, allowing the real snapshot and DB to close.
	done := make(chan error, 1)
	go func() {
		if closeErr := basis.Close(); closeErr != nil {
			done <- closeErr
			return
		}
		database.writeMu.Lock()
		database.writeMu.Unlock()
		database.durablePublishMu.Lock()
		database.durablePublishMu.Unlock()
		done <- database.Close()
	}()
	select {
	case closeErr := <-done:
		if !errors.Is(closeErr, os.ErrClosed) {
			t.Fatalf("shutdown lost original physical debt: %v", closeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed DATA group cleanup deadlocked shutdown")
	}
	if operations.closeCalls.Load() != 1 || group.nativeResources != original || group.dataCleanupFailure.payload != group {
		t.Fatal("shutdown replayed disposal or lost original group custody")
	}
}
