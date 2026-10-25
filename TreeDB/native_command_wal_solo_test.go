package treedb

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeCommandWALSoloDurableUsesOriginalSync(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	before := database.Stats()
	nextLSN := database.backend.CommandWALNextLSN()
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil }); err != nil {
		t.Fatal(err)
	}
	after := database.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 0)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.bypass.reason.solo_durable_sync_total", 1)
	if got := database.backend.CommandWALNextLSN(); got != nextLSN+1 {
		t.Fatalf("singleton assigned %d frames, want one canonical command", got-nextLSN)
	}
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 0 {
		t.Fatalf("durable announcements after ACK=%d", got)
	}
}

// No group configuration or eligibility-disabling observer is changed here.
// The completion observer holds callers only after their producer/raw gates end.
func TestNativeCommandWALAnnouncesBeforeProducerSerialization(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	seed, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}})
	if _, err := c.InsertBatch([][]byte{[]byte("c"), []byte("d")}, [][]byte{seed, seed}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before := database.Stats()
	admitted, completed := make(chan struct{}, 4), make(chan struct{}, 4)
	releaseAdmission, releaseCompletion := make(chan struct{}), make(chan struct{})
	var admissionOnce, completionOnce sync.Once
	defer admissionOnce.Do(func() { close(releaseAdmission) })
	defer completionOnce.Do(func() { close(releaseCompletion) })
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission {
			admitted <- struct{}{}
			<-releaseAdmission
		}
		return nil
	})
	defer restore()
	database.commandWALGroupCommit.testBeforeNativeComplete = func() { completed <- struct{}{}; <-releaseCompletion }
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	errs := make(chan error, 4)
	for _, id := range []string{"a", "b", "c", "d"} {
		go func(id string) {
			_, _, err := c.Update([]byte(id), func([]byte) ([]byte, bool, error) { return doc, true, nil })
			errs <- err
		}(id)
	}
	for range 4 {
		select {
		case <-admitted:
		case <-time.After(5 * time.Second):
			t.Fatal("native requests did not reach admission")
		}
	}
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 4 {
		t.Fatalf("announcements before producer=%d want4", got)
	}
	admissionOnce.Do(func() { close(releaseAdmission) })
	for range 4 {
		select {
		case <-completed:
		case <-time.After(5 * time.Second):
			t.Fatal("native requests did not release producer gates")
		}
	}
	group := &database.commandWALGroupCommit
	group.mu.Lock()
	pending := group.pendingCount
	group.mu.Unlock()
	if pending != 4 || group.durableIntents.Load() != 0 {
		t.Fatalf("pending=%d announcements=%d", pending, group.durableIntents.Load())
	}
	requirePublicStatDelta(t, before, database.Stats(), "treedb.command_wal.file_sync.calls_total", 0)
	completionOnce.Do(func() { close(releaseCompletion) })
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	after := database.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.bypass.reason.solo_durable_sync_total", 0)
}

func TestNativeCommandWALAnnouncementReleasedBeforeAssignment(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	nextLSN := database.backend.CommandWALNextLSN()
	failure := errors.New("native admission refused")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission {
			return failure
		}
		return nil
	})
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	_, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
	restore()
	if !errors.Is(err, failure) || errors.Is(err, collections.ErrCommitAmbiguous) {
		t.Fatalf("preassignment error=%v", err)
	}
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 0 || database.backend.CommandWALNextLSN() != nextLSN {
		t.Fatalf("refusal announcements=%d next=%d want%d", got, database.backend.CommandWALNextLSN(), nextLSN)
	}
	// A callback panic exercises the coordinator's deferred gates and the same
	// completion's withdrawal, without claiming native producer panic recovery.
	completion, err := database.PrepareNativeCommandWAL(1, true)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer completion.ReleaseNativeCommandWAL()
		defer func() {
			if recover() == nil {
				t.Error("append did not panic")
			}
		}()
		_ = completion.RegisterNativeCommandWAL(func(bool) (uint64, error) { panic("append panic") })
	}()
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 0 {
		t.Fatalf("panic retained announcements=%d", got)
	}
	if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil }); err != nil {
		t.Fatalf("write after unassigned refusal/panic=%v", err)
	}
}

func TestNativeCommandWALSoloAssignedSyncFailureRetainsIdentity(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	beforeDoc, err := c.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	nextLSN := database.backend.CommandWALNextLSN()
	failure := errors.New("native solo command sync failed")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			return failure
		}
		return nil
	})
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	_, _, err = c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
	restore()
	if !errors.Is(err, failure) || !errors.Is(err, collections.ErrCommitAmbiguous) {
		t.Fatalf("assigned sync failure=%v", err)
	}
	if got := database.backend.CommandWALNextLSN(); got != nextLSN+1 {
		t.Fatalf("assigned next=%d want%d", got, nextLSN+1)
	}
	if got, err := c.Get([]byte("a")); err != nil || !bytes.Equal(got, beforeDoc) {
		t.Fatalf("failed solo installed row=%x error=%v", got, err)
	}
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 0 {
		t.Fatalf("durable announcements after failure=%d", got)
	}
	if _, _, err := c.Update([]byte("b"), func([]byte) ([]byte, bool, error) { t.Error("callback after assigned failure"); return doc, true, nil }); err == nil {
		t.Fatal("write after assigned failure succeeded")
	}
}
