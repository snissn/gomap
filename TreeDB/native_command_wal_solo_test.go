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

func TestNativeCommandWALSoloDurableUsesCapturedSync(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	before := database.Stats()
	nextLSN := database.backend.CommandWALNextLSN()
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil }); err != nil {
		t.Fatal(err)
	}
	after := database.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.bypass.reason.solo_durable_sync_total", 0)
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

// The captured singleton installs before its final fence. A failed fence owns
// the original assigned identity and installed but unacknowledged mutation.
func TestNativeCommandWALSoloAssignedSyncFailureRetainsIdentity(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	before := database.Stats()
	nextLSN := database.backend.CommandWALNextLSN()
	failure := errors.New("native solo command sync failed")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			return failure
		}
		return nil
	})
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	_, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
	restore()
	if !errors.Is(err, failure) || !errors.Is(err, collections.ErrCommitAmbiguous) {
		t.Fatalf("assigned sync failure=%v", err)
	}
	if got := database.backend.CommandWALNextLSN(); got != nextLSN+1 {
		t.Fatalf("assigned next=%d want%d", got, nextLSN+1)
	}
	if got, err := c.Get([]byte("a")); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("ambiguous installed row=%x error=%v", got, err)
	}
	if got := database.commandWALGroupCommit.durableIntents.Load(); got != 0 {
		t.Fatalf("durable announcements after failure=%d", got)
	}
	if got := statMapUint64(t, database.Stats(), "treedb.command_wal.durable_wal_lsn"); got != statMapUint64(t, before, "treedb.command_wal.durable_wal_lsn") {
		t.Fatalf("failed final fence advanced durable frontier=%d", got)
	}
	if _, _, err := c.Update([]byte("b"), func([]byte) ([]byte, bool, error) { t.Error("callback after assigned failure"); return doc, true, nil }); err == nil {
		t.Fatal("write after assigned failure succeeded")
	}
}

// Only the actual captured-file observer is installed: collector hooks would
// disable singleton eligibility and silently exercise the queued fallback.
func TestNativeCommandWALSoloCapturedFenceReleasesGatesBeforeACK(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	before := database.Stats()
	group := &database.commandWALGroupCommit
	group.mu.Lock()
	beforeTimer := group.timer
	group.mu.Unlock()
	nextLSN := database.backend.CommandWALNextLSN()
	entered, release := make(chan struct{}), make(chan struct{})
	successorAppended := make(chan struct{})
	var unblockOnce, holdOnce, successorOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(release) }) }
	first, second := make(chan error, 1), make(chan error, 1)
	firstJoined, secondJoined := make(chan struct{}), make(chan struct{})
	secondStarted := false
	t.Cleanup(func() {
		unblock()
		joins := []<-chan struct{}{firstJoined}
		if secondStarted {
			joins = append(joins, secondJoined)
		}
		for _, joined := range joins {
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Error("native completion did not join after fence release")
			}
		}
	})
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if e.Point == durabilitycut.AfterDependencyAppend && e.LSN == nextLSN+1 {
			successorOnce.Do(func() { close(successorAppended) })
		}
		if e.Point == durabilitycut.BeforeDependencyFileSync && e.LSN == nextLSN {
			if e.StablePath == nil {
				return errors.New("captured native fence has no retained file")
			}
			holdOnce.Do(func() { close(entered); <-release })
		}
		return nil
	})
	defer restore()
	doc, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	go func() {
		defer close(firstJoined)
		_, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
		first <- err
	}()
	select {
	case <-entered:
	case err := <-first:
		t.Fatalf("native ACK preceded captured fence: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("singleton did not reach its actual captured fence")
	}
	if got := database.backend.CommandWALNextLSN(); got != nextLSN+1 {
		t.Fatalf("singleton prefix next=%d want%d", got, nextLSN+1)
	}
	if got, err := c.Get([]byte("a")); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("captured fence preceded complete native installation: %x/%v", got, err)
	}
	if got := statMapUint64(t, database.Stats(), "treedb.command_wal.durable_wal_lsn"); got != statMapUint64(t, before, "treedb.command_wal.durable_wal_lsn") {
		t.Fatalf("held final fence advanced durable frontier=%d", got)
	}
	if !database.commandWALPublicOperationGate.TryLock() {
		t.Fatal("captured sync retained public operation gate")
	}
	database.commandWALPublicOperationGate.Unlock()
	group.mu.Lock()
	timer := group.timer
	group.mu.Unlock()
	if timer != beforeTimer {
		t.Fatal("idle native singleton created a collection timer")
	}
	requirePublicStatDelta(t, before, database.Stats(), "treedb.command_wal.group_commit.trigger.timeout_total", 0)
	secondStarted = true
	go func() {
		defer close(secondJoined)
		_, _, err := c.Update([]byte("b"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
		second <- err
	}()
	select {
	case <-successorAppended: // real assignment requires BOTH producer and raw gates
	case err := <-second:
		t.Fatalf("successor returned before assigning under held sync: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("captured sync retained producer or raw publisher gate")
	}
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			t.Fatalf("ACK overtook captured fence: %v", err)
		default:
		}
	}
	unblock()
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("native ACK did not complete after physical fence")
		}
	}
}
