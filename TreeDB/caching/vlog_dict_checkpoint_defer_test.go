package caching

import (
	"testing"
)

func TestValueLogDictPublishDeferredDuringCheckpointAndKickedAfter(t *testing.T) {
	db := &DB{
		valueLogDictKickCh: make(chan struct{}, 1),
	}

	// Outside Checkpoint: must not defer.
	if db.deferValueLogDictPublishDuringCheckpoint() {
		t.Fatal("deferred dict publish when checkpointing=false")
	}
	if db.valueLogDictPublishDeferred.Load() {
		t.Fatal("valueLogDictPublishDeferred set when checkpointing=false")
	}

	// During Checkpoint: must defer and record pending flag without kicking yet.
	db.checkpointing.Store(true)
	if !db.deferValueLogDictPublishDuringCheckpoint() {
		t.Fatal("expected dict publish deferral while checkpointing=true")
	}
	if !db.valueLogDictPublishDeferred.Load() {
		t.Fatal("expected valueLogDictPublishDeferred=true while checkpointing=true")
	}
	select {
	case <-db.valueLogDictKickCh:
		t.Fatal("unexpected kick while checkpointing=true")
	default:
	}

	// Simulate Checkpoint exit defer clearing checkpointing and kicking.
	db.checkpointing.Store(false)
	db.maybeKickDeferredValueLogDictPublish()
	select {
	case <-db.valueLogDictKickCh:
	default:
		t.Fatal("expected valueLogDictKick after checkpointing cleared")
	}
}

func TestValueLogDictPublishDeferredAcrossOuterCheckpointScope(t *testing.T) {
	db := &DB{
		valueLogDictKickCh: make(chan struct{}, 1),
	}

	// Simulate public checkpointCachedForPublicCommandWAL holding an outer
	// deferral scope across CheckpointDeferringCommandWALCleanup + fallback +
	// WAL cleanup.
	db.EnterCheckpointDictDeferral()
	db.checkpointing.Store(true)

	if !db.deferValueLogDictPublishDuringCheckpoint() {
		t.Fatal("expected deferral while inner checkpointing and outer scope are active")
	}

	// Inner CheckpointDeferringCommandWALCleanup finishes and clears
	// checkpointing, but outer scope is still active for fallback + WAL cleanup.
	db.checkpointing.Store(false)
	db.maybeKickDeferredValueLogDictPublish()
	select {
	case <-db.valueLogDictKickCh:
		t.Fatal("unexpected kick before outer checkpoint deferral scope exited")
	default:
	}

	// Another attempt during fallback/cleanup must still defer.
	if !db.deferValueLogDictPublishDuringCheckpoint() {
		t.Fatal("expected deferral while outer scope remains active during fallback/cleanup")
	}
	select {
	case <-db.valueLogDictKickCh:
		t.Fatal("unexpected kick while outer checkpoint deferral scope remained active")
	default:
	}

	// Nested scope does not release prematurely.
	db.EnterCheckpointDictDeferral()
	db.LeaveCheckpointDictDeferral()
	select {
	case <-db.valueLogDictKickCh:
		t.Fatal("unexpected kick while outer nesting level still > 0")
	default:
	}

	// Leaving the outermost scope kicks valueLogDictLoop immediately.
	db.LeaveCheckpointDictDeferral()
	select {
	case <-db.valueLogDictKickCh:
	default:
		t.Fatal("expected valueLogDictKick once outer checkpoint deferral scope exited")
	}
	if db.valueLogDictPublishDeferred.Load() {
		t.Fatal("valueLogDictPublishDeferred remained set after kick")
	}
}
