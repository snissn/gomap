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
	if db.valueLogDictPublishDeferred.Swap(false) {
		db.valueLogDictKick()
	}
	select {
	case <-db.valueLogDictKickCh:
	default:
		t.Fatal("expected valueLogDictKick after checkpointing cleared")
	}
}
