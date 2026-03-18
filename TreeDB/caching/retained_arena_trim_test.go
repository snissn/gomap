package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestTrimRetainedArenasAfterFlush_CheckpointPathTrimsAppendOnlyCaches(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolStateForTest(t)
	batchArenaPoolTestMu.Lock()
	t.Cleanup(batchArenaPoolTestMu.Unlock)
	resetBatchArenaPoolsForTest()

	db := &DB{
		mutableShards: make([]memShard, 1),
	}

	leaseCount := postCheckpointAppendOnlyMemLeaseKeep + 6
	for i := 0; i < leaseCount; i++ {
		mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(4<<20, appendOnlyEstimatedBytesPerEntryDefault)
		db.appendOnlyMemLeases = append(db.appendOnlyMemLeases, mt)
	}

	shard := &db.mutableShards[0]
	for i := 0; i < 64; i++ {
		chunk := make([]byte, 0, appendOnlyDirectValueArenaDefaultChunk)
		shard.appendOnlyDirectValueArena.retained = append(shard.appendOnlyDirectValueArena.retained, chunk)
		shard.appendOnlyDirectValueArena.retainedBytes += int64(cap(chunk))
	}

	db.trimRetainedArenasAfterFlush(true)

	if got := len(db.appendOnlyMemLeases); got > postCheckpointAppendOnlyMemLeaseKeep {
		t.Fatalf("append-only mem leases=%d want <= %d", got, postCheckpointAppendOnlyMemLeaseKeep)
	}
	maxDirectRetained := int64(appendOnlyDirectValueArenaRetainMaxBytes / 4)
	if got := shard.appendOnlyDirectValueArena.retainedBytes; got > maxDirectRetained {
		t.Fatalf("direct arena retained bytes=%d want <= %d", got, maxDirectRetained)
	}
}

func TestTrimAppendOnlyMemLeases_DroppedLeasesReturnToPool(t *testing.T) {
	var db DB

	keep := 2
	leaseCount := keep + 6
	for i := 0; i < leaseCount; i++ {
		mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(4<<20, appendOnlyEstimatedBytesPerEntryDefault)
		db.appendOnlyMemLeases = append(db.appendOnlyMemLeases, mt)
	}

	db.trimAppendOnlyMemLeases(keep, 4<<20)

	if got := len(db.appendOnlyMemLeases); got != keep {
		t.Fatalf("append-only mem leases=%d want %d", got, keep)
	}

	reused := 0
	for i := 0; i < leaseCount-keep; i++ {
		if v := db.appendOnlyMemPool.Get(); v != nil {
			if _, ok := v.(*memtable.AppendOnly); ok {
				reused++
			}
		}
	}
	if reused == 0 {
		t.Fatalf("expected trimmed append-only leases to be returned to mem pool")
	}
}
