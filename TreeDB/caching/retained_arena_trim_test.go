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
