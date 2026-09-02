package caching

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCanPiggybackCommandWALCheckpointPublishSingleLaneCombinedQueue(t *testing.T) {
	db := &DB{
		flushBuildConcurrency:  2,
		flushThreshold:         1 << 20,
		flushBackendMaxEntries: 1024,
		queue: []memtable.Table{
			commandWALCheckpointPublishTestMemtable(t, "a"),
			commandWALCheckpointPublishTestMemtable(t, "b"),
		},
		queueLaneIDs: []uint16{0, 0},
	}
	db.SetCommandWALCheckpointPublishHook(func(bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		return 2, []backenddb.CommandWALLSNRange{{First: 1, Last: 2}}, nil
	})

	if !db.canPiggybackCommandWALCheckpointPublish(true) {
		t.Fatal("expected single-lane combined checkpoint queue to piggyback command WAL publish")
	}

	db.queueLaneIDs = []uint16{0, 1}
	if db.canPiggybackCommandWALCheckpointPublish(true) {
		t.Fatal("expected multi-lane checkpoint queue to skip command WAL publish piggyback")
	}

	db.queueLaneIDs = []uint16{0, 0}
	db.flushBackendMaxEntries = 1
	if !db.canPiggybackCommandWALCheckpointPublish(true) {
		t.Fatal("expected chunked checkpoint queue to piggyback command WAL publish on the final sync batch")
	}
}

func commandWALCheckpointPublishTestMemtable(t *testing.T, key string) memtable.Table {
	t.Helper()
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	mt.Set([]byte(key), []byte("value"))
	mt.Freeze()
	return mt
}
