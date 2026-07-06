package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
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

func TestFlushSingleCanonicalStreamedCommandWALPublishFinalChunk(t *testing.T) {
	for _, tc := range []struct {
		name        string
		entries     int
		wantBatches int
	}{
		{name: "partial-final-chunk", entries: 5, wantBatches: 3},
		{name: "exact-final-chunk", entries: 4, wantBatches: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &commandPublishRecordingBackend{MockBackend: NewMockBackend()}
			db, err := Open(t.TempDir(), backend, Options{
				FlushThreshold:             1 << 60,
				FlushBackendMaxEntries:     2,
				FlushBackendMaxBatches:     -1,
				FlushSpanRunTargetPlanning: false,
				MemtableShards:             1,
				JournalLanes:               1,
			})
			if err != nil {
				t.Fatalf("Open cache: %v", err)
			}
			defer func() { _ = db.Close() }()

			for i := 0; i < tc.entries; i++ {
				setMutable(db, []byte{byte('a' + i)}, []byte("value"))
			}
			db.mu.Lock()
			if err := db.rotateMemtableLocked(false); err != nil {
				db.mu.Unlock()
				t.Fatalf("rotate memtable: %v", err)
			}
			db.mu.Unlock()

			publish := &checkpointCommandWALPublish{appliedLSN: 7, ranges: []backenddb.CommandWALLSNRange{{First: 1, Last: 7}}}
			if !db.flushLaneOnceWithCollectionMode(false, 0, publish, flushCollectionBackground) {
				t.Fatalf("flushLaneOnceWithCollectionMode returned false")
			}
			if !publish.consumed {
				t.Fatalf("command WAL publish was not consumed")
			}
			if len(backend.batches) != tc.wantBatches {
				t.Fatalf("backend batches=%d want %d", len(backend.batches), tc.wantBatches)
			}
			for i, b := range backend.batches {
				want := uint64(0)
				if i == len(backend.batches)-1 {
					want = 7
				}
				if b.publishLSN != want {
					t.Fatalf("batch %d publish LSN=%d want %d", i, b.publishLSN, want)
				}
			}
			if got := requireStatUint64(t, db.Stats(), "treedb.cache.flush_span_run.backend_chunks_total"); got != uint64(tc.wantBatches) {
				t.Fatalf("backend chunks=%d want %d", got, tc.wantBatches)
			}
		})
	}
}

func TestFlushSingleCanonicalStreamedOverlappingMemtables(t *testing.T) {
	backend := &commandPublishRecordingBackend{MockBackend: NewMockBackend()}
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:             1 << 60,
		FlushBackendMaxEntries:     2,
		FlushBackendMaxBatches:     -1,
		FlushSpanRunTargetPlanning: false,
		MemtableShards:             1,
		JournalLanes:               1,
	})
	if err != nil {
		t.Fatalf("Open cache: %v", err)
	}
	defer func() { _ = db.Close() }()

	setMutable(db, []byte("a"), []byte("old-a"))
	setMutable(db, []byte("b"), []byte("old-b"))
	setMutable(db, []byte("c"), []byte("old-c"))
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate old memtable: %v", err)
	}
	db.mu.Unlock()

	setMutable(db, []byte("b"), []byte("new-b"))
	deleteMutable(db, []byte("c"))
	setMutable(db, []byte("d"), []byte("new-d"))
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate new memtable: %v", err)
	}
	db.mu.Unlock()

	publish := &checkpointCommandWALPublish{appliedLSN: 11, ranges: []backenddb.CommandWALLSNRange{{First: 1, Last: 11}}}
	if !db.flushLaneOnceWithCollectionMode(false, 0, publish, flushCollectionBackground) {
		t.Fatalf("flushLaneOnceWithCollectionMode returned false")
	}
	if !publish.consumed {
		t.Fatalf("command WAL publish was not consumed")
	}
	if len(backend.batches) != 2 {
		t.Fatalf("backend batches=%d want 2", len(backend.batches))
	}
	if backend.batches[0].publishLSN != 0 {
		t.Fatalf("first batch publish LSN=%d want 0", backend.batches[0].publishLSN)
	}
	if backend.batches[1].publishLSN != 11 {
		t.Fatalf("final batch publish LSN=%d want 11", backend.batches[1].publishLSN)
	}

	for _, tc := range []struct {
		key  string
		want []byte
	}{
		{key: "a", want: []byte("old-a")},
		{key: "b", want: []byte("new-b")},
		{key: "d", want: []byte("new-d")},
	} {
		got, err := backend.Get([]byte(tc.key))
		if err != nil {
			t.Fatalf("backend.Get(%s): %v", tc.key, err)
		}
		if string(got) != string(tc.want) {
			t.Fatalf("backend.Get(%s)=%q want %q", tc.key, got, tc.want)
		}
	}
	got, err := backend.Get([]byte("c"))
	if err != nil {
		t.Fatalf("backend.Get(c): %v", err)
	}
	if got != nil {
		t.Fatalf("backend.Get(c)=%q want nil tombstone", got)
	}
	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.shadowed_ops_total"); got != 2 {
		t.Fatalf("shadowed_ops_total=%d want 2", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.source_point_ops_total"); got != 6 {
		t.Fatalf("source_point_ops_total=%d want 6", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.planned_point_ops_total"); got != 4 {
		t.Fatalf("planned_point_ops_total=%d want 4", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.planned_ops_total"); got != 4 {
		t.Fatalf("planned_ops_total=%d want 4", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.backend_chunks_total"); got != 2 {
		t.Fatalf("backend chunks=%d want 2", got)
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

type commandPublishRecordingBackend struct {
	*MockBackend
	batches []*commandPublishRecordingBatch
}

func (b *commandPublishRecordingBackend) NewBatch() batch.Interface {
	return b.newBatch()
}

func (b *commandPublishRecordingBackend) NewBatchWithSize(int) batch.Interface {
	return b.newBatch()
}

func (b *commandPublishRecordingBackend) newBatch() batch.Interface {
	rb := &commandPublishRecordingBatch{
		Interface: b.MockBackend.NewBatch(),
	}
	b.batches = append(b.batches, rb)
	return rb
}

type commandPublishRecordingBatch struct {
	batch.Interface
	publishLSN uint64
}

func (b *commandPublishRecordingBatch) SetCommandWALPublish(appliedLSN uint64, _ []backenddb.CommandWALLSNRange) error {
	b.publishLSN = appliedLSN
	return nil
}
