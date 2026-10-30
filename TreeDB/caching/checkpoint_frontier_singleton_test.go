package caching

import (
	"slices"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// These fixtures exercise capture/accounting only; no other Table methods run.
type checkpointFrontierSizedTable struct {
	memtable.Table
	size int64
}

func (m *checkpointFrontierSizedTable) Size() int64 { return m.size }

func TestCheckpointFrontierSingletonCapture(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mem       memtable.Table
		ids       []uint64
		lanes     []uint16
		wantBytes uint64
		wantLane  uint16
		missing   bool
	}{
		{name: "positive", mem: &checkpointFrontierSizedTable{size: 123}, ids: []uint64{7}, lanes: []uint16{3}, wantBytes: 123, wantLane: 3},
		{name: "nil memtable", ids: []uint64{7}},
		{name: "negative size", mem: &checkpointFrontierSizedTable{size: -1}, ids: []uint64{7}},
		{name: "zero ID", ids: []uint64{0}, missing: true},
		{name: "missing ID", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{queue: []memtable.Table{tc.mem}, queueIDs: tc.ids, queueLaneIDs: tc.lanes}
			f := db.captureCheckpointFrontierLocked()
			if !f.captured || f.missingQueueID != tc.missing {
				t.Fatalf("capture flags = %t/%t", f.captured, f.missingQueueID)
			}
			if tc.missing {
				if f.singletonID != 0 || f.idCount() != 0 || f.units != 0 || f.bytes != 0 || f.laneCount() != 0 || f.contains(0) {
					t.Fatalf("missing-ID frontier = %+v", f)
				}
				return
			}
			if f.ids != nil || f.laneUnits != nil || f.singletonID != 7 || f.units != 1 || f.bytes != tc.wantBytes || f.idCount() != 1 || f.laneCount() != 1 {
				t.Fatalf("singleton capture = %+v", f)
			}
			unit, ok := f.lookup(7)
			if !ok || unit.bytes != tc.wantBytes || unit.laneID != tc.wantLane || !f.contains(7) || f.contains(0) || f.contains(8) {
				t.Fatalf("singleton membership = %+v/%t", unit, ok)
			}
		})
	}
	for _, db := range []*DB{nil, {}} {
		f := db.captureCheckpointFrontierLocked()
		if !f.captured || f.idCount() != 0 || f.laneCount() != 0 || f.missingQueueID {
			t.Fatalf("empty capture = %+v", f)
		}
	}
}

func TestCheckpointFrontierSingletonMatchesMapResidualAndLanes(t *testing.T) {
	single := (&DB{queue: []memtable.Table{&checkpointFrontierSizedTable{size: 100}}, queueIDs: []uint64{7}, queueLaneIDs: []uint16{3}}).captureCheckpointFrontierLocked()
	mapped := checkpointFrontier{ids: map[uint64]checkpointFrontierUnit{7: {bytes: 100, laneID: 3}}, laneUnits: map[uint16]uint64{3: 1}, units: 1, bytes: 100, captured: true}
	for _, tc := range []struct {
		name   string
		queue  []memtable.Table
		ids    []uint64
		lanes  []uint16
		want   checkpointFrontierResidualStats
		active []bool
	}{
		{name: "unchanged", queue: []memtable.Table{&checkpointFrontierSizedTable{size: 100}}, ids: []uint64{7}, lanes: []uint16{3}, want: checkpointFrontierResidualStats{frontierUnits: 1, frontierBytes: 100, frontierLanes: 1}, active: []bool{false, false, false, true}},
		{name: "live size and lane", queue: []memtable.Table{&checkpointFrontierSizedTable{size: 40}}, ids: []uint64{7}, lanes: []uint16{1}, want: checkpointFrontierResidualStats{frontierUnits: 1, frontierBytes: 40, frontierLanes: 1, drainedBytes: 60}, active: []bool{false, true, false, false}},
		{name: "captured size fallback and invalid live lane", queue: []memtable.Table{nil}, ids: []uint64{7}, lanes: []uint16{9}, want: checkpointFrontierResidualStats{frontierUnits: 1, frontierBytes: 100, frontierLanes: 1}, active: []bool{true, false, false, false}},
		{name: "post append and missing live lane", queue: []memtable.Table{nil, &checkpointFrontierSizedTable{size: 25}}, ids: []uint64{7, 8}, want: checkpointFrontierResidualStats{frontierUnits: 1, frontierBytes: 100, frontierLanes: 1, postUnits: 1, postBytes: 25, postLanes: 1}, active: []bool{true, false, false, false}},
		{name: "drained", want: checkpointFrontierResidualStats{drainedUnits: 1, drainedBytes: 100, drainedLanes: 1}, active: []bool{false, false, false, false}},
		{name: "replaced by post unit", queue: []memtable.Table{&checkpointFrontierSizedTable{size: 25}}, ids: []uint64{8}, lanes: []uint16{2}, want: checkpointFrontierResidualStats{drainedUnits: 1, drainedBytes: 100, drainedLanes: 1, postUnits: 1, postBytes: 25, postLanes: 1}, active: []bool{false, false, false, false}},
		{name: "duplicate live ID retains map semantics", queue: []memtable.Table{nil, nil}, ids: []uint64{7, 7}, lanes: []uint16{1, 2}, want: checkpointFrontierResidualStats{frontierUnits: 2, frontierBytes: 200, frontierLanes: 1}, active: []bool{false, true, true, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{queue: tc.queue, queueIDs: tc.ids, queueLaneIDs: tc.lanes}
			for _, f := range []checkpointFrontier{single, mapped} {
				if got := db.checkpointFrontierResidualStatsLocked(f); got != tc.want {
					t.Fatalf("residual singleton=%t: %+v want %+v", f.singletonID != 0, got, tc.want)
				}
				var scratch [4]bool
				active, count := db.checkpointFrontierActiveLanes(f, 4, scratch[:])
				wantCount := 0
				for _, on := range tc.active {
					if on {
						wantCount++
					}
				}
				if !slices.Equal(active, tc.active) || count != wantCount {
					t.Fatalf("active singleton=%t: %v/%d want %v/%d", f.singletonID != 0, active, count, tc.active, wantCount)
				}
			}
		})
	}
}

func TestCheckpointFrontierMapFallbackPartialDrainAndDuplicates(t *testing.T) {
	db := &DB{queue: []memtable.Table{&checkpointFrontierSizedTable{size: 10}, &checkpointFrontierSizedTable{size: 20}, &checkpointFrontierSizedTable{size: 30}}, queueIDs: []uint64{7, 7, 9}, queueLaneIDs: []uint16{1, 2, 2}}
	f := db.captureCheckpointFrontierLocked()
	if f.singletonID != 0 || f.idCount() != 2 || f.units != 3 || f.bytes != 60 || f.laneCount() != 2 || f.laneUnits[1] != 1 || f.laneUnits[2] != 2 {
		t.Fatalf("duplicate-ID fallback = %+v", f)
	}
	if unit, ok := f.lookup(7); !ok || unit.bytes != 20 || unit.laneID != 2 {
		t.Fatalf("duplicate lookup = %+v/%t", unit, ok)
	}
	// One copy remains; ID lookup retains the last captured duplicate's size and
	// lane, while total and per-lane capture accounting includes all three rows.
	db.queue, db.queueIDs, db.queueLaneIDs = []memtable.Table{nil}, []uint64{7}, []uint16{0}
	want := checkpointFrontierResidualStats{frontierUnits: 1, frontierBytes: 20, frontierLanes: 1, drainedUnits: 2, drainedBytes: 40, drainedLanes: 2}
	if got := db.checkpointFrontierResidualStatsLocked(f); got != want {
		t.Fatalf("partial duplicate drain = %+v want %+v", got, want)
	}
	// Multiple rows with just one usable ID still take the unchanged map path.
	db.queue, db.queueIDs = []memtable.Table{nil, nil}, []uint64{7, 0}
	f = db.captureCheckpointFrontierLocked()
	if f.singletonID != 0 || f.idCount() != 1 || f.units != 1 || !f.missingQueueID {
		t.Fatalf("mixed missing-ID fallback = %+v", f)
	}
}

func TestCheckpointFrontierSingletonCaptureLifetime(t *testing.T) {
	mem := &checkpointFrontierSizedTable{size: 100}
	db := &DB{queue: []memtable.Table{mem}, queueIDs: []uint64{7}, queueLaneIDs: []uint16{3}}
	f := db.captureCheckpointFrontierLocked()
	db.setActiveCheckpointFrontier(f)
	copy, active := db.activeCheckpointFrontier()
	if !active {
		t.Fatal("frontier not active")
	}
	// Reuse all source storage and publish a new cut before clearing it. A saved
	// active-frontier copy must not borrow mutable queue or DB frontier storage.
	mem.size, db.queueIDs[0], db.queueLaneIDs[0] = 900, 8, 1
	next := db.captureCheckpointFrontierLocked()
	db.setActiveCheckpointFrontier(next)
	db.clearActiveCheckpointFrontier()
	if _, active := db.activeCheckpointFrontier(); active {
		t.Fatal("clear retained active frontier")
	}
	for _, saved := range []checkpointFrontier{f, copy} {
		unit, ok := saved.lookup(7)
		if !ok || saved.contains(8) || unit.bytes != 100 || unit.laneID != 3 || saved.units != 1 || saved.bytes != 100 {
			t.Fatalf("saved frontier changed: %+v/%+v", saved, unit)
		}
	}
	if !next.contains(8) || next.contains(7) || next.bytes != 900 {
		t.Fatalf("new cut = %+v", next)
	}
}

func TestCheckpointFrontierSingletonConcurrentPublication(t *testing.T) {
	db := &DB{}
	f := checkpointFrontier{singletonID: 7, singletonLane: 3, units: 1, bytes: 100, captured: true}
	db.setActiveCheckpointFrontier(f)
	var wg sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				copy, active := db.activeCheckpointFrontier()
				if active {
					unit, ok := copy.lookup(7)
					if !ok || unit.bytes != 100 || unit.laneID != 3 || copy.idCount() != 1 || copy.laneCount() != 1 {
						t.Error("active copy changed")
						return
					}
				}
			}
		}()
	}
	for i := 0; i < 100; i++ {
		db.clearActiveCheckpointFrontier()
		db.setActiveCheckpointFrontier(f)
	}
	wg.Wait()
}

func TestCheckpointFrontierSingletonFilterStopsAtPostFrontier(t *testing.T) {
	for _, f := range []checkpointFrontier{
		{singletonID: 7, units: 1, captured: true},
		{ids: map[uint64]checkpointFrontierUnit{7: {}}, captured: true},
	} {
		// The second unit uses the collected ID fallback; the fourth must not be
		// selected across a post-frontier barrier even though its ID matches.
		units := []flushUnit{{id: 7, memBytes: 10, memLen: 1}, {memBytes: 20, memLen: 2}, {id: 8, memBytes: 30, memLen: 3}, {id: 7}}
		ids := []uint64{7, 7, 8, 7}
		got, gotIDs, bytes, ops := filterFlushUnitsToCheckpointFrontier(units, ids, f)
		if len(got) != 2 || !slices.Equal(gotIDs, []uint64{7, 7}) || bytes != 30 || ops != 3 {
			t.Fatalf("prefix singleton=%t: units=%d ids=%v bytes=%d ops=%d", f.singletonID != 0, len(got), gotIDs, bytes, ops)
		}
	}
}
