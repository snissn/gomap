package caching

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCheckpointFrontierActiveLanesScratch(t *testing.T) {
	for _, lanes := range []int{1, 64, 65, 257} {
		t.Run(fmt.Sprint(lanes), func(t *testing.T) {
			db := &DB{
				queue:        make([]memtable.Table, 5),
				queueIDs:     []uint64{1, 2, 3, 4}, // Missing IDs remain outside the cut.
				queueLaneIDs: []uint16{uint16(lanes - 1), uint16(lanes), 0, 0},
			}
			frontier := checkpointFrontier{ids: map[uint64]checkpointFrontierUnit{1: {}, 2: {}}}
			var scratch [64]bool
			for i := range scratch {
				scratch[i] = true
			}
			active, count := db.checkpointFrontierActiveLanes(frontier, lanes, scratch[:])
			want := make([]bool, lanes)
			want[0], want[lanes-1] = true, true // Invalid lane falls back to zero.
			wantCount := 2
			if lanes == 1 {
				wantCount = 1
			}
			if !slices.Equal(active, want) || count != wantCount {
				t.Fatalf("active=%v count=%d want %v/%d", active, count, want, wantCount)
			}
			if lanes <= len(scratch) {
				if &active[0] != &scratch[0] {
					t.Fatal("inline result does not use caller storage")
				}
				for _, untouched := range scratch[lanes:] {
					if !untouched {
						t.Fatal("changed scratch beyond requested lanes")
					}
				}
			} else if &active[0] == &scratch[0] {
				t.Fatal("oversized result did not use independent fallback storage")
			}

			// Reuse both inline and fallback storage after the queue changes.
			db.queueIDs = []uint64{3, 4, 5, 6, 7}
			active, count = db.checkpointFrontierActiveLanes(frontier, lanes, active)
			if count != 0 || slices.Contains(active, true) {
				t.Fatalf("stale active lanes after reuse: %v/%d", active, count)
			}
			active[lanes-1] = true
			active, count = db.checkpointFrontierActiveLanes(checkpointFrontier{}, lanes, active)
			if count != 0 || slices.Contains(active, true) {
				t.Fatal("empty frontier retained stale active lanes")
			}
			active[0] = true
			active, count = (*DB)(nil).checkpointFrontierActiveLanes(frontier, lanes, active)
			if count != 0 || slices.Contains(active, true) {
				t.Fatal("nil database retained stale active lanes")
			}
		})
	}
}

func TestCheckpointFrontierActiveLanesConcurrentScratch(t *testing.T) {
	db := &DB{
		queue:        make([]memtable.Table, 4),
		queueIDs:     []uint64{1, 2, 3, 4},
		queueLaneIDs: []uint16{0, 1}, // Missing lane metadata also falls back to zero.
	}
	var wg sync.WaitGroup
	errors := make(chan string, 8)
	for worker := 0; worker < cap(errors); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var scratch [64]bool
			for i := 0; i < 100; i++ {
				id := uint64(1 + (worker+i)%4)
				frontier := checkpointFrontier{ids: map[uint64]checkpointFrontierUnit{id: {}}}
				active, count := db.checkpointFrontierActiveLanes(frontier, 4, scratch[:])
				lane := 0
				if id == 2 {
					lane = 1
				}
				if count != 1 || !active[lane] {
					errors <- fmt.Sprintf("worker %d cut %d active=%v count=%d", worker, id, active, count)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
