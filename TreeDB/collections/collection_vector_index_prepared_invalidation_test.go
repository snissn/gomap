package collections

import (
	"runtime"
	"testing"
	"time"
)

func TestPreparedSearchInvalidationReplacementBuild(t *testing.T) {
	for _, exact := range []bool{true, false} {
		name := "broad-waits"
		if exact {
			name = "exact-old-does-not-wait"
		}
		t.Run(name, func(t *testing.T) {
			slot := collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: "graph"}
			old, replacement := &collectionVectorIndexPreparedSearch{}, &collectionVectorIndexPreparedSearch{}
			// The real acquisition path replaces the slot with a building entry
			// whose prepared field remains nil until installation under this mutex.
			entry := &collectionVectorIndexPreparedSearchCacheEntry{building: true, ready: make(chan struct{})}
			col := &Collection{vectorBufferedSearch: map[collectionVectorIndexPreparedSearchCacheSlot]*collectionVectorIndexPreparedSearchCacheEntry{slot: entry}}
			var target *collectionVectorIndexPreparedSearch
			if exact {
				target = old
			}
			done := make(chan struct{})
			go func() {
				col.invalidateCollectionVectorIndexPreparedSearch(slot, target)
				close(done)
			}()
			deadline := time.Now().Add(5 * time.Second)
			returned, waited := false, false
			for !returned && !waited && time.Now().Before(deadline) {
				select {
				case <-done:
					returned = true
				default:
				}
				col.vectorBufferedSearchMu.Lock()
				waited = col.vectorBufferedSearchWaits != 0
				col.vectorBufferedSearchMu.Unlock()
				runtime.Gosched()
			}
			// Release even on a failing assertion; never strand the invalidator.
			col.vectorBufferedSearchMu.Lock()
			entry.prepared, entry.building = replacement, false
			close(entry.ready)
			col.vectorBufferedSearchMu.Unlock()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("invalidation did not finish after installation")
			}
			if exact && (!returned || waited) {
				t.Fatal("exact-old invalidation waited on unrelated replacement build")
			}
			if !exact && (!waited || returned) {
				t.Fatal("broad invalidation did not wait for installation")
			}
			if exact && (col.vectorBufferedSearch[slot] != entry || replacement.closed || old.closed) {
				t.Fatal("exact-old invalidation changed replacement or unowned old object")
			}
			if !exact && (col.vectorBufferedSearch[slot] != nil || !replacement.closed) {
				t.Fatal("broad invalidation did not remove and close replacement")
			}
		})
	}
}
