package workstats

import (
	"encoding/json"
	"sync"
	"testing"
)

func TestConcurrentSnapshotMonotonic(t *testing.T) {
	before := Read()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				IndexedJSON.ScalarRows.Add(1)
			}
		})
	}
	previous := before.IndexedJSON.ScalarRows
	for range 20 {
		current := Read()
		if current.IndexedJSON.ScalarRows < previous || current.OriginUnixNano != before.OriginUnixNano {
			t.Fatal("snapshot decreased or reset")
		}
		previous = current.IndexedJSON.ScalarRows
	}
	wg.Wait()
	after := Read()
	if after.IndexedJSON.ScalarRows-before.IndexedJSON.ScalarRows != 4000 {
		t.Fatal("lost concurrent increments")
	}
	raw, err := json.Marshal(IndexedJSONStats{})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]uint64
	if err = json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 5 {
		t.Fatalf("zero fields omitted: %s", raw)
	}
}
