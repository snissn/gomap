package caching

import (
	"testing"
)

func TestCOWReadCutInlineFrozenZeroSliceAllocs(t *testing.T) {
	var cut cowReadCut
	for frozen := 0; frozen <= cowReadCutInlineFrozen; frozen++ {
		allocs := testing.AllocsPerRun(200, func() {
			cut = cowReadCut{}
			cut.initStorageCap(cowReadCutInlineShards, frozen, frozen)
			for i := range cut.shards {
				cut.shards[i].shard = i
			}
			for i := range cut.frozen {
				cut.frozen[i].shard = i % cowReadCutInlineShards
			}
			cut.buildDomains()
			if len(cut.shards) != cowReadCutInlineShards || len(cut.frozen) != frozen || len(cut.sources) != cowReadCutInlineShards+frozen || len(cut.domains) != cowReadCutInlineShards {
				t.Fatalf("unexpected cut lengths for frozen=%d: shards=%d frozen=%d sources=%d domains=%d",
					frozen, len(cut.shards), len(cut.frozen), len(cut.sources), len(cut.domains))
			}
		})
		if allocs != 0 {
			t.Fatalf("frozen=%d initStorageCap+buildDomains allocated %.0f objects, want 0", frozen, allocs)
		}
	}
}

func TestNewActiveCOWSnapshotInitialState(t *testing.T) {
	s1 := newActiveCOWSnapshot()
	s2 := newActiveCOWSnapshot()
	if s1 == s2 {
		t.Fatal("newActiveCOWSnapshot reused pointer identity")
	}
	if s1.generation.Load() != 1 || s1.closed.Load() || s1.readState.Load() != 0 {
		t.Fatalf("unexpected initial snapshot state: gen=%d closed=%v readState=%d",
			s1.generation.Load(), s1.closed.Load(), s1.readState.Load())
	}
}
