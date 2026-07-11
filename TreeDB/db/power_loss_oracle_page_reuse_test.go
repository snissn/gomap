package db

import (
	"bytes"
	"fmt"
	"testing"
)

// TestPowerLossOracleRecoverablePagesRemainPinnedUntilDurableCoverage is the
// converted witness for the former recoverable-page-reuse counterexample.
// Pages reachable from either recoverable meta slot must not enter the
// freelist until a durable publication has displaced the older generation.
func TestPowerLossOracleRecoverablePagesRemainPinnedUntilDurableCoverage(t *testing.T) {
	d, err := Open(Options{
		Dir:                    t.TempDir(),
		ChunkSize:              64 * 1024,
		Durability:             DurabilityWALOffRelaxed,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
		FreelistRegionRadius:   -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	const keys = 5000
	writeGeneration := func(tag byte, sync bool) error {
		b := d.NewBatch().(*Batch)
		defer b.Close()
		value := bytes.Repeat([]byte{tag}, 32)
		for i := 0; i < keys; i++ {
			if err := b.Set([]byte(fmt.Sprintf("reuse/%04d", i)), value); err != nil {
				return err
			}
		}
		if sync {
			return b.WriteSync()
		}
		return b.Write()
	}

	if err := writeGeneration('a', true); err != nil {
		t.Fatalf("write stable generation: %v", err)
	}
	oldSequence := d.State().CommitSeq
	if err := writeGeneration('b', false); err != nil {
		t.Fatalf("write retirement generation: %v", err)
	}
	if err := writeGeneration('c', false); err != nil {
		t.Fatalf("write visible generation: %v", err)
	}
	idx := d.idx.Load()
	if idx == nil {
		t.Fatal("missing current index generation")
	}
	pinned := idx.allocator.Counters()
	if pinned.FreeIDs != 0 {
		t.Fatalf("recoverable generation pages entered freelist before durable coverage: %+v", pinned)
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("first durable coverage: %v", err)
	}
	// Background publication may already have advanced both alternating meta
	// slots. If one can still select generation 'a', publish one more durable
	// generation to displace it deterministically.
	if s := d.rootPublication.snapshot(); s.oldestRecoverableCommitSeq <= oldSequence {
		if err := writeGeneration('d', true); err != nil {
			t.Fatalf("displace second recoverable meta slot: %v", err)
		}
	}
	d.Prune()
	reclaimable := idx.allocator.Counters()
	if reclaimable.FreeIDs == 0 {
		t.Fatalf("displaced recoverable generation remained pinned after both meta slots advanced: before=%+v after=%+v", pinned, reclaimable)
	}

	if err := writeGeneration('e', false); err != nil {
		t.Fatalf("write reusable generation: %v", err)
	}
	reused := idx.allocator.Counters()
	if reused.ReuseAllocPages <= reclaimable.ReuseAllocPages {
		t.Fatalf("eligible freelist pages were not reused: before=%+v after=%+v", reclaimable, reused)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("durably publish reused generation: %v", err)
	}
}
