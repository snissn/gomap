package freelist

import (
	"bytes"
	"reflect"
	"testing"
)

func TestFreelistGenerationV1_ChunkPruneMatchesPerPageBytesAndAliases(t *testing.T) {
	for _, mode := range []string{"private", "persistent", "durable-parent-dirty-children", "materialized"} {
		t.Run(mode, func(t *testing.T) {
			for _, horizon := range []struct {
				name string
				cap  ReuseCapability
			}{
				{"recovery", RecoveryHorizon{7, 0, 0}.capability()},
				{"reader", RecoveryHorizon{9, 6, 0}.capability()},
				{"history", RecoveryHorizon{9, 8, 5}.capability()},
				{"invalid", ReuseCapability{}},
			} {
				t.Run(horizon.name, func(t *testing.T) {
					retired := make(map[uint64]uint64)
					for _, chunk := range []uint64{0, 1, 2, 15, 16, 255, 256, 1 << 24} {
						for i, seq := range []uint64{1, 4, 5, 6, 7, 8, 9} {
							retired[chunk<<freelistChunkShift|uint64(i+2)] = seq
						}
					}
					// All 256 bits, including every word boundary, plus a whole
					// blocked sibling exercise in-place summary updates during traversal.
					for offset := uint64(0); offset < freelistChunkSize; offset++ {
						retired[32<<freelistChunkShift|offset] = 1
						retired[33<<freelistChunkShift|offset] = 9
					}
					base := MustNewFreelistGenerationV1(10, 1<<42, []uint64{100, 400}, retired)
					if mode == "materialized" {
						base = materializeTestGeneration(t, base, 11, NewMemoryPageStoreV1())
					} else if mode == "durable-parent-dirty-children" {
						// Existing private-copy contract: a durable parent does not
						// confer permission to mutate its unmaterialized children.
						base.root.pageID = 900
					}
					heldBase := pruneChunkSnapshot(t, base.root)
					actual := NewFreelistTxn(base, NewReservationLedger())
					if mode == "durable-parent-dirty-children" {
						// As in PrivateFirstCopyIsolatesDirtyChildren, the first
						// private write must detach this synthetic durable parent.
						// Keep the logical state identical, even for a no-op prune.
						actual.Retire(2, 1)
					}
					if mode == "persistent" {
						if _, err := actual.cloneForAllocatorPrepare(); err != nil {
							t.Fatal(err)
						}
					}
					// The real preparation clone detaches dirty rollback aliases.
					rollback := actual
					if mode == "private" {
						var err error
						actual, err = rollback.cloneForPrivateAllocatorPrepare()
						if err != nil {
							t.Fatal(err)
						}
					}
					heldRollback := pruneChunkSnapshot(t, rollback.root)
					heldPersistent := actual.root
					reference := NewFreelistTxn(base, NewReservationLedger())
					if mode == "durable-parent-dirty-children" {
						reference.Retire(2, 1)
					}
					before := actual.Stats()
					var items, chunks, lag uint64
					if err := walkState(reference.root, 0, func(c *stateChunk) error {
						changed := false
						for offset, seq := range c.retired {
							if seq == 0 || !horizon.cap.permits(seq) {
								continue
							}
							items++
							changed = true
							if value := horizon.cap.oldestRecoverableCommitSeq - seq; value > lag {
								lag = value
							}
							id := c.chunkNo<<freelistChunkShift | uint64(offset)
							reference.mutate(id>>freelistChunkShift, func(next *stateChunk) { next.setFree(uint64(offset), true) })
						}
						if changed {
							chunks++
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					visits := pruneExpectedVisits(actual.root, horizon.cap)
					actual.PruneWithCapability(horizon.cap)
					after := actual.Stats()
					if after.StateMutationItems-before.StateMutationItems != items || after.StateMutationPaths-before.StateMutationPaths != chunks || after.PageVisits-before.PageVisits != visits || after.ReuseLag != lag {
						t.Fatalf("prune stats before=%+v after=%+v want items/paths/visits/lag=%d/%d/%d/%d", before, after, items, chunks, visits, lag)
					}
					if err := validateCanonicalStateV2(actual.root, true); err != nil {
						t.Fatal(err)
					}
					if got := pruneChunkSnapshot(t, actual.root); !reflect.DeepEqual(got, pruneChunkSnapshot(t, reference.root)) {
						t.Fatal("prune state differs from per-page reference")
					}
					actual.PruneWithCapability(horizon.cap)
					if got := actual.Stats(); got != after {
						t.Fatalf("repeated prune changed stats: got %+v want %+v", got, after)
					}
					candidateID := candidateIDFromString("chunk-prune-byte-equivalence")
					got, err := actual.MaterializeCandidate(12, 12, candidateID, NewMemoryPageStoreV1())
					if err != nil {
						t.Fatal(err)
					}
					want, err := reference.MaterializeCandidate(12, 12, candidateID, NewMemoryPageStoreV1())
					if err != nil {
						t.Fatal(err)
					}
					if got.GenerationRef() != want.GenerationRef() || !reflect.DeepEqual(got.ReservationRecord(), want.ReservationRecord()) {
						t.Fatal("generation or reservation differs from per-page reference")
					}
					gotPages, wantPages := got.Pages(), want.Pages()
					if len(gotPages) != len(wantPages) {
						t.Fatalf("page counts differ: %d vs %d", len(gotPages), len(wantPages))
					}
					for i := range gotPages {
						if gotPages[i].PageID != wantPages[i].PageID || !bytes.Equal(gotPages[i].Data, wantPages[i].Data) {
							t.Fatalf("page %d differs from per-page reference", i)
						}
					}
					if !reflect.DeepEqual(heldBase, pruneChunkSnapshot(t, base.root)) {
						t.Fatal("immutable base changed")
					}
					if mode == "persistent" && !reflect.DeepEqual(heldRollback, pruneChunkSnapshot(t, heldPersistent)) {
						t.Fatal("persistent prune changed retained dirty alias")
					}
					if mode == "private" && !reflect.DeepEqual(heldRollback, pruneChunkSnapshot(t, rollback.root)) {
						t.Fatal("private prune or materialization changed rollback alias")
					}
				})
			}
		})
	}
}

func pruneChunkSnapshot(t *testing.T, root *stateNode) map[uint64]stateChunk {
	t.Helper()
	chunks := make(map[uint64]stateChunk)
	if err := walkState(root, 0, func(c *stateChunk) error { chunks[c.chunkNo] = *c; return nil }); err != nil {
		t.Fatal(err)
	}
	return chunks
}

// Count eligible nodes in the whole original topology, independently of the
// production traversal's subtree early return. Eligible children imply eligible
// ancestors through the maintained minimum-retirement summary.
func pruneExpectedVisits(n *stateNode, cap ReuseCapability) uint64 {
	if n == nil || cap.oldestRecoverableCommitSeq == 0 {
		return 0
	}
	var visits uint64
	if n.retiredCount != 0 && n.minRetiredSeq != 0 && cap.permits(n.minRetiredSeq) {
		visits++
	}
	for _, child := range n.child {
		visits += pruneExpectedVisits(child, cap)
	}
	return visits
}
