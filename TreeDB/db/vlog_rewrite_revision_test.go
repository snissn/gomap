package db

import (
	"bytes"
	"context"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestRewriteSwapPreservesWinningCurrentRevision(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, Durability: DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoErr(t, database)
	oldPtrs := appendPointersInNewSegment(t, dir, 0, 1, 600000, 2, func(int) []byte { return []byte("real-old-frame") })
	newPtrs := appendPointersInNewSegment(t, dir, 0, 2, 600010, 2, func(int) []byte { return []byte("real-new-frame") })
	swaps := []rewriteSwap{{key: []byte("a"), oldPtr: oldPtrs[0], newPtr: newPtrs[0]}, {key: []byte("b"), oldPtr: oldPtrs[1], newPtr: newPtrs[1]}}
	seed := database.NewBatch().(*Batch)
	defer seed.Close()
	for _, swap := range swaps {
		if err := seed.SetPointerWithRevision(swap.key, swap.oldPtr, 11); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.WriteSync(); err != nil {
		t.Fatal(err)
	}
	closeNoErr(t, seed)
	// A planner's old pointer may still match after a logical publication has
	// changed only its revision. Read both from the current winning entry.
	update := database.NewBatch().(*Batch)
	defer update.Close()
	if err := update.SetPointerWithRevision(swaps[0].key, swaps[0].oldPtr, page.EntryRevision(73)); err != nil {
		t.Fatal(err)
	}
	if err := update.SetPointerWithRevision(swaps[1].key, swaps[1].newPtr, page.EntryRevision(91)); err != nil {
		t.Fatal(err)
	}
	if err := update.Write(); err != nil {
		t.Fatal(err)
	}
	update.Close()
	snapshot := database.AcquireSnapshot()
	defer snapshot.Close()
	physical := batchpkg.New(database.valueLogManager, database.InlineThreshold())
	defer physical.Close()
	delta, err := collectRewriteSwapPointerMatches(&snapshot.tree, physical, swaps, false)
	defer releaseValueLogRefDelta(delta)
	if err != nil {
		t.Fatal(err)
	}
	entries := physical.SortedEntries()
	if len(entries) != 1 || entries[0].Revision != 73 || entries[0].ValuePtr != swaps[0].newPtr {
		t.Fatalf("matched rewrite must preserve current revision and exclude stale pointer: %+v", entries)
	}
}

// Real selected-source rewrite must preserve logical metadata while replacing
// persistent physical pointers. ReserveRIDs is the supported producer callback:
// its first call occurs after candidate capture and before pointer-swap publish.
func TestSelectedValueLogRewriteRevisionLifetime(t *testing.T) {
	modes := []struct {
		name       string
		command    bool
		durability DurabilityMode
	}{
		{"no-wal", false, DurabilityWALOffRelaxed},
		{"command-wal-durable", true, DurabilityDurable},
		{"command-wal-relaxed", true, DurabilityWALOnRelaxed},
	}
	for _, mode := range modes {
		for _, change := range []string{"unchanged", "revision-only-winner", "different-pointer-winner"} {
			t.Run(mode.name+"/"+change, func(t *testing.T) {
				dir := t.TempDir()
				opts := Options{Dir: dir, CommandWAL: mode.command, Durability: mode.durability, DisableBackgroundPrune: true, KeepRecent: 1,
					ValueLog: ValueLogOptions{PointerThreshold: 1, Compression: ValueLogCompressionOff}}
				database, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if database != nil {
						if err := database.Close(); err != nil {
							t.Errorf("Close: %v", err)
						}
					}
				})
				if database.commandWAL != mode.command {
					t.Fatal("wrong native WAL mode")
				}
				original := bytes.Repeat([]byte("selected-persistent-payload/"), 128)
				replacement := bytes.Repeat([]byte("unselected-new-winner/"), 128)
				selected := appendPointersInNewSegment(t, dir, 0, 1, 500000, 2, func(int) []byte { return original })
				outside := appendPointersInNewSegment(t, dir, 0, 2, 500010, 1, func(int) []byte { return replacement })[0]
				write := database.NewBatch().(*Batch)
				defer write.Close()
				for i, key := range []string{"a", "b"} {
					if err := write.SetPointerWithRevision([]byte(key), selected[i], page.EntryRevision(73+i)); err != nil {
						t.Fatal(err)
					}
				}
				if err := write.SetPointerWithRevision([]byte("outside"), outside, 91); err != nil {
					t.Fatal(err)
				}
				if err := write.WriteSync(); err != nil {
					t.Fatal(err)
				}
				closeNoErr(t, write)
				old := database.AcquireSnapshot()
				if old == nil {
					t.Fatal("missing old snapshot")
				}
				defer old.Close()
				assertSelectedRewriteRevision(t, old, "a", original, 73, selected[0])
				assertSelectedRewriteRevision(t, old, "b", original, 74, selected[1])
				calls := 0
				nextRID := uint64(1000000)
				changedPtr := selected[1]
				changedValue := original
				changedRevision := page.EntryRevision(74)
				stats, err := database.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
					SourceFileIDs: []uint32{selected[0].FileID}, BatchSize: 2, SyncEachBatch: true,
					ReserveRIDs: func(count int) (uint64, error) {
						calls++
						if calls == 1 && change != "unchanged" {
							changedRevision = 991
							if change == "different-pointer-winner" {
								changedPtr = outside
								changedValue = replacement
							}
							winner := database.NewBatch().(*Batch)
							defer winner.Close()
							if err := winner.SetPointerWithRevision([]byte("b"), changedPtr, changedRevision); err != nil {
								return 0, err
							}
							if err := winner.WriteSync(); err != nil {
								return 0, err
							}
						}
						start := nextRID
						nextRID += uint64(count)
						return start, nil
					},
				})
				if err != nil {
					t.Fatalf("real selected rewrite: %v", err)
				}
				if calls != 1 || stats.RecordsCopied != 2 || stats.SourceSegmentsUnreferenced != 1 {
					t.Fatalf("candidate capture/copy not exercised: callbackCalls%d stats%+v", calls, stats)
				}
				current := database.AcquireSnapshot()
				if current == nil {
					t.Fatal("missing current snapshot")
				}
				defer current.Close()
				a, err := current.GetEntryExact([]byte("a"))
				if err != nil {
					t.Fatal(err)
				}
				b, err := current.GetEntryExact([]byte("b"))
				if err != nil {
					t.Fatal(err)
				}
				if a.Flags&node.FlagPointer == 0 || a.ValuePtr.FileID == selected[0].FileID {
					t.Fatal("selected pointer a was not physically rewritten")
				}
				if change == "different-pointer-winner" {
					if b.ValuePtr != outside {
						t.Fatal("stale rewrite replaced the newer pointer winner")
					}
				} else if b.Flags&node.FlagPointer == 0 || b.ValuePtr.FileID == selected[1].FileID {
					t.Fatal("selected pointer b was not physically rewritten")
				}
				assertSelectedRewriteRevision(t, current, "a", original, 73, a.ValuePtr)
				assertSelectedRewriteRevision(t, current, "b", changedValue, changedRevision, b.ValuePtr)
				assertSelectedRewriteRevision(t, current, "outside", replacement, 91, outside)
				closeNoErr(t, current)
				// The old native snapshot retains the old physical resource, even though
				// current winners no longer need its selected segment after rewrite.
				assertSelectedRewriteRevision(t, old, "a", original, 73, selected[0])
				assertSelectedRewriteRevision(t, old, "b", original, 74, selected[1])
				closeNoErr(t, old)
				closeNoErr(t, database)
				database = nil
				database, err = Open(opts)
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				reopened := database.AcquireSnapshot()
				defer reopened.Close()
				assertSelectedRewriteRevision(t, reopened, "a", original, 73, a.ValuePtr)
				assertSelectedRewriteRevision(t, reopened, "b", changedValue, changedRevision, b.ValuePtr)
				assertSelectedRewriteRevision(t, reopened, "outside", replacement, 91, outside)
				t.Logf("copied%d selected frames; winning b revision%d; old pointers pinned and reopened revisions retained", stats.RecordsCopied, changedRevision)
			})
		}
	}
}

func assertSelectedRewriteRevision(t *testing.T, s *Snapshot, key string, want []byte, revision page.EntryRevision, ptr page.ValuePtr) {
	t.Helper()
	entry, err := s.GetEntryExact([]byte(key))
	if err != nil {
		t.Fatalf("entry %s: %v", key, err)
	}
	if entry.Flags&node.FlagPointer == 0 || entry.ValuePtr != ptr || entry.Revision != revision {
		t.Fatalf("entry %s pointer%+v revision%d want pointer%+v revision%d", key, entry.ValuePtr, entry.Revision, ptr, revision)
	}
	got, gotRevision, err := s.GetVersioned([]byte(key))
	if err != nil || !bytes.Equal(got, want) || gotRevision != revision {
		t.Fatalf("value %s len%d revision%d want len%d revision%d err%v", key, len(got), gotRevision, len(want), revision, err)
	}
}
