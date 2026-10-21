package treedb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Exercise physical retirement, not merely reads surviving a no-op GC. The old
// value is replaced at the SAME timestamp, so retained MVCC history does not
// keep its segment reachable. A snapshot captured before checkpoint owns its
// original memtable generation; its child iterator becomes the final owner.
func TestCOWOrderedGCRetiresOldOnlySegmentAfterFinalOwner(t *testing.T) {
	// Keep segment production and explicit public GC independent of automatic
	// generational maintenance. These switches do not disable ValueLogGC or
	// the manager's owned zombie-deletion completion worker.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_CHECKPOINT_KICK", "1")
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := cowOrderedDurabilityOptions(profile, t.TempDir())
			opts.MemtableShards = 1
			opts.ValueLog.Generational.Policy = backenddb.ValueLogGenerationHotWarmCold
			opts.ValueLog.Generational.HotSegmentTargetBytes = 4 << 10
			db := cowOrderedDurabilityOpen(t, opts)
			defer db.Close()
			store := mvcc.New(db)
			key := []byte("retire-old-only-segment")
			const timestamp, replacements = 10, 12
			physicalKey, err := mvcckey.Encode(key, timestamp)
			if err != nil {
				t.Fatal(err)
			}
			valueFor := func(round int) []byte {
				// Every incompressible-by-policy record exceeds the hot target.
				// Checkpoints can use separate native producer lanes; observe
				// distinct pointers below rather than assuming one file/write.
				return bytes.Repeat([]byte(fmt.Sprintf("persistent-round-%03d/", round)), 512)
			}
			write := func(round int) {
				t.Helper()
				if err := store.CommitAt(timestamp, []mvcc.Mutation{{Key: key, Value: valueFor(round)}}, mvcc.CommitRelaxed); err != nil {
					t.Fatalf("same-timestamp replacement %d: %v", round, err)
				}
			}
			write(0)
			oldCut := db.AcquireSnapshot()
			if oldCut == nil {
				t.Fatal("original memtable cut unavailable")
			}
			oldCutOpen := true
			defer func() {
				if oldCutOpen {
					_ = oldCut.Close()
				}
			}()
			oldEncoded, oldRevision, err := oldCut.GetVersioned(physicalKey)
			if err != nil || oldRevision == treedb.LegacyEntryRevision {
				t.Fatalf("original pointer version: revision=%d err=%v", oldRevision, err)
			}
			oldEncoded = bytes.Clone(oldEncoded)
			oldEntry, err := oldCut.GetEntryExact(physicalKey)
			if err != nil || oldEntry.Flags&node.FlagPointer == 0 || oldEntry.ValuePtr.FileID == 0 {
				t.Fatalf("original entry did not use a persistent pointer: %+v err=%v", oldEntry, err)
			}
			// DisableSideStores makes opts.Dir the backend storage root.
			oldPath := valuelog.SegmentPath(backenddb.ValueLogDirPath(opts.Dir), oldEntry.ValuePtr.FileID)
			oldInfo, err := os.Stat(oldPath)
			if err != nil {
				t.Fatal(err)
			}
			oldIterator, err := oldCut.Iterator(physicalKey, nil)
			if err != nil {
				t.Fatal(err)
			}
			iteratorOpen := true
			defer func() {
				if iteratorOpen {
					_ = oldIterator.Close()
				}
			}()
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			fileIDs := map[uint32]struct{}{page.ValueLogSegmentID(oldEntry.ValuePtr.FileID): {}}
			var current []cowOrderedPhysicalVersion
			// Fixed churn advances both durable recovery slots and all current
			// overlays beyond the original source. This never retries writes or
			// checkpoints until a desired GC result happens.
			for round := 1; round <= replacements; round++ {
				write(round)
				if err := db.Checkpoint(); err != nil {
					t.Fatalf("replacement checkpoint %d: %v", round, err)
				}
				current = cowOrderedCapturePhysical(t, db, []cowOrderedVersion{{key: key, ts: timestamp, state: mvcc.Present, value: valueFor(round)}})
				if len(current) != 1 || current[0].revision <= oldRevision {
					t.Fatalf("replacement %d lost native revision progress: %+v", round, current)
				}
				fileIDs[page.ValueLogSegmentID(current[0].ptr.FileID)] = struct{}{}
			}
			if len(fileIDs) < 4 || page.ValueLogSegmentID(current[0].ptr.FileID) == page.ValueLogSegmentID(oldEntry.ValuePtr.FileID) {
				t.Fatalf("fixed trace did not produce independent successor segments: distinct=%d old=%+v current=%+v", len(fileIDs), oldEntry.ValuePtr, current[0].ptr)
			}
			gc, err := db.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{Mode: treedb.ValueLogGCModeStrict})
			if err != nil || gc.FailClosedToDryRun || gc.SegmentsEligible == 0 || gc.BytesEligible == 0 {
				t.Fatalf("real strict GC did not retire unreachable source segments: %+v err=%v", gc, err)
			}
			requireOriginalFile := func(stage string) {
				t.Helper()
				info, err := os.Stat(oldPath)
				if err != nil || !os.SameFile(oldInfo, info) {
					t.Fatalf("%s lost the original persistent segment identity: %v", stage, err)
				}
			}
			requireOriginalFile("held snapshot after GC")
			if value, revision, err := oldCut.GetVersioned(physicalKey); err != nil || revision != oldRevision || !bytes.Equal(value, oldEncoded) {
				t.Fatalf("GC invalidated original history cut: revision=%d err=%v equal=%t", revision, err, bytes.Equal(value, oldEncoded))
			}
			oldIterator.Seek(physicalKey)
			if !oldIterator.Valid() || !bytes.Equal(oldIterator.Key(), physicalKey) || !bytes.Equal(oldIterator.Value(), oldEncoded) || oldIterator.Error() != nil {
				t.Fatalf("GC invalidated original history iterator: valid=%t err=%v", oldIterator.Valid(), oldIterator.Error())
			}
			cowOrderedRequirePhysical(t, db, current)

			if err := oldCut.Close(); err != nil {
				t.Fatal(err)
			}
			oldCutOpen = false
			if oldIterator.Valid() || !errors.Is(oldIterator.Error(), backenddb.ErrClosed) {
				t.Fatal("snapshot Close did not invalidate its still-owned child iterator")
			}
			requireOriginalFile("final child iterator still owns cut")
			iteratorErr := oldIterator.Close()
			iteratorOpen = false
			if iteratorErr != nil {
				t.Fatal(iteratorErr)
			}

			// No second GC, write, checkpoint, or DB Close can help this pass.
			// Wait only for the existing original zombie worker, which may have
			// deferred unlink until this cut's final identity pin was released.
			deadline := time.Now().Add(5 * time.Second)
			for {
				_, err := os.Stat(oldPath)
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				if err != nil || time.Now().After(deadline) {
					t.Fatalf("final owner did not retire exact old-only segment %q: err=%v first_gc=%+v", oldPath, err, gc)
				}
				time.Sleep(10 * time.Millisecond)
			}
			cowOrderedRequireVersions(t, store, []cowOrderedVersion{{key: key, ts: timestamp, state: mvcc.Present, value: valueFor(replacements)}})
			cowOrderedRequirePhysical(t, db, current)
		})
	}
}
