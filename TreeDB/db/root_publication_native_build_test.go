package db

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestNativePhysicalAppendPinLifetime(t *testing.T) {
	for _, outcome := range []string{"publish", "abandon", "append_failure"} {
		t.Run(outcome, func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			appender, err := newReplayInlineAppender(database, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer appender.close()
			database.SetValueLogAppender(appender)
			defer database.SetValueLogAppender(nil)
			group, err := database.BeginRootPublicationBuildGroup()
			if err != nil {
				t.Fatal(err)
			}
			defer group.Close()
			ptrs, err := group.AppendNativeValueLogValues([][]byte{
				bytes.Repeat([]byte("large value"), 512),
				bytes.Repeat([]byte("second large value"), 512),
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(ptrs) != 2 || ptrs[0] == ptrs[1] || ptrs[0].FileID != ptrs[1].FileID {
				t.Fatalf("need two distinct append pointers sharing one pin: %v", ptrs)
			}
			database.pendingValueLogAppendMu.Lock()
			ptrCount, fileRefs := len(database.pendingValueLogAppendPtrRefs), database.pendingValueLogAppendFileIDRefs[ptrs[0].FileID]
			database.pendingValueLogAppendMu.Unlock()
			if ptrCount != 0 || fileRefs != 1 {
				t.Fatalf("physical group must own one segment pin and no pointer pin: ptrs=%d file refs=%d", ptrCount, fileRefs)
			}
			if outcome == "append_failure" {
				database.SetValueLogAppender(nil)
				if _, err := group.AppendNativeValueLogValues([][]byte{[]byte("failure")}); !errors.Is(err, ErrValueLogAppenderUnavailable) {
					t.Fatalf("append error=%v", err)
				}
			} else {
				delta := batch.New(nil, 1)
				defer delta.Close()
				if err := delta.SetPointer([]byte("native/doc"), ptrs[0]); err != nil {
					t.Fatal(err)
				}
				if err := group.BuildNativeSystemDelta(delta); err != nil {
					t.Fatal(err)
				}
				if outcome == "publish" {
					final := database.NewPhysicalBatch().(*Batch)
					defer final.Close()
					if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
						t.Fatal(err)
					}
					if err := final.WriteSync(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := group.Close(); err != nil {
				t.Fatal(err)
			}
			database.pendingValueLogAppendMu.Lock()
			ptrCount, fileCount := len(database.pendingValueLogAppendPtrRefs), len(database.pendingValueLogAppendFileIDRefs)
			database.pendingValueLogAppendMu.Unlock()
			if ptrCount != 0 || fileCount != 0 {
				t.Fatalf("completed group leaked append pins: ptrs=%d files=%d", ptrCount, fileCount)
			}
		})
	}
}

// Preexisting persistent pointers have no AppendNativeValueLogValues call in
// this group. Fresh append tests alone cannot detect missing NoTouch pins.
func TestNativeNoTouchPointerPinCustody(t *testing.T) {
	for _, mode := range []string{"cold", "warm", "warm-range", "metadata-only", "append-failure"} {
		t.Run(mode, func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			writer := newRewriteWriter(ValueLogDirPath(database.dir), 0, 6, 4096)
			writer.ConfigureLeafLog(LeafLogDirPath(database.dir), rewriteLeafLogLaneID, 0)
			database.SetLeafPageLog(writer)
			database.SetCurrentValueLogReadBarrier(func(uint32) error { return writer.Flush() })
			defer database.SetCurrentValueLogReadBarrier(nil)
			defer writer.Close()
			var ptrs [6]page.ValuePtr
			for i := range ptrs {
				ptrs[i] = appendPointersInNewSegment(t, database.dir, 0, uint32(i+1), uint64(100+i), 1, func(int) []byte { return []byte(fmt.Sprintf("persistent-old-%d", i)) })[0]
			}
			if err := database.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			seed := database.NewBatch().(*Batch)
			defer seed.Close()
			for i := 0; i < 5; i++ {
				if err := seed.SetPointer([]byte(fmt.Sprintf("old/%d", i)), ptrs[i]); err != nil {
					t.Fatal(err)
				}
			}
			if err := seed.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if mode != "cold" {
				table := memtable.NewAppendOnlyWithEntryCapacity(5)
				for i := 0; i < 5; i++ {
					table.SetEntry([]byte(fmt.Sprintf("old/%d", i)), nil, ptrs[i], node.FlagPointer)
				}
				table.Freeze()
				if _, err := database.PublishSystemRootIterator(table.NewIterator(nil, nil)); err != nil {
					t.Fatal(err)
				}
			}
			basis := database.AcquireSnapshot()
			defer basis.Close()
			group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
			if err != nil {
				t.Fatal(err)
			}
			defer group.Close()
			if len(group.pinnedValueLogSegments) != 0 || len(database.pendingValueLogAppendPtrs()) != 0 {
				t.Fatal("fixture accidentally supplied fresh append/group pins")
			}

			delta := batch.New(nil, 16)
			defer delta.Close()
			expectedFiles := make(map[uint32]bool)
			if mode == "metadata-only" {
				if err := delta.Set([]byte("new/inline"), []byte("i")); err != nil {
					t.Fatal(err)
				}
			} else {
				for i := 0; i < 5; i++ {
					if err := delta.SetPointerViewNoTouchWithRevision([]byte(fmt.Sprintf("new/%c", 'a'+i)), ptrs[i], page.EntryRevision(41+i)); err != nil {
						t.Fatal(err)
					}
					expectedFiles[ptrs[i].FileID] = true
				}
				// Superseded and deleted submissions stay conservatively pinned;
				// their logical references must not enter the exact ref delta.
				if err := delta.Set([]byte("new/c"), []byte("x")); err != nil {
					t.Fatal(err)
				}
				if err := delta.Delete([]byte("new/d")); err != nil {
					t.Fatal(err)
				}
				if mode == "warm-range" {
					if err := delta.DeleteRange([]byte("new/b"), []byte("new/c")); err != nil {
						t.Fatal(err)
					}
				}
				if len(delta.TouchedValueLogSegments()) != 0 {
					t.Fatal("NoTouch fixture acquired metadata pins")
				}
			}
			// A caller can provide metadata for a file absent from all entries.
			// Keep this independent path when no submitted pointer put names it.
			delta.NoteTouchedValueLogFileID(ptrs[5].FileID)
			expectedFiles[ptrs[5].FileID] = true
			appendFailure := errors.New("original native outer-leaf append failure")
			observed := 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point != durabilitycut.BeforeDependencyAppend || event.Resource != durabilitycut.ResourceOuterLeaf || event.Root != database.dir {
					return nil
				}
				observed++
				// The builder already holds group.mu. Do not reenter it here.
				for fileID := range expectedFiles {
					if _, ok := group.pinnedValueLogSegments[fileID]; !ok {
						return fmt.Errorf("missing pre-apply native segment pin: file=%d mode=%s", fileID, mode)
					}
				}
				if mode == "append-failure" {
					return appendFailure
				}
				return nil
			})
			if mode == "cold" {
				// The backend's empty system tree has a physical root already.
				// Request the supported cold private-root primitive explicitly;
				// this output is abandoned, never assigned to the system basis.
				_, err = group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: delta})
			} else {
				err = group.BuildNativeSystemDelta(delta)
			}
			restore()
			if observed == 0 {
				t.Fatal("fixture never entered physical outer-leaf append")
			}
			if mode == "append-failure" {
				if !errors.Is(err, appendFailure) || !group.failed {
					t.Fatalf("original failed build error/custody: failed=%t err=%v", group.failed, err)
				}
				if err := group.BuildNativeSystemDelta(delta); err == nil {
					t.Fatal("failed original group accepted replay")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			for fileID := range expectedFiles {
				database.pendingValueLogAppendMu.Lock()
				refs := database.pendingValueLogAppendFileIDRefs[fileID]
				database.pendingValueLogAppendMu.Unlock()
				if refs != 1 {
					t.Fatalf("group segment refs=%d want=1 file=%d", refs, fileID)
				}
			}
			if mode != "append-failure" {
				changes := make(map[uint32]int64)
				if group.vlogRefDelta == nil {
					t.Fatal("missing exact reference delta")
				}
				if err := group.vlogRefDelta.forEachChange(func(id uint32, n int64) error { changes[id] += n; return nil }); err != nil {
					t.Fatal(err)
				}
				for i, ptr := range ptrs {
					want := int64(0)
					if mode != "metadata-only" && (i == 0 || i == 4 || (i == 1 && mode != "warm-range")) {
						want = 1
					}
					if changes[ptr.FileID] != want {
						t.Fatalf("logical ref delta file%d=%d want=%d (pins must not substitute refs)", i, changes[ptr.FileID], want)
					}
				}
				if mode != "cold" {
					// Reapply identical pointers: original segment pins deduplicate,
					// while exact reference accounting remains net unchanged.
					if err := group.BuildNativeSystemDelta(delta); err != nil {
						t.Fatal(err)
					}
					for fileID := range expectedFiles {
						database.pendingValueLogAppendMu.Lock()
						refs := database.pendingValueLogAppendFileIDRefs[fileID]
						database.pendingValueLogAppendMu.Unlock()
						if refs != 1 {
							t.Fatalf("repeated build duplicated segment pin: file=%d refs=%d", fileID, refs)
						}
					}
					final := database.NewPhysicalBatch().(*Batch)
					defer final.Close()
					if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
						t.Fatal(err)
					}
					if err := final.WriteSync(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := group.Close(); err != nil {
				t.Fatal(err)
			}
			if len(database.pendingValueLogAppendPtrs()) != 0 || len(database.pendingValueLogAppendFileIDs()) != 0 {
				t.Fatal("group completion/abandon leaked append pins")
			}
			for i := 0; i < 5; i++ {
				got, err := basis.Get([]byte(fmt.Sprintf("old/%d", i)))
				if err != nil || string(got) != fmt.Sprintf("persistent-old-%d", i) {
					t.Fatalf("held original pointer %d lost: %q err=%v", i, got, err)
				}
			}
		})
	}
}
