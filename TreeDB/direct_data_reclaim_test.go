package treedb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	tree "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/page"
)

// Exercise real backend-root writes and natural value-log rotations through the
// maintained public factory. Requested COW physical policy uses ordinary btree
// cache services in this role; this does not certify native semantic consumers.
func TestDirectDATAReclaimsAfterOldViewRelease(t *testing.T) {
	opts := directDATAOptions(directDATAFixtureDir(t, "reclaim"))
	opts.MemtableShards = 2
	opts.ValueLog.Compression = tree.ValueLogCompressionOff
	opts.ValueLog.Generational.Policy = backenddb.ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.HotSegmentTargetBytes = 4 << 10
	opts.ValueLog.Generational.LeafSegmentTargetBytes = 4 << 10
	backend, cleanup, stats, err := tree.OpenBackendWithCachedLeafLogStats(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanup != nil {
			if err := cleanup(); err != nil {
				t.Error(err)
			}
		}
	})
	actual := stats()
	if backend.HasNativeSemanticBridge() || actual["treedb.cache.memtable_mode"] != "btree" || actual["treedb.profile.resolved"] != string(tree.ProfileNoWALFast) || actual["treedb.write_path.redo_log"] != "off" {
		t.Fatalf("backend-root NoWAL activation: native=%t stats=%v", backend.HasNativeSemanticBridge(), actual)
	}

	// Fixed logical churn, rather than a retry-until-reclaimed loop. Each batch
	// replaces one pointer, updates inline data, and alternates insert/delete.
	const rounds, pinnedRounds = 32, 8
	states := make(map[uint64]map[string][]byte, rounds+1)
	fileIDs := make(map[uint32]struct{})
	write := func(epoch int) (uint64, page.ValuePtr) {
		t.Helper()
		value := bytes.Repeat([]byte(fmt.Sprintf("persistent-pointer-%03d/", epoch)), 512)
		ptrs, err := backend.AppendValueLogValues([][]byte{value})
		if err != nil || len(ptrs) != 1 || ptrs[0].FileID == 0 {
			t.Fatalf("append epoch %d: %v %v", epoch, ptrs, err)
		}
		batch := backend.NewBatch()
		pointerBatch, ok := batch.(interface {
			SetPointer([]byte, page.ValuePtr) error
		})
		if !ok {
			_ = batch.Close()
			t.Fatal("backend batch missing exported SetPointer")
		}
		inline := []byte(fmt.Sprintf("inline-%03d", epoch))
		changeErr := errors.Join(batch.Set([]byte("a"), inline), pointerBatch.SetPointer([]byte("b"), ptrs[0]))
		want := map[string][]byte{"a": inline, "b": value}
		if epoch%2 == 0 {
			changeErr = errors.Join(changeErr, batch.Set([]byte("c"), []byte("present")))
			want["c"] = []byte("present")
		} else {
			changeErr = errors.Join(changeErr, batch.Delete([]byte("c")))
		}
		if changeErr != nil {
			_ = batch.Close()
			t.Fatal(changeErr)
		}
		if err := errors.Join(batch.Write(), batch.Close()); err != nil {
			t.Fatal("ordinary volatile batch", err)
		}
		accepted, ok := backend.StateToken()
		if !ok {
			t.Fatal("accepted backend state unavailable")
		}
		if err := backend.Checkpoint(); err != nil {
			t.Fatal("explicit DATA seal", err)
		}
		// Batch publication transfers the pending appender pointers into the
		// root's ordinary ownership; no test-only release or GC override is used.
		states[accepted.CommitSeq] = want
		fileIDs[ptrs[0].FileID] = struct{}{}
		directDATAValues(t, backend.Get, want)
		return accepted.CommitSeq, ptrs[0]
	}
	seedCommit, seedPtr := write(0)
	seedSlots := directDATAReadSlots(t, opts.Dir)
	seedSlot := 0
	if seedSlots[1].meta.CommitSeq > seedSlots[0].meta.CommitSeq {
		seedSlot = 1
	}
	seedRecord := seedSlots[seedSlot].meta.RootRecordPageID
	if seedSlots[seedSlot].meta.CommitSeq != seedCommit {
		t.Fatal("seed accepted root was not sealed")
	}
	held := backend.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing seed snapshot")
	}
	t.Cleanup(func() { _ = held.Close() })
	oldFile, oldIdentity, err := held.PinnedValueLogFile(seedPtr.FileID)
	if err != nil {
		t.Fatal("seed registered persistent file", err)
	}
	oldPath := oldFile.Name()
	oldInfo, err := oldFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var reusedSeed bool
	var deletedBytes int64
	var pinnedHigh, finalHigh, finalRetired uint64
	var finalSlots [2]directDATASlot
	for epoch := 1; epoch <= rounds; epoch++ {
		commit, _ := write(epoch)
		finalSlots = directDATAReadSlots(t, opts.Dir)
		newest := 0
		if finalSlots[1].meta.CommitSeq > finalSlots[0].meta.CommitSeq {
			newest = 1
		}
		if finalSlots[newest].meta.CommitSeq != commit || finalSlots[1-newest].meta.CommitSeq >= commit {
			t.Fatalf("epoch %d did not advance two DATA slots: %+v", epoch, finalSlots)
		}
		view := backend.AcquireSnapshot()
		if view == nil {
			t.Fatal("missing immutable freelist view")
		}
		generation, loadErr := freelist.LoadGenerationV1(view.Pager(), finalSlots[newest].record.Freelist)
		if loadErr != nil {
			_ = view.Close()
			t.Fatal(loadErr)
		}
		for _, extent := range generation.ReservationRecord().Entries() {
			if (extent.Kind == freelist.ReservationReusedData || extent.Kind == freelist.ReservationTargetMetadata) && seedRecord >= extent.StartPageID && seedRecord-extent.StartPageID < uint64(extent.Count) {
				if epoch <= pinnedRounds {
					t.Fatalf("seed record %d reused while old view held at commit %d", seedRecord, commit)
				}
				reusedSeed = true
			}
		}
		if epoch <= pinnedRounds && generation.Allocatable(seedRecord) {
			t.Fatalf("seed record %d became allocatable while old view held", seedRecord)
		}
		finalHigh, finalRetired = generation.HighWater(), generation.RetiredCount()
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
		gc, err := backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{ObservedSourceFileIDs: []uint32{seedPtr.FileID}})
		if err != nil {
			t.Fatalf("epoch %d public GC: %v", epoch, err)
		}
		deletedBytes += gc.BytesDeleted
		if epoch <= pinnedRounds {
			directDATAValues(t, held.Get, states[seedCommit])
			current, err := os.Stat(oldPath)
			if err != nil || !os.SameFile(oldInfo, current) {
				t.Fatalf("held pointer lost its original persistent file: %v", err)
			}
			if gc.ObservedSourceSegmentsDeleted != 0 {
				t.Fatal("GC deleted held pointer source")
			}
		}
		if epoch == pinnedRounds {
			pinnedHigh = finalHigh
			if err := held.Close(); err != nil {
				t.Fatal("release original view", err)
			}
		}
	}
	if len(fileIDs) < 3 || !reusedSeed || deletedBytes == 0 {
		t.Fatalf("missing natural rotations/reuse/reclaim: files=%d reusedSeed=%t deletedBytes=%d", len(fileIDs), reusedSeed, deletedBytes)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("released unreachable pointer source was not reclaimed: %v", err)
	}
	for _, key := range []string{"treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total"} {
		if got := backend.Stats()[key]; got != "0" {
			t.Fatalf("NoWAL route used WAL: %s=%q", key, got)
		}
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatal("final drain", err)
	}
	closeOriginal := cleanup
	cleanup = nil
	if err := closeOriginal(); err != nil {
		t.Fatal(err)
	}
	if slots := directDATAReadSlots(t, opts.Dir); slots != finalSlots {
		t.Fatal("final drain/Close changed the sealed roots")
	}
	newest := 0
	if finalSlots[1].meta.CommitSeq > finalSlots[0].meta.CommitSeq {
		newest = 1
	}
	latest, err := tree.Open(directDATAReadOnlyOptions(opts))
	if err != nil {
		t.Fatal("latest reopen after reuse and GC", err)
	}
	directDATAValues(t, latest.Get, states[finalSlots[newest].meta.CommitSeq])
	if err := latest.Close(); err != nil {
		t.Fatal(err)
	}

	// Keep the newest META bytes before damaging that slot in the original
	// namespace. Copying dependencies would invalidate their stable identities.
	index, err := os.OpenFile(filepath.Join(opts.Dir, "maindb", "index.db"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	image := make([]byte, page.PageSize)
	offset := int64(newest) * int64(page.PageSize)
	if _, err := index.ReadAt(image, offset); err != nil {
		_ = index.Close()
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directDATAFixtureDir(t, "reclaim-meta"), "newest-meta.page"), image, 0600); err != nil {
		_ = index.Close()
		t.Fatal(err)
	}
	image[page.PageHeaderSize] ^= 1
	_, writeErr := index.WriteAt(image, offset)
	if err := errors.Join(writeErr, index.Sync(), index.Close()); err != nil {
		t.Fatal(err)
	}
	fallback, err := tree.Open(directDATAReadOnlyOptions(opts))
	if err != nil {
		t.Fatal("fallback reopen after actual reuse and GC", err)
	}
	defer fallback.Close()
	olderCommit := finalSlots[1-newest].meta.CommitSeq
	if got := fallback.Stats()["treedb.durable_root.commit_seq"]; got != strconv.FormatUint(olderCommit, 10) {
		t.Fatalf("fallback commit=%s want=%d", got, olderCommit)
	}
	directDATAValues(t, fallback.Get, states[olderCommit])
	t.Logf("backend-root DATA: rounds=%d pinned=%d pointerFiles=%d seedRecord=%d reused=%t reclaimedBytes=%d highWaterPinned=%d final=%d finalRetired=%d oldIdentity=%+v survivingCommit=%d", rounds, pinnedRounds, len(fileIDs), seedRecord, reusedSeed, deletedBytes, pinnedHigh, finalHigh, finalRetired, oldIdentity, olderCommit)
}
