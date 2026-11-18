package db

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafGenerationPlan_ReopensWithUnusableAdvisoryIndex(t *testing.T) {
	for _, scenario := range []string{"unchanged", "missing", "malformed", "truncated", "wrong_segment", "wrong_lengths"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			db, leafLog, opts := openLeafLogLaneGCTestDB(t, 32<<10)
			t.Cleanup(func() { closeLeafLogLaneGCTestDB(t, db, leafLog) })
			writeLeafGenerationKeys(t, db, "older", 1024, 'a')
			writeLeafGenerationKeys(t, db, "newer", 1024, 'b')
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			before, err := db.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var sealedRawID uint32
			for _, generation := range before.Generations {
				if generation.State == leafGenerationStateSealed && generation.LivePages > 0 {
					sealedRawID = generation.FileIDs[0]
					break
				}
			}
			if sealedRawID == 0 {
				t.Fatalf("natural rollover created no live sealed generation: %+v", before.Generations)
			}
			closeLeafLogLaneGCTestDB(t, db, leafLog)
			db, leafLog = nil, nil

			path := leafGenerationRecordLengthIndexPath(opts.Dir, sealedRawID)
			contents, err := os.ReadFile(path)
			if err != nil || len(contents) < 24 {
				t.Fatalf("produced advisory index %q: %d bytes, %v", path, len(contents), err)
			}
			switch scenario {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				copy(contents[:4], "bad!")
			case "truncated":
				contents = contents[:9]
			case "wrong_segment":
				binary.LittleEndian.PutUint32(contents[8:12], sealedRawID+1)
			case "wrong_lengths":
				for offset := 20; offset+4 <= len(contents); offset += 8 {
					binary.LittleEndian.PutUint32(contents[offset:offset+4], 1)
				}
			}
			if scenario != "unchanged" && scenario != "missing" {
				if err := os.WriteFile(path, contents, 0600); err != nil {
					t.Fatal(err)
				}
			}

			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen with unchanged leaf data: %v", err)
			}
			t.Cleanup(func() { closeLeafLogLaneGCTestDB(t, reopened, nil) })
			for i := 0; i < 1024; i++ {
				expectLeafGenerationValue(t, reopened, leafGenerationKey("older", i), 'a')
				expectLeafGenerationValue(t, reopened, leafGenerationKey("newer", i), 'b')
			}
			scanCounter := withLeafGenerationRecordLengthIndexScanCounter(t)
			after, err := reopened.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{})
			if err != nil {
				t.Fatalf("first plan after checked reopen and complete reads: %v", err)
			}
			if !reflect.DeepEqual(before.liveStats, after.liveStats) {
				t.Fatalf("live totals changed with unchanged leaf data: before=%+v after=%+v", before.liveStats, after.liveStats)
			}
			if scanned := scanCounter.Load(); scanned != 0 {
				t.Fatalf("first plan unexpectedly performed %d full segment scans", scanned)
			}
			if scenario == "wrong_lengths" {
				checkLeafPlanWithoutInlineLength(t, reopened, sealedRawID)
			}
		})
	}
}

func checkLeafPlanWithoutInlineLength(t *testing.T, db *DB, sealedRawID uint32) {
	t.Helper()
	ctx := context.Background()
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing reopened snapshot")
	}
	defer func() {
		if err := snap.Close(); err != nil {
			t.Errorf("snapshot Close: %v", err)
		}
	}()
	var ptr page.LeafLogPtr
	if err := leafrefscan.Walk(ctx, snap.state.RootPageID, snap.idx.pager.Get, nil, func(ref page.LeafLogPtr) error {
		if ref.FileID == sealedRawID && ref.RecordLength() != 0 && ptr.FileID == 0 {
			ptr = ref
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if ptr.FileID == 0 {
		t.Fatalf("missing live sealed pointer for file %d", sealedRawID)
	}
	withLength, err := db.newLeafGenerationScanContext(snap)
	if err != nil {
		t.Fatal(err)
	}
	want, err := db.scanLeafGenerationPtrTotals(withLength, nil, ptr)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the actual physical record reference and model the supported
	// persisted representation with an omitted inline length.
	inlineLength := ptr.RecordLength()
	ptr.RecordLengthHint &^= inlineLength
	withoutLength, err := db.newLeafGenerationScanContext(snap)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.scanLeafGenerationPtrTotals(withoutLength, nil, ptr)
	if err != nil {
		t.Fatalf("authoritative accounting for pointer without inline length: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("false sidecar length changed accounting for unchanged source: got=%+v want=%+v", got, want)
	}
	retainedFileState := withoutLength.fileStateByID[ptr.FileID]
	if withLength.fileStateByID[ptr.FileID].sealedExtent != 0 || retainedFileState.sealedExtent < ptr.Offset+uint64(inlineLength) {
		t.Fatalf("expected a checked sealed extent only for the omitted length: inline=%d omitted=%d",
			withLength.fileStateByID[ptr.FileID].sealedExtent, retainedFileState.sealedExtent)
	}
	length, err := db.leafGenerationRecordLengthFromSource(retainedFileState, ptr)
	if err != nil || length != inlineLength {
		t.Fatalf("authoritative header length = %d, want %d, err=%v", length, inlineLength, err)
	}
	if err := retainedFileState.checkRecordExtent(retainedFileState.sealedExtent, inlineLength); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("record past the checked sealed extent: %v", err)
	}
	// Test authority errors without changing or closing the DB-owned source.
	retained := snap.state.ValueLogSet.Files[ptr.ValueLogFileID()]
	closed, err := os.Open(retained.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	truncated, err := os.CreateTemp(t.TempDir(), "leaf-header-only")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := truncated.Close(); err != nil {
			t.Error(err)
		}
	})
	var header [valuelog.HeaderSize]byte
	recordStart := int64(ptr.Offset) - 4
	if _, err := retained.File.ReadAt(header[:], recordStart); err != nil {
		t.Fatal(err)
	}
	if _, err := truncated.WriteAt(header[:], recordStart); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		source *valuelog.File
		want   error
	}{
		{nil, valuelog.ErrFileNotFound},
		{&valuelog.File{ID: retained.ID + 1, Path: retained.Path, File: retained.File}, valuelog.ErrFileNotFound},
		{&valuelog.File{ID: retained.ID, Path: retained.Path, File: closed}, os.ErrClosed},
		{&valuelog.File{ID: retained.ID, Path: retained.Path, File: truncated}, io.ErrUnexpectedEOF},
	} {
		state := *snap.state
		state.ValueLogSet = &valuelog.Set{Files: map[uint32]*valuelog.File{retained.ID: test.source}}
		failed, err := db.newLeafGenerationScanContext(&Snapshot{state: &state})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.scanLeafGenerationPtrTotals(failed, nil, ptr); !errors.Is(err, test.want) {
			t.Fatalf("expected authoritative source error %v, got %v", test.want, err)
		}
	}
}
