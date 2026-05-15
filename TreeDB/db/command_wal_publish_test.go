package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCommandWALAppliedCommandLSNMetaFieldRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if state == nil {
		t.Fatalf("missing state")
	}
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if got := db.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after publish=%d, want 1", got)
	}
	if got := db.Stats()["treedb.applied_command_lsn"]; got != "1" {
		t.Fatalf("stats applied_command_lsn=%q, want 1", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("reopen AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALAppliedCommandLSNAlternatingMetaPages(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publish lsn 1: %v", err)
	}
	firstMetaPage := db.metaPageID
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 2, Last: 2}}, true); err != nil {
		t.Fatalf("publish lsn 2: %v", err)
	}
	secondMetaPage := db.metaPageID
	if firstMetaPage == secondMetaPage {
		t.Fatalf("meta page did not alternate: first=%d second=%d", firstMetaPage, secondMetaPage)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("reopen AppliedCommandLSN=%d, want 2", got)
	}
}

func TestCommandWALLegacyMetaDecodeIgnoresReservedAppliedLSNBytes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	activeMetaPage := db.metaPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	writeLegacyMetaReservedBytes(t, dir, activeMetaPage, 12345)
	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()
	if got := reopen.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 without command WAL V1 in-page marker", got)
	}
}

func TestCommandWALRootsAndAppliedCommandLSNPublishAtomically(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	before := *db.State()
	if err := db.publishCommandWALRoots(before.RootPageID, before.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	activeMetaPage := db.metaPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	corruptIndexPageByte(t, dir, activeMetaPage)
	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after corrupt active meta: %v", err)
	}
	defer reopen.Close()
	after := reopen.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("CommitSeq=%d, want previous durable tuple %d", after.CommitSeq, before.CommitSeq)
	}
	if after.AppliedCommandLSN != before.AppliedCommandLSN {
		t.Fatalf("AppliedCommandLSN=%d, want previous durable tuple %d", after.AppliedCommandLSN, before.AppliedCommandLSN)
	}
	if after.RootPageID != before.RootPageID || after.SystemRootPageID != before.SystemRootPageID {
		t.Fatalf("roots=(%d,%d), want previous durable tuple (%d,%d)", after.RootPageID, after.SystemRootPageID, before.RootPageID, before.SystemRootPageID)
	}
}

func TestCommandWALPublishHelperRejectsRootsWithoutAppliedLSN(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	before := db.State()
	err = db.publishCommandWALRoots(before.RootPageID+1, before.SystemRootPageID, before.AppliedCommandLSN, nil, false)
	if !errors.Is(err, ErrCommandWALSplitPublish) {
		t.Fatalf("publishCommandWALRoots error=%v, want ErrCommandWALSplitPublish", err)
	}
	after := db.State()
	if after.CommitSeq != before.CommitSeq || after.AppliedCommandLSN != before.AppliedCommandLSN {
		t.Fatalf("state changed after rejected publish: before=%+v after=%+v", before, after)
	}
}

func TestCommandWALAppliedLSNContiguousPrefixOnly(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current uint64
		next    uint64
		covered []CommandWALLSNRange
		wantErr error
	}{
		{name: "same", current: 5, next: 5},
		{name: "single", current: 5, next: 6, covered: []CommandWALLSNRange{{First: 6, Last: 6}}},
		{name: "adjacent", current: 5, next: 8, covered: []CommandWALLSNRange{{First: 6, Last: 7}, {First: 8, Last: 8}}},
		{name: "overlap", current: 5, next: 8, covered: []CommandWALLSNRange{{First: 6, Last: 7}, {First: 7, Last: 9}}},
		{name: "gap", current: 5, next: 8, covered: []CommandWALLSNRange{{First: 7, Last: 8}}, wantErr: ErrCommandWALAppliedLSNNonContig},
		{name: "regression", current: 5, next: 4, wantErr: ErrCommandWALAppliedLSNRegression},
		{name: "same with stale coverage", current: 5, next: 5, covered: []CommandWALLSNRange{{First: 6, Last: 6}}, wantErr: ErrCommandWALAppliedLSNNonContig},
		{name: "empty coverage", current: 5, next: 6, wantErr: ErrCommandWALAppliedLSNNonContig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateContiguousAppliedCommandLSN(tc.current, tc.next, tc.covered)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("validateContiguousAppliedCommandLSN: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("validateContiguousAppliedCommandLSN error=%v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCommandWALAppliedLSNValidationDoesNotMutateCoverage(t *testing.T) {
	covered := []CommandWALLSNRange{{First: 3, Last: 3}, {First: 1, Last: 2}}
	original := append([]CommandWALLSNRange(nil), covered...)
	if err := validateContiguousAppliedCommandLSN(0, 3, covered); err != nil {
		t.Fatalf("validateContiguousAppliedCommandLSN: %v", err)
	}
	if !reflect.DeepEqual(covered, original) {
		t.Fatalf("coverage mutated: got %+v want %+v", covered, original)
	}
}

func TestCommandWALAppliedLSNContiguousPrefixMatchesModelStress(t *testing.T) {
	rng := rand.New(rand.NewSource(0x5eed))
	for iter := 0; iter < 1000; iter++ {
		current := uint64(rng.Intn(12))
		next := uint64(rng.Intn(16))
		if iter%11 == 0 {
			next = current
		}
		if iter%37 == 0 {
			current = ^uint64(0)
			next = ^uint64(0)
		}
		ranges := make([]CommandWALLSNRange, rng.Intn(8))
		for i := range ranges {
			first := uint64(rng.Intn(18))
			last := first + uint64(rng.Intn(5))
			if rng.Intn(13) == 0 {
				first = 0
			}
			if rng.Intn(17) == 0 {
				last = first - 1
			}
			ranges[i] = CommandWALLSNRange{First: first, Last: last}
		}
		if rng.Intn(2) == 0 {
			sort.Slice(ranges, func(i, j int) bool {
				if ranges[i].First != ranges[j].First {
					return ranges[i].First < ranges[j].First
				}
				return ranges[i].Last < ranges[j].Last
			})
		}

		err := validateContiguousAppliedCommandLSN(current, next, ranges)
		wantErr := modelValidateContiguousAppliedCommandLSN(current, next, ranges)
		if (err == nil) != (wantErr == nil) {
			t.Fatalf("iter %d current=%d next=%d ranges=%+v err=%v wantErr=%v", iter, current, next, ranges, err, wantErr)
		}
		if wantErr != nil && !errors.Is(err, wantErr) {
			t.Fatalf("iter %d current=%d next=%d ranges=%+v err=%v wantErr=%v", iter, current, next, ranges, err, wantErr)
		}
	}
}

func TestCommandWALReadOnlyOpenWithUnappliedFrameFailsRecoveryRequired(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-only error=%v, want ErrRecoveryRequired", err)
	}
	_, err = openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("openReadOnlyNoLock error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALReadOnlyOpenAllowsFramesCoveredByAppliedLSN(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("covered command WAL segment should remain until cleanup proof: %v", err)
	}
}

func TestCommandWALWriteOpenSkipsCoveredFramesBeforeLegacyReplay(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open read-write with covered command WAL: %v", err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("covered command WAL segment should be skipped, not raw-replayed or removed: %v", err)
	}
}

func TestCommandWALLegacyReplayFilterPreservesNonCommandOrderAroundCoveredFrame(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	coveredPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	info, err := os.Stat(coveredPath)
	if err != nil {
		t.Fatalf("Stat covered command segment: %v", err)
	}

	segments := []logSegment{
		{lane: 0, seq: 0, path: "/tmp/value-before.log", size: 123, valueLog: true},
		{lane: 0, seq: 1, path: coveredPath, size: info.Size()},
		{lane: 0, seq: 2, path: "/tmp/value-after.log", size: 456, valueLog: true},
		{lane: 0, seq: 3, path: "/tmp/commit-000003.log", size: 789},
	}
	filtered, err := filterCommandWALSegmentsForLegacyReplay(segments, 1, 0)
	if err != nil {
		t.Fatalf("filterCommandWALSegmentsForLegacyReplay: %v", err)
	}
	got := make([]string, 0, len(filtered))
	for _, seg := range filtered {
		got = append(got, filepath.Base(seg.path))
	}
	want := []string{"value-before.log", "value-after.log", "commit-000003.log"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered order=%v, want %v", got, want)
	}
}

func TestCommandWALLegacyReplayFilterRejectsDuplicateCoveredLSNAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 1)
	segments, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("listWALSegments: %v", err)
	}

	_, err = filterCommandWALSegmentsForLegacyReplay(segments, 1, 0)
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("filterCommandWALSegmentsForLegacyReplay error=%v, want ErrCommandWALDuplicateLSN", err)
	}
}

func TestCommandWALWriteOpenRejectsUnappliedFramesUntilDispatcher(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 2)

	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-write with unapplied command WAL error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALWriteOpenRejectsFirstUnappliedFrameUntilDispatcher(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-write with first unapplied command WAL error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALWALOffOpenRejectsUnappliedFramesUntilDispatcher(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	_, err = Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open WAL-off with unapplied command WAL error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALOpenFailsClosedOnCorruptTypedSegmentEvenWhenCovered(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALSegmentFrames(t, dir, 1, 1, 1)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("Open read-only error=%v, want ErrCommandWALDuplicateLSN", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("Open read-write error=%v, want ErrCommandWALDuplicateLSN", err)
	}
}

func TestCommandWALOpenFailsClosedOnDuplicateLSNAcrossCoveredSegments(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 9, []CommandWALLSNRange{{First: 1, Last: 9}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 9)
	writeCommandWALFrame(t, dir, 2, 9)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("Open read-only error=%v, want ErrCommandWALDuplicateLSN", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("Open read-write error=%v, want ErrCommandWALDuplicateLSN", err)
	}
}

func TestCommandWALOpenFailsClosedOnNonActiveTerminalTailEvenWhenCovered(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)
	appendCommandWALTail(t, dir, 1, []byte{0xde, 0xad, 0xbe})
	writeCommandWALFrame(t, dir, 2, 2)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-only error=%v, want ErrCommandWALTerminalTail", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-write error=%v, want ErrCommandWALTerminalTail", err)
	}
}

func TestCommandWALOpenFailsClosedOnTypedTailWithHigherLegacyRawSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)
	appendCommandWALTail(t, dir, 1, []byte{0xde, 0xad, 0xbe})
	writeLegacyRawWALFrame(t, dir, 999, 999)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-only error=%v, want ErrCommandWALTerminalTail", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-write error=%v, want ErrCommandWALTerminalTail", err)
	}
}

func TestCommandWALOpenAllowsActiveTypedTailWithHigherPartialLegacyAliasSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)
	appendCommandWALTail(t, dir, 1, []byte{0xde, 0xad, 0xbe})
	writePartialLegacyAliasWALSegmentTail(t, dir, 999, []byte{0xca, 0xfe})

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only with active typed tail and higher partial legacy alias segment: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}
	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open read-write with active typed tail and higher partial legacy alias segment: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
}

func TestCommandWALOpenAllowsActivePartialFirstFrameTail(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)
	writePartialCommandWALSegmentTail(t, dir, 2, []byte{0xde, 0xad})

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only with active partial first frame: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}
	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open read-write with active partial first frame: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
}

func TestCommandWALOpenFailsClosedOnNonActivePartialFirstFrameTail(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writePartialCommandWALSegmentTail(t, dir, 1, []byte{0xde, 0xad})
	writeCommandWALFrame(t, dir, 2, 2)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-only error=%v, want ErrCommandWALTerminalTail", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-write error=%v, want ErrCommandWALTerminalTail", err)
	}
}

func TestCommandWALCheckpointCleanupDeletesOnlyCoveredSegments(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 3)
	if err := os.WriteFile(filepath.Join(WALDirPath(dir), "commit-l0-000003.log"), nil, 0o600); err != nil {
		t.Fatalf("write active empty segment: %v", err)
	}

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	if len(decisions) != 2 {
		t.Fatalf("len(decisions)=%d, want 2", len(decisions))
	}
	removed := map[string]bool{}
	for _, decision := range decisions {
		removed[filepath.Base(decision.Path)] = decision.Removed
		if decision.Removed && decision.MaxLSN > 1 {
			t.Fatalf("removed uncovered segment: %+v", decision)
		}
	}
	if !removed["commit-l0-000001.log"] {
		t.Fatalf("covered non-active segment was not removed: %+v", decisions)
	}
	if removed["commit-l0-000002.log"] {
		t.Fatalf("uncovered segment was removed: %+v", decisions)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat=%v, want removed", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log")); err != nil {
		t.Fatalf("uncovered segment stat=%v, want present", err)
	}
}

func TestCommandWALCheckpointCleanupRetainsActiveCoveredSegment(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)
	writeLegacyRawWALFrame(t, dir, 999, 999)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 2, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	decisionByName := map[string]commandWALSegmentCleanupDecision{}
	for _, decision := range decisions {
		decisionByName[filepath.Base(decision.Path)] = decision
	}
	if got := decisionByName["commit-l0-000001.log"]; !got.Covered || got.Active || !got.Removed {
		t.Fatalf("old covered segment decision=%+v, want covered non-active removed", got)
	}
	if got := decisionByName["commit-l0-000002.log"]; !got.Covered || got.Active || !got.Removed {
		t.Fatalf("covered segment before legacy barrier decision=%+v, want covered non-active removed", got)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log")); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat=%v, want removed", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000999.log")); err != nil {
		t.Fatalf("legacy raw WAL segment stat=%v, want retained", err)
	}
}

func TestCommandWALCheckpointCleanupRetainsActiveCoveredTypedSegment(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	if len(decisions) != 1 {
		t.Fatalf("len(decisions)=%d, want 1", len(decisions))
	}
	if got := decisions[0]; !got.Covered || !got.Active || got.Removed {
		t.Fatalf("active covered typed segment decision=%+v, want covered active retained", got)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("active covered typed segment stat=%v, want retained", err)
	}
}

func TestCommandWALCheckpointCleanupReturnsScanErrors(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALSegmentFrames(t, dir, 1, 2, 1)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 2, 0)
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN error=%v, want duplicate LSN", err)
	}
	if len(decisions) != 1 || decisions[0].Error == "" {
		t.Fatalf("cleanup decisions=%+v, want recorded scan error", decisions)
	}
}

func TestCommandWALCheckpointCleanupRejectsDuplicateCoveredLSNAcrossSegmentsBeforeRemove(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 1)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN error=%v, want ErrCommandWALDuplicateLSN", err)
	}
	if len(decisions) != 2 {
		t.Fatalf("len(decisions)=%d, want 2", len(decisions))
	}
	for _, decision := range decisions {
		if decision.Removed {
			t.Fatalf("cleanup removed segment despite scan error: %+v", decision)
		}
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("covered segment stat=%v, want retained on scan error", err)
	}
}

func TestCommandWALSegmentMaxLSNStreamsFrames(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALSegmentFrames(t, dir, 1, 1, 2, 3)

	path := filepath.Join(WALDirPath(dir), "commit-l0-000001.log")
	maxLSN, typed, err := commandWALSegmentMaxLSN(path, 0, true)
	if err != nil {
		t.Fatalf("commandWALSegmentMaxLSN: %v", err)
	}
	if !typed || maxLSN != 3 {
		t.Fatalf("typed=%t maxLSN=%d, want typed maxLSN=3", typed, maxLSN)
	}
}

func TestCommandWALSegmentClassifierRequiresParsedName(t *testing.T) {
	for _, tc := range []struct {
		name string
		want bool
	}{
		{name: "commit-l0-000001.log", want: true},
		{name: "commit-lane-readme.log", want: false},
		{name: "commit-l-foo.log", want: false},
		{name: "commit-l0-000001.tmp", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := isCommandWALLaneSegment(logSegment{path: filepath.Join(t.TempDir(), tc.name)})
			if got != tc.want {
				t.Fatalf("isCommandWALLaneSegment(%q)=%t, want %t", tc.name, got, tc.want)
			}
		})
	}
}

func modelValidateContiguousAppliedCommandLSN(current, next uint64, covered []CommandWALLSNRange) error {
	if next < current {
		return ErrCommandWALAppliedLSNRegression
	}
	if next == current {
		if len(covered) != 0 {
			return ErrCommandWALAppliedLSNNonContig
		}
		return nil
	}
	if current == ^uint64(0) || len(covered) == 0 {
		return ErrCommandWALAppliedLSNNonContig
	}
	ranges := append([]CommandWALLSNRange(nil), covered...)
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].First != ranges[j].First {
			return ranges[i].First < ranges[j].First
		}
		return ranges[i].Last < ranges[j].Last
	})
	cursor := current + 1
	for _, r := range ranges {
		if r.First == 0 || r.Last < r.First {
			return ErrCommandWALAppliedLSNNonContig
		}
		if r.Last < cursor {
			continue
		}
		if r.First > cursor {
			return ErrCommandWALAppliedLSNNonContig
		}
		if r.Last >= next {
			return nil
		}
		if r.Last == ^uint64(0) {
			return ErrCommandWALAppliedLSNNonContig
		}
		nextCursor := r.Last + 1
		if nextCursor <= r.Last {
			return ErrCommandWALAppliedLSNNonContig
		}
		cursor = nextCursor
	}
	return ErrCommandWALAppliedLSNNonContig
}

func TestCommandWALSegmentMaxLSNFailsClosedOnNonIncreasingLSN(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALSegmentFrames(t, dir, 1, 1, 1)

	path := filepath.Join(WALDirPath(dir), "commit-l0-000001.log")
	_, typed, err := commandWALSegmentMaxLSN(path, 0, true)
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("commandWALSegmentMaxLSN error=%v, want ErrCommandWALDuplicateLSN", err)
	}
	if !typed {
		t.Fatalf("typed=false, want true for duplicate typed command segment")
	}
}

func writeCommandWALFrame(t *testing.T, dir string, segmentSeq uint64, lsn uint64) {
	t.Helper()
	writeCommandWALSegmentFrames(t, dir, segmentSeq, lsn)
}

func writeCommandWALSegmentFrames(t *testing.T, dir string, segmentSeq uint64, lsns ...uint64) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, segmentSeq))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, lsn := range lsns {
		if err := w.AppendCommand(commitlog.CommandEnvelope{
			LSN:           lsn,
			Kind:          commitlog.CommandKindRawKVBatch,
			Scope:         commitlog.CommandScopeRawKV,
			PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
		}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func writeLegacyRawWALFrame(t *testing.T, dir string, segmentSeq uint64, seq uint64) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, segmentSeq))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter legacy raw: %v", err)
	}
	if err := w.AppendBatch([]commitlog.Record{{
		Op:    commitlog.OpSetInline,
		Key:   []byte("legacy-key"),
		Value: []byte("legacy-value"),
		Seq:   seq,
	}}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendBatch legacy raw: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close legacy raw writer: %v", err)
	}
}

func writePartialCommandWALSegmentTail(t *testing.T, dir string, segmentSeq uint64, tail []byte) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, segmentSeq))
	if err := os.WriteFile(path, tail, 0o600); err != nil {
		t.Fatalf("WriteFile partial command WAL tail: %v", err)
	}
}

func writePartialLegacyAliasWALSegmentTail(t *testing.T, dir string, segmentSeq uint64, tail []byte) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, fmt.Sprintf("commit-%06d.log", segmentSeq))
	if err := os.WriteFile(path, tail, 0o600); err != nil {
		t.Fatalf("WriteFile partial legacy WAL alias tail: %v", err)
	}
}

func appendCommandWALTail(t *testing.T, dir string, segmentSeq uint64, tail []byte) {
	t.Helper()
	path := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, segmentSeq))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile append command WAL tail: %v", err)
	}
	if _, err := f.Write(tail); err != nil {
		_ = f.Close()
		t.Fatalf("Write command WAL tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close command WAL tail: %v", err)
	}
}

func corruptIndexPageByte(t *testing.T, dir string, pageID uint64) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, indexFileName), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile index: %v", err)
	}
	defer f.Close()
	off := int64(pageID*page.PageSize + page.PageHeaderSize + 7)
	if _, err := f.WriteAt([]byte{0xff}, off); err != nil {
		t.Fatalf("WriteAt corrupt meta page: %v", err)
	}
}

func writeLegacyMetaReservedBytes(t *testing.T, dir string, pageID uint64, reserved uint64) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, indexFileName), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile index: %v", err)
	}
	defer f.Close()
	buf := make([]byte, page.PageSize)
	off := int64(pageID * page.PageSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		t.Fatalf("ReadAt meta page: %v", err)
	}
	binary.LittleEndian.PutUint64(buf[page.PageHeaderSize+60:page.PageHeaderSize+68], reserved)
	node.NewNode(buf).UpdateChecksum()
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatalf("WriteAt meta page: %v", err)
	}
}
