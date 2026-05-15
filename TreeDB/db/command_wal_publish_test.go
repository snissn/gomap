package db

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
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
	_, err = openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true, ChunkSize: defaultChunkSize})
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

func TestCommandWALBackupManifestShapeIncludesAppliedLSNAndRanges(t *testing.T) {
	data, err := json.Marshal(commandWALBackupManifest{
		AppliedCommandLSN: 7,
		WALRanges: []commandWALBackupWALRange{{
			Lane:     0,
			Segment:  1,
			FirstLSN: 1,
			LastLSN:  7,
			Path:     "wal/commit-l0-000001.log",
			Bytes:    128,
		}},
		CleanedWALRanges: []commandWALBackupWALRange{{
			Lane:     0,
			Segment:  0,
			FirstLSN: 1,
			LastLSN:  3,
		}},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got["applied_command_lsn"].(float64) != 7 {
		t.Fatalf("manifest applied_command_lsn=%v, want 7; json=%s", got["applied_command_lsn"], data)
	}
	if _, ok := got["wal_ranges"]; !ok {
		t.Fatalf("manifest missing wal_ranges: %s", data)
	}
	if _, ok := got["cleaned_wal_ranges"]; !ok {
		t.Fatalf("manifest missing cleaned_wal_ranges: %s", data)
	}
}

func writeCommandWALFrame(t *testing.T, dir string, segmentSeq uint64, lsn uint64) {
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
	if err := w.AppendCommand(commitlog.CommandEnvelope{
		LSN:           lsn,
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommand: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
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
