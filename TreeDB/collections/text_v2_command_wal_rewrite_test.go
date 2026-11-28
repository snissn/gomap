package collections

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func openTextV2CommandWALOrdinaryDB5829(tb testing.TB, profile treedb.Profile, dir string) (*backenddb.DB, func() error) {
	tb.Helper()
	opts := treedb.OptionsFor(profile, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	d, closeDB, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		tb.Fatalf("OpenBackendWithCachedLeafLog(%s): %v", profile, err)
	}
	var closed atomic.Bool
	closeOnce := func() error {
		if closed.Swap(true) {
			return nil
		}
		return closeDB()
	}
	if t, ok := tb.(*testing.T); ok {
		t.Cleanup(func() {
			_ = closeOnce()
		})
	}
	return d, closeOnce
}

func openTextV2CommandWALNativeDB5829(tb testing.TB, profile treedb.Profile, dir string) (*treedb.DB, *backenddb.DB, nativeHNSWAdmissionBudget, func() error) {
	tb.Helper()
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	d, err := treedb.Open(opts)
	if err != nil {
		tb.Fatalf("treedb.Open(%s, cow_btree): %v", profile, err)
	}
	var closed atomic.Bool
	closeOnce := func() error {
		if closed.Swap(true) {
			return nil
		}
		return d.Close()
	}
	b, err := d.NativeSemanticBackend()
	if err != nil {
		_ = closeOnce()
		tb.Fatalf("NativeSemanticBackend: %v", err)
	}
	cut, err := b.CaptureNativeSemanticState()
	if err != nil {
		_ = closeOnce()
		tb.Fatalf("CaptureNativeSemanticState: %v", err)
	}
	budget := cut.NativeAllocator().(nativeHNSWAdmissionBudget)
	if err := cut.Close(); err != nil {
		_ = closeOnce()
		tb.Fatalf("cut.Close: %v", err)
	}
	if t, ok := tb.(*testing.T); ok {
		t.Cleanup(func() {
			_ = closeOnce()
		})
	}
	return d, b, budget, closeOnce
}

func newTextV2CommandWALManager5829(t *testing.T, d *backenddb.DB) (*CollectionManager, func() error) {
	t.Helper()
	mgr := NewCollectionManager(d)
	var closed atomic.Bool
	closeOnce := func() error {
		if closed.Swap(true) {
			return nil
		}
		return mgr.Close()
	}
	t.Cleanup(func() {
		_ = closeOnce()
	})
	return mgr, closeOnce
}

func copyCommandWALSegments5829(t *testing.T, srcWALDir, dstWALDir string) {
	t.Helper()
	if err := os.MkdirAll(dstWALDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dstWALDir, err)
	}
	entries, err := os.ReadDir(srcWALDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", srcWALDir, err)
	}
	copied := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !commitlog.IsCommandSegmentName(name) {
			continue
		}
		srcPath := filepath.Join(srcWALDir, name)
		dstPath := filepath.Join(dstWALDir, name)
		in, err := os.Open(srcPath)
		if err != nil {
			t.Fatalf("Open(%s): %v", srcPath, err)
		}
		out, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			_ = in.Close()
			t.Fatalf("OpenFile(%s): %v", dstPath, err)
		}
		_, copyErr := io.Copy(out, in)
		closeInErr := in.Close()
		closeOutErr := out.Close()
		if err := errors.Join(copyErr, closeInErr, closeOutErr); err != nil {
			t.Fatalf("copy WAL segment %s -> %s: %v", srcPath, dstPath, err)
		}
		copied++
	}
	if copied == 0 {
		t.Fatalf("copyCommandWALSegments5829(%s -> %s): no command WAL segments found", srcWALDir, dstWALDir)
	}
}

func appendUnappliedCommandWALFrame5829(t *testing.T, backendDir string, env commitlog.CommandEnvelope) {
	t.Helper()
	walDir := backenddb.WALDirPath(backendDir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir wal: %v", err)
	}
	maxSeq := uint64(0)
	for _, entry := range entries {
		if entry.IsDir() || !commitlog.IsCommandSegmentName(entry.Name()) {
			continue
		}
		var lane, seq uint64
		if n, _ := fmt.Sscanf(entry.Name(), "commit-l%d-%d.log", &lane, &seq); n == 2 && seq > maxSeq {
			maxSeq = seq
		}
	}
	if maxSeq == 0 {
		maxSeq = uint64(len(entries)) + 100
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, maxSeq+1))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter(%s): %v", path, err)
	}
	if err := w.AppendCommand(env); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommand: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func assertTextV2StatusZeroDebt5829(t *testing.T, col *Collection, indexName string, wantLive uint64) (TextIndexStatus, TextIndexStorageStats) {
	t.Helper()
	status, err := col.TextIndexStatus(indexName)
	if err != nil {
		t.Fatalf("TextIndexStatus(%q): %v", indexName, err)
	}
	if status.Version != TextIndexVersionV2 || !status.Ready || !status.Readable || !status.Writable || status.FailClosed {
		t.Fatalf("status=%+v want ready v2", status)
	}
	if status.LiveDocuments != wantLive || status.DeletedDocuments != 0 || status.MicroPostingBlocks != 0 || status.DeltaPostingBlocks != 0 || status.StalePostings != 0 {
		t.Fatalf("status=%+v want live=%d deleted=0 micro=0 delta=0 stale=0", status, wantLive)
	}
	storage, err := col.TextIndexStorageStats(indexName)
	if err != nil {
		t.Fatalf("TextIndexStorageStats(%q): %v", indexName, err)
	}
	if storage.Documents != wantLive || storage.V2DeletedDocs != 0 || storage.V2MicroPostingBlocks != 0 || storage.V2DeltaPostingBlocks != 0 {
		t.Fatalf("storage=%+v want live=%d deleted=0 micro=0 delta=0", storage, wantLive)
	}
	if storage.V2SealedPostingBlocks != status.SealedPostingBlocks || storage.V2PostingBlocks != status.PostingBlocks {
		t.Fatalf("status=%+v disagrees with storage=%+v", status, storage)
	}
	return status, storage
}

func searchIDs5829(t *testing.T, col *Collection, opts TextSearchOptions) []string {
	t.Helper()
	resp, err := col.SearchText(opts)
	if err != nil {
		t.Fatalf("SearchText(%+v): %v", opts, err)
	}
	ids := make([]string, len(resp.Results))
	for i, r := range resp.Results {
		ids[i] = string(r.DocumentID)
	}
	return ids
}

func TestTextV2CommandWALRewriteAndMaintain5829(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			d, closeDB := openTextV2CommandWALOrdinaryDB5829(t, profile, dir)
			mgr := NewCollectionManager(d)
			closed := false
			cleanup := func() {
				if closed {
					return
				}
				closed = true
				if err := mgr.Close(); err != nil {
					t.Errorf("mgr.Close: %v", err)
				}
				if err := closeDB(); err != nil {
					t.Errorf("closeDB: %v", err)
				}
			}
			t.Cleanup(cleanup)

			meta := &CollectionMeta{
				Name: "docs",
				TextIndexes: []TextIndexDefinition{{
					Name:           "lexical",
					Version:        TextIndexVersionV2,
					StorePositions: true,
					Fields:         []TextIndexField{{Field: "body"}},
				}},
			}
			if _, err := mgr.CreateCollection(meta); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := mgr.OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			if _, err := col.InsertBatch(
				[][]byte{[]byte("d1"), []byte("d2"), []byte("d3"), []byte("d4")},
				[][]byte{
					[]byte(`{"body":"refund policy guide"}`),
					[]byte(`{"body":"refund shipping policy"}`),
					[]byte(`{"body":"billing guide refund"}`),
					[]byte(`{"body":"shipping handbook"}`),
				},
			); err != nil {
				t.Fatalf("InsertBatch: %v", err)
			}
			if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
				return []byte(`{"body":"refund updated guide"}`), true, nil
			}); err != nil {
				t.Fatalf("Update d1: %v", err)
			}
			if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
				t.Fatalf("DeleteBatch d2 deleted=%d err=%v", deleted, err)
			}

			beforeStatus, err := col.TextIndexStatus("lexical")
			if err != nil {
				t.Fatalf("TextIndexStatus before: %v", err)
			}
			if beforeStatus.MicroPostingBlocks == 0 || beforeStatus.DeletedDocuments == 0 || beforeStatus.StalePostings == 0 {
				t.Fatalf("beforeStatus=%+v want micro>0 deleted>0 stale>0", beforeStatus)
			}

			// 1. Direct RewriteTextIndex under CommandWAL.
			baseLSN := d.State().AppliedCommandLSN
			rwStats, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
			if err != nil {
				t.Fatalf("RewriteTextIndex: %v", err)
			}
			if rwStats.Noop || rwStats.TombstoneDocIDEntriesPurged == 0 || rwStats.StalePostingsPurged == 0 {
				t.Fatalf("rwStats=%+v want concrete rewrite purging tombstones and stale postings", rwStats)
			}
			if gotLSN := d.State().AppliedCommandLSN; gotLSN <= baseLSN {
				t.Fatalf("AppliedCommandLSN=%d want > %d", gotLSN, baseLSN)
			}
			frames := collectionCommandWALFrames(t, d.Dir())
			if len(frames) == 0 {
				t.Fatalf("expected WAL frames after RewriteTextIndex")
			}
			lastFrame := frames[len(frames)-1]
			if lastFrame.Kind != commitlog.CommandKindCollectionRewriteTextIndex ||
				lastFrame.Scope != commitlog.CommandScopeCollection ||
				lastFrame.PayloadFormat != commitlog.PayloadFormatCollectionRewriteTextIndexV1 {
				t.Fatalf("last WAL frame=%+v want CommandKindCollectionRewriteTextIndex/Collection/V1", lastFrame)
			}
			decoded, err := commitlog.DecodeCollectionRewriteTextIndexPayload(lastFrame.Payload)
			if err != nil {
				t.Fatalf("DecodeCollectionRewriteTextIndexPayload: %v", err)
			}
			wantPayload := commitlog.CollectionRewriteTextIndexPayload{
				Collection: "docs",
				IndexName:  "lexical",
			}
			if decoded != wantPayload {
				t.Fatalf("decoded WAL payload=%+v want %+v", decoded, wantPayload)
			}
			assertTextV2StatusZeroDebt5829(t, col, "lexical", 3)

			// 2. Fresh debt + MaintainTextIndex.
			if _, _, err := col.Update([]byte("d3"), func([]byte) ([]byte, bool, error) {
				return []byte(`{"body":"refund updated policy"}`), true, nil
			}); err != nil {
				t.Fatalf("Update d3: %v", err)
			}
			if deleted, err := col.DeleteBatch([][]byte{[]byte("d4")}); err != nil || deleted != 1 {
				t.Fatalf("DeleteBatch d4 deleted=%d err=%v", deleted, err)
			}
			maintIdx, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
				Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1},
			})
			if err != nil {
				t.Fatalf("MaintainTextIndex: %v", err)
			}
			if maintIdx.IndexesRewritten != 1 {
				t.Fatalf("MaintainTextIndex stats=%+v want IndexesRewritten=1", maintIdx)
			}
			assertTextV2StatusZeroDebt5829(t, col, "lexical", 2)

			// 3. Fresh debt + MaintainTextIndexes with RunStorageCompaction: true.
			if _, err := col.Insert([]byte("d5"), []byte(`{"body":"refund updated handbook"}`)); err != nil {
				t.Fatalf("Insert d5: %v", err)
			}
			if deleted, err := col.DeleteBatch([][]byte{[]byte("d5")}); err != nil || deleted != 1 {
				t.Fatalf("DeleteBatch d5 deleted=%d err=%v", deleted, err)
			}
			maintMgr, err := mgr.MaintainTextIndexes(context.Background(), TextIndexMaintenanceOptions{
				Policy:               TextIndexMaintenancePolicy{MinDeletedDocuments: 1},
				RunStorageCompaction: true,
			})
			if err != nil {
				t.Fatalf("MaintainTextIndexes(RunStorageCompaction:true): %v", err)
			}
			if maintMgr.IndexesRewritten != 1 || !maintMgr.StorageCompacted || maintMgr.StorageCompaction == nil {
				t.Fatalf("MaintainTextIndexes stats=%+v want rewritten=1 and StorageCompacted=true", maintMgr)
			}
			assertTextV2StatusZeroDebt5829(t, col, "lexical", 2)

			rankedBefore := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
			phraseBefore := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10})
			if len(rankedBefore) != 2 || len(phraseBefore) != 2 {
				t.Fatalf("ranked=%v phrase=%v want 2 hits each (d1, d3)", rankedBefore, phraseBefore)
			}

			// Close without calling Checkpoint() and reopen.
			cleanup()
			reopened, closeReopened := openTextV2CommandWALOrdinaryDB5829(t, profile, dir)
			reopenedMgr := NewCollectionManager(reopened)
			defer func() {
				if err := reopenedMgr.Close(); err != nil {
					t.Errorf("reopenedMgr.Close: %v", err)
				}
				if err := closeReopened(); err != nil {
					t.Errorf("closeReopened: %v", err)
				}
			}()
			reopenedCol, err := reopenedMgr.OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection reopened: %v", err)
			}
			assertTextV2StatusZeroDebt5829(t, reopenedCol, "lexical", 2)
			rankedAfter := searchIDs5829(t, reopenedCol, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
			phraseAfter := searchIDs5829(t, reopenedCol, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10})
			if !reflect.DeepEqual(rankedAfter, rankedBefore) || !reflect.DeepEqual(phraseAfter, phraseBefore) {
				t.Fatalf("reopen search mismatch ranked=%v/%v phrase=%v/%v", rankedAfter, rankedBefore, phraseAfter, phraseBefore)
			}

			// Subsequent writes + second rewrite after reopen.
			if _, err := reopenedCol.Insert([]byte("d6"), []byte(`{"body":"refund updated invoice"}`)); err != nil {
				t.Fatalf("Insert d6: %v", err)
			}
			if deleted, err := reopenedCol.DeleteBatch([][]byte{[]byte("d3")}); err != nil || deleted != 1 {
				t.Fatalf("DeleteBatch d3 deleted=%d err=%v", deleted, err)
			}
			secondRW, err := reopenedCol.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
			if err != nil {
				t.Fatalf("second RewriteTextIndex: %v", err)
			}
			if secondRW.Noop {
				t.Fatalf("secondRW=%+v want concrete rewrite", secondRW)
			}
			assertTextV2StatusZeroDebt5829(t, reopenedCol, "lexical", 2)
			phraseFinal := searchIDs5829(t, reopenedCol, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10})
			if len(phraseFinal) != 2 {
				t.Fatalf("phraseFinal=%v want 2 hits (d1, d6)", phraseFinal)
			}
		})
	}
}

func TestTextV2CommandWALUnappliedReplayAndNoop5829(t *testing.T) {
	// Phase A: Capture a live CommandWAL DB with CreateCollection + InsertBatch + Update + DeleteBatch + RewriteTextIndex,
	// then replay the EXACT live-emitted WAL stream from LSN 0 into a fresh directory (proving true crash/unapplied WAL replay).
	liveDir := t.TempDir()
	d, closeDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, liveDir)
	mgr, closeMgr := newTextV2CommandWALManager5829(t, d)
	meta := &CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "body"}},
		}},
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2"), []byte("d3")},
		[][]byte{
			[]byte(`{"body":"refund policy guide"}`),
			[]byte(`{"body":"refund stale doc"}`),
			[]byte(`{"body":"shipping refund policy"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"refund updated guide"}`), true, nil
	}); err != nil {
		t.Fatalf("Update d1: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch d2: deleted=%d err=%v", deleted, err)
	}
	liveRW, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
	if err != nil {
		t.Fatalf("live RewriteTextIndex: %v", err)
	}
	if liveRW.Noop {
		t.Fatalf("liveRW=%+v want concrete rewrite", liveRW)
	}
	liveStatus, liveStorage := assertTextV2StatusZeroDebt5829(t, col, "lexical", 2)
	liveRanked := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
	livePhrase := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10})
	liveAppliedLSN := d.State().AppliedCommandLSN
	liveFrames := collectionCommandWALFrames(t, d.Dir())
	if len(liveFrames) == 0 || liveFrames[len(liveFrames)-1].Kind != commitlog.CommandKindCollectionRewriteTextIndex {
		t.Fatalf("last live frame=%+v want CommandKindCollectionRewriteTextIndex", liveFrames)
	}
	liveBackendDir := d.Dir()
	// Capture format.json and unpruned WAL segments BEFORE clean close (which checkpoints/prunes WAL).
	replayDir := t.TempDir()
	replayMainDir := replayDir
	if err := os.MkdirAll(replayMainDir, 0o755); err != nil {
		t.Fatalf("MkdirAll replayMainDir: %v", err)
	}
	fmtBytes, err := os.ReadFile(filepath.Join(liveBackendDir, "format.json"))
	if err != nil {
		t.Fatalf("ReadFile format.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(replayMainDir, "format.json"), fmtBytes, 0o644); err != nil {
		t.Fatalf("WriteFile format.json: %v", err)
	}
	copyCommandWALSegments5829(t, backenddb.WALDirPath(liveBackendDir), backenddb.WALDirPath(replayMainDir))
	if err := closeMgr(); err != nil {
		t.Fatalf("mgr.Close: %v", err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("closeDB: %v", err)
	}

	replayBefore := workstats.Read().Replay
	replayedDB, closeReplayed := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, replayDir)
	replayAfter := workstats.Read().Replay
	if replayAfter.FramesApplied <= replayBefore.FramesApplied {
		_ = closeReplayed()
		t.Fatalf("expected WAL replay to apply frames: before=%+v after=%+v", replayBefore, replayAfter)
	}
	if got := replayedDB.State().AppliedCommandLSN; got != liveAppliedLSN {
		_ = closeReplayed()
		t.Fatalf("replayed AppliedCommandLSN=%d want %d", got, liveAppliedLSN)
	}
	replayedMgr, closeReplayedMgr := newTextV2CommandWALManager5829(t, replayedDB)
	replayedCol, err := replayedMgr.OpenCollection("docs")
	if err != nil {
		_ = replayedMgr.Close()
		_ = closeReplayed()
		t.Fatalf("OpenCollection replayed: %v", err)
	}
	replayedStatus, replayedStorage := assertTextV2StatusZeroDebt5829(t, replayedCol, "lexical", 2)
	if replayedStatus.SealedPostingBlocks != liveStatus.SealedPostingBlocks ||
		replayedStatus.RootGeneration != liveStatus.RootGeneration ||
		replayedStorage.V2PositionEntries != liveStorage.V2PositionEntries {
		_ = replayedMgr.Close()
		_ = closeReplayed()
		t.Fatalf("replayed status/storage mismatch: status=%+v/%+v storage=%+v/%+v", replayedStatus, liveStatus, replayedStorage, liveStorage)
	}
	replayedRanked := searchIDs5829(t, replayedCol, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
	replayedPhrase := searchIDs5829(t, replayedCol, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10})
	if !reflect.DeepEqual(replayedRanked, liveRanked) || !reflect.DeepEqual(replayedPhrase, livePhrase) {
		_ = replayedMgr.Close()
		_ = closeReplayed()
		t.Fatalf("replayed search mismatch ranked=%v/%v phrase=%v/%v", replayedRanked, liveRanked, replayedPhrase, livePhrase)
	}

	// Phase B: Replay noop handling — checkpoint the already-sealed zero-debt DB, close it,
	// append an unapplied Force=false CommandKindCollectionRewriteTextIndex frame at baseLSN+1,
	// reopen and verify PublishCommandWALNoop advances AppliedCommandLSN cleanly.
	if err := replayedDB.Checkpoint(); err != nil {
		_ = replayedMgr.Close()
		_ = closeReplayed()
		t.Fatalf("Checkpoint: %v", err)
	}
	baseLSN := replayedDB.State().AppliedCommandLSN
	replayedBackendDir := replayedDB.Dir()
	if err := closeReplayedMgr(); err != nil {
		t.Fatalf("replayedMgr.Close: %v", err)
	}
	if err := closeReplayed(); err != nil {
		t.Fatalf("closeReplayed: %v", err)
	}
	noopPayload, err := commitlog.EncodeCollectionRewriteTextIndexPayload(commitlog.CollectionRewriteTextIndexPayload{
		Collection: "docs",
		IndexName:  "lexical",
	})
	if err != nil {
		t.Fatalf("EncodeCollectionRewriteTextIndexPayload: %v", err)
	}
	appendUnappliedCommandWALFrame5829(t, replayedBackendDir, commitlog.CommandEnvelope{
		Version:         commitlog.CommandFrameVersionV2,
		LSN:             baseLSN + 1,
		DurabilityClass: commitlog.CommandDurabilityDurable,
		Kind:            commitlog.CommandKindCollectionRewriteTextIndex,
		Scope:           commitlog.CommandScopeCollection,
		PayloadFormat:   commitlog.PayloadFormatCollectionRewriteTextIndexV1,
		Payload:         noopPayload,
	})

	noopBefore := workstats.Read().Replay
	noopDB, closeNoop := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, replayDir)
	defer func() {
		if err := closeNoop(); err != nil {
			t.Errorf("closeNoop: %v", err)
		}
	}()
	noopAfter := workstats.Read().Replay
	if noopAfter.FramesApplied <= noopBefore.FramesApplied {
		t.Fatalf("expected noop rewrite replay frame to apply: before=%+v after=%+v", noopBefore, noopAfter)
	}
	if got := noopDB.State().AppliedCommandLSN; got != baseLSN+1 {
		t.Fatalf("AppliedCommandLSN after replay noop=%d want %d", got, baseLSN+1)
	}
	noopMgr := NewCollectionManager(noopDB)
	defer func() {
		if err := noopMgr.Close(); err != nil {
			t.Errorf("noopMgr.Close: %v", err)
		}
	}()
	noopCol, err := noopMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection after noop replay: %v", err)
	}
	assertTextV2StatusZeroDebt5829(t, noopCol, "lexical", 2)

	// Subsequent live write + second rewrite after replay.
	if _, err := noopCol.Insert([]byte("d4"), []byte(`{"body":"refund updated receipt"}`)); err != nil {
		t.Fatalf("Insert d4: %v", err)
	}
	if deleted, err := noopCol.DeleteBatch([][]byte{[]byte("d3")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch d3: deleted=%d err=%v", deleted, err)
	}
	if rw2, err := noopCol.RewriteTextIndex("lexical", TextIndexRewriteOptions{}); err != nil || rw2.Noop {
		t.Fatalf("post-replay RewriteTextIndex rw2=%+v err=%v", rw2, err)
	}
	assertTextV2StatusZeroDebt5829(t, noopCol, "lexical", 2)

	// Phase C: Direct payload format mismatch check on replay handler.
	if err := replayCollectionRewriteTextIndexCommandWAL(noopDB, commitlog.CommandEnvelope{
		LSN:           baseLSN + 99,
		Kind:          commitlog.CommandKindCollectionRewriteTextIndex,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionRebuildVectorIndexV1,
		Payload:       noopPayload,
	}); !errors.Is(err, commitlog.ErrCorrupt) {
		t.Fatalf("replay format mismatch err=%v want ErrCorrupt", err)
	}
}

func TestTextV2CommandWALZeroWALFramesOnNoopAndBudget5829(t *testing.T) {
	dir := t.TempDir()
	d, closeDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, dir)
	mgr := NewCollectionManager(d)
	defer func() {
		if err := mgr.Close(); err != nil {
			t.Errorf("mgr.Close: %v", err)
		}
		if err := closeDB(); err != nil {
			t.Errorf("closeDB: %v", err)
		}
	}()

	meta := &CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "body"}},
		}},
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2"), []byte("d3")},
		[][]byte{
			[]byte(`{"body":"alpha beta gamma delta"}`),
			[]byte(`{"body":"alpha epsilon zeta"}`),
			[]byte(`{"body":"beta eta theta"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch d2 deleted=%d err=%v", deleted, err)
	}

	assertUnchanged := func(label string, framesBefore int, lsnBefore uint64) {
		t.Helper()
		framesAfter := len(collectionCommandWALFrames(t, d.Dir()))
		lsnAfter := d.State().AppliedCommandLSN
		if framesAfter != framesBefore || lsnAfter != lsnBefore {
			t.Fatalf("%s appended WAL frame(s): frames %d->%d lsn %d->%d", label, framesBefore, framesAfter, lsnBefore, lsnAfter)
		}
	}

	// 1. DryRun: true appends zero WAL frames.
	framesBefore := len(collectionCommandWALFrames(t, d.Dir()))
	lsnBefore := d.State().AppliedCommandLSN
	dryStats, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
		DryRun: true,
		Force:  true,
	})
	if err != nil {
		t.Fatalf("DryRun MaintainTextIndex: %v", err)
	}
	if dryStats.IndexesRewritten != 0 {
		t.Fatalf("DryRun stats=%+v want IndexesRewritten=0", dryStats)
	}
	assertUnchanged("DryRun", framesBefore, lsnBefore)

	// 2. Below-threshold skip appends zero WAL frames.
	belowStats, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
		Policy: TextIndexMaintenancePolicy{
			MinMicroPostingBlocks:   10000,
			MinRewritePostingBlocks: 10000,
			MinStalePostings:        10000,
			MinDeletedDocuments:     10000,
		},
	})
	if err != nil {
		t.Fatalf("Below-threshold MaintainTextIndex: %v", err)
	}
	if belowStats.IndexesRewritten != 0 || belowStats.IndexesSkipped != 1 {
		t.Fatalf("belowStats=%+v want skipped=1 rewritten=0", belowStats)
	}
	assertUnchanged("BelowThreshold", framesBefore, lsnBefore)

	// 3. Budget-exhausted run appends zero WAL frames.
	budgetStats, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{
		MaxTerms: 1,
	})
	if err != nil {
		t.Fatalf("Budget-exhausted RewriteTextIndex: %v", err)
	}
	if !budgetStats.BudgetExhausted || !budgetStats.Noop {
		t.Fatalf("budgetStats=%+v want BudgetExhausted=true Noop=true", budgetStats)
	}
	assertUnchanged("BudgetExhausted", framesBefore, lsnBefore)

	// Seal debt once so we can test zero-debt fast-skip.
	if rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{}); err != nil || rw.Noop {
		t.Fatalf("sealing RewriteTextIndex rw=%+v err=%v", rw, err)
	}

	// 4. Zero-debt fast-skip on MaintainTextIndex and RewriteTextIndex appends zero WAL frames.
	framesBefore = len(collectionCommandWALFrames(t, d.Dir()))
	lsnBefore = d.State().AppliedCommandLSN
	if maintZero, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{}); err != nil || maintZero.IndexesRewritten != 0 {
		t.Fatalf("zero-debt MaintainTextIndex stats=%+v err=%v", maintZero, err)
	}
	assertUnchanged("ZeroDebtMaintain", framesBefore, lsnBefore)
	if rwZero, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{}); err != nil || !rwZero.Noop {
		t.Fatalf("zero-debt RewriteTextIndex stats=%+v err=%v", rwZero, err)
	}
	assertUnchanged("ZeroDebtRewrite", framesBefore, lsnBefore)
}

func TestTextV2CommandWALReplayOptionsFidelity5829(t *testing.T) {
	t.Run("DisableTombstonePurge", func(t *testing.T) {
		liveDir := t.TempDir()
		d, closeDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, liveDir)
		mgr, closeMgr := newTextV2CommandWALManager5829(t, d)
		meta := &CollectionMeta{
			Name: "docs",
			TextIndexes: []TextIndexDefinition{{
				Name:           "lexical",
				Version:        TextIndexVersionV2,
				StorePositions: true,
				Fields:         []TextIndexField{{Field: "body"}},
			}},
		}
		if _, err := mgr.CreateCollection(meta); err != nil {
			t.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection("docs")
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		if _, err := col.InsertBatch(
			[][]byte{[]byte("d1"), []byte("d2")},
			[][]byte{[]byte(`{"body":"refund policy"}`), []byte(`{"body":"refund deleted"}`)},
		); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}
		if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
			t.Fatalf("DeleteBatch d2 deleted=%d err=%v", deleted, err)
		}
		rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{DisableTombstonePurge: true})
		if err != nil {
			t.Fatalf("RewriteTextIndex(DisableTombstonePurge): %v", err)
		}
		if rw.Noop || rw.TombstoneDocIDEntriesPurged != 0 {
			t.Fatalf("rw=%+v want concrete rewrite with 0 tombstones purged", rw)
		}
		liveStatus, err := col.TextIndexStatus("lexical")
		if err != nil {
			t.Fatalf("TextIndexStatus: %v", err)
		}
		if liveStatus.DeletedDocuments != 1 || liveStatus.MicroPostingBlocks != 0 || liveStatus.StalePostings != 0 {
			t.Fatalf("liveStatus=%+v want deleted=1 micro=0 stale=0", liveStatus)
		}
		liveBackendDir := d.Dir()
		liveLSN := d.State().AppliedCommandLSN
		replayDir := t.TempDir()
		replayMainDir := replayDir
		if err := os.MkdirAll(replayMainDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		fmtBytes, err := os.ReadFile(filepath.Join(liveBackendDir, "format.json"))
		if err != nil {
			t.Fatalf("ReadFile format.json: %v", err)
		}
		if err := os.WriteFile(filepath.Join(replayMainDir, "format.json"), fmtBytes, 0o644); err != nil {
			t.Fatalf("WriteFile format.json: %v", err)
		}
		copyCommandWALSegments5829(t, backenddb.WALDirPath(liveBackendDir), backenddb.WALDirPath(replayMainDir))
		if err := closeMgr(); err != nil {
			t.Fatalf("mgr.Close: %v", err)
		}
		if err := closeDB(); err != nil {
			t.Fatalf("closeDB: %v", err)
		}

		rd, closeRD := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, replayDir)
		defer func() { _ = closeRD() }()
		if rd.State().AppliedCommandLSN != liveLSN {
			t.Fatalf("replayed LSN=%d want %d", rd.State().AppliedCommandLSN, liveLSN)
		}
		rmgr := NewCollectionManager(rd)
		defer func() { _ = rmgr.Close() }()
		rcol, err := rmgr.OpenCollection("docs")
		if err != nil {
			t.Fatalf("OpenCollection replayed: %v", err)
		}
		rstatus, err := rcol.TextIndexStatus("lexical")
		if err != nil {
			t.Fatalf("replayed TextIndexStatus: %v", err)
		}
		if rstatus.DeletedDocuments != 1 || rstatus.MicroPostingBlocks != 0 || rstatus.StalePostings != 0 {
			t.Fatalf("replayed status=%+v want deleted=1 micro=0 stale=0", rstatus)
		}
		rstorage, err := rcol.TextIndexStorageStats("lexical")
		if err != nil {
			t.Fatalf("replayed TextIndexStorageStats: %v", err)
		}
		if rstorage.V2DeletedDocs != 1 || rstorage.V2MicroPostingBlocks != 0 {
			t.Fatalf("replayed storage=%+v want deleted=1 micro=0", rstorage)
		}
	})

	t.Run("TargetPostingsPerBlock", func(t *testing.T) {
		liveDir := t.TempDir()
		d, closeDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, liveDir)
		mgr, closeMgr := newTextV2CommandWALManager5829(t, d)
		meta := &CollectionMeta{
			Name: "docs",
			TextIndexes: []TextIndexDefinition{{
				Name:           "lexical",
				Version:        TextIndexVersionV2,
				StorePositions: true,
				Fields:         []TextIndexField{{Field: "body"}},
			}},
		}
		if _, err := mgr.CreateCollection(meta); err != nil {
			t.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection("docs")
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		ids := make([][]byte, 12)
		docs := make([][]byte, 12)
		for i := range ids {
			ids[i] = []byte(fmt.Sprintf("doc-%02d", i))
			docs[i] = []byte(`{"body":"common"}`)
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}
		rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{
			TargetPostingsPerBlock: 2,
			Force:                  true,
		})
		if err != nil {
			t.Fatalf("RewriteTextIndex(TargetPostingsPerBlock=2): %v", err)
		}
		if rw.Noop {
			t.Fatalf("rw=%+v want concrete rewrite", rw)
		}
		liveStatus, liveStorage := assertTextV2StatusZeroDebt5829(t, col, "lexical", 12)
		if liveStatus.SealedPostingBlocks < 6 {
			t.Fatalf("liveStatus.SealedPostingBlocks=%d want >= 6 for 12 docs at target=2", liveStatus.SealedPostingBlocks)
		}
		liveBackendDir := d.Dir()
		liveLSN := d.State().AppliedCommandLSN
		replayDir := t.TempDir()
		replayMainDir := replayDir
		if err := os.MkdirAll(replayMainDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		fmtBytes, err := os.ReadFile(filepath.Join(liveBackendDir, "format.json"))
		if err != nil {
			t.Fatalf("ReadFile format.json: %v", err)
		}
		if err := os.WriteFile(filepath.Join(replayMainDir, "format.json"), fmtBytes, 0o644); err != nil {
			t.Fatalf("WriteFile format.json: %v", err)
		}
		copyCommandWALSegments5829(t, backenddb.WALDirPath(liveBackendDir), backenddb.WALDirPath(replayMainDir))
		if err := closeMgr(); err != nil {
			t.Fatalf("mgr.Close: %v", err)
		}
		if err := closeDB(); err != nil {
			t.Fatalf("closeDB: %v", err)
		}

		rd, closeRD := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, replayDir)
		defer func() { _ = closeRD() }()
		if rd.State().AppliedCommandLSN != liveLSN {
			t.Fatalf("replayed LSN=%d want %d", rd.State().AppliedCommandLSN, liveLSN)
		}
		rmgr := NewCollectionManager(rd)
		defer func() { _ = rmgr.Close() }()
		rcol, err := rmgr.OpenCollection("docs")
		if err != nil {
			t.Fatalf("OpenCollection replayed: %v", err)
		}
		rstatus, rstorage := assertTextV2StatusZeroDebt5829(t, rcol, "lexical", 12)
		if rstatus.SealedPostingBlocks != liveStatus.SealedPostingBlocks || rstorage.V2SealedPostingBlocks != liveStorage.V2SealedPostingBlocks {
			t.Fatalf("replayed sealed blocks=%d/%d want %d/%d", rstatus.SealedPostingBlocks, rstorage.V2SealedPostingBlocks, liveStatus.SealedPostingBlocks, liveStorage.V2SealedPostingBlocks)
		}
	})
}

func TestTextV2CommandWALUnifiedLexicalAndVector5829(t *testing.T) {
	dir := t.TempDir()
	d, closeDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, dir)
	mgr, closeMgr := newTextV2CommandWALManager5829(t, d)
	vecDef := columnGraphRebuildVectorIndexDefinitionV2A(3, 4)
	cfg := columnGraphRebuildColumnStoreConfigV2A(3)
	cfg.RetainedPayload = ColumnRetainedPayloadFull
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingJSON
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
		VectorIndexes: []VectorIndexDefinition{vecDef},
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}},
		}},
	}
	if _, err := mgr.CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2"), []byte("d3"), []byte("d4")},
		[][]byte{
			[]byte(`{"time_us":1001,"kind":"doc","did":"did-1","title":"refund","body":"refund policy guide","embedding":[1,0,0]}`),
			[]byte(`{"time_us":1002,"kind":"doc","did":"did-2","title":"refund","body":"refund deleted draft","embedding":[0.95,0.05,0]}`),
			[]byte(`{"time_us":1003,"kind":"doc","did":"did-3","title":"billing","body":"refund updated policy","embedding":[0.8,0.2,0]}`),
			[]byte(`{"time_us":1004,"kind":"doc","did":"did-4","title":"shipping","body":"shipping handbook","embedding":[0,0,1]}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"time_us":1001,"kind":"doc","did":"did-1","title":"refund","body":"refund updated guide","embedding":[1,0,0]}`), true, nil
	}); err != nil {
		t.Fatalf("Update d1: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch d2 deleted=%d err=%v", deleted, err)
	}

	maintStats, err := mgr.MaintainTextIndexes(context.Background(), TextIndexMaintenanceOptions{
		Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1},
	})
	if err != nil {
		t.Fatalf("MaintainTextIndexes: %v", err)
	}
	if maintStats.IndexesRewritten != 1 {
		t.Fatalf("MaintainTextIndexes stats=%+v want IndexesRewritten=1", maintStats)
	}
	vecStatus, err := col.RebuildVectorIndex(vecDef.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, vecStatus, vecDef.Name)
	assertTextV2StatusZeroDebt5829(t, col, "lexical", 3)

	frames := collectionCommandWALFrames(t, d.Dir())
	var sawTextRewrite, sawVectorRebuild bool
	for _, f := range frames {
		if f.Kind == commitlog.CommandKindCollectionRewriteTextIndex {
			sawTextRewrite = true
		}
		if f.Kind == commitlog.CommandKindCollectionRebuildVectorIndex {
			sawVectorRebuild = true
		}
	}
	if !sawTextRewrite || !sawVectorRebuild {
		t.Fatalf("WAL frames missing expected kinds: sawTextRewrite=%t sawVectorRebuild=%t", sawTextRewrite, sawVectorRebuild)
	}

	rankedBefore := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
	phraseBefore := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10})
	hybridBefore, err := col.SearchHybrid(HybridSearchOptions{
		TopK:   3,
		Text:   &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		Vector: &HybridVectorQuery{IndexName: vecDef.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact},
	})
	if err != nil {
		t.Fatalf("SearchHybrid before reopen: %v", err)
	}
	if len(hybridBefore.Results) == 0 {
		t.Fatalf("expected non-empty SearchHybrid results")
	}

	// Uncheckpointed close and reopen.
	if err := closeMgr(); err != nil {
		t.Fatalf("mgr.Close: %v", err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("closeDB: %v", err)
	}

	reopened, closeReopened := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, dir)
	reopenedMgr := NewCollectionManager(reopened)
	defer func() {
		if err := reopenedMgr.Close(); err != nil {
			t.Errorf("reopenedMgr.Close: %v", err)
		}
		if err := closeReopened(); err != nil {
			t.Errorf("closeReopened: %v", err)
		}
	}()
	reopenedCol, err := reopenedMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTextV2StatusZeroDebt5829(t, reopenedCol, "lexical", 3)
	rankedAfter := searchIDs5829(t, reopenedCol, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
	phraseAfter := searchIDs5829(t, reopenedCol, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10})
	if !reflect.DeepEqual(rankedAfter, rankedBefore) || !reflect.DeepEqual(phraseAfter, phraseBefore) {
		t.Fatalf("reopened text search mismatch ranked=%v/%v phrase=%v/%v", rankedAfter, rankedBefore, phraseAfter, phraseBefore)
	}
	hybridAfter, err := reopenedCol.SearchHybrid(HybridSearchOptions{
		TopK:   3,
		Text:   &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		Vector: &HybridVectorQuery{IndexName: vecDef.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact},
	})
	if err != nil {
		t.Fatalf("SearchHybrid after reopen: %v", err)
	}
	if !reflect.DeepEqual(hybridAfter.Results, hybridBefore.Results) {
		t.Fatalf("reopened SearchHybrid mismatch got=%+v want=%+v", hybridAfter.Results, hybridBefore.Results)
	}
}

func TestTextV2CommandWALNativeCOW5829(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			_, b, budget, closeD := openTextV2CommandWALNativeDB5829(t, profile, dir)
			mgr, closeMgr := newTextV2CommandWALManager5829(t, b)
			meta := &CollectionMeta{
				Name: "docs",
				TextIndexes: []TextIndexDefinition{{
					Name:           "lexical",
					Version:        TextIndexVersionV2,
					StorePositions: true,
					Fields:         []TextIndexField{{Field: "body"}},
				}},
			}
			if _, err := mgr.CreateCollection(meta); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := mgr.OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			if _, err := col.InsertBatch(
				[][]byte{[]byte("d1"), []byte("d2"), []byte("d3")},
				[][]byte{
					[]byte(`{"body":"refund policy guide"}`),
					[]byte(`{"body":"refund stale doc"}`),
					[]byte(`{"body":"shipping refund policy"}`),
				},
			); err != nil {
				t.Fatalf("InsertBatch: %v", err)
			}
			if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
				return []byte(`{"body":"refund updated guide"}`), true, nil
			}); err != nil {
				t.Fatalf("Update d1: %v", err)
			}
			if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
				t.Fatalf("DeleteBatch d2 deleted=%d err=%v", deleted, err)
			}

			// Verify RunStorageCompaction still returns ErrNativeSemanticUnsupported without advancing LSN.
			lsnBefore := b.CommandWALNextLSN()
			if _, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
				RunStorageCompaction: true,
			}); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("MaintainTextIndex(RunStorageCompaction) err=%v want ErrNativeSemanticUnsupported", err)
			}
			if b.CommandWALNextLSN() != lsnBefore {
				t.Fatalf("RunStorageCompaction refusal advanced WAL LSN %d->%d", lsnBefore, b.CommandWALNextLSN())
			}

			// Verify DryRun appends zero WAL frames on native COW.
			if _, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
				DryRun: true,
				Force:  true,
			}); err != nil {
				t.Fatalf("DryRun MaintainTextIndex: %v", err)
			}
			if b.CommandWALNextLSN() != lsnBefore {
				t.Fatalf("DryRun advanced WAL LSN %d->%d", lsnBefore, b.CommandWALNextLSN())
			}

			// Run live RewriteTextIndex on native COW under CommandWAL without checkpointing.
			baseLSN := b.CommandWALNextLSN() - 1
			rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
			if err != nil {
				t.Fatalf("native RewriteTextIndex: %v", err)
			}
			if rw.Noop || rw.TombstoneDocIDEntriesPurged == 0 || rw.StalePostingsPurged == 0 {
				t.Fatalf("native rw=%+v want concrete rewrite", rw)
			}
			if b.CommandWALNextLSN() <= baseLSN+1 {
				t.Fatalf("native CommandWALNextLSN=%d want > %d", b.CommandWALNextLSN(), baseLSN+1)
			}
			liveStatus, liveStorage := assertTextV2StatusZeroDebt5829(t, col, "lexical", 2)
			rankedLive := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
			phraseLive := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10})

			// Capture the full unpruned WAL stream (including the live-emitted CommandKindCollectionRewriteTextIndex frame)
			// BEFORE clean close checkpoints/prunes WAL, so we can force a true unapplied WAL replay from LSN 0 in cow_btree mode.
			liveBackendDir := b.Dir()
			frames := collectionCommandWALFrames(t, liveBackendDir)
			var rewriteFrame *commitlog.CommandEnvelope
			for i := len(frames) - 1; i >= 0; i-- {
				if frames[i].Kind == commitlog.CommandKindCollectionRewriteTextIndex {
					f := frames[i]
					rewriteFrame = &f
					break
				}
			}
			if rewriteFrame == nil {
				t.Fatalf("expected CommandKindCollectionRewriteTextIndex in native COW WAL frames (count=%d)", len(frames))
			}
			replayDir := t.TempDir()
			replayMainDir := replayDir
			if err := os.MkdirAll(replayMainDir, 0o755); err != nil {
				t.Fatalf("MkdirAll replayMainDir: %v", err)
			}
			fmtBytes, err := os.ReadFile(filepath.Join(liveBackendDir, "format.json"))
			if err != nil {
				t.Fatalf("ReadFile format.json: %v", err)
			}
			if err := os.WriteFile(filepath.Join(replayMainDir, "format.json"), fmtBytes, 0o644); err != nil {
				t.Fatalf("WriteFile format.json: %v", err)
			}
			copyCommandWALSegments5829(t, backenddb.WALDirPath(liveBackendDir), backenddb.WALDirPath(replayMainDir))

			// Close live native COW DB WITHOUT calling Checkpoint() and verify reopen.
			if err := closeMgr(); err != nil {
				t.Fatalf("mgr.Close live: %v", err)
			}
			if err := closeD(); err != nil {
				t.Fatalf("d.Close live: %v", err)
			}
			if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
				t.Fatalf("live Close retained native budget %+v", s)
			}

			// Reopen live dir (uncheckpointed close+reopen), verify MaintainTextIndex, then test in-place post-checkpoint unapplied replay.
			d2, b2, budget2, closeD2 := openTextV2CommandWALNativeDB5829(t, profile, dir)
			mgr2, closeMgr2 := newTextV2CommandWALManager5829(t, b2)
			col2, err := mgr2.OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection reopened native: %v", err)
			}
			assertTextV2StatusZeroDebt5829(t, col2, "lexical", 2)
			if got := searchIDs5829(t, col2, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10}); !reflect.DeepEqual(got, rankedLive) {
				t.Fatalf("reopened native ranked=%v want %v", got, rankedLive)
			}
			if got := searchIDs5829(t, col2, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10}); !reflect.DeepEqual(got, phraseLive) {
				t.Fatalf("reopened native phrase=%v want %v", got, phraseLive)
			}
			// Create fresh debt and run MaintainTextIndex on native COW under CommandWAL.
			if _, err := col2.Insert([]byte("d4"), []byte(`{"body":"refund updated bonus"}`)); err != nil {
				t.Fatalf("Insert d4: %v", err)
			}
			if deleted, err := col2.DeleteBatch([][]byte{[]byte("d4")}); err != nil || deleted != 1 {
				t.Fatalf("DeleteBatch d4 deleted=%d err=%v", deleted, err)
			}
			maintStats, err := col2.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
				Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1},
			})
			if err != nil || maintStats.IndexesRewritten != 1 {
				t.Fatalf("native MaintainTextIndex stats=%+v err=%v", maintStats, err)
			}
			assertTextV2StatusZeroDebt5829(t, col2, "lexical", 2)

			// Also create fresh debt, checkpoint, close, append an in-place unapplied rewrite frame on real physical files, and reopen.
			if _, err := col2.Insert([]byte("d5"), []byte(`{"body":"refund updated temp"}`)); err != nil {
				t.Fatalf("Insert d5: %v", err)
			}
			if deleted, err := col2.DeleteBatch([][]byte{[]byte("d5")}); err != nil || deleted != 1 {
				t.Fatalf("DeleteBatch d5 deleted=%d err=%v", deleted, err)
			}
			if err := d2.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint d2: %v", err)
			}
			postCheckpointLSN := b2.State().AppliedCommandLSN
			dirBackend := b2.Dir()
			if err := closeMgr2(); err != nil {
				t.Fatalf("mgr2.Close: %v", err)
			}
			if err := closeD2(); err != nil {
				t.Fatalf("d2.Close: %v", err)
			}
			if s := budget2.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
				t.Fatalf("reopened Close retained native budget %+v", s)
			}
			inPlaceEnv := *rewriteFrame
			inPlaceEnv.LSN = postCheckpointLSN + 1
			appendUnappliedCommandWALFrame5829(t, dirBackend, inPlaceEnv)
			inPlaceBefore := workstats.Read().Replay
			_, b3, budget3, closeD3 := openTextV2CommandWALNativeDB5829(t, profile, dir)
			inPlaceAfter := workstats.Read().Replay
			if inPlaceAfter.FramesApplied <= inPlaceBefore.FramesApplied {
				t.Fatalf("expected in-place native post-checkpoint replay: before=%+v after=%+v", inPlaceBefore, inPlaceAfter)
			}
			if got := b3.State().AppliedCommandLSN; got != postCheckpointLSN+1 {
				t.Fatalf("in-place native replayed LSN=%d want %d", got, postCheckpointLSN+1)
			}
			mgr3, closeMgr3 := newTextV2CommandWALManager5829(t, b3)
			col3, err := mgr3.OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection in-place replayed native: %v", err)
			}
			assertTextV2StatusZeroDebt5829(t, col3, "lexical", 2)
			if err := closeMgr3(); err != nil {
				t.Fatalf("mgr3.Close: %v", err)
			}
			if err := closeD3(); err != nil {
				t.Fatalf("d3.Close: %v", err)
			}
			if s := budget3.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
				t.Fatalf("in-place replayed Close retained native budget %+v", s)
			}

			// Forced full unapplied WAL replay from LSN 0 into replayDir using the exact live-emitted native COW WAL stream.
			replayBefore := workstats.Read().Replay
			_, rb, rbudget, closeRD := openTextV2CommandWALNativeDB5829(t, profile, replayDir)
			replayAfter := workstats.Read().Replay
			if replayAfter.FramesApplied <= replayBefore.FramesApplied {
				t.Fatalf("expected native COW unapplied WAL replay to apply rewrite frame: before=%+v after=%+v", replayBefore, replayAfter)
			}
			rmgr, closeRMgr := newTextV2CommandWALManager5829(t, rb)
			rcol, err := rmgr.OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection replayed native: %v", err)
			}
			rstatus, rstorage := assertTextV2StatusZeroDebt5829(t, rcol, "lexical", 2)
			if rstatus.SealedPostingBlocks != liveStatus.SealedPostingBlocks || rstorage.V2PositionEntries != liveStorage.V2PositionEntries {
				t.Fatalf("replayed native status/storage mismatch: %+v/%+v vs %+v/%+v", rstatus, rstorage, liveStatus, liveStorage)
			}
			if got := searchIDs5829(t, rcol, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10}); !reflect.DeepEqual(got, rankedLive) {
				t.Fatalf("replayed native ranked=%v want %v", got, rankedLive)
			}
			if got := searchIDs5829(t, rcol, TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund updated"}, TopK: 10}); !reflect.DeepEqual(got, phraseLive) {
				t.Fatalf("replayed native phrase=%v want %v", got, phraseLive)
			}
			if err := closeRMgr(); err != nil {
				t.Fatalf("rmgr.Close: %v", err)
			}
			if err := closeRD(); err != nil {
				t.Fatalf("rd.Close: %v", err)
			}
			if s := rbudget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
				t.Fatalf("replayed Close retained native budget %+v", s)
			}
		})
	}
}

func TestTextV2CommandWALConcurrentSearchDuringRewrite5829(t *testing.T) {
	dir := t.TempDir()
	d, closeDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, dir)
	mgr := NewCollectionManager(d)
	defer func() {
		if err := mgr.Close(); err != nil {
			t.Errorf("mgr.Close: %v", err)
		}
		if err := closeDB(); err != nil {
			t.Errorf("closeDB: %v", err)
		}
	}()

	meta := &CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "body"}},
		}},
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	const initialDocs = 48
	ids := make([][]byte, initialDocs)
	docs := make([][]byte, initialDocs)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		docs[i] = []byte(fmt.Sprintf(`{"body":"refund policy anchor token%d"}`, i%7))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	var stop atomic.Bool
	errCh := make(chan error, 8)
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for !stop.Load() {
				resp, err := col.SearchText(TextSearchOptions{
					IndexName: "lexical",
					Query:     "refund",
					TopK:      10,
				})
				if err != nil {
					select {
					case errCh <- fmt.Errorf("worker %d ranked search: %w", worker, err):
					default:
					}
					return
				}
				if len(resp.Results) == 0 {
					select {
					case errCh <- fmt.Errorf("worker %d ranked search returned 0 results", worker):
					default:
					}
					return
				}
				phrase, err := col.SearchText(TextSearchOptions{
					IndexName: "lexical",
					Phrase:    &TextSearchPhraseQuery{Query: "refund policy"},
					TopK:      10,
				})
				if err != nil {
					select {
					case errCh <- fmt.Errorf("worker %d phrase search: %w", worker, err):
					default:
					}
					return
				}
				if len(phrase.Results) == 0 {
					select {
					case errCh <- fmt.Errorf("worker %d phrase search returned 0 results", worker):
					default:
					}
					return
				}
				if _, err := col.TextIndexStatus("lexical"); err != nil {
					select {
					case errCh <- fmt.Errorf("worker %d TextIndexStatus: %w", worker, err):
					default:
					}
					return
				}
			}
		}(r)
	}

	for round := 0; round < 4; round++ {
		updateID := []byte(fmt.Sprintf("doc-%03d", round+10))
		if _, _, err := col.Update(updateID, func([]byte) ([]byte, bool, error) {
			return []byte(fmt.Sprintf(`{"body":"refund policy revised round%d"}`, round)), true, nil
		}); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("round %d Update: %v", round, err)
		}
		delID := []byte(fmt.Sprintf("doc-%03d", round+30))
		if deleted, err := col.DeleteBatch([][]byte{delID}); err != nil || deleted != 1 {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("round %d DeleteBatch deleted=%d err=%v", round, deleted, err)
		}
		if round%2 == 0 {
			if _, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{}); err != nil {
				stop.Store(true)
				wg.Wait()
				t.Fatalf("round %d RewriteTextIndex: %v", round, err)
			}
		} else {
			if _, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
				Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1},
			}); err != nil {
				stop.Store(true)
				wg.Wait()
				t.Fatalf("round %d MaintainTextIndex: %v", round, err)
			}
		}
	}

	stop.Store(true)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	assertTextV2StatusZeroDebt5829(t, col, "lexical", initialDocs-4)
}

func BenchmarkTextV2RewriteCommandWAL(b *testing.B) {
	docsN := textV2ContractEnvInt2623("TREEDB_TEXT_V2_REWRITE_DOCS", 512)
	if docsN < 32 {
		docsN = 32
	}
	updates := docsN / 8
	deletes := docsN / 16
	b.ReportAllocs()
	b.ReportMetric(float64(docsN), "docs/op")
	var last TextIndexRewriteStats
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		d, closeDB := openTextV2CommandWALOrdinaryDB5829(b, treedb.ProfileCommandWALDurable, b.TempDir())
		mgr := NewCollectionManager(d)
		meta := &CollectionMeta{
			Name: "docs",
			TextIndexes: []TextIndexDefinition{{
				Name:    "lexical",
				Version: TextIndexVersionV2,
				Fields:  []TextIndexField{{Field: "body"}},
			}},
		}
		if _, err := mgr.CreateCollection(meta); err != nil {
			b.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection("docs")
		if err != nil {
			b.Fatalf("OpenCollection: %v", err)
		}
		ids := make([][]byte, docsN)
		docs := make([][]byte, docsN)
		for j := range ids {
			ids[j] = []byte(fmt.Sprintf("doc-%06d-%d", j, i))
			docs[j] = []byte(fmt.Sprintf(`{"body":"refund common policy token%d"}`, j%31))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			b.Fatalf("InsertBatch: %v", err)
		}
		for j := 0; j < updates; j++ {
			id := append([]byte(nil), ids[j]...)
			if _, _, err := col.Update(id, func([]byte) ([]byte, bool, error) {
				return []byte(`{"body":"updated common policy"}`), true, nil
			}); err != nil {
				b.Fatalf("Update: %v", err)
			}
		}
		deleteIDs := make([][]byte, deletes)
		for j := 0; j < deletes; j++ {
			deleteIDs[j] = ids[updates+j]
		}
		if deleted, err := col.DeleteBatch(deleteIDs); err != nil || deleted != deletes {
			b.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
		}
		b.StartTimer()
		stats, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
		b.StopTimer()
		if err != nil {
			b.Fatalf("RewriteTextIndex: %v", err)
		}
		last = stats
		if err := mgr.Close(); err != nil {
			b.Fatalf("mgr.Close: %v", err)
		}
		if err := closeDB(); err != nil {
			b.Fatalf("closeDB: %v", err)
		}
	}
	b.ReportMetric(float64(last.PostingBlocksRead), "posting_blocks_read/op")
	b.ReportMetric(float64(last.PostingBlocksWritten), "posting_blocks_written/op")
	b.ReportMetric(float64(last.PostingBlocksDeleted), "posting_blocks_deleted/op")
	b.ReportMetric(float64(last.StalePostingsPurged), "stale_postings_purged/op")
	b.ReportMetric(float64(last.TombstoneDocIDEntriesPurged), "tombstones_purged/op")
}
