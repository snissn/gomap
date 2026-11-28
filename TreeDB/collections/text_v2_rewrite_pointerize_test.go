package collections

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func openTextV2RewritePointerizeNoWALDB5838(t *testing.T, dir string) (*backenddb.DB, func() error) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.PointerThreshold = 512
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	d, closeDB, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog(ProfileNoWALFast): %v", err)
	}
	var closed atomic.Bool
	closeOnce := func() error {
		if closed.Swap(true) {
			return nil
		}
		return closeDB()
	}
	t.Cleanup(func() {
		_ = closeOnce()
	})
	if !d.HasValueLogAppender() {
		t.Fatal("database has no value-log appender")
	}
	return d, closeOnce
}

func assertTextV2RootEntryPointerized5838(t *testing.T, d *backenddb.DB, colName, rootName string, key []byte) int {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, colName)
	if err != nil {
		t.Fatalf("loadCollectionCatalog(%q): %v", colName, err)
	}
	if catalog == nil {
		t.Fatalf("collection catalog %q not found", colName)
	}
	rootID := catalog.rootID(rootName)
	if rootID == 0 {
		t.Fatalf("root %q has zero rootID", rootName)
	}
	entry, err := snap.GetEntryAtRoot(rootID, key)
	if err != nil {
		t.Fatalf("GetEntryAtRoot(%q, %x): %v", rootName, key, err)
	}
	value, err := snap.GetAtRoot(rootID, key)
	if err != nil {
		t.Fatalf("GetAtRoot(%q, %x): %v", rootName, key, err)
	}
	maxInline := page.PageSize - node.NodeHeaderSize - node.DirectoryEntrySize - 7 - page.EntryRevisionSize - len(key)
	if len(value) <= maxInline {
		t.Fatalf("root %q key %x value len=%d unexpectedly fits inline max=%d", rootName, key, len(value), maxInline)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		t.Fatalf("root %q key %x flags=%#x ptr=%+v want value-log pointer (FlagPointer)", rootName, key, entry.Flags, entry.ValuePtr)
	}
	return len(value)
}

func assertTextV2PostingTermPointerized5838(t *testing.T, d *backenddb.DB, colName, indexName, term string) int {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, colName)
	if err != nil {
		t.Fatalf("loadCollectionCatalog(%q): %v", colName, err)
	}
	if catalog == nil {
		t.Fatalf("collection catalog %q not found", colName)
	}
	rootName := collectionTextV2PostingBlocksRootName(colName, indexName)
	rootID := catalog.rootID(rootName)
	if rootID == 0 {
		t.Fatalf("posting root %q has zero rootID", rootName)
	}
	prefix := encodeTextV2PostingBlockTermPrefix(term)
	it, err := snap.IteratorAtRoot(rootID, prefix, nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot(%q): %v", rootName, err)
	}
	var keys [][]byte
	for it.Valid() {
		k := it.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		keys = append(keys, bytes.Clone(k))
		it.Next()
	}
	iterErr := it.Error()
	closeErr := it.Close()
	if iterErr != nil {
		t.Fatalf("posting iterator error: %v", iterErr)
	}
	if closeErr != nil {
		t.Fatalf("posting iterator close: %v", closeErr)
	}
	if len(keys) != 1 {
		t.Fatalf("term %q posting block count=%d want 1", term, len(keys))
	}
	return assertTextV2RootEntryPointerized5838(t, d, colName, rootName, keys[0])
}

func assertSortedStringSetEqual5838(t *testing.T, label string, got, want []string) {
	t.Helper()
	gotSorted := append([]string(nil), got...)
	wantSorted := append([]string(nil), want...)
	sort.Strings(gotSorted)
	sort.Strings(wantSorted)
	if !reflect.DeepEqual(gotSorted, wantSorted) {
		t.Fatalf("%s mismatch: got %d (%v) want %d (%v)", label, len(gotSorted), gotSorted, len(wantSorted), wantSorted)
	}
}

func populateLargeDocMapDataset5838(t *testing.T, col *Collection) (wantLiveIDs, wantUpdatedIDs, wantPhraseIDs []string) {
	t.Helper()
	const (
		totalDocs   = 140
		deleteCount = 8
		updateStart = 8
		updateEnd   = 12
		phraseEnd   = 16
	)
	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		id := fmt.Sprintf("gh_item:pr:%04d:chunk:000:%s", i, strings.Repeat("a", 34))
		ids[i] = []byte(id)
		switch {
		case i < deleteCount:
			docs[i] = []byte(`{"kind":"pr","body":"alpha deleted_only common_term exact phrase target"}`)
		case i < updateEnd:
			docs[i] = []byte(`{"kind":"pr","body":"alpha old_only common_term"}`)
		case i < phraseEnd:
			docs[i] = []byte(`{"kind":"pr","body":"alpha steady_only common_term exact phrase target"}`)
		default:
			docs[i] = []byte(`{"kind":"pr","body":"alpha steady_only common_term"}`)
		}
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch(%d docs): %v", totalDocs, err)
	}
	// Delete and update documents inside the first 128-ordinal docMap block so
	// tombstone purging must rewrite the >6 KiB docMap block.
	if deleted, err := col.DeleteBatch(ids[:deleteCount]); err != nil || deleted != deleteCount {
		t.Fatalf("DeleteBatch(%d): deleted=%d err=%v", deleteCount, deleted, err)
	}
	for i := updateStart; i < updateEnd; i++ {
		if _, _, err := col.Update(ids[i], func([]byte) ([]byte, bool, error) {
			return []byte(`{"kind":"pr","body":"alpha updated_only common_term exact phrase target"}`), true, nil
		}); err != nil {
			t.Fatalf("Update(%s): %v", ids[i], err)
		}
	}

	wantLiveIDs = make([]string, 0, totalDocs-deleteCount)
	wantUpdatedIDs = make([]string, 0, updateEnd-updateStart)
	wantPhraseIDs = make([]string, 0, phraseEnd-updateStart)
	for i := deleteCount; i < totalDocs; i++ {
		id := string(ids[i])
		wantLiveIDs = append(wantLiveIDs, id)
		if i < updateEnd {
			wantUpdatedIDs = append(wantUpdatedIDs, id)
		}
		if i < phraseEnd {
			wantPhraseIDs = append(wantPhraseIDs, id)
		}
	}
	return wantLiveIDs, wantUpdatedIDs, wantPhraseIDs
}

func assertLargeDocMapSearch5838(t *testing.T, col *Collection, wantLiveIDs, wantUpdatedIDs, wantPhraseIDs []string) (ranked, phrase []string) {
	t.Helper()
	ranked = searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Query: "common_term", TopK: 200})
	assertSortedStringSetEqual5838(t, "ranked common_term", ranked, wantLiveIDs)

	if deletedHits := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Query: "deleted_only", TopK: 20}); len(deletedHits) != 0 {
		t.Fatalf("deleted_only hits=%v want empty", deletedHits)
	}
	if oldHits := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Query: "old_only", TopK: 20}); len(oldHits) != 0 {
		t.Fatalf("old_only hits=%v want empty", oldHits)
	}
	updatedHits := searchIDs5829(t, col, TextSearchOptions{IndexName: "lexical", Query: "updated_only", TopK: 20})
	assertSortedStringSetEqual5838(t, "updated_only", updatedHits, wantUpdatedIDs)

	phrase = searchIDs5829(t, col, TextSearchOptions{
		IndexName: "lexical",
		Phrase:    &TextSearchPhraseQuery{Query: "exact phrase target"},
		TopK:      20,
	})
	assertSortedStringSetEqual5838(t, "phrase exact phrase target", phrase, wantPhraseIDs)
	return ranked, phrase
}

func TestTextV2RewritePointerizesLargeDocMapBlocksOrdinary(t *testing.T) {
	dir := t.TempDir()
	d, closeDB := openTextV2RewritePointerizeNoWALDB5838(t, dir)
	mgr, closeMgr := newTextV2CommandWALManager5829(t, d)

	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:          DocumentFormatJSON,
			DataRootStoragePolicy:   RootStoragePager,
			IndexStateStoragePolicy: RootStoragePager,
		},
		Indexes: []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}},
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "body"}},
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	wantLiveIDs, wantUpdatedIDs, wantPhraseIDs := populateLargeDocMapDataset5838(t, col)

	maint, err := mgr.MaintainTextIndexes(context.Background(), TextIndexMaintenanceOptions{
		Force:                true,
		RunStorageCompaction: true,
	})
	if err != nil {
		t.Fatalf("MaintainTextIndexes: %v", err)
	}
	if maint.IndexesRewritten != 1 || !maint.StorageCompacted {
		t.Fatalf("MaintainTextIndexes stats=%+v want IndexesRewritten=1 and StorageCompacted=true", maint)
	}
	if len(maint.Collections) != 1 || len(maint.Collections[0].Indexes) != 1 {
		t.Fatalf("MaintainTextIndexes collections=%+v want 1 collection with 1 index", maint.Collections)
	}
	rw := maint.Collections[0].Indexes[0].Rewrite
	if rw.Noop || rw.TombstoneDocMapEntriesPurged == 0 {
		t.Fatalf("rewrite stats=%+v want concrete rewrite with TombstoneDocMapEntriesPurged>0", rw)
	}

	assertTextV2StatusZeroDebt5829(t, col, "lexical", uint64(len(wantLiveIDs)))
	docMapRoot := collectionTextV2DocMapRootName("docs", "lexical")
	assertTextV2RootEntryPointerized5838(t, d, "docs", docMapRoot, encodeTextV2BlockKey(1))
	rankedBefore, phraseBefore := assertLargeDocMapSearch5838(t, col, wantLiveIDs, wantUpdatedIDs, wantPhraseIDs)

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := closeMgr(); err != nil {
		t.Fatalf("closeMgr: %v", err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("closeDB: %v", err)
	}

	reopenedDB, closeReopenedDB := openTextV2RewritePointerizeNoWALDB5838(t, dir)
	reopenedMgr, closeReopenedMgr := newTextV2CommandWALManager5829(t, reopenedDB)
	reopenedCol, err := reopenedMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTextV2StatusZeroDebt5829(t, reopenedCol, "lexical", uint64(len(wantLiveIDs)))
	assertTextV2RootEntryPointerized5838(t, reopenedDB, "docs", docMapRoot, encodeTextV2BlockKey(1))
	rankedAfter, phraseAfter := assertLargeDocMapSearch5838(t, reopenedCol, wantLiveIDs, wantUpdatedIDs, wantPhraseIDs)
	if !reflect.DeepEqual(rankedAfter, rankedBefore) || !reflect.DeepEqual(phraseAfter, phraseBefore) {
		t.Fatalf("reopen search mismatch ranked=%v/%v phrase=%v/%v", rankedAfter, rankedBefore, phraseAfter, phraseBefore)
	}
	if err := closeReopenedMgr(); err != nil {
		t.Fatalf("closeReopenedMgr: %v", err)
	}
	if err := closeReopenedDB(); err != nil {
		t.Fatalf("closeReopenedDB: %v", err)
	}
}

func TestTextV2RewritePointerizesLargeDocMapBlocksCommandWALAndReplay(t *testing.T) {
	liveDir := t.TempDir()
	d, closeDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, liveDir)
	if !d.HasValueLogAppender() {
		t.Fatal("command-WAL database has no value-log appender")
	}
	mgr, closeMgr := newTextV2CommandWALManager5829(t, d)

	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "docs",
		Indexes: []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}},
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "body"}},
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	wantLiveIDs, wantUpdatedIDs, wantPhraseIDs := populateLargeDocMapDataset5838(t, col)

	rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
	if err != nil {
		t.Fatalf("RewriteTextIndex: %v", err)
	}
	if rw.Noop || rw.TombstoneDocMapEntriesPurged == 0 {
		t.Fatalf("RewriteTextIndex stats=%+v want concrete rewrite with TombstoneDocMapEntriesPurged>0", rw)
	}

	docMapRoot := collectionTextV2DocMapRootName("docs", "lexical")
	liveStatus, liveStorage := assertTextV2StatusZeroDebt5829(t, col, "lexical", uint64(len(wantLiveIDs)))
	assertTextV2RootEntryPointerized5838(t, d, "docs", docMapRoot, encodeTextV2BlockKey(1))
	liveRanked, livePhrase := assertLargeDocMapSearch5838(t, col, wantLiveIDs, wantUpdatedIDs, wantPhraseIDs)
	liveAppliedLSN := d.State().AppliedCommandLSN
	liveBackendDir := d.Dir()

	// Copy only format.json and commit-l*.log segments (never the lock file) into
	// a fresh directory so reopening exercises full LSN-0 CommandWAL replay via
	// replayInlineAppender.
	replayDir := t.TempDir()
	replayMainDir := replayDir
	if err := os.MkdirAll(replayMainDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", replayMainDir, err)
	}
	fmtBytes, err := os.ReadFile(filepath.Join(liveBackendDir, "format.json"))
	if err != nil {
		t.Fatalf("ReadFile format.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(replayMainDir, "format.json"), fmtBytes, 0o644); err != nil {
		t.Fatalf("WriteFile format.json: %v", err)
	}
	copyCommandWALSegments5829(t, backenddb.WALDirPath(liveBackendDir), backenddb.WALDirPath(replayMainDir))

	// Close without Checkpoint() so reopening liveDir also verifies durable
	// command-WAL persistence without explicit checkpoint.
	if err := closeMgr(); err != nil {
		t.Fatalf("closeMgr: %v", err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("closeDB: %v", err)
	}

	reopenedDB, closeReopenedDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, liveDir)
	reopenedMgr, closeReopenedMgr := newTextV2CommandWALManager5829(t, reopenedDB)
	reopenedCol, err := reopenedMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTextV2StatusZeroDebt5829(t, reopenedCol, "lexical", uint64(len(wantLiveIDs)))
	assertTextV2RootEntryPointerized5838(t, reopenedDB, "docs", docMapRoot, encodeTextV2BlockKey(1))
	reopenedRanked, reopenedPhrase := assertLargeDocMapSearch5838(t, reopenedCol, wantLiveIDs, wantUpdatedIDs, wantPhraseIDs)
	if !reflect.DeepEqual(reopenedRanked, liveRanked) || !reflect.DeepEqual(reopenedPhrase, livePhrase) {
		t.Fatalf("reopen search mismatch ranked=%v/%v phrase=%v/%v", reopenedRanked, liveRanked, reopenedPhrase, livePhrase)
	}
	if err := closeReopenedMgr(); err != nil {
		t.Fatalf("closeReopenedMgr: %v", err)
	}
	if err := closeReopenedDB(); err != nil {
		t.Fatalf("closeReopenedDB: %v", err)
	}

	// Crash-recovery replay from LSN 0 into fresh replayDir.
	replayBefore := workstats.Read().Replay
	replayedDB, closeReplayedDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, replayDir)
	replayAfter := workstats.Read().Replay
	if replayAfter.FramesApplied <= replayBefore.FramesApplied {
		t.Fatalf("expected WAL replay to apply frames: before=%+v after=%+v", replayBefore, replayAfter)
	}
	if got := replayedDB.State().AppliedCommandLSN; got != liveAppliedLSN {
		t.Fatalf("replayed AppliedCommandLSN=%d want %d", got, liveAppliedLSN)
	}
	replayedMgr, closeReplayedMgr := newTextV2CommandWALManager5829(t, replayedDB)
	replayedCol, err := replayedMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection replayed: %v", err)
	}
	replayedStatus, replayedStorage := assertTextV2StatusZeroDebt5829(t, replayedCol, "lexical", uint64(len(wantLiveIDs)))
	if replayedStatus.SealedPostingBlocks != liveStatus.SealedPostingBlocks ||
		replayedStatus.RootGeneration != liveStatus.RootGeneration ||
		replayedStorage.V2PositionEntries != liveStorage.V2PositionEntries {
		t.Fatalf("replayed status/storage mismatch: status=%+v/%+v storage=%+v/%+v", replayedStatus, liveStatus, replayedStorage, liveStorage)
	}
	assertTextV2RootEntryPointerized5838(t, replayedDB, "docs", docMapRoot, encodeTextV2BlockKey(1))
	replayedRanked, replayedPhrase := assertLargeDocMapSearch5838(t, replayedCol, wantLiveIDs, wantUpdatedIDs, wantPhraseIDs)
	if !reflect.DeepEqual(replayedRanked, liveRanked) || !reflect.DeepEqual(replayedPhrase, livePhrase) {
		t.Fatalf("replayed search mismatch ranked=%v/%v phrase=%v/%v", replayedRanked, liveRanked, replayedPhrase, livePhrase)
	}
	if err := closeReplayedMgr(); err != nil {
		t.Fatalf("closeReplayedMgr: %v", err)
	}
	if err := closeReplayedDB(); err != nil {
		t.Fatalf("closeReplayedDB: %v", err)
	}
}

func TestTextV2RewritePointerizesLargePostingBlock(t *testing.T) {
	dir := t.TempDir()
	d, closeDB := openTextV2RewritePointerizeNoWALDB5838(t, dir)
	mgr, closeMgr := newTextV2CommandWALManager5829(t, d)

	fields := make([]TextIndexField, 8)
	for i := range fields {
		fields[i] = TextIndexField{Field: fmt.Sprintf("f%d", i)}
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:          DocumentFormatJSON,
			DataRootStoragePolicy:   RootStoragePager,
			IndexStateStoragePolicy: RootStoragePager,
		},
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         fields,
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	const (
		totalDocs   = 400
		deleteCount = 10
		phraseEnd   = 20
	)
	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		// Keep doc IDs short so docMap blocks remain inline and this test isolates
		// an oversized sealed posting block (>4 KiB) during RewriteTextIndex.
		ids[i] = []byte(fmt.Sprintf("d%04d", i))
		f0 := "common_token steady_doc"
		if i < deleteCount {
			f0 = "common_token deleted_token target phrase"
		} else if i < phraseEnd {
			f0 = "common_token target phrase"
		}
		docs[i] = []byte(fmt.Sprintf(
			`{"f0":%q,"f1":"common_token","f2":"common_token","f3":"common_token","f4":"common_token","f5":"common_token","f6":"common_token","f7":"common_token"}`,
			f0,
		))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch(%d docs): %v", totalDocs, err)
	}
	if deleted, err := col.DeleteBatch(ids[:deleteCount]); err != nil || deleted != deleteCount {
		t.Fatalf("DeleteBatch(%d): deleted=%d err=%v", deleteCount, deleted, err)
	}

	rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{
		TargetPostingsPerBlock: 512,
	})
	if err != nil {
		t.Fatalf("RewriteTextIndex(TargetPostingsPerBlock=512): %v", err)
	}
	if rw.Noop || rw.StalePostingsPurged == 0 {
		t.Fatalf("RewriteTextIndex stats=%+v want concrete rewrite with StalePostingsPurged>0", rw)
	}

	wantLiveIDs := make([]string, 0, totalDocs-deleteCount)
	wantPhraseIDs := make([]string, 0, phraseEnd-deleteCount)
	for i := deleteCount; i < totalDocs; i++ {
		id := string(ids[i])
		wantLiveIDs = append(wantLiveIDs, id)
		if i < phraseEnd {
			wantPhraseIDs = append(wantPhraseIDs, id)
		}
	}

	assertTextV2StatusZeroDebt5829(t, col, "lexical", uint64(len(wantLiveIDs)))
	assertTextV2PostingTermPointerized5838(t, d, "docs", "lexical", "common_token")

	assertPostingSearch := func(target *Collection) (ranked, phrase []string) {
		t.Helper()
		ranked = searchIDs5829(t, target, TextSearchOptions{IndexName: "lexical", Query: "common_token", TopK: totalDocs + 50})
		assertSortedStringSetEqual5838(t, "ranked common_token", ranked, wantLiveIDs)
		if deletedHits := searchIDs5829(t, target, TextSearchOptions{IndexName: "lexical", Query: "deleted_token", TopK: 20}); len(deletedHits) != 0 {
			t.Fatalf("deleted_token hits=%v want empty", deletedHits)
		}
		phrase = searchIDs5829(t, target, TextSearchOptions{
			IndexName: "lexical",
			Phrase:    &TextSearchPhraseQuery{Query: "target phrase"},
			TopK:      30,
		})
		assertSortedStringSetEqual5838(t, "phrase target phrase", phrase, wantPhraseIDs)
		return ranked, phrase
	}
	rankedBefore, phraseBefore := assertPostingSearch(col)

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := closeMgr(); err != nil {
		t.Fatalf("closeMgr: %v", err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("closeDB: %v", err)
	}

	reopenedDB, closeReopenedDB := openTextV2RewritePointerizeNoWALDB5838(t, dir)
	reopenedMgr, closeReopenedMgr := newTextV2CommandWALManager5829(t, reopenedDB)
	reopenedCol, err := reopenedMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTextV2StatusZeroDebt5829(t, reopenedCol, "lexical", uint64(len(wantLiveIDs)))
	assertTextV2PostingTermPointerized5838(t, reopenedDB, "docs", "lexical", "common_token")
	rankedAfter, phraseAfter := assertPostingSearch(reopenedCol)
	if !reflect.DeepEqual(rankedAfter, rankedBefore) || !reflect.DeepEqual(phraseAfter, phraseBefore) {
		t.Fatalf("reopen search mismatch ranked=%v/%v phrase=%v/%v", rankedAfter, rankedBefore, phraseAfter, phraseBefore)
	}
	if err := closeReopenedMgr(); err != nil {
		t.Fatalf("closeReopenedMgr: %v", err)
	}
	if err := closeReopenedDB(); err != nil {
		t.Fatalf("closeReopenedDB: %v", err)
	}
}

func TestTextV2RewritePointerizesNoIndexCollectionLargeDocMapBlocks(t *testing.T) {
	// Text-only collection (zero scalar indexes): exercises both insertBatchNoIndex
	// text delta pointerization and RewriteTextIndex pointerization.
	dir := t.TempDir()
	d, closeDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, dir)
	mgr, closeMgr := newTextV2CommandWALManager5829(t, d)

	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "body"}},
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	wantLiveIDs, wantUpdatedIDs, wantPhraseIDs := populateLargeDocMapDataset5838(t, col)

	rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
	if err != nil {
		t.Fatalf("RewriteTextIndex: %v", err)
	}
	if rw.Noop || rw.TombstoneDocMapEntriesPurged == 0 {
		t.Fatalf("RewriteTextIndex stats=%+v want concrete rewrite with TombstoneDocMapEntriesPurged>0", rw)
	}

	docMapRoot := collectionTextV2DocMapRootName("docs", "lexical")
	assertTextV2StatusZeroDebt5829(t, col, "lexical", uint64(len(wantLiveIDs)))
	assertTextV2RootEntryPointerized5838(t, d, "docs", docMapRoot, encodeTextV2BlockKey(1))
	rankedBefore, phraseBefore := assertLargeDocMapSearch5838(t, col, wantLiveIDs, wantUpdatedIDs, wantPhraseIDs)

	if err := closeMgr(); err != nil {
		t.Fatalf("closeMgr: %v", err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("closeDB: %v", err)
	}

	reopenedDB, closeReopenedDB := openTextV2CommandWALOrdinaryDB5829(t, treedb.ProfileCommandWALDurable, dir)
	reopenedMgr, closeReopenedMgr := newTextV2CommandWALManager5829(t, reopenedDB)
	reopenedCol, err := reopenedMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTextV2StatusZeroDebt5829(t, reopenedCol, "lexical", uint64(len(wantLiveIDs)))
	assertTextV2RootEntryPointerized5838(t, reopenedDB, "docs", docMapRoot, encodeTextV2BlockKey(1))
	rankedAfter, phraseAfter := assertLargeDocMapSearch5838(t, reopenedCol, wantLiveIDs, wantUpdatedIDs, wantPhraseIDs)
	if !reflect.DeepEqual(rankedAfter, rankedBefore) || !reflect.DeepEqual(phraseAfter, phraseBefore) {
		t.Fatalf("reopen search mismatch ranked=%v/%v phrase=%v/%v", rankedAfter, rankedBefore, phraseAfter, phraseBefore)
	}
	if err := closeReopenedMgr(); err != nil {
		t.Fatalf("closeReopenedMgr: %v", err)
	}
	if err := closeReopenedDB(); err != nil {
		t.Fatalf("closeReopenedDB: %v", err)
	}
}
