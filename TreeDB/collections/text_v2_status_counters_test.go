package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func assertTextV2StatusMatchesStorageStats5828(t *testing.T, col *Collection, indexName, stage string, wantStale uint64) TextIndexStatus {
	t.Helper()
	status, err := col.TextIndexStatus(indexName)
	if err != nil {
		t.Fatalf("%s TextIndexStatus: %v", stage, err)
	}
	if !status.Ready || status.Version != TextIndexVersionV2 || status.RewriteMergeState != TextIndexRewriteMergeStateReady || status.FailClosed {
		t.Fatalf("%s status=%+v want ready v2 with unchanged RewriteMergeState", stage, status)
	}
	stats, err := col.TextIndexStorageStats(indexName)
	if err != nil {
		t.Fatalf("%s TextIndexStorageStats: %v", stage, err)
	}
	if status.LiveDocuments != stats.V2LiveDocuments ||
		status.DeletedDocuments != stats.V2DeletedDocs ||
		status.PostingBlocks != stats.V2PostingBlocks ||
		status.SealedPostingBlocks != stats.V2SealedPostingBlocks ||
		status.MicroPostingBlocks != stats.V2MicroPostingBlocks ||
		status.DeltaPostingBlocks != stats.V2DeltaPostingBlocks ||
		status.StalePostings != wantStale ||
		status.RootGeneration != stats.V2RootGeneration {
		t.Fatalf("%s status=%+v stats=%+v wantStale=%d", stage, status, stats, wantStale)
	}
	return status
}

func deleteTextRootKey5828(t *testing.T, d *backenddb.DB, collection, rootName string, key []byte) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	baseRoot := catalog.rootID(rootName)
	policy, err := collectionRootStoragePolicyForDB(d, catalog.meta, rootName)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("root policy: %v", err)
	}
	_ = snap.Close()
	table := newCollectionRunTable(1)
	table.DeleteSteal(bytes.Clone(key))
	table.Freeze()
	defer resetCollectionTables([]memtable.Table{table})
	iter := table.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	ordered := []backenddb.OrderedRootDeltaPublishInput{{BaseRoot: baseRoot, Iter: iter, StoragePolicy: policy}}
	_, rootIDs, err := d.PublishOrderedRootDeltaGroupWithSystemBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("unexpected root ids %d", len(rootIDs))
		}
		current := d.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		return buildSystemTargetIterator(current, map[string][]byte{systemCollectionRootKey(rootName): encodeRootID(rootIDs[0])})
	})
	if err != nil {
		t.Fatalf("publish delete key: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}
}

func TestTextV2StatusCountersLifecycle5828(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2"), []byte("d3")},
		[][]byte{
			[]byte(`{"title":"alpha shared","body":"shared beta"}`),
			[]byte(`{"title":"gamma","body":"delta epsilon"}`),
			[]byte(`{"title":"zeta","body":"eta"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	def := TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title"}, {Field: "body"}},
	}
	if _, _, err := col.CreateTextIndex(def); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}

	// 1. After backfill: all sealed, zero micro/delta/stale.
	st := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "backfill", 0)
	if st.LiveDocuments != 3 || st.DeletedDocuments != 0 || st.SealedPostingBlocks == 0 || st.MicroPostingBlocks != 0 || st.DeltaPostingBlocks != 0 || st.RootGeneration != 1 {
		t.Fatalf("backfill status=%+v", st)
	}

	// 2. Live insert: adds micro blocks, zero stale postings.
	if _, err := col.Insert([]byte("d4"), []byte(`{"title":"theta","body":"iota"}`)); err != nil {
		t.Fatalf("Insert d4: %v", err)
	}
	st = assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "insert", 0)
	if st.LiveDocuments != 4 || st.MicroPostingBlocks == 0 {
		t.Fatalf("insert status=%+v", st)
	}

	// 3. Cross-field update: d1 had unique terms {alpha, shared, beta} = 3 (shared in both title & body counts ONCE).
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"title":"kappa","body":"lambda"}`), true, nil
	}); err != nil {
		t.Fatalf("Update d1: %v", err)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "cross-field update", 3)

	// 4. Update-to-empty-text: d2 had {gamma, delta, epsilon} = 3 unique terms -> 0 new micro blocks, +3 stale postings.
	beforeEmpty := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "before-empty", 3)
	if _, _, err := col.Update([]byte("d2"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"title":"","body":""}`), true, nil
	}); err != nil {
		t.Fatalf("Update d2 to empty: %v", err)
	}
	afterEmpty := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "update-to-empty", 6)
	if afterEmpty.MicroPostingBlocks != beforeEmpty.MicroPostingBlocks {
		t.Fatalf("update-to-empty micro blocks before=%d after=%d want unchanged", beforeEmpty.MicroPostingBlocks, afterEmpty.MicroPostingBlocks)
	}

	// 5. Delete d3 ({zeta, eta} = 2 unique terms): DeletedDocuments=1, StalePostings=8.
	if err := col.Delete([]byte("d3")); err != nil {
		t.Fatalf("Delete d3: %v", err)
	}
	st = assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "delete", 8)
	if st.LiveDocuments != 3 || st.DeletedDocuments != 1 {
		t.Fatalf("delete status=%+v", st)
	}

	// 6. Rewrite: clears deleted docs, micro blocks, stale postings.
	rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
	if err != nil {
		t.Fatalf("RewriteTextIndex: %v", err)
	}
	if rw.StalePostingsPurged != 8 {
		t.Fatalf("rewrite StalePostingsPurged=%d want 8", rw.StalePostingsPurged)
	}
	st = assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "rewrite", 0)
	if st.DeletedDocuments != 0 || st.MicroPostingBlocks != 0 || st.DeltaPostingBlocks != 0 || st.StalePostings != 0 {
		t.Fatalf("rewrite status=%+v", st)
	}

	// 7. Checkpoint & reopen persistence.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	col, err = NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedStatus := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "reopen", 0)
	if reopenedStatus.LiveDocuments != st.LiveDocuments ||
		reopenedStatus.SealedPostingBlocks != st.SealedPostingBlocks ||
		reopenedStatus.RootGeneration != st.RootGeneration {
		t.Fatalf("reopened status=%+v want %+v", reopenedStatus, st)
	}
}

func TestTextV2StatusValueCodecCountersAndOverflow5828(t *testing.T) {
	status := textV2IndexStatusValue{
		FormatVersion:       textV2FormatVersion,
		RootGeneration:      19,
		StatsGeneration:     19,
		DocMapGeneration:    18,
		NormGeneration:      18,
		TermGeneration:      19,
		NextOrdinal:         500,
		LiveDocuments:       400,
		DeletedDocuments:    25,
		SealedPostingBlocks: 1234,
		MicroPostingBlocks:  56,
		DeltaPostingBlocks:  7,
		StalePostings:       890,
	}
	raw := encodeTextV2IndexStatusValue(status)
	got, err := decodeTextV2IndexStatusValue(raw)
	if err != nil {
		t.Fatalf("decodeTextV2IndexStatusValue: %v", err)
	}
	if got != status {
		t.Fatalf("decoded=%+v want %+v", got, status)
	}

	cases := []struct {
		name string
		raw  []byte
	}{
		{
			name: "truncated legacy 9-field payload",
			raw: func() []byte {
				out := []byte{textV2StatusValueVersion}
				out = appendTextUvarint(out, uint64(textV2FormatVersion))
				out = appendTextUvarint(out, 1)
				out = appendTextUvarint(out, 1)
				out = appendTextUvarint(out, 1)
				out = appendTextUvarint(out, 1)
				out = appendTextUvarint(out, 1)
				out = appendTextUvarint(out, 2)
				out = appendTextUvarint(out, 1)
				out = appendTextUvarint(out, 0)
				return out
			}(),
		},
		{
			name: "truncated before stale postings",
			raw:  raw[:len(raw)-2],
		},
		{
			name: "trailing byte",
			raw:  append(bytes.Clone(raw), 0x00),
		},
		{
			name: "sealed plus micro overflow",
			raw: encodeTextV2IndexStatusValue(textV2IndexStatusValue{
				FormatVersion:       textV2FormatVersion,
				RootGeneration:      1,
				StatsGeneration:     1,
				DocMapGeneration:    1,
				NormGeneration:      1,
				TermGeneration:      1,
				NextOrdinal:         2,
				LiveDocuments:       1,
				SealedPostingBlocks: math.MaxUint64,
				MicroPostingBlocks:  1,
			}),
		},
		{
			name: "sealed plus micro plus delta overflow",
			raw: encodeTextV2IndexStatusValue(textV2IndexStatusValue{
				FormatVersion:       textV2FormatVersion,
				RootGeneration:      1,
				StatsGeneration:     1,
				DocMapGeneration:    1,
				NormGeneration:      1,
				TermGeneration:      1,
				NextOrdinal:         2,
				LiveDocuments:       1,
				SealedPostingBlocks: math.MaxUint64 - 5,
				MicroPostingBlocks:  5,
				DeltaPostingBlocks:  1,
			}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeTextV2IndexStatusValue(tc.raw); !errors.Is(err, ErrTextIndexStorageCorrupt) {
				t.Fatalf("decode err=%v want ErrTextIndexStorageCorrupt", err)
			}
		})
	}
}

func TestTextV2StatusCrossFieldDedupAndUpdateToEmptyMaintenance5828(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2"), []byte("d3")},
		[][]byte{
			[]byte(`{"title":"shared","body":"shared"}`),
			[]byte(`{"title":"dup","body":"dup"}`),
			[]byte(`{"title":"","body":"alpha beta"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title"}, {Field: "body"}},
	}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}

	// Update d1: "shared" appears in both title & body -> StalePostings increments by 1 (not 2).
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"title":"fresh","body":"fresh"}`), true, nil
	}); err != nil {
		t.Fatalf("Update d1: %v", err)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "update cross-field single term", 1)

	// Delete d2: "dup" appears in both title & body -> StalePostings increments by 1 (total 2, not 4).
	if err := col.Delete([]byte("d2")); err != nil {
		t.Fatalf("Delete d2: %v", err)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "delete cross-field single term", 2)

	// Rewrite must pass fail-closed cross-check (StalePostingsPurged == 2).
	rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
	if err != nil {
		t.Fatalf("RewriteTextIndex: %v", err)
	}
	if rw.StalePostingsPurged != 2 {
		t.Fatalf("StalePostingsPurged=%d want 2", rw.StalePostingsPurged)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "post-rewrite zero debt", 0)

	// Update d3 to empty text on a zero-debt sealed index:
	// DeletedDocuments == 0, MicroPostingBlocks == 0, DeltaPostingBlocks == 0, StalePostings == 2.
	if _, _, err := col.Update([]byte("d3"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"title":"","body":""}`), true, nil
	}); err != nil {
		t.Fatalf("Update d3 to empty: %v", err)
	}
	st := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "update-to-empty only stale debt", 2)
	if st.DeletedDocuments != 0 || st.MicroPostingBlocks != 0 || st.DeltaPostingBlocks != 0 || st.StalePostings != 2 {
		t.Fatalf("status=%+v want only StalePostings=2", st)
	}

	// MaintainTextIndex must NOT falsely fast-skip because StalePostings == 2 > 0!
	maint, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
		Policy: TextIndexMaintenancePolicy{MinStalePostings: 1},
	})
	if err != nil {
		t.Fatalf("MaintainTextIndex: %v", err)
	}
	if maint.IndexesRewritten != 1 || len(maint.Indexes) != 1 || !maint.Indexes[0].Applied || maint.Indexes[0].Noop || maint.Indexes[0].Rewrite.StalePostingsPurged != 2 {
		t.Fatalf("MaintainTextIndex stats=%+v want stale postings purged", maint)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "after stale-only maintenance", 0)
}

func publishPendingTextV2InsertForFlushHookTest5828(t *testing.T, d *backenddb.DB, collection, indexName string, docID, doc []byte) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil || catalog == nil {
		t.Fatalf("loadCollectionCatalog: catalog=%v err=%v", catalog, err)
	}
	def, ok := findTextIndex(catalog.meta.TextIndexes, indexName)
	if !ok {
		t.Fatalf("missing index %q", indexName)
	}
	opts, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		t.Fatalf("collectionPlannerOptions: %v", err)
	}
	var rootNames []string
	baseRootIDs := make(map[string]uint64, 8)
	var policies []backenddb.OrderedRootStoragePolicy
	var tables []memtable.Table
	primaryRoot := collectionPrimaryRootName(catalog.meta.Name)
	primaryTable := newCollectionRunTable(1)
	primaryTable.SetSteal(bytes.Clone(docID), bytes.Clone(doc))
	primaryTable.Freeze()
	primaryPolicy, err := collectionRootStoragePolicyForDB(d, catalog.meta, primaryRoot)
	if err != nil {
		resetCollectionTables([]memtable.Table{primaryTable})
		t.Fatalf("primary policy: %v", err)
	}
	rootNames = append(rootNames, primaryRoot)
	baseRootIDs[primaryRoot] = catalog.rootID(primaryRoot)
	policies = append(policies, primaryPolicy)
	tables = append(tables, primaryTable)
	if err := appendSingleTextV2IndexMutationDeltas(
		snap,
		catalog,
		opts,
		def,
		[]textDocumentMutation{{documentID: docID, newDocument: doc, setNew: true}},
		&rootNames,
		baseRootIDs,
		&policies,
		&tables,
	); err != nil {
		resetCollectionTables(tables)
		t.Fatalf("appendSingleTextV2IndexMutationDeltas: %v", err)
	}
	defer resetCollectionTables(tables)
	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(rootNames))
	iters := make([]iterator.UnsafeIterator, 0, len(rootNames))
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()
	for i, rootName := range rootNames {
		it := tables[i].NewIterator(nil, nil)
		iters = append(iters, it)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      baseRootIDs[rootName],
			Iter:          it,
			StoragePolicy: policies[i],
		})
	}
	_, _, err = d.PublishOrderedRootDeltaGroupWithSystemBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		current := d.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		updates := make(map[string][]byte, len(rootNames))
		for i, rootName := range rootNames {
			updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
		}
		return buildSystemTargetIterator(current, updates)
	})
	if err != nil {
		t.Fatalf("publish buffered insert delta: %v", err)
	}
}

func TestTextV2MaintainBufferedUnflushedWriteNotMissed5828(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"initial sealed"}`)); err != nil {
		t.Fatalf("Insert d1: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "body"}},
	}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	preStatus := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "zero-debt before maintenance", 0)
	if preStatus.MicroPostingBlocks != 0 || preStatus.LiveDocuments != 1 {
		t.Fatalf("pre-maintenance status=%+v want 1 sealed doc and zero debt", preStatus)
	}

	// Simulate an unflushed buffered write-domain mutation landing during
	// flushCollectionWriteDomainsForSchemaMutation(). Because the O(1) zero-debt
	// check runs AFTER the write-domain flush and snapshot acquisition,
	// MaintainTextIndex must see the newly flushed micro blocks and NOT fast-skip.
	flushCalls := 0
	restoreFlushHook := setCollectionSchemaMutationFlushHookForTest(func() {
		flushCalls++
		if flushCalls == 1 {
			publishPendingTextV2InsertForFlushHookTest5828(t, d, "docs", "lexical", []byte("buffered"), []byte(`{"body":"unflushed pending micro"}`))
		}
	})
	defer restoreFlushHook()

	maint, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
		Policy: TextIndexMaintenancePolicy{MinMicroPostingBlocks: 1},
	})
	if err != nil {
		t.Fatalf("MaintainTextIndex: %v", err)
	}
	if flushCalls != 1 {
		t.Fatalf("flushCalls=%d want 1", flushCalls)
	}
	if maint.IndexesRewritten != 1 || len(maint.Indexes) != 1 || !maint.Indexes[0].Applied || maint.Indexes[0].Noop {
		t.Fatalf("MaintainTextIndex stats=%+v want flushed micro blocks rewritten without false fast-skip", maint)
	}
	postStatus := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "post-maintenance buffered write", 0)
	if postStatus.LiveDocuments != 2 || postStatus.MicroPostingBlocks != 0 {
		t.Fatalf("post-maintenance status=%+v want 2 live docs and 0 micro blocks", postStatus)
	}

	// Follow-up maintenance with no new buffered writes still invokes the write-domain
	// flush first, then takes the O(1) zero-debt fast-skip without scanning blocks.
	skipped, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
		MaxPostingBlocks: 1,
	})
	if err != nil {
		t.Fatalf("follow-up MaintainTextIndex: %v", err)
	}
	if flushCalls != 2 {
		t.Fatalf("follow-up flushCalls=%d want 2", flushCalls)
	}
	if skipped.BudgetExhausted || skipped.IndexesRewritten != 0 || len(skipped.Indexes) != 1 || !skipped.Indexes[0].Noop || skipped.Indexes[0].SkippedReason != TextIndexMaintenanceSkipReasonNoDebt {
		t.Fatalf("follow-up stats=%+v want clean zero-debt fast-skip", skipped)
	}
}

func TestTextV2MaintainZeroDebtFastSkipAndOverrides5828(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	const docsN = 40
	ids := make([][]byte, docsN)
	docs := make([][]byte, docsN)
	for i := 0; i < docsN; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		docs[i] = []byte(fmt.Sprintf(`{"body":"common unique%03d"}`, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "body"}},
	}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	zeroStatus := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "initial sealed", 0)
	if zeroStatus.SealedPostingBlocks < 20 {
		t.Fatalf("SealedPostingBlocks=%d want >= 20", zeroStatus.SealedPostingBlocks)
	}

	// 1. O(1) zero-debt fast-skip proof: MaxPostingBlocks=1 is far smaller than SealedPostingBlocks,
	// so any posting-block scan would exhaust budget.
	skipped, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
		MaxPostingBlocks:     1,
		RunStorageCompaction: true,
	})
	if err != nil {
		t.Fatalf("MaintainTextIndex fast-skip: %v", err)
	}
	if skipped.BudgetExhausted || skipped.IndexesRewritten != 0 || skipped.StorageCompacted || skipped.StorageCompaction != nil || len(skipped.Indexes) != 1 {
		t.Fatalf("fast-skip stats=%+v want clean zero-debt fast-skip without budget exhaustion or compaction", skipped)
	}
	idx := skipped.Indexes[0]
	if !idx.Noop || idx.Applied || idx.Triggered || idx.SkippedReason != TextIndexMaintenanceSkipReasonNoDebt {
		t.Fatalf("fast-skip index stats=%+v want Noop=true SkippedReason=no_debt", idx)
	}
	if idx.StorageBefore.Version != TextIndexVersionV2 ||
		idx.StorageBefore.Documents != zeroStatus.LiveDocuments ||
		idx.StorageBefore.V2LiveDocuments != zeroStatus.LiveDocuments ||
		idx.StorageBefore.V2DeletedDocs != 0 ||
		idx.StorageBefore.V2PostingBlocks != zeroStatus.SealedPostingBlocks ||
		idx.StorageBefore.V2SealedPostingBlocks != zeroStatus.SealedPostingBlocks ||
		idx.StorageBefore.V2RootGeneration != zeroStatus.RootGeneration ||
		idx.StorageBefore.V2RewriteMergeState != TextIndexRewriteMergeStateCompacted ||
		idx.StorageBefore.EncodedBytes != 0 {
		t.Fatalf("fast-skip StorageBefore=%+v want status-derived summary without full scan", idx.StorageBefore)
	}
	if idx.StorageAfter != (TextIndexStorageStats{}) {
		t.Fatalf("fast-skip StorageAfter=%+v want zero value", idx.StorageAfter)
	}

	// 2. Canceled context returns context.Canceled even on zero-debt index.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := col.MaintainTextIndex(canceledCtx, "lexical", TextIndexMaintenanceOptions{MaxPostingBlocks: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fast-skip err=%v want context.Canceled", err)
	}

	// 3. DryRun: true keeps full diagnostic inspection (EncodedBytes > 0).
	dry, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{DryRun: true})
	if err != nil {
		t.Fatalf("MaintainTextIndex DryRun: %v", err)
	}
	if len(dry.Indexes) != 1 || dry.Indexes[0].Applied || !dry.Indexes[0].DryRun || dry.Indexes[0].StorageBefore.EncodedBytes == 0 {
		t.Fatalf("DryRun stats=%+v want full diagnostic EncodedBytes > 0", dry)
	}

	// 4. Force: true bypasses fast-skip and rewrites.
	forced, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Force: true})
	if err != nil {
		t.Fatalf("MaintainTextIndex Force: %v", err)
	}
	if forced.IndexesRewritten != 1 || len(forced.Indexes) != 1 || !forced.Indexes[0].Applied || forced.Indexes[0].Rewrite.RootGenerationAfter != zeroStatus.RootGeneration+1 {
		t.Fatalf("Force stats=%+v want full rewrite applied", forced)
	}
	afterForce := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "after force", 0)

	// 5. Explicit TargetPostingsPerBlock: 16 bypasses fast-skip and splits the 40-doc "common" term into 3 blocks.
	repacked, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
		TargetPostingsPerBlock: 16,
		Policy:                 TextIndexMaintenancePolicy{MinRewritePostingBlocks: 1},
	})
	if err != nil {
		t.Fatalf("MaintainTextIndex TargetPostingsPerBlock=16: %v", err)
	}
	if repacked.IndexesRewritten != 1 || len(repacked.Indexes) != 1 || !repacked.Indexes[0].Applied {
		t.Fatalf("TargetPostingsPerBlock stats=%+v want rewrite applied", repacked)
	}
	afterRepack := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "after repack target=16", 0)
	if afterRepack.SealedPostingBlocks != afterForce.SealedPostingBlocks+2 {
		t.Fatalf("SealedPostingBlocks after repack=%d want %d", afterRepack.SealedPostingBlocks, afterForce.SealedPostingBlocks+2)
	}
}

func TestTextV2RewriteDisableTombstonePurgeCounters5828(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2")},
		[][]byte{[]byte(`{"body":"keep alpha"}`), []byte(`{"body":"drop beta gamma"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "body"}},
	}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if err := col.Delete([]byte("d2")); err != nil {
		t.Fatalf("Delete d2: %v", err)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "before DisableTombstonePurge rewrite", 3)

	rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{DisableTombstonePurge: true})
	if err != nil {
		t.Fatalf("RewriteTextIndex DisableTombstonePurge: %v", err)
	}
	if rw.StalePostingsPurged != 3 || rw.TombstoneDocIDEntriesPurged != 0 {
		t.Fatalf("rewrite stats=%+v want 3 stale postings purged and tombstones retained", rw)
	}
	st := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "after DisableTombstonePurge rewrite", 0)
	if st.DeletedDocuments != 1 || st.MicroPostingBlocks != 0 || st.DeltaPostingBlocks != 0 || st.StalePostings != 0 || st.PostingBlocks != st.SealedPostingBlocks {
		t.Fatalf("status=%+v want DeletedDocuments=1 StalePostings=0 Micro=0", st)
	}

	// Follow-up rewrite with tombstone purge enabled must pass fail-closed cross-check and clear DeletedDocuments.
	rw2, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
	if err != nil {
		t.Fatalf("RewriteTextIndex purge tombstones: %v", err)
	}
	if rw2.StalePostingsPurged != 0 || rw2.TombstoneDocIDEntriesPurged != 1 {
		t.Fatalf("second rewrite stats=%+v want 0 stale postings and 1 tombstone purged", rw2)
	}
	st2 := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "after tombstone purge rewrite", 0)
	if st2.DeletedDocuments != 0 || st2.StalePostings != 0 {
		t.Fatalf("status after second rewrite=%+v want zero debt", st2)
	}
}

func TestTextV2StatusCounterCorruptionFailClosed5828(t *testing.T) {
	newFixture := func(t *testing.T) (*backenddb.DB, *Collection, textV2IndexStatusValue) {
		t.Helper()
		d := openTextV2TestDB(t, t.TempDir(), false)
		t.Cleanup(func() { _ = d.Close() })
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
			t.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection("docs")
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		if _, err := col.InsertBatch(
			[][]byte{[]byte("d1"), []byte("d2")},
			[][]byte{[]byte(`{"body":"alpha beta"}`), []byte(`{"body":"beta gamma"}`)},
		); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}
		if _, _, err := col.CreateTextIndex(TextIndexDefinition{
			Name:    "lexical",
			Version: TextIndexVersionV2,
			Fields:  []TextIndexField{{Field: "body"}},
		}); err != nil {
			t.Fatalf("CreateTextIndex: %v", err)
		}
		var status textV2IndexStatusValue
		withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
			status = textV2StatusRootValue(t, snap, catalog, "docs", "lexical")
		})
		return d, col, status
	}

	genRoot := collectionTextV2GenerationsRootName("docs", "lexical")

	t.Run("corrupt sealed posting blocks", func(t *testing.T) {
		d, col, status := newFixture(t)
		status.SealedPostingBlocks++
		corruptTextRootValue(t, d, "docs", genRoot, encodeTextV2StatusKey(), encodeTextV2IndexStatusValue(status))
		if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("TextIndexStorageStats err=%v want ErrTextIndexStorageCorrupt", err)
		}
		if _, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{}); !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("RewriteTextIndex err=%v want ErrTextIndexStorageCorrupt", err)
		}
	})

	t.Run("corrupt micro posting blocks", func(t *testing.T) {
		d, col, status := newFixture(t)
		status.MicroPostingBlocks++
		corruptTextRootValue(t, d, "docs", genRoot, encodeTextV2StatusKey(), encodeTextV2IndexStatusValue(status))
		if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("TextIndexStorageStats err=%v want ErrTextIndexStorageCorrupt", err)
		}
		if _, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{}); !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("RewriteTextIndex err=%v want ErrTextIndexStorageCorrupt", err)
		}
	})

	t.Run("corrupt stale postings", func(t *testing.T) {
		d, col, status := newFixture(t)
		status.StalePostings++
		corruptTextRootValue(t, d, "docs", genRoot, encodeTextV2StatusKey(), encodeTextV2IndexStatusValue(status))
		if _, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{}); !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("RewriteTextIndex err=%v want ErrTextIndexStorageCorrupt", err)
		}
	})

	t.Run("corrupt status bytes", func(t *testing.T) {
		d, col, _ := newFixture(t)
		corruptTextRootValue(t, d, "docs", genRoot, encodeTextV2StatusKey(), []byte{textV2StatusValueVersion, 1})
		if _, err := col.TextIndexStatus("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("TextIndexStatus err=%v want ErrTextIndexStorageCorrupt", err)
		}
	})

	t.Run("missing status record", func(t *testing.T) {
		d, col, _ := newFixture(t)
		deleteTextRootKey5828(t, d, "docs", genRoot, encodeTextV2StatusKey())
		if _, err := col.TextIndexStatus("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("TextIndexStatus missing err=%v want ErrTextIndexStorageCorrupt", err)
		}
	})
}

func TestTextV2StatusNonPrimaryRolloutAndV1Unaffected5828(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"alpha beta"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:    "lexical_v1",
		Version: TextIndexVersionV1,
		Fields:  []TextIndexField{{Field: "body"}},
	}); err != nil {
		t.Fatalf("CreateTextIndex v1: %v", err)
	}

	v1Status, err := col.TextIndexStatus("lexical_v1")
	if err != nil {
		t.Fatalf("TextIndexStatus v1: %v", err)
	}
	if !v1Status.Ready || v1Status.Version != TextIndexVersionV1 || v1Status.FailClosed {
		t.Fatalf("v1 status=%+v want ready v1", v1Status)
	}
	if v1Status.LiveDocuments != 0 || v1Status.DeletedDocuments != 0 || v1Status.PostingBlocks != 0 ||
		v1Status.SealedPostingBlocks != 0 || v1Status.MicroPostingBlocks != 0 || v1Status.DeltaPostingBlocks != 0 ||
		v1Status.StalePostings != 0 || v1Status.RootGeneration != 0 {
		t.Fatalf("v1 status=%+v want all v2 counter fields zero", v1Status)
	}

	for _, mode := range []TextIndexRolloutMode{TextIndexRolloutShadow, TextIndexRolloutDualWrite, TextIndexRolloutDisabled} {
		st := textIndexStatusForDefinition("docs", TextIndexDefinition{
			Name:    "lexical_nonprimary",
			Version: TextIndexVersionV2,
			Rollout: mode,
			Fields:  []TextIndexField{{Field: "body"}},
		})
		if st.Ready || !st.FailClosed || st.FailClosedReason != "text_index_rollout_mode_unavailable" {
			t.Fatalf("mode %q status=%+v want FailClosed non-ready", mode, st)
		}
		if st.LiveDocuments != 0 || st.DeletedDocuments != 0 || st.PostingBlocks != 0 ||
			st.SealedPostingBlocks != 0 || st.MicroPostingBlocks != 0 || st.DeltaPostingBlocks != 0 ||
			st.StalePostings != 0 || st.RootGeneration != 0 {
			t.Fatalf("mode %q status=%+v want zero counters", mode, st)
		}
	}
}

func TestTextV2StatusCountersNativeCOWAndReopen5828(t *testing.T) {
	dir := t.TempDir()
	var manager *CollectionManager
	var closeOwner, checkpoint func() error
	var budget nativeHNSWAdmissionBudget

	closeAll := func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			manager = nil
		}
		if closeOwner != nil {
			if err := closeOwner(); err != nil {
				t.Error(err)
			}
			closeOwner = nil
		}
		if budget != nil {
			if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
				t.Errorf("full Close retained %+v", s)
			}
			budget = nil
		}
	}
	t.Cleanup(closeAll)

	open := func() {
		opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
		opts.MemtableMode = "cow_btree"
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1
		d, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		closeOwner, checkpoint = d.Close, d.Checkpoint
		b, err := d.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = NewCollectionManager(b)
		cut, err := b.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		budget = cut.NativeAllocator().(nativeHNSWAdmissionBudget)
		if err = cut.Close(); err != nil {
			t.Fatal(err)
		}
	}

	open()
	if _, err := manager.CreateCollection(&CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{
			{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title"}, {Field: "body"}}},
			{Name: "lexical_v1", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	// Empty V2 index on native COW.
	emptyStatus := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "native empty", 0)
	if emptyStatus.LiveDocuments != 0 || emptyStatus.PostingBlocks != 0 || emptyStatus.RootGeneration != 1 {
		t.Fatalf("native empty status=%+v", emptyStatus)
	}

	// V1 on native COW remains unaffected with zero V2 counters.
	v1Status, err := col.TextIndexStatus("lexical_v1")
	if err != nil {
		t.Fatalf("native v1 TextIndexStatus: %v", err)
	}
	if !v1Status.Ready || v1Status.Version != TextIndexVersionV1 || v1Status.PostingBlocks != 0 || v1Status.RootGeneration != 0 {
		t.Fatalf("native v1 status=%+v", v1Status)
	}

	// Insert multiple docs to create multiple posting blocks.
	ids := make([][]byte, 12)
	docs := make([][]byte, 12)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("d%02d", i))
		docs[i] = []byte(fmt.Sprintf(`{"title":"shared tok%02d","body":"shared body%02d"}`, i, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "native insert batch", 0)

	// Cross-field update on d00 ({shared, tok00, body00} = 3 unique terms).
	if _, _, err := col.Update([]byte("d00"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"title":"newtitle","body":"newbody"}`), true, nil
	}); err != nil {
		t.Fatalf("Update d00: %v", err)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "native cross-field update", 3)

	// Update d01 to empty text ({shared, tok01, body01} = 3 unique terms).
	if _, _, err := col.Update([]byte("d01"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"title":"","body":""}`), true, nil
	}); err != nil {
		t.Fatalf("Update d01 to empty: %v", err)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "native update-to-empty", 6)

	// Delete d02 ({shared, tok02, body02} = 3 unique terms).
	if err := col.Delete([]byte("d02")); err != nil {
		t.Fatalf("Delete d02: %v", err)
	}
	assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "native delete", 9)

	// Native COW rewrite clears debt and updates status.
	rw, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
	if err != nil {
		t.Fatalf("native RewriteTextIndex: %v", err)
	}
	if rw.StalePostingsPurged != 9 {
		t.Fatalf("native rewrite StalePostingsPurged=%d want 9", rw.StalePostingsPurged)
	}
	afterRewrite := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "native post-rewrite", 0)
	if afterRewrite.SealedPostingBlocks < 10 {
		t.Fatalf("native SealedPostingBlocks=%d want >= 10", afterRewrite.SealedPostingBlocks)
	}

	// Native COW O(1) zero-debt fast-skip proof with MaxPostingBlocks=1.
	skipped, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{MaxPostingBlocks: 1})
	if err != nil {
		t.Fatalf("native MaintainTextIndex fast-skip: %v", err)
	}
	if skipped.BudgetExhausted || skipped.IndexesRewritten != 0 || len(skipped.Indexes) != 1 || !skipped.Indexes[0].Noop || skipped.Indexes[0].SkippedReason != TextIndexMaintenanceSkipReasonNoDebt {
		t.Fatalf("native fast-skip stats=%+v want clean no_debt skip", skipped)
	}

	// Native COW admission-capacity failure on TextIndexStatus must fail cleanly with ErrCOWCapacity and zero lease leak.
	if err := func() error {
		r, e := col.nativeJSON.Capture(col.name)
		if e != nil {
			return e
		}
		defer func() { _ = r.Close() }()
		blocker, e := r.capture.NativeAllocator().AdmitNativeBytes(0)
		if e != nil {
			return e
		}
		defer blocker.Close()
		available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
		if e = blocker.Resize(available); e != nil {
			return e
		}
		if _, e = col.TextIndexStatus("lexical"); !errors.Is(e, memtable.ErrCOWCapacity) {
			t.Fatalf("exhausted native TextIndexStatus err=%v want ErrCOWCapacity", e)
		}
		return nil
	}(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint, close, reopen, verify status persistence on native COW.
	if err := checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	closeAll()
	open()
	col, err = manager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("reopen OpenCollection: %v", err)
	}
	reopenedStatus := assertTextV2StatusMatchesStorageStats5828(t, col, "lexical", "native reopen", 0)
	if reopenedStatus.LiveDocuments != afterRewrite.LiveDocuments ||
		reopenedStatus.SealedPostingBlocks != afterRewrite.SealedPostingBlocks ||
		reopenedStatus.RootGeneration != afterRewrite.RootGeneration {
		t.Fatalf("native reopened status=%+v want %+v", reopenedStatus, afterRewrite)
	}
}

var textV2StatusCountersBenchSink5828 uint64

func buildTextV2SealedBenchCollection5828(b *testing.B, minPostingBlocks uint64) (*backenddb.DB, *Collection, TextIndexStatus) {
	b.Helper()
	d := openTextV2TestDB(b, b.TempDir(), false)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		_ = d.Close()
		b.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		b.Fatalf("OpenCollection: %v", err)
	}

	const termsPerDoc = 10
	docsN := int((minPostingBlocks + termsPerDoc - 1) / termsPerDoc)
	ids := make([][]byte, docsN)
	docs := make([][]byte, docsN)
	var sb strings.Builder
	for i := 0; i < docsN; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
		sb.Reset()
		sb.WriteString(`{"body":"`)
		for j := 0; j < termsPerDoc; j++ {
			if j > 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(&sb, "term%05d", i*termsPerDoc+j)
		}
		sb.WriteString(`"}`)
		docs[i] = []byte(sb.String())
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		_ = d.Close()
		b.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "body"}},
	}); err != nil {
		_ = d.Close()
		b.Fatalf("CreateTextIndex: %v", err)
	}
	status, err := col.TextIndexStatus("lexical")
	if err != nil {
		_ = d.Close()
		b.Fatalf("TextIndexStatus setup: %v", err)
	}
	if status.SealedPostingBlocks < minPostingBlocks || status.MicroPostingBlocks != 0 || status.DeltaPostingBlocks != 0 || status.DeletedDocuments != 0 || status.StalePostings != 0 {
		_ = d.Close()
		b.Fatalf("bench fixture status=%+v want >= %d sealed posting blocks and zero debt", status, minPostingBlocks)
	}
	return d, col, status
}

func BenchmarkTextIndexStatusO1(b *testing.B) {
	d, col, setupStatus := buildTextV2SealedBenchCollection5828(b, 2500)
	defer func() { _ = d.Close() }()

	b.ReportAllocs()
	b.ReportMetric(float64(setupStatus.SealedPostingBlocks), "posting_blocks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status, err := col.TextIndexStatus("lexical")
		if err != nil {
			b.Fatalf("TextIndexStatus: %v", err)
		}
		textV2StatusCountersBenchSink5828 += status.SealedPostingBlocks + status.LiveDocuments + status.RootGeneration
	}
}

func BenchmarkTextIndexStorageStatsFullScan(b *testing.B) {
	d, col, setupStatus := buildTextV2SealedBenchCollection5828(b, 2500)
	defer func() { _ = d.Close() }()

	b.ReportAllocs()
	b.ReportMetric(float64(setupStatus.SealedPostingBlocks), "posting_blocks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats, err := col.TextIndexStorageStats("lexical")
		if err != nil {
			b.Fatalf("TextIndexStorageStats: %v", err)
		}
		textV2StatusCountersBenchSink5828 += stats.V2SealedPostingBlocks + stats.V2LiveDocuments + stats.V2RootGeneration
	}
}

func BenchmarkMaintainTextIndexZeroDebtSkip(b *testing.B) {
	d, col, setupStatus := buildTextV2SealedBenchCollection5828(b, 2500)
	defer func() { _ = d.Close() }()

	ctx := context.Background()
	opts := TextIndexMaintenanceOptions{}
	b.ReportAllocs()
	b.ReportMetric(float64(setupStatus.SealedPostingBlocks), "posting_blocks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats, err := col.MaintainTextIndex(ctx, "lexical", opts)
		if err != nil {
			b.Fatalf("MaintainTextIndex: %v", err)
		}
		if stats.IndexesRewritten != 0 || len(stats.Indexes) != 1 || !stats.Indexes[0].Noop || stats.Indexes[0].SkippedReason != TextIndexMaintenanceSkipReasonNoDebt {
			b.Fatalf("unexpected maintenance result: %+v", stats)
		}
		textV2StatusCountersBenchSink5828 += stats.Indexes[0].StorageBefore.V2SealedPostingBlocks
	}
}
