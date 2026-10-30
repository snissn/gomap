package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Preserve the real capture's optional physical readers. The inverse block is
// deliberately large enough to be pointer-valued: this counts DATA payload
// resolutions, not a format record sharing its physical leaf.
type textV2ForwardCountedCapture struct {
	backenddb.NativeSemanticCapture
	inverse      page.ValuePtr
	inverseReads int
}

func (c *textV2ForwardCountedCapture) NativeLeafPage(p page.LeafLogPtr, dst []byte) ([]byte, error) {
	return c.NativeSemanticCapture.(backenddb.NativePhysicalLeafReader).NativeLeafPage(p, dst)
}
func (c *textV2ForwardCountedCapture) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	return c.NativeSemanticCapture.(backenddb.NativePhysicalReader).NativeEntryAtRoot(root, key, copyValue)
}
func (c *textV2ForwardCountedCapture) NativeValueAtPointer(p page.ValuePtr, admit func(uint64) error) ([]byte, error) {
	if p == c.inverse {
		c.inverseReads++
	}
	return c.NativeSemanticCapture.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(p, admit)
}

// All data and identities originate in public mutations and a real capture.
// This test uses the existing captured executor only to count the exact inverse
// DATA value and keep the selected cut fixed while the public writer advances.
func TestNativeTextV2ForwardIdentityCapturedParity(t *testing.T) {
	for _, longIDs := range []bool{true, false} {
		name := "sealed_short_ids"
		if longIDs {
			name = "long_pointer_ids"
		}
		t.Run(name, func(t *testing.T) { runTextV2ForwardIdentityCapturedParity(t, longIDs) })
	}
}

func runTextV2ForwardIdentityCapturedParity(t *testing.T, longIDs bool) {
	owner, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	var ids [][]byte
	var documents [][]byte
	all, sparse := make(hybridScalarAllowSet), make(hybridScalarAllowSet)
	// Long IDs exercise the real pointer/cursor boundary. Short IDs separately
	// exercise positive rewrite and sealed serving under the same finite budget.
	for i := 0; i < 32; i++ {
		// Reverse ordinal versus lexical order, with identical BM25 scores.
		id := fmt.Sprintf("%03d-", 63-i)
		if longIDs {
			id += strings.Repeat("x", 256)
		}
		content := "refund policy"
		if i%7 == 0 {
			content = "other policy"
		}
		ids = append(ids, []byte(id))
		documents = append(documents, []byte(fmt.Sprintf(`{"content":%q,"tenant":"one"}`, content)))
		all[id] = struct{}{}
		if i%3 == 0 {
			sparse[id] = struct{}{}
		}
	}
	if _, err := c.InsertBatch(ids, documents); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	old, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	budget := old.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	counted := &textV2ForwardCountedCapture{NativeSemanticCapture: old.capture}
	old.capture = counted
	domain, err := old.borrowedNamedDomain(strings.TrimPrefix(collectionTextV2DocMapRootName("docs", "content"), "docs/"))
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := old.physicalScratch()
	if err != nil {
		t.Fatal(err)
	}
	inverse, err := domain.getEntry(encodeTextV2BlockKey(1), scratch, old.readLeaf)
	inverseIsPointer := inverse.Flags&node.FlagPointer != 0
	if err != nil || (longIDs && !inverseIsPointer) {
		t.Fatalf("inverse DATA must exercise pointer reads: flags=%v err=%v", inverse.Flags, err)
	}
	counted.inverse = inverse.ValuePtr
	run := func(read *NativeJSONRead, allow hybridScalarAllowSet, opts TextSearchOptions) TextSearchResponse {
		t.Helper()
		oracleOpts := opts
		oracleOpts.TopK = 64
		oracleOpts.textV2AllowedDocumentIDs = nil
		oracleOpts.textV2DisableBlockMax = true
		oracle, err := c.searchNativeText(oracleOpts, textSearchResultScoreOnly, read, TextSearchResponse{})
		if err != nil {
			t.Fatal("exact oracle", err)
		}
		if read == old && inverseIsPointer && counted.inverseReads == 0 {
			t.Fatal("unfiltered control did not resolve inverse DATA")
		}
		opts.textV2AllowedDocumentIDs = allow
		counted.inverseReads = 0
		got, err := c.searchNativeText(opts, textSearchResultScoreOnly, read, TextSearchResponse{})
		if err != nil {
			// Failure-only discriminator: the largest sorted ID is the failing
			// lookup in the original run. Distinguish stored identity from the
			// scoped ordered point cursor without changing the query path.
			direct, directErr := read.namedDomainGet(strings.TrimPrefix(collectionTextV2DocIDRootName("docs", "content"), "docs/"), encodeTextV2DocIDKey(ids[0]))
			t.Fatalf("filtered %v; direct largest forward ID bytes=%d err=%v", err, len(direct), directErr)
		}
		want := textV2ScalarPostFilterReference2874(oracle, allow, opts.TopK)
		assertTextV2ScalarResults2874(t, got.Results, want)
		if read == old && counted.inverseReads != 0 {
			t.Fatalf("known forward identities read inverse DATA %d times", counted.inverseReads)
		}
		if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.TextStateLookups != 0 {
			t.Fatalf("unexpected materialization %+v", got.Stats)
		}
		return got
	}
	shapes := []TextSearchOptions{
		{IndexName: "content", Query: "refund", TopK: 1, CandidateLimit: 64, MaxPostingsScanned: 1024},
		{IndexName: "content", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 5, CandidateLimit: 64, MaxPostingsScanned: 1024},
		{IndexName: "content", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 5, CandidateLimit: 64, MaxPostingsScanned: 1024},
		{IndexName: "content", Query: "refund AND policy", Operator: TextSearchOperatorAND, Explain: true, TopK: 5, CandidateLimit: 64, MaxPostingsScanned: 1024},
		{IndexName: "content", Phrase: &TextSearchPhraseQuery{Query: "refund policy"}, TopK: 5, CandidateLimit: 64, MaxPostingsScanned: 1024},
		{IndexName: "content", Query: "refund", TopK: 5, CandidateLimit: 64, MaxPostingsScanned: 1024, textV2DisableBlockMax: true},
	}
	for _, allow := range []hybridScalarAllowSet{sparse, all} {
		for _, opts := range shapes {
			run(old, allow, opts)
		}
	}
	// Native nonoverlapping micro blocks support block-max directly.
	micro := run(old, all, shapes[0])
	if micro.Stats.TextBlockMaxFallbacks != 0 {
		t.Fatal("nonoverlapping native micro blocks unexpectedly restarted")
	}
	saved := run(old, sparse, shapes[0])
	savedID := bytes.Clone(saved.Results[0].DocumentID)
	if _, _, err := c.Update(ids[1], func([]byte) ([]byte, bool, error) {
		return []byte(`{"content":"other policy","tenant":"one"}`), true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ids[3]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert(ids[3], []byte(`{"content":"refund policy","tenant":"one"}`)); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ids[6]); err != nil {
		t.Fatal(err)
	}
	currentAll, currentSparse := make(hybridScalarAllowSet), make(hybridScalarAllowSet)
	for id := range all {
		if id != string(ids[6]) {
			currentAll[id] = struct{}{}
		}
	}
	for id := range sparse {
		if id != string(ids[6]) {
			currentSparse[id] = struct{}{}
		}
	}
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, opts := range shapes {
		run(old, sparse, opts)
		run(current, currentSparse, opts)
		run(current, currentAll, opts)
	}
	// The update retains policy on its existing ordinal, overlapping that
	// term's old block. Refund was removed and has no replacement overlap.
	// The exact restart includes its second map/output admission.
	fallbackOpts := shapes[0]
	fallbackOpts.Query = "policy"
	fallback := run(current, currentAll, fallbackOpts)
	if fallback.Stats.TextBlockMaxFallbacks == 0 {
		t.Fatal("overlapping mutation did not exercise exact restart")
	}
	// Missing/deleted selected identities fail before posting traversal.
	missing := shapes[0]
	missing.textV2AllowedDocumentIDs = hybridScalarAllowSet{string(ids[6]): {}}
	bad, err := c.searchNativeText(missing, textSearchResultScoreOnly, current, TextSearchResponse{})
	if !errors.Is(err, ErrTextIndexStorageCorrupt) || len(bad.Results) != 0 || bad.Stats.TextPostingsScanned != 0 {
		t.Fatalf("deleted forward mapping response=%+v err=%v", bad, err)
	}
	var sealed *NativeJSONRead
	if !longIDs {
		stats, err := c.RewriteTextIndex("content", TextIndexRewriteOptions{TargetPostingsPerBlock: 8})
		if err != nil {
			t.Fatal(err)
		}
		if stats.Noop || stats.TermsRewritten == 0 || stats.PostingBlocksWritten == 0 || stats.RootGenerationAfter <= stats.RootGenerationBefore {
			t.Fatalf("positive rewrite did not publish sealed records: %+v", stats)
		}
		storage, err := c.TextIndexStorageStats("content")
		if err != nil {
			t.Fatal(err)
		}
		if storage.V2SealedPostingBlocks == 0 || storage.V2MicroPostingBlocks != 0 || storage.V2DeletedDocs != 0 {
			t.Fatalf("rewrite did not seal/purge: %+v", storage)
		}
		sealed, err = c.nativeJSON.Capture(c.name)
		if err != nil {
			t.Fatal(err)
		}
		defer sealed.Close()
		for _, allow := range []hybridScalarAllowSet{currentSparse, currentAll} {
			for _, opts := range shapes {
				run(sealed, allow, opts)
			}
		}
	}
	// Returned IDs have no alias into the query-local scalar map, capture, or
	// later result append. Public output survives all owner closures.
	if sealed != nil {
		if err := sealed.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved.Results[0].DocumentID, savedID) {
		t.Fatal("result identity changed after Close")
	}
	appended := append(saved.Results[0].DocumentID, '!')
	if len(appended) != len(savedID)+1 || !bytes.Equal(appended[:len(savedID)], savedID) {
		t.Fatal("result append lost identity")
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("full Close retained %+v", stats)
	}
}

// Internal publication injects corruption into a real public collection; it is
// not a supported application mutation or a new recovery/format contract.
func textV2ForwardPublishRecord(t *testing.T, c *Collection, root string, key, value []byte) {
	t.Helper()
	tx, err := c.nativeJSON.db.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	base := tx.State().(*nativeEngineVersion)
	collection, ok := base.names.Get(c.name)
	if !ok {
		t.Fatal("missing collection")
	}
	name := strings.TrimPrefix(root, c.name+"/")
	domain, ok := collection.domains.Get(name)
	if !ok {
		t.Fatal("missing domain", name)
	}
	next, err := prepareNativeNamedDomains(base, []nativeNamedCollectionChange{{collection: collection, sourceGeneration: collection.documentGeneration, domains: []nativeNamedDomainChange{{name: name, expectedInterpretation: domain.interpretation, entries: []memtable.COWMutation{{Key: key, Value: value, Flags: node.FlagInline}}}}}})
	if err != nil {
		if next != nil {
			err = errors.Join(err, finishPrivateNative(next.Release()))
		}
		t.Fatal(err)
	}
	pub := &backenddb.NativePreparedPublication{Basis: base, State: next, StartSequence: tx.LogicalSequence()}
	result, err := tx.CommitNative(pub, nil, false)
	if !result.Consumed {
		err = errors.Join(err, cancelPrivateNativePublication(pub))
	}
	if err != nil || !result.LogicalInstalled {
		t.Fatalf("fault publication %+v err=%v", result, err)
	}
}

func TestNativeTextV2ForwardIdentityCorruptionContract(t *testing.T) {
	roots := collectionTextV2RootNames("docs", "content")
	for _, root := range roots {
		t.Run("format_"+root, func(t *testing.T) {
			_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
			if _, err := c.Insert([]byte("a"), []byte(`{"content":"refund policy"}`)); err != nil {
				t.Fatal(err)
			}
			textV2ForwardPublishRecord(t, c, root, encodeTextV2FormatKey(), []byte{99})
			got, err := c.searchText(TextSearchOptions{IndexName: "content", Query: "refund", TopK: 1, textV2AllowedDocumentIDs: hybridScalarAllowSet{"a": {}}}, textSearchResultScoreOnly)
			if !errors.Is(err, ErrTextIndexStorageCorrupt) || len(got.Results) != 0 {
				t.Fatalf("filtered format bypass root=%s response=%+v err=%v", root, got, err)
			}
		})
	}
	for _, kind := range []string{"inverse", "norm_generation", "norm_tombstone", "duplicate_ordinal"} {
		t.Run(kind, func(t *testing.T) {
			_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
			if _, err := c.InsertBatch([][]byte{[]byte("z"), []byte("a")}, [][]byte{[]byte(`{"content":"refund policy"}`), []byte(`{"content":"refund policy"}`)}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.RewriteTextIndex("content", TextIndexRewriteOptions{TargetPostingsPerBlock: 1}); err != nil {
				t.Fatal(err)
			}
			read, err := c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			var charged uint64
			lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := newNativeTextCatalog(read, func(n uint64) error {
				if n > ^uint64(0)-charged {
					return memtable.ErrCOWCapacity
				}
				if err := lease.Resize(charged + n); err != nil {
					return err
				}
				charged += n
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, err := newTextV2SearchContext(read.capture.Snapshot(), catalog, read.meta.TextIndexes[0], []string{"refund"})
			if err != nil {
				t.Fatal(err)
			}
			doc, ok, err := readTextV2DocIDAtRoot(read.capture.Snapshot(), catalog, ctx.docIDRootName, []byte("z"))
			if err != nil || !ok {
				t.Fatal(doc, ok, err)
			}
			root, key, value := ctx.docMapRootName, encodeTextV2BlockKey(1), []byte{99}
			switch kind {
			case "norm_generation", "norm_tombstone":
				raw, found, e := collectionGetAppendAtCatalogRoot(read.capture.Snapshot(), catalog, ctx.normBlocksRootName, key, nil)
				if e != nil || !found {
					t.Fatal(e)
				}
				block, e := decodeTextV2NormBlockValue(raw)
				if e != nil {
					t.Fatal(e)
				}
				for i := range block.Entries {
					if block.Entries[i].Ordinal == doc.Ordinal {
						if kind == "norm_generation" {
							block.Entries[i].Generation++
						} else {
							block.Entries[i].Flags = textV2DocFlagTombstone
						}
					}
				}
				root, value = ctx.normBlocksRootName, encodeTextV2NormBlockValue(block)
			case "duplicate_ordinal":
				root, key, value = ctx.docIDRootName, encodeTextV2DocIDKey([]byte("a")), encodeTextV2DocIDValue(doc)
			}
			lease.Close()
			if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			textV2ForwardPublishRecord(t, c, root, key, value)
			for _, disable := range []bool{false, true} {
				opts := TextSearchOptions{IndexName: "content", Query: "refund", TopK: 1, textV2DisableBlockMax: disable, textV2AllowedDocumentIDs: hybridScalarAllowSet{"z": {}, "a": {}}}
				got, err := c.searchText(opts, textSearchResultScoreOnly)
				if kind == "inverse" {
					if err != nil || len(got.Results) != 1 || string(got.Results[0].DocumentID) != "a" {
						t.Fatalf("unconsumed inverse changed filtered response %+v err=%v", got, err)
					}
				} else if !errors.Is(err, ErrTextIndexStorageCorrupt) || len(got.Results) != 0 {
					t.Fatalf("consumed forward/norm corruption response=%+v err=%v", got, err)
				}
			}
			if kind == "inverse" {
				if _, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: "refund", TopK: 2}); !errors.Is(err, ErrTextIndexStorageCorrupt) {
					t.Fatalf("unfiltered inverse corruption %v", err)
				}
				if _, err := c.TextIndexStorageStats("content"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
					t.Fatalf("inspector inverse corruption %v", err)
				}
			}
		})
	}
}

func TestNativeTextV2ForwardIdentityAdmission(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	var pins []backenddb.NativeSemanticCapture
	defer func() {
		for i := len(pins) - 1; i >= 0; i-- {
			if err := pins[i].Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	pin := func() {
		t.Helper()
		cut, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		pins = append(pins, cut)
	}
	// Match the admission-cleanup fixture: retain every setup cut so earlier
	// retirement cannot change global ownership during the refusal check.
	pin()
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	meta := nativeTextPublicMeta()
	meta.Options.DocumentFormat = DocumentFormatJSON
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	pin()
	c, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	// The callback refuses the first query allocation; a page-sized ID would
	// fail publication before this admission control is reached.
	id := strings.Repeat("long", 16)
	if _, err := c.Insert([]byte(id), []byte(`{"content":"refund policy"}`)); err != nil {
		t.Fatal(err)
	}
	pin()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	var observed struct {
		calls, refusals, resizeAttempts, accepted int
		charged                                   uint64
	}
	refuse := false
	catalog, err := newNativeTextCatalog(read, func(n uint64) error {
		observed.calls++
		if refuse {
			observed.refusals++
			return memtable.ErrCOWCapacity
		}
		if n > ^uint64(0)-observed.charged {
			return memtable.ErrCOWCapacity
		}
		observed.resizeAttempts++
		if err := lease.Resize(observed.charged + n); err != nil {
			return err
		}
		observed.charged += n
		observed.accepted++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := newTextV2SearchContext(read.capture.Snapshot(), catalog, read.meta.TextIndexes[0], []string{"refund"})
	if err != nil {
		t.Fatal(err)
	}
	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	beforeBudget := budget.Stats()
	before := observed
	sequence := read.capture.LogicalSequence()
	refuse = true
	set, err := newTextV2SearchOrdinalAllowSet(read.capture.Snapshot(), catalog, ctx, legacyScalarIDs(hybridScalarAllowSet{id: {}}))
	if !errors.Is(err, memtable.ErrCOWCapacity) || set != nil {
		t.Fatal("unadmitted forward map", set, err)
	}
	// One refused preflight must return before any resize or accepted charge.
	// All other operation-owned counters and the captured sequence stay exact.
	want := before
	want.calls++
	want.refusals++
	if observed != want || read.capture.LogicalSequence() != sequence {
		t.Fatalf("refusal changed operation ownership before=%+v after=%+v want=%+v sequence=%d want=%d", before, observed, want, read.capture.LogicalSequence(), sequence)
	}
	afterBudget := budget.Stats()
	if afterBudget.ReservedBytes != beforeBudget.ReservedBytes || afterBudget.ExternalLeases != beforeBudget.ExternalLeases {
		t.Fatalf("refusal changed pinned ownership before=%+v after=%+v", beforeBudget, afterBudget)
	}
	refuse = false
	if set, err = newTextV2SearchOrdinalAllowSet(read.capture.Snapshot(), catalog, ctx, legacyScalarIDs(hybridScalarAllowSet{id: {}})); err != nil || set.size() != 1 {
		t.Fatal("reuse after refusal", set, err)
	}
	if observed.resizeAttempts <= before.resizeAttempts || observed.accepted <= before.accepted || observed.charged <= before.charged || observed.refusals != want.refusals {
		t.Fatalf("retry did not admit through the original operation lease: before=%+v after=%+v", before, observed)
	}
}
