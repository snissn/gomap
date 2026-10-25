package collections

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func nativeTextPublicMeta(versions ...TextIndexVersion) *CollectionMeta {
	version := TextIndexVersionV2
	if len(versions) != 0 {
		version = versions[0]
	}
	return &CollectionMeta{Name: "docs",
		Indexes:       []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}},
		VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}},
		TextIndexes:   []TextIndexDefinition{{Name: "content", Version: version, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple, StorePositions: true}},
	}
}

func openNativeTextPublic(t *testing.T, dir string, profile treedb.Profile, create bool, versions ...TextIndexVersion) (*treedb.DB, *CollectionManager, *Collection) {
	t.Helper()
	opts := treedb.OptionsFor(profile, dir)
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
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	if create {
		if _, err := manager.CreateCollection(nativeTextPublicMeta(versions...)); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	return owner, manager, collection
}

func requireNativeTextIDs(t *testing.T, c *Collection, query string, want ...string) {
	t.Helper()
	response, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: query, TopK: 8, IncludeDocuments: true})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(response.Results))
	for _, row := range response.Results {
		ids = append(ids, string(row.DocumentID))
	}
	if want == nil {
		want = []string{}
	}
	sort.Strings(ids)
	sort.Strings(want)
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("query %q ids=%v want=%v", query, ids, want)
	}
}

// Both canonical singleton CRUD and a real composite frame regenerate the
// maintained text image on process restart. This is not a power-loss test.
func TestNativeTextPublicCrashReplay(t *testing.T) {
	if dir := os.Getenv("GOMAP_NATIVE_TEXT_CRASH_DIR"); dir != "" {
		_, _, c := openNativeTextPublic(t, dir, treedb.Profile(os.Getenv("GOMAP_NATIVE_TEXT_CRASH_PROFILE")), true, TextIndexVersion(os.Getenv("GOMAP_NATIVE_TEXT_CRASH_VERSION")))
		doc := func(text string) []byte {
			return []byte(fmt.Sprintf(`{"tenant":"a","content":%q,"embedding":[1,0]}`, text))
		}
		if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{doc("refund policy"), doc("shipping policy")}); err != nil {
			t.Fatal(err)
		}
		update := UpdateBatchItem{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) { return doc("returns policy"), true, nil }}
		if os.Getenv("GOMAP_NATIVE_TEXT_CRASH_MODE") == "composite" {
			tx, err := c.nativeJSON.Begin(c.name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.UpdateBatch([]UpdateBatchItem{update}); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.DeleteBatch([][]byte{[]byte("b")}); err != nil {
				t.Fatal(err)
			}
			if err = tx.InsertBatch([][]byte{[]byte("c")}, [][]byte{doc("returns label")}); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := c.UpdateBatch([]UpdateBatchItem{update}); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete([]byte("b")); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Insert([]byte("c"), doc("returns label")); err != nil {
				t.Fatal(err)
			}
		}
		os.Exit(0)
	}
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
			for _, mode := range []string{"singleton", "composite"} {
				t.Run(string(version)+"/"+string(profile)+"/"+mode, func(t *testing.T) {
					dir := t.TempDir()
					cmd := exec.Command(os.Args[0], "-test.run=^TestNativeTextPublicCrashReplay$")
					cmd.Env = append(os.Environ(), "GOMAP_NATIVE_TEXT_CRASH_DIR="+dir, "GOMAP_NATIVE_TEXT_CRASH_PROFILE="+string(profile), "GOMAP_NATIVE_TEXT_CRASH_MODE="+mode, "GOMAP_NATIVE_TEXT_CRASH_VERSION="+string(version))
					if out, err := cmd.CombinedOutput(); err != nil {
						t.Fatalf("child: %v %s", err, out)
					}
					owner, _, c := openNativeTextPublic(t, dir, profile, false, version)
					requireNativeTextIDs(t, c, "returns", "a", "c")
					requireNativeTextIDs(t, c, "refund")
					requireNativeTextIDs(t, c, "shipping")
					if err := owner.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					requireNativeTextIDs(t, c, "returns", "a", "c")
				})
			}
		}
	}
}

func TestNativeTextPublicStatusConflict(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		t.Run(string(version), func(t *testing.T) { testNativeTextPublicStatusConflict(t, version) })
	}
}

func testNativeTextPublicStatusConflict(t *testing.T, version TextIndexVersion) {
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true, version)
	first, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	doc := []byte(`{"tenant":"a","content":"shared policy","embedding":[1,0]}`)
	if err = first.InsertBatch([][]byte{[]byte("a")}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if err = second.InsertBatch([][]byte{[]byte("b")}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if err = first.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = second.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
		t.Fatalf("shared text basis not fenced: %v", err)
	}
	requireNativeTextIDs(t, c, "shared", "a")
	if got, err := c.Get([]byte("b")); err != nil || got != nil {
		t.Fatalf("rejected row=%s err=%v", got, err)
	}
	// A new capture can accept the same independent row without losing a's
	// ordinal, posting or corpus count.
	if _, err = c.Insert([]byte("b"), doc); err != nil {
		t.Fatal(err)
	}
	requireNativeTextIDs(t, c, "shared", "a", "b")
	stats, err := c.TextIndexStorageStats("content")
	if err != nil || stats.Documents != 2 {
		t.Fatalf("sibling corpus count=%d err=%v", stats.Documents, err)
	}
	// These replacements keep corpus cardinality unchanged. V1 must also
	// fence shared field/term RMW state when the corpus key is not emitted.
	firstUpdate, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer firstUpdate.Close()
	secondUpdate, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer secondUpdate.Close()
	update := func(id, text string) UpdateBatchItem {
		return UpdateBatchItem{DocumentID: []byte(id), Update: func([]byte) ([]byte, bool, error) {
			return []byte(fmt.Sprintf(`{"tenant":"a","content":%q,"embedding":[1,0]}`, text)), true, nil
		}}
	}
	if _, err = firstUpdate.UpdateBatch([]UpdateBatchItem{update("a", "alpha shared policy")}); err != nil {
		t.Fatal(err)
	}
	if _, err = secondUpdate.UpdateBatch([]UpdateBatchItem{update("b", "beta shared policy")}); err != nil {
		t.Fatal(err)
	}
	if err = firstUpdate.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = secondUpdate.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
		t.Fatalf("unchanged-corpus shared stats not fenced: %v", err)
	}
	requireNativeTextIDs(t, c, "alpha", "a")
	requireNativeTextIDs(t, c, "beta")
	stats, err = c.TextIndexStorageStats("content")
	if err != nil || stats.Documents != 2 {
		t.Fatalf("updated corpus=%d err=%v", stats.Documents, err)
	}
}

func TestNativeTextPublicAdmissionCleanup(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		t.Run(string(version), func(t *testing.T) { testNativeTextPublicAdmissionCleanup(t, version) })
	}
}

func testNativeTextPublicAdmissionCleanup(t *testing.T, version TextIndexVersion) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var pins []backenddb.NativeSemanticCapture
	var manager *CollectionManager
	var budget nativeHNSWAdmissionBudget
	defer func() {
		for i := len(pins) - 1; i >= 0; i-- {
			if err := pins[i].Close(); err != nil {
				t.Error(err)
			}
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
		if budget != nil {
			stats := budget.Stats()
			if stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Errorf("full Close retained text receipts: %+v", stats)
			}
		}
	}()
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	pin := func() {
		t.Helper()
		cut, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		pins = append(pins, cut)
	}
	pin() // Retain every setup cut so teardown cannot hide refused-operation growth.
	budget = pins[0].NativeAllocator().(nativeHNSWAdmissionBudget)
	manager = NewCollectionManager(backend)
	if _, err = manager.CreateCollection(nativeTextPublicMeta(version)); err != nil {
		t.Fatal(err)
	}
	pin()
	c, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	doc := []byte(`{"tenant":"a","content":"refund policy","embedding":[1,0]}`)
	if _, err = c.Insert([]byte("a"), doc); err != nil {
		t.Fatal(err)
	}
	pin()
	requireNativeTextIDs(t, c, "refund", "a") // Include any first-read cache before measuring.
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	captureBytes := nativeHNSWAdmissionCaptureBytes(t, c, budget)
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	leave := captureBytes + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	if available < leave {
		t.Fatalf("setup exhausted capture boundary: available=%d leave=%d", available, leave)
	}
	if err = blocker.Resize(available - leave); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		before := budget.Stats()
		_, err = c.SearchText(TextSearchOptions{IndexName: "content", Query: "refund", TopK: 8})
		if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "native text query workspace") {
			t.Fatalf("attempt%d expected query workspace refusal: %v", attempt, err)
		}
		after := budget.Stats()
		if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatalf("attempt%d refused query changed receipts: before=%+v after=%+v", attempt, before, after)
		}
		if _, err = c.TextIndexStorageStats("content"); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("attempt%d expected inspection workspace refusal: %v", attempt, err)
		}
		after = budget.Stats()
		if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatalf("attempt%d refused inspection changed receipts: before=%+v after=%+v", attempt, before, after)
		}
	}
	blocker.Close()
	requireNativeTextIDs(t, c, "refund", "a")
	// A source-bounded analysis allowance refuses before any row/text publication.
	// Stage a modest canonical document, but exceed the text analysis allowance.
	// A much larger input can be refused earlier by ordinary scalar preparation.
	large := []byte(fmt.Sprintf(`{"tenant":"a","content":%q,"embedding":[1,0]}`, strings.Repeat("word ", 1<<13)))
	for attempt := 0; attempt < 3; attempt++ {
		before := budget.Stats()
		_, err = c.Insert([]byte("b"), large)
		if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "native text analysis workspace") {
			t.Fatalf("attempt%d expected analysis capacity refusal: %v", attempt, err)
		}
		after := budget.Stats()
		if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatalf("attempt%d refused insert changed receipts: before=%+v after=%+v", attempt, before, after)
		}
		if got, err := c.Get([]byte("b")); err != nil || got != nil {
			t.Fatalf("refused row=%s err=%v", got, err)
		}
	}
	if _, err = c.Insert([]byte("b"), doc); err != nil {
		t.Fatal(err)
	}
	pin()
	requireNativeTextIDs(t, c, "refund", "a", "b")
}

func TestNativeTextPublicBoundedImage(t *testing.T) {
	owner, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	const count = 64
	ids, docs := make([][]byte, count), make([][]byte, count)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%03d", i))
		docs[i] = []byte(fmt.Sprintf(`{"tenant":"a","content":"common policy document %d refund returns shipping delivery address contact support","embedding":[1,0]}`, i))
	}
	if _, err := c.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	response, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: "common", TopK: count, IncludeDocuments: true})
	if err != nil || len(response.Results) != count {
		t.Fatalf("bounded image search rows=%d err=%v", len(response.Results), err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	response, err = c.SearchText(TextSearchOptions{IndexName: "content", Query: "common", TopK: count, IncludeDocuments: true})
	if err != nil || len(response.Results) != count {
		t.Fatalf("physical image search rows=%d err=%v", len(response.Results), err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	t.Logf("64-row mixed-index default budget limits=%+v stats=%+v", budget.Limits(), budget.Stats())
}

// This regression starts at the actual public owner and ordinary Collection
// APIs. Keep the original fixture available for an old-source red execution.
func TestNativeTextPublicV2Lifecycle(t *testing.T) {
	testNativeTextPublicLifecycle(t, TextIndexVersionV2)
}

func TestNativeTextPublicV1Lifecycle(t *testing.T) {
	testNativeTextPublicLifecycle(t, TextIndexVersionV1)
}

func testNativeTextPublicLifecycle(t *testing.T, version TextIndexVersion) {
	t.Helper()
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			var owner *treedb.DB
			var manager *CollectionManager
			var collection *Collection
			open := func(create bool) {
				t.Helper()
				var err error
				owner, err = treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				backend, err := owner.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager = NewCollectionManager(backend)
				if create {
					_, err = manager.CreateCollection(&CollectionMeta{Name: "docs",
						Indexes:       []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}},
						VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}},
						TextIndexes:   []TextIndexDefinition{{Name: "content", Version: version, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple, StorePositions: true}},
					})
					if err != nil {
						t.Fatalf("ordinary native text create: %v", err)
					}
				}
				collection, err = manager.OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
			}
			closeOwner := func() {
				t.Helper()
				if manager != nil {
					if err := manager.Close(); err != nil {
						t.Fatal(err)
					}
					manager = nil
				}
				if owner != nil {
					if err := owner.Close(); err != nil {
						t.Fatal(err)
					}
					owner = nil
				}
			}
			defer func() {
				if manager != nil {
					_ = manager.Close()
				}
				if owner != nil {
					_ = owner.Close()
				}
			}()
			open(true)
			status, err := collection.TextIndexStatus("content")
			wantRoots := 7
			if version == TextIndexVersionV1 {
				wantRoots = 3
			}
			if err != nil || status.Version != version || !status.Ready || !status.Readable || !status.Writable || len(status.ActiveRootNames) != wantRoots {
				t.Fatalf("versioned status=%+v err=%v", status, err)
			}
			doc := func(content string, x, y int) []byte {
				payload := ""
				if version == TextIndexVersionV1 {
					payload = strings.Repeat("p", 8192)
				}
				return []byte(fmt.Sprintf(`{"tenant":"a","content":%q,"embedding":[%d,%d],"payload":%q}`, content, x, y, payload))
			}
			if _, err := collection.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{doc("refund policy", 1, 0), doc("shipping policy", 0, 1)}); err != nil {
				t.Fatal(err)
			}
			idx, err := collection.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4})
			if err != nil {
				t.Fatal(err)
			}
			if version == TextIndexVersionV1 {
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			old, err := collection.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			if version == TextIndexVersionV1 {
				entry, err := old.nativeRead.getEntry([]byte("a"))
				if err != nil || entry.Flags&node.FlagPointer == 0 {
					t.Fatalf("physical pointer fixture flags=%d err=%v", entry.Flags, err)
				}
			}
			search := func(query string, view *CollectionReadView, want ...string) {
				t.Helper()
				options := TextSearchOptions{IndexName: "content", Query: query, TopK: 8, IncludeDocuments: true}
				if strings.HasPrefix(query, `"`) {
					options.Query = ""
					options.Phrase = &TextSearchPhraseQuery{Query: strings.Trim(query, `"`)}
				}
				var response TextSearchResponse
				var err error
				if view == nil {
					response, err = collection.SearchText(options)
				} else {
					response, err = collection.searchTextAtReadView(options, textSearchResultFull, view)
				}
				if err != nil {
					t.Fatalf("search %q: %v", query, err)
				}
				ids := make([]string, 0, len(response.Results))
				for _, row := range response.Results {
					if !bytes.Contains(row.Document, []byte(strings.Trim(query, `"`))) {
						t.Fatalf("search %q document from different cut: %s", query, row.Document)
					}
					ids = append(ids, string(row.DocumentID))
				}
				if want == nil {
					want = []string{}
				}
				sort.Strings(ids)
				sort.Strings(want)
				if !reflect.DeepEqual(ids, want) {
					t.Fatalf("search %q ids=%v want=%v", query, ids, want)
				}
			}
			search("refund", nil, "a")
			boolean, err := collection.SearchText(TextSearchOptions{IndexName: "content", Query: "refund AND policy", TopK: 8})
			if err != nil || len(boolean.Results) != 1 || string(boolean.Results[0].DocumentID) != "a" {
				t.Fatalf("boolean results=%v err=%v", boolean.Results, err)
			}
			ranked, err := collection.SearchText(TextSearchOptions{IndexName: "content", Query: "policy", TopK: 8})
			if err != nil || len(ranked.Results) != 2 {
				t.Fatalf("ranked results=%v err=%v", ranked.Results, err)
			}
			for i, row := range ranked.Results {
				if row.Score <= 0 || row.Rank != i+1 || (i > 0 && ranked.Results[i-1].Score < row.Score) {
					t.Fatalf("invalid BM25 ranking: %+v", ranked.Results)
				}
			}
			if version == TextIndexVersionV1 {
				// Maintained V1 deliberately refuses phrase/proximity queries;
				// the V2 sibling proves the successful phrase contract.
				_, err := collection.SearchText(TextSearchOptions{IndexName: "content", Phrase: &TextSearchPhraseQuery{Query: "refund policy"}, TopK: 8})
				if !errors.Is(err, ErrTextIndexUnavailable) {
					t.Fatalf("V1 phrase refusal=%v", err)
				}
			} else {
				search(`"refund policy"`, nil, "a")
			}
			matched, modified, err := collection.Update([]byte("a"), func(current []byte) ([]byte, bool, error) {
				if !bytes.Contains(current, []byte("refund")) {
					return nil, false, fmt.Errorf("unexpected old document %s", current)
				}
				return doc("returns policy", 1, 0), true, nil
			})
			if err != nil || !matched || !modified {
				t.Fatalf("update matched=%v modified=%v err=%v", matched, modified, err)
			}
			if deleted, err := collection.DeleteDocument([]byte("b")); err != nil || !deleted {
				t.Fatalf("delete=%v err=%v", deleted, err)
			}
			search("refund", old, "a")
			search("shipping", old, "b")
			search("returns", nil, "a")
			search("refund", nil)
			search("shipping", nil)
			stats, err := collection.TextIndexStorageStats("content")
			if err != nil || stats.Documents != 1 {
				t.Fatalf("live text storage=%+v err=%v", stats, err)
			}
			rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 8})
			if err != nil || len(rows) != 1 {
				t.Fatalf("active HNSW after text CRUD: rows=%d err=%v", len(rows), err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			search("refund", old, "a")
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			closeOwner()
			open(false)
			search("returns", nil, "a")
			search("refund", nil)
			search("shipping", nil)
			closeOwner()
		})
	}
}
