package collections

import (
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func denseNormRequireIDs(t *testing.T, c *Collection, view *CollectionReadView, want int) {
	t.Helper()
	opts := TextSearchOptions{IndexName: "content", Query: "common", TopK: 16, IncludeDocuments: true}
	var got TextSearchResponse
	var err error
	if view == nil {
		got, err = c.SearchText(opts)
	} else {
		got, err = c.searchTextAtReadView(opts, textSearchResultFull, view)
	}
	if err != nil || len(got.Results) != want || got.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("results=%+v err=%v want=%d", got, err, want)
	}
}

func TestTextV2DenseNormPublicLifecycle(t *testing.T) {
	for _, native := range []bool{false, true} {
		for _, profile := range []treedb.Profile{treedb.ProfileNoWALFast, treedb.ProfileCommandWALDurable} {
			t.Run(fmt.Sprintf("native=%t/%s", native, profile), func(t *testing.T) {
				dir := t.TempDir()
				var manager *CollectionManager
				var closeOwner, checkpoint func() error
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
				}
				t.Cleanup(closeAll)
				open := func(create bool) *Collection {
					opts := treedb.OptionsFor(profile, dir)
					opts.MemtableMode = "cow_btree"
					opts.BackgroundCheckpointInterval = -1
					opts.BackgroundCheckpointIdleDuration = -1
					if native {
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
					} else {
						d, close, err := treedb.OpenBackendWithCachedLeafLog(opts)
						if err != nil {
							t.Fatal(err)
						}
						manager, closeOwner, checkpoint = NewCollectionManager(d), close, d.Checkpoint
					}
					if create {
						if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "content", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "content"}}, StorePositions: true}}}); err != nil {
							t.Fatal(err)
						}
					}
					c, err := manager.OpenCollection("docs")
					if err != nil {
						t.Fatal(err)
					}
					return c
				}
				c := open(true)
				ids, docs := make([][]byte, 8), make([][]byte, 8)
				for i := range ids {
					ids[i], docs[i] = []byte(fmt.Sprintf("d%d", i)), []byte(`{"content":"common original"}`)
				}
				if _, err := c.InsertBatch(ids, docs); err != nil {
					t.Fatal(err)
				}
				denseNormRequireIDs(t, c, nil, 8)
				view, err := c.OpenCollectionReadView()
				if err != nil {
					t.Fatal(err)
				}
				defer view.Close()
				if _, _, err = c.Update(ids[0], func([]byte) ([]byte, bool, error) { return []byte(`{"content":"updated only"}`), true, nil }); err != nil {
					t.Fatal(err)
				}
				if err = c.Delete(ids[1]); err != nil {
					t.Fatal(err)
				}
				// The old dense cut survives mixed generations and tombstones;
				// neither representation may normalize generation identities.
				denseNormRequireIDs(t, c, view, 8)
				denseNormRequireIDs(t, c, nil, 6)
				if profile == treedb.ProfileNoWALFast {
					if _, err := c.RewriteTextIndex("content", TextIndexRewriteOptions{TargetPostingsPerBlock: 8}); err != nil {
						t.Fatal(err)
					}
					denseNormRequireIDs(t, c, nil, 6)
				}
				if _, err := c.TextIndexStorageStats("content"); err != nil {
					t.Fatal(err)
				}
				if err := view.Close(); err != nil {
					t.Fatal(err)
				}
				if err := checkpoint(); err != nil {
					t.Fatal(err)
				}
				closeAll()
				c = open(false)
				denseNormRequireIDs(t, c, nil, 6)
			})
		}
	}
}

func TestTextV2DenseNormCommandWALReplay(t *testing.T) {
	if dir := os.Getenv("GOMAP_DENSE_NORM_REPLAY_DIR"); dir != "" {
		_, _, c := openNativeTextPublic(t, dir, treedb.ProfileCommandWALDurable, true)
		var ids, docs [][]byte
		for i := 0; i < 8; i++ {
			ids = append(ids, []byte(fmt.Sprintf("d%d", i)))
			docs = append(docs, []byte(`{"content":"common original"}`))
		}
		if _, err := c.InsertBatch(ids, docs); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.Update(ids[0], func([]byte) ([]byte, bool, error) { return []byte(`{"content":"updated only"}`), true, nil }); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ids[1]); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Exercise command replay without a graceful-close checkpoint.
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestTextV2DenseNormCommandWALReplay$")
	cmd.Env = append(os.Environ(), "GOMAP_DENSE_NORM_REPLAY_DIR="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v %s", err, out)
	}
	_, _, c := openNativeTextPublic(t, dir, treedb.ProfileCommandWALDurable, false)
	denseNormRequireIDs(t, c, nil, 6)
	got, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: "updated", TopK: 8})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, row := range got.Results {
		ids = append(ids, string(row.DocumentID))
	}
	sort.Strings(ids)
	if !reflect.DeepEqual(ids, []string{"d0"}) {
		t.Fatalf("replayed identities=%v", ids)
	}
}
