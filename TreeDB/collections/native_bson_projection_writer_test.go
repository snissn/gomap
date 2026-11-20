package collections

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeBSONProjectionWriterDirectoryFences(t *testing.T) {
	previous := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(previous)
	for _, tc := range []struct {
		name       string
		hotTarget  int64
		wantFiles  int
		wantFences int
	}{
		{name: "ordinary", wantFiles: 1, wantFences: 1},
		{name: "explicit-hot-target-with-rotations", hotTarget: 16 << 10, wantFiles: 16, wantFences: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const rows = 32
			ids, docs := make([][]byte, rows), make([][]byte, rows)
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("row-%03d", i))
				value, err := bson.Marshal(bson.D{{Key: "payload", Value: strings.Repeat("record body", 800)}, {Key: "row", Value: i}})
				if err != nil {
					t.Fatal(err)
				}
				docs[i] = value
			}

			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.ValueLog.Generational.HotSegmentTargetBytes = tc.hotTarget
			opts.ValueLog.Compression = treedb.ValueLogCompressionOff
			open := func(create bool) (*treedb.DB, *CollectionManager, *Collection) {
				t.Helper()
				owner, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				backend, err := owner.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager := NewCollectionManager(backend)
				if create {
					_, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}})
					if err != nil {
						t.Fatal(err)
					}
				}
				collection, err := manager.OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
				return owner, manager, collection
			}
			closeDB := func(owner *treedb.DB, manager *CollectionManager) {
				t.Helper()
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}
			owner, manager, coll := open(true)
			t.Cleanup(func() {
				if owner != nil {
					closeDB(owner, manager)
				}
			})

			var mu sync.Mutex
			files := make(map[string]bool)
			var before, after int
			valueDir := backenddb.ValueLogDirPath(filepath.Join(dir, "maindb"))
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource != durabilitycut.ResourceValueLog {
					return nil
				}
				mu.Lock()
				defer mu.Unlock()
				if event.Namespace == durabilitycut.NamespaceCreate && filepath.Dir(event.NewPath) == valueDir {
					files[event.NewPath] = true
				}
				if filepath.Clean(event.Path) == valueDir {
					switch event.Point {
					case durabilitycut.BeforeNewFileDirectorySync:
						before++
					case durabilitycut.AfterNewFileDirectorySync:
						after++
					}
				}
				return nil
			})
			_, insertErr := coll.InsertBatch(ids, docs)
			checkpointErr := owner.Checkpoint()
			restore()
			if insertErr != nil || checkpointErr != nil {
				t.Fatalf("insert=%v, checkpoint=%v", insertErr, checkpointErr)
			}
			mu.Lock()
			t.Logf("document files=%d, directory fences=%d/%d", len(files), before, after)
			if len(files) != tc.wantFiles || before != tc.wantFences || after != tc.wantFences {
				t.Errorf("want %d document files and %d paired fences", tc.wantFiles, tc.wantFences)
			}
			mu.Unlock()

			closeDB(owner, manager)
			owner = nil
			owner, manager, coll = open(false)
			for i, id := range ids {
				got, err := coll.Get(id)
				if err != nil || !bytes.Equal(got, docs[i]) {
					t.Fatalf("reopen document %q: length=%d, error=%v", id, len(got), err)
				}
			}
		})
	}
}
