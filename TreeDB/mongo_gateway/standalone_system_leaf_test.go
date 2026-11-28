package mongogateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStandaloneBSONSystemPagerLeaves(t *testing.T) {
	for _, policy := range []collections.RootStoragePolicy{collections.RootStorageDefault, collections.RootStorageCompressed} {
		name := "default"
		if policy == collections.RootStorageCompressed {
			name = "compressed"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			mainDir := dir
			opts := StandaloneOptions{
				Dir: dir, Profile: treedb.ProfileCommandWALRelaxed,
				DefaultCollectionOptions: collections.CollectionOptions{DataRootStoragePolicy: policy},
			}
			open := func() *StandaloneServer {
				t.Helper()
				server, err := OpenStandaloneServer(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := server.Close(); err != nil {
						t.Error(err)
					}
				})
				return server
			}
			owner := open()
			var appends, leafSyncs, indexSyncs atomic.Uint64
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Root != mainDir && filepath.Dir(event.Path) != mainDir {
					return nil
				}
				if event.Point == durabilitycut.BeforeDependencyAppend && event.Resource == durabilitycut.ResourceOuterLeaf {
					appends.Add(1)
				}
				if event.Point == durabilitycut.AfterDependencyFileSync {
					for _, path := range event.Paths {
						if strings.HasPrefix(filepath.Base(path), "value-l255-") && filepath.Ext(path) == ".log" {
							leafSyncs.Add(1)
						}
					}
				}
				if event.Point == durabilitycut.BeforeIndexDataSync {
					indexSyncs.Add(1)
				}
				return nil
			})
			defer restore()
			if _, err := owner.Collections.CreateCollection(&collections.CollectionMeta{
				Name: "db.rows", Options: owner.Options.DefaultCollectionOptions,
			}); err != nil {
				t.Fatal(err)
			}
			insert := func(server *StandaloneServer, id string, n int64) []byte {
				t.Helper()
				col, err := server.Collections.OpenCollection("db.rows")
				if err != nil {
					t.Fatal(err)
				}
				doc, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte(id)}, [][]byte{doc}); err != nil {
					t.Fatal(err)
				}
				return doc
			}
			first := insert(owner, "one", 1)
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			wantOuter := policy == collections.RootStorageCompressed
			t.Logf("outer appends=%d outer syncs=%d index syncs=%d", appends.Load(), leafSyncs.Load(), indexSyncs.Load())
			if (appends.Load() > 0) != wantOuter || (leafSyncs.Load() > 0) != wantOuter || indexSyncs.Load() == 0 {
				t.Fatalf("unexpected sync activity for external collection=%t", wantOuter)
			}

			check := func(server *StandaloneServer, id string, want []byte) {
				t.Helper()
				snapshot := server.Backend.AcquireSnapshot()
				if snapshot == nil {
					t.Fatal("physical snapshot unavailable")
				}
				defer func() {
					if err := snapshot.Close(); err != nil {
						t.Error(err)
					}
				}()
				system := snapshot.State().SystemRootPageID
				encoded, err := snapshot.GetAtRoot(system, []byte("collections/root/db.rows/primary"))
				if err != nil || len(encoded) != 8 {
					t.Fatalf("primary descriptor=%x err=%v", encoded, err)
				}
				primary := binary.BigEndian.Uint64(encoded)
				for _, root := range []struct {
					id        uint64
					wantOuter bool
				}{{system, false}, {primary, wantOuter}} {
					hasOuter := false
					err := leafrefscan.Walk(context.Background(), root.id, snapshot.Pager().Get, nil, func(page.LeafLogPtr) error {
						hasOuter = true
						return nil
					})
					if err != nil || hasOuter != root.wantOuter {
						t.Fatalf("root %d external=%t want=%t err=%v", root.id, hasOuter, root.wantOuter, err)
					}
				}
				col, err := server.Collections.OpenCollection("db.rows")
				if err != nil {
					t.Fatal(err)
				}
				got, err := col.Get([]byte(id))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("reopened %s=%x want=%x err=%v", id, got, want, err)
				}
			}
			reopened := open()
			check(reopened, "one", first)
			second := insert(reopened, "two", 2)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			check(open(), "two", second)

			format, ok, err := backenddb.LoadFormatConfig(mainDir)
			if err != nil || !ok || !format.IndexOuterLeavesInValueLog {
				t.Fatalf("outer-leaf format=%+v found=%t err=%v", format, ok, err)
			}
		})
	}
}
