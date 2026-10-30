package collections_test

import (
	"bytes"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionNativewireInsertBatchNoResultIDsTrustedBSONFormat(t *testing.T) {
	for _, native := range []bool{false, true} {
		for _, format := range []collections.DocumentFormat{"", collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
			t.Run(string(format)+map[bool]string{false: "/legacy", true: "/native"}[native], func(t *testing.T) {
				meta := &collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: format}}
				var backend *backenddb.DB
				var col *collections.Collection
				if native {
					opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
					opts.MemtableMode = "cow_btree"
					opts.FlushThreshold = 1 << 30
					opts.BackgroundCheckpointInterval = -1
					owner, err := treedb.Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := owner.Close(); err != nil {
							t.Error(err)
						}
					})
					if got := owner.Stats()["treedb.cache.memtable_mode"]; got != "cow_btree" {
						t.Fatalf("native memtable mode=%q, want cow_btree", got)
					}
					backend, err = owner.NativeSemanticBackend()
					if err != nil {
						t.Fatal(err)
					}
				} else {
					var err error
					backend, err = backenddb.Open(backenddb.Options{Dir: t.TempDir()})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := backend.Close(); err != nil {
							t.Error(err)
						}
					})
				}
				if got := backend.HasNativeSemanticBridge(); got != native {
					t.Fatalf("native backend=%t, want=%t", got, native)
				}
				manager := collections.NewCollectionManager(backend)
				t.Cleanup(func() {
					if err := manager.Close(); err != nil {
						t.Error(err)
					}
				})
				if got := manager.UsesNativeCommandWALApply(); got != native {
					t.Fatalf("native collection owner=%t, want=%t", got, native)
				}
				if _, err := manager.CreateCollection(meta); err != nil {
					t.Fatal(err)
				}
				var err error
				col, err = manager.OpenCollection(meta.Name)
				if err != nil {
					t.Fatal(err)
				}

				document := []byte(`{"value":"json"}`)
				if format == collections.DocumentFormatBSON {
					var err error
					document, err = bson.Marshal(bson.D{{Key: "value", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 255}}}})
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := col.NativewireInsertBatchNoResultIDs([][]byte{[]byte("seed")}, [][]byte{document}, false); err != nil {
					t.Fatal(err)
				}
				next := backend.CommandWALNextLSN()
				err = col.NativewireInsertBatchNoResultIDs([][]byte{[]byte("trusted")}, [][]byte{document}, true)
				if format == collections.DocumentFormatBSON {
					if err != nil {
						t.Fatal(err)
					}
					if got, err := col.Get([]byte("trusted")); err != nil || !bytes.Equal(got, document) {
						t.Fatalf("trusted BSON got=%x err=%v", got, err)
					}
				} else {
					if err == nil || !strings.Contains(err.Error(), "trusted BSON insert requires BSON document format") {
						t.Fatalf("trusted JSON error=%v, want BSON format rejection", err)
					}
					if rows, truncated, err := col.ScanDocuments(2); err != nil || truncated || len(rows) != 1 || backend.CommandWALNextLSN() != next {
						t.Fatalf("rejection changed collection: rows=%v truncated=%t next=%d want=%d err=%v", rows, truncated, backend.CommandWALNextLSN(), next, err)
					}
					if got, err := col.Get([]byte("trusted")); err != nil || got != nil {
						t.Fatalf("rejected document stored: got=%x err=%v", got, err)
					}
					if got, err := col.Get([]byte("seed")); err != nil || !bytes.Equal(got, document) {
						t.Fatalf("seed changed: got=%x err=%v", got, err)
					}
				}
				if err := col.NativewireInsertBatchNoResultIDs([][]byte{[]byte("ordinary")}, [][]byte{document}, false); err != nil {
					t.Fatal(err)
				}
				if got, err := col.Get([]byte("ordinary")); err != nil || !bytes.Equal(got, document) {
					t.Fatalf("ordinary insert got=%x err=%v", got, err)
				}
			})
		}
	}
}
