package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The maintained graph has one scalar cell per row. An unsupported array must
// not become a canonical row which neither reconciliation nor Close can read.
func TestNativeHNSWScalarArrayRefusesBeforePublication(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON} {
			for _, operation := range []string{"insert", "batch", "update"} {
				t.Run(fmt.Sprintf("%s/%s/%s", profile, format, operation), func(t *testing.T) {
					opts := treedb.OptionsFor(profile, t.TempDir())
					opts.MemtableMode = "cow_btree"
					opts.BackgroundCheckpointInterval = -1
					opts.BackgroundCheckpointIdleDuration = -1
					db, err := treedb.Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := db.Close(); err != nil {
							t.Error(err)
						}
					})
					backend, err := db.NativeSemanticBackend()
					if err != nil {
						t.Fatal(err)
					}
					manager := NewCollectionManager(backend)
					t.Cleanup(func() {
						if err := manager.Close(); err != nil {
							t.Error(err)
						}
					})
					def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}
					meta := &CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: format, AllowArrayValuesInIndex: true}, Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}}, VectorIndexes: []VectorIndexDefinition{def}}
					if _, err = manager.CreateCollection(meta); err != nil {
						t.Fatal(err)
					}
					c, err := manager.OpenCollection(meta.Name)
					if err != nil {
						t.Fatal(err)
					}
					encode := func(tenant any) []byte {
						t.Helper()
						row := map[string]any{"tenant": tenant, "embedding": []float64{1, 0}}
						var raw []byte
						var err error
						if format == DocumentFormatBSON {
							raw, err = bson.Marshal(row)
						} else {
							raw, err = json.Marshal(row)
						}
						if err != nil {
							t.Fatal(err)
						}
						return raw
					}
					good := encode("a")
					if _, err = c.Insert([]byte("existing"), good); err != nil {
						t.Fatal(err)
					}
					idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
					if err != nil {
						t.Fatal(err)
					}
					for _, array := range [][]string{{}, {"a"}, {"a", "b"}} {
						bad := encode(array)
						before, err := c.nativeJSON.Capture(c.name)
						if err != nil {
							t.Fatal(err)
						}
						sequence, generation := before.capture.LogicalSequence(), before.collection.documentGeneration
						if err = before.Close(); err != nil {
							t.Fatal(err)
						}
						lsn := backend.CommandWALNextLSN()
						calls := 0
						switch operation {
						case "insert":
							_, err = c.Insert([]byte("bad"), bad)
						case "batch":
							_, err = c.InsertBatch([][]byte{[]byte("prefix"), []byte("bad")}, [][]byte{good, bad})
						case "update":
							_, _, err = c.Update([]byte("existing"), func([]byte) ([]byte, bool, error) { calls++; return bad, true, nil })
						}
						if err == nil || errors.Is(err, ErrCommitAmbiguous) {
							t.Fatalf("array=%v must refuse before acceptance: %v", array, err)
						}
						if operation == "update" && calls != 1 {
							t.Fatalf("callback calls=%d", calls)
						}
						after, captureErr := c.nativeJSON.Capture(c.name)
						if captureErr != nil {
							t.Fatal(captureErr)
						}
						changed := after.capture.LogicalSequence() != sequence || after.collection.documentGeneration != generation || backend.CommandWALNextLSN() != lsn
						if err = after.Close(); err != nil {
							t.Fatal(err)
						}
						if changed {
							t.Fatal("rejection changed logical state, document generation or journal")
						}
						stored, err := c.Get([]byte("existing"))
						if err != nil || !bytes.Equal(stored, good) {
							t.Fatalf("existing row changed: %s %v", stored, err)
						}
						for _, id := range []string{"prefix", "bad"} {
							if raw, err := c.Get([]byte(id)); err != nil || len(raw) != 0 {
								t.Fatalf("rejected row %s present=%t: %v", id, len(raw) != 0, err)
							}
						}
						if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1}); err != nil {
							t.Fatalf("rejection poisoned graph: %v", err)
						}
					}
					if _, err = c.Insert([]byte("later"), good); err != nil {
						t.Fatalf("valid reuse: %v", err)
					}
				})
			}
		}
	}
}

func TestNativeHNSWInsertKeepsAcceptedIDOnMaintenanceFailure(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 1, 2, profile)
			defer closeAll()
			idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			bad, err := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{1.0, 0.0, 0.0}}})
			if err != nil {
				t.Fatal(err)
			}
			input := []byte("bad")
			id, err := c.Insert(input, bad)
			if !errors.Is(err, ErrCommitAmbiguous) || !bytes.Equal(id, input) {
				t.Errorf("accepted id=%q error=%v", id, err)
			}
			input[0] = 'x'
			if string(id) != "bad" {
				t.Errorf("returned accepted ID lost caller ownership: %q", id)
			}
			stored, err := c.Get([]byte("bad"))
			if err != nil || !bytes.Equal(stored, bad) {
				t.Fatalf("accepted document missing: %v", err)
			}
			good, err := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{1.0, 0.0}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = c.Update([]byte("bad"), func([]byte) ([]byte, bool, error) { return good, true, nil }); !errors.Is(err, ErrCommitAmbiguous) {
				t.Fatal(err)
			}
			if err = idx.Rebuild(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Explicit multikey indexes never become graph scalar columns. The new
// preflight must also leave ordinary array-capable collections usable.
func TestNativeHNSWScalarArrayOtherCapabilities(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON} {
		for _, graph := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/graph=%t", format, graph), func(t *testing.T) {
				opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
				opts.MemtableMode = "cow_btree"
				db, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				backend, err := db.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager := NewCollectionManager(backend)
				t.Cleanup(func() {
					if err := manager.Close(); err != nil {
						t.Error(err)
					}
				})
				meta := &CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: format, AllowArrayValuesInIndex: true}, Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString, MultiKey: graph}}}
				def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}
				if graph {
					meta.VectorIndexes = []VectorIndexDefinition{def}
				}
				if _, err = manager.CreateCollection(meta); err != nil {
					t.Fatal(err)
				}
				c, err := manager.OpenCollection(meta.Name)
				if err != nil {
					t.Fatal(err)
				}
				row := map[string]any{"tenant": []string{"a", "b"}, "embedding": []float64{1, 0}}
				var raw []byte
				if format == DocumentFormatBSON {
					raw, err = bson.Marshal(row)
				} else {
					raw, err = json.Marshal(row)
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err = c.Insert([]byte("array"), raw); err != nil {
					t.Fatal(err)
				}
				stored, err := c.Get([]byte("array"))
				if err != nil || !bytes.Equal(stored, raw) {
					t.Fatalf("array document: %v", err)
				}
				if graph {
					idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
					if err != nil {
						t.Fatal(err)
					}
					results, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
					if err != nil {
						t.Fatal(err)
					}
					requireVectorResultIDs(t, results, "array")
				}
			})
		}
	}
}
