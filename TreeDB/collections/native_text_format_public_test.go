package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// This is the same stored-format lifecycle on the maintained legacy owner and
// the public native owner. The public template encoder supplies input envelopes;
// the collection preparer assigns persisted and newly staged template definitions.
func TestNativeTextPublicStoredFormats(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatBSON, DocumentFormatTemplateV1} {
		for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
			for _, route := range []string{"legacy", "native", "legacy-import"} {
				t.Run(string(format)+"/"+string(version)+"/"+route, func(t *testing.T) {
					native := route == "native"
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
							stats := budget.Stats()
							if stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
								t.Errorf("stored format full Close retained receipts: %+v", stats)
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
						if native {
							owner, err := treedb.Open(opts)
							if err != nil {
								t.Fatal(err)
							}
							closeOwner, checkpoint = owner.Close, owner.Checkpoint
							backend, err := owner.NativeSemanticBackend()
							if err != nil {
								t.Fatal(err)
							}
							manager = NewCollectionManager(backend)
							cut, err := backend.CaptureNativeSemanticState()
							if err != nil {
								t.Fatal(err)
							}
							budget = cut.NativeAllocator().(nativeHNSWAdmissionBudget)
							if err = cut.Close(); err != nil {
								t.Fatal(err)
							}
						} else {
							backend, close, err := treedb.OpenBackendWithCachedLeafLog(opts)
							if err != nil {
								t.Fatal(err)
							}
							closeOwner, checkpoint = close, backend.Checkpoint
							manager = NewCollectionManager(backend)
						}
					}
					encode := func(content, nested, tag, shape string) []byte {
						document := map[string]any{"tenant": "one", "content": content, "nested": map[string]any{"body": nested}, "tags": []any{tag, 19, "", "policy"}, "unrelated": []byte{0, 1, 255}}
						if shape != "" {
							document[shape] = map[string]any{"new_nested_key": "dictionary"}
						}
						var out []byte
						var err error
						if format == DocumentFormatBSON {
							out, err = bson.Marshal(document)
						} else {
							out, err = json.Marshal(document)
						}
						if err != nil {
							t.Fatal(err)
						}
						if format == DocumentFormatTemplateV1 {
							out, err = EncodeTemplateV1DocumentJSON(out)
							if err != nil {
								t.Fatal(err)
							}
						}
						return out
					}
					open()
					if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: format}, Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}}}); err != nil {
						t.Fatal(err)
					}
					c, err := manager.OpenCollection("docs")
					if err != nil {
						t.Fatal(err)
					}
					if _, err = c.Insert([]byte("a"), encode("alpha policy", "nestedfirst", "tagfirst", "")); err != nil {
						t.Fatal(err)
					}
					if err = checkpoint(); err != nil {
						t.Fatal(err)
					}
					if _, err = c.Insert([]byte("b"), encode("alpha shipping", "nestedsecond", "tagsecond", "seed_shape")); err != nil {
						t.Fatal(err)
					}
					def := TextIndexDefinition{Name: "content", Version: version, Fields: []TextIndexField{{Field: "content"}, {Field: "nested.body"}, {Field: "tags"}}, Analyzer: TextAnalyzerSimple, StorePositions: version == TextIndexVersionV2}
					meta, stats, err := c.CreateTextIndex(def)
					if err != nil {
						t.Fatalf("stored-format seeded public CreateTextIndex: %v", err)
					}
					if len(meta.TextIndexes) != 1 || stats.DocumentsScanned != 2 {
						t.Fatalf("backfill meta=%+v stats=%+v", meta, stats)
					}
					requireNativeTextIDs(t, c, "alpha", "a", "b")
					requireNativeTextIDs(t, c, "nestedfirst", "a")
					requireNativeTextIDs(t, c, "tagsecond", "b")
					view, err := c.OpenCollectionReadView()
					if err != nil {
						t.Fatal(err)
					}
					defer view.Close()
					// Both shapes are new in this one publication. Native text must
					// use the transaction resolver overlay, not only the old cut.
					if _, err = c.InsertBatch([][]byte{[]byte("c"), []byte("d")}, [][]byte{encode("alpha current", "nestedthird", "tagthird", "new_shape_c"), encode("alpha current", "nestedfourth", "tagfourth", "new_shape_d")}); err != nil {
						t.Fatalf("new template shapes with text: %v", err)
					}
					if _, _, err = c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
						return encode("delta policy", "changednested", "changedtag", "updated_shape"), true, nil
					}); err != nil {
						t.Fatal(err)
					}
					if err = c.Delete([]byte("b")); err != nil {
						t.Fatal(err)
					}
					requireNativeTextIDs(t, c, "alpha", "c", "d")
					requireNativeTextIDs(t, c, "changednested", "a")
					if storage, err := c.TextIndexStorageStats("content"); err != nil || storage.Documents != 3 {
						t.Fatalf("corpus=%+v %v", storage, err)
					}
					if _, err = c.DropTextIndex("content"); err != nil {
						t.Fatal(err)
					}
					if _, err = c.SearchText(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 8}); !errors.Is(err, ErrIndexNotFound) {
						t.Fatalf("dropped index: %v", err)
					}
					old, err := c.searchTextAtReadView(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 8}, textSearchResultFull, view)
					if err != nil {
						t.Fatal(err)
					}
					ids := make([]string, 0, len(old.Results))
					for _, row := range old.Results {
						ids = append(ids, string(row.DocumentID))
					}
					sort.Strings(ids)
					if !reflect.DeepEqual(ids, []string{"a", "b"}) {
						t.Fatalf("held format cut=%v", ids)
					}
					if err = view.Close(); err != nil {
						t.Fatal(err)
					}
					if _, stats, err = c.CreateTextIndex(def); err != nil || stats.DocumentsScanned != 3 {
						t.Fatalf("recreate backfill=%+v %v", stats, err)
					}
					if err = checkpoint(); err != nil {
						t.Fatal(err)
					}
					closeAll()
					if route == "legacy-import" {
						// Import the actual current-binary legacy physical catalog,
						// text roots and persisted template definitions at first use.
						native = true
					}
					open()
					c, err = manager.OpenCollection("docs")
					if err != nil {
						t.Fatal(err)
					}
					requireNativeTextIDs(t, c, "alpha", "c", "d")
					requireNativeTextIDs(t, c, "delta", "a")
					var importedView *CollectionReadView
					if route == "legacy-import" {
						importedView, err = c.OpenCollectionReadView()
						if err != nil {
							t.Fatal(err)
						}
						defer importedView.Close()
					}
					if _, err = c.Insert([]byte("e"), encode("reopened policy", "reopenednested", "reopenedtag", "reopened_shape")); err != nil {
						t.Fatalf("reopened resolver mutation: %v", err)
					}
					requireNativeTextIDs(t, c, "reopenednested", "e")
					if importedView != nil {
						old, err := c.searchTextAtReadView(TextSearchOptions{IndexName: "content", Query: "reopenednested", TopK: 8}, textSearchResultFull, importedView)
						if err != nil || len(old.Results) != 0 {
							t.Fatalf("imported old cut observed new native document: %+v %v", old, err)
						}
						if err = importedView.Close(); err != nil {
							t.Fatal(err)
						}
						if err = checkpoint(); err != nil {
							t.Fatal(err)
						}
						closeAll()
						open()
						c, err = manager.OpenCollection("docs")
						if err != nil {
							t.Fatal(err)
						}
						requireNativeTextIDs(t, c, "alpha", "c", "d")
						requireNativeTextIDs(t, c, "reopenednested", "e")
						if stats, err := c.TextIndexStorageStats("content"); err != nil || stats.Documents != 4 {
							t.Fatalf("imported native corpus after second reopen: %+v %v", stats, err)
						}
					}
				})
			}
		}
	}
}

// The native private writer preserves the maintained canonical codec, including
// types which the text analyzer ignores rather than treating as plain strings.
func TestNativeTextBSONCodecAndAdmission(t *testing.T) {
	decimal, err := bson.ParseDecimal128("-123456789.0123456789")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bson.Marshal(bson.D{
		{Key: "text\n", Value: "quotes \" slash \\ control \x01 Unicode \u2028"},
		{Key: "number", Value: int64(math.MinInt64)}, {Key: "float", Value: math.Inf(1)},
		{Key: "decimal", Value: decimal}, {Key: "date", Value: bson.DateTime(math.MinInt64)},
		{Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 255}}},
		{Key: "regex", Value: bson.Regex{Pattern: "a\\b", Options: "mi"}},
		{Key: "scope", Value: bson.CodeWithScope{Code: "return a", Scope: bson.D{{Key: "a", Value: "inner"}}}},
		{Key: "nested", Value: bson.A{nil, true, bson.D{{Key: "value", Value: "nested"}}}},
		{Key: "oid", Value: bson.NewObjectID()}, {Key: "timestamp", Value: bson.Timestamp{T: math.MaxUint32, I: math.MaxUint32}},
		{Key: "min", Value: bson.MinKey{}}, {Key: "max", Value: bson.MaxKey{}}, {Key: "undefined", Value: bson.Undefined{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var admitted uint64
	actual, err := materializeTextBackfillDocumentJSON(raw, collectionOptions{documentFormat: DocumentFormatBSON, textMaterializationAdmit: func(n uint64) error { admitted += n; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := bson.MarshalExtJSON(bson.Raw(raw), true, false)
	if err != nil || !bytes.Equal(actual, expected) || cap(actual) != len(actual) || admitted <= uint64(len(actual)) {
		t.Fatalf("codec identity=%v cap=%d len=%d admission=%d error=%v", bytes.Equal(actual, expected), cap(actual), len(actual), admitted, err)
	}
	attempts := 0
	out, err := materializeTextBackfillDocumentJSON(raw, collectionOptions{documentFormat: DocumentFormatBSON, textMaterializationAdmit: func(uint64) error { attempts++; return memtable.ErrCOWCapacity }})
	if out != nil || !errors.Is(err, memtable.ErrCOWCapacity) || attempts != 1 {
		t.Fatalf("pre-conversion refusal output=%d attempts=%d error=%v", len(out), attempts, err)
	}
	if _, err := nativeTextBSONConversionCharge(^uint64(0), 1); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("overflow=%v", err)
	}
}

func TestNativeTextTemplateCodecAndAdmission(t *testing.T) {
	input, err := EncodeTemplateV1DocumentJSON([]byte(`{"text":"quotes \" and control \u0001","nested":{"body":"policy"},"tags":["alpha",19,"",null,"beta"],"number":1234567890123456789}`))
	if err != nil {
		t.Fatal(err)
	}
	stored, _, _, resolver, err := prepareTemplateV1InsertDocuments([][]byte{input}, nil, false, true)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := templateV1StoredDocumentJSON(stored[0], resolver)
	if err != nil {
		t.Fatal(err)
	}
	var charged uint64
	out, err := nativeTextTemplateJSON(stored[0], resolver, func(n uint64) error { charged += n; return nil })
	if err != nil || !bytes.Equal(out, expected) || charged <= uint64(len(out)) {
		t.Fatalf("template codec identity=%v charge=%d err=%v", bytes.Equal(out, expected), charged, err)
	}
	for _, refuse := range []int{1, 2} {
		calls := 0
		out, err = nativeTextTemplateJSON(stored[0], resolver, func(uint64) error {
			calls++
			if calls == refuse {
				return memtable.ErrCOWCapacity
			}
			return nil
		})
		if out != nil || !errors.Is(err, memtable.ErrCOWCapacity) || calls != refuse {
			t.Fatalf("refusal stage=%d calls=%d out=%d err=%v", refuse, calls, len(out), err)
		}
	}
	if _, err = nativeTextTemplateJSON(stored[0], nil, func(uint64) error { t.Fatal("missing resolver admitted conversion"); return nil }); !errors.Is(err, errTemplateV1MissingResolver) {
		t.Fatalf("missing resolver=%v", err)
	}
	malformed := append(bytes.Clone(stored[0]), 0)
	if _, err = nativeTextTemplateJSON(malformed, resolver, func(uint64) error { return nil }); err == nil {
		t.Fatal("trailing template values accepted")
	}
	if _, err = nativeTemplateMaterializationCharge(^uint64(0)); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("overflow=%v", err)
	}
}

func TestNativeTextBSONMalformedNestedSpan(t *testing.T) {
	for _, kind := range []byte{byte(bson.TypeEmbeddedDocument), byte(bson.TypeArray)} {
		for length := 0; length <= 4; length++ {
			t.Run(fmt.Sprintf("type%x/length%d", kind, length), func(t *testing.T) {
				// Full parent envelope, but a truncated child's claimed size. Four
				// bytes can pass driver Raw.Validate and previously panicked census.
				raw := []byte{12, 0, 0, 0, kind, 'x', 0, 0, 0, 0, 0, 0}
				binary.LittleEndian.PutUint32(raw[7:11], uint32(length))
				calls := 0
				out, err := materializeTextBackfillDocumentJSON(raw, collectionOptions{documentFormat: DocumentFormatBSON, textMaterializationAdmit: func(uint64) error { calls++; return nil }})
				if err == nil || out != nil || calls != 0 {
					t.Fatalf("malformed child output=%x admissions=%d err=%v", out, calls, err)
				}
			})
		}
	}
}

func TestNativeTextBSONCensusStackAdmission(t *testing.T) {
	var document any = bson.D{{Key: "leaf", Value: "policy"}}
	for level := 0; level < 65; level++ {
		document = bson.D{{Key: "child", Value: document}, {Key: "sibling", Value: true}}
	}
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	var cohorts []uint64
	count, err := nativeTextBSONElements(raw, func(n uint64) error { cohorts = append(cohorts, n); return nil })
	if err != nil || count != 131 || len(cohorts) < 2 {
		t.Fatalf("deep census elements=%d cohorts=%v err=%v", count, cohorts, err)
	}
	for _, refuse := range []int{1, len(cohorts)} {
		calls := 0
		_, err = nativeTextBSONElements(raw, func(uint64) error {
			calls++
			if calls == refuse {
				return memtable.ErrCOWCapacity
			}
			return nil
		})
		if !errors.Is(err, memtable.ErrCOWCapacity) || calls != refuse {
			t.Fatalf("stack cohort refusal=%d calls=%d err=%v", refuse, calls, err)
		}
	}
}
