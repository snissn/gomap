package collections

import (
	"bytes"
	"errors"
	"fmt"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"strings"
	"testing"
)

// Representation-only exception: semantic controls, not a probabilistic Swiss RED.
// No adversarial Swiss hash behavior is inferred from these key populations.
func TestNativeTextDeterministicTermConstruction(t *testing.T) {
	for _, n := range []int{896, 897, 1024, 1025} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			var source strings.Builder
			for i := 0; i < n; i++ {
				fmt.Fprintf(&source, "word%d word%d ", i, i)
			}
			def := TextIndexDefinition{Version: TextIndexVersionV2, Analyzer: TextAnalyzerSimple, Fields: []TextIndexField{{Field: "content"}}, StorePositions: true, StoreOffsets: true}
			doc := []byte(fmt.Sprintf(`{"content":%q}`, source.String()))
			legacy, err := analyzeTextIndexDocument(def, doc)
			if err != nil {
				t.Fatal(err)
			}
			var selected [1]jsonParserIndexValue
			if handled, err := selectTextIndexDocumentJSONRootValues(def, doc, selected[:]); !handled || err != nil {
				t.Fatal(err)
			}
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			lease, err := budget.AcquireExternal(0)
			if err != nil {
				budget.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				lease.Close()
				budget.Close()
				if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
					t.Errorf("boundary Close retained receipts: %+v", stats)
				}
			})
			initial := budget.Stats()
			var charge uint64
			admit := func(n uint64) error {
				if n > ^uint64(0)-charge {
					return memtable.ErrCOWCapacity
				}
				if err := lease.Resize(charge + n); err != nil {
					return err
				}
				charge += n
				return nil
			}
			state, err := nativeTextPrepareCurrent(def, selected[:], admit)
			if err != nil {
				t.Fatal(err)
			}
			if charge == 0 || !bytes.Equal(encodeTextDocumentStateValue(state), encodeTextDocumentStateValue(textDocumentStateValueFromAnalysis(legacy))) {
				t.Fatal("semantic state differs")
			}
			census, err := nativeTextRootValueTermBound(def, selected[0], admit)
			if err != nil || census != uint64(n) {
				t.Fatalf("census=%d err=%v", census, err)
			}
			acc := &textTermAccumulator{admit: admit}
			acc.terms = acc.inline[:0]
			for i := 0; i < n; i++ {
				if err := acc.AddTextToken(TextToken{Term: fmt.Sprintf("word%d", i)}); err != nil {
					t.Fatal(err)
				}
			}
			if len(acc.terms) != n {
				t.Fatal("terms lost")
			}
			if stats := budget.Stats(); charge == 0 || stats.ReservedBytes != initial.ReservedBytes+charge || stats.ExternalLeases != 1 {
				t.Fatalf("actual boundary receipt differs: charged=%d stats=%+v", charge, stats)
			}
		})
	}
}

// The real original finite owner refuses combined cell/lane growth BEFORE an
// existing value or a new key changes. Successful prefixes remain charged until
// Close; removing actual pressure permits that SAME operation to succeed.
func TestNativeTextConstructorOriginalLeaseRefusal(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.AcquireExternal(0)
	if err != nil {
		t.Fatal(err)
	}
	var charged uint64
	admit := func(n uint64) error {
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	}
	values, err := newTextConstructorValues[int](admit)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err = values.set(fmt.Sprint(i), i+1); err != nil {
			t.Fatal(err)
		}
	}
	blocker, err := budget.AcquireExternal(0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		values = nil
		blocker.Close()
		lease.Close()
		budget.Close()
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.ExternalLeases != 0 {
			t.Errorf("Close retained receipts: %+v", stats)
		}
	}()
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if err = blocker.Resize(available); err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	prefix := charged
	if err = values.set("next", 9); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("finite original parent did not refuse growth: %v", err)
	}
	if _, ok := values.get("next"); ok {
		t.Fatal("refused key published")
	}
	for i := 0; i < 8; i++ {
		if got, ok := values.get(fmt.Sprint(i)); !ok || got != i+1 {
			t.Fatal("refusal changed successful prefix")
		}
	}
	if after := budget.Stats(); after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases || charged != prefix {
		t.Fatalf("refusal changed receipts: before=%+v after=%+v", before, after)
	}
	blocker.Close()
	if err = values.set("next", 9); err != nil {
		t.Fatal(err)
	}
	if got, ok := values.get("next"); !ok || got != 9 || charged <= prefix {
		t.Fatal("retry did not use original admitted owner")
	}
}

func TestNativeTextNestedConcreteAnalysisParity(t *testing.T) {
	def := TextIndexDefinition{Version: TextIndexVersionV2, Analyzer: TextAnalyzerSimple, Fields: []TextIndexField{{Field: "article.content"}}, StorePositions: true, StoreOffsets: true, AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"THE", "the", ""}}}
	documents := [][]byte{
		[]byte(`{"article":{"content":"first"},"article":{"content":"The last last 東京"}}`),
		[]byte(`{"article":{"content":"first"},"article":{"other":"replacement"}}`),
		[]byte(`{"art\u0069cle":{"cont\u0065nt":["","The alpha",7,null,"beta\u0020gamma",""]}}`),
		[]byte(`{"article":[{"content":"ignored"}]}`),
		[]byte(`{"article":{"content":"\ud800 alpha"},"\ud800":"irrelevant"}`),
		[]byte("{\"article\":{\"content\":\"alpha " + string([]byte{0xff}) + " beta\"},\"" + string([]byte{0xff}) + "\":1}"),
		[]byte(`{"article":{"content":123456789012345678901234567890}}`),
		[]byte(`{"article":{"content":"alpha"}} {}`), []byte(`[]`), []byte(`{"article":`),
	}
	for i, doc := range documents {
		legacy, oldErr := analyzeTextIndexDocument(def, doc)
		var charge uint64
		got, err := nativeTextDocumentState(def, doc, collectionOptions{textMaterializationAdmit: func(n uint64) error { charge += n; return nil }})
		if (err == nil) != (oldErr == nil) {
			t.Fatalf("case%d error differs: old=%v new=%v", i, oldErr, err)
		}
		if err == nil && (!bytes.Equal(encodeTextDocumentStateValue(got), encodeTextDocumentStateValue(textDocumentStateValueFromAnalysis(legacy))) || charge == 0) {
			t.Fatalf("case%d nested state differs", i)
		}
	}
}

// A real ordinary native phase covers both one document crossing the original
// Swiss cardinality boundary and a union of individually small documents.
func TestNativeTextPublicConstructorUnion897(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		t.Run(string(version), func(t *testing.T) {
			owner, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true, version)
			read, err := c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
			read.Close()
			t.Cleanup(func() {
				if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
					t.Errorf("full Close retained constructor receipts: %+v", stats)
				}
			})
			// Cleanup functions run in reverse registration order. Close explicitly here
			// so the zero oracle precedes the fixture's idempotent final cleanup.
			defer func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			document := func(text string) []byte {
				return []byte(fmt.Sprintf(`{"tenant":"a","embedding":[1,0],"content":%q}`, text))
			}
			var source strings.Builder
			for i := 0; i < 897; i++ {
				fmt.Fprintf(&source, "wide%d ", i)
			}
			if _, err = c.Insert([]byte("wide"), document(source.String())); err != nil {
				t.Fatal(err)
			}
			requireNativeTextIDs(t, c, "wide896", "wide")
			ids := make([][]byte, 33)
			docs := make([][]byte, 33)
			for row := range ids {
				ids[row] = []byte(fmt.Sprintf("small%d", row))
				source.Reset()
				for term := 0; term < 28; term++ {
					fmt.Fprintf(&source, "union%d ", row*28+term)
				}
				docs[row] = document(source.String())
			}
			if _, err = c.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			requireNativeTextIDs(t, c, "union923", "small32")
			matched, modified, err := c.Update([]byte("wide"), func([]byte) ([]byte, bool, error) { return document("replacement"), true, nil })
			if err != nil || !matched || !modified {
				t.Fatalf("update matched=%v modified=%v err=%v", matched, modified, err)
			}
			requireNativeTextIDs(t, c, "wide896")
			requireNativeTextIDs(t, c, "replacement", "wide")
			if deleted, err := c.DeleteDocument(ids[32]); err != nil || !deleted {
				t.Fatalf("delete=%v err=%v", deleted, err)
			}
			requireNativeTextIDs(t, c, "union923")
			requireNativeTextIDs(t, c, "union0", "small0")
		})
	}
}
