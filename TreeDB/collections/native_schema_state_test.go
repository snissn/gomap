package collections

import (
	"errors"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeSchemaOwnsBackingAndSharesUntilExplicitReplacement(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(bytes uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(bytes) }
	nameBacking := strings.Repeat("padding", 1000) + "collection"
	meta := CollectionMeta{
		Name:        nameBacking[len(nameBacking)-len("collection"):],
		Indexes:     []IndexDefinition{{Name: "index", Field: "field", Components: []IndexComponent{{Field: "field"}}}},
		TextIndexes: []TextIndexDefinition{{Name: "text", Fields: []TextIndexField{{Field: "body"}}, AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"stop"}}}},
	}
	schema, err := newNativeSchemaState(&meta, admit)
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.StringData(meta.Name) == unsafe.StringData(schema.meta.Name) {
		t.Fatal("schema retained caller substring backing")
	}
	meta.Indexes[0].Components[0].Field = "changed"
	meta.TextIndexes[0].AnalyzerOptions.StopWords[0] = "changed"
	if got := schema.meta.Indexes[0].Components[0].Field; got != "field" {
		t.Fatal("caller changed immutable schema", got)
	}
	if got := schema.meta.TextIndexes[0].AnalyzerOptions.StopWords[0]; got != "stop" {
		t.Fatal("caller changed analyzer schema", got)
	}
	if allocations := testing.AllocsPerRun(100, func() { schema.retain(); completion := schema.release(); completion.Complete() }); allocations != 0 {
		t.Fatalf("schema retain/release allocated %g", allocations)
	}
	budget.Close()
	completion := schema.release()
	if budget.Stats().TotalBytes == 0 {
		t.Fatal("descriptor refunded before owner detach")
	}
	schema = nil
	completion.Complete()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeSchemaRefusalDoesNotAllocateOrCopyMetadata(t *testing.T) {
	meta := CollectionMeta{Name: "collection", Indexes: []IndexDefinition{{Name: "index", Field: "field", Components: []IndexComponent{{Field: "field"}}}}}
	refused := errors.New("original budget refused")
	admit := func(uint64) (cowbtree.OwnedMapLease, error) { return nil, refused }
	var got error
	if allocations := testing.AllocsPerRun(100, func() { _, got = newNativeSchemaState(&meta, admit) }); allocations != 0 {
		t.Fatalf("metadata allocated before refusal: %g", allocations)
	}
	if !errors.Is(got, refused) {
		t.Fatal(got)
	}
	if meta.Indexes[0].Components[0].Field != "field" {
		t.Fatal("refused schema changed input")
	}
}
