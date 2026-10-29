package collections

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Repeated public scalar publications must request the existing checkpoint
// owner when source slots exhaust, even below the byte maintenance watermark.
func TestNativeScalarPublicSourcePressure(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			_, _, c := openNativeTextPublic(t, t.TempDir(), profile, true)
			original := []byte(`{"tenant":"original","content":"held source"}`)
			if _, err := c.Insert([]byte("row-00000"), original); err != nil {
				t.Fatal(err)
			}
			held, err := c.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			ids := [][]byte{[]byte("row-00000")}
			before, err := held.FetchDocumentsByID(ids, DocumentFetchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			budget := held.nativeRead.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
			for i := 0; i < treedb.DefaultCOWMemtableLimits().MaxSources+4; i++ {
				beforeWrite := budget.Stats()
				id := []byte(fmt.Sprintf("new-%03d", i))
				if _, err := c.Insert(id, []byte(`{"tenant":"new","content":"current source"}`)); err != nil {
					t.Fatalf("insert %d: %v; before=%+v stats=%+v limits=%+v", i, err, beforeWrite, budget.Stats(), budget.Limits())
				}
			}
			after, err := held.FetchDocumentsByID(ids, DocumentFetchOptions{})
			if err != nil || !before.Results[0].Found || !after.Results[0].Found || !bytes.Equal(before.Results[0].Document, after.Results[0].Document) {
				t.Fatalf("held document changed: before=%+v after=%+v err=%v", before, after, err)
			}
			rows, err := c.FindByIndex("tenant", "new")
			if err != nil || len(rows) != treedb.DefaultCOWMemtableLimits().MaxSources+4 {
				t.Fatalf("scalar read after source pressure: rows=%q err=%v", rows, err)
			}
			requireNativeTextIDs(t, c, "held", "row-00000")
		})
	}
}
