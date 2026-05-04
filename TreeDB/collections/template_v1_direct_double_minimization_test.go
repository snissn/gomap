package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionDirectUpdateTemplateV1DoubleSameEffectiveSkipsSecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "scores",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatTemplateV1,
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "score", Field: "score", ValueType: IndexValueDouble},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("scores")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const (
		oldScore = "2.5"
		newScore = "3.5"
	)
	seed := mustTemplateV1DoubleDocument(t, oldScore, "before")
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{seed}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{
		collectionPrimaryRootName("scores"),
		collectionTemplateRootName("scores"),
		collectionSecondaryRootName("scores", "score"),
	}
	before := mustTemplateV1DirectDoubleRootIDs(t, d, "scores", rootNames...)
	assertTemplateV1DirectDoubleIDs(t, col, 2.5, "u1")

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return mustTemplateV1DoubleDocument(t, oldScore, "after"), true, nil
	})
	if err != nil {
		t.Fatalf("direct template-v1 same-score update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct template-v1 same-score update matched=%v modified=%v want true/true", matched, modified)
	}

	afterSame := mustTemplateV1DirectDoubleRootIDs(t, d, "scores", rootNames...)
	if afterSame[collectionPrimaryRootName("scores")] == before[collectionPrimaryRootName("scores")] {
		t.Fatalf("primary root did not change for template-v1 replacement")
	}
	templateRoot := collectionTemplateRootName("scores")
	if afterSame[templateRoot] != before[templateRoot] {
		t.Fatalf("template root changed from %d to %d for same template shape", before[templateRoot], afterSame[templateRoot])
	}
	scoreRoot := collectionSecondaryRootName("scores", "score")
	if afterSame[scoreRoot] != before[scoreRoot] {
		t.Fatalf("score secondary root changed from %d to %d for same template-v1 double value", before[scoreRoot], afterSame[scoreRoot])
	}
	assertTemplateV1DirectDoubleIDs(t, col, 2.5, "u1")

	matched, modified, err = col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return mustTemplateV1DoubleDocument(t, newScore, "changed-score"), true, nil
	})
	if err != nil {
		t.Fatalf("direct template-v1 changed-score update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct template-v1 changed-score update matched=%v modified=%v want true/true", matched, modified)
	}
	afterChanged := mustTemplateV1DirectDoubleRootIDs(t, d, "scores", rootNames...)
	if afterChanged[scoreRoot] == afterSame[scoreRoot] {
		t.Fatalf("score secondary root did not change for changed template-v1 double value")
	}
	if afterChanged[templateRoot] != afterSame[templateRoot] {
		t.Fatalf("template root changed from %d to %d for same shape score update", afterSame[templateRoot], afterChanged[templateRoot])
	}
	assertTemplateV1DirectDoubleIDs(t, col, 2.5)
	assertTemplateV1DirectDoubleIDs(t, col, 3.5, "u1")
}

func mustTemplateV1DoubleDocument(t *testing.T, score string, note string) []byte {
	t.Helper()
	doc, err := EncodeTemplateV1DocumentJSON([]byte(`{"score":{"$numberDouble":"` + score + `"},"note":"` + note + `"}`))
	if err != nil {
		t.Fatalf("encode template-v1 document: %v", err)
	}
	return doc
}

func mustTemplateV1DirectDoubleRootIDs(t *testing.T, d *backenddb.DB, collectionName string, rootNames ...string) map[string]uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("missing catalog for collection %q", collectionName)
	}
	out := make(map[string]uint64, len(rootNames))
	for _, name := range rootNames {
		rootID := catalog.rootID(name)
		if rootID == 0 {
			t.Fatalf("root %q was not persisted", name)
		}
		out[name] = rootID
	}
	return out
}

func assertTemplateV1DirectDoubleIDs(t *testing.T, col *Collection, score float64, want ...string) {
	t.Helper()
	ids, err := col.FindByIndexValue("score", score)
	if err != nil {
		t.Fatalf("find score %v: %v", score, err)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if len(got) != len(wantSorted) {
		t.Fatalf("score %v ids=%q want %q", score, ids, want)
	}
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			t.Fatalf("score %v ids=%q want %q", score, ids, want)
		}
	}
}
