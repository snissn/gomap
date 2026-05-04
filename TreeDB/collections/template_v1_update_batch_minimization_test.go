package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1UpdateBatchSeparatesTemplateAndSecondaryRootWork(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatTemplateV1,
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{templateV1UpdateBatchMinDoc(t, `{"email":"ada@example.com","city":"hnl","note":"seed"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := templateV1UpdateBatchMinRoots(t, d)
	templateV1UpdateBatchMinRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	templateV1UpdateBatchMinRequireIndexIDs(t, col, "city", "hnl", "u1")

	sameTemplateDoc := templateV1UpdateBatchMinDoc(t, `{"email":"ada@example.com","city":"hnl","note":"same-template"}`)
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return sameTemplateDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch same template/index values: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("same-template results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("same-template secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("same-template index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 2; got != want {
		t.Fatalf("same-template index unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexChecks, 0; got != want {
		t.Fatalf("same-template unique checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 1; got != want {
		t.Fatalf("same-template unique skips=%d want %d", got, want)
	}
	afterSameTemplate := templateV1UpdateBatchMinRoots(t, d)
	for _, rootName := range []string{
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if afterSameTemplate[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for template-v1 UpdateBatch with unchanged template/index values", rootName, before[rootName], afterSameTemplate[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("users"); afterSameTemplate[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified template-v1 replacement", rootName)
	}

	newTemplateDoc := templateV1UpdateBatchMinDoc(t, `{"email":"ada@example.com","city":"hnl","note":"new-template","updated":true}`)
	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return newTemplateDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch new template shape: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("new-template results=%+v want one matched modified row", got)
	}
	stats = col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("new-template secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("new-template index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 2; got != want {
		t.Fatalf("new-template index unchanged=%d want %d", got, want)
	}
	afterNewTemplate := templateV1UpdateBatchMinRoots(t, d)
	if rootName := collectionTemplateRootName("users"); afterNewTemplate[rootName] == afterSameTemplate[rootName] {
		t.Fatalf("template root %q did not change for new template-v1 shape", rootName)
	}
	for _, rootName := range []string{
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if afterNewTemplate[rootName] != afterSameTemplate[rootName] {
			t.Fatalf("secondary root %q changed from %d to %d for template-only shape update", rootName, afterSameTemplate[rootName], afterNewTemplate[rootName])
		}
	}

	cityDoc := templateV1UpdateBatchMinDoc(t, `{"email":"ada@example.com","city":"sea","note":"city-changed","updated":true}`)
	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return cityDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch city value: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("city results=%+v want one matched modified row", got)
	}
	stats = col.LastUpdateStats()
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("city update index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("city update index unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexChecks, 0; got != want {
		t.Fatalf("city update unique checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 1; got != want {
		t.Fatalf("city update unique skips=%d want %d", got, want)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("city update secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "city" || run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("city secondary run stats=%+v want city delete+set with key bytes", run)
	}
	afterCity := templateV1UpdateBatchMinRoots(t, d)
	if rootName := collectionSecondaryRootName("users", "city"); afterCity[rootName] == afterNewTemplate[rootName] {
		t.Fatalf("city root %q did not change for indexed city update", rootName)
	}
	for _, rootName := range []string{
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "email"),
	} {
		if afterCity[rootName] != afterNewTemplate[rootName] {
			t.Fatalf("root %q changed from %d to %d for city-only template-v1 UpdateBatch", rootName, afterNewTemplate[rootName], afterCity[rootName])
		}
	}
	templateV1UpdateBatchMinRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	templateV1UpdateBatchMinRequireIndexIDs(t, col, "city", "hnl")
	templateV1UpdateBatchMinRequireIndexIDs(t, col, "city", "sea", "u1")
}

func templateV1UpdateBatchMinDoc(tb testing.TB, rawJSON string) []byte {
	tb.Helper()
	doc, err := EncodeTemplateV1DocumentJSON([]byte(rawJSON))
	if err != nil {
		tb.Fatalf("encode template-v1 document %s: %v", rawJSON, err)
	}
	return doc
}

func templateV1UpdateBatchMinRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		tb.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		tb.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("users"),
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	}
	roots := make(map[string]uint64, len(names))
	for _, name := range names {
		rootID := catalog.rootID(name)
		if rootID == 0 {
			tb.Fatalf("root %q was not persisted", name)
		}
		roots[name] = rootID
	}
	return roots
}

func templateV1UpdateBatchMinRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
	tb.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		tb.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	if len(ids) != len(want) {
		tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	for i := range want {
		if !bytes.Equal(ids[i], []byte(want[i])) {
			tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}
