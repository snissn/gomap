package collections

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionManagerStatsResetPerIndexAggregatesAfterDropIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString},
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
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"email":"grace@example.com","city":"hnl"}`), true, nil
		},
	}}); err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	beforeDrop := mgr.StatsSnapshot()
	if got := collectionStatsIndexByNameForTest(t, beforeDrop, "email").Changed; got == 0 {
		t.Fatalf("email changed aggregate before drop=%d want positive", got)
	}

	if _, err := col.DropIndex("email"); err != nil {
		t.Fatalf("drop email index: %v", err)
	}
	afterDrop := mgr.StatsSnapshot()
	if got, want := afterDrop.UpdateBatchIndexStatsCount, 1; got != want {
		t.Fatalf("index stats count after drop=%d want %d", got, want)
	}
	city := afterDrop.UpdateBatchIndexStats[0]
	if city.IndexName != "city" || city.IndexOrdinal != 0 {
		t.Fatalf("remaining index stat after drop=%+v want city ordinal 0", city)
	}
	if city.Changed != 0 ||
		city.Unchanged != 0 ||
		city.UniqueChecks != 0 ||
		city.UniqueCheckSkips != 0 ||
		city.SecondaryRuns != 0 ||
		city.SecondaryDeletes != 0 ||
		city.SecondarySets != 0 ||
		city.SecondaryKeyBytes != 0 {
		t.Fatalf("remaining city stats after drop=%+v want reset counters", city)
	}
}

func TestCollectionManagerStatsResetPerIndexAggregatesAfterDropIndexes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString},
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "status", Field: "status", ValueType: IndexValueString},
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
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl","status":"active"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"email":"grace@example.com","city":"sea","status":"active"}`), true, nil
		},
	}}); err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	beforeDrop := mgr.StatsSnapshot()
	if got := collectionStatsIndexByNameForTest(t, beforeDrop, "email").Changed; got == 0 {
		t.Fatalf("email changed aggregate before drop=%d want positive", got)
	}
	if got := collectionStatsIndexByNameForTest(t, beforeDrop, "city").Changed; got == 0 {
		t.Fatalf("city changed aggregate before drop=%d want positive", got)
	}

	if _, err := col.DropIndexes([]string{"email", "status"}); err != nil {
		t.Fatalf("drop email/status indexes: %v", err)
	}
	afterDrop := mgr.StatsSnapshot()
	if got, want := afterDrop.UpdateBatchIndexStatsCount, 1; got != want {
		t.Fatalf("index stats count after DropIndexes=%d want %d", got, want)
	}
	city := afterDrop.UpdateBatchIndexStats[0]
	if city.IndexName != "city" || city.IndexOrdinal != 0 {
		t.Fatalf("remaining index stat after DropIndexes=%+v want city ordinal 0", city)
	}
	if city.Changed != 0 ||
		city.Unchanged != 0 ||
		city.UniqueChecks != 0 ||
		city.UniqueCheckSkips != 0 ||
		city.SecondaryRuns != 0 ||
		city.SecondaryDeletes != 0 ||
		city.SecondarySets != 0 ||
		city.SecondaryKeyBytes != 0 {
		t.Fatalf("remaining city stats after DropIndexes=%+v want reset counters", city)
	}
}

func collectionStatsIndexByNameForTest(tb testing.TB, stats CollectionManagerStats, name string) CollectionUpdateIndexStats {
	tb.Helper()
	for _, stat := range stats.UpdateBatchIndexStats[:stats.UpdateBatchIndexStatsCount] {
		if stat.IndexName == name {
			return stat
		}
	}
	tb.Fatalf("missing index stat %q in %+v", name, stats.UpdateBatchIndexStats[:stats.UpdateBatchIndexStatsCount])
	return CollectionUpdateIndexStats{}
}
