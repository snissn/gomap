package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionUpdateChangedUniqueFlushesPendingReservationsBeforeConflict(t *testing.T) {
	for _, tc := range []struct {
		name     string
		queue    bool
		wantUnit int
	}{
		{name: "mutable", queue: false, wantUnit: 0},
		{name: "queued", queue: true, wantUnit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()

			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name: "users",
				Options: CollectionOptions{
					BufferedIndexedWrites: true,
				},
				Indexes: []IndexDefinition{
					{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
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
				[][]byte{[]byte(`{"email":"old@example.com"}`)},
			); err != nil {
				t.Fatalf("insert durable document: %v", err)
			}
			if err := col.Flush(); err != nil {
				t.Fatalf("flush durable document: %v", err)
			}
			if _, err := col.InsertBatch(
				[][]byte{[]byte("u2")},
				[][]byte{[]byte(`{"email":"pending@example.com"}`)},
			); err != nil {
				t.Fatalf("insert pending unique document: %v", err)
			}
			if tc.queue {
				col.writeDomain.mu.Lock()
				if !rotateIndexedMutableToFlushUnitLocked(col.writeDomain) {
					col.writeDomain.mu.Unlock()
					t.Fatal("rotate indexed mutable state returned false")
				}
				col.writeDomain.mu.Unlock()
			}
			stats := mgr.StatsSnapshot()
			if got := stats.PendingDocuments; got != 1 {
				t.Fatalf("pending documents before update=%d want 1", got)
			}
			if got := stats.PendingIndexedFlushUnits; got != tc.wantUnit {
				t.Fatalf("pending indexed flush units before update=%d want %d", got, tc.wantUnit)
			}

			matched, modified, err := col.Update([]byte("u1"), setJSONEmail("pending@example.com"))
			if !errors.Is(err, ErrUniqueIndexConflict) {
				t.Fatalf("Update err=%v want ErrUniqueIndexConflict", err)
			}
			if matched || modified {
				t.Fatalf("Update matched/modified=%v/%v want false/false on rejected unique change", matched, modified)
			}
			stats = mgr.StatsSnapshot()
			if got := stats.PendingDocuments; got != 0 {
				t.Fatalf("pending documents after rejected update=%d want 0 after forced drain", got)
			}
			if got := stats.PendingIndexedFlushUnits; got != 0 {
				t.Fatalf("pending indexed flush units after rejected update=%d want 0 after forced drain", got)
			}
			got, err := col.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("get u1: %v", err)
			}
			if !bytes.Equal(got, []byte(`{"email":"old@example.com"}`)) {
				t.Fatalf("u1 after rejected update=%q want old email", got)
			}
			got, err = col.Get([]byte("u2"))
			if err != nil {
				t.Fatalf("get u2: %v", err)
			}
			if !bytes.Equal(got, []byte(`{"email":"pending@example.com"}`)) {
				t.Fatalf("u2 after forced drain=%q want pending email", got)
			}
			ids, err := col.FindByIndexValue("email", "pending@example.com")
			if err != nil {
				t.Fatalf("find pending email: %v", err)
			}
			if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
				t.Fatalf("pending email ids=%q want [u2]", ids)
			}
		})
	}
}
