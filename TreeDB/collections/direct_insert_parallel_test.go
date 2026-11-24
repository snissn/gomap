package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestDirectInsertBatchParallelMultiRootPublishAndReopen(t *testing.T) {
	modes := []struct {
		name       string
		commandWAL bool
		reverseIDs bool
	}{
		{name: "no_wal_sorted", commandWAL: false, reverseIDs: false},
		{name: "no_wal_unsorted", commandWAL: false, reverseIDs: true},
		{name: "command_wal_relaxed_sorted", commandWAL: true, reverseIDs: false},
		{name: "command_wal_relaxed_unsorted", commandWAL: true, reverseIDs: true},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			openOpts := func() backenddb.Options {
				opts := backenddb.Options{
					Dir:        dir,
					Durability: backenddb.DurabilityWALOffRelaxed,
				}
				if tc.commandWAL {
					opts.Durability = backenddb.DurabilityWALOnRelaxed
					opts.CommandWAL = true
				}
				return opts
			}

			db, err := backenddb.Open(openOpts())
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			mgr := NewCollectionManager(db)
			// Disable indexed write memtables to force direct multi-root publish with
			// 2 secondary indexes (1 unique + 1 non-unique) -> 4 disjoint roots.
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name: "users",
				Options: CollectionOptions{
					DisableIndexedWriteMemtables: true,
				},
				Indexes: []IndexDefinition{
					{Name: "age_idx", Field: "age", ValueType: IndexValueInt64},
					{Name: "email_idx", Field: "email", ValueType: IndexValueString, Unique: true},
				},
			}); err != nil {
				_ = db.Close()
				t.Fatalf("create collection: %v", err)
			}
			col, err := mgr.OpenCollection("users")
			if err != nil {
				_ = db.Close()
				t.Fatalf("open collection: %v", err)
			}

			const totalDocs = 128
			const batchSize = 32
			for start := 0; start < totalDocs; start += batchSize {
				ids := make([][]byte, 0, batchSize)
				docs := make([][]byte, 0, batchSize)
				for i := 0; i < batchSize; i++ {
					idx := start + i
					if tc.reverseIDs {
						idx = start + (batchSize - 1 - i)
					}
					ids = append(ids, []byte(fmt.Sprintf("doc-%04d", idx)))
					docs = append(docs, []byte(fmt.Sprintf(`{"age":%d,"email":"user-%04d@example.com"}`, idx%16, idx)))
				}
				gotIDs, err := col.InsertBatch(ids, docs)
				if err != nil {
					_ = db.Close()
					t.Fatalf("insert batch start=%d: %v", start, err)
				}
				if len(gotIDs) != batchSize {
					_ = db.Close()
					t.Fatalf("insert batch start=%d len=%d want=%d", start, len(gotIDs), batchSize)
				}
				for i := range ids {
					if !bytes.Equal(gotIDs[i], ids[i]) {
						_ = db.Close()
						t.Fatalf("insert batch start=%d id[%d]=%q want=%q", start, i, gotIDs[i], ids[i])
					}
				}
			}

			// Duplicate primary and unique conflicts must still reject cleanly.
			if _, err := col.InsertBatch(
				[][]byte{[]byte("doc-0005"), []byte("doc-9999")},
				[][]byte{[]byte(`{"age":1,"email":"dup-primary@example.com"}`), []byte(`{"age":1,"email":"fresh@example.com"}`)},
			); !errors.Is(err, ErrDocumentExists) {
				_ = db.Close()
				t.Fatalf("expected ErrDocumentExists, got %v", err)
			}
			if _, err := col.InsertBatch(
				[][]byte{[]byte("doc-9998"), []byte("doc-9999")},
				[][]byte{[]byte(`{"age":1,"email":"user-0007@example.com"}`), []byte(`{"age":1,"email":"fresh2@example.com"}`)},
			); !errors.Is(err, ErrUniqueIndexConflict) {
				_ = db.Close()
				t.Fatalf("expected ErrUniqueIndexConflict, got %v", err)
			}

			if err := db.Close(); err != nil {
				t.Fatalf("close db: %v", err)
			}

			// Reopen and verify primary + both secondary indexes.
			db, err = backenddb.Open(openOpts())
			if err != nil {
				t.Fatalf("reopen db: %v", err)
			}
			defer func() { _ = db.Close() }()
			mgr = NewCollectionManager(db)
			col, err = mgr.OpenCollection("users")
			if err != nil {
				t.Fatalf("reopen collection: %v", err)
			}

			for i := 0; i < totalDocs; i++ {
				id := []byte(fmt.Sprintf("doc-%04d", i))
				want := []byte(fmt.Sprintf(`{"age":%d,"email":"user-%04d@example.com"}`, i%16, i))
				got, err := col.Get(id)
				if err != nil {
					t.Fatalf("get %q: %v", id, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("get %q = %q want %q", id, got, want)
				}
			}
			// Verify unique secondary index lookup.
			emailMatches, err := col.FindByIndex("email_idx", "user-0042@example.com")
			if err != nil {
				t.Fatalf("find email_idx: %v", err)
			}
			if len(emailMatches) != 1 || string(emailMatches[0]) != "doc-0042" {
				t.Fatalf("email_idx lookup got %d matches: %q", len(emailMatches), emailMatches)
			}
			// Verify non-unique secondary index range query.
			ageMatches, _, err := col.FindByIndexRange("age_idx", IndexRangeOptions{
				Lower: IndexRangeBound{Value: int64(3), Inclusive: true},
				Upper: IndexRangeBound{Value: int64(3), Inclusive: true},
			})
			if err != nil {
				t.Fatalf("find age_idx range: %v", err)
			}
			if len(ageMatches) != totalDocs/16 {
				t.Fatalf("age_idx range got %d matches want %d", len(ageMatches), totalDocs/16)
			}
		})
	}
}
