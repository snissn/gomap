package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionIndexedAsyncPrepareRejectsRootBaseMismatchAndPreservesPendingVisibility(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	leftMgr := NewCollectionManager(d)
	if _, err := leftMgr.CreateCollection(&CollectionMeta{
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
	left, err := leftMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open left collection: %v", err)
	}
	if _, err := left.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("stage left pending insert: %v", err)
	}

	right, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("open right collection: %v", err)
	}
	if _, err := right.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"grace@example.com"}`)},
	); err != nil {
		t.Fatalf("insert right durable row: %v", err)
	}
	if err := right.Flush(); err != nil {
		t.Fatalf("flush right row: %v", err)
	}

	work, err := left.prepareIndexedAsyncPublish()
	if work != nil {
		if work.pin != nil {
			_ = work.pin.Close()
		}
		t.Fatal("prepare returned work despite root-base mismatch")
	}
	if !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("prepare err=%v want ErrConcurrentMutation", err)
	}

	got, err := left.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get pending left row after mismatch: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com"}`); !bytes.Equal(got, want) {
		t.Fatalf("pending left row=%q want %q", got, want)
	}
	ids, err := left.FindByIndexValue("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find pending email after mismatch: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("pending email ids=%q want [u1]", ids)
	}
}
