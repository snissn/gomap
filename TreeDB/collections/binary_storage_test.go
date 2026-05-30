package collections

import (
	"bytes"
	"testing"
	"go.mongodb.org/mongo-driver/v2/bson"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionBSONBinaryStorage(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	manager := NewCollectionManager(db)
	
	collName := "testcoll"
	_, err = manager.CreateCollection(&CollectionMeta{
		Name: collName,
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	})
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := manager.OpenCollection(collName)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	binaryData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	doc := bson.D{
		{Key: "_id", Value: int64(1)},
		{Key: "data", Value: bson.Binary{Data: binaryData, Subtype: 0x00}},
	}
	
	bsonDoc, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Insert
	_, err = coll.Insert([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, bsonDoc)
	if err != nil {
		t.Fatalf("Collection Insert failed: %v", err)
	}

	// Get
	retrieved, err := coll.Get([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
	if err != nil {
		t.Fatalf("Collection Get failed: %v", err)
	}
	
	if !bytes.Equal(bsonDoc, retrieved) {
		t.Fatal("Retrieved document does not match inserted document")
	}
}
