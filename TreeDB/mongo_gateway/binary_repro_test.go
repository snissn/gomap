package mongogateway

import (
	"bytes"
	"testing"
	"go.mongodb.org/mongo-driver/v2/bson"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestServerAllowsBinaryBSONType(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	
	binaryData := []byte{0x01, 0x02, 0x03, 0x04}
	commandDoc := mustDocument(t, bson.D{
		{Key: "insert", Value: "testcollection"},
		{Key: "documents", Value: bson.A{
			bson.D{
				{Key: "_id", Value: 1},
				{Key: "field1", Value: bson.Binary{Data: binaryData, Subtype: 0x00}},
			},
		}},
		{Key: "$db", Value: "testdb"},
	})

	req, err := wire.AppendMsgMessage(nil, 213, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := server.ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	resp := readMsgResponse(t, rw.w.Bytes(), 213)
	
	// ASSERTION: Should succeed (ok=1)
	// This will fail currently because the gateway rejects the binary type.
	assertOK(t, resp)
}
