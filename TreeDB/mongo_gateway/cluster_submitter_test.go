package mongogateway

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	treenativewire "github.com/snissn/gomap/TreeDB/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoClusterSubmitterCall struct {
	entry    raftentry.CommandEntryV1
	metadata treenativewire.ClusterRequestMetadata
}

type mongoClusterFakeSubmitter struct {
	mu    sync.Mutex
	calls []mongoClusterSubmitterCall
}

func (f *mongoClusterFakeSubmitter) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata treenativewire.ClusterRequestMetadata) (treenativewire.ClusterSubmitResult, error) {
	if ctx == nil {
		return treenativewire.ClusterSubmitResult{}, fmt.Errorf("nil submit context")
	}
	decoded, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{RequestMetadata: metadata})
	if err != nil {
		return treenativewire.ClusterSubmitResult{}, err
	}
	f.mu.Lock()
	f.calls = append(f.calls, mongoClusterSubmitterCall{entry: decoded, metadata: metadata})
	f.mu.Unlock()
	return treenativewire.ClusterSubmitResult{
		ActualAck:        iwire.AckVisible,
		ResponseSections: mongoClusterFakeResponseSections(decoded.Decoded),
	}, nil
}

func (f *mongoClusterFakeSubmitter) snapshotCalls() []mongoClusterSubmitterCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]mongoClusterSubmitterCall(nil), f.calls...)
}

func mongoClusterFakeResponseSections(entry iwire.DeterministicEntry) []iwire.Section {
	ids := mongoClusterTestIDs(entry.Sections)
	switch entry.CommandID {
	case iwire.CommandInsertBatch:
		return []iwire.Section{mongoClusterTestMeta("inserted_count", len(ids))}
	case iwire.CommandUpdateBSONSet:
		return []iwire.Section{mongoClusterTestMeta("matched_count", 1, "modified_count", 1)}
	case iwire.CommandDeleteBatch:
		return []iwire.Section{mongoClusterTestMeta("deleted_count", len(ids))}
	default:
		return []iwire.Section{mongoClusterTestMeta()}
	}
}

func mongoClusterTestMeta(counts ...any) iwire.Section {
	values := []struct {
		key   string
		value string
	}{{key: "actual_ack_policy", value: "1"}}
	for i := 0; i+1 < len(counts); i += 2 {
		values = append(values, struct {
			key   string
			value string
		}{key: counts[i].(string), value: fmt.Sprint(counts[i+1])})
	}
	payload := binary.AppendUvarint(nil, uint64(len(values)))
	for _, value := range values {
		payload = mongoClusterAppendString(payload, value.key)
		payload = mongoClusterAppendString(payload, value.value)
	}
	return iwire.Section{ID: iwire.SectionResponseMeta, Bytes: payload}
}

func mongoClusterAppendString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func mongoClusterTestIDs(sections []iwire.Section) [][]byte {
	for _, section := range sections {
		if section.ID == iwire.SectionDocumentIDs {
			ids, err := iwire.DecodeByteVectorItems(section.Bytes, iwire.Limits{})
			if err != nil {
				return nil
			}
			return ids
		}
	}
	return nil
}

func TestClusterSubmitterInsertRoutesCommandEntryNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter

	response := serveCommand(t, server, 325801, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("command id=%d want insert_batch", got)
	}
	if _, err := server.Collections.OpenCollection("app.users"); err == nil {
		t.Fatal("cluster insert created local collection")
	} else if err != collections.ErrCollectionNotFound {
		t.Fatalf("OpenCollection after cluster insert err=%v want collection not found", err)
	}
}

func TestClusterSubmitterUpdateBSONSetRoutesCountsAndNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 325802, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	}))

	submitter := &mongoClusterFakeSubmitter{}
	server.ClusterSubmitter = submitter
	response := serveCommand(t, server, 325803, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	assertInt32(t, response, "nModified", 1)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandUpdateBSONSet {
		t.Fatalf("command id=%d want update_bson_set", got)
	}
	found := serveCommand(t, server, 325804, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(t, found)
	if len(batch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(batch))
	}
	if got, ok := batch[0].Lookup("name").StringValueOK(); !ok || got != "Ada" {
		t.Fatalf("local name after cluster update=%q ok=%v want Ada", got, ok)
	}
}

func TestClusterSubmitterDeleteRoutesCommandEntry(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter

	response := serveCommand(t, server, 325805, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandDeleteBatch {
		t.Fatalf("command id=%d want delete_batch", got)
	}
}

func TestClusterSubmitterRejectsWriteConcern(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter

	response := serveCommand(t, server, 325806, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterRejectsUnsupportedClusterMutationNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 325807, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	}))

	submitter := &mongoClusterFakeSubmitter{}
	server.ClusterSubmitter = submitter
	response := serveCommand(t, server, 325808, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
	found := serveCommand(t, server, 325809, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(t, found)
	if len(batch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(batch))
	}
	if got, ok := batch[0].Lookup("age").Int32OK(); !ok || got != 1 {
		t.Fatalf("local age after rejected cluster update=%d ok=%v want 1", got, ok)
	}
}
