package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMutationCommandsRoundTrip(t *testing.T) {
	client, mgr, db := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
		Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	ids, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","name":"Ada"}`),
			[]byte(`{"email":"grace@example.com","name":"Grace"}`),
		},
		AckFlushed,
	)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if len(ids) != 2 || string(ids[0]) != "u1" || string(ids[1]) != "u2" {
		t.Fatalf("insert ids=%q", ids)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("direct get: %v", err)
	}
	if !bytes.Contains(doc, []byte(`"Ada"`)) {
		t.Fatalf("u1 doc=%s", doc)
	}
	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	handleIDs, err := client.InsertBatchHandle(ctx, handle, collections.DocumentFormatJSON,
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"katherine@example.com","name":"Katherine"}`)},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("InsertBatchHandle: %v", err)
	}
	if len(handleIDs) != 1 || string(handleIDs[0]) != "u3" {
		t.Fatalf("handle insert ids=%q", handleIDs)
	}
	if err := client.InsertBatchHandleNoResult(ctx, handle, collections.DocumentFormatJSON,
		[][]byte{[]byte("u4")},
		[][]byte{[]byte(`{"email":"dorothy@example.com","name":"Dorothy"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatchHandleNoResult: %v", err)
	}
	noResultDoc, err := col.Get([]byte("u4"))
	if err != nil {
		t.Fatalf("direct get no-result insert: %v", err)
	}
	if !bytes.Contains(noResultDoc, []byte(`"Dorothy"`)) {
		t.Fatalf("u4 doc=%s", noResultDoc)
	}
	guard, err := client.replicatedMutationGuard(ctx, "insert_batch_no_ids")
	if err != nil {
		t.Fatalf("mutation guard: %v", err)
	}
	noIDsBody, err := appendInsertBatchRequestBodyRefFlags(nil, "users", 0, false, collections.DocumentFormatJSON,
		[][]byte{[]byte("u5")},
		[][]byte{[]byte(`{"email":"mary@example.com","name":"Mary"}`)},
		AckVisible,
		iwire.CommandFlagOmitResultIDs,
		guard,
	)
	if err != nil {
		t.Fatalf("append no-result insert: %v", err)
	}
	_, noIDsResponse, err := client.roundTrip(ctx, iwire.FrameRequest, noIDsBody, iwire.FrameResponse)
	if err != nil {
		t.Fatalf("roundTrip no-result insert: %v", err)
	}
	noIDsSections, err := iwire.DecodeSections(noIDsResponse, client.limits)
	if err != nil {
		t.Fatalf("decode no-result response: %v", err)
	}
	if _, ok, err := singletonSection(noIDsSections, iwire.SectionDocumentIDs); err != nil {
		t.Fatalf("no-result document_ids section: %v", err)
	} else if ok {
		t.Fatalf("no-result response unexpectedly included document_ids")
	}
	if inserted, err := responseCount(noIDsSections, "inserted_count"); err != nil || inserted != 1 {
		t.Fatalf("no-result inserted_count=%d err=%v want 1", inserted, err)
	}

	matched, modified, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("missing")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","name":"Ada Lovelace"}`),
			[]byte(`{"email":"nobody@example.com","name":"Nobody"}`),
		},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}
	if matched != 1 || modified != 1 {
		t.Fatalf("matched=%d modified=%d want 1/1", matched, modified)
	}
	deleted, err := client.DeleteBatch(ctx, "users", [][]byte{[]byte("u2"), []byte("missing")}, AckSynced)
	if err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d want 1", deleted)
	}
	if err := client.FlushCollection(ctx, "users"); err != nil {
		t.Fatalf("FlushCollection: %v", err)
	}
	if err := client.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if err := client.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if stats := db.Stats(); len(stats) == 0 {
		t.Fatalf("empty backend stats after checkpoint")
	}
}

func TestMutationUpdateBSONSetRoundTrip(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	original, err := bson.Marshal(bson.D{
		{Key: "field0", Value: []byte("old")},
		{Key: "field1", Value: "keep"},
	})
	if err != nil {
		t.Fatalf("marshal original: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatBSON,
		[][]byte{[]byte("u1")},
		[][]byte{original},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	matched, modified, err := client.UpdateBSONSet(ctx, "users", []byte("u1"), []collections.BSONSetField{
		{Key: "field0", Value: mustNativewireBSONRawValue(t, []byte("new"))},
	}, AckVisible)
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if matched != 1 || modified != 1 {
		t.Fatalf("UpdateBSONSet matched=%d modified=%d want 1/1", matched, modified)
	}
	matched, modified, err = client.UpdateBSONSet(ctx, "users", []byte("missing"), []collections.BSONSetField{
		{Key: "field0", Value: mustNativewireBSONRawValue(t, []byte("new"))},
	}, AckVisible)
	if err != nil {
		t.Fatalf("UpdateBSONSet missing: %v", err)
	}
	if matched != 0 || modified != 0 {
		t.Fatalf("UpdateBSONSet missing matched=%d modified=%d want 0/0", matched, modified)
	}

	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	updated, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get updated: %v", err)
	}
	gotSubtype, gotBinary, ok := bson.Raw(updated).Lookup("field0").BinaryOK()
	if !ok || gotSubtype != 0 || string(gotBinary) != "new" {
		t.Fatalf("field0 subtype=%d value=%q ok=%v want binary new", gotSubtype, gotBinary, ok)
	}
	if got := bson.Raw(updated).Lookup("field1").StringValue(); got != "keep" {
		t.Fatalf("field1=%q want keep", got)
	}
}

func TestMutationUpdateBSONSetRejectsInvalidRequests(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("CreateCollection users: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "json_users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
	}); err != nil {
		t.Fatalf("CreateCollection json_users: %v", err)
	}

	_, _, err := client.UpdateBSONSet(ctx, "users", []byte("u1"), []collections.BSONSetField{
		{Key: "field0", Value: mustNativewireBSONRawValue(t, "a")},
		{Key: "field0", Value: mustNativewireBSONRawValue(t, "b")},
	}, AckVisible)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("duplicate field err=%v want invalid command", err)
	}

	for _, badKey := range []string{"", "_id", "$set", "a.b", "a\x00b", string([]byte{0xff})} {
		_, _, err := client.UpdateBSONSet(ctx, "users", []byte("u1"), []collections.BSONSetField{
			{Key: badKey, Value: mustNativewireBSONRawValue(t, "a")},
		}, AckVisible)
		if !isRemoteError(err, iwire.ErrInvalidCommand) {
			t.Fatalf("bad key %q err=%v want invalid command", badKey, err)
		}
	}

	_, _, err = client.UpdateBSONSet(ctx, "users", []byte("u1"), []collections.BSONSetField{
		{Key: "field0", Value: bson.RawValue{Type: bson.TypeString, Value: []byte{0xff}}},
	}, AckVisible)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("invalid raw value err=%v want invalid command", err)
	}

	_, _, err = client.UpdateBSONSet(ctx, "json_users", []byte("u1"), []collections.BSONSetField{
		{Key: "field0", Value: mustNativewireBSONRawValue(t, "a")},
	}, AckVisible)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("json collection err=%v want invalid command", err)
	}
}

func TestMutationUpdateBSONSetCommandWALAndIdempotency(t *testing.T) {
	client, mgr, _ := serveCommandWALCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	doc, err := bson.Marshal(bson.D{{Key: "field0", Value: "old"}})
	if err != nil {
		t.Fatalf("marshal original: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatBSON,
		[][]byte{[]byte("u1")},
		[][]byte{doc},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	idempotentCtx := WithIdempotencyKey(ctx, []byte("update-u1-field0"))
	fields := []collections.BSONSetField{{Key: "field0", Value: mustNativewireBSONRawValue(t, "new")}}
	matched, modified, err := client.UpdateBSONSet(idempotentCtx, "users", []byte("u1"), fields, AckVisible)
	if err != nil {
		t.Fatalf("first UpdateBSONSet: %v", err)
	}
	if matched != 1 || modified != 1 {
		t.Fatalf("first UpdateBSONSet matched=%d modified=%d want 1/1", matched, modified)
	}
	matched, modified, err = client.UpdateBSONSet(idempotentCtx, "users", []byte("u1"), fields, AckVisible)
	if err != nil {
		t.Fatalf("idempotent replay UpdateBSONSet: %v", err)
	}
	if matched != 1 || modified != 0 {
		t.Fatalf("idempotent replay matched=%d modified=%d want logical noop 1/0", matched, modified)
	}

	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	updated, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get updated: %v", err)
	}
	if got := bson.Raw(updated).Lookup("field0").StringValue(); got != "new" {
		t.Fatalf("field0=%q want new", got)
	}
}

func TestMutationInsertBatchOmitResultIDsJSONAndBSON(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	jsonDoc := []byte(`{"email":"ada@example.com","name":"Ada"}`)
	bsonDoc, err := bson.Marshal(bson.D{{Key: "email", Value: "grace@example.com"}, {Key: "name", Value: "Grace"}})
	if err != nil {
		t.Fatalf("marshal bson: %v", err)
	}

	tests := []struct {
		name       string
		collection string
		format     collections.DocumentFormat
		doc        []byte
	}{
		{name: "json", collection: "users_json", format: collections.DocumentFormatJSON, doc: jsonDoc},
		{name: "bson", collection: "users_bson", format: collections.DocumentFormatBSON, doc: bsonDoc},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
				Name: tc.collection,
				Options: collections.CollectionOptions{
					DocumentFormat: tc.format,
				},
			}); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			guard, err := client.replicatedMutationGuard(ctx, "insert_batch_no_ids_"+tc.name)
			if err != nil {
				t.Fatalf("mutation guard: %v", err)
			}
			body, err := appendInsertBatchRequestBodyRefFlags(nil, tc.collection, 0, false, tc.format,
				[][]byte{[]byte("u1")},
				[][]byte{tc.doc},
				AckVisible,
				iwire.CommandFlagOmitResultIDs,
				guard,
			)
			if err != nil {
				t.Fatalf("append insert: %v", err)
			}
			_, response, err := client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
			if err != nil {
				t.Fatalf("roundTrip omit-result insert: %v", err)
			}
			sections, err := iwire.DecodeSections(response, client.limits)
			if err != nil {
				t.Fatalf("decode omit-result response: %v", err)
			}
			if _, ok, err := singletonSection(sections, iwire.SectionDocumentIDs); err != nil {
				t.Fatalf("document_ids section: %v", err)
			} else if ok {
				t.Fatalf("omit-result response unexpectedly included document_ids")
			}
			if inserted, err := responseCount(sections, "inserted_count"); err != nil || inserted != 1 {
				t.Fatalf("inserted_count=%d err=%v want 1", inserted, err)
			}

			col, err := mgr.OpenCollection(tc.collection)
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}
			got, err := col.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("get inserted document: %v", err)
			}
			if len(got) == 0 {
				t.Fatal("get inserted document returned empty payload")
			}
		})
	}
}

func TestMutationSingleReplaceBatchSemantics(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name:    "users",
		Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","name":"Ada"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	matched, modified, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","name":"Ada Lovelace"}`)},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("ReplaceBatch existing: %v", err)
	}
	if matched != 1 || modified != 1 {
		t.Fatalf("existing matched=%d modified=%d want 1/1", matched, modified)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if !bytes.Contains(doc, []byte(`"Ada Lovelace"`)) {
		t.Fatalf("u1 doc=%s", doc)
	}

	matched, modified, err = client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","name":"Ada Lovelace"}`)},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("ReplaceBatch identical: %v", err)
	}
	if matched != 1 || modified != 0 {
		t.Fatalf("identical matched=%d modified=%d want 1/0", matched, modified)
	}

	matched, modified, err = client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("missing")},
		[][]byte{[]byte(`{"email":"missing@example.com","name":"Missing"}`)},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("ReplaceBatch missing: %v", err)
	}
	if matched != 0 || modified != 0 {
		t.Fatalf("missing matched=%d modified=%d want 0/0", matched, modified)
	}
}

func TestMutationSingleReplaceBatchSharesMetadataReadLock(t *testing.T) {
	client, server, _, _ := serveCollectionPipeWithServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"before"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	server.metadataMu.RLock()
	locked := true
	unlock := func() {
		if locked {
			server.metadataMu.RUnlock()
			locked = false
		}
	}
	defer unlock()

	done := make(chan error, 1)
	go func() {
		matched, modified, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
			[][]byte{[]byte("u1")},
			[][]byte{[]byte(`{"name":"after"}`)},
			AckVisible,
		)
		if err == nil && (matched != 1 || modified != 1) {
			err = errors.New("unexpected replace counts")
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ReplaceBatch: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		unlock()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatalf("ReplaceBatch blocked behind metadata read lock")
	}
}

func TestMutationInsertBatchSharesMetadataReadLock(t *testing.T) {
	client, server, mgr, _ := serveCollectionPipeWithServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	server.metadataMu.RLock()
	locked := true
	unlock := func() {
		if locked {
			server.metadataMu.RUnlock()
			locked = false
		}
	}
	defer unlock()

	done := make(chan error, 1)
	go func() {
		ids, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
			[][]byte{[]byte("u1")},
			[][]byte{[]byte(`{"name":"Ada"}`)},
			AckVisible,
		)
		if err == nil && (len(ids) != 1 || string(ids[0]) != "u1") {
			err = errors.New("unexpected insert IDs")
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		unlock()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatalf("InsertBatch blocked behind metadata read lock")
	}

	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if !bytes.Contains(doc, []byte(`"Ada"`)) {
		t.Fatalf("u1 doc=%s", doc)
	}
}

func TestMutationConcurrentSingleReplaceBatch(t *testing.T) {
	client, server, mgr, _ := serveCollectionPipeWithServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name:    "users",
		Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	const workers = 32
	ids := make([][]byte, workers)
	docs := make([][]byte, workers)
	for i := range ids {
		n := strconv.Itoa(i)
		ids[i] = []byte("u" + n)
		docs[i] = []byte(`{"email":"u` + n + `@example.com","name":"before"}`)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON, ids, docs, AckVisible); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := range ids {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, cleanup, err := NewInProcessClient(ctx, server)
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = cleanup() }()
			n := strconv.Itoa(i)
			matched, modified, err := c.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
				[][]byte{[]byte("u" + n)},
				[][]byte{[]byte(`{"email":"u` + n + `@example.com","name":"after` + n + `"}`)},
				AckVisible,
			)
			if err != nil {
				errCh <- err
				return
			}
			if matched != 1 || modified != 1 {
				errCh <- errors.New("unexpected replace counts")
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent ReplaceBatch: %v", err)
		}
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	for i := range ids {
		doc, err := col.Get(ids[i])
		if err != nil {
			t.Fatalf("Get %s: %v", ids[i], err)
		}
		want := []byte(`"name":"after` + strconv.Itoa(i) + `"`)
		if !bytes.Contains(doc, want) {
			t.Fatalf("%s doc=%s want %s", ids[i], doc, want)
		}
	}
}

func TestMutationInsertResponseShapingFlags(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	guard, err := client.replicatedMutationGuard(ctx, "insert_omit_meta")
	if err != nil {
		t.Fatalf("mutation guard omit meta: %v", err)
	}
	body, err := appendInsertBatchRequestBodyRefFlags(nil, "users", 0, false, collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckVisible,
		iwire.CommandFlagOmitResponseMeta,
		guard,
	)
	if err != nil {
		t.Fatalf("append omit meta insert: %v", err)
	}
	_, response, err := client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if err != nil {
		t.Fatalf("roundTrip omit meta insert: %v", err)
	}
	client.clearCatalogVersionAfterOpaqueMutation()
	sections, err := iwire.DecodeSections(response, client.limits)
	if err != nil {
		t.Fatalf("decode omit meta response: %v", err)
	}
	if _, ok, err := singletonSection(sections, iwire.SectionDocumentIDs); err != nil {
		t.Fatalf("document_ids section: %v", err)
	} else if !ok {
		t.Fatal("omit meta response missing document_ids")
	}
	if _, ok, err := singletonSection(sections, iwire.SectionResponseMeta); err != nil {
		t.Fatalf("response_meta section: %v", err)
	} else if ok {
		t.Fatal("omit meta response unexpectedly included response_meta")
	}

	guard, err = client.replicatedMutationGuard(ctx, "insert_empty_response")
	if err != nil {
		t.Fatalf("mutation guard empty response: %v", err)
	}
	body, err = appendInsertBatchRequestBodyRefFlags(nil, "users", 0, false, collections.DocumentFormatJSON,
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"x":2}`)},
		AckVisible,
		iwire.CommandFlagOmitResultIDs|iwire.CommandFlagOmitResponseMeta,
		guard,
	)
	if err != nil {
		t.Fatalf("append empty response insert: %v", err)
	}
	_, response, err = client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if err != nil {
		t.Fatalf("roundTrip empty response insert: %v", err)
	}
	client.clearCatalogVersionAfterOpaqueMutation()
	if len(response) != 0 {
		t.Fatalf("combined omit flags response len=%d want 0", len(response))
	}
}

func TestInsertBatchNoResultRetainsCatalogVersionCache(t *testing.T) {
	client, server, _, _ := serveCollectionPipeWithServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	if err := client.InsertBatchHandleNoResult(ctx, handle, collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("first InsertBatchHandleNoResult: %v", err)
	}
	waitForCounter(t, server, "commands.stats.requests_total", 2)

	if err := client.InsertBatchHandleNoResult(ctx, handle, collections.DocumentFormatJSON,
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"x":2}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("second InsertBatchHandleNoResult: %v", err)
	}
	if got := server.Stats()[nativeStatsPrefix+"commands.stats.requests_total"]; got != "2" {
		t.Fatalf("stats requests after cached no-result insert=%s want 2", got)
	}
}

func TestMutationInsertRejectsUnsupportedFastFlags(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	guard, err := client.replicatedMutationGuard(ctx, "insert_bad_flags")
	if err != nil {
		t.Fatalf("mutation guard: %v", err)
	}
	body, err := appendInsertBatchRequestBodyRefFlags(nil, "users", 0, false, collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckVisible,
		1<<12,
		guard,
	)
	if err != nil {
		t.Fatalf("append insert: %v", err)
	}
	_, _, err = client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("InsertBatch unsupported flags err=%v want unsupported feature", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestMutationInvalidIDRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		ids  [][]byte
		code iwire.ErrorCode
	}{
		{name: "duplicate", ids: [][]byte{[]byte("u1"), []byte("u1")}, code: iwire.ErrDuplicateDocumentID},
		{name: "empty", ids: [][]byte{[]byte("u1"), []byte{}}, code: iwire.ErrInvalidCommand},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _, _ := serveCollectionPipe(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatalf("Hello: %v", err)
			}
			if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
				tc.ids,
				[][]byte{[]byte(`{"x":1}`), []byte(`{"x":2}`)},
				AckVisible,
			)
			if !isRemoteError(err, tc.code) {
				t.Fatalf("InsertBatch err=%v want code %d", err, tc.code)
			}
		})
	}
}

func TestRejectDuplicateIDsSmallBatchAllocFree(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation guard is noisy under -race")
	}
	ids := make([][]byte, 128)
	for i := range ids {
		ids[i] = []byte("doc-" + strconv.Itoa(i))
	}
	if err := rejectDuplicateIDs(ids); err != nil {
		t.Fatalf("rejectDuplicateIDs: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if err := rejectDuplicateIDs(ids); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("rejectDuplicateIDs allocs/run=%v want 0", allocs)
	}
}

func TestRejectDuplicateIDsDetectsSmallAndLargeDuplicates(t *testing.T) {
	for _, count := range []int{512, 513} {
		for _, copied := range []bool{false, true} {
			ids := make([][]byte, count)
			for i := range ids {
				ids[i] = []byte("doc-" + strconv.Itoa(i))
			}
			ids[count-1] = ids[17]
			if copied {
				ids[count-1] = append([]byte(nil), ids[17]...)
			}
			err := rejectDuplicateIDs(ids)
			var protocolErr *iwire.ProtocolError
			if !errors.As(err, &protocolErr) || protocolErr.Code != iwire.ErrDuplicateDocumentID {
				t.Fatalf("rejectDuplicateIDs(%d, copied=%v) err=%v want duplicate document id", count, copied, err)
			}
		}
	}
}

func TestInsertBatchFastDecodeErrorReturnsBeforeDispatch(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	body, err := appendInsertBatchRequestBody(nil, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		nil,
		AckVisible,
	)
	if err != nil {
		t.Fatalf("append request: %v", err)
	}
	_, _, err = client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("InsertBatch malformed err=%v want invalid command", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestInsertBatchFastRejectsEmptyIDBeforeDispatch(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("")},
		[][]byte{[]byte(`{"x":1}`)},
		AckVisible,
	)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("InsertBatch empty ID err=%v want invalid command", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	records, truncated, err := col.ScanDocuments(10)
	if err != nil {
		t.Fatalf("ScanDocuments: %v", err)
	}
	if truncated || len(records) != 0 {
		t.Fatalf("records=%+v truncated=%v want empty collection", records, truncated)
	}
}

func TestMutationExplicitZeroAckUsesDefault(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	guard, err := client.replicatedMutationGuard(ctx, "insert_batch_zero_ack")
	if err != nil {
		t.Fatalf("mutation guard: %v", err)
	}
	req := append(guard,
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatJSON),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"x":1}`))},
		ackSection(0),
	)
	sections, err := client.commandSections(ctx, iwire.CommandInsertBatch, req...)
	if err != nil {
		t.Fatalf("InsertBatch explicit zero ack: %v", err)
	}
	if actual, err := responseAckPolicy(sections); err != nil || actual != AckVisible {
		t.Fatalf("actual ack=%v err=%v want visible", actual, err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if doc, err := col.Get([]byte("u1")); err != nil || len(doc) == 0 {
		t.Fatalf("Get u1 doc=%q err=%v", doc, err)
	}
}

func TestMutationUnsupportedAckPolicyRejectedBeforeWrite(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	guard, err := client.replicatedMutationGuard(ctx, "insert_batch_bad_ack")
	if err != nil {
		t.Fatalf("mutation guard: %v", err)
	}
	req := append(guard,
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatJSON),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"x":1}`))},
		ackSection(AckPolicy(99)),
	)
	_, err = client.commandSections(ctx, iwire.CommandInsertBatch, req...)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("InsertBatch unsupported ack err=%v want invalid command", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestMutationTemplateRecordsSectionFeedsTemplateV1Insert(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatTemplateV1,
		},
		Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueString}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	var encoder collections.TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	templateRecords, stored1 := splitTemplateInputEnvelopeForTest(t, doc1)
	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if !bytes.HasPrefix(doc2, []byte(templateV1HashMagic)) {
		t.Fatalf("doc2 should be hash-addressed TD1H, got %q", doc2[:4])
	}

	guard, err := client.replicatedMutationGuard(ctx, "insert_batch_template_records")
	if err != nil {
		t.Fatalf("mutation guard: %v", err)
	}
	req := append(guard,
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatTemplateV1),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"), []byte("u2"))},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, stored1, doc2)},
		iwire.Section{ID: iwire.SectionTemplateRecords, Bytes: iwire.AppendByteVector(nil, templateRecords...)},
	)
	if _, err := client.commandSections(ctx, iwire.CommandInsertBatch, req...); err != nil {
		t.Fatalf("InsertBatch template_records: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids, truncated, err := col.FindByIndexValueLimit("email", "grace@example.com", 10)
	if err != nil {
		t.Fatalf("FindByIndexValueLimit: %v", err)
	}
	if truncated || len(ids) != 1 || string(ids[0]) != "u2" {
		t.Fatalf("ids=%q truncated=%v want u2", ids, truncated)
	}
}

func TestMutationInvalidBSONRejectedBeforeTrustedInsert(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name:    "users",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatBSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte{0x05, 0x00}},
		AckVisible,
	)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("InsertBatch invalid BSON err=%v want invalid command", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestMutationReplaceValidatesBSONBeforeSkippingMissingIDs(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name:    "users",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	original, err := bson.Marshal(bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}})
	if err != nil {
		t.Fatalf("marshal original: %v", err)
	}
	changed, err := bson.Marshal(bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Changed"}})
	if err != nil {
		t.Fatalf("marshal changed: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatBSON,
		[][]byte{[]byte("u1")},
		[][]byte{original},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	_, _, err = client.ReplaceBatch(ctx, "users", collections.DocumentFormatBSON,
		[][]byte{[]byte("missing"), []byte("u1")},
		[][]byte{{0x05, 0x00}, changed},
		AckVisible,
	)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("ReplaceBatch invalid BSON err=%v want invalid command", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("u1 changed after rejected replace: got %v want %v", got, original)
	}
}

func TestMutationRaftAckRejected(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckRaftCommitted,
	)
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("InsertBatch raft ack err=%v want durability unavailable", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestMutationGuardWithoutBackendRejectedBeforeWrite(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection direct: %v", err)
	}
	_, err = client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckSynced,
	)
	if nativeCodeOf(err) != iwire.ErrInvalidCommand {
		t.Fatalf("InsertBatch guard err=%v code=%d want invalid command", err, nativeCodeOf(err))
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestMutationReplaceRejectsUnsupportedReplacementModeBeforeWrite(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"Ada"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	guard, err := client.replicatedMutationGuard(ctx, "replace_batch_invalid_mode")
	if err != nil {
		t.Fatalf("mutation guard: %v", err)
	}
	req := append(guard,
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatJSON),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"name":"Ada Changed"}`))},
		iwire.Section{ID: iwire.SectionReplacementMode, Bytes: []byte{2}},
		ackSection(AckVisible),
	)
	_, err = client.commandSections(ctx, iwire.CommandReplaceBatch, req...)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("ReplaceBatch invalid replacement_mode err=%v want invalid command", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if !bytes.Contains(doc, []byte(`"Ada"`)) || bytes.Contains(doc, []byte(`Changed`)) {
		t.Fatalf("u1 changed after rejected replace: %s", doc)
	}
}

func TestMutationBarrierRejectsUnsatisfiedAckPolicy(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err := client.commandSections(ctx, iwire.CommandFlushCollection, collectionNameRef("users"), ackSection(AckSynced))
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("FlushCollection synced ack err=%v want durability unavailable", err)
	}
	_, err = client.commandSections(ctx, iwire.CommandFlushAll, ackSection(AckSynced))
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("FlushAll synced ack err=%v want durability unavailable", err)
	}
	_, err = client.commandSections(ctx, iwire.CommandCheckpoint, ackSection(AckRaftCommitted))
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("Checkpoint raft ack err=%v want durability unavailable", err)
	}
}

func TestMutationBarrierDefaultAckPolicyHonored(t *testing.T) {
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{DefaultAckPolicy: AckSynced})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if err := client.FlushCollection(ctx, "users"); !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("FlushCollection default synced err=%v want durability unavailable", err)
	}
	_, err := client.commandSections(ctx, iwire.CommandFlushAll)
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("FlushAll default synced err=%v want durability unavailable", err)
	}
}

func TestMutationBarrierAckAPIs(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if err := client.FlushCollectionWithAck(ctx, "users", AckFlushed); err != nil {
		t.Fatalf("FlushCollectionWithAck flushed: %v", err)
	}
	if err := client.FlushAllWithAck(ctx, AckFlushed); err != nil {
		t.Fatalf("FlushAllWithAck flushed: %v", err)
	}
	if err := client.CheckpointWithAck(ctx, AckSynced); err != nil {
		t.Fatalf("CheckpointWithAck synced: %v", err)
	}
	if err := client.FlushCollectionWithAck(ctx, "users", AckSynced); !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("FlushCollectionWithAck synced err=%v want durability unavailable", err)
	}
}

func TestNativewireAckSyncedOptsUpInWALOnRelaxed(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err = client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckSynced,
	)
	if err != nil {
		t.Fatalf("InsertBatch synced relaxed: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if !bytes.Contains(doc, []byte(`"x":1`)) {
		t.Fatalf("u1 after synced relaxed insert: %s", doc)
	}
	if err := client.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint default relaxed: %v", err)
	}
	if err := client.CheckpointWithAck(ctx, AckSynced); err != nil {
		t.Fatalf("CheckpointWithAck synced relaxed: %v", err)
	}
}

func TestMutationCatalogVersionCacheSurvivesSuccessfulDataMutation(t *testing.T) {
	client, _, db := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	version := clientCatalogVersion(t, client, ctx)
	beforeCommit := catalogVersion(t, db)
	client.catalogVersionPlusOne.Store(version + 1)
	ids, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("insert ids=%q want u1", ids)
	}
	afterCommit := catalogVersion(t, db)
	if afterCommit <= beforeCommit {
		t.Fatalf("backend commit seq did not advance after data mutation: before=%d after=%d", beforeCommit, afterCommit)
	}
	if got := client.catalogVersionPlusOne.Load(); got != version+1 {
		t.Fatalf("catalogVersionPlusOne=%d want %d", got, version+1)
	}
	if after := clientCatalogVersion(t, client, ctx); after != version {
		t.Fatalf("data mutation changed catalog version from %d to %d", version, after)
	}
}

func TestMutationCatalogGuardAllowsPriorDataCommitVersion(t *testing.T) {
	client, _, db := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	version := clientCatalogVersion(t, client, ctx)
	beforeCommit := catalogVersion(t, db)
	guardedCtx := WithExpectedCatalogVersion(ctx, version)
	if _, err := client.InsertBatch(guardedCtx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	afterCommit := catalogVersion(t, db)
	if afterCommit <= beforeCommit {
		t.Fatalf("backend commit seq did not advance after data mutation: before=%d after=%d", beforeCommit, afterCommit)
	}

	matched, modified, err := client.ReplaceBatch(guardedCtx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":2}`)},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("ReplaceBatch with catalog version predating data commit: %v", err)
	}
	if matched != 1 || modified != 1 {
		t.Fatalf("matched=%d modified=%d want 1/1", matched, modified)
	}
	if after := clientCatalogVersion(t, client, ctx); after != version {
		t.Fatalf("data mutations changed catalog version from %d to %d", version, after)
	}
}

func TestCatalogMetadataVersionIgnoresDataRootChanges(t *testing.T) {
	client, server, _, db := serveCollectionPipeWithServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	version := clientCatalogVersion(t, client, ctx)
	beforeCatalog, beforeOK := server.catalogMetadataFingerprint()
	if !beforeOK {
		t.Fatalf("catalogMetadataFingerprint before data mutation failed")
	}
	beforeCommit := catalogVersion(t, db)
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	afterCommit := catalogVersion(t, db)
	if afterCommit <= beforeCommit {
		t.Fatalf("backend commit seq did not advance after data mutation: before=%d after=%d", beforeCommit, afterCommit)
	}

	server.bumpCatalogVersionIfCatalogMetadataChanged(beforeCatalog, beforeOK)
	if after := clientCatalogVersion(t, client, ctx); after != version {
		t.Fatalf("data root change bumped catalog version from %d to %d", version, after)
	}
}

func TestMutationCatalogGuardRejectsPriorVersionAfterMetadataChange(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	version := clientCatalogVersion(t, client, ctx)
	if _, err := client.CreateIndex(WithExpectedCatalogVersion(ctx, version), "users", collections.IndexDefinition{
		Name:      "email",
		Field:     "email",
		ValueType: collections.IndexValueString,
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	after := clientCatalogVersion(t, client, ctx)
	if after == version {
		t.Fatalf("metadata mutation did not advance catalog version: %d", after)
	}

	_, err := client.InsertBatch(WithExpectedCatalogVersion(ctx, version), "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
		AckVisible,
	)
	if !isRemoteError(err, iwire.ErrCatalogVersionMismatch) {
		t.Fatalf("InsertBatch with stale schema version err=%v want catalog mismatch", err)
	}
	if _, err := client.InsertBatch(WithExpectedCatalogVersion(ctx, after), "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch with current schema version: %v", err)
	}
}

func TestMutationCatalogVersionCacheClearsAfterMismatch(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	version := clientCatalogVersion(t, client, ctx)
	client.catalogVersionPlusOne.Store(version + 2)
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckVisible,
	)
	if !isRemoteError(err, iwire.ErrCatalogVersionMismatch) {
		t.Fatalf("InsertBatch err=%v want catalog mismatch", err)
	}
	if got := client.catalogVersionPlusOne.Load(); got != 0 {
		t.Fatalf("catalogVersionPlusOne=%d want cleared", got)
	}
}

func TestMutationMissingResponseCountsClearCatalogVersionCache(t *testing.T) {
	client := &Client{}
	replaceSections := []iwire.Section{{
		ID: iwire.SectionResponseMeta,
		Bytes: appendAckMetaPayloadVersion(nil, AckVisible, 42, true,
			responseMetaCount{key: "matched_count", value: 1},
		),
	}}
	client.catalogVersionPlusOne.Store(99)
	if _, _, err := client.replaceBatchCountsFromResponse(replaceSections); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("replace count validation err=%v code=%d want malformed", err, nativeCodeOf(err))
	}
	if got := client.catalogVersionPlusOne.Load(); got != 0 {
		t.Fatalf("replace catalogVersionPlusOne=%d want cleared", got)
	}

	deleteSections := []iwire.Section{{
		ID:    iwire.SectionResponseMeta,
		Bytes: appendAckMetaPayloadVersion(nil, AckVisible, 42, true),
	}}
	client.catalogVersionPlusOne.Store(99)
	if _, err := client.deleteBatchCountFromResponse(deleteSections); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("delete count validation err=%v code=%d want malformed", err, nativeCodeOf(err))
	}
	if got := client.catalogVersionPlusOne.Load(); got != 0 {
		t.Fatalf("delete catalogVersionPlusOne=%d want cleared", got)
	}
}

func TestResponseCountRequiresKey(t *testing.T) {
	_, err := responseCount([]iwire.Section{ackMeta(AckVisible)}, "deleted_count")
	if nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("responseCount err=%v code=%d want malformed", err, nativeCodeOf(err))
	}
}

func TestResponseCountRejectsMalformedInteger(t *testing.T) {
	_, err := responseCount([]iwire.Section{iwire.Section{
		ID:    iwire.SectionResponseMeta,
		Bytes: appendStringMap(nil, map[string]string{"deleted_count": "not-an-int"}),
	}}, "deleted_count")
	if nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("responseCount err=%v code=%d want malformed", err, nativeCodeOf(err))
	}
}

func TestResponseMetaFieldsScansCountsAndCatalogVersion(t *testing.T) {
	payload := appendAckMetaPayloadVersion(nil, AckVisible, 42, true,
		responseMetaCount{key: "matched_count", value: 3},
		responseMetaCount{key: "modified_count", value: 2},
	)
	fields, err := decodeResponseMetaFields(payload, "matched_count", "modified_count")
	if err != nil {
		t.Fatalf("decodeResponseMetaFields: %v", err)
	}
	if !fields.hasCatalogVersion || fields.catalogVersion != 42 {
		t.Fatalf("catalog version=%d has=%v want 42/true", fields.catalogVersion, fields.hasCatalogVersion)
	}
	if !fields.hasCount1 || fields.count1 != 3 {
		t.Fatalf("matched count=%d has=%v want 3/true", fields.count1, fields.hasCount1)
	}
	if !fields.hasCount2 || fields.count2 != 2 {
		t.Fatalf("modified count=%d has=%v want 2/true", fields.count2, fields.hasCount2)
	}
}

func TestResponseMetaFieldsPreservesLastDuplicateValue(t *testing.T) {
	payload := responseMetaPayloadForTest(
		"catalog_version", "41",
		"matched_count", "1",
		"catalog_version", "42",
		"matched_count", "3",
	)
	fields, err := decodeResponseMetaFields(payload, "matched_count", "")
	if err != nil {
		t.Fatalf("decodeResponseMetaFields: %v", err)
	}
	if !fields.hasCatalogVersion || fields.catalogVersion != 42 {
		t.Fatalf("catalog version=%d has=%v want 42/true", fields.catalogVersion, fields.hasCatalogVersion)
	}
	if !fields.hasCount1 || fields.count1 != 3 {
		t.Fatalf("matched count=%d has=%v want 3/true", fields.count1, fields.hasCount1)
	}
}

func TestResponseMetaFieldsRejectsTrailingBytes(t *testing.T) {
	payload := append(appendAckMetaPayload(nil, AckVisible), 0)
	if _, err := decodeResponseMetaFields(payload, "", ""); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeResponseMetaFields err=%v code=%d want malformed", err, nativeCodeOf(err))
	}
}

func responseMetaPayloadForTest(pairs ...string) []byte {
	var dst []byte
	dst = binary.AppendUvarint(dst, uint64(len(pairs)/2))
	for i := 0; i+1 < len(pairs); i += 2 {
		dst = appendString(dst, pairs[i])
		dst = appendString(dst, pairs[i+1])
	}
	return dst
}

func mustNativewireBSONRawValue(t testing.TB, value any) bson.RawValue {
	t.Helper()
	valueType, raw, err := bson.MarshalValue(value)
	if err != nil {
		t.Fatalf("MarshalValue(%T): %v", value, err)
	}
	return bson.RawValue{Type: valueType, Value: raw}
}

func assertDocumentMissing(t *testing.T, mgr *collections.CollectionManager, collectionName, id string) {
	t.Helper()
	col, err := mgr.OpenCollection(collectionName)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc, err := col.Get([]byte(id))
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if doc != nil {
		t.Fatalf("document %q exists after rejected mutation: %s", id, doc)
	}
}

func splitTemplateInputEnvelopeForTest(t *testing.T, raw []byte) ([][]byte, []byte) {
	t.Helper()
	pos := 0
	if !consumeTemplateMagic(raw, &pos, templateV1InputMagic) {
		t.Fatalf("template doc missing TD1I envelope")
	}
	count, err := readUvarintField(raw, &pos, "template_count")
	if err != nil {
		t.Fatalf("read template count: %v", err)
	}
	records := make([][]byte, 0, int(count))
	for i := uint64(0); i < count; i++ {
		if len(raw)-pos < sha256.Size {
			t.Fatalf("template id %d truncated", i)
		}
		pos += sha256.Size
		recordLen, err := readUvarintField(raw, &pos, "template_record_len")
		if err != nil {
			t.Fatalf("read template record len: %v", err)
		}
		if recordLen > uint64(len(raw)-pos) {
			t.Fatalf("template record %d length exceeds payload", i)
		}
		records = append(records, raw[pos:pos+int(recordLen)])
		pos += int(recordLen)
	}
	return records, raw[pos:]
}

func responseAckPolicy(sections []iwire.Section) (AckPolicy, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionResponseMeta)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, protocolError(iwire.ErrMalformedFrame, "missing response_meta")
	}
	values, err := decodeStringMap(raw)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(values["actual_ack_policy"], 10, 64)
	if err != nil {
		return 0, err
	}
	return AckPolicy(n), nil
}
