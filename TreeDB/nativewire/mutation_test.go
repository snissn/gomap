package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
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
	if !bytes.HasPrefix(doc2, []byte(templateV1StoredMagic)) {
		t.Fatalf("doc2 should be compact TD1D, got %q", doc2[:4])
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

func TestNativewireAckSyncedRejectedInWALOnRelaxed(t *testing.T) {
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
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("InsertBatch synced relaxed err=%v want durability unavailable", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
	if err := client.CheckpointWithAck(ctx, AckSynced); !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("CheckpointWithAck synced relaxed err=%v want durability unavailable", err)
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
	version := catalogVersion(t, db)
	client.catalogVersionPlusOne.Store(version + 1)
	deleted, err := client.DeleteBatch(ctx, "users", [][]byte{[]byte("missing")}, AckVisible)
	if err != nil {
		t.Fatalf("DeleteBatch missing: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted=%d want 0", deleted)
	}
	after := catalogVersion(t, db)
	if got := client.catalogVersionPlusOne.Load(); got != after+1 {
		t.Fatalf("catalogVersionPlusOne=%d want %d", got, after+1)
	}
	if after != version {
		t.Fatalf("missing delete changed catalog version from %d to %d", version, after)
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
	client.catalogVersionPlusOne.Store(1)
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
