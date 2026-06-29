package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type fakeClusterSubmitter struct {
	mu           sync.Mutex
	calls        []fakeClusterSubmitCall
	status       ClusterAdmissionStatus
	admissionErr error
	resultHook   func(raftentry.CommandEntryV1, ClusterRequestMetadata, ClusterSubmitResult) (ClusterSubmitResult, error)
}

type admissionClusterSubmitter struct {
	*fakeClusterSubmitter
	status ClusterAdmissionStatus
	err    error
}

type fakeClusterSubmitCall struct {
	entry    raftentry.CommandEntryV1
	metadata ClusterRequestMetadata
}

type noAdmissionClusterSubmitter struct{}

func (noAdmissionClusterSubmitter) SubmitCommandEntryV1(context.Context, []byte, ClusterRequestMetadata) (ClusterSubmitResult, error) {
	panic("unexpected submit")
}

func (f *admissionClusterSubmitter) ClusterAdmissionStatus(context.Context) (ClusterAdmissionStatus, error) {
	if f.err != nil {
		return ClusterAdmissionStatus{}, f.err
	}
	return f.status, nil
}

func (f *fakeClusterSubmitter) ClusterAdmissionStatus(context.Context) (ClusterAdmissionStatus, error) {
	if f.admissionErr != nil {
		return ClusterAdmissionStatus{}, f.admissionErr
	}
	if f.status == (ClusterAdmissionStatus{}) {
		return ClusterLeaderAdmission(), nil
	}
	return f.status, nil
}

func (f *fakeClusterSubmitter) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata ClusterRequestMetadata) (ClusterSubmitResult, error) {
	decoded, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{RequestMetadata: metadata})
	if err != nil {
		return ClusterSubmitResult{}, err
	}
	call := fakeClusterSubmitCall{
		entry:    decoded,
		metadata: cloneClusterRequestMetadata(metadata),
	}
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
	result, err := fakeClusterSubmitResponse(decoded, metadata)
	if err != nil {
		return ClusterSubmitResult{}, err
	}
	if f.resultHook != nil {
		return f.resultHook(decoded, metadata, result)
	}
	return result, nil
}

func (f *fakeClusterSubmitter) snapshot() []fakeClusterSubmitCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeClusterSubmitCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func fakeClusterSubmitResponse(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata) (ClusterSubmitResult, error) {
	actualAck := metadata.AckPolicy
	if actualAck == 0 {
		actualAck = AckVisible
	}
	var sections []iwire.Section
	switch entry.Decoded.CommandID {
	case iwire.CommandCreateCollection:
		meta, err := metadataSection(entry.Decoded.Sections, iwire.SectionCollectionMeta)
		if err != nil {
			return ClusterSubmitResult{}, err
		}
		sections = []iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: bytes.Clone(meta)}}
	case iwire.CommandInsertBatch:
		ids, err := deterministicEntryDocumentIDs(entry)
		if err != nil {
			return ClusterSubmitResult{}, err
		}
		if !metadata.OmitResultIDs {
			rawIDs, err := metadataSection(entry.Decoded.Sections, iwire.SectionDocumentIDs)
			if err != nil {
				return ClusterSubmitResult{}, err
			}
			sections = append(sections, iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: bytes.Clone(rawIDs)})
		}
		if !metadata.OmitResponseMeta {
			sections = append(sections, ackMetaCountsVersion(actualAck, 0, true, responseMetaCount{key: "inserted_count", value: len(ids)}))
		}
	case iwire.CommandReplaceBatch:
		ids, err := deterministicEntryDocumentIDs(entry)
		if err != nil {
			return ClusterSubmitResult{}, err
		}
		sections = append(sections, ackMetaCountsVersion(actualAck, 0, true,
			responseMetaCount{key: "matched_count", value: len(ids)},
			responseMetaCount{key: "modified_count", value: len(ids)},
		))
	case iwire.CommandUpdateBSONSet:
		ids, err := deterministicEntryDocumentIDs(entry)
		if err != nil {
			return ClusterSubmitResult{}, err
		}
		sections = append(sections, ackMetaCountsVersion(actualAck, 0, true,
			responseMetaCount{key: "matched_count", value: len(ids)},
			responseMetaCount{key: "modified_count", value: len(ids)},
		))
	case iwire.CommandDeleteBatch:
		ids, err := deterministicEntryDocumentIDs(entry)
		if err != nil {
			return ClusterSubmitResult{}, err
		}
		sections = append(sections, ackMetaCountsVersion(actualAck, 0, true, responseMetaCount{key: "deleted_count", value: len(ids)}))
	default:
		return ClusterSubmitResult{}, protocolError(iwire.ErrUnsupportedFeature, "fake submitter does not support command %d", entry.Decoded.CommandID)
	}
	return ClusterSubmitResult{
		ActualAck:        actualAck,
		ResponseSections: sections,
	}, nil
}

func deterministicEntryDocumentIDs(entry raftentry.CommandEntryV1) ([][]byte, error) {
	rawIDs, err := metadataSection(entry.Decoded.Sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, err
	}
	return iwire.DecodeByteVectorItems(rawIDs, iwire.Limits{})
}

func cloneClusterRequestMetadata(metadata ClusterRequestMetadata) ClusterRequestMetadata {
	metadata.TraceContext = bytes.Clone(metadata.TraceContext)
	return metadata
}

func TestClusterAdmissionMissingProviderFailsClosed(t *testing.T) {
	err := AdmitClusterMutation(context.Background(), noAdmissionClusterSubmitter{})
	if nativeCodeOf(err) != iwire.ErrDurabilityUnavailable {
		t.Fatalf("admission err=%v code=%d want durability unavailable", err, nativeCodeOf(err))
	}
}

func TestClusterAdmissionLeaderRoutesThroughSubmitter(t *testing.T) {
	submitter := &admissionClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		status:               ClusterLeaderAdmission(),
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"Ada"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	calls := submitter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("submitter calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("command=%d want insert_batch", got)
	}
}

func TestClusterAdmissionFollowerRejectsNativeMutationsBeforeLocalMutation(t *testing.T) {
	submitter := &admissionClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		status:               ClusterFollowerAdmission("node-a:7000", "not leader"),
	}
	client, _, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	meta := collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}
	if _, err := mgr.CreateCollection(&meta); err != nil {
		t.Fatalf("direct CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	original := mustBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}})
	if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("u1")}, [][]byte{original}); err != nil {
		t.Fatalf("seed InsertBatchValidatedBSON: %v", err)
	}

	insertDoc := mustBSONDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "Grace"}})
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatBSON, [][]byte{[]byte("u2")}, [][]byte{insertDoc}, AckVisible); !isRemoteError(err, iwire.ErrReadOnly) {
		t.Fatalf("InsertBatch err=%v want read-only", err)
	}
	if got, err := col.Get([]byte("u2")); err != nil || got != nil {
		t.Fatalf("u2 after rejected insert got=%v err=%v want missing", got, err)
	}

	replaceDoc := mustBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Grace"}})
	if _, _, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatBSON, [][]byte{[]byte("u1")}, [][]byte{replaceDoc}, AckVisible); !isRemoteError(err, iwire.ErrReadOnly) {
		t.Fatalf("ReplaceBatch err=%v want read-only", err)
	}
	if _, _, err := client.UpdateBSONSet(ctx, "users", []byte("u1"), []collections.BSONSetField{
		{Key: "name", Value: mustNativewireBSONRawValue(t, "Grace")},
	}, AckVisible); !isRemoteError(err, iwire.ErrReadOnly) {
		t.Fatalf("UpdateBSONSet err=%v want read-only", err)
	}
	if _, err := client.DeleteBatch(ctx, "users", [][]byte{[]byte("u1")}, AckVisible); !isRemoteError(err, iwire.ErrReadOnly) {
		t.Fatalf("DeleteBatch err=%v want read-only", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1 after rejected mutations: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("u1 changed after rejected mutations: got %v want %v", got, original)
	}
	if calls := submitter.snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func TestClusterAdmissionMetadataMutationsRejectBeforeLocalMutation(t *testing.T) {
	tests := []struct {
		name     string
		status   ClusterAdmissionStatus
		wantCode iwire.ErrorCode
	}{
		{
			name:     "follower",
			status:   ClusterFollowerAdmission("node-a:7000", "not leader"),
			wantCode: iwire.ErrReadOnly,
		},
		{
			name:     "unavailable",
			status:   ClusterUnavailableAdmission("cluster admission unavailable"),
			wantCode: iwire.ErrDurabilityUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			submitter := &admissionClusterSubmitter{
				fakeClusterSubmitter: &fakeClusterSubmitter{},
				status:               tt.status,
			}
			client, _, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatalf("Hello: %v", err)
			}
			meta := collections.CollectionMeta{
				Name: "users",
				Options: collections.CollectionOptions{
					DocumentFormat: collections.DocumentFormatBSON,
				},
			}
			if _, err := mgr.CreateCollection(&meta); err != nil {
				t.Fatalf("direct CreateCollection users: %v", err)
			}
			col, err := mgr.OpenCollection("users")
			if err != nil {
				t.Fatalf("OpenCollection users: %v", err)
			}
			if _, err := col.CreateIndex(collections.IndexDefinition{
				Name:      "email",
				Field:     "email",
				ValueType: collections.IndexValueString,
			}); err != nil {
				t.Fatalf("direct CreateIndex email: %v", err)
			}

			if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "admins"}); !isRemoteError(err, tt.wantCode) {
				t.Fatalf("CreateCollection err=%v want code %d", err, tt.wantCode)
			}
			if _, err := mgr.OpenCollection("admins"); !errors.Is(err, collections.ErrCollectionNotFound) {
				t.Fatalf("OpenCollection admins err=%v want collection not found", err)
			}

			if _, err := client.CreateIndex(ctx, "users", collections.IndexDefinition{
				Name:      "name",
				Field:     "name",
				ValueType: collections.IndexValueString,
			}); !isRemoteError(err, tt.wantCode) {
				t.Fatalf("CreateIndex err=%v want code %d", err, tt.wantCode)
			}
			indexes := col.Meta().Indexes
			if len(indexes) != 1 || indexes[0].Name != "email" {
				t.Fatalf("indexes after rejected create_index=%+v want only email", indexes)
			}

			if _, err := client.DropIndex(ctx, "users", "email"); !isRemoteError(err, tt.wantCode) {
				t.Fatalf("DropIndex err=%v want code %d", err, tt.wantCode)
			}
			indexes = col.Meta().Indexes
			if len(indexes) != 1 || indexes[0].Name != "email" {
				t.Fatalf("indexes after rejected drop_index=%+v want email retained", indexes)
			}
			if calls := submitter.snapshot(); len(calls) != 0 {
				t.Fatalf("submitter calls=%d want 0", len(calls))
			}
		})
	}
}

func TestClusterAdmissionUnavailableRejectsBeforeSubmit(t *testing.T) {
	submitter := &admissionClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		status:               ClusterUnavailableAdmission("cluster admission unavailable"),
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"Ada"}`)},
		AckVisible,
	)
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("InsertBatch err=%v want durability unavailable", err)
	}
	if calls := submitter.snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterReceivesDecodableDeterministicEntries(t *testing.T) {
	submitter := &fakeClusterSubmitter{}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"name":"Ada"}`), []byte(`{"name":"Grace"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	matched, modified, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"Ada Lovelace"}`)},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}
	if matched != 1 || modified != 1 {
		t.Fatalf("ReplaceBatch matched=%d modified=%d want 1/1", matched, modified)
	}
	deleted, err := client.DeleteBatch(ctx, "users", [][]byte{[]byte("u2")}, AckVisible)
	if err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d want 1", deleted)
	}

	calls := submitter.snapshot()
	want := []iwire.CommandID{
		iwire.CommandCreateCollection,
		iwire.CommandInsertBatch,
		iwire.CommandReplaceBatch,
		iwire.CommandDeleteBatch,
	}
	if len(calls) != len(want) {
		t.Fatalf("submitter calls=%d want %d", len(calls), len(want))
	}
	for i, call := range calls {
		if call.entry.Decoded.CommandID != want[i] {
			t.Fatalf("call %d command=%d want %d", i, call.entry.Decoded.CommandID, want[i])
		}
		if len(call.entry.Bytes) == 0 {
			t.Fatalf("call %d has empty deterministic entry bytes", i)
		}
		if call.entry.Digest != raftentry.CommandDigestV1ForBytes(call.entry.Bytes, raftentry.DecodeOptions{}) {
			t.Fatalf("call %d digest mismatch", i)
		}
		if call.metadata.AckPolicy != AckVisible {
			t.Fatalf("call %d ack=%d want visible", i, call.metadata.AckPolicy)
		}
	}
}

func TestClusterSubmitterRequestOnlyFieldsDoNotAlterDeterministicEntry(t *testing.T) {
	submitter := &fakeClusterSubmitter{}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	guard := []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("same-logical-insert")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, clientCatalogVersion(t, client, ctx))},
	}
	common := append(guard,
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatJSON),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"name":"Ada"}`))},
	)
	first := append([]iwire.Section{}, common...)
	first = append(first,
		ackSection(AckVisible),
		iwire.Section{ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, 100)},
		iwire.Section{ID: iwire.SectionTraceContext, Bytes: []byte("trace-a")},
		iwire.Section{ID: iwire.SectionCompression, Bytes: []byte("none")},
	)
	second := append([]iwire.Section{}, common...)
	second = append(second,
		ackSection(AckFlushed),
		iwire.Section{ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, 200)},
		iwire.Section{ID: iwire.SectionTraceContext, Bytes: []byte("trace-b")},
		iwire.Section{ID: iwire.SectionCompression, Bytes: []byte("zstd")},
	)
	if _, err := client.commandSections(ctx, iwire.CommandInsertBatch, first...); err != nil {
		t.Fatalf("first InsertBatch: %v", err)
	}
	if _, err := client.commandSections(ctx, iwire.CommandInsertBatch, second...); err != nil {
		t.Fatalf("second InsertBatch: %v", err)
	}

	calls := submitter.snapshot()
	if len(calls) != 2 {
		t.Fatalf("submitter calls=%d want 2", len(calls))
	}
	if !bytes.Equal(calls[0].entry.Bytes, calls[1].entry.Bytes) {
		t.Fatalf("deterministic entry bytes changed with request-only fields")
	}
	if calls[0].entry.Digest != calls[1].entry.Digest {
		t.Fatalf("deterministic digest changed with request-only fields")
	}
	if calls[0].metadata.AckPolicy != AckVisible || calls[1].metadata.AckPolicy != AckFlushed {
		t.Fatalf("metadata ack policies=%d/%d want visible/flushed", calls[0].metadata.AckPolicy, calls[1].metadata.AckPolicy)
	}
	if calls[0].metadata.DeadlineUnixNanos != 100 || calls[1].metadata.DeadlineUnixNanos != 200 {
		t.Fatalf("metadata deadlines=%d/%d want 100/200", calls[0].metadata.DeadlineUnixNanos, calls[1].metadata.DeadlineUnixNanos)
	}
	if string(calls[0].metadata.TraceContext) != "trace-a" || string(calls[1].metadata.TraceContext) != "trace-b" {
		t.Fatalf("metadata traces=%q/%q", calls[0].metadata.TraceContext, calls[1].metadata.TraceContext)
	}
	if calls[0].metadata.Compression != "none" || calls[1].metadata.Compression != "zstd" {
		t.Fatalf("metadata compression=%q/%q", calls[0].metadata.Compression, calls[1].metadata.Compression)
	}
}

func TestClusterSubmitterCatalogGuardDoesNotBlockSubmitterReplay(t *testing.T) {
	submitter := &fakeClusterSubmitter{}
	client, server, _, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	version, err := server.currentCatalogVersion()
	if err != nil {
		t.Fatalf("currentCatalogVersion: %v", err)
	}
	server.catalogVersion.Add(1)

	sections := []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("stale-catalog-replay")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)},
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatJSON),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"name":"Ada"}`))},
		ackSection(AckVisible),
	}
	if _, err := client.commandSections(ctx, iwire.CommandInsertBatch, sections...); err != nil {
		t.Fatalf("guarded InsertBatch should reach submitter despite stale local catalog guard: %v", err)
	}
	calls := submitter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("submitter calls=%d want 1", len(calls))
	}
	if calls[0].entry.Decoded.CommandID != iwire.CommandInsertBatch {
		t.Fatalf("command=%d want InsertBatch", calls[0].entry.Decoded.CommandID)
	}
}

func TestClusterSubmitterUnsupportedCommandFailsBeforeMutation(t *testing.T) {
	submitter := &fakeClusterSubmitter{}
	client, _, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	meta := collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}
	if _, err := mgr.CreateCollection(&meta); err != nil {
		t.Fatalf("direct CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	original := mustBSONDocument(t, bson.D{{Key: "field0", Value: "old"}})
	if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("u1")}, [][]byte{original}); err != nil {
		t.Fatalf("seed InsertBatchValidatedBSON: %v", err)
	}

	_, err = client.CreateIndex(ctx, "users", collections.IndexDefinition{
		Name:      "field0",
		Field:     "field0",
		ValueType: collections.IndexValueString,
	})
	if !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("CreateIndex cluster err=%v want unsupported feature", err)
	}
	if len(submitter.snapshot()) != 0 {
		t.Fatalf("unsupported command reached submitter")
	}
	if indexes := col.Meta().Indexes; len(indexes) != 0 {
		t.Fatalf("index created after unsupported cluster command: %+v", indexes)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("u1 changed after unsupported cluster command: got %v want %v", got, original)
	}
}

func TestClusterSubmitterRaftCommittedSucceedsWithRecoverableCommit(t *testing.T) {
	submitter := &fakeClusterSubmitter{
		resultHook: func(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata, result ClusterSubmitResult) (ClusterSubmitResult, error) {
			result.ActualAck = AckRaftCommitted
			result.CommittedRecoverable = true
			return replaceResponseAckPolicy(result, AckRaftCommitted), nil
		},
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	sections, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
	if err != nil {
		t.Fatalf("InsertBatch raft_committed cluster: %v", err)
	}
	actualAck, err := responseAckPolicy(sections)
	if err != nil {
		t.Fatalf("responseAckPolicy: %v", err)
	}
	if actualAck != AckRaftCommitted {
		t.Fatalf("actual ack=%d want raft_committed", actualAck)
	}
	calls := submitter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("submitter calls=%d want 1", len(calls))
	}
	if calls[0].metadata.AckPolicy != AckRaftCommitted {
		t.Fatalf("submitter ack=%d want raft_committed", calls[0].metadata.AckPolicy)
	}
}

func TestClusterSubmitterRaftCommittedRequiresRecoverableCommit(t *testing.T) {
	submitter := &fakeClusterSubmitter{}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("InsertBatch raft_committed cluster err=%v want durability unavailable", err)
	}
	calls := submitter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("submitter calls=%d want 1", len(calls))
	}
	if calls[0].metadata.AckPolicy != AckRaftCommitted {
		t.Fatalf("submitter ack=%d want raft_committed", calls[0].metadata.AckPolicy)
	}
}

func TestClusterSubmitterRaftCommittedRequiresConsensusAck(t *testing.T) {
	submitter := &fakeClusterSubmitter{
		resultHook: func(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata, result ClusterSubmitResult) (ClusterSubmitResult, error) {
			result.ActualAck = AckSynced
			result.CommittedRecoverable = true
			return replaceResponseAckPolicy(result, AckSynced), nil
		},
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("InsertBatch uncommitted raft ack err=%v want durability unavailable", err)
	}
	if got := len(submitter.snapshot()); got != 1 {
		t.Fatalf("submitter calls=%d want 1", got)
	}
}

func TestClusterSubmitterRaftCommittedRejectsLyingResponseMetadata(t *testing.T) {
	submitter := &fakeClusterSubmitter{
		resultHook: func(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata, result ClusterSubmitResult) (ClusterSubmitResult, error) {
			result.ActualAck = AckRaftCommitted
			result.CommittedRecoverable = true
			return replaceResponseAckPolicy(result, AckSynced), nil
		},
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
	if !isRemoteError(err, iwire.ErrInternal) {
		t.Fatalf("InsertBatch lying raft ack metadata err=%v want internal", err)
	}
	if got := len(submitter.snapshot()); got != 1 {
		t.Fatalf("submitter calls=%d want 1", got)
	}
}

func TestClusterSubmitterRaftCommittedDoesNotSatisfyLocalAck(t *testing.T) {
	submitter := &fakeClusterSubmitter{
		resultHook: func(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata, result ClusterSubmitResult) (ClusterSubmitResult, error) {
			result.ActualAck = AckRaftCommitted
			result.CommittedRecoverable = true
			return replaceResponseAckPolicy(result, AckRaftCommitted), nil
		},
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckVisible, "u1")...)
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("InsertBatch visible upgraded to raft_committed err=%v want durability unavailable", err)
	}
	calls := submitter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("submitter calls=%d want 1", len(calls))
	}
	if calls[0].metadata.AckPolicy != AckVisible {
		t.Fatalf("submitter ack=%d want visible", calls[0].metadata.AckPolicy)
	}
}

func TestClusterSubmitterRaftCommittedAdmissionFailsBeforeSubmit(t *testing.T) {
	submitter := &fakeClusterSubmitter{
		status: ClusterFollowerAdmission("node-a:7000", "not leader"),
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
	if !isRemoteError(err, iwire.ErrReadOnly) {
		t.Fatalf("InsertBatch follower raft_committed err=%v want read-only", err)
	}
	if got := len(submitter.snapshot()); got != 0 {
		t.Fatalf("submitter calls=%d want 0", got)
	}
}

func clusterInsertBatchSections(t *testing.T, client *Client, ctx context.Context, ack AckPolicy, id string) []iwire.Section {
	t.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("cluster-insert-" + id)},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, clientCatalogVersion(t, client, ctx))},
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatJSON),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte(id))},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"name":"Ada"}`))},
	}
	if ack != 0 {
		sections = append(sections, ackSection(ack))
	}
	return sections
}

func replaceResponseAckPolicy(result ClusterSubmitResult, policy AckPolicy) ClusterSubmitResult {
	for i, section := range result.ResponseSections {
		if section.ID == iwire.SectionResponseMeta {
			result.ResponseSections[i] = ackMeta(policy)
			return result
		}
	}
	result.ResponseSections = append(result.ResponseSections, ackMeta(policy))
	return result
}
