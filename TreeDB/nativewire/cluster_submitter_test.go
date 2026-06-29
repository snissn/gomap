package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type fakeClusterSubmitter struct {
	mu    sync.Mutex
	calls []fakeClusterSubmitCall
}

type fakeClusterSubmitCall struct {
	entry    raftentry.CommandEntryV1
	metadata ClusterRequestMetadata
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
	return fakeClusterSubmitResponse(decoded, metadata)
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

	err = client.FlushCollection(ctx, "users")
	if !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("FlushCollection cluster err=%v want unsupported feature", err)
	}
	if len(submitter.snapshot()) != 0 {
		t.Fatalf("unsupported command reached submitter")
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("u1 changed after unsupported cluster command: got %v want %v", got, original)
	}
}

func TestClusterSubmitterRaftCommittedRequiresProvenResult(t *testing.T) {
	submitter := &fakeClusterSubmitter{}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"Ada"}`)},
		AckRaftCommitted,
	)
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("InsertBatch raft_committed cluster err=%v want durability unavailable", err)
	}
	if got := len(submitter.snapshot()); got != 1 {
		t.Fatalf("submitter calls=%d want 1", got)
	}
}

func TestClusterSubmitterRaftCommittedDoesNotSatisfyLocalAck(t *testing.T) {
	err := validateClusterSubmitResult(ClusterRequestMetadata{AckPolicy: AckSynced}, ClusterSubmitResult{
		ActualAck:            AckRaftCommitted,
		CommittedRecoverable: true,
		ResponseSections:     []iwire.Section{ackMeta(AckRaftCommitted)},
	})
	if nativeCodeOf(err) != iwire.ErrDurabilityUnavailable {
		t.Fatalf("validateClusterSubmitResult err=%v want durability unavailable", err)
	}
}
