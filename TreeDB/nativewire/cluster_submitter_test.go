package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
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

type recordingRaftCommandSubmitter struct {
	groupID raftcluster.GroupID
	manager *collections.CollectionManager
	status  raftcluster.AdmissionStatus
	err     error
	mu      sync.Mutex
	calls   []recordingRaftCommandSubmitCall
}

type recordingRaftCommandSubmitCall struct {
	entry    raftentry.CommandEntryV1
	metadata raftentry.RequestMetadataV1
}

func (s *recordingRaftCommandSubmitter) Config() raftcluster.ResolvedConfig {
	return raftcluster.ResolvedConfig{GroupID: s.groupID}
}

func (s *recordingRaftCommandSubmitter) ClusterAdmissionStatus(context.Context) (raftcluster.AdmissionStatus, error) {
	if s.status == (raftcluster.AdmissionStatus{}) {
		return raftcluster.LeaderAdmission(), nil
	}
	return s.status, nil
}

func (s *recordingRaftCommandSubmitter) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1) (raftcluster.SubmitResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return raftcluster.SubmitResultV1{}, ctx.Err()
	default:
	}
	decoded, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{RequestMetadata: metadata})
	if err != nil {
		return raftcluster.SubmitResultV1{}, err
	}
	s.mu.Lock()
	index := uint64(len(s.calls) + 1)
	s.calls = append(s.calls, recordingRaftCommandSubmitCall{entry: decoded, metadata: cloneClusterRequestMetadata(metadata)})
	s.mu.Unlock()
	if s.err != nil {
		return raftcluster.SubmitResultV1{}, s.err
	}
	applyResult, err := s.applyForTest(decoded)
	if err != nil {
		return raftcluster.SubmitResultV1{}, err
	}
	actualAck := metadata.AckPolicy
	if actualAck == 0 {
		actualAck = iwire.AckVisible
	}
	expectedTarget := decoded.Target.Clone()
	committed := raftcluster.CommittedCommandEntryV1{
		Term:                     1,
		Index:                    index,
		Bytes:                    bytes.Clone(entry),
		CurrentCatalogVersion:    7,
		HasCurrentCatalogVersion: true,
		SyncLocalCommandWAL:      true,
		RequestMetadata:          cloneClusterRequestMetadata(metadata),
		ExpectedTarget:           &expectedTarget,
	}
	return raftcluster.SubmitResultV1{
		ActualAck:            actualAck,
		CommittedRecoverable: actualAck == iwire.AckRaftCommitted,
		DecodedEntry:         decoded,
		ApplyResult:          applyResult,
		CommittedEntry:       committed,
		Evidence: raftcluster.CommitEvidenceV1{
			Kind:                raftcluster.CommitEvidenceProductionConsensusV1,
			GroupID:             s.groupID,
			Term:                1,
			Index:               index,
			Committed:           true,
			ProductionConsensus: true,
		},
		CatalogVersion:    7,
		HasCatalogVersion: true,
	}, nil
}

func (s *recordingRaftCommandSubmitter) applyForTest(entry raftentry.CommandEntryV1) (raftentry.ApplyResultV1, error) {
	switch entry.Decoded.CommandID {
	case iwire.CommandCreateCollection:
		if s.manager != nil {
			rawMeta, err := metadataSection(entry.Decoded.Sections, iwire.SectionCollectionMeta)
			if err != nil {
				return raftentry.ApplyResultV1{}, err
			}
			meta, err := decodeCollectionMeta(rawMeta)
			if err != nil {
				return raftentry.ApplyResultV1{}, err
			}
			meta, err = normalizeClientCollectionMeta(meta)
			if err != nil {
				return raftentry.ApplyResultV1{}, err
			}
			if _, err := s.manager.CreateCollection(&meta); err != nil {
				return raftentry.ApplyResultV1{}, err
			}
		}
		return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1, MatchedCount: 1}, nil
	case iwire.CommandInsertBatch:
		rawIDs, err := metadataSection(entry.Decoded.Sections, iwire.SectionDocumentIDs)
		if err != nil {
			return raftentry.ApplyResultV1{}, err
		}
		ids, err := iwire.DecodeByteVectorItems(rawIDs, iwire.Limits{})
		if err != nil {
			return raftentry.ApplyResultV1{}, err
		}
		return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: int64(len(ids)), MatchedCount: int64(len(ids))}, nil
	default:
		return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1, MatchedCount: 1}, nil
	}
}

func (s *recordingRaftCommandSubmitter) snapshotCalls() []recordingRaftCommandSubmitCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]recordingRaftCommandSubmitCall, len(s.calls))
	copy(out, s.calls)
	return out
}

type routingClusterSubmitter struct {
	*fakeClusterSubmitter

	routeMu sync.Mutex
	target  ClusterRouteTarget
	err     error
	routes  []ClusterRouteRequest
}

func (r *routingClusterSubmitter) ClusterRoute(ctx context.Context, req ClusterRouteRequest) (ClusterRouteTarget, error) {
	r.routeMu.Lock()
	r.routes = append(r.routes, req)
	r.routeMu.Unlock()
	if r.err != nil {
		return ClusterRouteTarget{}, r.err
	}
	target := r.target
	target.Members = append([]string(nil), target.Members...)
	return target, nil
}

func (r *routingClusterSubmitter) snapshotRoutes() []ClusterRouteRequest {
	r.routeMu.Lock()
	defer r.routeMu.Unlock()
	return append([]ClusterRouteRequest(nil), r.routes...)
}

type staticClusterRouteProvider struct {
	mu     sync.Mutex
	target ClusterRouteTarget
	err    error
	routes []ClusterRouteRequest
}

func (p *staticClusterRouteProvider) ClusterRoute(ctx context.Context, req ClusterRouteRequest) (ClusterRouteTarget, error) {
	p.mu.Lock()
	p.routes = append(p.routes, req)
	p.mu.Unlock()
	if p.err != nil {
		return ClusterRouteTarget{}, p.err
	}
	target := p.target
	target.Members = append([]string(nil), target.Members...)
	return target, nil
}

func (p *staticClusterRouteProvider) snapshotRoutes() []ClusterRouteRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ClusterRouteRequest(nil), p.routes...)
}

type placementRouteClusterSubmitter struct {
	*fakeClusterSubmitter

	routeMu  sync.Mutex
	provider CatalogClusterRouteProvider
	routes   []ClusterRouteRequest
}

func (p *placementRouteClusterSubmitter) ClusterRoute(ctx context.Context, req ClusterRouteRequest) (ClusterRouteTarget, error) {
	p.routeMu.Lock()
	p.routes = append(p.routes, req)
	p.routeMu.Unlock()
	return p.provider.ClusterRoute(ctx, req)
}

func (p *placementRouteClusterSubmitter) snapshotRoutes() []ClusterRouteRequest {
	p.routeMu.Lock()
	defer p.routeMu.Unlock()
	return append([]ClusterRouteRequest(nil), p.routes...)
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
	metadata.ClusterRouteMembers = append([]string(nil), metadata.ClusterRouteMembers...)
	return metadata
}

func assertNativeClusterTokenRouteMetadata(tb testing.TB, call fakeClusterSubmitCall, mode raftplacement.PlacementModeV1, partition string, token uint64) {
	tb.Helper()
	meta := call.metadata
	if !meta.ClusterRouteKnown {
		tb.Fatal("metadata missing cluster route")
	}
	if meta.ClusterRouteShape != string(ClusterRouteShapeToken) {
		tb.Fatalf("route shape=%q want token", meta.ClusterRouteShape)
	}
	if meta.ClusterRouteGroupID != "group-a" || meta.ClusterRouteLeaderHint != "node-a" || meta.ClusterRoutePlacementMode != string(mode) {
		tb.Fatalf("metadata route target group=%q leader=%q mode=%q", meta.ClusterRouteGroupID, meta.ClusterRouteLeaderHint, meta.ClusterRoutePlacementMode)
	}
	if meta.ClusterRouteKey != string(raftplacement.RouteKeyDocumentIDV1) {
		tb.Fatalf("metadata route key=%q want %q", meta.ClusterRouteKey, raftplacement.RouteKeyDocumentIDV1)
	}
	if !meta.ClusterRouteTokenKnown || meta.ClusterRouteToken != token || meta.ClusterRoutePartitionID != partition {
		tb.Fatalf("metadata token known/token/partition=%v/%d/%q want true/%d/%q", meta.ClusterRouteTokenKnown, meta.ClusterRouteToken, meta.ClusterRoutePartitionID, token, partition)
	}
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

func TestClusterRoutePreflightAddsMetadata(t *testing.T) {
	submitter := &routingClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		target: ClusterRouteTarget{
			GroupID:       "group-a",
			Members:       []string{"node-a", "node-b"},
			LeaderHint:    "node-a",
			PlacementMode: "collection",
		},
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	routes := submitter.snapshotRoutes()
	if len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	if got := routes[0]; got.Database != "default" || got.Catalog != "default" || got.Collection != "users" || got.CommandID != iwire.CommandCreateCollection || got.CommandName != "create_collection" {
		t.Fatalf("route request=%+v want default/default/users create_collection", got)
	}
	calls := submitter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("submitter calls=%d want 1", len(calls))
	}
	meta := calls[0].metadata
	if !meta.ClusterRouteKnown {
		t.Fatal("metadata missing cluster route")
	}
	if meta.ClusterRouteDatabase != "default" || meta.ClusterRouteCatalog != "default" || meta.ClusterRouteCollection != "users" {
		t.Fatalf("metadata route identity=%s/%s/%s want default/default/users", meta.ClusterRouteDatabase, meta.ClusterRouteCatalog, meta.ClusterRouteCollection)
	}
	if meta.ClusterRouteShape != string(ClusterRouteShapeCollection) {
		t.Fatalf("metadata route shape=%q want collection", meta.ClusterRouteShape)
	}
	if meta.ClusterRouteGroupID != "group-a" || meta.ClusterRouteLeaderHint != "node-a" || meta.ClusterRoutePlacementMode != "collection" {
		t.Fatalf("metadata route target group=%q leader=%q mode=%q", meta.ClusterRouteGroupID, meta.ClusterRouteLeaderHint, meta.ClusterRoutePlacementMode)
	}
	if meta.ClusterRouteTokenKnown || meta.ClusterRouteToken != 0 || meta.ClusterRoutePartitionID != "" {
		t.Fatalf("collection route unexpectedly included token metadata known/token/partition=%v/%d/%q", meta.ClusterRouteTokenKnown, meta.ClusterRouteToken, meta.ClusterRoutePartitionID)
	}
	if !reflect.DeepEqual(meta.ClusterRouteMembers, []string{"node-a", "node-b"}) {
		t.Fatalf("metadata route members=%v want [node-a node-b]", meta.ClusterRouteMembers)
	}
}

func TestClusterRoutePreflightRejectsBeforeSubmitter(t *testing.T) {
	submitter := &routingClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		err:                  errors.New("unplaced collection"),
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
	if !isRemoteError(err, iwire.ErrReadOnly) {
		t.Fatalf("InsertBatch err=%v want read-only", err)
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	if calls := submitter.snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func TestClusterRoutePreflightRejectsMissingPlacementMode(t *testing.T) {
	submitter := &routingClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		target: ClusterRouteTarget{
			GroupID: "group-a",
			Members: []string{"node-a", "node-b"},
		},
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
	if !isRemoteError(err, iwire.ErrReadOnly) {
		t.Fatalf("InsertBatch err=%v want read-only", err)
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	if calls := submitter.snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func TestClusterRoutePreflightRejectsFanoutTokenBatch(t *testing.T) {
	submitter := &routingClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		target: ClusterRouteTarget{
			PlacementMode:   string(raftplacement.PlacementModeRingV1),
			Shape:           ClusterRouteShapeTokenBatch,
			TokenBatchClass: string(raftplacement.TokenBatchRouteFanoutRequiredV1),
		},
	}
	request := ClusterRouteRequest{
		Database:    "default",
		Catalog:     "default",
		Collection:  "users",
		CommandID:   iwire.CommandInsertBatch,
		CommandName: "insert_batch",
		Shape:       ClusterRouteShapeTokenBatch,
		Tokens:      []uint64{1, 2},
	}
	if _, _, err := PreflightClusterRoute(context.Background(), submitter, request); codeOf(err) != iwire.ErrReadOnly || !strings.Contains(err.Error(), "requires fanout before submit") {
		t.Fatalf("token batch fanout preflight err=%v want fanout read-only", err)
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	if calls := submitter.snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func TestClusterRoutePreflightRejectsUnsupportedQueryShape(t *testing.T) {
	submitter := &routingClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		target: ClusterRouteTarget{
			GroupID:       "group-a",
			PlacementMode: string(raftplacement.PlacementModeCollectionV1),
			Shape:         ClusterRouteShapeCollection,
		},
	}
	request := ClusterRouteRequest{
		Database:    "default",
		Catalog:     "default",
		Collection:  "users",
		CommandID:   iwire.CommandIndexLookup,
		CommandName: "index_lookup",
		Shape:       ClusterRouteShapeQuery,
	}
	if _, _, err := PreflightClusterRoute(context.Background(), submitter, request); codeOf(err) != iwire.ErrReadOnly || !strings.Contains(err.Error(), "query route shape is not supported") {
		t.Fatalf("query preflight err=%v want unsupported query read-only", err)
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("route calls=%d want 0 before unsupported query provider call", len(routes))
	}
	if calls := submitter.snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func TestClusterRoutePreflightRejectsUnsupportedTokenRouteKey(t *testing.T) {
	request := ClusterRouteRequest{
		Database:    "default",
		Catalog:     "default",
		Collection:  "users",
		CommandID:   iwire.CommandInsertBatch,
		CommandName: "insert_batch",
		Shape:       ClusterRouteShapeToken,
		TokenKnown:  true,
		Token:       11,
	}

	tests := []struct {
		name     string
		routeKey string
		wantMsg  string
	}{
		{name: "missing", routeKey: "", wantMsg: "missing route key"},
		{name: "unsupported", routeKey: "tenant_id", wantMsg: "unsupported route key \"tenant_id\""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			submitter := &routingClusterSubmitter{
				fakeClusterSubmitter: &fakeClusterSubmitter{},
				target: ClusterRouteTarget{
					GroupID:       "group-a",
					Members:       []string{"node-a", "node-b"},
					LeaderHint:    "node-a",
					PlacementMode: string(raftplacement.PlacementModeRingV1),
					RouteKey:      tt.routeKey,
					Shape:         ClusterRouteShapeToken,
					TokenKnown:    true,
					Token:         11,
					PartitionID:   "p0",
				},
			}
			if _, _, err := PreflightClusterRoute(context.Background(), submitter, request); codeOf(err) != iwire.ErrReadOnly || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("route key preflight err=%v want read-only containing %q", err, tt.wantMsg)
			}
			if routes := submitter.snapshotRoutes(); len(routes) != 1 {
				t.Fatalf("route calls=%d want 1", len(routes))
			}
			if calls := submitter.snapshot(); len(calls) != 0 {
				t.Fatalf("submitter calls=%d want 0", len(calls))
			}
		})
	}
}

func TestCatalogClusterRouteProviderRoutesResolvedCatalog(t *testing.T) {
	collectionProvider := NewCatalogClusterRouteProvider(mustNativewireRouteTestCatalog(t, raftplacement.PlacementModeCollectionV1))
	collectionTarget, err := collectionProvider.ClusterRoute(context.Background(), ClusterRouteRequest{
		Database:   "default",
		Catalog:    "default",
		Collection: "users",
		Shape:      ClusterRouteShapeCollection,
	})
	if err != nil {
		t.Fatalf("ClusterRoute collection: %v", err)
	}
	if collectionTarget.GroupID != "group-a" || collectionTarget.LeaderHint != "node-a" || collectionTarget.PlacementMode != string(raftplacement.PlacementModeCollectionV1) || collectionTarget.Shape != ClusterRouteShapeCollection {
		t.Fatalf("collection target=%+v want group-a/node-a collection", collectionTarget)
	}
	if !reflect.DeepEqual(collectionTarget.Members, []string{"node-a", "node-b"}) {
		t.Fatalf("collection members=%v want [node-a node-b]", collectionTarget.Members)
	}

	for _, mode := range []raftplacement.PlacementModeV1{raftplacement.PlacementModeTokenV1, raftplacement.PlacementModeRingV1} {
		t.Run(string(mode), func(t *testing.T) {
			provider := NewCatalogClusterRouteProvider(mustNativewireRouteBatchTestCatalog(t, mode))
			tokenTarget, err := provider.ClusterRoute(context.Background(), ClusterRouteRequest{
				Database:   "default",
				Catalog:    "default",
				Collection: "users",
				Shape:      ClusterRouteShapeToken,
				TokenKnown: true,
				Token:      12,
			})
			if err != nil {
				t.Fatalf("ClusterRoute token: %v", err)
			}
			if tokenTarget.GroupID != "group-a" || tokenTarget.LeaderHint != "node-a" || tokenTarget.PlacementMode != string(mode) || tokenTarget.Shape != ClusterRouteShapeToken {
				t.Fatalf("token target=%+v want group-a/node-a %s token", tokenTarget, mode)
			}
			if tokenTarget.RouteKey != string(raftplacement.RouteKeyDocumentIDV1) {
				t.Fatalf("token route key=%q want %q", tokenTarget.RouteKey, raftplacement.RouteKeyDocumentIDV1)
			}
			if !tokenTarget.TokenKnown || tokenTarget.Token != 12 || tokenTarget.PartitionID != "p1" {
				t.Fatalf("token target token known/token/partition=%v/%d/%q want true/12/p1", tokenTarget.TokenKnown, tokenTarget.Token, tokenTarget.PartitionID)
			}

			tests := []struct {
				name      string
				tokens    []uint64
				wantClass raftplacement.TokenBatchRouteClassV1
				wantGroup string
			}{
				{
					name:      "same_partition",
					tokens:    []uint64{1, 2},
					wantClass: raftplacement.TokenBatchRouteSamePartitionV1,
					wantGroup: "group-a",
				},
				{
					name:      "same_group_multi_partition",
					tokens:    []uint64{1, 12},
					wantClass: raftplacement.TokenBatchRouteSameGroupV1,
					wantGroup: "group-a",
				},
				{
					name:      "fanout_required",
					tokens:    []uint64{1, 20},
					wantClass: raftplacement.TokenBatchRouteFanoutRequiredV1,
				},
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					target, err := provider.ClusterRoute(context.Background(), ClusterRouteRequest{
						Database:   "default",
						Catalog:    "default",
						Collection: "users",
						Shape:      ClusterRouteShapeTokenBatch,
						Tokens:     tc.tokens,
					})
					if err != nil {
						t.Fatalf("ClusterRoute token batch: %v", err)
					}
					if target.PlacementMode != string(mode) || target.Shape != ClusterRouteShapeTokenBatch || target.TokenBatchClass != string(tc.wantClass) {
						t.Fatalf("token batch target=%+v want mode=%s class=%s", target, mode, tc.wantClass)
					}
					if target.RouteKey != string(raftplacement.RouteKeyDocumentIDV1) {
						t.Fatalf("token batch route key=%q want %q", target.RouteKey, raftplacement.RouteKeyDocumentIDV1)
					}
					if target.GroupID != tc.wantGroup {
						t.Fatalf("token batch group=%q want %q", target.GroupID, tc.wantGroup)
					}
					if tc.wantClass == raftplacement.TokenBatchRouteFanoutRequiredV1 && (target.LeaderHint != "" || len(target.Members) != 0) {
						t.Fatalf("fanout target unexpectedly selected leader/members: %+v", target)
					}
				})
			}
		})
	}
}

func TestClusterRoutePreflightSkipsMalformedDeterministicEntry(t *testing.T) {
	submitter := &routingClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		err:                  errors.New("unplaced collection"),
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	guard, err := client.replicatedMutationGuard(ctx, "malformed_insert_before_route")
	if err != nil {
		t.Fatalf("mutation guard: %v", err)
	}
	body, err := appendInsertBatchRequestBodyRefFlags(nil, "users", 0, false, collections.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"name":"Ada"}`)},
		AckVisible,
		0,
		guard,
	)
	if err != nil {
		t.Fatalf("append insert: %v", err)
	}
	_, _, err = client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("InsertBatch err=%v want invalid command", err)
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("route calls=%d want 0", len(routes))
	}
	if calls := submitter.snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func TestClusterRoutePreflightTokenPlacementAcceptsSingleID(t *testing.T) {
	catalog := mustNativewireRouteTestCatalog(t, raftplacement.PlacementModeTokenV1)
	submitter := &placementRouteClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		provider:             NewCatalogClusterRouteProvider(catalog),
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
	if err != nil {
		t.Fatalf("InsertBatch token placement: %v", err)
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	} else {
		token := raftplacement.DocumentIDTokenV1([]byte("u1"))
		if routes[0].Shape != ClusterRouteShapeToken || !routes[0].TokenKnown || routes[0].Token != token {
			t.Fatalf("route request=%+v want token route token=%d", routes[0], token)
		}
	}
	calls := submitter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("submitter calls=%d want 1", len(calls))
	}
	assertNativeClusterTokenRouteMetadata(t, calls[0], raftplacement.PlacementModeTokenV1, "p0", raftplacement.DocumentIDTokenV1([]byte("u1")))
}

func TestClusterMutationDocumentTokenHonorsDecodeLimits(t *testing.T) {
	id := []byte("configured-limit-id")
	rawIDs := iwire.AppendByteVector(nil, id)
	cmd := iwire.ValidatedCommand{
		Header: iwire.CommandHeader{ID: iwire.CommandInsertBatch},
		Known: []iwire.Section{
			{ID: iwire.SectionDocumentIDs, Bytes: rawIDs},
		},
	}

	if _, _, err := clusterMutationDocumentToken(cmd, iwire.Limits{MaxByteVectorBytes: uint64(len(id) - 1)}); codeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("clusterMutationDocumentToken low limit err=%v code=%d want resource exhausted", err, codeOf(err))
	}
	token, ok, err := clusterMutationDocumentToken(cmd, iwire.Limits{MaxByteVectorBytes: uint64(len(id))})
	if err != nil {
		t.Fatalf("clusterMutationDocumentToken configured limit: %v", err)
	}
	if !ok || token != raftplacement.DocumentIDTokenV1(id) {
		t.Fatalf("token ok/token=%v/%d want true/%d", ok, token, raftplacement.DocumentIDTokenV1(id))
	}
}

func TestClusterRoutePreflightTokenPlacementSingleIDMutationCommands(t *testing.T) {
	tests := []struct {
		name    string
		command iwire.CommandID
		run     func(context.Context, *Client) error
	}{
		{
			name:    "insert_batch",
			command: iwire.CommandInsertBatch,
			run: func(ctx context.Context, client *Client) error {
				_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
					[][]byte{[]byte("u1")},
					[][]byte{[]byte(`{"name":"Ada"}`)},
					AckVisible,
				)
				return err
			},
		},
		{
			name:    "replace_batch",
			command: iwire.CommandReplaceBatch,
			run: func(ctx context.Context, client *Client) error {
				_, _, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
					[][]byte{[]byte("u1")},
					[][]byte{[]byte(`{"name":"Ada"}`)},
					AckVisible,
				)
				return err
			},
		},
		{
			name:    "delete_batch",
			command: iwire.CommandDeleteBatch,
			run: func(ctx context.Context, client *Client) error {
				_, err := client.DeleteBatch(ctx, "users", [][]byte{[]byte("u1")}, AckVisible)
				return err
			},
		},
		{
			name:    "update_bson_set",
			command: iwire.CommandUpdateBSONSet,
			run: func(ctx context.Context, client *Client) error {
				_, _, err := client.UpdateBSONSet(ctx, "users", []byte("u1"), []collections.BSONSetField{
					{Key: "name", Value: mustNativewireBSONRawValue(t, "Ada")},
				}, AckVisible)
				return err
			},
		},
	}
	for _, mode := range []raftplacement.PlacementModeV1{raftplacement.PlacementModeTokenV1, raftplacement.PlacementModeRingV1} {
		for _, tc := range tests {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				catalog := mustNativewireRouteTestCatalog(t, mode)
				submitter := &placementRouteClusterSubmitter{
					fakeClusterSubmitter: &fakeClusterSubmitter{},
					provider:             NewCatalogClusterRouteProvider(catalog),
				}
				client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := client.Hello(ctx); err != nil {
					t.Fatalf("Hello: %v", err)
				}
				if err := tc.run(ctx, client); err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
				token := raftplacement.DocumentIDTokenV1([]byte("u1"))
				routes := submitter.snapshotRoutes()
				if len(routes) != 1 {
					t.Fatalf("route calls=%d want 1", len(routes))
				}
				if got := routes[0]; got.CommandID != tc.command || got.CommandName != tc.name || got.Shape != ClusterRouteShapeToken || !got.TokenKnown || got.Token != token {
					t.Fatalf("route request=%+v want command=%d name=%s token=%d", got, tc.command, tc.name, token)
				}
				calls := submitter.snapshot()
				if len(calls) != 1 {
					t.Fatalf("submitter calls=%d want 1", len(calls))
				}
				if calls[0].entry.Decoded.CommandID != tc.command {
					t.Fatalf("submitted command=%d want %d", calls[0].entry.Decoded.CommandID, tc.command)
				}
				assertNativeClusterTokenRouteMetadata(t, calls[0], mode, "p0", token)
			})
		}
	}
}

func TestClusterRoutePreflightCollectionPlacementAcceptsMultiIDBatch(t *testing.T) {
	catalog := mustNativewireRouteTestCatalog(t, raftplacement.PlacementModeCollectionV1)
	submitter := &placementRouteClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		provider:             NewCatalogClusterRouteProvider(catalog),
	}
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"name":"Ada"}`), []byte(`{"name":"Grace"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch collection placement multi-ID: %v", err)
	}
	routes := submitter.snapshotRoutes()
	if len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	wantTokens := []uint64{raftplacement.DocumentIDTokenV1([]byte("u1")), raftplacement.DocumentIDTokenV1([]byte("u2"))}
	if got := routes[0]; got.Shape != ClusterRouteShapeTokenBatch || got.TokenKnown || !reflect.DeepEqual(got.Tokens, wantTokens) {
		t.Fatalf("multi-ID route request=%+v want token_batch tokens=%v", got, wantTokens)
	}
	calls := submitter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("submitter calls=%d want 1", len(calls))
	}
	meta := calls[0].metadata
	if !meta.ClusterRouteKnown || meta.ClusterRouteShape != string(ClusterRouteShapeCollection) || meta.ClusterRoutePlacementMode != string(raftplacement.PlacementModeCollectionV1) {
		t.Fatalf("metadata route known/shape/mode=%v/%q/%q want collection", meta.ClusterRouteKnown, meta.ClusterRouteShape, meta.ClusterRoutePlacementMode)
	}
}

func TestClusterRoutePreflightTokenPlacementRejectsMultiID(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *Client) error
	}{
		{
			name: "insert_batch",
			run: func(ctx context.Context, client *Client) error {
				_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
					[][]byte{[]byte("u1"), []byte("u2")},
					[][]byte{[]byte(`{"name":"Ada"}`), []byte(`{"name":"Grace"}`)},
					AckVisible,
				)
				return err
			},
		},
		{
			name: "replace_batch",
			run: func(ctx context.Context, client *Client) error {
				_, _, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
					[][]byte{[]byte("u1"), []byte("u2")},
					[][]byte{[]byte(`{"name":"Ada"}`), []byte(`{"name":"Grace"}`)},
					AckVisible,
				)
				return err
			},
		},
		{
			name: "delete_batch",
			run: func(ctx context.Context, client *Client) error {
				_, err := client.DeleteBatch(ctx, "users", [][]byte{[]byte("u1"), []byte("u2")}, AckVisible)
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			catalog := mustNativewireRouteTestCatalog(t, raftplacement.PlacementModeRingV1)
			submitter := &placementRouteClusterSubmitter{
				fakeClusterSubmitter: &fakeClusterSubmitter{},
				provider:             NewCatalogClusterRouteProvider(catalog),
			}
			client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterSubmitter: submitter})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatalf("Hello: %v", err)
			}
			if err := tc.run(ctx, client); !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "requires command split before submit") {
				t.Fatalf("%s multi-ID token placement err=%v want read-only", tc.name, err)
			}
			if routes := submitter.snapshotRoutes(); len(routes) != 1 {
				t.Fatalf("route calls=%d want 1", len(routes))
			} else if routes[0].Shape != ClusterRouteShapeTokenBatch || routes[0].TokenKnown || len(routes[0].Tokens) != 2 {
				t.Fatalf("multi-ID route request=%+v want token_batch with two tokens", routes[0])
			}
			if calls := submitter.snapshot(); len(calls) != 0 {
				t.Fatalf("submitter calls=%d want 0", len(calls))
			}
		})
	}
}

func TestClusterRoutePreflightRejectsNativeQueryRouteBeforeLocalRead(t *testing.T) {
	catalog := mustNativewireRouteTestCatalog(t, raftplacement.PlacementModeRingV1)
	submitter := &placementRouteClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		provider:             NewCatalogClusterRouteProvider(catalog),
	}
	client, _, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, _, err := client.IndexLookup(ctx, "users", "email", "ada@example.com", CursorLimits{})
	if !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "query route shape is not supported") {
		t.Fatalf("IndexLookup cluster route err=%v want unsupported query read-only", err)
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("route calls=%d want 0 before unsupported query provider call", len(routes))
	}
	if calls := submitter.snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func TestClusterRoutePreflightRejectsNativeGetManyBeforeLocalRead(t *testing.T) {
	catalog := mustNativewireRouteTestCatalog(t, raftplacement.PlacementModeRingV1)
	submitter := &placementRouteClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		provider:             NewCatalogClusterRouteProvider(catalog),
	}
	client, _, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, _, err := client.GetMany(ctx, "users", [][]byte{[]byte("u1")})
	if !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "query route shape is not supported") {
		t.Fatalf("GetMany cluster route err=%v want unsupported query read-only", err)
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("route calls=%d want 0 before unsupported query provider call", len(routes))
	}
	if calls := submitter.snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func mustNativewireRouteTestCatalog(tb testing.TB, mode raftplacement.PlacementModeV1) raftplacement.ResolvedCatalogV1 {
	tb.Helper()
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Features: raftplacement.DefaultFeatureSet(),
		Groups: []raftplacement.GroupV1{
			{
				ID:         "group-a",
				Members:    []raftcluster.NodeID{"node-a", "node-b"},
				LeaderHint: "node-a",
			},
		},
		Placements: []raftplacement.CollectionPlacementV1{
			nativewireRouteTestPlacement(mode),
		},
	})
	if err != nil {
		tb.Fatalf("Validate route test catalog: %v", err)
	}
	return catalog
}

func mustNativewireRouteBatchTestCatalog(tb testing.TB, mode raftplacement.PlacementModeV1) raftplacement.ResolvedCatalogV1 {
	tb.Helper()
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Features: raftplacement.DefaultFeatureSet(),
		Groups: []raftplacement.GroupV1{
			{
				ID:         "group-a",
				Members:    []raftcluster.NodeID{"node-a", "node-b"},
				LeaderHint: "node-a",
			},
			{
				ID:         "group-b",
				Members:    []raftcluster.NodeID{"node-c", "node-d"},
				LeaderHint: "node-c",
			},
		},
		Placements: []raftplacement.CollectionPlacementV1{
			{
				Collection: raftplacement.CollectionRefV1{
					Database:   "default",
					Catalog:    "default",
					Collection: "users",
				},
				Mode: mode,
				TokenPartitions: []raftplacement.TokenPartitionV1{
					{ID: "p0", GroupID: "group-a", Start: 0, End: 9},
					{ID: "p1", GroupID: "group-a", Start: 10, End: 19},
					{ID: "p2", GroupID: "group-b", Start: 20, End: ^uint64(0)},
				},
			},
		},
	})
	if err != nil {
		tb.Fatalf("Validate batch route test catalog: %v", err)
	}
	return catalog
}

func mustNativewireCollectionGroupRouteCatalog(tb testing.TB) raftplacement.ResolvedCatalogV1 {
	tb.Helper()
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Features: raftplacement.DefaultFeatureSet(),
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a", "node-b"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-c", "node-d"}, LeaderHint: "node-c"},
		},
		Placements: []raftplacement.CollectionPlacementV1{
			{
				Collection: raftplacement.CollectionRefV1{
					Database:   "default",
					Catalog:    "default",
					Collection: "users",
				},
				Mode:    raftplacement.PlacementModeCollectionV1,
				GroupID: "group-b",
			},
		},
	})
	if err != nil {
		tb.Fatalf("Validate collection group route catalog: %v", err)
	}
	return catalog
}

func mustNativewireTokenGroupRouteCatalog(tb testing.TB, mode raftplacement.PlacementModeV1, groupBStart uint64) raftplacement.ResolvedCatalogV1 {
	tb.Helper()
	partitions := make([]raftplacement.TokenPartitionV1, 0, 2)
	if groupBStart > 0 {
		partitions = append(partitions, raftplacement.TokenPartitionV1{ID: "p0", GroupID: "group-a", Start: 0, End: groupBStart - 1})
	}
	partitions = append(partitions, raftplacement.TokenPartitionV1{ID: "p1", GroupID: "group-b", Start: groupBStart, End: ^uint64(0)})
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Features: raftplacement.DefaultFeatureSet(),
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a", "node-b"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-c", "node-d"}, LeaderHint: "node-c"},
		},
		Placements: []raftplacement.CollectionPlacementV1{
			{
				Collection: raftplacement.CollectionRefV1{
					Database:   "default",
					Catalog:    "default",
					Collection: "users",
				},
				Mode:            mode,
				TokenPartitions: partitions,
			},
		},
	})
	if err != nil {
		tb.Fatalf("Validate token group route catalog: %v", err)
	}
	return catalog
}

func nativewireRouteTestPlacement(mode raftplacement.PlacementModeV1) raftplacement.CollectionPlacementV1 {
	placement := raftplacement.CollectionPlacementV1{
		Collection: raftplacement.CollectionRefV1{
			Database:   "default",
			Catalog:    "default",
			Collection: "users",
		},
		Mode: mode,
	}
	if mode == raftplacement.PlacementModeCollectionV1 {
		placement.GroupID = "group-a"
		return placement
	}
	placement.TokenPartitions = []raftplacement.TokenPartitionV1{
		{
			ID:      "p0",
			GroupID: "group-a",
			Start:   0,
			End:     ^uint64(0),
		},
	}
	return placement
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

func TestClusterSubmitterCatalogVersionUpdateIsMonotonic(t *testing.T) {
	server := NewServer(ServerOptions{})
	if err := server.updateCatalogVersionFromClusterSubmitResult(ClusterSubmitResult{HasCatalogVersion: true, CatalogVersion: 10}); err != nil {
		t.Fatalf("update explicit version 10: %v", err)
	}
	if err := server.updateCatalogVersionFromClusterSubmitResult(ClusterSubmitResult{HasCatalogVersion: true, CatalogVersion: 8}); err != nil {
		t.Fatalf("update explicit version 8: %v", err)
	}
	if got := server.catalogVersion.Load(); got != 10 {
		t.Fatalf("catalog version after stale explicit update=%d want 10", got)
	}

	if err := server.updateCatalogVersionFromClusterSubmitResult(ClusterSubmitResult{
		ResponseSections: []iwire.Section{ackMetaCountsVersion(AckVisible, 12, true)},
	}); err != nil {
		t.Fatalf("update response-meta version 12: %v", err)
	}
	if err := server.updateCatalogVersionFromClusterSubmitResult(ClusterSubmitResult{
		ResponseSections: []iwire.Section{ackMetaCountsVersion(AckVisible, 11, true)},
	}); err != nil {
		t.Fatalf("update response-meta version 11: %v", err)
	}
	if got := server.catalogVersion.Load(); got != 12 {
		t.Fatalf("catalog version after stale response-meta update=%d want 12", got)
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

func TestClusterSubmitterStatsTrackRaftCommittedAndFollowerReject(t *testing.T) {
	t.Run("raft_committed_success", func(t *testing.T) {
		submitter := &fakeClusterSubmitter{
			resultHook: func(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata, result ClusterSubmitResult) (ClusterSubmitResult, error) {
				time.Sleep(2 * time.Millisecond)
				result.ActualAck = AckRaftCommitted
				result.CommittedRecoverable = true
				return replaceResponseAckPolicy(result, AckRaftCommitted), nil
			},
		}
		client, server, _, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Hello(ctx); err != nil {
			t.Fatalf("Hello: %v", err)
		}
		if _, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...); err != nil {
			t.Fatalf("InsertBatch raft_committed cluster: %v", err)
		}
		for key, want := range map[string]uint64{
			"cluster_submit.requests_total":           1,
			"cluster_submit.success_total":            1,
			"cluster_submit.errors_total":             0,
			"cluster_submit.ack_raft_committed_total": 1,
			"cluster_submit.read_only_total":          0,
		} {
			if got := nativewireTestStatUint64(t, server, key); got != want {
				t.Fatalf("stat %s=%d want %d", key, got, want)
			}
		}
		if got := nativewireTestStatUint64(t, server, "cluster_submit.nanos_total"); got == 0 {
			t.Fatalf("cluster_submit.nanos_total=0 want >0")
		}
	})

	t.Run("follower_reject", func(t *testing.T) {
		submitter := &fakeClusterSubmitter{
			status: ClusterFollowerAdmission("node-a:7000", "not leader"),
		}
		client, server, _, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Hello(ctx); err != nil {
			t.Fatalf("Hello: %v", err)
		}
		_, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
		if !isRemoteError(err, iwire.ErrReadOnly) {
			t.Fatalf("InsertBatch follower raft_committed err=%v want read-only", err)
		}
		for key, want := range map[string]uint64{
			"cluster_submit.requests_total":           1,
			"cluster_submit.success_total":            0,
			"cluster_submit.errors_total":             1,
			"cluster_submit.read_only_total":          1,
			"cluster_submit.ack_raft_committed_total": 0,
		} {
			if got := nativewireTestStatUint64(t, server, key); got != want {
				t.Fatalf("stat %s=%d want %d", key, got, want)
			}
		}
		if got := len(submitter.snapshot()); got != 0 {
			t.Fatalf("submitter calls=%d want 0", got)
		}
	})

	t.Run("durability_unavailable", func(t *testing.T) {
		submitter := &fakeClusterSubmitter{
			status: ClusterUnavailableAdmission("cluster admission unavailable"),
		}
		client, server, _, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Hello(ctx); err != nil {
			t.Fatalf("Hello: %v", err)
		}
		_, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
		if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
			t.Fatalf("InsertBatch unavailable raft_committed err=%v want durability unavailable", err)
		}
		for key, want := range map[string]uint64{
			"cluster_submit.requests_total":               1,
			"cluster_submit.success_total":                0,
			"cluster_submit.errors_total":                 1,
			"cluster_submit.durability_unavailable_total": 1,
			"cluster_submit.commit_ambiguous_total":       0,
			"cluster_submit.read_only_total":              0,
			"cluster_submit.ack_raft_committed_total":     0,
		} {
			if got := nativewireTestStatUint64(t, server, key); got != want {
				t.Fatalf("stat %s=%d want %d", key, got, want)
			}
		}
		if got := len(submitter.snapshot()); got != 0 {
			t.Fatalf("submitter calls=%d want 0", got)
		}
	})

	t.Run("commit_ambiguous", func(t *testing.T) {
		submitter := &fakeClusterSubmitter{
			resultHook: func(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata, result ClusterSubmitResult) (ClusterSubmitResult, error) {
				return ClusterSubmitResult{}, raftcluster.ErrCommitAmbiguous
			},
		}
		client, server, _, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Hello(ctx); err != nil {
			t.Fatalf("Hello: %v", err)
		}
		_, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
		if !isRemoteError(err, iwire.ErrCommitAmbiguous) {
			t.Fatalf("InsertBatch ambiguous raft_committed err=%v want commit ambiguous", err)
		}
		for key, want := range map[string]uint64{
			"cluster_submit.requests_total":               1,
			"cluster_submit.success_total":                0,
			"cluster_submit.errors_total":                 1,
			"cluster_submit.commit_ambiguous_total":       1,
			"cluster_submit.durability_unavailable_total": 0,
			"cluster_submit.read_only_total":              0,
			"cluster_submit.ack_raft_committed_total":     0,
		} {
			if got := nativewireTestStatUint64(t, server, key); got != want {
				t.Fatalf("stat %s=%d want %d", key, got, want)
			}
		}
		if got := len(submitter.snapshot()); got != 1 {
			t.Fatalf("submitter calls=%d want 1", got)
		}
	})
}

func TestClusterSubmitterStatsRecordPanicAsError(t *testing.T) {
	submitter := &fakeClusterSubmitter{
		resultHook: func(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata, result ClusterSubmitResult) (ClusterSubmitResult, error) {
			panic("submit panic")
		},
	}
	client, server, _, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{ClusterSubmitter: submitter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	body, err := appendCommandRequestBody(nil, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
	if err != nil {
		t.Fatalf("append request body: %v", err)
	}
	sections, err := iwire.DecodeSections(body, server.limits)
	if err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	cmd, err := server.registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("validate request sections: %v", err)
	}

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("handleClusterMutation did not re-panic")
			}
		}()
		_, _ = server.handleClusterMutation(ctx, iwire.Header{Type: iwire.FrameRequest, RequestID: 1}, cmd)
	}()

	for key, want := range map[string]uint64{
		"cluster_submit.requests_total":           1,
		"cluster_submit.success_total":            0,
		"cluster_submit.errors_total":             1,
		"cluster_submit.ack_raft_committed_total": 0,
	} {
		if got := nativewireTestStatUint64(t, server, key); got != want {
			t.Fatalf("stat %s=%d want %d", key, got, want)
		}
	}
}

func TestRaftClusterSubmitterConcreteBridgeCreateInsertRaftCommitted(t *testing.T) {
	client, server, mgr, _ := serveRaftClusterBridgePipe(t, raftcluster.LeaderAdmission())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	before := clientCatalogVersion(t, client, ctx)
	createSections := raftClusterCreateCollectionSectionsWithMeta(collections.CollectionMeta{
		Name: "users",
		Indexes: []collections.IndexDefinition{{
			Name:      "by_name",
			Field:     "name",
			ValueType: collections.IndexValueString,
		}},
	}, before, AckRaftCommitted)
	createResponse, err := client.commandSections(ctx, iwire.CommandCreateCollection, createSections...)
	if err != nil {
		t.Fatalf("CreateCollection raft bridge: %v", err)
	}
	meta, err := firstMetaFromResponse(createResponse)
	if err != nil {
		t.Fatalf("CreateCollection response meta: %v", err)
	}
	if meta.Name != "users" {
		t.Fatalf("created collection=%q want users", meta.Name)
	}
	if meta.Options.DataRootStoragePolicy != collections.RootStorageFast ||
		meta.Options.IndexStateStoragePolicy != collections.RootStorageFast ||
		len(meta.Indexes) != 1 ||
		meta.Indexes[0].StoragePolicy != collections.RootStorageFast {
		t.Fatalf("created storage policies data=%q index=%q indexes=%+v, want fast policies", meta.Options.DataRootStoragePolicy, meta.Options.IndexStateStoragePolicy, meta.Indexes)
	}
	created, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users after create: %v", err)
	}
	applied := created.Meta()
	if got, want := encodeCollectionMeta(meta), encodeCollectionMeta(applied); !bytes.Equal(got, want) {
		t.Fatalf("create response meta=%+v want applied catalog meta=%+v", meta, applied)
	}
	if ack, ok, err := responseMetaAckPolicy(createResponse); err != nil || !ok || ack != AckRaftCommitted {
		t.Fatalf("create response ack=(%d,%v,%v) want raft_committed", ack, ok, err)
	}
	afterCreate, err := server.currentCatalogVersion()
	if err != nil {
		t.Fatalf("server currentCatalogVersion: %v", err)
	}
	if afterCreate <= before {
		t.Fatalf("catalog version after create=%d want > %d", afterCreate, before)
	}

	insertResponse, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckRaftCommitted, "u1")...)
	if err != nil {
		t.Fatalf("InsertBatch raft bridge: %v", err)
	}
	if ack, ok, err := responseMetaAckPolicy(insertResponse); err != nil || !ok || ack != AckRaftCommitted {
		t.Fatalf("insert response ack=(%d,%v,%v) want raft_committed", ack, ok, err)
	}
	inserted, err := responseCount(insertResponse, "inserted_count")
	if err != nil {
		t.Fatalf("inserted_count: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("inserted_count=%d want 1", inserted)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"name":"Ada"}`)) {
		t.Fatalf("stored doc=%s want Ada JSON", got)
	}
	_ = clientCatalogVersion(t, client, ctx)

	replaceMatched, replaceModified, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON, [][]byte{[]byte("u1")}, [][]byte{[]byte(`{"name":"Ada"}`)}, AckRaftCommitted)
	if err != nil {
		t.Fatalf("ReplaceBatch no-op raft bridge: %v", err)
	}
	if replaceMatched != 1 || replaceModified != 0 {
		t.Fatalf("ReplaceBatch no-op matched/modified=%d/%d want 1/0", replaceMatched, replaceModified)
	}

	bsonCreateVersion := clientCatalogVersion(t, client, ctx)
	bsonCreateSections := raftClusterCreateCollectionSectionsWithMeta(collections.CollectionMeta{
		Name: "bson_users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}, bsonCreateVersion, AckRaftCommitted)
	if _, err := client.commandSections(ctx, iwire.CommandCreateCollection, bsonCreateSections...); err != nil {
		t.Fatalf("CreateCollection BSON raft bridge: %v", err)
	}
	_ = clientCatalogVersion(t, client, ctx)
	bsonDoc := testNativewireBSONDocument(t, bson.D{{Key: "_id", Value: "b1"}, {Key: "city", Value: "hnl"}})
	if _, err := client.InsertBatch(ctx, "bson_users", collections.DocumentFormatBSON, [][]byte{[]byte("b1")}, [][]byte{bsonDoc}, AckRaftCommitted); err != nil {
		t.Fatalf("InsertBatch BSON raft bridge: %v", err)
	}
	updateMatched, updateModified, err := client.UpdateBSONSet(ctx, "bson_users", []byte("b1"), []collections.BSONSetField{{Key: "city", Value: testNativewireBSONSetRawValue(t, "hnl")}}, AckRaftCommitted)
	if err != nil {
		t.Fatalf("UpdateBSONSet no-op raft bridge: %v", err)
	}
	if updateMatched != 1 || updateModified != 0 {
		t.Fatalf("UpdateBSONSet no-op matched/modified=%d/%d want 1/0", updateMatched, updateModified)
	}
}

func TestRaftClusterSubmitterConcreteBridgeVisibleAckDoesNotClaimRaft(t *testing.T) {
	client, _, _, _ := serveRaftClusterBridgePipe(t, raftcluster.LeaderAdmission())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	version := clientCatalogVersion(t, client, ctx)
	if _, err := client.commandSections(ctx, iwire.CommandCreateCollection, raftClusterCreateCollectionSections("users", version, AckRaftCommitted)...); err != nil {
		t.Fatalf("CreateCollection raft bridge: %v", err)
	}
	insertResponse, err := client.commandSections(ctx, iwire.CommandInsertBatch, clusterInsertBatchSections(t, client, ctx, AckVisible, "u1")...)
	if err != nil {
		t.Fatalf("InsertBatch visible bridge: %v", err)
	}
	if ack, ok, err := responseMetaAckPolicy(insertResponse); err != nil || !ok || ack != AckVisible {
		t.Fatalf("visible response ack=(%d,%v,%v) want visible", ack, ok, err)
	}
}

func TestRaftClusterSubmitterConcreteBridgeOmitResponseMetaAdvancesCatalogVersion(t *testing.T) {
	client, server, _, _ := serveRaftClusterBridgePipe(t, raftcluster.LeaderAdmission())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	before := clientCatalogVersion(t, client, ctx)
	createSections := raftClusterCreateCollectionSections("users", before, AckRaftCommitted)
	response := commandSectionsWithFlags(t, client, ctx, iwire.CommandCreateCollection, iwire.CommandFlagOmitResponseMeta, createSections...)
	if _, ok, err := responseMetaAckPolicy(response); err != nil || ok {
		t.Fatalf("response meta ack=(%v,%v) want omitted", ok, err)
	}
	meta, err := firstMetaFromResponse(response)
	if err != nil {
		t.Fatalf("CreateCollection response meta: %v", err)
	}
	if meta.Name != "users" {
		t.Fatalf("created collection=%q want users", meta.Name)
	}
	afterCreate, err := server.currentCatalogVersion()
	if err != nil {
		t.Fatalf("server currentCatalogVersion: %v", err)
	}
	if afterCreate <= before {
		t.Fatalf("catalog version after omitted response_meta create=%d want > %d", afterCreate, before)
	}
}

func TestRaftClusterSubmitterRequiresCollectionManagerBeforeSubmit(t *testing.T) {
	sections := append([]iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})},
	}, raftClusterCreateCollectionSections("users", 7, AckRaftCommitted)...)
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}

	cluster := raftClusterBridgeTestConfig(nil)
	cluster.Dir = t.TempDir()
	preflightCalls := 0
	commitCalls := 0
	applier := &recordingRaftClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}}
	bridge, err := raftcluster.NewSingleGroupSubmitter(raftcluster.SingleGroupSubmitterOptions{
		Cluster:           cluster,
		AdmissionProvider: raftcluster.StaticAdmissionProvider{Status: raftcluster.LeaderAdmission()},
		CommitSource: raftcluster.CommitSourceFunc(func(_ context.Context, req raftcluster.CommitCommandEntryV1Request) (raftcluster.CommitCommandEntryV1Result, error) {
			commitCalls++
			return raftcluster.CommitCommandEntryV1Result{
				Entry: raftcluster.CommittedCommandEntryV1{
					Term:                     1,
					Index:                    1,
					Bytes:                    bytes.Clone(req.EntryBytes),
					CurrentCatalogVersion:    req.CurrentCatalogVersion,
					HasCurrentCatalogVersion: req.HasCurrentCatalogVersion,
					SyncLocalCommandWAL:      req.SyncLocalCommandWAL,
					RequestMetadata:          req.RequestMetadata,
					ExpectedTarget:           req.ExpectedTarget,
				},
				Evidence: raftcluster.CommitEvidenceV1{
					Kind:                raftcluster.CommitEvidenceProductionConsensusV1,
					GroupID:             cluster.GroupID,
					NodeID:              cluster.NodeID,
					LeaderID:            cluster.NodeID,
					Term:                1,
					Index:               1,
					Committed:           true,
					ProductionConsensus: true,
				},
			}, nil
		}),
		Preflight: raftcluster.CommandEntryPreflightFunc(func(context.Context, raftcluster.CommandEntryPreflightRequestV1) (raftcluster.CommandEntryPreflightResultV1, error) {
			preflightCalls++
			return raftcluster.CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: raftcluster.CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) { return 7, true, nil }),
	})
	if err != nil {
		t.Fatalf("NewSingleGroupSubmitter: %v", err)
	}

	_, err = NewRaftClusterSubmitter(bridge).SubmitCommandEntryV1(context.Background(), entry, ClusterRequestMetadata{
		RequestID: 17,
		AckPolicy: AckRaftCommitted,
	})
	if nativeCodeOf(err) != iwire.ErrInvalidCommand {
		t.Fatalf("SubmitCommandEntryV1 err=%v code=%d want invalid-command", err, nativeCodeOf(err))
	}
	if preflightCalls != 0 || commitCalls != 0 || applier.calls != 0 {
		t.Fatalf("bridge touched before collection-manager validation: preflight=%d commit=%d apply=%d", preflightCalls, commitCalls, applier.calls)
	}
}

func TestRaftClusterSubmitterConcreteBridgeMissingCollectionPreflightDoesNotConsumeIndex(t *testing.T) {
	client, _, mgr, _ := serveRaftClusterBridgePipe(t, raftcluster.LeaderAdmission())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON, [][]byte{[]byte("u-missing")}, [][]byte{[]byte(`{"name":"Missing"}`)}, AckRaftCommitted)
	if err == nil {
		t.Fatal("InsertBatch missing collection unexpectedly succeeded")
	}
	if _, openErr := mgr.OpenCollection("users"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after rejected insert err=%v want ErrCollectionNotFound", openErr)
	}
	version := clientCatalogVersion(t, client, ctx)
	if _, err := client.commandSections(ctx, iwire.CommandCreateCollection, raftClusterCreateCollectionSections("users", version, AckRaftCommitted)...); err != nil {
		t.Fatalf("CreateCollection after rejected insert: %v", err)
	}
	_ = clientCatalogVersion(t, client, ctx)
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON, [][]byte{[]byte("u1")}, [][]byte{[]byte(`{"name":"Ada"}`)}, AckRaftCommitted); err != nil {
		t.Fatalf("InsertBatch after create: %v", err)
	}
}

func TestRaftClusterSubmitterShapesResponseFromBridgeDecodedEntry(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	defer func() { _ = db.Close() }()
	manager := collections.NewCollectionManager(db)

	defaultLimits := iwire.DefaultLimits()
	limits := defaultLimits
	limits.MaxSectionLen = defaultLimits.MaxSectionLen + (1 << 20)
	entry := raftClusterLargeInsertBatchEntry(t, limits, int(defaultLimits.MaxSectionLen)+1)
	applier := &recordingRaftClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	cluster := raftClusterBridgeTestConfig(nil)
	cluster.Dir = t.TempDir()
	bridge, err := raftcluster.NewSingleGroupSubmitter(raftcluster.SingleGroupSubmitterOptions{
		Cluster:           cluster,
		AdmissionProvider: raftcluster.StaticAdmissionProvider{Status: raftcluster.LeaderAdmission()},
		CommitSource: raftcluster.NewSequencedCommitSource(raftcluster.SequencedCommitSourceOptions{
			GroupID:             cluster.GroupID,
			NodeID:              cluster.NodeID,
			LeaderID:            cluster.NodeID,
			EvidenceKind:        raftcluster.CommitEvidenceProductionConsensusV1,
			ProductionConsensus: true,
		}),
		Preflight: raftcluster.CommandEntryPreflightFunc(func(context.Context, raftcluster.CommandEntryPreflightRequestV1) (raftcluster.CommandEntryPreflightResultV1, error) {
			return raftcluster.CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: raftcluster.CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) { return 7, true, nil }),
		DecodeLimits:           limits,
	})
	if err != nil {
		t.Fatalf("NewSingleGroupSubmitter: %v", err)
	}

	result, err := NewRaftClusterSubmitter(bridge, manager).SubmitCommandEntryV1(context.Background(), entry, ClusterRequestMetadata{
		RequestID: 17,
		AckPolicy: AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("SubmitCommandEntryV1: %v", err)
	}
	if result.ActualAck != AckRaftCommitted || !result.CommittedRecoverable {
		t.Fatalf("ack/recoverable=%d/%v want raft_committed/true", result.ActualAck, result.CommittedRecoverable)
	}
	if applier.calls != 1 {
		t.Fatalf("apply calls=%d want 1", applier.calls)
	}
	if _, err := metadataSection(result.ResponseSections, iwire.SectionDocumentIDs); err != nil {
		t.Fatalf("document_ids response section: %v", err)
	}
	inserted, err := responseCount(result.ResponseSections, "inserted_count")
	if err != nil {
		t.Fatalf("inserted_count: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("inserted_count=%d want 1", inserted)
	}
}

func TestRaftClusterSubmitterCatalogVersionMismatchMapsToNativeError(t *testing.T) {
	err := nativeErrorForRaftClusterSubmit(raftcluster.ErrCatalogVersionMismatch)
	if code, ok := iwire.ErrorCodeOf(err); !ok || code != iwire.ErrCatalogVersionMismatch {
		t.Fatalf("nativeErrorForRaftClusterSubmit code=%v ok=%v err=%v, want catalog-version-mismatch", code, ok, err)
	}
}

func TestRaftClusterSubmitterLocalAckUnavailableMapsToNativeError(t *testing.T) {
	err := nativeErrorForRaftClusterSubmit(raftcluster.ErrLocalAckUnavailable)
	if code, ok := iwire.ErrorCodeOf(err); !ok || code != iwire.ErrDurabilityUnavailable {
		t.Fatalf("nativeErrorForRaftClusterSubmit code=%v ok=%v err=%v, want durability-unavailable", code, ok, err)
	}
}

func TestRaftClusterSubmitterRouteGroupMismatchMapsToReadOnly(t *testing.T) {
	err := nativeErrorForRaftClusterSubmit(raftcluster.ErrRouteGroupMismatch)
	if code, ok := iwire.ErrorCodeOf(err); !ok || code != iwire.ErrReadOnly {
		t.Fatalf("nativeErrorForRaftClusterSubmit code=%v ok=%v err=%v, want read-only", code, ok, err)
	}
}

func TestRaftClusterSubmitterPreservesCollectionConflictCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want iwire.ErrorCode
	}{
		{
			name: "duplicate_document_id",
			err:  fmt.Errorf("raftapply: %w", collections.ErrDuplicateDocumentID),
			want: iwire.ErrDuplicateDocumentID,
		},
		{
			name: "document_exists",
			err:  fmt.Errorf("raftapply: %w", collections.ErrDocumentExists),
			want: iwire.ErrDocumentExists,
		},
		{
			name: "unique_index_conflict",
			err:  fmt.Errorf("raftapply: %w", collections.ErrUniqueIndexConflict),
			want: iwire.ErrUniqueIndexConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nativeErrorForDeterministicCode(raftentry.ErrorRejectedConflictV1, tt.err)
			if code, ok := iwire.ErrorCodeOf(err); !ok || code != tt.want {
				t.Fatalf("nativeErrorForDeterministicCode code=%v ok=%v err=%v, want %v", code, ok, err, tt.want)
			}
		})
	}
}

func TestRaftClusterSubmitterRouteGroupMismatchRejectsBeforeLocalMutation(t *testing.T) {
	provider := &staticClusterRouteProvider{
		target: ClusterRouteTarget{
			GroupID:       "group-b",
			Members:       []string{"node-c", "node-d"},
			LeaderHint:    "node-c",
			PlacementMode: "collection",
		},
	}
	client, _, mgr, _ := serveRaftClusterBridgePipeWithRoute(t, raftcluster.LeaderAdmission(), provider)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	version := clientCatalogVersion(t, client, ctx)
	_, err := client.commandSections(ctx, iwire.CommandCreateCollection, raftClusterCreateCollectionSections("users", version, AckRaftCommitted)...)
	if !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "route group") {
		t.Fatalf("CreateCollection route mismatch err=%v want read-only route group mismatch", err)
	}
	if _, openErr := mgr.OpenCollection("users"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after route mismatch err=%v want ErrCollectionNotFound", openErr)
	}
	routes := provider.snapshotRoutes()
	if len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	if got := routes[0]; got.Database != "default" || got.Catalog != "default" || got.Collection != "users" || got.CommandID != iwire.CommandCreateCollection {
		t.Fatalf("route request=%+v want default/default/users create_collection", got)
	}
}

func TestRaftClusterSubmitterGroupRoutedDispatcherRoutesCollectionWrite(t *testing.T) {
	provider := &staticClusterRouteProvider{
		target: ClusterRouteTarget{
			GroupID:       "group-b",
			Members:       []string{"node-b", "node-c"},
			LeaderHint:    "node-b",
			PlacementMode: "collection",
			Shape:         ClusterRouteShapeCollection,
		},
	}
	client, groupA, groupB, mgr, _ := serveGroupRoutedRaftClusterBridgePipe(t, provider)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	version := clientCatalogVersion(t, client, ctx)
	if _, err := client.commandSections(ctx, iwire.CommandCreateCollection, raftClusterCreateCollectionSections("users", version, AckVisible)...); err != nil {
		t.Fatalf("CreateCollection group-routed dispatcher: %v", err)
	}
	if _, err := mgr.OpenCollection("users"); err != nil {
		t.Fatalf("OpenCollection users after routed create: %v", err)
	}
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	calls := groupB.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("group-b calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandCreateCollection {
		t.Fatalf("group-b command=%d want create_collection", got)
	}
	meta := calls[0].metadata
	if !meta.ClusterRouteKnown || meta.ClusterRouteShape != string(ClusterRouteShapeCollection) || meta.ClusterRouteGroupID != "group-b" || meta.ClusterRouteLeaderHint != "node-b" || meta.ClusterRoutePlacementMode != "collection" {
		t.Fatalf("group-b route metadata=%+v want collection route to group-b/node-b", meta)
	}
}

func TestRaftClusterSubmitterGroupRoutedDispatcherRoutesSingleTokenWrite(t *testing.T) {
	token := raftplacement.DocumentIDTokenV1([]byte("u1"))
	provider := &staticClusterRouteProvider{
		target: ClusterRouteTarget{
			GroupID:       "group-b",
			Members:       []string{"node-b", "node-c"},
			LeaderHint:    "node-b",
			PlacementMode: "token",
			RouteKey:      string(raftplacement.RouteKeyDocumentIDV1),
			Shape:         ClusterRouteShapeToken,
			TokenKnown:    true,
			Token:         token,
			PartitionID:   "p0",
		},
	}
	client, groupA, groupB, _, _ := serveGroupRoutedRaftClusterBridgePipe(t, provider)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON, [][]byte{[]byte("u1")}, [][]byte{[]byte(`{"name":"Ada"}`)}, AckVisible); err != nil {
		t.Fatalf("InsertBatch group-routed dispatcher: %v", err)
	}
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	calls := groupB.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("group-b calls=%d want 1", len(calls))
	}
	meta := calls[0].metadata
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("group-b command=%d want insert_batch", got)
	}
	if !meta.ClusterRouteKnown || meta.ClusterRouteShape != string(ClusterRouteShapeToken) || meta.ClusterRouteGroupID != "group-b" || meta.ClusterRouteKey != string(raftplacement.RouteKeyDocumentIDV1) || !meta.ClusterRouteTokenKnown || meta.ClusterRouteToken != token || meta.ClusterRoutePartitionID != "p0" {
		t.Fatalf("group-b token route metadata=%+v want group-b token=%d p0", meta, token)
	}
}

func TestRaftClusterSubmitterGroupRoutedDispatcherCatalogRoutesCollectionOwner(t *testing.T) {
	provider := NewCatalogClusterRouteProvider(mustNativewireCollectionGroupRouteCatalog(t))
	client, groupA, groupB, mgr, _ := serveGroupRoutedRaftClusterBridgePipe(t, provider)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	version := clientCatalogVersion(t, client, ctx)
	if _, err := client.commandSections(ctx, iwire.CommandCreateCollection, raftClusterCreateCollectionSections("users", version, AckVisible)...); err != nil {
		t.Fatalf("CreateCollection catalog group-routed dispatcher: %v", err)
	}
	if _, err := mgr.OpenCollection("users"); err != nil {
		t.Fatalf("OpenCollection users after catalog routed create: %v", err)
	}
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	calls := groupB.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("group-b calls=%d want 1", len(calls))
	}
	meta := calls[0].metadata
	if !meta.ClusterRouteKnown || meta.ClusterRouteShape != string(ClusterRouteShapeCollection) || meta.ClusterRouteGroupID != "group-b" || meta.ClusterRouteLeaderHint != "node-c" || meta.ClusterRoutePlacementMode != string(raftplacement.PlacementModeCollectionV1) {
		t.Fatalf("group-b catalog route metadata=%+v want collection owner group-b/node-c", meta)
	}
}

func TestRaftClusterSubmitterGroupRoutedDispatcherCatalogRoutesSingleTokenOwner(t *testing.T) {
	id := []byte("u1")
	token := raftplacement.DocumentIDTokenV1(id)
	provider := NewCatalogClusterRouteProvider(mustNativewireTokenGroupRouteCatalog(t, raftplacement.PlacementModeRingV1, token))
	client, groupA, groupB, _, _ := serveGroupRoutedRaftClusterBridgePipe(t, provider)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON, [][]byte{id}, [][]byte{[]byte(`{"name":"Ada"}`)}, AckVisible); err != nil {
		t.Fatalf("InsertBatch catalog group-routed dispatcher: %v", err)
	}
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	calls := groupB.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("group-b calls=%d want 1", len(calls))
	}
	meta := calls[0].metadata
	if !meta.ClusterRouteKnown || meta.ClusterRouteShape != string(ClusterRouteShapeToken) || meta.ClusterRouteGroupID != "group-b" || meta.ClusterRouteLeaderHint != "node-c" || meta.ClusterRouteKey != string(raftplacement.RouteKeyDocumentIDV1) || !meta.ClusterRouteTokenKnown || meta.ClusterRouteToken != token || meta.ClusterRoutePartitionID != "p1" {
		t.Fatalf("group-b catalog token route metadata=%+v want group-b/node-c token=%d p1", meta, token)
	}
}

func TestRaftClusterSubmitterGroupRoutedDispatcherRejectsUnknownGroupBeforeSubmit(t *testing.T) {
	token := raftplacement.DocumentIDTokenV1([]byte("u1"))
	provider := &staticClusterRouteProvider{
		target: ClusterRouteTarget{
			GroupID:       "group-z",
			Members:       []string{"node-z"},
			LeaderHint:    "node-z",
			PlacementMode: "token",
			RouteKey:      string(raftplacement.RouteKeyDocumentIDV1),
			Shape:         ClusterRouteShapeToken,
			TokenKnown:    true,
			Token:         token,
			PartitionID:   "p0",
		},
	}
	client, groupA, groupB, _, _ := serveGroupRoutedRaftClusterBridgePipe(t, provider)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON, [][]byte{[]byte("u1")}, [][]byte{[]byte(`{"name":"Ada"}`)}, AckVisible)
	if !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "route target unknown") {
		t.Fatalf("InsertBatch unknown group err=%v want read-only route target unknown", err)
	}
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	if calls := groupB.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-b calls=%d want 0", len(calls))
	}
}

func TestRaftClusterSubmitterConcreteBridgeFollowerRejectsBeforeApply(t *testing.T) {
	client, _, mgr, _ := serveRaftClusterBridgePipe(t, raftcluster.FollowerAdmission("node-b", "not leader"))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	version := clientCatalogVersion(t, client, ctx)
	_, err := client.commandSections(ctx, iwire.CommandCreateCollection, raftClusterCreateCollectionSections("users", version, AckRaftCommitted)...)
	if !isRemoteError(err, iwire.ErrReadOnly) {
		t.Fatalf("CreateCollection follower err=%v want read-only", err)
	}
	if _, openErr := mgr.OpenCollection("users"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after follower rejection err=%v want ErrCollectionNotFound", openErr)
	}
}

func commandSectionsWithFlags(t testing.TB, client *Client, ctx context.Context, commandID iwire.CommandID, flags uint64, sections ...iwire.Section) []iwire.Section {
	t.Helper()
	body, err := appendCommandHeaderSectionFlags(nil, commandID, flags)
	if err != nil {
		t.Fatalf("append command header: %v", err)
	}
	for _, section := range sections {
		body, err = iwire.AppendSection(body, section)
		if err != nil {
			t.Fatalf("append section %d: %v", section.ID, err)
		}
	}
	_, response, err := client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if err != nil {
		t.Fatalf("roundTrip: %v", err)
	}
	decoded, err := iwire.DecodeSections(response, client.limits)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return decoded
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

func serveRaftClusterBridgePipe(t testing.TB, admission raftcluster.AdmissionStatus) (*Client, *Server, *collections.CollectionManager, *backenddb.DB) {
	t.Helper()
	return serveRaftClusterBridgePipeWithRoute(t, admission, nil)
}

func serveRaftClusterBridgePipeWithRoute(t testing.TB, admission raftcluster.AdmissionStatus, routeProvider ClusterRouteProvider) (*Client, *Server, *collections.CollectionManager, *backenddb.DB) {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          t.TempDir(),
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		t.Fatalf("open command WAL db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	cluster := raftClusterBridgeTestConfig(db)
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB:      db,
		Cluster: cluster,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("open raft FSM: %v", err)
	}
	bridge, err := raftcluster.NewSingleGroupSubmitter(raftcluster.SingleGroupSubmitterOptions{
		Cluster:           cluster,
		AdmissionProvider: raftcluster.StaticAdmissionProvider{Status: admission},
		CommitSource: raftcluster.NewSequencedCommitSource(raftcluster.SequencedCommitSourceOptions{
			GroupID:             cluster.GroupID,
			NodeID:              cluster.NodeID,
			LeaderID:            cluster.NodeID,
			EvidenceKind:        raftcluster.CommitEvidenceProductionConsensusV1,
			ProductionConsensus: true,
		}),
		Preflight: fsm,
		Applier:   fsm,
		CatalogVersionProvider: raftcluster.CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) {
			state := db.State()
			if state == nil {
				return 0, false, nil
			}
			return state.CommitSeq, true, nil
		}),
	})
	if err != nil {
		_ = fsm.Close()
		_ = db.Close()
		t.Fatalf("NewSingleGroupSubmitter: %v", err)
	}
	var submitter ClusterSubmitter = NewRaftClusterSubmitter(bridge, mgr)
	if routeProvider != nil {
		submitter = NewRoutedRaftClusterSubmitter(bridge, routeProvider, mgr)
	}
	server := NewServer(ServerOptions{
		Collections:      mgr,
		Backend:          db,
		ClusterSubmitter: submitter,
	})
	client, _ := servePipe(t, server)
	t.Cleanup(func() {
		_ = fsm.Close()
		_ = db.Close()
	})
	return client, server, mgr, db
}

func serveGroupRoutedRaftClusterBridgePipe(t testing.TB, routeProvider ClusterRouteProvider) (*Client, *recordingRaftCommandSubmitter, *recordingRaftCommandSubmitter, *collections.CollectionManager, *backenddb.DB) {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	groupA := &recordingRaftCommandSubmitter{groupID: "group-a", manager: mgr}
	groupB := &recordingRaftCommandSubmitter{groupID: "group-b", manager: mgr}
	registry, err := raftcluster.NewGroupSubmitterRegistryV1([]raftcluster.GroupSubmitterV1{
		{GroupID: "group-a", Submitter: groupA},
		{GroupID: "group-b", Submitter: groupB},
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewGroupSubmitterRegistryV1: %v", err)
	}
	dispatcher, err := raftcluster.NewGroupRoutedSubmitter(raftcluster.GroupRoutedSubmitterOptions{Registry: registry})
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewGroupRoutedSubmitter: %v", err)
	}
	server := NewServer(ServerOptions{
		Collections:      mgr,
		Backend:          db,
		ClusterSubmitter: NewRoutedRaftClusterSubmitter(dispatcher, routeProvider, mgr),
	})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	return client, groupA, groupB, mgr, db
}

type recordingRaftClusterApplier struct {
	calls  int
	result raftentry.ApplyResultV1
	err    error
}

func (a *recordingRaftClusterApplier) ApplyCommittedCommandEntryV1(context.Context, raftcluster.CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	a.calls++
	return a.result, a.err
}

func raftClusterBridgeTestConfig(db *backenddb.DB) raftcluster.Config {
	dir := ""
	if db != nil {
		dir = db.Dir()
	}
	return raftcluster.Config{
		Dir:     dir,
		NodeID:  "node-a",
		GroupID: "group-a",
		Peers: []raftcluster.Peer{
			{ID: "node-a", Address: "127.0.0.1:9301"},
			{ID: "node-b", Address: "127.0.0.1:9302"},
			{ID: "node-c", Address: "127.0.0.1:9303"},
		},
	}
}

func raftClusterLargeInsertBatchEntry(tb testing.TB, limits iwire.Limits, documentLen int) []byte {
	tb.Helper()
	document := bytes.Repeat([]byte{'x'}, documentLen)
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("raftcluster/large-insert")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 7)},
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatJSON),
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("large-1"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, document)},
	}
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		tb.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := iwire.AppendDeterministicEntryWithLimits(nil, cmd, limits)
	if err != nil {
		tb.Fatalf("AppendDeterministicEntryWithLimits: %v", err)
	}
	return entry
}

func raftClusterCreateCollectionSections(name string, catalogVersion uint64, ack AckPolicy) []iwire.Section {
	return raftClusterCreateCollectionSectionsWithMeta(collections.CollectionMeta{Name: name}, catalogVersion, ack)
}

func raftClusterCreateCollectionSectionsWithMeta(meta collections.CollectionMeta, catalogVersion uint64, ack AckPolicy) []iwire.Section {
	sections := []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("cluster-create-" + meta.Name)},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
		{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(meta)},
	}
	if ack != 0 {
		sections = append(sections, ackSection(ack))
	}
	return sections
}

func BenchmarkRaftClusterSubmitterConcreteBridgeUpdateBSONSet(b *testing.B) {
	client, _, _, _ := serveRaftClusterBridgePipe(b, raftcluster.LeaderAdmission())
	ctx := context.Background()
	if err := client.Hello(ctx); err != nil {
		b.Fatalf("Hello: %v", err)
	}
	version := clientCatalogVersion(b, client, ctx)
	createSections := raftClusterCreateCollectionSectionsWithMeta(collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}, version, AckRaftCommitted)
	if _, err := client.commandSections(ctx, iwire.CommandCreateCollection, createSections...); err != nil {
		b.Fatalf("CreateCollection raft bridge: %v", err)
	}
	_ = clientCatalogVersion(b, client, ctx)
	id := []byte("u1")
	doc := testNativewireBSONDocument(b, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatBSON, [][]byte{id}, [][]byte{doc}, AckRaftCommitted); err != nil {
		b.Fatalf("InsertBatch seed raft bridge: %v", err)
	}
	fields := []collections.BSONSetField{{Key: "city", Value: testNativewireBSONSetRawValue(b, "hnl")}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matched, modified, err := client.UpdateBSONSet(ctx, "users", id, fields, AckRaftCommitted)
		if err != nil {
			b.Fatalf("UpdateBSONSet raft bridge: %v", err)
		}
		if matched != 1 || modified != 0 {
			b.Fatalf("UpdateBSONSet matched/modified=%d/%d want 1/0", matched, modified)
		}
	}
	b.ReportMetric(float64(b.N), "ops_total")
}

func testNativewireBSONDocument(tb testing.TB, document bson.D) []byte {
	tb.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		tb.Fatalf("Marshal BSON: %v", err)
	}
	return encoded
}

func testNativewireBSONSetRawValue(tb testing.TB, value any) bson.RawValue {
	tb.Helper()
	typ, raw, err := bson.MarshalValue(value)
	if err != nil {
		tb.Fatalf("MarshalValue(%T): %v", value, err)
	}
	return bson.RawValue{Type: typ, Value: raw}
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
