package nativewire

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func seedReadCollection(t *testing.T, mgr *collections.CollectionManager) *collections.Collection {
	t.Helper()
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
		Indexes: []collections.IndexDefinition{
			{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: collections.IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1"), []byte("u2")}, [][]byte{
		[]byte(`{"email":"ada@example.com","city":"hnl","name":"Ada"}`),
		[]byte(`{"email":"grace@example.com","city":"hnl","name":"Grace"}`),
	}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	return col
}

func TestReadCommandsParity(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	col := seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	docs, present, err := client.GetMany(ctx, "users", [][]byte{[]byte("u2"), []byte("missing"), []byte("u1")})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if got, want := present, []bool{true, false, true}; !boolSlicesEqual(got, want) {
		t.Fatalf("present=%v want %v", got, want)
	}
	directU2, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("direct get u2: %v", err)
	}
	if !bytes.Equal(docs[0], directU2) || len(docs[1]) != 0 {
		t.Fatalf("docs mismatch docs=%q directU2=%q", docs, directU2)
	}
	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	handleDocs, handlePresent, err := client.GetManyHandle(ctx, handle, [][]byte{[]byte("u1"), []byte("missing")})
	if err != nil {
		t.Fatalf("GetManyHandle: %v", err)
	}
	if got, want := handlePresent, []bool{true, false}; !boolSlicesEqual(got, want) {
		t.Fatalf("handle present=%v want %v", got, want)
	}
	if !bytes.Contains(handleDocs[0], []byte(`"Ada"`)) || len(handleDocs[1]) != 0 {
		t.Fatalf("handle docs=%q", handleDocs)
	}
	missingDocs, missingPresent, err := client.GetManyHandle(ctx, handle, [][]byte{[]byte("missing")})
	if err != nil {
		t.Fatalf("GetManyHandle missing after present: %v", err)
	}
	if got, want := missingPresent, []bool{false}; !boolSlicesEqual(got, want) {
		t.Fatalf("missing present after present=%v want %v", got, want)
	}
	if len(missingDocs) != 1 || len(missingDocs[0]) != 0 {
		t.Fatalf("missing docs after present=%q", missingDocs)
	}

	ids, truncated, err := client.IndexLookup(ctx, "users", "email", "ada@example.com", CursorLimits{MaxItems: 10})
	if err != nil {
		t.Fatalf("IndexLookup: %v", err)
	}
	if truncated || len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("lookup ids=%q truncated=%v", ids, truncated)
	}
	directIDs, directTruncated, err := col.FindByIndexValueLimit("email", "ada@example.com", 10)
	if err != nil {
		t.Fatalf("direct FindByIndexValueLimit: %v", err)
	}
	if directTruncated != truncated || !byteMatrixEqual(ids, directIDs) {
		t.Fatalf("lookup ids=%q trunc=%v direct ids=%q trunc=%v", ids, truncated, directIDs, directTruncated)
	}

	rangeIDs, rangeTruncated, err := client.IndexRange(ctx, "users", "city", IndexRange{
		Lower:          Scalar{Value: "h"},
		LowerInclusive: true,
		Upper:          Scalar{Value: "z"},
		UpperInclusive: true,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("IndexRange: %v", err)
	}
	if rangeTruncated || len(rangeIDs) != 2 {
		t.Fatalf("range ids=%q truncated=%v", rangeIDs, rangeTruncated)
	}
}

func TestReadDefaultReportsLocalStaleMetadata(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	result, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if !result.ReadMeta.Valid || result.ReadMeta.ActualConsistency != ConsistencyLocalStale {
		t.Fatalf("read meta=%+v want local-stale", result.ReadMeta)
	}
	if result.ReadMeta.ServingNode != "" || result.ReadMeta.LeaderNode != "" || result.ReadMeta.HasAppliedIndex {
		t.Fatalf("standalone read meta unexpectedly reported cluster fields: %+v", result.ReadMeta)
	}
}

func TestReadMetadataUsesDocumentedConsistencyPolicyNames(t *testing.T) {
	tests := []struct {
		name   string
		policy ConsistencyPolicy
		wire   string
	}{
		{name: "local_stale", policy: ConsistencyLocalStale, wire: "local_stale"},
		{name: "leader_read", policy: ConsistencyLeaderRead, wire: "leader_read"},
		{name: "linearizable", policy: ConsistencyLinearizable, wire: "linearizable"},
		{name: "lease_read", policy: ConsistencyLeaseRead, wire: "lease_read"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := appendReadMetaPayload(nil, ReadMetadata{ActualConsistency: tt.policy})
			fields, err := decodeStringMap(payload)
			if err != nil {
				t.Fatalf("decode fields: %v", err)
			}
			if got := fields["actual_consistency_policy"]; got != tt.wire {
				t.Fatalf("actual_consistency_policy=%q want %q", got, tt.wire)
			}
			if got := fields["actual_consistency"]; got != tt.wire {
				t.Fatalf("actual_consistency=%q want %q", got, tt.wire)
			}
			meta, err := decodeReadMetadataPayload(payload)
			if err != nil {
				t.Fatalf("decode read metadata: %v", err)
			}
			if !meta.Valid || meta.ActualConsistency != tt.policy {
				t.Fatalf("decoded meta=%+v want policy %d", meta, tt.policy)
			}
		})
	}
}

func TestReadStrongConsistencyRequiresCoordinator(t *testing.T) {
	for _, policy := range []ConsistencyPolicy{ConsistencyLeaderRead, ConsistencyLinearizable, ConsistencyLeaseRead} {
		t.Run(consistencyPolicyName(policy), func(t *testing.T) {
			client, mgr, _ := serveCollectionPipe(t)
			seedReadCollection(t, mgr)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatalf("Hello: %v", err)
			}

			_, err := client.OpenScanWithOptions(ctx, "users", CursorLimits{MaxItems: 1}, ReadOptions{ConsistencyPolicy: policy})
			if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
				t.Fatalf("OpenScan policy=%s err=%v want consistency_unavailable", consistencyPolicyName(policy), err)
			}
		})
	}
}

func TestReadConsistencyPolicyRejectedForUnsupportedReadCommands(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	tests := []struct {
		name      string
		commandID iwire.CommandID
		sections  []iwire.Section
	}{
		{name: "list_collections", commandID: iwire.CommandListCollections},
		{name: "list_indexes", commandID: iwire.CommandListIndexes, sections: []iwire.Section{collectionNameRef("users")}},
		{name: "open_collection", commandID: iwire.CommandOpenCollection, sections: []iwire.Section{collectionNameRef("users")}},
		{name: "close_collection", commandID: iwire.CommandCloseCollection, sections: []iwire.Section{collectionHandleRef(handle)}},
		{name: "stats", commandID: iwire.CommandStats},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, policy := range []ConsistencyPolicy{ConsistencyLocalStale, ConsistencyLinearizable} {
				t.Run(consistencyPolicyName(policy), func(t *testing.T) {
					sections := append([]iwire.Section(nil), tt.sections...)
					sections = append(sections, consistencyPolicySection(policy))
					_, err := client.commandSections(ctx, tt.commandID, sections...)
					if !isRemoteError(err, iwire.ErrUnsupportedFeature) {
						t.Fatalf("%s policy=%s err=%v want unsupported_feature", tt.name, consistencyPolicyName(policy), err)
					}
				})
			}
		})
	}
}

func TestReadConsistencyPolicyRejectsMalformedOrUnknown(t *testing.T) {
	tests := []struct {
		name     string
		payload  []byte
		wantCode iwire.ErrorCode
	}{
		{name: "trailing-bytes", payload: []byte{byte(ConsistencyLocalStale), 0}, wantCode: iwire.ErrMalformedFrame},
		{name: "unknown", payload: []byte{99}, wantCode: iwire.ErrInvalidCommand},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mgr, _ := serveCollectionPipe(t)
			seedReadCollection(t, mgr)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatalf("Hello: %v", err)
			}

			_, err := client.commandSections(ctx, iwire.CommandIndexLookup,
				collectionNameRef("users"),
				iwire.Section{ID: iwire.SectionIndexName, Bytes: encodeIndexName("email")},
				iwire.Section{ID: iwire.SectionIndexValue, Bytes: mustEncodeScalar(t, "ada@example.com")},
				iwire.Section{ID: iwire.SectionConsistencyPolicy, Bytes: tt.payload},
			)
			if !isRemoteError(err, tt.wantCode) {
				t.Fatalf("IndexLookup err=%v want code %d", err, tt.wantCode)
			}
		})
	}
}

func TestReadCoordinatorProvesStrongConsistencyMetadata(t *testing.T) {
	coord := &fakeClusterReadCoordinator{
		result: ClusterReadResult{
			ActualConsistency: ConsistencyLinearizable,
			ServingNode:       "node-a",
			LeaderNode:        "node-a",
			AppliedIndex:      42,
			HasAppliedIndex:   true,
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterReadCoordinator: coord})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	result, err := client.GetManyWithOptions(ctx, "users", [][]byte{[]byte("u1")}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if err != nil {
		t.Fatalf("GetManyWithOptions: %v", err)
	}
	if len(coord.calls) != 1 {
		t.Fatalf("coordinator calls=%d want 1", len(coord.calls))
	}
	call := coord.calls[0]
	if call.Policy != ConsistencyLinearizable || call.CommandID != iwire.CommandGetMany || call.CommandName != "get_many" {
		t.Fatalf("coordinator call=%+v", call)
	}
	if got, want := result.Present, []bool{true}; !boolSlicesEqual(got, want) {
		t.Fatalf("present=%v want %v", got, want)
	}
	if !bytes.Contains(result.Docs[0], []byte(`"Ada"`)) {
		t.Fatalf("doc=%q want Ada", result.Docs[0])
	}
	if !result.ReadMeta.Valid ||
		result.ReadMeta.ActualConsistency != ConsistencyLinearizable ||
		result.ReadMeta.ServingNode != "node-a" ||
		result.ReadMeta.LeaderNode != "node-a" ||
		!result.ReadMeta.HasAppliedIndex ||
		result.ReadMeta.AppliedIndex != 42 {
		t.Fatalf("read meta=%+v", result.ReadMeta)
	}
}

func TestReadCoordinatorActualConsistencyMustSatisfyRequest(t *testing.T) {
	coord := &fakeClusterReadCoordinator{
		result: ClusterReadResult{ActualConsistency: ConsistencyLocalStale},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterReadCoordinator: coord})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err := client.IndexLookupWithOptions(ctx, "users", "email", "ada@example.com", CursorLimits{}, ReadOptions{ConsistencyPolicy: ConsistencyLeaderRead})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("IndexLookupWithOptions err=%v want consistency_unavailable", err)
	}
	if len(coord.calls) != 1 || coord.calls[0].Policy != ConsistencyLeaderRead {
		t.Fatalf("coordinator calls=%+v", coord.calls)
	}
}

func TestReadMetadataSectionListResponsesRespectSectionLimit(t *testing.T) {
	coord := &fakeClusterReadCoordinator{
		result: ClusterReadResult{
			ActualConsistency: ConsistencyLinearizable,
			ServingNode:       strings.Repeat("s", 512),
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: coord,
		Limits:                 iwire.Limits{MaxSectionLen: 256},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err := client.IndexLookupWithOptions(ctx, "users", "email", "ada@example.com", CursorLimits{}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("IndexLookupWithOptions err=%v want resource_exhausted", err)
	}
}

func TestReadMetadataSectionListResponsesRespectFrameLimit(t *testing.T) {
	coord := &fakeClusterReadCoordinator{
		result: ClusterReadResult{
			ActualConsistency: ConsistencyLinearizable,
			ServingNode:       strings.Repeat("s", 384),
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: coord,
		Limits:                 iwire.Limits{MaxFrameSize: 320},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err := client.IndexLookupWithOptions(ctx, "users", "email", "ada@example.com", CursorLimits{}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("IndexLookupWithOptions err=%v want resource_exhausted", err)
	}
}

func TestCursorNextCannotStrengthenOpenScanConsistency(t *testing.T) {
	coord := &fakeClusterReadCoordinator{
		result: ClusterReadResult{ActualConsistency: ConsistencyLinearizable},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterReadCoordinator: coord})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if first.Cursor.CursorID == 0 || !first.Cursor.HasMore {
		t.Fatalf("first cursor=%+v", first.Cursor)
	}
	if !first.ReadMeta.Valid || first.ReadMeta.ActualConsistency != ConsistencyLocalStale {
		t.Fatalf("open scan read meta=%+v want local-stale", first.ReadMeta)
	}
	if len(coord.calls) != 0 {
		t.Fatalf("coordinator calls after local scan=%+v want none", coord.calls)
	}

	_, err = client.CursorNextWithOptions(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("strong CursorNext err=%v want consistency_unavailable", err)
	}
	if len(coord.calls) != 0 {
		t.Fatalf("coordinator calls after rejected cursor next=%+v want none", coord.calls)
	}

	second, err := client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("CursorNext after rejected strong request: %v", err)
	}
	if !second.ReadMeta.Valid || second.ReadMeta.ActualConsistency != ConsistencyLocalStale {
		t.Fatalf("cursor next read meta=%+v want local-stale", second.ReadMeta)
	}
	assertDocumentsResult(t, second, []string{"u2"}, []string{`{"email":"grace@example.com","city":"hnl","name":"Grace"}`})
}

func TestCursorNextUsesOpenScanReadMetadata(t *testing.T) {
	coord := &fakeClusterReadCoordinator{
		result: ClusterReadResult{
			ActualConsistency: ConsistencyLinearizable,
			ServingNode:       "node-a",
			LeaderNode:        "node-a",
			AppliedIndex:      99,
			HasAppliedIndex:   true,
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{ClusterReadCoordinator: coord})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScanWithOptions(ctx, "users", CursorLimits{MaxItems: 1}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if err != nil {
		t.Fatalf("OpenScanWithOptions: %v", err)
	}
	if first.Cursor.CursorID == 0 || !first.Cursor.HasMore {
		t.Fatalf("first cursor=%+v", first.Cursor)
	}
	if len(coord.calls) != 1 || coord.calls[0].CommandID != iwire.CommandOpenScan {
		t.Fatalf("coordinator calls after open scan=%+v", coord.calls)
	}
	if !first.ReadMeta.Valid ||
		first.ReadMeta.ActualConsistency != ConsistencyLinearizable ||
		first.ReadMeta.ServingNode != "node-a" ||
		first.ReadMeta.LeaderNode != "node-a" ||
		!first.ReadMeta.HasAppliedIndex ||
		first.ReadMeta.AppliedIndex != 99 {
		t.Fatalf("open scan read meta=%+v", first.ReadMeta)
	}

	second, err := client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("CursorNext: %v", err)
	}
	if len(coord.calls) != 1 {
		t.Fatalf("coordinator calls after cursor next=%+v want no additional call", coord.calls)
	}
	if !second.ReadMeta.Valid ||
		second.ReadMeta.ActualConsistency != ConsistencyLinearizable ||
		second.ReadMeta.ServingNode != "node-a" ||
		second.ReadMeta.LeaderNode != "node-a" ||
		!second.ReadMeta.HasAppliedIndex ||
		second.ReadMeta.AppliedIndex != 99 {
		t.Fatalf("cursor next read meta=%+v", second.ReadMeta)
	}
	assertDocumentsResult(t, second, []string{"u2"}, []string{`{"email":"grace@example.com","city":"hnl","name":"Grace"}`})
}

func TestIndexLookupWithoutLimitsReturnsAllMatches(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexLookup(ctx, "users", "city", "hnl", CursorLimits{})
	if err != nil {
		t.Fatalf("IndexLookup: %v", err)
	}
	if truncated || len(ids) != 2 {
		t.Fatalf("ids=%q truncated=%v want two untruncated matches", ids, truncated)
	}
}

func TestIndexLookupByteOnlyLimitTruncatesIDs(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexLookup(ctx, "users", "city", "hnl", CursorLimits{MaxBytes: 2})
	if err != nil {
		t.Fatalf("IndexLookup: %v", err)
	}
	if !truncated || len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("ids=%q truncated=%v want first ID with truncation", ids, truncated)
	}
}

func TestIndexLookupByteLimitCanReturnEmptyBatch(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexLookup(ctx, "users", "city", "hnl", CursorLimits{MaxBytes: 1})
	if err != nil {
		t.Fatalf("IndexLookup: %v", err)
	}
	if !truncated || len(ids) != 0 {
		t.Fatalf("ids=%q truncated=%v want empty truncated batch", ids, truncated)
	}
}

func TestIndexLookupDefaultResultBoundUsesWireLimit(t *testing.T) {
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		Limits: iwire.Limits{MaxByteVectorItems: 1},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexLookup(ctx, "users", "city", "hnl", CursorLimits{})
	if err != nil {
		t.Fatalf("IndexLookup: %v", err)
	}
	if !truncated || len(ids) != 1 {
		t.Fatalf("ids=%q truncated=%v want one bounded match with truncation", ids, truncated)
	}
}

func TestIndexRangeDefaultResultBoundUsesWireLimit(t *testing.T) {
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		Limits: iwire.Limits{MaxByteVectorItems: 1},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexRange(ctx, "users", "city", IndexRange{
		LowerUnbounded: true,
		UpperUnbounded: true,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("IndexRange: %v", err)
	}
	if !truncated || len(ids) != 1 {
		t.Fatalf("ids=%q truncated=%v want one bounded range result with truncation", ids, truncated)
	}
}

func TestIndexRangeByteOnlyLimitTruncatesIDs(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexRange(ctx, "users", "city", IndexRange{
		LowerUnbounded: true,
		UpperUnbounded: true,
		MaxBytes:       2,
	})
	if err != nil {
		t.Fatalf("IndexRange: %v", err)
	}
	if !truncated || len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("ids=%q truncated=%v want first ID with truncation", ids, truncated)
	}
}

func TestGetManyResponseRespectsFrameLimit(t *testing.T) {
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{Limits: iwire.Limits{MaxFrameSize: 256}})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{bytes.Repeat([]byte("x"), 512)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := client.GetMany(ctx, "users", [][]byte{[]byte("u1")}); !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("GetMany err=%v want resource exhausted", err)
	}
}

func TestGetManyResponseRespectsByteVectorLimit(t *testing.T) {
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{Limits: iwire.Limits{MaxByteVectorBytes: 64}})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{bytes.Repeat([]byte("x"), 128)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := client.GetMany(ctx, "users", [][]byte{[]byte("u1")}); !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("GetMany err=%v want resource exhausted", err)
	}
}

func TestGetManyResponseByteVectorLimitIgnoresLengthTableOverhead(t *testing.T) {
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{Limits: iwire.Limits{MaxByteVectorBytes: 64}})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	ids := make([][]byte, 40)
	docs := make([][]byte, 40)
	for i := range ids {
		ids[i] = []byte{byte(i + 1)}
		docs[i] = []byte("x")
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	got, present, err := client.GetMany(ctx, "users", ids)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != len(ids) || len(present) != len(ids) {
		t.Fatalf("GetMany lengths docs=%d present=%d want %d", len(got), len(present), len(ids))
	}
	for i := range ids {
		if !present[i] || !bytes.Equal(got[i], docs[i]) {
			t.Fatalf("GetMany[%d]=%q present=%v want %q present=true", i, got[i], present[i], docs[i])
		}
	}
}

func TestDecodeReadResultsRejectsTrailingTruncatedBytes(t *testing.T) {
	sections := []iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		{ID: iwire.SectionTruncated, Bytes: []byte{1, 0}},
	}
	if _, _, err := decodeIDsAndTruncated(sections, iwire.DefaultLimits()); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeIDsAndTruncated err=%v code=%d want malformed", err, nativeCodeOf(err))
	}
	sections = []iwire.Section{
		{ID: iwire.SectionCursorMeta, Bytes: encodeCursorMeta(CursorMeta{})},
		{ID: iwire.SectionTruncated, Bytes: []byte{1, 0}},
	}
	if _, err := decodeDocumentsResult(sections, iwire.DefaultLimits()); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeDocumentsResult err=%v code=%d want malformed", err, nativeCodeOf(err))
	}
}

func TestIndexRangeByteOnlyLimitDoesNotBecomeQueryLimit(t *testing.T) {
	server := NewServer(ServerOptions{})
	_, opts, limits, err := server.indexRangeRequest([]iwire.Section{
		{ID: iwire.SectionIndexName, Bytes: encodeIndexName("city")},
		{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(CursorLimits{MaxBytes: 1 << 20})},
	})
	if err != nil {
		t.Fatalf("indexRangeRequest: %v", err)
	}
	if opts.Limit != 0 {
		t.Fatalf("opts.Limit=%d want 0 for byte-only limits", opts.Limit)
	}
	if limits.MaxBytes != 1<<20 {
		t.Fatalf("limits.MaxBytes=%d want byte limit preserved", limits.MaxBytes)
	}

	_, opts, _, err = server.indexRangeRequest([]iwire.Section{
		{ID: iwire.SectionIndexName, Bytes: encodeIndexName("city")},
		{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(CursorLimits{MaxItems: 7, MaxBytes: 1 << 20})},
	})
	if err != nil {
		t.Fatalf("indexRangeRequest with item limit: %v", err)
	}
	if opts.Limit != 7 {
		t.Fatalf("opts.Limit=%d want explicit item limit", opts.Limit)
	}
}

func TestIndexRangeOmittedBoundsAreUnbounded(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexRange(ctx, "users", "city", IndexRange{
		LowerUnbounded: true,
		UpperUnbounded: true,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("IndexRange: %v", err)
	}
	if truncated || len(ids) != 2 {
		t.Fatalf("ids=%q truncated=%v want full unbounded range", ids, truncated)
	}
}

func TestCursorOwnerIsolation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db})
	t.Cleanup(func() {
		_ = server.Close()
		_ = db.Close()
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	clientA, cleanupA, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatalf("NewInProcessClient A: %v", err)
	}
	defer func() { _ = cleanupA() }()
	clientB, cleanupB, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatalf("NewInProcessClient B: %v", err)
	}
	defer func() { _ = cleanupB() }()

	first, err := clientA.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if first.Cursor.CursorID == 0 {
		t.Fatalf("first=%+v want cursor", first)
	}
	if _, err := clientB.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1}); !isRemoteError(err, iwire.ErrCursorNotFound) {
		t.Fatalf("CursorNext by other connection err=%v want cursor_not_found", err)
	}
}

func TestCursorCleanupOnConnectionClose(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db})
	t.Cleanup(func() {
		_ = server.Close()
		_ = db.Close()
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if first.Cursor.CursorID == 0 || server.openCursorCount() != 1 {
		t.Fatalf("cursor id=%d count=%d want one open cursor", first.Cursor.CursorID, server.openCursorCount())
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if got := server.openCursorCount(); got != 0 {
		t.Fatalf("openCursorCount=%d want 0 after connection close", got)
	}
}

func TestCursorIdleTimeoutReap(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, CursorIdleTimeout: 20 * time.Millisecond})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if first.Cursor.CursorID == 0 || server.openCursorCount() != 1 {
		t.Fatalf("cursor id=%d count=%d want one open cursor", first.Cursor.CursorID, server.openCursorCount())
	}
	server.cursorMu.Lock()
	if cursor := server.cursors[first.Cursor.CursorID]; cursor != nil {
		cursor.lastUsed = time.Now().Add(-server.cursorIdleTimeout - time.Second)
	}
	server.cursorMu.Unlock()
	server.reapExpiredCursors()
	if got := server.openCursorCount(); got != 0 {
		t.Fatalf("openCursorCount=%d want 0 after idle timeout reap", got)
	}
}

func TestDecodeDocumentsResultRejectsMismatchedVectors(t *testing.T) {
	_, err := decodeDocumentsResult([]iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("a"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte("{}"), []byte("{}"))},
	}, iwire.DefaultLimits())
	if nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeDocumentsResult err=%v code=%d want malformed frame", err, nativeCodeOf(err))
	}
}

func TestDecodeIDsAndTruncatedRejectsTrailingTruncatedBytes(t *testing.T) {
	_, _, err := decodeIDsAndTruncated([]iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("a"))},
		{ID: iwire.SectionTruncated, Bytes: []byte{1, 0}},
	}, iwire.DefaultLimits())
	if nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeIDsAndTruncated err=%v code=%d want malformed frame", err, nativeCodeOf(err))
	}
}

func TestDecodeDocumentsResultRejectsTrailingTruncatedBytes(t *testing.T) {
	_, err := decodeDocumentsResult([]iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("a"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte("{}"))},
		{ID: iwire.SectionTruncated, Bytes: []byte{1, 0}},
	}, iwire.DefaultLimits())
	if nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeDocumentsResult err=%v code=%d want malformed frame", err, nativeCodeOf(err))
	}
}

func TestOpenScanReportsTruncatedRetainedWindow(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, MaxScanDocuments: 1})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 10})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if len(first.IDs) != 1 || first.Cursor.CursorID != 0 || first.Cursor.HasMore || !first.Truncated {
		t.Fatalf("first=%+v want one truncated terminal batch", first)
	}
}

func TestOpenScanReadMetadataCountsTowardSectionLimit(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{
		Collections:      mgr,
		Backend:          db,
		MaxScanDocuments: 1,
		Limits:           iwire.Limits{MaxSections: 4},
	})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err = client.OpenScan(ctx, "users", CursorLimits{MaxItems: 10})
	if !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("OpenScan err=%v want resource exhausted", err)
	}
}

func TestOpenScanReadMetadataAllowsNonTruncatedSectionLimit(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{
		Collections: mgr,
		Backend:     db,
		Limits:      iwire.Limits{MaxSections: 4},
	})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	result, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 10})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if result.Truncated || result.Cursor.HasMore {
		t.Fatalf("result=%+v want terminal non-truncated scan", result)
	}
	if !result.ReadMeta.Valid || result.ReadMeta.ActualConsistency != ConsistencyLocalStale {
		t.Fatalf("read meta=%+v want local-stale", result.ReadMeta)
	}
	assertDocumentsResult(t, result,
		[]string{"u1", "u2"},
		[]string{
			`{"email":"ada@example.com","city":"hnl","name":"Ada"}`,
			`{"email":"grace@example.com","city":"hnl","name":"Grace"}`,
		},
	)
}

func TestGetManyReadMetadataCountsTowardSectionLimit(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{
		Collections: mgr,
		Backend:     db,
		Limits:      iwire.Limits{MaxSections: 2},
	})
	state := &connState{}
	seedReadCollection(t, mgr)
	t.Cleanup(func() { _ = db.Close() })

	_, err = server.handleGetManyBody(state, []iwire.Section{
		collectionNameRef("users"),
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
	}, nil, ReadMetadata{Valid: true, ActualConsistency: ConsistencyLocalStale})
	if code, ok := iwire.ErrorCodeOf(err); !ok || code != iwire.ErrResourceExhausted {
		t.Fatalf("handleGetManyBody err=%v want resource exhausted", err)
	}
}

func TestOpenScanReportsTruncatedWhenCursorRetentionExceeded(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, MaxCursorRetainedBytes: 1})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if len(first.IDs) != 1 || first.Cursor.CursorID != 0 || first.Cursor.HasMore || !first.Truncated {
		t.Fatalf("first=%+v want one truncated terminal batch", first)
	}
}

func TestOpenScanDefaultBatchHonorsFrameLimit(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name:    "users",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := []byte(`{"payload":"` + strings.Repeat("x", 512) + `"}`)
	if _, err := col.InsertBatch([][]byte{[]byte("u1"), []byte("u2")}, [][]byte{doc, doc}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	server := NewServer(ServerOptions{
		Collections:            mgr,
		Backend:                db,
		DefaultCursorBatchSize: 10,
		Limits:                 iwire.Limits{MaxFrameSize: 700},
	})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScan(ctx, "users", CursorLimits{})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if len(first.IDs) != 1 || first.Cursor.CursorID == 0 || !first.Cursor.HasMore {
		t.Fatalf("first=%+v want one frame-limited cursor batch", first)
	}
	if err := client.CursorClose(ctx, first.Cursor.CursorID); err != nil {
		t.Fatalf("CursorClose cleanup: %v", err)
	}
}

func TestOpenScanCursorLifecycle(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if len(first.IDs) != 1 || first.Cursor.CursorID == 0 || !first.Cursor.HasMore {
		t.Fatalf("first=%+v", first)
	}
	assertDocumentsResult(t, first, []string{"u1"}, []string{`{"email":"ada@example.com","city":"hnl","name":"Ada"}`})
	second, err := client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 10})
	if err != nil {
		t.Fatalf("CursorNext: %v", err)
	}
	if len(second.IDs) != 1 || second.Cursor.HasMore {
		t.Fatalf("second=%+v", second)
	}
	assertDocumentsResult(t, second, []string{"u2"}, []string{`{"email":"grace@example.com","city":"hnl","name":"Grace"}`})
	_, err = client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1})
	if !isRemoteError(err, iwire.ErrCursorNotFound) {
		t.Fatalf("CursorNext exhausted error=%v want cursor_not_found", err)
	}
}

func TestCursorIdleReaperRunsWithoutFollowupRequest(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	seedReadCollection(t, mgr)
	server := NewServer(ServerOptions{
		Collections:       mgr,
		Backend:           db,
		CursorIdleTimeout: 20 * time.Millisecond,
	})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if first.Cursor.CursorID == 0 {
		t.Fatalf("first=%+v want open cursor", first)
	}
	waitForOpenCursorCount(t, server, 0)
	_, err = client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1})
	if !isRemoteError(err, iwire.ErrCursorNotFound) {
		t.Fatalf("CursorNext after idle reap error=%v want cursor_not_found", err)
	}
	waitForCounter(t, server, "cursors.closed_total", 1)
	waitForCounter(t, server, "cursors.timeouts_total", 1)
}

func TestCursorConnectionCloseIncrementsClosedCounter(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	seedReadCollection(t, mgr)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if first.Cursor.CursorID == 0 {
		t.Fatalf("first=%+v want open cursor", first)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	waitForOpenCursorCount(t, server, 0)
	waitForCounter(t, server, "cursors.closed_total", 1)
}

func TestInProcessClientCursorIdleReaperRunsWithoutFollowupRequest(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	seedReadCollection(t, mgr)
	server := NewServer(ServerOptions{
		Collections:       mgr,
		Backend:           db,
		CursorIdleTimeout: 20 * time.Millisecond,
	})
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	defer cleanup()

	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if first.Cursor.CursorID == 0 {
		t.Fatalf("first=%+v want open cursor", first)
	}
	waitForOpenCursorCount(t, server, 0)
	_, err = client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1})
	if !isRemoteError(err, iwire.ErrCursorNotFound) {
		t.Fatalf("CursorNext after idle reap error=%v want cursor_not_found", err)
	}
}

func TestCursorNextRequiresCursorRef(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	_, err = client.commandSections(ctx, iwire.CommandCursorNext, iwire.Section{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(CursorLimits{MaxItems: 1})})
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("CursorNext without cursor_ref err=%v want invalid command", err)
	}
	_, err = client.commandSectionsOnStream(ctx, first.Cursor.CursorID+1, iwire.CommandCursorNext,
		iwire.Section{ID: iwire.SectionCursorRef, Bytes: encodeCursorRef(first.Cursor.CursorID)},
		iwire.Section{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(CursorLimits{MaxItems: 1})},
	)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("CursorNext stream/cursor_ref mismatch err=%v want invalid command", err)
	}
	if err := client.CursorClose(ctx, first.Cursor.CursorID); err != nil {
		t.Fatalf("CursorClose cleanup: %v", err)
	}
}

func TestCursorClose(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if err := client.CursorClose(ctx, first.Cursor.CursorID); err != nil {
		t.Fatalf("CursorClose: %v", err)
	}
	_, err = client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1})
	if !isRemoteError(err, iwire.ErrCursorNotFound) {
		t.Fatalf("CursorNext after close error=%v want cursor_not_found", err)
	}
}

func TestOpenScanEnforcesMaxOpenCursors(t *testing.T) {
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{MaxOpenCursors: 1})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan first: %v", err)
	}
	if first.Cursor.CursorID == 0 {
		t.Fatalf("first=%+v want cursor", first)
	}
	if _, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1}); !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("OpenScan second err=%v want resource exhausted", err)
	}
	if err := client.CursorClose(ctx, first.Cursor.CursorID); err != nil {
		t.Fatalf("CursorClose cleanup: %v", err)
	}
}

func TestCursorCloseRequiresStreamID(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if err := client.CursorClose(ctx, 0); !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("CursorClose zero err=%v want invalid command", err)
	}
}

type fakeClusterReadCoordinator struct {
	result ClusterReadResult
	err    error
	calls  []ClusterReadRequest
}

func (f *fakeClusterReadCoordinator) CoordinateRead(ctx context.Context, request ClusterReadRequest) (ClusterReadResult, error) {
	f.calls = append(f.calls, request)
	return f.result, f.err
}

func mustEncodeScalar(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := encodeScalar(value)
	if err != nil {
		t.Fatalf("encodeScalar(%v): %v", value, err)
	}
	return raw
}

func boolSlicesEqual(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func byteMatrixEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func assertDocumentsResult(t *testing.T, result DocumentsResult, wantIDs []string, wantDocs []string) {
	t.Helper()
	if len(result.IDs) != len(wantIDs) || len(result.Docs) != len(wantDocs) {
		t.Fatalf("result lens ids=%d docs=%d want ids=%d docs=%d result=%+v", len(result.IDs), len(result.Docs), len(wantIDs), len(wantDocs), result)
	}
	for i := range wantIDs {
		if string(result.IDs[i]) != wantIDs[i] {
			t.Fatalf("id[%d]=%q want %q", i, result.IDs[i], wantIDs[i])
		}
		if string(result.Docs[i]) != wantDocs[i] {
			t.Fatalf("doc[%d]=%q want %q", i, result.Docs[i], wantDocs[i])
		}
	}
}

func waitForOpenCursorCount(t *testing.T, server *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := server.openCursorCount(); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("openCursorCount=%d want %d", server.openCursorCount(), want)
}

func waitForCounter(t *testing.T, server *Server, key string, want int) {
	t.Helper()
	fullKey := nativeStatsPrefix + key
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := server.Stats()
		got, err := strconv.Atoi(stats[fullKey])
		if err == nil && got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	stats := server.Stats()
	t.Fatalf("%s=%q want %d", fullKey, stats[fullKey], want)
}
