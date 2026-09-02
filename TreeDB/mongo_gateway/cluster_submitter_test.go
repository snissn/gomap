package mongogateway

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

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	treenativewire "github.com/snissn/gomap/TreeDB/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoClusterSubmitterCall struct {
	entry    raftentry.CommandEntryV1
	metadata treenativewire.ClusterRequestMetadata
	ctxErr   error
}

type mongoClusterFakeSubmitter struct {
	mu                       sync.Mutex
	calls                    []mongoClusterSubmitterCall
	actualAck                iwire.AckPolicy
	committedRecoverable     bool
	committedApplied         bool
	responseSections         []iwire.Section
	overrideResponseSections bool
	status                   treenativewire.ClusterAdmissionStatus
	admissionErr             error
	submitHook               func()
	submitErr                error
}

type mongoConfirmingVectorPartitionSubmitterV1 struct {
	*mongoClusterFakeSubmitter
	mu                        sync.Mutex
	confirmations             []iwire.CommandID
	confirmationContextErrors []error
}

func (s *mongoConfirmingVectorPartitionSubmitterV1) RequiresVectorPartitionMutationAdmissionV1(context.Context) (bool, error) {
	return true, nil
}

func (s *mongoConfirmingVectorPartitionSubmitterV1) SubmitCommandEntryWithPreCommitV1(ctx context.Context, entry []byte, metadata treenativewire.ClusterRequestMetadata, preCommit func(context.Context) error) (treenativewire.ClusterSubmitResult, error) {
	if err := preCommit(ctx); err != nil {
		return treenativewire.ClusterSubmitResult{}, err
	}
	return s.SubmitCommandEntryV1(ctx, entry, metadata)
}

func (s *mongoConfirmingVectorPartitionSubmitterV1) AdmitVectorPartitionMutationV1(context.Context, iwire.CommandID, []iwire.Section) error {
	return nil
}

func (s *mongoConfirmingVectorPartitionSubmitterV1) ConfirmVectorPartitionMutationV1(ctx context.Context, command iwire.CommandID, _ []iwire.Section) error {
	s.mu.Lock()
	s.confirmations = append(s.confirmations, command)
	s.confirmationContextErrors = append(s.confirmationContextErrors, ctx.Err())
	s.mu.Unlock()
	return nil
}

type mongoClusterAdmissionSubmitter struct {
	*mongoClusterFakeSubmitter
	status treenativewire.ClusterAdmissionStatus
	err    error
}

type mongoRecordingRaftCommandSubmitter struct {
	groupID  raftcluster.GroupID
	features raftcluster.FeatureSet
	status   raftcluster.AdmissionStatus
	err      error
	discard  bool
	mu       sync.Mutex
	calls    []mongoRecordingRaftCommandSubmitCall
}

type mongoRecordingRaftCommandSubmitCall struct {
	entry    raftentry.CommandEntryV1
	metadata raftentry.RequestMetadataV1
}

func (s *mongoRecordingRaftCommandSubmitter) Config() raftcluster.ResolvedConfig {
	return raftcluster.ResolvedConfig{GroupID: s.groupID, Features: s.features}
}

func (s *mongoRecordingRaftCommandSubmitter) ClusterAdmissionStatus(context.Context) (raftcluster.AdmissionStatus, error) {
	if s.status == (raftcluster.AdmissionStatus{}) {
		return raftcluster.LeaderAdmission(), nil
	}
	return s.status, nil
}

func (s *mongoRecordingRaftCommandSubmitter) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1) (raftcluster.SubmitResultV1, error) {
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
	if !s.discard {
		s.calls = append(s.calls, mongoRecordingRaftCommandSubmitCall{entry: decoded, metadata: cloneMongoClusterRequestMetadata(metadata)})
	}
	s.mu.Unlock()
	if s.err != nil {
		return raftcluster.SubmitResultV1{}, s.err
	}
	actualAck := metadata.AckPolicy
	if actualAck == 0 {
		actualAck = iwire.AckVisible
	}
	ids := mongoClusterTestIDs(decoded.Decoded.Sections)
	affected := int64(len(ids))
	if affected == 0 {
		affected = 1
	}
	expectedTarget := decoded.Target.Clone()
	return raftcluster.SubmitResultV1{
		ActualAck:            actualAck,
		CommittedRecoverable: actualAck == iwire.AckRaftCommitted,
		DecodedEntry:         decoded,
		ApplyResult: raftentry.ApplyResultV1{
			Status:        raftentry.ApplyStatusApplied,
			AffectedCount: affected,
			MatchedCount:  affected,
		},
		CommittedEntry: raftcluster.CommittedCommandEntryV1{
			Term:                     1,
			Index:                    index,
			Bytes:                    append([]byte(nil), entry...),
			CurrentCatalogVersion:    60,
			HasCurrentCatalogVersion: true,
			SyncLocalCommandWAL:      true,
			RequestMetadata:          cloneMongoClusterRequestMetadata(metadata),
			ExpectedTarget:           &expectedTarget,
		},
		Evidence: raftcluster.CommitEvidenceV1{
			Kind:                raftcluster.CommitEvidenceProductionConsensusV1,
			GroupID:             s.groupID,
			Term:                1,
			Index:               index,
			Committed:           true,
			ProductionConsensus: true,
		},
		CatalogVersion:    60,
		HasCatalogVersion: true,
	}, nil
}

func (s *mongoRecordingRaftCommandSubmitter) snapshotCalls() []mongoRecordingRaftCommandSubmitCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]mongoRecordingRaftCommandSubmitCall, len(s.calls))
	copy(out, s.calls)
	return out
}

type mongoStaticClusterRouteProvider struct {
	mu     sync.Mutex
	target treenativewire.ClusterRouteTarget
	err    error
	routes []treenativewire.ClusterRouteRequest
}

type mongoSensitiveRouteError struct {
	message string
	route   treenativewire.ClusterRouteErrorMetadata
}

func (e mongoSensitiveRouteError) Error() string {
	return e.message
}

func (e mongoSensitiveRouteError) ClusterRouteErrorMetadata() treenativewire.ClusterRouteErrorMetadata {
	return e.route
}

func (p *mongoStaticClusterRouteProvider) ClusterRoute(ctx context.Context, req treenativewire.ClusterRouteRequest) (treenativewire.ClusterRouteTarget, error) {
	p.mu.Lock()
	p.routes = append(p.routes, req)
	p.mu.Unlock()
	if p.err != nil {
		return treenativewire.ClusterRouteTarget{}, p.err
	}
	target := p.target
	target.Members = append([]string(nil), target.Members...)
	return target, nil
}

func (p *mongoStaticClusterRouteProvider) snapshotRoutes() []treenativewire.ClusterRouteRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]treenativewire.ClusterRouteRequest(nil), p.routes...)
}

type mongoRemoteOwnerRouteProvider struct {
	mu     sync.Mutex
	err    error
	routes []treenativewire.ClusterRouteRequest
}

func (p *mongoRemoteOwnerRouteProvider) ClusterRoute(ctx context.Context, req treenativewire.ClusterRouteRequest) (treenativewire.ClusterRouteTarget, error) {
	p.mu.Lock()
	p.routes = append(p.routes, req)
	p.mu.Unlock()
	if p.err != nil {
		return treenativewire.ClusterRouteTarget{}, p.err
	}
	return treenativewire.ClusterRouteTarget{
		GroupID:       "group-z",
		Members:       []string{"node-z", "node-y"},
		LeaderHint:    "node-z",
		PlacementMode: string(raftplacement.PlacementModeRingV1),
		RouteKey:      string(raftplacement.RouteKeyDocumentIDV1),
		Shape:         treenativewire.ClusterRouteShapeToken,
		TokenKnown:    req.TokenKnown,
		Token:         req.Token,
		PartitionID:   "p9",
	}, nil
}

func (p *mongoRemoteOwnerRouteProvider) snapshotRoutes() []treenativewire.ClusterRouteRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]treenativewire.ClusterRouteRequest(nil), p.routes...)
}

type mongoRoutingClusterSubmitter struct {
	*mongoClusterFakeSubmitter

	routeMu sync.Mutex
	target  treenativewire.ClusterRouteTarget
	err     error
	routes  []treenativewire.ClusterRouteRequest
}

func (r *mongoRoutingClusterSubmitter) ClusterRoute(ctx context.Context, req treenativewire.ClusterRouteRequest) (treenativewire.ClusterRouteTarget, error) {
	r.routeMu.Lock()
	r.routes = append(r.routes, req)
	r.routeMu.Unlock()
	if r.err != nil {
		return treenativewire.ClusterRouteTarget{}, r.err
	}
	target := r.target
	target.Members = append([]string(nil), target.Members...)
	return target, nil
}

func (r *mongoRoutingClusterSubmitter) snapshotRoutes() []treenativewire.ClusterRouteRequest {
	r.routeMu.Lock()
	defer r.routeMu.Unlock()
	return append([]treenativewire.ClusterRouteRequest(nil), r.routes...)
}

type mongoPlacementRouteClusterSubmitter struct {
	*mongoClusterFakeSubmitter

	routeMu  sync.Mutex
	provider treenativewire.CatalogRouteResolverV1
	routes   []treenativewire.ClusterRouteRequest
}

type mongoStaticCatalogRouteProviderForTest struct {
	resolver treenativewire.CatalogRouteResolverV1
}

func newMongoStaticCatalogRouteProviderForTest(catalog raftplacement.ResolvedCatalogV1) mongoStaticCatalogRouteProviderForTest {
	return mongoStaticCatalogRouteProviderForTest{
		resolver: treenativewire.NewCatalogRouteResolverV1(catalog),
	}
}

func (p mongoStaticCatalogRouteProviderForTest) ClusterRoute(ctx context.Context, req treenativewire.ClusterRouteRequest) (treenativewire.ClusterRouteTarget, error) {
	return p.resolver.ResolveCatalogRouteV1(ctx, req)
}

// mongoTestCatalogRouteValidator is deliberately confined to this test file.
// Production routed submitters must validate against CatalogMetaAuthorityV1.
type mongoTestCatalogRouteValidator struct{}

func (mongoTestCatalogRouteValidator) ValidateCatalogRouteMetadata(context.Context, raftentry.RequestMetadataV1) error {
	return nil
}

func (p *mongoPlacementRouteClusterSubmitter) ClusterRoute(ctx context.Context, req treenativewire.ClusterRouteRequest) (treenativewire.ClusterRouteTarget, error) {
	p.routeMu.Lock()
	p.routes = append(p.routes, req)
	p.routeMu.Unlock()
	return p.provider.ResolveCatalogRouteV1(ctx, req)
}

func (p *mongoPlacementRouteClusterSubmitter) snapshotRoutes() []treenativewire.ClusterRouteRequest {
	p.routeMu.Lock()
	defer p.routeMu.Unlock()
	return append([]treenativewire.ClusterRouteRequest(nil), p.routes...)
}

func (f *mongoClusterAdmissionSubmitter) ClusterAdmissionStatus(context.Context) (treenativewire.ClusterAdmissionStatus, error) {
	if f.err != nil {
		return treenativewire.ClusterAdmissionStatus{}, f.err
	}
	return f.status, nil
}

func (f *mongoClusterFakeSubmitter) ClusterAdmissionStatus(context.Context) (treenativewire.ClusterAdmissionStatus, error) {
	if f.admissionErr != nil {
		return treenativewire.ClusterAdmissionStatus{}, f.admissionErr
	}
	if f.status == (treenativewire.ClusterAdmissionStatus{}) {
		return treenativewire.ClusterLeaderAdmission(), nil
	}
	return f.status, nil
}

func (f *mongoClusterFakeSubmitter) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata treenativewire.ClusterRequestMetadata) (treenativewire.ClusterSubmitResult, error) {
	if ctx == nil {
		return treenativewire.ClusterSubmitResult{}, fmt.Errorf("nil submit context")
	}
	decoded, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{RequestMetadata: metadata})
	if err != nil {
		return treenativewire.ClusterSubmitResult{}, err
	}
	ctxErr := ctx.Err()
	f.mu.Lock()
	f.calls = append(f.calls, mongoClusterSubmitterCall{entry: decoded, metadata: metadata, ctxErr: ctxErr})
	f.mu.Unlock()
	if ctxErr != nil {
		return treenativewire.ClusterSubmitResult{}, ctxErr
	}
	if f.submitHook != nil {
		f.submitHook()
	}
	actualAck := f.actualAck
	if actualAck == 0 {
		actualAck = iwire.AckVisible
	}
	responseSections := mongoClusterFakeResponseSections(decoded.Decoded, actualAck)
	if f.overrideResponseSections {
		responseSections = append([]iwire.Section(nil), f.responseSections...)
	}
	return treenativewire.ClusterSubmitResult{
		ActualAck:            actualAck,
		CommittedRecoverable: f.committedRecoverable,
		CommittedApplied:     f.committedApplied,
		ResponseSections:     responseSections,
	}, f.submitErr
}

func (f *mongoClusterFakeSubmitter) snapshotCalls() []mongoClusterSubmitterCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]mongoClusterSubmitterCall(nil), f.calls...)
}

func mongoClusterFakeResponseSections(entry iwire.DeterministicEntry, actualAck iwire.AckPolicy) []iwire.Section {
	ids := mongoClusterTestIDs(entry.Sections)
	switch entry.CommandID {
	case iwire.CommandInsertBatch:
		return []iwire.Section{mongoClusterTestMetaWithAck(actualAck, "inserted_count", len(ids))}
	case iwire.CommandUpdateBSONSet:
		return []iwire.Section{mongoClusterTestMetaWithAck(actualAck, "matched_count", 1, "modified_count", 1)}
	case iwire.CommandDeleteBatch:
		return []iwire.Section{mongoClusterTestMetaWithAck(actualAck, "deleted_count", len(ids))}
	default:
		return []iwire.Section{mongoClusterTestMetaWithAck(actualAck)}
	}
}

func mongoClusterTestMeta(counts ...any) iwire.Section {
	return mongoClusterTestMetaWithAck(iwire.AckVisible, counts...)
}

func mongoClusterTestMetaWithAck(actualAck iwire.AckPolicy, counts ...any) iwire.Section {
	values := []struct {
		key   string
		value string
	}{{key: "actual_ack_policy", value: fmt.Sprint(actualAck)}}
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

func cloneMongoClusterRequestMetadata(metadata treenativewire.ClusterRequestMetadata) treenativewire.ClusterRequestMetadata {
	metadata.TraceContext = append([]byte(nil), metadata.TraceContext...)
	metadata.ClusterRouteMembers = append([]string(nil), metadata.ClusterRouteMembers...)
	return metadata
}

func mongoClusterStaticCatalogVersion(version uint64) ClusterCatalogVersionProvider {
	return func(context.Context) (uint64, error) {
		return version, nil
	}
}

func setMongoClusterTestSubmitter(server *Server, submitter *mongoClusterFakeSubmitter, catalogVersion uint64) {
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(catalogVersion)
}

func setMongoClusterAdmissionTestSubmitter(server *Server, submitter *mongoClusterAdmissionSubmitter, catalogVersion uint64) {
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(catalogVersion)
}

func assertErrmsgContains(tb testing.TB, doc wire.Document, want string) {
	tb.Helper()
	got, ok := bson.Raw(doc).Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(got, want) {
		tb.Fatalf("errmsg=%q typeOK=%v want containing %q", got, ok, want)
	}
}

func assertStringField(tb testing.TB, doc wire.Document, key, want string) {
	tb.Helper()
	got, ok := bson.Raw(doc).Lookup(key).StringValueOK()
	if !ok || got != want {
		tb.Fatalf("%s=%q typeOK=%v want %q", key, got, ok, want)
	}
}

func assertStringArrayField(tb testing.TB, doc wire.Document, key string, want []string) {
	tb.Helper()
	raw, ok := bson.Raw(doc).Lookup(key).ArrayOK()
	if !ok {
		tb.Fatalf("%s missing or non-array", key)
	}
	values, err := raw.Values()
	if err != nil {
		tb.Fatalf("%s values: %v", key, err)
	}
	if len(values) != len(want) {
		tb.Fatalf("%s len=%d want %d", key, len(values), len(want))
	}
	for i, value := range values {
		got, ok := value.StringValueOK()
		if !ok || got != want[i] {
			tb.Fatalf("%s[%d]=%q typeOK=%v want %q", key, i, got, ok, want[i])
		}
	}
}

func assertMongoOwnerBoundIndexPolicyError(tb testing.TB, doc wire.Document) {
	tb.Helper()
	assertBool(tb, doc, "treedbClusterError", true)
	assertStringField(tb, doc, "treedbErrorClass", "route_rejected")
	for _, key := range []string{
		"treedbLeaderHint",
		"treedbRouteGroup",
		"treedbRouteLeaderHint",
		"treedbRouteDatabase",
		"treedbRouteCatalog",
		"treedbRouteCollection",
		"treedbRouteShape",
		"treedbRoutePlacementMode",
		"treedbRouteKey",
		"treedbRoutePartitionId",
		"treedbRouteMembers",
		"treedbRouteTokenKnown",
		"treedbRouteToken",
	} {
		assertNoField(tb, doc, key)
	}
}

func TestMongoClusterRouteCommandErrorRedactsSensitiveRouteReasonAndMetadata(t *testing.T) {
	const secret = "tenant-secret-collection"
	response, err := mongoClusterRouteCommandError(mongoSensitiveRouteError{
		message: "route failed for " + secret,
		route: treenativewire.ClusterRouteErrorMetadata{
			Class:      "remote_owner_redirect",
			Database:   "secret-db",
			Catalog:    "secret-catalog",
			Collection: secret,
			GroupID:    "group-z",
		},
	})
	if err != nil {
		t.Fatalf("mongoClusterRouteCommandError: %v", err)
	}
	assertCommandError(t, response, "BadValue")
	assertStringField(t, response, "treedbErrorClass", "remote_owner_redirect")
	assertStringField(t, response, "treedbRouteGroup", "group-z")
	for _, value := range []string{secret, "secret-db", "secret-catalog"} {
		if raw, ok := bson.Raw(response).Lookup("errmsg").StringValueOK(); !ok || strings.Contains(raw, value) {
			t.Fatalf("errmsg=%q ok=%v exposes namespace %q", raw, ok, value)
		}
	}
	for _, key := range []string{"treedbRouteDatabase", "treedbRouteCatalog", "treedbRouteCollection"} {
		assertNoField(t, response, key)
	}
}

func assertNoField(tb testing.TB, doc wire.Document, key string) {
	tb.Helper()
	if got := bson.Raw(doc).Lookup(key); got.Type != 0 {
		tb.Fatalf("%s present with BSON type %v, want absent", key, got.Type)
	}
}

func assertMongoUsers(tb testing.TB, server *Server, want map[string]string) {
	tb.Helper()
	found := serveCommand(tb, server, 326899, bson.D{
		{Key: "find", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(tb, found)
	got := make(map[string]string, len(batch))
	for _, doc := range batch {
		id, idOK := doc.Lookup("_id").StringValueOK()
		name, nameOK := doc.Lookup("name").StringValueOK()
		if !idOK || !nameOK {
			tb.Fatalf("user doc missing string _id/name: %v", doc)
		}
		got[id] = name
	}
	if len(got) != len(want) {
		tb.Fatalf("users=%v want %v", got, want)
	}
	for id, wantName := range want {
		if gotName, ok := got[id]; !ok || gotName != wantName {
			tb.Fatalf("user %q name=%q present=%v want %q", id, gotName, ok, wantName)
		}
	}
}

func mongoClusterCallCatalogVersion(tb testing.TB, call mongoClusterSubmitterCall) uint64 {
	tb.Helper()
	version, n := binary.Uvarint(call.entry.Target.ExpectedCatalogVersion)
	if n <= 0 || n != len(call.entry.Target.ExpectedCatalogVersion) {
		tb.Fatalf("expected_catalog_version bytes=%v decode n=%d", call.entry.Target.ExpectedCatalogVersion, n)
	}
	return version
}

func mongoClusterCallIdempotencyKey(tb testing.TB, call mongoClusterSubmitterCall) string {
	tb.Helper()
	if len(call.entry.IdempotencyKey) == 0 {
		tb.Fatal("missing idempotency key")
	}
	return string(call.entry.IdempotencyKey)
}

func assertMongoClusterCallAckPolicy(tb testing.TB, call mongoClusterSubmitterCall, want iwire.AckPolicy) {
	tb.Helper()
	if call.metadata.AckPolicy != want {
		tb.Fatalf("metadata ack policy=%d want %d", call.metadata.AckPolicy, want)
	}
}

func assertMongoClusterRouteMetadata(tb testing.TB, call mongoClusterSubmitterCall, database, collection string) {
	tb.Helper()
	meta := call.metadata
	if !meta.ClusterRouteKnown {
		tb.Fatal("metadata missing cluster route")
	}
	if meta.ClusterRouteDatabase != database || meta.ClusterRouteCatalog != "default" || meta.ClusterRouteCollection != collection {
		tb.Fatalf("metadata route identity=%s/%s/%s want %s/default/%s", meta.ClusterRouteDatabase, meta.ClusterRouteCatalog, meta.ClusterRouteCollection, database, collection)
	}
	if meta.ClusterRouteShape != string(treenativewire.ClusterRouteShapeCollection) {
		tb.Fatalf("metadata route shape=%q want collection", meta.ClusterRouteShape)
	}
	if meta.ClusterRouteGroupID != "group-a" || meta.ClusterRouteLeaderHint != "node-a" || meta.ClusterRoutePlacementMode != "collection" {
		tb.Fatalf("metadata route target group=%q leader=%q mode=%q", meta.ClusterRouteGroupID, meta.ClusterRouteLeaderHint, meta.ClusterRoutePlacementMode)
	}
	if meta.ClusterRouteTokenKnown || meta.ClusterRouteToken != 0 || meta.ClusterRoutePartitionID != "" {
		tb.Fatalf("collection route unexpectedly included token metadata known/token/partition=%v/%d/%q", meta.ClusterRouteTokenKnown, meta.ClusterRouteToken, meta.ClusterRoutePartitionID)
	}
	if len(meta.ClusterRouteMembers) != 2 || meta.ClusterRouteMembers[0] != "node-a" || meta.ClusterRouteMembers[1] != "node-b" {
		tb.Fatalf("metadata route members=%v want [node-a node-b]", meta.ClusterRouteMembers)
	}
}

func assertMongoClusterTokenRouteMetadata(tb testing.TB, call mongoClusterSubmitterCall, database, collection string, mode raftplacement.PlacementModeV1, partition string, token uint64) {
	tb.Helper()
	meta := call.metadata
	if !meta.ClusterRouteKnown {
		tb.Fatal("metadata missing cluster route")
	}
	if meta.ClusterRouteDatabase != database || meta.ClusterRouteCatalog != "default" || meta.ClusterRouteCollection != collection {
		tb.Fatalf("metadata route identity=%s/%s/%s want %s/default/%s", meta.ClusterRouteDatabase, meta.ClusterRouteCatalog, meta.ClusterRouteCollection, database, collection)
	}
	if meta.ClusterRouteShape != string(treenativewire.ClusterRouteShapeToken) {
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

func mongoClusterRouteTokenForValue(tb testing.TB, value any) uint64 {
	tb.Helper()
	key, err := encodePrimaryKey(mustRawValue(tb, value))
	if err != nil {
		tb.Fatalf("encode primary key: %v", err)
	}
	return raftplacement.DocumentIDTokenV1(key)
}

func newMongoPlacementRouteTestServer(tb testing.TB, mode raftplacement.PlacementModeV1) (*Server, *mongoPlacementRouteClusterSubmitter) {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	if _, err := server.Collections.CreateCollection(server.defaultCollectionMeta("app.users")); err != nil {
		tb.Fatalf("create app.users: %v", err)
	}
	submitter := &mongoPlacementRouteClusterSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		provider:                  treenativewire.NewCatalogRouteResolverV1(mustMongoRouteTestCatalog(tb, mode)),
	}
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(60)
	return server, submitter
}

func newMongoGroupRoutedDispatcherTestServer(tb testing.TB, provider treenativewire.ClusterRouteProvider) (*Server, *mongoRecordingRaftCommandSubmitter, *mongoRecordingRaftCommandSubmitter) {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	if _, err := server.Collections.CreateCollection(server.defaultCollectionMeta("app.users")); err != nil {
		tb.Fatalf("create app.users: %v", err)
	}
	groupA := &mongoRecordingRaftCommandSubmitter{groupID: "group-a"}
	groupB := &mongoRecordingRaftCommandSubmitter{groupID: "group-b"}
	registry, err := raftcluster.NewGroupSubmitterRegistryV1([]raftcluster.GroupSubmitterV1{
		{GroupID: "group-a", Submitter: groupA},
		{GroupID: "group-b", Submitter: groupB},
	})
	if err != nil {
		tb.Fatalf("NewGroupSubmitterRegistryV1: %v", err)
	}
	dispatcher, err := raftcluster.NewCatalogMetaGroupRoutedSubmitter(registry, mongoTestCatalogRouteValidator{})
	if err != nil {
		tb.Fatalf("NewCatalogMetaGroupRoutedSubmitter: %v", err)
	}
	server.ClusterSubmitter = treenativewire.NewRoutedRaftClusterSubmitter(dispatcher, provider, server.Collections)
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(60)
	return server, groupA, groupB
}

func newMongoCatalogMetaGroupRoutedDispatcherTestServer(tb testing.TB, provider treenativewire.ClusterRouteProvider, authority *raftplacement.CatalogMetaAuthorityV1) (*Server, *mongoRecordingRaftCommandSubmitter, *mongoRecordingRaftCommandSubmitter) {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	if _, err := server.Collections.CreateCollection(server.defaultCollectionMeta("app.users")); err != nil {
		tb.Fatalf("create app.users: %v", err)
	}
	groupA := &mongoRecordingRaftCommandSubmitter{groupID: "group-a"}
	groupB := &mongoRecordingRaftCommandSubmitter{groupID: "group-b"}
	registry, err := raftcluster.NewGroupSubmitterRegistryV1([]raftcluster.GroupSubmitterV1{
		{GroupID: "group-a", Submitter: groupA},
		{GroupID: "group-b", Submitter: groupB},
	})
	if err != nil {
		tb.Fatalf("NewGroupSubmitterRegistryV1: %v", err)
	}
	dispatcher, err := raftcluster.NewCatalogMetaGroupRoutedSubmitter(registry, authority)
	if err != nil {
		tb.Fatalf("NewCatalogMetaGroupRoutedSubmitter: %v", err)
	}
	server.ClusterSubmitter = treenativewire.NewRoutedRaftClusterSubmitter(dispatcher, provider, server.Collections)
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(60)
	return server, groupA, groupB
}

func newMongoCatalogMetaAuthority(t testing.TB) (*raftplacement.CatalogMetaAuthorityV1, *raftcluster.CatalogMetaRaftProviderV1) {
	t.Helper()
	authority := raftplacement.NewCatalogMetaAuthorityV1()
	_, transport := hraft.NewInmemTransport("meta-a")
	t.Cleanup(func() { _ = transport.Close() })
	features := raftcluster.FeatureSet{
		ConfigVersion: raftcluster.SupportedConfigVersion,
		Required: []raftcluster.RequiredFeature{
			{Name: raftcluster.FeatureSingleGroupProvider, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureSingleGroupProvider]},
			{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]},
		},
	}
	provider, err := raftcluster.OpenCatalogMetaRaftProviderV1(raftcluster.CatalogMetaRaftProviderOptionsV1{
		Cluster: raftcluster.Config{
			Dir: t.TempDir(), NodeID: "meta-a", GroupID: "meta",
			Features: features,
			Peers:    []raftcluster.Peer{{ID: "meta-a", Address: "meta-a", Capabilities: features}},
		},
		State: authority, Transport: transport, Bootstrap: true,
	})
	if err != nil {
		t.Fatalf("OpenCatalogMetaRaftProviderV1: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		status, err := provider.ClusterAdmissionStatus(ctx)
		if err != nil {
			t.Fatalf("ClusterAdmissionStatus: %v", err)
		}
		if status.Leader {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait catalog meta leader: %v", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if _, _, err := provider.SubmitCatalogMetaCommandV1(ctx, mongoCatalogMetaCommand(t, 0, 1)); err != nil {
		t.Fatalf("submit initial catalog meta: %v", err)
	}
	return authority, provider
}

func mongoCatalogMetaCommand(t testing.TB, expected, epoch uint64) []byte {
	t.Helper()
	record, err := raftplacement.NewCatalogMetaRecordV1(epoch, raftplacement.CatalogV1{
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a", "node-b"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b", "node-c"}, LeaderHint: "node-b"},
		},
		Placements: []raftplacement.CollectionPlacementV1{{
			Collection: raftplacement.CollectionRefV1{Database: "app", Catalog: "default", Collection: "users"},
			Mode:       raftplacement.PlacementModeCollectionV1,
			GroupID:    "group-a",
		}},
	})
	if err != nil {
		t.Fatalf("NewCatalogMetaRecordV1: %v", err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{ExpectedEpoch: expected, Record: record})
	if err != nil {
		t.Fatalf("EncodeCatalogMetaCommandV1: %v", err)
	}
	return command
}

func newMongoRaftBridgeTestServer(tb testing.TB, admission raftcluster.AdmissionStatus) *Server {
	tb.Helper()
	return newMongoRaftBridgeTestServerWithRoute(tb, admission, nil)
}

func newMongoRaftBridgeTestServerWithRoute(tb testing.TB, admission raftcluster.AdmissionStatus, routeProvider treenativewire.ClusterRouteProvider) *Server {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          tb.TempDir(),
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		tb.Fatalf("open command WAL db: %v", err)
	}
	cluster := mongoRaftBridgeTestConfig(db)
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB:      db,
		Cluster: cluster,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err != nil {
		_ = db.Close()
		tb.Fatalf("open raft FSM: %v", err)
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
		tb.Fatalf("NewSingleGroupSubmitter: %v", err)
	}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	var submitter treenativewire.ClusterSubmitter = treenativewire.NewRaftClusterSubmitter(bridge, server.Collections)
	if routeProvider != nil {
		submitter = treenativewire.NewRoutedRaftClusterSubmitter(bridge, routeProvider, server.Collections)
	}
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = func(context.Context) (uint64, error) {
		state := db.State()
		if state == nil {
			return 0, errors.New("missing DB state")
		}
		return state.CommitSeq, nil
	}
	tb.Cleanup(func() {
		_ = fsm.Close()
		_ = db.Close()
	})
	return server
}

func mongoRaftBridgeTestConfig(db *backenddb.DB) raftcluster.Config {
	dir := ""
	if db != nil {
		dir = db.Dir()
	}
	return raftcluster.Config{
		Dir:     dir,
		NodeID:  "node-a",
		GroupID: "group-a",
		Peers: []raftcluster.Peer{
			{ID: "node-a", Address: "127.0.0.1:9401"},
			{ID: "node-b", Address: "127.0.0.1:9402"},
			{ID: "node-c", Address: "127.0.0.1:9403"},
		},
	}
}

func mustMongoRouteTestCatalog(tb testing.TB, mode raftplacement.PlacementModeV1) raftplacement.ResolvedCatalogV1 {
	tb.Helper()
	placement := raftplacement.CollectionPlacementV1{
		Collection: raftplacement.CollectionRefV1{
			Database:   "app",
			Catalog:    "default",
			Collection: "users",
		},
		Mode: mode,
	}
	if mode == raftplacement.PlacementModeCollectionV1 {
		placement.GroupID = "group-a"
	} else {
		placement.TokenPartitions = []raftplacement.TokenPartitionV1{
			{
				ID:      "p0",
				GroupID: "group-a",
				Start:   0,
				End:     ^uint64(0),
			},
		}
	}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Features: raftplacement.DefaultFeatureSet(),
		Groups: []raftplacement.GroupV1{
			{
				ID:         "group-a",
				Members:    []raftcluster.NodeID{"node-a", "node-b"},
				LeaderHint: "node-a",
			},
		},
		Placements: []raftplacement.CollectionPlacementV1{placement},
	})
	if err != nil {
		tb.Fatalf("Validate route test catalog: %v", err)
	}
	return catalog
}

func mustMongoCollectionGroupRouteCatalog(tb testing.TB) raftplacement.ResolvedCatalogV1 {
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
					Database:   "app",
					Catalog:    "default",
					Collection: "users",
				},
				Mode:    raftplacement.PlacementModeCollectionV1,
				GroupID: "group-b",
			},
		},
	})
	if err != nil {
		tb.Fatalf("Validate Mongo collection group route catalog: %v", err)
	}
	return catalog
}

func mustMongoTokenGroupRouteCatalog(tb testing.TB, mode raftplacement.PlacementModeV1, groupBStart uint64) raftplacement.ResolvedCatalogV1 {
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
					Database:   "app",
					Catalog:    "default",
					Collection: "users",
				},
				Mode:            mode,
				TokenPartitions: partitions,
			},
		},
	})
	if err != nil {
		tb.Fatalf("Validate Mongo token group route catalog: %v", err)
	}
	return catalog
}

func TestClusterAdmissionMongoLeaderRoutesThroughSubmitter(t *testing.T) {
	submitter := &mongoClusterAdmissionSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		status:                    treenativewire.ClusterLeaderAdmission(),
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterAdmissionTestSubmitter(server, submitter, 31)

	response := serveCommand(t, server, 326801, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	calls := submitter.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("submit calls=%d want create+insert", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandCreateCollection {
		t.Fatalf("first command id=%d want create_collection", got)
	}
	if got := calls[1].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("second command id=%d want insert_batch", got)
	}
	assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckVisible)
	assertMongoClusterCallAckPolicy(t, calls[1], iwire.AckVisible)
}

func TestClusterRoutePreflightMongoUsesNamespaceBeforeGatewayName(t *testing.T) {
	submitter := &mongoRoutingClusterSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		target: treenativewire.ClusterRouteTarget{
			GroupID:       "group-a",
			Members:       []string{"node-a", "node-b"},
			LeaderHint:    "node-a",
			PlacementMode: "collection",
		},
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(50)

	response := serveCommand(t, server, 326811, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)

	routes := submitter.snapshotRoutes()
	if len(routes) != 2 {
		t.Fatalf("route calls=%d want create+insert", len(routes))
	}
	if got := routes[0]; got.Database != "app" || got.Catalog != "default" || got.Collection != "users" || got.CommandID != iwire.CommandCreateCollection || got.CommandName != "create_collection" {
		t.Fatalf("first route request=%+v want app/default/users create_collection", got)
	}
	insertKey, err := encodePrimaryKey(mustRawValue(t, "u1"))
	if err != nil {
		t.Fatalf("encode primary key: %v", err)
	}
	insertToken := raftplacement.DocumentIDTokenV1(insertKey)
	if got := routes[1]; got.Database != "app" || got.Catalog != "default" || got.Collection != "users" || got.CommandID != iwire.CommandInsertBatch || got.CommandName != "insert_batch" || got.Shape != treenativewire.ClusterRouteShapeToken || !got.TokenKnown || got.Token != insertToken {
		t.Fatalf("second route request=%+v want app/default/users insert_batch token=%d", got, insertToken)
	}
	calls := submitter.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("submit calls=%d want 2", len(calls))
	}
	if got := mongoClusterCallCollectionMetaName(t, calls[0]); got != "app.users" {
		t.Fatalf("collection_meta name=%q want app.users", got)
	}
	assertMongoClusterRouteMetadata(t, calls[0], "app", "users")
	assertMongoClusterRouteMetadata(t, calls[1], "app", "users")
}

func TestClusterRoutePreflightMongoRejectsBeforeSubmitter(t *testing.T) {
	submitter := &mongoRoutingClusterSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		err:                       errors.New("tenant-secret-db/users stored at /srv/private/tenant-secret"),
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(51)

	response := serveCommand(t, server, 326812, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "NotWritablePrimary")
	assertErrmsgContains(t, response, "Mongo gateway cluster route rejected")
	assertBool(t, response, "treedbClusterError", true)
	assertStringField(t, response, "treedbErrorClass", "route_provider_rejected")
	errmsg, ok := bson.Raw(response).Lookup("errmsg").StringValueOK()
	if !ok {
		t.Fatalf("errmsg missing or non-string: %+v", response)
	}
	for _, secret := range []string{"tenant-secret-db", "users", "/srv/private/tenant-secret"} {
		if strings.Contains(errmsg, secret) {
			t.Fatalf("provider route error exposes %q: %+v", secret, response)
		}
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterRoutePreflightMongoTokenPlacementSingleIDWritesFailClosedWithoutIndexPolicy(t *testing.T) {
	tests := []struct {
		name        string
		command     iwire.CommandID
		commandName string
		run         func(testing.TB, *Server) wire.Document
	}{
		{
			name:        "insert",
			command:     iwire.CommandInsertBatch,
			commandName: "insert_batch",
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336101, bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
					{Key: "$db", Value: "app"},
				})
			},
		},
		{
			name:        "update",
			command:     iwire.CommandUpdateBSONSet,
			commandName: "update_bson_set",
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336102, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Ada"}}}}},
					}}},
					{Key: "$db", Value: "app"},
				})
			},
		},
		{
			name:        "delete",
			command:     iwire.CommandDeleteBatch,
			commandName: "delete_batch",
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336103, bson.D{
					{Key: "delete", Value: "users"},
					{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
					{Key: "$db", Value: "app"},
				})
			},
		},
	}
	for _, mode := range []raftplacement.PlacementModeV1{raftplacement.PlacementModeTokenV1, raftplacement.PlacementModeRingV1} {
		for _, tc := range tests {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				server, submitter := newMongoPlacementRouteTestServer(t, mode)
				response := tc.run(t, server)
				assertCommandError(t, response, "NotWritablePrimary")
				assertErrmsgContains(t, response, "authoritative collection and index metadata is bound")
				assertBool(t, response, "treedbClusterError", true)
				assertStringField(t, response, "treedbErrorClass", "route_rejected")
				token := mongoClusterRouteTokenForValue(t, "u1")
				routes := submitter.snapshotRoutes()
				if len(routes) != 1 {
					t.Fatalf("route calls=%d want 1", len(routes))
				}
				if got := routes[0]; got.Database != "app" || got.Catalog != "default" || got.Collection != "users" || got.CommandID != tc.command || got.CommandName != tc.commandName || got.Shape != treenativewire.ClusterRouteShapeToken || !got.TokenKnown || got.Token != token {
					t.Fatalf("route request=%+v want app/default/users %s token=%d", got, tc.commandName, token)
				}
				calls := submitter.snapshotCalls()
				if len(calls) != 0 {
					t.Fatalf("submit calls=%d want 0", len(calls))
				}
			})
		}
	}
}

func TestClusterRoutePreflightMongoCollectionPlacementAcceptsMultiIDBatch(t *testing.T) {
	server, submitter := newMongoPlacementRouteTestServer(t, raftplacement.PlacementModeCollectionV1)
	response := serveCommand(t, server, 336107, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "Grace"}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)

	routes := submitter.snapshotRoutes()
	if len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	wantTokens := []uint64{
		mongoClusterRouteTokenForValue(t, "u1"),
		mongoClusterRouteTokenForValue(t, "u2"),
	}
	if got := routes[0]; got.Shape != treenativewire.ClusterRouteShapeTokenBatch || got.TokenKnown || len(got.Tokens) != len(wantTokens) || got.Tokens[0] != wantTokens[0] || got.Tokens[1] != wantTokens[1] {
		t.Fatalf("multi-ID route request=%+v want token_batch tokens=%v", got, wantTokens)
	}
	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	assertMongoClusterRouteMetadata(t, calls[0], "app", "users")
}

func TestClusterRoutePreflightMongoTokenPlacementRejectsMultiIDWrites(t *testing.T) {
	tests := []struct {
		name       string
		wantRoutes int
		run        func(testing.TB, *Server) wire.Document
	}{
		{
			name:       "insert",
			wantRoutes: 1,
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336104, bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{
						bson.D{{Key: "_id", Value: "u1"}},
						bson.D{{Key: "_id", Value: "u2"}},
					}},
					{Key: "$db", Value: "app"},
				})
			},
		},
		{
			name: "update",
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336105, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{
						bson.D{
							{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
							{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Ada"}}}}},
						},
						bson.D{
							{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
							{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
						},
					}},
					{Key: "$db", Value: "app"},
				})
			},
		},
		{
			name: "delete",
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336106, bson.D{
					{Key: "delete", Value: "users"},
					{Key: "deletes", Value: bson.A{
						bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}},
						bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "limit", Value: int32(1)}},
					}},
					{Key: "$db", Value: "app"},
				})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, submitter := newMongoPlacementRouteTestServer(t, raftplacement.PlacementModeRingV1)
			response := tc.run(t, server)
			if ok, okOK := bson.Raw(response).Lookup("ok").DoubleOK(); !okOK || ok != 0 {
				t.Fatalf("multi write unexpectedly accepted: %s", response)
			}
			routes := submitter.snapshotRoutes()
			if len(routes) != tc.wantRoutes {
				t.Fatalf("route calls=%d want %d", len(routes), tc.wantRoutes)
			}
			if calls := submitter.snapshotCalls(); len(calls) != 0 {
				t.Fatalf("submit calls=%d want 0", len(calls))
			}
		})
	}
}

func TestClusterMultiWriteRejectsBeforeLocalLookupOrSubmit(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  bson.D
	}{
		{
			name: "update",
			cmd: bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{
				bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(1)}}}}}},
				bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(2)}}}}}},
			}}, {Key: "$db", Value: "app"}},
		},
		{
			name: "delete",
			cmd: bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{
				bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "limit", Value: int32(1)}},
				bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "limit", Value: int32(1)}},
			}}, {Key: "$db", Value: "app"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, submitter := newMongoPlacementRouteTestServer(t, raftplacement.PlacementModeRingV1)
			lookups := 0
			server.clusterCollectionLookupHook = func() { lookups++ }
			response := serveCommand(t, server, 336109, tc.cmd)
			if ok, okOK := bson.Raw(response).Lookup("ok").DoubleOK(); !okOK || ok != 0 {
				t.Fatalf("multi write unexpectedly accepted: %s", response)
			}
			if lookups != 0 {
				t.Fatalf("local collection lookups=%d want 0", lookups)
			}
			if routes := submitter.snapshotRoutes(); len(routes) != 0 {
				t.Fatalf("route calls=%d want 0", len(routes))
			}
			if calls := submitter.snapshotCalls(); len(calls) != 0 {
				t.Fatalf("submit calls=%d want 0", len(calls))
			}
		})
	}
}

func TestClusterRoutePreflightMongoShardKeyFindMapsTokenThenFailsClosed(t *testing.T) {
	for _, mode := range []raftplacement.PlacementModeV1{
		raftplacement.PlacementModeTokenV1,
		raftplacement.PlacementModeRingV1,
	} {
		t.Run(string(mode), func(t *testing.T) {
			server, submitter := newMongoPlacementRouteTestServer(t, mode)
			response := serveCommand(t, server, 336108, bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "$db", Value: "app"},
			})
			assertCommandError(t, response, "NotWritablePrimary")
			assertErrmsgContains(t, response, "Mongo gateway cluster route target for _id find is disabled until the serving collection-store identity is bound to the owner Raft proof")
			assertBool(t, response, "treedbClusterError", true)
			assertStringField(t, response, "treedbErrorClass", "route_rejected")
			routes := submitter.snapshotRoutes()
			if len(routes) != 1 {
				t.Fatalf("route calls=%d want 1", len(routes))
			}
			wantToken := mongoClusterRouteTokenForValue(t, "u1")
			if got := routes[0]; got.Database != "app" || got.Catalog != "default" || got.Collection != "users" ||
				got.CommandName != "find" || got.Shape != treenativewire.ClusterRouteShapeToken ||
				!got.TokenKnown || got.Token != wantToken {
				t.Fatalf("find route request=%+v want app/default/users token=%d", got, wantToken)
			}
			if calls := submitter.snapshotCalls(); len(calls) != 0 {
				t.Fatalf("submit calls=%d want 0", len(calls))
			}
		})
	}
}

func TestClusterRoutePreflightMongoRejectsNonShardAndSecondaryIndexReads(t *testing.T) {
	tests := []struct {
		name  string
		index *collections.IndexDefinition
	}{
		{name: "non_shard_key"},
		{
			name: "secondary_index",
			index: &collections.IndexDefinition{
				Name:      "name_1",
				Field:     "name",
				ValueType: collections.IndexValueString,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, submitter := newMongoPlacementRouteTestServer(t, raftplacement.PlacementModeRingV1)
			if tc.index != nil {
				col, err := server.Collections.OpenCollection("app.users")
				if err != nil {
					t.Fatalf("open app.users: %v", err)
				}
				if _, err := col.CreateIndex(*tc.index); err != nil {
					t.Fatalf("create test index: %v", err)
				}
			}
			response := serveCommand(t, server, 336109, bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{{Key: "name", Value: "Ada"}}},
				{Key: "$db", Value: "app"},
			})
			assertCommandError(t, response, "NotWritablePrimary")
			if routes := submitter.snapshotRoutes(); len(routes) != 0 {
				t.Fatalf("route calls=%d want 0 before unsupported query provider call", len(routes))
			}
			if calls := submitter.snapshotCalls(); len(calls) != 0 {
				t.Fatalf("submit calls=%d want 0", len(calls))
			}
		})
	}
}

func TestClusterRoutePreflightMongoRejectsNonShardKeyWrites(t *testing.T) {
	tests := []struct {
		name string
		run  func(testing.TB, *Server) wire.Document
	}{
		{
			name: "update",
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336109, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{
						bson.D{
							{Key: "q", Value: bson.D{{Key: "name", Value: "Ada"}}},
							{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "hnl"}}}}},
						},
						bson.D{
							{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
							{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "hnl"}}}}},
						},
					}},
					{Key: "$db", Value: "app"},
				})
			},
		},
		{
			name: "delete",
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336110, bson.D{
					{Key: "delete", Value: "users"},
					{Key: "deletes", Value: bson.A{
						bson.D{{Key: "q", Value: bson.D{{Key: "name", Value: "Ada"}}}, {Key: "limit", Value: int32(1)}},
						bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "limit", Value: int32(1)}},
					}},
					{Key: "$db", Value: "app"},
				})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, submitter := newMongoPlacementRouteTestServer(t, raftplacement.PlacementModeRingV1)
			response := tc.run(t, server)
			assertCommandError(t, response, "BadValue")
			if routes := submitter.snapshotRoutes(); len(routes) != 0 {
				t.Fatalf("route calls=%d want 0 before unsupported query provider call", len(routes))
			}
			if calls := submitter.snapshotCalls(); len(calls) != 0 {
				t.Fatalf("submit calls=%d want 0", len(calls))
			}
		})
	}
}

func TestClusterRoutePreflightMongoRejectsTokenRingMutationsWithoutOwnerBoundIndexPolicy(t *testing.T) {
	tests := []struct {
		name            string
		index           collections.IndexDefinition
		missingMetadata bool
		nilManager      bool
		run             func(testing.TB, *Server) wire.Document
		wantError       string
		wantRoutes      int
	}{
		{
			name: "secondary_index_update",
			index: collections.IndexDefinition{
				Name:      "name_1",
				Field:     "name",
				ValueType: collections.IndexValueString,
			},
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336111, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Ada"}}}}},
					}}},
					{Key: "$db", Value: "app"},
				})
			},
			wantError:  "authoritative collection and index metadata is bound",
			wantRoutes: 1,
		},
		{
			name: "global_unique_insert",
			index: collections.IndexDefinition{
				Name:      "email_1",
				Field:     "email",
				ValueType: collections.IndexValueString,
				Unique:    true,
			},
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336112, bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "ada@example.test"}}}},
					{Key: "$db", Value: "app"},
				})
			},
			wantError:  "authoritative collection and index metadata is bound",
			wantRoutes: 1,
		},
		{
			name:            "missing_local_metadata",
			missingMetadata: true,
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336113, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Ada"}}}}},
					}}},
					{Key: "$db", Value: "app"},
				})
			},
			wantError: "authoritative collection and index metadata is bound",
		},
		{
			name:       "missing_collection_manager",
			nilManager: true,
			run: func(tb testing.TB, server *Server) wire.Document {
				return serveCommand(tb, server, 336114, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Ada"}}}}},
					}}},
					{Key: "$db", Value: "app"},
				})
			},
			wantError:  "authoritative collection and index metadata is bound",
			wantRoutes: 1,
		},
	}
	for _, mode := range []raftplacement.PlacementModeV1{
		raftplacement.PlacementModeTokenV1,
		raftplacement.PlacementModeRingV1,
	} {
		for _, tc := range tests {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				server, submitter := newMongoPlacementRouteTestServer(t, mode)
				if tc.missingMetadata {
					db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
					if err != nil {
						t.Fatalf("open empty metadata db: %v", err)
					}
					t.Cleanup(func() { _ = db.Close() })
					server.Collections = collections.NewCollectionManager(db)
				} else if tc.nilManager {
					server.Collections = nil
				} else {
					col, err := server.Collections.OpenCollection("app.users")
					if err != nil {
						t.Fatalf("open app.users: %v", err)
					}
					if _, err := col.CreateIndex(tc.index); err != nil {
						t.Fatalf("create test index: %v", err)
					}
				}
				response := tc.run(t, server)
				assertCommandError(t, response, "NotWritablePrimary")
				assertErrmsgContains(t, response, tc.wantError)
				assertBool(t, response, "treedbClusterError", true)
				assertStringField(t, response, "treedbErrorClass", "route_rejected")
				if routes := submitter.snapshotRoutes(); len(routes) != tc.wantRoutes {
					t.Fatalf("route calls=%d want %d", len(routes), tc.wantRoutes)
				}
				if calls := submitter.snapshotCalls(); len(calls) != 0 {
					t.Fatalf("submit calls=%d want 0", len(calls))
				}
			})
		}
	}
}

func TestMongoGroupRoutedDispatcherRoutesCollectionBatchWrite(t *testing.T) {
	provider := &mongoStaticClusterRouteProvider{
		target: treenativewire.ClusterRouteTarget{
			GroupID:       "group-b",
			Members:       []string{"node-b", "node-c"},
			LeaderHint:    "node-b",
			PlacementMode: "collection",
			Shape:         treenativewire.ClusterRouteShapeCollection,
		},
	}
	server, groupA, groupB := newMongoGroupRoutedDispatcherTestServer(t, provider)
	response := serveCommand(t, server, 336301, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "Grace"}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 2)
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	calls := groupB.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("group-b calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("group-b command=%d want insert_batch", got)
	}
	meta := calls[0].metadata
	if !meta.ClusterRouteKnown || meta.ClusterRouteShape != string(treenativewire.ClusterRouteShapeCollection) || meta.ClusterRouteGroupID != "group-b" || meta.ClusterRouteLeaderHint != "node-b" || meta.ClusterRoutePlacementMode != "collection" {
		t.Fatalf("group-b collection route metadata=%+v want collection route to group-b/node-b", meta)
	}
	routes := provider.snapshotRoutes()
	if len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	if got := routes[0]; got.Shape != treenativewire.ClusterRouteShapeTokenBatch || len(got.Tokens) != 2 {
		t.Fatalf("route request=%+v want token_batch classification for two IDs", got)
	}
}

func TestCatalogMetaMongoAndSharedSubmitProofMatrix(t *testing.T) {
	authority, metaRaft := newMongoCatalogMetaAuthority(t)
	proof, err := authority.CurrentCatalogProof(context.Background())
	if err != nil {
		t.Fatalf("CurrentCatalogProof: %v", err)
	}
	provider, err := treenativewire.NewCatalogMetaClusterRouteProvider(authority, authority.CurrentCatalogProof, metaRaft)
	if err != nil {
		t.Fatalf("NewCatalogMetaClusterRouteProvider: %v", err)
	}
	server, groupA, groupB := newMongoCatalogMetaGroupRoutedDispatcherTestServer(t, provider, authority)
	response := serveCommand(t, server, 336350, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "Grace"}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	if calls := groupB.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-b calls=%d want 0", len(calls))
	}
	calls := groupA.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("group-a calls=%d want 1", len(calls))
	}
	if got := calls[0].metadata; got.CatalogMetaEpoch != proof.Epoch || got.CatalogMetaDigest != proof.Digest {
		t.Fatalf("Mongo shared-submit proof=%d/%s want %d/%s", got.CatalogMetaEpoch, got.CatalogMetaDigest, proof.Epoch, proof.Digest)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := metaRaft.SubmitCatalogMetaCommandV1(ctx, mongoCatalogMetaCommand(t, 1, 2)); err != nil {
		t.Fatalf("submit catalog epoch 2: %v", err)
	}
	staleProvider, err := treenativewire.NewCatalogMetaClusterRouteProvider(authority, func(context.Context) (raftplacement.CatalogProofV1, error) {
		return proof, nil
	}, metaRaft)
	if err != nil {
		t.Fatalf("NewCatalogMetaClusterRouteProvider stale: %v", err)
	}
	staleServer, staleA, staleB := newMongoCatalogMetaGroupRoutedDispatcherTestServer(t, staleProvider, authority)
	staleResponse := serveCommand(t, staleServer, 336351, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u3"}, {Key: "name", Value: "Katherine"}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, staleResponse, "NotWritablePrimary")
	if len(staleA.snapshotCalls()) != 0 || len(staleB.snapshotCalls()) != 0 {
		t.Fatal("stale Mongo catalog proof reached a group submitter")
	}
}

func TestMongoGroupRoutedDispatcherRejectsSingleTokenWriteWithoutOwnerBoundIndexPolicy(t *testing.T) {
	token := mongoClusterRouteTokenForValue(t, "u1")
	provider := &mongoStaticClusterRouteProvider{
		target: treenativewire.ClusterRouteTarget{
			GroupID:       "group-b",
			Members:       []string{"node-b", "node-c"},
			LeaderHint:    "node-b",
			PlacementMode: "token",
			RouteKey:      string(raftplacement.RouteKeyDocumentIDV1),
			Shape:         treenativewire.ClusterRouteShapeToken,
			TokenKnown:    true,
			Token:         token,
			PartitionID:   "p0",
		},
	}
	server, groupA, groupB := newMongoGroupRoutedDispatcherTestServer(t, provider)
	response := serveCommand(t, server, 336302, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "NotWritablePrimary")
	assertErrmsgContains(t, response, "authoritative collection and index metadata is bound")
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	calls := groupB.snapshotCalls()
	if len(calls) != 0 {
		t.Fatalf("group-b calls=%d want 0", len(calls))
	}
}

func BenchmarkCatalogMetaMongoMutationAdmission(b *testing.B) {
	authority, metaRaft := newMongoCatalogMetaAuthority(b)
	provider, err := treenativewire.NewCatalogMetaClusterRouteProvider(authority, authority.CurrentCatalogProof, metaRaft)
	if err != nil {
		b.Fatalf("NewCatalogMetaClusterRouteProvider: %v", err)
	}
	server, groupA, groupB := newMongoCatalogMetaGroupRoutedDispatcherTestServer(b, provider, authority)
	groupA.discard = true
	groupB.discard = true
	request := bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "Grace"}},
		}},
		{Key: "$db", Value: "app"},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err := serveCommandResult(server, 336500, request)
		if err != nil {
			b.Fatal(err)
		}
		if ok, typeOK := bson.Raw(response).Lookup("ok").DoubleOK(); !typeOK || ok != 1 {
			b.Fatalf("Mongo response ok=%v typeOK=%v", ok, typeOK)
		}
	}
}

func TestMongoGroupRoutedDispatcherCatalogRejectsSingleTokenOwnerWithoutIndexPolicy(t *testing.T) {
	token := mongoClusterRouteTokenForValue(t, "u1")
	provider := newMongoStaticCatalogRouteProviderForTest(mustMongoTokenGroupRouteCatalog(t, raftplacement.PlacementModeRingV1, token))
	server, groupA, groupB := newMongoGroupRoutedDispatcherTestServer(t, provider)
	response := serveCommand(t, server, 336305, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "NotWritablePrimary")
	assertErrmsgContains(t, response, "authoritative collection and index metadata is bound")
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	calls := groupB.snapshotCalls()
	if len(calls) != 0 {
		t.Fatalf("group-b calls=%d want 0", len(calls))
	}
}

func TestMongoRoutedMetadataReadsFailClosedBeforeLocalCatalogObservation(t *testing.T) {
	server, submitter := newMongoPlacementRouteTestServer(t, raftplacement.PlacementModeRingV1)
	commands := []struct {
		name    string
		request bson.D
	}{
		{
			name: "listCollections",
			request: bson.D{
				{Key: "listCollections", Value: int32(1)},
				{Key: "$db", Value: "app"},
			},
		},
		{
			name: "listDatabases",
			request: bson.D{
				{Key: "listDatabases", Value: int32(1)},
				{Key: "$db", Value: "admin"},
			},
		},
		{
			name: "listIndexes",
			request: bson.D{
				{Key: "listIndexes", Value: "users"},
				{Key: "$db", Value: "app"},
			},
		},
	}
	for i, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			response := serveCommand(t, server, int32(336320+i), command.request)
			assertCommandError(t, response, "NotWritablePrimary")
			assertErrmsgContains(t, response, "authoritative catalog metadata")
			assertBool(t, response, "treedbClusterError", true)
			assertStringField(t, response, "treedbErrorClass", "route_rejected")
			for _, key := range []string{
				"treedbRouteDatabase",
				"treedbRouteCatalog",
				"treedbRouteCollection",
			} {
				assertNoField(t, response, key)
			}
		})
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("metadata route calls=%+v want none", routes)
	}
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("metadata submit calls=%d want none", len(calls))
	}
}

func TestMongoGroupRoutedDispatcherRejectsUnknownGroupBeforeSubmit(t *testing.T) {
	token := mongoClusterRouteTokenForValue(t, "u1")
	provider := &mongoStaticClusterRouteProvider{
		target: treenativewire.ClusterRouteTarget{
			GroupID:       "group-z",
			Members:       []string{"node-z"},
			LeaderHint:    "node-z",
			PlacementMode: "token",
			RouteKey:      string(raftplacement.RouteKeyDocumentIDV1),
			Shape:         treenativewire.ClusterRouteShapeToken,
			TokenKnown:    true,
			Token:         token,
			PartitionID:   "p0",
		},
	}
	server, groupA, groupB := newMongoGroupRoutedDispatcherTestServer(t, provider)
	response := serveCommand(t, server, 336303, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "NotWritablePrimary")
	assertErrmsgContains(t, response, "authoritative collection and index metadata is bound")
	assertMongoOwnerBoundIndexPolicyError(t, response)
	users, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("OpenCollection app.users: %v", err)
	}
	key, err := encodePrimaryKey(mustRawValue(t, "u1"))
	if err != nil {
		t.Fatalf("encode primary key: %v", err)
	}
	if got, err := users.Get(key); err != nil || got != nil {
		t.Fatalf("local app.users Get(u1)=%v err=%v want missing document", bson.Raw(got), err)
	}
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	if calls := groupB.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-b calls=%d want 0", len(calls))
	}
}

func TestMongoGroupRoutedDispatcherUnknownOwnerErrorsBeforeSubmit(t *testing.T) {
	token := mongoClusterRouteTokenForValue(t, "u1")
	provider := &mongoStaticClusterRouteProvider{
		target: treenativewire.ClusterRouteTarget{
			GroupID:       "group-z",
			PlacementMode: string(raftplacement.PlacementModeRingV1),
			RouteKey:      string(raftplacement.RouteKeyDocumentIDV1),
			Shape:         treenativewire.ClusterRouteShapeToken,
			TokenKnown:    true,
			Token:         token,
			PartitionID:   "p0",
		},
	}
	server, groupA, groupB := newMongoGroupRoutedDispatcherTestServer(t, provider)
	response := serveCommand(t, server, 336313, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "NotWritablePrimary")
	assertErrmsgContains(t, response, "authoritative collection and index metadata is bound")
	assertMongoOwnerBoundIndexPolicyError(t, response)
	users, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("OpenCollection app.users: %v", err)
	}
	key, err := encodePrimaryKey(mustRawValue(t, "u1"))
	if err != nil {
		t.Fatalf("encode primary key: %v", err)
	}
	if got, err := users.Get(key); err != nil || got != nil {
		t.Fatalf("local app.users Get(u1)=%v err=%v want missing document", bson.Raw(got), err)
	}
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	if calls := groupB.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-b calls=%d want 0", len(calls))
	}
}

func TestMongoGroupRoutedDispatcherMissingOwnerErrorsBeforeSubmit(t *testing.T) {
	token := mongoClusterRouteTokenForValue(t, "u1")
	provider := &mongoStaticClusterRouteProvider{
		target: treenativewire.ClusterRouteTarget{
			PlacementMode: string(raftplacement.PlacementModeRingV1),
			RouteKey:      string(raftplacement.RouteKeyDocumentIDV1),
			Shape:         treenativewire.ClusterRouteShapeToken,
			TokenKnown:    true,
			Token:         token,
			PartitionID:   "p0",
		},
	}
	server, groupA, groupB := newMongoGroupRoutedDispatcherTestServer(t, provider)
	response := serveCommand(t, server, 336314, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "NotWritablePrimary")
	assertErrmsgContains(t, response, "cluster route rejected")
	assertBool(t, response, "treedbClusterError", true)
	assertStringField(t, response, "treedbErrorClass", "missing_owner")
	assertNoField(t, response, "treedbRouteGroup")
	assertNoField(t, response, "treedbLeaderHint")
	assertNoField(t, response, "treedbRouteLeaderHint")
	users, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("OpenCollection app.users: %v", err)
	}
	key, err := encodePrimaryKey(mustRawValue(t, "u1"))
	if err != nil {
		t.Fatalf("encode primary key: %v", err)
	}
	if got, err := users.Get(key); err != nil || got != nil {
		t.Fatalf("local app.users Get(u1)=%v err=%v want missing document", bson.Raw(got), err)
	}
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	if calls := groupB.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-b calls=%d want 0", len(calls))
	}
}

func TestMongoGroupRoutedDispatcherRemoteOwnerErrorsForMutations(t *testing.T) {
	provider := &mongoRemoteOwnerRouteProvider{}
	server, groupA, groupB := newMongoGroupRoutedDispatcherTestServer(t, provider)
	users, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("OpenCollection app.users: %v", err)
	}
	key1, err := encodePrimaryKey(mustRawValue(t, "u1"))
	if err != nil {
		t.Fatalf("encode u1 primary key: %v", err)
	}
	original := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}})
	if _, err := users.InsertBatchValidatedBSON([][]byte{key1}, [][]byte{original}); err != nil {
		t.Fatalf("seed InsertBatchValidatedBSON: %v", err)
	}

	insertResponse := serveCommand(t, server, 336304, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "Grace"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, insertResponse, "NotWritablePrimary")
	assertErrmsgContains(t, insertResponse, "authoritative collection and index metadata is bound")
	assertMongoOwnerBoundIndexPolicyError(t, insertResponse)
	key2, err := encodePrimaryKey(mustRawValue(t, "u2"))
	if err != nil {
		t.Fatalf("encode u2 primary key: %v", err)
	}
	if got, err := users.Get(key2); err != nil || got != nil {
		t.Fatalf("local app.users Get(u2)=%v err=%v want missing document", bson.Raw(got), err)
	}

	updateResponse := serveCommand(t, server, 336305, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}, {Key: "profile", Value: bson.D{{Key: "city", Value: "HNL"}}}, {Key: "tags", Value: bson.A{"a"}}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, updateResponse, "NotWritablePrimary")
	assertMongoOwnerBoundIndexPolicyError(t, updateResponse)

	deleteResponse := serveCommand(t, server, 336306, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, deleteResponse, "NotWritablePrimary")
	assertMongoOwnerBoundIndexPolicyError(t, deleteResponse)

	got, err := users.Get(key1)
	if err != nil {
		t.Fatalf("local app.users Get(u1): %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("local app.users u1 changed after remote-owner mutations: got %v want %v", bson.Raw(got), bson.Raw(original))
	}
	if calls := groupA.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	if calls := groupB.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("group-b calls=%d want 0", len(calls))
	}
	if routes := provider.snapshotRoutes(); len(routes) != 3 {
		t.Fatalf("route calls=%d want insert+update+delete", len(routes))
	}
}

func TestClusterAdmissionMongoFollowerRejectsWritesBeforeLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 326802, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	}))

	submitter := &mongoClusterAdmissionSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		status:                    treenativewire.ClusterFollowerAdmission("node-a:27017", "not leader"),
	}
	setMongoClusterAdmissionTestSubmitter(server, submitter, 32)

	createResponse := serveCommand(t, server, 326803, bson.D{
		{Key: "create", Value: "admins"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, createResponse, "NotWritablePrimary")
	assertErrmsgContains(t, createResponse, "node-a:27017")
	assertBool(t, createResponse, "treedbClusterError", true)
	assertStringField(t, createResponse, "treedbErrorClass", "not_leader")
	assertStringField(t, createResponse, "treedbLeaderHint", "node-a:27017")
	if _, err := server.Collections.OpenCollection("app.admins"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection app.admins err=%v want collection not found", err)
	}

	insertResponse := serveCommand(t, server, 326804, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "Grace"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, insertResponse, "NotWritablePrimary")
	assertMongoUsers(t, server, map[string]string{"u1": "Ada"})

	updateResponse := serveCommand(t, server, 326805, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, updateResponse, "NotWritablePrimary")
	assertMongoUsers(t, server, map[string]string{"u1": "Ada"})

	deleteResponse := serveCommand(t, server, 326806, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, deleteResponse, "NotWritablePrimary")
	assertMongoUsers(t, server, map[string]string{"u1": "Ada"})
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterAdmissionMongoFollowerRejectsMissingCollectionNoOps(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterAdmissionSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		status:                    treenativewire.ClusterFollowerAdmission("node-a:27017", "not leader"),
	}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterAdmissionTestSubmitter(server, submitter, 34)

	updateResponse := serveCommand(t, server, 326808, bson.D{
		{Key: "update", Value: "missing"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, updateResponse, "NotWritablePrimary")
	assertErrmsgContains(t, updateResponse, "node-a:27017")

	deleteResponse := serveCommand(t, server, 326809, bson.D{
		{Key: "delete", Value: "missing"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, deleteResponse, "NotWritablePrimary")
	assertErrmsgContains(t, deleteResponse, "node-a:27017")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
	if _, err := server.Collections.OpenCollection("app.missing"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection app.missing err=%v want collection not found", err)
	}
}

func TestClusterAdmissionMongoUnavailableRejectsBeforeSubmit(t *testing.T) {
	submitter := &mongoClusterAdmissionSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		status:                    treenativewire.ClusterUnavailableAdmission("cluster admission unavailable"),
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterAdmissionTestSubmitter(server, submitter, 33)

	response := serveCommand(t, server, 326807, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "ShutdownInProgress")
	assertErrmsgContains(t, response, "cluster admission unavailable")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterHelloReflectsAdmissionWritablePrimary(t *testing.T) {
	tests := []struct {
		name                string
		submitter           treenativewire.ClusterSubmitter
		wantWritablePrimary bool
	}{
		{
			name: "leader",
			submitter: &mongoClusterAdmissionSubmitter{
				mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
				status:                    treenativewire.ClusterLeaderAdmission(),
			},
			wantWritablePrimary: true,
		},
		{
			name: "follower",
			submitter: &mongoClusterAdmissionSubmitter{
				mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
				status:                    treenativewire.ClusterFollowerAdmission("node-a:27017", "not leader"),
			},
		},
		{
			name: "unavailable",
			submitter: &mongoClusterAdmissionSubmitter{
				mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
				status:                    treenativewire.ClusterUnavailableAdmission("cluster unavailable"),
			},
		},
		{
			name:      "admission error",
			submitter: &mongoClusterFakeSubmitter{admissionErr: errors.New("admission check failed")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := NewServer()
			server.ClusterSubmitter = tc.submitter
			response := serveCommand(t, server, 326810, bson.D{
				{Key: "hello", Value: int32(1)},
				{Key: "$db", Value: "admin"},
			})
			assertOK(t, response)
			assertBool(t, response, "isWritablePrimary", tc.wantWritablePrimary)
			assertBool(t, response, "ismaster", tc.wantWritablePrimary)
		})
	}
}

func mongoClusterCallCollectionMetaName(tb testing.TB, call mongoClusterSubmitterCall) string {
	tb.Helper()
	raw := call.entry.Target.CollectionMeta
	version, off, err := clusterReadUvarint(raw)
	if err != nil {
		tb.Fatalf("collection_meta version: %v", err)
	}
	if version != clusterCollectionMetaV5 {
		tb.Fatalf("collection_meta version=%d want %d", version, clusterCollectionMetaV5)
	}
	name, err := clusterReadString(raw, &off)
	if err != nil {
		tb.Fatalf("collection_meta name: %v", err)
	}
	return name
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
	setMongoClusterTestSubmitter(server, submitter, 7)

	response := serveCommand(t, server, 325801, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)

	calls := submitter.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("submit calls=%d want 2", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandCreateCollection {
		t.Fatalf("first command id=%d want create_collection", got)
	}
	if got := mongoClusterCallCollectionMetaName(t, calls[0]); got != "app.users" {
		t.Fatalf("collection_meta name=%q want app.users", got)
	}
	if got := calls[1].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("command id=%d want insert_batch", got)
	}
	if got := mongoClusterCallCatalogVersion(t, calls[0]); got != 7 {
		t.Fatalf("create expected catalog version=%d want 7", got)
	}
	if got := mongoClusterCallCatalogVersion(t, calls[1]); got != 7 {
		t.Fatalf("insert expected catalog version=%d want 7", got)
	}
	assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckVisible)
	assertMongoClusterCallAckPolicy(t, calls[1], iwire.AckVisible)
	if _, err := server.Collections.OpenCollection("app.users"); err == nil {
		t.Fatal("cluster insert created local collection")
	} else if err != collections.ErrCollectionNotFound {
		t.Fatalf("OpenCollection after cluster insert err=%v want collection not found", err)
	}
}

func TestClusterSubmitterInsertSkipsAutoCreateForKnownLocalCollection(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	if _, err := server.Collections.CreateCollection(server.defaultCollectionMeta("app.users")); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	submitter := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(server, submitter, 20)
	response := serveCommand(t, server, 325820, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("command id=%d want insert_batch", got)
	}
	assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckVisible)
}

func TestClusterSubmitterIdempotencyKeysIncludeServerNonce(t *testing.T) {
	serverA := NewServer()
	serverA.ClusterIdempotencyNonce = "gateway-epoch-a"
	serverA.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	submitterA := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(serverA, submitterA, 21)

	serverB := NewServer()
	serverB.ClusterIdempotencyNonce = "gateway-epoch-b"
	serverB.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	submitterB := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(serverB, submitterB, 21)

	for _, server := range []*Server{serverA, serverB} {
		response := serveCommand(t, server, 325821, bson.D{
			{Key: "insert", Value: "users"},
			{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
			{Key: "$db", Value: "app"},
		})
		assertOK(t, response)
	}

	callsA := submitterA.snapshotCalls()
	callsB := submitterB.snapshotCalls()
	if len(callsA) != 2 || len(callsB) != 2 {
		t.Fatalf("submit calls A=%d B=%d want 2 each", len(callsA), len(callsB))
	}
	keyA := mongoClusterCallIdempotencyKey(t, callsA[1])
	keyB := mongoClusterCallIdempotencyKey(t, callsB[1])
	if keyA == keyB {
		t.Fatalf("idempotency keys matched across gateway epochs: %q", keyA)
	}
	if !strings.HasPrefix(keyA, "mongo-gateway/gateway-epoch-a/insert_batch/") {
		t.Fatalf("idempotency key A=%q missing gateway epoch", keyA)
	}
	if !strings.HasPrefix(keyB, "mongo-gateway/gateway-epoch-b/insert_batch/") {
		t.Fatalf("idempotency key B=%q missing gateway epoch", keyB)
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
	setMongoClusterTestSubmitter(server, submitter, 8)
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

	if calls := submitter.snapshotCalls(); len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
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

func TestClusterSubmitterGenericUpdatesFailClosedWithoutLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 325830, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int32(10)}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	}))
	submitter := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(server, submitter, 8)
	assertCommandError(t, serveCommand(t, server, 325829, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: int32(10)}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}}}}},
		{Key: "$db", Value: "app"},
	}), "BadValue")
	for _, update := range []bson.D{
		{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}},
		{{Key: "$set", Value: bson.D{{Key: "profile.name", Value: "Grace"}}}},
		{{Key: "$set", Value: bson.D{{Key: "code", Value: deeplyNestedCodeWithScopeValue(mongoMutationMaxBSONNesting - 1)}}}},
		{{Key: "$push", Value: bson.D{{Key: "events", Value: "login"}}}},
		{{Key: "name", Value: "Grace"}},
	} {
		response := serveCommand(t, server, 325831, bson.D{
			{Key: "update", Value: "users"},
			{Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: update}}}},
			{Key: "$db", Value: "app"},
		})
		assertCommandError(t, response, "BadValue")
	}
	assertCommandError(t, serveCommand(t, server, 325834, bson.D{
		{Key: "update", Value: "missing"},
		{Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: int32(10)}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}}}}},
		{Key: "$db", Value: "app"},
	}), "BadValue")
	assertCommandError(t, serveCommand(t, server, 325833, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}}, {Key: "upsert", Value: true}}}},
		{Key: "$db", Value: "app"},
	}), "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("generic updates submitted %d cluster calls", len(calls))
	}
	if _, err := server.Collections.OpenCollection("app.missing"); err == nil {
		t.Fatal("generic routed update created missing collection")
	}
	found := serveCommand(t, server, 325832, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	doc := cursorFirstBatch(t, found)[0]
	if age, _ := doc.Lookup("age").Int32OK(); age != 10 {
		t.Fatalf("local age=%d want 10", age)
	}
	if name, _ := doc.Lookup("name").StringValueOK(); name != "Ada" {
		t.Fatalf("local name=%q want Ada", name)
	}
}

func TestClusterSubmitterUpdateSubmitsPriorOrderedItemsBeforeUnsupported(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 18)

	response := serveCommand(t, server, 325818, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
				{Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")

	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterUpdateSubmitsPriorOrderedItemsBeforeUnsupportedUpsert(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 18)

	response := serveCommand(t, server, 325819, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Ada"}}}}},
				{Key: "upsert", Value: true},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")

	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterUpdateMissingCollectionReturnsZeroCounts(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 23)

	response := serveCommand(t, server, 325823, bson.D{
		{Key: "update", Value: "missing"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	assertInt32(t, response, "nModified", 0)

	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterMissingCollectionRejectsUnsupportedUpdates(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 24)
	for i, update := range []bson.D{
		{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}},
		{{Key: "name", Value: "Grace"}},
		{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}},
	} {
		item := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: update}}
		if i == 2 {
			item = append(item, bson.E{Key: "upsert", Value: true})
		}
		response := serveCommand(t, server, int32(325824+i), bson.D{
			{Key: "update", Value: "missing"},
			{Key: "updates", Value: bson.A{item}},
			{Key: "$db", Value: "app"},
		})
		assertCommandError(t, response, "BadValue")
	}
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
	if _, err := server.Collections.OpenCollection("app.missing"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("missing collection was created: %v", err)
	}
}

func TestClusterSubmitterDeleteRoutesCommandEntry(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 9)

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
	assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckVisible)
}

func TestClusterSubmitterDeleteRequiresLimitBeforeSubmit(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 9)

	response := serveCommand(t, server, 325806, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "FailedToParse")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("missing limit submitted %d cluster mutations", len(calls))
	}
}

func TestClusterSubmitterDeleteRejectsMultiLimitBeforeSubmit(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 9)
	response := serveCommand(t, server, 325807, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(0)}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("limit:0 submitted %d cluster mutations", len(calls))
	}
}

func TestClusterSubmitterDeleteDeduplicatesDuplicateIDs(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 19)

	response := serveCommand(t, server, 325819, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}},
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterDeleteSubmitsPriorOrderedItemsBeforeUnsupported(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 22)

	response := serveCommand(t, server, 325822, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}},
			bson.D{{Key: "q", Value: bson.D{{Key: "name", Value: "Ada"}}}, {Key: "limit", Value: int32(1)}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")

	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterDeleteMissingCollectionReturnsZeroCount(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 24)

	response := serveCommand(t, server, 325824, bson.D{
		{Key: "delete", Value: "missing"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)

	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterMajorityWriteConcernRejectsMissingCollectionNoops(t *testing.T) {
	t.Run("update", func(t *testing.T) {
		db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		defer func() { _ = db.Close() }()

		submitter := &mongoClusterFakeSubmitter{actualAck: iwire.AckRaftCommitted, committedRecoverable: true}
		server := NewServer()
		server.Collections = collections.NewCollectionManager(db)
		server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
		setMongoClusterTestSubmitter(server, submitter, 30)

		response := serveCommand(t, server, 325832, bson.D{
			{Key: "update", Value: "missing"},
			{Key: "updates", Value: bson.A{bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
			}}},
			{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
			{Key: "$db", Value: "app"},
		})
		assertCommandError(t, response, "WriteConcernFailed")
		assertErrmsgContains(t, response, "missing collection no-op")
		if calls := submitter.snapshotCalls(); len(calls) != 0 {
			t.Fatalf("submit calls=%d want 0", len(calls))
		}
	})

	t.Run("delete", func(t *testing.T) {
		db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		defer func() { _ = db.Close() }()

		submitter := &mongoClusterFakeSubmitter{actualAck: iwire.AckRaftCommitted, committedRecoverable: true}
		server := NewServer()
		server.Collections = collections.NewCollectionManager(db)
		server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
		setMongoClusterTestSubmitter(server, submitter, 31)

		response := serveCommand(t, server, 325833, bson.D{
			{Key: "delete", Value: "missing"},
			{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
			{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
			{Key: "$db", Value: "app"},
		})
		assertCommandError(t, response, "WriteConcernFailed")
		assertErrmsgContains(t, response, "missing collection no-op")
		if calls := submitter.snapshotCalls(); len(calls) != 0 {
			t.Fatalf("submit calls=%d want 0", len(calls))
		}
	})
}

func TestClusterSubmitterVisibleAckConfirmsCommittedVectorMutationV1(t *testing.T) {
	submitter := &mongoConfirmingVectorPartitionSubmitterV1{mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{committedApplied: true}}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(32)

	response := serveCommand(t, server, 325834, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	submitter.mu.Lock()
	confirmations := append([]iwire.CommandID(nil), submitter.confirmations...)
	submitter.mu.Unlock()
	if !reflect.DeepEqual(confirmations, []iwire.CommandID{iwire.CommandCreateCollection}) {
		t.Fatalf("confirmations=%v want create_collection", confirmations)
	}
}

func TestClusterSubmitterConfirmsCommittedVectorMutationBeforeReturningPostApplyErrorV1(t *testing.T) {
	submitErr := errors.New("post-apply catalog refresh failed")
	submitter := &mongoConfirmingVectorPartitionSubmitterV1{mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{committedApplied: true, submitErr: submitErr}}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(32)
	raw, err := bson.Marshal(bson.D{{Key: "create", Value: "users"}, {Key: "$db", Value: "app"}})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	response, err := server.clusterCreateCollectionResponse(context.Background(), wire.Document(raw))
	if err != nil {
		t.Fatalf("cluster create response: %v", err)
	}
	assertErrmsgContains(t, response, submitErr.Error())
	submitter.mu.Lock()
	confirmations := append([]iwire.CommandID(nil), submitter.confirmations...)
	submitter.mu.Unlock()
	if !reflect.DeepEqual(confirmations, []iwire.CommandID{iwire.CommandCreateCollection}) {
		t.Fatalf("confirmations=%v want create_collection", confirmations)
	}
}

func TestLegacyRaftClusterSubmitterMongoRequiresVectorAdmissionFromClusterFeatureV1(t *testing.T) {
	features := raftcluster.DefaultFeatureSet()
	features.Required = append(features.Required, raftcluster.RequiredFeature{
		Name:    raftcluster.FeatureVectorPartitionLifecycle,
		Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle],
	})
	bridge := &mongoRecordingRaftCommandSubmitter{groupID: "group-a", features: features}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = treenativewire.NewRaftClusterSubmitter(bridge)
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(32)

	response := serveCommand(t, server, 325836, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "NotWritablePrimary")
	assertErrmsgContains(t, response, "vector partition lifecycle admission is not configured")
	if calls := bridge.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("legacy Mongo constructor submitted %d entries before required lifecycle admission", len(calls))
	}
}

func TestClusterSubmitterCommittedVectorConfirmationOutlivesClientCancellationV1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	submitter := &mongoConfirmingVectorPartitionSubmitterV1{mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{committedApplied: true, submitHook: cancel}}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(32)
	raw, err := bson.Marshal(bson.D{{Key: "create", Value: "users"}, {Key: "$db", Value: "app"}})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	response, err := server.clusterCreateCollectionResponse(ctx, wire.Document(raw))
	if err != nil {
		t.Fatalf("cluster create after client cancellation: %v", err)
	}
	assertOK(t, response)
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("request context err=%v want canceled", ctx.Err())
	}
	submitter.mu.Lock()
	defer submitter.mu.Unlock()
	if len(submitter.confirmationContextErrors) != 1 || submitter.confirmationContextErrors[0] != nil {
		t.Fatalf("confirmation context errors=%v want [nil]", submitter.confirmationContextErrors)
	}
}

func TestClusterSubmitterRecoverableOnlyDoesNotConfirmVectorMutationV1(t *testing.T) {
	submitter := &mongoConfirmingVectorPartitionSubmitterV1{mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{committedRecoverable: true}}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(32)

	response := serveCommand(t, server, 325835, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	submitter.mu.Lock()
	confirmationCount := len(submitter.confirmations)
	submitter.mu.Unlock()
	if confirmationCount != 0 {
		t.Fatalf("recoverable-only submission confirmations=%d want 0", confirmationCount)
	}
}

func TestClusterSubmitterWriteConcernAccepted(t *testing.T) {
	tests := []struct {
		name                 string
		writeConcern         any
		actualAck            iwire.AckPolicy
		committedRecoverable bool
		wantAck              iwire.AckPolicy
	}{
		{name: "absent", wantAck: iwire.AckVisible},
		{name: "default document", writeConcern: bson.D{}, wantAck: iwire.AckVisible},
		{name: "w one", writeConcern: bson.D{{Key: "w", Value: int32(1)}}, wantAck: iwire.AckVisible},
		{name: "majority", writeConcern: bson.D{{Key: "w", Value: "majority"}}, actualAck: iwire.AckRaftCommitted, committedRecoverable: true, wantAck: iwire.AckRaftCommitted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			submitter := &mongoClusterFakeSubmitter{
				actualAck:            tc.actualAck,
				committedRecoverable: tc.committedRecoverable,
			}
			server := NewServer()
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
			setMongoClusterTestSubmitter(server, submitter, 10)

			command := bson.D{
				{Key: "create", Value: "users"},
				{Key: "$db", Value: "app"},
			}
			if tc.writeConcern != nil {
				command = append(command, bson.E{Key: "writeConcern", Value: tc.writeConcern})
			}
			response := serveCommand(t, server, 325806, command)
			assertOK(t, response)
			calls := submitter.snapshotCalls()
			if len(calls) != 1 {
				t.Fatalf("submit calls=%d want 1", len(calls))
			}
			assertMongoClusterCallAckPolicy(t, calls[0], tc.wantAck)
		})
	}
}

func TestClusterSubmitterConcreteBridgeMajorityInsert(t *testing.T) {
	server := newMongoRaftBridgeTestServer(t, raftcluster.LeaderAdmission())

	response := serveCommand(t, server, 325807, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	if got, ok := bson.Raw(response).Lookup("n").Int32OK(); !ok || got != 1 {
		t.Fatalf("insert n=%d ok=%v want 1/true", got, ok)
	}
	assertMongoUsers(t, server, map[string]string{"u1": "Ada"})
}

func TestClusterSubmitterConcreteBridgeRouteGroupMismatchNotWritablePrimary(t *testing.T) {
	provider := &mongoRoutingClusterSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		target: treenativewire.ClusterRouteTarget{
			GroupID:       "group-b",
			Members:       []string{"node-c", "node-d"},
			LeaderHint:    "node-c",
			PlacementMode: "collection",
		},
	}
	server := newMongoRaftBridgeTestServerWithRoute(t, raftcluster.LeaderAdmission(), provider)

	response := serveCommand(t, server, 325808, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "NotWritablePrimary")
	assertErrmsgContains(t, response, "cluster route rejected")
	assertBool(t, response, "treedbClusterError", true)
	assertStringField(t, response, "treedbErrorClass", "remote_owner_redirect")
	assertStringField(t, response, "treedbLeaderHint", "node-c")
	assertStringField(t, response, "treedbRouteGroup", "group-b")
	if _, err := server.Collections.OpenCollection("app.users"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection app.users after route mismatch err=%v want collection not found", err)
	}
	routes := provider.snapshotRoutes()
	if len(routes) != 1 {
		t.Fatalf("route calls=%d want 1", len(routes))
	}
	if got := routes[0]; got.Database != "app" || got.Catalog != "default" || got.Collection != "users" || got.CommandID != iwire.CommandCreateCollection {
		t.Fatalf("route request=%+v want app/default/users create_collection", got)
	}
}

func TestMongoClusterMutationCommandErrorPreservesCommitAmbiguousOverDuplicateKey(t *testing.T) {
	clusterErr := errors.Join(
		&iwire.ProtocolError{Code: iwire.ErrCommitAmbiguous, Reason: "commit reached consensus but response failed"},
		collections.ErrDuplicateDocumentID,
	)
	response, err := mongoClusterMutationCommandError(clusterErr)
	if err != nil {
		t.Fatalf("mongoClusterMutationCommandError: %v", err)
	}
	assertCommandError(t, response, "ShutdownInProgress")
	assertStringField(t, response, "treedbErrorClass", "commit_ambiguous")
}

func TestMongoClusterMutationCommandErrorPreservesCollectionCommitAmbiguousOverDuplicateKey(t *testing.T) {
	clusterErr := &collections.CommitAmbiguousError{
		Operation: "insert",
		Err:       collections.ErrDuplicateDocumentID,
	}
	response, err := mongoClusterMutationCommandError(clusterErr)
	if err != nil {
		t.Fatalf("mongoClusterMutationCommandError: %v", err)
	}
	assertCommandError(t, response, "ShutdownInProgress")
	assertStringField(t, response, "treedbErrorClass", "commit_ambiguous")
}

func TestMongoClusterMutationCommandErrorClassifiesLeaderHintBeforeRouteText(t *testing.T) {
	clusterErr := &iwire.ProtocolError{Code: iwire.ErrReadOnly, Reason: "not leader; leader_hint=router-1:27017"}
	response, err := mongoClusterMutationCommandError(clusterErr)
	if err != nil {
		t.Fatalf("mongoClusterMutationCommandError: %v", err)
	}
	assertCommandError(t, response, "NotWritablePrimary")
	assertStringField(t, response, "treedbErrorClass", "not_leader")
	assertStringField(t, response, "treedbLeaderHint", "router-1:27017")
}

func TestMongoClusterMutationCommandErrorPreservesRawLeaderHint(t *testing.T) {
	clusterErr := &iwire.ProtocolError{Code: iwire.ErrReadOnly, Reason: "not leader; leader_hint=node+z"}
	response, err := mongoClusterMutationCommandError(clusterErr)
	if err != nil {
		t.Fatalf("mongoClusterMutationCommandError: %v", err)
	}
	assertCommandError(t, response, "NotWritablePrimary")
	assertStringField(t, response, "treedbErrorClass", "not_leader")
	assertStringField(t, response, "treedbLeaderHint", "node+z")
}

func TestMongoClusterMutationCommandErrorDecodesRouteMetadataLeaderHint(t *testing.T) {
	clusterErr := &iwire.ProtocolError{Code: iwire.ErrReadOnly, Reason: "cluster route rejected; route_error_class=remote_owner_redirect route_group=group+z leader_hint=node+z"}
	response, err := mongoClusterMutationCommandError(clusterErr)
	if err != nil {
		t.Fatalf("mongoClusterMutationCommandError: %v", err)
	}
	assertCommandError(t, response, "NotWritablePrimary")
	assertStringField(t, response, "treedbErrorClass", "remote_owner_redirect")
	assertStringField(t, response, "treedbLeaderHint", "node z")
	assertStringField(t, response, "treedbRouteGroup", "group z")
	assertStringField(t, response, "treedbRouteLeaderHint", "node z")
}

func TestMongoClusterMutationCommandErrorSurfacesRemoteWireRouteMetadata(t *testing.T) {
	clusterErr := &treenativewire.WireError{
		Code:    iwire.ErrReadOnly,
		Message: "cluster route rejected; route_error_class=remote_owner_redirect route_group=group+z leader_hint=node+z",
	}
	response, err := mongoClusterMutationCommandError(clusterErr)
	if err != nil {
		t.Fatalf("mongoClusterMutationCommandError: %v", err)
	}
	assertCommandError(t, response, "NotWritablePrimary")
	assertBool(t, response, "treedbClusterError", true)
	assertStringField(t, response, "treedbErrorClass", "remote_owner_redirect")
	assertStringField(t, response, "treedbLeaderHint", "node z")
	assertStringField(t, response, "treedbRouteGroup", "group z")
	assertStringField(t, response, "treedbRouteLeaderHint", "node z")
}

func TestMongoClusterMutationCommandErrorClassifiesQueryRouteAsRouteRejected(t *testing.T) {
	clusterErr := &iwire.ProtocolError{Code: iwire.ErrReadOnly, Reason: "cluster query route shape is not supported before scatter planning"}
	response, err := mongoClusterMutationCommandError(clusterErr)
	if err != nil {
		t.Fatalf("mongoClusterMutationCommandError: %v", err)
	}
	assertCommandError(t, response, "NotWritablePrimary")
	assertStringField(t, response, "treedbErrorClass", "route_rejected")
}

func TestMongoClusterMutationCommandErrorClassifiesHintlessFollowerAsNotLeader(t *testing.T) {
	clusterErr := &iwire.ProtocolError{Code: iwire.ErrReadOnly, Reason: "not cluster leader"}
	response, err := mongoClusterMutationCommandError(clusterErr)
	if err != nil {
		t.Fatalf("mongoClusterMutationCommandError: %v", err)
	}
	assertCommandError(t, response, "NotWritablePrimary")
	assertStringField(t, response, "treedbErrorClass", "not_leader")
	assertNoField(t, response, "treedbLeaderHint")
}

func TestMongoClusterLeaderHintUsesStructuredCarrier(t *testing.T) {
	err := mongoClusterLeaderHintTestError{leaderHint: "node-c"}
	if got := mongoClusterLeaderHint(err); got != "node-c" {
		t.Fatalf("mongoClusterLeaderHint=%q want node-c", got)
	}
}

func TestMongoClusterMutationCommandErrorDoesNotLabelLocalConfigError(t *testing.T) {
	response, err := mongoClusterMutationCommandError(errors.New("Mongo gateway cluster submitter is not configured"))
	if err != nil {
		t.Fatalf("mongoClusterMutationCommandError: %v", err)
	}
	assertCommandError(t, response, "BadValue")
	assertNoField(t, response, "treedbClusterError")
	assertNoField(t, response, "treedbErrorClass")
	assertNoField(t, response, "treedbLeaderHint")
}

type mongoClusterLeaderHintTestError struct {
	leaderHint string
}

func (e mongoClusterLeaderHintTestError) Error() string {
	return "route group rejected"
}

func (e mongoClusterLeaderHintTestError) ClusterLeaderHint() string {
	return e.leaderHint
}

func TestClusterSubmitterRejectsUnsupportedWriteConcernBeforeSubmitOrLocalMutation(t *testing.T) {
	tests := []struct {
		name         string
		writeConcern any
		extraFields  bson.D
	}{
		{name: "writeConcern not document", writeConcern: "majority"},
		{name: "journaling", writeConcern: bson.D{{Key: "j", Value: true}}},
		{name: "wtimeout", writeConcern: bson.D{{Key: "wtimeout", Value: int32(1)}}},
		{name: "wtimeoutMS", writeConcern: bson.D{{Key: "wtimeoutMS", Value: int32(1)}}},
		{name: "unacknowledged", writeConcern: bson.D{{Key: "w", Value: int32(0)}}},
		{name: "numeric greater than one", writeConcern: bson.D{{Key: "w", Value: int32(2)}}},
		{name: "tag string", writeConcern: bson.D{{Key: "w", Value: "rack-a"}}},
		{name: "malformed w type", writeConcern: bson.D{{Key: "w", Value: true}}},
		{name: "unknown option", writeConcern: bson.D{{Key: "fsync", Value: true}}},
		{name: "retryable write marker", extraFields: bson.D{{Key: "txnNumber", Value: int64(1)}}},
		{name: "transaction start marker", extraFields: bson.D{{Key: "startTransaction", Value: true}}},
		{name: "transaction autocommit marker", extraFields: bson.D{{Key: "autocommit", Value: false}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = db.Close() }()

			server := NewServer()
			server.Collections = collections.NewCollectionManager(db)
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
			assertOK(t, serveCommand(t, server, 325806, bson.D{
				{Key: "insert", Value: "users"},
				{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
				{Key: "$db", Value: "app"},
			}))

			submitter := &mongoClusterFakeSubmitter{}
			setMongoClusterTestSubmitter(server, submitter, 10)
			command := bson.D{
				{Key: "insert", Value: "users"},
				{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "Grace"}}}},
				{Key: "$db", Value: "app"},
			}
			if tc.writeConcern != nil {
				command = append(command, bson.E{Key: "writeConcern", Value: tc.writeConcern})
			}
			command = append(command, tc.extraFields...)
			response := serveCommand(t, server, 325807, command)
			assertCommandError(t, response, "BadValue")
			if calls := submitter.snapshotCalls(); len(calls) != 0 {
				t.Fatalf("submit calls=%d want 0", len(calls))
			}
			assertMongoUsers(t, server, map[string]string{"u1": "Ada"})
		})
	}
}

func TestClusterSubmitterMajorityWriteConcernRoutesAckPolicy(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		submitter := &mongoClusterFakeSubmitter{actualAck: iwire.AckRaftCommitted, committedRecoverable: true}
		server := NewServer()
		server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
		setMongoClusterTestSubmitter(server, submitter, 25)

		response := serveCommand(t, server, 325825, bson.D{
			{Key: "create", Value: "users"},
			{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
			{Key: "$db", Value: "app"},
		})
		assertOK(t, response)
		calls := submitter.snapshotCalls()
		if len(calls) != 1 {
			t.Fatalf("submit calls=%d want 1", len(calls))
		}
		if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandCreateCollection {
			t.Fatalf("command id=%d want create_collection", got)
		}
		assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckRaftCommitted)
	})

	t.Run("insert", func(t *testing.T) {
		submitter := &mongoClusterFakeSubmitter{actualAck: iwire.AckRaftCommitted, committedRecoverable: true}
		server := NewServer()
		server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
		setMongoClusterTestSubmitter(server, submitter, 26)

		response := serveCommand(t, server, 325826, bson.D{
			{Key: "insert", Value: "users"},
			{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
			{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
			{Key: "$db", Value: "app"},
		})
		assertOK(t, response)
		assertInt32(t, response, "n", 1)
		calls := submitter.snapshotCalls()
		if len(calls) != 2 {
			t.Fatalf("submit calls=%d want create+insert", len(calls))
		}
		if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandCreateCollection {
			t.Fatalf("first command id=%d want create_collection", got)
		}
		if got := calls[1].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
			t.Fatalf("second command id=%d want insert_batch", got)
		}
		assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckRaftCommitted)
		assertMongoClusterCallAckPolicy(t, calls[1], iwire.AckRaftCommitted)
	})

	t.Run("update", func(t *testing.T) {
		db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		defer func() { _ = db.Close() }()

		server := NewServer()
		server.Collections = collections.NewCollectionManager(db)
		server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
		assertOK(t, serveCommand(t, server, 325827, bson.D{
			{Key: "insert", Value: "users"},
			{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
			{Key: "$db", Value: "app"},
		}))

		submitter := &mongoClusterFakeSubmitter{actualAck: iwire.AckRaftCommitted, committedRecoverable: true}
		setMongoClusterTestSubmitter(server, submitter, 27)
		response := serveCommand(t, server, 325828, bson.D{
			{Key: "update", Value: "users"},
			{Key: "updates", Value: bson.A{bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
			}}},
			{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
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
		assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckRaftCommitted)
		assertMongoUsers(t, server, map[string]string{"u1": "Ada"})
	})

	t.Run("delete", func(t *testing.T) {
		db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		defer func() { _ = db.Close() }()

		server := NewServer()
		server.Collections = collections.NewCollectionManager(db)
		server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
		assertOK(t, serveCommand(t, server, 325829, bson.D{
			{Key: "insert", Value: "users"},
			{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
			{Key: "$db", Value: "app"},
		}))

		submitter := &mongoClusterFakeSubmitter{actualAck: iwire.AckRaftCommitted, committedRecoverable: true}
		setMongoClusterTestSubmitter(server, submitter, 28)
		response := serveCommand(t, server, 325830, bson.D{
			{Key: "delete", Value: "users"},
			{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
			{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
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
		assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckRaftCommitted)
		assertMongoUsers(t, server, map[string]string{"u1": "Ada"})
	})
}

func TestClusterSubmitterMajorityWriteConcernRequiresRaftCommittedProof(t *testing.T) {
	tests := []struct {
		name                 string
		actualAck            iwire.AckPolicy
		committedRecoverable bool
		wantOK               bool
	}{
		{name: "raft committed recoverable", actualAck: iwire.AckRaftCommitted, committedRecoverable: true, wantOK: true},
		{name: "visible ack", actualAck: iwire.AckVisible, committedRecoverable: true},
		{name: "missing recoverability", actualAck: iwire.AckRaftCommitted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			submitter := &mongoClusterFakeSubmitter{
				actualAck:            tc.actualAck,
				committedRecoverable: tc.committedRecoverable,
			}
			server := NewServer()
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
			setMongoClusterTestSubmitter(server, submitter, 29)

			response := serveCommand(t, server, 325831, bson.D{
				{Key: "create", Value: "users"},
				{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
				{Key: "$db", Value: "app"},
			})
			if tc.wantOK {
				assertOK(t, response)
			} else {
				assertCommandError(t, response, "WriteConcernFailed")
			}
			calls := submitter.snapshotCalls()
			if len(calls) != 1 {
				t.Fatalf("submit calls=%d want 1", len(calls))
			}
			assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckRaftCommitted)
		})
	}
}

func TestClusterSubmitterUsesRequestContext(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 11)

	raw, err := bson.Marshal(bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response, err := server.commandResponse(ctx, "insert", wire.Document(raw), nil, 1)
	if err != nil {
		t.Fatalf("commandResponse: %v", err)
	}
	assertCommandError(t, response, "BadValue")

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if !errors.Is(calls[0].ctxErr, context.Canceled) {
		t.Fatalf("submit context err=%v want context canceled", calls[0].ctxErr)
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
	setMongoClusterTestSubmitter(server, submitter, 12)
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

func TestClusterSubmitterCreateRoutesCommandEntryNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 13)

	response := serveCommand(t, server, 325810, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandCreateCollection {
		t.Fatalf("command id=%d want create_collection", got)
	}
	if got := mongoClusterCallCatalogVersion(t, calls[0]); got != 13 {
		t.Fatalf("expected catalog version=%d want 13", got)
	}
	if got := mongoClusterCallCollectionMetaName(t, calls[0]); got != "app.users" {
		t.Fatalf("collection_meta name=%q want app.users", got)
	}
	assertMongoClusterCallAckPolicy(t, calls[0], iwire.AckVisible)
	if _, err := server.Collections.OpenCollection("app.users"); err == nil {
		t.Fatal("cluster create created local collection")
	} else if err != collections.ErrCollectionNotFound {
		t.Fatalf("OpenCollection after cluster create err=%v want collection not found", err)
	}
}

func TestClusterSubmitterRejectsIndexDDLNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 325811, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "name", Value: int32(1)}}},
			{Key: "name", Value: "name_1"},
		}}},
		{Key: "$db", Value: "app"},
	}))

	submitter := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(server, submitter, 14)
	createResponse := serveCommand(t, server, 325812, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "name", Value: "age_1"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, createResponse, "BadValue")
	assertErrmsgContains(t, createResponse, "does not support secondary or global unique index DDL")
	compoundResponse := serveCommand(t, server, 325816, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(-1)}}},
			{Key: "name", Value: "tenant_1_createdAt_-1"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, compoundResponse, "BadValue")
	assertErrmsgContains(t, compoundResponse, "does not support secondary or global unique index DDL")
	uniqueResponse := serveCommand(t, server, 325814, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"},
			{Key: "unique", Value: true},
			{Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, uniqueResponse, "BadValue")
	assertErrmsgContains(t, uniqueResponse, "does not support secondary or global unique index DDL")
	dropResponse := serveCommand(t, server, 325813, bson.D{
		{Key: "dropIndexes", Value: "users"},
		{Key: "index", Value: "name_1"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, dropResponse, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open local collection: %v", err)
	}
	indexes := col.MetaView().Indexes
	if len(indexes) != 1 || indexes[0].Name != "name_1" {
		t.Fatalf("local indexes after rejected cluster DDL=%+v want only name_1", indexes)
	}
}

func TestClusterSubmitterRequiresCatalogVersionProvider(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter

	response := serveCommand(t, server, 325814, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterRequiresInsertedCount(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{
		responseSections:         []iwire.Section{mongoClusterTestMeta()},
		overrideResponseSections: true,
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 15)

	response := serveCommand(t, server, 325815, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	calls := submitter.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("submit calls=%d want 2", len(calls))
	}
	if got := calls[1].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("second command id=%d want insert_batch", got)
	}
}

func TestClusterSubmitterRejectsAckPolicyMismatch(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{
		actualAck:                iwire.AckSynced,
		responseSections:         []iwire.Section{mongoClusterTestMeta("inserted_count", 1)},
		overrideResponseSections: true,
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 16)

	response := serveCommand(t, server, 325816, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
}

func TestClusterSubmitterRejectsRaftCommittedForVisibleRequest(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{
		actualAck:            iwire.AckRaftCommitted,
		committedRecoverable: true,
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 17)

	response := serveCommand(t, server, 325817, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "WriteConcernFailed")
	if calls := submitter.snapshotCalls(); len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
}
